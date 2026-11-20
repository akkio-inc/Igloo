package federation

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TestLoopbackTokenMessage checks that the proxies and gateways, which any
// process on the host can connect to, refuse at once a message claiming
// to carry the token in a huge body, rather than allocating and waiting
// for it before checking the token.
func TestLoopbackTokenMessage(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	token := newToken()

	pg := New("crm", Postgres)
	pg.Host, pg.Database, pg.User = "127.0.0.1", "db", "u"
	p, err := newProxy(pg, token, "", log)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()

	sf := New("sf", Snowflake)
	sf.Account, sf.User, sf.Database, sf.Password = "myorg-acct", "u", "db", "pw"
	g, err := newSnowflakeGateway(sf, token, "", log)
	if err != nil {
		t.Fatal(err)
	}
	defer g.close()

	for name, port := range map[string]int{"proxy": p.port(), "gateway": g.port()} {
		conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			t.Fatal(err)
		}
		fe := pgproto3.NewFrontend(conn, conn)
		fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersion30,
			Parameters: map[string]string{"user": "igloo", "database": "x"}})
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if msg, err := fe.Receive(); err != nil {
			t.Fatalf("%s: %v", name, err)
		} else if _, ok := msg.(*pgproto3.AuthenticationCleartextPassword); !ok {
			t.Fatalf("%s: %T", name, msg)
		}
		// A password message of a gigabyte, of which nothing follows.
		header := []byte{'p', 0, 0, 0, 0}
		binary.BigEndian.PutUint32(header[1:], 1<<30)
		if _, err := conn.Write(header); err != nil {
			t.Fatal(err)
		}
		// The connection is closed, not kept waiting for the body.
		_, err = conn.Read(make([]byte, 1))
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%s waits for the body of an oversized token message", name)
		} else if err == nil {
			t.Errorf("%s answered an oversized token message", name)
		}
		_ = conn.Close()
	}
}

// TestListenLoopbackPastTheHighPorts checks that the proxies and gateways
// find a port DuckDB's extensions accept on a system that hands its ports
// out in order and has reached those above maxPort, as macOS does: 20
// tries, each port given back at once, met 20 such ports in a row, and no
// connection was attached.
func TestListenLoopbackPastTheHighPorts(t *testing.T) {
	real := listenAny
	t.Cleanup(func() { listenAny = real })
	// The system offers 40 ports above maxPort before any other.
	next, offered := maxPort+1, 0
	listenAny = func() (net.Listener, error) {
		for offered < 40 && next <= 65535 {
			ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(next)))
			next++
			if err == nil {
				offered++
				return ln, nil
			}
		}
		return real()
	}
	ln, err := listenLoopback()
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	if port := ln.Addr().(*net.TCPAddr).Port; port > maxPort {
		t.Errorf("listening on port %d, above the highest DuckDB's extensions accept, %d", port, maxPort)
	}
	if offered != 40 {
		t.Errorf("the system offered %d ports above %d, want 40", offered, maxPort)
	}
}

// TestAttachWithoutAPort checks that Igloo starts without a connection whose
// proxy or gateway cannot listen, with the reason kept. The proxy's nil
// pointer became an endpoint that was not nil, which Attach then closed:
// Igloo stopped with a nil dereference where it should have started.
func TestAttachWithoutAPort(t *testing.T) {
	installExtensions(t)
	real := listenAny
	t.Cleanup(func() { listenAny = real })
	listenAny = func() (net.Listener, error) { return nil, errors.New("too many open files") }
	store := NewMemoryStore()
	sf := New("sf", Snowflake)
	sf.Account, sf.User, sf.Database, sf.Password = "myorg-acct", "u", "db", "pw"
	pg := New("crm", Postgres)
	pg.Host, pg.Database, pg.User, pg.Password = "127.0.0.1", "db", "u", "pw"
	for _, c := range []Connection{sf, pg} {
		if err := store.Create(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	m, eng := startEngine(t, store)
	expect(t, eng, "SELECT 1", "1")
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, name := range []string{"sf", "crm"} {
		if why := m.failed[name]; !strings.Contains(why, "too many open files") {
			t.Errorf("connection %s: left out with reason %q, want the listener's error", name, why)
		}
	}
}
