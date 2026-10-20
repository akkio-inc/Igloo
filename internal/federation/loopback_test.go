package federation

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
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
