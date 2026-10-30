package pgwire

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

// testLoginTimeout is the login timeout of these tests' servers, long
// enough for a login that is not left hanging on a busy machine.
const testLoginTimeout = 2 * time.Second

// stalledLogin connects as user and starts a password login, which it
// then leaves hanging: it reads the server's request for SCRAM and sends
// nothing more. With tlsConfig, it asks for TLS first.
func stalledLogin(t *testing.T, addr, user string, tlsConfig *tls.Config) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if tlsConfig != nil {
		req := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), sslRequestCode)
		if _, err := c.Write(req); err != nil {
			t.Fatal(err)
		}
		answer := make([]byte, 1)
		if _, err := io.ReadFull(c, answer); err != nil || answer[0] != 'S' {
			t.Fatalf("SSLRequest: %q, %v", answer, err)
		}
		tc := tls.Client(c, tlsConfig)
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		c = tc
	}
	body := binary.BigEndian.AppendUint32(nil, 3<<16)
	for _, s := range []string{"user", user, "database", "igloo", ""} {
		body = append(append(body, s...), 0)
	}
	if _, err := c.Write(append(binary.BigEndian.AppendUint32(nil, uint32(4+len(body))), body...)); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 5)
	if _, err := io.ReadFull(c, header); err != nil {
		t.Fatal(err)
	}
	msg := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
	if _, err := io.ReadFull(c, msg); err != nil {
		t.Fatal(err)
	}
	if header[0] != 'R' || binary.BigEndian.Uint32(msg) != authSASL {
		t.Fatalf("expected a SCRAM request, got %q %q", header[0], msg)
	}
	return c
}

// closedByServer waits up to within for the server to close c, from which
// the client reads nothing.
func closedByServer(c net.Conn, within time.Duration) error {
	_ = c.SetReadDeadline(time.Now().Add(within))
	_, err := c.Read(make([]byte, 1))
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		return fmt.Errorf("still open after %s", within)
	case err == nil:
		return errors.New("the server sent something")
	}
	return nil
}

// Logins that clients leave hanging end after the login timeout, and with
// them their hold on the failed-login limits: a client stalling as many
// password logins for a user as it may have failures used to keep that
// user from signing in with a password for as long as it liked.
func TestStalledLoginsTimeOut(t *testing.T) {
	t.Parallel()
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("tls=%v", encrypted), func(t *testing.T) {
			t.Parallel()
			var clientTLS *tls.Config
			ts := startServer(t, func(o *Options) {
				o.LoginTimeout = testLoginTimeout
				if encrypted {
					o.TLS = selfSignedTLS(t)
					clientTLS = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // a test's own certificate
				}
			})
			var stalled []net.Conn
			for range userFailures {
				stalled = append(stalled, stalledLogin(t, ts.addr, "igloo", clientTLS))
			}
			for _, c := range stalled {
				if err := closedByServer(c, testLoginTimeout+5*time.Second); err != nil {
					t.Errorf("a stalled login: %v", err)
				}
			}
			ts.srv.limiter.mu.Lock()
			running := len(ts.srv.limiter.running)
			ts.srv.limiter.mu.Unlock()
			if running != 0 {
				t.Errorf("%d logins still count as running", running)
			}

			connURL := ts.url("igloo", adminPassword)
			if encrypted {
				connURL = strings.Replace(connURL, "sslmode=disable", "sslmode=require", 1)
			}
			conn := connect(t, connURL, pgx.QueryExecModeSimpleProtocol)
			mustExec(t, conn, "SELECT 1")
			// A session, once signed in, has no deadline.
			time.Sleep(testLoginTimeout + 500*time.Millisecond)
			mustExec(t, conn, "SELECT 1")
		})
	}
}

// Connections that send nothing count against the connections that may be
// signing in at once, of which one address may have half, and are closed
// after the login timeout: a client could otherwise open as many as it
// liked, each holding a 16 MiB buffer, outside MaxConnections.
func TestSilentConnectionsAreLimited(t *testing.T) {
	t.Parallel()
	ts := startServer(t, func(o *Options) {
		o.MaxConnections = 4
		o.LoginTimeout = testLoginTimeout
	})
	// From loopback, they count only toward the limit of all.
	var silent []net.Conn
	for range minStarting {
		c, err := net.Dial("tcp", ts.addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		silent = append(silent, c)
	}
	// They count once accepted.
	for deadline := time.Now().Add(testLoginTimeout / 2); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		ts.srv.startup.mu.Lock()
		open := ts.srv.startup.open
		ts.srv.startup.mu.Unlock()
		if open == len(silent) {
			break
		}
	}
	if _, err := tryConnect(ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "53300" {
		t.Errorf("a login past the limit: %v", err)
	}
	for _, c := range silent {
		if err := closedByServer(c, testLoginTimeout+5*time.Second); err != nil {
			t.Errorf("a silent connection: %v", err)
		}
	}
	conn := connect(t, ts.url("igloo", adminPassword), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SELECT 1")
}

// addrConn is a connection from addr, for the limits alone.
type addrConn struct {
	net.Conn
	addr net.Addr
}

func (c addrConn) RemoteAddr() net.Addr        { return c.addr }
func (c addrConn) SetDeadline(time.Time) error { return nil }

// Connections from one public address may hold half of the sign-in slots.
// Those from a loopback or private address, which the scale-to-zero
// gateway, a pooler or a Kubernetes Service gives every client it passes
// on, may hold them all.
func TestSignInSlotsPerAddress(t *testing.T) {
	t.Parallel()
	for addr, want := range map[string]int{
		"203.0.113.7": minStarting / 2,
		"10.0.0.5":    minStarting,
		"127.0.0.1":   minStarting,
		"100.64.1.2":  minStarting,
	} {
		l := newStartupLimits(time.Minute, 4, nil)
		admitted := 0
		for range minStarting + 1 {
			c := l.admit(addrConn{addr: &net.TCPAddr{IP: net.ParseIP(addr), Port: 50000}})
			if !c.(*startupConn).refused {
				admitted++
			}
		}
		if admitted != want {
			t.Errorf("from %s: %d signing in at once, want %d", addr, admitted, want)
		}
	}
}

// A connection's read buffer is small; a message larger than it is read
// all the same, up to the largest a client may send.
func TestReadBufferIsSmall(t *testing.T) {
	t.Parallel()
	r := buffer.NewReader(slog.New(slog.NewTextHandler(io.Discard, nil)), strings.NewReader(""), 0)
	if size := r.Buffer.(*bufio.Reader).Size(); size > 64<<10 {
		t.Errorf("a connection's read buffer is %d bytes", size)
	}
	if r.MaxMessageSize != buffer.DefaultBufferSize {
		t.Errorf("largest message: %d bytes", r.MaxMessageSize)
	}

	ts := startServer(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			var n int
			if err := conn.QueryRow(t.Context(), "SELECT length('"+strings.Repeat("x", 1<<20)+"')").Scan(&n); err != nil || n != 1<<20 {
				t.Errorf("a 1 MiB query: %d, %v", n, err)
			}
		})
	}
}
