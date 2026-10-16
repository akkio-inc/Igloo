package federation

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A proxy stands between DuckDB and one connection's remote database.
// DuckDB attaches the connection as a database on 127.0.0.1 at the proxy's
// port, signing in with a random token; the proxy checks the token, signs
// in to the remote database with the connection's current settings, and
// from then on relays the protocol, watching just enough of it to:
//
//   - give up on a remote database that leaves a statement unanswered for
//     longer than the connection's query_timeout (the remote database is
//     also told to stop statements after that long), so a database that
//     hangs cannot hold DuckDB's workers forever;
//   - make every remote session read-only, whatever DuckDB sends;
//   - refuse a MySQL server's request to upload a local file (LOAD DATA
//     LOCAL INFILE), which DuckDB's MySQL client would otherwise honor.
//
// DuckDB's extensions keep their connections in pools, so most queries do
// not sign in again.

// keepAlive is how often an idle connection to a remote database is probed,
// so one whose host vanished is noticed.
const keepAlive = 15 * time.Second

// silenceGrace is added to a connection's query_timeout before the proxy
// gives up on a silent remote database, so the database's own statement
// timeout normally ends the statement first, with a clearer error.
const silenceGrace = 10 * time.Second

// tokens are the proxy tokens of this process, which Scrub removes from
// messages. DuckDB's extensions quote the whole connection string, password
// included, when they fail to connect.
var tokens sync.Map

// Scrub replaces proxy tokens in s with ***.
func Scrub(s string) string {
	tokens.Range(func(k, _ any) bool {
		s = strings.ReplaceAll(s, k.(string), "***")
		return true
	})
	return s
}

// newToken returns a random token for DuckDB to sign in to proxies with.
func newToken() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	t := hex.EncodeToString(b[:])
	tokens.Store(t, true)
	return t
}

// proxy relays DuckDB's connections to one connection's remote database.
type proxy struct {
	name  string
	ln    net.Listener
	token string
	// attached is the connection as DuckDB attached it: its type and
	// database stay as they were until a restart.
	attached Connection
	secrets  string
	log      *slog.Logger
	// failures counts failed sign-ins to the remote database in a row,
	// and lastErr says why the last one failed; see signedIn.
	failures atomic.Int32
	lastErr  atomic.Pointer[string]

	mu       sync.Mutex
	current  Connection
	sessions map[*relay]bool
	closed   bool
	// params are what a PostgreSQL server reported the last time Igloo
	// signed in (see servePostgres); greeting and status are what a MySQL
	// server did (see serveMySQL).
	params   map[string]string
	greeting *mysqlGreeting
	status   uint16
}

// probe signs in to the remote database and out again: a check that it can
// be reached, which also teaches the proxy what the server reports.
func (p *proxy) probe() error {
	c := p.settings()
	switch c.Type {
	case Postgres:
		conn, _, _, _, err := p.signInPostgres(c, nil)
		if err != nil {
			return err
		}
		return conn.Close()
	case MySQL:
		up, g, err := p.dialMySQL(c)
		if err != nil {
			return err
		}
		defer up.conn.Close() //nolint:errcheck
		// A client's answer, with utf8mb4 (45) as the character set.
		hs := &handshakeResponse{maxPacket: 1 << 24, charset: 45}
		flags := g.caps & (capProtocol41 | capSecureConnection | capPluginAuth | capPluginAuthLenenc | capTransactions | capMultiResults)
		if _, err := p.signInMySQL(up, g, c, flags, hs); err != nil {
			return err
		}
		return up.writePacket(0, []byte{comQuit})
	}
	return fmt.Errorf("unsupported type %q", c.Type)
}

// newProxy listens for DuckDB's connections for c. secrets is the
// directory of password_secret files.
func newProxy(c Connection, token, secrets string, log *slog.Logger) (*proxy, error) {
	ln, err := listenLoopback()
	if err != nil {
		return nil, fmt.Errorf("connection %s: listen for DuckDB: %w", c.Name, err)
	}
	p := &proxy{
		name: c.Name, ln: ln, token: token, attached: c, current: c, secrets: secrets,
		log: log.With("connection", c.Name), sessions: map[*relay]bool{},
	}
	go p.serve()
	return p, nil
}

// maxPort is the highest port DuckDB's MySQL extension accepts.
const maxPort = 65353

// listenLoopback listens on a free port of 127.0.0.1 that DuckDB's
// extensions accept.
func listenLoopback() (net.Listener, error) {
	for range 20 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil || ln.Addr().(*net.TCPAddr).Port <= maxPort {
			return ln, err
		}
		_ = ln.Close()
	}
	return nil, errors.New("no free port")
}

// port is the port DuckDB connects to.
func (p *proxy) port() int { return p.ln.Addr().(*net.TCPAddr).Port }

// settings returns the connection's current settings, with its attached
// type and database.
func (p *proxy) settings() Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.current
	c.Type, c.Database, c.Schemas = p.attached.Type, p.attached.Database, p.attached.Schemas
	return c
}

// update applies new settings. Sessions signed in with the old ones and not
// in the middle of a statement are closed, so DuckDB signs in again with the
// new ones; busy ones close when their statement ends.
func (p *proxy) update(c Connection) {
	p.mu.Lock()
	p.current = c
	sessions := make([]*relay, 0, len(p.sessions))
	for r := range p.sessions {
		sessions = append(sessions, r)
	}
	p.mu.Unlock()
	p.failures.Store(0)
	for _, r := range sessions {
		r.retire()
	}
}

// close stops the proxy and every session through it.
func (p *proxy) close() {
	p.mu.Lock()
	p.closed = true
	sessions := p.sessions
	p.sessions = map[*relay]bool{}
	p.mu.Unlock()
	_ = p.ln.Close()
	for r := range sessions {
		r.close()
	}
}

func (p *proxy) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				p.log.Warn("proxy stopped accepting connections", "err", err)
			}
			return
		}
		go p.handle(conn)
	}
}

func (p *proxy) handle(client net.Conn) {
	c := p.settings()
	var err error
	switch c.Type {
	case Postgres:
		err = p.servePostgres(client, c)
	case MySQL:
		err = p.serveMySQL(client, c)
	default:
		err = fmt.Errorf("unsupported type %q", c.Type)
	}
	_ = client.Close()
	if err != nil && !errors.Is(err, errClientGone) {
		p.log.Debug("proxied session ended", "err", err)
	}
}

// errClientGone ends a session that DuckDB closed.
var errClientGone = errors.New("DuckDB closed the connection")

// checkToken compares what DuckDB signed in with to the proxy's token.
func (p *proxy) checkToken(got []byte) bool {
	return subtle.ConstantTimeCompare(got, []byte(p.token)) == 1
}

// password returns the password to sign in to the remote database with:
// the stored one, or the current contents of the password_secret file.
func (p *proxy) password(c Connection) (string, error) {
	return readPassword(c, p.secrets)
}

func readPassword(c Connection, secrets string) (string, error) {
	if c.Unusable != "" {
		return "", errors.New(c.Unusable)
	}
	if c.PasswordSecret == "" {
		return c.Password, nil
	}
	if secrets == "" {
		return "", errors.New("password_secret needs IGLOO_CONNECTION_SECRETS_DIR")
	}
	if !validSecretName(c.PasswordSecret) {
		return "", fmt.Errorf("invalid password_secret %q", c.PasswordSecret)
	}
	b, err := os.ReadFile(filepath.Join(secrets, c.PasswordSecret))
	if err != nil {
		return "", fmt.Errorf("read password_secret %s: %w", c.PasswordSecret, errors.Unwrap(err))
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// dial connects to c's remote database, giving up after c.ConnectTimeout.
func dial(ctx context.Context, c Connection) (net.Conn, error) {
	d := net.Dialer{Timeout: c.ConnectTimeout, KeepAlive: keepAlive}
	return d.DialContext(ctx, "tcp", c.Address())
}

// track registers a session, unless the proxy is closed.
func (p *proxy) track(r *relay) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.sessions[r] = true
	return true
}

func (p *proxy) untrack(r *relay) {
	p.mu.Lock()
	delete(p.sessions, r)
	p.mu.Unlock()
}

// signedIn records whether signing in to the remote database worked,
// logging the first failure in a row and the recovery after it.
func (p *proxy) signedIn(err error) {
	if err == nil {
		p.lastErr.Store(nil)
		if p.failures.Swap(0) > 0 {
			p.log.Info("remote database reachable again")
		}
		return
	}
	msg := Scrub(err.Error())
	p.lastErr.Store(&msg)
	if p.failures.Add(1) == 1 {
		p.log.Warn("cannot sign in to the remote database", "err", msg)
	}
}

// lastError says why the last sign-in failed, or "" if it worked.
func (p *proxy) lastError() string {
	if e := p.lastErr.Load(); e != nil {
		return *e
	}
	return ""
}
