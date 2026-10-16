package flightsql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

const (
	// busyWait bounds how long a call waits for the statement its session
	// is running: long enough for a canceled statement to stop, short
	// enough that a client waiting on itself (a second query sent before it
	// reads the first one's result) gets an error rather than a hang.
	busyWait = 30 * time.Second
	// maxUnfetched is how many queries a session keeps that a client
	// described (GetFlightInfo) but did not fetch; older ones are dropped.
	maxUnfetched = 64
)

var (
	errSignIn = status.Error(codes.Unauthenticated,
		"sign in first: send a handshake with basic authentication (user name and password), then the bearer token it returns")
	errNoSession = status.Error(codes.Unauthenticated,
		"this bearer token has no session on this connection; sessions end with the connection they were opened on and when Igloo restarts; sign in again")
	errSessionEnded = status.Error(codes.Unauthenticated, "the session ended; sign in again")
	errNoStatement  = status.Error(codes.NotFound,
		"no such statement in this session: it was closed, a query's result was already fetched, or the session is new")
)

// conn is a client connection and the sessions opened on it.
type conn struct {
	mu     sync.Mutex
	closed bool
	// sessions are keyed by the SHA-256 of their bearer token, so looking
	// one up takes the same time whatever the token.
	sessions map[[32]byte]*session
}

// add registers sess under key, unless the connection has closed. If a
// session already has key, as when two calls with the same personal
// access token arrive at once, it returns that one instead.
func (c *conn) add(key [32]byte, sess *session) (*session, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, false
	}
	if other, ok := c.sessions[key]; ok {
		return other, false
	}
	if c.sessions == nil {
		c.sessions = make(map[[32]byte]*session)
	}
	c.sessions[key] = sess
	return sess, true
}

func (c *conn) get(key [32]byte) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[key]
}

// remove takes the session with key off the connection, if it is there.
func (c *conn) remove(key [32]byte) *session {
	c.mu.Lock()
	defer c.mu.Unlock()
	sess := c.sessions[key]
	delete(c.sessions, key)
	return sess
}

// close ends the connection's sessions, each once its running statement,
// which the connection's end cancels, has stopped.
func (c *conn) close() {
	c.mu.Lock()
	c.closed = true
	sessions := c.sessions
	c.sessions = nil
	c.mu.Unlock()
	for _, sess := range sessions {
		go sess.close()
	}
}

// session is a signed-in client's SQL session, an embedded session of the
// PostgreSQL server. Like a PostgreSQL connection it runs one statement at
// a time and keeps its settings, temporary tables and prepared statements
// until it ends.
type session struct {
	s      *Server
	es     *pgwire.EmbeddedSession
	conn   *conn
	key    [32]byte
	client string
	// busy holds a token while a call uses the embedded session, which is
	// not safe for concurrent use.
	busy chan struct{}
	// done is closed when the session ends.
	done chan struct{}

	mu sync.Mutex
	// stmts are the prepared statements, by handle.
	stmts map[string]*statement
	// unfetched are the handles of queries waiting for DoGet, oldest
	// first.
	unfetched []string
	// running is the query DoGet is running now, and runningHandle its
	// handle, for CancelFlightInfo.
	running       *statement
	runningHandle string
}

// statement is a statement prepared in a session.
type statement struct {
	ps     *pgwire.PreparedStatement
	schema *arrow.Schema
	// once marks a query from CommandStatementQuery: it runs once, when
	// fetched, and is then released.
	once bool
	// params are the parameter values a DoPut bound, one set per row; the
	// statement runs once for each.
	params [][]any
}

type sessionKey struct{}

func sessionFrom(ctx context.Context) *session {
	sess, _ := ctx.Value(sessionKey{}).(*session)
	return sess
}

// Handshake signs a client in with HTTP basic authentication and opens a
// session, whose bearer token it returns in the authorization header. The
// password is checked exactly as for a SQL login, with the same failure
// delay, logging and login history.
func (svc *service) Handshake(stream flight.FlightService_HandshakeServer) error {
	s, ctx := svc.s, stream.Context()
	c, _ := ctx.Value(connKey{}).(*conn)
	scheme, cred := authorization(ctx)
	if c == nil || !strings.EqualFold(scheme, "Basic") {
		return status.Error(codes.Unauthenticated,
			"sign in with basic authentication: send the user name and password in the handshake's authorization header")
	}
	name, password, ok := parseBasic(cred)
	if !ok {
		return status.Error(codes.Unauthenticated, "malformed basic authentication header")
	}
	name = strings.ToLower(strings.TrimSpace(name))
	user, err := s.sql.AuthenticateClient(ctx, name, password, peerAddr(ctx), "flight sql")
	if errors.Is(err, pgwire.ErrLoginFailed) {
		return status.Errorf(codes.Unauthenticated, "password authentication failed for user %q", name)
	}
	if err != nil {
		s.log.Warn("sign-in failed", "user", name, "err", err)
		return status.Error(codes.Unavailable, "could not check the password; try again")
	}

	token := make([]byte, 32)
	_, _ = rand.Read(token)
	bearer := base64.RawURLEncoding.EncodeToString(token)
	if _, err := s.open(ctx, c, sha256.Sum256([]byte(bearer)), user); err != nil {
		return err
	}
	return stream.SendHeader(metadata.Pairs("authorization", "Bearer "+bearer))
}

// session returns the session of the bearer token the call carries, which
// must have been opened on the same connection. A token Igloo issued some
// other way, such as a personal access token, opens a session on first
// use.
func (s *Server) session(ctx context.Context) (*session, error) {
	c, _ := ctx.Value(connKey{}).(*conn)
	scheme, token := authorization(ctx)
	if c == nil || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return nil, errSignIn
	}
	key := sha256.Sum256([]byte(token))
	if sess := c.get(key); sess != nil {
		return sess, nil
	}
	if s.tokens == nil {
		return nil, errNoSession
	}
	user, err := s.tokens.AuthenticateToken(ctx, token, peerAddr(ctx))
	if err != nil {
		return nil, errNoSession
	}
	return s.open(ctx, c, key, user)
}

// open opens a session for user on connection c under key.
func (s *Server) open(ctx context.Context, c *conn, key [32]byte, user auth.User) (*session, error) {
	client := "flight sql"
	if addr := peerAddr(ctx); addr != nil {
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			host = addr.String()
		}
		client += " " + host
	}
	es, err := s.sql.OpenEmbedded(ctx, user, client)
	if err != nil {
		return nil, statusOf(err)
	}
	if ua := metadata.ValueFromIncomingContext(ctx, "user-agent"); len(ua) > 0 {
		es.SetApplicationName(truncate(ua[0], 100))
	}
	sess := &session{
		s: s, es: es, conn: c, key: key, client: client,
		busy: make(chan struct{}, 1), done: make(chan struct{}),
		stmts: make(map[string]*statement),
	}
	s.sessions.Add(1)
	got, added := c.add(key, sess)
	if !added {
		sess.close()
		if got == nil {
			return nil, status.Error(codes.Unavailable, "the connection closed")
		}
	}
	return got, nil
}

// close ends the session once the statement it is running, if any, has
// stopped.
func (sess *session) close() {
	close(sess.done)
	sess.busy <- struct{}{} // never released: the session is over
	if err := sess.es.Close(); err != nil {
		sess.s.log.Warn("could not close a session", "err", err)
	}
	sess.s.sessions.Done()
}

// acquire waits for the session to be free, so the caller may use it.
func (sess *session) acquire(ctx context.Context) error {
	timer := time.NewTimer(busyWait)
	defer timer.Stop()
	select {
	case sess.busy <- struct{}{}:
		select {
		case <-sess.done:
			sess.release()
			return errSessionEnded
		default:
			return nil
		}
	case <-sess.done:
		return errSessionEnded
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		return status.Error(codes.FailedPrecondition,
			"this session is still running another statement: a session runs one at a time, as a PostgreSQL connection does; "+
				"read or cancel the other statement's result first, or use another connection")
	}
}

// tryAcquire takes the session if it is free.
func (sess *session) tryAcquire() bool {
	select {
	case sess.busy <- struct{}{}:
		return true
	default:
		return false
	}
}

func (sess *session) release() { <-sess.busy }

// prepare prepares query in the session, which the caller holds, and
// returns the statement's handle. A query that runs once is dropped when
// fetched, or when too many others wait to be.
func (sess *session) prepare(ctx context.Context, query string, once bool) (*statement, []byte, error) {
	ps, err := sess.es.Prepare(ctx, query)
	if err != nil {
		return nil, nil, statusOf(err)
	}
	schema, err := ps.ArrowSchema()
	if err != nil {
		_ = ps.Close()
		return nil, nil, statusOf(err)
	}
	st := &statement{ps: ps, schema: schema, once: once}
	handle := make([]byte, 16)
	_, _ = rand.Read(handle)

	sess.mu.Lock()
	sess.stmts[string(handle)] = st
	var stale *statement
	if once {
		sess.unfetched = append(sess.unfetched, string(handle))
		if len(sess.unfetched) > maxUnfetched {
			stale = sess.stmts[sess.unfetched[0]]
			delete(sess.stmts, sess.unfetched[0])
			sess.unfetched = sess.unfetched[1:]
		}
	}
	sess.mu.Unlock()
	if stale != nil {
		_ = stale.ps.Close()
	}
	return st, handle, nil
}

// lookup returns the statement with handle and the parameters bound to it.
func (sess *session) lookup(handle []byte) (*statement, [][]any, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st, ok := sess.stmts[string(handle)]
	if !ok {
		return nil, nil, false
	}
	return st, st.params, true
}

// take returns the statement with handle and its parameters for a run,
// and takes a query that runs once out of the session.
func (sess *session) take(handle []byte) (*statement, [][]any, bool) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st, ok := sess.stmts[string(handle)]
	if !ok {
		return nil, nil, false
	}
	if st.once {
		sess.forget(string(handle))
	}
	return st, st.params, true
}

// forget takes the statement with handle out of the session's maps. The
// caller holds sess.mu.
func (sess *session) forget(handle string) {
	delete(sess.stmts, handle)
	for i, h := range sess.unfetched {
		if h == handle {
			sess.unfetched = append(sess.unfetched[:i], sess.unfetched[i+1:]...)
			break
		}
	}
}

// bind sets the parameter values the statement with handle runs with.
func (sess *session) bind(handle []byte, params [][]any) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	st, ok := sess.stmts[string(handle)]
	if ok {
		st.params = params
	}
	return ok
}

// run runs st, which the caller holds the session for, once for each set
// of parameters, or once without any, sending the results to out.
func (sess *session) run(ctx context.Context, st *statement, params [][]any, out pgwire.ResultWriter) error {
	if len(params) == 0 {
		params = [][]any{nil}
	}
	for _, p := range params {
		if err := st.ps.Run(ctx, p, out); err != nil {
			return err
		}
	}
	return nil
}

// authorization returns the scheme and credentials of the call's
// authorization header.
func authorization(ctx context.Context) (scheme, cred string) {
	v := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(v) == 0 {
		return "", ""
	}
	scheme, cred, _ = strings.Cut(strings.TrimSpace(v[0]), " ")
	return scheme, strings.TrimSpace(cred)
}

// parseBasic decodes basic authentication credentials. Some clients
// leave out base64 padding.
func parseBasic(cred string) (name, password string, ok bool) {
	b, err := base64.StdEncoding.DecodeString(cred)
	if err != nil {
		if b, err = base64.RawStdEncoding.DecodeString(cred); err != nil {
			return "", "", false
		}
	}
	return strings.Cut(string(b), ":")
}

func peerAddr(ctx context.Context) net.Addr {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}
