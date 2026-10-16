package pgwire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// Embedded sessions run SQL in process for users who sign in some other way
// than the wire protocol: the web console and Arrow Flight SQL. A statement
// takes the same path as one from a network client (parse,
// prepareStatement, runCancelable), so authorization, statement timeouts,
// cancellation, metrics and logging apply identically; only the transport
// differs.

// ErrLoginFailed is returned by Authenticate for an unknown user and for a
// wrong password alike.
var ErrLoginFailed = errors.New("password authentication failed")

// ErrUserDisabled is returned by Authenticate, after the password checked
// out, for a user who may not sign in.
var ErrUserDisabled = errors.New("this user is not permitted to sign in")

// LoginBlockedError is returned by Authenticate after too many failed
// logins for the user or from the address.
type LoginBlockedError struct{ Wait time.Duration }

func (e *LoginBlockedError) Error() string {
	return "too many failed sign-ins; try again in " + roundUp(e.Wait)
}

// ErrStopRows, returned by ResultWriter.Row, ends a statement's rows early,
// as a client closing a portal does. The statement counts as successful.
var ErrStopRows = errors.New("enough rows")

// consoleLogin marks log lines of logins through the web console.
var consoleLogin = []any{"client", "console"}

// Authenticate checks name's password against the user's stored SCRAM
// verifier, for sign-ins through the web console. It behaves like a SQL
// login: unknown users go through the same work against mock credentials,
// failures wait as long and count towards the same limits, and every
// outcome is logged and counted. Access tokens do not sign in here.
func (s *Server) Authenticate(ctx context.Context, name, password string, addr net.Addr) (auth.User, error) {
	return s.AuthenticateClient(ctx, name, password, addr, "console")
}

// AuthenticateClient is Authenticate for sign-ins through client, such as
// "flight sql", which the log and the login history name.
func (s *Server) AuthenticateClient(ctx context.Context, name, password string, addr net.Addr, client string) (auth.User, error) {
	how := login{method: audit.MethodPassword, attrs: []any{"client", client}}
	done, wait := s.limiter.begin(name, addr)
	defer done()
	if wait > 0 {
		s.signInRefused(name, addr, client, audit.MethodPassword, "too many failed logins")
		return auth.User{}, &LoginBlockedError{Wait: wait}
	}
	user, err := s.users.Get(ctx, name)
	known := err == nil
	if err != nil && !errors.Is(err, auth.ErrNotFound) {
		return auth.User{}, fmt.Errorf("look up user: %w", err)
	}
	creds, reason := s.mockCredentials(name), "unknown user"
	if known {
		reason = "wrong password"
		allowed, err := s.passwordAllowed(ctx, user)
		if err != nil {
			return auth.User{}, err
		}
		switch {
		case !user.HasPassword():
			known, reason = false, "user has no password"
		case !allowed:
			known, reason = false, "password sign-in is off for this user"
		default:
			if creds, err = auth.ParseVerifier(user.Verifier); err != nil {
				s.log.Error("stored password verifier is invalid", "user", name)
				creds, known = s.mockCredentials(name), false
			}
		}
	}
	if !auth.CheckSecret(creds, password) || !known {
		s.loginRejected(name, addr, nil, reason, how)
		return auth.User{}, ErrLoginFailed
	}
	if user.Disabled {
		s.signInRefused(name, addr, client, audit.MethodPassword, "user is disabled")
		return auth.User{}, ErrUserDisabled
	}
	s.limiter.succeeded(name)
	s.loginSucceeded(user, addr, how)
	return user, nil
}

// SignedIn counts, logs and records a sign-in to the web console that used
// no password, such as single sign-on; method is one of audit's.
func (s *Server) SignedIn(user auth.User, addr net.Addr, method string) {
	s.loginSucceeded(user, addr, login{method: method, attrs: consoleLogin})
}

// SignInRefused counts, logs and records a refused sign-in to the web
// console. name is whom it was for, as far as known, and reason, for the
// history, why it was refused.
func (s *Server) SignInRefused(name string, addr net.Addr, method, reason string) {
	s.signInRefused(name, addr, "console", method, reason)
}

// signInRefused is SignInRefused for a sign-in through client.
func (s *Server) signInRefused(name string, addr net.Addr, client, method, reason string) {
	s.metrics.login(false)
	s.recordLogin(name, addr, client, nil, reason, login{method: method})
	s.log.Warn("login refused", "user", name, "addr", addr, "method", method, "reason", reason, "client", client)
}

// UserChanged makes the open sessions of the named user on this replica
// read the user again before their next statement, after a change made
// outside SQL, such as roles that single sign-on gave.
func (s *Server) UserChanged(name string) { s.sessions.expire(name) }

// EmbeddedSession is a session opened in process with OpenEmbedded. It is
// not safe for concurrent use.
type EmbeddedSession struct {
	srv   *Server
	sess  *session
	types *pgtype.Map
}

// OpenEmbedded opens a session for user, who signed in through the web
// console or Flight SQL. client says where the user is, for the list of
// sessions and the history. Like a network session it holds a DuckDB
// connection, counts against MaxConnections and can be canceled; Close
// ends it.
func (s *Server) OpenEmbedded(ctx context.Context, user auth.User, client string) (*EmbeddedSession, error) {
	if !s.acquireConn() {
		s.log.Warn("connection refused: too many sessions", "user", user.Name, "max", s.maxConns, "client", client)
		return nil, pgerror.WithCode(errors.New("sorry, too many clients already"), codes.TooManyConnections)
	}
	sess := &session{
		user:     user,
		checked:  time.Now(),
		timeout:  s.timeout,
		activity: s.activity,
		open:     make(map[*wire.PreparedStatement]*statement),
		client:   client,
	}
	ctx = context.WithValue(ctx, sessionKey{}, sess)
	// Registers the session, so it is listed and can be canceled.
	s.backendKeyData(ctx)
	if _, err := s.openSession(ctx); err != nil {
		return nil, err
	}
	return &EmbeddedSession{srv: s, sess: sess, types: pgtype.NewMap()}, nil
}

// User is the session's user as of its last role check.
func (e *EmbeddedSession) User() auth.User { return e.sess.currentUser() }

// Close ends the session and releases its DuckDB connection.
func (e *EmbeddedSession) Close() error {
	return e.srv.closeSession(context.WithValue(context.Background(), sessionKey{}, e.sess))
}

// Column describes a column of a statement's result.
type Column struct {
	Name string
	// Type is the PostgreSQL type SQL clients receive, such as "bigint".
	Type string
}

// ResultWriter receives what a statement returns.
type ResultWriter interface {
	// Columns is called before the first row of a statement that returns
	// rows.
	Columns(cols []Column) error
	// Row receives a row: each value is a string in PostgreSQL's text
	// format, as psql shows it, or nil for NULL. The slice is reused.
	// Returning ErrStopRows ends the rows early.
	Row(values []any) error
	// Complete receives the command tag, such as "SELECT 3" or
	// "INSERT 0 5", once the statement has finished.
	Complete(tag string) error
}

// Run runs query as the session's user and sends its result to out. The
// query goes through the same path as a simple-protocol query from a
// network client; send one statement at a time, as psql does. A
// cancellation of ctx cancels the statement. Errors carry SQLSTATE codes
// (see pgerror.Flatten).
func (e *EmbeddedSession) Run(ctx context.Context, query string, out ResultWriter) error {
	return e.run(ctx, query, out, false)
}

// RunQuery is Run for a query that only reads, which the console runs a
// second time to download its whole result. Any other statement is refused
// before it runs.
func (e *EmbeddedSession) RunQuery(ctx context.Context, query string, out ResultWriter) error {
	return e.run(ctx, query, out, true)
}

func (e *EmbeddedSession) run(ctx context.Context, query string, out ResultWriter, readOnly bool) error {
	ctx = context.WithValue(ctx, sessionKey{}, e.sess)
	stmts, err := e.srv.parse(ctx, wire.Query{Query: query, SimpleQuery: true})
	if err != nil {
		return pgError(err)
	}
	if len(stmts) == 0 {
		return wire.NewErrUndefinedStatement()
	}
	if readOnly {
		for _, ps := range stmts {
			if st, ok := e.sess.open[ps]; !ok || !st.onlyReads() {
				for _, ps := range stmts {
					_ = e.sess.release(ps)
				}
				return pgerror.WithCode(errors.New("only a query (SELECT) can run again for a download"),
					codes.FeatureNotSupported)
			}
		}
	}

	for _, ps := range stmts {
		if err := e.runStatement(ctx, ps, nil, out); err != nil {
			return err
		}
	}
	return nil
}

// runStatement runs ps, one statement parse prepared, with params and
// sends its result to out. ctx carries the session.
func (e *EmbeddedSession) runStatement(ctx context.Context, ps *wire.PreparedStatement, params []wire.Parameter, out ResultWriter) error {
	st, ok := e.sess.open[ps]
	w := &embeddedWriter{out: out, columns: ps.Columns(), types: e.types, canStop: ok && st.onlyReads()}
	if len(w.columns) > 0 {
		if err := out.Columns(columnsOf(w.columns)); err != nil {
			return err
		}
		w.values = make([]any, len(w.columns))
	}
	err := ps.Run(ctx, w, params)
	switch {
	case errors.Is(err, ErrStopRows):
		err = out.Complete(fmt.Sprintf("SELECT %d", w.written))
	case err == nil && w.tag != "":
		err = out.Complete(w.tag)
	}
	return pgError(err)
}

func columnsOf(wcols wire.Columns) []Column {
	cols := make([]Column, len(wcols))
	for i, c := range wcols {
		cols[i] = Column{Name: c.Name, Type: typeName(c.Oid)}
	}
	return cols
}

// onlyReads reports whether running the statement again changes nothing.
func (st *statement) onlyReads() bool {
	return st.kind == returnsRows &&
		(st.stmtType == duckdb.STATEMENT_TYPE_SELECT || st.stmtType == duckdb.STATEMENT_TYPE_RELATION)
}

// embeddedWriter is the wire.DataWriter statements write to in an embedded
// session. It renders values in PostgreSQL's text format, as a client
// receives them over the wire.
type embeddedWriter struct {
	out     ResultWriter
	columns wire.Columns
	types   *pgtype.Map
	values  []any
	buf     []byte
	written uint32
	tag     string
	// canStop says the statement only reads, so ErrStopRows may end it.
	// Any other statement, such as INSERT ... RETURNING, runs to the end,
	// its remaining rows unsent, so it is never cut short.
	canStop bool
	stopped bool
}

func (w *embeddedWriter) Row(values []any) error {
	if len(values) != len(w.columns) {
		return fmt.Errorf("row has %d values for %d columns", len(values), len(w.columns))
	}
	if !w.stopped {
		for i, v := range values {
			w.values[i] = nil
			if v == nil {
				continue
			}
			b, err := w.types.Encode(w.columns[i].Oid, pgtype.TextFormatCode, v, w.buf[:0])
			if err != nil {
				return fmt.Errorf("column %q: %w", w.columns[i].Name, err)
			}
			if b != nil {
				w.values[i] = string(b)
			}
			w.buf = b
		}
		switch err := w.out.Row(w.values); {
		case errors.Is(err, ErrStopRows) && w.canStop:
			return err
		case errors.Is(err, ErrStopRows):
			w.stopped = true
		case err != nil:
			return err
		}
	}
	w.written++
	return nil
}

func (w *embeddedWriter) Written() uint32 { return w.written }

func (w *embeddedWriter) Empty() error {
	if w.written != 0 {
		return wire.ErrDataWritten
	}
	return nil
}

func (w *embeddedWriter) Columns() wire.Columns { return w.columns }

func (w *embeddedWriter) Complete(tag string) error {
	w.tag = tag
	return nil
}

func (w *embeddedWriter) CopyIn(wire.FormatCode) (*wire.CopyReader, error) {
	return nil, pgerror.WithCode(errors.New("COPY FROM STDIN needs a SQL client such as psql"), codes.FeatureNotSupported)
}

// typeNames are how the console labels the PostgreSQL types Igloo sends
// (see wireType).
var typeNames = map[uint32]string{
	pgtype.BoolOID: "boolean", pgtype.ByteaOID: "bytea", pgtype.Int8OID: "bigint",
	pgtype.Int2OID: "smallint", pgtype.Int4OID: "integer", pgtype.TextOID: "text",
	pgtype.JSONOID: "json", pgtype.Float4OID: "real", pgtype.Float8OID: "double precision",
	pgtype.VarcharOID: "varchar", pgtype.DateOID: "date", pgtype.TimeOID: "time",
	pgtype.TimestampOID: "timestamp", pgtype.TimestamptzOID: "timestamptz",
	pgtype.IntervalOID: "interval", pgtype.NumericOID: "numeric", pgtype.UUIDOID: "uuid",
}

func typeName(oid uint32) string {
	if name, ok := typeNames[oid]; ok {
		return name
	}
	return fmt.Sprintf("oid %d", oid)
}

// SessionInfo describes an open session on this replica.
type SessionInfo struct {
	PID       int32
	User      string
	Role      auth.Role
	Client    string
	Connected time.Time
	// Statement is the text of the running statement and Started is when
	// it began; both are empty while the session is idle.
	Statement string
	Started   time.Time
}

// Sessions lists the sessions open on this replica, oldest first.
func (s *Server) Sessions() []SessionInfo {
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	connected := make(map[*session]time.Time, len(r.byPID))
	for _, sess := range r.byPID {
		all = append(all, sess)
		connected[sess] = sess.added
	}
	r.mu.Unlock()

	infos := make([]SessionInfo, 0, len(all))
	for _, sess := range all {
		sess.mu.Lock()
		infos = append(infos, SessionInfo{
			PID: sess.pid, User: sess.user.Name, Role: sess.user.Role, Client: sess.client,
			Connected: connected[sess], Statement: sess.statement, Started: sess.started,
		})
		sess.mu.Unlock()
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Connected.Before(infos[j].Connected) })
	return infos
}

// CancelStatement cancels the statement running in session pid on this
// replica, as a cancel request from its client would. It reports whether a
// statement was running. by is who asked, for the log.
func (s *Server) CancelStatement(pid int32, by string) bool {
	s.sessions.mu.Lock()
	sess := s.sessions.byPID[pid]
	s.sessions.mu.Unlock()
	if sess == nil {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.running == nil {
		return false
	}
	sess.running()
	s.log.Info("statement canceled", "user", sess.user.Name, "pid", pid, "by", by)
	return true
}
