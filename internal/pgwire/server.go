// Package pgwire serves Igloo over the PostgreSQL wire protocol, so psql, BI
// tools and ordinary Postgres drivers can connect without an Igloo client.
package pgwire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/types"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
)

// Engine runs queries; *engine.Engine implements it.
type Engine interface {
	Connect(ctx context.Context) (*duckdb.Conn, error)
	// CountStatements counts the statements in a query without running them.
	CountStatements(query string) (int, error)
	// LoadSchemas makes catalog views list every table's real columns.
	LoadSchemas(ctx context.Context, conn *duckdb.Conn) error
	// LocalDatabase is the name of DuckDB's own database.
	LocalDatabase() string
}

// Options configures the server.
type Options struct {
	Users auth.Store
	// TLS, when set, lets clients encrypt the connection.
	TLS *tls.Config
	// RequireTLS refuses sessions that do not use TLS. It needs TLS.
	RequireTLS bool
	Metrics    *Metrics
	// RoleRefresh is how often a session re-reads its user's role, so role
	// changes and dropped users take effect on open connections. Zero means
	// 30 seconds.
	RoleRefresh time.Duration
	// StatementTimeout is the default limit on how long a statement may
	// run; sessions can change it with SET statement_timeout. Zero means none.
	StatementTimeout time.Duration
	// MaxConnections caps concurrent sessions. Zero means no cap.
	MaxConnections int
	// Version is Igloo's version, reported by SELECT version().
	Version string
	// DDL carries out views and comments on the Iceberg catalog. Nil
	// without a catalog.
	DDL *ddl.Manager
	// Activity, if set, records statements while they run.
	Activity *activity.Tracker
}

// serverParameters are reported to clients at login. Drivers use them to
// decide how to quote strings and parse dates.
var serverParameters = wire.Parameters{
	"standard_conforming_strings": "on",
	"DateStyle":                   "ISO, MDY",
	"IntervalStyle":               "postgres",
	"TimeZone":                    "UTC",
	"integer_datetimes":           "on",
}

// Server is a Postgres-protocol front end for the query engine.
type Server struct {
	wire        *wire.Server
	requireTLS  bool
	engine      Engine
	users       auth.Store
	metrics     *Metrics
	log         *slog.Logger
	sessions    *registry
	roleRefresh time.Duration
	timeout     time.Duration
	maxConns    int
	conns       atomic.Int32
	version     string
	ddl         *ddl.Manager
	activity    *activity.Tracker
	// mockSecret derives the credentials a login for an unknown user runs
	// against (see mockCredentials).
	mockSecret []byte
}

// New builds a server that runs queries on engine.
func New(engine Engine, opts Options, log *slog.Logger) (*Server, error) {
	if opts.Users == nil {
		return nil, errors.New("pgwire: Options.Users is required")
	}
	mockSecret, err := opts.Users.MockSecret(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load mock authentication secret: %w", err)
	}

	s := &Server{
		engine:      engine,
		requireTLS:  opts.RequireTLS && opts.TLS != nil,
		users:       opts.Users,
		metrics:     opts.Metrics,
		log:         log,
		sessions:    newRegistry(),
		roleRefresh: opts.RoleRefresh,
		timeout:     opts.StatementTimeout,
		maxConns:    opts.MaxConnections,
		version:     opts.Version,
		ddl:         opts.DDL,
		activity:    opts.Activity,
		mockSecret:  mockSecret,
	}
	if s.roleRefresh == 0 {
		s.roleRefresh = 30 * time.Second
	}
	s.sessions.abandoned = s.releaseConn

	options := []wire.OptionFn{
		wire.SessionAuthStrategy(s.authenticate),
		wire.BackendKeyData(s.backendKeyData),
		wire.CancelRequest(s.cancelRequest),
		wire.SessionMiddleware(s.openSession),
		wire.CloseConn(s.closeSession),
		wire.Statements(newStatementCache),
		wire.Portals(newPortalCache),
		wire.TxStatus(txStatus),
		wire.GlobalParameters(serverParameters),
		// The protocol library logs query text at debug level, which would
		// include passwords from CREATE USER; keep it at info or above.
		wire.Logger(slog.New(minLevel{log.Handler(), slog.LevelInfo})),
	}
	if opts.TLS != nil {
		options = append(options, wire.TLSConfig(opts.TLS))
	}

	srv, err := wire.NewServer(s.parse, options...)
	if err != nil {
		return nil, err
	}
	s.wire = srv
	return s, nil
}

// Serve accepts connections on l until Shutdown is called.
func (s *Server) Serve(l net.Listener) error {
	if s.requireTLS {
		l = requireTLS(l)
	}
	return s.wire.Serve(l)
}

// Shutdown stops accepting connections and waits for open ones to finish
// until ctx is done.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.wire.Shutdown(ctx)
}

// session is the state of one client connection.
type session struct {
	pid, secret int32
	added       time.Time
	conn        *duckdb.Conn
	// open holds every DuckDB statement prepared for this client, keyed by
	// the wire statement that owns it, so none outlive the connection.
	open     map[*wire.PreparedStatement]*statement
	inTx     bool
	txFailed bool
	// timeout limits each statement; zero means none.
	timeout time.Duration
	// activity records statements while they run; it may be nil.
	activity *activity.Tracker
	// statements holds the client's named prepared statements, once it
	// has prepared one.
	statements *statementCache
	// portals holds the client's portals, once it has bound one.
	portals *portalCache
	// cursors are the open SQL cursors, by name.
	cursors map[string]*cursor

	// mu guards the fields below, which cancel requests and user changes
	// reach from other connections.
	mu      sync.Mutex
	user    auth.User
	checked time.Time
	running context.CancelFunc
}

type sessionKey struct{}

func sessionFrom(ctx context.Context) *session {
	sess, _ := ctx.Value(sessionKey{}).(*session)
	return sess
}

func (sess *session) currentUser() auth.User {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.user
}

// backendKeyData gives the client the key it needs to cancel its queries.
func (s *Server) backendKeyData(ctx context.Context) (int32, int32) {
	sess := sessionFrom(ctx)
	if sess == nil {
		return 0, 0
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	sess.pid = int32(binary.BigEndian.Uint32(b[:4]) &^ (1 << 31))
	sess.secret = int32(binary.BigEndian.Uint32(b[4:]))
	s.sessions.add(sess)
	return sess.pid, sess.secret
}

// cancelRequest stops the running query of the session that pid and secret
// identify. Wrong keys are ignored silently, as in PostgreSQL.
func (s *Server) cancelRequest(_ context.Context, pid, secret int32) error {
	s.sessions.cancel(pid, secret)
	return nil
}

func (s *Server) openSession(ctx context.Context) (context.Context, error) {
	sess := sessionFrom(ctx)
	if sess == nil {
		return ctx, errors.New("session was not authenticated")
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		s.sessions.remove(sess)
		s.releaseConn()
		return ctx, fmt.Errorf("open session: %w", err)
	}
	sess.conn = conn
	for _, stmt := range sessionCatalogSetup(sess.user.Name, sess.user.Role == auth.RoleAdmin, s.version) {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			s.sessions.remove(sess)
			s.releaseConn()
			return ctx, errors.Join(fmt.Errorf("set up session catalog: %w", err), conn.Close())
		}
	}
	s.metrics.sessionOpened(string(sess.user.Role))
	return ctx, nil
}

func (s *Server) closeSession(ctx context.Context) error {
	sess := sessionFrom(ctx)
	if sess == nil || sess.conn == nil {
		return nil
	}
	s.sessions.remove(sess)
	s.releaseConn()
	s.metrics.sessionClosed(string(sess.currentUser().Role))

	sess.closeCursors(ctx, true)
	var errs []error
	for ps := range sess.open {
		errs = append(errs, sess.release(ps))
	}
	return errors.Join(append(errs, sess.conn.Close())...)
}

// release closes the DuckDB statement behind ps.
func (sess *session) release(ps *wire.PreparedStatement) error {
	st, ok := sess.open[ps]
	if !ok {
		return nil
	}
	delete(sess.open, ps)
	return st.close()
}

// refreshUser re-reads the session's user when the cached role is stale, so
// role changes and dropped users take effect on open connections.
func (s *Server) refreshUser(ctx context.Context, sess *session) (auth.User, error) {
	sess.mu.Lock()
	user, fresh := sess.user, time.Since(sess.checked) < s.roleRefresh
	sess.mu.Unlock()
	if fresh {
		return user, nil
	}

	latest, err := s.users.Get(ctx, user.Name)
	if errors.Is(err, auth.ErrNotFound) {
		return user, pgerror.WithSeverity(pgerror.WithCode(
			fmt.Errorf("user %q no longer exists", user.Name), codes.InvalidAuthorizationSpecification),
			pgerror.LevelFatal)
	}
	if err != nil {
		// Keep the last known role if the metadata database blips.
		s.log.Warn("could not refresh user", "user", user.Name, "err", err)
		return user, nil
	}

	sess.mu.Lock()
	sess.user, sess.checked = latest, time.Now()
	sess.mu.Unlock()
	return latest, nil
}

// acquireConn reserves a session slot, reporting false when the server is
// at MaxConnections.
func (s *Server) acquireConn() bool {
	if s.conns.Add(1) > int32(s.maxConns) && s.maxConns > 0 {
		s.conns.Add(-1)
		return false
	}
	return true
}

func (s *Server) releaseConn() { s.conns.Add(-1) }

// errStatementTimeout matches PostgreSQL's message for the same event.
var errStatementTimeout = pgerror.WithCode(errors.New("canceling statement due to statement timeout"), codes.QueryCanceled)

// runCancelable runs fn with a context that a cancel request, or the
// session's statement timeout, can end.
func (sess *session) runCancelable(ctx context.Context, fn func(context.Context) error) error {
	defer sess.activity.Begin()()
	var cancel context.CancelFunc
	if sess.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, sess.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	sess.mu.Lock()
	sess.running = cancel
	sess.mu.Unlock()

	defer func() {
		sess.mu.Lock()
		sess.running = nil
		sess.mu.Unlock()
		cancel()
	}()

	err := fn(ctx)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errStatementTimeout
	}
	return err
}

// observe updates transaction state after a statement runs, so ReadyForQuery
// tells clients and poolers whether a transaction is open or failed.
func (sess *session) observe(st *statement, err error) {
	if st != nil && st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION {
		if err == nil || isTransactionEnd(st.query) {
			switch transactionChange(st.query) {
			case 1:
				sess.inTx, sess.txFailed = true, false
			case -1:
				sess.inTx, sess.txFailed = false, false
				// Cursors without WITH HOLD end with their transaction.
				sess.closeCursors(context.Background(), false)
			}
			return
		}
	}
	if err != nil && sess.inTx {
		sess.txFailed = true
	}
}

func txStatus(ctx context.Context) types.ServerStatus {
	sess := sessionFrom(ctx)
	switch {
	case sess == nil || !sess.inTx:
		return types.ServerIdle
	case sess.txFailed:
		return types.ServerTransactionFailed
	}
	return types.ServerTransactionBlock
}

// listsColumns matches catalog queries that need every table's schema
// loaded first (see Engine.LoadSchemas): ones that read columns, and ones
// that read table OIDs, because loading a schema gives the table a new OID
// and clients such as psql look up an OID before listing its columns.
var listsColumns = regexp.MustCompile(`(?i)\b(pg_attribute|pg_class|information_schema\s*\.\s*columns|duckdb_columns)\b`)

var deallocateAll = regexp.MustCompile(`(?is)^\s*DEALLOCATE\s+(?:PREPARE\s+)?ALL\s*;?\s*$`)

var showPasswordEncryption = regexp.MustCompile(`(?is)^\s*SHOW\s+password_encryption\s*;?\s*$`)

func (s *Server) parse(ctx context.Context, q wire.Query) (wire.PreparedStatements, error) {
	sess := sessionFrom(ctx)
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}

	if stmt, ok := s.statementTimeoutCommand(sess, q.Query); ok {
		return stmt, nil
	}

	// psycopg sends this after DDL to drop its prepared statements; DuckDB
	// has no DEALLOCATE ALL.
	if deallocateAll.MatchString(q.Query) {
		return wire.Prepared(wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if c := sess.statements; c != nil {
				for name := range c.names {
					if err := c.Delete(ctx, name); err != nil {
						return err
					}
				}
			}
			return w.Complete("DEALLOCATE ALL")
		})), nil
	}

	if tag, ok := ignoredCommand(q.Query); ok {
		return wire.Prepared(wire.NewStatement(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			return w.Complete(tag)
		})), nil
	}

	// psql's \password asks which hash to send before ALTER USER.
	if showPasswordEncryption.MatchString(q.Query) {
		return wire.Prepared(wire.NewStatement(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{"scram-sha-256"}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "password_encryption", Oid: pgtype.TextOID, Width: -1}}))), nil
	}

	if cmds, ok, err := parseUserCommands(q.Query); ok {
		if err != nil {
			return nil, err
		}
		if len(cmds) > 1 && !q.SimpleQuery {
			return nil, pgerror.WithCode(errors.New("cannot prepare several statements at once"), codes.Syntax)
		}
		var stmts wire.PreparedStatements
		for _, cmd := range cmds {
			var columns wire.Columns
			if cmd.verb == "SHOW" {
				columns = userColumns
			}
			stmts = append(stmts, wire.NewStatement(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				start := time.Now()
				err := s.runUserCommand(ctx, sess, cmd, w)
				s.metrics.query(start, err)
				return err
			}, wire.WithColumns(columns)))
		}
		return stmts, nil
	}

	if ps, ok, err := s.catalogDDL(ctx, sess, user, q.Query); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.cursorCommand(ctx, sess, user, q); ok || err != nil {
		return ps, err
	}

	st, err := s.prepareStatement(ctx, sess, user, q.Query, q.ParameterOIDs)
	if err != nil {
		return nil, err
	}

	var ps *wire.PreparedStatement
	ps = wire.NewStatement(func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
		if q.SimpleQuery {
			// Simple-protocol statements run once.
			defer sess.release(ps) //nolint:errcheck
		}

		args, err := decodeParams(params, st.params)
		if err != nil {
			return pgerror.WithCode(err, codes.InvalidTextRepresentation)
		}

		start := time.Now()
		err = sess.runCancelable(ctx, func(ctx context.Context) error {
			return runWithCommitRetry(ctx, sess, st, func() error { return st.run(ctx, args, w) })
		})
		if isIcebergNoReplace(err) {
			if sess.inTx {
				err = pgerror.WithCode(errors.New("CREATE OR REPLACE TABLE on an Iceberg table cannot run inside a transaction block"),
					codes.ActiveSQLTransaction)
			} else if r, ok := s.replaceable(sess, st); ok {
				err = sess.runCancelable(ctx, func(ctx context.Context) error { return s.replaceTable(ctx, sess, r) })
				if err == nil {
					err = w.Complete("CREATE TABLE")
				}
			}
		}
		s.metrics.query(start, err)
		sess.observe(st, err)
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		return pgError(err)
	}, wire.WithColumns(st.columns), wire.WithParameters(st.params))

	sess.open[ps] = st
	return wire.Prepared(ps), nil
}

// prepareStatement rewrites text for DuckDB, checks it against the user's
// role and prepares it on the session's connection. oids are the parameter
// types the client declared. Errors carry PostgreSQL codes.
func (s *Server) prepareStatement(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	query := rewriteSessionUser(rewriteCatalogQuery(text), user.Name)
	if s.ddl != nil {
		query = s.ddl.RewriteViewRefs(query)
		if readsComments.MatchString(query) {
			if err := s.ddl.RefreshComments(ctx); err != nil {
				s.log.Warn("could not load comments", "err", err)
			}
		}
	}

	// Preparing a multi-statement query runs all but the last statement, so
	// only admins, whose statements need no checks, may send one. If the
	// count fails, the query does not parse; stop here rather than let the
	// session connection interpret it.
	if user.Role != auth.RoleAdmin {
		query = sessionScoped(query)
		n, err := s.engine.CountStatements(query)
		if err != nil {
			return nil, pgerror.WithCode(err, codes.Syntax)
		}
		if n > 1 {
			s.metrics.query(time.Now(), errDenied)
			return nil, pgerror.WithCode(
				errors.New("send one statement at a time; only admins may send several in one query"),
				codes.InsufficientPrivilege)
		}
	}

	if listsColumns.MatchString(query) {
		// Best effort: a table that fails to load keeps its placeholder.
		if err := s.engine.LoadSchemas(ctx, sess.conn); err != nil {
			s.log.Warn("could not load table schemas", "err", err)
		}
	}
	st, err := prepare(ctx, sess.conn, query, oids)
	if err != nil && s.ddl != nil && missingRelation.MatchString(err.Error()) {
		// Perhaps a view another replica just created: sync and try again.
		if changed, _ := s.ddl.SyncViews(ctx); changed {
			query = s.ddl.RewriteViewRefs(query)
			st, err = prepare(ctx, sess.conn, query, oids)
		}
	}
	if err != nil {
		sess.observe(nil, err)
		return nil, pgError(err)
	}
	err = authorize(user.Role, st.stmtType, query)
	if err == nil {
		err = s.authorizeLocal(user.Role, st.stmtType, text)
	}
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		s.metrics.query(time.Now(), err)
		_ = st.close()
		return nil, err
	}
	return st, nil
}

// statementTimeoutCommand handles SET, RESET and SHOW statement_timeout,
// which Igloo enforces itself rather than DuckDB.
func (s *Server) statementTimeoutCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(fn, opts...)), true
	}

	if m := setStatementTimeout.FindStringSubmatch(query); m != nil {
		d, isDefault, err := parseStatementTimeout(m[1])
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return pgerror.WithCode(err, codes.InvalidParameterValue)
			}
			sess.timeout = d
			if isDefault {
				sess.timeout = s.timeout
			}
			return w.Complete("SET")
		})
	}
	if resetStatementTimeout.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.timeout = s.timeout
			return w.Complete("RESET")
		})
	}
	if showStatementTimeout.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{formatTimeout(sess.timeout)}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "statement_timeout", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

// errDenied stands in for a refused statement in metrics.
var errDenied = pgerror.WithCode(errors.New("denied"), codes.InsufficientPrivilege)

// registry tracks sessions so cancel requests, which arrive on a separate
// connection, and user changes can reach them.
type registry struct {
	mu    sync.Mutex
	byPID map[int32]*session
	// abandoned is called for each session dropped because its connection
	// died between login and setup, to give back its connection slot.
	abandoned func()
}

func newRegistry() *registry {
	return &registry{byPID: make(map[int32]*session)}
}

func (r *registry) add(sess *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Drop sessions whose connection failed between login and setup.
	for pid, other := range r.byPID {
		if other.conn == nil && time.Since(other.added) > time.Minute {
			delete(r.byPID, pid)
			if r.abandoned != nil {
				r.abandoned()
			}
		}
	}
	sess.added = time.Now()
	r.byPID[sess.pid] = sess
}

func (r *registry) remove(sess *session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byPID[sess.pid] == sess {
		delete(r.byPID, sess.pid)
	}
}

func (r *registry) cancel(pid, secret int32) {
	r.mu.Lock()
	sess := r.byPID[pid]
	r.mu.Unlock()
	if sess == nil || subtle.ConstantTimeEq(sess.secret, secret) != 1 {
		return
	}
	sess.mu.Lock()
	if sess.running != nil {
		sess.running()
	}
	sess.mu.Unlock()
}

// expire makes every session of user re-read its role before its next
// statement.
func (r *registry) expire(user string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sess := range r.byPID {
		sess.mu.Lock()
		if sess.user.Name == user {
			sess.checked = time.Time{}
		}
		sess.mu.Unlock()
	}
}

// statementCache releases the DuckDB statement behind a named statement when
// the client replaces or closes it. Without this, clients that prepare a
// fresh unnamed statement for every query would leak one per query.
type statementCache struct {
	wire.StatementCache
	names map[string]*wire.PreparedStatement
}

func newStatementCache() wire.StatementCache {
	return &statementCache{
		StatementCache: wire.DefaultStatementCacheFn(),
		names:          make(map[string]*wire.PreparedStatement),
	}
}

func (c *statementCache) Set(ctx context.Context, name string, ps *wire.PreparedStatement) error {
	if err := c.drop(ctx, name); err != nil {
		return err
	}
	if sess := sessionFrom(ctx); sess != nil {
		sess.statements = c
	}
	c.names[name] = ps
	return c.StatementCache.Set(ctx, name, ps)
}

func (c *statementCache) Delete(ctx context.Context, name string) error {
	if err := c.drop(ctx, name); err != nil {
		return err
	}
	return c.StatementCache.Delete(ctx, name)
}

func (c *statementCache) drop(ctx context.Context, name string) error {
	ps, ok := c.names[name]
	if !ok {
		return nil
	}
	delete(c.names, name)
	if sess := sessionFrom(ctx); sess != nil {
		return sess.release(ps)
	}
	return nil
}

// minLevel drops log records below a level.
type minLevel struct {
	slog.Handler
	level slog.Level
}

func (h minLevel) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level && h.Handler.Enabled(ctx, l)
}

func (h minLevel) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minLevel{h.Handler.WithAttrs(attrs), h.level}
}

func (h minLevel) WithGroup(name string) slog.Handler {
	return minLevel{h.Handler.WithGroup(name), h.level}
}
