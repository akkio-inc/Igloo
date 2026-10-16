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
	"slices"
	"strings"
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
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sharing"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/tasks"
	"github.com/akkio-inc/igloo/internal/workload"
)

// Engine runs queries; *engine.Engine implements it.
type Engine interface {
	Connect(ctx context.Context) (*duckdb.Conn, error)
	// ConnectMetered opens a session's connection, with the Meter that
	// reads what its statements used and closes it.
	ConnectMetered(ctx context.Context) (*duckdb.Conn, *engine.Meter, error)
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
	// Grants keeps custom roles and grants. Nil means Users if it keeps
	// them too, and otherwise a store in memory.
	Grants auth.GrantStore
	// Audit, if set, records the query and login history.
	Audit *audit.Log
	// Catalog is the name in SQL of the catalog grants apply to. Empty
	// means the engine's Iceberg catalog.
	Catalog string
	// Tasks keeps tasks, pipes, materialized views and the load history,
	// and runs them. Nil means a store in memory, whose jobs only run when
	// asked to.
	Tasks *tasks.Scheduler
	// Tokens keeps access tokens, Accounts what users are besides their
	// password and role, and SSO single sign-on links and group mappings.
	// Each nil means Users if it keeps them too; without one, what needs
	// it is unavailable.
	Tokens   auth.TokenStore
	Accounts auth.AccountStore
	SSO      auth.SSOStore
	// DisableSSOPasswords refuses password logins of users linked to
	// single sign-on; they sign in with access tokens instead.
	DisableSSOPasswords bool
	// TokenMaxLifetime caps how long an access token may last. Zero means
	// 365 days.
	TokenMaxLifetime time.Duration
	// AccessTokens, if set, lets users without a password sign in to SQL
	// with an identity provider's access token (see accesstoken.go).
	AccessTokens AccessTokenVerifier
	// Policies keeps masking policies, row access policies and tags. Nil
	// means a store in memory.
	Policies policy.Store
	// AllowedLocations are where users other than admins may read files
	// from while tables have policies (IGLOO_ALLOWED_LOCATIONS), and
	// Warehouse is the warehouse's location, whose files they may not.
	AllowedLocations []string
	Warehouse        string
	// Settings keeps the workload settings of users and roles. Nil means
	// Grants if it keeps them too.
	Settings auth.SettingStore
	// Workload holds statements back for their users' limits. Nil means
	// a controller that knows no memory budget.
	Workload *workload.Controller
	// Pool names the compute pool this replica belongs to, for the
	// history.
	Pool string
	// Sharing carries out Delta Sharing statements and keeps external
	// tables. Nil when it is not set up.
	Sharing *sharing.Service

	// external and externalFiles stand in for Sharing's external tables
	// and the prefix of their files, in tests.
	external      externalCatalog
	externalFiles string
}

// externalCatalog is what statements need of Delta Sharing's external
// tables, views in the local database over files that only Igloo's proxy
// serves (*sharing.Consumer implements it).
type externalCatalog interface {
	// IsExternal reports whether schema.name is an external table, and
	// how many Parquet scans its view makes.
	IsExternal(schema, name string) (scans int, ok bool)
	// TableNames lists the external tables in schema.
	TableNames(schema string) []string
	// Prepare points a statement's references to external tables at their
	// views, and makes sure those list the tables' current files.
	Prepare(ctx context.Context, query string) (string, error)
	// Sync loads external tables created on other replicas.
	Sync(ctx context.Context) (bool, error)
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
	grants      auth.GrantStore
	audit       *audit.Log
	catalog     string
	tasks       *tasks.Scheduler
	// gov holds the policies (see protect.go).
	gov      *governance
	settings auth.SettingStore
	workload *workload.Controller
	pool     string
	// shares carries out Delta Sharing statements, and external are its
	// external tables.
	shares   *sharing.Service
	external externalCatalog
	// ddlGen counts statements run here that may change the catalog; see
	// catalogGeneration.
	ddlGen atomic.Uint64
	// mockSecret derives the credentials a login for an unknown user runs
	// against (see mockCredentials).
	mockSecret []byte

	tokens              auth.TokenStore
	accounts            auth.AccountStore
	sso                 auth.SSOStore
	tokenChecker        *auth.TokenChecker
	disableSSOPasswords bool
	tokenMaxLifetime    time.Duration
	accessTokens        AccessTokenVerifier
	// limiter refuses logins after too many failures.
	limiter *loginLimiter
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
		grants:      opts.Grants,
		audit:       opts.Audit,
		catalog:     opts.Catalog,
		tasks:       opts.Tasks,
		settings:    opts.Settings,
		workload:    opts.Workload,
		pool:        opts.Pool,
		shares:      opts.Sharing,
		external:    opts.external,
		mockSecret:  mockSecret,

		tokens:              opts.Tokens,
		accounts:            opts.Accounts,
		sso:                 opts.SSO,
		disableSSOPasswords: opts.DisableSSOPasswords,
		tokenMaxLifetime:    opts.TokenMaxLifetime,
		accessTokens:        opts.AccessTokens,
		limiter:             newLoginLimiter(),
	}
	if s.tasks == nil {
		s.tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{Log: log})
	}
	if s.roleRefresh == 0 {
		s.roleRefresh = 30 * time.Second
	}
	if s.grants == nil {
		if g, ok := opts.Users.(auth.GrantStore); ok {
			s.grants = g
		} else {
			s.grants = auth.NewMemoryGrants(opts.Users)
		}
	}
	if s.tokens == nil {
		s.tokens, _ = opts.Users.(auth.TokenStore)
	}
	if s.accounts == nil {
		s.accounts, _ = opts.Users.(auth.AccountStore)
	}
	if s.sso == nil {
		s.sso, _ = opts.Users.(auth.SSOStore)
	}
	if s.tokens != nil {
		s.tokenChecker = auth.NewTokenChecker(opts.Users, s.tokens)
	}
	if s.tokenMaxLifetime == 0 {
		s.tokenMaxLifetime = 365 * 24 * time.Hour
	}
	policies := opts.Policies
	if policies == nil {
		policies = policy.NewMemoryStore()
	}
	externalFiles := opts.externalFiles
	if opts.Sharing != nil {
		s.external, externalFiles = opts.Sharing.Consumer(), opts.Sharing.ProxyPrefix()
	}
	if s.gov, err = newGovernance(context.Background(), policies, s.roleRefresh, log, engine.Connect,
		opts.AllowedLocations, opts.Warehouse, externalFiles); err != nil {
		return nil, err
	}
	if e, ok := engine.(interface {
		SetPolicies(func() *policy.Snapshot)
	}); ok {
		e.SetPolicies(func() *policy.Snapshot { return s.gov.snapshot(context.Background()) })
	}
	if s.settings == nil {
		s.settings, _ = s.grants.(auth.SettingStore)
	}
	if s.workload == nil {
		s.workload = workload.NewController(0)
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
	err := s.wire.Shutdown(ctx)
	s.gov.close()
	return err
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
	// client describes where the session comes from, for the console's
	// list of sessions and the history.
	client string
	// settingGen counts the session's SET statements, which may change
	// how its statements resolve names.
	settingGen uint64
	// tokenID and tokenName identify the Igloo access token the session
	// signed in with, if any: the session ends when the token is revoked
	// or, at tokenExpires, expires.
	tokenID, tokenName string
	tokenExpires       time.Time
	// method is how the session signed in: one of audit's Method
	// constants, or empty for the console's sessions.
	method string
	// sens is what the session's statements may name that policies
	// concern, and settings whether its settings let policies apply (see
	// protect.go).
	sens     *sensitivity
	settings *settingsCheck

	// mu guards the fields below, which cancel requests and user changes
	// reach from other connections.
	mu      sync.Mutex
	user    auth.User
	checked time.Time
	running context.CancelFunc
	// statement and started describe the running statement.
	statement string
	started   time.Time
	// privs are the user's grants, loaded when first needed.
	privs privilegeSnapshot
	// id identifies the session in the history; see sessionID.
	id string
	// appName is the client's application_name.
	appName string

	// wl is what the session's statements run under: query tag, limits
	// and meter (see workload.go).
	wl workloadState
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

// methodTask marks the sessions tasks, pipes and materialized views run
// in.
const methodTask = "task"

// bearer reports whether the session acts on a token's authority, Igloo's
// or the identity provider's, rather than a password's or single
// sign-on's: it signed in with one, or it runs a task, which a token's
// session may have created. Such a session may not mint its user a
// longer-lived credential: a token or a password.
func (sess *session) bearer() bool {
	switch sess.method {
	case audit.MethodToken, audit.MethodAccessToken, methodTask:
		return true
	}
	return false
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
	conn, meter, err := s.engine.ConnectMetered(ctx)
	if err != nil {
		s.sessions.remove(sess)
		s.releaseConn()
		return ctx, fmt.Errorf("open session: %w", err)
	}
	sess.conn, sess.wl.meter, sess.wl.controller = conn, meter, s.workload
	for _, stmt := range sessionCatalogSetup(sess.user.Name, sess.user.Role == auth.RoleAdmin, s.version) {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			s.sessions.remove(sess)
			s.releaseConn()
			return ctx, errors.Join(fmt.Errorf("set up session catalog: %w", err), meter.Close())
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
	return errors.Join(append(errs, sess.wl.meter.Close())...)
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
// role changes, disabled and dropped users and revoked or expired access
// tokens take effect on open connections.
func (s *Server) refreshUser(ctx context.Context, sess *session) (auth.User, error) {
	sess.mu.Lock()
	user, fresh := sess.user, time.Since(sess.checked) < s.roleRefresh
	sess.mu.Unlock()
	ended := func(format string, args ...any) (auth.User, error) {
		return user, pgerror.WithSeverity(pgerror.WithCode(
			fmt.Errorf(format, args...), codes.InvalidAuthorizationSpecification), pgerror.LevelFatal)
	}
	if !sess.tokenExpires.IsZero() && !time.Now().Before(sess.tokenExpires) {
		return ended("the access token %q this session signed in with expired", sess.tokenName)
	}
	if fresh {
		return user, nil
	}

	latest, err := s.users.Get(ctx, user.Name)
	if errors.Is(err, auth.ErrNotFound) {
		return ended("user %q no longer exists", user.Name)
	}
	if err == nil && latest.Disabled {
		return ended("user %q is not permitted to log in", user.Name)
	}
	if err == nil && sess.tokenID != "" && s.tokens != nil {
		if _, _, terr := s.tokens.TokenByID(ctx, sess.tokenID); errors.Is(terr, auth.ErrTokenNotFound) {
			return ended("the access token %q this session signed in with was revoked", sess.tokenName)
		} else if terr != nil {
			err = terr
		}
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
// session's statement timeout, can end. query is the statement as the
// client sent it, which the console lists while it runs.
func (sess *session) runCancelable(ctx context.Context, query string, fn func(context.Context) error) error {
	defer sess.activity.Begin()()
	var cancel context.CancelFunc
	if timeout := sess.statementTimeout(); timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	sess.mu.Lock()
	sess.running, sess.statement, sess.started = cancel, query, time.Now()
	sess.mu.Unlock()

	defer func() {
		sess.mu.Lock()
		sess.running, sess.statement, sess.started = nil, "", time.Time{}
		sess.mu.Unlock()
		cancel()
	}()

	// The statement may wait here for its user's limits; the timeout and
	// cancel requests apply while it does.
	release, err := sess.admit(ctx)
	if err != nil {
		return err
	}
	defer release()
	err = fn(ctx)
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

	s.refreshLimits(ctx, sess, user)

	if stmt, ok := s.statementTimeoutCommand(sess, q.Query); ok {
		return stmt, nil
	}
	if stmt, ok := s.queryTagCommand(sess, q.Query); ok {
		return stmt, nil
	}

	// psycopg sends this after DDL to drop its prepared statements; DuckDB
	// has no DEALLOCATE ALL.
	if deallocateAll.MatchString(q.Query) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if c := sess.statements; c != nil {
				for name := range c.names {
					if err := c.Delete(ctx, name); err != nil {
						return err
					}
				}
			}
			return w.Complete("DEALLOCATE ALL")
		}))), nil
	}

	if tag, ok := ignoredCommand(q.Query); ok {
		appName, setsAppName := applicationNameValue(q.Query)
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if setsAppName {
				sess.setApplicationName(appName)
			}
			return w.Complete(tag)
		}))), nil
	}

	// psql's \password asks which hash to send before ALTER USER.
	if showPasswordEncryption.MatchString(q.Query) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{"scram-sha-256"}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}), wire.WithColumns(wire.Columns{{Name: "password_encryption", Oid: pgtype.TextOID, Width: -1}}))), nil
	}

	if cmds, ok, err := parseUserCommands(q.Query); ok {
		if err == nil && len(cmds) > 1 && !q.SimpleQuery {
			err = pgerror.WithCode(errors.New("cannot prepare several statements at once"), codes.Syntax)
		}
		if err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, err
		}
		var stmts wire.PreparedStatements
		for _, cmd := range cmds {
			stmts = append(stmts, wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				start := time.Now()
				err := s.runUserCommand(ctx, sess, cmd, w)
				s.metrics.query(start, err)
				return err
			}), wire.WithColumns(cmd.columns())))
		}
		return stmts, nil
	}

	if err := s.checkWarehouseFiles(ctx, user, q.Query); err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, err
	}

	if ps, ok, err := s.loadingCommand(sess, q.Query); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.policyCommand(ctx, sess, q); ok || err != nil {
		return ps, err
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
		tw := &tagWriter{DataWriter: w}
		if err := s.recheck(ctx, sess, st); err != nil {
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: st.access, policies: st.policies()})
			return err
		}
		err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			return runWithCommitRetry(ctx, sess, st, func() error {
				return sess.profiled(ctx, st, func() error { return st.run(ctx, args, tw) })
			})
		})
		var used *audit.Resources
		if err == nil {
			used = sess.measure(st)
		}
		queued := sess.wl.queued
		if isIcebergNoReplace(err) {
			if sess.inTx {
				err = pgerror.WithCode(errors.New("CREATE OR REPLACE TABLE on an Iceberg table cannot run inside a transaction block"),
					codes.ActiveSQLTransaction)
			} else if r, ok := s.replaceable(sess, st); ok {
				err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error { return s.replaceTable(ctx, sess, r) })
				if err == nil {
					err = tw.Complete("CREATE TABLE")
				}
			}
		}
		s.metrics.query(start, err)
		sess.observe(st, err)
		if err == nil {
			switch {
			case changesCatalog(st.stmtType):
				s.ddlGen.Add(1)
			case st.stmtType == duckdb.STATEMENT_TYPE_SET || st.stmtType == duckdb.STATEMENT_TYPE_VARIABLE_SET:
				sess.settingGen++
			}
			if st.effect != nil {
				s.applyEffect(ctx, sess, st.effect)
			}
		}
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: tw.rows(), acc: st.access,
			policies: st.policies(), used: used, queued: queued})
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		return pgError(err)
	}, wire.WithColumns(st.columns), wire.WithParameters(st.params))

	sess.open[ps] = st
	return wire.Prepared(ps), nil
}

// prepareStatement rewrites text for DuckDB, checks it against the user's
// role and grants and prepares it on the session's connection. oids are
// the parameter types the client declared. Errors carry PostgreSQL codes,
// and statements that fail here are recorded in the history.
func (s *Server) prepareStatement(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	start := time.Now()
	st, err := s.prepareChecked(ctx, sess, user, text, oids)
	if err != nil {
		if pgerror.GetCode(err) == codes.InsufficientPrivilege {
			s.metrics.query(time.Now(), err)
		}
		s.recordQuery(sess, queryRecord{text: text, start: start, err: err, rows: -1})
		return nil, err
	}
	return st, nil
}

// rewrite points the names in text that Igloo serves itself (PostgreSQL's
// catalog, the history views, the catalog's views) at where they are.
func (s *Server) rewrite(ctx context.Context, user auth.User, text string) string {
	query := rewriteHistoryRefs(rewriteSessionUser(rewriteCatalogQuery(text), user.Name), s.engine.LocalDatabase())
	if s.ddl != nil {
		query = s.ddl.RewriteViewRefs(query)
		if readsComments.MatchString(query) {
			if err := s.ddl.RefreshComments(ctx); err != nil {
				s.log.Warn("could not load comments", "err", err)
			}
		}
	}
	return query
}

// prepareChecked is prepareStatement without recording failures, for the
// statements Igloo runs for a user to carry out one of its own commands
// (COPY INTO, materialized views): they are checked exactly like the
// user's own.
func (s *Server) prepareChecked(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	fail := func(err error) (*statement, error) { return nil, err }
	query := s.rewrite(ctx, user, text)

	// Preparing runs all but the last of several statements, so this
	// comes first.
	if err := forbidden(query); err != nil {
		return fail(err)
	}

	// Preparing a multi-statement query runs all but the last statement, so
	// only admins, whose statements need no checks, may send one. If the
	// count fails, the query does not parse; stop here rather than let the
	// session connection interpret it.
	pivot := false // a query with a PIVOT that DuckDB runs as several
	if user.Role != auth.RoleAdmin {
		query = sessionScoped(query)
		n, err := s.engine.CountStatements(query)
		if err != nil {
			return fail(pgerror.WithCode(err, codes.Syntax))
		}
		if n > 1 {
			if err := multipleStatements(user.Role, query); err != nil {
				return fail(err)
			}
			pivot = true
		}
	} else if strings.Contains(strings.TrimRight(strings.TrimSpace(query), "; \t\r\n"), ";") {
		// Preparing may run statements that change the catalog or the
		// session's settings; count it as doing both.
		s.ddlGen.Add(1)
		sess.settingGen++
	}
	// Policies are applied to one statement at a time, before it runs;
	// preparing several runs all but the last.
	if err := s.protectBeforePrepare(ctx, sess, user, query, pivot); err != nil {
		s.metrics.query(time.Now(), errDenied)
		return fail(err)
	}

	// Confine the files a non-admin names, before DuckDB prepares the
	// statement: preparing a replacement scan or a schema-inferring read
	// opens the file, so this cannot wait until after. When a table has a
	// policy, the policy rewrite checks the files instead (guardFiles stops).
	if err := s.guardFiles(ctx, sess, user, query); err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		s.metrics.query(time.Now(), err)
		return fail(pgError(err))
	}

	if listsColumns.MatchString(query) {
		// Best effort: a table that fails to load keeps its placeholder.
		if err := s.engine.LoadSchemas(ctx, sess.conn); err != nil {
			s.log.Warn("could not load table schemas", "err", err)
		}
	}
	if s.external != nil {
		// External tables the statement names must list their current
		// files before DuckDB binds their views.
		external, err := s.external.Prepare(ctx, query)
		if err != nil {
			return fail(pgerror.WithCode(err, codeExternal))
		}
		query = external
	}
	st, err := prepare(ctx, sess.conn, query, oids)
	if err != nil && s.ddl != nil && missingRelation.MatchString(err.Error()) {
		// Perhaps a view another replica just created: sync and try again.
		if changed, _ := s.ddl.SyncViews(ctx); changed {
			query = s.ddl.RewriteViewRefs(query)
			st, err = prepare(ctx, sess.conn, query, oids)
		}
	}
	if err != nil && s.external != nil && missingRelation.MatchString(err.Error()) {
		// Or an external table another replica just created.
		if changed, _ := s.external.Sync(ctx); changed {
			external, perr := s.external.Prepare(ctx, query)
			if perr != nil {
				return fail(pgerror.WithCode(perr, codeExternal))
			}
			query = external
			st, err = prepare(ctx, sess.conn, query, oids)
		}
	}
	if err != nil {
		sess.observe(nil, err)
		return fail(pgError(err))
	}
	if pivot && st.stmtType != duckdb.STATEMENT_TYPE_SELECT {
		// multipleStatements let through what it takes for a query.
		s.log.Error("a PIVOT statement that is not a query was prepared", "user", user.Name, "type", st.stmtType)
		_ = st.close()
		return fail(pgerror.WithCode(errors.New("permission denied: only admins may run this PIVOT"), codes.InsufficientPrivilege))
	}
	// Apply policies: what runs is the statement rewritten to read
	// protected tables through them.
	text, g, ran, err := s.policyRewrite(ctx, sess, user, st.stmtType, query, oids)
	if err == nil && text != query {
		st.original = query
		err = st.adopt(ctx, sess.conn, text)
	} else if err == nil && ran {
		err = st.reprepare(ctx, sess.conn)
	}
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		s.metrics.query(time.Now(), err)
		_ = st.close()
		return fail(pgError(err))
	}
	st.guard = g
	err = authorize(user.Role, st.stmtType, query)
	if err != nil {
		// The built-in role does not allow it; grants may.
		st.access, err = s.authorizeByGrants(ctx, sess, user, st, query, err)
	}
	if err == nil {
		err = s.authorizeLocal(user.Role, st.stmtType, text)
	}
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		_ = st.close()
		return fail(err)
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_DROP || st.stmtType == duckdb.STATEMENT_TYPE_ALTER {
		st.effect = s.ddlEffect(ctx, sess, query)
	}
	if st.access != nil {
		// Checking grants ran statements on the session's connection.
		if err := st.reprepare(ctx, sess.conn); err != nil {
			_ = st.close()
			return fail(pgError(err))
		}
	}
	return st, nil
}

// multipleStatements decides whether a non-admin may run query, which
// DuckDB parses as several statements, all but the last of which run while
// it is prepared, before any check that needs the prepared statement.
//
// Only a query (a SELECT) with a PIVOT that takes its columns from the data
// qualifies. DuckDB finds the columns first, with one generated statement
// per such PIVOT: CREATE TEMP TYPE __pivot_enum_<uuid> AS ENUM (SELECT
// DISTINCT <column> FROM <the PIVOT's source> ...), which stays in the
// session's temp schema. That is the only way its parser turns one
// statement into several (Transformer::CreatePivotStatement in DuckDB 1.5).
// Those statements read what the PIVOT reads, so the query's text is
// checked as a SELECT's before they run; the query itself is checked as
// usual once prepared, and must be a SELECT.
//
// Any other statement with such a PIVOT (CREATE TABLE AS, INSERT, EXPLAIN)
// is refused: DuckDB then also wraps the statements in BEGIN and COMMIT,
// or in SET current_transaction_invalidation_policy inside a transaction
// (StatementPreprocessor), so preparing would run the statement itself and
// leave only the COMMIT to check. A statement is a query when its first
// word says so; DuckDB's grammar decides a statement's kind by it too. Users
// with the none role are refused: what they may read is known only from
// the prepared statement.
func multipleStatements(role auth.Role, query string) error {
	err := errors.New("send one statement at a time; only admins may send several in one query")
	words := sqllex.CodeWords(query)
	if len(sqllex.Split(query)) == 1 && (slices.Contains(words, "PIVOT") || slices.Contains(words, "PIVOT_WIDER")) {
		switch {
		case role == auth.RoleNone:
			err = errors.New("permission denied: a PIVOT that takes its columns from the data runs a query before " +
				"Igloo can check it; list the columns instead, as in PIVOT ... ON col IN ('a', 'b')")
		case !startsQuery(query):
			err = errors.New("permission denied: only admins may use a PIVOT that takes its columns from the data " +
				"outside a query; list the columns instead, as in PIVOT ... ON col IN ('a', 'b')")
		default:
			return authorize(role, duckdb.STATEMENT_TYPE_SELECT, query)
		}
	}
	return pgerror.WithCode(err, codes.InsufficientPrivilege)
}

// startsQuery reports whether query's first word makes it a SELECT
// statement in DuckDB's grammar. WITH does not: a CTE may come before
// INSERT, UPDATE or DELETE.
func startsQuery(query string) bool {
	toks := sqlTokens(query)
	if len(toks) == 0 {
		return false
	}
	switch toks[0].keyword {
	case "SELECT", "FROM", "PIVOT", "PIVOT_WIDER", "UNPIVOT", "PIVOT_LONGER":
		return true
	}
	return toks[0].punct == '('
}

// recheckGrants decides again, before it runs, a statement that grants
// allowed, if the decision may be stale: the session's grants were reloaded
// since, the catalog or the session's settings changed, or the decision is
// as old as the role refresh interval. A prepared statement then outlives a
// REVOKE no longer than a session's cached grants do, and cannot be made to
// read a table nobody checked by replacing a view or changing the search
// path before it runs. force decides again regardless, as after policies
// rewrote the statement anew.
func (s *Server) recheckGrants(ctx context.Context, sess *session, st *statement, force bool) error {
	if st.access == nil {
		return nil
	}
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return err
	}
	privs, err := s.privileges(ctx, sess, user)
	if !force && err == nil && privs == st.access.privs && time.Since(st.access.checked) < s.roleRefresh &&
		st.access.catalogGen == s.catalogGeneration() && st.access.settingGen == sess.settingGen {
		return nil
	}
	roleErr := authorize(user.Role, st.stmtType, st.sourceText())
	if roleErr == nil {
		return nil
	}
	acc, err := s.authorizeByGrants(ctx, sess, user, st, st.sourceText(), roleErr)
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return err
	}
	if err := st.reprepare(ctx, sess.conn); err != nil {
		return pgError(err)
	}
	st.access = acc
	return nil
}

// statementTimeoutCommand handles SET, RESET and SHOW statement_timeout,
// which Igloo enforces itself rather than DuckDB.
func (s *Server) statementTimeoutCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, query, fn), opts...)), true
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
			if err := w.Row([]any{formatTimeout(sess.statementTimeout())}); err != nil {
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

// expire makes every session of user re-read its role and grants before
// its next statement.
func (r *registry) expire(user string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sess := range r.byPID {
		sess.mu.Lock()
		if sess.user.Name == user {
			sess.checked = time.Time{}
			sess.privs.loaded = time.Time{}
			sess.wl.loaded = time.Time{}
		}
		sess.mu.Unlock()
	}
}

// expireAll makes every session re-read its grants before its next
// statement, as after a change to a custom role.
func (r *registry) expireAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sess := range r.byPID {
		sess.mu.Lock()
		sess.privs.loaded = time.Time{}
		sess.wl.loaded = time.Time{}
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
