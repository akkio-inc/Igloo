// Package pgwire serves Igloo over the PostgreSQL wire protocol, so psql, BI
// tools and ordinary Postgres drivers can connect without an Igloo client.
package pgwire

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"database/sql/driver"
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
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/prefetch"
	"github.com/akkio-inc/igloo/internal/sharing"
	"github.com/akkio-inc/igloo/internal/snowflake"
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
	// CatalogLock and CatalogShare take the lock that keeps changes to the
	// structure of the catalog's in-memory set of tables from running
	// alongside walks and binds of it, exclusively and shared; each returns the
	// func that releases it. See engine.Engine.CatalogLock.
	CatalogLock() func()
	CatalogShare() func()
	// CatalogShareContext is CatalogShare that gives up when ctx ends, and
	// CatalogLockedFor is how long the lock has been held exclusively.
	CatalogShareContext(ctx context.Context) (func(), error)
	CatalogLockedFor() time.Duration
	// CatalogChanged says the catalog changed other than through DuckDB's
	// own requests, so that DuckDB's next listings of it ask the catalog
	// again (see catalogproxy's listings); ListingsGeneration changes
	// whenever what those listings list may have.
	CatalogChanged()
	ListingsGeneration() uint64
	// ForgetMissingSchema makes DuckDB forget a schema of the catalog it
	// took for missing, if the catalog has it, and reports whether it did.
	ForgetMissingSchema(ctx context.Context, schema string) bool
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
	// Scanner lists statements' scans, and Prefetcher reads ahead what they
	// are about to read (see prefetch.go); either nil turns it off.
	Scanner    *distributed.Scanner
	Prefetcher *prefetch.Prefetcher
	// IDDomains keeps ID domains and their members (package iddomain). Nil
	// means a store in memory.
	IDDomains iddomain.Store
	// IDDomainsOn is IGLOO_ID_DOMAINS: columns may join ID domains.
	IDDomainsOn bool
	// IDSyncMinRows is IGLOO_ID_SYNC_MIN_ROWS: writes of fewer rows leave
	// their new values' ids to a mapping pass shortly after (see
	// idassign.go); 0 means none do.
	IDSyncMinRows int64
	// Bitmaps counts the distinct integer IDs of ID domains in roaring
	// bitmaps (the rb extension, which the engine must have loaded) in
	// statements that read at least BitmapMinRows rows; 0 means every such
	// statement (see bitmaps.go).
	Bitmaps       bool
	BitmapMinRows int64
	// BitmapIndexes lets tables have bitmap indexes, which Igloo builds
	// and answers counts of distinct ids from (IGLOO_BITMAP_INDEXES, with
	// IDDomainsOn and the rb extension; see bitmapindex.go).
	BitmapIndexes bool
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
	// CleanRooms carries out clean room statements; the server runs their
	// templates (see Execute). Nil when they are not set up.
	CleanRooms *cleanroom.Service

	// Federation keeps the connections to other databases. Nil means
	// there are none and CREATE CONNECTION is unavailable.
	Federation *federation.Manager
	// Clones makes zero-copy clones and manages tables' branches and tags.
	// Nil without a catalog.
	Clones *clone.Manager

	// Notify keeps notification integrations and sends alerts'
	// notifications. Nil means a service in memory that reaches only
	// public addresses.
	Notify *notify.Service

	// Metadata keeps the catalog's metadata for the catalog functions.
	// Nil means DDL, if set.
	Metadata MetadataSource
	// Owners keeps the owners of the catalog's objects. Nil means Grants
	// if it keeps them too, and otherwise a store in memory.
	Owners auth.OwnerStore
	// Lineage, if set, is told about every statement that wrote a table
	// (see catalog.go).
	Lineage LineageSink
	// Erasure carries out erasure requests. Nil without a catalog.
	Erasure *erasure.Service
	// ClonePurge, if set, decides whether a table dropped has its files
	// deleted with it (clone.Guard.MayPurge; see catalogguard.go).
	ClonePurge func(ctx context.Context, what string, name []string, location string, inCatalog bool) clone.PurgeVerdict

	// Distributed, if set, runs statements that read large tables in
	// parts on workers (docs/distributed-execution.md).
	Distributed *distributed.Coordinator

	// external and externalFiles stand in for Sharing's external tables
	// and the prefix of their files, and partners for its recipients and
	// providers, in tests.
	external      externalCatalog
	externalFiles string
	partners      partnerDirectory
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
	// CatalogTables describes them for the data catalog.
	CatalogTables() []catalog.TableMeta
	// Prepare points a statement's references to external tables at their
	// views, and makes sure those list the tables' current files.
	Prepare(ctx context.Context, query string) (string, error)
	// Sync loads external tables created on other replicas.
	Sync(ctx context.Context) (bool, error)
	// ProviderOf names the provider an external table reads from.
	ProviderOf(schema, name string) string
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
	loginWait   time.Duration
	maxConns    int
	conns       atomic.Int32
	version     string
	ddl         *ddl.Manager
	activity    *activity.Tracker
	grants      auth.GrantStore
	audit       *audit.Log
	catalog     string
	tasks       *tasks.Scheduler
	notify      *notify.Service
	clones      *clone.Manager
	erasure     *erasure.Service
	// clonePurge is Options.ClonePurge (see catalogguard.go).
	clonePurge func(ctx context.Context, what string, name []string, location string, inCatalog bool) clone.PurgeVerdict
	// gov holds the policies (see protect.go).
	gov *governance
	// idDomains keeps ID domains; idDomainsOn lets columns join them.
	idDomains   iddomain.Store
	idDomainsOn bool
	// members caches which tables have columns that hold ids.
	members memberCache
	// idHolder names this replica in ID domains' leases, idSeq numbers its
	// assignments, and idMapped is what maintenance found mapped (see
	// idassign.go).
	idHolder string
	idSeq    atomic.Uint64
	idMapped idMapped
	// idSyncMinRows and idDeferred are the writes that leave their ids to
	// a mapping pass shortly after, and those passes (see idassign.go).
	idSyncMinRows int64
	idDeferred    idDeferral
	// afterNewValues, if set, runs when an assignment has found new values
	// and is about to take the lease, for tests.
	afterNewValues func()
	// idComplete caches which member tables' snapshots have every row's
	// ids (see idcount.go).
	idComplete idComplete
	// scanner and prefetcher read ahead for statements, or are nil.
	scanner    *distributed.Scanner
	prefetcher *prefetch.Prefetcher
	// bitmaps and bitmapMinRows choose the statements that count distinct
	// IDs in roaring bitmaps, and bitmapReruns counts those run again with
	// DuckDB's hash (see bitmaps.go).
	bitmaps       bool
	bitmapMinRows int64
	bitmapReruns  atomic.Int64
	// bitmapIndexesOn is Options.BitmapIndexes, and indexBuilds runs their
	// maintenance in the background (see bitmapbuild.go).
	bitmapIndexesOn bool
	indexBuilds     indexBuilds
	// indexRewrites counts the SELECTs answered from bitmap indexes.
	indexRewrites atomic.Int64
	settings      auth.SettingStore
	workload      *workload.Controller
	pool          string
	// shares carries out Delta Sharing statements, and external are its
	// external tables.
	shares   *sharing.Service
	external externalCatalog
	// fed keeps the connections to other databases (see connreads.go), and
	// remotes is what reads are checked against: fed, unless a test
	// stands in for it.
	fed     *federation.Manager
	remotes connectionSource
	dist    *distributed.Coordinator
	// failLocal, if set, runs before a statement runs on this replica, and
	// fails it with its error; tests use it to make a statement run out of
	// memory here.
	failLocal func(ctx context.Context, st *statement) error
	// rewriteHook, if set, is the last of Igloo's rewrites of a statement
	// before it is checked and prepared (see rewrite); tests use it to
	// write what only Igloo may, such as the columns of ID domains' ids.
	rewriteHook func(string) string
	// rooms carries out clean room statements; partners are Delta
	// Sharing's recipients and providers, which partners are.
	rooms    *cleanroom.Service
	partners partnerDirectory
	// ddlGen counts statements run here that may change the catalog; see
	// catalogGeneration. catalogChanged adds to it.
	ddlGen atomic.Uint64
	// tables is the index of tables the checks resolve names with (see
	// indexedTables).
	tables tableIndex
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

	meta       MetadataSource
	owners     auth.OwnerStore
	ownerCache ownerCache
	lineage    LineageSink
	// connections lists the connections' tables for the catalog.
	connections connectionListing
	// analyzer works out what statements read and wrote, for the
	// history; nil without one.
	analyzer *analyzer
	// sketches says DuckDB has the datasketches extension's functions,
	// once sketchesKnown (see hasSketches).
	sketchesMu    sync.Mutex
	sketchesKnown bool
	sketches      bool
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
		loginWait:   loginCatalogWait,
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
		fed:         opts.Federation,
		notify:      opts.Notify,
		clones:      opts.Clones,
		dist:        opts.Distributed,
		erasure:     opts.Erasure,
		rooms:       opts.CleanRooms,
		partners:    opts.partners,
		mockSecret:  mockSecret,

		clonePurge: opts.ClonePurge,

		tokens:              opts.Tokens,
		accounts:            opts.Accounts,
		sso:                 opts.SSO,
		disableSSOPasswords: opts.DisableSSOPasswords,
		tokenMaxLifetime:    opts.TokenMaxLifetime,
		accessTokens:        opts.AccessTokens,
		limiter:             newLoginLimiter(),

		meta:    opts.Metadata,
		owners:  opts.Owners,
		lineage: opts.Lineage,
	}
	if s.meta == nil && opts.DDL != nil {
		s.meta = opts.DDL
	}
	if s.audit != nil {
		s.analyzer = newAnalyzer(s, s.audit.Immediate())
	}
	if s.tasks == nil {
		s.tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{Log: log})
	}
	if s.fed != nil {
		s.remotes = s.fed
	}
	if s.notify == nil {
		s.notify = notify.New(notify.Options{Log: log})
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
	if s.owners == nil {
		if o, ok := s.grants.(auth.OwnerStore); ok {
			s.owners = o
		} else {
			s.owners = auth.NewMemoryGrants(opts.Users)
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
	s.idDomains, s.idDomainsOn, s.idHolder = opts.IDDomains, opts.IDDomainsOn, newIDHolder()
	s.idSyncMinRows = opts.IDSyncMinRows
	s.scanner, s.prefetcher = opts.Scanner, opts.Prefetcher
	s.bitmaps, s.bitmapMinRows, s.bitmapIndexesOn = opts.Bitmaps, opts.BitmapMinRows, opts.BitmapIndexes
	if s.idDomains == nil {
		s.idDomains = iddomain.NewMemoryStore()
	}
	externalFiles := opts.externalFiles
	if opts.Sharing != nil {
		s.external, externalFiles = opts.Sharing.Consumer(), opts.Sharing.ProxyPrefix()
		if s.partners == nil {
			s.partners = sharingPartners{opts.Sharing}
		}
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
	if e, ok := engine.(catalogEngine); ok {
		e.SetCatalogViewer(s.CatalogViewer)
	}
	// Every change DuckDB makes to the catalog is checked (see
	// catalogguard.go).
	if e, ok := engine.(interface {
		SetCatalogGuard(catalogproxy.Guard)
	}); ok {
		e.SetCatalogGuard(s)
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
		wire.ExtendTypes(extendTypes),
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
	s.idDeferred.stop()
	if s.analyzer != nil {
		s.analyzer.close(ctx)
	}
	s.gov.close()
	return err
}

// session is the state of one client connection.
type session struct {
	pid, secret int32
	added       time.Time
	conn        *duckdb.Conn
	// memo keeps what the checks of the statement being prepared would
	// otherwise ask DuckDB more than once; nil outside prepareChecked.
	memo *checkMemo
	// bitmapsRestore sets rb_count_distinct back after the statement
	// running with it on (see setBitmaps); nil otherwise.
	bitmapsRestore func()
	// temps says the session may have temporary tables or views, which
	// the server's index of tables leaves out (see indexedTables).
	temps bool
	// open holds every DuckDB statement prepared for this client, keyed by
	// the wire statement that owns it, so none outlive the connection.
	open     map[*wire.PreparedStatement]*statement
	inTx     bool
	txFailed bool
	// changes counts the statements in the transaction block that may have
	// changed something, and savepoints are its savepoints (see
	// savepointCommand).
	changes    int
	savepoints []savepoint
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
	// prefetchOff turns reading ahead off for the session's statements
	// (SET igloo_prefetch = off).
	prefetchOff bool
	// tokenID and tokenName identify the Igloo access token the session
	// signed in with, if any: the session ends when the token is revoked
	// or, at tokenExpires, expires.
	tokenID, tokenName string
	tokenExpires       time.Time
	// method is how the session signed in: one of audit's Method
	// constants, or empty for the console's sessions.
	method string
	// job is the task, pipe or materialized view the session runs, as
	// "task nightly", for the history.
	job string
	// ddlCount counts the session's DDL, which may change its temporary
	// objects; names is what decides what names in its statements mean,
	// as last read (see objects.go).
	ddlCount uint64
	names    *sessionNames
	// sens is what the session's statements may name that policies
	// concern, and settings whether its settings let policies apply (see
	// protect.go).
	sens     *sensitivity
	settings *settingsCheck
	// distMode is the session's distributed_execution; empty means auto.
	distMode distributed.Mode
	// dialect is the session's own sql_dialect, startDialect the one its
	// connection asked for; empty means not set (see sqlDialect).
	dialect, startDialect string
	// catHolds are the holds on the engine's catalog lock this session's
	// steps have taken and not released, catHeld how the engine lock is held
	// for them, and catRelease what releases it, so the several steps that
	// reach the catalog's in-memory set while one statement is handled can
	// take it without deadlocking (see holdCatalog). Touched only by the
	// goroutine handling the session's statements, which is serial.
	catHolds   []*catalogHold
	catHeld    catalogMode
	catRelease func()

	// mu guards the fields below, which cancel requests and user changes
	// reach from other connections.
	mu      sync.Mutex
	user    auth.User
	checked time.Time
	running context.CancelFunc
	// statement and started describe the running statement; shed says
	// ShedMemory canceled it.
	statement string
	started   time.Time
	shed      bool
	// privs are the user's grants, loaded when first needed.
	privs privilegeSnapshot
	// fixed, when set, are the session's grants for good, and its user is
	// never read again: it runs a clean room's template as the room's
	// identity, which no store knows (see openRoomSession).
	fixed *auth.Privileges
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

// loginCatalogWait bounds how long opening a session waits for the catalog
// lock (see openSession); it is Server.loginWait, which tests shorten.
const loginCatalogWait = 30 * time.Second

func (s *Server) openSession(ctx context.Context) (context.Context, error) {
	sess := sessionFrom(ctx)
	if sess == nil {
		return ctx, errors.New("session was not authenticated")
	}
	// Opening the connection selects the catalog's default schema, and the
	// session's setup below defines its macros and views; both bind against
	// the catalog like any statement does (see the bind hold below), so the
	// session holds the lock shared through both. A listing or DDL holds it
	// exclusively while it runs, and one that runs long would keep every
	// login waiting with it, so a login waits at most loginWait for it.
	releaseCatalog, err := s.shareCatalogForLogin(ctx, sess)
	if err != nil {
		s.sessions.remove(sess)
		s.releaseConn()
		return ctx, err
	}
	defer releaseCatalog()
	conn, meter, err := s.engine.ConnectMetered(ctx)
	if err != nil {
		s.sessions.remove(sess)
		s.releaseConn()
		return ctx, fmt.Errorf("open session: %w", err)
	}
	sess.conn, sess.wl.meter, sess.wl.controller = conn, meter, s.workload
	// Binding a statement looks its tables up in the catalog's in-memory set
	// and replaces their entries there, and DuckDB binds a statement that
	// reads the catalog again each time it runs, not only when it is
	// prepared. Hold the catalog lock shared for every bind on this
	// connection, so none runs while DDL renames or drops a table or a
	// listing walks the set (see shareCatalog); running the statement after
	// that holds nothing more.
	conn.SetBindHold(func() func() { return s.shareCatalog(sess) })
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
	user, fresh := sess.user, time.Since(sess.checked) < s.roleRefresh || sess.fixed != nil
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

// errMemoryShed is what a statement ShedMemory canceled fails with.
var errMemoryShed = pgerror.WithCode(errors.New("canceling statement: the server was running out of memory, "+
	"and this statement had run the longest; run it again, or on less data at a time"), codes.OutOfMemory)

// runLocal runs st on this replica, unless failLocal, a test's, fails it.
func (s *Server) runLocal(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) error {
	if s.failLocal != nil {
		if err := s.failLocal(ctx, st); err != nil {
			return err
		}
	}
	return s.runStatement(ctx, sess, st, args, w)
}

// rearm returns the context to run a statement again under after ctx, the
// one runCancelable gave it, ended because the memory watch canceled it:
// one under outer that a cancel request or the memory watch can end, with
// what is left of the statement timeout. While ctx lasts, it is ctx.
func (sess *session) rearm(outer, ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	var again context.Context
	var cancel context.CancelFunc
	if d, ok := ctx.Deadline(); ok {
		again, cancel = context.WithDeadline(outer, d)
	} else {
		again, cancel = context.WithCancel(outer)
	}
	sess.mu.Lock()
	sess.running, sess.shed = cancel, false
	sess.mu.Unlock()
	return again, cancel
}

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
	sess.running, sess.statement, sess.started, sess.shed = cancel, redactQuery(query), time.Now(), false
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
	if err != nil && ctx.Err() != nil {
		sess.mu.Lock()
		shed := sess.shed
		sess.mu.Unlock()
		if shed {
			return errMemoryShed
		}
	}
	return err
}

// ShedMemory cancels the statement that has run longest on this replica,
// for memlimit.Watch, when the process is about to run out of memory:
// DuckDB keeps to its memory limit only for what its buffer manager
// tracks, and the kernel would otherwise kill the process, and every
// session with it. The statement fails with SQLSTATE 53200
// (out_of_memory). It reports whether a statement was running.
func (s *Server) ShedMemory(available int64) bool {
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	for _, sess := range r.byPID {
		all = append(all, sess)
	}
	r.mu.Unlock()
	var longest *session
	var since time.Time
	for _, sess := range all {
		sess.mu.Lock()
		if sess.running != nil && (longest == nil || sess.started.Before(since)) {
			longest, since = sess, sess.started
		}
		sess.mu.Unlock()
	}
	if longest == nil {
		s.log.Warn("memory is running out, and no statement is running to cancel", "available", memlimit.Format(available))
		return false
	}
	longest.mu.Lock()
	defer longest.mu.Unlock()
	if longest.running == nil {
		return false
	}
	longest.shed = true
	longest.running()
	s.log.Warn("memory is running out; canceled the statement that had run longest", "available", memlimit.Format(available),
		"user", longest.user.Name, "pid", longest.pid, "running_for", time.Since(longest.started).Round(time.Millisecond))
	return true
}

// observe updates transaction state after a statement runs, so ReadyForQuery
// tells clients and poolers whether a transaction is open or failed.
func (sess *session) observe(st *statement, err error) {
	if st != nil && st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION {
		if err == nil || isTransactionEnd(st.query) {
			// BEGIN inside a block, and COMMIT or ROLLBACK outside one,
			// change nothing (see redundantTransaction).
			switch change := transactionChange(st.query); {
			case change > 0 && !sess.inTx:
				sess.inTx, sess.txFailed = true, false
				sess.savepoints, sess.changes = nil, 0
			case change < 0 && sess.inTx:
				sess.inTx, sess.txFailed = false, false
				sess.savepoints, sess.changes = nil, 0
				// Cursors without WITH HOLD end with their transaction.
				sess.closeCursors(context.Background(), false)
			}
			return
		}
	}
	switch {
	case err != nil && sess.inTx:
		sess.txFailed = true
	case st != nil && sess.inTx && !st.onlyReads():
		sess.changes++
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
var listsColumns = regexp.MustCompile(`(?i)\b(pg_attribute|pg_class|information_schema"?\s*\.\s*"?columns|pg_information_schema_columns|duckdb_columns)\b`)

// listsCatalog matches a statement whose run walks the catalog's in-memory
// set of tables and views and so, unlike a plain data read, holds the
// catalog lock exclusively while it runs (see catalogRunLock). It covers
// DuckDB's catalog listing functions and the pg_catalog, information_schema
// and sqlite_master relations BI tools and drivers read, by the names the
// statement is written with and the names Igloo rewrites them to, and SHOW,
// DESCRIBE and PRAGMA. It over-approximates: a statement matched needlessly
// only runs on its own when it could have run alongside others.
var listsCatalog = regexp.MustCompile(`(?is)(^\s*(SHOW|DESCRIBE|DESC|PRAGMA)\b)|` +
	`\b(sqlite_master|sqlite_schema|sqlite_temp_master|sqlite_temp_schema|` +
	`duckdb_tables|duckdb_views|duckdb_columns|duckdb_constraints|duckdb_databases|duckdb_schemas|` +
	`information_schema|information_schema_tables|information_schema_schemata|` +
	`pg_catalog|pg_class|pg_attribute|pg_namespace|pg_tables|pg_views|pg_matviews|` +
	`pg_information_schema_columns|pg_description|pg_type|pg_proc)\b`)

var deallocateAll = regexp.MustCompile(`(?is)^\s*DEALLOCATE\s+(?:PREPARE\s+)?ALL\s*;?\s*$`)

var showPasswordEncryption = regexp.MustCompile(`(?is)^\s*SHOW\s+password_encryption\s*;?\s*$`)

func (s *Server) parse(ctx context.Context, q wire.Query) (wire.PreparedStatements, error) {
	// DuckDB reads SQL up to the first NUL, while Igloo's checks read all
	// of it; PostgreSQL refuses NUL in text, and so does Igloo, from the
	// web console and Flight SQL too, so the two never disagree.
	if strings.IndexByte(q.Query, 0) >= 0 {
		return nil, pgerror.WithCode(errors.New(`invalid byte sequence for encoding "UTF8": 0x00`), codes.CharacterNotInRepertoire)
	}
	sess := sessionFrom(ctx)
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return nil, err
	}

	s.refreshLimits(ctx, sess, user)
	if ps, ok := s.dialectCommand(sess, q.Query); ok {
		return ps, nil
	}
	if ps, ok := s.prefetchCommand(sess, q.Query); ok {
		return ps, nil
	}
	// In the Snowflake dialect the statement is translated first, so that
	// all that follows, the checks of the user's role, grants and policies
	// above all, sees only the DuckDB statement Igloo will run.
	var tr *translation
	if sess.sqlDialect() == dialectSnowflake && !isIglooSQL(ctx) {
		if parts := snowflake.Split(q.Query); q.SimpleQuery && len(parts) > 1 {
			return s.snowflakeScript(ctx, sess, q, parts)
		}
		if tr, err = s.translate(ctx, sess, q.Query); err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, err
		}
		if tr.Command != "" {
			// A statement that changes nothing in Igloo, such as USE
			// WAREHOUSE.
			tag := tr.Command
			ps := wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				return w.Complete(tag)
			})))
			return withNotices(ps, tr.Notices), nil
		}
		if !tr.Untranslated {
			q.Query = tr.SQL
		}
		// The translation decodes Snowflake's string escapes, which could
		// spell a NUL; the translator refuses them, and this checks again.
		if strings.IndexByte(q.Query, 0) >= 0 {
			return nil, pgerror.WithCode(errors.New(`invalid byte sequence for encoding "UTF8": 0x00`), codes.CharacterNotInRepertoire)
		}
	}
	ps, err := s.parseDuckDB(ctx, sess, user, q, tr)
	if err != nil || tr == nil {
		return ps, err
	}
	if after := tr.After; len(after) > 0 {
		// Such as the clustering key of CREATE TABLE ... CLUSTER BY, set
		// once the table is created and before the client hears it is.
		for _, p := range ps {
			p.WrapFn(func(fn wire.PreparedStatementFn) wire.PreparedStatementFn {
				return func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
					return fn(ctx, afterWriter{DataWriter: w, run: func() error { return pgError(s.runAfter(ctx, sess, after)) }}, params)
				}
			})
		}
	}
	return withNotices(ps, tr.Notices), nil
}

// afterWriter runs what a statement leaves for after it (see
// snowflake.Result.After) when the statement completes, before the client
// hears that it did; if that fails, the client gets the error instead.
type afterWriter struct {
	wire.DataWriter
	run func() error
}

func (a afterWriter) Complete(tag string) error {
	if err := a.run(); err != nil {
		return err
	}
	return a.DataWriter.Complete(tag)
}

// withNotices has each of stmts send notices, as warnings, before it runs.
func withNotices(stmts wire.PreparedStatements, notices []string) wire.PreparedStatements {
	if len(notices) == 0 {
		return stmts
	}
	for _, ps := range stmts {
		ps.WrapFn(func(fn wire.PreparedStatementFn) wire.PreparedStatementFn {
			return func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
				if n, ok := w.(wire.Noticer); ok {
					for _, msg := range notices {
						if err := n.Notice("01000", msg); err != nil {
							return err
						}
					}
				}
				return fn(ctx, w, params)
			}
		})
	}
	return stmts
}

// parseDuckDB parses q, a statement or simple query in DuckDB's dialect,
// for parse; tr is how it was translated from Snowflake's, or nil.
func (s *Server) parseDuckDB(ctx context.Context, sess *session, user auth.User, q wire.Query, tr *translation) (wire.PreparedStatements, error) {
	// User statements, alone or together, go to parseUserCommands below.
	if parts := sqllex.Split(q.Query); q.SimpleQuery && len(parts) > 1 {
		if _, users, _ := parseUserCommands(q.Query); !users {
			if ps, ok, err := s.policyCommand(ctx, sess, q); ok || err != nil {
				return ps, err
			}
			for _, p := range parts {
				if err := aloneError(p.Text); err != nil {
					s.metrics.query(time.Now(), err)
					s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
					return nil, err
				}
			}
			return s.parseScript(sess, parts), nil
		}
	}
	if err := sess.checkAborted(q.Query); err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, err
	}

	if stmt, ok := s.statementTimeoutCommand(sess, q.Query); ok {
		return stmt, nil
	}
	if stmt, ok := s.queryTagCommand(sess, q.Query); ok {
		return stmt, nil
	}
	if stmt, ok := s.distributedCommand(sess, q.Query); ok {
		return stmt, nil
	}
	if stmt, ok := s.showCommand(sess, q.Query); ok {
		return stmt, nil
	}
	if stmt, ok := s.savepointCommand(sess, q.Query); ok {
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

	if ps, ok, err := s.notificationCommand(sess, q.Query); ok || err != nil {
		return ps, err
	}
	if ps, ok, err := s.roomStatement(sess, q); ok || err != nil {
		return ps, err
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

	if ps, ok, err := s.ownerCommand(sess, q.Query); ok || err != nil {
		return ps, err
	}

	if err := s.checkWarehouseFiles(ctx, user, q.Query); err != nil {
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, err
	}

	if ps, ok, err := s.loadingCommand(sess, q.Query); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.erasureCommand(sess, q.Query); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.cloneStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.refStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.sortStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.domainStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.indexStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.depthStatement(sess, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.policyCommand(ctx, sess, q); ok || err != nil {
		return ps, err
	}

	if cmd, ok, err := parseConnectionCommand(q.Query); ok {
		if err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, err
		}
		return wire.Prepared(wire.NewStatement(s.audited(sess, q.Query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			start := time.Now()
			err := s.runConnectionCommand(ctx, sess, cmd, w)
			s.metrics.query(start, err)
			return err
		}), wire.WithColumns(cmd.columns()))), nil
	}

	if ps, ok, err := s.catalogDDL(ctx, sess, user, q.Query); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.cursorCommand(ctx, sess, user, q); ok || err != nil {
		return ps, err
	}

	if ps, ok, err := s.copyToCommand(ctx, sess, user, q); ok || err != nil {
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
		// A statement counting distinct IDs in roaring bitmaps runs, and
		// is rechecked, with the rb extension's setting on, as it was
		// prepared (see bitmaps.go).
		sess.bitmapsRestore = sess.setBitmaps(ctx, st.bitmaps)
		defer func() {
			if sess.bitmapsRestore != nil {
				sess.bitmapsRestore()
				sess.bitmapsRestore = nil
			}
		}()
		err = sess.checkAborted(st.query)
		if err == nil {
			// Rechecking re-plans with EXPLAIN and may re-apply policies,
			// both of which read and fill in the catalog's set; release
			// before the run takes the side it needs (see holdCatalog,
			// catalogRunLock).
			relist := s.holdCatalog(sess)
			err = s.recheck(ctx, sess, st)
			relist()
		}
		if err == nil {
			// One that counts through ID domains' ids reads the snapshots
			// that have them (see idcount.go); looking them up asks the
			// catalog, which holds nothing, and preparing again takes the
			// lock it needs.
			bitmaps := st.bitmaps
			if err = s.refreshIDCounts(ctx, sess, st); err == nil {
				// And one answered from bitmap indexes, their tables'
				// current snapshots, or the tables (see bitmaprewrite.go).
				err = s.refreshIndexes(ctx, sess, st)
			}
			if err == nil && st.bitmaps != bitmaps {
				if sess.bitmapsRestore != nil {
					sess.bitmapsRestore()
				}
				sess.bitmapsRestore = sess.setBitmaps(ctx, st.bitmaps)
			}
		}
		if err != nil {
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: st.access, policies: st.policies()})
			return err
		}
		var parts distParts
		// Hold the catalog lock only while the statement runs, not through
		// the metrics and ownership bookkeeping below, to keep the exclusive
		// window short (see catalogRunLock, holdCatalog).
		releaseCatalog := s.catalogRunLock(sess, st)
		outer := ctx
		err = sess.runCancelable(ctx, q.Query, func(ctx context.Context) error {
			runOnce := func() error {
				p, done, err := s.runDistributed(ctx, sess, sess.currentUser(), st, args, tw, sessionMode(sess, sess.currentUser()))
				parts = p
				if done {
					return err
				}
				if p.release != nil {
					defer p.release()
				}
				// It runs here, as auto mode chose or for want of workers:
				// read ahead what its scans are about to read (see
				// prefetch.go). Workers read ahead for their own tasks.
				stopPrefetch := s.prefetchStatement(ctx, sess, st)
				if parts.local > 0 && tw.Written() == 0 {
					// Auto mode chose this replica on an estimate that can run
					// far short (see guardLocal).
					guard := guardLocal(ctx, parts.local, tw)
					err = s.runLocal(guard.ctx, sess, st, args, tw)
					stopPrefetch()
					if fired, ran := guard.stop(tw); fired && ctx.Err() == nil && outer.Err() == nil {
						return s.afterGuard(ctx, sess, st, args, tw, &parts, ran)
					}
				} else {
					err = s.runLocal(ctx, sess, st, args, tw)
					stopPrefetch()
				}
				if st.bitmaps && bitmapsDidNotFit(err) {
					// Run again here with the hash, below, rather than on
					// workers with bitmaps.
					return err
				}
				if err != nil && parts.key != "" && tw.Written() == 0 && outer.Err() == nil && sess.ranOutOfMemory(ctx, err) {
					return s.afterOutOfMemory(outer, ctx, sess, st, args, tw, &parts, err)
				}
				return err
			}
			run := func() error {
				err := runOnce()
				if st.bitmaps && bitmapsDidNotFit(err) && tw.Written() == 0 && outer.Err() == nil {
					return s.rerunWithoutBitmaps(ctx, sess, st, err, runOnce)
				}
				return err
			}
			switch {
			case st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION:
				return sess.runTransaction(ctx, st, tw, run)
			case st.changesIceberg:
				return sess.outsideTransaction(ctx, run)
			case st.stmtType == duckdb.STATEMENT_TYPE_SET || st.stmtType == duckdb.STATEMENT_TYPE_VARIABLE_SET:
				return s.runSetting(ctx, sess, st, args, tw)
			}
			return run()
		})
		releaseCatalog()
		var used *audit.Resources
		if err == nil {
			used = sess.measure(st)
		}
		queued := sess.wl.queued
		s.metrics.query(start, err)
		sess.observe(st, err)
		if err == nil {
			switch {
			case changesCatalog(st.stmtType):
				s.catalogChanged()
				sess.ddlCount++
			case st.stmtType == duckdb.STATEMENT_TYPE_SET || st.stmtType == duckdb.STATEMENT_TYPE_VARIABLE_SET:
				sess.settingGen++
			}
			if st.effect != nil {
				s.applyEffect(ctx, sess, st.effect)
			}
			for _, c := range st.created {
				s.recordOwner(ctx, user, c)
			}
		}
		if err == nil && parts.key != "" {
			// How long it took where it ran, for where it runs next time.
			s.dist.Observe(parts.key, parts.workers, time.Since(cmp.Or(parts.since, start)))
		}
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: tw.rows(), acc: st.access,
			policies: st.policies(), used: used, queued: queued, analyze: st.sourceText(), stmtType: st.stmtType, parts: parts})
		if err != nil {
			s.log.Debug("statement failed", "user", user.Name, "err", err)
		}
		return pgError(err)
	}, wire.WithColumns(tr.columns(st.columns)), wire.WithParameters(st.params))
	if tr != nil && !tr.Untranslated {
		st.names = make([]string, len(st.columns))
		for i, c := range tr.columns(st.columns) {
			st.names[i] = c.Name
		}
	}

	sess.open[ps] = st
	return wire.Prepared(ps), nil
}

// catalogMode is how a step holds the engine's catalog lock.
type catalogMode int

const (
	catalogFree catalogMode = iota
	catalogShared
	catalogExclusive
)

// catalogHold is one step's hold on the catalog lock (see holdCatalog).
type catalogHold struct{ mode catalogMode }

// holdCatalog takes the engine's catalog lock exclusively for one step of
// handling a statement that reaches the catalog's in-memory set of tables
// (see engine.Engine.CatalogLock and catalogRunLock). shareCatalog takes it
// shared, for DuckDB binding a statement (the bind hold openSession sets):
// binds are safe alongside each other, not alongside the rest. Holds are
// reentrant per session: the several steps that reach the set while one
// statement is handled (preparing it, binding it, rechecking it, running it,
// and the commands that resolve or list catalog objects) each take one
// without deadlocking, and the engine lock is held once, in the strongest
// mode the session's holds need. A step that needs it exclusively inside one
// that holds it shared has the session let go of it and take it exclusively,
// and back once that step is done; that is safe because the session's steps
// change holds only between DuckDB's calls, never during one, so nothing of
// the session's is reaching the set while it holds no lock. The returned
// func releases this step's hold. With no catalog attached there is no set
// to guard, so they do nothing.
func (s *Server) holdCatalog(sess *session) func() {
	return s.catalogHold(sess, catalogExclusive)
}

// shareCatalog takes the catalog lock shared for one step; see holdCatalog.
func (s *Server) shareCatalog(sess *session) func() {
	return s.catalogHold(sess, catalogShared)
}

func (s *Server) catalogHold(sess *session, mode catalogMode) func() {
	if s.catalogName() == "" {
		return func() {}
	}
	h := &catalogHold{mode: mode}
	sess.catHolds = append(sess.catHolds, h)
	s.relockCatalog(sess)
	return func() {
		if i := slices.Index(sess.catHolds, h); i >= 0 {
			sess.catHolds = slices.Delete(sess.catHolds, i, i+1)
			s.relockCatalog(sess)
		}
	}
}

// relockCatalog holds the engine's catalog lock as the session's holds need:
// exclusively if one of them is exclusive, shared if any is, else not.
func (s *Server) relockCatalog(sess *session) {
	want := catalogFree
	for _, h := range sess.catHolds {
		want = max(want, h.mode)
	}
	if want == sess.catHeld {
		return
	}
	s.unlockCatalog(sess)
	switch want {
	case catalogExclusive:
		sess.catRelease = s.engine.CatalogLock()
	case catalogShared:
		sess.catRelease = s.engine.CatalogShare()
	}
	sess.catHeld = want
}

// unlockCatalog lets go of the engine's catalog lock the session holds, if
// any, keeping its holds.
func (s *Server) unlockCatalog(sess *session) {
	if sess.catRelease != nil {
		sess.catRelease()
		sess.catRelease = nil
	}
	sess.catHeld = catalogFree
}

// withoutCatalog runs wait, which waits for another goroutine that takes the
// catalog lock itself, with the session's hold on the lock let go of, and
// takes it back as the session's holds need once wait returns. Like the
// changes of mode in relockCatalog, it must be called between DuckDB's
// calls, never during one; it does not change the session's holds.
func (s *Server) withoutCatalog(sess *session, wait func()) {
	if sess == nil || sess.catRelease == nil {
		wait()
		return
	}
	s.unlockCatalog(sess)
	defer s.relockCatalog(sess)
	wait()
}

// shareCatalogForLogin is shareCatalog for opening a session, which waits
// at most loginWait for the engine's lock and then fails, saying how long a
// listing or DDL has held it.
func (s *Server) shareCatalogForLogin(ctx context.Context, sess *session) (func(), error) {
	if s.catalogName() == "" || sess.catHeld != catalogFree {
		return s.shareCatalog(sess), nil
	}
	wctx, cancel := context.WithTimeout(ctx, s.loginWait)
	defer cancel()
	release, err := s.engine.CatalogShareContext(wctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		held := s.engine.CatalogLockedFor().Round(time.Second)
		s.log.Warn("login refused: the catalog lock is busy", "user", sess.user.Name, "waited", s.loginWait, "held_for", held)
		msg := fmt.Sprintf("cannot open a session: waited %s for the catalog lock", s.loginWait)
		if held > 0 {
			msg += fmt.Sprintf(", which a statement listing or changing the catalog has held for %s", held)
		}
		return nil, pgerror.WithCode(errors.New(msg+"; try again later"), codes.CannotConnectNow)
	}
	// The session holds the lock now: the hold below takes it no further.
	sess.catRelease, sess.catHeld = release, catalogShared
	return s.shareCatalog(sess), nil
}

// catalogRunLock holds the catalog lock exclusively for a run that changes
// the catalog or lists it, and not at all for any other run.
//
// DuckDB-Iceberg's one in-memory set of the catalog's tables is the shared
// state that is not safe under concurrency (see engine.Engine.CatalogLock).
// DDL that renames or drops a table removes entries from it without
// DuckDB-Iceberg's own lock, and a listing walks it while a bind replaces
// the entries of the tables it names, so the runs that do either take the
// lock exclusively and run on their own: DDL, by its statement type (create,
// drop, alter, attach, detach, whatever the database: the last statement of
// several an admin sends may be one, and the Iceberg DDL Igloo recognizes,
// changesIceberg, is always one), EXECUTE of a statement prepared in SQL,
// which may be any of these, and listings (information_schema, pg_catalog,
// duckdb_tables()/views()/columns(), SHOW, DESCRIBE, PRAGMA; see
// listsCatalog). Preparing and rechecking any statement take it exclusively
// too, since Igloo's checks list the catalog (see prepareChecked, recheck).
//
// The run of any other statement holds the lock only while DuckDB binds the
// statement again to run it, which it does each time for a statement that
// reads the catalog, replacing the entries of the tables it names (see the
// bind hold openSession sets). Those binds hold it shared: they take
// DuckDB-Iceberg's own lock, so they run alongside each other, just not
// alongside DDL, a listing or a prepare.
//
// After that bind the run holds nothing: a data read scans the files of the
// tables it bound, and a write (INSERT, UPDATE, DELETE, MERGE) commits a new
// snapshot of them — neither changes the set or walks it. Holding nothing
// lets reads and writes run concurrently and, as importantly, keeps a long
// run from blocking another session that needs to prepare, list or change
// the catalog, and a write from blocking a second writer whose commit it
// must see to retry on a conflict. Statements that do not reach the catalog
// (SET, transaction control) and sessions with no catalog attached hold
// nothing either.
//
// CREATE TABLE ... AS of an Iceberg table outside a transaction block is
// carried out in steps that take the lock themselves, exclusively only to
// change the catalog (see runStagedCreate).
func (s *Server) catalogRunLock(sess *session, st *statement) func() {
	if st.staged != nil && !sess.inTx {
		return func() {}
	}
	if st.changesIceberg || st.listsCatalog || changesCatalog(st.stmtType) || st.stmtType == duckdb.STATEMENT_TYPE_EXECUTE {
		return s.holdCatalog(sess)
	}
	return func() {}
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
// catalog and settings, the history views, the catalog's views) at where
// they are, computes aggregates over whole results without holding them (see
// rewriteWholeCounts), and counts distinct pairs a row each (see
// rewritePairCounts).
func (s *Server) rewrite(ctx context.Context, sess *session, user auth.User, text string) string {
	query, _ := s.rewriteCounting(ctx, sess, user, text, false)
	return query
}

// rewriteCounting is rewrite that, with ids set, also counts member
// columns through ID domains' ids where it may (see idcount.go), before
// the pair counts, and returns what it counts through them.
func (s *Server) rewriteCounting(ctx context.Context, sess *session, user auth.User, text string, ids bool) (string, *idCounts) {
	query := rewriteCurrentSetting(rewriteSystemSchemaRefs(text, s.catalogName()))
	query = rewriteHistoryRefs(rewriteSessionUser(rewriteCatalogQuery(query), user.Name), s.engine.LocalDatabase())
	if s.ddl != nil {
		query = s.withoutIcebergCascade(ctx, sess, s.ddl.RewriteViewRefs(query))
	}
	if readsMetadata.MatchString(query) {
		s.refreshMetadata(ctx, sess)
	}
	var counts *idCounts
	if ids {
		query, counts = s.rewriteIDCounts(ctx, sess, query)
	}
	query = s.rewritePairCounts(ctx, sess, s.rewriteWholeCounts(ctx, sess, query))
	if s.rewriteHook != nil {
		query = s.rewriteHook(query)
	}
	return query, counts
}

// prepareChecked is prepareStatement without recording failures, for the
// statements Igloo runs for a user to carry out one of its own commands
// (COPY INTO, materialized views): they are checked exactly like the
// user's own.
func (s *Server) prepareChecked(ctx context.Context, sess *session, user auth.User, text string, oids []uint32) (*statement, error) {
	// Preparing reads and fills in the catalog's in-memory set of tables:
	// DuckDB binds the statement (and suggests names for a missing table),
	// and Igloo resolves names, loads schemas and checks grants the same way,
	// filling in each table it names on first use. Hold the exclusive side so
	// no other statement reads the set while this changes it (see
	// holdCatalog).
	defer s.holdCatalog(sess)()
	if sess.memo == nil {
		sess.memo = &checkMemo{plans: map[string]any{}}
		defer func() { sess.memo = nil }()
	}
	if !sess.temps && mayCreateTemporary(text) {
		sess.temps = true
	}
	fail := func(err error) (*statement, error) { return nil, err }
	var viewGen uint64
	if s.ddl != nil {
		viewGen = s.ddl.Generation()
	}
	rewritten, counts := s.rewriteCounting(ctx, sess, user, text, true)
	query, err := s.resolveRefReads(ctx, sess, rewritten)
	if err != nil {
		return fail(err)
	}
	// Writes keep the columns that hold ID domains' ids right (see
	// idwrite.go).
	query, ids, err := s.memberWrites(ctx, sess, query)
	if err != nil {
		return fail(err)
	}

	// Preparing runs all but the last of several statements, so this
	// comes first.
	if err := forbidden(query); err != nil {
		return fail(err)
	}
	// The columns that hold ID domains' ids are Igloo's (see iddomains.go),
	// and so are bitmap indexes' tables (see bitmaprewrite.go).
	if err := hiddenReferences(text); err != nil {
		return fail(err)
	}
	if err := indexReferences(text); err != nil {
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
			if err := s.pivotReachesConnection(ctx, sess, query); err != nil {
				s.metrics.query(time.Now(), errDenied)
				return fail(err)
			}
			pivot = true
		}
	} else if strings.Contains(strings.TrimRight(strings.TrimSpace(query), "; \t\r\n"), ";") {
		// Preparing may run statements that change the catalog or the
		// session's settings; count it as doing both.
		s.catalogChanged()
		sess.settingGen++
		sess.ddlCount++
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
	// Likewise the table functions that read a connection's table by name.
	if err := s.guardTableArgs(ctx, sess, user, query); err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		s.metrics.query(time.Now(), err)
		return fail(pgError(err))
	}
	// What a CREATE makes, whose owner its user becomes, is worked out
	// before it is prepared, since afterwards that could make preparing
	// it again necessary (see reprepare).
	created := s.createdObjects(ctx, sess, query)

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
	// Columns Iceberg has no type for become strings (see icebergTypes).
	query = s.icebergTypes(ctx, sess, query)
	// A UNION ALL whose branches select from the same rows computes them
	// once (see unionfactor.go), and a subquery the statement repeats is
	// computed once (see shared.go). If DuckDB cannot prepare that, say
	// because the subquery refers to the query around it, the statement
	// runs as written.
	// Counts bitmap indexes answer read them (see bitmaprewrite.go).
	indexed := query
	query, indexPins, indexCandidate := s.rewriteIndexes(ctx, sess, user, query, counts)
	// A statement that counts ID domains' IDs in roaring bitmaps is
	// prepared with the rb extension's setting on (see bitmaps.go).
	bitmaps := s.bitmapsFor(ctx, sess, query)
	defer sess.setBitmaps(ctx, bitmaps)()
	written := query
	if factored := s.factorUnions(ctx, sess, query); factored != "" {
		query = factored
	}
	var st *statement
	if shared := s.shareSubqueries(ctx, sess, query); shared != "" {
		if st, err = prepare(ctx, sess.conn, shared, oids); err == nil {
			query = shared
		} else {
			s.log.Debug("statement runs with its repeated subqueries as written", "err", err)
			st = nil
		}
	}
	if st == nil {
		st, err = prepare(ctx, sess.conn, query, oids)
		if err != nil && query != written {
			s.log.Debug("statement runs with its unions as written", "err", err)
			query = written
			st, err = prepare(ctx, sess.conn, query, oids)
		}
	}
	if err != nil && s.ddl != nil && missingRelation.MatchString(err.Error()) {
		// Perhaps a view another replica just created: sync and try again,
		// also when this sync changed nothing because another one, of the
		// background or another session, brought the view meanwhile.
		if changed, _ := s.ddl.SyncViews(ctx); changed || s.ddl.Generation() != viewGen {
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
	if err != nil && s.catalogName() != "" {
		// Or a schema DuckDB took for missing before this replica or
		// another created it, which it goes on taking for missing (see
		// Engine.ForgetMissingSchema).
		if schema := missingSchemaOf(err.Error()); schema != "" {
			forgot := false
			s.withoutCatalog(sess, func() { forgot = s.engine.ForgetMissingSchema(ctx, schema) })
			if forgot {
				st, err = prepare(ctx, sess.conn, query, oids)
			}
		}
	}
	if err != nil && st == nil {
		// Or an INSERT whose rows come from the * of a member table, which
		// DuckDB expands with the columns of ids: prepared as policies
		// rewrite it, without them (see idwrite.go). Policies and checks
		// apply below as to any statement.
		if early := s.prepareWithoutIDs(ctx, sess, user, query, oids); early != nil {
			st, err = early, nil
		}
	}
	if err != nil {
		sess.observe(nil, err)
		err = shownParameterError(query, err, func(name string) bool { return s.isDuckDBSetting(ctx, sess, name) })
		return fail(pgError(s.connectionHint(ctx, err)))
	}
	st.bitmaps = bitmaps
	if indexCandidate && st.stmtType == duckdb.STATEMENT_TYPE_SELECT {
		st.indexPins, st.indexed, st.text = indexPins, indexed, text
	}
	if pivot && st.stmtType != duckdb.STATEMENT_TYPE_SELECT {
		// multipleStatements let through what it takes for a query.
		s.log.Error("a PIVOT statement that is not a query was prepared", "user", user.Name, "type", st.stmtType)
		_ = st.close()
		return fail(pgerror.WithCode(errors.New("permission denied: only admins may run this PIVOT"), codes.InsufficientPrivilege))
	}
	// Apply policies: what runs is the statement rewritten to read
	// protected tables through them.
	protected, g, ran, err := s.policyRewrite(ctx, sess, user, st.stmtType, query, oids)
	if err == nil && protected != query {
		st.original = query
		err = st.adopt(ctx, sess.conn, protected)
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
	if user.Role != auth.RoleAdmin && st.stmtType == duckdb.STATEMENT_TYPE_SET {
		// A parameter no one has is not one only admins may set.
		if err := s.unknownParameter(ctx, sess, query); err != nil {
			_ = st.close()
			return fail(err)
		}
	}
	err = authorize(user.Role, st.stmtType, query)
	if err != nil {
		// The built-in role does not allow it; grants may.
		st.access, err = s.authorizeByGrants(ctx, sess, user, st, query, err)
	}
	if err == nil {
		err = s.authorizeLocal(ctx, sess, user.Role, st.stmtType, text, protected)
	}
	if err == nil {
		// What it reads from connections, whatever the role, with a
		// record that has it checked again when they change.
		st.access, err = s.authorizeConnectionReads(ctx, sess, user, st, st.access)
	}
	ranWrites := false
	if err == nil {
		// What it changes of the tables clean rooms keep, whatever the role.
		ranWrites, err = s.keptWrites(ctx, sess, user, st.stmtType, query)
	}
	if err == nil {
		// Nothing changes ID domains' dictionaries or bitmap indexes but
		// Igloo.
		err = iglooSchemaWrites(st.stmtType, query)
	}
	if err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		_ = st.close()
		return fail(err)
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_DROP || st.stmtType == duckdb.STATEMENT_TYPE_ALTER {
		st.effect = s.ddlEffect(ctx, sess, query)
	}
	// Preparing several statements ran all but the last.
	if st.stmtType == duckdb.STATEMENT_TYPE_CREATE || len(sqllex.Split(query)) > 1 {
		st.created = created
	}
	if changesCatalog(st.stmtType) {
		st.changesIceberg = s.changesIceberg(ctx, sess, st.stmtType, query)
	}
	if st.changesIceberg && st.stmtType == duckdb.STATEMENT_TYPE_CREATE && len(st.params) == 0 {
		st.staged = s.stagedCreateOf(ctx, sess, st.query)
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_INSERT {
		st.ids = ids
	}
	if counts != nil && st.stmtType == duckdb.STATEMENT_TYPE_SELECT {
		st.idCounts, st.text = counts, text
	}
	// A statement that lists the catalog's tables or views walks the set and
	// holds the catalog lock exclusively when it runs (see catalogRunLock);
	// checked against the statement as written and as rewritten to the
	// relations Igloo answers it with.
	st.listsCatalog = listsCatalog.MatchString(text) || listsCatalog.MatchString(query)
	if ranWrites || st.access != nil && st.access.ranStatements {
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

// recheckGrants decides again, before it runs, a statement of a user who is
// not an admin, or of an admin that reads connections, if the decision may
// be stale: the session's grants were reloaded since, the catalog, the
// connections or the session's settings changed, or the decision is as old
// as the role refresh interval. A prepared statement then outlives a REVOKE
// no longer than a session's cached grants do, and cannot be made to read a
// table nobody checked by replacing a view, attaching a connection or
// changing the search path before it runs. force decides again regardless,
// as after policies rewrote the statement anew.
func (s *Server) recheckGrants(ctx context.Context, sess *session, st *statement, force bool) error {
	if st.access == nil {
		// An admin's statement that reads no connection.
		return nil
	}
	user, err := s.refreshUser(ctx, sess)
	if err != nil {
		return err
	}
	privs, err := s.privileges(ctx, sess, user)
	if !force && err == nil && (privs == st.access.privs || st.access.privs == nil && user.Role == auth.RoleAdmin) &&
		time.Since(st.access.checked) < s.roleRefresh &&
		st.access.catalogGen == s.catalogGeneration() && st.access.settingGen == sess.settingGen {
		return nil
	}
	// A refusal leaves st.access as it was, stale, so the statement is
	// decided again if it is run again.
	var acc *access
	if roleErr := authorize(user.Role, st.stmtType, st.sourceText()); roleErr != nil {
		if acc, err = s.authorizeByGrants(ctx, sess, user, st, st.sourceText(), roleErr); err != nil {
			s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
			return err
		}
	}
	// What it reads from connections, whatever the role.
	if acc, err = s.authorizeConnectionReads(ctx, sess, user, st, acc); err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return err
	}
	if acc == nil {
		// An admin's statement that reads no connection.
		st.access = nil
		return nil
	}
	if acc.ranStatements {
		if err := st.reprepare(ctx, sess.conn); err != nil {
			return pgError(err)
		}
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

// showCommand answers SHOW of the PostgreSQL settings in pgSettings.
func (s *Server) showCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	m := showSetting.FindStringSubmatch(query)
	if m == nil {
		return nil, false
	}
	setting, ok := pgSettings[strings.ToLower(strings.Join(strings.Fields(strings.Trim(m[1], `"`)), " "))]
	if !ok {
		return nil, false
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		value := setting.value
		switch setting.name {
		case "TimeZone", "search_path":
			v, err := settingValue(ctx, sess.conn, setting.name)
			if err != nil {
				return pgError(err)
			}
			value = v
		case "application_name":
			value = sess.applicationName()
		case "is_superuser":
			value = "off"
			if sess.currentUser().Role == auth.RoleAdmin {
				value = "on"
			}
		}
		if err := w.Row([]any{value}); err != nil {
			return err
		}
		return w.Complete("SHOW")
	}), wire.WithColumns(wire.Columns{{Name: setting.name, Oid: pgtype.TextOID, Width: -1}}))), true
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

// catalogChanged notes a change to the catalog's objects made here: for the
// rechecks of prepared statements (catalogGeneration), and for DuckDB's
// listings of the catalog, which the engine's catalog proxy keeps for a few
// seconds and forgets on the changes DuckDB makes itself, but not on those
// Igloo makes another way.
func (s *Server) catalogChanged() {
	s.ddlGen.Add(1)
	s.engine.CatalogChanged()
}
