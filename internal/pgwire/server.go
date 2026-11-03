// Package pgwire serves Igloo over the PostgreSQL wire protocol, so psql, BI
// tools and ordinary Postgres drivers can connect without an Igloo client.
package pgwire

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/types"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/deletefiles"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/engine/terminate"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/lockorder"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/prefetch"
	"github.com/akkio-inc/igloo/internal/sharing"
	"github.com/akkio-inc/igloo/internal/snowflake"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/tasks"
	"github.com/akkio-inc/igloo/internal/tsforecast"
	"github.com/akkio-inc/igloo/internal/uploads"
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
	// UseDefaultSchema puts a connection back on the schema and search
	// path a new one starts with.
	UseDefaultSchema(ctx context.Context, conn *duckdb.Conn) error
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
	// CatalogTryLock takes the lock exclusively if it can within wait,
	// without queuing for it (see HoldTable).
	CatalogTryLock(ctx context.Context, wait time.Duration) (func(), bool)
	// CatalogChanged says the catalog changed other than through DuckDB's
	// own requests, so that DuckDB's next listings of it ask the catalog
	// again (see catalogproxy's listings); ListingsGeneration changes
	// whenever what those listings list may have.
	CatalogChanged()
	ListingsGeneration() uint64
	// DataVersion goes up whenever a table of the catalog may have
	// changed, through this replica or otherwise, and TablesVersion
	// changes whenever one of the tables loads loaded may have (see
	// resultcache.go).
	DataVersion() uint64
	TablesVersion(loads []catalogproxy.Load) uint64
	// OnEmptyCache has the engine empty what purge empties whenever it
	// empties its read cache for erasure requests.
	OnEmptyCache(purge func())
	// RecordLoads records the catalog's tables DuckDB loads until the func
	// it returns is called, and ReloadTables loads such tables again, so
	// that DataVersion goes up if any changed meanwhile.
	RecordLoads() func() []catalogproxy.Load
	// SharedRefusals counts DuckDB's catalog requests refused while the
	// catalog lock was held shared (see sharedprepare.go).
	SharedRefusals() uint64
	ReloadTables(ctx context.Context, loads []catalogproxy.Load) error
	// DistinctCounts returns the distinct counts of the catalog's tables'
	// columns, by catalog.schema.table as DuckDB names it, lower-cased, and
	// a generation that changes whenever they do (see analyze.go).
	DistinctCounts() (uint64, map[string]map[string]int64)
	// ForgetMissingSchema makes DuckDB forget a schema of the catalog it
	// took for missing, if the catalog has it, and reports whether it did.
	ForgetMissingSchema(ctx context.Context, schema string) bool
	// ForgetTable has DuckDB's next DROP TABLE of the catalog's table
	// name of schema, which the catalog renamed to as behind DuckDB's
	// back, erase DuckDB's entry for it and nothing in the catalog, until
	// stop is called.
	ForgetTable(schema, name, as string) (stop func())
	// BitmapsLoaded reports whether DuckDB has Igloo's rb extension.
	BitmapsLoaded() bool
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
	// AccessMaxStaleness is how long statements may still be checked
	// against the user, grants and policies last read when the metadata
	// database cannot be read; past it they are refused. Zero means four
	// times RoleRefresh.
	AccessMaxStaleness time.Duration
	// StatementTimeout is the default limit on how long a statement may
	// run; sessions can change it with SET statement_timeout. Zero means none.
	StatementTimeout time.Duration
	// MaxConnections caps concurrent sessions, and connections signing in
	// (see startup.go). Zero means no cap.
	MaxConnections int
	// LoginTimeout is how long a client may take to sign in, from
	// connecting. Zero means 30 seconds.
	LoginTimeout time.Duration
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
	// are about to read (see prefetch.go); either nil turns that off. Without
	// a Scanner, nothing estimates what a statement holds before its first run
	// (see Server.planned).
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
	// QueryMemory is DuckDB's memory limit, which CREATE TABLE ... AS
	// plans aggregates in passes against (see aggpasses.go); 0 runs none
	// in passes.
	QueryMemory int64
	// HeavyInBackground runs heavy reads in a second DuckDB of this process
	// whose threads give way to the first's (see background.go).
	HeavyInBackground bool
	// ResultCacheSize is IGLOO_RESULT_CACHE_SIZE: the memory the results
	// the server keeps of queries may take (see resultcache.go); 0 keeps
	// none.
	ResultCacheSize int64
	// SortSpillBudget is IGLOO_SORT_SPILL_BUDGET: how many bytes a write
	// of rows in order may spill to sort them at once; a larger one is
	// sorted a range at a time (see sortedwrite.go). 0 means half the
	// spill directory's free space; negative, every write sorts at once.
	SortSpillBudget int64
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
	// RewriteMemory is what writing a table's files again may hold at once
	// (IGLOO_MAINTENANCE_MEMORY), as when ids are filled in (see idfill.go);
	// 0 is filerewrite's default.
	RewriteMemory int64
	// idFillRewriteRows, when set, replaces idFillRewriteRows (for tests).
	idFillRewriteRows int64
	// MemoryTotal is the memory the process may use (memlimit.Plan.Total),
	// below a sixty-fourth of which ShedMemory gives up on a statement it
	// canceled that does not stop, and exits (see exitForMemory); 0 never.
	MemoryTotal int64
	// FreeMemory, when set, makes DuckDB give back the memory it can,
	// dropping its caches, before ShedMemory gives up on work no statement
	// owns any more (see orphanedWork).
	FreeMemory func(context.Context)
	// shedStuckAfter and exit, when set, replace shedStuckAfter and
	// os.Exit (for tests); stuckGrace replaces stuckGrace.
	shedStuckAfter time.Duration
	exit           func(code int)
	stuckGrace     time.Duration
	// ReportRewrite has the statement layer recognize the counting CTEs
	// of audience reports and answer them through the report API
	// (IGLOO_REPORT_REWRITE; see reportrecognize.go).
	ReportRewrite bool
	// ExclusivePrepares has every statement prepared under the exclusive
	// catalog lock, none alongside others (IGLOO_SHARED_PREPARES=false;
	// see sharedprepare.go).
	ExclusivePrepares bool
	// reportRewriteAnyPath, when set, has recognition replace CTEs whose
	// calls would count the tables' values too (for tests of its parity).
	reportRewriteAnyPath bool
	// reportOutcomes, when set, hears what recognition did with each
	// statement's CTEs (for tests).
	reportOutcomes func([]reportOutcome)
	// reportFallback, when set, hears of each recognized statement that
	// failed, which then ran as written (for tests).
	reportFallback func(recognized string, err error)
	// UploadKeys are what it takes to see the store's unfinished uploads, which
	// a statement's end aborts under its directories.
	UploadKeys uploads.Keys
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
	// AI runs the AI functions and keeps AI integrations (see ai.go). Nil
	// means a runtime in memory that reaches only public addresses.
	AI *ai.Runtime

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

	// Models keeps the rows of models (Snowflake's ML classes; see
	// models.go). Nil means a store in memory. ModelRuntime holds their
	// blobs and scores them; nil means the engine's (engine.Models), or
	// one in memory.
	Models       models.Store
	ModelRuntime *models.Runtime
	// trainRows, in tests, bounds the rows Igloo's classes train on, in
	// place of models.TrainRowLimit.
	trainRows int

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
	// ai runs the AI functions; aiMaxCalls and aiMaxCost are a statement's
	// default caps on them (IGLOO_AI_MAX_CALLS, IGLOO_AI_MAX_COST_USD).
	ai         *ai.Runtime
	aiMaxCalls int64
	aiMaxCost  float64
	// models keeps models' rows, and modelRuntime their blobs; trainRows
	// is Options.trainRows.
	models       models.Store
	modelRuntime *models.Runtime
	trainRows    int
	// forecasting forecasts with the model pack's models (see
	// tsforecast.go).
	forecasting *tsforecast.Service
	// clonePurge is Options.ClonePurge (see catalogguard.go).
	clonePurge func(ctx context.Context, what string, name []string, location string, inCatalog bool) clone.PurgeVerdict
	// gov holds the policies (see protect.go).
	gov *governance
	// maxStale is how long statements may be checked against the users,
	// grants and policies last read (see refreshAccess).
	maxStale time.Duration
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
	// sortBudget is Options.SortSpillBudget.
	sortBudget int64
	// results keeps queries' results (see resultcache.go); nil when it
	// keeps none.
	results *resultCache
	// refreshFraction is the share of a full refresh's reads past which
	// reading a change costs too much (defaultFullFraction when zero), and
	// deleteTargets what refreshes know of which data files position
	// delete files delete rows of (see refresh.go).
	refreshFraction float64
	deleteTargets   *deletefiles.Targets
	// queryMemory is Options.QueryMemory.
	queryMemory int64
	// functions is what statements' rewrites and the result cache know of
	// DuckDB's functions (see functionlist.go).
	functions functionList
	// sharedSens is the sensitivity of sessions with no temporary views or
	// macros of their own (see sensitivity).
	sharedSens struct {
		mu   lockorder.Mutex
		sens *sensitivity
	}
	// afterNewValues, if set, runs when an assignment has found new values
	// and is about to take the lease, for tests.
	afterNewValues func()
	// idComplete caches which member tables' snapshots have every row's
	// ids (see idcount.go).
	idComplete idComplete
	// scanner and prefetcher read ahead for statements, or are nil; scanner
	// also lists what statements read for Server.planned.
	scanner    *distributed.Scanner
	scanNames  scanNames
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
	// memoryCritical, shedStuckAfter and exitProcess decide when and how
	// ShedMemory gives up on a statement that does not stop.
	memoryCritical int64
	shedStuckAfter time.Duration
	exitProcess    func(code int)
	freeMemory     func(context.Context)
	// stuck lets the client of a statement that does not stop once
	// canceled go (see watchStuck).
	stuck *stuckWatch
	// orphans is what ShedMemory knows of the work of statements it
	// canceled once nothing runs (see orphanedWork).
	orphans orphans
	// reportRewrite is Options.ReportRewrite; reportRecognized counts the
	// CTEs it answered through the report API, and reportUnrecognized the
	// statements prepared again as written after a recognized form failed.
	reportRewrite        atomic.Bool
	exclusivePrepares    bool
	rewriteMemory        int64
	idFillRewriteRows    int64
	reportRecognized     atomic.Int64
	reportUnrecognized   atomic.Int64
	reportOutcomes       func([]reportOutcome)
	reportFallback       func(recognized string, err error)
	reportRewriteAnyPath bool
	// reportThroughIDs counts the report API's calls that read their tables
	// through ids (see reportids.go).
	reportThroughIDs atomic.Int64
	// reportFromIndexes counts those answered from bitmap indexes (see
	// reportindex.go).
	reportFromIndexes atomic.Int64
	// reportPlanned counts how many times a call's reading of bitmap
	// indexes, or of its tables through ids, was planned (see reportCache).
	reportPlanned atomic.Int64
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
	// streamMerged says statements read merges of sorted rows as they are
	// made, through distributed.MergedFunction, which New registered on the
	// engine's database.
	streamMerged bool
	// beforeRefresh, if set, runs before a statement is checked and
	// prepared again for its run, with the context that ends them (for
	// tests).
	beforeRefresh func(context.Context)
	// countsGen is the generation of the distinct counts last handed to
	// DuckDB, and countsMu serializes handing them (see
	// registerDistinctCounts).
	countsMu  lockorder.Mutex
	countsGen atomic.Uint64
	// beforePrepare, if set, runs as a statement a client sent (text)
	// starts to be prepared, with the context that ends its preparing (for
	// tests).
	beforePrepare func(ctx context.Context, text string)
	// heavy says which statements run in the second DuckDB (see
	// background.go), and lane is that DuckDB; heavy is nil to run all in
	// the first.
	heavy *heavyHistory
	lane  backgroundLane
	// beforeFill, if set, runs as a staged CREATE TABLE ... AS starts to
	// fill its table, with the context that ends the fill (for tests).
	beforeFill func(context.Context)
	// afterOverwrite, if set, runs once a refresh's overwrite is committed,
	// before it writes the view's state; an error fails the refresh (for
	// tests).
	afterOverwrite func(ctx context.Context) error
	// beforeOverwrite, if set, runs once a full refresh wrote its files
	// into dir, before it commits them, with the refresh's context; an
	// error fails the refresh (for tests).
	beforeOverwrite func(ctx context.Context, dir string) error
	// splitPairs writes report calls' pair counts as workers split them
	// (see scanCounts) whether or not they fit workers (pairsFitWorkers),
	// and unsplitPairs never; preferHybrid answers them as hybrids rather
	// than through ids where both may (see reportStatement); for tests.
	splitPairs, unsplitPairs, preferHybrid bool
	// failLocal, if set, runs before a statement runs on this replica, and
	// fails it with its error; tests use it to make a statement run out of
	// memory here.
	failLocal func(ctx context.Context, st *statement) error
	// failFill, if set, runs before a fill's query runs on this replica
	// (see fillOn), and fails it with its error.
	failFill func(ctx context.Context, query string) error
	// writesRunning holds the notes' keys of this replica's statements whose
	// workers write a table's files (see tableWrite).
	writesRunning sync.Map
	// lateWrites counts the statements that will look again for what their
	// tasks still wrote, until lateWritesStop closes (see tableWrite.end);
	// lateWritesWait, if set, is what they wait for instead of a while.
	lateWrites     sync.WaitGroup
	lateWritesStop chan struct{}
	lateWritesWait func()
	stopLateWrites sync.Once
	// clientWatchStop ends the watch for clients that went away (see
	// clientgone.go), which watchingClients starts once.
	clientWatchStop chan struct{}
	watchingClients sync.Once
	stopClientWatch sync.Once
	// uploadKeys is Options.UploadKeys.
	uploadKeys uploads.Keys
	// uploadStoreFor, in tests, is the store of unfinished writes of a directory.
	uploadStoreFor func(dir string) (uploads.Store, error)
	// replicaWrites counts the INSERTs running on this replica by the names of their
	// tables, under replicaWritesMu (see replicaWrite).
	replicaWritesMu lockorder.Mutex
	replicaWrites   map[string]int
	// beforeWriteCommit, if set, runs before files workers wrote under dir
	// are committed, and fails the commit with its error (see tableWrite).
	beforeWriteCommit func(ctx context.Context, dir string) error
	// rewriteHook, if set, is the last of Igloo's rewrites of a statement
	// before it is checked and prepared (see rewrite); tests use it to
	// write what only Igloo may, such as the columns of ID domains' ids.
	rewriteHook func(string) string
	// rooms carries out clean room statements; partners are Delta
	// Sharing's recipients and providers, which partners are.
	rooms    *cleanroom.Service
	partners partnerDirectory
	// ddlGen counts statements run here that may change the catalog; see
	// catalogGeneration. catalogChanged adds to it. defsGen counts those
	// of them that may change what a statement's text means other than
	// through the tables it loads (see resultGeneration).
	ddlGen, defsGen atomic.Uint64
	// tables and views are the indexes of tables and views the checks
	// resolve names with (see indexedTables).
	tables, views tableIndex
	// columns is what DuckDB bound of tables' columns, for planning
	// statements for workers (see boundColumns).
	columns columnCache
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
	// startup holds connections to the limits of signing in.
	startup *startupLimits

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
	sketchesMu    lockorder.Mutex
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
		ai:          opts.AI,
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
		startup:             newStartupLimits(opts.LoginTimeout, opts.MaxConnections, log),

		meta:    opts.Metadata,
		owners:  opts.Owners,
		lineage: opts.Lineage,
	}
	if s.meta == nil && opts.DDL != nil {
		s.meta = opts.DDL
	}
	s.streamMerged = s.registerMerged()
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
	if s.ai == nil {
		guard := notify.NewGuard(nil, nil)
		s.ai = ai.NewRuntime(ai.Options{Dial: guard.DialContext, CheckURL: func(u string) error { _, err := guard.CheckURL(u); return err },
			Cache: true, MaxCalls: 1000, MaxCostUSD: 1, Log: log})
	}
	s.aiMaxCalls, s.aiMaxCost = s.ai.Caps()
	s.models, s.modelRuntime, s.trainRows = opts.Models, opts.ModelRuntime, opts.trainRows
	if s.models == nil {
		s.models = models.NewMemoryStore()
	}
	if s.modelRuntime == nil {
		if e, ok := engine.(interface{ Models() *models.Runtime }); ok {
			s.modelRuntime = e.Models()
		}
	}
	if s.modelRuntime == nil {
		s.modelRuntime = models.NewRuntime()
	}
	if s.modelRuntime.Blobs() == nil {
		s.modelRuntime.SetBlobs(models.NewMemory())
	}
	if e, ok := engine.(interface{ Forecasting() *tsforecast.Service }); ok {
		s.forecasting = e.Forecasting()
	}
	if s.forecasting == nil {
		s.forecasting = tsforecast.New(modelpack.Open(""))
	}
	if s.roleRefresh == 0 {
		s.roleRefresh = 30 * time.Second
	}
	s.maxStale = cmp.Or(opts.AccessMaxStaleness, 4*s.roleRefresh)
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
	s.idSyncMinRows, s.sortBudget = opts.IDSyncMinRows, opts.SortSpillBudget
	s.results = newResultCache(opts.ResultCacheSize, opts.Metrics)
	if s.results != nil {
		// What erasure requests delete goes from kept results too.
		engine.OnEmptyCache(s.results.purge)
	}
	s.deleteTargets = deletefiles.NewTargets()
	s.queryMemory = opts.QueryMemory
	if opts.HeavyInBackground {
		s.heavy = newHeavyHistory()
		if e, ok := s.engine.(interface{ WatchForeground(func() bool) }); ok {
			e.WatchForeground(s.workload.HeldBehindSecond)
		}
	}
	s.scanner, s.prefetcher = opts.Scanner, opts.Prefetcher
	s.bitmaps, s.bitmapMinRows, s.bitmapIndexesOn = opts.Bitmaps, opts.BitmapMinRows, opts.BitmapIndexes
	s.reportRewrite.Store(opts.ReportRewrite)
	s.exclusivePrepares = opts.ExclusivePrepares
	if opts.MemoryTotal > 0 {
		s.memoryCritical = memlimit.Critical(opts.MemoryTotal)
	}
	s.shedStuckAfter = cmp.Or(opts.shedStuckAfter, shedStuckAfter)
	s.stuck = &stuckWatch{grace: cmp.Or(opts.stuckGrace, stuckGrace), log: s.log}
	s.exitProcess = opts.exit
	s.freeMemory = opts.FreeMemory
	if s.exitProcess == nil {
		s.exitProcess = os.Exit
	}
	s.rewriteMemory = opts.RewriteMemory
	s.idFillRewriteRows = cmp.Or(opts.idFillRewriteRows, idFillRewriteRows)
	s.reportOutcomes, s.reportFallback = opts.reportOutcomes, opts.reportFallback
	s.reportRewriteAnyPath = opts.reportRewriteAnyPath
	s.uploadKeys = opts.UploadKeys
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
	s.gov.maxStale = s.maxStale
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
	s.lateWritesStop = make(chan struct{})
	s.clientWatchStop = make(chan struct{})
	return s, nil
}

// Serve accepts connections on l until Shutdown is called.
func (s *Server) Serve(l net.Listener) error {
	if s.requireTLS {
		l = requireTLS(l)
	}
	s.watchingClients.Do(func() { go s.watchClients() })
	return s.wire.Serve(s.startup.listener(l))
}

// Shutdown stops accepting connections and waits for open ones to finish
// until ctx is done.
func (s *Server) Shutdown(ctx context.Context) error {
	s.indexBuilds.stopped.Store(true)
	err := s.wire.Shutdown(ctx)
	s.stopClientWatch.Do(func() { close(s.clientWatchStop) })
	s.stopLateWrites.Do(func() { close(s.lateWritesStop) })
	s.lateWrites.Wait()
	err = errors.Join(err, s.closeBackground())
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
	// left is set once the session has left the server (see leave).
	left atomic.Bool
	// memo keeps what the checks of the statement being prepared would
	// otherwise ask DuckDB more than once; nil outside prepareChecked.
	memo *checkMemo
	// noIndexes keeps the statement being prepared from reading bitmap
	// indexes (see rerunWithoutIndexes), and noBitmaps from counting in
	// roaring bitmaps at all (see rerunWithoutBitmaps).
	noIndexes bool
	noBitmaps bool
	// reports keeps, while a statement is prepared, what planning its
	// report calls found (see reportCache).
	reports *reportCache
	// phases times the steps of preparing a statement, or readying one to
	// run, for the debug log (see timePhases).
	phases *prepPhases
	// bitmapVars are the variables that hold the bitmaps of the statements
	// Igloo wrote (see hybridBitmaps), by name.
	bitmapVars map[string]*bitmapVar
	// bitmapsRestore sets rb_count_distinct back after the statement
	// running with it on (see setBitmaps); nil otherwise.
	bitmapsRestore func()
	// temps says the session may have temporary tables or views, which
	// the server's index of tables leaves out (see indexedTables), and
	// tempMacros that it may have temporary macros of its own, which the
	// server's list of functions leaves out (see functionSets).
	temps, tempMacros bool
	// open holds every DuckDB statement prepared for this client, keyed by
	// the wire statement that owns it, so none outlive the connection.
	open     map[*wire.PreparedStatement]*statement
	inTx     bool
	txFailed bool
	// txTables are the tables the open transaction block read, which
	// maintenance leaves be until it ends.
	txTables txTables
	// sqlPrepared says, of each statement prepared in SQL by lower-case
	// name, whether EXECUTE of it holds the catalog lock (see
	// executeHolds).
	sqlPrepared map[string]bool
	// bgConn is its connection to the second DuckDB, once a heavy statement
	// has run there (see background.go).
	bgConn atomic.Pointer[backgroundConn]
	// outer is the context runCancelable runs the statement's steps under,
	// which a shed by the memory watch does not end, while it runs.
	outer context.Context
	// bindAlone has DuckDB's binds on the session's connection hold the
	// catalog lock exclusively rather than shared, while a run whose bind
	// the catalog proxy refused binds again (see runBound).
	bindAlone bool
	// held, while set, keeps queries recorded for the history (see
	// holdRecords).
	held *[]queryRecord
	// sfTemps are the temporary tables and views that statements in
	// Snowflake's dialect created in a schema's name, which DuckDB keeps in
	// temp.main (see noteTemporary).
	sfTemps map[snowflake.TempName]bool
	// flattenLists says the statement being parsed unnests FLATTENs' lists
	// that their FROM items compute (see flattenForms).
	flattenLists bool
	// modelResults are the tokens of models' methods' results the
	// session's statements read, and modelMemo the token of each method
	// call the statement being parsed has computed (see mlrewrite.go).
	modelResults []string
	modelMemo    map[string]string
	// modelPins are the models the session's ai_predict calls trained,
	// which the runtime keeps until the session ends.
	modelPins []string
	// modelAllowed are the model blobs and results the session's
	// statements resolved, which alone its statements may name (see
	// checkModelCalls), with the order they came in.
	modelAllowed map[string]modelAllowance
	modelSeq     int
	// training collects, while CREATE trains a model, the tables its
	// statements read, and those the models they used had read (see
	// noteTraining).
	training *[]string
	// changes counts the statements in the transaction block that may have
	// changed something, and savepoints are its savepoints (see
	// savepointCommand).
	changes    int
	savepoints []savepoint
	// timeout limits each statement's run, and queueTimeout its wait to
	// start (SET statement_queued_timeout); zero means none.
	timeout, queueTimeout time.Duration
	// queued says the statement running waits to start (see
	// runCancelable).
	queued bool
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
	// settingGen counts the session's SET and PRAGMA statements, which may
	// change how its statements resolve names and what they return.
	settingGen uint64
	// shapePath is where the session's names resolve, as of settingGen
	// shapePathGen, for its statements' shapes (see workloadShape).
	shapePath      string
	shapePathGen   uint64
	shapePathKnown bool
	// prefetchOff turns reading ahead off for the session's statements
	// (SET igloo_prefetch = off), reportRewriteOff recognizing their reports
	// (SET igloo_report_rewrite = off), indexesOff their reads of bitmap
	// indexes (SET igloo_bitmap_indexes = off) and idCountsOff their counts
	// through ID domains' ids (SET igloo_id_counts = off; see switches.go).
	prefetchOff, reportRewriteOff, indexesOff, idCountsOff bool
	// resultCacheOff keeps the session's statements from using kept
	// results (SET igloo_result_cache = off, or Snowflake's
	// USE_CACHED_RESULT = FALSE), and resultSettings is its settings'
	// digest for the result cache (see resultcache.go).
	resultCacheOff bool
	resultSettings *settingsDigest
	// resultSalt keeps the results of a session that may have temporary
	// objects its own (see resultKey).
	resultSalt uint64
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
	// catWait is how long the session waited for the engine lock since its
	// last statement was recorded (see recordQuery), and catSince when it
	// last took it exclusively, for logging long holds (see unlockCatalog).
	catWait  time.Duration
	catSince time.Time
	// sharedPrepare is set while prepareChecked prepares a statement holding
	// the catalog lock shared, which stops at DuckDB's first failure to
	// prepare it (see sharedprepare.go).
	sharedPrepare bool

	// mu guards the fields below, which cancel requests and user changes
	// reach from other connections.
	mu      lockorder.Mutex
	user    auth.User
	checked time.Time
	running context.CancelFunc
	// statement and started describe the running statement; shed says
	// ShedMemory canceled it, at shedAt, and exiting that it was canceled
	// because Igloo exits (see exitForMemory).
	statement string
	started   time.Time
	// runShape is the running statement's shape, inBackground says it runs in the
	// second DuckDB, and elsewhere on workers, for the statements beside it (see
	// besideIsHeavy and othersRunning).
	runShape     string
	inBackground bool
	elsewhere    bool
	shed         bool
	shedAt       time.Time
	exiting      bool
	// stuck, set for a client's session, lets the client go when its
	// statement does not stop once canceled; watched is the context of the
	// statement's run it watches (see watchStuck).
	stuck   *stuckWatch
	watched context.Context
	// socket is the client's connection, for a client's session, which is
	// looked at while a statement runs; clientGone, under mu, says it closed,
	// once logged (see clientgone.go).
	socket     syscall.RawConn
	clientGone bool
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

	// aiBound says the statement being prepared calls an AI function, and
	// aiRun is the running statement's AI functions' Statement (see
	// ai.go). Both are reached from DuckDB's threads.
	aiBound atomic.Bool
	aiRun   atomic.Pointer[ai.Statement]
	// aiIntegration, aiMaxCalls, aiMaxCost and aiThreshold are the
	// session's AI settings (see aiSettingCommand); zero means unset.
	aiIntegration string
	aiMaxCalls    int64
	aiMaxCost     float64
	aiThreshold   float64
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
	s.sessions.connected(sess, conn)
	sess.wl.meter, sess.wl.controller = meter, s.workload
	// Binding a statement looks its tables up in the catalog's in-memory set
	// and replaces their entries there, and DuckDB binds a statement that
	// reads the catalog again each time it runs, not only when it is
	// prepared. Hold the catalog lock shared for every bind on this
	// connection, so none runs while DDL renames or drops a table or a
	// listing walks the set (see shareCatalog); running the statement after
	// that holds nothing more.
	conn.SetBindHold(func() func() {
		if sess.bindAlone {
			return s.holdCatalog(sess)
		}
		return s.shareCatalog(sess)
	})
	// A C++ exception that escapes DuckDB in one of this connection's calls
	// that bind, run or drop a plan, which nothing can catch, ends this
	// session rather than the process (see parked): the iceberg extension's
	// scan state threw Out of Memory out of its destructor in the optimizer of
	// a statement bound again at the replica's memory limit, and the process
	// ended with every session's statement.
	ended := func(what string) { s.parked(ctx, sess, what) }
	conn.SetCallGuard(func(call func()) { terminate.Statement(ended, call) })
	for _, stmt := range sessionCatalogSetup(sess.user.Name, sess.user.Role == auth.RoleAdmin, s.version, s.engine.BitmapsLoaded()) {
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
	s.leave(sess)

	sess.closeCursors(ctx, true)
	sess.dropModelResults(s.modelRuntime)
	var errs []error
	for ps := range sess.open {
		errs = append(errs, sess.release(ps))
	}
	errs = append(errs, sess.closeBackgroundConn())
	return errors.Join(append(errs, sess.wl.meter.Close())...)
}

// leave takes sess out of the server's sessions and gives back its
// connection slot, once: when its client's connection closes, or when one of
// its calls into DuckDB parked its thread (see parked), after which its
// connection's close may never be handled.
func (s *Server) leave(sess *session) {
	if !sess.left.CompareAndSwap(false, true) {
		return
	}
	s.sessions.remove(sess)
	s.releaseConn()
	s.metrics.sessionClosed(string(sess.currentUser().Role))
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
	user, checked := sess.user, sess.checked
	fresh := time.Since(checked) < s.roleRefresh || sess.fixed != nil
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
		// Keep the last known role if the metadata database blips, but
		// not for longer than maxStale (see refreshAccess).
		s.log.Warn("could not refresh user", "user", user.Name, "err", err)
		if age := time.Since(checked); age >= s.maxStale {
			return user, staleAccess("the session's user", age)
		}
		return user, nil
	}

	sess.mu.Lock()
	sess.user, sess.checked = latest, time.Now()
	sess.mu.Unlock()
	return latest, nil
}

// refreshAccess reads again, when they are due, what a statement is checked
// against besides the session's user, which refreshUser reads: the
// session's grants, if it has loaded them, the policies and ID domains'
// members. Every statement goes through both, as it is prepared and before
// each run.
//
// Should the metadata database fail, the user, grants, policies and
// members last read stay in force, so that a blip does not fail statements, but for no
// longer than maxStale (IGLOO_ACCESS_MAX_STALENESS): past it, statements
// are refused with staleAccess's error until they can be read again, since
// a user disabled, a token or grant revoked or a policy attached
// meanwhile, by another replica, would not apply. A cache that a change on
// this replica expired counts as that old at once.
func (s *Server) refreshAccess(ctx context.Context, sess *session, user auth.User) error {
	sess.mu.Lock()
	loaded := sess.privs.privs != nil
	sess.mu.Unlock()
	if loaded {
		if _, err := s.privileges(ctx, sess, user); err != nil {
			return err
		}
	}
	if _, err := s.gov.current(ctx); err != nil {
		return err
	}
	return s.membersCurrent(ctx)
}

// staleAccess refuses a statement because what, which it would be checked
// against, has not been read from the metadata database for age.
func staleAccess(what string, age time.Duration) error {
	return pgerror.WithCode(fmt.Errorf("cannot check this statement: %s could not be read from the metadata database "+
		"for %s, and may have changed since (IGLOO_ACCESS_MAX_STALENESS); try again once it can be reached",
		what, age.Round(time.Second)), codes.System)
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
// errCanceledBeforeRun is a cancel request's error for a statement that it
// ended before it ran.
var errCanceledBeforeRun = pgerror.WithCode(errors.New("canceling statement due to user request"), codes.QueryCanceled)

var errStatementTimeout = pgerror.WithCode(errors.New("canceling statement due to statement timeout"), codes.QueryCanceled)

// errMemoryShed is what a statement ShedMemory canceled fails with.
var errMemoryShed = pgerror.WithCode(errors.New("canceling statement: the server was running out of memory, "+
	"and this statement had run the longest; run it again, or on less data at a time"), codes.OutOfMemory)

// errServerExiting fails the statements running when Igloo exits because a
// statement it canceled for memory did not stop (see exitForMemory).
var errServerExiting = pgerror.WithCode(errors.New("canceling statement: Igloo is restarting, because a statement it "+
	"canceled for running out of memory did not stop and memory was nearly gone; run this statement again once it is back"), codes.AdminShutdown)

// runLocal runs st on this replica, unless failLocal, a test's, fails it.
func (s *Server) runLocal(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) error {
	if s.failLocal != nil {
		if err := s.failLocal(ctx, st); err != nil {
			return err
		}
	}
	return s.runStatement(ctx, sess, st, args, w)
}

// maxBlockRaces is how often runHere runs a statement again whose read of
// the read cache raced another over a block.
const maxBlockRaces = 3

// runHere is runLocal, which it calls again, up to maxBlockRaces times,
// when the statement's read of the read cache raced another over a block's
// file (see prefetch.BlockRace) before it sent a row, outside a
// transaction, which the failure aborted: the same read succeeds a moment
// later, and a missing block of the cache must not fail a statement.
func (s *Server) runHere(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w *tagWriter) error {
	for again := 0; ; again++ {
		err := s.runLocal(ctx, sess, st, args, w)
		if again == maxBlockRaces || !prefetch.BlockRace(err) || w.Written() > 0 || sess.inTx || ctx.Err() != nil {
			return err
		}
		s.log.Warn("a statement's read of the read cache raced another over a block; running it again", "user", sess.currentUser().Name,
			"retry", again+1, "err", err)
	}
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
	if sess.watched != nil {
		// The stuck watch watches the run again (see watchStuck).
		sess.watched = again
	}
	sess.mu.Unlock()
	return again, cancel
}

// cancelable returns a context that a cancel request, the session's
// statement timeout and the memory watch (ShedMemory) end, as runCancelable's,
// for what Igloo does for a statement before it runs it; query is the
// statement as the client sent it. done ends it, and returns err as the
// client is to read it.
func (sess *session) cancelable(ctx context.Context, query string) (context.Context, func(err error) error) {
	var cancel context.CancelFunc
	if timeout := sess.statementTimeout(); timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	sess.mu.Lock()
	sess.running, sess.statement, sess.started, sess.shed = cancel, redactQuery(query), time.Now(), false
	sess.mu.Unlock()
	return ctx, func(err error) error {
		ended := ctx.Err()
		sess.mu.Lock()
		shed, exiting := sess.shed, sess.exiting
		sess.running, sess.statement, sess.started = nil, "", time.Time{}
		sess.mu.Unlock()
		cancel()
		switch {
		case ended == nil:
			return err
		case errors.Is(ended, context.DeadlineExceeded):
			return errStatementTimeout
		case shed:
			return errMemoryShed
		case exiting:
			return errServerExiting
		}
		// A cancel request, whether or not the step it ended noticed.
		return errCanceledBeforeRun
	}
}

// runCancelable runs fn with a context that a cancel request, or the
// session's statement timeout, can end. query is the statement as the
// client sent it, which the console lists while it runs.
//
// The statement may first wait to start, behind the replica's and its
// user's limits (see admit), for as long as its queued timeout allows; its
// statement timeout counts from when it starts, as Snowflake's
// STATEMENT_TIMEOUT_IN_SECONDS and STATEMENT_QUEUED_TIMEOUT_IN_SECONDS do.
// Cancel requests end it either way.
func (sess *session) runCancelable(ctx context.Context, query string, fn func(context.Context) error) error {
	defer sess.activity.Begin()()
	// What a step run under it may run again under once the memory watch
	// stopped it (see rearm).
	defer func(was context.Context) { sess.outer = was }(sess.outer)
	sess.outer = ctx
	ctx, cancel := context.WithCancel(ctx)
	sess.mu.Lock()
	sess.running, sess.statement, sess.started, sess.shed, sess.queued = cancel, redactQuery(query), time.Now(), false, true
	sess.runShape = sess.wl.controller.Alike(sess.wl.exact, sess.wl.shape)
	sess.mu.Unlock()

	defer func() {
		sess.mu.Lock()
		sess.running, sess.statement, sess.started, sess.queued = nil, "", time.Time{}, false
		sess.runShape, sess.elsewhere = "", false
		sess.mu.Unlock()
		cancel()
	}()

	queued, cancelQueued := ctx, context.CancelFunc(func() {})
	if timeout := sess.queuedTimeout(); timeout > 0 {
		queued, cancelQueued = context.WithTimeout(ctx, timeout)
	}
	release, leave, err := sess.admit(queued)
	cancelQueued()
	if err != nil {
		return err
	}
	defer release()
	sess.mu.Lock()
	sess.started, sess.queued = time.Now(), false
	sess.mu.Unlock()
	if timeout := sess.statementTimeout(); timeout > 0 {
		var cancelRun context.CancelFunc
		ctx, cancelRun = context.WithTimeout(ctx, timeout)
		defer cancelRun()
	}
	// Once canceled or past its timeout it holds nobody up, however long DuckDB
	// takes to stop it, its slots and what it reserved or holds of the memory:
	// statements behind a report that went on after its client had gone started
	// only when it was stopped. One the memory watch canceled keeps them, as what
	// it holds is why, and it may run again (see rearm).
	stopLeaving := context.AfterFunc(ctx, func() {
		sess.mu.Lock()
		shed := sess.shed
		sess.mu.Unlock()
		if !shed {
			leave()
		}
	})
	defer stopLeaving()
	stopWatch := sess.watchStuck(ctx)
	err = fn(ctx)
	stopWatch()
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
		sess.mu.Lock()
		exiting := sess.exiting
		sess.mu.Unlock()
		if exiting {
			return errServerExiting
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
	var longest, stuck *session
	var since, stuckSince time.Time
	for _, sess := range all {
		sess.mu.Lock()
		switch {
		case sess.running == nil, sess.queued:
			// Running nothing, or waiting to start, holding no memory.
		case sess.shed:
			// Canceled already, and still running.
			if stuck == nil || sess.shedAt.Before(stuckSince) {
				stuck, stuckSince = sess, sess.shedAt
			}
		case longest == nil || sess.started.Before(since):
			longest, since = sess, sess.started
		}
		sess.mu.Unlock()
	}
	acted := false
	if stuck != nil {
		s.orphans.reset()
	}
	if stuck != nil && time.Since(stuckSince) >= s.shedStuckAfter {
		if s.memoryCritical > 0 && available < s.memoryCritical {
			s.exitForMemory(stuck, available, all)
			return true
		}
		stuck.conn.Interrupt()
		stuck.interruptBackground()
		s.log.Warn("a statement canceled for memory has not stopped; interrupted it again", "available", memlimit.Format(available),
			"user", stuck.user.Name, "pid", stuck.pid, "canceled_for", time.Since(stuckSince).Round(time.Millisecond))
		acted = true
	}
	if longest == nil {
		if stuck == nil {
			return s.orphanedWork(available, all)
		}
		return acted
	}
	s.orphans.reset()
	longest.mu.Lock()
	defer longest.mu.Unlock()
	if longest.running == nil {
		return false
	}
	longest.shed, longest.shedAt = true, time.Now()
	longest.running()
	s.orphans.canceled(longest.pid, longest.user.Name, longest.statement, longest.shedAt)
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
				sess.txTables.clear()
				// Cursors without WITH HOLD end with their transaction.
				sess.closeCursors(context.Background(), false)
			}
			return
		}
	}
	if st != nil {
		// A block keeps the tables of a statement that ran in it by a way
		// other than runStatement; outside one, they are forgotten.
		sess.noteTx(st)
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
// DESCRIBE and PRAGMA, as statements and as subqueries (SELECT name FROM
// (SHOW TABLES)). It over-approximates: a statement matched needlessly only
// runs on its own when it could have run alongside others.
var listsCatalog = regexp.MustCompile(`(?is)(^\s*(SHOW|DESCRIBE|DESC|PRAGMA)\b)|(\(\s*(SHOW|DESCRIBE|DESC)\b)|` +
	`\b(sqlite_master|sqlite_schema|sqlite_temp_master|sqlite_temp_schema|` +
	`duckdb_tables|duckdb_views|duckdb_columns|duckdb_constraints|duckdb_databases|duckdb_schemas|` +
	`information_schema|information_schema_tables|information_schema_schemata|information_schema_functions|` +
	`pg_catalog|pg_class|pg_attribute|pg_namespace|pg_tables|pg_views|pg_matviews|` +
	`pg_information_schema_columns|pg_description|pg_type|pg_proc)\b`)

// describesRelation matches DESCRIBE (or DESC) of one table or view, by its name:
// DuckDB binds what it describes, as a query's FROM, and walks nothing, so it holds
// the catalog lock shared, as any bind, not exclusively, as listsCatalog's
// statements. A BI tool describes a table before each query it builds on it, and
// each held up every bind beside it.
var describesRelation = regexp.MustCompile(`(?is)^\s*(DESCRIBE|DESC)\s+((("([^"]|"")*"|[a-z_][a-z0-9_$]*)\s*\.\s*){0,2}` +
	`("([^"]|"")*"|[a-z_][a-z0-9_$]*))\s*;?\s*$`)

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
	if err == nil {
		err = s.refreshAccess(ctx, sess, user)
	}
	if err != nil {
		return nil, err
	}

	s.refreshLimits(ctx, sess, user)
	// Models' methods are computed once per statement as it is parsed,
	// however many times parsing tries it (see modelRewrite).
	sess.modelMemo = nil
	if ps, ok := s.sessionResetCommand(sess, q.Query); ok {
		return ps, nil
	}
	if ps, ok := s.setConfigCommand(sess, q.Query); ok {
		return ps, nil
	}
	// COPY ... FROM STDIN, in either dialect: it is PostgreSQL's.
	if c, ok, err := parseCopyFromStdin(q.Query); ok {
		if err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, err
		}
		return s.copyFromStdinStatement(sess, q.Query, c), nil
	}
	if ps, ok := s.dialectCommand(sess, q.Query); ok {
		return ps, nil
	}
	if ps, ok := s.prefetchCommand(sess, q.Query); ok {
		return ps, nil
	}
	if ps, ok := s.switchCommand(sess, q.Query); ok {
		return ps, nil
	}
	if ps, ok := s.aiSettingCommand(sess, q.Query); ok {
		return ps, nil
	}
	// In the Snowflake dialect the statement is translated first, so that
	// all that follows, the checks of the user's role, grants and policies
	// above all, sees only the DuckDB statement Igloo will run.
	var tr *translation
	source := q.Query
	if sess.sqlDialect() == dialectSnowflake && !isIglooSQL(ctx) {
		if parts := snowflake.Split(q.Query); q.SimpleQuery && len(parts) > 1 {
			return s.snowflakeScript(ctx, sess, q, parts)
		}
		if tr, err = s.translate(ctx, sess, q.Query, false); err != nil {
			s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
			return nil, err
		}
		if tr.FlattenLists > 0 && tr.Command == "" && !tr.Untranslated {
			defer func() { sess.flattenLists = false }()
			tr = s.flattenForm(ctx, sess, source, tr)
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
	// DROP SCHEMA ... CASCADE of the catalog's schema, which Igloo carries
	// out drop by drop.
	if c, ok := s.parseSchemaCascade(ctx, sess, q.Query); ok {
		return s.schemaCascadeStatement(sess, q.Query, c), nil
	}
	if tr == nil || tr.Untranslated {
		ps, err := s.parseDuckDB(ctx, sess, user, q, tr)
		if err != nil || tr == nil {
			return ps, err
		}
		return withNotices(ps, tr.Notices), nil
	}
	ps, tr, err := s.parseTranslated(ctx, sess, user, q, source, tr)
	if err != nil {
		return nil, err
	}
	if err := s.checkViewColumns(ctx, sess, tr); err != nil {
		s.recordQuery(sess, queryRecord{text: source, start: time.Now(), err: err, rows: -1})
		return nil, err
	}
	if created, dropped := tr.Temporary, tr.Dropped; created != nil || dropped != nil {
		// A temporary object named in a schema is read by that name once
		// the statement that creates it has succeeded.
		for _, p := range ps {
			p.WrapFn(func(fn wire.PreparedStatementFn) wire.PreparedStatementFn {
				return func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
					return fn(ctx, afterWriter{DataWriter: w, run: func() error { sess.noteTemporary(created, dropped); return nil }}, params)
				}
			})
		}
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

// noteTemporary records the temporary table or view a statement in
// Snowflake's dialect created in a schema's name, and forgets one it
// dropped, so that later statements read it by that name (see
// snowflake.Options.Temporary).
func (sess *session) noteTemporary(created, dropped *snowflake.TempName) {
	if sess.sfTemps == nil {
		sess.sfTemps = map[snowflake.TempName]bool{}
	}
	if dropped != nil {
		delete(sess.sfTemps, *dropped)
	}
	if created != nil {
		sess.sfTemps[*created] = true
	}
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

	// Statements on Snowflake's ML classes, and models' methods in queries
	// (see models.go and mlrewrite.go).
	if ps, ok, err := s.modelStatement(ctx, sess, q); ok || err != nil {
		return ps, err
	}
	if rewritten, err := s.modelRewrite(ctx, sess, user, q.Query); err != nil {
		err = pgError(err)
		s.recordQuery(sess, queryRecord{text: q.Query, start: time.Now(), err: err, rows: -1})
		return nil, err
	} else if rewritten != q.Query {
		q.Query = rewritten
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
			if err := sess.deallocateAll(ctx); err != nil {
				return err
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

	if ps, ok, err := s.aiCommand(ctx, sess, q.Query); ok || err != nil {
		return ps, err
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

	if ps, ok, err := s.analyzeStatement(sess, q); ok || err != nil {
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

	if ps, ok := s.memoStatement(ctx, sess, user, q, tr); ok {
		return ps, nil
	}
	return s.preparedQuery(ctx, sess, user, q, tr)
}

// preparedQuery prepares q, a statement in DuckDB's dialect as translated
// by tr if at all, and returns what runs it.
func (s *Server) preparedQuery(ctx context.Context, sess *session, user auth.User, q wire.Query, tr *translation) (wire.PreparedStatements, error) {
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
		// A statement that calls AI functions starts them now, before it
		// binds again and reads its tables (see beginAI).
		var aist *ai.Statement
		explain, explaining := plainExplain(st)
		if st.ai {
			if aist, err = s.beginAI(ctx, sess, st, explaining); err != nil {
				s.metrics.query(start, err)
				s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: st.access, policies: st.policies()})
				return pgError(err)
			}
		}
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
		// One Igloo wrote to read bitmaps from the session's variables has
		// them set again for its run, which drops them after (see
		// hybridBitmaps).
		defer sess.resetBitmapVariables(ctx)
		// What Igloo does for the statement before it runs (setting its
		// bitmaps' variables, checking it, preparing it again, which for a
		// report call reads bitmap indexes) is ended as its run is: by a
		// cancel request, the statement timeout or the memory watch.
		pctx, prepared := sess.cancelable(ctx, q.Query)
		if s.beforeRefresh != nil {
			s.beforeRefresh(pctx)
		}
		readied := s.timePhases(ctx, sess, "statement readied to run")
		bitmaps := st.bitmaps
		err = s.readyStatement(pctx, sess, st)
		if err == nil && st.bitmaps != bitmaps {
			if sess.bitmapsRestore != nil {
				sess.bitmapsRestore()
			}
			sess.bitmapsRestore = sess.setBitmaps(ctx, st.bitmaps)
		}
		checked := false
		if err == nil && st.reportCall {
			// Its bitmaps there and of their cohorts' latest versions,
			// whatever its shape reads of them. Prepared for bitmaps that
			// are not, it is prepared again, now (see refreshIndexes), and
			// checked again.
			switch cerr := sess.checkBitmapVariables(pctx, st.query); {
			case cerr == nil:
				checked = true
			case !st.reportRan:
				st.reportRan = true
				err = s.refreshIndexes(pctx, sess, st)
			default:
				err = cerr
			}
			st.reportRan = true
		}
		if err == nil && !checked {
			err = sess.checkBitmapVariables(pctx, st.query)
		}
		if err == nil {
			// The models and results it names are still as they were when
			// it was prepared, which an erasure request may have purged
			// since (see checkModelCalls).
			err = sess.checkModelCalls(st.prepared, s.modelRuntime.Gen())
		}
		readied()
		err = prepared(err)
		if err != nil {
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: -1, acc: st.access, policies: st.policies()})
			return err
		}
		// A result kept of the same statement, or one another session's
		// run of it is about to give, goes out as it is (see
		// resultcache.go); a cancel request and the statement timeout end
		// the wait for one.
		served, fill := false, (*resultFill)(nil)
		if s.results != nil && !sess.resultCacheOff && resultCandidate(sess, st, tw) {
			cctx, cached := sess.cancelable(ctx, q.Query)
			var key *resultKey
			served, fill, key, err = s.fromCache(cctx, sess, st, args, tw)
			err = cached(err)
			if key != nil && err == nil {
				// The next of the same text is answered before it is
				// prepared (see memoStatement).
				s.noteStatement(ctx, sess, user, q.Query, st, *key, tr.columns(st.columns))
			}
		}
		if served || err != nil {
			var used *audit.Resources
			if err == nil {
				// It read nothing and used nothing.
				used = &audit.Resources{}
			}
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: tw.rows(), acc: st.access,
				policies: st.policies(), used: used, stmtType: st.stmtType})
			return pgError(err)
		}
		tw.fill = fill
		// Whatever happens to the run, those waiting for it go on.
		defer s.finishResult(fill, errUnfinished)
		var parts distParts
		// Hold the catalog lock only while the statement runs, not through
		// the metrics and ownership bookkeeping below, to keep the exclusive
		// window short (see catalogRunLock, holdCatalog).
		releaseCatalog := s.catalogRunLock(sess, st)
		outer := ctx
		// A query or a change of data waits its turn behind the replica's
		// limit of statements at once, if there is one.
		sess.wl.counted = countsOnReplica(sess, st)
		sess.wl.shape, sess.wl.exact = "", ""
		if sess.wl.counted || st.staged != nil {
			sess.wl.shape = s.workloadShape(ctx, sess, st.shapeText())
			sess.wl.exact = s.workloadExact(sess, st.shapeText())
		}
		shape, exact := sess.wl.shape, sess.wl.exact
		sess.wl.apart = s.runsApart(sess, st)
		sess.wl.planned = func(ctx context.Context) (int64, bool) { return s.planned(ctx, sess, st) }
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
					// far short (see guardAfter).
					guard := guardLocalBy(ctx, func() time.Duration { return guardAfter(parts.local, parts.alt, parts.budget, s.othersRunning(sess)) }, tw)
					err = s.runHere(guard.ctx, sess, st, args, tw)
					stopPrefetch()
					if fired, ran := guard.stop(tw); fired && ctx.Err() == nil && outer.Err() == nil {
						return s.afterGuard(ctx, sess, st, args, tw, &parts, ran)
					}
				} else {
					err = s.runPlaced(ctx, outer, sess, st, args, tw, &parts)
					stopPrefetch()
				}
				if (st.bitmaps || len(st.indexPins) > 0) && bitmapsDidNotFit(err) {
					// Run again here from the tables or with the hash,
					// below, rather than on workers with bitmaps.
					return err
				}
				if err != nil && parts.key != "" && tw.Written() == 0 && outer.Err() == nil && sess.ranOutOfMemory(ctx, err) {
					return s.afterOutOfMemory(outer, ctx, sess, st, args, tw, &parts, err)
				}
				return err
			}
			run := func() error {
				err := runOnce()
				if len(st.indexPins) > 0 && bitmapsDidNotFit(err) && tw.Written() == 0 && outer.Err() == nil {
					err = s.rerunWithoutIndexes(ctx, sess, st, err, runOnce)
				}
				if st.bitmaps && bitmapsDidNotFit(err) && tw.Written() == 0 && outer.Err() == nil {
					return s.rerunWithoutBitmaps(ctx, sess, st, err, runOnce)
				}
				return err
			}
			// Its AI functions run under aist (see aiRunning).
			return sess.aiRunning(ctx, aist, func() error {
				if explaining {
					// EXPLAIN says what the AI functions would send, and which
					// model each forecast's name means (see tsforecast.go).
					var extra [][]any
					if aist != nil {
						extra = s.explainAI(ctx, sess, aist, explain, args)
					}
					if extra = append(extra, s.explainForecasts(ctx, sess, explain)...); len(extra) > 0 {
						tw.DataWriter = &extraRows{DataWriter: tw.DataWriter, rows: extra}
					}
				}
				switch {
				case st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION && !sess.inTx && opensBlock(st):
					return s.beginBlock(ctx, sess, func() error { return sess.runTransaction(ctx, st, tw, run) })
				case st.stmtType == duckdb.STATEMENT_TYPE_TRANSACTION:
					return sess.runTransaction(ctx, st, tw, run)
				case st.changesIceberg:
					return sess.outsideTransaction(ctx, run)
				case st.stmtType == duckdb.STATEMENT_TYPE_SET || st.stmtType == duckdb.STATEMENT_TYPE_VARIABLE_SET:
					return s.runSetting(ctx, sess, st, args, tw)
				}
				return run()
			})
		})
		var spend *ai.Spend
		if aist != nil && !explaining {
			sp := aist.End()
			spend = &sp
			aiNotice(w, sp)
		}
		sess.wl.counted, sess.wl.shape, sess.wl.exact, sess.wl.apart, sess.wl.planned = false, "", "", false, nil
		if st.filled != nil {
			// A fill's query ran on workers, or here, as a read of it would
			// (see fillOn): the history says where, and auto mode learns how
			// long it took.
			parts, st.filled = *st.filled, nil
		}
		releaseCatalog()
		s.finishResult(fill, err)
		var used *audit.Resources
		if err == nil {
			used = sess.measure(st)
		}
		// A run planned to no rows says nothing of others of its shape (see
		// workload.ReadNothing).
		measured := used != nil && !workload.ReadNothing(used.BytesRead, used.CPUTime)
		if measured && tr != nil && tr.flattenKey != "" {
			s.workload.ObserveForm(tr.flattenKey, tr.flattenLists, used.CPUTime)
		}
		peak, sampled := sess.wl.peak, sess.wl.sampled
		if used != nil && parts.workers == 0 && s.heavy != nil && (measured || !s.heavy.known(shape)) {
			// Which of this replica's DuckDBs the next like it runs in.
			s.heavy.observe(shape, used.CPUTime, sess.wl.ranInBackground)
		}
		switch {
		case parts.workers > 0:
			// One that ran on workers held most of its memory there.
			if err == nil {
				s.workload.ObserveOnWorkers(shape)
				s.workload.ObserveOnWorkers(exact)
			}
		case measured:
			// What it held here, for the next statement like it, and the next the same.
			for _, key := range []string{shape, exact} {
				s.workload.Observe(key, workload.FootprintOf(used.MemoryAllocated, peak, sampled))
				s.workload.ObserveCost(key, used.CPUTime)
			}
			s.workload.ObserveTook(shape, time.Since(start)-sess.wl.queued)
		case used != nil:
			s.workload.ObserveEmpty(exact)
		case err != nil && sampled:
			// It ended before its end, holding at least that much, or, if it
			// ran a while, all the memory: it may have held any of what the
			// statements beside it were charged with.
			if time.Since(start)-sess.wl.queued >= workload.LongUnfinished {
				peak = max(peak, s.queryMemory)
			}
			s.workload.ObserveUnfinished(shape, peak)
			s.workload.ObserveUnfinished(exact, peak)
		}
		if !sampled {
			peak = 0
		}
		queued := sess.wl.queued
		s.metrics.query(start, err)
		sess.observe(st, err)
		if err == nil {
			switch {
			case changesCatalog(st.stmtType) && tablesOnly(st):
				// DuckDB made the change, which the catalog proxy saw: its
				// listings and the tables' versions follow it already
				// (see resultGeneration).
				s.ddlGen.Add(1)
				sess.ddlCount++
			case changesCatalog(st.stmtType):
				s.catalogChanged()
				sess.ddlCount++
			case st.stmtType == duckdb.STATEMENT_TYPE_SET || st.stmtType == duckdb.STATEMENT_TYPE_VARIABLE_SET,
				st.stmtType == duckdb.STATEMENT_TYPE_PRAGMA:
				// A PRAGMA may change a setting too.
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
			s.dist.ObserveOn(parts.key, parts.workers, parts.large, time.Since(cmp.Or(parts.since, start)))
			if parts.workers == 0 && peak > 0 {
				// What it held here, against what its plan said it would.
				s.dist.ObservePeak(parts.key, peak)
			}
		}
		s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: tw.rows(), acc: st.access,
			policies: st.policies(), used: used, queued: queued, analyze: st.sourceText(), stmtType: st.stmtType, parts: parts, ai: spend,
			shape: shape, exact: exact, peak: peak, form: formOf(tr)})
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

// parked ends the session one of whose calls into DuckDB parked its thread
// (see openSession's call guard), as what escaped DuckDB there says: it lets
// go of the session's hold on the catalog lock and of what its statement was
// admitted with, which the thread, waiting for good, would keep from every
// other session, takes the session out of the server's, so the memory watch
// does not wait on its statement, and closes the client's connection. The
// thread keeps its DuckDB connection, and what DuckDB held for its statement.
func (s *Server) parked(ctx context.Context, sess *session, what string) {
	sess.mu.Lock()
	text, user, pid := sess.statement, sess.user.Name, sess.pid
	sess.mu.Unlock()
	sum := sha256.Sum256([]byte(text))
	s.log.Error("a C++ exception escaped DuckDB on a statement's thread, where nothing could catch it; the statement and its session end, "+
		"and its thread and DuckDB connection wait for good", "pid", pid, "user", user, "statement_sha256", hex.EncodeToString(sum[:8]), "what", what)
	s.unlockCatalog(sess)
	s.leave(sess)
	if done := sess.wl.admitted; done != nil {
		done()
	}
	if c := wire.AcceptedConn(ctx); c != nil {
		_ = c.Close()
	}
}

// execHeld runs statements of Igloo's own that change the catalog, DDL of
// its tables and schemas, holding the catalog lock exclusively, as a user's
// DDL runs (see catalogRunLock): a drop erases from the catalog's in-memory
// set of tables, and creating one looks through it.
func (s *Server) execHeld(ctx context.Context, sess *session, stmts ...string) error {
	defer s.holdCatalog(sess)()
	for _, q := range stmts {
		if _, err := sess.conn.ExecContext(ctx, q, nil); err != nil {
			return err
		}
	}
	return nil
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
	start := time.Now()
	switch want {
	case catalogExclusive:
		sess.catRelease = s.engine.CatalogLock()
		sess.catSince = time.Now()
		sess.catWait += sess.catSince.Sub(start)
	case catalogShared:
		sess.catRelease = s.engine.CatalogShare()
		sess.catWait += time.Since(start)
	}
	sess.catHeld = want
}

// catalogHoldLogged is how long a session holds the catalog lock
// exclusively, keeping every other session's binds, listings and DDL
// waiting, before letting go of it is logged.
const catalogHoldLogged = time.Second

// unlockCatalog lets go of the engine's catalog lock the session holds, if
// any, keeping its holds.
func (s *Server) unlockCatalog(sess *session) {
	if sess.catRelease != nil {
		var held time.Duration
		if sess.catHeld == catalogExclusive {
			held = time.Since(sess.catSince)
		}
		sess.catRelease()
		sess.catRelease = nil
		if held >= catalogHoldLogged {
			// The session's backend pid, which its client was given (not its
			// history's session_id, which takes the session's mutex).
			s.log.Info("held the catalog lock exclusively, keeping other sessions' statements waiting",
				"pid", sess.pid, "held", held.Round(time.Millisecond))
		}
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
// changesIceberg, is always one), EXECUTE of a statement prepared in SQL
// that may be any of these (see executeHolds), and listings
// (information_schema, pg_catalog, duckdb_tables()/views()/columns(), SHOW,
// DESCRIBE, PRAGMA; see listsCatalog). Preparing and rechecking any
// statement take it exclusively too, since Igloo's checks list the catalog
// (see prepareChecked, recheck).
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
// CREATE TABLE ... AS of an Iceberg table, which runs outside any
// transaction block, and of a temporary table are carried out in steps that
// take the lock themselves, exclusively only to change the catalog (see
// runStagedCreate, runTempCreate): as single statements, a long one, a
// bucketed copy or dbt's temporary table of an incremental model's new
// rows, held the lock for its whole run, and no session could sign in.
func (s *Server) catalogRunLock(sess *session, st *statement) func() {
	// CREATE TABLE ... AS in steps, of an Iceberg table, which runs outside
	// a transaction block (see outsideTransaction), or of a temporary one.
	if st.staged != nil || st.tempFill != nil {
		return func() {}
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_EXECUTE && !sess.executeHolds(st.query) {
		return func() {}
	}
	lists := st.listsCatalog && !describesRelation.MatchString(st.query)
	if st.changesIceberg || lists || changesCatalog(st.stmtType) || st.stmtType == duckdb.STATEMENT_TYPE_EXECUTE {
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
	// Preparing can take seconds: recognizing reports, writing the report
	// API's calls, computing their cohorts' bitmaps and counts, all on the
	// session's connection. A cancel request, the statement timeout and the
	// memory watch end it as they end the statement's run (see cancelable);
	// otherwise a cancel request sent while it is prepared would be lost, and
	// the statement would run to the end.
	pctx, prepared := sess.cancelable(ctx, text)
	if s.beforePrepare != nil {
		s.beforePrepare(pctx, text)
	}
	st, err := s.prepareChecked(pctx, sess, user, text, oids)
	if err != nil && st == nil {
		// An admin's DROP TABLE of a name that a create nobody committed
		// holds releases it (see clearLeftovers).
		if again := s.dropUnseen(pctx, sess, user, text, err); again != "" {
			st, err = s.prepareChecked(pctx, sess, user, again, oids)
		}
	}
	if err = prepared(err); err != nil && st != nil {
		_ = st.close()
		st = nil
	}
	if err != nil {
		if pgerror.GetCode(err) == codes.InsufficientPrivilege {
			s.metrics.query(time.Now(), err)
		}
		s.recordQuery(sess, queryRecord{text: text, start: start, err: err, rows: -1})
		return nil, err
	}
	return st, nil
}

// readyStatement is what Igloo does for a prepared statement before each
// run, under its cancel (ctx), and EXPLAIN DISTRIBUTED before it plans
// what the run would: it checks the session's transaction, sets the
// variables of the bitmaps it reads, checks it again, and prepares it again
// where it counts through ID domains' ids, as their snapshots stand now,
// or reads bitmap indexes. Preparing again may turn its counting in roaring
// bitmaps on or off (st.bitmaps), which the caller applies to the session.
func (s *Server) readyStatement(ctx context.Context, sess *session, st *statement) error {
	if err := sess.checkAborted(st.query); err != nil {
		return err
	}
	if err := sess.ensureBitmapVariables(ctx, st.query); err != nil {
		return err
	}
	// DuckDB plans the statement again as it runs it, with the distinct
	// counts of the tables its prepare loaded.
	s.registerDistinctCounts(ctx, sess)
	// Rechecking re-plans with EXPLAIN and may re-apply policies, both of
	// which read and fill in the catalog's set; release before the run
	// takes the side it needs (see holdCatalog, catalogRunLock). A
	// statement with nothing to check again, as an admin's that reads no
	// connection while the policies, the catalog and the session's settings
	// stay as they were, holds nothing here: every run of one would
	// otherwise wait its turn for the lock with the statements being
	// prepared.
	deciding := sess.phase("recheck")
	again, err := s.needsRecheck(ctx, sess, st)
	deciding()
	if err != nil {
		return err
	}
	if again {
		locked := sess.phase("catalog_lock")
		relist := s.holdCatalog(sess)
		locked()
		rechecking := sess.phase("recheck")
		err := s.recheck(ctx, sess, st)
		rechecking()
		relist()
		if err != nil {
			return err
		}
	}
	// One that counts through ID domains' ids reads the snapshots that have
	// them (see idcount.go); looking them up asks the catalog, which holds
	// nothing, and preparing again takes the lock it needs.
	refreshing := sess.phase("refresh_ids")
	err = s.refreshIDCounts(ctx, sess, st)
	refreshing()
	if err != nil {
		return err
	}
	// And one answered from bitmap indexes, their tables' current
	// snapshots, or the tables (see bitmaprewrite.go).
	defer sess.phase("refresh_indexes")()
	return s.refreshIndexes(ctx, sess, st)
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

// prepareHeld is prepareChecked holding the catalog lock as mode says.
func (s *Server) prepareHeld(ctx context.Context, sess *session, user auth.User, text string, oids []uint32, mode catalogMode) (st *statement, err error) {
	// Preparing reads and fills in the catalog's in-memory set of tables:
	// DuckDB binds the statement (and suggests names for a missing table),
	// and Igloo resolves names, loads schemas and checks grants the same way,
	// filling in each table it names on first use. Hold the exclusive side so
	// no other statement reads the set while this changes it (see
	// holdCatalog), or the shared side for a query that may be prepared
	// alongside others (see sharedprepare.go).
	defer s.timePhases(ctx, sess, "statement prepared")()
	locked := sess.phase("catalog_lock")
	defer s.catalogHold(sess, mode)()
	if mode == catalogShared {
		sess.sharedPrepare = true
		defer func() { sess.sharedPrepare = false }()
	}
	locked()
	// DuckDB plans it with the tables' distinct counts as last loaded.
	s.registerDistinctCounts(ctx, sess)
	// The catalog's tables it reads: those DuckDB loads while this holds
	// the lock, as no other bind runs alongside (see memoStatement), or with
	// the tables of binds alongside when it holds it shared.
	loads := s.engine.RecordLoads()
	loadsGen := s.catalogGeneration()
	// Whether it calls AI functions: DuckDB binds them as it prepares it
	// (see aiHook).
	sess.aiBound.Store(false)
	defer func() {
		if l := loads(); st != nil {
			st.loads, st.loadsGen = l, loadsGen
			st.ai = sess.aiBound.Load()
			// DuckDB has read the tables' snapshots for the block.
			sess.noteTx(st)
		}
		if sess.sqlPrepared != nil && len(sqllex.Split(text)) > 1 {
			// Preparing several ran all but the last, a PREPARE among them
			// perhaps (see executeHolds).
			sess.sqlPrepared = nil
		}
	}()
	defer sess.cacheReports()()
	if sess.memo == nil {
		sess.memo = &checkMemo{plans: map[string]any{}}
		defer func() { sess.memo = nil }()
	}
	if !(sess.temps && sess.tempMacros) && mayCreateTemporary(text) {
		sess.temps, sess.tempMacros = true, true
	}
	// The counting CTEs of audience reports become calls of the report API
	// (see reportrecognize.go), which the statement then goes through as
	// one its user wrote. If it fails that way, for whatever reason, it is
	// prepared as written, and fails or not as written.
	recognizing := sess.phase("recognize")
	recognized := s.recognizeReports(ctx, sess, text)
	recognizing()
	if recognized != text {
		txFailed := sess.txFailed
		st, err := s.prepareCheckedText(ctx, sess, user, text, recognized, oids)
		if err == nil {
			return st, nil
		}
		// The statement as written decides whether a transaction fails.
		// (DuckDB's own fails only with errors in running a statement,
		// not in binding one, which is how a recognized form fails.)
		sess.txFailed = txFailed
		s.reportUnrecognized.Add(1)
		s.metrics.reportFallback()
		s.log.Debug("a statement whose reports were recognized runs as written", "err", err)
		if s.reportFallback != nil {
			s.reportFallback(recognized, err)
		}
	}
	return s.prepareCheckedText(ctx, sess, user, text, text, oids)
}

// prepareCheckedText is prepareChecked of text, which is source, the
// statement as written, or what recognizing its reports made of it.
func (s *Server) prepareCheckedText(ctx context.Context, sess *session, user auth.User, source, text string, oids []uint32) (*statement, error) {
	fail := func(err error) (*statement, error) { return nil, err }
	// Calls of the report API become the SQL they stand for before
	// anything else looks at the statement (see reportapi.go). The
	// statement keeps its text as written, which preparing again writes
	// again, as the tables read through ids change.
	asGiven := text
	calls := sess.phase("report_calls")
	text, reportPins, reportIndexPins, err := s.reportCalls(ctx, sess, text)
	calls()
	if err != nil {
		return fail(err)
	}
	// A call that counts distinct pairs in roaring bitmaps (see
	// pairBitmaps) runs as statements that count in bitmaps do, again
	// without them if they do not fit.
	reportBitmaps := text != asGiven && strings.Contains(text, "rb_count_distinct(")
	var viewGen uint64
	if s.ddl != nil {
		viewGen = s.ddl.Generation()
	}
	counting := sess.phase("counting")
	rewritten, counts := s.rewriteCounting(ctx, sess, user, text, true)
	counting()
	if len(reportPins) > 0 {
		if counts == nil {
			counts = &idCounts{}
		}
		counts.pins = append(counts.pins, reportPins...)
	}
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
	// Igloo's model functions name only what this session's statements
	// resolved, after their checks (see mlrewrite.go).
	if err := sess.checkModelCalls(query, s.modelRuntime.Gen()); err != nil {
		return fail(err)
	}
	// The columns that hold ID domains' ids are Igloo's (see iddomains.go),
	// and so are bitmap indexes' tables (see bitmaprewrite.go): checked in
	// the statement as written, and in what a caller gave the report API
	// (reportOwnNames), not in the SQL it writes, which reads them.
	if err := hiddenReferences(source); err != nil {
		return fail(err)
	}
	if err := indexReferences(source); err != nil {
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
	protecting := sess.phase("policies_before")
	err = s.protectBeforePrepare(ctx, sess, user, query, pivot)
	protecting()
	if err != nil {
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
	indexing := sess.phase("index_rewrite")
	query, indexPins, indexCandidate := s.rewriteIndexes(ctx, sess, user, query, counts)
	indexing()
	if len(indexPins) > 0 {
		s.metrics.indexStatement()
	}
	// The report API's calls read from bitmap indexes keep theirs too.
	indexPins = append(indexPins, reportIndexPins...)
	indexCandidate = indexCandidate || len(reportIndexPins) > 0
	// A statement that counts ID domains' IDs in roaring bitmaps is
	// prepared with the rb extension's setting on (see bitmaps.go).
	bitmaps := s.bitmapsFor(ctx, sess, query) || reportBitmaps
	defer sess.setBitmaps(ctx, bitmaps)()
	written := query
	sharing := sess.phase("share")
	if factored := s.factorUnions(ctx, sess, query); factored != "" {
		query = factored
	}
	shared := s.shareSubqueries(ctx, sess, query)
	sharing()
	preparing := sess.phase("duckdb_prepare")
	var st *statement
	if shared != "" {
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
	preparing()
	if err != nil && sess.sharedPrepare {
		// Prepared again under the exclusive lock, which the steps below
		// need (see sharedprepare.go).
		return fail(err)
	}
	if err != nil && s.ddl != nil && missingRelation.MatchString(err.Error()) {
		// Perhaps a view another replica just created, or one this replica
		// could not make when it last synced, which DuckDB cannot bind (see
		// ddl's stubLocal): sync and try again, also when this sync changed
		// nothing because another one, of the background or another
		// session, brought the view meanwhile. A view that still cannot be
		// made fails the statement with why.
		if changed, _ := s.ddl.SyncViews(ctx); changed || s.ddl.Generation() != viewGen {
			query = s.ddl.RewriteViewRefs(query)
			st, err = prepare(ctx, sess.conn, query, oids)
		}
		err = s.ddl.Explain(err)
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
		st.indexPins, st.indexed, st.text = indexPins, indexed, source
	}
	if pivot && st.stmtType != duckdb.STATEMENT_TYPE_SELECT {
		// multipleStatements let through what it takes for a query.
		s.log.Error("a PIVOT statement that is not a query was prepared", "user", user.Name, "type", st.stmtType)
		_ = st.close()
		return fail(pgerror.WithCode(errors.New("permission denied: only admins may run this PIVOT"), codes.InsufficientPrivilege))
	}
	// Apply policies: what runs is the statement rewritten to read
	// protected tables through them.
	policies := sess.phase("policies")
	protected, g, ran, err := s.policyRewrite(ctx, sess, user, st.stmtType, query, oids, st.indexPins)
	if err == nil && protected != query {
		st.original = query
		err = st.adopt(ctx, sess.conn, protected)
	} else if err == nil && ran {
		err = st.reprepare(ctx, sess.conn)
	}
	policies()
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
	granting := sess.phase("grants")
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
	granting()
	if err == nil {
		// Nothing changes ID domains' dictionaries or bitmap indexes but
		// Igloo.
		err = s.iglooSchemaWrites(ctx, sess, st.stmtType, query)
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
	if !st.changesIceberg && st.stmtType == duckdb.STATEMENT_TYPE_CREATE && len(st.params) == 0 {
		st.tempFill = s.tempCreateOf(st.query)
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_INSERT {
		st.ids = ids
	}
	if counts != nil && st.stmtType == duckdb.STATEMENT_TYPE_SELECT {
		st.idCounts, st.text = counts, source
	}
	if source != text && st.stmtType == duckdb.STATEMENT_TYPE_SELECT {
		st.reportCall, st.text = true, source
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
	if !force && !s.grantsStale(ctx, sess, user, st) {
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

// grantsStale reports whether st's grants, which were checked (st.access), must be
// checked again for user: the user's privileges, the catalog or the session's
// settings changed, or they were checked longer than roleRefresh ago.
func (s *Server) grantsStale(ctx context.Context, sess *session, user auth.User, st *statement) bool {
	privs, err := s.privileges(ctx, sess, user)
	return err != nil || !(privs == st.access.privs || st.access.privs == nil && user.Role == auth.RoleAdmin) ||
		time.Since(st.access.checked) >= s.roleRefresh ||
		st.access.catalogGen != s.catalogGeneration() || st.access.settingGen != sess.settingGen
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
	// statement_queued_timeout, the longest a statement may wait to start
	// (see runCancelable); none unless set.
	if m := setQueuedTimeout.FindStringSubmatch(query); m != nil {
		d, isDefault, err := parseStatementTimeout(m[1])
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err != nil {
				return pgerror.WithCode(errors.New(strings.ReplaceAll(err.Error(), "statement_timeout", "statement_queued_timeout")),
					codes.InvalidParameterValue)
			}
			sess.queueTimeout = d
			if isDefault {
				sess.queueTimeout = 0
			}
			return w.Complete("SET")
		})
	}
	if resetQueuedTimeout.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.queueTimeout = 0
			return w.Complete("RESET")
		})
	}
	if showQueuedTimeout.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{formatTimeout(sess.queuedTimeout())}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "statement_queued_timeout", Oid: pgtype.TextOID, Width: -1}}))
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
	mu    lockorder.Mutex
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

// connected gives sess its connection, under r.mu: add reads other
// sessions' connections, while their own goroutines set them up.
func (r *registry) connected(sess *session, conn *duckdb.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess.conn = conn
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
	s.defsGen.Add(1)
	s.engine.CatalogChanged()
}
