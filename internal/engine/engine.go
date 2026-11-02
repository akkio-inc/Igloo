// Package engine runs SQL on an embedded DuckDB that is attached to the
// Iceberg catalog. Each client session gets its own DuckDB connection; the
// catalog attachment and storage credentials are shared by all of them.
package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/azurecreds"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/prefetch"
	"github.com/akkio-inc/igloo/internal/tsforecast"
)

// attachRetryInterval is how long Open waits between attempts to reach the
// catalog. In a fresh deployment the catalog often starts after Igloo.
const attachRetryInterval = 2 * time.Second

// Extensions are the DuckDB extensions Igloo uses: object storage (httpfs,
// aws, azure), Iceberg (which reads manifests with avro), TPC-H data for
// demos and benchmarks, the read cache (cache_httpfs, see cache.go),
// geospatial functions and sketches (see media.go), and connections to
// PostgreSQL and MySQL databases (see package federation), which are loaded
// only when a connection needs them. The image installs them at build time.
var Extensions = append([]string{"httpfs", "aws", "azure", "iceberg", "avro", "tpch", "cache_httpfs", "postgres", "mysql"}, mediaExtensions...)

// communityExtensions come from DuckDB's community repository rather than
// the core one. DuckDB checks their signatures on load just the same.
var communityExtensions = map[string]bool{"cache_httpfs": true, "h3": true, "datasketches": true}

// Engine owns the embedded DuckDB database.
type Engine struct {
	connector *duckdb.Connector
	parser    *parser
	// catalog is the attached Iceberg catalog's name in SQL, if any.
	catalog string
	// proxy carries DuckDB's requests to the catalog, if any, and makes
	// each change happen once (see package catalogproxy).
	proxy *catalogproxy.Proxy
	// local is DuckDB's own database, where Igloo keeps its copies of the
	// catalog's views: DuckDB cannot keep views in an Iceberg catalog.
	local string
	// connectStmts run on every new connection so unqualified names
	// resolve to the catalog's default schema, and to the views kept for
	// it in the local database.
	connectStmts []string
	// catalogViewer supplies the catalog functions; see SetCatalogViewer.
	catalogViewer atomic.Pointer[CatalogViewer]
	// cache is the on-disk read cache, or nil when it is off.
	cache *readCache
	// prefetcher reads ahead into the cache, or is nil (see setupPrefetch).
	prefetcher *prefetch.Prefetcher
	// history supplies the history functions; see SetHistory.
	history atomic.Pointer[historyConfig]
	// jobHistory supplies the job history functions; see SetJobHistory.
	jobHistory atomic.Pointer[jobHistoryConfig]
	// alertHistory supplies igloo_alert_history(); see SetAlertHistory.
	alertHistory atomic.Pointer[alertHistoryConfig]
	// locations are where COPY INTO may load from; see Locations.
	locations []string
	// policies supplies the policy reference functions; see SetPolicies.
	policies atomic.Pointer[func() *policy.Snapshot]
	// erasure supplies igloo.erasure_requests; see SetErasure.
	erasure atomic.Pointer[erasureConfig]
	// aiHook connects the AI functions to sessions; see SetAI.
	aiHook atomic.Pointer[aiHookBox]
	// models scores models and serves their methods' results (see Models).
	models *models.Runtime
	// purges are what EmptyCache empties beside the read cache (see
	// OnEmptyCache).
	purgeMu sync.Mutex
	purges  []func()
	// forecasting forecasts with the model pack's models (see Forecasting).
	forecasting *tsforecast.Service
	// catMu keeps binding, walking and changing the attached catalog's
	// in-memory set of tables apart. See CatalogLock. catLocked is when it
	// was taken exclusively, in Unix nanoseconds, or 0 while it is not.
	catMu     sync.RWMutex
	catLocked atomic.Int64
	// catShared counts the holders of its shared side.
	catShared atomic.Int64
	// releaseTempDir lets go of the spill directory; see claimTempDir.
	releaseTempDir func()
	// bitmaps says the rb extension is loaded (see loadRB).
	bitmaps bool
	log     *slog.Logger
	// cfg is what it was opened with, and isBackground says it is the
	// second DuckDB of a process, for heavy statements (see background.go);
	// bg is that one, opened at its first use, and the rest its upkeep.
	cfg          config.Config
	isBackground bool
	bgMu         sync.Mutex
	bg           *Engine
	bgErr        error
	bgAt         time.Time
	bgStop       chan struct{}
	bgUsers      atomic.Int64
	bgWake       chan struct{}
	balanceMu    sync.Mutex
	// limit is the memory limit memlimit gives it, for the two together;
	// current what it is now set to; fgRead when a statement that reads
	// tables last ran in it, in Unix nanoseconds.
	limit, current, fgRead atomic.Int64
	// fgHeld, if set, says statements wait in line with none running in the
	// first (see WatchForeground).
	fgHeld atomic.Pointer[func() bool]
	// bgOver is what the process holds past what the two DuckDBs hold (see
	// balance).
	bgOver atomic.Int64
}

// OpenPartitionFiles is how many files of a partitioned write DuckDB keeps
// open, one a partition: a write into a table of more partitions than that,
// such as bucket(2048, column), makes small files again.
const OpenPartitionFiles = 1024

// Open starts DuckDB and, when a catalog is configured, attaches it. It keeps
// retrying the attach until ctx is done. Once set up, DuckDB is locked down
// so SQL cannot reach files or URLs outside the warehouse. First it deletes
// the files an earlier process left in the spill directory (see
// claimTempDir).
func Open(ctx context.Context, cfg config.Config, log *slog.Logger, opts ...Option) (*Engine, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	release := claimTempDir(cfg.DuckDB.TempDir, log)
	connector, err := duckdb.NewConnector(rbDSN(cfg.DuckDB.Path, rbToLoad(cfg)), nil)
	if err != nil {
		release()
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	p, err := newParser()
	if err != nil {
		err = errors.Join(err, connector.Close())
		release()
		return nil, err
	}

	e := &Engine{connector: connector, parser: p, releaseTempDir: release, log: log, cfg: cfg, isBackground: o.background,
		bgWake: make(chan struct{}, 1)}
	if err := e.setup(ctx, cfg, log, o); err != nil {
		p.close()
		err = errors.Join(err, connector.Close())
		if e.proxy != nil {
			err = errors.Join(err, e.proxy.Close())
		}
		release()
		return nil, err
	}
	if e.cache != nil && !e.isBackground {
		// The first DuckDB keeps the cache they share to its size.
		e.startCache(log)
	}

	return e, nil
}

// CountStatements returns how many statements query holds, without running
// any of them.
func (e *Engine) CountStatements(query string) (int, error) {
	return e.parser.count(query)
}

// Connect opens a connection for one client session.
func (e *Engine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	conn := c.(*duckdb.Conn)
	if err := e.UseDefaultSchema(ctx, conn); err != nil {
		return nil, errors.Join(err, conn.Close())
	}

	return conn, nil
}

// UseDefaultSchema puts conn back on the schema and search path a new
// connection starts with: the catalog's default schema, then its views'
// schema in the local database. DuckDB's own default, which its RESET of
// the search path restores, is its own database with an empty path.
func (e *Engine) UseDefaultSchema(ctx context.Context, conn *duckdb.Conn) error {
	for _, stmt := range e.connectStmts {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return fmt.Errorf("select default schema: %w", err)
		}
	}
	return nil
}

// sqlConn opens a database/sql connection to DuckDB, which duckdb-go
// registers table functions through, and returns the func that closes it
// and its pool. The functions stay: they belong to the database, not the
// connection. Left open, the pool's idle connection would keep the database
// open after Close, and DuckDB deletes its spill files only as the database
// closes.
func (e *Engine) sqlConn(ctx context.Context) (*sql.Conn, func(), error) {
	db := sql.OpenDB(keepOpen{e.connector})
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, errors.Join(err, db.Close())
	}
	return conn, func() { _, _ = conn.Close(), db.Close() }, nil
}

// RegisterFunctions runs register on a connection of the engine's
// database, to register functions every session's statements may call,
// as DuckDB registers them for the database.
func (e *Engine) RegisterFunctions(ctx context.Context, register func(*sql.Conn) error) error {
	conn, release, err := e.sqlConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	return register(conn)
}

// keepOpen hides the engine's connector's Close from database/sql, which
// closes the connector a pool was opened on along with the pool.
type keepOpen struct{ driver.Connector }

// Catalog is the Iceberg catalog's name in SQL, or "" without one.
func (e *Engine) Catalog() string { return e.catalog }

// LocalDatabase is the name of DuckDB's own database.
func (e *Engine) LocalDatabase() string { return e.local }

// CatalogLock keeps the embedded DuckDB from crashing when the attached
// Iceberg catalog's tables are walked and changed at the same time. It takes
// the lock exclusively; CatalogShare takes it shared.
//
// DuckDB-Iceberg keeps one in-memory set of the catalog's tables per schema
// of the attached catalog, shared by every session's connection, and three
// things reach it:
//
//   - Binding a statement looks each table it names up in the set and, under
//     DuckDB-Iceberg's lock on the set, replaces the table's entry with a
//     fresh one. DuckDB binds a statement that reads the catalog again each
//     time it runs, not only when it is prepared.
//   - A listing (duckdb_tables(), duckdb_views(), duckdb_columns(),
//     information_schema, pg_catalog, SHOW, DESCRIBE, a missing-table
//     suggestion) walks the set under that lock and reads the entries it
//     found after letting go of it; its transaction keeps them alive, but
//     only as the set had them at its latest walk, so when a bind replaces
//     an entry between two walks of one listing, the second walk frees what
//     the first one is still reading.
//   - A rename or drop finds the table's entry and, when it commits, erases
//     it from the set without that lock.
//
// So an erase alongside a bind or a walk, or a bind between a listing's
// walks, on this instance corrupts the set or frees memory still in use:
// the process segfaults, or a later listing loops forever, taking down every
// session on the replica. A dbt run swapping tables (CTAS, rename, drop)
// while a BI tool browses schemas or a dashboard refreshes hits this.
//
// So the steps that reach the set hold this lock while they run. DDL,
// listings, and preparing a statement (Igloo's checks list the catalog) hold
// it exclusively and run one at a time; binding a statement, which DuckDB
// does again as each run starts, holds it shared, alongside other binds,
// which DuckDB-Iceberg's own lock keeps apart. What does not reach the set —
// reading table data once bound, and INSERT/UPDATE/DELETE writing and
// committing a snapshot of the tables bound — holds nothing. The pgwire
// layer decides which steps take it and how (see Server.catalogRunLock,
// holdCatalog, shareCatalog, and the bind hold Server.openSession sets on
// each session's connection). Callers outside a session take it
// exclusively. DDL on another replica or engine reaches this replica only
// through the Iceberg REST catalog, not this set, so it cannot crash this
// instance and this lock does not cover it; a listing here may lag such a
// change by a sync.
//
// The catalog proxy holds every holder to this: while the lock is held
// shared, it refuses the requests DuckDB-Iceberg makes before it walks the
// set (listing a schema's tables) or erases from it (a drop or a rename),
// so DuckDB fails the statement instead (see catalogproxy's shared holds,
// and SharedRefusals). A step that walks or erases without the exclusive
// side fails rather than crash the instance, and a query prepared under
// the shared side is prepared again under the exclusive one, as a run is
// bound again under it (see pgwire's runBound). The proxy cannot serve the
// request, nor wait for the other binds to end: it cannot tell which
// session made it, and the session that did holds the shared side itself.
//
// Once it has the lock, it gives back the catalog's names to the tables
// that binds named in another case (see respell), before the holder lists
// anything.
//
// It returns the func that releases it, for use with defer:
//
//	defer e.CatalogLock()()
//
// It is not reentrant (sync.RWMutex): the pgwire layer makes it reentrant
// per session (see holdCatalog); a caller outside a session holds it at most
// once, and never takes it from inside a DuckDB call on a connection whose
// session holds it.
func (e *Engine) CatalogLock() func() {
	e.catMu.Lock()
	e.catLocked.Store(time.Now().UnixNano())
	e.respell()
	return func() {
		e.catLocked.Store(0)
		e.catMu.Unlock()
	}
}

// CatalogTryLock is CatalogLock for work that may as well not happen,
// such as maintenance's commit of a rewrite of a table's files: it takes
// the lock only when nobody holds it, trying for up to wait, without
// queuing for it as CatalogLock does, which would hold up every session's
// binds behind a long holder. ok is false, with nothing to release, when
// it did not get the lock in time or ctx ended.
func (e *Engine) CatalogTryLock(ctx context.Context, wait time.Duration) (release func(), ok bool) {
	deadline := time.Now().Add(wait)
	for !e.catMu.TryLock() {
		if time.Now().After(deadline) {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(5 * time.Millisecond):
		}
	}
	e.catLocked.Store(time.Now().UnixNano())
	e.respell()
	return func() {
		e.catLocked.Store(0)
		e.catMu.Unlock()
	}, true
}

// CatalogShare takes the catalog lock shared, for binding a statement
// against the catalog's in-memory set (see CatalogLock), and returns the
// func that releases it.
func (e *Engine) CatalogShare() func() {
	e.catMu.RLock()
	return e.shared()
}

// shared counts a holder of the shared side of the catalog lock, which the
// caller has taken, and returns the func that lets go of it.
func (e *Engine) shared() func() {
	e.catShared.Add(1)
	return func() {
		e.catShared.Add(-1)
		e.catMu.RUnlock()
	}
}

// SharedRefusals is how many of DuckDB's catalog requests the catalog proxy
// refused because the catalog lock was held shared (see CatalogLock).
func (e *Engine) SharedRefusals() uint64 { return e.proxy.Refusals() }

// CatalogShareContext is CatalogShare that stops waiting, and returns ctx's
// error, when ctx ends. A sync.RWMutex cannot stop waiting, so a goroutine
// waits in its place in the lock's queue, as RLock does, behind writers that
// came first and ahead of those that come later, and hands the lock over;
// if ctx ended first, it lets go of the lock as soon as it gets it.
func (e *Engine) CatalogShareContext(ctx context.Context) (func(), error) {
	if e.catMu.TryRLock() {
		return e.shared(), nil
	}
	got, gaveUp := make(chan struct{}), make(chan struct{})
	go func() {
		e.catMu.RLock()
		select {
		case got <- struct{}{}:
		case <-gaveUp:
			e.catMu.RUnlock()
		}
	}()
	select {
	case <-got:
		return e.shared(), nil
	case <-ctx.Done():
		close(gaveUp)
		return nil, ctx.Err()
	}
}

// CatalogLockedFor is how long the catalog lock has been held exclusively,
// or 0 if it is not.
func (e *Engine) CatalogLockedFor() time.Duration {
	if at := e.catLocked.Load(); at != 0 {
		return time.Since(time.Unix(0, at))
	}
	return 0
}

// CatalogChanged says the catalog changed other than through DuckDB (a view
// Igloo wrote, a clone, a table another engine created through Igloo's
// Iceberg REST endpoint), so that DuckDB's next listings of it ask the
// catalog again rather than take the catalog proxy's (see catalogproxy's
// listings).
func (e *Engine) CatalogChanged() { e.proxy.Invalidate() }

// TableSpelling returns how the catalog spells the name of its table name
// in namespace, as far as this instance knows; ok is false when it does
// not know the table.
func (e *Engine) TableSpelling(namespace []string, name string) (string, bool) {
	return e.proxy.Spelling(namespace, name)
}

// ListingsGeneration changes whenever what DuckDB's listings of the
// catalog list may have changed (see catalogproxy.Proxy.Generation).
func (e *Engine) ListingsGeneration() uint64 { return e.proxy.Generation() }

// DataVersion goes up whenever a table of the catalog may have changed (see
// catalogproxy.Proxy.DataVersion).
func (e *Engine) DataVersion() uint64 { return e.proxy.DataVersion() }

// TablesVersion changes whenever one of the tables loads loaded may have
// changed (see catalogproxy.Proxy.TablesVersion).
func (e *Engine) TablesVersion(loads []catalogproxy.Load) uint64 { return e.proxy.TablesVersion(loads) }

// RecordLoads records the catalog's tables DuckDB loads until the func it
// returns is called, which returns them (see catalogproxy's RecordLoads).
func (e *Engine) RecordLoads() func() []catalogproxy.Load { return e.proxy.RecordLoads() }

// DistinctCounts returns the distinct counts of the catalog's tables'
// columns, as the latest loads of the tables had them, by
// catalog.schema.table as DuckDB names it, lower-cased, and a generation
// that changes whenever they do (see catalogproxy's DistinctCounts).
func (e *Engine) DistinctCounts() (uint64, map[string]map[string]int64) {
	gen, tables := e.proxy.DistinctCounts()
	out := make(map[string]map[string]int64, len(tables))
	for k, v := range tables {
		out[strings.ToLower(e.catalog)+"."+k] = v
	}
	return gen, out
}

// ReloadTables loads the tables of loads again, so that DataVersion goes
// up if any changed since they were loaded (see catalogproxy's Reload).
func (e *Engine) ReloadTables(ctx context.Context, loads []catalogproxy.Load) error {
	return e.proxy.Reload(ctx, loads)
}

// ForgetMissingSchema makes DuckDB forget that schema, of the catalog, is
// missing, if the catalog has it, and reports whether it did: a statement
// that failed for want of the schema then binds when prepared again.
// DuckDB remembers a schema as missing once a lookup of a table in it found
// neither, until it stops, whoever creates the schema afterwards (see
// catalogproxy's namespaces.go).
func (e *Engine) ForgetMissingSchema(ctx context.Context, schema string) bool {
	if e.catalog == "" || schema == "" {
		return false
	}
	ns := strings.Split(schema, ".")
	if exists, ok := e.proxy.NamespaceExists(ctx, ns); !ok || !exists {
		return false
	}
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return false
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	exec := func(q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	// With the lock held no session binds, so the existence check the
	// proxy answers with 404 is the DROP's: DuckDB then forgets the
	// schema, and asks the catalog to drop nothing. Should it have found
	// the schema anyway, the drop it put off to the commit is rolled back.
	release := e.CatalogLock()
	defer release()
	defer e.proxy.HideNamespace(ns)()
	if err := exec("BEGIN"); err != nil {
		return false
	}
	err = exec("DROP SCHEMA IF EXISTS " + ident(e.catalog) + "." + ident(schema))
	if rerr := exec("ROLLBACK"); err == nil {
		err = rerr
	}
	return err == nil
}

// ForgetTable has DuckDB forget the table name of schema, of the catalog,
// which the catalog renamed to as though DuckDB heard the rename fail, so
// that DuckDB stops listing it: until stop is called, DuckDB's next DROP
// TABLE of name loads the table as as, and drops nothing in the catalog
// (see catalogproxy's Forget). The caller runs the drop holding the
// catalog lock exclusively.
func (e *Engine) ForgetTable(schema, name, as string) (stop func()) {
	return e.proxy.Forget(strings.Split(schema, "."), name, as)
}

// LoadSchemas makes the catalog views (information_schema, pg_catalog,
// duckdb_columns) show the real columns of every Iceberg table. DuckDB loads
// an Iceberg table's schema the first time the table is used and until then
// lists a placeholder column named "__"; tools that browse schemas would
// see only that. Loaded schemas are shared by all sessions, so this costs
// one catalog request per table not yet used.
func (e *Engine) LoadSchemas(ctx context.Context, conn *duckdb.Conn) error {
	if e.catalog == "" {
		return nil
	}
	tables, err := e.placeholders(ctx, conn)
	if err != nil {
		return err
	}
	return e.loadSchemas(ctx, conn, tables, nil)
}

// WarmSchemas loads, in the background of a replica that has just started,
// the schemas LoadSchemas would load for the first statement that lists
// columns, so that statement does not wait for them all. It takes the
// catalog lock as statements do: exclusively to list what is still to
// load, and shared for each table it loads, one at a time, so DDL and
// listings wait for no more than one table.
func (e *Engine) WarmSchemas(ctx context.Context) error {
	if e.catalog == "" {
		return nil
	}
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	release := e.CatalogLock()
	tables, err := e.placeholders(ctx, conn)
	release()
	if err != nil {
		return err
	}
	return e.loadSchemas(ctx, conn, tables, e.CatalogShare)
}

// placeholders lists the Iceberg tables whose schemas DuckDB has not loaded
// yet, as schema and name.
func (e *Engine) placeholders(ctx context.Context, conn *duckdb.Conn) ([][2]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT schema_name, table_name FROM duckdb_columns()
		WHERE database_name = $1 AND column_name = '__' AND data_type = 'UNKNOWN'`,
		[]driver.NamedValue{{Ordinal: 1, Value: e.catalog}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var tables [][2]string
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			return tables, nil
		}
		tables = append(tables, [2]string{dest[0].(string), dest[1].(string)})
	}
}

// loadSchemas loads tables' schemas. hold, if set, is taken around each
// load (see WarmSchemas); otherwise the caller keeps the rest of Igloo off
// the catalog meanwhile (see CatalogLock).
func (e *Engine) loadSchemas(ctx context.Context, conn *duckdb.Conn, tables [][2]string, hold func() func()) error {
	if len(tables) == 0 {
		return nil
	}
	// Each load waits on the catalog and on object storage, so they run
	// side by side, on conn and connections of their own: a catalog of
	// hundreds of tables took seconds one at a time. Loading binds the
	// table, which DuckDB-Iceberg's own lock keeps safe alongside other
	// binds.
	jobs := make(chan [2]string)
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	load := func(c *duckdb.Conn) {
		for t := range jobs {
			if ctx.Err() != nil {
				continue
			}
			stmt := "DESCRIBE " + ident(e.catalog) + "." + ident(t[0]) + "." + ident(t[1])
			release := func() {}
			if hold != nil {
				release = hold()
			}
			_, err := c.ExecContext(ctx, stmt, nil)
			release()
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("load schema of %s.%s: %w", t[0], t[1], err))
				mu.Unlock()
			}
		}
	}
	wg.Go(func() { load(conn) })
	for range min(len(tables), schemaLoaders) - 1 {
		c, err := e.connector.Connect(ctx)
		if err != nil {
			break
		}
		wg.Go(func() {
			defer c.Close() //nolint:errcheck
			load(c.(*duckdb.Conn))
		})
	}
	for _, t := range tables {
		jobs <- t
	}
	close(jobs)
	wg.Wait()
	return errors.Join(errs...)
}

// schemaLoaders bounds how many tables' schemas LoadSchemas loads at once.
const schemaLoaders = 16

// Ping checks that DuckDB can run a query.
func (e *Engine) Ping(ctx context.Context) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	_, err = c.(*duckdb.Conn).ExecContext(ctx, "SELECT 1", nil)
	return err
}

// SetCatalogGuard has g check every change DuckDB asks of the catalog
// from now on (see catalogproxy.Guard). Without a catalog it does
// nothing.
func (e *Engine) SetCatalogGuard(g catalogproxy.Guard) {
	if e.proxy != nil {
		e.proxy.SetGuard(g)
	}
}

// HoldCommit holds schema.name's next commit from the catalog, and has DuckDB
// write its files under dir (see catalogproxy.CommitHold). It returns nil if
// another hold has the table, or there is no catalog.
func (e *Engine) HoldCommit(schema, name, dir string) *catalogproxy.CommitHold {
	if e.proxy == nil {
		return nil
	}
	return e.proxy.HoldCommit([]string{schema}, name, dir)
}

// Close shuts DuckDB down. Sessions must be closed first: DuckDB's database
// closes, deleting its spill files, as the last connection to it closes.
func (e *Engine) Close() error {
	var err error
	e.bgMu.Lock()
	if e.bg != nil {
		close(e.bgStop)
		err = e.bg.Close()
		e.bg = nil
	}
	e.bgMu.Unlock()
	if e.prefetcher != nil {
		e.prefetcher.Close()
	}
	e.stopCache()
	e.parser.close()
	err = errors.Join(err, e.connector.Close())
	if e.proxy != nil {
		err = errors.Join(err, e.proxy.Close())
	}
	if e.releaseTempDir != nil {
		e.releaseTempDir()
	}
	return err
}

func (e *Engine) setup(ctx context.Context, cfg config.Config, log *slog.Logger, o options) error {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer c.Close() //nolint:errcheck

	conn := c.(*duckdb.Conn)
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}

	query := func(q string) (string, error) { return queryString(ctx, conn, q) }
	if err := configure(cfg, serverThreadsPerCore, exec, query); err != nil {
		return err
	}
	if e.bitmaps, err = loadRB(cfg, log, exec, query); err != nil {
		return err
	}
	if e.local, err = queryString(ctx, conn, "SELECT current_database()"); err != nil {
		return fmt.Errorf("read database name: %w", err)
	}
	e.locations = loadLocations(cfg)
	// Functions over Igloo's own state, which the background DuckDB does
	// not hold: a statement that calls one fails to bind there, and runs in
	// the first (see background.go).
	if !o.background {
		if err := e.registerCatalog(ctx); err != nil {
			return fmt.Errorf("register the catalog functions: %w", err)
		}
		if err := e.registerHistory(ctx); err != nil {
			return fmt.Errorf("register query history: %w", err)
		}
		if err := e.registerJobHistory(ctx); err != nil {
			return fmt.Errorf("register task and load history: %w", err)
		}
		if err := e.registerAlertHistory(ctx); err != nil {
			return fmt.Errorf("register alert history: %w", err)
		}
		if err := e.registerPolicies(ctx); err != nil {
			return fmt.Errorf("register policy references: %w", err)
		}
		if err := e.registerErasure(ctx); err != nil {
			return fmt.Errorf("register erasure requests: %w", err)
		}
		if err := e.registerAI(ctx); err != nil {
			return fmt.Errorf("register the AI functions: %w", err)
		}
		if err := e.setupModels(ctx, cfg, true); err != nil {
			return fmt.Errorf("register model functions: %w", err)
		}
		if err := e.setupForecasting(ctx, cfg); err != nil {
			return fmt.Errorf("register forecasting functions: %w", err)
		}
	}

	if cfg.Catalog.URI == "" {
		log.Warn("IGLOO_CATALOG_URI is not set; tables will not persist")
		// Load the image's extensions while files can still be read.
		if cfg.DuckDB.ExtensionDir != "" {
			for _, stmt := range extensionStatements() {
				if err := exec(stmt); err != nil {
					return fmt.Errorf("load extensions: %w", err)
				}
			}
		}
		if err := beforeLockdown(ctx, conn, o); err != nil {
			return err
		}
		return e.lockDown(exec, cfg, o.sandboxDirs())
	}

	useAzureIdentity(cfg.Storage, log)
	for _, stmt := range storageStatements(cfg.Storage) {
		if err := exec(stmt); err != nil {
			// Never echo the statement: it can contain a secret key.
			return fmt.Errorf("configure object storage: %w", err)
		}
	}
	if err := e.setupCache(exec, cfg, log); err != nil {
		return err
	}

	// DuckDB reaches the catalog through a proxy that applies each commit
	// once, whatever the catalog answers (see package catalogproxy), and
	// deletes the files of commits the catalog refused.
	proxyOpts := catalogproxy.Options{
		Catalog: cfg.Catalog.URI,
		Files: func(ctx context.Context) (iceio.IO, error) {
			return catalog.FS(ctx, cfg.Storage, cfg.Storage.Location)
		},
		Log: log,
	}
	for _, f := range o.catalogProxy {
		f(&proxyOpts)
	}
	if e.proxy, err = catalogproxy.New(proxyOpts); err != nil {
		return err
	}
	e.proxy.SetSharedHold(func() bool { return e.catShared.Load() > 0 && e.catLocked.Load() == 0 })
	// The proxy gives tables Igloo creates a target file size, which
	// DuckDB refuses to write a partitioned table with unless told to
	// ignore it there, as it does when a table has none.
	if err := exec("SET GLOBAL ignore_target_file_size_for_partitioned_tables = true"); err != nil {
		return fmt.Errorf("configure iceberg writes: %w", err)
	}
	// DuckDB keeps 100 of a partitioned write's files open, and past that
	// closes them and starts new ones: a CREATE TABLE ... AS of a table
	// partitioned by bucket(256, column) wrote thousands of files of a few
	// thousand rows, each a request to object storage, at a fifth of a
	// core for 35 minutes, as the shuffle files of distributed execution
	// did (see distributed's worker). With every partition's file open, a
	// write makes a file a partition, whose row groups maintenance sizes
	// for that many (see maintenance's sizeRowGroups).
	if err := exec(fmt.Sprintf("SET GLOBAL partitioned_write_max_open_files = %d", OpenPartitionFiles)); err != nil {
		return fmt.Errorf("configure iceberg writes: %w", err)
	}
	attach := attachStatement(cfg.Catalog, e.proxy.URL())
	for {
		err := exec(attach)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return fmt.Errorf("attach catalog %s: %w", cfg.Catalog.URI, err)
		}
		log.Warn("catalog not reachable yet, retrying", "uri", cfg.Catalog.URI, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(attachRetryInterval):
		}
	}

	// Replicas starting together race to create the schema; the catalog
	// rejects all but one, and a second attempt then finds it exists.
	schema := ident(cfg.Catalog.Name) + "." + ident(cfg.Catalog.Schema)
	for attempt := 1; ; attempt++ {
		err := exec("CREATE SCHEMA IF NOT EXISTS " + schema)
		if err == nil {
			break
		}
		if attempt == 3 || ctx.Err() != nil {
			return fmt.Errorf("create default schema %s: %w", schema, err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	views := ident(e.local) + "." + ident(cfg.Catalog.Schema)
	if err := exec("CREATE SCHEMA IF NOT EXISTS " + views); err != nil {
		return fmt.Errorf("create schema %s for views: %w", views, err)
	}
	e.connectStmts = []string{
		"USE " + schema,
		"SET SESSION search_path = " + literal(schema+","+views),
	}
	e.catalog = cfg.Catalog.Name

	if err := beforeLockdown(ctx, conn, o); err != nil {
		return err
	}
	if err := e.lockDown(exec, cfg, o.sandboxDirs()); err != nil {
		return err
	}

	log.Info("attached catalog", "uri", cfg.Catalog.URI, "warehouse", cfg.Catalog.Warehouse,
		"location", cfg.Storage.Location, "as", cfg.Catalog.Name)
	return nil
}

// queryString runs a query that returns one string.
func queryString(ctx context.Context, conn *duckdb.Conn, query string) (string, error) {
	rows, err := conn.QueryContext(ctx, query, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	v, _ := dest[0].(string)
	return v, nil
}

func (e *Engine) lockDown(exec func(string) error, cfg config.Config, extra []string) error {
	// DuckDB sets up its secret manager on first use and cannot once
	// external access is off, so make sure it is set up now.
	if err := exec("SELECT count(*) FROM duckdb_secrets()"); err != nil {
		return fmt.Errorf("initialize secrets: %w", err)
	}
	cacheDir := ""
	if e.cache != nil {
		cacheDir = e.cache.dir
	}
	for _, stmt := range lockdownStatements(cfg, cacheDir, extra...) {
		if err := exec(stmt); err != nil {
			return fmt.Errorf("lock down duckdb (%s): %w", stmt, err)
		}
	}
	return nil
}

// Threads per core when the warehouse is in object storage and
// IGLOO_DUCKDB_THREADS is unset. Reading object storage cold waits on
// requests, not on cores: a thread reads a row group at a time, and row
// groups are often small (DuckDB writes 122,880 rows to one), so at one
// thread a core the cores sit idle. On a machine of 8 cores, a scan of
// 23 GB cold read 64 MB/s on 8 threads, 100 on 16, 172 on 32 and 265 on
// 64; a report spent 66 s on 8 threads, 33 on 16 and 30 on 32, and 12 s
// warm on all three. Workers read cold more than the server, which keeps
// a warm cache and needs room for the memory more threads take.
const (
	serverThreadsPerCore = 2
	workerThreadsPerCore = 4
)

// minThreadMemory is the DuckDB memory each thread past one a core must
// have, or there are fewer of them. Every thread keeps its own hash tables
// and buffers, so threads beyond the cores split the memory limit into
// shares too small for aggregates and joins, which then spill. On a machine
// of 8 cores with a memory limit of 18.6 GiB, two reports of
// COUNT(DISTINCT)s over 93 million unnested rows took 120-139 s on 16
// threads (1.2 GiB each), spilling 4.2 GiB, 48 s on 8 (2.3 GiB each) and
// 46-51 s on 9 (2.1 GiB each), while five others took as long on 9 as on 16.
// Fewer threads also keep fewer reads of object storage in flight, which
// slows cold scans (see serverThreadsPerCore).
const minThreadMemory = 2 << 30

// leastThreadMemory is the DuckDB memory every thread must have, or there are
// fewer of them, whatever the cores: DuckDB's own minimum of 125 MB a thread. A pod
// is not given fewer cores for having little memory. In a container of 1 GiB on a
// machine of 16 cores, under a limit of 512 MiB, a grouping that spills 40 million
// groups held 700 to 780 MiB of anonymous memory on 8 and 16 threads and took 9 to 11
// s; on 4 it held 570 MiB and took 1.4 s. Past that, its pod's working set went past
// what the memory watch leaves, which canceled it in about one run in three.
const leastThreadMemory = 125 << 20

// threadsFor returns DuckDB's threads for cfg: IGLOO_DUCKDB_THREADS if set;
// when the warehouse is in object storage, perCore threads a core, but no
// more than leave each minThreadMemory of memory, DuckDB's memory limit in
// bytes (unknown if 0), and otherwise 0, DuckDB's own default of one a core;
// and never so many that one has less than leastThreadMemory.
func threadsFor(cfg config.Config, perCore int, memory int64) int {
	if cfg.DuckDB.Threads > 0 {
		return cfg.DuckDB.Threads
	}
	// The cores this process may use: a container's CPU limit, if it has one.
	cores := runtime.GOMAXPROCS(0)
	n := 0
	switch cfg.Storage.Type {
	case config.StorageS3, config.StorageGCS, config.StorageAzure:
		n = perCore * cores
		if memory > 0 {
			n = min(n, max(cores, int(memory/minThreadMemory)))
		}
	}
	if memory <= 0 {
		return n
	}
	most := max(int(memory/leastThreadMemory), 1)
	switch {
	case n > most:
		return most
	case n == 0 && cores > most:
		return most
	}
	return n
}

// processMemory is the memory the process may use (memlimit.Total), zero
// where it cannot be read. A variable, for tests.
var processMemory = func() int64 {
	total, _ := memlimit.Total()
	return total
}

// configure applies cfg's DuckDB settings with exec, its memory limit
// memlimit.DuckDBLimit of the process's memory where cfg sets none, then
// its threads (see threadsFor), which depend on the memory limit those
// settings leave, as query reads it.
func configure(cfg config.Config, perCore int, exec func(string) error, query func(string) (string, error)) error {
	d := cfg.DuckDB
	d.Threads = 0
	if d.MemoryLimit == "" {
		// DuckDB's own default, 80% of the machine's memory, leaves too
		// little for what it holds past its limit as it spills, beside
		// the rest of the process and the memory watch's margins.
		if n := memlimit.DuckDBLimit(processMemory()); n > 0 {
			d.MemoryLimit = fmt.Sprintf("%dMiB", n>>20)
		}
	}
	for _, stmt := range settingStatements(d) {
		if err := exec(stmt); err != nil {
			return fmt.Errorf("configure duckdb (%s): %w", stmt, err)
		}
	}
	var memory int64
	if v, err := query("SELECT current_setting('memory_limit')::VARCHAR"); err == nil {
		memory, _ = config.ParseSize(v)
	}
	if n := threadsFor(cfg, perCore, memory); n > 0 {
		stmt := fmt.Sprintf("SET threads = %d", n)
		if err := exec(stmt); err != nil {
			return fmt.Errorf("configure duckdb (%s): %w", stmt, err)
		}
	}
	return nil
}

func settingStatements(d config.DuckDB) []string {
	var stmts []string
	if d.ExtensionDir != "" {
		// Use only the extensions baked into the image; never fetch code at
		// runtime.
		stmts = append(stmts,
			"SET extension_directory = "+literal(d.ExtensionDir),
			"SET autoinstall_known_extensions = false",
		)
	}
	if d.TempDir != "" {
		stmts = append(stmts, "SET temp_directory = "+literal(d.TempDir))
	}
	if d.MemoryLimit != "" {
		stmts = append(stmts, "SET memory_limit = "+literal(d.MemoryLimit))
	}
	if d.MaxTempSize != "" {
		stmts = append(stmts, "SET max_temp_directory_size = "+literal(d.MaxTempSize))
	}
	if d.Threads > 0 {
		stmts = append(stmts, fmt.Sprintf("SET threads = %d", d.Threads))
	}
	// Table files never change once written, so their footers can stay parsed.
	// A dashboard's statements read the same files again and again: 96 of them
	// at ten times the data took 214 s rather than 233, the slowest tenth 107 s
	// rather than 132. DuckDB evicts them when memory is short.
	return append(stmts, "SET parquet_metadata_cache = true")
}

// storageStatements loads every extension Igloo needs and registers the
// credentials DuckDB uses to read and write table files. Extensions must be
// loaded here: after lockdown DuckDB cannot read extension files.
func storageStatements(s config.Storage) []string {
	stmts := extensionStatements()
	scope := storageScope(s)

	switch s.Type {
	case config.StorageGCS:
		stmts = append(stmts, secret(scope,
			"TYPE gcs",
			"KEY_ID "+literal(s.GCS.HMACKeyID),
			"SECRET "+literal(s.GCS.HMACSecret),
		))

	case config.StorageAzure:
		stmts = append(stmts, "LOAD azure")
		if s.Azure.AccountKey != "" {
			conn := fmt.Sprintf("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=%s",
				s.Azure.Account, s.Azure.AccountKey, strings.TrimPrefix(s.Azure.Host, s.Azure.Account+".dfs."))
			stmts = append(stmts, secret(scope, "TYPE azure", "CONNECTION_STRING "+literal(conn)))
		} else {
			stmts = append(stmts, secret(scope,
				"TYPE azure",
				"PROVIDER credential_chain",
				"CHAIN "+literal(azureChain()),
				"ACCOUNT_NAME "+literal(s.Azure.Account),
			))
		}

	default:
		opts := []string{
			"TYPE s3",
			"REGION " + literal(s.S3.Region),
			"URL_STYLE " + literal(s.S3.URLStyle),
			fmt.Sprintf("USE_SSL %t", s.S3.UseSSL),
		}
		if s.S3.Endpoint != "" {
			opts = append(opts, "ENDPOINT "+literal(s.S3.Endpoint))
		}
		if s.S3.AccessKeyID != "" {
			opts = append(opts, "KEY_ID "+literal(s.S3.AccessKeyID), "SECRET "+literal(s.S3.SecretAccessKey))
		} else {
			// IAM roles for service accounts, instance profiles, env vars.
			// Those credentials expire (after an hour for IRSA), so DuckDB
			// must fetch new ones as they do.
			stmts = append(stmts, "LOAD aws")
			opts = append(opts, "PROVIDER credential_chain", "REFRESH auto")
		}
		stmts = append(stmts, secret(scope, opts...))
	}

	return stmts
}

// useAzureIdentity has DuckDB ask this process for an Azure identity's tokens, where it
// uses an identity (see package azurecreds).
func useAzureIdentity(s config.Storage, log *slog.Logger) {
	if s.Type != config.StorageAzure || s.Azure.AccountKey != "" {
		return
	}
	if _, err := azurecreds.Shared(log); err != nil {
		log.Warn("no Azure identity for DuckDB's tokens; it asks for them itself", "err", err)
	}
}

// azureChain is the credentials of DuckDB's secret for an Azure warehouse with no account
// key: its managed identity credential asks this process if it serves them, and
// otherwise it has what the identity's own services give.
func azureChain() string {
	if azurecreds.Serving() {
		return "managed_identity"
	}
	return "workload_identity;managed_identity;env"
}

// extensionStatements load the extensions every deployment uses.
func extensionStatements() []string {
	return append([]string{"LOAD httpfs", "LOAD iceberg", "LOAD avro", "LOAD tpch"}, mediaStatements()...)
}

// secret registers Igloo's storage credentials, for the paths of scope.
func secret(scope string, opts ...string) string {
	if scope != "" {
		opts = append(opts, scope)
	}
	return "CREATE OR REPLACE SECRET igloo_storage (" + strings.Join(opts, ", ") + ")"
}

// storageScope limits Igloo's storage credentials to the warehouse and the
// allowed locations, the object storage Igloo reads and writes for itself.
// DuckDB then uses them for no other path: the tables of Iceberg REST
// catalogs attached as connections (package federation) are read with the
// credentials their catalogs hand out, or none, wherever their catalogs say
// their files are, never with Igloo's.
func storageScope(s config.Storage) string {
	var scopes []string
	if s.Location != "" {
		scopes = append(scopes, s.Root())
	}
	for _, loc := range s.AllowedLocations {
		scopes = append(scopes, strings.TrimSuffix(loc, "/")+"/")
	}
	if len(scopes) == 0 {
		return ""
	}
	return "SCOPE " + list(scopes)
}

// attachStatement attaches the Iceberg REST catalog at endpoint, the
// catalog proxy. Igloo uses its own storage credentials rather than ones
// vended by the catalog, so it works with object stores that have no STS
// endpoint. DROP TABLE never asks the catalog to purge the table's files
// (DuckDB's default for REST catalogs other than AWS's, made explicit):
// the catalog would delete everything under the table's location, files
// that clones of it share included. DuckDB never deletes the files it
// wrote for a commit that failed (REMOVE_FILES_ON_DELETE false): when the
// catalog does not confirm a commit's outcome, the commit may have landed
// and its files be the table's. The proxy deletes those of commits the
// catalog refused, and the orphan cleanup those of statements that failed
// or were rolled back before their commit.
func attachStatement(c config.Catalog, endpoint string) string {
	return fmt.Sprintf(
		"ATTACH %s AS %s (TYPE iceberg, ENDPOINT %s, AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none', PURGE_REQUESTED false, REMOVE_FILES_ON_DELETE false)",
		literal(c.Warehouse), ident(c.Name), literal(endpoint),
	)
}

// lockdownStatements stop SQL from reading or writing anything but the
// warehouse, the allowed locations, the spill directory, the read cache
// (cacheDir, if any) and extra, the locations of the tables of Iceberg REST
// catalogs attached as connections: no local files, no other buckets, no
// arbitrary URLs.
// DuckDB refuses to loosen these settings once external access is off, so
// they hold for every session. They apply without a catalog too, so a
// development server is no more open than a production one.
//
// The cache extension checks the same list before it touches its files, so
// the cache works only if its directory is on it. That gives SQL no more
// than it has: the cache holds copies of warehouse files, which SQL can
// read anyway, and a writer able to plant a block there (with COPY TO) can
// already overwrite the warehouse file itself.
func lockdownStatements(cfg config.Config, cacheDir string, extra ...string) []string {
	var dirs []string
	// Every entry ends in a slash so a directory is a strict prefix: land/
	// does not match landing-private/. The config validates and normalises
	// the warehouse and allowed locations the same way (see safeLocation).
	add := func(dir string) {
		if dir != "" {
			dirs = append(dirs, strings.TrimSuffix(dir, "/")+"/")
		}
	}
	if cfg.Storage.Location != "" {
		add(cfg.Storage.Root())
	}
	for _, loc := range cfg.Storage.AllowedLocations {
		add(loc)
	}
	add(cfg.DuckDB.TempDir)
	add(cacheDir)
	for _, dir := range extra {
		add(dir)
	}

	return []string{
		"SET allowed_directories = " + list(dirs),
		// CREATE TABLE AS on an Iceberg catalog makes sure a relative
		// directory named "data" exists before writing to the table's real
		// location in object storage. Allowing that exact path keeps CTAS
		// working; the image ships it as an empty directory the server
		// cannot write to.
		"SET allowed_paths = ['data']",
		"SET enable_external_access = false",
	}
}

// InstallExtensions downloads Extensions for this DuckDB version and platform
// into dir. Container builds run it so the image works offline.
func InstallExtensions(ctx context.Context, dir string) error {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return err
	}
	defer connector.Close() //nolint:errcheck

	c, err := connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)

	stmts := []string{"SET extension_directory = " + literal(dir)}
	for _, ext := range Extensions {
		stmt := "INSTALL " + ext
		if communityExtensions[ext] {
			stmt += " FROM community"
		}
		stmts = append(stmts, stmt)
	}
	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func list(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = literal(item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func ident(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
