// Package ddl carries out the DDL that DuckDB cannot run against the Iceberg
// catalog: views, and comments on schemas, tables, views and columns. It
// also keeps what the catalog's metadata says about every object, for the
// catalog functions (see comments.go).
//
// Both are stored in the catalog the standard Iceberg way, so they are
// shared by every replica and outlive restarts: a view is an Iceberg view
// with DuckDB SQL, a comment is a table's or view's "comment" property or a
// column's doc string. DuckDB reads neither, so each replica keeps a copy of
// every view in DuckDB's own database (memory.public.v for lake.public.v)
// and serves comments through igloo_comments().
package ddl

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/iddomain"
)

const (
	// syncInterval is how often a replica picks up views that other
	// replicas created, replaced or dropped.
	syncInterval = 30 * time.Second
	// metadataTTL is how old the comments, columns and table sizes that
	// the catalog functions list may be.
	metadataTTL = 30 * time.Second
	// changedDelay is how long after a write its table's metadata is read
	// again; writes in between are read with it.
	changedDelay = 2 * time.Second
)

var (
	// ErrExists is returned for a view or table that already exists.
	ErrExists = errors.New("already exists")
	// ErrNotFound is returned for a view, table or column that does not
	// exist.
	ErrNotFound = errors.New("does not exist")
)

type key struct{ schema, name string }

func keyOf(schema, name string) key {
	return key{strings.ToLower(schema), strings.ToLower(name)}
}

// Manager keeps views and comments.
type Manager struct {
	cat *catalog.Client
	eng *engine.Engine
	log *slog.Logger

	// mu serializes view changes and syncs applying what they listed, and
	// guards the fields below up to known.
	mu sync.Mutex
	// conn runs syncs.
	conn  *duckdb.Conn
	views map[key]catalog.View
	// edits counts this replica's changes to views. edited holds, for each
	// view changed since the last sync applied began listing, edits as of
	// its last change (see sync).
	edits  uint64
	edited map[key]uint64
	// syncs counts the syncs begun, and applied is the number of the last
	// begun of those that applied what they listed.
	syncs, applied uint64
	// failed are the catalog's views whose local copies could not be made,
	// with why, an *unloadedError: each has a stub that no statement can read
	// (see stubLocal), and every sync tries it again.
	failed map[key]error
	// known is the set of view keys, read without locking by IsView.
	known atomic.Pointer[map[key]bool]
	// unloaded holds failed's errors by the name of the relation each stub
	// reads, read without locking by Explain.
	unloaded atomic.Pointer[map[string]error]
	// gen counts changes to the local copies; see Generation.
	gen atomic.Uint64

	// meta is the catalog's metadata as last read. readMu serializes
	// reading all of it; metaMu guards the fields below and changes to
	// meta.
	meta        atomic.Pointer[catalog.Metadata]
	readMu      sync.Mutex
	metaMu      sync.Mutex
	metaAt      time.Time
	metaReading bool
	// touched are the objects read again on their own, and when.
	touched map[key]time.Time
	// changed are the tables to read again after changedDelay.
	changed map[key][2]string
}

// New makes a manager for the catalog cat that eng is attached to, and
// loads the catalog's views.
func New(ctx context.Context, cat *catalog.Client, eng *engine.Engine, log *slog.Logger) (*Manager, error) {
	conn, err := eng.Connect(ctx)
	if err != nil {
		return nil, err
	}
	m := &Manager{cat: cat, eng: eng, conn: conn, views: map[key]catalog.View{}, edited: map[key]uint64{},
		failed: map[key]error{}, log: log.With("component", "ddl"), touched: map[key]time.Time{}}
	m.known.Store(&map[key]bool{})
	m.unloaded.Store(&map[string]error{})
	// Loading the views reads and fills in the catalog's set of tables; hold
	// the exclusive side (see Run). Nothing else runs yet, but this keeps the
	// rule that every sync outside a statement holds the lock.
	release := eng.CatalogLock()
	_, err = m.SyncViews(ctx)
	release()
	if err != nil {
		m.log.Warn("could not load views", "err", err)
	}
	return m, nil
}

// Close releases the manager's connection.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn.Close()
}

// Run polls the catalog's views (see PollViews) every syncInterval until
// ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Polling lists the catalog's views and binds their bodies, which
			// reads and fills in its in-memory set of tables; hold the
			// exclusive side so nothing else reaches the set on this instance
			// while it does (see engine.Engine.CatalogLock). A statement's own
			// SyncViews takes this lock at the pgwire layer, so sync itself
			// does not.
			release := m.eng.CatalogLock()
			_, err := m.PollViews(ctx)
			release()
			if err != nil && ctx.Err() == nil {
				m.log.Warn("could not sync views", "err", err)
			}
		}
	}
}

// Catalog is the Iceberg catalog's name in SQL.
func (m *Manager) Catalog() string { return m.eng.Catalog() }

// IsView reports whether schema.name is a view in the catalog.
func (m *Manager) IsView(schema, name string) bool {
	return (*m.known.Load())[keyOf(schema, name)]
}

// Views returns the catalog's views in schema, as of the last sync, by
// name.
func (m *Manager) Views(schema string) []catalog.View {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []catalog.View
	for _, v := range m.views {
		if strings.EqualFold(v.Schema, schema) {
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b catalog.View) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// Generation changes whenever a local copy of a view may have changed, so
// callers can tell that statements reading views may now read other
// tables.
func (m *Manager) Generation() uint64 { return m.gen.Load() }

// SyncViews makes the local copies match the catalog's views, and makes
// again any that have gone missing, say because an admin dropped one.
// changed says whether anything changed. Statements that name a relation
// DuckDB does not have sync so before they fail.
func (m *Manager) SyncViews(ctx context.Context) (changed bool, err error) {
	return m.sync(ctx, true)
}

// PollViews is SyncViews except that it takes the local copies this
// replica made to be there still. Finding those that are not lists the
// views of every database DuckDB has attached, and DuckDB-Iceberg then
// lists the tables of each of the catalog's schemas: a session that
// renames or drops one of them at the same time, as dbt does all through a
// run, can leave DuckDB listing a table the catalog no longer has, or crash
// it.
func (m *Manager) PollViews(ctx context.Context) (changed bool, err error) {
	return m.sync(ctx, false)
}

// sync is SyncViews, or PollViews without checkLocal.
//
// Listing the catalog's views takes a while, and is done without holding
// up view changes or other syncs, so the list can be older than what the
// local copies already show. Applied, it would undo that: a session would
// not find the view it had just created, would find one it had just
// dropped or renamed, or would read an earlier definition. So the views
// this replica changed after listing began are left as those changes made
// them, which is as new as the catalog has them or newer, for the next
// sync to look at again. And a list is not applied at all once a sync that
// began later has applied its own.
func (m *Manager) sync(ctx context.Context, checkLocal bool) (changed bool, err error) {
	m.mu.Lock()
	m.syncs++
	this, since := m.syncs, m.edits
	m.mu.Unlock()
	defs, err := m.cat.DuckDBViews(ctx)
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if this < m.applied {
		return false, nil
	}
	m.applied = this
	defer func() {
		if changed {
			m.gen.Add(1)
		}
	}()
	listed := func(k key) bool { return m.edited[k] <= since }
	for k, n := range m.edited {
		if n <= since {
			delete(m.edited, k)
		}
	}

	want := map[key]catalog.View{}
	for _, v := range defs {
		want[keyOf(v.Schema, v.Name)] = v
	}
	for k, v := range m.views {
		if _, ok := want[k]; !ok && listed(k) {
			if err := m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(v.Schema, v.Name)); err != nil {
				return changed, err
			}
			delete(m.views, k)
			changed = true
		}
	}
	for k := range m.failed {
		if _, ok := want[k]; !ok && listed(k) {
			if err := m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(k.schema, k.name)); err != nil {
				return changed, err
			}
			delete(m.failed, k)
			changed = true
		}
	}

	// Local copies can also go missing, say if an admin drops one; they are
	// recreated too.
	var present map[key]bool
	if checkLocal {
		if present, err = m.localViews(ctx); err != nil {
			return changed, err
		}
	}
	missing := func(k key) bool { return checkLocal && !present[k] }

	// A view can read other views, which must exist locally first. Create
	// what can be created, and repeat while that makes progress.
	pending := map[key]catalog.View{}
	for k, v := range want {
		if !listed(k) {
			continue
		}
		_, failed := m.failed[k]
		if cur, ok := m.views[k]; !ok || failed || missing(k) || cur.Version != v.Version || cur.SQL != v.SQL {
			pending[k] = v
		}
	}
	for len(pending) > 0 {
		var errs []error
		why := map[key]error{}
		progress := false
		for k, v := range pending {
			// Let names in v resolve against other pending views too.
			m.publish(pending)
			if err := m.createLocal(ctx, m.conn, v); err != nil {
				// Reading a view that is a stub itself, it says why that one
				// could not be made.
				err = m.Explain(err)
				errs = append(errs, fmt.Errorf("view %s.%s: %w", v.Schema, v.Name, err))
				why[k] = err
				continue
			}
			m.views[k] = v
			delete(m.failed, k)
			delete(pending, k)
			progress, changed = true, true
		}
		if !progress {
			for k, v := range pending {
				m.stubLocal(ctx, v, why[k])
				if _, ok := m.failed[k]; !ok {
					changed = true
				}
				m.failed[k] = &unloadedError{schema: v.Schema, name: v.Name, why: why[k]}
			}
			m.publish(nil)
			return changed, errors.Join(errs...)
		}
	}
	m.publish(nil)
	return changed, nil
}

// edit records that this replica is changing the views keys, whose local
// copies the change keeps up to date: syncs that are listing the
// catalog's views now leave them alone. m.mu must be held.
func (m *Manager) edit(keys ...key) {
	m.edits++
	for _, k := range keys {
		m.edited[k] = m.edits
	}
}

// publish updates the set IsView reads: the local views plus extra.
func (m *Manager) publish(extra map[key]catalog.View) {
	known := make(map[key]bool, len(m.views)+len(m.failed)+len(extra))
	for k := range m.views {
		known[k] = true
	}
	for k := range m.failed {
		known[k] = true
	}
	for k := range extra {
		known[k] = true
	}
	m.known.Store(&known)
	unloaded := make(map[string]error, len(m.failed))
	for k, why := range m.failed {
		unloaded[absentName(k)] = why
	}
	m.unloaded.Store(&unloaded)
}

// CreateView creates or, with replace, replaces the view schema.name as
// query, with the given column names if any. It runs query on conn, the
// creating session's connection, so names in it resolve as they do for that
// session.
func (m *Manager) CreateView(ctx context.Context, conn *duckdb.Conn, schema, name string, columns []string, query string, replace bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.gen.Add(1)
	k := keyOf(schema, name)
	m.edit(k)

	exists, err := m.cat.CheckTableExists(ctx, catalog.Ident(schema, name))
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("a table named %q %w", name, ErrExists)
	}
	if _, ok := m.views[k]; ok && !replace {
		return fmt.Errorf("view %q %w", name, ErrExists)
	}
	if query, err = m.qualifyTables(ctx, query); err != nil {
		return err
	}
	if query, err = m.qualifyOwnSchema(ctx, conn, schema, query); err != nil {
		return err
	}

	// Create the local copy first: DuckDB checks the SQL and works out the
	// columns, which the catalog records.
	local := m.localName(schema, name)
	if err := m.exec(ctx, conn, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(m.eng.LocalDatabase())+"."+quoteIdent(schema)); err != nil {
		return err
	}
	stmt := "CREATE OR REPLACE VIEW " + local
	if len(columns) > 0 {
		stmt += " (" + quoteList(columns) + ")"
	}
	body, err := hideIDs(ctx, conn, query)
	if err != nil {
		m.restore(ctx, k, schema, name)
		return err
	}
	if err := m.exec(ctx, conn, stmt+" AS "+m.rewrite(body)); err != nil {
		m.restore(ctx, k, schema, name)
		return err
	}
	cols, err := m.describe(ctx, conn, local)
	if err != nil {
		m.restore(ctx, k, schema, name)
		return err
	}

	err = m.cat.PutView(ctx, catalog.View{Schema: schema, Name: name, SQL: query, Columns: cols}, replace)
	if errors.Is(err, catalog.ErrViewExists) {
		err = fmt.Errorf("view %q %w", name, ErrExists)
	}
	if err != nil {
		m.restore(ctx, k, schema, name)
		return err
	}
	v, ok, err := m.cat.LoadDuckDBView(ctx, schema, name)
	if err != nil || !ok {
		// Created, but the version is unknown; the next sync records it.
		v = catalog.View{Schema: schema, Name: name, SQL: query, Columns: cols}
	}
	m.views[k] = v
	delete(m.failed, k)
	m.publish(nil)
	m.reloadTable(ctx, schema, name, true)
	return nil
}

// restore puts back the local copy of a view after a failed change: the
// previous definition if there was one, otherwise nothing.
func (m *Manager) restore(ctx context.Context, k key, schema, name string) {
	ctx = context.WithoutCancel(ctx)
	if old, ok := m.views[k]; ok {
		if err := m.createLocal(ctx, m.conn, old); err == nil {
			return
		}
	}
	_ = m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(schema, name))
}

// DropView drops the view schema.name.
func (m *Manager) DropView(ctx context.Context, schema, name string, ifExists bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.gen.Add(1)
	k := keyOf(schema, name)
	m.edit(k)

	err := m.cat.DropView(ctx, catalog.Ident(schema, name))
	_, local := m.views[k]
	if _, stub := m.failed[k]; stub {
		local = true
	}
	switch {
	case errors.Is(err, icecat.ErrNoSuchView):
		// Without a local copy of a catalog view, the local database's
		// object of the name, if any, is not the catalog's to drop.
		if !local {
			if ifExists {
				return nil
			}
			return fmt.Errorf("view %q %w", name, ErrNotFound)
		}
	case err != nil:
		return err
	}
	if err := m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(schema, name)); err != nil {
		return err
	}
	delete(m.views, k)
	delete(m.failed, k)
	m.publish(nil)
	m.reloadTable(ctx, schema, name, true)
	return nil
}

// TableExists asks the catalog itself, not DuckDB's copy of it, whether
// the table schema.name exists.
func (m *Manager) TableExists(ctx context.Context, schema, name string) (bool, error) {
	return m.cat.CheckTableExists(ctx, catalog.Ident(schema, name))
}

// RenameView renames the view schema.from to to, in the same schema. Views
// that read it by its old name fail until they are replaced, as in DuckDB.
func (m *Manager) RenameView(ctx context.Context, schema, from, to string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.gen.Add(1)
	m.edit(keyOf(schema, from), keyOf(schema, to))
	v, ok := m.views[keyOf(schema, from)]
	if !ok {
		return fmt.Errorf("view %q %w", from, ErrNotFound)
	}
	if _, taken := m.views[keyOf(schema, to)]; taken {
		return fmt.Errorf("view %q %w", to, ErrExists)
	}
	exists, err := m.cat.CheckTableExists(ctx, catalog.Ident(schema, to))
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("a table named %q %w", to, ErrExists)
	}

	err = m.cat.RenameView(ctx, schema, from, to)
	switch {
	case errors.Is(err, catalog.ErrViewExists):
		return fmt.Errorf("view %q %w", to, ErrExists)
	case errors.Is(err, icecat.ErrNoSuchView):
		return fmt.Errorf("view %q %w", from, ErrNotFound)
	case err != nil:
		return err
	}
	delete(m.views, keyOf(schema, from))
	v.Name = to
	m.views[keyOf(schema, to)] = v
	m.publish(nil)
	m.reloadTable(ctx, schema, from, true)
	m.reloadTable(ctx, schema, to, true)
	// The catalog has the new name; if the local copies cannot follow, the
	// next sync puts them right.
	err = m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(schema, from))
	if err == nil {
		err = m.createLocal(ctx, m.conn, v)
	}
	if err != nil {
		m.log.Warn("could not rename the local copy of a view", "view", schema+"."+to, "err", err)
	}
	return nil
}

// createLocal creates or replaces the local copy of v.
func (m *Manager) createLocal(ctx context.Context, conn *duckdb.Conn, v catalog.View) error {
	schema := quoteIdent(m.eng.LocalDatabase()) + "." + quoteIdent(v.Schema)
	if err := m.exec(ctx, conn, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		return err
	}
	// Views created before CreateView qualified their own schema's names,
	// or by another engine, may still name its tables bare.
	query := v.SQL
	if q, err := m.qualifyOwnSchema(ctx, conn, v.Schema, query); err == nil {
		query = q
	}
	body, err := hideIDs(ctx, conn, query)
	if err != nil {
		return err
	}
	body = m.rewrite(body)
	stmt := "CREATE OR REPLACE VIEW " + m.localName(v.Schema, v.Name)
	if len(v.Columns) > 0 {
		names := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			names[i] = c.Name
		}
		stmt += " (" + quoteList(m.visibleNames(ctx, conn, v, body, names)) + ")"
	}
	return m.exec(ctx, conn, stmt+" AS "+body)
}

// visibleNames returns the column names to give the local copy of v, whose
// body (as hideIDs leaves it) returns no columns of ids. Views created
// before hideIDs recorded columns counting them: a star over a member table
// included its columns of ids, so the recorded names include those columns'
// own names, or, under a column list, the list's names shifted onto them
// with the last column's own name after. Such names are put back as the
// view meant them: without the names of columns of ids, and cut to as many
// columns as the body returns. Names that already fit are returned as is.
func (m *Manager) visibleNames(ctx context.Context, conn *duckdb.Conn, v catalog.View, body string, names []string) []string {
	cols, err := m.describeQuery(ctx, conn, body)
	if err != nil || len(cols) == len(names) {
		return names
	}
	fixed := make([]string, 0, len(names))
	for _, n := range names {
		if !iddomain.IsHidden(n) {
			fixed = append(fixed, n)
		}
	}
	if len(fixed) > len(cols) {
		fixed = fixed[:len(cols)]
	}
	if len(fixed) == len(names) {
		return names
	}
	m.log.Warn("view records columns of ID domains' ids; loading it without them, and CREATE OR REPLACE VIEW records the right columns",
		"view", v.Schema+"."+v.Name, "recorded", len(names), "columns", len(cols))
	return fixed
}

// describeQuery returns the columns query returns.
func (m *Manager) describeQuery(ctx context.Context, conn *duckdb.Conn, query string) ([]catalog.Column, error) {
	rows, err := conn.QueryContext(ctx, "SELECT column_name, column_type FROM (DESCRIBE "+query+")", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []catalog.Column
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, catalog.Column{Name: name, Type: typ})
	}
}

// stubLocal makes the local copy of v, a catalog view whose copy could not
// be made, a view of v's columns that DuckDB lists but cannot bind: it
// reads a relation that the sync's connection makes for the purpose, alone
// can see, and drops again. A statement that reads the view, directly or
// through others, thus fails to bind for want of a relation, as it would
// before the replica had the view at all, and so syncs before it fails
// (see SyncViews): a stub made from a list older than the statement, as
// one made in the middle of dbt's swap of a view it reads, does not fail
// it. If the view still cannot be made, the statement fails with why (see
// Explain), as reading the view fails should the relation not drop.
func (m *Manager) stubLocal(ctx context.Context, v catalog.View, why error) {
	msg := (&unloadedError{schema: v.Schema, name: v.Name, why: why}).Error()
	fail := "error(" + quoteLiteral(msg) + ")"
	cols := make([]string, len(v.Columns))
	for i, c := range v.Columns {
		value := "NULL"
		if i == 0 {
			value = fail
		}
		cols[i] = "CAST(" + value + " AS " + c.Type + ") AS " + quoteIdent(c.Name)
	}
	if err := m.exec(ctx, m.conn, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(m.eng.LocalDatabase())+"."+quoteIdent(v.Schema)); err != nil {
		return
	}
	absent := "temp.main." + quoteIdent(absentName(keyOf(v.Schema, v.Name)))
	from := " FROM " + absent
	if m.exec(ctx, m.conn, "CREATE OR REPLACE TEMP VIEW "+absent+" AS SELECT 1") != nil {
		from = ""
	}
	stmt := "CREATE OR REPLACE VIEW " + m.localName(v.Schema, v.Name) + " AS SELECT "
	if len(cols) == 0 || m.exec(ctx, m.conn, stmt+strings.Join(cols, ", ")+from) != nil {
		_ = m.exec(ctx, m.conn, stmt+fail+" AS error"+from)
	}
	if from != "" {
		_ = m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+absent)
	}
}

// absentName is the name of the relation the stub of the view k reads (see
// stubLocal).
func absentName(k key) string {
	sum := sha256.Sum256([]byte(k.schema + "\x00" + k.name))
	return "igloo_unloaded_" + hex.EncodeToString(sum[:8])
}

// absentRelation matches DuckDB's error for a statement that read a stub,
// and the name of the relation the stub reads.
var absentRelation = regexp.MustCompile(`Table with name (igloo_unloaded_[0-9a-f]{16}) does not exist`)

// Explain returns, for err, DuckDB's error for a statement that read the
// stub of a view this replica could not make, an error that says why it
// could not be made; it returns any other error as is.
func (m *Manager) Explain(err error) error {
	if err == nil {
		return nil
	}
	if match := absentRelation.FindStringSubmatch(err.Error()); match != nil {
		if why, ok := (*m.unloaded.Load())[match[1]]; ok {
			return why
		}
	}
	return err
}

// unloadedError is why the local copy of the view schema.name could not be
// made. It unwraps to the error that making it failed with, which decides
// its kind, such as DuckDB's for a missing table.
type unloadedError struct {
	schema, name string
	why          error
}

func (e *unloadedError) Error() string {
	return fmt.Sprintf("view %s.%s could not be loaded: %s", e.schema, e.name, firstLine(e.why))
}

func (e *unloadedError) Unwrap() error { return e.why }

// firstLine is the first line of err's message, without DuckDB's excerpt
// of the SQL.
func firstLine(err error) string {
	if err == nil {
		return "unknown error"
	}
	line, _, _ := strings.Cut(err.Error(), "\n")
	return line
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// localViews returns the views in the local database.
func (m *Manager) localViews(ctx context.Context) (map[key]bool, error) {
	rows, err := m.conn.QueryContext(ctx, "SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = $1 AND NOT internal",
		[]driver.NamedValue{{Ordinal: 1, Value: m.eng.LocalDatabase()}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := map[key]bool{}
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		schema, _ := dest[0].(string)
		name, _ := dest[1].(string)
		out[keyOf(schema, name)] = true
	}
}

// describe returns the columns of the local view named local.
func (m *Manager) describe(ctx context.Context, conn *duckdb.Conn, local string) ([]catalog.Column, error) {
	rows, err := conn.QueryContext(ctx, "SELECT column_name, column_type FROM (DESCRIBE "+local+")", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []catalog.Column
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, catalog.Column{Name: name, Type: typ})
	}
}

func (m *Manager) localName(schema, name string) string {
	return quoteIdent(m.eng.LocalDatabase()) + "." + quoteIdent(schema) + "." + quoteIdent(name)
}

func (m *Manager) exec(ctx context.Context, conn *duckdb.Conn, stmt string) error {
	_, err := conn.ExecContext(ctx, stmt, nil)
	return err
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteIdent(n)
	}
	return strings.Join(quoted, ", ")
}
