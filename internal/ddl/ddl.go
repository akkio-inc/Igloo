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
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/engine"
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

	// mu serializes view changes and syncs, and guards conn and views.
	mu sync.Mutex
	// conn runs syncs.
	conn  *duckdb.Conn
	views map[key]catalog.View
	// known is the set of view keys, read without locking by IsView.
	known atomic.Pointer[map[key]bool]
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
	m := &Manager{cat: cat, eng: eng, conn: conn, views: map[key]catalog.View{}, log: log.With("component", "ddl"),
		touched: map[key]time.Time{}}
	m.known.Store(&map[key]bool{})
	if _, err := m.SyncViews(ctx); err != nil {
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

// Run syncs views every syncInterval until ctx is canceled.
func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(syncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := m.SyncViews(ctx); err != nil && ctx.Err() == nil {
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

// SyncViews makes the local copies match the catalog's views. changed says
// whether anything changed.
func (m *Manager) SyncViews(ctx context.Context) (changed bool, err error) {
	defs, err := m.cat.DuckDBViews(ctx)
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	defer func() {
		if changed {
			m.gen.Add(1)
		}
	}()

	want := map[key]catalog.View{}
	for _, v := range defs {
		want[keyOf(v.Schema, v.Name)] = v
	}
	for k, v := range m.views {
		if _, ok := want[k]; !ok {
			if err := m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(v.Schema, v.Name)); err != nil {
				return changed, err
			}
			delete(m.views, k)
			changed = true
		}
	}

	// Local copies can also go missing, say if an admin drops one; they are
	// recreated too.
	present, err := m.localViews(ctx)
	if err != nil {
		return changed, err
	}

	// A view can read other views, which must exist locally first. Create
	// what can be created, and repeat while that makes progress.
	pending := map[key]catalog.View{}
	for k, v := range want {
		if cur, ok := m.views[k]; !ok || !present[k] || cur.Version != v.Version || cur.SQL != v.SQL {
			pending[k] = v
		}
	}
	for len(pending) > 0 {
		var errs []error
		progress := false
		for k, v := range pending {
			// Let names in v resolve against other pending views too.
			m.publish(pending)
			if err := m.createLocal(ctx, m.conn, v); err != nil {
				errs = append(errs, fmt.Errorf("view %s.%s: %w", v.Schema, v.Name, err))
				continue
			}
			m.views[k] = v
			delete(pending, k)
			progress, changed = true, true
		}
		if !progress {
			m.publish(nil)
			return changed, errors.Join(errs...)
		}
	}
	m.publish(nil)
	return changed, nil
}

// publish updates the set IsView reads: the local views plus extra.
func (m *Manager) publish(extra map[key]catalog.View) {
	known := make(map[key]bool, len(m.views)+len(extra))
	for k := range m.views {
		known[k] = true
	}
	for k := range extra {
		known[k] = true
	}
	m.known.Store(&known)
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
	if err := m.exec(ctx, conn, stmt+" AS "+m.rewrite(query)); err != nil {
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

	err := m.cat.DropView(ctx, catalog.Ident(schema, name))
	_, local := m.views[k]
	switch {
	case errors.Is(err, icecat.ErrNoSuchView):
		if !local && !ifExists {
			return fmt.Errorf("view %q %w", name, ErrNotFound)
		}
	case err != nil:
		return err
	}
	if err := m.exec(ctx, m.conn, "DROP VIEW IF EXISTS "+m.localName(schema, name)); err != nil {
		return err
	}
	delete(m.views, k)
	m.publish(nil)
	m.reloadTable(ctx, schema, name, true)
	return nil
}

// createLocal creates or replaces the local copy of v.
func (m *Manager) createLocal(ctx context.Context, conn *duckdb.Conn, v catalog.View) error {
	schema := quoteIdent(m.eng.LocalDatabase()) + "." + quoteIdent(v.Schema)
	if err := m.exec(ctx, conn, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		return err
	}
	stmt := "CREATE OR REPLACE VIEW " + m.localName(v.Schema, v.Name)
	if len(v.Columns) > 0 {
		names := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			names[i] = c.Name
		}
		stmt += " (" + quoteList(names) + ")"
	}
	return m.exec(ctx, conn, stmt+" AS "+m.rewrite(v.SQL))
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
