package engine

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
)

// HistorySchema is the schema of the local database that holds the
// query_history and login_history views, so they read as
// igloo.query_history and igloo.login_history.
const HistorySchema = "igloo"

// History serves igloo_query_history(), igloo_login_history() and
// igloo_usage().
type History interface {
	Queries(ctx context.Context, v audit.Viewer, p audit.Page) ([]audit.Query, error)
	Logins(ctx context.Context, v audit.Viewer, p audit.Page) ([]audit.Login, error)
	Usage(ctx context.Context, v audit.Viewer, since, until time.Time) ([]audit.Usage, error)
}

// HistoryViewer says whose history a statement may read. ctx is the context
// the statement was prepared with; the returned function is called when it
// runs, and reports false when there is nobody to show history to.
type HistoryViewer func(ctx context.Context) func() (audit.Viewer, bool)

type historyConfig struct {
	source History
	viewer HistoryViewer
}

// SetHistory makes the history functions list what source returns, to the
// viewer that viewer works out for each statement. Until it is set, they
// are empty.
func (e *Engine) SetHistory(source History, viewer HistoryViewer) {
	e.history.Store(&historyConfig{source: source, viewer: viewer})
}

// historyPage is how many records the history functions fetch at a time.
const historyPage = 1000

// registerHistory defines igloo_query_history() and igloo_login_history(),
// and the views igloo.query_history and igloo.login_history over them.
// Each lists only what the session that runs it may see: everything for
// admins, their own records for everyone else. Records come newest first,
// read page by page, so a LIMIT reads little.
//
// The functions take no arguments: duckdb-go aborts the process when a
// table function is passed NULL.
func (e *Engine) registerHistory(ctx context.Context) error {
	// Not closed: closing a DB closes its connector.
	db := sql.OpenDB(e.connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck

	types, err := newHistoryTypes()
	if err != nil {
		return err
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_query_history", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			return &historySource[audit.Query]{
				columns: types.queryColumns(),
				viewer:  e.historyViewer(ctx),
				fetch: func(h History, v audit.Viewer, p audit.Page) ([]audit.Query, error) {
					return h.Queries(context.Background(), v, p)
				},
				history: e.historySource(),
				key:     func(q audit.Query) (time.Time, string) { return q.Start, q.ID },
				values:  queryValues,
			}, nil
		},
	})
	if err != nil {
		return err
	}
	err = duckdb.RegisterTableUDF(conn, "igloo_login_history", duckdb.RowTableFunction{
		BindArgumentsContext: func(ctx context.Context, _ map[string]any, _ ...any) (duckdb.RowTableSource, error) {
			return &historySource[audit.Login]{
				columns: types.loginColumns(),
				viewer:  e.historyViewer(ctx),
				fetch: func(h History, v audit.Viewer, p audit.Page) ([]audit.Login, error) {
					return h.Logins(context.Background(), v, p)
				},
				history: e.historySource(),
				key:     func(x audit.Login) (time.Time, string) { return x.Time, x.ID },
				values:  loginValues,
			}, nil
		},
	})
	if err != nil {
		return err
	}
	if err := e.registerUsage(conn, types); err != nil {
		return err
	}

	schema := ident(e.local) + "." + ident(HistorySchema)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"CREATE OR REPLACE VIEW " + schema + ".query_history AS SELECT * FROM system.main.igloo_query_history()",
		"CREATE OR REPLACE VIEW " + schema + ".login_history AS SELECT * FROM system.main.igloo_login_history()",
		"CREATE OR REPLACE VIEW " + schema + ".usage AS SELECT * FROM system.main.igloo_usage()",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func (e *Engine) historySource() History {
	if c := e.history.Load(); c != nil {
		return c.source
	}
	return nil
}

// historyViewer works out the viewer for a statement prepared with ctx.
// Statements from outside a session, such as the view syncs, see nothing.
func (e *Engine) historyViewer(ctx context.Context) func() (audit.Viewer, bool) {
	c := e.history.Load()
	if c == nil || c.viewer == nil {
		return func() (audit.Viewer, bool) { return audit.Viewer{}, false }
	}
	if v := c.viewer(ctx); v != nil {
		return v
	}
	return func() (audit.Viewer, bool) { return audit.Viewer{}, false }
}

// historySource lists records page by page as DuckDB asks for rows, so a
// query that stops early, such as one with a LIMIT, reads little.
type historySource[T any] struct {
	columns []duckdb.ColumnInfo
	page    audit.Page
	viewer  func() (audit.Viewer, bool)
	history History
	fetch   func(History, audit.Viewer, audit.Page) ([]T, error)
	key     func(T) (time.Time, string)
	values  func(T) []any

	who     *audit.Viewer
	buf     []T
	pos     int
	drained bool
}

func (s *historySource[T]) ColumnInfos() []duckdb.ColumnInfo     { return s.columns }
func (s *historySource[T]) Cardinality() *duckdb.CardinalityInfo { return nil }
func (s *historySource[T]) Init()                                {}

func (s *historySource[T]) FillRow(row duckdb.Row) (bool, error) {
	if s.pos >= len(s.buf) {
		if s.drained || !s.next() {
			return false, nil
		}
		if err := s.load(); err != nil {
			return false, err
		}
		if len(s.buf) == 0 {
			return false, nil
		}
	}
	r := s.buf[s.pos]
	s.pos++
	for i, v := range s.values(r) {
		if err := row.SetRowValue(i, v); err != nil {
			return false, err
		}
	}
	return true, nil
}

// next decides, on the first call, who the rows are for; it reports false
// when there is nobody, or no history.
func (s *historySource[T]) next() bool {
	if s.who == nil {
		v, ok := s.viewer()
		if !ok || s.history == nil {
			s.drained = true
			return false
		}
		s.who = &v
	}
	return true
}

func (s *historySource[T]) load() error {
	p := s.page
	p.Limit = historyPage
	if len(s.buf) > 0 {
		p.AfterTime, p.AfterID = s.key(s.buf[len(s.buf)-1])
	}
	rows, err := s.fetch(s.history, *s.who, p)
	if err != nil {
		return fmt.Errorf("read history: %w", err)
	}
	s.buf, s.pos = rows, 0
	if len(rows) < historyPage {
		s.drained = true
	}
	return nil
}

type historyTypes struct {
	varchar, timestamp, double, bigint, list, date duckdb.TypeInfo
}

func newHistoryTypes() (historyTypes, error) {
	var t historyTypes
	var err error
	if t.varchar, err = duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR); err != nil {
		return t, err
	}
	if t.timestamp, err = duckdb.NewTypeInfo(duckdb.TYPE_TIMESTAMP_TZ); err != nil {
		return t, err
	}
	if t.double, err = duckdb.NewTypeInfo(duckdb.TYPE_DOUBLE); err != nil {
		return t, err
	}
	if t.bigint, err = duckdb.NewTypeInfo(duckdb.TYPE_BIGINT); err != nil {
		return t, err
	}
	if t.date, err = duckdb.NewTypeInfo(duckdb.TYPE_DATE); err != nil {
		return t, err
	}
	t.list, err = duckdb.NewListInfo(t.varchar)
	return t, err
}

func (t historyTypes) queryColumns() []duckdb.ColumnInfo {
	return []duckdb.ColumnInfo{
		{Name: "query_id", T: t.varchar},
		{Name: "session_id", T: t.varchar},
		{Name: "user_name", T: t.varchar},
		{Name: "role", T: t.varchar},
		{Name: "start_time", T: t.timestamp},
		{Name: "end_time", T: t.timestamp},
		{Name: "duration_ms", T: t.double},
		{Name: "outcome", T: t.varchar},
		{Name: "sqlstate", T: t.varchar},
		{Name: "row_count", T: t.bigint},
		{Name: "statement_type", T: t.varchar},
		{Name: "query_text", T: t.varchar},
		{Name: "tables_read", T: t.list},
		{Name: "tables_written", T: t.list},
		{Name: "client_address", T: t.varchar},
		{Name: "application_name", T: t.varchar},
		{Name: "policies", T: t.list},
		{Name: "query_tag", T: t.varchar},
		{Name: "pool", T: t.varchar},
		{Name: "queued_ms", T: t.double},
		{Name: "cpu_time_ms", T: t.double},
		{Name: "bytes_read", T: t.bigint},
		{Name: "memory_allocated_bytes", T: t.bigint},
	}
}

func queryValues(q audit.Query) []any {
	var rows any
	if q.Rows >= 0 {
		rows = q.Rows
	}
	// What DuckDB did not measure is NULL.
	var cpu, read, memory any
	if r := q.Resources; r != nil {
		cpu, read, memory = millis(r.CPUTime), r.BytesRead, r.MemoryAllocated
	}
	return []any{q.ID, q.SessionID, q.User, q.Role, q.Start, q.Start.Add(q.Duration),
		millis(q.Duration), q.Outcome, nullString(q.SQLState), rows, q.StatementType, q.Text,
		nullList(q.TablesRead), nullList(q.TablesWritten), nullString(q.ClientAddress), nullString(q.ApplicationName),
		nullList(q.Policies), nullString(q.QueryTag), nullString(q.Pool), millis(q.Queued), cpu, read, memory}
}

func millis(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func (t historyTypes) loginColumns() []duckdb.ColumnInfo {
	return []duckdb.ColumnInfo{
		{Name: "event_time", T: t.timestamp},
		{Name: "user_name", T: t.varchar},
		{Name: "result", T: t.varchar},
		{Name: "reason", T: t.varchar},
		{Name: "client_address", T: t.varchar},
		{Name: "application_name", T: t.varchar},
		{Name: "session_id", T: t.varchar},
		{Name: "auth_method", T: t.varchar},
		{Name: "token_name", T: t.varchar},
	}
}

func loginValues(x audit.Login) []any {
	return []any{x.Time, x.User, x.Result, nullString(x.Reason), nullString(x.ClientAddress),
		nullString(x.ApplicationName), nullString(x.SessionID), nullString(x.Method), nullString(x.Token)}
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullList(s []string) any {
	if s == nil {
		return nil
	}
	return s
}
