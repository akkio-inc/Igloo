package distributed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// The harness runs distributed execution in one process: tables are
// Parquet files on local disk standing in for Iceberg tables, workers are
// real Worker servers on localhost with their own locked-down DuckDB, and a
// "session" DuckDB with a view per table plays the coordinator's session,
// where results are compared with running each statement directly.

const testSecret = "a-test-secret-that-is-long-enough-1234"

type harness struct {
	t       *testing.T
	dir     string
	session *sql.Conn
	db      *sql.DB
	tables  map[string]*Table
	files   map[string][]File
	co      *Coordinator
	workers []*Worker
	lns     []net.Listener
	log     *slog.Logger
	// removed are the directories of exchanges the coordinator removed.
	removedMu sync.Mutex
	removed   []string
	// views are the queries of the views addView made.
	views map[string]string
	// hook, if set, sees every record the coordinator and workers log.
	hook atomic.Pointer[func(slog.Record)]
	// rb, if set, is the rb extension the session and workers load, and
	// extensions are DuckDB's that they load from IGLOO_TEST_EXTENSION_DIR.
	rb         string
	extensions []string
	// wrap, if set, wraps the listener of each worker startWorker starts.
	wrap func(i int, ln net.Listener) net.Listener
	// groupFunctions are the functions the session's GroupFunctions names.
	groupFunctions map[string]bool
	// erasure, if set, is what worker i purges for erasure requests, and
	// connected, if set, sees each connection worker i opens for a task.
	erasure   func(i int) WorkerErasure
	connected func(i int)
	// noPlans keeps the session from planning statements for auto mode's
	// working sets (see harnessEnv.Explain).
	noPlans bool
}

// hookHandler passes records to the harness's hook, and on to its handler.
type hookHandler struct {
	h *harness
	slog.Handler
}

func (hh hookHandler) Enabled(context.Context, slog.Level) bool { return true }
func (hh hookHandler) Handle(ctx context.Context, r slog.Record) error {
	if f := hh.h.hook.Load(); f != nil {
		(*f)(r)
	}
	if hh.Handler.Enabled(ctx, r.Level) {
		return hh.Handler.Handle(ctx, r)
	}
	return nil
}
func (hh hookHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return hookHandler{hh.h, hh.Handler.WithAttrs(as)}
}
func (hh hookHandler) WithGroup(name string) slog.Handler {
	return hookHandler{hh.h, hh.Handler.WithGroup(name)}
}

// attr returns the value of record r's attribute key, as a string.
func attr(r slog.Record, key string) string {
	var v string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			v = a.Value.String()
			return false
		}
		return true
	})
	return v
}

// fileSource serves the harness's tables.
type fileSource struct{ h *harness }

func (s fileSource) Table(_ context.Context, schema, name string) (*Table, error) {
	t, ok := s.h.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("no table %s", name)
	}
	return t, nil
}

func (s fileSource) Files(_ context.Context, t *Table) ([]File, error) {
	return s.h.files[t.key()], nil
}

func (s fileSource) Refresh(context.Context, *Table) error { return nil }

// recordingStorage is the harness's warehouse storage, which records the
// directories of statements' exchanges the coordinator removes.
type recordingStorage struct {
	Storage
	h *harness
}

func (s recordingStorage) List(ctx context.Context, dir string, fn func(string, time.Time) error) error {
	if strings.Contains(dir, "/"+config.ShuffleDir+"/") {
		s.h.removedMu.Lock()
		s.h.removed = append(s.h.removed, dir)
		s.h.removedMu.Unlock()
	}
	return s.Storage.List(ctx, dir, fn)
}

// harnessEnv resolves names in the harness's session.
type harnessEnv struct{ h *harness }

func (e harnessEnv) Table(_ context.Context, parts []string) (*Table, bool, error) {
	name := strings.ToLower(parts[len(parts)-1])
	if len(parts) > 1 && !strings.EqualFold(parts[len(parts)-2], "main") {
		return nil, false, nil
	}
	t, ok := e.h.tables[name]
	return t, ok, nil
}

func (e harnessEnv) Describe(ctx context.Context, query string) ([]Column, error) {
	rows, err := e.h.session.QueryContext(ctx, "DESCRIBE "+query)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []Column
	for rows.Next() {
		var name, typ string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
			return nil, err
		}
		cols = append(cols, Column{Name: name, Type: typ})
	}
	return cols, rows.Err()
}

func (e harnessEnv) UserFunction(context.Context, string) (bool, error) { return false, nil }

// Explain plans query on the session, as the server's sessions do; with
// noPlans, it plans nothing, as for tests whose tables' sizes are set by
// hand, which DuckDB's plan of the harness's small tables would not see.
func (e harnessEnv) Explain(ctx context.Context, query string) (string, error) {
	if e.h.noPlans {
		return "", errors.New("the harness plans nothing")
	}
	var plan string
	err := e.h.session.QueryRowContext(ctx, "EXPLAIN (FORMAT JSON) "+query).Scan(new(string), &plan)
	return plan, err
}

func (e harnessEnv) GroupFunction(name string) bool { return e.h.groupFunctions[name] }

// ViewDefinition returns the views addView made. Names in them mean the
// harness's tables and views.
func (e harnessEnv) ViewDefinition(_ context.Context, parts []string) (*ViewDefinition, bool, error) {
	q, ok := e.h.views[strings.ToLower(parts[len(parts)-1])]
	if !ok || len(parts) > 1 && !strings.EqualFold(parts[len(parts)-2], "main") {
		return nil, false, nil
	}
	return &ViewDefinition{Query: q, Resolve: func(_ context.Context, parts []string) ([]string, bool, error) {
		name := strings.ToLower(parts[len(parts)-1])
		if _, ok := e.h.tables[name]; ok || e.h.views[name] != "" {
			return []string{"memory", "main", name}, true, nil
		}
		return nil, false, nil
	}}, true, nil
}

// View reports views the session has besides the tables' own, which the
// harness defines as views over their files.
func (e harnessEnv) View(ctx context.Context, parts []string) (bool, error) {
	name := strings.ToLower(parts[len(parts)-1])
	if _, ok := e.h.tables[name]; ok {
		return false, nil
	}
	var n int
	err := e.h.session.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_views() WHERE lower(view_name) = "+quoteLiteral(name)+" AND NOT internal").Scan(&n)
	return n > 0, err
}

func (e harnessEnv) Setting(ctx context.Context, name string) (string, error) {
	var v string
	err := e.h.session.QueryRowContext(ctx, "SELECT current_setting("+quoteLiteral(name)+")::VARCHAR").Scan(&v)
	return v, err
}

func (e harnessEnv) Aggregates(ctx context.Context) ([]string, error) {
	rows, err := e.h.session.QueryContext(ctx, AggregatesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// connector adapts a DuckDB connector to Connector.
type connector struct{ c *duckdb.Connector }

func (c connector) Connect(ctx context.Context) (*duckdb.Conn, error) {
	conn, err := c.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return conn.(*duckdb.Conn), nil
}

// seenConnector tells seen of each connection it opens.
type seenConnector struct {
	connector
	seen func()
}

func (c seenConnector) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c.seen()
	return c.connector.Connect(ctx)
}

// newHarness generates TPC-H data at scale sf, writes each table as files
// Parquet files, and starts n workers.
func newHarness(t *testing.T, sf float64, files, n int) *harness {
	t.Helper()
	return newHarnessRB(t, sf, files, n, "")
}

// newHarnessRB is newHarness whose session and workers load the rb
// extension at rb, if not empty, as Igloo loads it.
func newHarnessRB(t *testing.T, sf float64, files, n int, rb string) *harness {
	t.Helper()
	return newHarnessWith(t, sf, files, n, rb)
}

// newHarnessWith is newHarnessRB whose session and workers also load
// extensions, DuckDB's, from IGLOO_TEST_EXTENSION_DIR, as Igloo loads them.
func newHarnessWith(t *testing.T, sf float64, files, n int, rb string, extensions ...string) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), tables: map[string]*Table{}, files: map[string][]File{}, rb: rb, extensions: extensions}
	var base slog.Handler = slog.NewTextHandler(io.Discard, nil)
	if testing.Verbose() {
		base = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	h.log = slog.New(hookHandler{h, base})
	db, err := sql.Open("duckdb", h.dsn())
	if err != nil {
		t.Fatal(err)
	}
	h.db = db
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if h.session, err = db.Conn(ctx); err != nil {
		t.Fatal(err)
	}
	if rb != "" {
		h.exec("LOAD " + quoteLiteral(rb))
	}
	for _, stmt := range h.extensionLoads() {
		h.exec(stmt)
	}
	h.exec("INSTALL tpch")
	h.exec("LOAD tpch")
	h.exec(fmt.Sprintf("CALL dbgen(sf = %g)", sf))
	for _, name := range []string{"lineitem", "orders", "customer", "part", "partsupp", "supplier", "nation", "region"} {
		h.addTable(name, name, files)
	}
	h.startWorkers(n)
	return h
}

// extensionLoads load the harness's extensions from
// IGLOO_TEST_EXTENSION_DIR.
func (h *harness) extensionLoads() []string {
	if len(h.extensions) == 0 {
		return nil
	}
	out := []string{"SET extension_directory = " + quoteLiteral(os.Getenv("IGLOO_TEST_EXTENSION_DIR"))}
	for _, e := range h.extensions {
		out = append(out, "LOAD "+e)
	}
	return out
}

func (h *harness) exec(query string) {
	h.t.Helper()
	if _, err := h.session.ExecContext(context.Background(), query); err != nil {
		h.t.Fatalf("%s: %v", query, err)
	}
}

// addTable writes the rows of source (a table or a query) as table name in
// files Parquet files with Iceberg-style field IDs, and replaces name in
// the session with a view over them.
func (h *harness) addTable(name, source string, files int) {
	h.t.Helper()
	h.addNestedTable(name, source, files, nil)
}

// addNestedTable is addTable for a table whose columns' field IDs, nested
// ones included, are nested's (ID and Nested; the types are the
// source's). Without nested, columns are numbered from 1.
func (h *harness) addNestedTable(name, source string, files int, nested []Column) {
	h.t.Helper()
	ctx := context.Background()
	dir := filepath.Join(h.dir, "warehouse", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	h.exec(fmt.Sprintf("CREATE OR REPLACE TEMP TABLE __src AS SELECT * FROM %s", source))
	cols, err := harnessEnv{h}.Describe(ctx, "SELECT * FROM __src")
	if err != nil {
		h.t.Fatal(err)
	}
	if nested != nil && len(nested) != len(cols) {
		h.t.Fatalf("%d columns for %d", len(nested), len(cols))
	}
	var ids []string
	for i := range cols {
		cols[i].ID = i + 1
		if nested != nil {
			cols[i].ID, cols[i].Nested = nested[i].ID, nested[i].Nested
		}
		ids = append(ids, quoteIdent(cols[i].Name)+": "+fieldIDs(cols[i].ID, cols[i].Nested))
	}
	tbl := &Table{Schema: "main", Name: name, Columns: cols}
	var fs []File
	for i := range files {
		path := filepath.Join(dir, fmt.Sprintf("part-%d.parquet", i))
		h.exec(fmt.Sprintf("COPY (SELECT * FROM __src WHERE hash(rowid) %% %d = %d) TO %s (FORMAT parquet, FIELD_IDS {%s})",
			files, i, quoteLiteral(path), strings.Join(ids, ", ")))
		info, err := os.Stat(path)
		if err != nil {
			h.t.Fatal(err)
		}
		var rows int64
		if err := h.session.QueryRowContext(ctx, "SELECT count(*) FROM read_parquet("+quoteLiteral(path)+")").Scan(&rows); err != nil {
			h.t.Fatal(err)
		}
		fs = append(fs, File{Path: path, Size: info.Size(), Rows: rows})
		tbl.Bytes += info.Size()
	}
	h.exec("DROP TABLE __src")
	h.exec(fmt.Sprintf("DROP TABLE IF EXISTS main.%s", quoteIdent(name)))
	h.exec(fmt.Sprintf("CREATE OR REPLACE VIEW main.%s AS SELECT * FROM read_parquet([%s])", quoteIdent(name), strings.Join(quoted(paths(fs)), ", ")))
	h.tables[strings.ToLower(name)] = tbl
	h.files[tbl.key()] = fs
}

// dsn is the harness's DuckDB path: one that may load the rb extension,
// which is not signed, when it loads it.
func (h *harness) dsn() string {
	if h.rb != "" {
		return "?allow_unsigned_extensions=true"
	}
	return ""
}

// fieldIDs is a column's field IDs as COPY's FIELD_IDS option takes them.
func fieldIDs(id int, nest *Nesting) string {
	if nest == nil {
		return strconv.Itoa(id)
	}
	parts := []string{"__duckdb_field_id: " + strconv.Itoa(id)}
	for _, f := range nest.Fields {
		parts = append(parts, quoteIdent(f.Name)+": "+fieldIDs(f.ID, f.Nested))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// addView creates a view in the session, which the planner may read as
// its query.
func (h *harness) addView(name, query string) {
	h.t.Helper()
	h.exec(fmt.Sprintf("CREATE OR REPLACE VIEW main.%s AS %s", quoteIdent(name), query))
	if h.views == nil {
		h.views = map[string]string{}
	}
	h.views[strings.ToLower(name)] = query
}

func quoted(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = quoteLiteral(s)
	}
	return out
}

// startWorkers starts n workers and a coordinator that uses them.
func (h *harness) startWorkers(n int) {
	h.t.Helper()
	var addrs []string
	for i := range n {
		h.startWorker(i)
		addrs = append(addrs, h.lns[i].Addr().String())
	}
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2, Pool: PoolOptions{Addresses: addrs},
		Storage: recordingStorage{NewStorage(icebergio.LocalFS{}), h},
	}, h.log)
	if err != nil {
		h.t.Fatal(err)
	}
	h.co = co
	h.t.Cleanup(func() { co.Close() })
}

func (h *harness) startWorker(i int) {
	h.t.Helper()
	tmp := filepath.Join(h.dir, fmt.Sprintf("worker-%d", i))
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		h.t.Fatal(err)
	}
	// Locked down as the server and workers lock DuckDB down: once, for
	// every connection.
	c, err := duckdb.NewConnector(h.dsn(), nil)
	if err != nil {
		h.t.Fatal(err)
	}
	setup, err := c.Connect(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	var load []string
	if h.rb != "" {
		load = []string{"LOAD " + quoteLiteral(h.rb), "SET allow_unsigned_extensions = false"}
	}
	load = append(h.extensionLoads(), load...)
	for _, stmt := range append(load,
		"SET threads = 2",
		"SET allowed_directories = ["+quoteLiteral(filepath.Join(h.dir, "warehouse")+"/")+", "+quoteLiteral(tmp+"/")+"]",
		"SET enable_external_access = false",
	) {
		if _, err := setup.(driver.ExecerContext).ExecContext(context.Background(), stmt, nil); err != nil {
			h.t.Fatal(err)
		}
	}
	setup.Close()
	h.t.Cleanup(func() { c.Close() })
	opts := WorkerOptions{
		Engine: connector{c}, Secret: testSecret, Version: "test", ID: fmt.Sprintf("worker-%d", i), TempDir: tmp,
		Locations: []string{filepath.Join(h.dir, "warehouse")}, Cores: 8, MemoryBytes: 4 << 30,
	}
	if h.erasure != nil {
		opts.Erasure = h.erasure(i)
	}
	if h.connected != nil {
		opts.Engine = seenConnector{connector{c}, func() { h.connected(i) }}
	}
	w, err := NewWorker(opts, h.log)
	if err != nil {
		h.t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	if i < len(h.lns) {
		ln.Close()
		if ln, err = net.Listen("tcp", h.lns[i].Addr().String()); err != nil {
			h.t.Fatal(err)
		}
		h.lns[i], h.workers[i] = ln, w
	} else {
		h.lns, h.workers = append(h.lns, ln), append(h.workers, w)
	}
	if h.wrap != nil {
		ln = h.wrap(i, ln)
	}
	go w.Serve(ln) //nolint:errcheck
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Shutdown(ctx)
	})
}

// run plans and executes query distributed and returns its rows, or
// ok = false if it ran on one machine.
func (h *harness) run(ctx context.Context, query string, mode Mode) (rows [][]any, cols []string, plan *Plan, ex *Execution, err error) {
	plan, err = h.co.Plan(ctx, query, harnessEnv{h}, mode)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ex, err = h.co.Execute(ctx, plan, h.settings(ctx))
	if err != nil {
		return nil, nil, plan, nil, err
	}
	defer ex.Close()
	rows, cols, err = h.query(ctx, ex.SQL)
	return rows, cols, plan, ex, err
}

func (h *harness) settings(ctx context.Context) []Setting {
	var out []Setting
	for _, name := range SettingNames {
		var v sql.NullString
		if err := h.session.QueryRowContext(ctx, "SELECT current_setting("+quoteLiteral(name)+")::VARCHAR").Scan(&v); err == nil && v.Valid {
			out = append(out, Setting{Name: name, Value: v.String})
		}
	}
	return out
}

func (h *harness) query(ctx context.Context, query string) ([][]any, []string, error) {
	rows, err := h.session.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close() //nolint:errcheck
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		out = append(out, vals)
	}
	return out, cols, rows.Err()
}

// check runs query on one machine and distributed and compares the
// results. It reports whether the query ran distributed.
func (h *harness) check(query string) bool {
	h.t.Helper()
	ctx := context.Background()
	want, wantCols, err := h.query(ctx, query)
	if err != nil {
		h.t.Fatalf("%s\nlocal: %v", short(query), err)
	}
	got, gotCols, plan, ex, err := h.run(ctx, query, ModeOn)
	if IsUnsupported(err) {
		h.t.Logf("ran locally: %s: %v", short(query), err)
		return false
	}
	if err != nil {
		h.t.Fatalf("%s\ndistributed: %v", short(query), err)
	}
	if !slices.Equal(wantCols, gotCols) {
		h.t.Errorf("%s\ncolumns: got %v, want %v", short(query), gotCols, wantCols)
	}
	ordered := hasOrderBy(query)
	if msg := compareRows(want, got, ordered); msg != "" {
		h.t.Errorf("%s\n%s\n%s%s", short(query), msg, plan.describe(), ex.exchanges)
	}
	return true
}

// describe lists a plan's stages and their SQL, for test failures.
func (p *Plan) describe() string {
	var b strings.Builder
	for _, s := range p.Stages {
		fmt.Fprintf(&b, "%s: %s\n", s, s.SQL)
	}
	return b.String()
}

// exchanges counts a plan's exchanges for purpose.
func (p *Plan) exchanges(purpose string) int {
	n := 0
	for _, s := range p.Stages {
		if s.Out != nil && s.Out.purpose == purpose {
			n++
		}
	}
	return n
}

func short(q string) string {
	q = strings.Join(strings.Fields(q), " ")
	if len(q) > 300 {
		q = q[:300] + "..."
	}
	return q
}

func hasOrderBy(q string) bool {
	return strings.Contains(strings.ToUpper(strings.Join(strings.Fields(q), " ")), "ORDER BY")
}

// compareRows compares results: exactly, except that floating-point
// values may differ in their last digits, as they do in DuckDB between
// runs with different numbers of threads. Unordered results are compared
// as sorted lists.
func compareRows(want, got [][]any, ordered bool) string {
	if len(want) != len(got) {
		return fmt.Sprintf("%d rows, want %d", len(got), len(want))
	}
	if !ordered {
		sortRows(want)
		sortRows(got)
	}
	for i := range want {
		for j := range want[i] {
			if !sameValue(want[i][j], got[i][j]) {
				return fmt.Sprintf("row %d column %d: got %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], want[i][j], want[i][j])
			}
		}
	}
	return ""
}

func sortRows(rows [][]any) {
	sort.SliceStable(rows, func(a, b int) bool { return fmt.Sprint(rows[a]) < fmt.Sprint(rows[b]) })
}

func sameValue(a, b any) bool {
	fa, aok := a.(float64)
	fb, bok := b.(float64)
	if aok && bok {
		if fa == fb || (math.IsNaN(fa) && math.IsNaN(fb)) {
			return true
		}
		return math.Abs(fa-fb) <= 1e-9*math.Max(math.Abs(fa), math.Abs(fb))
	}
	if fa, ok := a.(float32); ok {
		fb, ok := b.(float32)
		return ok && math.Abs(float64(fa-fb)) <= 1e-5*math.Max(math.Abs(float64(fa)), math.Abs(float64(fb)))
	}
	return fmt.Sprintf("%T %v", a, a) == fmt.Sprintf("%T %v", b, b)
}
