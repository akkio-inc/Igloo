package distributed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
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
	// removed are the directories the coordinator removed.
	removed []string
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

func (s fileSource) RemoveAll(_ context.Context, _ *Table, dir string) error {
	s.h.removed = append(s.h.removed, dir)
	return os.RemoveAll(dir)
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

// connector adapts a DuckDB connector to Connector.
type connector struct{ c *duckdb.Connector }

func (c connector) Connect(ctx context.Context) (*duckdb.Conn, error) {
	conn, err := c.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return conn.(*duckdb.Conn), nil
}

// newHarness generates TPC-H data at scale sf, writes each table as files
// Parquet files, and starts n workers.
func newHarness(t *testing.T, sf float64, files, n int) *harness {
	t.Helper()
	h := &harness{t: t, dir: t.TempDir(), tables: map[string]*Table{}, files: map[string][]File{},
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if testing.Verbose() {
		h.log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	h.db = db
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if h.session, err = db.Conn(ctx); err != nil {
		t.Fatal(err)
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
	var ids []string
	for i := range cols {
		cols[i].ID = i + 1
		ids = append(ids, fmt.Sprintf("%s: %d", quoteIdent(cols[i].Name), cols[i].ID))
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
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Threads: 2, Pool: PoolOptions{Addresses: addrs},
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
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		h.t.Fatal(err)
	}
	setup, err := c.Connect(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	for _, stmt := range []string{
		"SET threads = 2",
		"SET allowed_directories = [" + quoteLiteral(filepath.Join(h.dir, "warehouse")+"/") + ", " + quoteLiteral(tmp+"/") + "]",
		"SET enable_external_access = false",
	} {
		if _, err := setup.(driver.ExecerContext).ExecContext(context.Background(), stmt, nil); err != nil {
			h.t.Fatal(err)
		}
	}
	setup.Close()
	h.t.Cleanup(func() { c.Close() })
	w, err := NewWorker(WorkerOptions{
		Engine: connector{c}, Secret: testSecret, Version: "test", ID: fmt.Sprintf("worker-%d", i), TempDir: tmp,
		Locations: []string{filepath.Join(h.dir, "warehouse")}, Threads: 2, MemoryBytes: 4 << 30,
	}, h.log)
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
	got, gotCols, plan, _, err := h.run(ctx, query, ModeOn)
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
		h.t.Errorf("%s\n%s\nfragment: %s", short(query), msg, plan.Jobs[0].Fragment)
	}
	return true
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
