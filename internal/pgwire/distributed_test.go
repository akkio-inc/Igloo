package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
)

const testWorkerSecret = "the-test-worker-secret-0123456789abcdef"

// lakeSource serves the stand-in catalog's tables to workers: each time a
// table is looked up, it is written to a Parquet file with field IDs, as
// an Iceberg table's data files are. Like the Iceberg source, it reads
// tables beneath grants and policies, with a connection of the engine's
// own.
type lakeSource struct {
	dir string
	mu  sync.Mutex
	// conn is the engine's, set once the server runs.
	conn  *duckdb.Conn
	files map[string][]distributed.File
}

func (s *lakeSource) Table(ctx context.Context, schema, name string) (*distributed.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.conn.QueryContext(ctx, fmt.Sprintf(`SELECT column_name, data_type FROM duckdb_columns()
		WHERE database_name = 'lake' AND schema_name = %s AND table_name = %s ORDER BY column_index`,
		quoteLiteral(schema), quoteLiteral(name)), nil)
	if err != nil {
		return nil, err
	}
	var cols []distributed.Column
	var ids []string
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			rows.Close()
			return nil, err
		}
		c := distributed.Column{Name: dest[0].(string), Type: dest[1].(string), ID: len(cols) + 1}
		cols = append(cols, c)
		ids = append(ids, fmt.Sprintf("%s: %d", quoteIdent(c.Name), c.ID))
	}
	rows.Close()
	t := &distributed.Table{Schema: schema, Name: name, Columns: cols}
	var files []distributed.File
	for i := range 3 {
		path := filepath.Join(s.dir, fmt.Sprintf("%s.%s.%d.%d.parquet", schema, name, time.Now().UnixNano(), i))
		// Not by rowid: with the spatial extension loaded, DuckDB 1.5.6 fails
		// to plan a COPY that filters on it.
		if _, err := s.conn.ExecContext(ctx, fmt.Sprintf("COPY (SELECT * EXCLUDE (__n) FROM (SELECT *, row_number() OVER () AS __n FROM lake.%s.%s) WHERE __n %% 3 = %d) TO %s (FORMAT parquet, FIELD_IDS {%s})",
			quoteIdent(schema), quoteIdent(name), i, quoteLiteral(path), strings.Join(ids, ", ")), nil); err != nil {
			return nil, err
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		files = append(files, distributed.File{Path: path, Size: info.Size()})
		t.Bytes += info.Size()
	}
	s.files[schema+"."+name] = files
	return t, nil
}

func (s *lakeSource) Files(_ context.Context, t *distributed.Table) ([]distributed.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.files[t.Schema+"."+t.Name], nil
}

// Refresh does nothing: Table wrote the table's current rows.
func (s *lakeSource) Refresh(context.Context, *distributed.Table) error { return nil }

func (s *lakeSource) RemoveAll(_ context.Context, _ *distributed.Table, dir string) error {
	return os.RemoveAll(dir)
}

// workerEngine is a test worker's DuckDB. While gate holds them, tasks
// wait to begin.
type workerEngine struct {
	c    *duckdb.Connector
	gate *taskGate
}

func (e workerEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	if err := e.gate.wait(ctx); err != nil {
		return nil, err
	}
	c, err := e.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c.(*duckdb.Conn), nil
}

// taskGate holds workers' tasks back while it is closed, and counts the
// tasks waiting and those whose statement ended while they waited.
type taskGate struct {
	mu       sync.Mutex
	open     chan struct{}
	waiting  int
	canceled int
}

func newTaskGate() *taskGate {
	g := &taskGate{open: make(chan struct{})}
	close(g.open)
	return g
}

// hold makes tasks wait until the function it returns is called.
func (g *taskGate) hold() func() {
	g.mu.Lock()
	defer g.mu.Unlock()
	ch := make(chan struct{})
	g.open = ch
	return func() { close(ch) }
}

func (g *taskGate) wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	ch := g.open
	g.waiting++
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.waiting--
		g.mu.Unlock()
	}()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		g.canceled++
		g.mu.Unlock()
		return ctx.Err()
	}
}

func (g *taskGate) counts() (waiting, canceled int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting, g.canceled
}

// startTestWorker starts a worker that may read dir, and returns its
// address and a function that stops it.
func startTestWorker(t *testing.T, dir string, i int, gate *taskGate) (string, func()) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "worker")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	setup, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"SET allowed_directories = [" + quoteLiteral(dir+"/") + ", " + quoteLiteral(tmp+"/") + "]",
		"SET enable_external_access = false",
	} {
		if _, err := setup.(driver.ExecerContext).ExecContext(context.Background(), stmt, nil); err != nil {
			t.Fatal(err)
		}
	}
	setup.Close()
	w, err := distributed.NewWorker(distributed.WorkerOptions{
		Engine: workerEngine{c, gate}, Secret: testWorkerSecret, Version: "test", ID: fmt.Sprintf("w%d", i), TempDir: tmp,
		Locations: []string{dir}, Threads: 1, MemoryBytes: 1 << 30,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go w.Serve(ln) //nolint:errcheck
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = w.Shutdown(ctx)
		ln.Close()
	}
	t.Cleanup(func() {
		stop()
		c.Close()
	})
	return ln.Addr().String(), stop
}

type distServer struct {
	*catalogServer
	source *lakeSource
	stops  []func()
	// gate holds the workers' tasks back when a test asks it to.
	gate *taskGate
}

func startDistServer(t *testing.T, workers int, configure ...func(*Options)) *distServer {
	t.Helper()
	return startDistServerIn(t, t.TempDir(), workers, configure...)
}

// startDistServerIn starts a server whose tables' files, and whatever else
// tests write, are in dir, which its DuckDB and the workers may read.
func startDistServerIn(t *testing.T, dir string, workers int, configure ...func(*Options)) *distServer {
	t.Helper()
	ds := &distServer{source: &lakeSource{dir: dir, files: map[string][]distributed.File{}}, gate: newTaskGate()}
	var addrs []string
	for i := range workers {
		addr, stop := startTestWorker(t, dir, i, ds.gate)
		addrs = append(addrs, addr)
		ds.stops = append(ds.stops, stop)
	}
	// Partial results go to the server's spill directory, which its
	// DuckDB may read.
	spill := filepath.Join(t.TempDir(), "spill")
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: ds.source, Secret: testWorkerSecret, Version: "test", TempDir: spill, Threads: 1,
		Pool: distributed.PoolOptions{Addresses: addrs},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	cfg := config.Config{
		// The extensions production loads, where scripts/test.sh provides them.
		DuckDB:  config.DuckDB{TempDir: spill, ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")},
		Storage: config.Storage{AllowedLocations: []string{dir + "/"}},
	}
	ds.catalogServer = startCatalogServerWithConfig(t, cfg, append([]func(*Options){func(o *Options) { o.Distributed = co }}, configure...)...)
	conn, err := ds.srv.engine.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ds.source.conn = conn
	mustExec(t, ds.catalogServer.admin, "INSERT INTO lake.public.orders SELECT i, i % 7 FROM range(3, 2000) t(i)")
	return ds
}

// lastWorkers returns the workers column of the newest history record of
// query text.
func lastWorkers(t *testing.T, h *audit.MemoryStore, text string) int {
	t.Helper()
	qs, err := h.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if q.Text == text {
			return q.Workers
		}
	}
	t.Fatalf("no history for %q", text)
	return 0
}

func TestDistributedStatements(t *testing.T) {
	ds := startDistServer(t, 2)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ds.testServer.admin(t, mode)
			ctx := context.Background()
			query := "SELECT amount, count(*) AS n, sum(id) AS s FROM orders GROUP BY amount ORDER BY amount"
			read := func() string {
				rows, err := conn.Query(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				var b strings.Builder
				for rows.Next() {
					vals, err := rows.Values()
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprintln(&b, vals...)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return b.String()
			}

			mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
			mustExec(t, conn, "SET distributed_execution = off")
			local := read()
			if n := lastWorkers(t, ds.history, query); n != 0 {
				t.Errorf("with distributed_execution off, %d workers ran it", n)
			}
			mustExec(t, conn, "SET distributed_execution = on")
			var shown string
			if err := conn.QueryRow(ctx, "SHOW distributed_execution").Scan(&shown); err != nil || shown != "on" {
				t.Errorf("SHOW distributed_execution: %q, %v", shown, err)
			}
			if got := read(); got != local {
				t.Errorf("distributed result differs:\n%s\nlocal:\n%s", got, local)
			}
			if n := lastWorkers(t, ds.history, query); n != 2 {
				t.Errorf("history shows %d workers, want 2", n)
			}

			// Parameters are bound where the combined result is computed.
			var n int64
			having := "SELECT count(*) FROM (SELECT amount FROM orders GROUP BY amount HAVING count(*) > $1)"
			if err := conn.QueryRow(ctx, having, 280).Scan(&n); err != nil || n != 7 {
				t.Errorf("parameter in HAVING: %d, %v", n, err)
			}
			if mode != pgx.QueryExecModeSimpleProtocol {
				if w := lastWorkers(t, ds.history, having); w != 2 {
					t.Errorf("a statement with a parameter in HAVING ran on %d workers, want 2", w)
				}
			}

			// Inside a transaction a statement runs here.
			mustExec(t, conn, "BEGIN")
			if read() != local {
				t.Error("result in a transaction differs")
			}
			mustExec(t, conn, "COMMIT")
			if n := lastWorkers(t, ds.history, query); n != 0 {
				t.Errorf("in a transaction, %d workers ran it", n)
			}
			mustExec(t, conn, "RESET distributed_execution")
			if err := conn.QueryRow(ctx, "SHOW distributed_execution").Scan(&shown); err != nil || shown != "auto" {
				t.Errorf("after RESET: %q, %v", shown, err)
			}
			if _, err := conn.Exec(ctx, "SET distributed_execution = sometimes"); pgCode(err) != "22023" {
				t.Errorf("bad value: %v", err)
			}
		})
	}
}

// Grants hold: a user reads through workers only what they may read.
func TestDistributedGrants(t *testing.T) {
	ds := startDistServer(t, 2)
	nina := ds.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, nina, "SET distributed_execution = on")
	expectDenied(t, nina, "SELECT amount, count(*) FROM orders GROUP BY amount", "SELECT on table public.orders")
	mustExec(t, ds.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	query := "SELECT amount, count(*) FROM orders GROUP BY amount"
	if got := count(t, nina, "SELECT count(*) FROM ("+query+")"); got != 9 {
		t.Errorf("got %d groups", got)
	}
	expectAllowed(t, nina, query)
	if n := lastWorkers(t, ds.history, query); n != 2 {
		t.Errorf("history shows %d workers for a granted table", n)
	}
	expectDenied(t, nina, "SELECT o.amount, count(*) FROM orders o JOIN secrets s ON o.id = s.id GROUP BY 1", "SELECT on table public.secrets")
}

// When the workers are gone, statements run here.
func TestDistributedFallback(t *testing.T) {
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET distributed_execution = on")
	for _, stop := range ds.stops {
		stop()
	}
	query := "SELECT amount, count(*) FROM orders GROUP BY amount"
	if got := count(t, conn, "SELECT count(*) FROM ("+query+")"); got != 9 {
		t.Errorf("got %d groups", got)
	}
	expectAllowed(t, conn, query)
	if n := lastWorkers(t, ds.history, query); n != 0 {
		t.Errorf("history shows %d workers", n)
	}
}
