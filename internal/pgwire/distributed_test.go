package pgwire

import (
	"context"
	"database/sql/driver"
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
// an Iceberg table's data files are.
type lakeSource struct {
	dir string
	mu  sync.Mutex
	// conn is an admin's connection to the server, set once it runs.
	conn  *pgx.Conn
	files map[string][]distributed.File
}

func (s *lakeSource) Table(ctx context.Context, schema, name string) (*distributed.Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.conn.Query(ctx, fmt.Sprintf("DESCRIBE lake.%s.%s", quoteIdent(schema), quoteIdent(name)))
	if err != nil {
		return nil, err
	}
	var cols []distributed.Column
	var ids []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		c := distributed.Column{Name: vals[0].(string), Type: vals[1].(string), ID: len(cols) + 1}
		cols = append(cols, c)
		ids = append(ids, fmt.Sprintf("%s: %d", quoteIdent(c.Name), c.ID))
	}
	rows.Close()
	t := &distributed.Table{Schema: schema, Name: name, Columns: cols}
	var files []distributed.File
	for i := range 3 {
		path := filepath.Join(s.dir, fmt.Sprintf("%s.%s.%d.%d.parquet", schema, name, time.Now().UnixNano(), i))
		if _, err := s.conn.Exec(ctx, fmt.Sprintf("COPY (SELECT * FROM lake.%s.%s WHERE hash(rowid) %% 3 = %d) TO %s (FORMAT parquet, FIELD_IDS {%s})",
			quoteIdent(schema), quoteIdent(name), i, quoteLiteral(path), strings.Join(ids, ", "))); err != nil {
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

type workerEngine struct{ c *duckdb.Connector }

func (e workerEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := e.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c.(*duckdb.Conn), nil
}

// startTestWorker starts a worker that may read dir, and returns its
// address and a function that stops it.
func startTestWorker(t *testing.T, dir string, i int) (string, func()) {
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
		Engine: workerEngine{c}, Secret: testWorkerSecret, Version: "test", ID: fmt.Sprintf("w%d", i), TempDir: tmp,
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
}

func startDistServer(t *testing.T, workers int) *distServer {
	t.Helper()
	dir := t.TempDir()
	ds := &distServer{source: &lakeSource{dir: dir, files: map[string][]distributed.File{}}}
	var addrs []string
	for i := range workers {
		addr, stop := startTestWorker(t, dir, i)
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
		DuckDB:  config.DuckDB{TempDir: spill},
		Storage: config.Storage{AllowedLocations: []string{dir + "/"}},
	}
	ds.catalogServer = startCatalogServerWithConfig(t, cfg, func(o *Options) { o.Distributed = co })
	ds.source.conn = ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
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
