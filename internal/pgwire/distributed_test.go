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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
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
	// size, if set, is the size every table claims, so that auto mode
	// weighs it as a large one.
	size int64
	// missing, if set, lists files that are not there, which workers fail
	// to read.
	missing bool
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
	if s.size > 0 {
		t.Bytes = s.size
	}
	s.files[schema+"."+name] = files
	return t, nil
}

func (s *lakeSource) Files(_ context.Context, t *distributed.Table) ([]distributed.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	files := s.files[t.Schema+"."+t.Name]
	if s.missing {
		files = slices.Clone(files)
		for i := range files {
			files[i].Path += ".missing"
		}
	}
	return files, nil
}

// Refresh does nothing: Table wrote the table's current rows.
func (s *lakeSource) Refresh(context.Context, *distributed.Table) error { return nil }

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
	// A canceled task stops waiting as it is counted canceled, at once:
	// counted apart, a test that saw the cancel could still see it wait.
	select {
	case <-ch:
		g.mu.Lock()
		g.waiting--
		g.mu.Unlock()
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		g.waiting--
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
func startTestWorker(t *testing.T, dir string, i int, gate *taskGate) (string, *distributed.Worker, func()) {
	t.Helper()
	return startTestWorkerRB(t, dir, i, gate, "")
}

// startTestWorkerRB is startTestWorker with the rb extension at rb loaded
// before the worker's lockdown, as production workers load it; "" loads
// none.
func startTestWorkerRB(t *testing.T, dir string, i int, gate *taskGate, rb string) (string, *distributed.Worker, func()) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "worker")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		t.Fatal(err)
	}
	dsn := ""
	var load []string
	if rb != "" {
		dsn = "?allow_unsigned_extensions=true"
		load = []string{"LOAD " + quoteLiteral(rb), "SET allow_unsigned_extensions = false"}
	}
	c, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	setup, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range append(load,
		"SET allowed_directories = ["+quoteLiteral(dir+"/")+", "+quoteLiteral(tmp+"/")+"]",
		"SET enable_external_access = false",
	) {
		if _, err := setup.(driver.ExecerContext).ExecContext(context.Background(), stmt, nil); err != nil {
			t.Fatal(err)
		}
	}
	setup.Close()
	w, err := distributed.NewWorker(distributed.WorkerOptions{
		Engine: workerEngine{c, gate}, Secret: testWorkerSecret, Version: "test", ID: fmt.Sprintf("w%d", i), TempDir: tmp,
		Locations: []string{dir}, Cores: 1, MemoryBytes: 1 << 30,
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
	return ln.Addr().String(), w, stop
}

type distServer struct {
	*catalogServer
	source  *lakeSource
	workers []*distributed.Worker
	stops   []func()
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
	return startDistServerWith(t, dir, workers, nil, configure...)
}

// startDistServerWith is startDistServerIn with the coordinator's options
// changed by coordinate, if set.
func startDistServerWith(t *testing.T, dir string, workers int, coordinate func(*distributed.Options), configure ...func(*Options)) *distServer {
	t.Helper()
	return startDistServerRB(t, dir, workers, "", coordinate, configure...)
}

// startDistServerRB is startDistServerWith with workers that load the rb
// extension at rb, if set.
func startDistServerRB(t *testing.T, dir string, workers int, rb string, coordinate func(*distributed.Options), configure ...func(*Options)) *distServer {
	t.Helper()
	ds := &distServer{source: &lakeSource{dir: dir, files: map[string][]distributed.File{}}, gate: newTaskGate()}
	var addrs []string
	for i := range workers {
		addr, w, stop := startTestWorkerRB(t, dir, i, ds.gate, rb)
		addrs = append(addrs, addr)
		ds.workers = append(ds.workers, w)
		ds.stops = append(ds.stops, stop)
	}
	// Partial results go to the server's spill directory, which its
	// DuckDB may read.
	spill := filepath.Join(t.TempDir(), "spill")
	opts := distributed.Options{
		Source: ds.source, Secret: testWorkerSecret, Version: "test", TempDir: spill, Cores: 1, Storage: distributed.NewStorage(icebergio.LocalFS{}),
		Pool: distributed.PoolOptions{Addresses: addrs},
		// Exchanges between stages go under the warehouse, which here is dir.
		Warehouse: dir + "/",
	}
	if coordinate != nil {
		coordinate(&opts)
	}
	co, err := distributed.NewCoordinator(opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
	return lastRun(t, h, text).Workers
}

// lastRun returns the history's latest statement of text.
func lastRun(t *testing.T, h *audit.MemoryStore, text string) audit.Query {
	t.Helper()
	qs, err := h.Queries(context.Background(), audit.Viewer{All: true}, audit.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range qs {
		if q.Text == text {
			return q
		}
	}
	t.Fatalf("no history for %q", text)
	return audit.Query{}
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
			if q := lastRun(t, ds.history, query); q.Workers != 0 || q.PlanKey != "" {
				t.Errorf("with distributed_execution off, %d workers ran it, plan key %q", q.Workers, q.PlanKey)
			}
			mustExec(t, conn, "SET distributed_execution = on")
			var shown string
			if err := conn.QueryRow(ctx, "SHOW distributed_execution").Scan(&shown); err != nil || shown != "on" {
				t.Errorf("SHOW distributed_execution: %q, %v", shown, err)
			}
			if got := read(); got != local {
				t.Errorf("distributed result differs:\n%s\nlocal:\n%s", got, local)
			}
			if q := lastRun(t, ds.history, query); q.Workers != 2 || q.Stages != 1 || q.Exchanges != 0 || q.PlanKey == "" {
				t.Errorf("history shows %d workers, %d stages, %d exchanges and plan key %q; want 2, 1, 0 and a key", q.Workers, q.Stages, q.Exchanges, q.PlanKey)
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
	// Not an admin: as many workers as the table's size calls for.
	if n := lastWorkers(t, ds.history, query); n != 1 {
		t.Errorf("history shows %d workers for a granted table", n)
	}
	expectDenied(t, nina, "SELECT o.amount, count(*) FROM orders o JOIN secrets s ON o.id = s.id GROUP BY 1", "SELECT on table public.secrets")
}

// A worker about to run out of memory cancels its task (ShedMemory): the
// statement runs again in smaller tasks, and if those run out too, fails
// with SQLSTATE 53200 (out_of_memory), rather than run here, which would
// run out the same way.
func TestDistributedOutOfMemory(t *testing.T) {
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET distributed_execution = on")
	ctx := context.Background()
	query := "SELECT amount, count(*) FROM orders GROUP BY amount"
	// shed waits for a task to wait at the gate, and has the workers shed
	// what they run.
	shed := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for waiting, _ := ds.gate.counts(); waiting < 1 && time.Now().Before(deadline); waiting, _ = ds.gate.counts() {
			time.Sleep(10 * time.Millisecond)
		}
		_, before := ds.gate.counts()
		shed := false
		for _, w := range ds.workers {
			shed = w.ShedMemory(64<<20) || shed
		}
		if !shed {
			t.Fatal("no worker had a task to shed")
		}
		for _, canceled := ds.gate.counts(); canceled == before && time.Now().Before(deadline); _, canceled = ds.gate.counts() {
			time.Sleep(10 * time.Millisecond)
		}
	}
	run := func() <-chan error {
		errc := make(chan error, 1)
		go func() {
			rows, err := conn.Query(ctx, query)
			n := 0
			if err == nil {
				for rows.Next() {
					n++
				}
				err = rows.Err()
			}
			if err == nil && n != 9 {
				err = fmt.Errorf("%d groups", n)
			}
			errc <- err
		}()
		return errc
	}

	// Shed once: it runs again, and completes.
	release := sync.OnceFunc(ds.gate.hold())
	errc := run()
	shed()
	release()
	if err := <-errc; err != nil {
		t.Fatalf("after one shed: %v", err)
	}

	// Shed every time: the first run and its two in smaller tasks.
	release = sync.OnceFunc(ds.gate.hold())
	defer release()
	errc = run()
	for range 3 {
		shed()
	}
	release()
	err := <-errc
	if pgCode(err) != "53200" || !strings.Contains(err.Error(), "out of memory") {
		t.Fatalf("want 53200 for a worker out of memory, got %v", err)
	}
	if q := lastRun(t, ds.history, query); q.SQLState != "53200" {
		t.Errorf("history: %s %s", q.Outcome, q.SQLState)
	}
}

// A statement that auto mode runs here, and that runs out of memory here
// before it sends a row, runs again on workers, and the client gets its
// rows as if nothing had happened; the next time, it runs on workers
// directly. So does one the memory watch cancels (ShedMemory).
func TestDistributedAfterOutOfMemory(t *testing.T) {
	ds := startDistServer(t, 2)
	// Large enough for auto mode to weigh, small enough for one machine,
	// which reads it cold in about 45 s.
	ds.source.size = 300 << 20
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	ctx := context.Background()
	read := func(query string) string {
		t.Helper()
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
			t.Fatalf("%s: %v", query, err)
		}
		return b.String()
	}
	for _, c := range []struct {
		name, query string
		fail        func(ctx context.Context) error
	}{
		{"DuckDB out of memory", "SELECT amount, count(*) AS n FROM orders GROUP BY amount ORDER BY amount",
			func(context.Context) error {
				return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to pin block of size 256.0 KiB"}
			}},
		{"canceled by the memory watch", "SELECT amount, sum(id) AS s FROM orders GROUP BY amount ORDER BY amount",
			func(ctx context.Context) error {
				if !ds.srv.ShedMemory(64 << 20) {
					return errors.New("nothing to shed")
				}
				<-ctx.Done()
				return ctx.Err()
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustExec(t, conn, "SET distributed_execution = off")
			want := read(c.query)
			mustExec(t, conn, "RESET distributed_execution")
			var mu sync.Mutex
			failed := 0
			ds.srv.failLocal = func(ctx context.Context, st *statement) error {
				if !strings.Contains(st.query, "FROM orders GROUP BY amount") {
					return nil
				}
				mu.Lock()
				failed++
				mu.Unlock()
				return c.fail(ctx)
			}
			defer func() { ds.srv.failLocal = nil }()
			if got := read(c.query); got != want {
				t.Errorf("after running out of memory here:\n%s\nwant:\n%s", got, want)
			}
			if q := lastRun(t, ds.history, c.query); q.Workers == 0 || q.Outcome != audit.OutcomeOK || q.PlanKey == "" {
				t.Errorf("history: %d workers, %s %s, plan key %q", q.Workers, q.Outcome, q.SQLState, q.PlanKey)
			}
			if got := read(c.query); got != want {
				t.Errorf("run again:\n%s\nwant:\n%s", got, want)
			}
			mu.Lock()
			defer mu.Unlock()
			if failed != 1 {
				t.Errorf("ran here %d times, want once", failed)
			}
			if q := lastRun(t, ds.history, c.query); q.Workers == 0 {
				t.Error("run again, it ran here")
			}
		})
	}
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

// A statement whose workers fail, and that would not fit in this replica's
// memory, fails rather than run here, which would run out of memory after
// as long as one machine takes. The same statement over a table that fits
// still runs here.
func TestDistributedNoFallbackShortOfMemory(t *testing.T) {
	ds := startDistServerWith(t, t.TempDir(), 2, func(o *distributed.Options) { o.MemoryBytes = 64 << 20 })
	ds.source.size = 300 << 20
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET distributed_execution = on")
	// Workers fail to read the tables' files.
	ds.source.missing = true
	// A table joined to itself, both sides exchanged by key: a working set
	// several times this replica's 64 MiB.
	_, err := conn.Exec(context.Background(), "SELECT a.amount, count(*) FROM orders AS a JOIN orders AS b ON a.id = b.id GROUP BY a.amount")
	if err == nil || !strings.Contains(err.Error(), "needs more memory than this replica has") {
		t.Fatalf("want a failure for want of memory, got %v", err)
	}
	// At the table's own size, a few kilobytes, it fits, and runs here.
	ds.source.size = 0
	if got := count(t, conn, "SELECT count(*) FROM (SELECT a.amount, count(*) FROM orders AS a JOIN orders AS b ON a.id = b.id GROUP BY a.amount)"); got != 9 {
		t.Errorf("got %d groups", got)
	}
}

// EXPLAIN DISTRIBUTED says where a statement would run and why, its
// estimates, partitions and stages, without running a task; and why one
// that cannot run on workers runs here.
func TestDistributedExplain(t *testing.T) {
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	explain := func(query string) map[string]string {
		t.Helper()
		rows, err := conn.Query(context.Background(), "EXPLAIN DISTRIBUTED "+query)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]string{}
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			out[k] += v
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	release := ds.gate.hold()
	defer release()
	mustExec(t, conn, "SET distributed_execution = on")
	got := explain("SELECT amount, count(*) FROM orders GROUP BY amount")
	stage := ""
	for k, v := range got {
		if strings.HasPrefix(k, "stage 0 split ") && strings.Contains(v, "count_star()") {
			stage = k
		}
	}
	if got["runs on"] != "workers" || stage == "" || got["partitions"] == "" || !strings.Contains(got["estimate"], "workers") {
		t.Errorf("forced: %v", got)
	}
	if waiting, _ := ds.gate.counts(); waiting != 0 {
		t.Errorf("%d tasks ran for an EXPLAIN", waiting)
	}
	// Auto mode keeps a table this small here, and says why.
	mustExec(t, conn, "RESET distributed_execution")
	if got := explain("SELECT amount, count(*) FROM orders GROUP BY amount"); got["runs on"] != "this replica" || got["reason"] == "" {
		t.Errorf("auto: %v", got)
	}
	if got := explain("CREATE TABLE lake.public.never AS SELECT 1 AS x"); got["runs on"] != "this replica" || !strings.Contains(got["reason"], "only queries") {
		t.Errorf("a CREATE: %v", got)
	}
	if n := count(t, conn, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'never'"); n != 0 {
		t.Errorf("EXPLAIN DISTRIBUTED created the table")
	}
}

// Snowflake's FLATTEN, translated to unnests of Igloo's sf_ macros, runs
// on workers, which define the same macros; a session that redefines one
// of them computes something else, and runs such statements here.
func TestDistributedFlatten(t *testing.T) {
	ds := startDistServer(t, 2)
	mustExec(t, ds.admin, `CREATE TABLE lake.public.events AS SELECT i AS id,
		'["a' || (i % 3) || '", "b' || (i % 5) || '"]' AS tags FROM range(3000) t(i)`)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	// Each run's query is its own, to find its history record by: the
	// history records statements as translated, in the background.
	query := func(run int) string {
		return fmt.Sprintf(`SELECT f.value::string AS tag, COUNT(DISTINCT e.id) AS n, %d AS run FROM events e,
			LATERAL FLATTEN(input => e.tags) f GROUP BY 1 ORDER BY 1`, run)
	}
	read := func(run int) string {
		rows, err := conn.Query(ctx, query(run))
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(&b, vals[:2]...)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	workers := func(run int) int {
		marker := fmt.Sprintf(`%d AS "RUN"`, run)
		for range 100 {
			qs, err := ds.history.Queries(ctx, audit.Viewer{All: true}, audit.Page{Limit: 10})
			if err != nil {
				t.Fatalf("history: %v", err)
			}
			for _, q := range qs {
				if strings.Contains(q.Text, "sf_flatten_values") && strings.Contains(q.Text, marker) {
					return q.Workers
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("no history record of run %d", run)
		return 0
	}

	mustExec(t, conn, "SET distributed_execution = off")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	local := read(1)
	if !strings.Contains(local, "a0 1000") || strings.Count(local, "\n") != 8 {
		t.Fatalf("local result:\n%s", local)
	}
	mustExec(t, conn, "RESET sql_dialect")
	mustExec(t, conn, "SET distributed_execution = on")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	if got := read(2); got != local {
		t.Errorf("distributed result differs:\n%s\nlocal:\n%s", got, local)
	}
	if n := workers(2); n != 2 {
		t.Errorf("FLATTEN ran on %d workers, want 2", n)
	}

	mustExec(t, conn, "RESET sql_dialect")
	// Only each array's first element.
	mustExec(t, conn, "CREATE OR REPLACE TEMP MACRO sf_flatten_values(x, m) AS [json_extract(CAST(x AS VARCHAR), '$[0]')]")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	if read(3) == local {
		t.Fatal("redefining sf_flatten_values changed nothing")
	}
	if n := workers(3); n != 0 {
		t.Errorf("with a session's own sf_flatten_values, FLATTEN ran on %d workers", n)
	}
}

// A statement over a view reads the view's query on the workers, with the
// view's names resolved as in its body, and grants hold: a user who reads
// only what grants allow reads through a view what the grant check found
// it reads, and nothing else.
func TestDistributedViews(t *testing.T) {
	ds := startDistServer(t, 2)
	mustExec(t, ds.admin, "CREATE VIEW lake.public.v_orders (order_id, amount) AS SELECT id, amount FROM orders WHERE amount > 0")
	mustExec(t, ds.admin, "CREATE VIEW lake.sales.v_orders_codes AS SELECT o.id, o.amount, s.code FROM public.orders o LEFT JOIN public.secrets s ON o.id = s.id")
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	for _, query := range []string{
		"SELECT amount, count(*) AS n, sum(order_id) AS s FROM v_orders GROUP BY amount ORDER BY amount",
		"SELECT amount, count(code) AS n FROM lake.sales.v_orders_codes GROUP BY amount ORDER BY amount",
	} {
		mustExec(t, conn, "SET distributed_execution = off")
		local := rowsOf(t, conn, query)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := rowsOf(t, conn, query); got != local || local == "" {
			t.Errorf("%s: distributed\n%s\nlocal\n%s", query, got, local)
		}
		if n := lastWorkers(t, ds.history, query); n != 2 {
			t.Errorf("%s ran on %d workers, want 2", query, n)
		}
	}

	nina := ds.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ds.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, nina, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, nina, "SET distributed_execution = on")
	query := "SELECT amount, count(*) FROM v_orders GROUP BY amount"
	expectAllowed(t, nina, query)
	if n := lastWorkers(t, ds.history, query); n != 1 {
		t.Errorf("a view over a granted table ran on %d workers, want 1", n)
	}
	expectDenied(t, nina, "SELECT amount, count(code) FROM lake.sales.v_orders_codes GROUP BY amount", "SELECT on table public.secrets")
}

// rowsOf returns a query's rows, one line each.
func rowsOf(t *testing.T, conn *pgx.Conn, query string) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query)
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
