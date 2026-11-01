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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	wire "github.com/jeroenrinzema/psql-wire"

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
		id := strconv.Itoa(c.ID)
		if elem, ok := strings.CutSuffix(c.Type, "[]"); ok && !strings.ContainsAny(elem, "[(") {
			// A list of a plain type, nested as Iceberg's catalog says: its
			// element has a field ID of its own.
			c.Nested = &distributed.Nesting{Kind: "list", Fields: []distributed.Field{{Name: "element", ID: 1000 + c.ID}}}
			id = fmt.Sprintf("{__duckdb_field_id: %d, element: %d}", c.ID, 1000+c.ID)
		}
		cols = append(cols, c)
		ids = append(ids, fmt.Sprintf("%s: %s", quoteIdent(c.Name), id))
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
	return startTestWorkerCores(t, dir, i, gate, rb, 1)
}

// startTestWorkerCores is startTestWorkerRB of a worker that reports cores,
// for auto mode's estimates.
func startTestWorkerCores(t *testing.T, dir string, i int, gate *taskGate, rb string, cores int) (string, *distributed.Worker, func()) {
	t.Helper()
	return startTestWorkerLimited(t, dir, i, gate, rb, cores, 1<<30, "")
}

// startTestWorkerLimited is startTestWorkerCores with a worker that takes its
// memory for memory, and a DuckDB limited to limit, on two threads, if set.
func startTestWorkerLimited(t *testing.T, dir string, i int, gate *taskGate, rb string, cores int, memory int64, limit string) (string, *distributed.Worker, func()) {
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
	if limit != "" {
		load = append(load, "SET memory_limit = "+quoteLiteral(limit), "SET threads = 2", "SET temp_directory = "+quoteLiteral(filepath.Join(tmp, "spill")))
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
	// Workers score models, reading their blobs from the warehouse, dir.
	if err := registerModelScoring(c, dir); err != nil {
		t.Fatal(err)
	}
	// And forecast with the models of the pack in dir/model-pack.
	if err := registerForecasting(c, filepath.Join(dir, "model-pack")); err != nil {
		t.Fatal(err)
	}
	w, err := distributed.NewWorker(distributed.WorkerOptions{
		Engine: workerEngine{c, gate}, Secret: testWorkerSecret, Version: "test", ID: fmt.Sprintf("w%d", i), TempDir: tmp,
		Locations: []string{dir}, Cores: cores, MemoryBytes: memory,
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
	return startDistServerCores(t, dir, workers, 1, rb, coordinate, configure...)
}

// startDistServerCores is startDistServerRB with workers that report cores
// each, for auto mode's estimates.
func startDistServerCores(t *testing.T, dir string, workers, cores int, rb string, coordinate func(*distributed.Options), configure ...func(*Options)) *distServer {
	t.Helper()
	ds := &distServer{source: &lakeSource{dir: dir, files: map[string][]distributed.File{}}, gate: newTaskGate()}
	var addrs []string
	for i := range workers {
		addr, w, stop := startTestWorkerCores(t, dir, i, ds.gate, rb, cores)
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
		Models:  config.Models{Dir: filepath.Join(dir, "model-pack")},
	}
	ds.catalogServer = startCatalogServerWithConfig(t, cfg, append([]func(*Options){func(o *Options) { o.Distributed = co }}, configure...)...)
	// Models' blobs go to the warehouse, where the workers read them.
	ds.srv.modelRuntime.SetBlobs(localBlobs(dir))
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
	t.Parallel()
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
			// The groups of amount, whose values ANALYZE did not count, a
			// second stage completes, through an exchange.
			if q := lastRun(t, ds.history, query); q.Workers != 2 || q.Stages != 2 || q.Exchanges != 1 || q.PlanKey == "" {
				t.Errorf("history shows %d workers, %d stages, %d exchanges and plan key %q; want 2, 2, 1 and a key", q.Workers, q.Stages, q.Exchanges, q.PlanKey)
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
	t.Parallel()
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
//
// It does not run in parallel: a shed must take a task of the statement's
// latest attempt, and on a busy machine a task of one it gave up on can
// still be running when the next shed comes. Run alone, it can still fail
// that way, if rarely: shed waits for the tasks it saw waiting to see they
// were canceled, but not for one still on its way to the gate.
func TestDistributedOutOfMemory(t *testing.T) {
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET distributed_execution = on")
	ctx := context.Background()
	query := "SELECT amount, count(*) FROM orders GROUP BY amount"
	// shed waits for a task to wait at the gate, and has the workers shed
	// what they run. The statement then gives up on that attempt and
	// cancels its other tasks: shed waits until every task that waited has
	// seen it was canceled, so that the next shed finds none of them still
	// running (a task counts as waiting, and running, until it sees it).
	shed := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for waiting, _ := ds.gate.counts(); waiting < 1 && time.Now().Before(deadline); waiting, _ = ds.gate.counts() {
			time.Sleep(10 * time.Millisecond)
		}
		waiting, before := ds.gate.counts()
		shed := false
		for _, w := range ds.workers {
			shed = w.ShedMemory(64<<20) || shed
		}
		if !shed {
			t.Fatal("no worker had a task to shed")
		}
		for _, canceled := ds.gate.counts(); canceled < before+waiting && time.Now().Before(deadline); _, canceled = ds.gate.counts() {
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
	t.Parallel()
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

// A statement auto mode runs here, on an estimate that can run far short,
// runs on workers once it has run here past three times its estimate (and
// a minimum) before sending a row: the client gets its rows as if it had
// run there, the history marks it, and the next time it runs on workers
// directly. One that finishes in its time runs here, and a write is never
// stopped. At ten times the data, a report estimated at 51 s took 1,798 s
// here and 362 s on workers.
func TestDistributedGuard(t *testing.T) {
	ds := startDistServer(t, 2)
	// Large enough for auto mode to weigh, small enough for one machine,
	// which it estimates at about 45 s: far past the minimum the test
	// sets, which the guard waits for.
	ds.source.size = 300 << 20
	// Package variables: this test does not run in parallel.
	defer func(f, a float64, m time.Duration) { guardFactor, guardAlternative, guardMinimum = f, a, m }(guardFactor, guardAlternative, guardMinimum)
	guardFactor, guardAlternative, guardMinimum = 0, 0, 300*time.Millisecond
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
	slow := "SELECT amount, count(*) AS n FROM orders GROUP BY amount ORDER BY amount"
	quick := "SELECT amount, max(id) AS m FROM orders GROUP BY amount ORDER BY amount"
	mustExec(t, conn, "SET distributed_execution = off")
	wantSlow, wantQuick := read(slow), read(quick)
	mustExec(t, conn, "RESET distributed_execution")

	var slowRuns, quickRuns, writeRuns atomic.Int32
	ds.srv.failLocal = func(ctx context.Context, st *statement) error {
		switch {
		case strings.Contains(st.query, "CREATE TABLE"):
			// A write here that runs past the guard's minimum.
			writeRuns.Add(1)
			time.Sleep(2 * guardMinimum)
		case strings.Contains(st.query, "count(*) AS n FROM orders"):
			// It runs here until stopped.
			slowRuns.Add(1)
			<-ctx.Done()
			return ctx.Err()
		case strings.Contains(st.query, "max(id) AS m FROM orders"):
			quickRuns.Add(1)
		}
		return nil
	}
	defer func() { ds.srv.failLocal = nil }()

	if got := read(slow); got != wantSlow {
		t.Errorf("guarded:\n%s\nwant:\n%s", got, wantSlow)
	}
	if q := lastRun(t, ds.history, slow); q.Workers == 0 || q.Outcome != audit.OutcomeOK || q.PlanKey == "" {
		t.Errorf("guarded, history: %d workers, %s %s, plan key %q", q.Workers, q.Outcome, q.SQLState, q.PlanKey)
	}
	if got := read(slow); got != wantSlow {
		t.Errorf("run again:\n%s\nwant:\n%s", got, wantSlow)
	}
	if q := lastRun(t, ds.history, slow); q.Workers == 0 {
		t.Error("run again, it ran here")
	}

	// In its time: here, as auto mode chose.
	if got := read(quick); got != wantQuick {
		t.Errorf("in its time:\n%s\nwant:\n%s", got, wantQuick)
	}
	if q := lastRun(t, ds.history, quick); q.Workers != 0 {
		t.Errorf("a statement that ran in its time here ran on %d workers", q.Workers)
	}

	// A write runs here to the end, however long.
	mustExec(t, conn, "CREATE TABLE memory.main.guarded AS SELECT amount, count(*) AS n FROM orders GROUP BY amount")
	if n := count(t, conn, "SELECT count(*) FROM memory.main.guarded"); n != 9 {
		t.Errorf("the write left %d rows", n)
	}
	if slowRuns.Load() != 1 || quickRuns.Load() != 1 || writeRuns.Load() != 1 {
		t.Errorf("ran here %d, %d and %d times, want each once", slowRuns.Load(), quickRuns.Load(), writeRuns.Load())
	}
}

// A guard does not stop a statement that has begun sending its rows, and
// once it stopped one, the statement sends nothing.
func TestGuardGate(t *testing.T) {
	const after = 50 * time.Millisecond
	ctx := context.Background()

	streaming := &tagWriter{DataWriter: &countingWriter{}}
	g := guardLocal(ctx, after, streaming)
	if err := streaming.Row([]any{1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * after)
	if fired, _ := g.stop(streaming); fired {
		t.Error("a statement sending rows was stopped")
	}
	if err := streaming.Row([]any{2}); err != nil {
		t.Errorf("rows after the guard stopped watching: %v", err)
	}

	quiet := &tagWriter{DataWriter: &countingWriter{}}
	g = guardLocal(ctx, after, quiet)
	select {
	case <-g.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the guard did not stop a quiet statement")
	}
	if err := quiet.Row([]any{1}); !errors.Is(err, errGuardFired) {
		t.Errorf("a row after the guard fired: %v", err)
	}
	if err := quiet.Complete("SELECT 0"); !errors.Is(err, errGuardFired) {
		t.Errorf("completing after the guard fired: %v", err)
	}
	if fired, ran := g.stop(quiet); !fired || ran < after {
		t.Errorf("fired %v after %s", fired, ran)
	}
	if n := quiet.DataWriter.(*countingWriter).rows; n != 0 {
		t.Errorf("%d rows reached the client after the guard fired", n)
	}
}

// TestGuardAfter checks when the guard runs a statement auto mode ran here
// on workers instead: past three times its estimate here, or its estimate
// once while other statements run beside it, and only once its run here
// has taken as long as the workers' estimate, start included; never before
// the minimum or a run's budget.
func TestGuardAfter(t *testing.T) {
	const s = time.Second
	for _, c := range []struct {
		name             string
		est, alt, budget time.Duration
		load             int
		want             time.Duration
	}{
		// E1-like at ten times the data: estimated at 13 s here and 30 s on
		// workers; it took 58 to 97 s here and 50 s on workers. The fixed
		// 90 s floor let it run 90 s here first.
		{"E1-like at 10x", 13 * s, 30 * s, 0, 0, 39 * s},
		// Workers stopped: their start counts, and the run here may take as
		// long before it moves.
		{"E1-like at 10x, workers stopped", 13 * s, 75 * s, 0, 0, 75 * s},
		// F3-like at ten times the data: 51 s here by the estimate, 1,798 s
		// in fact; 168 s estimated on workers, 150 to 190 s in fact.
		{"F3-like at 10x", 51 * s, 168 * s, 0, 0, 168 * s},
		// A short statement waits the minimum, not three times its estimate.
		{"short", 2 * s, 5 * s, 0, 0, guardMinimum},
		// Beside other statements, which it takes cores from: its estimate
		// once, here the workers' estimate. A report estimated at 65 s here
		// ran 577 s beside 16 users' dashboards, the guard waiting 17
		// times three times its estimate.
		{"beside seven others", 13 * s, 30 * s, 0, 7, 30 * s},
		{"F5-like beside dashboards", 65 * s, 61 * s, 0, 16, 65 * s},
		// Workers estimated far slower than the run here: it is not moved
		// before it has run as long as they would take.
		{"workers far slower", 10 * s, 600 * s, 0, 0, 600 * s},
		// Tried here after a run of 200 s on workers.
		{"budget", 14 * s, 30 * s, 200 * s, 0, 200 * s},
		{"workers' estimate unknown", 13 * s, 0, 0, 0, 90 * s},
	} {
		if got := guardAfter(c.est, c.alt, c.budget, c.load); got != c.want {
			t.Errorf("%s: after %s, want %s", c.name, got, c.want)
		}
	}
	// Moving once the run here has taken the workers' time bounds the
	// loss: a statement that would have run just past the guard here takes
	// the guard's time and then the workers', about twice the faster side
	// at most when the workers' estimate holds and the guard is not held
	// back by three times the estimate here.
	for _, c := range []struct{ est, alt time.Duration }{{13 * s, 30 * s}, {51 * s, 168 * s}, {5 * s, 40 * s}} {
		after := guardAfter(c.est, c.alt, 0, 0)
		if worst := float64(after+c.alt) / float64(c.alt); after > max(3*c.est, c.alt, guardMinimum) || worst > 2 && after != 3*c.est {
			t.Errorf("estimated %s here, %s on workers: after %s, up to %.1f times the workers' time", c.est, c.alt, after, worst)
		}
	}
}

// A heavy statement auto mode runs here does not take the replica's cores
// from short ones: started beside another statement, it runs on workers;
// running here alone when one starts beside it, the guard moves it to the
// workers within the guard's time, not three times its estimate; and the
// short statement beside it completes within a bound meanwhile. At ten
// times the data, beside 16 users' dashboards, a report estimated at 65 s
// here ran 577 s, the dashboards' median rising from 4 ms to 23 s.
func TestHeavyBesideShort(t *testing.T) {
	ds := startDistServer(t, 2)
	// Estimated at about 45 s here, under the threshold alone; twice that
	// beside another statement, over it, and the workers far faster.
	ds.source.size = 300 << 20
	defer func(a, s float64, m time.Duration) { guardAlternative, guardShared, guardMinimum = a, s, m }(guardAlternative, guardShared, guardMinimum)
	guardAlternative, guardShared, guardMinimum = 0, 0, 300*time.Millisecond
	ctx := context.Background()
	heavyConn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	shortConn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, c := range []*pgx.Conn{heavyConn, shortConn} {
		mustExec(t, c, "SET search_path = 'lake.public,memory.main'")
	}
	read := func(conn *pgx.Conn, query string) (string, error) {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return "", err
			}
			fmt.Fprintln(&b, vals...)
		}
		return b.String(), rows.Err()
	}
	guarded := "SELECT amount, count(*) AS n FROM orders GROUP BY amount ORDER BY amount"
	placed := "SELECT amount, sum(id) AS s FROM orders GROUP BY amount ORDER BY amount"
	mustExec(t, heavyConn, "SET distributed_execution = off")
	wantGuarded, err := read(heavyConn, guarded)
	if err != nil {
		t.Fatal(err)
	}
	wantPlaced, err := read(heavyConn, placed)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, heavyConn, "RESET distributed_execution")

	var heavyHere, placedHere atomic.Int32
	besideStarted := make(chan struct{}, 4)
	release := make(chan struct{})
	ds.srv.failLocal = func(ctx context.Context, st *statement) error {
		switch {
		case strings.Contains(st.query, "count(*) AS n FROM orders"):
			// Heavy here: it runs until stopped.
			heavyHere.Add(1)
			<-ctx.Done()
			return ctx.Err()
		case strings.Contains(st.query, "sum(id) AS s FROM orders"):
			placedHere.Add(1)
		case strings.Contains(st.query, "'beside'"):
			// A short statement's session, running while the test holds it.
			besideStarted <- struct{}{}
			<-release
		}
		return nil
	}
	defer func() { ds.srv.failLocal = nil }()

	// Alone, the heavy statement runs here; once a statement starts
	// beside it, the guard moves it to the workers.
	heavyDone := make(chan error, 1)
	var got string
	go func() {
		var err error
		got, err = read(heavyConn, guarded)
		heavyDone <- err
	}()
	deadline := time.Now().Add(20 * time.Second)
	for heavyHere.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if heavyHere.Load() == 0 {
		t.Fatal("the heavy statement did not start here")
	}
	besideDone := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := read(shortConn, "SELECT 'beside' AS x")
		besideDone <- err
	}()
	<-besideStarted
	select {
	case err := <-heavyDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the guard did not move the heavy statement while another ran beside it")
	}
	moved := time.Since(start)
	close(release)
	if err := <-besideDone; err != nil {
		t.Fatal(err)
	}
	if got != wantGuarded {
		t.Errorf("moved:\n%s\nwant:\n%s", got, wantGuarded)
	}
	if q := lastRun(t, ds.history, guarded); q.Workers == 0 || !strings.Contains(q.Placement, "moved by the guard") {
		t.Errorf("moved: %d workers, placement %q", q.Workers, q.Placement)
	}
	// Within the guard's minimum and a check or two, far short of three
	// times its estimate of about 45 s.
	if moved > 10*time.Second {
		t.Errorf("moved after %s beside another statement", moved)
	}

	// Started beside another statement, a heavy statement runs on workers
	// at once, and the short statement beside it completes meanwhile.
	release = make(chan struct{})
	besideDone = make(chan error, 1)
	go func() {
		_, err := read(shortConn, "SELECT 'beside' AS x")
		besideDone <- err
	}()
	<-besideStarted
	got, err = read(heavyConn, placed)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	shortStart := time.Now()
	if err := <-besideDone; err != nil {
		t.Fatal(err)
	}
	if short := time.Since(shortStart); short > 5*time.Second {
		t.Errorf("the short statement took %s to finish once released", short)
	}
	if got != wantPlaced || placedHere.Load() != 0 {
		t.Errorf("placed: ran here %d times\n%s\nwant:\n%s", placedHere.Load(), got, wantPlaced)
	}
	if q := lastRun(t, ds.history, placed); q.Workers == 0 || !strings.Contains(q.Placement, "other statements run on this replica") {
		t.Errorf("placed: %d workers, placement %q", q.Workers, q.Placement)
	}
}

// countingWriter counts the rows written to it.
type countingWriter struct {
	rows int
}

func (w *countingWriter) Row([]any) error                                  { w.rows++; return nil }
func (w *countingWriter) Written() uint32                                  { return uint32(w.rows) }
func (w *countingWriter) Empty() error                                     { return nil }
func (w *countingWriter) Columns() wire.Columns                            { return nil }
func (w *countingWriter) Complete(string) error                            { return nil }
func (w *countingWriter) CopyIn(wire.FormatCode) (*wire.CopyReader, error) { return nil, nil }

// When the workers are gone, statements run here.
func TestDistributedFallback(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	// The session's statement timeout ends it as it ends any statement:
	// planning reads the session's catalog and describes its parts, which
	// takes more than a millisecond.
	mustExec(t, conn, "SET distributed_execution = on")
	mustExec(t, conn, "SET statement_timeout = '1ms'")
	_, err := conn.Exec(context.Background(), "EXPLAIN DISTRIBUTED WITH g AS (SELECT amount, count(*) AS n FROM orders GROUP BY amount) "+
		"SELECT a.amount, a.n + b.n FROM g AS a JOIN g AS b ON a.amount = b.amount")
	if pgCode(err) != "57014" {
		t.Errorf("EXPLAIN DISTRIBUTED past its statement timeout: %v", err)
	}
	mustExec(t, conn, "RESET statement_timeout")
	if got := explain("SELECT amount, count(*) FROM orders GROUP BY amount"); got["runs on"] != "workers" {
		t.Errorf("after the timeout: %v", got)
	}
}

// Snowflake's FLATTEN, translated to unnests of Igloo's sf_ macros, runs
// on workers, which define the same macros; a session that redefines one
// of them computes something else, and runs such statements here.
func TestDistributedFlatten(t *testing.T) {
	t.Parallel()
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
				if strings.Contains(q.Text, "sf_flatten_") && strings.Contains(q.Text, marker) {
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
	// Only each array's first element, of FLATTEN's values and of those read
	// as text, which with the rb extension do not call sf_flatten_values
	// (see snowflake.MacrosFor).
	mustExec(t, conn, "CREATE OR REPLACE TEMP MACRO sf_flatten_values(x, m) AS [json_extract(CAST(x AS VARCHAR), '$[0]')]")
	mustExec(t, conn, "CREATE OR REPLACE TEMP MACRO sf_flatten_strings(x, m) AS [json_extract_string(CAST(x AS VARCHAR), '$[0]')]")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	if read(3) == local {
		t.Fatal("redefining FLATTEN's macros changed nothing")
	}
	if n := workers(3); n != 0 {
		t.Errorf("with a session's own FLATTEN macros, FLATTEN ran on %d workers", n)
	}
}

// A statement over a view reads the view's query on the workers, with the
// view's names resolved as in its body, and grants hold: a user who reads
// only what grants allow reads through a view what the grant check found
// it reads, and nothing else.
func TestDistributedViews(t *testing.T) {
	t.Parallel()
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
