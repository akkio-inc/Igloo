package distributed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/memlimit"
)

const failureQuery = `SELECT l_returnflag, l_linestatus, sum(l_quantity), count(*), avg(l_discount)
	FROM lineitem GROUP BY ALL ORDER BY ALL`

// A worker that dies in the middle of a task: its share runs on another
// worker and the result is complete.
func TestWorkerDiesMidTask(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	var killed atomic.Int32
	h.workers[1].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		killed.Add(1)
		// Drop the connection as a crashed process would.
		conn, _, err := rw.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return false
	}
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}
	if killed.Load() == 0 {
		t.Fatal("the failing worker got no task")
	}
}

// A worker whose address no longer answers is skipped.
func TestWorkerGone(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	// Refresh the pool's view while the worker is up, then stop it.
	h.co.pool.refresh(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.workers[2].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	h.lns[2].Close()
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}
}

// With every worker failing, the statement fails with an error that sends
// it to the coordinator, not a data error.
func TestAllWorkersFail(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			http.Error(rw, "boom", http.StatusInternalServerError)
			return false
		}
	}
	_, _, _, _, err := h.run(t.Context(), failureQuery, ModeOn)
	if err == nil || IsUnsupported(err) || dataError(err) {
		t.Fatalf("want a worker failure, got %v", err)
	}
}

// A worker about to run out of memory cancels the task that has run longest
// there (memlimit.Watch, Worker.ShedMemory), and the statement fails as out
// of memory: run again on another worker, the task would run out the same
// way.
func TestWorkerShedsMemory(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	if h.workers[0].ShedMemory(64 << 20) {
		t.Fatal("shed a task with none running")
	}
	started := make(chan struct{}, 4)
	h.workers[0].opts.Engine = hangingEngine{started}
	// Memory runs low once the worker runs a task.
	var low atomic.Bool
	go func() {
		<-started
		low.Store(true)
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	const total = 32 << 30
	go memlimit.WatchFunc(ctx, total, time.Millisecond, func() (int64, bool) {
		if low.Load() {
			return memlimit.Low(total) / 2, true
		}
		return total / 2, true
	}, h.workers[0].ShedMemory)
	_, _, _, _, err := h.run(ctx, failureQuery, ModeOn)
	if !OutOfMemory(err) || IsUnsupported(err) || dataError(err) {
		t.Fatalf("want the worker's out of memory, got %v", err)
	}
}

// oomEngine fails a worker's tasks as DuckDB does out of its memory limit,
// the first fails of them (all, if fails is negative), and runs the rest.
type oomEngine struct {
	inner Connector
	fails *atomic.Int32
	tried *atomic.Int32
}

func (e oomEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	e.tried.Add(1)
	for {
		n := e.fails.Load()
		if n == 0 {
			return e.inner.Connect(ctx)
		}
		if n < 0 || e.fails.CompareAndSwap(n, n-1) {
			return nil, &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory,
				Msg: "Out of Memory Error: failed to allocate data of size 2.0 MiB (24.5 GiB/24.6 GiB used)"}
		}
	}
}

// A task that runs out of DuckDB's memory on a worker fails as the memory
// watch's would, not as a broken worker: the statement runs again in
// smaller tasks, over twice the partitions, and returns what one machine
// does; when those run out too, it fails as out of memory rather than run
// on the coordinator.
func TestWorkerOutOfMemory(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	h.co.maxBroadcast = 1
	q := `SELECT o.o_orderpriority, count(*), sum(l.l_quantity) FROM lineitem AS l JOIN orders AS o ON o.o_orderkey = l.l_orderkey
		GROUP BY 1 ORDER BY 1`
	var fails, tried atomic.Int32
	inner := h.workers[0].opts.Engine
	h.workers[0].opts.Engine = oomEngine{inner, &fails, &tried}
	// Out of memory once: the statement runs again, and completes.
	fails.Store(1)
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	if fails.Load() > 0 || tried.Load() < 2 {
		t.Errorf("tasks tried %d times, %d failures left", tried.Load(), fails.Load())
	}
	// Out of memory every time: it fails as out of memory, after running
	// again memoryRetries times.
	fails.Store(-1)
	tried.Store(0)
	for _, w := range h.workers[1:] {
		w.opts.Engine = oomEngine{w.opts.Engine, &fails, &tried}
	}
	_, _, _, _, err := h.run(t.Context(), q, ModeOn)
	if !OutOfMemory(err) || IsUnsupported(err) || dataError(err) || !strings.Contains(err.Error(), "24.5 GiB") {
		t.Fatalf("want the task's out of memory, got %v", err)
	}
	if n := tried.Load(); n < memoryRetries+1 {
		t.Errorf("tasks tried %d times, want a run and %d more", n, memoryRetries)
	}
}

// A task over one partition that ran out of memory reads it all again in
// smaller tasks: at a hundred times the data, a report's tasks over one
// partition of 52.5 million rows ran out on every attempt at 202
// partitions, of exchanges of 10.6 billion rows on 16 workers. The
// statement runs again over four times the partitions, but no more than
// leave writers files of a million rows, nor maxRetryPartitions; a small
// exchange's statement runs again over twice its partitions, as any other.
func TestRetryPartitions(t *testing.T) {
	for _, c := range []struct {
		parts   int
		rows    int64
		workers int
		want    int
	}{
		{202, 10_605_000_000, 16, 632},
		{202, 1e12, 16, 808},
		{512, 1e12, 16, maxRetryPartitions},
		{32, 50_000_000, 16, 64},
		{200, 50_000_000, 16, maxPartitions},
		{256, 0, 16, 256},
	} {
		if got := retryPartitions(c.parts, c.rows, c.workers); got != c.want {
			t.Errorf("retryPartitions(%d, %d, %d) = %d, want %d", c.parts, c.rows, c.workers, got, c.want)
		}
	}
	err := &onePartition{err: fmt.Errorf("%w: a task on w:1: out of memory", ErrWorkerMemory), rows: 10}
	var one *onePartition
	if !OutOfMemory(err) || !errors.As(error(err), &one) || one.rows != 10 {
		t.Errorf("%v is not a worker out of memory over one partition", err)
	}
}

// A worker runs as many tasks at once as its cores and memory allow
// (taskSlots), one for the harness's, and the others wait: two stages that
// scan lineitem side by side, then a join's stage of a task per partition,
// run one task at a time, and the statement returns what one machine does.
func TestWorkerTaskSlots(t *testing.T) {
	for _, c := range []struct {
		cores  int
		memory int64
		want   int
	}{{2, 4 << 30, 1}, {8, 25 << 30, 2}, {16, 49 << 30, 4}, {64, 16 << 30, 2}, {8, 0, 2}} {
		if got := taskSlots(c.cores, c.memory); got != c.want {
			t.Errorf("taskSlots(%d, %d) = %d, want %d", c.cores, c.memory, got, c.want)
		}
	}
	h := newHarness(t, 0.01, 3, 1)
	h.co.maxBroadcast = h.tables["customer"].Bytes
	w := h.workers[0]
	var most atomic.Int32
	w.opts.Engine = countingEngine{Connector: w.opts.Engine, w: w, most: &most}
	if !h.check(`SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`) {
		t.Fatal("did not run distributed")
	}
	if n := most.Load(); n != 1 {
		t.Errorf("%d tasks ran at once, want 1", n)
	}
}

// countingEngine records the most tasks its worker ran at once.
type countingEngine struct {
	Connector
	w    *Worker
	most *atomic.Int32
}

func (e countingEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	e.w.runningMu.Lock()
	n := int32(len(e.w.running))
	e.w.runningMu.Unlock()
	for m := e.most.Load(); n > m && !e.most.CompareAndSwap(m, n); m = e.most.Load() {
	}
	// Long enough for the other tasks to arrive.
	time.Sleep(20 * time.Millisecond)
	return e.Connector.Connect(ctx)
}

// hangingEngine runs tasks until they are canceled, saying when each
// starts.
type hangingEngine struct{ started chan<- struct{} }

func (e hangingEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	e.started <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

// Canceling a statement stops its tasks on the workers.
func TestCancel(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	stopped := make(chan struct{}, 2)
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			// The server notices a client going away once it has read the
			// request.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			stopped <- struct{}{}
			return false
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, _, _, err := h.run(ctx, failureQuery, ModeOn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the deadline, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("took %s to stop", time.Since(start))
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker's task was not canceled")
	}
}

// A task writes one file for each partition of its exchange, however many
// partitions it has: past DuckDB's 100 open files, a partitioned COPY
// closed files and started new ones, and a task wrote tens of thousands.
func TestShuffleFiles(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	w := h.workers[0]
	conn, err := w.opts.Engine.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	const parts = 200
	dir := filepath.Join(h.dir, "warehouse", config.ShuffleDir, "q", "s0", "0", "a0")
	cols := []Column{{Name: "i", Type: "BIGINT"}, {Name: "s", Type: "VARCHAR"}, {Name: "__b", Type: "INTEGER"}}
	res, err := w.shuffle(t.Context(), conn, dir,
		fmt.Sprintf("SELECT i, 'x' || (i %% 1000) AS s, CAST(hash(i) %% %d AS INTEGER) AS __b FROM range(3000000) t(i)", parts), cols, parts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 3000000 || len(res.Files) != parts {
		t.Errorf("%d rows in %d files, want 3000000 in %d", res.Rows, len(res.Files), parts)
	}
	// What COPY says it wrote, file by file.
	for _, f := range res.Files {
		if info, err := os.Stat(f.Path); err != nil || f.Size != info.Size() || f.Rows == 0 {
			t.Fatalf("%s: %d rows of %d bytes, stat %v", f.Path, f.Rows, f.Size, err)
		}
	}
}

// An error in the data, such as a failed cast, is the statement's error,
// as it would be on one machine.
func TestDataError(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	_, _, _, _, err := h.run(t.Context(), `SELECT sum(CAST(l_comment AS INTEGER)) FROM lineitem`, ModeOn)
	var de *duckdb.Error
	if !errors.As(err, &de) || de.Type != duckdb.ErrorTypeConversion || !dataError(err) {
		t.Fatalf("want a conversion error, got %v", err)
	}
	if !strings.Contains(de.Msg, "INT32") && !strings.Contains(strings.ToLower(de.Msg), "convert") {
		t.Logf("message: %s", de.Msg)
	}
}

// Another secret cannot talk to the workers, and a task's files must lie
// in the warehouse.
func TestWorkerRefusesStrangers(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	other, err := newTransport(strings.Repeat("x", MinSecretLength))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: other.client}}
	if _, err := client.Get("https://" + h.lns[0].Addr().String() + statusPath); err == nil {
		t.Fatal("a client with another secret was accepted")
	}
	if resp, err := http.Get("http://" + h.lns[0].Addr().String() + statusPath); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("a plain HTTP client was accepted")
		}
	}

	w := h.workers[0]
	for _, path := range []string{"/etc/passwd", h.dir + "/warehouse/../secret.parquet", "s3://elsewhere/x.parquet"} {
		_, err := w.viewStatement(viewDef{Name: "__igloo_t0", Columns: []Column{{Name: "a", Type: "INTEGER", ID: 1}}, Files: []string{path}}, "")
		if err == nil {
			t.Errorf("view over %s was accepted", path)
		}
	}
	if _, err := w.viewStatement(viewDef{Name: "x; DROP TABLE y", Columns: []Column{{Name: "a", Type: "INTEGER", ID: 1}}}, ""); err == nil {
		t.Error("a view with a bad name was accepted")
	}
}

// A worker runs only SQL that reads its task's views: nothing that names a
// file, a table of a database or a table by name, whoever sent it.
func TestWorkerReadsOnlyItsViews(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	w := h.workers[0]
	li := h.tables["lineitem"]
	file := h.files[li.key()][0].Path
	view := viewDef{Name: "__igloo_split", Columns: li.Columns, Files: []string{file}}
	run := func(sql string) error {
		path, _, err := w.run(t.Context(), &taskRequest{Query: "q", Version: "test", Views: []viewDef{view}, SQL: sql})
		if path != "" {
			_ = os.Remove(path)
		}
		return err
	}
	if err := run("SELECT count(*) FROM __igloo_split WHERE l_quantity > (SELECT avg(l_quantity) FROM __igloo_split)"); err != nil {
		t.Errorf("a fragment over its views: %v", err)
	}
	if err := run("WITH x AS (SELECT * FROM __igloo_split) SELECT count(*) FROM x"); err != nil {
		t.Errorf("a fragment with a common table expression: %v", err)
	}
	if err := run("SELECT count(*) FROM __igloo_split, unnest([l_orderkey, l_partkey]) AS u(k), json_each(to_json([l_comment]))"); err != nil {
		t.Errorf("a fragment that unnests its rows' values: %v", err)
	}
	if err := run("SELECT count(*) FROM __igloo_split, range(2), generate_series(1, 3)"); err != nil {
		t.Errorf("a fragment joined to series of literals: %v", err)
	}
	for _, sql := range []string{
		"SELECT count(*) FROM read_parquet(" + quoteLiteral(file) + ")",
		"SELECT count(*) FROM __igloo_split, unnest((SELECT list(l_orderkey) FROM read_parquet(" + quoteLiteral(file) + ")))",
		"SELECT count(*) FROM __igloo_split, main.unnest([1])",
		"SELECT count(*) FROM __igloo_split, range(100000)",
		"SELECT count(*) FROM __igloo_split, range(l_orderkey)",
		"SELECT count(*) FROM " + quoteLiteral(file),
		"SELECT count(*) FROM __igloo_split WHERE l_orderkey IN (SELECT l_orderkey FROM read_parquet(" + quoteLiteral(file) + "))",
		"SELECT count(*) FROM main.lineitem",
		"SELECT count(*) FROM lineitem",
		"SELECT * FROM (SUMMARIZE __igloo_split)",
		"SELECT 1; SELECT 2",
	} {
		if err := run(sql); !IsUnsupported(err) {
			t.Errorf("%s: want it refused, got %v", sql, err)
		}
	}
}

// A worker that answers a task with an error ran it and is healthy: the
// statement goes back to the coordinator after that one answer, no worker
// is set aside, and no other worker is asked. (One user's statement that
// fails the same way everywhere must not take every worker out for
// everyone.)
func TestExecutionErrorKeepsWorkers(t *testing.T) {
	h := newHarness(t, 0.01, 3, 3)
	if n := len(h.co.pool.ready(t.Context())); n != 3 {
		t.Fatalf("%d workers ready before, want 3", n)
	}
	var asked atomic.Int32
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			asked.Add(1)
			w.fail(rw, http.StatusUnprocessableEntity, &taskErr{Message: "Binder Error: something", Type: int(duckdb.ErrorTypeBinder)})
			return false
		}
	}
	h.co.pool.refresh(t.Context())
	q := `SELECT l_returnflag, count(*) FROM lineitem GROUP BY 1`
	for range 3 {
		_, _, _, _, err := h.run(t.Context(), q, ModeOn)
		if err == nil || IsUnsupported(err) || dataError(err) {
			t.Fatalf("want an error that sends the statement to the coordinator, got %v", err)
		}
		var te *transportError
		if errors.As(err, &te) {
			t.Fatalf("an answered error taken for a transport failure: %v", err)
		}
	}
	if n := len(h.co.pool.ready(t.Context())); n != 3 {
		t.Errorf("%d workers usable after statements that failed on them, want 3", n)
	}
	// Each statement asked as many workers as it has tasks, once each.
	if n := asked.Load(); n > 9 {
		t.Errorf("%d tasks sent for three statements of three tasks", n)
	}
	// A worker that fails as a server is retried elsewhere and set aside.
	for _, w := range h.workers {
		w.beforeTask = nil
	}
	h.workers[0].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		http.Error(rw, "boom", http.StatusServiceUnavailable)
		return false
	}
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	if n := len(h.co.pool.ready(t.Context())); n != 2 {
		t.Errorf("%d workers usable after one failed as a server, want 2", n)
	}
}

// LIMIT and OFFSET past what workers could be asked for, or past what the
// coordinator should keep, run on one machine.
func TestLimitOverflow(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	for _, q := range []string{
		`SELECT l_orderkey, l_linenumber FROM lineitem WHERE l_quantity > 10 ORDER BY l_orderkey, l_linenumber LIMIT 9223372036854775807 OFFSET 5`,
		`SELECT l_orderkey FROM lineitem ORDER BY 1 LIMIT 2000000`,
	} {
		if _, _, _, _, err := h.run(t.Context(), q, ModeOn); !IsUnsupported(err) {
			t.Errorf("%s: want it to run on one machine, got %v", q, err)
		}
	}
	if n := len(h.co.pool.ready(t.Context())); n != 2 {
		t.Errorf("%d workers usable, want 2", n)
	}
}

// Partial results wait on the coordinator's disk until every worker has
// answered; past the spool's limit the statement runs on the coordinator
// instead, and a plain filtered SELECT, which would send every row through
// that disk for nothing, is not distributed at all.
func TestSpoolLimit(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	if _, err := h.co.Plan(t.Context(), `SELECT * FROM lineitem WHERE true`, harnessEnv{h}, ModeOn); !IsUnsupported(err) {
		t.Errorf("a plain filtered SELECT: %v", err)
	}
	h.co.opts.MaxSpoolBytes = 64 << 10
	defer func() { h.co.opts.MaxSpoolBytes = 0 }()
	plan, err := h.co.Plan(t.Context(), `SELECT DISTINCT l_orderkey, l_comment FROM lineitem`, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := h.co.Execute(t.Context(), plan, h.settings(t.Context()))
	if !IsUnsupported(err) {
		if ex != nil {
			ex.Close()
		}
		t.Fatalf("partial results past the spool's limit: %v", err)
	}
	if n := len(h.co.pool.ready(t.Context())); n != 2 {
		t.Errorf("%d workers usable, want 2", n)
	}
	// Small partial results fit.
	if !h.check(`SELECT l_returnflag, count(*) FROM lineitem GROUP BY 1 ORDER BY 1`) {
		t.Error("did not run distributed")
	}
}

// fakeCompute records the worker counts asked for.
type fakeCompute struct{ asked atomic.Int32 }

func (f *fakeCompute) Scale(context.Context, int) error { f.asked.Add(1); return nil }

// Forced by a user other than an admin, a statement runs only on workers
// already running, on no more of them than its tables' sizes call for, and
// never starts any; forced by an admin, it may.
func TestForcedByNonAdmins(t *testing.T) {
	h := newHarness(t, 0.01, 3, 3)
	for _, w := range h.workers {
		w.opts.MemoryBytes = 32 << 30
	}
	compute := &fakeCompute{}
	var addrs []string
	for _, ln := range h.lns {
		addrs = append(addrs, ln.Addr().String())
	}
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator2"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2, Storage: NewStorage(icebergio.LocalFS{}),
		Pool: PoolOptions{Addresses: addrs, Compute: compute, MaxWorkers: 8, StartTimeout: time.Second},
	}, h.log)
	if err != nil {
		t.Fatal(err)
	}
	defer co.Close()
	q := `SELECT l_returnflag, count(*) FROM lineitem GROUP BY 1`
	run := func(mode Mode) (*Execution, error) {
		plan, err := co.Plan(t.Context(), q, harnessEnv{h}, mode)
		if err != nil {
			return nil, err
		}
		ex, err := co.Execute(t.Context(), plan, h.settings(t.Context()))
		if ex != nil {
			ex.Close()
		}
		return ex, err
	}
	ex, err := run(ModeOnRunning)
	if err != nil {
		t.Fatal(err)
	}
	if ex.Workers != 1 || compute.asked.Load() != 0 {
		t.Errorf("forced by a non-admin: %d workers, %d starts", ex.Workers, compute.asked.Load())
	}
	if _, err := run(ModeOn); err != nil {
		t.Fatal(err)
	}
	if compute.asked.Load() == 0 {
		t.Error("forced by an admin, no workers were asked for")
	}
	// With no worker running, a non-admin's forced statement does not wait
	// for one.
	for i := range h.workers {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_ = h.workers[i].Shutdown(ctx)
		cancel()
		h.lns[i].Close()
	}
	co.pool.refresh(t.Context())
	before := compute.asked.Load()
	if _, err := co.Plan(t.Context(), q, harnessEnv{h}, ModeOnRunning); !IsUnsupported(err) {
		t.Errorf("no workers running: %v", err)
	}
	if compute.asked.Load() != before {
		t.Error("a non-admin's statement asked for workers")
	}
	// While they are stopped, what every worker reads in full may be as
	// large as the memory of the workers that last ran allows, not
	// defaultBroadcast: orders, joined on an expression it cannot be
	// partitioned by.
	li, orders := h.tables["lineitem"], h.tables["orders"]
	defer func(a, b int64) { li.Bytes, orders.Bytes = a, b }(li.Bytes, orders.Bytes)
	li.Bytes, orders.Bytes = 40<<30, 2<<30
	join := `SELECT count(*) FROM lineitem JOIN orders ON o_orderkey = l_orderkey + 0`
	if _, err := co.Plan(t.Context(), join, harnessEnv{h}, ModeOn); err != nil {
		t.Errorf("a table of 2 GiB for workers of 32 GiB, while they are stopped: %v", err)
	}
}

// scaleLog records what workers a pool asked for.
type scaleLog struct {
	mu    sync.Mutex
	asked []int
}

func (s *scaleLog) Scale(_ context.Context, n int) error {
	s.mu.Lock()
	s.asked = append(s.asked, n)
	s.mu.Unlock()
	return nil
}

func (s *scaleLog) stops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(slices.DeleteFunc(slices.Clone(s.asked), func(n int) bool { return n != 0 }))
}

// Workers stay up while a statement that runs on them runs, though no task
// of it runs, as when it runs on the coordinator after its tasks failed:
// they count as idle from when it ends.
func TestWorkersKeptForAStatement(t *testing.T) {
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	compute := &scaleLog{}
	p := newPool(PoolOptions{Compute: compute, MaxWorkers: 4, IdleAfter: time.Minute}, tr, "test", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.scale(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	release := p.keep()
	idle := []*worker{{addr: "a", status: workerStatus{IdleSeconds: 3600}}}
	p.mu.Lock()
	p.scaledAt, p.used = time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)
	p.mu.Unlock()
	p.stopIdle(t.Context(), idle)
	if n := compute.stops(); n != 0 {
		t.Fatalf("stopped the workers of a running statement, %d times", n)
	}
	release()
	release() // once only
	p.stopIdle(t.Context(), idle)
	if n := compute.stops(); n != 0 {
		t.Fatalf("stopped the workers as the statement ended, %d times", n)
	}
	p.mu.Lock()
	p.used = time.Now().Add(-2 * time.Minute)
	busy := p.busy
	p.mu.Unlock()
	if busy != 0 {
		t.Fatalf("%d statements still keep the workers", busy)
	}
	p.stopIdle(t.Context(), idle)
	if n := compute.stops(); n != 1 {
		t.Fatalf("idle workers after the statement: %d stops, want 1", n)
	}
}

// Workers a coordinator has just started stay up while they boot, though
// it has run no task yet and none answers: they stop only once it has
// neither used them nor started them for IdleAfter, and they report as
// long without tasks.
func TestIdleWorkers(t *testing.T) {
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	compute := &scaleLog{}
	p := newPool(PoolOptions{Compute: compute, MaxWorkers: 4, IdleAfter: time.Minute}, tr, "test", nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.scale(t.Context(), 4); err != nil {
		t.Fatal(err)
	}
	p.stopIdle(t.Context(), nil)
	if n := compute.stops(); n != 0 {
		t.Fatalf("stopped the workers it had just started, %d times", n)
	}
	// They answer, just started.
	booted := []*worker{{addr: "a", status: workerStatus{IdleSeconds: 1}}}
	p.stopIdle(t.Context(), booted)
	if n := compute.stops(); n != 0 {
		t.Fatalf("stopped workers that had just booted, %d times", n)
	}
	// A minute on, with no task, they stop.
	p.mu.Lock()
	p.scaledAt = time.Now().Add(-2 * time.Minute)
	p.mu.Unlock()
	p.stopIdle(t.Context(), []*worker{{addr: "a", status: workerStatus{IdleSeconds: 120}}})
	if n := compute.stops(); n != 1 {
		t.Fatalf("idle workers: %d stops, want 1", n)
	}
}
