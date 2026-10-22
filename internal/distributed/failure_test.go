package distributed

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
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
	for _, sql := range []string{
		"SELECT count(*) FROM read_parquet(" + quoteLiteral(file) + ")",
		"SELECT count(*) FROM __igloo_split, unnest((SELECT list(l_orderkey) FROM read_parquet(" + quoteLiteral(file) + ")))",
		"SELECT count(*) FROM __igloo_split, main.unnest([1])",
		"SELECT count(*) FROM __igloo_split, range(2)",
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
	compute := &fakeCompute{}
	var addrs []string
	for _, ln := range h.lns {
		addrs = append(addrs, ln.Addr().String())
	}
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator2"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Threads: 2,
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
}
