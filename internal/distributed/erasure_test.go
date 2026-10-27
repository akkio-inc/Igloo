package distributed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
)

// fakeLedger is the erasure requests' ledger, as coordinators keep workers
// in it.
type fakeLedger struct {
	mu      sync.Mutex
	latest  time.Time
	seen    map[string]string
	emptied map[string]time.Time
}

func (l *fakeLedger) LatestDeletion(context.Context) (time.Time, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.latest, nil
}

func (l *fakeLedger) WorkerSeen(_ context.Context, _, cache, host string, _ time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil {
		l.seen = map[string]string{}
	}
	l.seen[cache] = host
	return nil
}

func (l *fakeLedger) Emptied(_ context.Context, cache string, through time.Time, _ string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.emptied == nil {
		l.emptied = map[string]time.Time{}
	}
	l.emptied[cache] = through
	return nil
}

// erasureCache is a worker's read cache in these tests: a directory whose
// files stand for blocks of data files, with what it was purged through
// kept beside it, as on a disk that outlives the worker; events records
// its purges and the tasks' connections, in order.
type erasureCache struct {
	dir    string
	mu     sync.Mutex
	events []string
}

func (c *erasureCache) blocks() string { return filepath.Join(c.dir, "blocks") }
func (c *erasureCache) mark() string   { return filepath.Join(c.dir, "erased-through") }

func (c *erasureCache) event(e string) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

// cache puts a block in the cache, as a statement's read would.
func (c *erasureCache) cache(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(c.blocks(), "block-0"), []byte("rows"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (c *erasureCache) cached() int {
	entries, _ := os.ReadDir(c.blocks())
	return len(entries)
}

func (c *erasureCache) through() string {
	b, _ := os.ReadFile(c.mark())
	return string(b)
}

// withErasure has the harness's workers start again with read caches of
// their own (erasureCache), and returns the caches.
func withErasure(t *testing.T, h *harness) []*erasureCache {
	t.Helper()
	var caches []*erasureCache
	for i := range h.workers {
		c := &erasureCache{dir: filepath.Join(h.dir, "cache-"+h.workers[i].opts.ID)}
		if err := os.MkdirAll(c.blocks(), 0o700); err != nil {
			t.Fatal(err)
		}
		caches = append(caches, c)
	}
	h.erasure = func(i int) WorkerErasure {
		c := caches[i]
		var through time.Time
		if b, err := os.ReadFile(c.mark()); err == nil {
			through, _ = time.Parse(time.RFC3339Nano, string(b))
		}
		return WorkerErasure{Cache: "worker/kept/" + h.workers[i].opts.ID + "/disk:" + h.workers[i].opts.ID, Through: through,
			Purge: func(context.Context) error {
				c.event("purge")
				if err := os.RemoveAll(c.blocks()); err != nil {
					return err
				}
				return os.MkdirAll(c.blocks(), 0o700)
			},
			Save: func(at time.Time) error { return os.WriteFile(c.mark(), []byte(at.Format(time.RFC3339Nano)), 0o600) },
		}
	}
	h.connected = func(i int) { caches[i].event("task") }
	for i := range h.workers {
		h.restartWorker(i)
	}
	return caches
}

// restartWorker stops worker i and starts it again at its address.
func (h *harness) restartWorker(i int) {
	h.t.Helper()
	h.stopWorker(i)
	h.startWorker(i)
}

func (h *harness) stopWorker(i int) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.workers[i].Shutdown(ctx); err != nil {
		h.t.Fatal(err)
	}
}

const erasureQuery = "SELECT l_returnflag, count(*), sum(l_quantity) FROM lineitem GROUP BY 1"

// TestWorkersPurge: a coordinator that learns of an erasure request's
// deletion has every worker purge: its read cache, the footers it keeps,
// and the exchange files on its disk, its own and those it fetched. The
// statements after carry the deletion, and nothing more is purged; those
// of a coordinator that began them before it learned of it are refused,
// and run on that coordinator, whose next statements carry it.
func TestWorkersPurge(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	caches := withErasure(t, h)
	ctx := context.Background()
	if !h.check(erasureQuery) {
		t.Fatal("did not run distributed")
	}
	footers := 0
	for i, w := range h.workers {
		caches[i].cache(t)
		w.footersMu.Lock()
		footers += len(w.footerCache)
		w.footersMu.Unlock()
	}
	if footers == 0 {
		t.Fatal("the workers keep no footers: the test shows nothing")
	}
	// A statement's exchange files on worker-0's disk, and files it
	// fetched from another's.
	w0 := h.workers[0]
	exchange := filepath.Join(w0.local.root, "q1", "s1", "0", "a0", "__b=0", "data_0.parquet")
	fetched := filepath.Join(w0.fetchRoot, "task-1", "0.parquet")
	for _, f := range []string{exchange, fetched} {
		if err := os.MkdirAll(filepath.Dir(f), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("rows"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	w0.local.register("q1", map[string]int64{"s1/0/a0/__b=0/data_0.parquet": 4})

	latest := time.Now().UTC().Round(time.Microsecond)
	ledger := &fakeLedger{latest: latest}
	if err := h.co.PurgeWorkers(ctx, ledger); err != nil {
		t.Fatal(err)
	}
	for i, w := range h.workers {
		c := caches[i]
		w.footersMu.Lock()
		kept := len(w.footerCache)
		w.footersMu.Unlock()
		cache := w.opts.Erasure.Cache
		if c.cached() != 0 || kept != 0 || c.through() != latest.Format(time.RFC3339Nano) || !ledger.emptied[cache].Equal(latest) ||
			!strings.HasPrefix(ledger.seen[cache], w.opts.ID+" at ") {
			t.Errorf("%s: %d blocks, %d footers, through %q, ledger %v %q", w.opts.ID, c.cached(), kept, c.through(), ledger.emptied[cache],
				ledger.seen[cache])
		}
	}
	for _, f := range []string{exchange, fetched} {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("%s is left: %v", f, err)
		}
	}
	if _, _, err := w0.local.open("q1", "s1/0/a0/__b=0/data_0.parquet"); err == nil {
		t.Error("the worker still serves the exchange file")
	}
	if h.co.erased.Load() != latest.UnixNano() {
		t.Errorf("the coordinator knows of %d", h.co.erased.Load())
	}

	// The statements after carry the deletion: nothing more is purged.
	if !h.check(erasureQuery) {
		t.Fatal("did not run distributed")
	}
	for i, c := range caches {
		if n := strings.Count(strings.Join(c.events, ","), "purge"); n != 1 {
			t.Errorf("worker-%d purged %d times: %v", i, n, c.events)
		}
	}

	// A coordinator that has not learned of it.
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator-2"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2, Pool: PoolOptions{Addresses: h.co.opts.Pool.Addresses},
		Storage: NewStorage(icebergio.LocalFS{}),
	}, h.log)
	if err != nil {
		t.Fatal(err)
	}
	defer co.Close()
	execute := func() error {
		plan, err := co.Plan(ctx, erasureQuery, harnessEnv{h}, ModeOn)
		if err != nil {
			return err
		}
		ex, err := co.Execute(ctx, plan, h.settings(ctx))
		if err == nil {
			ex.Close()
		}
		return err
	}
	if err := execute(); !IsUnsupported(err) || !strings.Contains(err.Error(), "erasure request") {
		t.Errorf("a statement begun before the deletion: %v", err)
	}
	if co.erased.Load() != latest.UnixNano() {
		t.Errorf("the coordinator learned of %d", co.erased.Load())
	}
	if err := execute(); err != nil {
		t.Errorf("the next statement: %v", err)
	}
}

// TestRestartedWorkerPurges: a worker that was stopped, its cache on its
// disk, while an erasure request deleted files, purges before it runs a
// task when it starts again; a stopped worker is not recorded as running.
func TestRestartedWorkerPurges(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	caches := withErasure(t, h)
	ctx := context.Background()
	h.stopWorker(1)
	caches[1].cache(t)
	latest := time.Now().UTC().Round(time.Microsecond)
	ledger := &fakeLedger{latest: latest}
	if err := h.co.PurgeWorkers(ctx, ledger); err != nil {
		t.Fatal(err)
	}
	stopped := h.workers[1].opts.Erasure.Cache
	if _, ok := ledger.seen[stopped]; ok || caches[1].cached() != 1 || caches[0].through() == "" {
		t.Fatalf("seen %v; worker-1 caches %d blocks; worker-0 through %q", ledger.seen, caches[1].cached(), caches[0].through())
	}

	// Started again, it says it has not purged, and purges before its
	// first task.
	caches[1].events = nil
	h.startWorker(1)
	st, err := h.co.pool.status(ctx, h.lns[1].Addr().String())
	if err != nil || st.Erased != 0 || st.Cache != stopped {
		t.Fatalf("status %+v, %v", st, err)
	}
	if !h.check(erasureQuery) {
		t.Fatal("did not run distributed")
	}
	events := caches[1].events
	if len(events) < 2 || events[0] != "purge" || slices.Contains(events[1:], "purge") || caches[1].cached() != 0 ||
		caches[1].through() != latest.Format(time.RFC3339Nano) {
		t.Errorf("worker-1: events %v, %d blocks, through %q", events, caches[1].cached(), caches[1].through())
	}
}

// TestPurgeCancelsOlderTasks: a purge cancels the tasks of statements that
// began before the deletion and empties the cache only once they ended,
// so nothing they read lands after it; such statements' tasks are refused
// from then on.
func TestPurgeCancelsOlderTasks(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	caches := withErasure(t, h)
	w := h.workers[0]
	ctx := context.Background()
	tctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	done, err := w.admit(ctx, &runningTask{query: "q", started: time.Now(), cancel: cancel})
	if err != nil {
		t.Fatal(err)
	}
	purged := make(chan error, 1)
	go func() {
		w.erasureMu.Lock()
		defer w.erasureMu.Unlock()
		purged <- w.purgeLocked(ctx, 5)
	}()
	<-tctx.Done()
	if !errors.Is(context.Cause(tctx), errErased) {
		t.Errorf("canceled with %v", context.Cause(tctx))
	}
	time.Sleep(50 * time.Millisecond)
	if slices.Contains(caches[0].events, "purge") {
		t.Error("the cache was emptied while the task ran")
	}
	done()
	if err := <-purged; err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(caches[0].events, "purge") || w.purged.Load() != 5 {
		t.Errorf("events %v, purged through %d", caches[0].events, w.purged.Load())
	}
	_, err = w.admit(ctx, &runningTask{query: "q2", started: time.Now(), cancel: func(error) {}, erased: 4})
	var te *taskErr
	if !errors.As(err, &te) || te.Erased != 5 {
		t.Errorf("a task begun before the deletion: %v", err)
	}
}

// TestWorkerPurgesAlone: a worker that hears from no coordinator for its
// isolated purge time empties its caches by itself, and says when; one
// that hears from them keeps its cache.
func TestWorkerPurgesAlone(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	caches := withErasure(t, h)
	erasure := h.erasure
	h.erasure = func(i int) WorkerErasure {
		e := erasure(i)
		e.Isolated = 400 * time.Millisecond
		return e
	}
	h.restartWorker(0)
	ctx := context.Background()
	addr := h.lns[0].Addr().String()
	// Asked for its status every 100 ms, it keeps its cache.
	caches[0].cache(t)
	for range 15 {
		if _, err := h.co.pool.status(ctx, addr); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if caches[0].cached() != 1 {
		t.Fatal("a worker the coordinators reach emptied its cache")
	}
	// Left alone (the coordinator checks every few seconds), it empties
	// its cache.
	deadline := time.Now().Add(4 * time.Second)
	for caches[0].cached() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	st, err := h.co.pool.status(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	if caches[0].cached() != 0 || st.Alone == 0 {
		t.Errorf("%d blocks cached; status %+v", caches[0].cached(), st)
	}
}
