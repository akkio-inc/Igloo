package erasure

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/policy"
)

const workerSecret = "a-test-secret-that-is-long-enough-1234"

// clockLedger is the workers' ledger with the service's clock, which the
// tests move on.
type clockLedger struct {
	*WorkerLedger
	now func() time.Time
}

func (l clockLedger) WorkerSeen(ctx context.Context, cache, host string, _ time.Time) error {
	return l.WorkerLedger.WorkerSeen(ctx, cache, host, l.now())
}

// testWorker is a worker of distributed execution whose read cache is a
// directory that holds copies of data files, as a read cache's blocks do,
// and whose mark is kept beside it, as on a disk that outlives it.
type testWorker struct {
	t     *testing.T
	name  string
	dir   string
	eng   duckdbEngine
	ln    net.Listener
	w     *distributed.Worker
	addr  string
	cache string
}

func (tw *testWorker) cacheDir() string { return filepath.Join(tw.dir, "cache") }
func (tw *testWorker) mark() string     { return filepath.Join(tw.dir, "erased-through") }

// start starts the worker at its address, or a new one.
func (tw *testWorker) start() {
	t := tw.t
	t.Helper()
	if err := os.MkdirAll(tw.cacheDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	var through time.Time
	if b, err := os.ReadFile(tw.mark()); err == nil {
		through, _ = time.Parse(time.RFC3339Nano, string(b))
	}
	w, err := distributed.NewWorker(distributed.WorkerOptions{
		Engine: tw.eng, Secret: workerSecret, Version: "test", ID: tw.name, TempDir: filepath.Join(tw.dir, "tmp"),
		Locations: []string{tw.dir}, Cores: 1, MemoryBytes: 1 << 30,
		Erasure: distributed.WorkerErasure{Cache: tw.cache, Through: through,
			Purge: func(context.Context) error {
				if err := os.RemoveAll(tw.cacheDir()); err != nil {
					return err
				}
				return os.MkdirAll(tw.cacheDir(), 0o700)
			},
			Save: func(at time.Time) error { return os.WriteFile(tw.mark(), []byte(at.Format(time.RFC3339Nano)), 0o600) },
		},
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	addr := "127.0.0.1:0"
	if tw.addr != "" {
		addr = tw.addr
	}
	if tw.ln, err = net.Listen("tcp", addr); err != nil {
		t.Fatal(err)
	}
	tw.addr, tw.w = tw.ln.Addr().String(), w
	go w.Serve(tw.ln) //nolint:errcheck
}

func (tw *testWorker) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tw.w.Shutdown(ctx)
	_ = tw.ln.Close()
}

// cached counts the files the worker's cache holds.
func (tw *testWorker) cached() int {
	n := 0
	_ = filepath.WalkDir(tw.cacheDir(), func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// fill copies the files under dir into the worker's cache.
func (tw *testWorker) fill(dir string) {
	tw.t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(tw.cacheDir(), strings.ReplaceAll(path, string(filepath.Separator), "_")), b, 0o600)
	})
	if err != nil || tw.cached() == 0 {
		tw.t.Fatalf("fill the cache: %v", err)
	}
}

func startWorkers(t *testing.T, f *fixture, n int) ([]*testWorker, *distributed.Coordinator) {
	t.Helper()
	var ws []*testWorker
	var addrs []string
	for i := range n {
		name := fmt.Sprintf("worker-%d", i)
		tw := &testWorker{t: t, name: name, dir: filepath.Join(f.dir, name), eng: f.eng, cache: WorkerCache(name, "disk:"+name, true)}
		tw.start()
		t.Cleanup(tw.stop)
		ws = append(ws, tw)
		addrs = append(addrs, tw.addr)
	}
	co, err := distributed.NewCoordinator(distributed.Options{Secret: workerSecret, Version: "test", TempDir: filepath.Join(f.dir, "coordinator"),
		Warehouse: f.warehouse(), Storage: distributed.NewStorage(icebergio.LocalFS{}), Pool: distributed.PoolOptions{Addresses: addrs}},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	return ws, co
}

// TestRequestPurgesWorkers: with real workers of distributed execution, a
// request completes only once every worker purged its read cache of what
// it deleted. A worker whose cache outlives it and that is stopped blocks
// the request, which names it in its alert; started again, it purges, and
// the request completes. A worker deregistered is no longer waited for.
func TestRequestPurgesWorkers(t *testing.T) {
	f := newFixture(t)
	alerts := f.alerts()
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	ws, co := startWorkers(t, f, 2)
	ledger := clockLedger{NewWorkerLedger(f.store), func() time.Time { return f.svc.now() }}
	// The coordinators' watch registers the workers.
	if err := co.PurgeWorkers(f.ctx, ledger); err != nil {
		t.Fatal(err)
	}
	for _, tw := range ws {
		tw.fill(filepath.Join(f.dir, "warehouse", "crm"))
	}
	// worker-1 stops, its cache on its disk.
	ws[1].stop()

	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	// run runs the request, as the scheduler would, with the coordinators'
	// watch between runs, until it ends or is blocked.
	run := func() *Request {
		t.Helper()
		for range 12 {
			if err := co.PurgeWorkers(f.ctx, ledger); err != nil {
				t.Logf("purge workers: %v", err)
			}
			_ = f.svc.Run(f.ctx, r.ID)
			got, err := f.store.Get(f.ctx, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			switch got.Status {
			case StatusCompleted, StatusBlocked, StatusFailed, StatusHeld:
				return got
			}
			later := got.WaitUntil.Add(time.Minute)
			if got.WaitUntil.IsZero() {
				later = f.svc.now().Add(time.Minute)
			}
			f.svc.now = func() time.Time { return later }
		}
		t.Fatalf("request %s still runs", r.ID)
		return nil
	}
	got := run()
	f.blocked(got, StepCache, ws[1].cache, *alerts)
	if ws[0].cached() != 0 || ws[1].cached() == 0 {
		t.Errorf("worker-0 caches %d files, worker-1 %d", ws[0].cached(), ws[1].cached())
	}
	if !strings.Contains(got.Error, "worker-1 at "+ws[1].addr) {
		t.Errorf("the error does not name the worker: %q", got.Error)
	}

	// Started again on its disk, it purges, and the request completes.
	ws[1].start()
	got = run()
	f.completed(got)
	if ws[1].cached() != 0 || got.Verification.Workers != 0 {
		t.Errorf("worker-1 caches %d files; verification %+v", ws[1].cached(), got.Verification)
	}

	// A worker that will not come back is deregistered.
	ws[0].stop()
	ws[0].fill(filepath.Join(f.dir, "warehouse", "crm"))
	r, err = f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "email", Identifiers: []string{"carl@other.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	got = run()
	if got.Status != StatusBlocked || !strings.Contains(got.Error, ws[0].cache) {
		t.Fatalf("status %s at %s: %q", got.Status, got.Step, got.Error)
	}
	if err := f.svc.DeregisterWorker(f.ctx, "worker/kept/nope/disk:x"); !errors.Is(err, ErrNotFound) {
		t.Errorf("deregister an unknown worker: %v", err)
	}
	if err := f.svc.DeregisterWorker(f.ctx, MemoryCache+"test"); !errors.Is(err, ErrInvalid) {
		t.Errorf("deregister a replica's cache: %v", err)
	}
	if err := f.svc.DeregisterWorker(f.ctx, ws[0].cache); err != nil {
		t.Fatal(err)
	}
	f.completed(run())
}

// TestWorkerLedger: a worker that comes back with another cache leaves the
// one it had, once that is not running; caches that go with their workers
// leave the ledger a day after; a live worker's cache, of the same name
// or not, stays.
func TestWorkerLedger(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	l := NewWorkerLedger(store)
	now := time.Now()
	old := WorkerCache("w1", "disk:a", true)
	gone := WorkerCache("w2", "memory:b", false)
	other := WorkerCache("w3", "disk:c", true)
	for _, c := range []string{old, gone, other} {
		if err := l.WorkerSeen(ctx, c, "host", now.Add(-48*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	// A replica's cache is not a worker's.
	if err := l.WorkerSeen(ctx, MemoryCache+"r", "host", now); err == nil {
		t.Error("a replica's cache recorded as a worker's")
	}
	newer := WorkerCache("w1", "disk:d", true)
	if err := l.WorkerSeen(ctx, newer, "host", now); err != nil {
		t.Fatal(err)
	}
	caches, _ := store.Caches(ctx)
	var ids []string
	for _, c := range caches {
		ids = append(ids, c.ID)
	}
	if strings.Join(ids, ",") != newer+","+other {
		t.Errorf("ledger %q", ids)
	}
	// Two running workers of one name keep both.
	twin := WorkerCache("w1", "disk:e", true)
	if err := l.WorkerSeen(ctx, twin, "host", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if caches, _ := store.Caches(ctx); len(caches) != 3 {
		t.Errorf("ledger %+v", caches)
	}
}

// TestWorkersLeftOnClose: closing a request a stopped worker blocks
// records the worker it leaves.
func TestWorkersLeftOnClose(t *testing.T) {
	f := newFixture(t)
	f.alerts()
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	l := NewWorkerLedger(f.store)
	kept := WorkerCache("w1", "disk:a", true)
	ephemeral := WorkerCache("w2", "memory:b", false)
	for _, c := range []string{kept, ephemeral} {
		if err := l.WorkerSeen(f.ctx, c, c+" host", time.Now().Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	r := f.erase(byEmail)
	if r.Status != StatusBlocked || !strings.Contains(r.Error, kept) || strings.Contains(r.Error, ephemeral) {
		t.Fatalf("status %s: %q", r.Status, r.Error)
	}
	closed, err := f.svc.Close(f.ctx, r.ID, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != StatusClosed || len(closed.WorkersLeft) != 1 || !strings.Contains(closed.WorkersLeft[0], kept) ||
		!strings.Contains(closed.Error, "it leaves 1 workers") {
		t.Errorf("status %s, left %q, error %q", closed.Status, closed.WorkersLeft, closed.Error)
	}
}
