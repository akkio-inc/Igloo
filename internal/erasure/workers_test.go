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

func (l clockLedger) WorkerSeen(ctx context.Context, worker, cache, host string, _ time.Time) error {
	return l.WorkerLedger.WorkerSeen(ctx, worker, cache, host, l.now())
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
	// isolated is how long it goes without hearing from a coordinator
	// before it empties its cache by itself.
	isolated time.Duration
	// now is the request's clock, which these tests run ahead.
	now func() time.Time
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
		Erasure: distributed.WorkerErasure{Cache: tw.cache, Through: through, Now: tw.now,
			Purge: func(context.Context) error {
				if err := os.RemoveAll(tw.cacheDir()); err != nil {
					return err
				}
				return os.MkdirAll(tw.cacheDir(), 0o700)
			},
			Save:     func(at time.Time) error { return os.WriteFile(tw.mark(), []byte(at.Format(time.RFC3339Nano)), 0o600) },
			Isolated: tw.isolated,
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
		tw := &testWorker{t: t, name: name, dir: filepath.Join(f.dir, name), eng: f.eng, cache: WorkerCache(name, "disk:"+name, true, 0), now: func() time.Time { return f.svc.now() }}
		tw.start()
		t.Cleanup(tw.stop)
		ws = append(ws, tw)
		addrs = append(addrs, tw.addr)
	}
	return ws, newCoordinator(t, f, "coordinator", addrs...)
}

// newCoordinator is a coordinator of the workers at addrs.
func newCoordinator(t *testing.T, f *fixture, name string, addrs ...string) *distributed.Coordinator {
	t.Helper()
	co, err := distributed.NewCoordinator(distributed.Options{Secret: workerSecret, Version: "test", TempDir: filepath.Join(f.dir, name), ErasureNow: func() time.Time { return f.svc.now() },
		Warehouse: f.warehouse(), Storage: distributed.NewStorage(icebergio.LocalFS{}), Pool: distributed.PoolOptions{Addresses: addrs}},
		slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	return co
}

// runWith runs request id as the scheduler would, with co's watch of the
// workers before each run and the clock moved on after, until it ends or
// is blocked; seen, if set, sees each run's request.
func runWith(t *testing.T, f *fixture, co *distributed.Coordinator, ledger distributed.ErasureLedger, id string, seen func(*Request)) *Request {
	t.Helper()
	for range 16 {
		if err := co.PurgeWorkers(f.ctx, ledger); err != nil {
			t.Logf("purge workers: %v", err)
		}
		_ = f.svc.Run(f.ctx, id)
		got, err := f.store.Get(f.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if seen != nil {
			seen(got)
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
	t.Fatalf("request %s still runs", id)
	return nil
}

// TestRequestPurgesWorkers: with real workers of distributed execution, a
// request completes only once every worker purged its read cache of what
// it deleted. A worker whose cache outlives it and that is stopped blocks
// the request, which names it in its alert; started again, it purges, and
// the request completes. A worker deregistered is no longer waited for.
func TestRequestPurgesWorkers(t *testing.T) {
	f := newFixture(t)
	// The request's clock, which run moves on, stays behind the workers': they
	// purge through no later than their now.
	past := time.Now().Add(-2 * time.Hour)
	f.svc.now = func() time.Time { return past }
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

// TestIsolatedWorker: a worker whose cache goes with it, that runs but that
// no coordinator reaches, empties its cache by itself; a request counts it
// as purged only once its isolated purge time, and a margin, have passed
// since it deleted files, and waits for it until then.
func TestIsolatedWorker(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	tw := &testWorker{t: t, name: "pod-0", dir: filepath.Join(f.dir, "pod-0"), eng: f.eng, now: func() time.Time { return f.svc.now() }, isolated: 300 * time.Millisecond}
	tw.cache = WorkerCache(tw.name, "memory:pod-0", false, time.Second)
	tw.start()
	t.Cleanup(tw.stop)
	ledger := clockLedger{NewWorkerLedger(f.store), func() time.Time { return f.svc.now() }}
	reaching := newCoordinator(t, f, "coordinator", tw.addr)
	if err := reaching.PurgeWorkers(f.ctx, ledger); err != nil {
		t.Fatal(err)
	}
	tw.fill(filepath.Join(f.dir, "warehouse", "crm"))
	// From now on the coordinators cannot reach it, as when its pod is cut
	// off from them: it hears from none, and empties its cache.
	reaching.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gone := ln.Addr().String()
	ln.Close()
	co := newCoordinator(t, f, "coordinator-2", gone)
	deadline := time.Now().Add(10 * time.Second)
	for tw.cached() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if tw.cached() != 0 {
		t.Fatal("the worker cut off from the coordinators kept its cache")
	}

	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	waited := false
	got := runWith(t, f, co, ledger, r.ID, func(r *Request) {
		if r.Status == StatusCompleted && !r.DeletedThrough.IsZero() && f.svc.now().Before(r.DeletedThrough.Add(time.Second+isolatedMargin)) {
			t.Errorf("completed at %s, before the worker's time to purge alone had passed since %s", f.svc.now(), r.DeletedThrough)
		}
		waited = waited || r.Status == StatusWaiting && r.Step == StepCache
	})
	f.completed(got)
	if !waited {
		t.Error("the request did not wait for the worker no coordinator reaches")
	}
}

// TestUnknownWorker: a worker that does not say what it keeps, as one of
// another version of Igloo, keeps a request from completing, which names
// it, until it is upgraded; deregistered, it is no longer waited for.
func TestUnknownWorker(t *testing.T) {
	f := newFixture(t)
	alerts := f.alerts()
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	ws, co := startWorkers(t, f, 1)
	tw := ws[0]
	// It runs a version of Igloo that does not say what it keeps.
	tw.stop()
	tw.cache = ""
	tw.start()
	ledger := clockLedger{NewWorkerLedger(f.store), func() time.Time { return f.svc.now() }}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runWith(t, f, co, ledger, r.ID, nil)
	f.blocked(got, StepCache, "worker-0 at "+tw.addr+", Igloo test, which does not say what it keeps", *alerts)

	// Upgraded, it says what it keeps, and purges; the ledger forgets what
	// it was once that has not run for a while.
	tw.stop()
	tw.cache = WorkerCache(tw.name, "disk:"+tw.name, true, 0)
	tw.start()
	later := f.svc.now().Add(liveWindow + time.Minute)
	f.svc.now = func() time.Time { return later }
	f.completed(runWith(t, f, co, ledger, r.ID, nil))

	// Another one, deregistered.
	tw.stop()
	tw.cache = ""
	tw.start()
	r, err = f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "email", Identifiers: []string{"carl@other.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := runWith(t, f, co, ledger, r.ID, nil); got.Status != StatusBlocked {
		t.Fatalf("status %s at %s: %q", got.Status, got.Step, got.Error)
	}
	tw.stop()
	if err := f.svc.DeregisterWorker(f.ctx, unknownWorker("worker-0")); err != nil {
		t.Fatal(err)
	}
	f.completed(runWith(t, f, co, ledger, r.ID, nil))
}

// TestWorkerLedgerPurged: the ledger tells when a worker is purged of more
// than it was, and only then.
func TestWorkerLedgerPurged(t *testing.T) {
	ctx := context.Background()
	l := NewWorkerLedger(NewMemoryStore())
	n := 0
	l.Purged = func() { n++ }
	c := WorkerCache("w1", "disk:a", true, 0)
	if err := l.WorkerSeen(ctx, "w1", c, "host", time.Now()); err != nil {
		t.Fatal(err)
	}
	at := time.Now()
	for range 2 {
		if err := l.Emptied(ctx, c, at, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Emptied(ctx, c, at.Add(time.Second), ""); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("told %d times", n)
	}
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
	old := WorkerCache("w1", "disk:a", true, 0)
	gone := WorkerCache("w2", "memory:b", false, time.Minute)
	other := WorkerCache("w3", "disk:c", true, 0)
	for _, c := range []string{old, gone, other} {
		w, _ := parseWorkerCache(c)
		if err := l.WorkerSeen(ctx, w.worker, c, "host", now.Add(-48*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	// A replica's cache is not a worker's.
	if err := l.WorkerSeen(ctx, "r", MemoryCache+"r", "host", now); err == nil {
		t.Error("a replica's cache recorded as a worker's")
	}
	newer := WorkerCache("w1", "disk:d", true, 0)
	if err := l.WorkerSeen(ctx, "w1", newer, "host", now); err != nil {
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
	twin := WorkerCache("w1", "disk:e", true, 0)
	if err := l.WorkerSeen(ctx, "w1", twin, "host", now.Add(time.Second)); err != nil {
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
	kept := WorkerCache("w1", "disk:a", true, 0)
	ephemeral := WorkerCache("w2", "memory:b", false, time.Minute)
	for _, c := range []string{kept, ephemeral} {
		w, _ := parseWorkerCache(c)
		if err := l.WorkerSeen(f.ctx, w.worker, c, c+" host", time.Now().Add(-time.Hour)); err != nil {
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
