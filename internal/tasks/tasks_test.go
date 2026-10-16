package tasks

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

// testStore checks a Store that starts empty.
func testStore(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	// Jobs.
	task := Job{Kind: KindTask, Name: "nightly", Owner: "ana", Statement: "SELECT 1", Schedule: "0 6 * * *",
		Timezone: "America/New_York", State: StateStarted, NextRun: now.Add(time.Hour)}
	if err := s.CreateJob(ctx, task, false); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateJob(ctx, task, false); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate: %v", err)
	}
	pipe := Job{Kind: KindPipe, Name: "nightly", Owner: "bob", Statement: "COPY INTO t FROM 'x'", Schedule: "1 minute",
		State: StateStarted, NextRun: now.Add(time.Minute)}
	if err := s.CreateJob(ctx, pipe, false); err != nil {
		t.Fatalf("a pipe named like a task: %v", err)
	}
	mv := Job{Kind: KindMaterializedView, Name: "public.Totals", Owner: "ana", Statement: "SELECT 1", State: StateStarted}
	if err := s.CreateJob(ctx, mv, false); err != nil {
		t.Fatal(err)
	}
	got, err := s.Job(ctx, KindTask, "NIGHTLY")
	if err != nil || got.Owner != "ana" || got.Timezone != "America/New_York" || !got.NextRun.Equal(task.NextRun) || got.LastRun != nil {
		t.Fatalf("job: %+v %v", got, err)
	}
	if _, err := s.Job(ctx, KindTask, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing job: %v", err)
	}
	task.Statement = "SELECT 2"
	if err := s.CreateJob(ctx, task, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Job(ctx, KindTask, "nightly"); got.Statement != "SELECT 2" {
		t.Errorf("replace: %+v", got)
	}

	// Scheduling: started jobs with a next run, soonest first; a due run
	// is claimed once.
	sched, err := s.Scheduled(ctx)
	if err != nil || len(sched) != 2 || sched[0].Kind != KindPipe || sched[1].Kind != KindTask {
		t.Fatalf("scheduled: %+v %v", sched, err)
	}
	next := pipe.NextRun.Add(time.Minute)
	if ok, err := s.Claim(ctx, KindPipe, "nightly", sched[0].NextRun, next); !ok || err != nil {
		t.Fatalf("claim: %v %v", ok, err)
	}
	if ok, _ := s.Claim(ctx, KindPipe, "nightly", sched[0].NextRun, next.Add(time.Minute)); ok {
		t.Error("claimed twice")
	}
	if err := s.SetState(ctx, KindPipe, "nightly", StateSuspended, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.Claim(ctx, KindPipe, "nightly", next, next.Add(time.Minute)); ok {
		t.Error("claimed a suspended job")
	}
	if err := s.SetSchedule(ctx, KindTask, "nightly", "5 minutes", "", now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if sched, _ := s.Scheduled(ctx); len(sched) != 1 || sched[0].Schedule != "5 minutes" || sched[0].Timezone != "" {
		t.Errorf("after suspend and reschedule: %+v", sched)
	}
	if err := s.SetState(ctx, KindTask, "nope", StateSuspended, time.Time{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("state of a missing job: %v", err)
	}

	// Locks.
	unlock, ok, err := s.TryLock(ctx, JobKey(KindTask, "nightly"))
	if !ok || err != nil {
		t.Fatalf("lock: %v %v", ok, err)
	}
	if _, ok, _ := s.TryLock(ctx, JobKey(KindTask, "NIGHTLY")); ok {
		t.Error("locked twice")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	if _, err := s.Lock(waitCtx, JobKey(KindTask, "nightly")); err == nil {
		t.Error("Lock did not wait")
	}
	cancel()
	unlock()
	unlock2, err := s.Lock(ctx, JobKey(KindTask, "nightly"))
	if err != nil {
		t.Fatal(err)
	}
	unlock2()

	// Runs.
	runs := []Run{
		{ID: audit.NewID(now), Kind: KindTask, Name: "nightly", User: "ana", Trigger: TriggerSchedule, Scheduled: now,
			Start: now, Outcome: OutcomeRunning, Rows: -1},
		{ID: audit.NewID(now.Add(time.Second)), Kind: KindPipe, Name: "nightly", User: "bob", Trigger: TriggerSchedule,
			Start: now.Add(time.Second), End: now.Add(2 * time.Second), Outcome: OutcomeSucceeded, Rows: 5},
	}
	for _, r := range runs {
		if err := s.WriteRun(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AbandonRuns(ctx, KindTask, "nightly"); err != nil {
		t.Fatal(err)
	}
	all, err := s.Runs(ctx, audit.Viewer{All: true}, audit.Page{})
	if err != nil || len(all) != 2 || all[0].Kind != KindPipe || all[1].Outcome != OutcomeFailed || all[1].Error != abandoned {
		t.Fatalf("runs: %+v %v", all, err)
	}
	if mine, _ := s.Runs(ctx, audit.Viewer{User: "bob"}, audit.Page{}); len(mine) != 1 || mine[0].Rows != 5 {
		t.Errorf("bob's runs: %+v", mine)
	}
	if page, _ := s.Runs(ctx, audit.Viewer{All: true}, audit.Page{Limit: 1, AfterTime: all[0].Start, AfterID: all[0].ID}); len(page) != 1 || page[0].ID != all[1].ID {
		t.Errorf("next page: %+v", page)
	}
	if got, _ := s.Job(ctx, KindPipe, "nightly"); got.LastRun == nil || got.LastRun.Rows != 5 {
		t.Errorf("last run: %+v", got.LastRun)
	}
	if jobs, _ := s.Jobs(ctx, KindTask); len(jobs) != 1 || jobs[0].LastRun == nil || jobs[0].LastRun.Outcome != OutcomeFailed {
		t.Errorf("tasks: %+v", jobs)
	}

	// Loads.
	modified := time.Date(2026, 10, 16, 9, 0, 0, 123456789, time.UTC)
	loads := []Load{
		{ID: audit.NewID(now), Table: "lake.public.t", File: "s3://b/a.csv", Size: 10, Modified: modified, Status: LoadLoading,
			Start: now, User: "ana"},
		{ID: audit.NewID(now.Add(time.Second)), Table: "lake.public.t", File: "s3://b/b.csv", Size: 20, Modified: modified,
			Status: LoadFailed, FirstError: "bad", Start: now.Add(time.Second), User: "bob", Pipe: "p"},
	}
	if err := s.WriteLoads(ctx, loads); err != nil {
		t.Fatal(err)
	}
	loads[0].Status, loads[0].Rows, loads[0].End = LoadLoaded, 3, now.Add(time.Second)
	if err := s.WriteLoads(ctx, loads[:1]); err != nil {
		t.Fatal(err)
	}
	prev, err := s.PreviousLoads(ctx, "LAKE.public.t", []string{"s3://b/a.csv", "s3://b/b.csv", "s3://b/c.csv"})
	if err != nil || len(prev) != 2 || prev["s3://b/a.csv"].Status != LoadLoaded || prev["s3://b/a.csv"].Rows != 3 ||
		!prev["s3://b/a.csv"].SameVersion(10, modified) || prev["s3://b/a.csv"].SameVersion(11, modified) {
		t.Fatalf("previous loads: %+v %v", prev, err)
	}
	if l, _ := s.Loads(ctx, audit.Viewer{User: "bob"}, audit.Page{}); len(l) != 1 || l[0].Pipe != "p" || l[0].FirstError != "bad" {
		t.Errorf("bob's loads: %+v", l)
	}
	if err := s.DeleteLoads(ctx, []string{loads[1].ID}); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameTable(ctx, "lake.public.t", "lake.public.u"); err != nil {
		t.Fatal(err)
	}
	if prev, _ := s.PreviousLoads(ctx, "lake.public.u", []string{"s3://b/a.csv", "s3://b/b.csv"}); len(prev) != 1 {
		t.Errorf("after rename and delete: %+v", prev)
	}
	if err := s.RenameTable(ctx, "lake.public.totals", "lake.public.sums"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Job(ctx, KindMaterializedView, "public.sums"); err != nil {
		t.Errorf("materialized view after its table's rename: %v", err)
	}
	if err := s.ForgetTable(ctx, "lake.public.sums"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Job(ctx, KindMaterializedView, "public.sums"); !errors.Is(err, ErrNotFound) {
		t.Errorf("materialized view after its table was dropped: %v", err)
	}
	if err := s.ForgetTable(ctx, "lake.public.u"); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.Loads(ctx, audit.Viewer{All: true}, audit.Page{}); len(l) != 0 {
		t.Errorf("loads of a dropped table: %+v", l)
	}

	// Pruning removes old runs and failed loads only.
	if err := s.WriteLoads(ctx, []Load{
		{ID: "old-failed", Table: "lake.public.v", File: "f1", Status: LoadFailed, Start: now.Add(-time.Hour), User: "ana"},
		{ID: "old-loaded", Table: "lake.public.v", File: "f2", Status: LoadLoaded, Start: now.Add(-time.Hour), User: "ana"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Prune(ctx, now.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.Runs(ctx, audit.Viewer{All: true}, audit.Page{}); len(r) != 1 {
		t.Errorf("runs after prune: %+v", r)
	}
	if l, _ := s.Loads(ctx, audit.Viewer{All: true}, audit.Page{}); len(l) != 1 || l[0].ID != "old-loaded" {
		t.Errorf("loads after prune: %+v", l)
	}

	if err := s.DropJob(ctx, KindTask, "nightly"); err != nil {
		t.Fatal(err)
	}
	if err := s.DropJob(ctx, KindTask, "nightly"); !errors.Is(err, ErrNotFound) {
		t.Errorf("drop twice: %v", err)
	}
}

func TestMemoryStore(t *testing.T) {
	testStore(t, NewMemoryStore())
}

// TestPostgresStore runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresStore(t *testing.T) {
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgres(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_tasks.jobs, igloo_tasks.runs, igloo_tasks.loads"); err != nil {
		t.Fatal(err)
	}
	// Migrations are idempotent, as several replicas run them.
	if s2, err := OpenPostgres(ctx, url); err != nil {
		t.Fatal(err)
	} else {
		s2.Close()
	}
	testStore(t, s)
}

// fakeRunner records runs, each taking delay.
type fakeRunner struct {
	delay time.Duration
	err   error
	runs  atomic.Int64
	// running counts runs in progress, and most the most at once.
	running, most atomic.Int64
}

func (f *fakeRunner) RunJob(ctx context.Context, _ Job) (int64, error) {
	n := f.running.Add(1)
	defer f.running.Add(-1)
	for {
		m := f.most.Load()
		if n <= m || f.most.CompareAndSwap(m, n) {
			break
		}
	}
	f.runs.Add(1)
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return -1, ctx.Err()
	}
	return 1, f.err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runSchedulers(t *testing.T, r Runner, schedulers ...*Scheduler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, s := range schedulers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Run(ctx, r)
		}()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		for _, s := range schedulers {
			s.Wait(ctx)
		}
	})
}

// stores runs test on a MemoryStore, and on a PostgresStore when
// IGLOO_TEST_DATABASE_URL is set.
func stores(t *testing.T, test func(t *testing.T, store Store)) {
	t.Run("memory", func(t *testing.T) { test(t, NewMemoryStore()) })
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if url == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		s, err := OpenPostgres(context.Background(), url)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		if _, err := s.pool.Exec(context.Background(), "TRUNCATE igloo_tasks.jobs, igloo_tasks.runs, igloo_tasks.loads"); err != nil {
			t.Fatal(err)
		}
		test(t, s)
	})
}

func TestEachDueRunRunsOnce(t *testing.T) {
	stores(t, testEachDueRunRunsOnce)
}

func testEachDueRunRunsOnce(t *testing.T, store Store) {
	ctx := context.Background()
	// Three replicas share the store; a job due every second runs once a
	// second, not three times.
	a, b, c := NewScheduler(store, Options{}), NewScheduler(store, Options{}), NewScheduler(store, Options{})
	if err := store.CreateJob(ctx, Job{Kind: KindTask, Name: "t", Owner: "ana", Statement: "SELECT 1", Schedule: "1 second",
		State: StateStarted, NextRun: time.Now().Truncate(time.Second).Add(time.Second)}, false); err != nil {
		t.Fatal(err)
	}
	r := &fakeRunner{}
	runSchedulers(t, r, a, b, c)
	time.Sleep(3500 * time.Millisecond)
	// Each due time runs at most once. On a loaded host a due time may be
	// skipped because the previous run had not finished recording; that is
	// not a second run.
	runs, _ := store.Runs(ctx, audit.Viewer{All: true}, audit.Page{})
	ran := map[time.Time]int{}
	for _, run := range runs {
		switch {
		case run.Trigger != TriggerSchedule || run.Scheduled.IsZero():
			t.Errorf("run: %+v", run)
		case run.Outcome == OutcomeSucceeded && run.Rows == 1:
			ran[run.Scheduled]++
		case run.Outcome != OutcomeSkipped:
			t.Errorf("run: %+v", run)
		}
	}
	for at, n := range ran {
		if n > 1 {
			t.Errorf("the run due at %s ran %d times", at, n)
		}
	}
	if n := r.runs.Load(); n < 2 || n > 4 || int(n) != len(ran) {
		t.Errorf("runs in 3.5 seconds: %d, of %d due times", n, len(ran))
	}
}

func TestRunsNeverOverlap(t *testing.T) {
	stores(t, testRunsNeverOverlap)
}

func testRunsNeverOverlap(t *testing.T, store Store) {
	ctx := context.Background()
	s := NewScheduler(store, Options{})
	if err := store.CreateJob(ctx, Job{Kind: KindTask, Name: "slow", Owner: "ana", Statement: "SELECT 1", Schedule: "1 second",
		State: StateStarted, NextRun: time.Now()}, false); err != nil {
		t.Fatal(err)
	}
	r := &fakeRunner{delay: 2500 * time.Millisecond}
	runSchedulers(t, r, s, NewScheduler(store, Options{}))
	waitFor(t, "a skipped run", func() bool {
		runs, _ := store.Runs(ctx, audit.Viewer{All: true}, audit.Page{})
		for _, run := range runs {
			if run.Outcome == OutcomeSkipped {
				return true
			}
		}
		return false
	})
	if r.most.Load() != 1 {
		t.Errorf("%d runs at once", r.most.Load())
	}
	// A manual run of a running job is refused.
	j, _ := store.Job(ctx, KindTask, "slow")
	if _, err := s.RunNow(ctx, r, j); !errors.Is(err, ErrRunning) {
		t.Errorf("run now: %v", err)
	}
}

func TestOwnerGoneSuspends(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.CreateJob(ctx, Job{Kind: KindTask, Name: "orphan", Owner: "gone", Statement: "SELECT 1", Schedule: "1 second",
		State: StateStarted, NextRun: time.Now()}, false); err != nil {
		t.Fatal(err)
	}
	r := &fakeRunner{err: ErrOwnerGone}
	runSchedulers(t, r, NewScheduler(store, Options{}))
	waitFor(t, "suspend", func() bool {
		j, _ := store.Job(ctx, KindTask, "orphan")
		return j.State == StateSuspended
	})
	time.Sleep(1500 * time.Millisecond)
	if n := r.runs.Load(); n != 1 {
		t.Errorf("runs: %d", n)
	}
}

func TestIdlePipeChecksAreNotRecorded(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	s := NewScheduler(store, Options{})
	j := Job{Kind: KindPipe, Name: "p", Owner: "ana", Statement: "COPY INTO t FROM 'x'", Schedule: "1 minute", State: StateStarted}
	if _, err := s.RunNow(ctx, &fakeRunner{err: ErrIdle}, j); !errors.Is(err, ErrIdle) {
		t.Fatal(err)
	}
	if runs, _ := store.Runs(ctx, audit.Viewer{All: true}, audit.Page{}); len(runs) != 0 {
		t.Errorf("recorded: %+v", runs)
	}
	if run, err := s.RunNow(ctx, &fakeRunner{}, j); err != nil || run.Outcome != OutcomeSucceeded {
		t.Fatal(run, err)
	}
	if runs, _ := store.Runs(ctx, audit.Viewer{All: true}, audit.Page{}); len(runs) != 1 || runs[0].Trigger != TriggerManual {
		t.Errorf("recorded: %+v", runs)
	}
}

func TestNextDue(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	s := NewScheduler(store, Options{})
	if _, ok, err := s.NextDue(ctx); ok || err != nil {
		t.Fatalf("nothing scheduled: %v %v", ok, err)
	}
	at := time.Now().Add(time.Hour).Truncate(time.Second)
	for i, j := range []Job{
		{Kind: KindTask, Name: "later", State: StateStarted, NextRun: at.Add(time.Hour)},
		{Kind: KindTask, Name: "soon", State: StateStarted, NextRun: at},
		{Kind: KindTask, Name: "off", State: StateSuspended},
		{Kind: KindMaterializedView, Name: "public.mv", State: StateStarted},
	} {
		j.Owner, j.Statement = "ana", "SELECT 1"
		if err := store.CreateJob(ctx, j, false); err != nil {
			t.Fatal(i, err)
		}
	}
	// Answers are reused for a moment, unless something changed here.
	if _, ok, _ := s.NextDue(ctx); ok {
		t.Error("cached answer not reused")
	}
	s.Kick()
	if due, ok, err := s.NextDue(ctx); !ok || err != nil || !due.Equal(at) {
		t.Errorf("next due: %v %v %v", due, ok, err)
	}
}
