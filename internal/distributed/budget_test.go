package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestAutoLimits stops planning in auto mode once it made no progress for
// a tenth of the time a statement is expected to take on one machine,
// within autoStallFloor and maxAutoStall, or once planning took that time
// in all; a statement whose time is not known waits unknownStall for
// progress, and is not limited in all.
func TestAutoLimits(t *testing.T) {
	for _, tc := range []struct {
		seconds float64
		known   bool
		want    planLimits
	}{
		{0, true, planLimits{stall: autoStallFloor, total: autoStallFloor}},
		{1, true, planLimits{stall: autoStallFloor, total: time.Second}},
		{20, true, planLimits{stall: 2 * time.Second, total: 20 * time.Second}},
		{3600, true, planLimits{stall: maxAutoStall, total: time.Hour}},
		{0, false, planLimits{stall: unknownStall}},
	} {
		if got := autoLimits(tc.seconds, tc.known); got != tc.want {
			t.Errorf("autoLimits(%v, %v) = %+v, want %+v", tc.seconds, tc.known, got, tc.want)
		}
	}
}

// TestPlanningProgress keeps planning that progresses, planning stages or
// describing parts, more often than its stall limit, past that limit, and
// ends it once it stops, within the limit and a tick or two.
func TestPlanningProgress(t *testing.T) {
	const stall = 200 * time.Millisecond
	pl := &planner{}
	ctx, stop := limitPlanning(context.Background(), pl, planLimits{stall: stall})
	defer stop()
	for range 10 {
		time.Sleep(stall / 2)
		pl.progressed()
	}
	if ctx.Err() != nil {
		t.Fatalf("ended after %s of progress every %s: %v", time.Since(pl.started), stall/2, pl.stalled.Load())
	}
	stalled := time.Now()
	select {
	case <-ctx.Done():
	case <-time.After(5 * stall):
		t.Fatalf("not ended %s after its last progress", 5*stall)
	}
	if took := time.Since(stalled); took < stall-10*time.Millisecond {
		t.Errorf("ended %s after its last progress, within its limit of %s", took, stall)
	}
	if why, _ := pl.stalled.Load().(string); !strings.Contains(why, "for "+stall.String()) || !errors.Is(context.Cause(ctx), errPlanBudget) {
		t.Errorf("ended as %q (%v)", why, context.Cause(ctx))
	}
}

// lockWaitEnv is a session that waits for the catalog lock when told to.
type lockWaitEnv struct {
	harnessEnv
	mu    sync.Mutex
	total time.Duration
	from  time.Time
}

func (e *lockWaitEnv) wait(on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if on {
		e.from = time.Now()
		return
	}
	e.total += time.Since(e.from)
	e.from = time.Time{}
}

func (e *lockWaitEnv) CatalogWaited() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.from.IsZero() {
		return e.total
	}
	return e.total + time.Since(e.from)
}

// TestPlanningWaitsForTheCatalog keeps planning whose session waits for the
// catalog lock, past its limits, as a writer holding the lock is no sign that
// planning does not get on; and ends it as before once the wait is over.
func TestPlanningWaitsForTheCatalog(t *testing.T) {
	const stall = 200 * time.Millisecond
	env := &lockWaitEnv{}
	pl := &planner{env: env}
	ctx, stop := limitPlanning(context.Background(), pl, planLimits{stall: stall, total: 2 * stall})
	defer stop()
	env.wait(true)
	time.Sleep(5 * stall)
	if ctx.Err() != nil {
		t.Fatalf("ended while its session waited for the catalog lock: %v", pl.stalled.Load())
	}
	env.wait(false)
	ended := time.Now()
	select {
	case <-ctx.Done():
	case <-time.After(5 * stall):
		t.Fatalf("not ended %s after the wait", 5*stall)
	}
	if took := time.Since(ended); took < stall-10*time.Millisecond {
		t.Errorf("ended %s after the wait, within its limit of %s", took, stall)
	}
	if why, _ := pl.stalled.Load().(string); !strings.Contains(why, "for "+stall.String()) {
		t.Errorf("ended as %q", why)
	}
}

// slowEnv is a session whose describes each take delay, as one whose
// catalog answers slowly, or one that hangs; it counts them.
type slowEnv struct {
	harnessEnv
	delay     time.Duration
	describes *int
}

func (e slowEnv) Describe(ctx context.Context, query string) ([]Column, error) {
	if e.describes != nil {
		*e.describes++
	}
	select {
	case <-time.After(e.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return e.harnessEnv.Describe(ctx, query)
}

// TestCheapStatementGivesUpPlanning gives up planning, in auto mode, a
// statement expected to be quick on one machine whose planning stalls, a
// part's description taking longer than its limit; and does so again on
// its next run, rather than skip planning it from then on. Forced, it
// plans for as long as planning takes.
func TestCheapStatementGivesUpPlanning(t *testing.T) {
	h := householdHarness(t)
	env := slowEnv{harnessEnv: harnessEnv{h}, delay: 2 * autoStallFloor}
	ctx := context.Background()
	for run := 1; run <= 2; run++ {
		start := time.Now()
		_, err := h.co.Plan(ctx, householdReport, env, ModeAuto)
		took := time.Since(start)
		if !IsUnsupported(err) || !strings.Contains(err.Error(), "described no part for "+autoStallFloor.String()) {
			t.Errorf("auto mode, run %d: %v, want it stalled past %s", run, err, autoStallFloor)
		}
		if took > autoStallFloor+time.Second {
			t.Errorf("auto mode, run %d, gave up after %s, its planning stalling past %s", run, took, autoStallFloor)
		}
	}
	// Forced, with describes quicker than that but many of them, in all
	// longer than auto mode's limits for the statement.
	env.delay = 150 * time.Millisecond
	start := time.Now()
	if _, err := h.co.Plan(ctx, householdReport, env, ModeOn); err != nil {
		t.Errorf("forced: %v", err)
	}
	if took := time.Since(start); took <= 2*autoStallFloor {
		t.Errorf("forced planning took %s: the test's describes are too quick to show it is not limited", took)
	}
}

// TestStaysHereWithoutPlanning checks that a statement whose runs already decide
// that it stays on this replica does not plan again: planning a report of many
// stages took longer than the report. It plans where planning has a say: a first
// run, a run past the threshold, a run guarded or short of memory, a fill, forced,
// and one on far fewer workers than it could run on.
func TestStaysHereWithoutPlanning(t *testing.T) {
	h := householdHarness(t)
	for _, name := range []string{"households", "people"} {
		h.tables[name].Bytes = 2 << 40
	}
	ctx := context.Background()
	var described int
	env := slowEnv{harnessEnv: harnessEnv{h}, describes: &described}
	plan := func(mode Mode) (error, int) {
		before := described
		_, err := h.co.Plan(ctx, householdReport, env, mode)
		return err, described - before
	}
	first, _, err := h.co.plan(ctx, householdReport, env, ModeAuto, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if described == 0 {
		t.Fatal("its first run described nothing")
	}
	key := first.Key
	h.co.Observe(key, 0, 10*time.Second)
	err, n := plan(ModeAuto)
	if n != 0 || !IsUnsupported(err) || !strings.Contains(err.Error(), "it ran in 10 s on one machine") {
		t.Errorf("after a run of 10 s here: %d describes, %v", n, err)
	}
	if PlanKey(err) != key {
		t.Errorf("its key: %q, want %q", PlanKey(err), key)
	}
	if d, ok := LocalEstimate(err); !ok || d != 10*time.Second {
		t.Errorf("the guard's estimate: %s, %v", d, ok)
	}
	if _, n = plan(ModeOn); n == 0 {
		t.Error("forced, it did not plan")
	}
	// Past the threshold, planning has a say again.
	h.co.history.update(key, func(r *runs) { r.local = 90 })
	if _, n = plan(ModeAuto); n == 0 {
		t.Error("after a run of 90 s here it did not plan")
	}
	reset := func(r *runs) { r.local, r.dist, r.distWorkers, r.outOfMemory, r.guarded = 10, 0, 0, false, false }
	for name, change := range map[string]func(r *runs){
		"short of memory":                        func(r *runs) { r.outOfMemory = true },
		"guarded":                                func(r *runs) { r.guarded = true },
		"no slower on workers":                   func(r *runs) { r.dist, r.distWorkers = 11, 8 },
		"measured on far fewer workers than now": func(r *runs) { r.dist, r.distWorkers = 60, 1 },
	} {
		h.co.history.update(key, func(r *runs) { reset(r); change(r) })
		if _, n = plan(ModeAuto); n == 0 {
			t.Errorf("%s: it did not plan", name)
		}
	}
	h.co.history.update(key, func(r *runs) { reset(r); r.dist, r.distWorkers = 60, h.co.opts.Pool.MaxWorkers+8 })
	if err, n = plan(ModeAuto); n != 0 || !IsUnsupported(err) || !strings.Contains(err.Error(), "on one machine and 60 s on workers") {
		t.Errorf("slower on as many workers: %d describes, %v", n, err)
	}
}

// longReport is a report of a chain of n common table expressions, each
// grouping the households of the one before and joining them to the
// people, which the planner computes on workers stage after stage.
func longReport(n int) string {
	var b strings.Builder
	b.WriteString("WITH c0 AS (SELECT h.hh_id % 997 AS k, count(*) AS n, sum(h.weight) AS w FROM households AS h JOIN people AS p ON p.hh_id = h.hh_id GROUP BY 1)")
	for i := 1; i < n; i++ {
		fmt.Fprintf(&b, ", c%d AS (SELECT c.k %% %d AS k, sum(c.n) AS n, sum(c.w) AS w FROM c%d AS c JOIN people AS p ON p.person_id = c.k GROUP BY 1)", i, 997-i, i-1)
	}
	fmt.Fprintf(&b, " SELECT count(*), sum(n), sum(w) FROM c%d", n-1)
	return b.String()
}

// TestLongPlanningKeepsGoing plans, in auto mode, a report of two dozen
// stages and more whose parts each take a while to describe, as a
// catalog's tables loading in the session do, over tables one machine
// should take minutes to read: planning takes longer than the cap of
// 10 s that once sent such reports to one machine, but it plans stages
// all along, and plans them all.
func TestLongPlanningKeepsGoing(t *testing.T) {
	if testing.Short() {
		t.Skip("plans for over ten seconds")
	}
	h := householdHarness(t)
	for _, name := range []string{"households", "people"} {
		h.tables[name].Bytes = 2 << 40
	}
	q := longReport(14)
	var described int
	env := slowEnv{harnessEnv: harnessEnv{h}, delay: 450 * time.Millisecond, describes: &described}
	start := time.Now()
	plan, _, err := h.co.plan(context.Background(), q, env, ModeAuto, false, nil)
	took := time.Since(start)
	if err != nil {
		t.Fatalf("after %s: %v", took, err)
	}
	t.Logf("%d stages in %s, %d describes", len(plan.Stages), took, described)
	if len(plan.Stages) < 24 {
		t.Errorf("%d stages, want two dozen or more", len(plan.Stages))
	}
	if took <= 10*time.Second {
		t.Logf("planning took %s, within the old cap: the test's describes are quicker than meant", took)
	}
}

// TestPrintKeepsTexts prints a tree printed before from the text kept of
// it, without DuckDB, and a tree that differs anew; and forgets the texts
// kept once they grow past maxPrinted.
func TestPrintKeepsTexts(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	n, err := p.parse("SELECT k, sum(v) AS s FROM t WHERE k > 1 GROUP BY k")
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.print(n)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.printed) != 1 {
		t.Fatalf("%d texts kept, want 1", len(p.printed))
	}
	// Printed again with DuckDB's parser closed, the tree's text comes
	// from what was kept.
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	again, err := p.print(clone(n).(node))
	if err != nil || again != first {
		t.Fatalf("printed again: %q, %v; want %q", again, err, first)
	}

	if p, err = newParser(); err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.print(n); err != nil {
		t.Fatal(err)
	}
	other, err := p.parse("SELECT k, sum(v) AS s FROM t WHERE k > 2 GROUP BY k")
	if err != nil {
		t.Fatal(err)
	}
	text, err := p.print(other)
	if err != nil || text == first || !strings.Contains(text, "2") {
		t.Errorf("a tree of another constant printed as %q, %v", text, err)
	}
	if len(p.printed) != 2 {
		t.Errorf("%d texts kept, want 2", len(p.printed))
	}
	// A third tree's text, past maxPrinted, is all that is kept.
	p.printedBytes = maxPrinted
	third, err := p.parse("SELECT k, sum(v) AS s FROM t WHERE k > 3 GROUP BY k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.print(third); err != nil {
		t.Fatal(err)
	}
	key, _ := printKey(third)
	if _, kept := p.printed[key]; !kept || len(p.printed) != 1 {
		t.Errorf("past maxPrinted, kept %d texts, the last among them: %v", len(p.printed), kept)
	}
}

// TestToEnd checks stages' levels in a plan of a chain of stages: 1 for a stage
// whose rows no stage reads, otherwise one more than the highest level of the
// stages that read them, the highest of all as long as the plan's longest chain.
func TestToEnd(t *testing.T) {
	h := householdHarness(t)
	for _, name := range []string{"households", "people"} {
		h.tables[name].Bytes = 2 << 40
	}
	plan, _, err := h.co.plan(t.Context(), longReport(4), harnessEnv{h}, ModeOn, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := toEnd(plan.Stages)
	most := 0
	for _, s := range plan.Stages {
		want := 1
		for _, r := range plan.Stages {
			for _, in := range r.Inputs {
				if s.Out != nil && in.Exchange != nil && in.Exchange.ID == s.Out.ID {
					want = max(want, got[r.ID]+1)
				}
			}
		}
		if got[s.ID] != want {
			t.Errorf("stage %d: level %d, want %d\n%s", s.ID, got[s.ID], want, plan.describe())
		}
		most = max(most, got[s.ID])
	}
	if most != levels(plan.Stages) || most < 4 {
		t.Errorf("highest level %d, longest chain %d", most, levels(plan.Stages))
	}
}
