package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
