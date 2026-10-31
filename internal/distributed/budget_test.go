package distributed

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestAutoBudget checks auto mode's planning budget: a quarter of the time
// a statement is expected to take on one machine, within autoBudgetFloor
// and planBudget.
func TestAutoBudget(t *testing.T) {
	for _, tc := range []struct {
		seconds float64
		want    time.Duration
	}{
		{0, autoBudgetFloor},
		{1, autoBudgetFloor},
		{4.4, 1100 * time.Millisecond},
		{20, 5 * time.Second},
		{100, planBudget},
	} {
		if got := autoBudget(tc.seconds); got != tc.want {
			t.Errorf("autoBudget(%v) = %s, want %s", tc.seconds, got, tc.want)
		}
	}
}

// slowEnv is a session whose describes each take delay, as one whose
// catalog answers slowly, or a plan that describes without end.
type slowEnv struct {
	harnessEnv
	delay time.Duration
}

func (e slowEnv) Describe(ctx context.Context, query string) ([]Column, error) {
	select {
	case <-time.After(e.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return e.harnessEnv.Describe(ctx, query)
}

// TestCheapStatementGivesUpPlanning checks that auto mode gives up
// planning a statement expected to be quick on one machine within its
// small budget, though planning it would take seconds, where a forced run
// plans it through; and that its next run, within the hour, does not plan
// it at all.
func TestCheapStatementGivesUpPlanning(t *testing.T) {
	h := householdHarness(t)
	env := slowEnv{harnessEnv: harnessEnv{h}, delay: 100 * time.Millisecond}
	ctx := context.Background()

	start := time.Now()
	_, err := h.co.Plan(ctx, householdReport, env, ModeAuto)
	took := time.Since(start)
	if !IsUnsupported(err) || !strings.Contains(err.Error(), "budget of "+autoBudgetFloor.String()) {
		t.Errorf("auto mode: %v, want it past its budget of %s", err, autoBudgetFloor)
	}
	if took > autoBudgetFloor+time.Second {
		t.Errorf("auto mode gave up after %s, with a budget of %s", took, autoBudgetFloor)
	}

	// Forced, it plans for as long as planning takes, within planBudget.
	start = time.Now()
	if _, err := h.co.Plan(ctx, householdReport, env, ModeOn); err != nil {
		t.Errorf("forced: %v", err)
	}
	if took := time.Since(start); took <= autoBudgetFloor {
		t.Logf("forced planning took %s, within auto mode's budget: the test's describes are too quick", took)
	}

	// Run again, it goes to one machine at once.
	start = time.Now()
	_, err = h.co.Plan(ctx, householdReport, env, ModeAuto)
	if !IsUnsupported(err) || !strings.Contains(err.Error(), "lately") {
		t.Errorf("auto mode again: %v", err)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("auto mode again took %s", took)
	}
}
