package memlimit

import (
	"errors"
	"log/slog"
	"testing"
	"time"
)

// TestAdapt checks when Adapt changes DuckDB's memory limit: down at once
// while memory is short, never below half the configured limit, and back
// up a step at a time, only after memory has stayed plentiful.
func TestAdapt(t *testing.T) {
	const total, base = 64 << 30, 48 << 30 // High is 8 GiB
	var left int64 = 20 << 30
	var sets []int64
	fail := false
	a := &adapter{total: total, base: base, limit: base, log: slog.New(slog.DiscardHandler),
		available: func() (int64, bool) { return left, true },
		set: func(n int64) error {
			if fail {
				return errors.New("could not free up enough memory")
			}
			sets = append(sets, n)
			return nil
		}}
	now := time.Unix(0, 0)
	step := func(d time.Duration) {
		now = now.Add(d)
		a.step(now)
	}

	step(time.Second)
	if len(sets) != 0 {
		t.Fatalf("changed the limit with memory to spare: %v", sets)
	}
	// 3 GiB left, 5 short of High: down by 5 GiB and a step of 1 GiB.
	left = 3 << 30
	step(time.Second)
	if len(sets) != 1 || sets[0] != 42<<30 {
		t.Fatalf("with 3 GiB left: %v, want [42 GiB]", sets)
	}
	// Still short, but it never goes below half of base.
	left = 1 << 30
	for range 5 {
		step(time.Second)
	}
	if got := sets[len(sets)-1]; got != base/2 || a.limit != base/2 {
		t.Fatalf("limit %d, want the floor %d", got, base/2)
	}
	// A failed change leaves the limit as it was, to be tried again.
	a.limit, fail = base, true
	step(time.Second)
	if a.limit != base {
		t.Fatalf("a failed change moved the limit to %d", a.limit)
	}
	fail, a.limit = false, base/2
	n := len(sets)

	// Plentiful, but not for long enough yet: no change.
	left = 30 << 30
	step(time.Second)
	step(adaptCalm / 2)
	if len(sets) != n {
		t.Fatalf("raised the limit before memory had stayed plentiful: %v", sets[n:])
	}
	// Plentiful for adaptCalm: up by half of what is above twice High.
	step(adaptCalm)
	if len(sets) != n+1 || sets[n] != base/2+7<<30 {
		t.Fatalf("after %v plentiful: %v, want %d", adaptCalm, sets[n:], base/2+7<<30)
	}
	// Steps keep coming at that pace, and stop at base.
	for range 10 {
		step(adaptCalm)
	}
	if a.limit != base || sets[len(sets)-1] != base {
		t.Fatalf("limit %d after a long calm, want %d", a.limit, base)
	}
	// Between High and twice High nothing changes either way.
	left, n = 12<<30, len(sets)
	for range 100 {
		step(time.Minute)
	}
	if len(sets) != n {
		t.Fatalf("changed the limit with memory neither short nor plentiful: %v", sets[n:])
	}
}
