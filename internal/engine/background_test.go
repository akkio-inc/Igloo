package engine

import (
	"testing"
	"time"
)

// TestBalancedLimits checks how the two DuckDBs share the memory limit: a
// heavy statement alone may use it all, the first's caches giving way to it;
// while the first reads it may go a lead past what the heavy statement has let go
// of, up to three quarters, and the heavy statement has what is left, less the lead,
// down to a quarter. So what they hold never adds up to more than the limit and the
// lead, whatever either of them lets go of.
func TestBalancedLimits(t *testing.T) {
	t.Parallel()
	const total = 100
	for _, c := range []struct {
		mainHeld, bgHeld int64
		reading          bool
		wantMain, wantBg int64
	}{
		{0, 0, false, 100, 100},
		{0, 90, false, 25, 100},
		{0, 30, false, 70, 100},
		{40, 30, false, 70, 60},
		{90, 10, false, 90, 25},
		{90, 60, false, 75, 25},
		{0, 90, true, 16, 94},
		{0, 10, true, 90, 94},
		{40, 60, true, 46, 54},
		{46, 60, true, 46, 48},
		{60, 40, true, 66, 34},
		{90, 60, true, 46, 25},
		{0, 0, true, 100, 94},
	} {
		if m, b := balancedLimits(total, total/backgroundLeadLeast, c.mainHeld, c.bgHeld, c.reading); m != c.wantMain || b != c.wantBg {
			t.Errorf("holding %d and %d, reading %v: limits %d and %d, want %d and %d", c.mainHeld, c.bgHeld, c.reading, m, b, c.wantMain, c.wantBg)
		}
	}
	// With more to spare, the first goes further at once.
	if m, b := balancedLimits(total, total/backgroundLeadMost, 0, 90, true); m != 35 || b != 75 {
		t.Errorf("a longer lead: limits %d and %d", m, b)
	}
}

// TestLeadOf checks how far past what the background DuckDB has let go of the first
// may go: what the machine has to spare, between a sixteenth and a quarter of the limit.
func TestLeadOf(t *testing.T) {
	t.Parallel()
	const total = 800
	for _, c := range []struct{ spare, want int64 }{
		{-50, 50},
		{0, 50},
		{50, 50},
		{150, 150},
		{200, 200},
		{5000, 200},
	} {
		if got := leadOf(total, c.spare); got != c.want {
			t.Errorf("with %d to spare: %d, want %d", c.spare, got, c.want)
		}
	}
}

// TestRoomFor checks that the limits leave room for what the process holds past
// what they count.
func TestRoomFor(t *testing.T) {
	const total = 16 << 30
	for _, c := range []struct{ over, want int64 }{
		{0, total},
		{1 << 30, total},
		{2 << 30, total - 1<<30},
		{3 << 30, total - 2<<30},
		{100 << 30, 12 << 30},
	} {
		if got := roomFor(total, c.over); got != c.want {
			t.Errorf("%d past what they hold: %d, want %d", c.over, got, c.want)
		}
	}
	if processRSS() < 0 {
		t.Error("a process holds no less than nothing")
	}
}

// TestForegroundReading checks when the first DuckDB's caches keep their memory:
// after a read lately, unless statements are held behind the second's.
func TestForegroundReading(t *testing.T) {
	t.Parallel()
	e := &Engine{}
	if e.foregroundReading() {
		t.Error("with no read")
	}
	e.fgRead.Store(time.Now().UnixNano())
	if !e.foregroundReading() {
		t.Error("after a read")
	}
	held := false
	e.WatchForeground(func() bool { return held })
	if !e.foregroundReading() {
		t.Error("after a read, none held")
	}
	held = true
	if e.foregroundReading() {
		t.Error("after a read, statements held behind the second")
	}
}
