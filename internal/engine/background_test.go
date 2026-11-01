package engine

import "testing"

// TestBalancedLimits checks how the two DuckDBs share the memory limit: a
// heavy statement alone may use it all, the first's caches giving way to it;
// while the first reads it keeps what it holds, up to three quarters, and the
// heavy statement has what is left, down to a quarter.
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
		{0, 90, true, 75, 100},
		{0, 10, true, 90, 100},
		{40, 60, true, 75, 60},
		{90, 60, true, 75, 25},
	} {
		if m, b := balancedLimits(total, c.mainHeld, c.bgHeld, c.reading); m != c.wantMain || b != c.wantBg {
			t.Errorf("holding %d and %d, reading %v: limits %d and %d, want %d and %d", c.mainHeld, c.bgHeld, c.reading, m, b, c.wantMain, c.wantBg)
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
