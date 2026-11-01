//go:build linux && cgo

package memlimit

import "testing"

// TestMmapThresholdFixed checks that a large allocation freed does not raise
// glibc's threshold: a buffer over it is mapped of its own, and goes back to
// the system when freed.
func TestMmapThresholdFixed(t *testing.T) {
	t.Setenv("GLIBC_TUNABLES", "")
	fixMmapThreshold()
	if !mappedAfterFree(16<<20, 256<<10) {
		t.Error("a buffer of 256 KiB, after 16 MiB was freed, came from an arena")
	}
	if mappedAfterFree(16<<20, 64<<10) {
		t.Error("an allocation of 64 KiB, below the threshold, was mapped of its own")
	}
}
