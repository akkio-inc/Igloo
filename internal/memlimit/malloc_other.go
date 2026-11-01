//go:build !(linux && cgo)

package memlimit

// fixMmapThreshold does nothing: the threshold is glibc's (see
// malloc_linux.go).
func fixMmapThreshold() {}
