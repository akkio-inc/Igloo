//go:build !linux

package engine

// lowerThreadPriority fails: elsewhere a nice value is the whole process's.
func lowerThreadPriority(int) error { return ErrNoBackground }

// processRSS is the memory this process holds, 0 if it cannot tell.
func processRSS() int64 { return 0 }
