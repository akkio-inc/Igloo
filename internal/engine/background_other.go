//go:build !linux

package engine

// lowerThreadPriority fails: elsewhere a nice value is the whole process's.
func lowerThreadPriority(int) error { return ErrNoBackground }
