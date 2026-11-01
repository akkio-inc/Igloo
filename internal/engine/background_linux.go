//go:build linux

package engine

import "syscall"

// lowerThreadPriority gives the calling thread nice value n. On Linux a
// nice value is a thread's, not the process's, and threads it starts
// inherit it.
func lowerThreadPriority(n int) error {
	return syscall.Setpriority(syscall.PRIO_PROCESS, 0, n)
}
