//go:build linux

package engine

import (
	"bytes"
	"os"
	"strconv"
	"syscall"
)

// lowerThreadPriority gives the calling thread nice value n. On Linux a
// nice value is a thread's, not the process's, and threads it starts
// inherit it.
func lowerThreadPriority(n int) error {
	return syscall.Setpriority(syscall.PRIO_PROCESS, 0, n)
}

// processRSS is the memory this process holds, 0 if it cannot tell.
func processRSS() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	f := bytes.Fields(b)
	if len(f) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(string(f[1]), 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}
