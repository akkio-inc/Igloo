//go:build !linux

package federation

import "syscall"

// Without a way to signal one thread, signalThread signals the process:
// the system picks the thread, the one that waits for a scan about every
// other run on macOS.
func threadID() int { return 0 }

func signalThread(int) { _ = syscall.Kill(syscall.Getpid(), syscall.SIGURG) }
