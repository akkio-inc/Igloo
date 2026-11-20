//go:build linux

package federation

import "syscall"

// threadID names the calling thread, and signalThread sends that thread
// the signal Go's runtime preempts a thread with, which a program ignores.
func threadID() int { return syscall.Gettid() }

func signalThread(tid int) { _ = syscall.Tgkill(syscall.Getpid(), tid, syscall.SIGURG) }
