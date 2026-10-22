//go:build linux || darwin

package engine

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// lockExclusive takes f's lock exclusively if no other process holds it, and
// says whether it did.
func lockExclusive(f *os.File) (bool, error) {
	err := flock(f, syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return err == nil, err
}

// lockShared takes f's lock shared, or turns this process's exclusive hold
// into a shared one, waiting while another process holds it exclusively.
func lockShared(f *os.File) error {
	return flock(f, syscall.LOCK_SH)
}

func flock(f *os.File, how int) error {
	for {
		// A signal, such as Go's own preemption, interrupts the wait.
		if err := syscall.Flock(int(f.Fd()), how); !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// allocated is the disk space a file takes, less than its size where it has
// holes.
func allocated(info fs.FileInfo) uint64 {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Blocks) * 512
	}
	return uint64(info.Size())
}
