//go:build unix

package modelpack

import (
	"os"
	"syscall"
)

// mapFile maps the file into memory, read only.
func mapFile(f *os.File, size int64) ([]byte, func() error, error) {
	if size == 0 {
		return nil, func() error { return nil }, nil
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return syscall.Munmap(data) }, nil
}
