//go:build linux || darwin

package engine

import "syscall"

// diskSpace returns the size of the filesystem holding dir and the bytes
// free on it for unprivileged users, as DuckDB measures them.
func diskSpace(dir string) (total, avail uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	return st.Blocks * uint64(st.Bsize), st.Bavail * uint64(st.Bsize), nil
}
