//go:build linux || darwin

package distributed

import "syscall"

// noFollow opens a file without following a symbolic link in its place.
const noFollow = syscall.O_NOFOLLOW

// freeSpace returns the bytes free for unprivileged users on the
// filesystem holding dir.
func freeSpace(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Bavail * uint64(st.Bsize)), nil //nolint:gosec
}
