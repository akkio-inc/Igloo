//go:build !linux && !darwin

package distributed

import "errors"

// noFollow is not needed here: workers keep no exchanges on their disks.
const noFollow = 0

// freeSpace cannot tell the free space here: workers write their exchanges
// to object storage.
func freeSpace(string) (int64, error) {
	return 0, errors.New("free disk space is unknown on this platform")
}
