//go:build !linux && !darwin

package engine

import "errors"

// diskSpace is not implemented on this platform, which turns the read cache
// off.
func diskSpace(string) (total, avail uint64, err error) {
	return 0, 0, errors.New("measuring free disk space is not supported on this platform")
}
