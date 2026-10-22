//go:build !linux && !darwin

package engine

import (
	"errors"
	"io/fs"
	"os"
)

// Locking files is not implemented on this platform, so the engine leaves
// the files in its spill directory alone (see claimTempDir).
var errNoFileLocks = errors.New("locking files is not supported on this platform")

func lockExclusive(*os.File) (bool, error) { return false, errNoFileLocks }

func lockShared(*os.File) error { return errNoFileLocks }

func allocated(info fs.FileInfo) uint64 { return uint64(info.Size()) }
