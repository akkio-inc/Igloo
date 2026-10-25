package engine

// DuckDB spills to IGLOO_DUCKDB_TEMP_DIR (temp_directory) when a query needs
// more memory than its limit, and deletes its files there as its database
// closes, which Close leads to once every connection is closed. A process
// that is killed or crashes leaves them behind, and the next one does not
// delete them: a replica restarted while large queries spilled kept tens of
// GB it could not use, until its spill disk filled up. The engine deletes
// them as it opens, before DuckDB writes any.
//
// It may do so only when no other process uses the directory: DuckDB names
// its files the same in every process, so a live process's files look just
// like a dead one's. Every engine holds a lock on tempLockName in the
// directory, shared, for as long as it is open, and deletes the files only
// if it could take the lock exclusively first. The kernel releases a
// process's lock when the process ends, however it ends, and the lock holds
// between containers on one machine that mount the same volume.

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// tempLockName is the lock file in the spill directory.
const tempLockName = "igloo.lock"

// duckdbTempPrefixes start the names of the files DuckDB spills to: one per
// block size (duckdb_temp_storage_DEFAULT-0.tmp, ..._S128K-0.tmp) and one
// per block larger than those (duckdb_temp_block-<id>.block).
var duckdbTempPrefixes = []string{"duckdb_temp_storage_", "duckdb_temp_block-"}

// SortedWriteDirPrefix starts the names of the directories in the spill
// directory where a write sorted a range at a time keeps its rows until it
// has written them (see the pgwire package's sortedwrite.go). They are
// deleted with DuckDB's files.
const SortedWriteDirPrefix = "igloo-sorted-write-"

// FreeDiskSpace returns the bytes free for unprivileged users on the
// filesystem holding dir.
func FreeDiskSpace(dir string) (uint64, error) {
	_, avail, err := diskSpace(dir)
	return avail, err
}

// claimTempDir takes the spill directory dir for this process, first
// deleting the files DuckDB left there if no other process uses it, and
// returns the func that lets go of it once DuckDB is closed. Without dir, or
// when dir cannot be locked, it deletes nothing and holds nothing.
func claimTempDir(dir string, log *slog.Logger) func() {
	if dir == "" {
		return func() {}
	}
	// DuckDB creates the directory when it first spills; the lock must be
	// there before that.
	var f *os.File
	err := os.MkdirAll(dir, 0o700)
	if err == nil {
		// Locking needs only a file open for reading.
		f, err = os.OpenFile(filepath.Join(dir, tempLockName), os.O_RDONLY|os.O_CREATE, 0o600)
	}
	if err != nil {
		log.Warn("cannot lock the spill directory; leaving earlier runs' files in it", "dir", dir, "err", err)
		return func() {}
	}
	alone, err := lockExclusive(f)
	if err == nil {
		if alone {
			if files, freed := removeTempFiles(dir, log); files > 0 {
				log.Info("deleted DuckDB temporary files an earlier run left in the spill directory",
					"dir", dir, "files", files, "bytes_freed", freed)
			}
		} else {
			log.Warn("another process is using the spill directory, so its files stay; give each Igloo process its own IGLOO_DUCKDB_TEMP_DIR", "dir", dir)
		}
		// Waits for another process that is deleting files.
		err = lockShared(f)
	}
	if err != nil {
		log.Warn("cannot lock the spill directory; leaving earlier runs' files in it", "dir", dir, "err", err)
		_ = f.Close()
		return func() {}
	}
	return func() { _ = f.Close() }
}

// removeTempFiles deletes DuckDB's spill files in dir, and the directories
// of sorted writes, and returns how many files it deleted and the disk
// space they took.
func removeTempFiles(dir string, log *slog.Logger) (files int, freed uint64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		log.Warn("cannot list the spill directory", "dir", dir, "err", err)
		return 0, 0
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), SortedWriteDirPrefix) {
			path := filepath.Join(dir, entry.Name())
			n, size := filesIn(path)
			if err := os.RemoveAll(path); err != nil {
				log.Warn("cannot delete a sorted write's directory", "path", path, "err", err)
				continue
			}
			files, freed = files+n, freed+size
			continue
		}
		if !entry.Type().IsRegular() || !isTempFile(entry.Name()) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := entry.Info()
		if err == nil {
			err = os.Remove(path)
		}
		if err != nil {
			log.Warn("cannot delete a DuckDB temporary file", "path", path, "err", err)
			continue
		}
		files++
		freed += allocated(info)
	}
	return files, freed
}

// filesIn counts the files under dir and the disk space they take.
func filesIn(dir string) (files int, size uint64) {
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			files, size = files+1, size+allocated(info)
		}
		return nil
	})
	return files, size
}

func isTempFile(name string) bool {
	for _, prefix := range duckdbTempPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
