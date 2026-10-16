package sharing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"gocloud.dev/gcerrors"
)

// Row-level deletes. DuckDB deletes rows of an Iceberg table (DELETE,
// UPDATE, MERGE) by writing delete files beside the data files, and table
// maintenance merges them in later, or never for a table with few files.
// Delta Sharing recipients read data files only, so a data file with
// deletes is served as a copy with its deletes applied: Igloo reads it
// through iceberg-go, which applies positional and equality deletes and
// deletion vectors, and writes the rows that remain to a Parquet file in the
// warehouse's sharing area. A copy is named by the data file and delete
// files it comes from, which never change, so it is written once and serves
// every request that needs it; copies that no current version needs are
// deleted after materializedRetention.

const (
	// materializedDir holds the copies, under the warehouse root. Table
	// locations are directories the catalog names by UUID, so it is no
	// table's, and table maintenance never deletes its files.
	materializedDir = ".igloo/sharing/"
	// materializedRetention is how long a copy no current snapshot needs
	// is kept, for time travel and for URLs handed out earlier. Deleted
	// copies are written again when a request needs them.
	materializedRetention = 48 * time.Hour
	// materializedCheck is how long Igloo trusts that a copy it saw still
	// exists.
	materializedCheck = time.Hour
	// materializedRowGroup is the row group size of copies, DuckDB's.
	materializedRowGroup = 122_880
)

type materialized struct {
	size, count int64
	seen        time.Time
}

// materializer writes and keeps track of copies.
type materializer struct {
	files iceio.IO
	// root is the sharing area: the warehouse root plus materializedDir.
	root string

	mu    sync.Mutex
	known map[string]materialized
	// writing serializes copies of the same file within a process.
	writing map[string]*sync.Mutex
}

func newMaterializer(files iceio.IO, warehouse string) *materializer {
	return &materializer{files: files, root: warehouse + materializedDir, known: map[string]materialized{}, writing: map[string]*sync.Mutex{}}
}

// copyPath is where the copy of task's data file with its deletes lives.
func (m *materializer) copyPath(tableID string, task table.FileScanTask) string {
	var deletes []string
	for _, group := range [][]iceberg.DataFile{task.DeleteFiles, task.EqualityDeleteFiles, task.DeletionVectorFiles} {
		for _, d := range group {
			deletes = append(deletes, d.FilePath())
		}
	}
	slices.Sort(deletes)
	h := sha256.Sum256([]byte(task.File.FilePath() + "\n" + strings.Join(deletes, "\n")))
	return m.root + tableID + "/" + hex.EncodeToString(h[:16]) + ".parquet"
}

// copy returns the copy of task's data file with its deletes applied,
// writing it if it does not exist yet. sch is the snapshot's schema.
func (m *materializer) copy(ctx context.Context, tbl *table.Table, snapID int64, task table.FileScanTask, sch *iceberg.Schema) (dataFile, error) {
	path := m.copyPath(tbl.Metadata().TableUUID().String(), task)
	lock := m.lock(path)
	lock.Lock()
	defer lock.Unlock()

	info, ok := m.lookup(path)
	if !ok {
		var err error
		if info, ok, err = m.stat(path); err != nil {
			return dataFile{}, err
		}
	}
	if !ok {
		var err error
		if info, err = m.write(ctx, tbl, snapID, task, path); err != nil {
			return dataFile{}, fmt.Errorf("apply the deletes of %s: %w", task.File.FilePath(), err)
		}
	}
	m.remember(path, info)
	return dataFile{path: path, size: info.size, count: info.count, stats: copyStats(task.File, sch, info.count)}, nil
}

func (m *materializer) lock(path string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.writing[path]
	if !ok {
		l = &sync.Mutex{}
		m.writing[path] = l
	}
	return l
}

func (m *materializer) lookup(path string) (materialized, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.known[path]
	return info, ok && time.Since(info.seen) < materializedCheck
}

func (m *materializer) remember(path string, info materialized) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.known) > 100_000 {
		clear(m.known)
	}
	info.seen = time.Now()
	m.known[path] = info
}

// forget drops what Igloo knows about a copy, as when serving it found it
// gone.
func (m *materializer) forget(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.known, path)
}

// stat reports whether a copy exists, and its size and row count, from its
// Parquet footer.
func (m *materializer) stat(path string) (materialized, bool, error) {
	f, err := m.files.Open(path)
	if isNotExist(err) {
		return materialized{}, false, nil
	}
	if err != nil {
		return materialized{}, false, err
	}
	defer f.Close() //nolint:errcheck
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return materialized{}, false, err
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		// A copy that does not read is written again.
		return materialized{}, false, nil
	}
	defer rdr.Close() //nolint:errcheck
	return materialized{size: size, count: rdr.NumRows()}, true, nil
}

// isNotExist recognizes a missing file, locally or in object storage.
func isNotExist(err error) bool {
	return err != nil && (errors.Is(err, fs.ErrNotExist) || gcerrors.Code(err) == gcerrors.NotFound)
}

func (m *materializer) write(ctx context.Context, tbl *table.Table, snapID int64, task table.FileScanTask, path string) (materialized, error) {
	wfs, ok := m.files.(iceio.WriteFileIO)
	if !ok {
		return materialized{}, errors.New("the warehouse's file system cannot write")
	}
	schema, batches, err := tbl.Scan(table.WithSnapshotID(snapID)).ReadTasks(ctx, []table.FileScanTask{task})
	if err != nil {
		return materialized{}, err
	}
	out, err := wfs.Create(path)
	if err != nil {
		return materialized{}, err
	}
	cw := &countWriter{w: out}
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
		parquet.WithMaxRowGroupLength(materializedRowGroup),
		parquet.WithStoreDecimalAsInteger(true),
	)
	w, err := pqarrow.NewFileWriter(schema, cw, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		_ = out.Close()
		return materialized{}, err
	}
	var count int64
	for batch, err := range batches {
		if err != nil {
			_ = w.Close()
			return materialized{}, err
		}
		count += batch.NumRows()
		err = w.WriteBuffered(batch)
		batch.Release()
		if err != nil {
			_ = w.Close()
			return materialized{}, err
		}
	}
	// Closing the Parquet writer closes the file, which commits it.
	if err := w.Close(); err != nil {
		return materialized{}, err
	}
	return materialized{size: cw.n, count: count}, nil
}

type countWriter struct {
	w io.WriteCloser
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func (c *countWriter) Close() error { return c.w.Close() }

// copyStats are a copy's statistics: its own row count, and the original
// file's bounds, which still hold for fewer rows. Null counts may be lower
// now, so they are left out.
func copyStats(df iceberg.DataFile, sch *iceberg.Schema, count int64) string {
	var st deltaStats
	if err := json.Unmarshal([]byte(fileStats(df, sch)), &st); err != nil {
		return ""
	}
	st.NumRecords, st.NullCount = count, nil
	b, err := json.Marshal(st)
	if err != nil {
		return ""
	}
	return string(b)
}

// collect deletes the copies that no current version of a shared table
// needs and that are older than materializedRetention. needed holds the
// paths of the copies the current versions use.
func (m *materializer) collect(needed map[string]bool, now time.Time) (int, error) {
	walker, ok := m.files.(iceio.ListableIO)
	if !ok {
		return 0, nil
	}
	var stale []string
	err := walker.WalkDir(m.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if isNotExist(err) {
				return fs.SkipDir
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".parquet") || needed[path] {
			return nil
		}
		info, err := d.Info()
		if err != nil || now.Sub(info.ModTime()) < materializedRetention {
			return nil
		}
		stale = append(stale, path)
		return nil
	})
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, path := range stale {
		m.forget(path)
		if err := m.files.Remove(path); err == nil {
			removed++
		}
	}
	return removed, nil
}
