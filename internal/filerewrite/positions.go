package filerewrite

import (
	"context"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Position deletes a file at a time.
//
// iceberg-go loads every position delete file of the files it reads, whole,
// before it reads their rows. An UPDATE of every row of a large table, as
// DuckDB writes one, leaves delete files that cover all of it: 141 million
// positions, which took more memory than compaction's default budget, so
// the table was never compacted, and its hundreds of files stayed. Where
// the deletes alone are what keeps a rewrite over its budget, and they are
// all position deletes, the rewrite applies them itself instead: it reads
// the files without their deletes, one after another as it always does,
// and drops the rows each file's deletes name, loading only those, from
// the row groups of the delete files whose range of paths holds the file's
// (writers sort position deletes by path). What it holds at once is then
// one file's deletes, a few bytes a position.

// positionBytes is what a deleted position takes while it is applied: the
// position, and its share of the path it came with while it is read.
const positionBytes = 16

// positionsOnly reports whether every delete of tasks is a position delete
// in a Parquet file, which applyPositions can apply.
func positionsOnly(tasks []table.FileScanTask) bool {
	any := false
	for _, t := range tasks {
		if len(t.EqualityDeleteFiles) > 0 || len(t.DeletionVectorFiles) > 0 {
			return false
		}
		for _, d := range t.DeleteFiles {
			if d.ContentType() != iceberg.EntryContentPosDeletes || d.FileFormat() != iceberg.ParquetFile {
				return false
			}
			any = true
		}
	}
	return any
}

// fileDeleteBytes is the most memory one file's deletes take while
// applyPositions applies them: no more positions than the file has rows.
func fileDeleteBytes(tasks []table.FileScanTask) int64 {
	var most int64
	for _, t := range tasks {
		var n int64
		for _, d := range t.DeleteFiles {
			n += d.Count()
		}
		most = max(most, min(n, t.File.Count())*positionBytes)
	}
	return most
}

// withoutDeletes returns tasks with no delete files, for applyPositions.
func withoutDeletes(tasks []table.FileScanTask) []table.FileScanTask {
	out := make([]table.FileScanTask, len(tasks))
	for i, t := range tasks {
		t.DeleteFiles = nil
		out[i] = t
	}
	return out
}

// applyPositions returns the batches of recs, the rows of tasks' files in
// order without their deletes, with the rows that tasks' position deletes
// name dropped. It fails if a file's rows are not as many as its metadata
// says, which would put the positions out of step.
func applyPositions(ctx context.Context, fs iceio.IO, tasks []table.FileScanTask, recs func(yield func(arrow.RecordBatch, error) bool)) func(yield func(arrow.RecordBatch, error) bool) {
	return func(yield func(arrow.RecordBatch, error) bool) {
		mem := compute.GetAllocator(ctx)
		i, at := 0, int64(0) // the file being read, and the position of its next row
		var deleted []int64  // file i's deleted positions, sorted
		load := func() (err error) {
			deleted = nil
			if i < len(tasks) {
				deleted, err = filePositions(ctx, fs, tasks[i].DeleteFiles, tasks[i].File.FilePath(), mem)
			}
			return err
		}
		// advance moves past the files whose rows are all read; a batch
		// never spans two files, and files without rows give none.
		advance := func() error {
			for i < len(tasks) && at == tasks[i].File.Count() {
				i, at = i+1, 0
				if err := load(); err != nil {
					return err
				}
			}
			return nil
		}
		if err := load(); err != nil {
			yield(nil, err)
			return
		}
		for rec, err := range recs {
			if err == nil {
				err = advance()
			}
			if err == nil && i >= len(tasks) {
				err = fmt.Errorf("more rows than the files' metadata says")
			}
			if err != nil {
				if rec != nil {
					rec.Release()
				}
				yield(nil, err)
				return
			}
			out, err := dropPositions(ctx, mem, rec, at, deleted)
			at += rec.NumRows()
			rec.Release()
			if err == nil && at > tasks[i].File.Count() {
				err = fmt.Errorf("%s: more rows than its metadata says (%d)", tasks[i].File.FilePath(), tasks[i].File.Count())
			}
			if err != nil {
				if out != nil {
					out.Release()
				}
				yield(nil, err)
				return
			}
			if out != nil && !yield(out, nil) {
				return
			}
		}
		if err := advance(); err != nil {
			yield(nil, err)
		} else if i < len(tasks) {
			yield(nil, fmt.Errorf("%s: read %d rows, its metadata says %d", tasks[i].File.FilePath(), at, tasks[i].File.Count()))
		}
	}
}

// dropPositions returns rec, whose first row is at position at of its file,
// without the rows at deleted's positions; nil for none left.
func dropPositions(ctx context.Context, mem memory.Allocator, rec arrow.RecordBatch, at int64, deleted []int64) (arrow.RecordBatch, error) {
	n := rec.NumRows()
	lo, _ := slices.BinarySearch(deleted, at)
	hi, _ := slices.BinarySearch(deleted, at+n)
	if lo == hi {
		rec.Retain()
		return rec, nil
	}
	if int64(hi-lo) == n {
		return nil, nil
	}
	b := array.NewBooleanBuilder(mem)
	defer b.Release()
	b.Reserve(int(n))
	j := lo
	for r := int64(0); r < n; r++ {
		if j < hi && deleted[j] == at+r {
			b.UnsafeAppend(false)
			j++
			continue
		}
		b.UnsafeAppend(true)
	}
	keep := b.NewBooleanArray()
	defer keep.Release()
	return compute.FilterRecordBatch(compute.WithAllocator(ctx, mem), rec, keep, compute.DefaultFilterOptions())
}

// filePositions returns the positions of path that deletes name, sorted
// and without repeats, reading only the row groups whose range of paths
// holds path.
func filePositions(ctx context.Context, fs iceio.IO, deletes []iceberg.DataFile, path string, mem memory.Allocator) ([]int64, error) {
	var out []int64
	for _, d := range deletes {
		if ref := d.ReferencedDataFile(); ref != nil && *ref != path {
			continue
		}
		pos, err := deletePositions(ctx, fs, d.FilePath(), path, mem)
		if err != nil {
			return nil, fmt.Errorf("read the deletes %s: %w", d.FilePath(), err)
		}
		out = append(out, pos...)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

func deletePositions(ctx context.Context, fs iceio.IO, deletes, path string, mem memory.Allocator) ([]int64, error) {
	f, err := fs.Open(deletes)
	if err != nil {
		return nil, err
	}
	rdr, err := file.NewParquetReader(f, file.WithReadProps(parquet.NewReaderProperties(mem)))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	defer rdr.Close() //nolint:errcheck
	schema := rdr.MetaData().Schema
	pathCol, posCol := schema.ColumnIndexByName("file_path"), schema.ColumnIndexByName("pos")
	if pathCol < 0 || posCol < 0 {
		return nil, fmt.Errorf("no file_path and pos columns")
	}
	var groups []int
	for i := range rdr.NumRowGroups() {
		if mayHold(rdr.MetaData().RowGroup(i), pathCol, path) {
			groups = append(groups, i)
		}
	}
	if len(groups) == 0 {
		return nil, nil
	}
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, mem)
	if err != nil {
		return nil, err
	}
	rr, err := fr.GetRecordReader(ctx, []int{pathCol, posCol}, groups)
	if err != nil {
		return nil, err
	}
	defer rr.Release()
	var out []int64
	for rr.Next() {
		rec := rr.RecordBatch()
		paths, poss := rec.Column(0), rec.Column(1)
		pos, ok := poss.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("positions as %s", poss.DataType())
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			if stringAt(paths, r) == path {
				out = append(out, pos.Value(r))
			}
		}
	}
	return out, rr.Err()
}

// mayHold reports whether a row group's statistics leave room for path in
// its file_path column; without statistics, it may.
func mayHold(rg *metadata.RowGroupMetaData, col int, path string) bool {
	cc, err := rg.ColumnChunk(col)
	if err != nil {
		return true
	}
	ok, err := cc.StatsSet()
	if err != nil || !ok {
		return true
	}
	stats, err := cc.Statistics()
	if err != nil || stats == nil || !stats.HasMinMax() {
		return true
	}
	bs, ok := stats.(*metadata.ByteArrayStatistics)
	if !ok {
		return true
	}
	return string(bs.Min()) <= path && path <= string(bs.Max())
}

func stringAt(a arrow.Array, i int) string {
	switch c := a.(type) {
	case *array.String:
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Binary:
		return string(c.Value(i))
	case *array.Dictionary:
		if s, ok := c.Dictionary().(*array.String); ok {
			return s.Value(c.GetValueIndex(i))
		}
	}
	return ""
}
