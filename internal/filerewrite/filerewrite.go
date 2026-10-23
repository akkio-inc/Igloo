// Package filerewrite rewrites an Iceberg table's data files through Arrow
// within a memory budget, as compaction and erasure do.
//
// A rewrite decodes files into Arrow, where a row takes far more memory than
// in Parquet: DuckDB stores a repetitive string in a byte or two and Arrow
// stores it in full. iceberg-go's scan reads a group's files all at once, one
// per core, in batches of 131,072 rows however wide the rows are, holds every
// batch the readers get ahead of the writer, and queued 64 more for the
// writer (see iceberg-go's IGLOO_PATCHES.md), so compacting a table of large
// DuckDB-written files took tens of gigabytes and the kernel killed Igloo.
// Here files are read one at a time, in batches and row groups sized so that
// what a rewrite holds stays within a budget, and a rewrite whose memory goes
// over the budget anyway is stopped.
package filerewrite

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/memlimit"
)

const (
	// DefaultMemory is a rewrite's budget when none is given.
	DefaultMemory = 1 << 30
	// DefaultRowGroupRows is the row group size a rewrite writes, DuckDB's,
	// when the budget and the table's write.parquet.row-group-limit allow.
	DefaultRowGroupRows = 122_880
	// MinRowGroupRows is the smallest row group a rewrite writes. Files
	// whose rows are too wide for a row group this size to fit in the
	// budget are not rewritten: the rewrite fails with ErrOverBudget.
	MinRowGroupRows = 8192
	// probeRows rows of the first file are decoded to measure how
	// much memory a row takes.
	probeRows = 1024
	// deleteRowBytes is what a row of a delete file takes while a rewrite
	// applies it, in Arrow and in the sets of deleted positions and keys.
	deleteRowBytes = 64
)

// ErrOverBudget is the cause of a rewrite stopped, or refused, for its
// memory. Compaction and erasure both get IGLOO_MAINTENANCE_MEMORY.
var ErrOverBudget = errors.New("rewriting the files needs more memory than IGLOO_MAINTENANCE_MEMORY allows")

// Meter is an Arrow allocator that counts what a rewrite holds, and stops
// the rewrite, through stop, once that exceeds limit. iceberg-go reads
// and writes with the allocator of the context it is given (with Igloo's
// patch, the Parquet writer's buffers too), so this sees nearly all of it.
type Meter struct {
	memory.Allocator
	limit      int64
	live, peak atomic.Int64
	stop       context.CancelCauseFunc
}

func (m *Meter) add(n int) {
	live := m.live.Add(int64(n))
	if n <= 0 {
		return
	}
	for {
		peak := m.peak.Load()
		if live <= peak || m.peak.CompareAndSwap(peak, live) {
			break
		}
	}
	if live > m.limit && m.stop != nil {
		m.stop(fmt.Errorf("%w (%s)", ErrOverBudget, memlimit.Format(m.limit)))
	}
}

func (m *Meter) Allocate(n int) []byte {
	b := m.Allocator.Allocate(n)
	m.add(len(b))
	return b
}

func (m *Meter) Reallocate(n int, b []byte) []byte {
	old := len(b)
	b = m.Allocator.Reallocate(n, b)
	m.add(len(b) - old)
	return b
}

func (m *Meter) Free(b []byte) {
	m.add(-len(b))
	m.Allocator.Free(b)
}

// NewMeter returns a Meter over alloc that calls stop once more than limit
// is live; stop may be nil.
func NewMeter(alloc memory.Allocator, limit int64, stop context.CancelCauseFunc) *Meter {
	return &Meter{Allocator: alloc, limit: limit, stop: stop}
}

// Peak is the most the meter has seen live at once.
func (m *Meter) Peak() int64 { return m.peak.Load() }

// Live is what is allocated through the meter now.
func (m *Meter) Live() int64 { return m.live.Load() }

// Metered returns ctx with an allocator that counts what is allocated
// through it, and stops ctx once more than limit is live.
func Metered(ctx context.Context, limit int64) (context.Context, *Meter, context.CancelCauseFunc) {
	ctx, cancel := context.WithCancelCause(ctx)
	m := NewMeter(compute.GetAllocator(ctx), limit, cancel)
	return compute.WithAllocator(ctx, m), m, cancel
}

// Layout is how a rewrite reads and writes: Arrow batches of BatchRows
// rows, and row groups of RowGroupRows.
type Layout struct {
	BatchRows, RowGroupRows int64
}

// Plan picks a layout that keeps a rewrite within budget bytes. rowBytes is
// how much memory a row takes in Arrow, fixed what reading takes besides the
// rows (the compressed column chunks of a row group, and the deletes), and
// maxRowGroup the table's row group size.
//
// While a row group is written, the writer holds its rows in Arrow until it
// has chosen each column's encoding, and then encoded: about two row groups'
// worth. Up to eight batches, each a quarter of a row group, are on their
// way to it: two being read, three queued in the reader, two queued in the
// writer (with Igloo's patch to iceberg-go) and one being written. With room
// for rows twice as wide as measured, a row group gets an eighth of the
// budget.
func Plan(budget, rowBytes, fixed, maxRowGroup int64) (Layout, error) {
	rowBytes = max(rowBytes, 1)
	rows := min(maxRowGroup, max(budget-fixed, 0)/(8*rowBytes))
	if rows < min(MinRowGroupRows, maxRowGroup) {
		return Layout{}, fmt.Errorf("%w (%s): rows take %s each and reading a row group of the files %s",
			ErrOverBudget, memlimit.Format(budget), memlimit.Format(rowBytes), memlimit.Format(fixed))
	}
	return Layout{BatchRows: max(rows/4, 1), RowGroupRows: rows}, nil
}

// measure decodes the first rows of the first file of tasks that has any, and
// returns how much memory a row takes in Arrow, and the largest compressed
// row group of the file, which the reader holds while it decodes the group.
func measure(ctx context.Context, fs iceio.IO, tasks []table.FileScanTask) (rowBytes, rowGroup int64, err error) {
	mem := compute.GetAllocator(ctx)
	for _, task := range tasks {
		rowBytes, rowGroup, err := measureFile(ctx, fs, task.File.FilePath(), mem)
		if err != nil || rowBytes > 0 {
			return rowBytes, rowGroup, err
		}
	}
	return 1, 0, nil
}

func measureFile(ctx context.Context, fs iceio.IO, path string, mem memory.Allocator) (rowBytes, rowGroup int64, err error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, 0, err
	}
	rdr, err := file.NewParquetReader(f, file.WithReadProps(parquet.NewReaderProperties(mem)))
	if err != nil {
		_ = f.Close()
		return 0, 0, fmt.Errorf("read %s: %w", path, err)
	}
	defer rdr.Close() //nolint:errcheck
	first := -1
	for i := range rdr.NumRowGroups() {
		rg := rdr.MetaData().RowGroup(i)
		var size int64
		for j := range rg.NumColumns() {
			cc, err := rg.ColumnChunk(j)
			if err != nil {
				return 0, 0, err
			}
			size += cc.TotalCompressedSize()
		}
		rowGroup = max(rowGroup, size)
		if first < 0 && rg.NumRows() > 0 {
			first = i
		}
	}
	if first < 0 {
		return 0, rowGroup, nil
	}
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: probeRows}, mem)
	if err != nil {
		return 0, 0, err
	}
	rr, err := fr.GetRecordReader(ctx, nil, []int{first})
	if err != nil {
		return 0, 0, err
	}
	defer rr.Release()
	if !rr.Next() {
		if err := rr.Err(); err != nil {
			return 0, 0, fmt.Errorf("read %s: %w", path, err)
		}
		return 0, rowGroup, nil
	}
	rec := rr.RecordBatch()
	return (recordBytes(rec) + rec.NumRows() - 1) / rec.NumRows(), rowGroup, nil
}

// recordBytes is the size of rec's buffers.
func recordBytes(rec arrow.RecordBatch) int64 {
	var n int64
	var add func(arrow.ArrayData)
	add = func(d arrow.ArrayData) {
		for _, b := range d.Buffers() {
			if b != nil {
				n += int64(b.Len())
			}
		}
		for _, c := range d.Children() {
			add(c)
		}
		if dict, ok := d.Dictionary().(*array.Data); ok && dict != nil {
			add(dict)
		}
	}
	for _, c := range rec.Columns() {
		add(c.Data())
	}
	return n
}

// deleteBytes estimates the memory the deletes of tasks take while they are
// applied: the reader loads every delete file of a group before it reads
// the group's rows.
func deleteBytes(tasks []table.FileScanTask) int64 {
	seen := map[string]bool{}
	var n int64
	for _, t := range tasks {
		for _, d := range append(append([]iceberg.DataFile(nil), t.DeleteFiles...), t.EqualityDeleteFiles...) {
			if !seen[d.FilePath()] {
				seen[d.FilePath()] = true
				n += d.Count() * deleteRowBytes
			}
		}
	}
	return n
}

// withProperties returns tbl with props set, to read and write with; it is
// never committed.
func withProperties(tbl *table.Table, props iceberg.Properties) (*table.Table, error) {
	b, err := table.MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	if err != nil {
		return nil, err
	}
	if err := b.SetProperties(props); err != nil {
		return nil, err
	}
	meta, err := b.Build()
	if err != nil {
		return nil, err
	}
	return table.New(tbl.Identifier(), meta, tbl.MetadataLocation(), tbl.FS, nil), nil
}

// unsorted returns tbl with no default sort order, so that the files written
// through it record none.
func unsorted(tbl *table.Table) (*table.Table, error) {
	if tbl.Metadata().SortOrder().IsUnsorted() {
		return tbl, nil
	}
	b, err := table.MetadataBuilderFromBase(tbl.Metadata(), tbl.MetadataLocation())
	if err != nil {
		return nil, err
	}
	if _, err := b.GetSortOrderByID(table.UnsortedSortOrderID); err != nil {
		// A table without the unsorted order cannot say so.
		return tbl, nil
	}
	if err := b.SetDefaultSortOrderID(table.UnsortedSortOrderID); err != nil {
		return nil, err
	}
	meta, err := b.Build()
	if err != nil {
		return nil, err
	}
	return table.New(tbl.Identifier(), meta, tbl.MetadataLocation(), tbl.FS, nil), nil
}

// Rewrite writes the rows of tasks' files, with their deletes applied and,
// with transform, as transform returns them, into new files of tbl's
// target size, and returns them. It reads the files in order, one at a
// time, as the layout it plans says; budget is the memory it may hold, as
// counted by the allocator of ctx (see Metered). transform takes each batch
// it is given (it must release it) and returns the batch to write, or nil
// for none.
func Rewrite(ctx context.Context, tbl *table.Table, fs iceio.IO, tasks []table.FileScanTask, budget int64,
	transform func(arrow.RecordBatch) (arrow.RecordBatch, error)) ([]iceberg.DataFile, Layout, error) {
	rowBytes, rowGroup, err := measure(ctx, fs, tasks)
	if err != nil {
		return nil, Layout{}, fmt.Errorf("measure rows: %w", err)
	}
	maxRowGroup := int64(DefaultRowGroupRows)
	if n, err := strconv.ParseInt(tbl.Properties()[table.ParquetRowGroupLimitKey], 10, 64); err == nil && n > 0 {
		maxRowGroup = n
	}
	// The reader holds a row group's compressed column chunks, and two of
	// them where a batch spans two row groups.
	lay, err := Plan(budget, rowBytes, 2*rowGroup+deleteBytes(tasks), maxRowGroup)
	if err != nil {
		return nil, Layout{}, err
	}
	view, err := withProperties(tbl, iceberg.Properties{
		table.ParquetBatchSizeKey:     strconv.FormatInt(lay.BatchRows, 10),
		table.ParquetRowGroupLimitKey: strconv.FormatInt(lay.RowGroupRows, 10),
	})
	if err == nil {
		// The files are not sorted, whatever the table's order: they say so,
		// so that clustering knows them for files to sort.
		view, err = unsorted(view)
	}
	if err != nil {
		return nil, lay, err
	}
	projection, err := view.Scan().Projection()
	if err != nil {
		return nil, lay, err
	}
	schema, err := table.SchemaToArrowSchema(projection, nil, false, false)
	if err != nil {
		return nil, lay, err
	}
	// One reader, so batches arrive in the files' order without being held
	// back to reorder them, and are read only as fast as they are written.
	var records iter.Seq2[arrow.RecordBatch, error] = func(yield func(arrow.RecordBatch, error) bool) {
		_, recs, err := view.Scan(table.WitMaxConcurrency(1)).ReadTasks(ctx, tasks)
		if err != nil {
			yield(nil, err)
			return
		}
		for rec, err := range recs {
			if err == nil && transform != nil {
				if rec, err = transform(rec); err == nil && rec == nil {
					continue
				}
			}
			if !yield(rec, err) || err != nil {
				return
			}
		}
	}
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, view, schema, records, table.WithClusteredWrite()) {
		if err != nil {
			if cause := context.Cause(ctx); cause != nil {
				err = cause
			}
			return nil, lay, fmt.Errorf("rewrite %d files: %w", len(tasks), err)
		}
		files = append(files, df)
	}
	return files, lay, nil
}
