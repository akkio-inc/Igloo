package filerewrite

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// Sorted rewrites. Clustering (see package maintenance) writes a table's
// rows again in its sort order, so that the files' ranges of the order's
// first column do not overlap. The rows rarely fit in memory, so Sorted
// partitions them by ranges of the order, each small enough to sort within
// the budget, spilling each range to a file of its own; then it sorts the
// ranges one at a time, in order, into one stream of rows that the writer
// cuts into files of the table's target size.
//
//  1. It samples the order's columns of the files (reading those columns
//     only) and picks the ranges' bounds from the sample.
//  2. It reads the rows once, with their deletes applied, and appends each
//     to its range's spill file (Arrow IPC, compressed).
//  3. It reads each range back, sorts it by every column of the order and
//     writes it.
//
// Many rows can share a value of the whole order (all members of one
// audience, say), more than fit in one range. Such a value is the bound of
// several ranges in a row, and its rows are dealt out among them in turn:
// rows with equal values may come in any order, so the ranges still follow
// each other in the order, and none holds more than its share.

// sortedDir names the directory, under the table's data, of the files
// Sorted writes sorted by fields: clustered- and a hash of the fields.
func sortedDir(fields []SortField) string {
	h := fnv.New32a()
	for _, f := range fields {
		fmt.Fprintf(h, "%s\x00%t\x00%t\x00", f.Name, f.Descending, f.NullsFirst)
	}
	return fmt.Sprintf("clustered-%08x", h.Sum32())
}

// WrittenSorted reports whether the data file at path was written by Sorted,
// sorted by fields. Iceberg records a sort order in data files, but writers
// (iceberg-go's among them) record the table's default order in files they
// write unsorted too, and Igloo keeps clustering keys out of the table's
// order (see catalog.ClusteringKeyProperty), so Sorted marks its files by
// where it puts them.
func WrittenSorted(path string, fields []SortField) bool {
	return len(fields) > 0 && strings.Contains(path, "/"+sortedDir(fields)+"/")
}

// SortField is a column of a sort order, as a rewrite orders rows by it.
type SortField struct {
	// Name is the column's name in the table's schema; it must be a column
	// of a primitive type at the top level.
	Name       string
	Descending bool
	NullsFirst bool
}

// ErrNotSortable is the cause of a sorted rewrite refused for its columns:
// one is not a top-level column, or has a type the rewrite cannot order.
var ErrNotSortable = errors.New("the files cannot be sorted by the table's sort order")

const (
	// maxSample bounds the keys a sorted rewrite samples to pick its ranges.
	maxSample = 1 << 18
	// samplesPerRange is how many sampled keys a range should get.
	samplesPerRange = 64
)

// RunDir names the directory, under the one of the files Sorted writes, of
// a run of level level (see RunOf): L<level>/ and a name of its own.
func RunDir(level int) string {
	return "L" + strconv.Itoa(level) + "/" + uuid.NewString()
}

// RunOf returns the level and name of the run the data file at path belongs
// to, if Sorted wrote it, sorted by fields, into a directory RunDir named.
func RunOf(path string, fields []SortField) (level int, run string, ok bool) {
	mark := "/" + sortedDir(fields) + "/L"
	i := strings.Index(path, mark)
	if len(fields) == 0 || i < 0 {
		return 0, "", false
	}
	rest := path[i+len(mark):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 {
		return 0, "", false
	}
	level, err := strconv.Atoi(rest[:slash])
	end := strings.IndexByte(rest[slash+1:], '/')
	if err != nil || end <= 0 {
		return 0, "", false
	}
	return level, rest[slash+1 : slash+1+end], true
}

// Sorted writes the rows of tasks' files, with their deletes applied, sorted
// by fields, into new files of tbl's target size, and returns them in their
// order. budget is the memory it may hold, as counted by the allocator of
// ctx (see Metered); dir is an empty directory for its spill files, which it
// removes as it goes. The files go to a directory of their own (see
// WrittenSorted), or with run set to RunDir's name to that run's under it.
func Sorted(ctx context.Context, tbl *table.Table, fs iceio.IO, tasks []table.FileScanTask, budget int64,
	fields []SortField, dir, run string) ([]iceberg.DataFile, Layout, error) {
	if len(fields) == 0 {
		return nil, Layout{}, fmt.Errorf("%w: no columns", ErrNotSortable)
	}
	rowBytes, rowGroup, err := measure(ctx, fs, tasks)
	if err != nil {
		return nil, Layout{}, fmt.Errorf("measure rows: %w", err)
	}
	maxRowGroup := int64(DefaultRowGroupRows)
	if n, err := strconv.ParseInt(tbl.Properties()[table.ParquetRowGroupLimitKey], 10, 64); err == nil && n > 0 {
		maxRowGroup = n
	}
	// Reading and writing get half the budget, and the range being sorted
	// the other half: the range as read, concatenated, and sorted.
	lay, err := Plan(budget/2, rowBytes, 2*rowGroup+deleteBytes(tasks), maxRowGroup)
	if err != nil {
		return nil, Layout{}, err
	}
	rangeRows := max(int64(MinRowGroupRows), budget/2/(3*max(rowBytes, 1)))
	// The first column's bounds in the new files are exact, so that files
	// whose ranges only touch are not taken for overlapping.
	// The files go to a directory of their own, which tells clustering they
	// are sorted (see WrittenSorted).
	data := tbl.Properties()[table.WriteDataPathKey]
	if data == "" {
		data = strings.TrimSuffix(tbl.Location(), "/") + "/data"
	}
	view, err := withProperties(tbl, iceberg.Properties{
		table.ParquetBatchSizeKey:                                strconv.FormatInt(lay.BatchRows, 10),
		table.ParquetRowGroupLimitKey:                            strconv.FormatInt(lay.RowGroupRows, 10),
		table.MetricsModeColumnConfPrefix + "." + fields[0].Name: "full",
		table.WriteDataPathKey:                                   strings.TrimSuffix(strings.TrimSuffix(data, "/")+"/"+sortedDir(fields)+"/"+run, "/"),
	})
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
	cols := make([]int, len(fields))
	for i, f := range fields {
		cols[i] = -1
		for j, pf := range projection.Fields() {
			if pf.Name == f.Name {
				cols[i] = j
			}
		}
		if cols[i] < 0 {
			return nil, lay, fmt.Errorf("%w: %s is not a column at the top level", ErrNotSortable, f.Name)
		}
		if !orderable(schema.Field(cols[i]).Type) {
			return nil, lay, fmt.Errorf("%w: %s is %s", ErrNotSortable, f.Name, schema.Field(cols[i]).Type)
		}
	}
	mem := compute.GetAllocator(ctx)

	var total int64
	for _, t := range tasks {
		total += t.File.Count()
	}
	var splits [][]key
	if ranges := (total + rangeRows - 1) / rangeRows; ranges > 1 {
		if splits, err = sampleSplits(ctx, view, tasks, fields, ranges, total); err != nil {
			return nil, lay, fmt.Errorf("sample the sort columns: %w", err)
		}
	}
	order := tupleOrder(fields)

	// 2. Each row to its range's spill file.
	read := func(yield func(arrow.RecordBatch, error) bool) {
		_, recs, err := view.Scan(table.WitMaxConcurrency(1)).ReadTasks(ctx, tasks)
		if err != nil {
			yield(nil, err)
			return
		}
		for rec, err := range recs {
			if !yield(rec, err) || err != nil {
				return
			}
		}
	}
	sp := &spill{dir: dir, schema: schema, mem: mem, ranges: len(splits) + 1}
	sp.flushRows = min(max(budget/8/(int64(sp.ranges)*max(rowBytes, 1)), 256), max(lay.BatchRows, 256))
	defer sp.close()
	for rec, err := range read {
		if err != nil {
			return nil, lay, readErr(ctx, err)
		}
		err = sp.route(ctx, rec, cols, splits, order)
		rec.Release()
		if err != nil {
			return nil, lay, readErr(ctx, err)
		}
	}
	if err := sp.finish(); err != nil {
		return nil, lay, readErr(ctx, err)
	}

	// 3. Each range sorted, in order, into the writer.
	records := func(yield func(arrow.RecordBatch, error) bool) {
		for r := range sp.ranges {
			rec, err := sp.load(r)
			if err != nil {
				yield(nil, err)
				return
			}
			if rec == nil {
				continue
			}
			sorted, err := sortRecord(ctx, rec, cols, fields)
			rec.Release()
			if err != nil {
				yield(nil, err)
				return
			}
			n := sorted.NumRows()
			for off := int64(0); off < n; off += lay.BatchRows {
				part := sorted.NewSlice(off, min(off+lay.BatchRows, n))
				if !yield(part, nil) {
					sorted.Release()
					return
				}
			}
			sorted.Release()
		}
	}
	var files []iceberg.DataFile
	for df, err := range table.WriteRecords(ctx, view, schema, records, table.WithClusteredWrite()) {
		if err != nil {
			return nil, lay, fmt.Errorf("rewrite %d files sorted: %w", len(tasks), readErr(ctx, err))
		}
		files = append(files, df)
	}
	return files, lay, nil
}

// readErr is err, or the cause the context was stopped for, such as
// ErrOverBudget.
func readErr(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return err
}

// sampleSplits reads the sort columns of tasks' files and returns the
// bounds of about ranges ranges of their values, in the order's sequence: a
// row goes to the first range whose bound comes after it (see spill.route).
// A value may be the bound of several ranges.
func sampleSplits(ctx context.Context, view *table.Table, tasks []table.FileScanTask, fields []SortField, ranges, total int64) ([][]key, error) {
	size := int(min(max(ranges*samplesPerRange, 4096), maxSample))
	sample := make([][]key, 0, size)
	rng := rand.New(rand.NewPCG(uint64(total), uint64(ranges)))
	var seen int64
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	_, recs, err := view.Scan(table.WithSelectedFields(names...), table.WitMaxConcurrency(1)).ReadTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}
	at := func(rec arrow.RecordBatch, i int) []key {
		t := make([]key, len(fields))
		for c, f := range fields {
			idx := rec.Schema().FieldIndices(f.Name)
			if len(idx) == 0 {
				t[c] = key{null: true}
				continue
			}
			t[c] = keyAt(rec.Column(idx[0]), i).own()
		}
		return t
	}
	for rec, err := range recs {
		if err != nil {
			return nil, err
		}
		for i := range int(rec.NumRows()) {
			seen++
			if len(sample) < size {
				sample = append(sample, at(rec, i))
			} else if j := rng.Int64N(seen); j < int64(size) {
				sample[j] = at(rec, i)
			}
		}
		rec.Release()
	}
	slices.SortFunc(sample, tupleOrder(fields))
	var splits [][]key
	for r := int64(1); r < ranges && len(sample) > 0; r++ {
		splits = append(splits, sample[int(r*int64(len(sample))/ranges)])
	}
	return splits, nil
}

// spill holds the ranges' spill files while rows are routed to them.
type spill struct {
	dir       string
	schema    *arrow.Schema
	mem       memory.Allocator
	ranges    int
	flushRows int64
	writers   map[int]*rangeFile
	pending   map[int][][]arrow.Array
	rows      map[int]int64
	// dealt counts, by the first of the bounds a value repeats, the rows of
	// that value dealt out among the ranges it bounds.
	dealt map[int]int
}

type rangeFile struct {
	f *os.File
	w *ipc.FileWriter
}

func (s *spill) path(r int) string { return filepath.Join(s.dir, fmt.Sprintf("range-%06d.arrow", r)) }

// route appends each row of rec to its range: the first whose bound comes
// after the row's values of columns cols. A row whose values are the bound
// of several ranges goes to one of them or the range after, in turn.
func (s *spill) route(ctx context.Context, rec arrow.RecordBatch, cols []int, splits [][]key, order func(a, b []key) int) error {
	if s.writers == nil {
		s.writers, s.pending, s.rows, s.dealt = map[int]*rangeFile{}, map[int][][]arrow.Array{}, map[int]int64{}, map[int]int{}
	}
	n := int(rec.NumRows())
	if n == 0 {
		return nil
	}
	if len(splits) == 0 {
		rec.Retain()
		return s.add(ctx, 0, rec.Columns(), int64(n), rec.Release)
	}
	arrs := make([]arrow.Array, len(cols))
	for c, col := range cols {
		arrs[c] = rec.Column(col)
	}
	k := make([]key, len(cols))
	idx := map[int][]int32{}
	for i := range n {
		for c, a := range arrs {
			k[c] = keyAt(a, i)
		}
		r := sort.Search(len(splits), func(j int) bool { return order(splits[j], k) > 0 })
		if r > 0 && order(splits[r-1], k) == 0 {
			// The row's values are bounds lo to r-1: ranges lo+1 to r hold
			// them alone or first.
			lo := sort.Search(r, func(j int) bool { return order(splits[j], k) >= 0 })
			r = lo + 1 + s.dealt[lo]%(r-lo)
			s.dealt[lo]++
		}
		idx[r] = append(idx[r], int32(i))
	}
	for r, rows := range idx {
		ind := array.NewInt32Data(array.NewData(arrow.PrimitiveTypes.Int32, len(rows),
			[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(rows))}, nil, 0, 0))
		cols := make([]arrow.Array, rec.NumCols())
		var err error
		for c := range cols {
			if cols[c], err = compute.TakeArray(ctx, rec.Column(c), ind); err != nil {
				break
			}
		}
		ind.Release()
		if err != nil {
			releaseAll(cols)
			return err
		}
		if err := s.add(ctx, r, cols, int64(len(rows)), func() { releaseAll(cols) }); err != nil {
			return err
		}
	}
	return nil
}

// add queues cols, n rows of range r, and writes the range's queue out once
// it holds flushRows rows. release gives cols back once they are written.
func (s *spill) add(_ context.Context, r int, cols []arrow.Array, n int64, release func()) error {
	own := make([]arrow.Array, len(cols))
	for i, c := range cols {
		c.Retain()
		own[i] = c
	}
	release()
	s.pending[r] = append(s.pending[r], own)
	s.rows[r] += n
	if s.rows[r] >= s.flushRows {
		return s.flush(r)
	}
	return nil
}

// flush writes range r's queued rows to its spill file.
func (s *spill) flush(r int) error {
	parts := s.pending[r]
	if len(parts) == 0 {
		return nil
	}
	defer func() {
		for _, p := range parts {
			releaseAll(p)
		}
		delete(s.pending, r)
		delete(s.rows, r)
	}()
	rf := s.writers[r]
	if rf == nil {
		f, err := os.Create(s.path(r))
		if err != nil {
			return err
		}
		w, err := ipc.NewFileWriter(f, ipc.WithSchema(s.schema), ipc.WithAllocator(s.mem), ipc.WithZstd())
		if err != nil {
			_ = f.Close()
			return err
		}
		rf = &rangeFile{f: f, w: w}
		s.writers[r] = rf
	}
	cols := make([]arrow.Array, len(s.schema.Fields()))
	for c := range cols {
		arrs := make([]arrow.Array, len(parts))
		for i, p := range parts {
			arrs[i] = p[c]
		}
		var err error
		if len(arrs) == 1 {
			arrs[0].Retain()
			cols[c] = arrs[0]
		} else if cols[c], err = array.Concatenate(arrs, s.mem); err != nil {
			releaseAll(cols)
			return err
		}
	}
	rec := array.NewRecordBatch(s.schema, cols, s.rows[r])
	releaseAll(cols)
	defer rec.Release()
	return rf.w.Write(rec)
}

// finish writes out every range's queue and closes the spill files.
func (s *spill) finish() error {
	for r := range s.ranges {
		if err := s.flush(r); err != nil {
			return err
		}
	}
	for r, rf := range s.writers {
		if err := errors.Join(rf.w.Close(), rf.f.Close()); err != nil {
			return err
		}
		delete(s.writers, r)
	}
	return nil
}

// load reads range r's rows back as one record, or nil if it has none, and
// removes its spill file.
func (s *spill) load(r int) (arrow.RecordBatch, error) {
	f, err := os.Open(s.path(r))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(s.path(r))
	}()
	rd, err := ipc.NewFileReader(f, ipc.WithAllocator(s.mem))
	if err != nil {
		return nil, err
	}
	defer rd.Close() //nolint:errcheck
	var batches []arrow.RecordBatch
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	var n int64
	for i := range rd.NumRecords() {
		b, err := rd.RecordBatchAt(i)
		if err != nil {
			return nil, err
		}
		batches = append(batches, b)
		n += b.NumRows()
	}
	if n == 0 {
		return nil, nil
	}
	cols := make([]arrow.Array, len(s.schema.Fields()))
	for c := range cols {
		arrs := make([]arrow.Array, len(batches))
		for i, b := range batches {
			arrs[i] = b.Column(c)
		}
		if cols[c], err = array.Concatenate(arrs, s.mem); err != nil {
			releaseAll(cols)
			return nil, err
		}
	}
	rec := array.NewRecordBatch(s.schema, cols, n)
	releaseAll(cols)
	return rec, nil
}

// close releases what routing still holds, if it stopped early.
func (s *spill) close() {
	for r, parts := range s.pending {
		for _, p := range parts {
			releaseAll(p)
		}
		delete(s.pending, r)
	}
	for r, rf := range s.writers {
		_ = rf.w.Close()
		_ = rf.f.Close()
		delete(s.writers, r)
	}
}

func releaseAll(arrs []arrow.Array) {
	for _, a := range arrs {
		if a != nil {
			a.Release()
		}
	}
}

// sortRecord returns rec's rows sorted by fields, columns cols of rec.
func sortRecord(ctx context.Context, rec arrow.RecordBatch, cols []int, fields []SortField) (arrow.RecordBatch, error) {
	n := int(rec.NumRows())
	cmps := make([]func(i, j int) int, len(fields))
	for k, f := range fields {
		arr := rec.Column(cols[k])
		natural := compareWithin(arr)
		dir := 1
		if f.Descending {
			dir = -1
		}
		nullsFirst := f.NullsFirst
		cmps[k] = func(i, j int) int {
			ni, nj := arr.IsNull(i), arr.IsNull(j)
			switch {
			case ni && nj:
				return 0
			case ni:
				if nullsFirst {
					return -1
				}
				return 1
			case nj:
				if nullsFirst {
					return 1
				}
				return -1
			}
			return dir * natural(i, j)
		}
	}
	perm := make([]int32, n)
	for i := range perm {
		perm[i] = int32(i)
	}
	slices.SortStableFunc(perm, func(a, b int32) int {
		for _, c := range cmps {
			if r := c(int(a), int(b)); r != 0 {
				return r
			}
		}
		return 0
	})
	ind := array.NewInt32Data(array.NewData(arrow.PrimitiveTypes.Int32, n,
		[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Int32Traits.CastToBytes(perm))}, nil, 0, 0))
	defer ind.Release()
	out := make([]arrow.Array, rec.NumCols())
	for c := range out {
		var err error
		if out[c], err = compute.TakeArray(ctx, rec.Column(c), ind); err != nil {
			releaseAll(out)
			return nil, err
		}
	}
	sorted := array.NewRecordBatch(rec.Schema(), out, rec.NumRows())
	releaseAll(out)
	return sorted, nil
}

// orderable reports whether a sorted rewrite can order values of type t.
func orderable(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.BOOL, arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP, arrow.DURATION,
		arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.DECIMAL128:
		return true
	}
	return false
}

// compareWithin returns the natural order of two non-null values of arr.
func compareWithin(arr arrow.Array) func(i, j int) int {
	return func(i, j int) int { return compareKeys(keyAt(arr, i), keyAt(arr, j)) }
}

// key is one value of a sort column, as routing and sampling compare them.
type key struct {
	null bool
	kind byte // 'i' signed, 'u' unsigned, 'f' float, 's' bytes, 'd' decimal
	i    int64
	u    uint64
	f    float64
	s    string
	d    decimal128.Num
}

// own returns k with its bytes copied out of the array they point into.
func (k key) own() key {
	k.s = string(bytes.Clone([]byte(k.s)))
	return k
}

// keyAt returns the value at row i of arr.
func keyAt(arr arrow.Array, i int) key {
	if arr.IsNull(i) {
		return key{null: true}
	}
	switch a := arr.(type) {
	case *array.Boolean:
		if a.Value(i) {
			return key{kind: 'i', i: 1}
		}
		return key{kind: 'i'}
	case *array.Int8:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Int16:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Int32:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Int64:
		return key{kind: 'i', i: a.Value(i)}
	case *array.Uint8:
		return key{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint16:
		return key{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint32:
		return key{kind: 'u', u: uint64(a.Value(i))}
	case *array.Uint64:
		return key{kind: 'u', u: a.Value(i)}
	case *array.Float32:
		return key{kind: 'f', f: float64(a.Value(i))}
	case *array.Float64:
		return key{kind: 'f', f: a.Value(i)}
	case *array.Date32:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Date64:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Time32:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Time64:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Timestamp:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.Duration:
		return key{kind: 'i', i: int64(a.Value(i))}
	case *array.String:
		return key{kind: 's', s: a.Value(i)}
	case *array.LargeString:
		return key{kind: 's', s: a.Value(i)}
	case *array.Binary:
		return key{kind: 's', s: string(a.Value(i))}
	case *array.LargeBinary:
		return key{kind: 's', s: string(a.Value(i))}
	case *array.FixedSizeBinary:
		return key{kind: 's', s: string(a.Value(i))}
	case *array.Decimal128:
		return key{kind: 'd', d: a.Value(i)}
	}
	return key{null: true}
}

// compareKeys is the natural order of two non-null keys of one column. NaN
// comes after every number, as in Iceberg's order.
func compareKeys(a, b key) int {
	switch a.kind {
	case 'i':
		return cmp.Compare(a.i, b.i)
	case 'u':
		return cmp.Compare(a.u, b.u)
	case 'f':
		an, bn := math.IsNaN(a.f), math.IsNaN(b.f)
		switch {
		case an && bn:
			return 0
		case an:
			return 1
		case bn:
			return -1
		}
		return cmp.Compare(a.f, b.f)
	case 's':
		return cmp.Compare(a.s, b.s)
	case 'd':
		return a.d.Cmp(b.d)
	}
	return 0
}

// keyOrder is the order of a sort column's keys, NULLs included.
func keyOrder(f SortField) func(a, b key) int {
	return func(a, b key) int {
		switch {
		case a.null && b.null:
			return 0
		case a.null:
			if f.NullsFirst {
				return -1
			}
			return 1
		case b.null:
			if f.NullsFirst {
				return 1
			}
			return -1
		}
		if f.Descending {
			return compareKeys(b, a)
		}
		return compareKeys(a, b)
	}
}

// tupleOrder is the order of the values of a sort order's columns, one key
// each.
func tupleOrder(fields []SortField) func(a, b []key) int {
	orders := make([]func(a, b key) int, len(fields))
	for i, f := range fields {
		orders[i] = keyOrder(f)
	}
	return func(a, b []key) int {
		for i, o := range orders {
			if c := o(a[i], b[i]); c != 0 {
				return c
			}
		}
		return 0
	}
}
