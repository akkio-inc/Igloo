package distributed

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Sorted results merged. A statement whose result is the final rows of a
// stage, ordered by ORDER BY without LIMIT, had the coordinator sort all of
// them, in its memory and spilling to its disk. The stage's tasks now sort
// their own rows, and the coordinator merges their files by the order's
// columns into one, which the rest of the statement reads in order, without
// sorting: memory for a batch of rows a task, whatever the result. It does
// so where it can compare the values as DuckDB orders them: integers,
// floating-point numbers (NaN last), text and BLOBs by their bytes, without
// a collation, dates, times, timestamps, decimals and booleans; and where
// the session keeps the order rows are read in (preserve_insertion_order).
// Otherwise the coordinator sorts, as before.
//
// A plan whose session reads the merge as it is made (Plan.StreamMerged)
// gets it as a stream of batches, which the statement reads through a
// table function (see MergedFunction): rows reach the client as the
// coordinator merges them, rather than once it has merged every one into a
// file, which for hundreds of millions of rows took minutes before the
// first.

// mergeOrdered merges the files of the stage whose rows are the result of
// final, the statement the coordinator runs, when its tasks sorted them by
// final's ORDER BY (Stage.merge) and final only projects and filters them:
// the stage's result becomes the one merged file, or the stream that
// merges them as the statement reads it, and final loses its ORDER BY.
// Where they cannot be merged, final sorts them as before.
func (co *Coordinator) mergeOrdered(ctx context.Context, final node, stages []*Stage, run *execution, settings []setting, dir, qid string, stream bool) {
	from := child(final, "from_table")
	if str(final["type"]) != "SELECT_NODE" || str(from["type"]) != "BASE_TABLE" || len(list(final, "group_expressions")) > 0 ||
		child(final, "qualify") != nil || hasClass(final["select_list"], "WINDOW") || len(list(child(final, "cte_map"), "map")) > 0 {
		return
	}
	var s *Stage
	for _, st := range stages {
		if st.Out == nil && st.placeholder != "" && st.placeholder == str(from["table_name"]) {
			s = st
		}
	}
	mods := list(final, "modifiers")
	if s == nil || len(s.merge) == 0 || len(mods) != 1 || str(mods[0].(node)["type"]) != "ORDER_MODIFIER" {
		return
	}
	res := run.results[s.ID]
	if res == nil {
		return
	}
	keys, ok := resolveMerge(s.merge, res.columns, settings)
	if !ok {
		return
	}
	if stream {
		// Read as the session's statement reads it, which its cancel
		// stops; the files are removed with the execution's directory.
		r, err := newMergeReader(context.WithoutCancel(ctx), res.files, keys)
		if err == nil {
			if _, _, err = mergedColumns(r.Schema()); err != nil {
				r.Release()
			}
		}
		if err == nil {
			res.merged = &MergedView{Reader: r}
			final["modifiers"] = []any{}
			co.log.Debug("merging a stage's sorted rows as the statement reads them", "query", qid, "stage", s.String(), "files", len(res.files))
			return
		}
		if !errors.Is(err, errNotMergeable) || IsUnsupported(err) {
			co.log.Debug("could not merge a stage's sorted rows; the coordinator sorts them", "query", qid, "stage", s.String(), "err", err)
			return
		}
		// Columns the function does not return: merged into a file.
	}
	start := time.Now()
	out := filepath.Join(dir, fmt.Sprintf("%d-merged.parquet", s.ID))
	if err := mergeSorted(ctx, res.files, keys, out); err != nil {
		co.log.Debug("could not merge a stage's sorted rows; the coordinator sorts them", "query", qid, "stage", s.String(), "err", err)
		_ = os.Remove(out)
		return
	}
	for _, f := range res.files {
		_ = os.Remove(f)
	}
	res.files = []string{out}
	final["modifiers"] = []any{}
	co.log.Debug("merged a stage's sorted rows", "query", qid, "stage", s.String(), "seconds", time.Since(start).Seconds())
}

// mergeKey is a column of a coordinator-bound stage's rows that its tasks
// sort by and the coordinator merges by: order and nulls as the ORDER BY
// says them (ORDER_DEFAULT, ASCENDING or DESCENDING; ORDER_DEFAULT,
// NULLS FIRST or NULLS LAST).
type mergeKey struct {
	column       string
	order, nulls string
}

// mergeTypes are the DuckDB types whose values, as Parquet holds them,
// compare as DuckDB orders them, by prefix of the type's name.
var mergeTypes = []string{"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
	"FLOAT", "DOUBLE", "VARCHAR", "DATE", "TIMESTAMP", "TIME", "DECIMAL", "BOOLEAN", "BLOB"}

// mergeType reports whether values of DuckDB type t merge (see mergeTypes).
func mergeType(t string) bool {
	switch {
	case strings.HasPrefix(t, "TIME WITH TIME ZONE"), strings.HasPrefix(t, "TIMETZ"), strings.HasPrefix(t, "TIMESTAMP WITH TIME ZONE"),
		strings.HasPrefix(t, "TIMESTAMPTZ"):
		// Ordered by their instants, as Parquet holds them in UTC: fine for
		// TIMESTAMPTZ, not for TIME WITH TIME ZONE.
		return strings.HasPrefix(t, "TIMESTAMP")
	case strings.Contains(t, "[") || strings.Contains(t, "(") && !strings.HasPrefix(t, "DECIMAL"):
		return false
	}
	for _, p := range mergeTypes {
		if t == p || strings.HasPrefix(t, p+"(") || strings.HasPrefix(t, p+"_") || p == "DECIMAL" && strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// resolvedKey is a mergeKey with its column's position and its order
// worked out.
type resolvedKey struct {
	col              int
	desc, nullsFirst bool
}

// resolveMerge works out how keys order a stage's rows, of columns cols,
// in a session of settings; ok is false where the coordinator cannot merge
// them (see the top of this file).
func resolveMerge(keys []mergeKey, cols []Column, settings []setting) ([]resolvedKey, bool) {
	get := func(name string) string {
		for _, s := range settings {
			if strings.EqualFold(s.Name, name) {
				return strings.ToLower(strings.TrimSpace(s.Value))
			}
		}
		return ""
	}
	if v := get("preserve_insertion_order"); v == "false" || v == "0" {
		return nil, false
	}
	if get("default_collation") != "" {
		return nil, false
	}
	defaultDesc := false
	switch get("default_order") {
	case "", "asc", "ascending":
	case "desc", "descending":
		defaultDesc = true
	default:
		return nil, false
	}
	nullOrder := strings.ReplaceAll(get("default_null_order"), " ", "_")
	switch nullOrder {
	case "", "nulls_last", "nulls_first", "nulls_first_on_asc_last_on_desc", "nulls_last_on_asc_first_on_desc", "sqlite", "postgres":
	default:
		return nil, false
	}
	var out []resolvedKey
	for _, k := range keys {
		i := columnIndex(cols, k.column)
		if i < 0 || !mergeType(cols[i].Type) {
			return nil, false
		}
		r := resolvedKey{col: i, desc: defaultDesc}
		switch k.order {
		case "ASCENDING":
			r.desc = false
		case "DESCENDING":
			r.desc = true
		case "ORDER_DEFAULT", "":
		default:
			return nil, false
		}
		switch k.nulls {
		case "NULLS FIRST", "NULLS_FIRST":
			r.nullsFirst = true
		case "NULLS LAST", "NULLS_LAST":
		case "ORDER_DEFAULT", "":
			switch nullOrder {
			case "nulls_first":
				r.nullsFirst = true
			case "nulls_first_on_asc_last_on_desc", "sqlite":
				r.nullsFirst = !r.desc
			case "nulls_last_on_asc_first_on_desc", "postgres":
				r.nullsFirst = r.desc
			}
		default:
			return nil, false
		}
		out = append(out, r)
	}
	return out, len(out) > 0
}

// errNotMergeable means the files' columns cannot be compared as DuckDB
// orders them: the coordinator sorts instead.
var errNotMergeable = errors.New("rows not mergeable")

// mergeCursor is a file being merged: its current batch, retained, its
// current row, and the batch's key columns.
type mergeCursor struct {
	rr   pqarrow.RecordReader
	pf   *file.Reader
	rec  arrow.RecordBatch
	row  int
	keys []arrow.Array
}

// load reads the cursor's next batch that has rows; false at the end of
// the file.
func (c *mergeCursor) load(keys []resolvedKey) (bool, error) {
	for c.rr.Next() {
		rec := c.rr.RecordBatch()
		if rec.NumRows() == 0 {
			continue
		}
		rec.Retain()
		c.rec, c.row = rec, 0
		c.keys = c.keys[:0]
		for _, k := range keys {
			c.keys = append(c.keys, rec.Column(k.col))
		}
		return true, nil
	}
	return false, c.rr.Err()
}

// advance moves the cursor to its next row. Past its batch's last row, it
// releases that batch and reads the next; ok is false at the end of the
// file.
func (c *mergeCursor) advance(keys []resolvedKey) (ok bool, err error) {
	c.row++
	if c.row < int(c.rec.NumRows()) {
		return true, nil
	}
	c.rec.Release()
	c.rec = nil
	return c.load(keys)
}

func (c *mergeCursor) close() {
	if c.rec != nil {
		c.rec.Release()
	}
	if c.rr != nil {
		c.rr.Release()
	}
	if c.pf != nil {
		_ = c.pf.Close()
	}
}

// mergeHeap orders cursors by their current rows.
type mergeHeap struct {
	cs   []*mergeCursor
	keys []resolvedKey
	err  error
}

func (h *mergeHeap) Len() int      { return len(h.cs) }
func (h *mergeHeap) Swap(i, j int) { h.cs[i], h.cs[j] = h.cs[j], h.cs[i] }
func (h *mergeHeap) Less(i, j int) bool {
	return h.compare(h.cs[i], h.cs[j]) < 0
}
func (h *mergeHeap) Push(x any) { h.cs = append(h.cs, x.(*mergeCursor)) }
func (h *mergeHeap) Pop() any {
	c := h.cs[len(h.cs)-1]
	h.cs = h.cs[:len(h.cs)-1]
	return c
}

// compare orders the current rows of a and b by the keys.
func (h *mergeHeap) compare(a, b *mergeCursor) int {
	for i, k := range h.keys {
		x, y := a.keys[i], b.keys[i]
		xn, yn := x.IsNull(a.row), y.IsNull(b.row)
		switch {
		case xn && yn:
			continue
		case xn || yn:
			if xn == k.nullsFirst {
				return -1
			}
			return 1
		}
		c, err := compareValues(x, a.row, y, b.row)
		if err != nil && h.err == nil {
			h.err = err
		}
		if c != 0 {
			if k.desc {
				return -c
			}
			return c
		}
	}
	return 0
}

// compareValues compares non-null values of arrays of one type as DuckDB
// orders them.
func compareValues(x arrow.Array, i int, y arrow.Array, j int) (int, error) {
	switch a := x.(type) {
	case *array.Int8:
		return cmpOrdered(a.Value(i), y.(*array.Int8).Value(j)), nil
	case *array.Int16:
		return cmpOrdered(a.Value(i), y.(*array.Int16).Value(j)), nil
	case *array.Int32:
		return cmpOrdered(a.Value(i), y.(*array.Int32).Value(j)), nil
	case *array.Int64:
		return cmpOrdered(a.Value(i), y.(*array.Int64).Value(j)), nil
	case *array.Uint8:
		return cmpOrdered(a.Value(i), y.(*array.Uint8).Value(j)), nil
	case *array.Uint16:
		return cmpOrdered(a.Value(i), y.(*array.Uint16).Value(j)), nil
	case *array.Uint32:
		return cmpOrdered(a.Value(i), y.(*array.Uint32).Value(j)), nil
	case *array.Uint64:
		return cmpOrdered(a.Value(i), y.(*array.Uint64).Value(j)), nil
	case *array.Float32:
		return cmpFloat(float64(a.Value(i)), float64(y.(*array.Float32).Value(j))), nil
	case *array.Float64:
		return cmpFloat(a.Value(i), y.(*array.Float64).Value(j)), nil
	case *array.String:
		return strings.Compare(a.Value(i), y.(*array.String).Value(j)), nil
	case *array.LargeString:
		return strings.Compare(a.Value(i), y.(*array.LargeString).Value(j)), nil
	case *array.Binary:
		return bytes.Compare(a.Value(i), y.(*array.Binary).Value(j)), nil
	case *array.LargeBinary:
		return bytes.Compare(a.Value(i), y.(*array.LargeBinary).Value(j)), nil
	case *array.Date32:
		return cmpOrdered(a.Value(i), y.(*array.Date32).Value(j)), nil
	case *array.Date64:
		return cmpOrdered(a.Value(i), y.(*array.Date64).Value(j)), nil
	case *array.Timestamp:
		return cmpOrdered(a.Value(i), y.(*array.Timestamp).Value(j)), nil
	case *array.Time32:
		return cmpOrdered(a.Value(i), y.(*array.Time32).Value(j)), nil
	case *array.Time64:
		return cmpOrdered(a.Value(i), y.(*array.Time64).Value(j)), nil
	case *array.Decimal128:
		return a.Value(i).Cmp(y.(*array.Decimal128).Value(j)), nil
	case *array.Boolean:
		av, bv := a.Value(i), y.(*array.Boolean).Value(j)
		switch {
		case av == bv:
			return 0, nil
		case !av:
			return -1, nil
		}
		return 1, nil
	}
	return 0, fmt.Errorf("%w: a column of %s", errNotMergeable, x.DataType())
}

func cmpOrdered[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | arrow.Date32 | arrow.Date64 | arrow.Timestamp | arrow.Time32 | arrow.Time64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// cmpFloat orders floating-point numbers as DuckDB does: NaN after every
// other number, and -0 as 0.
func cmpFloat(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)
	switch {
	case an && bn:
		return 0
	case an:
		return 1
	case bn:
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// mergeableArrow reports whether compareValues compares values of dt.
func mergeableArrow(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.TIME32, arrow.TIME64, arrow.DECIMAL128, arrow.BOOL:
		return true
	}
	return false
}

// mergeBatch is how many rows a batch of merged rows holds, mergeRowGroup
// the most rows of a row group of a merged file, and mergeReadBuffer what
// the merge reads of each file at a time: it reads every task's file of a
// stage at once, hundreds of them, and a row group's column chunks read
// whole held megabytes each.
const (
	mergeBatch      = 8192
	mergeRowGroup   = 122_880
	mergeReadBuffer = 1 << 20
)

// mergeReader is an array.RecordReader of the rows of files, each sorted by
// keys, merged in order by them, in batches of mergeBatch rows. It reads
// each file a batch at a time, so that it holds a batch a file and the
// batch it makes, whatever the files hold; the first batch comes once each
// file's first is read. Its columns are copied value by value into the
// batch it makes where their types allow (see appender), and otherwise
// sliced from the files' batches and concatenated.
type mergeReader struct {
	ctx     context.Context
	refs    atomic.Int64
	schema  *arrow.Schema
	keys    []resolvedKey
	cursors []*mergeCursor
	h       *mergeHeap
	// b and appends copy rows value by value; without them, runs are the
	// batch's runs of consecutive rows of the files' batches, each holding
	// its batch.
	b       *array.RecordBuilder
	appends []func(arrow.Array, int)
	runs    []mergeRun
	rec     arrow.RecordBatch
	err     error
}

// mergeRun is consecutive rows, from start to end, of a file's batch.
type mergeRun struct {
	rec        arrow.RecordBatch
	start, end int
}

// newMergeReader opens files, each sorted by keys, to merge them. It fails
// with errNotMergeable where the columns cannot be compared, before it
// reads a row. ctx ends the merge.
func newMergeReader(ctx context.Context, files []string, keys []resolvedKey) (_ *mergeReader, err error) {
	mem := memory.DefaultAllocator
	r := &mergeReader{ctx: ctx, keys: keys}
	r.refs.Store(1)
	defer func() {
		if err != nil {
			r.Release()
		}
	}()
	props := parquet.NewReaderProperties(mem)
	props.BufferedStreamEnabled, props.BufferSize = true, mergeReadBuffer
	for _, path := range files {
		pf, err := file.OpenParquetFile(path, false, file.WithReadProps(props))
		if err != nil {
			return nil, err
		}
		c := &mergeCursor{pf: pf}
		r.cursors = append(r.cursors, c)
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: mergeBatch}, mem)
		if err != nil {
			return nil, err
		}
		s, err := fr.Schema()
		if err != nil {
			return nil, err
		}
		if r.schema == nil {
			r.schema = s
		} else if !r.schema.Equal(s) {
			return nil, fmt.Errorf("%w: files of different columns", errNotMergeable)
		}
		if c.rr, err = fr.GetRecordReader(ctx, nil, nil); err != nil {
			return nil, err
		}
	}
	if r.schema == nil {
		return nil, fmt.Errorf("%w: no files", errNotMergeable)
	}
	for _, k := range keys {
		if k.col >= len(r.schema.Fields()) || !mergeableArrow(r.schema.Field(k.col).Type) {
			return nil, fmt.Errorf("%w: column %d", errNotMergeable, k.col)
		}
	}
	r.h = &mergeHeap{keys: keys}
	for _, c := range r.cursors {
		ok, err := c.load(keys)
		if err != nil {
			return nil, err
		}
		if ok {
			r.h.cs = append(r.h.cs, c)
		}
	}
	heap.Init(r.h)
	b := array.NewRecordBuilder(mem, r.schema)
	for i := range r.schema.Fields() {
		f, ok := appender(b.Field(i))
		if !ok {
			b.Release()
			r.appends = nil
			return r, nil
		}
		r.appends = append(r.appends, f)
	}
	r.b = b
	return r, nil
}

func (r *mergeReader) Retain()                        { r.refs.Add(1) }
func (r *mergeReader) Schema() *arrow.Schema          { return r.schema }
func (r *mergeReader) RecordBatch() arrow.RecordBatch { return r.rec }
func (r *mergeReader) Record() arrow.RecordBatch      { return r.rec }
func (r *mergeReader) Err() error                     { return r.err }

// Release closes the files once the last reference is released.
func (r *mergeReader) Release() {
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.b != nil {
		r.b.Release()
	}
	r.dropRuns()
	for _, c := range r.cursors {
		c.close()
	}
	r.cursors = nil
}

// Next makes the next batch of merged rows.
func (r *mergeReader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.err != nil || r.h == nil || r.h.Len() == 0 {
		return false
	}
	if r.err = r.ctx.Err(); r.err != nil {
		return false
	}
	if r.b != nil {
		r.b.Reserve(mergeBatch)
	}
	n := 0
	for n < mergeBatch && r.h.Len() > 0 {
		c := r.h.cs[0]
		if r.b != nil {
			for i, f := range r.appends {
				f(c.rec.Column(i), c.row)
			}
		} else if last := len(r.runs) - 1; last >= 0 && r.runs[last].rec == c.rec && r.runs[last].end == c.row {
			r.runs[last].end++
		} else {
			c.rec.Retain()
			r.runs = append(r.runs, mergeRun{rec: c.rec, start: c.row, end: c.row + 1})
		}
		n++
		ok, err := c.advance(r.keys)
		if err != nil {
			r.err = err
			return false
		}
		if ok {
			heap.Fix(r.h, 0)
		} else {
			heap.Pop(r.h)
		}
		if r.h.err != nil {
			r.err = r.h.err
			return false
		}
	}
	if r.b != nil {
		r.rec = r.b.NewRecordBatch()
		return true
	}
	r.rec, r.err = r.concatRuns(n)
	r.dropRuns()
	return r.err == nil
}

// concatRuns makes a batch of the n rows of the runs.
func (r *mergeReader) concatRuns(n int) (arrow.RecordBatch, error) {
	cols := make([]arrow.Array, len(r.schema.Fields()))
	defer func() {
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
	}()
	parts := make([]arrow.Array, len(r.runs))
	for i := range cols {
		for j, run := range r.runs {
			parts[j] = array.NewSlice(run.rec.Column(i), int64(run.start), int64(run.end))
		}
		col, err := array.Concatenate(parts, memory.DefaultAllocator)
		for _, p := range parts {
			p.Release()
		}
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	return array.NewRecordBatch(r.schema, cols, int64(n)), nil
}

func (r *mergeReader) dropRuns() {
	for _, run := range r.runs {
		run.rec.Release()
	}
	r.runs = r.runs[:0]
}

// appender returns what copies a value of an array of b's type into b, for
// the types it copies value by value.
func appender(b array.Builder) (func(arrow.Array, int), bool) {
	switch b := b.(type) {
	case *array.Int8Builder:
		return appendOf(b, b.Append, (*array.Int8).Value), true
	case *array.Int16Builder:
		return appendOf(b, b.Append, (*array.Int16).Value), true
	case *array.Int32Builder:
		return appendOf(b, b.Append, (*array.Int32).Value), true
	case *array.Int64Builder:
		return appendOf(b, b.Append, (*array.Int64).Value), true
	case *array.Uint8Builder:
		return appendOf(b, b.Append, (*array.Uint8).Value), true
	case *array.Uint16Builder:
		return appendOf(b, b.Append, (*array.Uint16).Value), true
	case *array.Uint32Builder:
		return appendOf(b, b.Append, (*array.Uint32).Value), true
	case *array.Uint64Builder:
		return appendOf(b, b.Append, (*array.Uint64).Value), true
	case *array.Float32Builder:
		return appendOf(b, b.Append, (*array.Float32).Value), true
	case *array.Float64Builder:
		return appendOf(b, b.Append, (*array.Float64).Value), true
	case *array.StringBuilder:
		return appendOf(b, b.Append, (*array.String).Value), true
	case *array.LargeStringBuilder:
		return appendOf(b, b.Append, (*array.LargeString).Value), true
	case *array.BinaryBuilder:
		if b.Type().ID() == arrow.LARGE_BINARY {
			return appendOf(b, b.Append, (*array.LargeBinary).Value), true
		}
		return appendOf(b, b.Append, (*array.Binary).Value), true
	case *array.Date32Builder:
		return appendOf(b, b.Append, (*array.Date32).Value), true
	case *array.Date64Builder:
		return appendOf(b, b.Append, (*array.Date64).Value), true
	case *array.TimestampBuilder:
		return appendOf(b, b.Append, (*array.Timestamp).Value), true
	case *array.Time32Builder:
		return appendOf(b, b.Append, (*array.Time32).Value), true
	case *array.Time64Builder:
		return appendOf(b, b.Append, (*array.Time64).Value), true
	case *array.Decimal128Builder:
		return appendOf(b, b.Append, (*array.Decimal128).Value), true
	case *array.BooleanBuilder:
		return appendOf(b, b.Append, (*array.Boolean).Value), true
	}
	return nil, false
}

// appendOf is what copies values of arrays of type A, read by value, with
// add, into b, NULLs as NULLs.
func appendOf[A arrow.Array, T any](b array.Builder, add func(T), value func(A, int) T) func(arrow.Array, int) {
	return func(a arrow.Array, i int) {
		if a.IsNull(i) {
			b.AppendNull()
			return
		}
		add(value(a.(A), i))
	}
}

// mergeSorted merges files, each sorted by keys, into one Parquet file at
// out, sorted by them. It fails with errNotMergeable where the columns
// cannot be compared, before writing a row.
func mergeSorted(ctx context.Context, files []string, keys []resolvedKey, out string) (err error) {
	r, err := newMergeReader(ctx, files, keys)
	if err != nil {
		return err
	}
	defer r.Release()
	f, err := os.Create(out)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy), parquet.WithMaxRowGroupLength(mergeRowGroup),
		parquet.WithStoreDecimalAsInteger(true))
	// The writer closes what it writes to if it can: f is closed above.
	w, err := pqarrow.NewFileWriter(r.Schema(), struct{ io.Writer }{f}, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	for r.Next() {
		if err := w.WriteBuffered(r.RecordBatch()); err != nil {
			return err
		}
	}
	if err := r.Err(); err != nil {
		return err
	}
	return w.Close()
}
