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

// mergeOrdered merges the files of the stage whose rows are the result of
// final, the statement the coordinator runs, when its tasks sorted them by
// final's ORDER BY (Stage.merge) and final only projects and filters them:
// the stage's result becomes the one merged file, and final loses its
// ORDER BY. Where they cannot be merged, final sorts them as before.
func (co *Coordinator) mergeOrdered(ctx context.Context, final node, stages []*Stage, run *execution, settings []setting, dir, qid string) {
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
// returns that batch, for the caller to write what it needs of it and
// release, and reads the next; ok is false at the end of the file.
func (c *mergeCursor) advance(keys []resolvedKey) (left arrow.RecordBatch, ok bool, err error) {
	c.row++
	if c.row < int(c.rec.NumRows()) {
		return nil, true, nil
	}
	left, c.rec = c.rec, nil
	ok, err = c.load(keys)
	return left, ok, err
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

// mergeRowGroup is the most rows of a row group of a merged file.
const mergeRowGroup = 122_880

// mergeSorted merges files, each sorted by keys, into one Parquet file at
// out, sorted by them. It fails with errNotMergeable where the columns
// cannot be compared, before writing a row.
func mergeSorted(ctx context.Context, files []string, keys []resolvedKey, out string) (err error) {
	mem := memory.DefaultAllocator
	var cursors []*mergeCursor
	defer func() {
		for _, c := range cursors {
			c.close()
		}
	}()
	var schema *arrow.Schema
	for _, path := range files {
		pf, err := file.OpenParquetFile(path, false)
		if err != nil {
			return err
		}
		c := &mergeCursor{pf: pf}
		cursors = append(cursors, c)
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 8192}, mem)
		if err != nil {
			return err
		}
		s, err := fr.Schema()
		if err != nil {
			return err
		}
		if schema == nil {
			schema = s
		} else if !schema.Equal(s) {
			return fmt.Errorf("%w: files of different columns", errNotMergeable)
		}
		if c.rr, err = fr.GetRecordReader(ctx, nil, nil); err != nil {
			return err
		}
	}
	if schema == nil {
		return fmt.Errorf("%w: no files", errNotMergeable)
	}
	for _, k := range keys {
		if k.col >= len(schema.Fields()) || !mergeableArrow(schema.Field(k.col).Type) {
			return fmt.Errorf("%w: column %d", errNotMergeable, k.col)
		}
	}
	h := &mergeHeap{keys: keys}
	for _, c := range cursors {
		ok, err := c.load(keys)
		if err != nil {
			return err
		}
		if ok {
			h.cs = append(h.cs, c)
		}
	}
	heap.Init(h)

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
	w, err := pqarrow.NewFileWriter(schema, struct{ io.Writer }{f}, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	// Consecutive rows of one batch are written together, as a slice of it.
	var run arrow.RecordBatch
	start, end := 0, 0
	flush := func() error {
		if run == nil || end <= start {
			return nil
		}
		s := run.NewSlice(int64(start), int64(end))
		defer s.Release()
		return w.WriteBuffered(s)
	}
	rows := 0
	for h.Len() > 0 {
		c := h.cs[0]
		if c.rec != run || c.row != end {
			if err := flush(); err != nil {
				return err
			}
			run, start = c.rec, c.row
		}
		end = c.row + 1
		left, ok, err := c.advance(keys)
		if left != nil {
			// The run's batch is done: write it before letting it go.
			err = errors.Join(err, flush())
			left.Release()
			run = nil
		}
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(h, 0)
		} else {
			heap.Pop(h)
		}
		if h.err != nil {
			return h.err
		}
		if rows++; rows%65536 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if err := flush(); err != nil {
		return err
	}
	return w.Close()
}
