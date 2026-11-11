package distributed

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
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
// a collation, dates, times, timestamps, decimals and booleans; where the
// session keeps the order rows are read in (preserve_insertion_order); and on
// a coordinator of MergeCores cores and DuckDB threads or more. Otherwise the
// coordinator sorts, as before.
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
	if !mergesKeepUp(co.opts.Cores, co.opts.Threads) {
		co.log.Debug("sorting a stage's sorted rows again, as a merge would not keep up on this coordinator's cores", "query", qid,
			"stage", s.String(), "cores", co.opts.Cores, "threads", co.opts.Threads)
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

// MergeCores is the fewest cores (logical CPUs), and DuckDB threads, on which
// the coordinator merges a stage's sorted rows; on fewer, DuckDB sorts them,
// as one machine does. The merge decodes each file and merges groups of them
// on goroutines of their own, and DuckDB copies the merged rows in: 4 million
// rows from 32 sorted files, read through DuckDB as a statement reads them,
// took 0.98 times DuckDB's own sort of them on 16 cores and 1.03 to 1.07 on 4
// without hyperthreads, but 1.26 on CI's 4 vCPUs, which are 2 cores'
// hyperthreads, 1.5 on 2 cores and 1.8 on 1. Below 8 logical CPUs it is at
// best even with DuckDB's sort.
const MergeCores = 8

// mergesKeepUp says a merge of sorted rows keeps up with DuckDB's sort of
// them on cores cores (0 for those Go may use) and threads DuckDB threads (0
// for one a core).
func mergesKeepUp(cores, threads int) bool {
	if cores <= 0 {
		cores = runtime.GOMAXPROCS(0)
	}
	if threads <= 0 {
		threads = cores
	}
	return min(cores, threads) >= MergeCores
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

// mergeBatch is how many rows a batch of merged rows holds, mergeRowGroup
// the most rows of a row group of a merged file, and mergeReadBuffer what
// the merge reads of each file at a time: it reads every task's file of a
// stage at once, hundreds of them, and a row group's column chunks read
// whole held megabytes each. mergeAhead is how many batches the merge
// makes, and each file decodes, ahead of their reader.
const (
	mergeBatch      = 8192
	mergeRowGroup   = 122_880
	mergeReadBuffer = 1 << 20
	mergeAhead      = 2
)

// mergeGroup is the most files one goroutine merges: more are merged in
// groups of at most that many, at once, and the groups' merged rows merged
// in turn, so that the comparisons run on several cores; a variable for
// tests.
var mergeGroup = 8

// batchSource is what a cursor reads batches from: a file's record reader,
// or the merge of a group of files.
type batchSource interface {
	Next() bool
	RecordBatch() arrow.RecordBatch
	Err() error
	Release()
}

// mergeCursor is a file being merged: its current batch, retained, its
// current row, and the batch's key columns. A goroutine of its own decodes
// its batches ahead (see start), so that the files' decoding runs on as
// many cores as there are files, beside the merge.
type mergeCursor struct {
	rr   batchSource
	pf   *file.Reader
	rec  arrow.RecordBatch
	row  int
	keys []keyColumn

	batches  chan arrow.RecordBatch
	done     chan struct{}
	finished chan struct{}
	// err is the decoding's, set before batches closes.
	err error
}

// start decodes the file's batches that have rows, mergeAhead ahead.
func (c *mergeCursor) start() {
	c.batches = make(chan arrow.RecordBatch, mergeAhead)
	c.done = make(chan struct{})
	c.finished = make(chan struct{})
	go func() {
		defer close(c.finished)
		defer close(c.batches)
		for c.rr.Next() {
			rec := c.rr.RecordBatch()
			if rec.NumRows() == 0 {
				continue
			}
			rec.Retain()
			select {
			case c.batches <- rec:
			case <-c.done:
				rec.Release()
				return
			}
		}
		c.err = c.rr.Err()
	}()
}

// load takes the cursor's next batch; false at the end of the file.
func (c *mergeCursor) load(keys []resolvedKey) (bool, error) {
	rec, ok := <-c.batches
	if !ok {
		return false, c.err
	}
	c.rec, c.row = rec, 0
	c.keys = c.keys[:0]
	for _, k := range keys {
		c.keys = append(c.keys, newKeyColumn(rec.Column(k.col)))
	}
	return true, nil
}

// advance moves the cursor to its next row. Past its batch's last row, it
// releases that batch and takes the next; ok is false at the end of the
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

// close stops the decoding and closes the file.
func (c *mergeCursor) close() {
	if c.done != nil {
		close(c.done)
		for rec := range c.batches {
			rec.Release()
		}
		<-c.finished
	}
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

// keyColumn is a key column of a cursor's batch, which compares its values
// with those of the same column of another cursor's batch, by their type
// as DuckDB orders it (see newKeyColumn).
type keyColumn interface {
	// compare compares non-NULL value i with o's value j.
	compare(i int, o keyColumn, j int) int
	null(i int) bool
}

// nullable answers whether a value is NULL, cheaply where none is.
type nullable struct {
	a     arrow.Array
	nulls bool
}

func (n nullable) null(i int) bool { return n.nulls && n.a.IsNull(i) }

// orderedKey is a key of numbers, or of dates, times or timestamps by
// their ticks.
type orderedKey[T cmp.Ordered] struct {
	nullable
	v []T
}

func (k orderedKey[T]) compare(i int, o keyColumn, j int) int {
	return cmp.Compare(k.v[i], o.(orderedKey[T]).v[j])
}

// floatKey is a key of floating-point numbers, NaN after every other
// number, as DuckDB orders them.
type floatKey[T float32 | float64] struct {
	nullable
	v []T
}

func (k floatKey[T]) compare(i int, o keyColumn, j int) int {
	return cmpFloat(float64(k.v[i]), float64(o.(floatKey[T]).v[j]))
}

// stringKey is a key of text, by its bytes.
type stringKey struct {
	nullable
	a *array.String
}

func (k stringKey) compare(i int, o keyColumn, j int) int {
	return strings.Compare(k.a.Value(i), o.(stringKey).a.Value(j))
}

type largeStringKey struct {
	nullable
	a *array.LargeString
}

func (k largeStringKey) compare(i int, o keyColumn, j int) int {
	return strings.Compare(k.a.Value(i), o.(largeStringKey).a.Value(j))
}

// bytesKey is a key of BLOBs, by their bytes.
type bytesKey struct {
	nullable
	value func(int) []byte
}

func (k bytesKey) compare(i int, o keyColumn, j int) int {
	return bytes.Compare(k.value(i), o.(bytesKey).value(j))
}

type decimalKey struct {
	nullable
	v []decimal128.Num
}

func (k decimalKey) compare(i int, o keyColumn, j int) int {
	return k.v[i].Cmp(o.(decimalKey).v[j])
}

type boolKey struct {
	nullable
	a *array.Boolean
}

func (k boolKey) compare(i int, o keyColumn, j int) int {
	x, y := k.a.Value(i), o.(boolKey).a.Value(j)
	switch {
	case x == y:
		return 0
	case !x:
		return -1
	}
	return 1
}

// newKeyColumn returns a key column of a, an array of a type mergeableArrow
// reports.
func newKeyColumn(a arrow.Array) keyColumn {
	n := nullable{a: a, nulls: a.NullN() > 0}
	switch a := a.(type) {
	case *array.Int8:
		return orderedKey[int8]{n, a.Int8Values()}
	case *array.Int16:
		return orderedKey[int16]{n, a.Int16Values()}
	case *array.Int32:
		return orderedKey[int32]{n, a.Int32Values()}
	case *array.Int64:
		return orderedKey[int64]{n, a.Int64Values()}
	case *array.Uint8:
		return orderedKey[uint8]{n, a.Uint8Values()}
	case *array.Uint16:
		return orderedKey[uint16]{n, a.Uint16Values()}
	case *array.Uint32:
		return orderedKey[uint32]{n, a.Uint32Values()}
	case *array.Uint64:
		return orderedKey[uint64]{n, a.Uint64Values()}
	case *array.Float32:
		return floatKey[float32]{n, a.Float32Values()}
	case *array.Float64:
		return floatKey[float64]{n, a.Float64Values()}
	case *array.String:
		return stringKey{n, a}
	case *array.LargeString:
		return largeStringKey{n, a}
	case *array.Binary:
		return bytesKey{n, a.Value}
	case *array.LargeBinary:
		return bytesKey{n, a.Value}
	case *array.Date32:
		return orderedKey[arrow.Date32]{n, a.Date32Values()}
	case *array.Date64:
		return orderedKey[arrow.Date64]{n, a.Date64Values()}
	case *array.Timestamp:
		return orderedKey[arrow.Timestamp]{n, a.TimestampValues()}
	case *array.Time32:
		return orderedKey[arrow.Time32]{n, a.Time32Values()}
	case *array.Time64:
		return orderedKey[arrow.Time64]{n, a.Time64Values()}
	case *array.Decimal128:
		return decimalKey{n, a.Values()}
	case *array.Boolean:
		return boolKey{n, a}
	}
	panic(fmt.Sprintf("merge by a column of %s", a.DataType()))
}

// rowOrder orders the current rows of cursors a and b by keys, a tie by
// the cursors' order, so that the merge is the same every time.
func rowOrder(keys []resolvedKey, a, b *mergeCursor, ai, bi int) int {
	for i, k := range keys {
		x, y := a.keys[i], b.keys[i]
		xn, yn := x.null(a.row), y.null(b.row)
		switch {
		case xn && yn:
			continue
		case xn || yn:
			if xn == k.nullsFirst {
				return -1
			}
			return 1
		}
		if c := x.compare(a.row, y, b.row); c != 0 {
			if k.desc {
				return -c
			}
			return c
		}
	}
	return cmp.Compare(ai, bi)
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

// mergeableArrow reports whether keyColumn compares values of dt.
func mergeableArrow(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64, arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY,
		arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP, arrow.TIME32, arrow.TIME64, arrow.DECIMAL128, arrow.BOOL:
		return true
	}
	return false
}

// loserTree picks the cursor whose current row comes first, in about
// log2(cursors) comparisons a row: each inner node holds the cursor that
// lost the match there, and winner the one that won them all. An ended
// cursor loses every match.
type loserTree struct {
	cs     []*mergeCursor
	ended  []bool
	keys   []resolvedKey
	size   int
	losers []int
	winner int
}

func newLoserTree(cs []*mergeCursor, ended []bool, keys []resolvedKey) *loserTree {
	t := &loserTree{cs: cs, ended: ended, keys: keys, size: 1}
	for t.size < len(cs) {
		t.size *= 2
	}
	t.losers = make([]int, t.size)
	// Play each match bottom up: winners[node] is who won below it.
	winners := make([]int, 2*t.size)
	for i := range t.size {
		winners[t.size+i] = i
	}
	for node := t.size - 1; node >= 1; node-- {
		a, b := winners[2*node], winners[2*node+1]
		if t.beats(a, b) {
			winners[node], t.losers[node] = a, b
		} else {
			winners[node], t.losers[node] = b, a
		}
	}
	t.winner = winners[1]
	return t
}

// beats reports whether cursor a's row comes before cursor b's: a leaf
// past the cursors, or an ended cursor, comes after every other.
func (t *loserTree) beats(a, b int) bool {
	aOut := a >= len(t.cs) || t.ended[a]
	bOut := b >= len(t.cs) || t.ended[b]
	switch {
	case aOut:
		return false
	case bOut:
		return true
	}
	return rowOrder(t.keys, t.cs[a], t.cs[b], a, b) < 0
}

// replay plays the matches again from the winner's leaf up, once the
// winner has moved to its next row or ended.
func (t *loserTree) replay() {
	w := t.winner
	for node := (t.size + w) / 2; node >= 1; node /= 2 {
		if l := t.losers[node]; t.beats(l, w) {
			t.losers[node], w = w, l
		}
	}
	t.winner = w
}

// done reports whether every cursor ended.
func (t *loserTree) done() bool { return t.winner >= len(t.cs) || t.ended[t.winner] }

// mergeReader is an array.RecordReader of the rows of files, each sorted by
// keys, merged in order by them, in batches of mergeBatch rows. Each file
// decodes its batches in a goroutine of its own, and the merge runs in
// another, mergeAhead batches ahead of the reader, so that the reader, as
// DuckDB, copies one batch while the next is merged and the files' next
// decoded. It holds a few batches a file and the batches it makes, whatever
// the files hold; the first batch comes once each file's first is decoded.
// Its columns are copied value by value into the batch it makes where their
// types allow (see appender), and otherwise sliced from the files' batches
// and concatenated.
type mergeReader struct {
	ctx     context.Context
	refs    atomic.Int64
	schema  *arrow.Schema
	keys    []resolvedKey
	cursors []*mergeCursor
	ended   []bool
	tree    *loserTree
	// b and appends copy rows value by value; without them, runs are the
	// batch's runs of consecutive rows of the files' batches, each holding
	// its batch.
	b       *array.RecordBuilder
	appends []func(arrow.Array, int)
	runs    []mergeRun
	rec     arrow.RecordBatch
	err     error

	// batches are the merge's, made in a goroutine once Next is first
	// called, ahead of them; its error is set before batches closes.
	ahead    int
	started  bool
	batches  chan arrow.RecordBatch
	done     chan struct{}
	finished chan struct{}
	aheadErr error
}

// mergeRun is consecutive rows, from start to end, of a file's batch.
type mergeRun struct {
	rec        arrow.RecordBatch
	start, end int
}

// mergeReaderAhead is how many batches the merge a statement reads makes
// ahead of it: DuckDB fills its stream's buffer from the merge in bursts,
// between which the session reads the buffer, and a merge that waits for
// each burst leaves the two taking turns rather than running at once.
const mergeReaderAhead = 16

// newMergeReader opens files, each sorted by keys, to merge them. It fails
// with errNotMergeable where the columns cannot be compared, before it
// reads a row. ctx ends the merge.
func newMergeReader(ctx context.Context, files []string, keys []resolvedKey) (*mergeReader, error) {
	return openMerge(ctx, files, keys, mergeReaderAhead)
}

// openMerge opens files to merge, as newMergeReader does, making batches
// ahead of its reader.
func openMerge(ctx context.Context, files []string, keys []resolvedKey, ahead int) (_ *mergeReader, err error) {
	mem := memory.DefaultAllocator
	r := &mergeReader{ctx: ctx, keys: keys, ahead: ahead}
	r.refs.Store(1)
	defer func() {
		if err != nil {
			r.Release()
		}
	}()
	if len(files) > mergeGroup {
		// Groups of files merged at once, each into a cursor of this merge.
		groups := (len(files) + mergeGroup - 1) / mergeGroup
		for g := range groups {
			sub, err := openMerge(ctx, files[g*len(files)/groups:(g+1)*len(files)/groups], keys, mergeAhead)
			if err != nil {
				return nil, err
			}
			r.cursors = append(r.cursors, &mergeCursor{rr: sub})
			if r.schema == nil {
				r.schema = sub.schema
			} else if !r.schema.Equal(sub.schema) {
				return nil, fmt.Errorf("%w: files of different columns", errNotMergeable)
			}
		}
		return r, r.begin(mem)
	}
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
	return r, r.begin(mem)
}

// begin checks the merge's keys, starts its cursors, takes their first
// batches and readies what makes its batches.
func (r *mergeReader) begin(mem memory.Allocator) error {
	keys := r.keys
	for _, k := range keys {
		if k.col >= len(r.schema.Fields()) || !mergeableArrow(r.schema.Field(k.col).Type) {
			return fmt.Errorf("%w: column %d", errNotMergeable, k.col)
		}
	}
	for _, c := range r.cursors {
		c.start()
	}
	r.ended = make([]bool, len(r.cursors))
	for i, c := range r.cursors {
		ok, err := c.load(keys)
		if err != nil {
			return err
		}
		r.ended[i] = !ok
	}
	r.tree = newLoserTree(r.cursors, r.ended, keys)
	b := array.NewRecordBuilder(mem, r.schema)
	for i := range r.schema.Fields() {
		f, ok := appender(b.Field(i))
		if !ok {
			b.Release()
			r.appends = nil
			return nil
		}
		r.appends = append(r.appends, f)
	}
	r.b = b
	return nil
}

func (r *mergeReader) Retain()                        { r.refs.Add(1) }
func (r *mergeReader) Schema() *arrow.Schema          { return r.schema }
func (r *mergeReader) RecordBatch() arrow.RecordBatch { return r.rec }
func (r *mergeReader) Record() arrow.RecordBatch      { return r.rec }
func (r *mergeReader) Err() error                     { return r.err }

// Release stops the merge and closes the files once the last reference is
// released.
func (r *mergeReader) Release() {
	if r.refs.Add(-1) != 0 {
		return
	}
	if r.started {
		close(r.done)
		for rec := range r.batches {
			rec.Release()
		}
		<-r.finished
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

// Next takes the next batch of merged rows.
func (r *mergeReader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.err != nil || r.tree == nil {
		return false
	}
	if !r.started {
		r.start()
	}
	rec, ok := <-r.batches
	if !ok {
		r.err = r.aheadErr
		return false
	}
	r.rec = rec
	return true
}

// start merges batches in a goroutine, mergeAhead ahead of Next.
func (r *mergeReader) start() {
	r.started = true
	r.batches = make(chan arrow.RecordBatch, r.ahead)
	r.done = make(chan struct{})
	r.finished = make(chan struct{})
	go func() {
		defer close(r.finished)
		defer close(r.batches)
		for {
			rec, err := r.merge()
			if err != nil || rec == nil {
				r.aheadErr = err
				return
			}
			select {
			case r.batches <- rec:
			case <-r.done:
				rec.Release()
				return
			}
		}
	}()
}

// merge makes the next batch of merged rows, nil after the last.
func (r *mergeReader) merge() (arrow.RecordBatch, error) {
	if r.tree.done() {
		return nil, nil
	}
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	if r.b != nil {
		r.b.Reserve(mergeBatch)
	}
	n := 0
	for n < mergeBatch && !r.tree.done() {
		w := r.tree.winner
		c := r.cursors[w]
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
			return nil, err
		}
		r.ended[w] = !ok
		r.tree.replay()
	}
	if r.b != nil {
		return r.b.NewRecordBatch(), nil
	}
	rec, err := r.concatRuns(n)
	r.dropRuns()
	return rec, err
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
		if err := writeRowGroups(w, r.RecordBatch()); err != nil {
			return err
		}
	}
	if err := r.Err(); err != nil {
		return err
	}
	return w.Close()
}

// writeRowGroups writes rec to w, a writer of buffered row groups, in row
// groups of as many rows as take about RowGroupBytes at rec's bytes a row,
// or of mergeRowGroup rows, whichever is fewer: a merge of rows of bitmaps
// of megabytes each otherwise held 122,880 of them in its row group.
func writeRowGroups(w *pqarrow.FileWriter, rec arrow.RecordBatch) error {
	n := rec.NumRows()
	if n == 0 {
		return nil
	}
	var size int64
	for _, col := range rec.Columns() {
		size += recordBytes(col.Data())
	}
	fit := int64(RowGroupBytes) / max(size/n, 1)
	for lo := int64(0); lo < n; {
		held, err := w.RowGroupNumRows()
		if err != nil {
			return err
		}
		room := fit - int64(held)
		if held > 0 && room <= 0 {
			w.NewBufferedRowGroup()
			continue
		}
		hi := min(lo+max(room, 1), n)
		s := rec.NewSlice(lo, hi)
		err = w.WriteBuffered(s)
		s.Release()
		if err != nil {
			return err
		}
		lo = hi
	}
	return nil
}

// recordBytes is the bytes of an array's buffers and its children's.
func recordBytes(d arrow.ArrayData) int64 {
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += recordBytes(c)
	}
	return n
}
