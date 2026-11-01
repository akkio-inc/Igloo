package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
)

// Reads of more rows in order than memory holds, on this replica, sorted
// in runs and merged.
//
// DuckDB's sort of more than its memory limit holds takes more than the
// limit as it starts to return rows, up to 40% more, which on a machine
// with little to spare besides is the memory watch's to cancel, or the
// kernel's to kill: a sort's overshoot is not in what its buffer manager
// counts. A sort of what fits does not overshoot. So a statement whose
// plan estimates it would not fit runs as workers run such a statement,
// with this replica as its only worker:
//
//  1. One run of the query, without its ORDER BY, writes its rows to
//     uncompressed Parquet files in the spill directory, each small enough
//     to sort well within memory. That takes little memory itself.
//  2. Each file is sorted, one at a time, into a run of its own.
//  3. The runs are merged as the client reads them (see
//     distributed.OpenMerge), in memory for a few batches a run, by
//     goroutines beside the session.
//
// The query runs once, so the rows are those one statement read and what
// its functions returned. Only an ORDER BY of the result's columns, by
// name, alias or position, of types the merge compares as DuckDB orders
// them, and without LIMIT, is sorted so; anything else sorts at once, as
// before, as does a statement with parameters, one whose client reads a few
// rows at a time, or one the plan estimates fits.

const (
	// largeSortShare is how much of DuckDB's memory limit the plan's
	// estimate of what a sort takes must pass for it to sort in runs.
	largeSortShare = 2
	// largeSortFileShare is the share of the memory limit an unsorted file holds
	// on disk, uncompressed, and largeSortRowsShare the share of it that the rows
	// of a file take by rowWidth, whichever comes first: a run's sort peaked
	// at nearly four times the first and twice the second, which leaves it
	// about half of the memory limit.
	largeSortFileShare = 8
	largeSortRowsShare = 4
	// largeSortRowGroup is the rows of a row group of an unsorted file.
	largeSortRowGroup = 122_880
)

// largeSort is a read of a query's rows in order, to sort in runs.
type largeSort struct {
	ordered *orderedQuery
	// fileBytes and fileRowGroups bound an unsorted file.
	fileBytes, fileRowGroups int64
	estimate                 int64
}

// memoryLimit is DuckDB's memory limit, or 0 when it is not known.
func (s *Server) memoryLimit(ctx context.Context) int64 {
	e, ok := s.engine.(interface {
		MemoryLimit(ctx context.Context) int64
	})
	if !ok {
		return 0
	}
	return e.MemoryLimit(ctx)
}

// largeSortOf returns how to read st's rows in runs, or nil when it should
// run as written (see the top of this file).
func (s *Server) largeSortOf(ctx context.Context, sess *session, st *statement) *largeSort {
	if !s.streamMerged || s.sortBudget < 0 {
		return nil
	}
	query := strings.TrimRight(st.prepared, "; \t\r\n")
	pos := topLevelKeyword(query, "ORDER", "BY")
	// A plan costs a bind, which most statements should not pay: those
	// that keep few rows ask for none.
	if pos < 0 || topLevelKeyword(query, "LIMIT") >= 0 || topLevelKeyword(query, "GROUP", "BY") >= 0 {
		return nil
	}
	limit := s.memoryLimit(ctx)
	if limit <= 0 {
		return nil
	}
	rows, err := estimatedRows(ctx, sess, strings.TrimRight(query[:pos], " \t\r\n"))
	if err != nil || rows <= 0 {
		return nil
	}
	// However wide its columns (see rowWidth), which most statements stop
	// at, before their columns are read.
	if float64(rows)*float64(48*(len(st.columns)+1)) <= float64(limit)/largeSortShare {
		return nil
	}
	for _, name := range []string{"preserve_insertion_order", "default_collation"} {
		v, err := settingValue(ctx, sess.conn, name)
		if err != nil || name == "default_collation" && v != "" || name == "preserve_insertion_order" && !strings.EqualFold(v, "true") {
			return nil
		}
	}
	o := orderedQueryOf(ctx, sess, query)
	if o == nil {
		return nil
	}
	for _, c := range o.cols {
		if !distributed.MergeableType(c.typ) {
			return nil
		}
	}
	width := rowWidth(o.cols)
	estimate := int64(math.Min(float64(rows)*float64(width), math.MaxInt64/2))
	if estimate <= limit/largeSortShare {
		return nil
	}
	if budget := s.sortSpillBudget(ctx, sess); budget <= 0 || estimate > budget {
		return nil
	}
	return &largeSort{
		ordered:       o,
		fileBytes:     max(limit/largeSortFileShare, 1<<20),
		fileRowGroups: max(limit/largeSortRowsShare/width/largeSortRowGroup, 1),
		estimate:      estimate,
	}
}

// runLargeSort runs st, when its rows in order are more than memory holds,
// in runs (see the top of this file). done is false when st should run as
// written.
func (s *Server) runLargeSort(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) (done bool, err error) {
	if st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows || len(args) > 0 || len(st.params) > 0 || !drains(w) || st.tempFill != nil {
		return false, nil
	}
	l := s.largeSortOf(ctx, sess, st)
	if l == nil {
		return false, nil
	}
	tempDir, err := settingValue(ctx, sess.conn, "temp_directory")
	if err != nil || tempDir == "" {
		return false, nil
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	// The next engine to open the spill directory removes it, should the
	// process die.
	dir := filepath.Join(tempDir, engine.SortedWriteDirPrefix+hex.EncodeToString(b[:]))
	defer os.RemoveAll(dir) //nolint:errcheck
	asWritten := func(step string, err error) (bool, error) {
		if ctx.Err() != nil {
			return true, err
		}
		s.log.Warn("a read in order is sorted at once, as it could not be sorted in runs", "step", step, "err", err)
		return false, nil
	}
	files := filepath.Join(dir, "files")
	if err := os.MkdirAll(files, 0o700); err != nil {
		return asWritten("directory", err)
	}
	exec := func(q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	start := time.Now()

	// 1. The rows, in files small enough to sort well within memory.
	if err := exec("COPY (\n" + l.ordered.query + "\n) TO " + quoteLiteral(files) + " (FORMAT parquet, COMPRESSION uncompressed, ROW_GROUP_SIZE " +
		strconv.Itoa(largeSortRowGroup) + ", FILE_SIZE_BYTES " + strconv.FormatInt(l.fileBytes, 10) +
		", ROW_GROUPS_PER_FILE " + strconv.FormatInt(l.fileRowGroups, 10) + ")"); err != nil {
		return asWritten("files", err)
	}
	entries, err := os.ReadDir(files)
	if err != nil {
		return asWritten("list", err)
	}
	var unsorted []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".parquet") {
			unsorted = append(unsorted, filepath.Join(files, e.Name()))
		}
	}
	slices.Sort(unsorted)
	s.log.Debug("wrote a read's rows to files, to sort in runs", "files", len(unsorted), "seconds", time.Since(start).Seconds())

	// 2. Each sorted into a run, the file going as its run comes.
	var runs []string
	for i, f := range unsorted {
		run := filepath.Join(dir, fmt.Sprintf("run-%d.parquet", i))
		if err := exec("COPY (SELECT * FROM read_parquet(" + quoteLiteral(f) + ") ORDER BY " + l.ordered.orderBy() + ") TO " +
			quoteLiteral(run) + " (FORMAT parquet)"); err != nil {
			return asWritten("runs", err)
		}
		_ = os.Remove(f)
		runs = append(runs, run)
	}
	if len(runs) == 0 {
		return false, nil
	}
	s.log.Debug("sorted a read's rows in runs", "runs", len(runs), "seconds", time.Since(start).Seconds())

	// 3. The runs, merged as the client reads them.
	keys := make([]distributed.MergeKey, len(l.ordered.keys))
	for i, k := range l.ordered.keys {
		keys[i] = distributed.MergeKey{Column: slices.IndexFunc(l.ordered.cols, func(c columnType) bool { return c.name == k.column }), Desc: k.desc, NullsFirst: k.nullsFirst}
	}
	merged, err := distributed.OpenMerge(context.WithoutCancel(ctx), runs, keys)
	if err != nil {
		return asWritten("merge", err)
	}
	defer merged.Reader.Release()
	cols := make([]string, len(l.ordered.cols))
	for i, c := range l.ordered.cols {
		cols[i] = "CAST(" + quoteIdent(c.name) + " AS " + c.typ + ") AS " + quoteIdent(c.name)
	}
	final := "SELECT " + strings.Join(cols, ", ") + " FROM " + distributed.MergedFunction + "()"
	if err := st.runFinal(ctx, sess.conn, final, []*distributed.MergedView{merged}, args, w); err != nil {
		if w.Written() > 0 {
			return true, err
		}
		return asWritten("read", err)
	}
	s.metrics.largeSort()
	s.log.Info("read rows in order, sorted in runs", "runs", len(runs), "rows", w.Written(), "estimate_bytes", l.estimate, "seconds", time.Since(start).Seconds())
	return true, nil
}
