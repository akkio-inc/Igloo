package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
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
// its functions returned. An ORDER BY without LIMIT is sorted so. One of the
// result's columns, by name, alias or position, of types the merge compares
// as DuckDB orders them, the merge orders by those columns. Any other, of an
// expression or of other types, the rows take a column of its sort key
// (create_sort_key), whose bytes the merge orders them by, and columns of types
// Parquet or the merge would not pass on unchanged go through the files as text,
// and are cast back as they leave. A statement the plan estimates fits, or that
// keeps few rows, sorts at once, as does an ORDER BY with a subquery or a
// collation.
//
// A client that reads a few rows at a time, through a portal with a row limit,
// may run other statements on the session between its reads, which would end
// a result DuckDB streams on the session's connection: the merge is read on a
// connection of its own, which needs nothing of the session once the runs are
// sorted, until the portal has its last row or closes.

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
)

// sortKeyColumn is the column of a sort key that orders a read's runs.
const sortKeyColumn = "__igloo_sort_key"

// textTypes are the types that pass through the files as text unchanged, and
// types they nest; notText those that do not, wherever they nest.
var (
	textTypes = regexp.MustCompile(`(?i)^(U?HUGEINT|UUID|INTERVAL|JSON|TIME WITH TIME ZONE)$|^(STRUCT|MAP)\(|\]$`)
	notText   = regexp.MustCompile(`(?i)\b(UNION|ENUM|BIT|BIGNUM|VARINT|GEOMETRY)\b|\bCOLLATE\b`)
)

// sortKeyedQuery is query, a SELECT, without its ORDER BY and with a column of
// its order's sort key, and that order; nil if it is another statement, or
// its order cannot be told by a sort key: one that a subquery or a collation
// has a part in. Columns of cols the files cannot hold unchanged leave them
// cast to text (see the top of this file).
func sortKeyedQuery(ctx context.Context, sess *session, query string, args []driver.NamedValue) *orderedQuery {
	serialize := func(q string) (map[string]any, error) {
		text, err := queryOne(ctx, sess.conn, "SELECT system.main.json_serialize_sql($1::VARCHAR)::VARCHAR", q)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(strings.NewReader(text))
		dec.UseNumber()
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			return nil, err
		}
		if doc["error"] == true {
			return nil, errors.New("not a statement")
		}
		return doc, nil
	}
	nodeOf := func(doc map[string]any) map[string]any {
		statements := asList(doc["statements"])
		if len(statements) != 1 {
			return nil
		}
		return asMap(asMap(statements[0])["node"])
	}
	doc, err := serialize(query)
	if err != nil {
		return nil
	}
	node := nodeOf(doc)
	if str(node["type"]) != "SELECT_NODE" || len(asList(node["group_expressions"])) > 0 || node["having"] != nil ||
		str(node["aggregate_handling"]) != "STANDARD_HANDLING" {
		return nil
	}
	orders, ok := orderModifier(node)
	if !ok || len(orders) == 0 {
		return nil
	}
	defaultOrder, err := settingValue(ctx, sess.conn, "default_order")
	if err != nil {
		return nil
	}
	nullOrder, err := settingValue(ctx, sess.conn, "default_null_order")
	if err != nil {
		return nil
	}
	tmpl, err := serialize("SELECT create_sort_key(NULL, 'ASC NULLS LAST') AS " + sortKeyColumn)
	if err != nil {
		return nil
	}
	key := asMap(asList(nodeOf(tmpl)["select_list"])[0])
	constant := asList(key["children"])[1]
	items := asList(node["select_list"])
	star := false
	for _, it := range items {
		star = star || str(asMap(it)["class"]) == "STAR"
	}
	var children []any
	for _, o := range orders {
		item := asMap(o)
		expr := asMap(item["expression"])
		switch str(expr["class"]) {
		case "CONSTANT":
			// ORDER BY 2: the second column.
			f, ok := jsonNumber(asMap(expr["value"])["value"])
			if !ok || star || f != math.Trunc(f) || f < 1 || f > float64(len(items)) {
				return nil
			}
			expr = asMap(items[int(f)-1])
		case "COLUMN_REF":
			// A name is the result's column of that name first.
			if names := asList(expr["column_names"]); len(names) == 1 {
				for _, it := range items {
					if strings.EqualFold(str(asMap(it)["alias"]), str(names[0])) {
						expr = asMap(it)
					}
				}
			}
		}
		if orderedByOther(expr) {
			return nil
		}
		var k sortKey
		if !k.setOrder(item, strings.EqualFold(defaultOrder, "DESCENDING"), nullOrder) {
			return nil
		}
		e := asMap(cloneTree(expr))
		e["alias"] = ""
		modifiers := asMap(cloneTree(constant))
		asMap(modifiers["value"])["value"] = k.modifiers()
		children = append(children, e, modifiers)
	}
	key["children"] = children
	node["select_list"] = append(slices.Clone(items), key)
	node["modifiers"] = []any{}
	text, err := json.Marshal(doc)
	if err != nil {
		return nil
	}
	q, err := queryOne(ctx, sess.conn, "SELECT system.main.json_deserialize_sql($1::VARCHAR::JSON)", string(text))
	if err != nil {
		return nil
	}
	cols, err := describeWith(ctx, sess, "SELECT * FROM (\n"+q+"\n) AS "+sortSource, args)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var replaced []string
	for _, c := range cols {
		name := strings.ToLower(c.name)
		if seen[name] || strings.HasPrefix(name, "__igloo_") && !strings.EqualFold(c.name, sortKeyColumn) || notText.MatchString(c.typ) {
			return nil
		}
		seen[name] = true
		if !distributed.MergeableType(c.typ) {
			if !textTypes.MatchString(c.typ) {
				return nil
			}
			replaced = append(replaced, "CAST("+quoteIdent(c.name)+" AS VARCHAR) AS "+quoteIdent(c.name))
		}
	}
	if len(replaced) > 0 {
		q = "SELECT * REPLACE (" + strings.Join(replaced, ", ") + ") FROM (\n" + q + "\n) AS " + sortSource
	}
	return &orderedQuery{query: q, cols: cols, keys: []sortKey{{column: sortKeyColumn}}}
}

// orderedByOther reports whether e, an expression of an ORDER BY, has a part
// a sort key would not order by as the ORDER BY does.
func orderedByOther(e map[string]any) bool {
	found := false
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			if c := str(t["class"]); c == "SUBQUERY" || c == "COLLATE" {
				found = true
			}
			for _, c := range t {
				walk(c)
			}
		case []any:
			for _, c := range t {
				walk(c)
			}
		}
	}
	walk(e)
	return found
}

// largeSort is a read of a query's rows in order, to sort in runs.
type largeSort struct {
	ordered *orderedQuery
	// fileBytes and fileRowGroups bound an unsorted file, of row groups of
	// rowGroup rows.
	fileBytes, fileRowGroups, rowGroup int64
	estimate                           int64
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
func (s *Server) largeSortOf(ctx context.Context, sess *session, st *statement, args []driver.NamedValue) *largeSort {
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
	rows, err := estimatedRows(ctx, sess, strings.TrimRight(query[:pos], " \t\r\n"), args...)
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
	o := orderedQueryOf(ctx, sess, query, args...)
	if o == nil || slices.ContainsFunc(o.cols, func(c columnType) bool { return !distributed.MergeableType(c.typ) }) {
		if o = sortKeyedQuery(ctx, sess, query, args); o == nil {
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
	rowGroup := distributed.RowGroupRows(width)
	return &largeSort{
		ordered:       o,
		fileBytes:     max(limit/largeSortFileShare, 1<<20),
		fileRowGroups: max(limit/largeSortRowsShare/width/rowGroup, 1),
		rowGroup:      rowGroup,
		estimate:      estimate,
	}
}

// runRowGroup is the rows of a row group of the run of the unsorted file f:
// as many as take distributed.RowGroupBytes at the file's bytes a row,
// uncompressed as it is.
func runRowGroup(ctx context.Context, conn *duckdb.Conn, f string) (int64, error) {
	info, err := os.Stat(f)
	if err != nil {
		return 0, err
	}
	rows, err := queryRows(ctx, conn, "SELECT num_rows FROM parquet_file_metadata("+quoteLiteral(f)+")")
	if err != nil || len(rows) == 0 {
		return 0, fmt.Errorf("rows of %s: %w", f, err)
	}
	n, _ := rows[0][0].(int64)
	return distributed.RowGroupRows(info.Size() / max(n, 1)), nil
}

// runLargeSort runs st, when its rows in order are more than memory holds,
// in runs (see the top of this file). done is false when st should run as
// written.
func (s *Server) runLargeSort(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) (done bool, err error) {
	if st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows || st.tempFill != nil || drainerOf(w) == nil && arrowWriterOf(w) == nil {
		return false, nil
	}
	l := s.largeSortOf(ctx, sess, st, args)
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
	exec := func(q string, args []driver.NamedValue) error {
		_, err := sess.conn.ExecContext(ctx, q, args)
		return err
	}
	start := time.Now()

	// 1. The rows, in files small enough to sort well within memory.
	if err := exec("COPY (\n"+l.ordered.query+"\n) TO "+quoteLiteral(files)+" (FORMAT parquet, COMPRESSION uncompressed, ROW_GROUP_SIZE "+
		strconv.FormatInt(l.rowGroup, 10)+", FILE_SIZE_BYTES "+strconv.FormatInt(l.fileBytes, 10)+
		", ROW_GROUPS_PER_FILE "+strconv.FormatInt(l.fileRowGroups, 10)+")", args); err != nil {
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

	// 2. Each sorted into a run, the file going as its run comes, in row
	// groups of as many rows as take distributed.RowGroupBytes at the
	// file's bytes a row.
	var runs []string
	for i, f := range unsorted {
		run := filepath.Join(dir, fmt.Sprintf("run-%d.parquet", i))
		rowGroup, err := runRowGroup(ctx, sess.conn, f)
		if err != nil {
			return asWritten("runs", err)
		}
		if err := exec("COPY (SELECT * FROM read_parquet("+quoteLiteral(f)+") ORDER BY "+l.ordered.orderBy()+") TO "+
			quoteLiteral(run)+" (FORMAT parquet, ROW_GROUP_SIZE "+strconv.FormatInt(rowGroup, 10)+")", nil); err != nil {
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
	var cols []string
	for _, c := range l.ordered.cols {
		if !strings.EqualFold(c.name, sortKeyColumn) {
			cols = append(cols, "CAST("+quoteIdent(c.name)+" AS "+c.typ+") AS "+quoteIdent(c.name))
		}
	}
	final := "SELECT " + strings.Join(cols, ", ") + " FROM " + distributed.MergedFunction + "()"
	// The merge reads no more of the statement: it has no parameters left.
	read := &statement{stmtType: st.stmtType, query: st.query, columns: st.columns, names: st.names}
	conn := sess.conn
	if !drains(w) {
		// A client may run other statements between its reads.
		if conn, err = s.engine.Connect(ctx); err != nil {
			return asWritten("connection", err)
		}
		defer conn.Close() //nolint:errcheck
		read.streams = true
	}
	if err := read.runFinal(ctx, conn, final, []*distributed.MergedView{merged}, nil, w); err != nil {
		if w.Written() > 0 || errors.Is(err, wire.ErrSuspendedHandlerClosed) {
			return true, err
		}
		return asWritten("read", err)
	}
	s.metrics.largeSort()
	s.log.Info("read rows in order, sorted in runs", "runs", len(runs), "rows", w.Written(), "estimate_bytes", l.estimate, "seconds", time.Since(start).Seconds())
	return true, nil
}
