package pgwire

import (
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
)

// Writes of a query's rows in order, CREATE TABLE ... AS ... ORDER BY and
// INSERT INTO ... SELECT ... ORDER BY into a table of the Iceberg catalog,
// that are too large to sort at once are sorted a range of the order at a
// time.
//
// DuckDB sorts a query's whole result before the table's writer gets its
// first row, and spills what memory does not hold to the spill directory
// in its row layout, uncompressed: one sort of 2 billion rows outgrew the
// 150 GB free on a replica's disk, for a table of 60 GB. The order of a
// table's rows matters only as the order of its files (see
// sql.md#sort-orders-and-clustering), so the rows can be sorted in ranges
// of the order, each written after the one before it:
//
//  1. A sample of the query's result gives the bounds of ranges of the
//     order with about as many rows each.
//  2. One run of the query writes its rows to compressed Parquet files in
//     the spill directory, a directory for each range.
//  3. Ranges next to each other are sorted and written together, as many
//     at a time as the sort spill budget (IGLOO_SORT_SPILL_BUDGET)
//     allows, in the order's order.
//
// The query runs once to write the rows, so they are what it read and what
// its functions returned. The table gets the same rows in the same order
// as from one sort. An INSERT writes its batches in one transaction, so it
// commits once; CREATE TABLE ... AS fills its table under a temporary name
// (see stagedCreate). The Parquet files are deleted as the write ends, or
// by the next engine to open the spill directory if the process dies.
//
// A range's bounds are DuckDB's sort keys of the ORDER BY's values
// (create_sort_key), which sort as the ORDER BY does, whatever the columns'
// types, directions and NULLs. Only an ORDER BY of the result's columns,
// by name, alias or position, is sorted in ranges: one of other
// expressions sorts at once, as before, as does one with LIMIT or
// DISTINCT, an INSERT with parameters or into a member table, and a write
// whose plan estimates its rows fit the budget.

const (
	// maxSortRanges bounds the ranges a sorted write divides its rows
	// into, and so the directories it writes at once.
	maxSortRanges = 256
	// sortRangeCol and sortKeyCol are the columns a sorted write adds to
	// the query's rows while it divides them.
	sortRangeCol = "__igloo_range"
	sortKeyCol   = "__igloo_sort_key"
	// sortSource is the alias of the query in the statements of a sorted
	// write.
	sortSource = "__igloo_q"
)

// sortedWrite is a write of a query's rows in order, to sort a range of the
// order at a time (see the top of this file).
type sortedWrite struct {
	// insert starts the INSERTs that write the rows, up to their source:
	// INSERT INTO t (a, b), or INSERT INTO t BY NAME.
	insert string
	// query is the query without its ORDER BY.
	query string
	// keys are its ORDER BY, by the result's columns.
	keys []sortKey
	// estimate is the bytes the plan's estimate of the rows takes to sort,
	// and budget what the sort may spill.
	estimate, budget int64
}

// sortKey is a column of a query's result to sort by, with its direction
// and where its NULLs go.
type sortKey struct {
	column           string
	desc, nullsFirst bool
}

// modifiers is how DuckDB writes k's direction and NULLs, as in ORDER BY
// and create_sort_key.
func (k sortKey) modifiers() string {
	dir := "ASC"
	if k.desc {
		dir = "DESC"
	}
	if k.nullsFirst {
		return dir + " NULLS FIRST"
	}
	return dir + " NULLS LAST"
}

// orderBy is w's ORDER BY, by the rows' columns.
func (w *sortedWrite) orderBy() string {
	items := make([]string, len(w.keys))
	for i, k := range w.keys {
		items[i] = quoteIdent(k.column) + " " + k.modifiers()
	}
	return strings.Join(items, ", ")
}

// sortKey is the expression of the sort key of a row of w's query, which
// sortSource names.
func (w *sortedWrite) sortKey() string {
	args := make([]string, 0, 2*len(w.keys))
	for _, k := range w.keys {
		args = append(args, sortSource+"."+quoteIdent(k.column), quoteLiteral(k.modifiers()))
	}
	return "create_sort_key(" + strings.Join(args, ", ") + ")"
}

// head is p up to its source: INSERT INTO t (a, b), or INSERT INTO t BY
// NAME.
func (p parsedInsert) head() string {
	h := "INSERT INTO " + p.target
	if len(p.columns) > 0 {
		cols := make([]string, len(p.columns))
		for i, c := range p.columns {
			cols[i] = quoteIdent(c)
		}
		h += " (" + strings.Join(cols, ", ") + ")"
	}
	if p.byName {
		h += " BY NAME"
	}
	return h
}

// parseSortedInsert reads an INSERT whose rows come from a query, as
// parseInsert does, and one without a column list, which writes them by
// position.
func parseSortedInsert(text string) (parsedInsert, bool) {
	if p, ok := parseInsert(text); ok {
		return p, true
	}
	var p parsedInsert
	sc := &scanner{s: text}
	if !sc.keywords("INSERT", "INTO") {
		return p, false
	}
	sc.skip()
	start := sc.i
	if _, ok := sc.name(); !ok {
		return p, false
	}
	p.target = text[start:sc.i]
	sc.keywords("BY", "POSITION")
	sc.skip()
	p.source = strings.TrimRight(sc.rest(), "; \t\r\n")
	switch (&scanner{s: p.source}).word() {
	case "SELECT", "WITH", "FROM":
	default:
		return p, false
	}
	for _, t := range topWords(p.source) {
		if t.word == "RETURNING" || t.word == "CONFLICT" {
			return p, false
		}
	}
	return p, true
}

// runSortedInsert runs st, when it is an INSERT into a table of the
// catalog whose rows a query gives in order, too many to sort at once, a
// range of the order at a time. done is false when st should run as
// written.
func (s *Server) runSortedInsert(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) (done bool, err error) {
	if st.stmtType != duckdb.STATEMENT_TYPE_INSERT || st.ids != nil || len(args) > 0 || len(st.params) > 0 || s.sortBudget < 0 {
		return false, nil
	}
	p, ok := parseSortedInsert(st.prepared)
	if !ok {
		return false, nil
	}
	name, ok := (&scanner{s: p.target}).name()
	if !ok {
		return false, nil
	}
	if _, err := s.newTableTarget(ctx, sess, name); err != nil {
		return false, nil
	}
	sw := s.sortedWriteOf(ctx, sess, p.head(), p.source)
	if sw != nil {
		s.noWorkerWrite(p.target, "its rows are written in order, a range at a time")
	}
	if sw == nil {
		return false, nil
	}
	exec := func(ctx context.Context, q string) error {
		_, err := sess.conn.ExecContext(ctx, q, nil)
		return err
	}
	// In a block of the client's, the batches are part of it.
	own := !sess.inTx
	if own {
		if err := exec(ctx, "BEGIN TRANSACTION"); err != nil {
			return false, nil
		}
	}
	n, done, err := s.runSortedWrite(ctx, sess, sw)
	if own {
		if err == nil && done {
			err = exec(ctx, "COMMIT")
		} else {
			_ = exec(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}
	if !done && err == nil {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, w.Complete(commandTag(st.stmtType, st.query, n))
}

// sortedWriteOf returns how to write the rows of query, a statement that
// insert starts, a range of their order at a time, or nil when it should
// run as written: query has no ORDER BY of its result's columns alone, or
// its plan estimates its rows fit the sort spill budget.
func (s *Server) sortedWriteOf(ctx context.Context, sess *session, insert, query string) *sortedWrite {
	if s.sortBudget < 0 {
		return nil
	}
	o := orderedQueryOf(ctx, sess, query)
	if o == nil {
		return nil
	}
	w := &sortedWrite{insert: insert, query: o.query, keys: o.keys}
	if w.budget = s.sortSpillBudget(ctx, sess); w.budget <= 0 {
		return nil
	}
	rows, err := estimatedRows(ctx, sess, w.query)
	if err != nil {
		return nil
	}
	width := rowWidth(o.cols)
	w.estimate = int64(math.Min(float64(rows)*float64(width), math.MaxInt64/2))
	if w.estimate <= w.budget {
		return nil
	}
	return w
}

// orderedQuery is a query without its ORDER BY, the columns of its result,
// and its ORDER BY by those columns.
type orderedQuery struct {
	query string
	cols  []columnType
	keys  []sortKey
}

// orderBy is o's ORDER BY.
func (o *orderedQuery) orderBy() string {
	return (&sortedWrite{keys: o.keys}).orderBy()
}

// orderedQueryOf reads query, or returns nil when it has no ORDER BY of its
// result's columns alone, or a result that would not pass through Parquet
// files unchanged.
func orderedQueryOf(ctx context.Context, sess *session, query string, args ...driver.NamedValue) *orderedQuery {
	query = strings.TrimRight(query, "; \t\r\n")
	pos := topLevelKeyword(query, "ORDER", "BY")
	if pos < 0 || topLevelKeyword(query[pos+len("ORDER"):], "ORDER", "BY") >= 0 {
		return nil
	}
	node, ok := queryNode(ctx, sess, query)
	if !ok {
		return nil
	}
	orders, ok := orderModifier(node)
	if !ok || len(orders) == 0 {
		return nil
	}
	o := &orderedQuery{query: strings.TrimRight(query[:pos], " \t\r\n")}
	// What is left must be the query without its ORDER BY.
	if rest, ok := queryNode(ctx, sess, o.query); !ok {
		return nil
	} else if left, ok := orderModifier(rest); !ok || len(left) > 0 {
		return nil
	}
	var err error
	if o.cols, err = describeWith(ctx, sess, "SELECT * FROM (\n"+o.query+"\n) AS "+sortSource, args); err != nil || !sortableColumns(o.cols) {
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
	if o.keys, ok = sortKeysOf(node, orders, o.cols, strings.EqualFold(defaultOrder, "DESCENDING"), nullOrder); !ok {
		return nil
	}
	return o
}

// queryNode is DuckDB's parse of query's top node, if query is a query.
func queryNode(ctx context.Context, sess *session, query string) (map[string]any, bool) {
	tree, err := parseStatement(ctx, sess.conn, query)
	if err != nil {
		return nil, false
	}
	stmt, _ := tree.(map[string]any)
	node, ok := stmt["node"].(map[string]any)
	return node, ok
}

// orderModifier returns the items of the ORDER BY of node, a query's top
// node, or false when it has modifiers whose rows depend on the order:
// LIMIT, and DISTINCT, which DISTINCT ON keeps the first row of.
func orderModifier(node map[string]any) ([]any, bool) {
	var orders []any
	for _, m := range asList(node["modifiers"]) {
		mod, _ := m.(map[string]any)
		if str(mod["type"]) != "ORDER_MODIFIER" || orders != nil {
			return nil, false
		}
		orders = asList(mod["orders"])
		if orders == nil {
			orders = []any{}
		}
	}
	return orders, true
}

// sortKeysOf resolves orders, the ORDER BY of node, to columns of the
// result, cols, or reports false when an item is not one of them. Items
// without a direction or NULLs' place take the session's defaults.
func sortKeysOf(node map[string]any, orders []any, cols []columnType, defaultDesc bool, nullOrder string) ([]sortKey, bool) {
	keys := make([]sortKey, 0, len(orders))
	for _, o := range orders {
		item, _ := o.(map[string]any)
		expr, _ := item["expression"].(map[string]any)
		i := -1
		switch str(expr["class"]) {
		case "CONSTANT":
			// ORDER BY 2: the second column.
			v, _ := expr["value"].(map[string]any)
			if f, ok := jsonNumber(v["value"]); ok && f == math.Trunc(f) && f >= 1 && f <= float64(len(cols)) {
				i = int(f) - 1
			}
		case "COLUMN_REF":
			var names []string
			for _, n := range asList(expr["column_names"]) {
				names = append(names, str(n))
			}
			switch {
			case len(names) == 1:
				// A name is the result's column of that name first, as
				// DuckDB binds it.
				i = uniqueColumn(cols, names[0])
			case str(node["type"]) == "SELECT_NODE":
				i = selectedColumn(asList(node["select_list"]), len(cols), names)
			}
		}
		if i < 0 {
			return nil, false
		}
		k := sortKey{column: cols[i].name}
		if !k.setOrder(item, defaultDesc, nullOrder) {
			return nil, false
		}
		keys = append(keys, k)
	}
	return keys, true
}

// setOrder sets k's direction and where its NULLs go from item, an item of
// an ORDER BY in DuckDB's parse, under the session's defaults; false if it
// does not know them.
func (k *sortKey) setOrder(item map[string]any, defaultDesc bool, nullOrder string) bool {
	k.desc = defaultDesc
	switch str(item["type"]) {
	case "ASCENDING", "ORDER_ASCENDING":
		k.desc = false
	case "DESCENDING", "ORDER_DESCENDING":
		k.desc = true
	case "ORDER_DEFAULT", "":
	default:
		return false
	}
	switch strings.ReplaceAll(str(item["null_order"]), "_", " ") {
	case "NULLS FIRST":
		k.nullsFirst = true
	case "NULLS LAST":
	case "ORDER DEFAULT", "":
		switch strings.ToUpper(nullOrder) {
		case "NULLS_FIRST":
			k.nullsFirst = true
		case "NULLS_LAST":
		case "NULLS_FIRST_ON_ASC_LAST_ON_DESC":
			k.nullsFirst = !k.desc
		case "NULLS_LAST_ON_ASC_FIRST_ON_DESC":
			k.nullsFirst = k.desc
		default:
			return false
		}
	default:
		return false
	}
	return true
}

// jsonNumber reads a number of DuckDB's parse.
func jsonNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// uniqueColumn is the index of the only column of cols named name,
// whatever its case, or -1.
func uniqueColumn(cols []columnType, name string) int {
	found := -1
	for i, c := range cols {
		if strings.EqualFold(c.name, name) {
			if found >= 0 {
				return -1
			}
			found = i
		}
	}
	return found
}

// selectedColumn is the index of the result's column that selects the
// column names (such as t.a), or -1: the only item of the select list
// that is that column, when the list has an item for each of the
// result's n columns (no *).
func selectedColumn(selectList []any, n int, names []string) int {
	if len(selectList) != n {
		return -1
	}
	found := -1
	for i, e := range selectList {
		item, _ := e.(map[string]any)
		if str(item["class"]) != "COLUMN_REF" {
			continue
		}
		got := asList(item["column_names"])
		if len(got) != len(names) {
			continue
		}
		same := true
		for j, g := range got {
			same = same && strings.EqualFold(str(g), names[j])
		}
		if same {
			if found >= 0 {
				return -1
			}
			found = i
		}
	}
	return found
}

// lossyTypes are the types DuckDB does not read back from Parquet as it
// wrote them: HUGEINT as DOUBLE, BIT and BIGNUM as text; UNION it cannot
// write.
var lossyTypes = regexp.MustCompile(`(?i)\b(U?HUGEINT|BIT|BIGNUM|VARINT|UNION)\b`)

// sortableColumns reports whether a result of cols can pass through
// Parquet files unchanged, and be told apart from the columns a sorted
// write adds.
func sortableColumns(cols []columnType) bool {
	seen := map[string]bool{}
	for _, c := range cols {
		name := strings.ToLower(c.name)
		if seen[name] || strings.HasPrefix(name, "__igloo_") || lossyTypes.MatchString(c.typ) {
			return false
		}
		seen[name] = true
	}
	return len(cols) > 0
}

// rowWidth estimates the bytes a row of cols takes in a sort: what fixed
// width types take, and some for text and nested values.
func rowWidth(cols []columnType) int64 {
	width := int64(8)
	for _, c := range cols {
		t := strings.ToUpper(c.typ)
		switch {
		case t == "BOOLEAN" || t == "TINYINT" || t == "UTINYINT":
			width++
		case t == "SMALLINT" || t == "USMALLINT":
			width += 2
		case t == "INTEGER" || t == "UINTEGER" || t == "DATE" || t == "FLOAT":
			width += 4
		case t == "BIGINT" || t == "UBIGINT" || t == "DOUBLE" || strings.HasPrefix(t, "TIMESTAMP") || strings.HasPrefix(t, "TIME"):
			width += 8
		case strings.HasPrefix(t, "DECIMAL") || t == "UUID" || t == "INTERVAL":
			width += 16
		default:
			// Text, blobs and nested values: their header and some
			// content.
			width += 48
		}
	}
	return width
}

// sortSpillBudget is how many bytes a sort may spill: the configured
// budget, or half the free space in the spill directory; 0 when there is
// none.
func (s *Server) sortSpillBudget(ctx context.Context, sess *session) int64 {
	if s.sortBudget != 0 {
		return s.sortBudget
	}
	dir, err := settingValue(ctx, sess.conn, "temp_directory")
	if err != nil || dir == "" {
		return 0
	}
	free, err := engine.FreeDiskSpace(dir)
	if err != nil {
		return 0
	}
	return int64(free / 2)
}

// estimatedRows is how many rows DuckDB's plan of query estimates it
// gives.
func estimatedRows(ctx context.Context, sess *session, query string, args ...driver.NamedValue) (int64, error) {
	rows, err := sess.conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+query, args)
	if err != nil {
		return 0, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return 0, errors.New("no physical plan")
			}
			return 0, err
		}
		if key, _ := dest[0].(string); key != "physical_plan" || len(dest) < 2 {
			continue
		}
		var plan []any
		text, _ := dest[1].(string)
		if err := json.Unmarshal([]byte(text), &plan); err != nil || len(plan) == 0 {
			return 0, errors.New("unreadable physical plan")
		}
		return planRows(plan[0]), nil
	}
}

// planRows is the estimated rows of a node of DuckDB's plan: its own
// estimate, or else its children's, multiplied for a cross product and
// added up for a union.
func planRows(n any) int64 {
	node, _ := n.(map[string]any)
	extra, _ := node["extra_info"].(map[string]any)
	if v, err := strconv.ParseInt(str(extra["Estimated Cardinality"]), 10, 64); err == nil {
		return v
	}
	const most = math.MaxInt64 / 4
	var out int64
	for i, c := range asList(node["children"]) {
		r := planRows(c)
		switch str(node["name"]) {
		case "CROSS_PRODUCT":
			if i == 0 {
				out = r
			} else if r > 0 && out > most/r {
				out = most
			} else {
				out *= r
			}
		case "UNION":
			out = min(out+r, most)
		default:
			out = max(out, r)
		}
	}
	return out
}

// runSortedWrite writes w's rows a range of their order at a time (see
// the top of this file) and returns how many it wrote. done is false, with
// no error, when it failed before it wrote any: w then runs as written.
func (s *Server) runSortedWrite(ctx context.Context, sess *session, w *sortedWrite) (rows int64, done bool, err error) {
	tempDir, err := settingValue(ctx, sess.conn, "temp_directory")
	if err != nil || tempDir == "" {
		return 0, false, nil
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	dir := filepath.Join(tempDir, engine.SortedWriteDirPrefix+hex.EncodeToString(b[:]))
	defer os.RemoveAll(dir) //nolint:errcheck
	unsorted := func(step string, err error) (int64, bool, error) {
		if ctx.Err() != nil {
			return 0, true, err
		}
		s.log.Warn("a write in order is sorted at once, as it could not be sorted a range at a time", "step", step, "err", err)
		return 0, false, nil
	}
	exec := func(q string) (int64, error) {
		res, err := sess.conn.ExecContext(ctx, q, nil)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	}

	// 1. The ranges' bounds, from a sample of the rows' sort keys.
	ranges := int64(4)
	if w.budget > 0 {
		ranges = min(max(4*((w.estimate+w.budget-1)/w.budget), 4), maxSortRanges)
	}
	keys, width, err := s.sortSample(ctx, sess, w, int(min(max(ranges*200, 10_000), 100_000)))
	if err != nil {
		return unsorted("sample", err)
	}
	if len(keys) == 0 {
		// No rows, nothing to sort.
		n, err := exec(w.insert + "\n" + w.query)
		return n, true, err
	}
	bounds := rangeBounds(keys, int(ranges))

	// 2. The rows, divided into the ranges.
	divide := "COPY (SELECT * EXCLUDE (" + sortKeyCol + "), " + rangeOf(sortKeyCol, bounds, 0) + " AS " + sortRangeCol +
		" FROM (SELECT " + sortSource + ".*, " + w.sortKey() + " AS " + sortKeyCol + " FROM (\n" + w.query + "\n) AS " + sortSource + "))" +
		" TO " + quoteLiteral(dir) + " (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (" + sortRangeCol + "), ROW_GROUP_SIZE " +
		strconv.FormatInt(distributed.RowGroupRows(width), 10) + ")"
	if _, err := exec(divide); err != nil {
		return unsorted("divide", err)
	}
	files, err := rangeFiles(ctx, sess, dir)
	if err != nil {
		return unsorted("list", err)
	}

	// 3. Ranges next to each other, sorted and written together, as many
	// as the budget allows now that their files take some of the disk.
	budget := s.sortSpillBudget(ctx, sess)
	if budget <= 0 {
		budget = w.budget
	}
	batches := sortBatches(files, width, budget)
	for _, batch := range batches {
		list := make([]string, len(batch))
		for i, f := range batch {
			list[i] = quoteLiteral(f)
		}
		n, err := exec(w.insert + "\nSELECT * FROM read_parquet([" + strings.Join(list, ", ") + "], hive_partitioning = false) ORDER BY " + w.orderBy())
		if err != nil {
			return rows, true, err
		}
		rows += n
	}
	s.metrics.sortedWrite()
	s.log.Info("wrote rows in order a range at a time", "rows", rows, "ranges", len(bounds)+1, "batches", len(batches),
		"estimate_bytes", w.estimate, "budget_bytes", budget)
	return rows, true, nil
}

// sortSample returns the sort keys of a sample of n of w's rows, as hex in
// order, and how wide the rows are as text, on average, which overstates
// what a sort spills of them.
func (s *Server) sortSample(ctx context.Context, sess *session, w *sortedWrite, n int) (keys []string, width int64, err error) {
	q := "SELECT hex(" + w.sortKey() + "), strlen(CAST(" + sortSource + " AS VARCHAR)) FROM (\n" + w.query + "\n) AS " + sortSource +
		" USING SAMPLE reservoir(" + strconv.Itoa(n) + " ROWS) REPEATABLE (1)"
	rows, err := sess.conn.QueryContext(ctx, q, nil)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close() //nolint:errcheck
	var total int64
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, 0, err
			}
			break
		}
		k, _ := dest[0].(string)
		keys = append(keys, k)
		if l, ok := dest[1].(int64); ok {
			total += l
		}
	}
	if len(keys) == 0 {
		return nil, 0, nil
	}
	// Hex compares as the bytes it spells do.
	slices.Sort(keys)
	return keys, max(total/int64(len(keys)), 1), nil
}

// hexKey is a sort key as DuckDB's hex() spells it.
var hexKey = regexp.MustCompile(`^[0-9A-F]*$`)

// rangeBounds picks from keys, a sample's sort keys in order, the bounds
// of up to n ranges with about as many of them each: the first key of
// every range but the first.
func rangeBounds(keys []string, n int) []string {
	var bounds []string
	for i := 1; i < n; i++ {
		k := keys[i*len(keys)/n]
		if k == keys[0] || len(bounds) > 0 && bounds[len(bounds)-1] == k || !hexKey.MatchString(k) {
			continue
		}
		bounds = append(bounds, k)
	}
	return bounds
}

// rangeOf is the expression of the range, from lo, of a row whose sort key
// col holds, between bounds: a search, so a row takes a comparison for
// every halving of the bounds.
func rangeOf(col string, bounds []string, lo int) string {
	if len(bounds) == 0 {
		return strconv.Itoa(lo)
	}
	mid := len(bounds) / 2
	return "CASE WHEN " + col + " < unhex('" + bounds[mid] + "') THEN " + rangeOf(col, bounds[:mid], lo) +
		" ELSE " + rangeOf(col, bounds[mid+1:], lo+mid+1) + " END"
}

// rangeFile is a file of a range of a sorted write's rows.
type rangeFile struct {
	rng  int
	path string
	rows int64
}

// rangeFiles lists the files a sorted write divided its rows into, under
// dir, with their rows.
func rangeFiles(ctx context.Context, sess *session, dir string) ([]rangeFile, error) {
	rows, err := sess.conn.QueryContext(ctx, "SELECT file_name, num_rows FROM parquet_file_metadata("+
		quoteLiteral(filepath.Join(dir, "*", "*.parquet"))+")", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []rangeFile
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		path, _ := dest[0].(string)
		n, _ := dest[1].(int64)
		rng, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(filepath.Dir(path)), sortRangeCol+"="))
		if err != nil {
			return nil, errors.New("a file outside the ranges' directories: " + path)
		}
		out = append(out, rangeFile{rng: rng, path: path, rows: n})
	}
}

// sortBatches groups files into batches of ranges next to each other, in
// their order, each as large as budget allows for rows of width bytes; a
// range larger than that is a batch of its own.
func sortBatches(files []rangeFile, width, budget int64) [][]string {
	slices.SortFunc(files, func(a, b rangeFile) int {
		if a.rng != b.rng {
			return a.rng - b.rng
		}
		return strings.Compare(a.path, b.path)
	})
	var (
		out  [][]string
		cur  []string
		size int64
	)
	for i := 0; i < len(files); {
		j, rows := i, int64(0)
		for ; j < len(files) && files[j].rng == files[i].rng; j++ {
			rows += files[j].rows
		}
		need := rows * width
		if len(cur) > 0 && size+need > budget {
			out, cur, size = append(out, cur), nil, 0
		}
		for _, f := range files[i:j] {
			cur = append(cur, f.path)
		}
		size += need
		i = j
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}
