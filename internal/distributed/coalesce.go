package distributed

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Partitions per task. Every exchange of a statement has as many
// partitions as its largest table needs (see partitions), so a stage that
// reads small exchanges by partition would run a task for a few thousand
// rows, and read every table it reads in full once per task: a report's
// fourteen stages over an audience of 189,000 rows each ran 32 tasks, and
// read 80 GB of tables to join 6,000 rows a task. When a stage starts, the
// exchanges it reads are complete and their partitions' rows known, so its
// tasks each read several partitions, as many as fit in a task's share of
// a worker's memory at the memory a row takes in the hash tables that join
// or aggregate it. Exchanges hash keys alike, so a task that reads
// partitions p and q of every input still finds every match of its rows.
//
// Compressed bytes would not do: an exchange of 4.4 million person IDs took
// 25 MB, 6 bytes a row, and the hash tables counting them distinct several
// hundred bytes a row; tasks grouped by bytes spilled four times as much,
// and ran out of memory with two at a time. A partition is never split,
// and every worker gets a task.

// rowMemory is the memory a row of columns takes in the operators that
// read it, conservatively: twice its width, for its columns in a hash
// table's row layout and the copies that build and probe it, and 64 bytes
// for the hash, the pointers to it and the table's slack. Aggregates over
// DISTINCT values keep a second table of their groups and values.
func rowMemory(columns []Column) int64 {
	var width int64
	for _, c := range columns {
		width += typeWidth(c.Type)
	}
	return 2*width + 64
}

// typeWidth guesses the bytes a value of a DuckDB type takes in memory, in
// a row: more than typeBytes's guess at its compressed size, for strings
// and nested values above all. A list or map counts as listLength of its
// elements, and a struct as its fields together: a report's relation of
// structs of lists of strings, which FLATTEN builds, wrote rows of
// kilobytes, not of a nested value's 64 bytes.
func typeWidth(t string) int64 {
	t = strings.ToUpper(strings.TrimSpace(t))
	switch {
	case strings.HasSuffix(t, "[]"):
		return 16 + listLength*typeWidth(t[:len(t)-2])
	case strings.HasSuffix(t, "]"):
		// A fixed-size array, T[n].
		if open := strings.LastIndexByte(t, '['); open > 0 {
			if n, err := strconv.Atoi(t[open+1 : len(t)-1]); err == nil {
				return int64(n) * typeWidth(t[:open])
			}
		}
		return 16 + listLength*32
	case strings.HasPrefix(t, "MAP("), strings.HasPrefix(t, "STRUCT("), strings.HasPrefix(t, "UNION("):
		var fields int64
		var most int64
		for _, f := range splitTopLevel(t[strings.IndexByte(t, '(')+1 : len(t)-1]) {
			w := typeWidth(f)
			if !strings.HasPrefix(t, "MAP(") {
				w = typeWidth(fieldType(f))
			}
			fields += w
			most = max(most, w)
		}
		switch {
		case strings.HasPrefix(t, "MAP("):
			return 16 + listLength*fields
		case strings.HasPrefix(t, "UNION("):
			return 8 + most
		}
		return fields
	case strings.HasPrefix(t, "DECIMAL"):
		if open, close := strings.IndexByte(t, '('), strings.IndexByte(t, ','); open >= 0 && close > open {
			if p, err := strconv.Atoi(strings.TrimSpace(t[open+1 : close])); err == nil && p <= 18 {
				return 8
			}
		}
		return 16
	case strings.HasPrefix(t, "TIMESTAMP"), strings.HasPrefix(t, "TIME"):
		return 8
	}
	switch t {
	case "BOOLEAN", "TINYINT", "UTINYINT":
		return 1
	case "SMALLINT", "USMALLINT":
		return 2
	case "INTEGER", "UINTEGER", "FLOAT", "DATE":
		return 4
	case "BIGINT", "UBIGINT", "DOUBLE":
		return 8
	case "HUGEINT", "UHUGEINT", "UUID", "INTERVAL":
		return 16
	}
	// Strings, blobs and the rest: a short string inline, a long one out of
	// line.
	return 32
}

// listLength is how many elements typeWidth counts a list or map as.
const listLength = 8

// splitTopLevel splits a nested type's parameters at the commas outside
// parentheses and quotes.
func splitTopLevel(s string) []string {
	var out []string
	depth, start, quoted := 0, 0, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case quoted:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// fieldType is the type of a struct's or union's field, "name TYPE", the
// name perhaps quoted.
func fieldType(f string) string {
	f = strings.TrimSpace(f)
	if strings.HasPrefix(f, `"`) {
		if end := strings.Index(f[1:], `"`); end >= 0 {
			return f[end+2:]
		}
	}
	if sp := strings.IndexByte(f, ' '); sp >= 0 {
		return f[sp+1:]
	}
	return f
}

// taskMemory is the memory a task may plan on: half of the smallest share
// of a worker's memory limit that a task gets, its limit over the tasks it
// runs at once, or 0 if a worker does not say.
func taskMemory(ws []*worker) int64 {
	var share int64
	for _, w := range ws {
		if w.status.MemoryBytes <= 0 {
			return 0
		}
		slots := w.status.Slots
		if slots <= 0 {
			slots = taskSlots(w.status.Cores, w.status.MemoryBytes)
		}
		if m := w.status.MemoryBytes / int64(slots); share == 0 || m < share {
			share = m
		}
	}
	return share / 2
}

// exchangePartitions is how many partitions the exchanges of stages get on
// n workers, ws those known: enough for the workers and the largest table
// divided (see partitions), more if the exchanges' rows need them to fit a
// task's memory (partitionsFor), at a hundred times the data, but no more
// than their writers have memory for with wide rows (writerPartitions),
// and still one a worker.
func exchangePartitions(stages []*Stage, ws []*worker, n int, upload int64) int {
	var split int64
	for _, s := range stages {
		if in := s.split(); in != nil {
			split = max(split, in.Table.Bytes)
		}
	}
	parts := max(partitions(n, split, partitionBytes), min(partitionsFor(stages, taskMemory(ws), n), maxPartitions))
	if most := writerPartitions(stages, ws, upload, 1); most > 0 {
		parts = min(parts, max(most, n))
	}
	return parts
}

// partitionsFor returns how many partitions the exchanges of stages need
// for a task that reads one partition of each to hold its rows within
// budget, as far as estimates tell: an exchange's rows are those of the
// tables its stages read (a hundredth where a filter compares a column to
// a constant) and of the exchanges they read, each taking the memory of a
// row of the columns the stages write (rowMemory; see exchangeEstimates).
// Partitions that turn out small are read several to a task (see groups);
// a partition that turns out large is not divided, so its task spills.
// But no exchange gets more partitions than give each of its writer tasks,
// about one a worker of the workers, files of minPartitionRows. Without a
// budget it is 0.
func partitionsFor(stages []*Stage, budget int64, workers int) int {
	if budget <= 0 {
		return 0
	}
	rows, width := exchangeEstimates(stages)
	need := 0
	for x, r := range rows {
		mem := r * float64(2*width[x]+64)
		n := int(math.Ceil(mem / float64(budget)))
		n = min(n, max(1, int(r/float64(max(workers, 1)*minPartitionRows))))
		need = max(need, n)
	}
	return need
}

// minPartitionRows is the fewest rows, as estimated, that a partition of an
// exchange gets from each task that writes it. Each writer task writes a
// file a partition, in row groups that shrink as partitions grow (see
// shuffleRowGroup). At ten times the data, a report whose exchanges got 256
// partitions where 32 had done wrote files of 8,000 to 80,000 rows where
// they had been of a million, and its writers took about four times as
// long for the same rows (24.5 s against 5.1 s for 2 million rows), the
// report 362 s against 153. Estimates of rows run high, through joins
// above all, so the floor is set where they gave that report its 32.
const minPartitionRows = 1 << 20

// exchangeEstimates estimates each exchange's rows, as partitionsFor says,
// and the width of its rows by the types of the columns its stages write
// (see outputWidths), or of every column they read where what a stage
// writes cannot be told.
func exchangeEstimates(stages []*Stage) (rows map[*Exchange]float64, width map[*Exchange]int64) {
	rows, width = map[*Exchange]float64{}, map[*Exchange]int64{}
	// The width of each column an exchange holds, by name; nil if not
	// known.
	columns := map[*Exchange]map[string]int64{}
	for _, s := range stages {
		var in float64
		var w int64
		var views []stageView
		for _, i := range s.Inputs {
			v := stageView{name: strings.ToLower(i.View)}
			switch {
			case i.Table != nil:
				in += rowsRead(s.fragment, i.Table)
				cols := columnsRead(s.fragment, i.Table)
				v.columns = map[string]int64{}
				for _, c := range i.Table.Columns {
					if cols == nil || cols[strings.ToLower(c.Name)] {
						w += typeWidth(c.Type)
						v.columns[strings.ToLower(c.Name)] = typeWidth(c.Type)
						v.order = append(v.order, strings.ToLower(c.Name))
					}
				}
			case i.Exchange != nil:
				in += rows[i.Exchange]
				w += width[i.Exchange]
				v.columns = columns[i.Exchange]
				for name := range v.columns {
					v.order = append(v.order, name)
				}
				slices.Sort(v.order)
			}
			views = append(views, v)
		}
		if s.Out == nil {
			continue
		}
		rows[s.Out] += in
		widths, names := outputWidths(s, views)
		if widths == nil {
			width[s.Out] = max(width[s.Out], w)
			continue
		}
		var out int64
		cols := columns[s.Out]
		if cols == nil {
			cols = map[string]int64{}
			columns[s.Out] = cols
		}
		for i, cw := range widths {
			out += cw
			if names[i] != "" {
				cols[names[i]] = max(cols[names[i]], cw)
			}
		}
		width[s.Out] = max(width[s.Out], out)
	}
	return rows, width
}

// exprWidth guesses the width of an expression's value from the widths of
// the columns it names (lookup): a comparison or other test is a byte, a
// count or sum a number, a cast its type's width; concatenation and the
// like are as wide as their arguments together, an aggregate into a list
// listLength times that, and anything else as its widest argument.
func exprWidth(e node, lookup func([]string) int64) int64 {
	widest := func(args ...any) int64 {
		var w int64
		for _, a := range args {
			if n, ok := a.(node); ok && isExpr(n) {
				w = max(w, exprWidth(n, lookup))
			}
		}
		return w
	}
	together := func(args []any) int64 {
		var w int64
		for _, a := range args {
			if n, ok := a.(node); ok && isExpr(n) {
				w += exprWidth(n, lookup)
			}
		}
		return w
	}
	switch str(e["class"]) {
	case "COLUMN_REF":
		return lookup(columnNames(e))
	case "CONSTANT":
		return 8
	case "COMPARISON", "CONJUNCTION", "BETWEEN":
		return 1
	case "OPERATOR":
		switch str(e["type"]) {
		case "OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "COMPARE_IN", "COMPARE_NOT_IN":
			return 1
		}
		return max(widest(list(e, "children")...), 1)
	case "CAST":
		if id := str(child(e, "cast_type")["id"]); id != "" {
			return typeWidth(id)
		}
		return widest(e["child"])
	case "CASE":
		var w int64
		for _, c := range list(e, "case_checks") {
			w = max(w, widest(child(c.(node), "then_expr")))
		}
		return max(w, widest(e["else_expr"]), 1)
	case "FUNCTION", "WINDOW", "AGGREGATE":
		name := strings.ToLower(str(e["function_name"]))
		args := list(e, "children")
		switch {
		case numberFunctions[name]:
			return 16
		case listAggregates[name]:
			return 16 + listLength*max(together(args), 8)
		case joinFunctions[name]:
			return max(together(args), 8)
		}
		return max(widest(args...), 8)
	case "STAR":
		return defaultWidth * 8
	case "SUBQUERY":
		return defaultWidth
	}
	return defaultWidth
}

// stageView is an input of a stage, for outputWidths: its view's name and
// the width of each column of it the stage reads, by name in lower case,
// in order; columns is nil if they are not known.
type stageView struct {
	name    string
	columns map[string]int64
	order   []string
}

// defaultWidth is the width outputWidths takes a value it cannot tell the
// type of to have: a string's (see typeWidth), as the fields of the structs
// that FLATTEN builds mostly are.
const defaultWidth = 32

// Functions whose value is as wide as their arguments' together, holds
// many of them, or is a number or a count whatever they are.
var (
	joinFunctions  = map[string]bool{"concat": true, "concat_ws": true, "||": true, "struct_pack": true, "row": true, "list_value": true}
	listAggregates = map[string]bool{"list": true, "array_agg": true, "string_agg": true, "group_concat": true,
		"listagg": true, "histogram": true, "list_distinct": true, "array_distinct": true}
	numberFunctions = map[string]bool{"count": true, "count_star": true, "approx_count_distinct": true, "row_number": true,
		"rank": true, "dense_rank": true, "ntile": true, "sum": true, "avg": true, "mean": true, "stddev": true,
		"stddev_samp": true, "stddev_pop": true, "variance": true, "var_samp": true, "var_pop": true, "length": true,
		"hash": true, "epoch": true, "year": true, "month": true, "day": true, "date_part": true, "datediff": true, "date_diff": true}
)

// outputWidths returns the widths of the columns stage s writes, by the
// types of the columns of views, its inputs, that its fragment's SELECT
// list names, and the columns' names, by which later stages read them; nil
// if the fragment has no SELECT list to tell them by (see exprWidth). An
// exchange took the width of every column its rows were made of together
// (2,744 bytes a row, at a report's ten times the data), where the columns
// it holds took a fraction, and its partitions followed: 256, where 32
// sufficed.
func outputWidths(s *Stage, views []stageView) ([]int64, []string) {
	q := s.fragment
	for str(q["type"]) == "SET_OPERATION_NODE" {
		q = child(q, "left")
	}
	items := list(q, "select_list")
	if str(q["type"]) != "SELECT_NODE" || len(items) == 0 {
		return nil, nil
	}
	// The view each alias of the fragment's FROM clause means.
	alias := map[string]string{}
	walk(child(q, "from_table"), func(n node) bool {
		if n["subquery"] != nil {
			return false
		}
		if str(n["type"]) == "BASE_TABLE" {
			name := strings.ToLower(str(n["table_name"]))
			alias[strings.ToLower(cmp.Or(str(n["alias"]), name))] = name
		}
		return true
	})
	lookup := func(parts []string) int64 {
		name := strings.ToLower(parts[len(parts)-1])
		if len(parts) > 1 {
			if v, ok := alias[strings.ToLower(parts[len(parts)-2])]; ok {
				for _, view := range views {
					if w, ok := view.columns[name]; ok && view.name == v {
						return w
					}
				}
			}
		}
		for _, view := range views {
			if w, ok := view.columns[name]; ok {
				return w
			}
		}
		return defaultWidth
	}
	var widths []int64
	var names []string
	for _, item := range items {
		e, _ := item.(node)
		switch str(e["class"]) {
		case "STAR":
			rel := strings.ToLower(str(e["relation_name"]))
			for _, view := range views {
				if rel != "" && alias[rel] != view.name {
					continue
				}
				if view.columns == nil {
					return nil, nil
				}
				for _, name := range view.order {
					widths, names = append(widths, view.columns[name]), append(names, name)
				}
			}
		case "COLUMN_REF":
			parts := columnNames(e)
			widths = append(widths, lookup(parts))
			names = append(names, strings.ToLower(cmp.Or(str(e["alias"]), parts[len(parts)-1])))
		default:
			widths, names = append(widths, exprWidth(e, lookup)), append(names, strings.ToLower(str(e["alias"])))
		}
	}
	if len(s.Columns) == len(widths) {
		for i, c := range s.Columns {
			names[i] = strings.ToLower(c)
		}
	}
	return widths, names
}

// patternSelectivity is the share of a table's rows that matching one of
// its columns against a constant pattern, with LIKE, ILIKE or GLOB, is
// taken to keep, as DuckDB takes a filter it cannot estimate. At ten
// times the data, a report's 8.3 billion segment rows, of which LIKE
// 'SEG_HH_%' kept 400 million, were estimated whole, and exchanges
// partitioned to fit them got 243 partitions where 128 had done; their
// writers took a third longer.
const patternSelectivity = 0.2

// rowsRead estimates the rows fragment reads of table t: a hundredth where
// it compares a column of t to a constant, a fifth where it matches one
// against a constant pattern.
func rowsRead(fragment node, t *Table) float64 {
	r := float64(max(t.Rows, 0))
	switch {
	case filtersByConstant(fragment, t):
		r *= filterSelectivity
	case matchesPattern(fragment, t):
		r *= patternSelectivity
	}
	return r
}

// matchesPattern reports whether fragment matches a column of t against a
// constant pattern in a WHERE clause.
func matchesPattern(fragment node, t *Table) bool {
	cols := map[string]bool{}
	for _, c := range t.Columns {
		cols[strings.ToLower(c.Name)] = true
	}
	found := false
	walk(fragment, func(m node) bool {
		if found {
			return false
		}
		for _, c := range conjuncts(child(m, "where_clause")) {
			switch strings.ToLower(str(c["function_name"])) {
			case "~~", "~~*", "~~~", "prefix", "starts_with":
			default:
				continue
			}
			kids := list(c, "children")
			if len(kids) != 2 {
				continue
			}
			col, _ := kids[0].(node)
			pat, _ := kids[1].(node)
			names := list(col, "column_names")
			if str(col["class"]) == "COLUMN_REF" && str(pat["class"]) == "CONSTANT" && len(names) > 0 &&
				cols[strings.ToLower(str(names[len(names)-1]))] {
				found = true
			}
		}
		return !found
	})
	return found
}

// writerPartitions is the most partitions the stages' tasks can write an
// exchange of: each holds a row group of each partition (see
// shuffleRowGroup), of at least minShuffleRowGroup rows, which those of
// all partitions must fit a quarter of a task's share of the worker's
// memory in, at the width of the widest exchange's rows. Rows of 400
// strings took a writer of 3 GB out of memory over 128 partitions and 256,
// even in row groups of 2,048 rows. A file in object storage takes upload
// bytes more, its upload's buffer (see uploadBuffer). A task that takes
// slots of a worker's slots has as many shares (see taskRequest.Slots). It
// is 0 when the workers' memory is not known or no stage writes an
// exchange.
func writerPartitions(stages []*Stage, ws []*worker, upload int64, slots int) int {
	budget := taskMemory(ws) * int64(max(slots, 1)) / 2
	_, width := exchangeEstimates(stages)
	var widest int64
	for _, w := range width {
		widest = max(widest, w)
	}
	if budget <= 0 || widest <= 0 {
		return 0
	}
	return int(budget / (minShuffleRowGroup*widest + upload))
}

// uploadBytes is what a shuffle task's file takes beside its rows while
// it is written under the warehouse: an upload's buffer in object storage.
func uploadBytes(warehouse string) int64 {
	if uploadsInParts(warehouse) {
		return uploadBuffer
	}
	return 0
}

// smallCompletion is the most memory, as rowMemory reckons it, the partial
// results of a stage that completes an aggregation's groups may take for
// one task to complete them all: a statement grouped by a column of few
// values, which partitions complete as they would many, waits for one
// task rather than a round of them.
const smallCompletion = 64 << 20

// groups returns the partitions each task of stage s, which reads
// exchanges by partition, reads.
func (e *execution) groups(s *Stage) [][]int {
	mem := make([]int64, e.parts)
	var total int64
	for _, in := range s.Inputs {
		if in.Exchange == nil || !in.Partitioned {
			continue
		}
		x := e.exchanges[in.Exchange.ID]
		x.mu.Lock()
		per := rowMemory(x.columns)
		for p, rows := range x.partRows {
			mem[p] += rows * per
			total += rows * per
		}
		x.mu.Unlock()
	}
	if s.completes && total <= smallCompletion && e.smaller == 0 {
		all := make([]int, e.parts)
		for p := range all {
			all[p] = p
		}
		return [][]int{all}
	}
	slots := max(e.slots, 1)
	return groupPartitions(mem, taskMemory(e.ws)*int64(slots)>>e.smaller, max(totalSlots(e.ws)/slots, 1))
}

// totalSlots returns how many tasks the workers ws run at once together, and
// at least one each. A stage of grouped partitions got a task a worker, and
// workers that run two at once ran one: at ten times the data, a report's
// stages of 128 partitions in 22 tasks, each of six partitions, took 32 to
// 34 seconds where 128 tasks had taken 19 to 21, beside other stages'
// tasks in the workers' slots.
func totalSlots(ws []*worker) int {
	n := 0
	for _, w := range ws {
		s := w.status.Slots
		if s <= 0 {
			s = taskSlots(w.status.Cores, w.status.MemoryBytes)
		}
		n += max(s, 1)
	}
	return n
}

// divide splits each of shares, a worker's share of a divided table's
// files each, into up to k shares of about equal size, for smaller tasks,
// and returns them with the index of the worker each came from.
func divide(shares [][]File, k int) ([][]File, []int) {
	var out [][]File
	var owners []int
	for w, share := range shares {
		if k <= 1 || len(share) <= 1 {
			out, owners = append(out, share), append(owners, w)
			continue
		}
		pieces := make([][]File, min(k, len(share)))
		sizes := make([]int64, len(pieces))
		sorted := slices.Clone(share)
		slices.SortStableFunc(sorted, func(a, b File) int { return cmp.Compare(b.Size, a.Size) })
		for _, f := range sorted {
			j := slices.Index(sizes, slices.Min(sizes))
			pieces[j] = append(pieces[j], f)
			sizes[j] += max(f.Size, 1)
		}
		for _, p := range pieces {
			out, owners = append(out, p), append(owners, w)
		}
	}
	return out, owners
}

// groupPartitions groups partitions, of the memory mem says each takes,
// into at least tasks groups (or one each, if there are fewer), as few as
// keep each group within budget but for a partition larger than that
// alone, balanced by memory. Without a budget each partition is a group.
func groupPartitions(mem []int64, budget int64, tasks int) [][]int {
	if budget <= 0 || len(mem) <= max(tasks, 1) {
		out := make([][]int, len(mem))
		for p := range mem {
			out[p] = []int{p}
		}
		return out
	}
	n := min(len(mem), max(tasks, 1))
	groups := make([][]int, n)
	load := make([]int64, n)
	order := make([]int, len(mem))
	for p := range order {
		order[p] = p
	}
	// The largest first, each to the group that holds least, or to a group
	// of its own when that would take the group over budget.
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(mem[b], mem[a]) })
	for _, p := range order {
		g := 0
		for i := range groups {
			if load[i] < load[g] || load[i] == load[g] && len(groups[i]) < len(groups[g]) {
				g = i
			}
		}
		if len(groups[g]) > 0 && load[g]+mem[p] > budget {
			groups = append(groups, nil)
			load = append(load, 0)
			g = len(groups) - 1
		}
		groups[g] = append(groups[g], p)
		load[g] += mem[p]
	}
	for _, g := range groups {
		slices.Sort(g)
	}
	slices.SortFunc(groups, func(a, b []int) int { return cmp.Compare(a[0], b[0]) })
	return groups
}
