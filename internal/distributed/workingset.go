package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// A statement's working set on one machine, from DuckDB's own plan of it:
// the rows DuckDB estimates each operator that holds rows will hold, at
// the widths of their columns. Joins hold their build sides in hash
// tables, groupings their groups (no more than the distinct counts ANALYZE
// or Iceberg's statistics give their keys allow), distinct aggregates the
// pairs of groups and values, and sorts and windows their inputs. Auto
// mode estimated the working set at six times what the stages feeding
// joins and distinct counts read, compressed, which ran up to fifteen
// times off either way against the scale runs; DuckDB's plan knows which
// side a join builds on, what a filter leaves and how many groups a key
// makes. Where the session cannot say, the old estimate stands.
//
// DuckDB's estimates are corrected where the scale runs found them far
// off (see walk and joinRows). The branches of a UNION run one after
// another where memory is short, and hold no more than the largest of
// them: a report of 16 branches, each joining 400 million rows to 400
// million, was estimated at 793 GB by their sum, and held 33 GB, 14 GB of
// it in hash tables. DuckDB takes a join of rows to keys it knows nothing
// of to keep a fifth of them; joined to an audience of everyone, each of
// 157 million events kept its row. A join to one row, as of a scalar
// subquery's latest version, keeps as a filter by a constant does, where
// DuckDB keeps every row. A cross product with a small source of its own,
// as FLATTEN over a list of six objects becomes, makes a row for each
// pair, where DuckDB counts one. Those took a report's working set for 25
// GB where it held 51 GB in hash tables and spilled 55 GB, and another's
// for 439 GB where it held 9.5 GB.
//
// The plan also says what one machine reads (each table's columns that
// its scans project, once: the read cache serves them again) and the
// rows its hash tables take in, at hashMissRate a core where a table is
// larger than the caches (see planEstimate).

// Explainer is an Env whose session plans a query as DuckDB would run it
// on this replica: Explain returns DuckDB's EXPLAIN (FORMAT JSON) of query,
// its parameters bound to NULL.
type Explainer interface {
	Explain(ctx context.Context, query string) (string, error)
}

const (
	// hashEntryBytes is what a row of an aggregate's hash table or a sort
	// takes beside its columns: its hash, or its pointer; joinEntryBytes
	// what a row of a join's hash table takes: its hash, the pointer to the
	// next row of its chain, and its slot in the directory, twice as many
	// slots as rows.
	hashEntryBytes = 8
	joinEntryBytes = 32
	// maxBuildWidth bounds the columns a join's build side is taken to
	// keep, of the many its input has.
	maxBuildWidth = 48
	// planExprWidth is the width of a column the plan names by an
	// expression rather than a table's column.
	planExprWidth = 8
	// filterShare is the share of a scan's rows a filter keeps, where
	// DuckDB gives the scan no estimate of its own.
	filterShare = 0.2
	// hashMissRate is how many rows a core puts into or looks up in a hash
	// table larger than cacheBytes, a memory access each, where one that
	// fits the caches takes rowRate: 16 joins of 400 million rows to 400
	// million and 17 to 18 million took 2,166 core-seconds on one machine,
	// about 1e7 rows a core-second.
	hashMissRate = 1e7
	cacheBytes   = 64 << 20
	// smallSource is the most rows a side of a cross product without
	// tables of its own may have for the product to count every pair.
	smallSource = 1024
)

// heldFactors are how many times the rows and widths an operator holds by
// the plan DuckDB holds at its peak, for its layout: a hash join's
// directory of pointers, sized to a power of two, and its partitions; an
// aggregate's partitions; a window's sort beside its rows. Fitted against
// what DuckDB was seen holding (duckdb_memory(), as the server samples it
// for the query history's peaks) over joins of 4 million rows,
// groupings of 4 to 16 million groups, distinct counts, sorts and windows
// of 16 million rows (see TestWorkingSetCalibration).
// A window over the whole input without an order (OVER ()) buffers its
// rows once.
var heldFactors = map[string]float64{"join": 1.5, "group": 1.5, "distinct": 1.3, "sort": 1.2, "window": 2.0, "constant window": 1.0}

// planNode is an operator of DuckDB's EXPLAIN (FORMAT JSON).
type planNode struct {
	Name     string                     `json:"name"`
	Children []*planNode                `json:"children"`
	Extra    map[string]json.RawMessage `json:"extra_info"`
}

// planStats are what the statement's tables say of their columns, by
// lower-case name: their widths in memory, and their distinct counts where
// known; the tables' rows, for scans DuckDB gives no estimate; and the
// machine's threads, each of which aggregates its share of the rows apart
// before they are merged.
type planStats struct {
	width    map[string]float64
	distinct map[string]float64
	tables   []*Table
	threads  int
}

// newPlanStats collects the columns of a plan's tables, for a machine of
// threads DuckDB threads.
func newPlanStats(stages []*Stage, threads int) planStats {
	st := planStats{width: map[string]float64{}, distinct: map[string]float64{}, threads: max(threads, 1)}
	seen := map[*Table]bool{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table == nil {
				continue
			}
			if !seen[in.Table] {
				seen[in.Table] = true
				st.tables = append(st.tables, in.Table)
			}
			for _, c := range in.Table.Columns {
				name := strings.ToLower(c.Name)
				st.width[name] = max(st.width[name], memoryBytes(c.Type))
				if n := in.Table.Distinct[name]; n > 0 {
					st.distinct[name] = max(st.distinct[name], float64(n))
				}
			}
		}
	}
	return st
}

// memoryBytes guesses how many bytes a value of type typ takes in a hash
// table or sort: strings their 16 bytes and a short heap, nested values
// several of those.
func memoryBytes(typ string) float64 {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch {
	case strings.HasSuffix(t, "]"), strings.HasPrefix(t, "MAP"), strings.HasPrefix(t, "STRUCT"), strings.HasPrefix(t, "UNION"):
		return 64
	case strings.HasPrefix(t, "VARCHAR"), t == "BLOB":
		return 32
	}
	return max(typeBytes(t), 4)
}

// memoryScale corrects the working sets DuckDB's plans give by what
// statements were seen holding at their peaks on the replica, which its
// sampling of DuckDB's memory records (the query history's memory_peak):
// the logarithm of peak over estimate, averaged with a weight of a tenth
// for each new statement, within a factor of four, and only for
// statements of at least scaleFloor both ways, as smaller ones hold
// mostly DuckDB's buffers.
type memoryScale struct {
	mu    sync.Mutex
	log   float64
	count int
}

const (
	scaleFloor = 256 << 20
	scaleBound = 4
	scaleAfter = 3
)

// observe takes in a statement estimated to hold set bytes that held peak.
func (m *memoryScale) observe(set, peak float64) {
	if set < scaleFloor || peak < scaleFloor {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = 0.9*m.log + 0.1*math.Log(peak/set)
	m.log = max(min(m.log, math.Log(scaleBound)), -math.Log(scaleBound))
	m.count++
}

// scale is the correction, 1 until scaleAfter statements were taken in.
func (m *memoryScale) scale() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count < scaleAfter {
		return 1
	}
	return math.Exp(m.log)
}

// ObservePeak records that a statement whose plan key is key held peak
// bytes at its peak on this replica, where it ran to its end, as its
// replica's sampling of DuckDB's memory saw it: DuckDB's plans' working
// sets are corrected by such peaks against what they estimated (see
// memoryScale). A peak at the memory limit's share may be one spilled,
// and says less than it held.
func (co *Coordinator) ObservePeak(key string, peak int64) {
	if key == "" || peak <= 0 || co.opts.MemoryBytes > 0 && float64(peak) >= memoryShare*float64(co.opts.MemoryBytes) {
		return
	}
	if r, ok := co.history.get(key); ok && r.planSet > 0 {
		co.memory.observe(r.planSet, float64(peak))
	}
}

// heldPart is what one operator of a plan holds, for EXPLAIN DISTRIBUTED.
type heldPart struct {
	op          string
	rows, bytes float64
}

// workingSetOf estimates the bytes a statement whose DuckDB plan is plan
// (EXPLAIN (FORMAT JSON)) holds at its peak, by the plan's estimated rows
// and st's widths and distinct counts, and lists what each operator that
// holds rows holds. It fails for a plan with a scan whose rows neither
// DuckDB nor st knows.
func workingSetOf(plan string, st planStats) (float64, []heldPart, error) {
	p, err := planOf(plan, st)
	return p.set, p.parts, err
}

// planEstimate is what one machine does with a statement, by DuckDB's plan
// of it: set is its working set and parts what each operator holds; bytes
// what it reads of tables cold, each table's columns its scans project
// once, a hundredth of them for a table every scan of which is filtered
// by a constant (zero where no scan's table is known); work the
// core-seconds its hash tables take, joins' and aggregates'.
type planEstimate struct {
	set   float64
	parts []heldPart
	bytes int64
	work  float64
	// keeps is the share of the rows its scans read that its result has,
	// up to maxKeeps: about 1 for a copy, next to nothing for an
	// aggregate's few groups.
	keeps float64
	// fields are its result's rows times its columns, which a read sends
	// the client; sorted is what its largest sort or window holds.
	fields, sorted float64
}

// maxKeeps is the most rows a result is taken to have for each row its
// scans read, as joins multiply them; aggregateKeeps the most an
// aggregate's is. DuckDB takes an aggregate's groups for all its input
// rows, without distinct counts of its keys: a count(DISTINCT) of a
// billion IDs by day and group kept 1,500 rows.
const (
	maxKeeps       = 4
	aggregateKeeps = 0.01
)

// topAggregate says a plan's result is an aggregate's, through the
// operators that order, project, filter or limit its rows.
func topAggregate(n *planNode) bool {
	for n != nil {
		name := strings.ToUpper(n.Name)
		switch {
		case strings.Contains(name, "GROUP_BY"), strings.Contains(name, "AGGREGATE"):
			return true
		case len(n.Children) != 1:
			return false
		case name == "ORDER_BY", name == "PROJECTION", name == "FILTER", name == "TOP_N", strings.Contains(name, "LIMIT"):
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

// planOf estimates a statement by its DuckDB plan (see planEstimate).
func planOf(plan string, st planStats) (planEstimate, error) {
	var roots []*planNode
	if err := json.Unmarshal([]byte(plan), &roots); err != nil {
		return planEstimate{}, err
	}
	if len(roots) == 0 {
		return planEstimate{}, errors.New("an empty plan")
	}
	w := &setWalker{st: st, ctes: map[string]cte{}}
	var result float64
	var columns int
	aggregated := false
	for _, r := range roots {
		rows, cols := w.walk(r)
		result = rows.ours
		if result <= 0 {
			result = rows.duck
		}
		columns = len(cols)
		aggregated = topAggregate(r)
	}
	if w.unknown != "" {
		return planEstimate{}, errors.New("no estimate of the rows of " + w.unknown)
	}
	out := planEstimate{parts: w.parts, work: w.work, bytes: w.bytes(), fields: result * float64(max(columns, 1))}
	for _, p := range w.parts {
		if op := strings.ToUpper(p.op); strings.Contains(op, "ORDER_BY") || strings.Contains(op, "WINDOW") {
			out.sorted = max(out.sorted, p.bytes)
		}
	}
	if w.scanned > 0 {
		out.keeps = min(result/w.scanned, maxKeeps)
		if aggregated {
			out.keeps = min(out.keeps, aggregateKeeps)
		}
	}
	for _, p := range w.parts {
		out.set += p.bytes
	}
	return out, nil
}

type setWalker struct {
	st    planStats
	parts []heldPart
	// unknown names a scan whose rows nothing estimates.
	unknown string
	// ctes are the rows of the plan's common table expressions, by index,
	// for their scans; scans what each scan of a table reads; work the
	// core-seconds hash tables take (see planEstimate).
	ctes  map[string]cte
	scans []scanRead
	work  float64
	// scanned is the rows of the tables its scans read.
	scanned float64
}

// cte is a common table expression's rows and columns, for its scans.
type cte struct {
	rows rowsEstimate
	cols []column
}

// scanRead is what a scan reads of a table: the columns it projects, and
// whether it is filtered by a constant, or by one row's value.
type scanRead struct {
	t         *Table
	cols      columnSet
	selective bool
}

// bytes is what the scans read cold: each table's columns once, a
// hundredth of them where every scan of the table is filtered so.
func (w *setWalker) bytes() int64 {
	cols, all := map[*Table]columnSet{}, map[*Table]bool{}
	var order []*Table
	for _, s := range w.scans {
		if _, seen := cols[s.t]; !seen {
			order, all[s.t] = append(order, s.t), true
			cols[s.t] = columnSet{}
		}
		for c := range s.cols {
			cols[s.t][c] = true
		}
		all[s.t] = all[s.t] && s.selective
	}
	var total int64
	for _, t := range order {
		b := cols[t].bytes(t)
		if all[t] {
			b = int64(float64(b) * filterSelectivity)
		}
		total += b
	}
	return total
}

// hashRate is how many rows a core puts into or looks up in a hash table
// of bytes: rowRate where it fits the caches, hashMissRate otherwise.
func hashRate(bytes float64) float64 {
	if bytes > cacheBytes {
		return hashMissRate
	}
	return rowRate
}

// column is a column an operator outputs: its name if it is one, its
// width, and its distinct count, zero for unknown.
type column struct {
	name            string
	width, distinct float64
}

// estimate is an operator's rows: as DuckDB estimates them, and as the
// distinct counts of its join keys correct that (see walk); kept is the
// share of the rows of its tables its filters keep, 1 where none do or
// it is not known.
type rowsEstimate struct{ duck, ours, kept float64 }

// scaled is DuckDB's estimate of rows d corrected as an operator's input,
// in, was: by the ratio of ours to DuckDB's.
func (in rowsEstimate) scaled(d float64) float64 {
	if in.duck <= 0 {
		return d
	}
	return d * in.ours / in.duck
}

// walk adds what n and its children hold, and returns n's rows and its
// output columns. DuckDB estimates a join of tables without statistics of
// their keys' distinct values by rules of thumb: a TPC-H join of customers
// and suppliers by nation, many to many, it took for 2,000 rows where it
// made 12 million, and built a hash table of 18 million. Where the
// distinct counts of an inner join's keys are known (ANALYZE, Iceberg's
// statistics), its rows are taken to be the product of its inputs' over
// the larger distinct count of each pair of keys, and the operators above
// it are corrected by as much.
func (w *setWalker) walk(n *planNode) (rowsEstimate, []column) {
	name := strings.ToUpper(n.Name)
	var rows []rowsEstimate
	var outs [][]column
	// Where each child's parts and scans begin, and end, the last.
	parts, scans := []int{len(w.parts)}, []int{len(w.scans)}
	for i, c := range n.Children {
		r, o := w.walk(c)
		rows, outs = append(rows, r), append(outs, o)
		parts, scans = append(parts, len(w.parts)), append(scans, len(w.scans))
		if name == "CTE" && i == 0 {
			w.ctes[strings.Join(n.strs("Table Index"), "")] = cte{r, o}
		}
	}
	if name == "UNION" && len(rows) > 1 {
		w.largestBranch(parts)
	}
	card, ok := n.cardinality()
	if !ok {
		for _, r := range rows {
			card = max(card, r.duck)
		}
	}
	child := func(i int) (rowsEstimate, []column) {
		if i >= 0 && i < len(rows) {
			return rows[i], outs[i]
		}
		return rowsEstimate{}, nil
	}
	in, inCols := child(0)
	est := rowsEstimate{duck: card, ours: in.scaled(card), kept: in.kept}
	if len(rows) == 0 {
		est.ours, est.kept = card, 1
	}
	switch {
	case name == "CTE_SCAN":
		// The common table expression's rows, as corrected.
		if c, found := w.ctes[strings.Join(n.strs("CTE Index"), "")]; found {
			est.ours, est.kept = c.rows.ours, c.rows.kept
			return est, limitDistinct(c.cols, est.ours)
		}
		return est, nil
	case strings.Contains(name, "SCAN"), strings.HasPrefix(name, "READ_"):
		cols := w.columns(n.strs("Projections"), nil)
		if len(n.Children) > 0 || name == "DUMMY_SCAN" || strings.HasPrefix(name, "CTE") || strings.HasPrefix(name, "DELIM") {
			return est, limitDistinct(cols, est.ours)
		}
		// A scan of a table: no more rows than the table has, which DuckDB
		// guesses for a table of many small files as a thousand rows a file
		// (a TPC-H nation of 25 rows in 8 files it took for 8,000), and
		// estimates not at all for an Iceberg scan.
		t := w.tableOf(cols)
		switch {
		case t != nil && t.Rows > 0 && (!ok || card > float64(t.Rows)):
			est.ours = float64(t.Rows)
			if len(n.strs("Filters")) > 0 {
				est.ours *= filterShare
			}
			if !ok {
				est.duck = est.ours
			}
		case !ok:
			w.unknown = name
		}
		switch {
		case t != nil && t.Rows > 0:
			w.scanned += float64(t.Rows)
		case ok:
			w.scanned += card
		}
		if t != nil {
			read := columnSet{}
			for _, c := range cols {
				read[c.name] = true
			}
			w.scans = append(w.scans, scanRead{t: t, cols: read, selective: filtersByValue(n.strs("Filters"))})
			if t.Rows > 0 {
				est.kept = min(1, est.ours/float64(t.Rows))
			}
		}
		return est, limitDistinct(cols, est.ours)
	case name == "PROJECTION" || name == "FILTER" && len(n.strs("Projections")) > 0:
		return est, limitDistinct(w.columns(n.strs("Projections"), inCols), est.ours)
	case strings.Contains(name, "JOIN") || name == "CROSS_PRODUCT":
		// DuckDB builds on the right, the last child, keeping its keys and
		// the columns the operators above read, which the plan does not
		// list: at most maxBuildWidth of them.
		build, buildCols := child(len(rows) - 1)
		entry := min(width(buildCols), maxBuildWidth) + joinEntryBytes
		w.hold(name, "join", build.ours, entry)
		if strings.Contains(name, "HASH") {
			w.work += (in.ours + build.ours) / hashRate(build.ours*entry)
		}
		var all []column
		for _, o := range outs {
			all = append(all, o...)
		}
		est.ours, est.kept = w.joinRows(n, rows, outs, card, scans)
		all = joinedDistinct(n, all)
		return est, limitDistinct(all, est.ours)
	case strings.HasSuffix(name, "GROUP_BY"):
		groups := w.columns(n.strs("Groups"), inCols)
		// Fewer groups where the input has fewer rows than DuckDB took, but
		// not more where it has more: joins and unnesting that multiply rows
		// seldom multiply the keys (a report's groups of what an unnest
		// made, taken for 945 million where its input was, made thousands).
		if in.duck > 0 && len(rows) > 0 {
			est.ours = card * min(1, in.ours/in.duck)
		}
		if ndv := product(groups); ndv > 0 {
			est.ours = min(est.ours, ndv)
		}
		est.ours = min(est.ours, max(in.ours, 1))
		// Each thread aggregates its share of the rows apart, all the
		// groups it meets, before they are merged.
		entries := min(in.ours, est.ours*float64(w.st.threads))
		if len(rows) == 0 {
			entries = est.ours
		}
		aggs := n.strs("Aggregates")
		entry := width(groups) + float64(len(aggs))*planExprWidth + hashEntryBytes
		w.hold(name, "group", entries, entry)
		w.distinct(name, aggs, inCols, in.ours, width(groups))
		w.work += in.ours / hashRate(entries*entry)
		return est, append(groups, make([]column, len(aggs))...)
	case name == "UNGROUPED_AGGREGATE":
		w.distinct(name, n.strs("Aggregates"), inCols, in.ours, 0)
		return rowsEstimate{duck: 1, ours: 1, kept: 1}, make([]column, len(n.strs("Aggregates")))
	case name == "ORDER_BY" || name == "WINDOW":
		est = in
		if name == "WINDOW" {
			kind := "window"
			if constantWindow(n.strs("Projections")) {
				kind = "constant window"
			}
			w.hold(name, kind, in.ours, width(inCols)+hashEntryBytes)
			return est, append(inCols, column{width: planExprWidth})
		}
		w.hold(name, "sort", in.ours, width(inCols)+hashEntryBytes)
		return est, inCols
	}
	return est, inCols
}

// equality matches a join condition of equal columns.
var equality = regexp.MustCompile(`^\s*(\S+)\s*=\s*(\S+)\s*$`)

// joinRows is a join's rows, and the share of its tables' rows its filters
// keep, by its inputs' (kept): for an inner join on equal columns whose
// distinct counts are all known, the product of its inputs' rows over the
// larger distinct count of each pair; otherwise DuckDB's estimate, card,
// corrected as its inputs were, but
//   - for an inner join on equal columns to one row, as of a scalar
//     subquery, the other side's rows a filter by a constant keeps: over
//     the distinct count of its key, or filterSelectivity of them, its
//     scans, from scans, counted as filtered so;
//   - for any other inner join on equal columns, no fewer than the rows of
//     the side with more, as its filters leave them, that the other side's
//     filters keep, each row of it joined to its key's row of the other;
//   - for a left join, no fewer than its left side's rows;
//   - for a cross product, or a join of a small source of its own, every
//     pair of its sides' rows.
func (w *setWalker) joinRows(n *planNode, rows []rowsEstimate, outs [][]column, card float64, scans []int) (float64, float64) {
	scaled, kept := card, 1.0
	for _, r := range rows {
		if r.duck > 0 {
			scaled *= r.ours / r.duck
		}
		kept *= r.kept
	}
	if len(rows) != 2 {
		return scaled, kept
	}
	l, r := rows[0], rows[1]
	conds := n.strs("Conditions")
	typ := strings.ToUpper(strings.Join(n.strs("Join Type"), ""))
	equal := len(conds) > 0
	for _, c := range conds {
		equal = equal && equality.MatchString(c)
	}
	small := func(i int) bool { return rows[i].ours <= smallSource && scans[i] == scans[i+1] }
	name := strings.ToUpper(n.Name)
	loop := strings.Contains(name, "NL_JOIN") || strings.Contains(name, "NESTED_LOOP")
	switch {
	case name == "CROSS_PRODUCT", loop && (len(conds)+len(n.strs("Condition")) == 0 || small(0) || small(1)):
		return max(scaled, l.ours*r.ours), kept
	case equal && (typ == "INNER" || typ == "SEMI") && (l.ours <= 1 || r.ours <= 1):
		side, other := 0, l
		if l.ours <= 1 {
			side, other = 1, r
		}
		share := filterSelectivity
		if c, found := w.joinKey(conds[0], outs[side]); found {
			share = 1 / c.distinct
		}
		for i := scans[side]; i < scans[side+1]; i++ {
			w.scans[i].selective = true
		}
		return max(other.ours*share, 1), other.kept * share
	case equal && typ == "INNER":
		if exact, found := w.distinctJoin(conds, rows, outs); found {
			return exact, kept
		}
		fact, dim := l, r
		if r.ours > l.ours {
			fact, dim = r, l
		}
		return max(scaled, fact.ours*dim.kept), kept
	case typ == "LEFT":
		return max(scaled, l.ours), l.kept
	}
	return scaled, kept
}

// joinKey is the column of side, a join's input's columns, that a join
// condition of equal columns compares, if its distinct count is known.
func (w *setWalker) joinKey(cond string, side []column) (column, bool) {
	m := equality.FindStringSubmatch(cond)
	if m == nil {
		return column{}, false
	}
	for _, name := range []string{bareName(m[1]), bareName(m[2])} {
		for _, c := range side {
			if c.name == name && c.distinct > 0 {
				return c, true
			}
		}
	}
	return column{}, false
}

// distinctJoin is the rows of an inner join on equal columns whose
// distinct counts are all known: the product of its inputs' rows over the
// larger distinct count of each pair; found is false where one is not.
func (w *setWalker) distinctJoin(conds []string, rows []rowsEstimate, outs [][]column) (float64, bool) {
	find := func(name string) (column, bool) {
		name = bareName(name)
		for _, o := range outs {
			for _, c := range o {
				if c.name == name && c.distinct > 0 {
					return c, true
				}
			}
		}
		return column{}, false
	}
	out := rows[0].ours * rows[1].ours
	for _, c := range conds {
		m := equality.FindStringSubmatch(c)
		if m == nil {
			return 0, false
		}
		a, okA := find(m[1])
		b, okB := find(m[2])
		if !okA || !okB {
			return 0, false
		}
		out /= max(a.distinct, b.distinct, 1)
	}
	return max(out, 1), true
}

// largestBranch keeps, of the parts the branches of a UNION hold, those of
// the branch that holds most: the branches run one after another where
// memory is short (see the package's notes above). parts are where each
// branch's parts begin in w.parts, and where the last ends.
func (w *setWalker) largestBranch(parts []int) {
	best, most := 0, -1.0
	for i := 0; i+1 < len(parts); i++ {
		var sum float64
		for _, p := range w.parts[parts[i]:parts[i+1]] {
			sum += p.bytes
		}
		if sum > most {
			best, most = i, sum
		}
	}
	kept := slices.Clone(w.parts[parts[best]:parts[best+1]])
	w.parts = append(w.parts[:parts[0]], kept...)
}

// filterValue matches a scan's filter by a constant: a column equal to a
// value or in a list of them.
var filterValue = regexp.MustCompile(`(?i)(^|[^<>!=])=\s*[^=]|\sIN\s*\(`)

// filtersByValue reports whether a scan's filters compare a column to a
// constant with = or IN, which DuckDB reads only the row groups of whose
// values may match (see filtersSelectively); IS NULL and ranges do not
// count.
func filtersByValue(filters []string) bool {
	for _, f := range filters {
		if filterValue.MatchString(f) {
			return true
		}
	}
	return false
}

// constantWindow reports whether a window's functions are all over the
// whole input without an order, OVER ().
func constantWindow(projections []string) bool {
	for _, p := range projections {
		if !strings.Contains(strings.ToUpper(p), "OVER ()") {
			return false
		}
	}
	return len(projections) > 0
}

// joinedDistinct gives the columns an inner join compares for equality
// the smaller of their distinct counts: a customer's nation joined to the
// nations of one region has that region's few.
func joinedDistinct(n *planNode, cols []column) []column {
	if !strings.EqualFold(strings.Join(n.strs("Join Type"), ""), "INNER") {
		return cols
	}
	out := slices.Clone(cols)
	for _, c := range n.strs("Conditions") {
		m := equality.FindStringSubmatch(c)
		if m == nil {
			continue
		}
		a, b := bareName(m[1]), bareName(m[2])
		least := math.Inf(1)
		for _, col := range out {
			if (col.name == a || col.name == b) && col.distinct > 0 {
				least = min(least, col.distinct)
			}
		}
		for i := range out {
			if (out[i].name == a || out[i].name == b) && !math.IsInf(least, 1) {
				out[i].distinct = least
			}
		}
	}
	return out
}

// bareName is a column's name without quotes or qualifier, in lower case.
func bareName(name string) string {
	name = strings.ToLower(strings.Trim(name, `"`))
	if k := strings.LastIndexByte(name, '.'); k >= 0 {
		name = strings.Trim(name[k+1:], `"`)
	}
	return name
}

// limitDistinct caps columns' distinct counts at an operator's rows.
func limitDistinct(cols []column, rows float64) []column {
	if rows <= 0 {
		return cols
	}
	out := make([]column, len(cols))
	for i, c := range cols {
		if c.distinct > rows {
			c.distinct = rows
		}
		out[i] = c
	}
	return out
}

// hold records that operator op, of kind (see heldFactors), holds rows of
// width bytes.
func (w *setWalker) hold(op, kind string, rows, width float64) {
	if rows > 0 {
		w.parts = append(w.parts, heldPart{op: op, rows: rows, bytes: rows * width * heldFactors[kind]})
	}
}

// tableOf is the one table of the statement's that has every column of
// cols, by name, if exactly one has.
func (w *setWalker) tableOf(cols []column) *Table {
	var found *Table
	for _, t := range w.st.tables {
		names := map[string]bool{}
		for _, c := range t.Columns {
			names[strings.ToLower(c.Name)] = true
		}
		all := len(cols) > 0
		for _, c := range cols {
			all = all && c.name != "" && names[c.name]
		}
		if all {
			if found != nil {
				return nil
			}
			found = t
		}
	}
	return found
}

// distinctArg matches an aggregate over DISTINCT values of a column.
var distinctArg = regexp.MustCompile(`(?i)\(\s*DISTINCT\s+([^),]+)`)

// distinct records the pairs of groups and values that aggregates over
// DISTINCT values hold: DuckDB keeps about every row of their input before
// it merges the threads' tables, whatever the values' distinct count (16
// million rows of 200,000 strings held 715 MB).
func (w *setWalker) distinct(op string, aggs []string, in []column, inRows, groupWidth float64) {
	for _, a := range aggs {
		m := distinctArg.FindStringSubmatch(a)
		if m == nil {
			continue
		}
		c := w.columns([]string{strings.TrimSpace(m[1])}, in)[0]
		w.hold(op+" DISTINCT", "distinct", inRows, groupWidth+c.width+hashEntryBytes)
	}
}

// columns resolves the expressions an operator lists: #i is the ith of its
// child's columns, a name a table's column, anything else an expression of
// planExprWidth.
func (w *setWalker) columns(exprs []string, in []column) []column {
	out := make([]column, len(exprs))
	for i, e := range exprs {
		e = strings.TrimSpace(e)
		if strings.HasPrefix(e, "#") {
			if j, err := strconv.Atoi(e[1:]); err == nil && j < len(in) {
				out[i] = in[j]
				continue
			}
		}
		name := strings.ToLower(strings.Trim(e, `"`))
		if k := strings.LastIndexByte(name, '.'); k >= 0 {
			name = name[k+1:]
		}
		if wd, ok := w.st.width[name]; ok {
			out[i] = column{name: name, width: wd, distinct: w.st.distinct[name]}
			continue
		}
		out[i] = column{width: planExprWidth}
	}
	return out
}

func width(cols []column) float64 {
	var w float64
	for _, c := range cols {
		w += max(c.width, planExprWidth/2)
	}
	return w
}

// product is the product of the columns' distinct counts, zero if any is
// unknown.
func product(cols []column) float64 {
	if len(cols) == 0 {
		return 0
	}
	p := 1.0
	for _, c := range cols {
		if c.distinct <= 0 {
			return 0
		}
		p *= c.distinct
		if p > 1e18 {
			return math.Inf(1)
		}
	}
	return p
}

// cardinality is the rows DuckDB estimates the operator outputs.
func (n *planNode) cardinality() (float64, bool) {
	s := strings.TrimLeft(strings.Join(n.strs("Estimated Cardinality"), ""), "~")
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && f >= 0
}

// strs reads an extra_info value: a list of strings, or a string of lines.
func (n *planNode) strs(key string) []string {
	raw, ok := n.Extra[key]
	if !ok {
		return nil
	}
	var list []string
	if json.Unmarshal(raw, &list) == nil {
		return list
	}
	var s string
	if json.Unmarshal(raw, &s) != nil || s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
