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
)

// heldFactors are how many times the rows and widths an operator holds by
// the plan DuckDB holds at its peak, for its layout: a hash join's
// directory of pointers, sized to a power of two, and its partitions; an
// aggregate's partitions; a window's sort beside its rows. Fitted against
// what DuckDB was seen holding (duckdb_memory(), as the server samples it
// for the query history's peaks) over joins of 4 million rows,
// groupings of 4 to 16 million groups, distinct counts, sorts and windows
// of 16 million rows (see TestWorkingSetCalibration).
var heldFactors = map[string]float64{"join": 1.5, "group": 1.5, "distinct": 1.3, "sort": 1.2, "window": 2.0}

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
	var roots []*planNode
	if err := json.Unmarshal([]byte(plan), &roots); err != nil {
		return 0, nil, err
	}
	if len(roots) == 0 {
		return 0, nil, errors.New("an empty plan")
	}
	w := &setWalker{st: st}
	for _, r := range roots {
		w.walk(r)
	}
	if w.unknown != "" {
		return 0, nil, errors.New("no estimate of the rows of " + w.unknown)
	}
	var total float64
	for _, p := range w.parts {
		total += p.bytes
	}
	return total, w.parts, nil
}

type setWalker struct {
	st    planStats
	parts []heldPart
	// unknown names a scan whose rows nothing estimates.
	unknown string
}

// column is a column an operator outputs: its name if it is one, its
// width, and its distinct count, zero for unknown.
type column struct {
	name            string
	width, distinct float64
}

// estimate is an operator's rows: as DuckDB estimates them, and as the
// distinct counts of its join keys correct that (see walk).
type rowsEstimate struct{ duck, ours float64 }

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
	var rows []rowsEstimate
	var outs [][]column
	for _, c := range n.Children {
		r, o := w.walk(c)
		rows, outs = append(rows, r), append(outs, o)
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
	est := rowsEstimate{duck: card, ours: in.scaled(card)}
	if len(rows) == 0 {
		est.ours = card
	}
	name := strings.ToUpper(n.Name)
	switch {
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
		return est, limitDistinct(cols, est.ours)
	case name == "PROJECTION" || name == "FILTER" && len(n.strs("Projections")) > 0:
		return est, limitDistinct(w.columns(n.strs("Projections"), inCols), est.ours)
	case strings.Contains(name, "JOIN") || name == "CROSS_PRODUCT":
		// DuckDB builds on the right, the last child, keeping its keys and
		// the columns the operators above read, which the plan does not
		// list: at most maxBuildWidth of them.
		build, buildCols := child(len(rows) - 1)
		w.hold(name, "join", build.ours, min(width(buildCols), maxBuildWidth)+joinEntryBytes)
		var all []column
		for _, o := range outs {
			all = append(all, o...)
		}
		est.ours = w.joinRows(n, rows, outs, card)
		all = joinedDistinct(n, all)
		return est, limitDistinct(all, est.ours)
	case strings.HasSuffix(name, "GROUP_BY"):
		groups := w.columns(n.strs("Groups"), inCols)
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
		w.hold(name, "group", entries, width(groups)+float64(len(aggs))*planExprWidth+hashEntryBytes)
		w.distinct(name, aggs, inCols, in.ours, width(groups))
		return est, append(groups, make([]column, len(aggs))...)
	case name == "UNGROUPED_AGGREGATE":
		w.distinct(name, n.strs("Aggregates"), inCols, in.ours, 0)
		return rowsEstimate{duck: 1, ours: 1}, make([]column, len(n.strs("Aggregates")))
	case name == "ORDER_BY" || name == "WINDOW":
		est = in
		if name == "WINDOW" {
			w.hold(name, "window", in.ours, width(inCols)+hashEntryBytes)
			return est, append(inCols, column{width: planExprWidth})
		}
		w.hold(name, "sort", in.ours, width(inCols)+hashEntryBytes)
		return est, inCols
	}
	return est, inCols
}

// equality matches a join condition of equal columns.
var equality = regexp.MustCompile(`^\s*(\S+)\s*=\s*(\S+)\s*$`)

// joinRows is a join's rows: for an inner join on equal columns whose
// distinct counts are all known, the product of its inputs' rows over the
// larger distinct count of each pair; otherwise DuckDB's estimate, card,
// corrected as its inputs were.
func (w *setWalker) joinRows(n *planNode, rows []rowsEstimate, outs [][]column, card float64) float64 {
	scaled := card
	for _, r := range rows {
		if r.duck > 0 {
			scaled *= r.ours / r.duck
		}
	}
	conds := n.strs("Conditions")
	if len(rows) != 2 || len(conds) == 0 || !strings.EqualFold(strings.Join(n.strs("Join Type"), ""), "INNER") {
		return scaled
	}
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
			return scaled
		}
		a, okA := find(m[1])
		b, okB := find(m[2])
		if !okA || !okB {
			return scaled
		}
		out /= max(a.distinct, b.distinct, 1)
	}
	return max(out, 1)
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
