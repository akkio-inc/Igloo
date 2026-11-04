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
	"time"
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
// A UNION's rows are all its branches' rows, though they do not run at
// once, and what its branches' filters keep the most any of them keeps. It
// took its first branch's: of a union of an audience, one version's rows of
// a lookup, and everyone, the audience's hundredth of the rows, so that a
// report joining its events to that union, which held hundreds of
// gigabytes and took 714 s on one machine, was estimated at 5.6 GB and 15
// s, and stayed there.
//
// What the estimates above a report's aggregates rest on are DuckDB's guesses
// at their groups, and each operator's rows are corrected as its inputs' were,
// so errors compound: at a hundred times the data that report, by what it took
// for a join of its metrics to themselves, was estimated at 37.9 TB. So without
// a join's keys' distinct counts it does not multiply the rows of the side with
// more, and a grouping by keys it computes keeps no more than aggregateKeeps of
// its input. Now 4.7 TB, nine tenths of it what its aggregates hold of their
// input, about what a tenth of the data came to.
//
// The plan also says what one machine reads (each table's columns that
// its scans project, once: the read cache serves them again) and the
// core-seconds its operators take, by their rows (see operatorWork).

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
	joinEntryBytes = 20
	// maxBuildWidth bounds the columns a join's build side is taken to
	// keep, of the many its input has.
	maxBuildWidth = 48
	// planExprWidth is the width of a column the plan names by an
	// expression rather than a table's column.
	planExprWidth = 8
	// smallSource is the most rows a side of a cross product without
	// tables of its own may have for the product to count every pair.
	smallSource = 1024
	// unnestValues is how many rows UNNEST makes of a row, where DuckDB counts
	// one and nothing here knows the lists. Lists of 3, 6, 2 and 2 values,
	// FLATTEN's of a report, made more than three of a row, and taking it for one
	// took the working set of a report that counts people distinct over them for
	// half what DuckDB held, 2.9 GB of 5.4 GB; a report's of six objects at 157
	// million events held about what one a row would come to.
	unnestValues = 2
	// A grouping's threads each aggregate what they can in a hash table of a
	// few thousand groups, and past that hand rows on and start anew, keeping
	// what they handed on, not one row a group. So what DuckDB held went by
	// steps, measured on 16 threads, of 16 and of 64 million rows at about 60
	// bytes a row: next to nothing up to 10,000 groups; from 20,000 to 200,000
	// groups, 14 to 20 million rows whatever the input; and from 500,000, 86%
	// of the input and more. groupsInTable, groupRowsAThread and groupRowsAGroup,
	// and groupsKeptWhole, are those steps. A thread's count of groups, as this
	// took before, gave a fifth of what it held at 200,000 groups.
	groupsInTable    = 15_000
	groupRowsAThread = 900_000
	groupRowsAGroup  = 2
	groupsKeptWhole  = 300_000
)

// groupEntries is how many rows a grouping of in rows into groups holds, its
// threads apart before they are merged: see groupsInTable.
func groupEntries(in, groups, threads float64) float64 {
	held := groups * threads
	switch {
	case groups >= groupsKeptWhole:
		held = in
	case groups >= groupsInTable:
		held = threads * (groupRowsAThread + groupRowsAGroup*groups)
	}
	return min(in, held)
}

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
var heldFactors = map[string]float64{"join": 1.6, "group": 1.65, "distinct": 1.45, "sort": 1.2, "window": 1.65, "constant window": 1.0,
	"materialized": 1.2, "list": 1.0}

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
	// owner is the table of the statement's that has a column, by its
	// lower-case name, where exactly one has it; span how many values its
	// bounds allow it, where they are known (see Table.Bounds), and
	// timestamp the columns that are timestamps.
	owner     map[string]string
	span      map[string]float64
	timestamp map[string]bool
	// bounds are the columns' bounds, as span is of them; date the columns
	// that are dates.
	bounds  map[string][2]float64
	date    map[string]bool
	tables  []*Table
	threads int
	// learned are the rows operators made before (see learned), if any;
	// with observe, the plan's estimates are the rows its operators made in
	// a run, and walking it teaches learned.
	learned *learned
	observe bool
	// exact says the plan's estimates are the rows its operators made, as
	// when observing: they are taken as they are.
	exact bool
}

// newPlanStats collects the columns of a plan's tables, for a machine of
// threads DuckDB threads.
func newPlanStats(stages []*Stage, threads int) planStats {
	var tables []*Table
	seen := map[*Table]bool{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table != nil && !seen[in.Table] {
				seen[in.Table] = true
				tables = append(tables, in.Table)
			}
		}
	}
	return tableStats(tables, threads)
}

// tableStats collects the columns of tables, for a machine of threads DuckDB
// threads.
func tableStats(tables []*Table, threads int) planStats {
	st := planStats{width: map[string]float64{}, distinct: map[string]float64{}, owner: map[string]string{}, span: map[string]float64{},
		timestamp: map[string]bool{}, bounds: map[string][2]float64{}, date: map[string]bool{}, tables: tables, threads: max(threads, 1)}
	owners := map[string]map[string]bool{}
	for _, t := range tables {
		for _, c := range t.Columns {
			name := strings.ToLower(c.Name)
			if owners[name] == nil {
				owners[name] = map[string]bool{}
			}
			owners[name][strings.ToLower(t.Schema+"."+t.Name)] = true
			st.width[name] = max(st.width[name], memoryBytes(c.Type))
			if b, ok := t.Bounds[name]; ok && b[1] >= b[0] {
				st.span[name] = max(st.span[name], b[1]-b[0]+1)
				if old, seen := st.bounds[name]; seen {
					b = [2]float64{min(old[0], b[0]), max(old[1], b[1])}
				}
				st.bounds[name] = b
			}
			switch typ := strings.ToUpper(c.Type); {
			case strings.HasPrefix(typ, "TIMESTAMP"):
				st.timestamp[name] = true
			case typ == "DATE":
				st.date[name] = true
			}
			if n := t.Distinct[name]; n > 0 {
				st.distinct[name] = max(st.distinct[name], float64(n))
			}
		}
	}
	for name, ts := range owners {
		if len(ts) == 1 {
			for t := range ts {
				st.owner[name] = t
			}
		}
	}
	return st
}

// WorkingSet is what a statement holds on one machine whose DuckDB runs
// threads threads, as auto mode estimates it for this replica: from plan,
// DuckDB's EXPLAIN (FORMAT JSON) of the statement, and tables, those it
// reads (see ScanPlan.Tables). ok is false where they cannot say, as for a
// scan of a table not among tables.
func WorkingSet(plan string, tables []*Table, threads int) (bytes int64, ok bool) {
	p, err := planOf(plan, tableStats(tables, threads))
	if err != nil {
		return 0, false
	}
	return int64(p.set), true
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

// What a scan's readers hold: each of the threads that read a row group of
// the table at once holds the part of it the columns the scan reads take,
// compressed, about scanChunkShare times over (the chunk as read, its pages
// decoded, the next one read ahead), as the table's data files' statistics
// give their sizes (see Table.ColumnBytes); where they do not, a buffer of
// scanThreadBytes and scanWidthShare of the columns' width a row. A sum of
// a column of a hundred million doubles held 230 MB on 19 threads, of the
// same rows' column of a thousand values 22 MB; a dashboard's tiles, joining
// a few thousand impressions to campaigns and grouping them, held 100 to
// 300 MB, where their hash tables held next to nothing. The table's row
// groups are as many rows as rowGroupRows makes of its bytes. Scans run
// one pipeline at a time, so the largest counts.
const (
	scanChunkShare  = 3.0
	scanThreadBytes = 11 << 20
	scanWidthShare  = 0.135
)

// scanBytes is what a scan of t's columns cols, width bytes wide a row,
// holds on threads threads.
func scanBytes(t *Table, cols []column, threads int) float64 {
	if t == nil || t.Rows <= 0 {
		return 0
	}
	rows := float64(rowGroupRows(t))
	busy := min(float64(threads), math.Ceil(float64(t.Rows)/rows))
	if compressed, ok := columnsBytes(t, cols); ok {
		return busy * scanChunkShare * compressed * min(rows/float64(t.Rows), 1)
	}
	// No more than the columns take whole, about as often over: a table of
	// a few rows holds next to nothing.
	return min(busy*(scanThreadBytes+scanWidthShare*min(rows, float64(t.Rows))*width(cols)), scanChunkShare*float64(t.Rows)*width(cols))
}

// columnsBytes is what cols take in t's data files, compressed; ok is
// false if one's size is not known.
func columnsBytes(t *Table, cols []column) (float64, bool) {
	if len(t.ColumnBytes) == 0 || len(cols) == 0 {
		return 0, false
	}
	var sum float64
	for _, c := range cols {
		n, ok := t.ColumnBytes[c.name]
		if !ok {
			return 0, false
		}
		sum += float64(n)
	}
	return sum, true
}

// rowGroupRows is how many rows t's row groups have, as Igloo writes them:
// about 16 MiB of its compressed bytes a row (see filerewrite.TargetRows),
// 500,000 where its bytes are not known.
func rowGroupRows(t *Table) int64 {
	const target, least, most, unknown = 16 << 20, 122_880, 16 << 20, 500_000
	if t.Bytes <= 0 || t.Rows <= 0 {
		return unknown
	}
	return min(max(int64(float64(target)*float64(t.Rows)/float64(t.Bytes)), least), most)
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
// replica's sampling of DuckDB's memory saw it. It is its working set when
// it runs again, the most of its runs so, rather than its plan's estimate:
// of statements run alone again and again, every run's peak was within
// twice what the next held, where plans are for 80% of them on a first run.
// DuckDB's plans' other working sets are corrected by such peaks against
// what they estimated (see memoryScale). A peak at the memory limit's share
// may be one spilled, and says less than it held.
func (co *Coordinator) ObservePeak(key string, peak int64) {
	if key == "" || peak <= 0 || co.opts.MemoryBytes > 0 && float64(peak) >= memoryShare*float64(co.opts.MemoryBytes) {
		return
	}
	if r, ok := co.history.get(key); ok && r.planSet > 0 {
		co.memory.observe(r.planSet, float64(peak))
	}
	co.history.update(key, func(r *runs) { r.peak = max(r.peak, float64(peak)) })
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
// core-seconds its operators take (see operatorWork).
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
	// the client; sorted is what its largest sort or window holds; joins
	// the share of the working set its joins' hash tables hold.
	fields, sorted, joins float64
	// rows are its result's rows, which a fill writes.
	rows float64
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
		case name == "CTE" && len(n.Children) == 2:
			n = n.Children[1]
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
	if aggregated && w.scanned > 0 {
		// DuckDB's guess at its groups, a client's to take on any way the
		// statement runs: at a hundred times the data, an aggregate by a table's
		// genre, of a few dozen rows, was estimated at 3,300 s of sending them
		// on 2 workers or 16, which took 2 for 3,999 s as near 16's 3,375, and
		// it took 449 s there and 25 s on 16.
		result = min(result, aggregateKeeps*w.scanned)
	}
	if w.scanHeld > 0 {
		w.parts = append(w.parts, heldPart{op: "SCAN", bytes: w.scanHeld})
	}
	out := planEstimate{parts: w.parts, work: w.work, bytes: w.bytes(), fields: result * float64(max(columns, 1)), rows: result}
	var joins, all float64
	for _, p := range w.parts {
		op := strings.ToUpper(p.op)
		if strings.Contains(op, "ORDER_BY") || strings.Contains(op, "WINDOW") {
			out.sorted = max(out.sorted, p.bytes)
		}
		if strings.Contains(op, "JOIN") {
			joins += p.bytes
		}
		all += p.bytes
	}
	if all > 0 {
		out.joins = joins / all
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
	// core-seconds its operators take (see operatorWork).
	ctes  map[string]cte
	scans []scanRead
	work  float64
	// scanned is the rows of the tables its scans read.
	scanned float64
	// scanHeld is what its largest scan's readers hold (see scanBytes).
	scanHeld float64
	// filterIn are the columns of the input of the filter being estimated
	// (see valuesShare).
	filterIn []column
	// delim is the correlated subquery the walk is in (see delimScope).
	delim delimScope
}

// delimScope is a delim join's correlated subquery, as its DELIM_SCAN and
// UNNEST see it: the rows of the join's input, which its DELIM_SCAN makes at
// most, their distinct values, and how many of their columns the join takes
// the subquery's rows back by. A FLATTEN of a row's object, as a report's
// profile of an audience writes it, unnests a list of the row's own columns,
// which the plan does not list: as many values as the columns it joins by.
// DuckDB took each DELIM_SCAN of such a report for every row of the largest
// table it read, 282 million, where it made 189,000.
type delimScope struct {
	rows float64
	cols int
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

// What a row costs DuckDB's operators, in seconds of a thread, fitted to the
// time each operator of the estimate corpus's statements took in their runs'
// profiles, by the rows it and its inputs made (bench/estimates.py keeps
// each operator's seconds): a scan's row, and each byte of the row it hands
// on; a hash join's row probed, row built on and row made; a delim join's row
// of input; an aggregate's row in and group made, and its row in again for
// each aggregate over distinct values or over all of a group's (median,
// list); a window's or a sort's row times log2 of their rows where it
// orders, and a window's row where it does not; an UNNEST's row made, of a
// list the statement writes, as FLATTEN of a row's object does, or of one a
// column holds; and a filter's row in. A statement's operators took 0.86 of
// its run's time on its machine's cores (operatorShare), at the median of
// those of a second or more.
//
// Hash tables' rows alone, the estimate before, missed what made reports
// long: a report's profile of an audience, FLATTENing its people's objects
// of 6 to 20 columns, spent 19 of its 90 s in UNNEST, at 1.9 µs a row, 17
// in scans of tables of 7 to 45 columns, and 26 building on 80 million
// rows, and was estimated at a third of it; the corpus's report that unnests
// three values of each of 14 million orders took 16 s and was estimated at
// 1.3. Over the operators of half a second or more, by their rows in the
// runs, the costs put 76 hash joins of 106, 71 aggregates of 126, 13 windows
// of 21, 13 unnests of 15 and 76 scans of 222 within twice what they took;
// over whole statements whose operators took 2 s or more, 100 of 157.
const (
	scanRowCost       = 20e-9
	scanByteCost      = 0.62e-9
	joinProbeCost     = 41e-9
	joinBuildCost     = 120e-9
	joinOutCost       = 56e-9
	delimInCost       = 140e-9
	aggregateInCost   = 31e-9
	aggregateOutCost  = 240e-9
	distinctInCost    = 120e-9
	holisticInCost    = 190e-9
	orderedRowCost    = 22e-9
	windowRowCost     = 100e-9
	writtenUnnestCost = 1.2e-6
	storedUnnestCost  = 5e-9
	filterInCost      = 6.4e-9
	operatorShare     = 0.86
)

// holistic matches an aggregate over all of a group's values, which it keeps.
var holistic = regexp.MustCompile(`(?i)\b(list|array_agg|string_agg|median|quantile\w*|mode|approx_quantile|histogram)\(`)

// operatorWork is the core-seconds an operator n takes, by the rows its
// children made, in, and it made, out, as made of columns cols; flattens says
// it is within a delim join's subquery that unnests a row's own columns (see
// delimScope).
func operatorWork(n *planNode, in []rowsEstimate, out rowsEstimate, cols []column, flattens bool) float64 {
	name := strings.ToUpper(n.Name)
	var all float64
	for _, r := range in {
		all += r.ours
	}
	ordered := func(rows float64) float64 { return rows * math.Log2(max(rows, 2)) * orderedRowCost }
	var cost float64
	switch {
	case len(in) == 0 && (name == "ICEBERG_SCAN" || name == "TABLE_SCAN" || name == "SEQ_SCAN" || strings.HasPrefix(name, "READ_")):
		cost = out.ours * (scanRowCost + scanByteCost*width(cols))
	case strings.HasSuffix(name, "DELIM_JOIN") && len(in) > 0:
		cost = in[0].ours * delimInCost
	case strings.Contains(name, "JOIN") && len(in) >= 2:
		cost = in[0].ours*joinProbeCost + in[len(in)-1].ours*joinBuildCost + out.ours*joinOutCost
	case strings.HasSuffix(name, "GROUP_BY"), name == "UNGROUPED_AGGREGATE":
		cost = all * aggregateInCost
		if name != "UNGROUPED_AGGREGATE" {
			cost += out.ours * aggregateOutCost
		}
		for _, a := range n.strs("Aggregates") {
			if strings.Contains(strings.ToUpper(a), "DISTINCT") {
				cost += all * distinctInCost
			}
			if holistic.MatchString(a) {
				cost += all * holisticInCost
			}
		}
	case name == "WINDOW":
		p := strings.ToUpper(strings.Join(n.strs("Projections"), " "))
		if strings.Contains(p, "PARTITION BY") || strings.Contains(p, "ORDER BY") {
			cost = ordered(all)
		} else {
			cost = all * windowRowCost
		}
	case name == "ORDER_BY":
		cost = ordered(all)
	case name == "UNNEST", name == "INOUT_FUNCTION" && unnestFunction(n):
		if _, written := writtenValues(n); written || flattens {
			cost = out.ours * writtenUnnestCost
		} else {
			cost = out.ours * storedUnnestCost
		}
	case name == "FILTER":
		cost = all * filterInCost
	}
	return cost / operatorShare
}

// notDistinct is how many of conds compare columns IS NOT DISTINCT FROM, as a
// delim join takes its subquery's rows back by its input's columns.
func notDistinct(conds []string) int {
	n := 0
	for _, c := range conds {
		n += strings.Count(strings.ToUpper(c), "IS NOT DISTINCT FROM")
	}
	return n
}

// column is a column an operator outputs: its name if it is one, its
// width, and its distinct count, zero for unknown.
type column struct {
	name            string
	width, distinct float64
	// table is the statement's table it is a column of, where that is
	// known: what learned keeps what its groupings and joins made by.
	table string
	// span is how many values its bounds allow it, zero if not known.
	span float64
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
func (w *setWalker) walk(n *planNode) (made rowsEstimate, madeCols []column) {
	name := strings.ToUpper(n.Name)
	var rows []rowsEstimate
	var outs [][]column
	// Where each child's parts and scans begin, and end, the last.
	parts, scans := []int{len(w.parts)}, []int{len(w.scans)}
	outer := w.delim
	for i, c := range n.Children {
		r, o := w.walk(c)
		rows, outs = append(rows, r), append(outs, o)
		parts, scans = append(parts, len(w.parts)), append(scans, len(w.scans))
		if name == "CTE" && i == 0 {
			w.ctes[strings.Join(n.strs("Table Index"), "")] = cte{r, o}
		}
		if strings.HasSuffix(name, "DELIM_JOIN") && i == 0 {
			// Its subquery's, after its input (see delimScope).
			w.delim = delimScope{rows: r.ours, cols: notDistinct(n.strs("Conditions"))}
		}
	}
	w.delim = outer
	defer func() { w.work += operatorWork(n, rows, made, madeCols, w.delim.cols > 0) }()
	if name == "UNION" && len(rows) > 1 {
		w.largestBranch(parts)
	}
	card, ok := n.cardinality()
	if !ok {
		for _, r := range rows {
			card = max(card, r.duck)
		}
		if name == "CROSS_PRODUCT" && len(rows) == 2 {
			// Every pair, as DuckDB takes it in the operators above, which
			// say so where it does not: took for its larger side's rows, what
			// they say counted the product's other side twice.
			card = rows[0].duck * rows[1].duck
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
	case name == "CTE" && len(rows) == 2:
		// A statement's rows are its body's, not those of what it reads.
		return rows[1], outs[1]
	case name == "TOP_N":
		// No more than it keeps, for a client that takes as many.
		if top, err := strconv.ParseFloat(strings.TrimSpace(strings.Join(n.strs("Top"), "")), 64); err == nil {
			est.ours, est.duck = min(est.ours, top), min(est.duck, top)
		}
		return est, inCols
	case name == "UNION" && len(rows) > 1:
		return unionRows(rows, card, ok), unionColumns(outs)
	case name == "CTE_SCAN":
		// The common table expression's rows, as corrected.
		if c, found := w.ctes[strings.Join(n.strs("CTE Index"), "")]; found {
			est.ours, est.kept = c.rows.ours, c.rows.kept
			return est, limitDistinct(c.cols, est.ours)
		}
		return est, nil
	case strings.Contains(name, "SCAN"), strings.HasPrefix(name, "READ_"):
		cols := w.columns(n.strs("Projections"), nil)
		if name == "DELIM_SCAN" && w.delim.rows > 0 {
			// No more than its delim join's input (see delimScope).
			est.ours = min(est.ours, w.delim.rows)
		}
		if len(n.Children) > 0 || name == "DUMMY_SCAN" || strings.HasPrefix(name, "CTE") || strings.HasPrefix(name, "DELIM") {
			return est, limitDistinct(cols, est.ours)
		}
		// A scan of a table: no more rows than the table has, which DuckDB
		// guesses for a table of many small files as a thousand rows a file
		// (a TPC-H nation of 25 rows in 8 files it took for 8,000), and
		// estimates not at all for an Iceberg scan.
		t := w.scanTable(cols, n.strs("Filters"))
		switch {
		case t != nil && t.Rows > 0 && len(n.strs("Filters")) > 0 && !w.st.exact && !w.st.observe:
			// The share of the table its filters keep, by what they compare
			// (see filtered), not DuckDB's fifth a filter.
			w.filterIn = cols
			est.ours = max(w.filtered(n.strs("Filters"), float64(t.Rows), card, ok), 1)
			w.filterIn = nil
			if !ok {
				est.duck = est.ours
			}
		case t != nil && t.Rows > 0 && (!ok || card > float64(t.Rows)):
			est.ours = float64(t.Rows)
			if !ok {
				est.duck = est.ours
			}
		case !ok:
			w.unknown = name
		}
		// What scans of the table with the same filters kept before (see
		// learned).
		if key := scanKey(t, n.strs("Filters")); key != "" && t.Rows > 0 {
			if w.st.observe && ok {
				w.st.learned.observeScan(key, card/float64(t.Rows))
			} else if share, known := w.st.learned.scanFor(key); known {
				est.ours = max(share*float64(t.Rows), 1)
			}
		}
		switch {
		case t != nil && t.Rows > 0:
			w.scanned += float64(t.Rows)
		case ok:
			w.scanned += card
		}
		if t != nil {
			w.scanHeld = max(w.scanHeld, scanBytes(t, cols, w.st.threads))
			read := columnSet{}
			for _, c := range cols {
				read[c.name] = true
			}
			w.scans = append(w.scans, scanRead{t: t, cols: read, selective: filtersByValue(n.strs("Filters"))})
			if t.Rows > 0 {
				est.kept = min(1, est.ours/float64(t.Rows))
			}
		}
		return est, limitDistinct(fixValues(cols, n.strs("Filters")), est.ours)
	case name == "FILTER" && len(rows) == 1 && len(n.strs("Expression")) > 0 && !w.st.exact && !w.st.observe:
		// What its expression keeps of its input (see filtered), where
		// DuckDB takes a fifth of it for each filter it cannot estimate: a
		// common table expression of every person filtered by when each was
		// last seen, which kept 9.3 million of 10, was taken for 400,000.
		w.filterIn = inCols
		est.ours = max(w.filtered(n.strs("Expression"), in.ours, in.scaled(card), ok), min(in.ours, 1))
		w.filterIn = nil
		if in.ours > 0 {
			est.kept = in.kept * min(est.ours/in.ours, 1)
		}
		kept := fixValues(inCols, n.strs("Expression"))
		if p := n.strs("Projections"); len(p) > 0 {
			return est, limitDistinct(w.columns(p, kept), est.ours)
		}
		return est, limitDistinct(kept, est.ours)
	case name == "PROJECTION" || name == "FILTER" && len(n.strs("Projections")) > 0:
		return est, limitDistinct(w.columns(n.strs("Projections"), inCols), est.ours)
	case strings.HasSuffix(name, "DELIM_JOIN") && len(rows) == 3:
		// A correlated subquery's join, or a LATERAL one's, as UNNEST over a
		// row's own values makes: its first child's rows are materialized
		// whole, and joined to what the subquery makes of their distinct
		// values (its second child's join, the third their grouping). A right
		// one builds its join's hash table on those rows, which the join's
		// own build side, an empty scan, stands for; a left one keeps them
		// apart and builds on the subquery's. Each of the rows gets the rows
		// the subquery makes of its values: as many as an UNNEST's list has,
		// one otherwise (DuckDB takes the subquery for one row a value).
		lhs, lhsCols := child(0)
		if strings.HasPrefix(name, "RIGHT") {
			w.hold(name, "join", lhs.ours, min(width(lhsCols), maxBuildWidth)+joinEntryBytes)
		} else {
			w.hold(name, "materialized", lhs.ours, width(lhsCols))
		}
		fan := 1.0
		if u := findUnnest(n.Children[1]); u != nil {
			fan = listValues(u, notDistinct(n.strs("Conditions")))
		}
		est.ours, est.kept = lhs.ours*fan, lhs.kept
		_, sub := child(1)
		return est, limitDistinct(append(slices.Clone(lhsCols), sub...), est.ours)
	case strings.Contains(name, "JOIN") || name == "CROSS_PRODUCT":
		// DuckDB builds on the right, the last child, keeping its keys and
		// the columns the operators above read, which the plan does not
		// list: at most maxBuildWidth of them.
		build, buildCols := child(len(rows) - 1)
		entry := min(width(buildCols), maxBuildWidth) + joinEntryBytes
		w.hold(name, "join", build.ours, entry)
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
		key := groupingKey(groups)
		if w.st.observe && len(rows) > 0 && ok {
			w.st.learned.observeGroups(key, card, in.duck)
		}
		learnedGroups, known := w.st.learned.groupsFor(key, in.ours)
		if known && !w.st.observe {
			est.ours = learnedGroups
		}
		ndv := w.distinctCount(groups)
		if ndv > 0 {
			est.ours = min(est.ours, ndv)
		} else if len(groups) > 0 && !known {
			// One group by DuckDB's estimate, of keys nothing knows, is no
			// one row to join to (see joinRows): it took a report's two
			// cohorts' sizes for one, and the report's scans below that join
			// for a filter's hundredth of what they read.
			est.ours = max(est.ours, 2)
		}
		est.ours = min(est.ours, max(in.ours, 1))
		// Each thread aggregates its share of the rows apart, all the
		// groups it meets, before they are merged.
		entries := groupEntries(in.ours, est.ours, float64(w.st.threads))
		if ndv <= 0 && !known {
			// How many groups only by DuckDB's estimate, often far too many:
			// what the steps say of them is no better than a thread's count.
			entries = min(entries, est.ours*float64(w.st.threads))
		}
		if len(rows) == 0 {
			entries = est.ours
		}
		aggs := n.strs("Aggregates")
		entry := width(groups) + float64(len(aggs))*planExprWidth + hashEntryBytes
		w.hold(name, "group", entries, entry)
		w.distinct(name, aggs, inCols, groups, in.ours)
		w.lists(name, aggs, inCols, in.ours)
		if computedKeys(groups) {
			// Keys it computes, as a report's dimensions and their values:
			// rows above it are few, whatever it holds on its way to them
			// (see aggregateKeeps).
			est.ours = max(min(est.ours, aggregateKeeps*in.ours), min(est.ours, 2))
		}
		return est, append(groups, make([]column, len(aggs))...)
	case name == "UNGROUPED_AGGREGATE":
		w.distinct(name, n.strs("Aggregates"), inCols, nil, in.ours)
		return rowsEstimate{duck: 1, ours: 1, kept: 1}, make([]column, len(n.strs("Aggregates")))
	case name == "UNNEST", name == "INOUT_FUNCTION" && unnestFunction(n):
		// As many rows as its lists have values (see listValues).
		est.ours *= listValues(n, w.delim.cols)
		return est, limitDistinct(inCols, est.ours)
	case name == "ORDER_BY" || name == "WINDOW":
		est = in
		if name == "WINDOW" {
			kind := "window"
			if constantWindow(n.strs("Projections")) {
				kind = "constant window"
			}
			entry := width(inCols) + hashEntryBytes
			if kind == "constant window" {
				entry = width(inCols)
			}
			w.hold(name, kind, in.ours, entry)
			return est, append(inCols, column{width: planExprWidth})
		}
		w.hold(name, "sort", in.ours, width(inCols)+hashEntryBytes)
		return est, inCols
	}
	return est, inCols
}

// unionRows is a UNION's rows, of its branches' rows: DuckDB's estimate of
// them, card, if ok says it has one, or else their sum, as the operators
// above it take it (a scan of it as a common table expression does), and
// ours their sum, of which its branches' filters keep the most any keeps.
func unionRows(rows []rowsEstimate, card float64, ok bool) rowsEstimate {
	var est rowsEstimate
	for _, r := range rows {
		est.duck += r.duck
		est.ours += r.ours
		est.kept = max(est.kept, r.kept)
	}
	if ok {
		est.duck = card
	}
	return est
}

// unionColumns are a UNION's columns: its first branch's, named and as wide,
// each of as many distinct values as its branches' come to, if all know
// theirs.
func unionColumns(branches [][]column) []column {
	out := slices.Clone(branches[0])
	for _, b := range branches[1:] {
		for i := range out {
			switch {
			case i >= len(b) || b[i].distinct <= 0:
				out[i].distinct = 0
			default:
				out[i].distinct += b[i].distinct
			}
			if i < len(b) {
				out[i].width = max(out[i].width, b[i].width)
				if b[i].table != out[i].table || b[i].name != out[i].name {
					out[i].table = ""
				}
			}
		}
	}
	return out
}

// computedKeys says a grouping has keys, and none is a table's column, which
// may have as many values as rows, and none has a distinct count.
func computedKeys(groups []column) bool {
	return len(groups) > 0 && !slices.ContainsFunc(groups, func(c column) bool { return c.name != "" || c.distinct > 0 })
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
//     filters keep, each row of it joined to its key's row of the other, and
//     no more than that side's: nothing says it multiplies them, however
//     DuckDB's guesses at its inputs compound (see the package's notes);
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
	if equal && typ == "INNER" {
		// What a join by the same columns made before (see learned).
		if key := joinKeyOf(joinPairs(conds, outs)); key != "" {
			if w.st.observe {
				w.st.learned.observeJoin(key, l.duck, r.duck, card)
			} else if out, ok := w.st.learned.joinFor(key, l.ours, r.ours); ok {
				return out, kept
			}
		}
	}
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
			if w.scans[i].selective {
				// DuckDB's rows already are its filter's: one audience's rows,
				// as a lookup's are taken for by every audience's share of
				// them, of which the latest version is most of a large one.
				share = 1
			}
			w.scans[i].selective = true
		}
		return max(other.ours*share, 1), other.kept * share
	case (equal || strings.Contains(name, "HASH")) && typ == "INNER":
		// A hash join compares at least one pair of columns for equality,
		// whatever else its conditions compare (a report's join of a year's
		// sales to the next year's by item and store, syear = syear + 1).
		if exact, found := w.distinctJoin(conds, rows, outs); found {
			return exact, kept
		}
		fact, dim, side := l, r, 0
		if r.ours > l.ours {
			fact, dim, side = r, l, 1
		}
		// The other side's filters keep of the keys the side with more has,
		// not of all of its own: a filter of two centuries of dates that
		// keeps a year, joined to five years of sales, keeps a fifth of
		// them, not a two-hundredth. So the share they keep is taken of the
		// keys' bounds the side with more has, where both sides' are known.
		dimKept := dim.kept
		pairs := joinPairs(conds, outs)
		for _, p := range pairs {
			if f, d := p[side], p[1-side]; f.span > 0 && d.span > f.span {
				dimKept *= d.span / f.span
			}
		}
		capped := max(min(scaled, fact.ours), fact.ours*min(dimKept, 1))
		// No fewer than its keys' bounds make of its inputs' rows: keys of at
		// most span values, drawn apart, make l*r/span rows or more, as five
		// memberships of each person, joined to each person's events, made
		// five rows of each event.
		if span := joinSpan(pairs); span > 0 {
			capped = max(capped, l.ours*r.ours/span)
		}
		return capped, kept
	case equal && typ == "SEMI":
		// Each key of the other side keeps a row at least, where its rows are
		// that many: DuckDB takes it to keep a fifth of them.
		return min(max(scaled, r.ours), l.ours), kept
	case typ == "LEFT":
		return max(min(scaled, max(l.ours, r.ours)), l.ours), l.kept
	}
	return scaled, kept
}

// joinPairs are the columns, of a join's two inputs' (outs), that its
// conditions of equal columns compare, nil if one compares columns not
// found so.
func joinPairs(conds []string, outs [][]column) [][2]column {
	if len(outs) != 2 {
		return nil
	}
	find := func(name string, side []column) (column, bool) {
		name = bareName(name)
		for _, c := range side {
			if c.name == name {
				return c, true
			}
		}
		return column{}, false
	}
	var pairs [][2]column
	for _, c := range conds {
		m := equality.FindStringSubmatch(c)
		if m == nil {
			return nil
		}
		a, okA := find(m[1], outs[0])
		b, okB := find(m[2], outs[1])
		if !okA || !okB {
			a, okA = find(m[2], outs[0])
			b, okB = find(m[1], outs[1])
		}
		if !okA || !okB {
			return nil
		}
		pairs = append(pairs, [2]column{a, b})
	}
	return pairs
}

// joinSpan is the most combinations of values the keys of a join's pairs
// of columns allow, by their bounds: the product over the pairs of the
// larger span of each; zero unless every pair's bounds are known.
func joinSpan(pairs [][2]column) float64 {
	if len(pairs) == 0 {
		return 0
	}
	span := 1.0
	for _, p := range pairs {
		if p[0].span <= 0 || p[1].span <= 0 {
			return 0
		}
		span *= max(p[0].span, p[1].span)
	}
	return span
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
	pairs := joinPairs(conds, outs)
	if len(pairs) == 0 {
		return 0, false
	}
	// Each row of one side meets no more rows of the other than the other's
	// table has of a value of its keys: a fact's row meets one dimension's.
	// So the inputs' estimates, however far off, are not multiplied past it.
	out, dupL, dupR := rows[0].ours*rows[1].ours, math.Inf(1), math.Inf(1)
	for _, p := range pairs {
		if p[0].distinct <= 0 || p[1].distinct <= 0 {
			return 0, false
		}
		out /= max(p[0].distinct, p[1].distinct, 1)
		dupL, dupR = min(dupL, w.dup(p[0])), min(dupR, w.dup(p[1]))
	}
	out = min(out, rows[1].ours*dupL, rows[0].ours*dupR)
	return max(out, 1), true
}

// dup is how many rows of its table a value of column c has, on average,
// by its distinct count; infinity where that is not known.
func (w *setWalker) dup(c column) float64 {
	if c.distinct <= 0 || c.table == "" {
		return math.Inf(1)
	}
	for _, t := range w.st.tables {
		if t.Rows > 0 && strings.EqualFold(t.Schema+"."+t.Name, c.table) {
			return max(float64(t.Rows)/c.distinct, 1)
		}
	}
	return math.Inf(1)
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

// What a filter keeps of its input: the product of what each of its
// conjuncts keeps. DuckDB takes each filter it has no statistics for to keep
// a fifth, and its products ran far short: filters that a column is set, or
// not empty, or not one value keep nearly every row, and the scans of a
// dashboard burst's audiences and attributes, filtered so, were taken for a
// twentieth of what they read. So a conjunct keeps:
//   - all of its rows, as IS NOT NULL, != and <>;
//   - a twentieth, as IS NULL;
//   - for the ranges of a column with bounds (an integer, a date, a
//     timestamp), taken together, the share of the bounds they leave;
//   - for equality with a constant, one over the column's distinct count,
//     or else over the values its bounds allow, for an integer or a date;
//     for IN, as many times that as its list has values;
//   - for an OR, what its branches keep, together, as many as there are;
//   - half, for anything else, as LIKE or a function's result;
//   - as DuckDB estimates it, for equality or a range of a column of which
//     the statement's tables tell neither: DuckDB may know more (Igloo hands
//     it ANALYZE's distinct counts), and took an audience of 3.3 million of
//     412 million memberships for one million where a fifth would be 82
//     million.
//
// A scan's optional filters, the values a filter above it compares, as
// DuckDB pushes them down to skip what it can, keep all its rows: the filter
// above does the keeping.
const (
	nullShare    = 0.05
	equalShare   = 0.2
	unknownShare = 0.5
)

// filtered is the rows a filter of exprs, its conjuncts or lists of them,
// keeps of in rows, of which DuckDB estimated it keeps duck, if ok: a
// fifth, for all of its conjuncts but equalities, and the rest for its
// equalities. What DuckDB estimates stands for the conjuncts this does not.
func (w *setWalker) filtered(exprs []string, in, duck float64, ok bool) float64 {
	var conj []string
	for _, e := range exprs {
		if !strings.HasPrefix(e, "optional: ") {
			conj = append(conj, filterConjuncts(e)...)
		}
	}
	f := w.conjunction(conj)
	switch {
	case !f.deferred:
		return in * f.share
	case !ok:
		return in * f.share * equalShare
	case f.others && !f.deferredOther:
		// DuckDB's fifth is for the conjuncts estimated here.
		duck /= equalShare
	}
	return min(duck, in) * f.share
}

// filter is what conjuncts keep together (see filtered): share, of the
// conjuncts estimated here; deferred says one is left to DuckDB's estimate,
// and deferredOther that one of those is not an equality; others that one
// estimated here is not an equality.
type filter struct {
	share                           float64
	deferred, deferredOther, others bool
}

// conjunction is what conjuncts keep together.
func (w *setWalker) conjunction(conj []string) filter {
	f := filter{share: 1}
	ranges := map[string][2]float64{}
	for _, c := range conj {
		if col, side, v, ok := w.rangeOf(c); ok {
			r, seen := ranges[col]
			if !seen {
				r = w.st.bounds[col]
			}
			if side == 0 {
				r[0] = max(r[0], v)
			} else {
				r[1] = min(r[1], v)
			}
			ranges[col], f.others = r, true
			continue
		}
		share, equal, known := w.conjunctShare(c)
		switch {
		case !known:
			f.deferred = true
			f.deferredOther = f.deferredOther || !equal
		case equal:
			f.share *= share
		default:
			f.share, f.others = f.share*share, true
		}
	}
	for col, r := range ranges {
		b := w.st.bounds[col]
		f.share *= min(max(w.spanOf(col, r)/w.spanOf(col, b), 0), 1)
	}
	return f
}

// rangeOf is a conjunct comparing a column with bounds to a constant: the
// column, the side of the column's values it leaves (0 those at least v, 1
// those at most v) and v, in the units of its bounds.
func (w *setWalker) rangeOf(c string) (col string, side int, v float64, ok bool) {
	m := rangeCompare.FindStringSubmatch(c)
	if m == nil {
		return "", 0, 0, false
	}
	col = strings.ToLower(m[1])
	if b, known := w.st.bounds[col]; !known || b[1] <= b[0] {
		return "", 0, 0, false
	}
	v, ok = w.constant(col, strings.TrimSpace(m[3]))
	if !ok {
		return "", 0, 0, false
	}
	step := 0.0
	if w.discrete(col) {
		step = 1
	}
	switch m[2] {
	case ">":
		return col, 0, v + step, true
	case ">=":
		return col, 0, v, true
	case "<":
		return col, 1, v - step, true
	}
	return col, 1, v, true
}

// discrete says column col's values are whole steps of its bounds: an
// integer's or a date's, not a timestamp's or a fraction's.
func (w *setWalker) discrete(col string) bool {
	b := w.st.bounds[col]
	return !w.st.timestamp[col] && b[0] == math.Trunc(b[0]) && b[1] == math.Trunc(b[1])
}

// spanOf is how many values of column col lie from r[0] to r[1]: as many
// as whole steps, both ends counted, for a discrete column.
func (w *setWalker) spanOf(col string, r [2]float64) float64 {
	if w.discrete(col) {
		return r[1] - r[0] + 1
	}
	return r[1] - r[0]
}

// fixValues is cols with the distinct counts of those that conjuncts of
// exprs, a filter's, hold to constants no more than those constants.
func fixValues(cols []column, exprs []string) []column {
	out := slices.Clone(cols)
	for _, e := range exprs {
		if strings.HasPrefix(e, "optional: ") {
			continue
		}
		for _, c := range filterConjuncts(e) {
			col, k, ok := fixedValues(c)
			if !ok {
				continue
			}
			for i := range out {
				if out[i].name == col && (out[i].distinct <= 0 || out[i].distinct > k) {
					out[i].distinct = k
				}
			}
		}
	}
	return out
}

// fixedValues is the column a conjunct holds to constants, and how many:
// as equality, IN or an OR of those of one column.
func fixedValues(c string) (col string, k float64, ok bool) {
	c = unwrap(strings.TrimSpace(c))
	if branches := splitTop(c, " OR "); len(branches) > 1 {
		for _, b := range branches {
			bc, bk, bok := fixedValues(b)
			if !bok || (col != "" && bc != col) {
				return "", 0, false
			}
			col, k = bc, k+bk
		}
		return col, k, true
	}
	if m := inList.FindStringSubmatch(c); m != nil {
		return strings.ToLower(m[1]), float64(len(splitTop(m[2], ", "))), true
	}
	if m := equalCompare.FindStringSubmatch(c); m != nil && !strings.ContainsAny(m[2], "(") {
		return strings.ToLower(m[1]), 1, true
	}
	return "", 0, false
}

// filterConjuncts splits e at its top-level ANDs, outer parentheses removed.
func filterConjuncts(e string) []string {
	e = unwrap(strings.TrimSpace(e))
	var out []string
	depth, start, quote := 0, 0, false
	for i := 0; i < len(e); i++ {
		switch c := e[i]; {
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(e[i:], " AND "):
			out = append(out, unwrap(strings.TrimSpace(e[start:i])))
			start = i + len(" AND ")
		}
	}
	return append(out, unwrap(strings.TrimSpace(e[start:])))
}

// unwrap removes parentheses around the whole of e.
func unwrap(e string) string {
	for len(e) > 1 && e[0] == '(' && e[len(e)-1] == ')' {
		depth, whole := 0, true
		for i := 0; i < len(e)-1; i++ {
			if e[i] == '(' {
				depth++
			} else if e[i] == ')' {
				depth--
			}
			if depth == 0 {
				whole = false
				break
			}
		}
		if !whole {
			break
		}
		e = strings.TrimSpace(e[1 : len(e)-1])
	}
	return e
}

var (
	rangeCompare = regexp.MustCompile(`^"?([A-Za-z_][A-Za-z0-9_]*)"?\s*(>=|<=|>|<)\s*(.+)$`)
	equalCompare = regexp.MustCompile(`^"?([A-Za-z_][A-Za-z0-9_]*)"?\s*=\s*(.+)$`)
	inList       = regexp.MustCompile(`(?i)^"?([A-Za-z_][A-Za-z0-9_]*)"?\s+IN\s*\((.*)\)$`)
	dateLiteral  = regexp.MustCompile(`^'(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?))?'(?:::\w+(?: WITH TIME ZONE)?)?$`)
)

// conjunctShare is what one conjunct of a filter keeps (see filtered);
// equal says it is an equality with constants, and known that this knows
// what it keeps.
func (w *setWalker) conjunctShare(c string) (share float64, equal, known bool) {
	upper := strings.ToUpper(c)
	switch {
	case strings.HasPrefix(upper, "NOT ") && strings.HasSuffix(upper, "IS NULL)"), strings.HasSuffix(upper, " IS NOT NULL"):
		return 1, false, true
	case strings.HasSuffix(upper, " IS NULL"):
		return nullShare, false, true
	case strings.Contains(c, "!=") || strings.Contains(c, "<>") || strings.Contains(upper, " NOT IN "):
		return 1, false, true
	}
	if branches := splitTop(c, " OR "); len(branches) > 1 {
		// Its branches' shares, summed: DuckDB takes an OR of the values a
		// store's city or a household's size may have for a fifth, which
		// keep a tenth or a quarter.
		sum := 0.0
		for _, b := range branches {
			f := w.conjunction(filterConjuncts(b))
			if f.deferred {
				f.share *= equalShare
			}
			sum += f.share
		}
		return min(sum, 1), false, true
	}
	if rangeCompare.MatchString(c) {
		return 0, false, false
	}
	if m := inList.FindStringSubmatch(c); m != nil {
		share, known := w.valuesShare(strings.ToLower(m[1]), float64(len(splitTop(m[2], ", "))))
		return share, true, known
	}
	if m := equalCompare.FindStringSubmatch(c); m != nil && !strings.ContainsAny(m[2], "(") {
		share, known := w.valuesShare(strings.ToLower(m[1]), 1)
		return share, true, known
	}
	return unknownShare, false, true
}

// valuesShare is what equality of column col with one of k constants
// keeps: k over its distinct count, where known, or else over the values
// its bounds allow, for a discrete column (a year, a month, a date), which
// has no more. Its count is the filter's input's where that has it: a
// scan of dates DuckDB filtered to three years for the filter above it,
// one year and two months of the others, keeps that filter a third of its
// rows, not a two-hundredth of them.
func (w *setWalker) valuesShare(col string, k float64) (float64, bool) {
	for _, c := range w.filterIn {
		if c.name == col && c.distinct > 0 {
			return min(k/c.distinct, 1), true
		}
	}
	if d := w.st.distinct[col]; d > 0 {
		return min(k/d, 1), true
	}
	if b, ok := w.st.bounds[col]; ok && b[1] > b[0] && w.discrete(col) {
		return min(k/w.spanOf(col, b), 1), true
	}
	return 0, false
}

// constant is value, a constant compared to col, in the units of col's
// bounds: a number as it is, a date as days since 1970, a timestamp as
// microseconds.
func (w *setWalker) constant(col, value string) (float64, bool) {
	if f, err := strconv.ParseFloat(strings.Trim(value, "'"), 64); err == nil {
		return f, true
	}
	m := dateLiteral.FindStringSubmatch(value)
	if m == nil {
		return 0, false
	}
	layout, text := "2006-01-02", m[1]
	if m[2] != "" {
		layout, text = "2006-01-02 15:04:05", m[1]+" "+m[2]
	}
	at, err := time.Parse(layout, text)
	if err != nil {
		return 0, false
	}
	switch {
	case w.st.timestamp[col]:
		return float64(at.UnixMicro()), true
	case w.st.date[col]:
		return math.Floor(float64(at.Unix()) / 86400), true
	}
	return 0, false
}

// splitTop splits e at sep where sep is outside parentheses and quotes.
func splitTop(e, sep string) []string {
	var out []string
	depth, start, quote := 0, 0, false
	for i := 0; i < len(e); i++ {
		switch c := e[i]; {
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(e[i:], sep):
			out = append(out, e[start:i])
			start = i + len(sep)
		}
	}
	return append(out, e[start:])
}

// unnestFunction says a table in-out function is UNNEST, as DuckDB plans
// the UNNEST of a LATERAL join.
func unnestFunction(n *planNode) bool {
	return strings.EqualFold(strings.TrimSpace(strings.Join(n.strs("Name"), "")), "unnest")
}

// findUnnest is the first UNNEST in n's subtree, if any.
func findUnnest(n *planNode) *planNode {
	name := strings.ToUpper(n.Name)
	if name == "UNNEST" || name == "INOUT_FUNCTION" && unnestFunction(n) {
		return n
	}
	for _, c := range n.Children {
		if u := findUnnest(c); u != nil {
			return u
		}
	}
	return nil
}

// listValues is how many rows an UNNEST, n, makes of a row: as many as the
// list it unnests has, where the statement writes it (see writtenValues);
// within a delim join's subquery that takes its rows back by delimCols of
// its input's columns, as a FLATTEN of a row's object of as many columns
// does, as many as those (see delimScope); unnestValues otherwise. A
// report's four FLATTENs of people's objects made 6, 8.2, 20 and 19.2 rows of
// each, and took 6, 12, 20 and 26 of their columns back.
func listValues(n *planNode, delimCols int) float64 {
	if k, ok := writtenValues(n); ok {
		return k
	}
	if delimCols > 0 {
		return float64(delimCols)
	}
	return unnestValues
}

// writtenValues is how many values the list an UNNEST, n, unnests has where
// the statement writes it, as a report's FLATTEN of its row's own values
// does (list_value(a, b, c) makes three); ok is false otherwise.
func writtenValues(n *planNode) (values float64, ok bool) {
	var exprs []string
	for _, c := range n.Children {
		exprs = append(exprs, c.strs("Projections")...)
	}
	exprs = append(exprs, n.strs("Projections")...)
	for _, e := range exprs {
		if k := listArgs(e); k > 0 {
			return float64(k), true
		}
	}
	return 0, false
}

// listArgs is how many values a list_value(...) expression at the start of
// e lists, zero if e is not one.
func listArgs(e string) int {
	e = strings.TrimSpace(e)
	const head = "list_value("
	if !strings.HasPrefix(strings.ToLower(e), head) {
		return 0
	}
	depth, args, quote := 0, 1, byte(0)
	for i := len(head); i < len(e); i++ {
		c := e[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(' || c == '[' || c == '{':
			depth++
		case c == ')' || c == ']' || c == '}':
			if depth == 0 {
				return args
			}
			depth--
		case c == ',' && depth == 0:
			args++
		}
	}
	return 0
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
	found := w.tablesOf(cols)
	if len(found) != 1 {
		return nil
	}
	return found[0]
}

// tablesOf are the statement's tables that have every column of cols, by
// name.
func (w *setWalker) tablesOf(cols []column) []*Table {
	var found []*Table
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
			found = append(found, t)
		}
	}
	return found
}

// identifier matches the names a scan's filters compare.
var identifier = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)

// scanTable is the table a scan reads, by the columns it projects and those
// its filters compare: a dashboard's campaigns and impressions both have
// campaign_id, which is all a scan of either for a join may project, and
// only the campaigns' scan filters by advertiser.
func (w *setWalker) scanTable(cols []column, filters []string) *Table {
	found := w.tablesOf(cols)
	if len(found) <= 1 {
		return w.tableOf(cols)
	}
	all := slices.Clone(cols)
	for _, f := range filters {
		f = literal.ReplaceAllString(f, "")
		for _, name := range identifier.FindAllString(f, -1) {
			if _, ok := w.st.width[strings.ToLower(name)]; ok {
				all = append(all, column{name: strings.ToLower(name)})
			}
		}
	}
	return w.tableOf(all)
}

// distinctArg matches an aggregate over DISTINCT values of a column.
var distinctArg = regexp.MustCompile(`(?i)\(\s*DISTINCT\s+([^),]+)`)

// distinct records the pairs of groups and values that aggregates over
// DISTINCT values hold: DuckDB groups them as it groups rows (see
// groupEntries), keeping about every row of their input where the pairs are
// many (16 million rows of 200,000 strings held 715 MB) and next to nothing
// where they are few (a count of 1,000 values in 10 groups of 100 million
// rows held 125 MB); where how many is not known, every row.
func (w *setWalker) distinct(op string, aggs []string, in, groups []column, inRows float64) {
	for _, a := range aggs {
		m := distinctArg.FindStringSubmatch(a)
		if m == nil {
			continue
		}
		c := w.columns([]string{strings.TrimSpace(m[1])}, in)[0]
		entries := inRows
		if pairs := w.distinctCount(append(slices.Clone(groups), c)); pairs > 0 {
			entries = groupEntries(inRows, pairs, float64(w.st.threads))
		}
		w.hold(op+" DISTINCT", "distinct", entries, width(groups)+c.width+hashEntryBytes)
	}
}

// listAgg matches an aggregate that keeps every value of its input, in a
// list or a string, and its argument.
var listAgg = regexp.MustCompile(`(?i)^\s*(list|array_agg|string_agg|group_concat|listagg|histogram)\(\s*([^,)]+)`)

// listEntryBytes is what a value an aggregate keeps in a list takes beside
// itself: its list's segments and their headers. Lists of a hundred
// million values in ten million groups held 10.6 GB.
const listEntryBytes = 64

// lists records the values aggregates that keep them hold: every row of
// their input.
func (w *setWalker) lists(op string, aggs []string, in []column, inRows float64) {
	for _, a := range aggs {
		m := listAgg.FindStringSubmatch(a)
		if m == nil {
			continue
		}
		c := w.columns([]string{strings.TrimSpace(m[2])}, in)[0]
		w.hold(op+" LIST", "list", inRows, c.width+listEntryBytes)
	}
}

// distinctCount is how many distinct combinations of values cols have, at
// most: the product of their distinct counts, as statistics or groupings by
// each before (see learned) give them, or what their bounds allow; zero if
// one's is not known.
func (w *setWalker) distinctCount(cols []column) float64 {
	if len(cols) == 0 {
		return 0
	}
	p := 1.0
	for _, c := range cols {
		d := c.distinct
		if d <= 0 {
			d = w.st.learned.distinctOf(groupingKey([]column{c}))
		}
		if d <= 0 || math.IsInf(d, 1) {
			d = c.span
		}
		if d <= 0 {
			return 0
		}
		p *= d
		if p > 1e18 {
			return math.Inf(1)
		}
	}
	return p
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
			out[i] = column{name: name, width: wd, distinct: w.st.distinct[name], table: w.st.owner[name], span: w.st.span[name]}
			continue
		}
		out[i] = column{width: planExprWidth, span: w.timeSpan(e)}
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

// timeUnits are how many microseconds a date_trunc part spans, and
// timeField matches date_trunc of a column, and a cast of one to a date.
var (
	timeUnits = map[string]float64{"second": 1e6, "minute": 60e6, "hour": 3600e6, "day": 86400e6, "week": 7 * 86400e6,
		"month": 28 * 86400e6, "quarter": 90 * 86400e6, "year": 365 * 86400e6}
	timeTrunc = regexp.MustCompile(`(?i)^date_trunc\('(\w+)',\s*"?(\w+)"?\)$`)
	timeDate  = regexp.MustCompile(`(?i)^CAST\("?(\w+)"? AS DATE\)$`)
	timePart  = regexp.MustCompile(`(?i)^(hour|dayofweek|isodow|dayofmonth|day|month|quarter)\("?(\w+)"?\)$`)
)

// timeSpan is how many values an expression that truncates or takes a part
// of a timestamp column with bounds has, as a dashboard groups its
// impressions by day: zero if e is not one or the bounds are not known.
func (w *setWalker) timeSpan(e string) float64 {
	parts := map[string]float64{"hour": 24, "dayofweek": 7, "isodow": 7, "dayofmonth": 31, "day": 31, "month": 12, "quarter": 4}
	if m := timePart.FindStringSubmatch(e); m != nil {
		return parts[strings.ToLower(m[1])]
	}
	unit, col := 0.0, ""
	if m := timeTrunc.FindStringSubmatch(e); m != nil {
		unit, col = timeUnits[strings.ToLower(m[1])], strings.ToLower(m[2])
	} else if m := timeDate.FindStringSubmatch(e); m != nil {
		unit, col = timeUnits["day"], strings.ToLower(m[1])
	}
	span := w.st.span[col]
	if unit <= 0 || span <= 0 {
		return 0
	}
	if !w.st.timestamp[col] {
		// A date's bounds are days.
		unit /= 86400e6
	}
	return math.Ceil(span/max(unit, 1)) + 1
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
