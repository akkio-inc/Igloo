package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"time"
)

// In auto mode a statement runs on workers only when that is faster than
// one machine: workers that start a statement only to finish it later
// than the coordinator would have are a loss whatever they cost. Igloo
// estimates both times from what the statement's parts read, and corrects
// the estimates with what the same statement took before (see history).
//
// The rates are rough, from the scale runs, and per core, since DuckDB
// runs several threads a core on object storage: a machine reads object
// storage cold at about 16 MB/s a core at four threads a core, and less at
// fewer (coldRateAt), as DuckDB's threads are capped at 2 GiB of its
// memory each (a worker of 8 cores and 24.6 GiB runs 12); tables a stage
// filters selectively count a hundredth of their bytes and rows; and a
// statement on workers pays for its start and for each stage (startCost,
// stageCost). Against the scale environment's reports at 1x, 10x and
// 100x on a coordinator of 16 cores and 16 workers of 8, the estimates pick
// the side that was faster by half again, or either where the two were
// closer (see TestScaleReports), but for statements whose rows cost far
// more than their count says: unnesting FLATTEN's arrays (a report that
// took 251 s on one machine at 10x, estimated at 7 s), and joins through a
// view's computed keys counted distinct (691 s on one machine and 170 s on
// workers, estimated at 160 s and 153 s). Those run on one machine first,
// and their history sends them to workers after (see choose). How many
// workers, or the large worker, is weighed in fleet.go.
const (
	coldRate = 16e6
	// startCost and stageCost are what a statement on workers takes beside
	// reading: its own start, and each stage's round of tasks, which write
	// their exchanges' partitions to object storage, the next stage listing
	// and reading them. At ten times the data on 16 workers, reports of 2
	// or 3 stages over a few gigabytes took 12 to 40 s, and of 23 to 26
	// stages 153 to 183 s.
	startCost = 8.0
	stageCost = 6.0
	// DefaultThreshold is how long a statement must take on one machine,
	// by its estimate or its last run there, before auto mode runs it on
	// workers, unless it would run short of memory: below it, the
	// estimates are too rough to bet on.
	DefaultThreshold = time.Minute
	// A statement runs short of memory on the coordinator when its working
	// set, workingSetFactor times what its stages that write exchanges
	// read, is over memoryShare of the coordinator's memory limit: the
	// joins and distinct counts over people that such stages feed hold
	// several times their tables' compressed bytes.
	workingSetFactor = 3
	memoryShare      = 0.7
	// heldFactor is how many times the compressed bytes its exchanges'
	// stages read, as selective filters leave them, auto mode takes a
	// statement's working set on one machine to be. At ten times the data,
	// reports whose stages read 12 GB so took 621 to 691 s on one machine
	// of 49 GiB, counting people distinct, and 172 s for one of 14 GB; their
	// workers took 84 to 188 s.
	heldFactor = 6
	// Workers must be estimated this much faster than one machine.
	speedup = 1.5
	// rowRate is how many rows a core goes through a second for each pass
	// over them (see rowWork). It is the fastest of the scale environment's
	// reports on one machine of 16 cores: two that went through 1.1 and
	// 1.35e10 such rows at 1x took 3 to 6 s, as filters the estimate does
	// not see cut their rows; one that joins a 400-million-row table to
	// itself 16 times went through 6.8e10 at 10x in 1,073 s. So the rows
	// set a floor under one machine's time, which only plans with far more
	// rows than bytes rise above.
	rowRate = 1.5e8
	// filterSelectivity is the share of a table's rows a filter on one of
	// its columns by a constant is taken to keep (see rowWork).
	filterSelectivity = 0.01
	// workerThreadsPerCore are the threads a core coldRate is measured at
	// (see engine's thread defaults).
	workerThreadsPerCore = 4
	// exploreEvery is how seldom auto mode runs a statement where it has
	// not run, against the estimates, to learn how long it takes there
	// (see choose); exploreUnder is how short its runs where it has must
	// be for that to be worth a try.
	exploreEvery = 24 * time.Hour
	exploreUnder = 10 * time.Minute
	// exploreWithin is how many times what a statement took on one side
	// the other side's estimate may be for that side to be worth a try:
	// at ten times the data, a report that took 57 s on workers was tried
	// on one machine, estimated at 176 s and short of memory, and took 251
	// s and spilled 8.4 GB, where the runs exploring fixed had been
	// estimated close (160 s against 153), or had taken several times
	// their own estimate (a one machine's 7 s that took longer than the
	// workers' 51).
	exploreWithin = 2
	// spillWithin is how many times one machine's estimate workers may be
	// estimated at and still run a statement that one machine should spill:
	// its estimate is then a floor, which ran four to thirty-five times
	// short at ten times the data (a report estimated at 56 s took 220 s
	// on one machine and 66 s on workers). Two estimated to spill that ran
	// five times as fast on one machine had workers estimated at 1.7 and 2
	// times its estimate.
	spillWithin = 1.5
	// slowOnWorkers is how many times one machine's estimate a statement
	// that ran only on workers must have taken there for auto mode to try
	// one machine once, though one machine should run short of memory: an
	// estimate short of memory ran four to five times short where the
	// statement spilled (two reports estimated at 157 s took 809 s and 610
	// s on one machine), so ten times it is past what spilling explains.
	// A dashboard's statements at ten times the data, estimated at 14 s on
	// one machine and short of memory, took 1,312 s split over two workers
	// that shared the replica's machine, and stayed on workers after it
	// from the query history, through a burst on workers of their own;
	// the replica alone ran them in 25 s at the median, 96 at once.
	slowOnWorkers = 10.0
)

// estimate is how long a planned statement would take on one machine and
// on workers.
type estimate struct {
	// bytes are what its stages read of tables, as selective filters leave
	// them; exchanged what the stages that write exchanges read of them, in
	// full; levels its stages' longest chain, and stages how many there are;
	// rows the rows its stages go through (see rowWork).
	bytes, exchanged int64
	levels, stages   int
	rows             float64
	// oneMachine is the coordinator's time, onWorkers that of workers,
	// as many as workers says, or of the large worker if large, in seconds,
	// by the estimates (see pick for the choice, which what the statement
	// took before corrects).
	oneMachine, onWorkers float64
	workers               int
	large                 bool
	// memory says the working set is over memoryShare of the coordinator's
	// memory, by workingSetFactor, and spills that it is by heldFactor, as
	// local says, whose oom says it should run out of memory there.
	memory, spills bool
	local          spill
	// options are the ways on workers weighed, dollars says their costs are
	// in dollars, and fleet why the one in workers and large was chosen.
	options []option
	dollars bool
	fleet   string
}

// coldRateAt is how fast a core reads object storage cold at
// threadsPerCore threads a core. A machine of 8 cores read through its read
// cache at 56 MB/s on 8 threads, 91 on 16 and 131 on 32: 7, 11.4 and
// 16.4 MB/s a core, about as the 0.6th power of the threads.
func coldRateAt(threadsPerCore float64) float64 {
	return coldRate * math.Min(1, math.Pow(math.Max(threadsPerCore, 0.25)/workerThreadsPerCore, 0.6))
}

// machine is what the coordinator knows of itself for an estimate: its
// cores and DuckDB threads and its memory limit, zero for unknown.
type machine struct {
	cores, threads int
	memory         int64
	// ranHere says a statement of the same shape as the one estimated ran
	// to its end on this machine, allocating allocated bytes over its run
	// (see Footprints).
	ranHere   bool
	allocated int64
}

// Footprints is an Env whose session knows what statements of the shape
// of the one being planned did on this replica: Footprint returns what the
// latest such statement allocated over its run, which it completed here,
// and ok is false when none has. One machine is then not short of memory
// for the statement, whatever its estimate says, and spills only if what
// it allocated, all of its run's allocations, is over its share: a
// dashboard's statements at ten times the data, estimated short of memory,
// went to workers and took 250 s there, where the replica alone ran them
// in 20 s at the median, 96 at once.
type Footprints interface {
	Footprint() (allocated int64, ok bool)
}

// fleet is what the coordinator knows of its workers for an estimate.
type fleet struct {
	// cores are a worker's cores, and threads its DuckDB threads, zero for
	// unknown; running how many workers run; most how many may run; start
	// how long starting more takes, and tail how long workers started stay
	// up idle after.
	cores, threads, running, most int
	start, tail                   time.Duration
	// memory is the smallest worker's memory limit, and price what a
	// worker costs an hour, zero for unknown.
	memory int64
	price  float64
	// large is the compute pool's large worker, if it has one the
	// statement may run on.
	large *largeWorker
}

// estimateTimes estimates a plan's times on the coordinator, m, and on
// workers, f, from what its stages read and the rows they go through.
func estimateTimes(stages []*Stage, m machine, f fleet) estimate {
	return planFeatures(stages).estimate(m, f)
}

// features are what a plan's estimates are made of.
type features struct {
	// bytes are what its stages read of tables, as selective filters leave
	// them (see filtersSelectively); exchanged what the stages that write
	// exchanges read of tables in full, and held as selective filters leave
	// it.
	bytes, exchanged int64
	held             float64
	// rows are the rows its stages go through (see rowWork); levels its
	// stages' longest chain, and stages how many there are.
	rows           float64
	levels, stages int
}

// set is the working set of a plan of features x on one machine:
// heldFactor times what its exchanges' stages read, as selective filters
// leave it.
func (x features) set() float64 { return heldFactor * x.held }

// planFeatures measures a plan for its estimates.
func planFeatures(stages []*Stage) features {
	var x features
	// What the stages read of each table, and what those that write
	// exchanges read: the columns their fragments refer to. A table every
	// stage that reads it filters selectively counts filterSelectivity of
	// those bytes.
	tables := map[string]*Table{}
	read, exchanged := map[string]columnSet{}, map[string]columnSet{}
	selective := map[string]bool{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table == nil {
				continue
			}
			k := in.Table.key()
			if _, seen := tables[k]; !seen {
				selective[k] = true
			}
			tables[k] = in.Table
			selective[k] = selective[k] && filtersSelectively(s.fragment, in.Table)
			cols := columnsRead(s.fragment, in.Table)
			read[k] = union(read, k, cols)
			if s.Out != nil {
				exchanged[k] = union(exchanged, k, cols)
			}
		}
	}
	for k, cols := range read {
		b := cols.bytes(tables[k])
		if selective[k] {
			b = int64(float64(b) * filterSelectivity)
		}
		x.bytes += b
	}
	for k, cols := range exchanged {
		b := cols.bytes(tables[k])
		x.exchanged += b
		if selective[k] {
			x.held += float64(b) * filterSelectivity
		} else {
			x.held += float64(b)
		}
	}
	x.levels, x.stages = levels(stages), len(stages)
	x.rows = rowWork(stages, filtersSelectively)
	return x
}

// estimate estimates the times of a plan of features x on the coordinator,
// m, and on workers, f.
func (x features) estimate(m machine, f fleet) estimate {
	e := estimate{bytes: x.bytes, exchanged: x.exchanged, levels: x.levels, stages: x.stages, rows: x.rows}
	// Short of memory on the coordinator, which a statement whose workers
	// fail then does not fall back to, by the bytes its exchanges' stages
	// read.
	share := memoryShare * float64(m.memory)
	e.memory = m.memory > 0 && float64(workingSetFactor*x.exchanged) > share && !m.ranHere
	// The ways on workers, and the one auto mode would take without what
	// the statement took before (see plan, which corrects them).
	e.options = x.options(f)
	_, _, e.dollars = f.prices()
	e.pick(runs{}, f)

	// One machine reads cold from object storage at its threads a core: its
	// read cache holds what it read lately, which a statement's first run
	// had seldom read. At ten times the data, reports' first runs took 24
	// to 259 s and their second 3 to 51 s, and an estimate that took the
	// cache for warm put a report of 258 s at 15 s.
	cores := float64(max(m.cores, 1))
	threadsPerCore := float64(max(m.threads, m.cores)) / cores
	e.oneMachine = float64(e.bytes) / (cores * coldRateAt(threadsPerCore))
	// What it reads, or what it computes over the rows, whichever takes
	// longer: a report that reads 2 GB of a table but joins its 400 million
	// rows to themselves 16 times is no 1-second statement.
	e.oneMachine = max(e.oneMachine, e.rows/(cores*rowRate))
	// Short of memory, it takes longer, spilling, as much as its working
	// set is over its share of memory (see spillOn): heldFactor times what
	// its exchanges' stages read as the selective filters leave it, or what
	// a statement of its shape allocated over its run here, which fits if
	// that is within its share.
	set := x.set()
	if m.ranHere {
		set = float64(m.allocated)
	}
	e.local = spillOn(set, m.memory)
	e.spills = e.local.factor > 1
	e.oneMachine *= e.local.factor
	return e
}

// pick chooses the way on workers e's options say, corrected by what the
// statement took before, r (see correct and pick): workers is how many, or
// 1 and large for the large worker, and onWorkers its time by the
// estimates; with no way on workers, workers is zero and onWorkers
// infinite.
func (e *estimate) pick(r runs, f fleet) {
	e.workers, e.large, e.onWorkers, e.fleet = 0, false, math.Inf(1), ""
	if len(e.options) == 0 {
		return
	}
	correct(e.options, r, f)
	best, why, ok := pick(e.options, e.dollars)
	e.fleet = why
	if ok {
		e.workers, e.large, e.onWorkers = best.workers, best.large, best.estimate
	}
}

// rowWork estimates the rows a plan's stages go through: each stage's
// tables' rows and the rows of the exchanges it reads, taken as the rows
// that fed them, once for the scan and once more for each join and for
// grouping in its fragment. A table its fragment filters, as a report
// picks one audience out of every audience's members, counts
// filterSelectivity of its rows.
func rowWork(stages []*Stage, filters func(node, *Table) bool) float64 {
	fed := map[*Exchange]float64{}
	var work float64
	for _, s := range stages {
		var in float64
		for _, i := range s.Inputs {
			switch {
			case i.Table != nil:
				rows := float64(max(i.Table.Rows, 0))
				if filters(s.fragment, i.Table) {
					rows *= filterSelectivity
				}
				in += rows
			case i.Exchange != nil:
				in += fed[i.Exchange]
			}
		}
		work += in * float64(1+operators(s.fragment))
		if s.Out != nil {
			fed[s.Out] += in
		}
	}
	return work
}

// filtersByConstant reports whether fragment compares a column of t to a
// constant, with = or IN, in a WHERE clause.
func filtersByConstant(fragment node, t *Table) bool {
	cols := map[string]bool{}
	for _, c := range t.Columns {
		cols[strings.ToLower(c.Name)] = true
	}
	found := false
	walk(fragment, func(m node) bool {
		where := child(m, "where_clause")
		if found || where == nil {
			return !found
		}
		for _, c := range conjuncts(where) {
			var col, val node
			switch str(c["type"]) {
			case "COMPARE_EQUAL":
				col, val = child(c, "left"), child(c, "right")
				if str(col["class"]) != "COLUMN_REF" {
					col, val = val, col
				}
			case "COMPARE_IN":
				kids := list(c, "children")
				if len(kids) < 2 {
					continue
				}
				col, _ = kids[0].(node)
				val, _ = kids[1].(node)
			default:
				continue
			}
			if str(col["class"]) != "COLUMN_REF" || str(val["class"]) != "CONSTANT" {
				continue
			}
			names := list(col, "column_names")
			if len(names) > 0 && cols[strings.ToLower(str(names[len(names)-1]))] {
				found = true
			}
		}
		return !found
	})
	return found
}

// filtersSelectively reports whether fragment keeps few of t's rows, as
// auto mode's estimates take it: it compares a column of t to a constant
// or to a subquery's value, with = or IN (see selectiveFilter), as reports
// pick one audience, or its latest version, out of a lookup of all of
// them. DuckDB reads only the row groups whose values may match, and an
// audience's members are written together.
func filtersSelectively(fragment node, t *Table) bool {
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
			if !selectiveFilter(c) {
				continue
			}
			var col node
			switch str(c["type"]) {
			case "COMPARE_EQUAL":
				if col = child(c, "left"); str(col["class"]) != "COLUMN_REF" {
					col = child(c, "right")
				}
			case "COMPARE_IN":
				col, _ = list(c, "children")[0].(node)
			default:
				col = child(c, "child")
			}
			names := list(col, "column_names")
			if str(col["class"]) == "COLUMN_REF" && len(names) > 0 && cols[strings.ToLower(str(names[len(names)-1]))] {
				found = true
			}
		}
		return !found
	})
	return found
}

// operators counts the joins in a fragment, and its grouping.
func operators(fragment node) int {
	n := 0
	walk(fragment, func(m node) bool {
		if str(m["type"]) == "JOIN" {
			n++
		}
		if len(list(m, "group_expressions")) > 0 {
			n++
		}
		return true
	})
	return n
}

// columnSet is the columns of a table a plan reads, by lower-case name;
// nil means all of them.
type columnSet map[string]bool

// columnsRead returns the columns of t that a stage's fragment refers to:
// every column if its select list has a star, which it writes out, and
// otherwise those it names anywhere, under any qualifier. A star in a
// subquery, as a view's s.* is, passes columns through for the names
// around it to pick. A stage without a fragment, as in tests, reads all.
func columnsRead(fragment node, t *Table) columnSet {
	if fragment == nil {
		return nil
	}
	for _, item := range list(fragment, "select_list") {
		if n, ok := item.(node); ok && str(n["class"]) == "STAR" {
			return nil
		}
	}
	named := map[string]bool{}
	walk(fragment, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			for _, part := range list(n, "column_names") {
				named[strings.ToLower(str(part))] = true
			}
		}
		return true
	})
	cols := columnSet{}
	for _, c := range t.Columns {
		if name := strings.ToLower(c.Name); named[name] {
			cols[name] = true
		}
	}
	if len(cols) == 0 && len(t.Columns) > 0 {
		// count(*): DuckDB reads a column all the same.
		cols[strings.ToLower(t.Columns[0].Name)] = true
	}
	return cols
}

// union returns sets[k], if any, with cols; nil, as either, is all
// columns.
func union(sets map[string]columnSet, k string, cols columnSet) columnSet {
	prev, ok := sets[k]
	switch {
	case !ok:
		return cols
	case prev == nil || cols == nil:
		return nil
	}
	out := maps.Clone(prev)
	maps.Copy(out, cols)
	return out
}

// bytes estimates what reading the columns s of t takes: t's size times
// the columns' share of its columns, weighed by their types (typeBytes).
func (s columnSet) bytes(t *Table) int64 {
	if s == nil {
		return t.Bytes
	}
	var all, some float64
	for _, c := range t.Columns {
		w := typeBytes(c.Type)
		all += w
		if s[strings.ToLower(c.Name)] {
			some += w
		}
	}
	if all == 0 {
		return t.Bytes
	}
	return int64(float64(t.Bytes) * some / all)
}

// typeBytes guesses how many compressed bytes a value of type typ takes,
// to weigh a table's columns, whose sizes Igloo does not know: a report
// that reads three of the 420 columns of a table of 77 GB reads a few of
// its gigabytes, not all of them.
func typeBytes(typ string) float64 {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch {
	case strings.HasSuffix(t, "]"), strings.HasPrefix(t, "MAP"), strings.HasPrefix(t, "STRUCT"), strings.HasPrefix(t, "UNION"):
		return 32
	case t == "BOOLEAN", t == "TINYINT", t == "UTINYINT":
		return 1
	case t == "SMALLINT", t == "USMALLINT":
		return 2
	case t == "INTEGER", t == "UINTEGER", t == "FLOAT", t == "DATE":
		return 4
	case t == "HUGEINT", t == "UHUGEINT", t == "UUID", t == "INTERVAL", t == "BLOB":
		return 16
	case strings.HasPrefix(t, "DECIMAL"):
		if p, _, ok := strings.Cut(strings.TrimPrefix(t, "DECIMAL("), ","); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 18 {
				return 16
			}
		}
		return 8
	case strings.HasPrefix(t, "VARCHAR"):
		return 16
	}
	return 8
}

// levels is the length of the longest chain of stages each of which reads
// what the one before it wrote: the rounds of tasks a statement waits for.
func levels(stages []*Stage) int {
	depth := map[*Exchange]int{}
	most := 0
	// Stages come after those whose exchanges they read.
	for _, s := range stages {
		d := 1
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				d = max(d, depth[in.Exchange]+1)
			}
		}
		if s.Out != nil {
			depth[s.Out] = max(depth[s.Out], d)
		}
		most = max(most, d)
	}
	return most
}

// choose decides whether a statement whose plan key is key and whose
// estimates are e runs on workers, from e and what the statement took
// before (r, if found), and says why. It may run a statement where it has
// not run, against the estimates (explore), at most once in exploreEvery,
// to learn how long it takes there, but never one that ran on one machine
// within threshold on workers, nor on a side estimated at more than
// exploreWithin times what it took on the other, nor on one machine where
// it should not fit.
func choose(e estimate, r runs, found bool, threshold time.Duration, now time.Time) (workers, explore bool, reason string) {
	limit := threshold.Seconds()
	worth := func(oneMachine float64) bool {
		return e.workers > 0 && (oneMachine > limit || e.spills) && e.onWorkers < oneMachine/speedup
	}
	// Measured on one side only, a statement that took under exploreUnder
	// there is tried once a day on the other, whatever the estimates say,
	// so that the history comes to compare runs with runs: an estimate
	// that is wrong would otherwise keep it on the slower side for good,
	// as a run on workers that beat an estimate of one machine far too
	// high would.
	mayExplore := now.Sub(r.explored) >= exploreEvery
	// Worth trying where its estimate is close to what the statement took
	// on the other side, and, on one machine, where it should fit.
	tryWorkers := func(took float64) bool { return e.onWorkers < exploreWithin*took }
	tryOne := func(took float64) bool {
		return e.oneMachine < exploreWithin*took && !e.memory && !e.spills && !r.heavy
	}
	switch {
	case found && r.outOfMemory && e.workers > 0:
		return true, false, "it ran out of memory on one machine"
	case found && r.guarded && e.workers > 0:
		return true, false, fmt.Sprintf("it ran on one machine for %.0f s, far past its estimate", r.local)
	case found && r.dist > 0 && r.distWorkers > 0 && e.workers >= 2*r.distWorkers && mayExplore:
		// Measured on far fewer workers than it would run on now, as
		// after an earlier session's runs on a smaller fleet: a report
		// that took 230 s on 4 workers took 84 s on 16, but stayed on one
		// machine at 178 s.
		return true, true, fmt.Sprintf("it ran in %.0f s on %d workers; trying %d once, estimated at %.0f s", r.dist, r.distWorkers, e.workers, e.onWorkers)
	case found && r.local > 0 && r.dist > 0:
		if r.dist < r.local {
			return true, false, fmt.Sprintf("it ran in %.0f s on workers and %.0f s on one machine", r.dist, r.local)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine and %.0f s on workers", r.local, r.dist)
	case found && r.local > 0:
		switch {
		case r.local < limit:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, under %.0f s", r.local, limit)
		case e.workers > 0 && mayExplore && r.local < exploreUnder.Seconds() && tryWorkers(r.local):
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers once, estimated at %.0f s", r.local, e.onWorkers)
		case r.local < e.oneMachine/2:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, well under the %.0f s estimated", r.local, e.oneMachine)
		case worth(r.local):
			return true, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
		case e.workers > 0 && mayExplore && tryWorkers(r.local):
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers, estimated at %.0f s", r.local, e.onWorkers)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
	case found && r.dist > 0:
		switch {
		case mayExplore && r.dist < exploreUnder.Seconds() && tryOne(r.dist):
			return false, true, fmt.Sprintf("it ran in %.0f s on workers; trying one machine once, estimated at %.0f s", r.dist, e.oneMachine)
		case mayExplore && !r.heavy && r.dist > slowOnWorkers*e.oneMachine:
			// Far slower on workers than one machine should take it, even
			// short of memory: without this it stayed there for good.
			return false, true, fmt.Sprintf("it ran in %.0f s on workers, over %.0f times one machine's estimate of %.0f s; trying one machine once",
				r.dist, slowOnWorkers, e.oneMachine)
		case e.memory || e.spills:
			// One machine's estimate runs four or five times short where it
			// spills: two reports that took 146 s and 141 s on workers,
			// against one machine's 157 s, took 809 s and 610 s there.
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should run short of memory", r.dist)
		case r.heavy:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, whose exchanges held %.1f GB, more than one machine's memory holds", r.dist, float64(r.exchangeBytes)/1e9)
		case r.dist <= e.onWorkers/2 || r.dist < e.oneMachine/speedup:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine)
	case e.spills && e.workers > 0 && e.onWorkers <= spillWithin*e.oneMachine:
		return true, false, fmt.Sprintf("one machine should take %.0f s or more, spilling, and workers %.0f s", e.oneMachine, e.onWorkers)
	case worth(e.oneMachine):
		return true, false, fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers)
	}
	return false, false, fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers)
}

// planKey identifies a statement in the history of its runs: its syntax
// tree without positions, without its literals other than integers and
// without the snapshots it reads tables at, and the sizes of the tables
// its stages read, each to a factor of two. A BI tool's statement run
// again for another day is the same statement; one over tables that have
// grown fourfold is not.
func planKey(stmt node, stages []*Stage) string {
	shape := rewrite(clone(stmt), func(n node) (node, bool) {
		if at := child(n, "at_clause"); at != nil {
			// The snapshot a pin names moves with every commit, as the
			// statement Igloo writes to answer from bitmap indexes does.
			n["at_clause"] = node{"unit": at["unit"]}
		}
		if str(n["class"]) != "CONSTANT" {
			return nil, false
		}
		if _, ok := intConstant(n); ok {
			// Positions in GROUP BY and ORDER BY, LIMITs: part of the shape.
			return nil, false
		}
		return node{"class": "CONSTANT"}, true
	})
	h := sha256.New()
	h.Write([]byte(canonical(shape)))
	sizes := map[string]int{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table != nil {
				sizes[in.Table.key()] = bits.Len64(uint64(max(in.Table.Bytes, 0)))
			}
		}
	}
	for _, k := range slices.Sorted(maps.Keys(sizes)) {
		fmt.Fprintf(h, "\x00%s\x00%d", k, sizes[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
