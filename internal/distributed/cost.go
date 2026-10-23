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
// The rates are rough, from the first scale runs, and per core, since
// DuckDB runs several threads a core on object storage: the coordinator
// scans its warm cache at about 150 MB/s a core; a machine reads object
// storage cold at about 16 MB/s a core at four threads a core, the
// workers' default, and less at fewer (coldRateAt); and each level of
// stages, a round of tasks that waits for the round before it, costs about
// 4 s of writing and reading object storage and of waiting for its slowest
// task. At 1x on four workers, reports that took 6 to 56 s on one machine
// took 37 to 176 s on workers. The coordinator reads warm only what its
// read cache can hold: at 100x, a report's tables are several terabytes
// against a cache of under one.
const (
	localRate = 150e6
	coldRate  = 16e6
	levelCost = 4.0
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
	// Workers must be estimated this much faster than one machine.
	speedup = 1.5
	// spillFactor bounds how much longer one machine short of memory is
	// estimated to take. The working set is guessed from the sizes of the
	// tables its stages read, whole, where a report reads a few columns of
	// some and a filtered audience of others: two reports that ran in 15 s
	// on one machine at 10x were guessed at 7 times its memory.
	spillFactor = 2
	// workerThreadsPerCore are the threads a core coldRate is measured at
	// (see engine's thread defaults).
	workerThreadsPerCore = 4
	// exploreEvery is how seldom auto mode runs a statement where it has
	// not run, against the estimates, to learn how long it takes there
	// (see choose); exploreUnder is how short its runs where it has must
	// be for that to be worth a try.
	exploreEvery = 24 * time.Hour
	exploreUnder = 10 * time.Minute
)

// estimate is how long a planned statement would take on one machine and
// on workers.
type estimate struct {
	// bytes are what its stages read of tables; exchanged what the stages
	// that write exchanges read of them; levels its stages' longest chain.
	bytes, exchanged int64
	levels           int
	// oneMachine is the coordinator's time, onWorkers that of workers,
	// as many as workers says, in seconds.
	oneMachine, onWorkers float64
	workers               int
	// memory says the working set is over memoryShare of the coordinator's
	// memory.
	memory bool
}

// coldRateAt is how fast a core reads object storage cold at
// threadsPerCore threads a core. A machine of 8 cores read through its read
// cache at 56 MB/s on 8 threads, 91 on 16 and 131 on 32: 7, 11.4 and
// 16.4 MB/s a core, about as the 0.6th power of the threads.
func coldRateAt(threadsPerCore float64) float64 {
	return coldRate * math.Min(1, math.Pow(math.Max(threadsPerCore, 0.25)/workerThreadsPerCore, 0.6))
}

// machine is what the coordinator knows of itself for an estimate: its
// cores and DuckDB threads, its memory limit and what its read cache may
// hold, zero for unknown or none.
type machine struct {
	cores, threads int
	memory, cache  int64
}

// fleet is what the coordinator knows of its workers for an estimate.
type fleet struct {
	// cores are a worker's cores; running how many workers run; most how
	// many may run; start how long starting more takes.
	cores, running, most int
	start                time.Duration
}

// estimateTimes estimates a plan's times on the coordinator, m, and on
// workers. The coordinator reads warm what its cache can hold, and the
// rest cold at its threads a core; short of memory, it takes longer by
// as much as its working set is over its share of memory, spilling, up to
// spillFactor times.
func estimateTimes(stages []*Stage, m machine, f fleet) estimate {
	var e estimate
	// What the stages read of each table, and what those that write
	// exchanges read: the columns their fragments refer to.
	tables := map[string]*Table{}
	read, exchanged := map[string]columnSet{}, map[string]columnSet{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table == nil {
				continue
			}
			k := in.Table.key()
			tables[k] = in.Table
			cols := columnsRead(s.fragment, in.Table)
			read[k] = union(read, k, cols)
			if s.Out != nil {
				exchanged[k] = union(exchanged, k, cols)
			}
		}
	}
	for k, cols := range read {
		e.bytes += cols.bytes(tables[k])
	}
	for k, cols := range exchanged {
		e.exchanged += cols.bytes(tables[k])
	}
	e.levels = levels(stages)
	cores := float64(max(m.cores, 1))
	warm := min(e.bytes, max(m.cache, 0))
	threadsPerCore := float64(max(m.threads, m.cores)) / cores
	e.oneMachine = float64(warm)/(cores*localRate) + float64(e.bytes-warm)/(cores*coldRateAt(threadsPerCore))
	if set := float64(workingSetFactor * e.exchanged); m.memory > 0 && set > memoryShare*float64(m.memory) {
		e.memory = true
		e.oneMachine *= min(set/(memoryShare*float64(m.memory)), spillFactor)
	}
	// The fewest workers within a fifth of the fastest time.
	e.onWorkers = math.Inf(1)
	times := make([]float64, f.most+1)
	best := math.Inf(1)
	for n := 1; n <= f.most; n++ {
		t := float64(e.levels)*levelCost + float64(e.bytes)/(float64(n*max(f.cores, 1))*coldRate)
		if n > f.running {
			t += f.start.Seconds()
		}
		times[n] = t
		best = min(best, t)
	}
	for n := 1; n <= f.most; n++ {
		if times[n] <= best*1.2 {
			e.onWorkers, e.workers = times[n], n
			break
		}
	}
	return e
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
// within threshold on workers.
func choose(e estimate, r runs, found bool, threshold time.Duration, now time.Time) (workers, explore bool, reason string) {
	limit := threshold.Seconds()
	worth := func(oneMachine float64) bool {
		return e.workers > 0 && (oneMachine > limit || e.memory) && e.onWorkers < oneMachine/speedup
	}
	// Measured on one side only, a statement that took under exploreUnder
	// there is tried once a day on the other, whatever the estimates say,
	// so that the history comes to compare runs with runs: an estimate
	// that is wrong would otherwise keep it on the slower side for good,
	// as a run on workers that beat an estimate of one machine far too
	// high would.
	mayExplore := now.Sub(r.explored) >= exploreEvery
	switch {
	case found && r.outOfMemory && e.workers > 0:
		return true, false, "it ran out of memory on one machine"
	case found && r.local > 0 && r.dist > 0:
		if r.dist < r.local {
			return true, false, fmt.Sprintf("it ran in %.0f s on workers and %.0f s on one machine", r.dist, r.local)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine and %.0f s on workers", r.local, r.dist)
	case found && r.local > 0:
		switch {
		case r.local < limit:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, under %.0f s", r.local, limit)
		case e.workers > 0 && mayExplore && r.local < exploreUnder.Seconds():
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers once, estimated at %.0f s", r.local, e.onWorkers)
		case r.local < e.oneMachine/2:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, well under the %.0f s estimated", r.local, e.oneMachine)
		case worth(r.local):
			return true, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
		case e.workers > 0 && mayExplore:
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers, estimated at %.0f s", r.local, e.onWorkers)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
	case found && r.dist > 0:
		switch {
		case mayExplore && r.dist < exploreUnder.Seconds():
			return false, true, fmt.Sprintf("it ran in %.0f s on workers; trying one machine once, estimated at %.0f s", r.dist, e.oneMachine)
		case r.dist <= e.onWorkers/2 || r.dist < e.oneMachine/speedup:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine)
	case worth(e.oneMachine):
		return true, false, fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers)
	}
	return false, false, fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers)
}

// planKey identifies a statement in the history of its runs: its syntax
// tree without positions and without its literals other than integers,
// and the sizes of the tables its stages read, each to a factor of two. A
// BI tool's statement run again for another day is the same statement;
// one over tables that have grown fourfold is not.
func planKey(stmt node, stages []*Stage) string {
	shape := rewrite(clone(stmt), func(n node) (node, bool) {
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
