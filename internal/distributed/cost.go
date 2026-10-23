package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
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
// scans its warm cache at about 150 MB/s a core; a worker reads object
// storage cold at about 16 MB/s a core, at four threads a core (a machine
// of 8 cores read 131 MB/s through the read cache on 32 threads); and each
// level of stages, a round of tasks that waits for the round before it,
// costs about 4 s of writing and reading object storage and of waiting for
// its slowest task. At 1x on four workers, reports that took 6 to 56 s on
// one machine took 37 to 176 s on workers.
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
	// exploreEvery is how seldom auto mode runs a statement on workers
	// against the estimates, to learn how long it takes there (see choose).
	exploreEvery = 24 * time.Hour
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

// fleet is what the coordinator knows of its workers for an estimate.
type fleet struct {
	// cores are a worker's cores; running how many workers run; most how
	// many may run; start how long starting more takes.
	cores, running, most int
	start                time.Duration
}

// estimateTimes estimates a plan's times on a coordinator of cores cores
// and memory bytes of memory, and on workers.
func estimateTimes(stages []*Stage, cores int, memory int64, f fleet) estimate {
	var e estimate
	read, exchanged := map[string]int64{}, map[string]int64{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table == nil {
				continue
			}
			read[in.Table.key()] = in.Table.Bytes
			if s.Out != nil {
				exchanged[in.Table.key()] = in.Table.Bytes
			}
		}
	}
	for _, b := range read {
		e.bytes += b
	}
	for _, b := range exchanged {
		e.exchanged += b
	}
	e.levels = levels(stages)
	e.oneMachine = float64(e.bytes) / (float64(max(cores, 1)) * localRate)
	e.memory = memory > 0 && float64(workingSetFactor*e.exchanged) > memoryShare*float64(memory)
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
// before (r, if found), and says why. It may run a statement on workers
// against the estimates (explore), at most once in exploreEvery, to learn
// how long it takes there, but never one that ran on one machine within
// threshold.
func choose(e estimate, r runs, found bool, threshold time.Duration, now time.Time) (workers, explore bool, reason string) {
	limit := threshold.Seconds()
	worth := func(oneMachine float64) bool {
		return e.workers > 0 && (oneMachine > limit || e.memory) && e.onWorkers < oneMachine/speedup
	}
	switch {
	case found && r.local > 0 && r.dist > 0:
		if r.dist < r.local {
			return true, false, fmt.Sprintf("it ran in %.0f s on workers and %.0f s on one machine", r.dist, r.local)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine and %.0f s on workers", r.local, r.dist)
	case found && r.local > 0:
		switch {
		case r.local < limit:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, under %.0f s", r.local, limit)
		case r.local < e.oneMachine/2:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, well under the %.0f s estimated", r.local, e.oneMachine)
		case worth(r.local):
			return true, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
		case e.workers > 0 && now.Sub(r.explored) >= exploreEvery:
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers, estimated at %.0f s", r.local, e.onWorkers)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
	case found && r.dist > 0:
		if r.dist <= e.onWorkers/2 || r.dist < e.oneMachine/speedup {
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
