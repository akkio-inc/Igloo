package workload

import (
	"slices"
	"sync"
	"time"
)

// What statements cost, from their runs. A statement that takes a second alone
// has no reason to wait for one that takes a minute: in arrival order, a
// dashboard's short queries at ten times the data waited behind its heavy ones
// for most of their time. The controller learns the CPU time of each shape's
// runs, the least of its latest ones, since a run beside others takes more than
// it would alone, and uses it twice: a statement waits in line by when it would
// end if it started when it came and ran alone, not by when it came (see
// Limits.Expect), and a shape that takes next to nothing does not wait in line
// at all (see Light). A heavy statement cannot starve: one that arrives after it
// goes first only if it would end first. Over the dashboard's 96 queries the
// slowest tenth took 94 s rather than 102, and they ran 2,800 s in all rather
// than 3,370.

// costRuns is how many of a shape's latest runs Expect and Light look at.
const costRuns = 8

// LightCPU is the CPU time under which a run makes its shape light.
const LightCPU = time.Second

// costs are the CPU times of each shape's latest runs.
type costs struct {
	mu    sync.Mutex
	cores int
	below time.Duration
	runs  map[string][]time.Duration
}

// WeighCosts makes the controller order its line by what statements cost on
// cores cores (see Expect), and treats those of a shape that took less than
// light of CPU time, in one of its latest runs, as light (see Light). Zero
// cores turn the order off and zero light turns the second off. It is set
// once, before statements run.
func (c *Controller) WeighCosts(cores int, light time.Duration) {
	c.costs.cores, c.costs.below = max(cores, 0), max(light, 0)
}

// ObserveCost records the CPU time, over all of DuckDB's threads, that a run
// of a statement of shape took on this replica; the server seeds them from
// the query history when it starts.
func (c *Controller) ObserveCost(shape string, cpu time.Duration) {
	if c == nil || shape == "" || cpu < 0 {
		return
	}
	k := &c.costs
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.runs == nil || len(k.runs) >= maxFootprints {
		k.runs = make(map[string][]time.Duration)
	}
	runs := append(k.runs[shape], cpu)
	if len(runs) > costRuns {
		runs = runs[len(runs)-costRuns:]
	}
	k.runs[shape] = runs
}

// least is the least CPU time of shape's latest runs; ok is false for a shape
// not seen yet.
func (k *costs) least(shape string) (cpu time.Duration, ok bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	runs := k.runs[shape]
	if len(runs) == 0 {
		return 0, false
	}
	return slices.Min(runs), true
}

// Expect is how long a statement of shape takes alone, as its latest runs
// say: their least CPU time over the replica's cores. It is zero for a shape
// not seen yet, which then waits in arrival order.
func (c *Controller) Expect(shape string) time.Duration {
	if c == nil || shape == "" || c.costs.cores == 0 {
		return 0
	}
	cpu, ok := c.costs.least(shape)
	if !ok {
		return 0
	}
	return cpu / time.Duration(c.costs.cores)
}

// Light reports whether statements of shape are light: one of the shape's
// latest runs took less than the CPU time WeighCosts gave. A light statement
// neither counts against the replica's limit nor reserves memory, since it
// holds little for little time, and starts at once. A shape not seen yet is
// not light.
func (c *Controller) Light(shape string) bool {
	if c == nil || shape == "" || c.costs.below == 0 {
		return false
	}
	cpu, ok := c.costs.least(shape)
	return ok && cpu < c.costs.below
}
