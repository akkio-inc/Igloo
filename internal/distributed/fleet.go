package distributed

import (
	"fmt"
	"math"
	"math/bits"
	"slices"
	"strings"
	"time"
)

// Spilling, the size of a statement's fleet, and the large worker. Auto
// mode weighs the ways a statement could run beside one machine: on 1, 2,
// 4, ... of the workers up to all those it may start, and, where a compute
// pool has one, on its large worker alone. Each way's time counts what its
// machines would spill (spillOn), and its cost the machine-seconds it
// bills at their prices: its workers', or every running worker's where
// idle workers do not stop alone, and the replica's, which waits for them
// (see costOn). The trade-off is explicit: the cheapest way whose time is
// within fleetMargin (a fifth) of the fastest way's, so that no way is
// taken that another beats on both time and cost (see pick). The times
// are corrected by what the statement took before on as many workers (see
// correct), and a size estimated near the fastest measured is tried once
// a day to measure it (see exploreFleet). Whether the statement runs on
// that way or on one machine is choose's to decide, as before.

const (
	// spillExponent is how a machine's time grows with its working set
	// over the memory it has for it, spilling: by that ratio to this power.
	// Against the scale runs on a machine of 49.3 GiB, by the working sets
	// they reached (what they held at the memory limit and what they
	// spilled), eleven reports took 1.2 to 2.9 times as long as without
	// spilling at 2 to 10 times the memory; fitted in logarithms, the curve
	// is within a factor of two of all but one, which took 11 times as long
	// at 6.6 (see TestSpillCalibration). The old bound of twice as long
	// held for six of them.
	spillExponent = 0.55
	// oomRatio is the working set, over the memory a machine has for it,
	// past which the machine is taken to run out of memory rather than
	// spill: at a hundred times the data, reports that reached 12 to 23
	// times the memory ran out on one machine of 49.3 GiB, after 13 to 27
	// minutes and up to 820 GB of spill, where one at 10.3 times finished,
	// spilling 330 GB.
	oomRatio = 12
	// slightSpill is how many times the memory a machine has for it a
	// working set may be estimated at and still count as fitting: the
	// estimates from DuckDB's plans ran up to half again high where nothing
	// spilled (a report estimated at 46 GB, 1.24 times the room of a
	// machine of 49.3 GiB, held 33 GB and ran in 98 s without spilling;
	// another estimated at 14 GB held 9.5 GB; over joins, groupings and
	// sorts of 16 million rows, up to a quarter high), and DuckDB spills a
	// slight overflow at little cost, 1.25 times as long by the curve at
	// half again the room. Every scale run that spilled reached twice the
	// room or more (see TestSpillCalibration), and a report estimated at
	// 5.8 times it held about 2.9 times.
	slightSpill = 1.5
	// fleetMargin is how many times the fastest way's time a way may take
	// and still be chosen for costing less. At ten times the data, a report
	// took 112 s on 8 workers and 127 s on 16 (13% more, for twice the
	// cost): 8; another 44 s on 16 and 75 s on 8 (70% more, for 7% less):
	// 16. Picking 4 workers, estimated within a fifth of 16 but measured at
	// 200 s, added 60% to a run to save a few percent, which a margin
	// against estimates alone allowed; measured times stand where there are
	// some (see correct and exploreFleet).
	fleetMargin = 1.2
	// costTie is the share of a way's cost within which another costs the
	// same (see pick).
	costTie = 0.02
	// fleetExploreWithin is how many times the fastest measured way's time
	// a way not measured may be estimated at, as corrected, and still be
	// tried once (see exploreFleet): the estimates run within a factor of
	// 1.35 of the runs at the root mean square (see levelCost).
	fleetExploreWithin = 1.4
	// levelCost is what a level of a statement's stages takes on workers
	// beside their reading: its round of tasks, which wait for the level
	// before; stages of one level run at once. workerReadShare is the share
	// of one machine's cold rate (coldRateAt) a worker's core reads and
	// computes its tasks' share at, as tasks read column chunks in parts
	// and write and read exchanges. Fitted over fifteen runs of seven
	// reports at ten times the data on 1 to 16 workers, the estimates are
	// within a factor of 1.35 at the root mean square of the logarithms,
	// where a cost a stage and one machine's rate were within 2.05, and
	// took 4 workers to be about as fast as 16 for a report that took 200
	// s on 4 and 127 s on 16 (see TestFleetCalibration).
	levelCost       = 5.0
	workerReadShare = 0.4
	// largeStageCost is what a level of stages takes on the large worker,
	// where its exchanges, if any, stay on its own disk: at ten times the
	// data, stages exchanging through workers' disks ran three to eight
	// times as fast as through object storage.
	largeStageCost = 2.0
)

// spill is what a machine that runs a statement, or a worker its share of
// one, holds against the memory it has for it.
type spill struct {
	// set is the working set, and room the memory for it (memoryShare of
	// the machine's limit), in bytes; room is zero when the machine's memory
	// is not known.
	set, room float64
	// factor is how many times as long the machine takes, spilling; oom
	// says it is taken to run out of memory instead.
	factor float64
	oom    bool
}

// spillOn estimates what a machine of memory bytes (its DuckDB limit) does
// with a working set of set bytes: nothing if it fits in memoryShare of the
// memory, by up to slightSpill, or if the memory is not known; otherwise it
// spills, and takes (set/room)^spillExponent times as long, or runs out of
// memory past oomRatio.
func spillOn(set float64, memory int64) spill {
	s := spill{set: set, room: memoryShare * float64(max(memory, 0)), factor: 1}
	if s.room <= 0 || set <= slightSpill*s.room {
		return s
	}
	ratio := set / s.room
	s.factor = math.Pow(ratio, spillExponent)
	s.oom = ratio > oomRatio
	return s
}

// String says what the machine would do, for EXPLAIN DISTRIBUTED.
func (s spill) String() string {
	switch {
	case s.room <= 0:
		return fmt.Sprintf("working set %s, memory not known", gigabytes(int64(s.set)))
	case s.oom:
		return fmt.Sprintf("working set %s, %.0f times the %s it has for it: runs out of memory", gigabytes(int64(s.set)), s.set/s.room, gigabytes(int64(s.room)))
	case s.factor > 1:
		return fmt.Sprintf("working set %s over the %s it has for it: spills about %s, %.1f times as long",
			gigabytes(int64(s.set)), gigabytes(int64(s.room)), gigabytes(int64(s.set-s.room)), s.factor)
	case s.set > s.room:
		return fmt.Sprintf("working set %s within half again the %s it has for it: fits, as such estimates run high", gigabytes(int64(s.set)), gigabytes(int64(s.room)))
	}
	return fmt.Sprintf("working set %s within the %s it has for it", gigabytes(int64(s.set)), gigabytes(int64(s.room)))
}

// largeWorker is what the coordinator knows of a compute pool's large
// worker for an estimate: its cores and DuckDB threads (zero for unknown),
// its memory limit, its price an hour (zero for unknown), whether it runs,
// how long starting it takes and how long it stays up idle after.
type largeWorker struct {
	cores, threads int
	memory         int64
	price          float64
	running        bool
	start, tail    time.Duration
}

// option is a way a statement could run on workers: on workers of the
// fleet, or on the large worker alone.
type option struct {
	workers int
	large   bool
	// estimate is its time by the estimates, and seconds as corrected by
	// what the statement took before (see correct); measured says seconds
	// is what it took on as many workers; failed that it failed there
	// lately, after failedAfter seconds.
	estimate, seconds float64
	measured          bool
	failed            bool
	failedAfter       float64
	// started is how many workers it starts; spill is what each of its
	// workers does with its share of the working set.
	started int
	spill   spill
	// cost is its machine-seconds at their prices, in dollars when prices
	// are known (see fleet.prices).
	cost float64
}

// String names the option, for reasons and EXPLAIN DISTRIBUTED.
func (o option) String() string {
	if o.large {
		return "the large worker"
	}
	if o.workers == 1 {
		return "1 worker"
	}
	return fmt.Sprintf("%d workers", o.workers)
}

// fleetSizes are the numbers of workers auto mode weighs for a statement:
// 1, 2, 4 and on below most, most, and the running workers, which start
// nothing.
func fleetSizes(running, most int) []int {
	var out []int
	for n := 1; n < most; n *= 2 {
		out = append(out, n)
	}
	out = append(out, most)
	if running > 0 && running < most && !slices.Contains(out, running) {
		out = append(out, running)
		slices.Sort(out)
	}
	return out
}

// options estimates the ways a plan of features x could run on f's
// workers: on each of fleetSizes of them, and on the large worker.
func (x features) options(f fleet) []option {
	var out []option
	if f.most > 0 {
		for _, n := range fleetSizes(f.running, f.most) {
			out = append(out, x.onFleet(n, f))
		}
	}
	if f.large != nil {
		out = append(out, x.onLarge(*f.large))
	}
	for i := range out {
		out[i].seconds = out[i].estimate
		out[i].cost = out[i].costOn(f)
	}
	return out
}

// threadsPerCore are a machine's DuckDB threads a core, workerThreadsPerCore
// when not known.
func threadsPerCore(threads, cores int) float64 {
	if threads > 0 && cores > 0 {
		return float64(threads) / float64(cores)
	}
	return workerThreadsPerCore
}

// onFleet estimates the plan's time on n of f's workers: a statement's own
// start, each level of its stages' round of tasks (levelCost), and what
// its stages read, at workerReadShare of one machine's cold rate, or the
// rows they go through, whichever takes longer, on n workers' cores at
// their threads a core; and the start of the workers that do not run.
//
// Workers do not spill as one machine does: their exchanges' partitions
// are sized to their memory, a task runs a few of them at once, and a task
// that runs out runs again over more of them. At a hundred times the data,
// 16 workers of 24.6 GiB ran reports whose stages fed 19 to 22 times one
// machine's memory in 790 to 1,025 s, at their memory limits, without a
// task running out. So a worker's share of the working set does not
// lengthen its time.
func (x features) onFleet(n int, f fleet) option {
	o := option{workers: n, spill: spillOn(x.set()/float64(n), f.memory)}
	o.spill.factor, o.spill.oom = 1, false
	cores := float64(n * max(f.cores, 1))
	rate := coldRateAt(threadsPerCore(f.threads, f.cores)) * workerReadShare
	work := max(float64(x.bytes)/(cores*rate), x.rows/(cores*rowRate))
	o.estimate = startCost + float64(max(x.levels, 1))*levelCost + work
	if n > f.running {
		o.estimate += f.start.Seconds()
		o.started = n - f.running
	}
	return o
}

// onLarge estimates the plan's time on the large worker l alone: as one
// machine of its cores, threads and memory reads and computes it (see
// features.estimate), spilling as the whole working set does there, with a
// statement's start, its stages' rounds on its own disk (largeStageCost),
// and its own start if it is stopped.
func (x features) onLarge(l largeWorker) option {
	o := option{workers: 1, large: true, spill: spillOn(x.set(), l.memory)}
	cores := float64(max(l.cores, 1))
	rate := coldRateAt(threadsPerCore(l.threads, l.cores))
	work := max(float64(x.bytes)/(cores*rate), x.rows/(cores*rowRate))
	o.estimate = startCost + float64(max(x.levels, 1))*largeStageCost + work*o.spill.factor
	if !l.running {
		o.estimate += l.start.Seconds()
		o.started = 1
	}
	return o
}

// prices are what a worker of f's fleet, its large worker and the replica
// cost an hour, and whether those are dollars. A price not known is taken
// to go with the machine's cores, from a worker's or the large worker's,
// or, with neither known, a worker of the fleet counts one: cloud machines
// of a family cost about as their size.
func (f fleet) prices() (small, large, replica float64, dollars bool) {
	small = f.price
	var lcores int
	if f.large != nil {
		large, lcores = f.large.price, f.large.cores
	}
	ratio := func(cores int) float64 {
		if f.cores > 0 && cores > 0 {
			return float64(cores) / float64(f.cores)
		}
		return 1
	}
	switch {
	case small > 0 && large > 0:
	case small > 0:
		large = small * ratio(lcores)
	case large > 0:
		small = large / ratio(lcores)
	case f.replicaPrice > 0:
		small = f.replicaPrice / ratio(f.replicaCores)
		large = small * ratio(lcores)
	default:
		return 1, ratio(lcores), ratio(f.replicaCores), false
	}
	replica = f.replicaPrice
	if replica <= 0 {
		replica = small * ratio(f.replicaCores)
	}
	return small, large, replica, true
}

// costOn is what the option costs on f's machines: its machines' seconds,
// the workers it starts staying up idle after it as long as they do (see
// pool.idleAfter), at their prices, and the replica's, which waits for
// the statement and runs its last step. Where idle workers do not stop
// alone while others work (see pool.shrink), every running worker bills
// for as long as the statement runs, so fewer of them cost no less.
func (o option) costOn(f fleet) float64 {
	small, large, replica, _ := f.prices()
	own := o.seconds * replica
	if o.large {
		tail := 0.0
		if o.started > 0 && f.large != nil {
			tail = f.large.tail.Seconds()
		}
		return (own + (o.seconds+tail)*large) / 3600
	}
	billed := o.workers
	if !f.alone {
		billed = max(billed, f.running)
	}
	return (own + (float64(billed)*o.seconds+float64(o.started)*f.tail.Seconds())*small) / 3600
}

// fleetBucket groups numbers of workers whose runs stand for each other:
// 1, 2 to 3, 4 to 7, 8 to 15 and so on, each of fleetSizes' powers of two
// in a group of its own.
func fleetBucket(n int) int { return bits.Len(uint(max(n, 0))) }

// correct sets the options' seconds from what the statement took before,
// r: an option on as many workers as a run, by fleetBucket, or on the large
// worker, takes what that took; another on the fleet takes its estimate
// times what the nearest run took over its own estimate, as the estimates
// ran short or long for the statement. The large worker is corrected only
// by its own runs: one machine without exchanges is another kind of run.
// An option on which the statement failed within exploreEvery takes no
// less than failurePenalty times what it ran before it failed.
func correct(opts []option, r runs, f fleet) {
	correctAt(opts, r, f, time.Now())
}

func correctAt(opts []option, r runs, f fleet, now time.Time) {
	for i := range opts {
		opts[i].seconds, opts[i].measured, opts[i].failed, opts[i].failedAfter = opts[i].estimate, false, false, 0
	}
	var near []option
	for i, o := range opts {
		switch {
		case o.large && r.large > 0:
			opts[i].seconds, opts[i].measured = r.large, true
		case !o.large && r.fleets[fleetBucket(o.workers)] > 0:
			opts[i].seconds, opts[i].measured = r.fleets[fleetBucket(o.workers)], true
		}
		if opts[i].measured && !o.large && o.estimate > 0 {
			near = append(near, opts[i])
		}
	}
	for i, o := range opts {
		if o.measured || o.large || len(near) == 0 {
			continue
		}
		// The nearest measured option, the larger on a tie.
		best := near[0]
		for _, m := range near[1:] {
			d, bd := abs(fleetBucket(m.workers)-fleetBucket(o.workers)), abs(fleetBucket(best.workers)-fleetBucket(o.workers))
			if d < bd || d == bd && m.workers > best.workers {
				best = m
			}
		}
		opts[i].seconds = o.estimate * best.seconds / best.estimate
	}
	for i, o := range opts {
		fail, failed := r.failures[fleetBucket(o.workers)].recent(now)
		if o.large {
			fail, failed = r.largeFailure.recent(now)
		}
		if failed {
			opts[i].seconds = max(o.seconds, failurePenalty*fail.seconds)
			opts[i].failed, opts[i].failedAfter = true, fail.seconds
		}
	}
	for i := range opts {
		opts[i].cost = opts[i].costOn(f)
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// pick chooses among opts, as corrected (see correct): of those that do not
// run out of memory, the cheapest of those within fleetMargin of the
// fastest, the one on fewer workers on a tie (costs within costTie); and
// says why. No option faster than the one picked and cheaper by more than
// costTie is passed over: it would be within the margin too. ok is false
// when none would finish.
func pick(opts []option, dollars bool) (best option, why string, ok bool) {
	fastest := -1
	for i, o := range opts {
		if !o.spill.oom && (fastest < 0 || o.seconds < opts[fastest].seconds) {
			fastest = i
		}
	}
	if fastest < 0 {
		return option{}, "every way on workers would run out of memory", false
	}
	limit := opts[fastest].seconds * fleetMargin
	chosen := fastest
	for i, o := range opts {
		if o.spill.oom || o.seconds > limit {
			continue
		}
		c := opts[chosen]
		if o.cost < c.cost || o.cost == c.cost && o.workers < c.workers {
			chosen = i
		}
	}
	// Costs within costTie of the cheapest are a tie, which fewer workers
	// win: they leave the others to other statements, as a small statement
	// on running workers that stay up whatever it runs on costs the same
	// on any number of them.
	cheapest := opts[chosen].cost
	for i, o := range opts {
		if !o.spill.oom && o.seconds <= limit && o.cost <= cheapest*(1+costTie) && o.workers < opts[chosen].workers {
			chosen = i
		}
	}
	best, f := opts[chosen], opts[fastest]
	var b strings.Builder
	fmt.Fprintf(&b, "%s, %.0f s", best, best.seconds)
	switch {
	case best.failed:
		fmt.Fprintf(&b, " as it failed there lately after %.0f s", best.failedAfter)
	case best.measured:
		b.WriteString(" as it took before")
	}
	switch {
	case len(opts) == 1:
	case chosen == fastest:
		b.WriteString(", the fastest")
		if next, ok := runnerUp(opts, chosen); ok && next.cost > best.cost {
			fmt.Fprintf(&b, ", for %s of the cost of %s", share(best.cost, next.cost), next)
		}
	default:
		fmt.Fprintf(&b, ", within a fifth of the fastest, %s at %.0f s, for %s of its cost", f, f.seconds, share(best.cost, f.cost))
	}
	if best.spill.factor > 1 {
		fmt.Fprintf(&b, "; spilling, %.1f times as long", best.spill.factor)
	}
	if !dollars && len(opts) > 1 {
		b.WriteString(" (prices not set: by worker-seconds)")
	}
	return best, b.String(), true
}

// exploreFleet is the option to run a statement on once, to measure it,
// when best was picked among opts as corrected by what it took before (r):
// the cheapest of those not measured whose time, as corrected, is within
// fleetExploreWithin of the fastest measured option's and that best does
// not beat on both time and cost, unless the statement was tried so
// within exploreEvery or nothing was measured. A report took 200 s on 4
// workers, its estimate, and 127 s on 16; the estimates, corrected by the
// run on 16, took 8 for 162 s, and it took 112 s there.
func exploreFleet(opts []option, best option, r runs, now time.Time) (option, bool) {
	if now.Sub(r.fleetExplored) < exploreEvery {
		return option{}, false
	}
	fastest := math.Inf(1)
	for _, o := range opts {
		if o.measured && !o.spill.oom {
			fastest = min(fastest, o.seconds)
		}
	}
	var out option
	found := false
	for _, o := range opts {
		switch {
		case o.measured, o.failed, o.large, o.spill.oom, math.IsInf(fastest, 1), o.seconds > fleetExploreWithin*fastest:
		case o.seconds >= best.seconds && o.cost >= best.cost:
		case !found || o.cost < out.cost:
			out, found = o, true
		}
	}
	return out, found
}

// runnerUp is the fastest option of opts but i that would finish.
func runnerUp(opts []option, i int) (option, bool) {
	var out option
	found := false
	for j, o := range opts {
		if j != i && !o.spill.oom && (!found || o.seconds < out.seconds) {
			out, found = o, true
		}
	}
	return out, found
}

// share writes a over b as a percentage.
func share(a, b float64) string {
	if b <= 0 {
		return "all"
	}
	return fmt.Sprintf("%.0f%%", 100*a/b)
}

// describeOptions lists the options weighed, for EXPLAIN DISTRIBUTED.
func describeOptions(opts []option, dollars bool) string {
	parts := make([]string, len(opts))
	for i, o := range opts {
		s := fmt.Sprintf("%s %.0f s", o, o.seconds)
		if o.failed {
			s += fmt.Sprintf(" (failed after %.0f s)", o.failedAfter)
		} else if o.measured {
			s += " (measured)"
		} else if math.Abs(o.seconds-o.estimate) > 0.5 {
			s += fmt.Sprintf(" (estimated %.0f s)", o.estimate)
		}
		if dollars {
			s += fmt.Sprintf(", $%.3f", o.cost)
		} else {
			s += fmt.Sprintf(", %.3f worker-hours", o.cost)
		}
		switch {
		case o.spill.oom:
			s += ", runs out of memory"
		case o.spill.factor > 1:
			s += fmt.Sprintf(", spills %.1fx", o.spill.factor)
		}
		parts[i] = s
	}
	return strings.Join(parts, "; ")
}
