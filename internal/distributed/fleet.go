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
// machines would spill (spillOn); of the ways estimated within a fifth of
// the fastest, it takes the cheapest, by the machine-seconds each takes at
// its price (see pick), and corrects the estimates with what the statement
// took before on as many workers (see correct). Whether the statement runs
// on that way or on one machine is choose's to decide, as before.

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
	// fleetMargin is how many times the fastest way's estimate a way may
	// take and still be chosen for costing less: estimates are rough, and a
	// fifth more time on a quarter of the workers is a better bet than the
	// last fifth bought with four times the machines. At ten times the
	// data, a report took 58 s on one machine and 50 s on 16 workers, most
	// of it the start and rounds of its stages, which more workers do not
	// shorten.
	fleetMargin = 1.2
	// largeStageCost is what a stage's round of tasks takes on the large
	// worker, where its exchanges, if any, stay on its own disk: at ten
	// times the data, stages exchanging through workers' disks ran three to
	// eight times as fast as through object storage (see stageCost).
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
// memory, or if the memory is not known; otherwise it spills, and takes
// (set/room)^spillExponent times as long, or runs out of memory past
// oomRatio.
func spillOn(set float64, memory int64) spill {
	s := spill{set: set, room: memoryShare * float64(max(memory, 0)), factor: 1}
	if s.room <= 0 || set <= s.room {
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
	// is what it took on as many workers.
	estimate, seconds float64
	measured          bool
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
// start, each stage's round of tasks and of the exchanges it writes to
// object storage, and what its stages read cold, or the rows they go
// through, whichever takes longer, on n workers' cores at their threads a
// core; and the start of the workers that do not run. At ten times the
// data on 16 workers, reports of 2 and 3 stages took 12 to 40 s, and of 23
// to 26 stages 153 to 183 s, over a few gigabytes.
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
	rate := coldRateAt(threadsPerCore(f.threads, f.cores))
	work := max(float64(x.bytes)/(cores*rate), x.rows/(cores*rowRate))
	o.estimate = startCost + float64(x.stages)*stageCost + work
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
	o.estimate = startCost + float64(x.stages)*largeStageCost + work*o.spill.factor
	if !l.running {
		o.estimate += l.start.Seconds()
		o.started = 1
	}
	return o
}

// prices are what a worker of f's fleet and its large worker cost an hour,
// and whether those are dollars. A price not known is taken to go with the
// machine's cores, from the other's, or, with neither known, a worker of
// the fleet counts one: cloud machines of a family cost about as their
// size.
func (f fleet) prices() (small, large float64, dollars bool) {
	small = f.price
	var lcores int
	if f.large != nil {
		large, lcores = f.large.price, f.large.cores
	}
	ratio := 1.0
	if f.cores > 0 && lcores > 0 {
		ratio = float64(lcores) / float64(f.cores)
	}
	switch {
	case small > 0 && large > 0:
	case small > 0:
		large = small * ratio
	case large > 0:
		small = large / ratio
	default:
		return 1, ratio, false
	}
	return small, large, true
}

// costOn is what the option costs on f's machines: its machines' seconds,
// the workers it starts staying up idle after it as long as they do (see
// pool.idleAfter), at their prices.
func (o option) costOn(f fleet) float64 {
	small, large, _ := f.prices()
	if o.large {
		tail := 0.0
		if o.started > 0 && f.large != nil {
			tail = f.large.tail.Seconds()
		}
		return (o.seconds + tail) * large / 3600
	}
	return (float64(o.workers)*o.seconds + float64(o.started)*f.tail.Seconds()) * small / 3600
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
func correct(opts []option, r runs, f fleet) {
	for i := range opts {
		opts[i].seconds, opts[i].measured = opts[i].estimate, false
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
// fastest, the one on fewer workers on a tie; and says why. ok is false
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
	best, f := opts[chosen], opts[fastest]
	var b strings.Builder
	fmt.Fprintf(&b, "%s, %.0f s", best, best.seconds)
	if best.measured {
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
		if o.measured {
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
