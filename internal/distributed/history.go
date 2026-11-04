package distributed

import (
	"cmp"
	clist "container/list"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// history remembers how long statements took, by plan key, on one machine
// and on workers, so that auto mode chooses the faster for the statements
// BI tools run again and again (see choose). It keeps the maxHistory keys
// used last. Each replica keeps its own, filled at start from the query
// history (Seed) and then by its own statements (Observe).
type history struct {
	mu    sync.Mutex
	order *clist.List
	byKey map[string]*clist.Element
}

// maxHistory bounds the plan keys a history keeps.
const maxHistory = 4096

// runs are what a statement took: averages of its runs' seconds on one
// machine and on workers, zero for none, weighing the last run as much as
// all the earlier ones, and when it last ran on workers against the
// estimates. outOfMemory says its last run on one machine ran out of
// memory.
type runs struct {
	key         string
	local, dist float64
	// distWorkers are the workers its last run on workers ran on, and dist
	// the average of its runs on about as many. fleets are those averages by
	// how many workers ran it (fleetBucket), for choosing how many next time
	// (see correct); large is the average of its runs on the large worker.
	distWorkers int
	fleets      map[int]float64
	large       float64
	// failures are when its runs on about as many workers (fleetBucket),
	// or on the large worker (largeFailure), last failed there, and after
	// how long, for choosing how many next time (see correct).
	failures     map[int]failure
	largeFailure failure
	explored     time.Time
	// fleetExplored is when it last ran on a number of workers it had not
	// run on, to measure it (see exploreFleet).
	fleetExplored time.Time
	outOfMemory   bool
	// guarded says it ran on one machine far past its estimate, and was
	// run on workers instead (see ObserveGuard).
	guarded bool
	// exchangeRows and exchangeBytes are what its last run on workers
	// exchanged, and fanout the most rows a stage of it wrote for each
	// row it read, as measured (see observeExchanges); zero before.
	exchangeRows, exchangeBytes int64
	fanout                      float64
	// heavy says, for choose, that what it exchanged would not fit in one
	// machine's memory share, held as heldFactor times its bytes; it is
	// worked out when the runs are read, not kept.
	heavy bool
	// planSet is the working set DuckDB's plan of it was estimated to hold,
	// before the correction by statements' peaks (see workingSet), kept so
	// that the plan is not read again for it, and to correct the estimates
	// by its peak (see ObservePeak); planBytes and planWork are what the
	// plan said one machine reads and its operators take (planEstimate).
	planSet    float64
	planBytes  int64
	planWork   float64
	planKeeps  float64
	planFields float64
	planSorted float64
	planJoins  float64
	planRows   float64
	// peak is the most it held at its peak in its runs to their end on
	// this replica, short of the share of memory at which it may have
	// spilled (see ObservePeak); zero if none. It stands for planSet.
	peak float64
	// localMemory, distMemory and largeMemory are the memory its runs had:
	// the replica's limit on one machine, the smallest worker's on workers,
	// the large worker's on it; zero when not known. Runs under other
	// conditions are not compared (see comparable).
	localMemory, distMemory, largeMemory int64
	// localAt is when its last run on one machine ended, for trying it
	// there again (see replicaRetryAfter).
	localAt time.Time
}

// failure is a statement's run on workers that failed after seconds, at.
type failure struct {
	seconds float64
	at      time.Time
}

// failurePenalty is how many times what a statement ran on a number of
// workers before it failed there a run there is taken to take, for
// exploreEvery after: it did not finish, and failing cost the run again.
// At a hundred times the data, a report that took 729 s on 16 workers
// failed there after 2,078 s, a worker out of memory, and took 1,537 s
// the next time, a task run again; the runs kept said 16 took 729 s.
const failurePenalty = 1.5

// recent returns f if it failed within exploreEvery before now.
func (f failure) recent(now time.Time) (failure, bool) {
	return f, f.at != (time.Time{}) && now.Sub(f.at) < exploreEvery
}

func newHistory() *history {
	return &history{order: clist.New(), byKey: map[string]*clist.Element{}}
}

// get returns the runs of key.
func (h *history) get(key string) (runs, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	el, ok := h.byKey[key]
	if !ok {
		return runs{}, false
	}
	h.order.MoveToFront(el)
	r := *el.Value.(*runs)
	r.fleets, r.failures = maps.Clone(r.fleets), maps.Clone(r.failures)
	return r, true
}

// update changes the runs of key, adding them if need be.
func (h *history) update(key string, fn func(*runs)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	el, ok := h.byKey[key]
	if !ok {
		el = h.order.PushFront(&runs{key: key})
		h.byKey[key] = el
		for h.order.Len() > maxHistory {
			oldest := h.order.Back()
			h.order.Remove(oldest)
			delete(h.byKey, oldest.Value.(*runs).key)
		}
	} else {
		h.order.MoveToFront(el)
	}
	fn(el.Value.(*runs))
}

// observe records a run of key on workers workers, on the large worker if
// large, or on one machine if neither, that took seconds, ending now (see
// observeAt).
func (h *history) observe(key string, workers int, large bool, seconds float64, memory int64) {
	h.observeAt(key, workers, large, seconds, memory, time.Now())
}

// observeAt is observe of a run that ended at at. Runs on workers average
// only with runs on about as many, by fleetBucket: twice the workers or
// half make another statement of it. A run on one machine replaces one
// there replicaRetryAfter or more before it rather than averaging with it:
// that one ran under another load.
func (h *history) observeAt(key string, workers int, large bool, seconds float64, memory int64, at time.Time) {
	h.update(key, func(r *runs) {
		var avg *float64
		switch {
		case large:
			if !alike(memory, r.largeMemory) {
				r.large = 0
			}
			avg, r.largeMemory = &r.large, memory
		case workers > 0:
			if !alike(memory, r.distMemory) {
				r.fleets, r.failures = nil, nil
			}
			if r.fleets == nil {
				r.fleets = map[int]float64{}
			}
			b := fleetBucket(workers)
			r.fleets[b] = mean(r.fleets[b], seconds)
			r.dist, r.distWorkers, r.distMemory = r.fleets[b], workers, memory
			return
		default:
			if !alike(memory, r.localMemory) {
				r.local, r.guarded = 0, false
			}
			if at.Sub(r.localAt) >= replicaRetryAfter {
				r.local = 0
			}
			avg = &r.local
			r.outOfMemory, r.localMemory = false, memory
			if at.After(r.localAt) {
				r.localAt = at
			}
		}
		*avg = mean(*avg, seconds)
	})
}

// observeFailure records that a run of key on workers workers, or on the
// large worker if large, failed after seconds, on workers of memory.
func (h *history) observeFailure(key string, workers int, large bool, seconds float64, memory int64, at time.Time) {
	h.update(key, func(r *runs) {
		f := failure{seconds: seconds, at: at}
		if large {
			if !alike(memory, r.largeMemory) {
				r.large = 0
			}
			r.largeFailure, r.largeMemory = f, memory
			return
		}
		if !alike(memory, r.distMemory) {
			r.fleets, r.failures = nil, nil
		}
		if r.failures == nil {
			r.failures = map[int]failure{}
		}
		r.failures[fleetBucket(workers)], r.distMemory = f, memory
	})
}

// mean weighs a run of seconds as much as the runs before it, avg, zero
// for none.
func mean(avg, seconds float64) float64 {
	if avg == 0 {
		return seconds
	}
	return (avg + seconds) / 2
}

// Run is a statement's past run, for auto mode's choice of where it runs
// next: its plan key (Plan.Key), the workers it ran on (0 for one
// machine) and how long it took, or that it ran out of memory.
type Run struct {
	Key     string
	Workers int
	// Large says it ran on the large worker (see OnLarge).
	Large       bool
	Duration    time.Duration
	OutOfMemory bool
	// Failed says it failed on Workers workers, or the large worker, after
	// Duration (see ObserveFailure), at At.
	Failed bool
	At     time.Time
	// Guarded says the guard moved it to workers, after Duration on one
	// machine of Memory (see ObserveGuard).
	Guarded bool
	// Memory is the memory it ran with (see RunMemory); a run that does not
	// say is not taken.
	Memory int64
	// Peak is the most it held, as its replica sampled it, of a run to its
	// end on one machine (see ObservePeak); zero if not known.
	Peak int64
}

// alike reports whether two machines' memory, a and b, are close enough,
// within a quarter, for runs on one to stand for runs on the other; an
// unknown one, zero, is alike any.
func alike(a, b int64) bool {
	return a == 0 || b == 0 || max(a, b) <= min(a, b)/4*5
}

// RunMemory is the memory a statement that ran on workers workers, or on
// this replica if none, had: the replica's limit, or the smallest limit
// the workers reported; the server records it with the statement's run
// (see Run.Memory), as the condition under which it took what it took.
func (co *Coordinator) RunMemory(workers int) int64 { return co.RunMemoryOn(workers, false) }

// RunMemoryOn is RunMemory of a statement that ran on the large worker if
// large: the large worker's limit.
func (co *Coordinator) RunMemoryOn(workers int, large bool) int64 {
	switch {
	case large:
		return co.largeMemory()
	case workers == 0:
		return co.opts.MemoryBytes
	case co.pool == nil:
		return 0
	}
	return co.pool.lastMemory()
}

// comparable leaves of r only the runs measured under the conditions
// statements run under now, the replica's memory m, the workers' w and the
// large worker's l: runs on a replica of 14 GB, or on workers of 6 GB
// beside it, are no guide on one of 22 GB with workers of their own, and
// once in the query history they kept a dashboard's statements on workers
// for a week.
func comparable(r runs, m, w, l int64) runs {
	if !alike(r.localMemory, m) {
		r.local, r.outOfMemory, r.guarded = 0, false, false
	}
	if !alike(r.distMemory, w) {
		r.dist, r.distWorkers, r.fleets, r.failures, r.exchangeRows, r.exchangeBytes, r.fanout = 0, 0, nil, nil, 0, 0, 0
	}
	if !alike(r.largeMemory, l) {
		r.large, r.largeFailure = 0, failure{}
	}
	return r
}

// Observe records how long a statement whose plan key is key (Plan.Key,
// PlanKey) took, on workers workers or on one machine.
func (co *Coordinator) Observe(key string, workers int, d time.Duration) {
	co.ObserveOn(key, workers, false, d)
}

// ObserveOn is Observe of a statement that ran on the large worker if
// large (Execution.Large).
func (co *Coordinator) ObserveOn(key string, workers int, large bool, d time.Duration) {
	if key != "" && d > 0 {
		co.history.observe(key, workers, large, d.Seconds(), co.RunMemoryOn(workers, large))
	}
}

// ObserveFailure records that a statement whose plan key is key failed
// on workers workers, or on the large worker if large, after d, so that
// auto mode takes that way to take longer for a while (see correct).
func (co *Coordinator) ObserveFailure(key string, workers int, large bool, d time.Duration) {
	if key != "" && d > 0 && (workers > 0 || large) {
		co.history.observeFailure(key, workers, large, d.Seconds(), co.RunMemoryOn(workers, large), time.Now())
	}
}

// ObserveOutOfMemory records that a statement whose plan key is key ran
// out of memory on one machine, so that auto mode runs it on workers next
// time (see choose).
func (co *Coordinator) ObserveOutOfMemory(key string) {
	co.observeOutOfMemory(key, co.opts.MemoryBytes)
}

func (co *Coordinator) observeOutOfMemory(key string, memory int64) {
	if key != "" {
		co.history.update(key, func(r *runs) { r.outOfMemory, r.localMemory = true, memory })
	}
}

// observeExchanges records what the exchanges of a statement's run on
// workers held, by its plan key: their rows and bytes in all, and the most
// rows a stage wrote for each row it read, of the stages that alone wrote
// their exchange. Auto mode keeps on workers a statement whose exchanges
// would not fit in one machine's memory (see choose), and EXPLAIN
// DISTRIBUTED shows them; an estimate of one machine by them is no
// better (a report whose workers exchanged 14 GB took 38 s on one
// machine, its plan there streaming), so they never move one to workers.
func (co *Coordinator) observeExchanges(key string, run *execution, stages []*Stage) {
	if key == "" || run == nil {
		return
	}
	var rows, bytes int64
	for _, x := range run.exchanges {
		x.mu.Lock()
		rows, bytes = rows+x.rows, bytes+x.bytes
		x.mu.Unlock()
	}
	producers := map[*Exchange]int{}
	for _, s := range stages {
		if s.Out != nil {
			producers[s.Out]++
		}
	}
	var fanout float64
	for _, s := range stages {
		if s.Out == nil || producers[s.Out] != 1 || run.exchanges[s.Out.ID] == nil {
			continue
		}
		var in int64
		for _, i := range s.Inputs {
			switch {
			case i.Table != nil:
				in += rowCount(run.files[i.Table.key()])
			case i.Exchange != nil && run.exchanges[i.Exchange.ID] != nil:
				in += run.exchanges[i.Exchange.ID].rows
			}
		}
		if in > 0 {
			fanout = max(fanout, float64(run.exchanges[s.Out.ID].rows)/float64(in))
		}
	}
	co.history.update(key, func(r *runs) {
		r.exchangeRows, r.exchangeBytes, r.fanout = rows, bytes, fanout
	})
}

// ObserveGuard records that a statement whose plan key is key ran on one
// machine, where auto mode had chosen to run it, for d, far past its
// estimate (the server's guard), and was run on workers instead: auto mode
// runs it on workers next time, without trying one machine again (see
// choose), so the guard cannot move it back and forth.
func (co *Coordinator) ObserveGuard(key string, d time.Duration) {
	co.observeGuard(key, d, co.opts.MemoryBytes)
	co.opts.Metrics.guard()
}

func (co *Coordinator) observeGuard(key string, d time.Duration, memory int64) {
	if key != "" {
		co.history.update(key, func(r *runs) {
			r.guarded, r.localMemory = true, memory
			r.local = max(r.local, d.Seconds())
		})
	}
}

// GuardPlacement is the placement (Plan.Placement) of a statement the guard
// moved to workers, on, after ran on this replica, estimated at estimate: the
// query history keeps it, and GuardedRun reads back what the guard learned
// when the server starts. Its row otherwise says only that it ran on workers,
// and a restart could try on one machine again what the guard had moved.
func (co *Coordinator) GuardPlacement(ran, estimate time.Duration, on string) string {
	return fmt.Sprintf("moved by the guard after %.0f s on this replica of %.1f GB, estimated at %.0f s; %s", ran.Seconds(),
		float64(co.opts.MemoryBytes)/1e9, estimate.Seconds(), on)
}

// guardPlaced matches what GuardedRun reads of GuardPlacement.
var guardPlaced = regexp.MustCompile(`^moved by the guard after (\d+) s on this replica of ([\d.]+) GB,`)

// GuardedRun reads a placement the query history recorded: how long the
// statement ran on this replica before the guard moved it, on how much memory.
func GuardedRun(placement string) (ran time.Duration, memory int64, ok bool) {
	m := guardPlaced.FindStringSubmatch(placement)
	if m == nil {
		return 0, 0, false
	}
	seconds, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, 0, false
	}
	gb, err := strconv.ParseFloat(m[2], 64)
	if err != nil {
		return 0, 0, false
	}
	return time.Duration(seconds * float64(time.Second)), int64(gb * 1e9), true
}

// Seed records past runs, oldest first, as from the query history when
// the server starts, with the memory they ran with; runs that do not say,
// as those recorded before Igloo kept it, are left out.
func (co *Coordinator) Seed(rs []Run) {
	for _, r := range rs {
		switch {
		case r.Key == "" || r.Memory == 0:
		case r.Guarded:
			co.observeGuard(r.Key, r.Duration, r.Memory)
		case r.Failed:
			co.history.observeFailure(r.Key, r.Workers, r.Large, r.Duration.Seconds(), r.Memory, cmp.Or(r.At, time.Now()))
		case r.OutOfMemory:
			co.observeOutOfMemory(r.Key, r.Memory)
		case r.Duration > 0:
			co.history.observeAt(r.Key, r.Workers, r.Large, r.Duration.Seconds(), r.Memory, r.At)
			if r.Workers == 0 && !r.Large && alike(r.Memory, co.opts.MemoryBytes) {
				co.ObservePeak(r.Key, r.Peak)
			}
		}
	}
}
