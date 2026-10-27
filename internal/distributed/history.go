package distributed

import (
	clist "container/list"
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
	// distWorkers are the workers its last run on workers ran on.
	distWorkers int
	explored    time.Time
	outOfMemory bool
	// guarded says it ran on one machine past guardFactor times its
	// estimate, and was run on workers instead (see ObserveGuard).
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
	// localMemory and distMemory are the memory its runs had: the replica's
	// limit on one machine, the smallest worker's on workers; zero when not
	// known. Runs under other conditions are not compared (see comparable).
	localMemory, distMemory int64
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
	return *el.Value.(*runs), true
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

// observe records a run of key on workers workers, or on one machine if
// none, that took seconds. Runs on workers average only with runs on about
// as many: twice the workers or half make another statement of it.
func (h *history) observe(key string, workers int, seconds float64, memory int64) {
	h.update(key, func(r *runs) {
		avg := &r.local
		if workers > 0 {
			avg = &r.dist
			if r.distWorkers > 0 && (workers >= 2*r.distWorkers || 2*workers <= r.distWorkers) || !alike(memory, r.distMemory) {
				*avg = 0
			}
			r.distWorkers, r.distMemory = workers, memory
		} else {
			if !alike(memory, r.localMemory) {
				*avg, r.guarded = 0, false
			}
			r.outOfMemory, r.localMemory = false, memory
		}
		if *avg == 0 {
			*avg = seconds
		} else {
			*avg = (*avg + seconds) / 2
		}
	})
}

// Run is a statement's past run, for auto mode's choice of where it runs
// next: its plan key (Plan.Key), the workers it ran on (0 for one
// machine) and how long it took, or that it ran out of memory.
type Run struct {
	Key         string
	Workers     int
	Duration    time.Duration
	OutOfMemory bool
	// Memory is the memory it ran with (see RunMemory); a run that does not
	// say is not taken.
	Memory int64
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
func (co *Coordinator) RunMemory(workers int) int64 {
	if workers == 0 {
		return co.opts.MemoryBytes
	}
	if co.pool == nil {
		return 0
	}
	return co.pool.lastMemory()
}

// comparable leaves of r only the runs measured under the conditions
// statements run under now, the replica's memory m and the workers' w:
// runs on a replica of 14 GB, or on workers of 6 GB beside it, are no
// guide on one of 22 GB with workers of their own, and once in the query
// history they kept a dashboard's statements on workers for a week.
func comparable(r runs, m, w int64) runs {
	if !alike(r.localMemory, m) {
		r.local, r.outOfMemory, r.guarded = 0, false, false
	}
	if !alike(r.distMemory, w) {
		r.dist, r.distWorkers, r.exchangeRows, r.exchangeBytes, r.fanout = 0, 0, 0, 0, 0
	}
	return r
}

// Observe records how long a statement whose plan key is key (Plan.Key,
// PlanKey) took, on workers workers or on one machine.
func (co *Coordinator) Observe(key string, workers int, d time.Duration) {
	if key != "" && d > 0 {
		co.history.observe(key, workers, d.Seconds(), co.RunMemory(workers))
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
// machine, where auto mode had chosen to run it, for d, past guardFactor
// times its estimate, and was run on workers instead: auto mode runs it on
// workers next time, without trying one machine again (see choose).
func (co *Coordinator) ObserveGuard(key string, d time.Duration) {
	if key != "" {
		co.history.update(key, func(r *runs) {
			r.guarded = true
			r.local = max(r.local, d.Seconds())
		})
	}
	co.opts.Metrics.guard()
}

// Seed records past runs, oldest first, as from the query history when
// the server starts, with the memory they ran with; runs that do not say,
// as those recorded before Igloo kept it, are left out.
func (co *Coordinator) Seed(rs []Run) {
	for _, r := range rs {
		switch {
		case r.Key == "" || r.Memory == 0:
		case r.OutOfMemory:
			co.observeOutOfMemory(r.Key, r.Memory)
		case r.Duration > 0:
			co.history.observe(r.Key, r.Workers, r.Duration.Seconds(), r.Memory)
		}
	}
}
