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
	explored    time.Time
	outOfMemory bool
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

// observe records a run of key on workers or not that took seconds.
func (h *history) observe(key string, onWorkers bool, seconds float64) {
	h.update(key, func(r *runs) {
		avg := &r.local
		if onWorkers {
			avg = &r.dist
		} else {
			r.outOfMemory = false
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
}

// Observe records how long a statement whose plan key is key (Plan.Key,
// PlanKey) took, on workers workers or on one machine.
func (co *Coordinator) Observe(key string, workers int, d time.Duration) {
	if key != "" && d > 0 {
		co.history.observe(key, workers > 0, d.Seconds())
	}
}

// ObserveOutOfMemory records that a statement whose plan key is key ran
// out of memory on one machine, so that auto mode runs it on workers next
// time (see choose).
func (co *Coordinator) ObserveOutOfMemory(key string) {
	if key != "" {
		co.history.update(key, func(r *runs) { r.outOfMemory = true })
	}
}

// Seed records past runs, oldest first, as from the query history when
// the server starts.
func (co *Coordinator) Seed(rs []Run) {
	for _, r := range rs {
		if r.OutOfMemory {
			co.ObserveOutOfMemory(r.Key)
			continue
		}
		co.Observe(r.Key, r.Workers, r.Duration)
	}
}
