package workload

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Controller holds statements back until their limits allow them to start,
// on one replica: a statement counts against its Slots while it runs, its
// memory reservation against the replica's query memory (DuckDB's memory
// limit), and, if it is Counted, against the replica's own limit of
// statements at once (see LimitReplica).
//
// Statements that must wait do so in the order they would end if each started
// when it came and ran alone (see Limits.Expect, costs.go), which is arrival
// order for statements with no history. A statement never overtakes one ahead
// of it that waits for the same slots, nor, if it reserves memory, one that
// waits for memory, so a large reservation is not starved by a stream of
// small ones: a later statement goes first only if it would end first.
// Statements whose limits differ do not hold each other up.
//
// DuckDB has one memory pool per process and cannot stop a statement at
// its reservation: one that needs more spills to disk once the replica's
// memory is used up. Reservations decide how many statements start at
// once, not what each may use; for hard isolation, run a separate compute
// pool.
type Controller struct {
	// budget is the replica's query memory in bytes; zero means unknown,
	// and reservations are then not enforced.
	budget int64

	// replicaMax is the replica's limit of counted statements at once;
	// zero means none.
	replicaMax int

	// history estimates reservations from what statements allocated (see
	// footprints.go).
	history footprints

	// forms pick the form of each statement shape's FLATTENs (see forms.go).
	forms flattenForms
	// costs say how long statements take and which are light (see costs.go).
	costs costs

	mu       sync.Mutex
	reserved int64
	running  map[string]int
	// replicaRunning counts the counted statements running.
	replicaRunning int
	// apartRunning counts the statements running in the second DuckDB.
	apartRunning int
	queue        []*waiter
	// active are the statements running under the controller's limits,
	// whose peaks SampleMemory keeps.
	active map[*waiter]struct{}
	// held is what DuckDB last held for running statements, as SampleMemory
	// read it; zero before it has.
	held int64

	waiting prometheus.Gauge
	waited  prometheus.Counter
}

// waiter is a statement waiting to start.
type waiter struct {
	slots  Slots
	memory int64
	// counted says it counts against the replica's limit.
	counted bool
	// unseen says nothing is known of what it holds, so that it waits while
	// the memory is nearly full (see full).
	unseen bool
	// apart says it runs in the second DuckDB (see Limits.Apart).
	apart bool
	// due is its place in line: when it came, plus how long it takes alone.
	due   time.Time
	ready chan struct{}
	// admitted is set, under the controller's lock, once the statement
	// holds its slot and reservation.
	admitted bool
	// peak is the most memory it was seen holding while it ran, and
	// sampled says a sample fell within its run (see SampleMemory).
	peak    int64
	sampled bool
}

// NewController enforces limits against budget bytes of query memory.
func NewController(budget int64) *Controller {
	return &Controller{
		budget:  budget,
		running: make(map[string]int),
		active:  make(map[*waiter]struct{}),
		waiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "igloo_statements_waiting",
			Help: "Statements waiting for the replica's or their role's concurrency or memory limit before they start.",
		}),
		waited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "igloo_statement_wait_seconds_total",
			Help: "Time statements spent waiting for the replica's or their role's limits.",
		}),
	}
}

// LimitReplica lets at most n statements that are Counted run at once on
// the replica, whoever runs them, as a Snowflake warehouse's
// MAX_CONCURRENCY_LEVEL does; further ones wait in line. Zero or less
// means no limit. It is set once, before statements run.
func (c *Controller) LimitReplica(n int) { c.replicaMax = max(n, 0) }

// ReplicaLimit is LimitReplica's limit; zero means none.
func (c *Controller) ReplicaLimit() int {
	if c == nil {
		return 0
	}
	return c.replicaMax
}

// Metrics are the controller's Prometheus metrics.
func (c *Controller) Metrics() []prometheus.Collector {
	return []prometheus.Collector{c.waiting, c.waited}
}

// Admit waits until a statement under l may start, or ctx ends, in which
// case it returns ctx's error. The statement must call release when it
// ends, which returns the most memory it was seen holding, peak, if a
// sample fell within its run, sampled (see SampleMemory). waited is how
// long it was held back.
func (c *Controller) Admit(ctx context.Context, l Limits) (release func() (peak int64, sampled bool), waited time.Duration, err error) {
	unlimited := func() (int64, bool) { return 0, false }
	if c == nil || !l.Limited() && !(l.Counted && c.replicaMax > 0) && !l.Unseen {
		return unlimited, 0, nil
	}
	w := &waiter{slots: l.Slots, memory: c.reservation(l.Memory), counted: l.Counted && c.replicaMax > 0, unseen: l.Unseen, apart: l.Apart, ready: make(chan struct{})}
	if w.slots.Max == 0 && w.memory == 0 && !w.counted && !w.unseen {
		return unlimited, 0, nil
	}
	w.due = time.Now().Add(l.Expect)
	c.mu.Lock()
	// After those due at the same time, so that statements with no history
	// keep their order.
	at := sort.Search(len(c.queue), func(i int) bool { return c.queue[i].due.After(w.due) })
	c.queue = slices.Insert(c.queue, at, w)
	c.admit()
	admitted := w.admitted
	c.mu.Unlock()

	if !admitted {
		start := time.Now()
		c.waiting.Inc()
		select {
		case <-w.ready:
		case <-ctx.Done():
		}
		c.waiting.Dec()
		waited = time.Since(start)
		c.waited.Add(waited.Seconds())

		c.mu.Lock()
		if !w.admitted {
			// Leaving the queue may let later statements start.
			c.remove(w)
			c.admit()
			c.mu.Unlock()
			return nil, waited, ctx.Err()
		}
		c.mu.Unlock()
	}
	var once sync.Once
	return func() (int64, bool) {
		once.Do(func() { c.release(w) })
		return w.peak, w.sampled
	}, waited, nil
}

// reservation is what a statement reserving memory bytes takes from the
// budget: all of it at most, so that a reservation larger than the
// replica's memory makes the statement wait until no other reservation
// is held, rather than forever.
func (c *Controller) reservation(memory int64) int64 {
	if c.budget <= 0 {
		return 0
	}
	return min(memory, c.budget)
}

// admit starts every waiting statement that may start, in the line's order.
// The caller holds c.mu.
func (c *Controller) admit() {
	blocked := map[string]bool{}
	memoryBlocked, replicaBlocked := false, false
	full := c.full()
	kept := c.queue[:0]
	for _, w := range c.queue {
		slotsFree := w.slots.Max == 0 || !blocked[w.slots.Key] && c.running[w.slots.Key] < w.slots.Max
		memoryFree := w.memory == 0 && !(w.unseen && full) || w.memory > 0 && !memoryBlocked && c.reserved+w.memory <= c.budget
		replicaFree := !w.counted || !replicaBlocked && c.replicaRunning < c.replicaMax
		if slotsFree && memoryFree && replicaFree {
			if w.slots.Max > 0 {
				c.running[w.slots.Key]++
			}
			if w.counted {
				c.replicaRunning++
			}
			c.reserved += w.memory
			if w.apart {
				c.apartRunning++
			} else {
				// What SampleMemory reads is the first DuckDB's.
				c.active[w] = struct{}{}
			}
			w.admitted = true
			close(w.ready)
			continue
		}
		// Later statements with the same slots wait behind this one
		// whatever it waits for; others reserving memory only if it waits
		// for memory, and other counted ones only if it waits for the
		// replica's limit, not when it waits for its slots alone.
		if w.slots.Max > 0 {
			blocked[w.slots.Key] = true
		}
		if !memoryFree && w.memory > 0 {
			memoryBlocked = true
		}
		if !replicaFree {
			replicaBlocked = true
		}
		kept = append(kept, w)
	}
	clear(c.queue[len(kept):])
	c.queue = kept
}

// fullShare is the share of the budget DuckDB must hold for the memory to
// count as nearly full.
const fullShare = 0.75

// full reports whether DuckDB holds nearly all the budget for statements
// running now: a statement nothing is known of then waits, since what it
// holds would make those running spill. Reservations cannot say so the first
// time a dashboard runs on a replica, when none of its statements has any:
// its heavy ones started together and spilled, and it took half as long again
// as the next time. The caller holds c.mu.
func (c *Controller) full() bool {
	return c.budget > 0 && len(c.active) > 0 && float64(c.held) >= fullShare*float64(c.budget)
}

// HeldBehindSecond reports whether statements wait in line while all that run
// run in the second DuckDB: what they wait for is that one's memory, so the
// first's caches keep nothing for anyone.
func (c *Controller) HeldBehindSecond() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.apartRunning > 0 && len(c.active) == 0 && len(c.queue) > 0
}

func (c *Controller) remove(w *waiter) {
	for i, other := range c.queue {
		if other == w {
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			return
		}
	}
}

func (c *Controller) release(w *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w.slots.Max > 0 {
		if c.running[w.slots.Key]--; c.running[w.slots.Key] <= 0 {
			delete(c.running, w.slots.Key)
		}
	}
	if w.counted {
		c.replicaRunning--
	}
	c.reserved -= w.memory
	if w.apart {
		c.apartRunning--
	}
	delete(c.active, w)
	c.admit()
}

// Running reports how many statements counting against key run now.
func (c *Controller) Running(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running[key]
}
