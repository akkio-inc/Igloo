package workload

import (
	"context"
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
// Statements that must wait do so in arrival order. A statement never
// overtakes an earlier one that waits for the same slots, nor, if it
// reserves memory, one that waits for memory, so a large reservation is not
// starved by a stream of small ones. Statements whose limits differ do not
// hold each other up.
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

	mu       sync.Mutex
	reserved int64
	running  map[string]int
	// replicaRunning counts the counted statements running.
	replicaRunning int
	queue          []*waiter
	// active are the statements running under the controller's limits,
	// whose peaks SampleMemory keeps.
	active map[*waiter]struct{}

	waiting prometheus.Gauge
	waited  prometheus.Counter
}

// waiter is a statement waiting to start.
type waiter struct {
	slots  Slots
	memory int64
	// counted says it counts against the replica's limit.
	counted bool
	ready   chan struct{}
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
	if c == nil || !l.Limited() && !(l.Counted && c.replicaMax > 0) {
		return unlimited, 0, nil
	}
	w := &waiter{slots: l.Slots, memory: c.reservation(l.Memory), counted: l.Counted && c.replicaMax > 0, ready: make(chan struct{})}
	if w.slots.Max == 0 && w.memory == 0 && !w.counted {
		return unlimited, 0, nil
	}
	c.mu.Lock()
	c.queue = append(c.queue, w)
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

// admit starts every waiting statement that may start, in arrival order.
// The caller holds c.mu.
func (c *Controller) admit() {
	blocked := map[string]bool{}
	memoryBlocked, replicaBlocked := false, false
	kept := c.queue[:0]
	for _, w := range c.queue {
		slotsFree := w.slots.Max == 0 || !blocked[w.slots.Key] && c.running[w.slots.Key] < w.slots.Max
		memoryFree := w.memory == 0 || !memoryBlocked && c.reserved+w.memory <= c.budget
		replicaFree := !w.counted || !replicaBlocked && c.replicaRunning < c.replicaMax
		if slotsFree && memoryFree && replicaFree {
			if w.slots.Max > 0 {
				c.running[w.slots.Key]++
			}
			if w.counted {
				c.replicaRunning++
			}
			c.reserved += w.memory
			c.active[w] = struct{}{}
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
		if !memoryFree {
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
	delete(c.active, w)
	c.admit()
}

// Running reports how many statements counting against key run now.
func (c *Controller) Running(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running[key]
}
