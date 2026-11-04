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
	took    localTimes

	// forms pick the form of each statement shape's FLATTENs (see forms.go).
	forms flattenForms
	// costs say how long statements take and which are light (see costs.go).
	costs costs

	mu       sync.Mutex
	reserved int64
	running  map[string]int
	// replicaRunning counts the counted statements running.
	replicaRunning int
	// longRunning counts the long statements running (see longAlone).
	longRunning int
	queue []*waiter
	// active are the statements running under the controller's limits in the
	// first DuckDB, and apart those running in the second, whose peaks
	// SampleMemory keeps.
	active, apart map[*waiter]struct{}
	// unseen are those of them nothing was known of when they started.
	unseen map[*waiter]struct{}
	// patience is how long a statement nothing is known of waits while none
	// such starts or ends, lastTurn when one last did, and lastLate when one
	// that had waited as long last went (see unseenPatience).
	patience time.Duration
	lastTurn time.Time
	lastLate time.Time
	// lateRunning counts the statements running that started late.
	lateRunning int
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
	// long says it takes the replica's cores for long (see longAlone).
	long bool
	// unseen says nothing is known of what it holds, so that it waits while
	// the memory is nearly full (see full).
	unseen bool
	// overdue says it is unseen and has waited for as long as it does (see
	// unseenPatience), and late that it is the one of them that may start.
	overdue, late bool
	// apart says it runs in the second DuckDB (see Limits.Apart).
	apart bool
	// due is its place in line: when it came, plus how long it takes alone.
	due time.Time
	// released says it has ended, and estimated that history set its memory, not
	// its role or user.
	released, estimated bool
	ready               chan struct{}
	// admitted is set, under the controller's lock, once the statement
	// holds its slot and reservation.
	admitted bool
	// seen is what it was seen holding while it ran (see SampleMemory).
	seen Seen
}

// Seen is what a statement was seen holding while it ran, as SampleMemory read
// what its DuckDB held for the statements running in it: Peak, the most of its
// share of that; Most, the most of all of it, more than which it cannot have
// held; and Sampled, whether a sample fell within its run.
type Seen struct {
	Peak, Most int64
	Sampled    bool
}

// NewController enforces limits against budget bytes of query memory.
func NewController(budget int64) *Controller {
	return &Controller{
		budget:  budget,
		running: make(map[string]int),
		active:  make(map[*waiter]struct{}),
		apart:   make(map[*waiter]struct{}),
		unseen:  make(map[*waiter]struct{}),

		patience: unseenPatience,
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
// ends, which returns what it was seen holding (see SampleMemory). waited is
// how long it was held back.
func (c *Controller) Admit(ctx context.Context, l Limits) (release func() Seen, waited time.Duration, err error) {
	unlimited := func() Seen { return Seen{} }
	long := l.Counted && l.Expect >= longAlone
	if c == nil || !l.Limited() && !(l.Counted && c.replicaMax > 0) && !l.Unseen && !long {
		return unlimited, 0, nil
	}
	w := &waiter{slots: l.Slots, memory: c.reservation(l.Memory), estimated: l.Estimated, counted: l.Counted && c.replicaMax > 0, long: long,
		unseen: l.Unseen, apart: l.Apart, ready: make(chan struct{})}
	if w.slots.Max == 0 && w.memory == 0 && !w.counted && !w.unseen && !w.long {
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
		c.wait(ctx, w)
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
	if l.Elsewhere != nil {
		*l.Elsewhere = func() { c.elsewhere(w) }
	}
	var once sync.Once
	return func() Seen {
		once.Do(func() { c.release(w) })
		c.mu.Lock()
		defer c.mu.Unlock()
		return w.seen
	}, waited, nil
}

// elsewhere has w, running, hold nothing of this replica from now on: it runs on
// workers, and what history said it would hold here and its place among the
// statements at once keep nobody from starting here. Its role's or user's limits
// stay, its slots and the memory they set, as they go by what its statements do
// wherever they run.
func (c *Controller) elsewhere(w *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w.released {
		return
	}
	if w.estimated {
		c.reserved -= w.memory
		w.memory = 0
	}
	if w.counted {
		c.replicaRunning--
		w.counted = false
	}
	if w.long {
		c.longRunning--
		w.long = false
	}
	delete(c.active, w)
	delete(c.apart, w)
	c.turn(w)
	c.notLate(w)
	c.admit()
}

// turn has w, running, no longer count as one nothing is known of that runs; if it
// did, and did not start late, the line of such has moved (see unseenPatience). The
// caller holds c.mu, before notLate.
func (c *Controller) turn(w *waiter) {
	if _, ok := c.unseen[w]; ok {
		delete(c.unseen, w)
		if !w.late {
			c.lastTurn = time.Now()
		}
	}
}

// notLate has w, running, no longer count as the late statement that runs. The
// caller holds c.mu.
func (c *Controller) notLate(w *waiter) {
	if w.late && w.admitted {
		c.lateRunning--
		w.late = false
	}
}

// wait waits until w may start, or ctx ends. One nothing is known of stops waiting
// for the memory once it has waited patience while no other such started or ended,
// one of them at a time (see admit).
func (c *Controller) wait(ctx context.Context, w *waiter) {
	var late <-chan time.Time
	if w.unseen {
		timer := time.NewTimer(c.patience)
		defer timer.Stop()
		late = timer.C
	}
	for {
		select {
		case <-w.ready:
			return
		case <-ctx.Done():
			return
		case <-late:
			c.mu.Lock()
			w.overdue, late = true, nil
			c.admit()
			if !w.admitted {
				// Again once the line may have stood still for patience, and the
				// last late one had the time to grow.
				late = time.After(max(c.patience-time.Since(c.lastTurn), unseenGrows-time.Since(c.lastLate), time.Second))
			}
			c.mu.Unlock()
		}
	}
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
	if c.lateRunning == 0 && time.Since(c.lastLate) >= unseenGrows && time.Since(c.lastTurn) >= c.patience &&
		!slices.ContainsFunc(c.queue, func(w *waiter) bool { return w.late }) {
		// The first overdue in line goes, whatever is held (see unseenPatience).
		if i := slices.IndexFunc(c.queue, func(w *waiter) bool { return w.overdue }); i >= 0 {
			c.queue[i].late, c.lastLate = true, time.Now()
		}
	}
	blocked := map[string]bool{}
	memoryBlocked, replicaBlocked, longBlocked := false, false, false
	kept := c.queue[:0]
	for _, w := range c.queue {
		slotsFree := w.slots.Max == 0 || !blocked[w.slots.Key] && c.running[w.slots.Key] < w.slots.Max
		memoryFree := w.memory == 0 && !(w.unseen && !w.late && c.full()) || w.memory > 0 && !memoryBlocked && c.reserved+w.memory <= c.budget && c.heldLeaves(w.memory)
		replicaFree := !w.counted || !replicaBlocked && c.replicaRunning < c.replicaMax
		coresFree := !w.long || !longBlocked && c.longRunning == 0
		if slotsFree && memoryFree && replicaFree && coresFree {
			if w.slots.Max > 0 {
				c.running[w.slots.Key]++
			}
			if w.counted {
				c.replicaRunning++
			}
			if w.long {
				c.longRunning++
			}
			c.reserved += w.memory
			if w.apart {
				c.apart[w] = struct{}{}
			} else {
				c.active[w] = struct{}{}
			}
			if w.unseen {
				c.unseen[w] = struct{}{}
				if !w.late {
					c.lastTurn = time.Now()
				}
			}
			if w.late {
				c.lateRunning++
			}
			w.admitted = true
			close(w.ready)
			continue
		}
		// Later statements with the same slots wait behind this one
		// whatever it waits for, and so do later long ones behind a long one;
		// others reserving memory only if it waits for memory, and other
		// counted ones only if it waits for the replica's limit, not when it
		// waits for its slots alone.
		if w.slots.Max > 0 {
			blocked[w.slots.Key] = true
		}
		if w.long {
			longBlocked = true
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

// unseenPatience is how long statements nothing is known of wait for the memory while
// none such starts or ends. What holds it may not end for hours: from no history, a
// report at a hundred times the data ran on one machine, on an estimate of 16 hours,
// its memory full, and a statement of a shape not seen yet that would have run on
// workers waited behind it, planning nothing, for as long as it ran. Past this they
// start whatever is held, one at a time: the next once the one before has ended, or
// gone to workers, as the line has not moved for them. While such statements start
// and end in their turn, as a dashboard's do the first time, the line moves, and none
// starts beside another: a dashboard's 324 statements at ten times its data, with no
// history, waited up to ten minutes each. Each waiting at most a minute, 45 started
// beside another and the dashboard took 1,916 s; with the line moving, 1,787 s.
const unseenPatience = time.Minute

// unseenGrows is how long after one that waited its patience the next may start.
const unseenGrows = 30 * time.Second

// full reports whether a statement nothing is known of must wait: another such
// runs, whose memory cannot show yet in what DuckDB holds and may grow to all of
// it, or DuckDB holds, or statements reserved, nearly all the budget. What they
// hold would make those running spill. One whose plan says it holds next to
// nothing is not taken for unknown (see Slight). Reservations cannot say so
// the first time a dashboard runs on a replica, when none of its statements has
// any: its heavy ones started together and spilled. Its 324 statements at ten
// times its data, from no history, took 3,145 s with up to three such at once
// while they were young, and 29,400 CPU-seconds where they take 14,000 once known;
// one at a time, 1,787 s and 14,100 CPU-seconds. The caller holds c.mu.
func (c *Controller) full() bool {
	if c.budget <= 0 || len(c.active) == 0 {
		return false
	}
	return len(c.unseen) > 0 || float64(max(c.held, c.reserved)) >= fullShare*float64(c.budget)
}

// heldLeaves reports whether what DuckDB holds for the statements running leaves
// room for a reservation of memory, of which a statement that reserves more than
// three quarters of the budget needs only that much, so that it does not wait for
// the short statements that reserve nothing to stop altogether. Reservations are
// estimates, and those under a sixteenth of the budget count for nothing: 8
// statements of a dashboard at ten times its data reserved 15.6 GiB of 20.4, held all
// of it and spilled 10 GiB more. Its 324 queries took 1,945 s with this, not 2,086.
// While one nothing is known of runs, DuckDB counts as holding nearly all the
// budget (see full), so that only a reservation of a quarter of it or less starts
// beside it. The caller holds c.mu.
func (c *Controller) heldLeaves(memory int64) bool {
	held := c.held
	if len(c.unseen) > 0 {
		held = max(held, int64(fullShare*float64(c.budget)))
	}
	return len(c.active) == 0 || held+min(memory, c.budget/4*3) <= c.budget
}

// Crowded reports whether a statement nothing is known of would wait to start now
// (see full): only then is what it holds worth estimating first (see Slight).
func (c *Controller) Crowded() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.full()
}

// Slight reports whether a statement nothing like has run of holds too little of
// the budget to count, as a reservation under a sixteenth of it does (see
// estimate), by what estimate says it holds, planShort times over; ok false says it
// cannot say. One that holds that little starts as a known one that holds as little
// does. Without a budget every statement is slight, and estimate is not called.
func (c *Controller) Slight(estimate func() (memory int64, ok bool)) bool {
	if c == nil || c.budget <= 0 {
		return true
	}
	memory, ok := estimate()
	return ok && memory*planShort < c.budget/minReservationShare
}

// planShort is how many times what a plan says a statement holds Slight takes it
// to hold. A dashboard's statements at ten times its data, joins and distinct counts
// that plans put at 0.1 to 1.1 GB, held 0.5 to 7.4 GB each, up to 45 times as
// much, and started together on 22 GB they spilled: the dashboard took 2,536 s.
// Taken sixteen times over, none of them counts as slight there, and it took 1,787 s,
// while a dashboard's tiles over a billion rows, which plans put at nothing, do.
const planShort = 16

// HeldBehindSecond reports whether statements wait in line while all that run
// run in the second DuckDB: what they wait for is that one's memory, so the
// first's caches keep nothing for anyone.
func (c *Controller) HeldBehindSecond() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.apart) > 0 && len(c.active) == 0 && len(c.queue) > 0
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
	w.released = true
	c.turn(w)
	c.notLate(w)
	if w.slots.Max > 0 {
		if c.running[w.slots.Key]--; c.running[w.slots.Key] <= 0 {
			delete(c.running, w.slots.Key)
		}
	}
	if w.counted {
		c.replicaRunning--
	}
	if w.long {
		c.longRunning--
	}
	c.reserved -= w.memory
	delete(c.active, w)
	delete(c.apart, w)
	c.admit()
}

// longAlone is how long a statement takes alone, its CPU time over the replica's
// cores (see Limits.Expect), to be long. Such statements use most of the cores
// for seconds on end: two beside each other each get about half, and neither
// ends sooner than one after the other would. So a long statement waits while
// another runs, in their order, and the others keep starting. Two of a
// dashboard's statements at ten times its data, of 15 s and 9 s alone on 16
// cores, held 20 to 30 GB each: once what they reserved said so, they started
// beside each other and beside the dashboard's other statements, and took 57 to
// 63 s and 45 to 57 s rather than 22 and 36 to 44 one after the other. Of a
// heavier dashboard's 324 statements, the 47 of 5 s or more used 10 to 16 of
// the 16 cores while they ran.
const longAlone = 5 * time.Second

// Running reports how many statements counting against key run now.
func (c *Controller) Running(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running[key]
}
