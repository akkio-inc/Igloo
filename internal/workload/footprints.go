package workload

import "sync"

// Reservations from statements' history. A replica's statements share one
// memory pool, so several heavy ones at once spill to disk, and on a disk
// that is busy enough they take many times as long as one after the other:
// a dashboard's burst of queries over billions of rows took 548 s with 8 running at
// once on 8 cores and 22 GB, against 365 s with 2. A fixed limit holds back
// the light statements with the heavy ones, though. The controller instead
// learns what each statement held at once (see Observe), by shape, the
// text with its literals masked, which a dashboard sends again and again
// with other audiences or dates, and gives the next statement of that shape
// a reservation of a fraction of it (see Estimate): a statement that held
// more than the replica's memory waits until the others' are released,
// while one that held little reserves nothing and starts at once.
//
// DuckDB 1.5 measures what a statement allocated over its run, which counts
// memory freed and allocated again each time, and the process's peak, not
// a statement's. Reserving what statements allocated, a dashboard's two
// heaviest statements at ten times the data, which allocated 84 GB each on
// a replica of 22 GB, reserved all of it: each waited 50 to 100 s for the
// others to end and then ran alone, and the dashboard's statements ran
// 662 s in all but waited 4,740. The controller now samples what DuckDB
// holds while statements run (see SampleMemory) and learns the smaller of
// that peak and what the statement allocated (see FootprintOf). It learns
// from runs that did not end too (see ObserveUnfinished): heavy statements
// that never ended, cancelled at their timeout, were never learned and
// started together, six or seven at once, until all spilled.

// minReservationShare is the smallest reservation, as a share of the
// budget, that an estimate makes; a smaller one is none.
const minReservationShare = 16

// maxFootprints bounds the shapes kept; they are forgotten all at once.
const maxFootprints = 4096

// footprints are what statements of each shape held at once, as of their
// latest runs.
type footprints struct {
	mu       sync.Mutex
	fraction float64
	held     map[string]int64
}

// EstimateFromHistory makes the controller reserve, for a statement that
// reserves nothing of its own, fraction of what the latest statement of its
// shape held (see Estimate). Zero or less turns that off. It is set
// once, before statements run.
func (c *Controller) EstimateFromHistory(fraction float64) {
	c.history.fraction = max(fraction, 0)
}

// Observe records what a statement of shape held at once on this replica,
// in a run it completed (see FootprintOf); the server seeds them from the
// query history when it starts, as its statements had them under the same
// memory limit.
func (c *Controller) Observe(shape string, footprint int64) {
	c.observe(shape, footprint, false)
}

// ObserveUnfinished records what a statement of shape was seen holding in a
// run that ended before its end, cancelled or failed: at least that much,
// so it raises what a shape's runs held and never lowers it.
func (c *Controller) ObserveUnfinished(shape string, held int64) {
	c.observe(shape, held, true)
}

func (c *Controller) observe(shape string, n int64, atLeast bool) {
	if c == nil || shape == "" || n < 0 {
		return
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held == nil || len(f.held) >= maxFootprints {
		f.held = make(map[string]int64)
	}
	if old, ok := f.held[shape]; atLeast && ok && old >= n {
		return
	}
	f.held[shape] = n
}

// FootprintOf is what Observe learns of a run that allocated allocated
// over its whole run and was seen holding at most peak, if sampled: the
// smaller of the two, since a run allocates at least what it holds at
// once, and peak may count memory that statements beside it held.
// Without a sample, the run was too short to measure, and allocated
// stands in.
func FootprintOf(allocated, peak int64, sampled bool) int64 {
	if !sampled {
		return allocated
	}
	return max(min(allocated, peak), 0)
}

// Estimate returns the memory a statement of shape is to reserve: the
// fraction of what the latest one held, or none for a shape not seen
// or whose reservation would be under a sixteenth of the replica's memory.
// Reservations larger than the memory are capped when the statement is
// admitted (see reservation).
func (c *Controller) Estimate(shape string) int64 {
	if c == nil || shape == "" || c.history.fraction == 0 || c.budget <= 0 {
		return 0
	}
	f := &c.history
	f.mu.Lock()
	held := f.held[shape]
	f.mu.Unlock()
	r := int64(float64(held) * f.fraction)
	if r < c.budget/minReservationShare {
		return 0
	}
	return r
}

// Footprint returns what the latest statement of shape held at once on
// this replica (see Observe); ok is false when none of its shape has run
// here. Auto mode takes it for proof that one machine is not short of
// memory for the statement, whatever its estimate says.
func (c *Controller) Footprint(shape string) (held int64, ok bool) {
	if c == nil || shape == "" {
		return 0, false
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	held, ok = f.held[shape]
	return held, ok
}
