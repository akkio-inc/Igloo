package workload

import "sync"

// Reservations from statements' history. A replica's statements share one
// memory pool, so several heavy ones at once spill to disk, and on a disk
// that is busy enough they take many times as long as one after the other:
// a dashboard's burst of queries over billions of rows took 548 s with 8 running at
// once on 8 cores and 22 GB, against 365 s with 2. A fixed limit holds back
// the light statements with the heavy ones, though. The controller instead
// learns what each statement allocated (see Observe), by shape, the text
// with its literals masked, which a dashboard sends again and again with
// other audiences or dates, and gives the next statement of that shape a
// reservation of a fraction of it (see Estimate): a statement that
// allocated more than the replica's memory waits until the others' are
// released, while one that allocated little reserves nothing and starts at
// once. DuckDB 1.5 measures no statement's peak, so the fraction stands in
// for how much of what a statement allocates it holds at once.

// minReservationShare is the smallest reservation, as a share of the
// budget, that an estimate makes; a smaller one is none.
const minReservationShare = 16

// maxFootprints bounds the shapes kept; they are forgotten all at once.
const maxFootprints = 4096

// footprints are what statements of each shape allocated, as of their
// latest runs.
type footprints struct {
	mu        sync.Mutex
	fraction  float64
	allocated map[string]int64
}

// EstimateFromHistory makes the controller reserve, for a statement that
// reserves nothing of its own, fraction of what the latest statement of its
// shape allocated (see Estimate). Zero or less turns that off. It is set
// once, before statements run.
func (c *Controller) EstimateFromHistory(fraction float64) {
	c.history.fraction = max(fraction, 0)
}

// Observe records what a statement of shape allocated over its run.
func (c *Controller) Observe(shape string, allocated int64) {
	if c == nil || shape == "" || c.history.fraction == 0 || allocated <= 0 {
		return
	}
	f := &c.history
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.allocated == nil || len(f.allocated) >= maxFootprints {
		f.allocated = make(map[string]int64)
	}
	f.allocated[shape] = allocated
}

// Estimate returns the memory a statement of shape is to reserve: the
// fraction of what the latest one allocated, or none for a shape not seen
// or whose reservation would be under a sixteenth of the replica's memory.
// Reservations larger than the memory are capped when the statement is
// admitted (see reservation).
func (c *Controller) Estimate(shape string) int64 {
	if c == nil || shape == "" || c.history.fraction == 0 || c.budget <= 0 {
		return 0
	}
	f := &c.history
	f.mu.Lock()
	allocated := f.allocated[shape]
	f.mu.Unlock()
	r := int64(float64(allocated) * f.fraction)
	if r < c.budget/minReservationShare {
		return 0
	}
	return r
}
