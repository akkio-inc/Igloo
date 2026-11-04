package workload

import (
	"context"
	"time"
)

// SampleMemory reads, every interval until ctx ends, the memory that each
// DuckDB, the first and then the second, holds for the statements running in
// it (read), and keeps for each statement running under the controller's
// limits the most of its share of what its DuckDB holds, its peak, and the most
// of all of it, which its release returns (see Seen). Statements beside each
// other share what their DuckDB holds by what they reserved, a statement that
// reserved nothing as if it reserved the smallest reservation there is, so a
// peak is exact for a statement alone and otherwise only an indication:
// FootprintOf takes it for what it holds only when it says more than what the
// statement allocated does, and the most of all of it for what it cannot have
// held more than.
func (c *Controller) SampleMemory(ctx context.Context, every time.Duration, read func(context.Context) (held [2]int64, ok bool)) {
	if c == nil {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if held, ok := read(ctx); ok {
			c.sample(held)
		}
	}
}

// sample charges what each DuckDB holds to the statements running in it, each
// its share.
func (c *Controller) sample(held [2]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	weight := func(w *waiter) int64 { return max(w.memory, c.budget/minReservationShare, 1) }
	for i, running := range [2]map[*waiter]struct{}{c.active, c.apart} {
		var weights int64
		for w := range running {
			weights += weight(w)
		}
		for w := range running {
			w.seen.Peak = max(w.seen.Peak, int64(float64(held[i])*float64(weight(w))/float64(weights)))
			w.seen.Most = max(w.seen.Most, held[i])
			w.seen.Sampled = true
		}
	}
	c.held = held[0]
	if len(c.queue) > 0 {
		// Those waiting for the memory to empty may start now.
		c.admit()
	}
}
