package workload

import (
	"context"
	"time"
)

// SampleMemory reads, every interval until ctx ends, the memory that
// DuckDB holds for running statements (read), and keeps for each statement
// running under the controller's limits the most of its share of it: its
// peak, which its release returns. Statements beside each other share what
// DuckDB holds by what they reserved, a statement that reserved nothing as if
// it reserved the smallest reservation there is, so a peak is exact for a
// statement alone and otherwise only an indication: FootprintOf takes it for
// what it holds only when it says more than what the statement allocated does.
func (c *Controller) SampleMemory(ctx context.Context, every time.Duration, read func(context.Context) (int64, error)) {
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
		if held, err := read(ctx); err == nil {
			c.sample(held)
		}
	}
}

// sample charges held to the statements running now, each its share.
func (c *Controller) sample(held int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	weight := func(w *waiter) int64 { return max(w.memory, c.budget/minReservationShare, 1) }
	var weights int64
	for w := range c.active {
		weights += weight(w)
	}
	for w := range c.active {
		w.peak = max(w.peak, int64(float64(held)*float64(weight(w))/float64(weights)))
		w.sampled = true
	}
	c.held = held
	if len(c.queue) > 0 {
		// Those waiting for the memory to empty may start now.
		c.admit()
	}
}
