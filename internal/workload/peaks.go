package workload

import (
	"context"
	"time"
)

// SampleMemory reads, every interval until ctx ends, the memory that
// DuckDB holds for running statements (read), and keeps for each statement
// running under the controller's limits the most of it that the
// statements beside it had not reserved: its peak, which its release
// returns. Statements beside it that hold more than they reserved, or
// reserved nothing, are counted in its peak too, so a peak errs high, and
// FootprintOf takes the smaller of it and what the statement allocated.
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

// sample charges held to the statements running now, less what the others
// reserved.
func (c *Controller) sample(held int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for w := range c.active {
		w.peak = max(w.peak, held-(c.reserved-w.memory))
		w.sampled = true
	}
}
