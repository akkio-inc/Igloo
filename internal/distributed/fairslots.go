package distributed

import (
	"context"
	"sync"
)

// fairSlots are a worker's task slots (see taskSlots), which a task waits for. In
// arrival order, a report's stage of many tasks held up every statement that came after
// it: a dashboard's one task waited for all of them. So a slot goes to the task of the
// statement that has started the fewest here, the first of them to come, which lets a
// short statement past a long one's tasks at the cost of one task's wait, and has
// long ones take turns. A task that needs more slots than are free holds up the
// others until they are, or it might never run.
type fairSlots struct {
	mu   sync.Mutex
	free int64
	// waiting are the tasks waiting, in the order they came.
	waiting []*slotWaiter
	// queries are the statements with tasks running or waiting.
	queries map[string]*slotQuery
}

// slotQuery is what fairSlots knows of a statement while it has tasks here.
type slotQuery struct {
	// started is the slots its tasks have taken, and tasks how many of them are
	// running or waiting.
	started int64
	tasks   int
}

// slotWaiter is a task waiting for its slots.
type slotWaiter struct {
	query *slotQuery
	n     int64
	ready chan struct{}
}

func newFairSlots(n int64) *fairSlots {
	return &fairSlots{free: n, queries: map[string]*slotQuery{}}
}

// acquire waits for n slots for a task of query, or until ctx ends.
func (f *fairSlots) acquire(ctx context.Context, query string, n int64) error {
	f.mu.Lock()
	q := f.queries[query]
	if q == nil {
		q = &slotQuery{}
		f.queries[query] = q
	}
	q.tasks++
	w := &slotWaiter{query: q, n: n, ready: make(chan struct{})}
	f.waiting = append(f.waiting, w)
	f.next()
	f.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-w.ready:
		// It had just got them.
		f.free += n
	default:
		for i, other := range f.waiting {
			if other == w {
				f.waiting = append(f.waiting[:i], f.waiting[i+1:]...)
				break
			}
		}
	}
	f.leave(query, q)
	f.next()
	return ctx.Err()
}

// release gives back n slots of a task of query.
func (f *fairSlots) release(query string, n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.free += n
	f.leave(query, f.queries[query])
	f.next()
}

// leave has q, query's, a task fewer, and forgets it with none left.
func (f *fairSlots) leave(query string, q *slotQuery) {
	if q == nil {
		return
	}
	if q.tasks--; q.tasks <= 0 {
		delete(f.queries, query)
	}
}

// next starts the tasks that may start. f.mu is held.
func (f *fairSlots) next() {
	for len(f.waiting) > 0 {
		pick := 0
		for i, w := range f.waiting {
			if w.query.started < f.waiting[pick].query.started {
				pick = i
			}
		}
		w := f.waiting[pick]
		if w.n > f.free {
			return
		}
		f.free -= w.n
		w.query.started += w.n
		f.waiting = append(f.waiting[:pick], f.waiting[pick+1:]...)
		close(w.ready)
	}
}
