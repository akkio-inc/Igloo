package distributed

import (
	"context"
	"sync"
	"time"
)

// fairSlots are a worker's task slots (see taskSlots), which a task waits for. In
// arrival order, a report's stage of many tasks held up every statement that came after
// it: a dashboard's one task waited for all of them. So a slot goes to the task of the
// statement that has started the fewest here, the first of them to come, which lets a
// short statement past a long one's tasks at the cost of one task's wait, and has
// long ones take turns. A task that needs more slots than are free holds up the
// others until they are, or it might never run.
//
// A task that runs again what a lost worker held (taskRequest.Repair) goes before
// all of them: its statement's readers wait for it, and as its statement's task it
// came after every task the statement had sent here, and a stage sends all of its
// tasks at once: one of 128 tasks puts 8 or 9 on each of 15 workers. With one of
// 16 workers powered off during a report, the tasks it had held took 44 and 45 s
// to run again.
//
// A statement that runs in the background (taskRequest.Background: a refresh, a
// pipe's load, a statement heavy enough to give way to the others) gives way to
// the others. Beside a full refresh on all 16 workers, a dashboard's tiles on
// workers took 9.7 s at the median and 42 s at the 95th percentile, against 6.6
// and 23 s with none. The fewest-started rule already let their tasks past the
// refresh's waiting ones, but in every lull the refresh's took all the slots,
// and the tiles' next tasks waited for those to end, or ran beside them, each
// task on all of the worker's threads. So background statements' tasks start
// only once no other statement's task has run or waited here for
// backgroundQuiet, and one of them goes before the others' once none has
// started here for backgroundAge: so dashboards that keep the workers busy slow
// them, and never stop them. With a floor of a quarter of the slots besides,
// which they got before other tasks, mixed windows ran 100 to 112 dashboards a
// minute against 190.7 before, those on workers taking 87 to 165 s at the 95th
// percentile beside a full refresh, against 38 to 46 s.
type fairSlots struct {
	mu   sync.Mutex
	free int64
	// started is when a background statement's task last started here.
	started time.Time
	// others is how many tasks of statements not in the background run or
	// wait here, and quiet when the last of them ended.
	others int
	quiet  time.Time
	// quietFor and ageFor are backgroundQuiet and backgroundAge, but in tests;
	// wake, if set, runs next at wakeAt, once a background statement's task may
	// start, for want of anything else that would.
	quietFor, ageFor time.Duration
	wake             *time.Timer
	wakeAt           time.Time
	now              func() time.Time
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
	// background says it runs in the background.
	background bool
}

// slotWaiter is a task waiting for its slots.
type slotWaiter struct {
	query *slotQuery
	n     int64
	// first says it goes before every task that does not.
	first bool
	ready chan struct{}
}

// backgroundKey marks a context whose statements run in the background (see
// WithBackground).
type backgroundKey struct{}

// WithBackground returns ctx marked so that statements executed under it run
// in the background: on each worker, their tasks give way to other
// statements' (see fairSlots). Refreshes, pipes' loads and statements heavy
// enough to give way to the others run so.
func WithBackground(ctx context.Context) context.Context {
	return context.WithValue(ctx, backgroundKey{}, true)
}

// InBackground reports whether ctx is marked by WithBackground.
func InBackground(ctx context.Context) bool {
	b, _ := ctx.Value(backgroundKey{}).(bool)
	return b
}

// backgroundQuiet is how long no other statement's task has run or waited on a
// worker before background statements' tasks start there, and backgroundAge how
// long after one of them last started there one goes before the others' anyway
// (see fairSlots).
const (
	backgroundQuiet = 2 * time.Second
	backgroundAge   = 10 * time.Second
)

func newFairSlots(n int64) *fairSlots {
	return &fairSlots{free: n, quietFor: backgroundQuiet, ageFor: backgroundAge,
		queries: map[string]*slotQuery{}, now: time.Now}
}

// acquire waits for n slots for a task of query, or until ctx ends.
// first, if set, puts it before every waiting task that is not; background
// says query runs in the background.
func (f *fairSlots) acquire(ctx context.Context, query string, n int64, first, background bool) error {
	f.mu.Lock()
	q := f.queries[query]
	if q == nil {
		q = &slotQuery{background: background}
		f.queries[query] = q
	}
	q.tasks++
	if !q.background {
		f.others++
	}
	w := &slotWaiter{query: q, n: n, first: first, ready: make(chan struct{})}
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
	if !q.background {
		if f.others--; f.others == 0 {
			f.quiet = f.now()
		}
	}
	if q.tasks--; q.tasks <= 0 {
		delete(f.queries, query)
	}
}

// next starts the tasks that may start. f.mu is held.
func (f *fairSlots) next() {
	// rank orders the waiting tasks: those that run again what a lost worker
	// held; a background statement's once none of theirs has started for
	// ageFor, however many other tasks wait; other statements'; and then
	// background statements', once the others have been quiet for quietFor, or
	// -1 for none yet.
	now := f.now()
	aged, quiet := f.started.Add(f.ageFor), f.quiet.Add(f.quietFor)
	rank := func(w *slotWaiter) int {
		switch {
		case w.first:
			return 0
		case !w.query.background:
			return 2
		case !now.Before(aged):
			return 1
		case f.others == 0 && !now.Before(quiet):
			return 3
		}
		return -1
	}
	for len(f.waiting) > 0 {
		pick, best, held := -1, -1, false
		for i, w := range f.waiting {
			r := rank(w)
			switch {
			case r < 0:
				held = true
			case pick < 0 || r < best:
				pick, best = i, r
			case r == best && w.query.started < f.waiting[pick].query.started:
				pick = i
			}
		}
		if pick < 0 {
			if held && f.free > 0 {
				// Nothing else may end to start them: a timer does.
				at := aged
				if f.others == 0 && quiet.Before(at) {
					at = quiet
				}
				f.wakeAfter(now, at)
			}
			return
		}
		w := f.waiting[pick]
		if w.n > f.free {
			return
		}
		f.free -= w.n
		w.query.started += w.n
		if w.query.background {
			f.started, aged = now, now.Add(f.ageFor)
		}
		f.waiting = append(f.waiting[:pick], f.waiting[pick+1:]...)
		close(w.ready)
	}
}

// wakeAfter has next run at at, unless it will before. f.mu is held.
func (f *fairSlots) wakeAfter(now, at time.Time) {
	if f.wake != nil && !f.wakeAt.After(at) {
		return
	}
	if f.wake != nil {
		f.wake.Stop()
	}
	f.wakeAt = at
	f.wake = time.AfterFunc(at.Sub(now), func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.wake = nil
		f.next()
	})
}
