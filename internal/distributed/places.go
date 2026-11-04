package distributed

import (
	"context"
	"maps"
	"slices"
	"sync"
)

// places are a statement's places on its workers: a task is sent to a worker
// once that worker has room for it, as many tasks as its slots run at once.
// Each task has a worker it goes to first (a split table's share of files,
// the worker whose read cache holds them from earlier statements; others,
// each worker in turn), and goes there when that worker has room before any
// other task does; a worker with room that no waiting task goes to first
// takes the task that has waited longest. Tasks wait in the order they came.
//
// A stage sent all of its tasks at once, each to its worker in turn, and
// ended when its slowest worker had run its share: in a report on 16
// workers, one worker, behind with scans of the stages before, started its
// share of a stage of 256 tasks 44 s after the others and ended it 41 s after
// them, and did so again in the two stages after it, while the fleet ran 4 to
// 14 of its 32 slots. A slow or lost worker now runs fewer of a stage's tasks,
// and the others more.
//
// Tasks that run again what a lost worker held take no place: they go first.
type places struct {
	mu sync.Mutex
	// room is how many more tasks each worker may take, by address, and gone
	// the workers the statement went on without.
	room map[string]int
	gone map[string]bool
	// waiting are the tasks waiting for a place, in the order they came.
	waiting []*placeWait
}

// placeWait is a task waiting for a place on one of ws, ws[first] first.
type placeWait struct {
	ws    []*worker
	first int
	// at is the index in ws of the worker it got, once ready is closed.
	at    int
	ready chan struct{}
}

// newPlaces gives each of ws room for as many tasks as its slots run at once,
// of tasks that take slots of them each, and one more, which waits there for
// a slot: a worker gives the slot a task frees to the statement that has
// started the fewest tasks there (see fairSlots), and with none of the
// statement's waiting, its next task, sent once the coordinator heard the
// last one end, found the slot taken by another statement's, and waited for
// that to end too.
func newPlaces(ws []*worker, slots int) *places {
	p := &places{room: map[string]int{}, gone: map[string]bool{}}
	for _, w := range ws {
		n := w.status.Slots
		if n <= 0 {
			n = taskSlots(w.status.Cores, w.status.MemoryBytes)
		}
		p.room[w.addr] = max(n/max(slots, 1), 1) + 1
	}
	return p
}

// take waits for a place on one of ws for a task that goes to ws[first]
// first, or until ctx ends. It returns the index in ws of the worker, and
// release, which gives the place back once the task has run.
func (p *places) take(ctx context.Context, ws []*worker, first int) (int, func(), error) {
	p.mu.Lock()
	w := &placeWait{ws: ws, first: first, ready: make(chan struct{})}
	if len(p.waiting) == 0 {
		if at, ok := p.fits(w); ok {
			p.mu.Unlock()
			return at, p.releaser(ws[at].addr), nil
		}
	}
	p.waiting = append(p.waiting, w)
	p.mu.Unlock()
	select {
	case <-w.ready:
		return w.at, p.releaser(ws[w.at].addr), nil
	case <-ctx.Done():
		p.mu.Lock()
		defer p.mu.Unlock()
		select {
		case <-w.ready:
			// Given a place as ctx ended: back to the others.
			p.room[ws[w.at].addr]++
			p.hand()
		default:
			p.waiting = slices.DeleteFunc(p.waiting, func(o *placeWait) bool { return o == w })
		}
		return 0, nil, context.Cause(ctx)
	}
}

// fits gives w a place, its first worker's if that has room, or else that of
// the worker of ws with the most room, if any has. p.mu is held.
func (p *places) fits(w *placeWait) (int, bool) {
	if a := w.ws[w.first].addr; p.room[a] > 0 && !p.gone[a] {
		p.room[a]--
		return w.first, true
	}
	best := -1
	for i, o := range w.ws {
		if !p.gone[o.addr] && p.room[o.addr] > 0 && (best < 0 || p.room[o.addr] > p.room[w.ws[best].addr]) {
			best = i
		}
	}
	if best < 0 {
		return 0, false
	}
	p.room[w.ws[best].addr]--
	return best, true
}

// releaser returns the func that gives back a place on the worker at addr.
func (p *places) releaser(addr string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.room[addr]++
			p.hand()
		})
	}
}

// hand gives the places free to the waiting tasks: each worker's to the first
// task that goes to it first, or else to the task that has waited longest of
// those that may run on it. p.mu is held.
func (p *places) hand() {
	addrs := slices.Sorted(maps.Keys(p.room))
	for given := true; given; {
		given = false
		for _, addr := range addrs {
			if p.room[addr] <= 0 || p.gone[addr] {
				continue
			}
			pick, at := -1, 0
			for i, w := range p.waiting {
				j := slices.IndexFunc(w.ws, func(o *worker) bool { return o.addr == addr })
				if j < 0 {
					continue
				}
				if j == w.first {
					pick, at = i, j
					break
				}
				if pick < 0 {
					pick, at = i, j
				}
			}
			if pick < 0 {
				continue
			}
			w := p.waiting[pick]
			p.waiting = slices.Delete(p.waiting, pick, pick+1)
			p.room[addr]--
			w.at = at
			close(w.ready)
			given = true
		}
	}
}

// leave gives no more places on the worker at addr, which the statement went
// on without: tasks that would go to it first go to the others.
func (p *places) leave(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gone[addr] = true
	p.hand()
}
