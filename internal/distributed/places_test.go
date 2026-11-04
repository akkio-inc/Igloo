package distributed

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPlaces checks who gets a worker's places: a task goes to its first worker
// while that has room, else to the worker with the most room; once none has,
// a place freed goes to the first task waiting that goes to that worker first,
// else to the task that has waited longest; a task that gives up waiting takes
// nothing; and a worker the statement went on without takes no more tasks.
func TestPlaces(t *testing.T) {
	ws := []*worker{{addr: "a"}, {addr: "b"}, {addr: "c"}}
	for _, w := range ws {
		w.status.Slots = 2
	}
	p := newPlaces(ws, 1)
	ctx := t.Context()
	at := func(first int) (int, func()) {
		t.Helper()
		i, release, err := p.take(ctx, ws, first)
		if err != nil {
			t.Fatal(err)
		}
		return i, release
	}
	// Two places each.
	got := []int{}
	var releases []func()
	for _, first := range []int{0, 0, 0, 1, 2, 2} {
		i, release := at(first)
		got = append(got, i)
		releases = append(releases, release)
	}
	if want := []int{0, 0, 1, 1, 2, 2}; !slices.Equal(got, want) {
		t.Fatalf("places %v, want %v", got, want)
	}
	// None free: they wait, in the order they came.
	type result struct {
		name string
		at   int
	}
	results := make(chan result, 8)
	var waits sync.WaitGroup
	wait := func(name string, ctx context.Context, first int) {
		before := waitingPlaces(p)
		waits.Add(1)
		go func() {
			defer waits.Done()
			i, release, err := p.take(ctx, ws, first)
			if err != nil {
				results <- result{name + " gave up", -1}
				return
			}
			results <- result{name, i}
			_ = release
		}()
		for waitingPlaces(p) == before {
			time.Sleep(time.Millisecond)
		}
	}
	gone, cancel := context.WithCancel(ctx)
	wait("first to a", ctx, 0)
	wait("first to c, gives up", gone, 2)
	wait("first to b", ctx, 1)
	cancel()
	if r := <-results; r.name != "first to c, gives up gave up" {
		t.Fatalf("%v, want the canceled task to give up", r)
	}
	// b's place goes to the task that goes to b first, though a's came before.
	releases[2]()
	if r := <-results; r != (result{"first to b", 1}) {
		t.Errorf("b freed: %v", r)
	}
	// c's goes to the task waiting longest, as none goes to c first.
	releases[4]()
	if r := <-results; r != (result{"first to a", 2}) {
		t.Errorf("c freed: %v", r)
	}
	// A worker gone takes no more: a's places go to the others.
	p.leave("a")
	releases[0]()
	wait("first to a again", ctx, 0)
	releases[5]()
	if r := <-results; r != (result{"first to a again", 2}) {
		t.Errorf("a gone, c freed: %v", r)
	}
	waits.Wait()
}

func waitingPlaces(p *places) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.waiting)
}

// TestPlacesBalanceASlowWorker runs a stage's tasks, each going first to its
// worker in turn, on three workers of one slot, one of them taking 25 times
// as long a task: it runs a few, and the others the rest.
func TestPlacesBalanceASlowWorker(t *testing.T) {
	ws := []*worker{{addr: "slow"}, {addr: "b"}, {addr: "c"}}
	for _, w := range ws {
		w.status.Slots = 1
	}
	p := newPlaces(ws, 1)
	var ran [3]atomic.Int64
	var tasks sync.WaitGroup
	for i := range 60 {
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			at, release, err := p.take(t.Context(), ws, i%len(ws))
			if err != nil {
				t.Error(err)
				return
			}
			defer release()
			ran[at].Add(1)
			if at == 0 {
				time.Sleep(50 * time.Millisecond)
			} else {
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
	tasks.Wait()
	slow, b, c := ran[0].Load(), ran[1].Load(), ran[2].Load()
	if slow+b+c != 60 || slow*4 > min(b, c) {
		t.Errorf("tasks by worker: %d (slow), %d, %d", slow, b, c)
	}
}
