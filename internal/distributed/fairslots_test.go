package distributed

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestFairSlots checks who gets a worker's slots: the statement that has started the
// fewest tasks, the first of them to come; that one that needs more than are free holds
// up the rest; and that a task that gives up waiting takes nothing with it.
func TestFairSlots(t *testing.T) {
	ctx := context.Background()
	f := newFairSlots(1)
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	start := func(query, name string, n int64) {
		t.Helper()
		before := waitingCount(f)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.acquire(ctx, query, n); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			f.release(query, n)
		}()
		for deadline := time.Now().Add(5 * time.Second); waitingCount(f) == before && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond)
		}
	}
	// The report holds the slot, with three more tasks waiting, and then a
	// dashboard's one comes, and another statement's two.
	if err := f.acquire(ctx, "report", 1); err != nil {
		t.Fatal(err)
	}
	start("report", "report 2", 1)
	start("report", "report 3", 1)
	start("report", "report 4", 1)
	start("dashboard", "dashboard", 1)
	start("other", "other 1", 1)
	start("other", "other 2", 1)
	f.release("report", 1)
	wg.Wait()
	want := []string{"dashboard", "other 1", "report 2", "other 2", "report 3", "report 4"}
	if len(order) != len(want) {
		t.Fatalf("%v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("%v, want %v", order, want)
			break
		}
	}
	if len(f.queries) != 0 || f.free != 1 {
		t.Errorf("after all: %d statements, %d free", len(f.queries), f.free)
	}

	// A task that needs both slots holds up one that needs one, once it is its turn.
	f = newFairSlots(2)
	if err := f.acquire(ctx, "a", 1); err != nil {
		t.Fatal(err)
	}
	both := make(chan error, 1)
	go func() { both <- f.acquire(ctx, "b", 2) }()
	for waitingCount(f) == 0 {
		time.Sleep(time.Millisecond)
	}
	one := make(chan error, 1)
	go func() { one <- f.acquire(ctx, "c", 1) }()
	for waitingCount(f) == 1 {
		time.Sleep(time.Millisecond)
	}
	select {
	case <-one:
		t.Fatal("a task of one slot went past one of two that came first")
	case <-both:
		t.Fatal("a task of two slots ran with one free")
	case <-time.After(20 * time.Millisecond):
	}
	f.release("a", 1)
	if err := <-both; err != nil {
		t.Fatal(err)
	}
	f.release("b", 2)
	if err := <-one; err != nil {
		t.Fatal(err)
	}
	f.release("c", 1)

	// One that gives up waiting leaves things as they were.
	f = newFairSlots(1)
	if err := f.acquire(ctx, "a", 1); err != nil {
		t.Fatal(err)
	}
	gone, cancel := context.WithCancel(ctx)
	failed := make(chan error, 1)
	go func() { failed <- f.acquire(gone, "b", 1) }()
	for waitingCount(f) == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-failed; err == nil {
		t.Fatal("a canceled task got a slot")
	}
	f.release("a", 1)
	if len(f.queries) != 0 || f.free != 1 || len(f.waiting) != 0 {
		t.Errorf("after one gave up: %d statements, %d free, %d waiting", len(f.queries), f.free, len(f.waiting))
	}
}

func waitingCount(f *fairSlots) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiting)
}
