package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestFairSlots checks who gets a worker's slots: a task that runs again what a lost
// worker held, before all others; then the statement that has started the
// fewest tasks, the first of them to come; that one that needs more than are free
// holds up the rest; and that a task that gives up waiting takes nothing with it.
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
			if err := f.acquire(ctx, query, n, false, false); err != nil {
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
	if err := f.acquire(ctx, "report", 1, false, false); err != nil {
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
	if err := f.acquire(ctx, "a", 1, false, false); err != nil {
		t.Fatal(err)
	}
	both := make(chan error, 1)
	go func() { both <- f.acquire(ctx, "b", 2, false, false) }()
	for waitingCount(f) == 0 {
		time.Sleep(time.Millisecond)
	}
	one := make(chan error, 1)
	go func() { one <- f.acquire(ctx, "c", 1, false, false) }()
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

	// A task that runs again what a lost worker held goes before tasks that came
	// before it, its own statement's and another's.
	queue := func(query, name string, first bool) {
		t.Helper()
		before := waitingCount(f)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f.acquire(ctx, query, 1, first, false); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			f.release(query, 1)
		}()
		for waitingCount(f) == before {
			time.Sleep(time.Millisecond)
		}
	}
	for _, c := range []struct {
		queue []string
		want  []string
	}{
		{[]string{"report 2", "dashboard", "report again"}, []string{"report again", "dashboard", "report 2"}},
	} {
		f = newFairSlots(1)
		if err := f.acquire(ctx, "report", 1, false, false); err != nil {
			t.Fatal(err)
		}
		order = nil
		for _, name := range c.queue {
			query := "report"
			if name == "dashboard" {
				query = "dashboard"
			}
			queue(query, name, strings.HasSuffix(name, "again"))
		}
		f.release("report", 1)
		wg.Wait()
		if !slices.Equal(order, c.want) {
			t.Errorf("%v, want %v", order, c.want)
		}
	}

	// One that gives up waiting leaves things as they were.
	f = newFairSlots(1)
	if err := f.acquire(ctx, "a", 1, false, false); err != nil {
		t.Fatal(err)
	}
	gone, cancel := context.WithCancel(ctx)
	failed := make(chan error, 1)
	go func() { failed <- f.acquire(gone, "b", 1, false, false) }()
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

// TestBackgroundSlots checks that a background statement's tasks take a worker's
// slots after other statements', only once no other statement's task has run or
// waited there for a while; and that one of them goes first once none has
// started for a while.
func TestBackgroundSlots(t *testing.T) {
	ctx := context.Background()
	f := newFairSlots(4)
	f.quietFor, f.ageFor = 50*time.Millisecond, time.Hour
	// The refresh takes every slot while nothing else runs.
	for range 4 {
		if err := f.acquire(ctx, "refresh", 1, false, true); err != nil {
			t.Fatal(err)
		}
	}
	queue, next, none := slotTurns(t, f)
	queue("refresh", "refresh 5", true)
	queue("tile 1", "tile 1", false)
	queue("tile 2", "tile 2", false)
	queue("tile 3", "tile 3", false)
	// As the refresh's tasks end, the tiles' go first, and the refresh's wait
	// for them, and for a while after them, though a slot is free.
	for _, want := range []string{"tile 1", "tile 2", "tile 3"} {
		f.release("refresh", 1)
		next(want)
	}
	f.release("refresh", 1)
	none("while the tiles ran")
	if err := f.acquire(ctx, "tile 4", 1, false, false); err != nil {
		t.Fatal(err)
	}
	f.release("tile 1", 1)
	f.release("tile 2", 1)
	f.release("tile 3", 1)
	none("while a tile ran")
	start := time.Now()
	f.release("tile 4", 1)
	next("refresh 5")
	if waited := time.Since(start); waited < 40*time.Millisecond {
		t.Errorf("the refresh started %s after the last tile ended", waited)
	}
	f.release("refresh", 1)
	settled(t, f, 4)

	// One slot: one of the refresh's tasks goes before the tiles' once none of
	// its own has started for ageFor.
	f = newFairSlots(1)
	f.quietFor, f.ageFor = time.Hour, 50*time.Millisecond
	if err := f.acquire(ctx, "tile a", 1, false, false); err != nil {
		t.Fatal(err)
	}
	queue, next, none = slotTurns(t, f)
	queue("refresh", "refresh 1", true)
	queue("tile b", "tile b", false)
	// None of its tasks has started here: the first goes first.
	f.release("tile a", 1)
	next("refresh 1")
	queue("refresh", "refresh 2", true)
	queue("tile c", "tile c", false)
	f.release("refresh", 1)
	next("tile b")
	queue("tile d", "tile d", false)
	f.release("tile b", 1)
	next("tile c")
	time.Sleep(60 * time.Millisecond)
	f.release("tile c", 1)
	next("refresh 2")
	none("while the refresh ran")
	f.release("refresh", 1)
	next("tile d")
	f.release("tile d", 1)
	settled(t, f, 1)
}

// slotTurns returns, for f's tests, what queues a task of query, called name
// as it gets a slot, and waits for it to wait; what waits for the next task to
// get a slot, which should be want; and what checks that none does for a while.
func slotTurns(t *testing.T, f *fairSlots) (queue func(query, name string, background bool), next func(want string), none func(after string)) {
	got := make(chan string, 16)
	queue = func(query, name string, background bool) {
		t.Helper()
		before := waitingCount(f)
		go func() {
			if err := f.acquire(context.Background(), query, 1, false, background); err != nil {
				t.Error(err)
				return
			}
			got <- name
		}()
		for waitingCount(f) == before {
			time.Sleep(time.Millisecond)
		}
	}
	next = func(want string) {
		t.Helper()
		select {
		case name := <-got:
			if name != want {
				t.Errorf("%s got the slot, want %s", name, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no one got the slot, want %s", want)
		}
	}
	none = func(after string) {
		t.Helper()
		select {
		case name := <-got:
			t.Errorf("%s got the slot %s", name, after)
		case <-time.After(20 * time.Millisecond):
		}
	}
	return queue, next, none
}

// settled checks that f, of n slots, holds and remembers nothing.
func settled(t *testing.T, f *fairSlots, n int64) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queries) != 0 || f.free != n || f.others != 0 || len(f.waiting) != 0 {
		t.Errorf("after all: %d statements, %d free, %d others, %d waiting", len(f.queries), f.free, f.others, len(f.waiting))
	}
}

// TestBackgroundTasks checks that the tasks of a statement executed under
// WithBackground say so to the workers, and others' do not.
func TestBackgroundTasks(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	var mu sync.Mutex
	seen := map[bool]int{}
	for _, w := range h.workers {
		w.beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return false
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var req taskRequest
			if json.Unmarshal(body, &req) == nil {
				mu.Lock()
				seen[req.Background]++
				mu.Unlock()
			}
			return true
		}
	}
	for _, background := range []bool{false, true} {
		ctx := t.Context()
		if background {
			ctx = WithBackground(ctx)
		}
		mu.Lock()
		clear(seen)
		mu.Unlock()
		if _, _, _, _, err := h.run(ctx, failureQuery, ModeOn); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		if seen[background] == 0 || seen[!background] != 0 {
			t.Errorf("in the background %v: tasks that said so %d, that did not %d", background, seen[true], seen[false])
		}
		mu.Unlock()
	}
}
