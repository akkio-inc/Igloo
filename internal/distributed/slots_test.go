package distributed

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"
)

// TestRetryShape runs a statement again after a task ran out of memory in
// smaller tasks: more partitions, as many as their writers have memory
// for. Where their writers have memory for less than twice the partitions,
// a task over one partition ran out of memory again over about the same;
// such a statement's tasks take twice a worker's slots instead, and twice
// the memory, with which their writers write twice the partitions; once
// they take all of a worker's slots, they take no more.
func TestRetryShape(t *testing.T) {
	big := &onePartition{rows: 1e12}
	capped := func(slots int) int { return 300 * slots }
	none := func(int) int { return 0 }
	for _, c := range []struct {
		name                string
		parts, slots        int
		one                 *onePartition
		most                int
		writers             func(int) int
		wantParts, wantSlot int
	}{
		{"one partition, writers short", 256, 1, big, 2, capped, 600, 2},
		{"once more, every slot taken", 600, 2, big, 2, capped, 600, 2},
		{"one partition, writers short, workers of 4 slots", 600, 2, big, 4, capped, maxRetryPartitions, 4},
		{"one partition, writers with room", 202, 1, big, 2, none, 808, 1},
		{"any task, writers short", 256, 1, nil, 2, capped, 256, 1},
		{"any task, writers with room", 32, 1, nil, 2, none, 64, 1},
	} {
		parts, slots := retryShape(c.parts, c.slots, c.one, 16, c.most, c.writers)
		if parts != c.wantParts || slots != c.wantSlot {
			t.Errorf("%s: %d partitions, %d slots a task; want %d and %d", c.name, parts, slots, c.wantParts, c.wantSlot)
		}
	}
}

// TestTaskSlots runs a task that takes two of a worker's two slots only
// once the task holding one of them is done, so that it has the worker's
// memory to itself.
func TestTaskSlots(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	w := h.workers[0]
	w.slotCount, w.slots = 2, semaphore.NewWeighted(2)
	holding, release, started := make(chan struct{}), make(chan struct{}), make(chan int, 2)
	taskFault = func(req *taskRequest) error {
		started <- req.Task
		if req.Task == 1 {
			close(holding)
			<-release
		}
		return errors.New("done")
	}
	defer func() { taskFault = nil }()
	ws := h.co.pool.ready(context.Background())
	if len(ws) != 1 {
		t.Fatalf("%d workers", len(ws))
	}
	send := func(id, slots int) <-chan error {
		done := make(chan error, 1)
		go func() {
			task := &taskRequest{Query: "q", Task: id, Version: "test", SQL: "SELECT 1", Slots: slots}
			_, _, err := h.co.dispatch(context.Background(), task, ws, 0, filepath.Join(t.TempDir(), "out.parquet"), nil)
			done <- err
		}()
		return done
	}
	first := send(1, 1)
	<-holding
	<-started
	second := send(2, 2)
	select {
	case id := <-started:
		t.Fatalf("task %d ran while another held one of the two slots it takes", id)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	<-first
	select {
	case id := <-started:
		if id != 2 {
			t.Errorf("task %d ran", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the task that takes both slots did not run once they were free")
	}
	<-second
}
