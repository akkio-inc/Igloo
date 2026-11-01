package distributed

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// shrinkLog is a Compute that can stop workers alone, and records what it
// was asked.
type shrinkLog struct {
	scaleLog
	mu      sync.Mutex
	stopped [][]RetiredWorker
	running []int
	refuse  bool
}

func (s *shrinkLog) Shrink(_ context.Context, retired []RetiredWorker, running int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refuse {
		return false, nil
	}
	s.stopped, s.running = append(s.stopped, retired), append(s.running, running)
	return true, nil
}

// While some workers work, the idle ones past one more are retired, take
// no more tasks and are left out of statements, and once retired a while
// are stopped alone; a retired worker serves again when a statement wants
// more workers than run; a worker whose cache outlives it is retired only
// once purged of what erasure requests deleted; and all idle, none are
// retired, as stopIdle stops them together.
func TestShrink(t *testing.T) {
	defer func(d time.Duration) { retireSettle = d }(retireSettle)
	retireSettle = 0
	h := newHarness(t, 0.01, 2, 4)
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	var addrs []string
	for _, ln := range h.lns {
		addrs = append(addrs, ln.Addr().String())
	}
	compute := &shrinkLog{}
	p := newPool(PoolOptions{Addresses: addrs, Compute: compute, MaxWorkers: 4, IdleAfter: time.Minute, Warm: 1}, tr, "test", nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	erased := int64(5)
	p.erased = func() int64 { return erased }
	ctx := t.Context()
	gone := "worker/ephemeral:60/w/cache"
	ws := func(states ...workerStatus) []*worker {
		out := make([]*worker, len(states))
		for i, st := range states {
			st.ID = addrs[i]
			out[i] = &worker{addr: addrs[i], status: st}
		}
		return out
	}

	// One busy, three idle for 2, 5 and 3 minutes: the busy one and the
	// least idle stay; the other two retire.
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone},
		workerStatus{IdleSeconds: 300, Cache: gone}, workerStatus{IdleSeconds: 180, Cache: gone}))
	retiring := map[string]bool{}
	for _, w := range p.answering(ctx) {
		retiring[w.addr] = w.status.Retiring
	}
	if retiring[addrs[0]] || retiring[addrs[1]] || !retiring[addrs[2]] || !retiring[addrs[3]] {
		t.Fatalf("retiring: %v", retiring)
	}
	if n := len(p.usable(p.refresh(ctx))); n != 2 {
		t.Errorf("%d usable workers, want the 2 not retired", n)
	}
	// The harness's coordinator, which knew all four, runs a statement on
	// the two others: the retired ones turn its tasks away.
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}

	// Retired a while: stopped alone, two of four.
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone},
		workerStatus{Retiring: true, Cache: gone}, workerStatus{Retiring: true, Cache: gone}))
	if len(compute.stopped) != 1 || len(compute.stopped[0]) != 2 || compute.running[0] != 4 || len(p.retired) != 0 {
		t.Fatalf("stopped %v of %v, still retired %v", compute.stopped, compute.running, p.retired)
	}
	// The test's Compute stops nothing: the two run on, retired.
	for _, a := range addrs[2:] {
		if _, err := p.retire(ctx, a, false); err != nil {
			t.Fatal(err)
		}
	}

	// Wanting more than run, a retired worker not stopped yet serves again.
	for _, a := range addrs[2:] {
		if _, err := p.retire(ctx, a, true); err != nil {
			t.Fatal(err)
		}
		p.retired[a] = time.Now()
	}
	p.refresh(ctx)
	if got, err := p.ensure(ctx, 3, false); err != nil || len(got) != 3 {
		t.Fatalf("ensure 3: %d, %v", len(got), err)
	}
	back := 0
	for _, a := range addrs[2:] {
		if st, err := p.status(ctx, a); err == nil && !st.Retiring {
			back++
		}
	}
	if back != 1 {
		t.Errorf("%d workers taken back, want 1", back)
	}
	p.unretire(ctx, 4)

	// A cache that outlives its worker, purged through 3 of 5: kept up.
	// Purged through 5: retired.
	kept := "worker/kept/w/cache"
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone},
		workerStatus{IdleSeconds: 300, Cache: kept, Erased: 3}))
	if st, _ := p.status(ctx, addrs[2]); st.Retiring {
		t.Error("retired a worker whose cache outlives it before it purged")
	}
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone},
		workerStatus{IdleSeconds: 300, Cache: kept, Erased: 5}))
	if st, _ := p.status(ctx, addrs[2]); !st.Retiring {
		t.Error("kept up a worker purged of every deletion")
	}
	p.unretire(ctx, 4)

	// All idle: left to stopIdle.
	p.shrink(ctx, ws(workerStatus{IdleSeconds: 300, Cache: gone}, workerStatus{IdleSeconds: 300, Cache: gone},
		workerStatus{IdleSeconds: 300, Cache: gone}))
	for _, w := range p.answering(ctx) {
		if w.status.Retiring {
			t.Errorf("retired %s with no worker busy", w.addr)
		}
	}

	// A Compute that stops only all workers: none retired.
	all := newPool(PoolOptions{Addresses: addrs, Compute: &scaleLog{}, MaxWorkers: 4, IdleAfter: time.Minute, Warm: 1}, tr, "test", nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	all.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 300, Cache: gone}, workerStatus{IdleSeconds: 300, Cache: gone}))
	for _, w := range all.answering(ctx) {
		if w.status.Retiring {
			t.Errorf("retired %s without a Compute that stops it alone", w.addr)
		}
	}

	// Retired, but the workers keep changing so Compute stops none: after
	// retireGiveUp it serves again.
	defer func(d time.Duration) { retireGiveUp = d }(retireGiveUp)
	compute.refuse = true
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone}, workerStatus{IdleSeconds: 300, Cache: gone}))
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone}, workerStatus{Retiring: true, Cache: gone}))
	if st, _ := p.status(ctx, addrs[2]); !st.Retiring || len(p.retired) != 1 {
		t.Fatalf("not stopped: retiring %v, retired %v", st.Retiring, p.retired)
	}
	retireGiveUp = 0
	p.shrink(ctx, ws(workerStatus{Running: 1, Cache: gone}, workerStatus{IdleSeconds: 120, Cache: gone}, workerStatus{Retiring: true, Cache: gone}))
	if st, _ := p.status(ctx, addrs[2]); st.Retiring || len(p.retired) != 0 {
		t.Errorf("given up: retiring %v, retired %v", st.Retiring, p.retired)
	}
}
