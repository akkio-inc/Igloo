package distributed

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fastBeats shortens heartbeats for a test.
func fastBeats(t *testing.T, every, timeout, patience time.Duration) {
	t.Helper()
	was1, was2, was3 := taskCheckEvery, taskCheckTimeout, heartbeatPatience
	t.Cleanup(func() { taskCheckEvery, taskCheckTimeout, heartbeatPatience = was1, was2, was3 })
	taskCheckEvery, taskCheckTimeout, heartbeatPatience = every, timeout, patience
}

// lostIn reports whether lost closed within d.
func lostIn(lost <-chan struct{}, d time.Duration) bool {
	select {
	case <-lost:
		return true
	case <-time.After(d):
		return false
	}
}

// TestHeartbeats checks when a worker is taken for lost: once it missed its answers in
// a row while another answered; not while it answers, however late within the
// timeout, nor for misses with answers between; and when nobody answers, only after
// heartbeatPatience, as this end may be what went silent. Those waiting on it share
// its heartbeat, and it ends with the last of them.
func TestHeartbeats(t *testing.T) {
	fastBeats(t, 10*time.Millisecond, 50*time.Millisecond, 300*time.Millisecond)
	var silent, flaky, checks atomic.Int64
	h := newHeartbeats(func(ctx context.Context, addr string) error {
		checks.Add(1)
		switch {
		case addr == "silent" && silent.Load() == 1, addr == "both" && silent.Load() == 1, addr == "alone" && silent.Load() == 1:
			<-ctx.Done()
			return ctx.Err()
		case addr == "flaky" && flaky.Add(1)%taskCheckMissesInt() == 0:
			return errors.New("no answer")
		case addr == "late":
			time.Sleep(30 * time.Millisecond)
		}
		return nil
	}, nil)

	lateGone, _, releaseLate := h.watch("late")
	defer releaseLate()
	flakyGone, _, releaseFlaky := h.watch("flaky")
	defer releaseFlaky()
	gone, why, release := h.watch("silent")
	_, _, again := h.watch("silent")
	if lostIn(gone, 200*time.Millisecond) {
		t.Fatal("a worker that answers was taken for lost")
	}
	silent.Store(1)
	start := time.Now()
	if !lostIn(gone, 5*time.Second) {
		t.Fatal("a worker that stopped answering was not taken for lost")
	}
	if took := time.Since(start); took > heartbeatPatience {
		t.Errorf("it took %s, past the patience there is with others answering", took)
	}
	if !errors.Is(why(), context.DeadlineExceeded) {
		t.Errorf("why: %v", why())
	}
	if !h.lostWithin("silent", time.Minute) || h.lostWithin("late", time.Minute) {
		t.Error("lostWithin is wrong")
	}
	release()
	again()
	if lostIn(lateGone, 0) || lostIn(flakyGone, 0) {
		t.Error("a worker answering late, or with some answers missed, was taken for lost")
	}
	releaseLate()
	releaseFlaky()

	// Nobody answers: patience first.
	silent.Store(0)
	quiet, _, releaseQuiet := h.watch("alone")
	defer releaseQuiet()
	both, _, releaseBoth := h.watch("both")
	defer releaseBoth()
	time.Sleep(50 * time.Millisecond)
	silent.Store(1)
	began := time.Now()
	if lostIn(both, heartbeatPatience/2) || lostIn(quiet, 0) {
		t.Errorf("workers were taken for lost %s after all went silent, this end perhaps", time.Since(began))
	}
	if !lostIn(both, 5*time.Second) || !lostIn(quiet, 5*time.Second) {
		t.Error("workers all silent past the patience were not taken for lost")
	}

	// With nobody else to ask, what the worker misses counts.
	solo, _, releaseSolo := h.watch("silent")
	defer releaseSolo()
	began = time.Now()
	if !lostIn(solo, 5*time.Second) {
		t.Fatal("the only worker waited on was not taken for lost")
	}
	if took := time.Since(began); took >= heartbeatPatience {
		t.Errorf("the only worker waited on took %s", took)
	}

	// Once nobody waits, nobody asks.
	releaseQuiet()
	releaseBoth()
	releaseSolo()
	time.Sleep(100 * time.Millisecond)
	seen := checks.Load()
	time.Sleep(100 * time.Millisecond)
	if n := checks.Load(); n != seen {
		t.Errorf("%d checks after everyone stopped waiting", n-seen)
	}
}

func taskCheckMissesInt() int64 { return int64(taskCheckMisses) }

// frozenWorker has h's worker i stop serving on the first freeze, its connections open
// and its kernel taking what is sent, as a machine that lost power closes nothing:
// not a killed process.
func frozenWorker(t *testing.T, h *harness, i int) *freezer {
	t.Helper()
	f := &freezer{thaw: make(chan struct{})}
	h.wrap = func(n int, ln net.Listener) net.Listener {
		if n != i {
			return ln
		}
		f.Listener = ln
		return f
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := h.workers[i].Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	h.startWorker(i)
	// Before the worker's own cleanup, which waits for its tasks.
	t.Cleanup(func() { close(f.thaw) })
	return f
}

// TestUnresponsiveWorkerFoundInSeconds has a worker stop answering in the middle of a
// task, closing no socket, at the real heartbeat's pace: the coordinator takes it for
// lost within seconds and runs its tasks on the others, and the result is complete.
func TestUnresponsiveWorkerFoundInSeconds(t *testing.T) {
	h := newHarness(t, 0.02, 6, 3)
	f := frozenWorker(t, h, 1)
	var frozenAt, foundAt atomic.Int64
	h.workers[1].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
		frozenAt.CompareAndSwap(0, time.Now().UnixNano())
		f.frozen.Store(true)
		select {
		case <-r.Context().Done():
		case <-f.thaw:
		}
		return false
	}
	hook := func(r slog.Record) {
		if strings.Contains(r.Message, "stopped answering while it ran a task") {
			foundAt.CompareAndSwap(0, time.Now().UnixNano())
		}
	}
	h.hook.Store(&hook)
	if !h.check(failureQuery) {
		t.Fatal("did not run distributed")
	}
	if frozenAt.Load() == 0 || foundAt.Load() == 0 {
		t.Fatalf("frozen at %d, found at %d", frozenAt.Load(), foundAt.Load())
	}
	if took := time.Duration(foundAt.Load() - frozenAt.Load()); took > 12*time.Second {
		t.Errorf("the worker was taken for lost %s after it stopped answering", took)
	}
}

// TestUnresponsiveWorkerWithExchanges has a worker stop answering once its tasks wrote
// exchange files to its disk, closing no socket, with readers about to fetch them: they
// give up on it within seconds rather than a minute, and the result is complete.
func TestUnresponsiveWorkerWithExchanges(t *testing.T) {
	h := localHarness(t, 3)
	f := frozenWorker(t, h, 1)
	var ran atomic.Int32
	var frozenAt, foundAt atomic.Int64
	hook := func(r slog.Record) {
		if strings.HasPrefix(r.Message, "a worker holding exchanged rows on its disk was lost") {
			foundAt.CompareAndSwap(0, time.Now().UnixNano())
		}
		if r.Message == "ran a stage" && strings.Contains(attr(r, "stage"), " to exchange ") && ran.Add(1) == 2 {
			frozenAt.Store(time.Now().UnixNano())
			f.frozen.Store(true)
		}
	}
	h.hook.Store(&hook)
	want, _, err := h.query(context.Background(), exchangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Error(msg)
	}
	if frozenAt.Load() == 0 || foundAt.Load() == 0 {
		t.Fatalf("frozen at %d, found at %d", frozenAt.Load(), foundAt.Load())
	}
	if took := time.Duration(foundAt.Load() - frozenAt.Load()); took > 20*time.Second {
		t.Errorf("the worker was taken for lost %s after it stopped answering", took)
	}
}
