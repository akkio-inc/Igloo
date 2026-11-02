package distributed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
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

// unpowered has the machines of the addresses off reports while a test runs take no
// connection, as a machine without power takes none: a dial waits until it gives up.
func unpowered(t *testing.T, off func(addr string) bool) {
	t.Helper()
	was := reachWorker
	t.Cleanup(func() { reachWorker = was })
	reachWorker = func(ctx context.Context, addr string) error {
		if off(addr) {
			<-ctx.Done()
			return ctx.Err()
		}
		return was(ctx, addr)
	}
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

// TestBusyWorkerNotTakenForLost has a worker whose status answers come past the
// heartbeat's timeout while its tasks keep every core busy, as workers' did under a
// billion rows of count(DISTINCT): its machine takes connections, so it is not taken
// for lost, its tasks finish there, and the rows are one machine's. A worker that is
// powered off in the same statement, answering nothing and taking no connection, still
// is, within seconds. One that takes connections but answers nothing is, after
// heartbeatSilence.
func TestBusyWorkerNotTakenForLost(t *testing.T) {
	fastBeats(t, 50*time.Millisecond, 200*time.Millisecond, 5*time.Second)
	defer func(silence time.Duration) { heartbeatSilence = silence }(heartbeatSilence)
	heartbeatSilence = 20 * time.Second
	h := localHarness(t, 4)
	f := frozenWorker(t, h, 2)
	var busy atomic.Bool
	h.workers[1].beforeStatus = func() {
		if busy.Load() {
			time.Sleep(4 * taskCheckTimeout)
		}
	}
	// Each of its tasks first keeps every core busy for a while.
	h.workers[1].beforeTask = func(http.ResponseWriter, *http.Request) bool {
		busy.Store(true)
		var wg sync.WaitGroup
		until := time.Now().Add(500 * time.Millisecond)
		for range runtime.NumCPU() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for x := 0; time.Now().Before(until); x++ {
					_ = x * x
				}
			}()
		}
		wg.Wait()
		return true
	}
	want, _, err := h.query(context.Background(), exchangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	var frozenAt, foundAt atomic.Int64
	var lost sync.Map
	hook := func(r slog.Record) {
		if strings.HasPrefix(r.Message, "a worker stopped answering; it is taken for lost") {
			lost.Store(attr(r, "worker"), true)
			foundAt.CompareAndSwap(0, time.Now().UnixNano())
		}
		if r.Message == "ran a stage" && strings.Contains(attr(r, "stage"), " to exchange ") && ran.Add(1) == 1 {
			frozenAt.Store(time.Now().UnixNano())
			f.frozen.Store(true)
		}
	}
	h.hook.Store(&hook)
	start := time.Now()
	got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Error(msg)
	}
	if !busy.Load() {
		t.Fatal("the busy worker got no task")
	}
	if _, ok := lost.Load(h.lns[1].Addr().String()); ok {
		t.Errorf("the busy worker was taken for lost, %s into the statement", time.Since(start))
	}
	if _, ok := lost.Load(h.lns[2].Addr().String()); !ok || frozenAt.Load() == 0 {
		t.Fatalf("the powered-off worker was not taken for lost (frozen at %d)", frozenAt.Load())
	}
	if took := time.Duration(foundAt.Load() - frozenAt.Load()); took > 5*time.Second {
		t.Errorf("the powered-off worker was taken for lost %s after it went", took)
	}

	// Taking connections but answering nothing, past heartbeatSilence.
	heartbeatSilence = 300 * time.Millisecond
	hb := newHeartbeats(func(ctx context.Context, addr string) error {
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	gone, why, release := hb.watch(h.lns[1].Addr().String())
	defer release()
	if !lostIn(gone, 5*time.Second) {
		t.Fatal("a worker that takes connections and answers nothing was not taken for lost")
	}
	if !strings.Contains(why().Error(), "took connections but answered no status") {
		t.Errorf("why: %v", why())
	}
}

// tasksRun counts the tasks h's workers are given.
func tasksRun(h *harness) *atomic.Int64 {
	var n atomic.Int64
	for _, w := range h.workers {
		w.beforeTask = func(http.ResponseWriter, *http.Request) bool {
			n.Add(1)
			return true
		}
	}
	return &n
}

// TestLostWorkerRunsAgainOnlyWhatItHeld has a worker stop answering, closing no socket,
// once its tasks wrote exchange files to its disk that readers have yet to read: the
// tasks it ran of them run again on the others, and the readers' on what those wrote,
// rather than the statement's parts all over, and the rows are what one machine returns.
func TestLostWorkerRunsAgainOnlyWhatItHeld(t *testing.T) {
	fastBeats(t, 50*time.Millisecond, 200*time.Millisecond, 5*time.Second)
	for _, afterStages := range []int{1, 2} {
		h := localHarness(t, 3)
		f := frozenWorker(t, h, 1)
		want, _, err := h.query(context.Background(), exchangeQuery)
		if err != nil {
			t.Fatal(err)
		}
		// A run with nothing lost, for how many tasks it takes.
		whole := tasksRun(h)
		if _, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn); err != nil {
			t.Fatal(err)
		}
		tasks := tasksRun(h)
		var ran, again, wholeAgain atomic.Int32
		hook := func(r slog.Record) {
			switch {
			case strings.Contains(r.Message, "ran again only its tasks that readers still need"):
				again.Add(1)
			case strings.Contains(r.Message, "running the statement's parts again without it"):
				wholeAgain.Add(1)
			case strings.Contains(r.Message, "could not run again only what a lost worker held"):
				t.Logf("%s: %s", r.Message, attr(r, "err"))
			case r.Message == "ran a stage" && strings.Contains(attr(r, "stage"), " to exchange ") && int(ran.Add(1)) == afterStages:
				f.frozen.Store(true)
			}
		}
		h.hook.Store(&hook)
		got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
		if err != nil {
			t.Fatalf("after %d stages: %v", afterStages, err)
		}
		if msg := compareRows(want, got, true); msg != "" {
			t.Errorf("after %d stages: %s", afterStages, msg)
		}
		if again.Load() == 0 || wholeAgain.Load() != 0 {
			t.Errorf("after %d stages: ran again only what it held %d times, and all of the statement's parts %d", afterStages, again.Load(), wholeAgain.Load())
		}
		t.Logf("after %d stages: %d tasks, %d with nothing lost", afterStages, tasks.Load(), whole.Load())
		// All of its parts again would have been more than twice as many, less what ran before.
		if tasks.Load() >= 2*whole.Load() {
			t.Errorf("after %d stages: %d tasks, %d with nothing lost", afterStages, tasks.Load(), whole.Load())
		}
	}
}

// TestRepeatable checks which stages' tasks may run again beside what they wrote before.
func TestRepeatable(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT a, sum(b) FROM t GROUP BY a":             true,
		"SELECT limit_of, sampled, random_id FROM t":     true,
		"SELECT 'limit random()' AS note FROM t":         true,
		"SELECT a, random() AS r FROM t":                 false,
		"SELECT a FROM t WHERE uuid() IS NOT NULL":       false,
		"SELECT a FROM t LIMIT 10":                       false,
		"SELECT a FROM t ORDER BY a LIMIT 10":            false,
		"SELECT a FROM t USING SAMPLE 10%":               false,
		"SELECT a FROM t TABLESAMPLE reservoir(10 ROWS)": false,
	} {
		if got := repeatable(&Stage{SQL: sql}); got != want {
			t.Errorf("%s: %v", sql, got)
		}
	}
	if repeatable(&Stage{SQL: "SELECT a FROM t", enough: 10}) {
		t.Error("a stage that takes any rows it needs")
	}
}

// frozenWorker has h's worker i stop serving on the first freeze, its connections open
// and its kernel taking what is sent, as a machine that lost power closes nothing:
// not a killed process. Nor does it take new connections.
func frozenWorker(t *testing.T, h *harness, i int) *freezer {
	t.Helper()
	f := &freezer{thaw: make(chan struct{})}
	var addr atomic.Value
	h.wrap = func(n int, ln net.Listener) net.Listener {
		if n != i {
			return ln
		}
		f.Listener = ln
		addr.Store(ln.Addr().String())
		return f
	}
	// Frozen, its machine takes no connection either.
	unpowered(t, func(a string) bool { return f.frozen.Load() && a == addr.Load() })
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
		if strings.HasPrefix(r.Message, "a worker stopped answering; it is taken for lost") || strings.HasPrefix(r.Message, "a worker holding exchanged rows on its disk was lost") {
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

// TestPieces checks how a task is divided: every unit in exactly one piece, the pieces
// as even as can be, and each with a directory of its own.
func TestPieces(t *testing.T) {
	units := []int{0, 1, 2, 3, 4, 5, 6}
	for k := 1; k <= 8; k++ {
		var all []int
		names := map[int]bool{}
		for j := range k {
			p := piece{i: 7, j: j, k: k}
			got := pieceOf(units, p)
			if n := len(got); n < len(units)/k || n > (len(units)+k-1)/k {
				t.Errorf("piece %d of %d: %v", j, k, got)
			}
			all = append(all, got...)
			if names[p.index()] {
				t.Errorf("piece %d of %d shares a directory", j, k)
			}
			names[p.index()] = true
		}
		slices.Sort(all)
		if !slices.Equal(all, units) {
			t.Errorf("%d pieces hold %v", k, all)
		}
		if k > 1 && names[wholeTask(7).index()] {
			t.Errorf("a piece of %d shares the undivided task's directory", k)
		}
	}
	if got := pieceOf(units, wholeTask(3)); !slices.Equal(got, units) {
		t.Errorf("an undivided task: %v", got)
	}
}

// TestLostMidTaskRunsAsSmallerTasks has a worker stop answering in the middle of a
// task, its sockets open: the task runs as smaller tasks on the others, one each, and
// the rows are exactly what one machine returns, also once the lost worker, let go,
// runs the task late and writes what it was given to, with exchanges in object storage
// and on workers' disks.
func TestLostMidTaskRunsAsSmallerTasks(t *testing.T) {
	fastBeats(t, 50*time.Millisecond, 200*time.Millisecond, 5*time.Second)
	for _, local := range []bool{false, true} {
		name := "object storage"
		if local {
			name = "workers' disks"
		}
		t.Run(name, func(t *testing.T) {
			// Shares of three files a worker, which a task lost can be divided by.
			h := newHarness(t, 0.01, 12, 4)
			h.co.maxBroadcast = 1
			h.co.opts.LocalExchanges = local
			f := frozenWorker(t, h, 1)
			want, _, err := h.query(context.Background(), exchangeQuery)
			if err != nil {
				t.Fatal(err)
			}
			late := make(chan struct{})
			var once sync.Once
			var froze, ranLate atomic.Int32
			h.workers[1].beforeTask = func(rw http.ResponseWriter, r *http.Request) bool {
				if froze.Add(1) > 1 {
					return false
				}
				f.frozen.Store(true)
				select {
				case <-late:
					// Let go: it runs the task, and nobody hears of it.
					ranLate.Add(1)
					return true
				case <-f.thaw:
					return false
				}
			}
			var split atomic.Int32
			var pieces atomic.Int64
			hook := func(r slog.Record) {
				if strings.Contains(r.Message, "running the task as smaller ones on the others") {
					split.Add(1)
					var n int64
					fmt.Sscan(attr(r, "tasks"), &n)
					pieces.Add(n)
					once.Do(func() { close(late) })
				}
			}
			h.hook.Store(&hook)
			got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			if msg := compareRows(want, got, true); msg != "" {
				t.Error(msg)
			}
			if split.Load() == 0 || pieces.Load() < 2 {
				t.Errorf("the lost worker's task ran as %d smaller ones, %d times", pieces.Load(), split.Load())
			}
			if ranLate.Load() == 0 {
				t.Errorf("the lost worker did not run its task late (given %d tasks)", froze.Load())
			}
		})
	}
}
