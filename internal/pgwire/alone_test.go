package pgwire

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/workload"
)

// TestOutOfMemoryRunsAgainAlone checks that a statement that runs out of
// memory, before it sent a row, runs again rather than fail. Beside another
// statement, it runs again once it holds the replica's whole memory, on as
// many threads: its second run waits until the other statement's reservation
// ends. Run out alone, it runs again on half the threads, and DuckDB's
// threads are as they were after. In a transaction block, which running it
// again would break, it fails as DuckDB failed it.
func TestOutOfMemoryRunsAgainAlone(t *testing.T) {
	t.Parallel()
	ctl := workload.NewController(1 << 30)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	ctx := context.Background()
	oom := &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 4.0 KiB (1.8 GiB/1.8 GiB used)"}
	var mu sync.Mutex
	var runs int
	var threads []string
	var otherEnded, secondRun time.Time
	// seen returns what the runs so far saw, and reset sets them back.
	seen := func() (int, []string) {
		mu.Lock()
		defer mu.Unlock()
		return runs, append([]string(nil), threads...)
	}
	reset := func() {
		mu.Lock()
		defer mu.Unlock()
		runs, threads = 0, nil
	}
	// Another statement runs, and holds some of the memory.
	other, _, err := ctl.Admit(ctx, workload.Limits{Memory: 100 << 20})
	if err != nil {
		t.Fatal(err)
	}
	unblock, holding := make(chan struct{}), make(chan struct{})
	// The other statement ends: the one nothing is known of, which the
	// controller counts as holding nearly all the memory, and the one holding
	// a reservation.
	otherEnds := sync.OnceFunc(func() {
		mu.Lock()
		otherEnded = time.Now()
		mu.Unlock()
		close(unblock)
		other()
	})
	var holderDone sync.WaitGroup
	t.Cleanup(func() { holderDone.Wait() })
	t.Cleanup(otherEnds)
	// Seen once, the statement starts beside the other, rather than wait in
	// line behind it for up to a minute as one nothing is known of does.
	if n := count(t, cs.admin, "SELECT 7 AS beside_marker"); n != 7 {
		t.Fatalf("got %d", n)
	}
	cs.srv.failLocal = func(ctx context.Context, st *statement) error {
		switch {
		case strings.Contains(st.query, "holder_marker"):
			close(holding)
			<-unblock
			return nil
		case !strings.Contains(st.query, "_marker"):
			return nil
		}
		v, err := settingValue(ctx, sessionFrom(ctx).conn, "threads")
		if err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		runs++
		threads = append(threads, v)
		if runs > 1 {
			secondRun = time.Now()
			return nil
		}
		go func() {
			time.Sleep(200 * time.Millisecond)
			otherEnds()
		}()
		return oom
	}
	holder := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	holderDone.Add(1)
	go func() {
		defer holderDone.Done()
		_, _ = holder.Exec(ctx, "SELECT 1 AS holder_marker")
	}()
	<-holding
	if n := count(t, cs.admin, "SELECT 7 AS beside_marker"); n != 7 {
		t.Fatalf("got %d", n)
	}
	if n, th := seen(); n != 2 || th[1] != th[0] {
		t.Errorf("beside another, it ran %d times on threads %v, want twice on as many", n, th)
	}
	mu.Lock()
	if secondRun.Before(otherEnded) || otherEnded.IsZero() {
		t.Error("the second run started before the other statement's reservation ended")
	}
	mu.Unlock()
	holderDone.Wait()

	reset()
	if n := count(t, cs.admin, "SELECT 9 AS fewer_marker"); n != 9 {
		t.Fatalf("got %d", n)
	}
	n, th := seen()
	was, _ := strconv.Atoi(th[0])
	if n != 2 || th[1] != strconv.Itoa(was/2) {
		t.Errorf("alone, it ran %d times on threads %v, want once on %d, then on %d", n, th, was, was/2)
	}
	if after := count(t, cs.admin, "SELECT current_setting('threads')::BIGINT"); after != int64(was) {
		t.Errorf("threads are %d after, not the %d they were", after, was)
	}

	// Out of memory even alone on one thread: the error says so, and what would
	// let it finish.
	cs.srv.failLocal = func(_ context.Context, st *statement) error {
		if strings.Contains(st.query, "never_marker") {
			return oom
		}
		return nil
	}
	never := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, never, "SET distributed_execution = 'off'")
	_, err = never.Exec(ctx, "SELECT 10 AS never_marker")
	if pgCode(err) != "53200" || !strings.Contains(err.Error(), "ran out of memory running alone, on one thread") ||
		!strings.Contains(err.Error(), "IGLOO_MEMORY_LIMIT") || !strings.Contains(err.Error(), "distributed_execution") {
		t.Errorf("out of memory alone on one thread: %v", err)
	}

	reset()
	cs.srv.failLocal = func(_ context.Context, st *statement) error {
		if strings.Contains(st.query, "block_marker") {
			mu.Lock()
			runs++
			mu.Unlock()
			return oom
		}
		return nil
	}
	block := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, block, "BEGIN")
	if _, err := block.Exec(ctx, "SELECT 8 AS block_marker"); pgCode(err) != "53200" {
		t.Errorf("in a transaction block: %v", err)
	}
	if n, _ := seen(); n != 1 {
		t.Errorf("in a transaction block it ran %d times, want 1", n)
	}
}

// TestOutOfMemoryRefreshRunsAgainAlone checks that a job's refresh of a
// dynamic table that runs out of memory beside another statement runs again
// once the other statement's reservation ends, rather than fail, and that the
// table then has the rows of its query.
func TestOutOfMemoryRefreshRunsAgainAlone(t *testing.T) {
	t.Parallel()
	ctl := workload.NewController(1 << 30)
	env := startRefreshServer(t, func(o *Options) { o.Workload = ctl })
	admin := env.admin
	ctx := context.Background()
	mustExec(t, admin, "CREATE TABLE spend (g VARCHAR, x INTEGER)")
	mustExec(t, admin, "INSERT INTO spend SELECT 'g' || (i % 4), i FROM range(1000) AS r(i)")
	mustExec(t, admin, "CREATE DYNAMIC TABLE spend_full TARGET_LAG = DOWNSTREAM WAREHOUSE = wh REFRESH_MODE = FULL AS "+
		"SELECT g, sum(x) AS sx FROM spend GROUP BY g")
	mustExec(t, admin, "INSERT INTO spend VALUES ('g9', 1)")

	// Another statement runs, and holds some of the memory.
	other, _, err := ctl.Admit(ctx, workload.Limits{Memory: 100 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var fills int
	var otherEnded, secondFill time.Time
	otherEnds := sync.OnceFunc(func() {
		mu.Lock()
		otherEnded = time.Now()
		mu.Unlock()
		other()
	})
	t.Cleanup(otherEnds)
	env.srv.failFill = func(context.Context, string) error {
		mu.Lock()
		defer mu.Unlock()
		if fills++; fills > 1 {
			secondFill = time.Now()
			return nil
		}
		go func() {
			time.Sleep(200 * time.Millisecond)
			otherEnds()
		}()
		return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: could not allocate block of size 2.3 GiB (43.4 GiB/44.2 GiB used)"}
	}
	t.Cleanup(func() { env.srv.failFill = nil })
	mustExec(t, admin, "ALTER DYNAMIC TABLE spend_full REFRESH")
	env.srv.failFill = nil
	mu.Lock()
	if fills != 2 {
		t.Errorf("the refresh filled the table %d times, want twice", fills)
	}
	if otherEnded.IsZero() || secondFill.Before(otherEnded) {
		t.Error("the refresh ran again before the other statement's reservation ended")
	}
	mu.Unlock()
	if n := count(t, admin, "SELECT count(*) FROM spend_full"); n != 5 {
		t.Errorf("%d groups after the refresh, want 5", n)
	}
}

// TestKeepPeak checks that a statement that ran again keeps the larger of its
// runs' sampled peaks.
func TestKeepPeak(t *testing.T) {
	t.Parallel()
	var w workloadState
	w.keepPeak(0, false)
	w.keepPeak(5<<30, true)
	w.keepPeak(1<<30, true)
	w.keepPeak(9<<30, false)
	if w.peak != 5<<30 || !w.sampled {
		t.Errorf("peak %d, sampled %v; want the failed run's 5 GiB", w.peak, w.sampled)
	}
}
