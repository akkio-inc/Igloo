package pgwire

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/workload"
)

// TestOutOfMemoryRunsAgainAlone checks that a statement that runs out of
// memory, before it sent a row, runs again rather than fail. Beside another
// statement, it runs again on as many threads once what it failed to get is
// free: its second run waits until the other statement, which holds most of
// the memory, ends. Run out alone, it runs again on half the threads, and
// DuckDB's threads are as they were after. In a transaction block, which
// running it again would break, it fails as DuckDB failed it.
func TestOutOfMemoryRunsAgainAlone(t *testing.T) {
	t.Parallel()
	ctl := workload.NewController(1 << 30)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	ctx := context.Background()
	oom := &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 4.0 KiB (1.8 GiB/1.8 GiB used)"}
	// Half the replica's memory: free once the other statement ends.
	big := &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 512.0 MiB (1.0 GiB/1.0 GiB used)"}
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
		if strings.Contains(st.query, "beside_marker") {
			return big
		}
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

// TestOutOfMemoryAtOnce checks that statements that run out of memory at
// once, beside a dashboard's load and a statement that holds most of the
// memory, stop nothing. The dashboard's statements, which reserve memory,
// keep starting; one of the six waits to run alone, and runs once nothing else
// holds memory; the others fail at once, saying another waits to run alone.
// Each of the six waited in line for all of the memory, holding back every
// statement behind it that reserved any, and on a replica of 42.2 GiB nothing
// started for 150 s.
func TestOutOfMemoryAtOnce(t *testing.T) {
	t.Parallel()
	ctl := workload.NewController(1 << 30)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	ctx := context.Background()
	// Seen once, they start together rather than one at a time, as statements
	// nothing is known of do.
	if n := count(t, cs.admin, "SELECT 0 AS oom_marker"); n != 0 {
		t.Fatalf("got %d", n)
	}
	// A statement holds most of the memory, as a refresh did.
	heavy, _, err := ctl.Admit(ctx, workload.Limits{Memory: 600 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { heavy() })
	var mu sync.Mutex
	failed := map[string]bool{}
	cs.srv.failLocal = func(_ context.Context, st *statement) error {
		if !strings.Contains(st.query, "oom_marker") {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if failed[st.query] {
			return nil
		}
		failed[st.query] = true
		return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 900.0 MiB (1.0 GiB/1.0 GiB used)"}
	}
	t.Cleanup(func() { cs.srv.failLocal = nil })

	// A dashboard's load: statements of 64 MiB each, back to back, four at once.
	stop := make(chan struct{})
	var started, timedOut atomic.Int64
	var load sync.WaitGroup
	for range 4 {
		load.Add(1)
		go func() {
			defer load.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				wctx, cancel := context.WithTimeout(ctx, time.Second)
				r, _, err := ctl.Admit(wctx, workload.Limits{Memory: 64 << 20})
				cancel()
				if err != nil {
					timedOut.Add(1)
					continue
				}
				started.Add(1)
				time.Sleep(10 * time.Millisecond)
				r()
			}
		}()
	}
	stopLoad := sync.OnceFunc(func() {
		close(stop)
		load.Wait()
	})
	t.Cleanup(stopLoad)

	const n = 6
	errs := make(chan error, n)
	for i := range n {
		conn := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
		go func() {
			_, err := conn.Exec(ctx, fmt.Sprintf("SELECT %d AS oom_marker", i+1))
			errs <- err
		}()
	}
	deadline := time.After(30 * time.Second)
	for busy := 0; busy < n-1; busy++ {
		select {
		case err := <-errs:
			if pgCode(err) != "53200" || !strings.Contains(err.Error(), "another was already waiting to run alone") {
				t.Fatalf("a statement that could not run alone: %v", err)
			}
		case <-deadline:
			t.Fatalf("%d of the %d statements that could not run alone failed in 30 s", busy, n-1)
		}
	}
	before := started.Load()
	time.Sleep(500 * time.Millisecond)
	if got := started.Load() - before; got < 10 || timedOut.Load() > 0 {
		t.Errorf("beside a statement waiting to run alone, %d of the dashboard's statements started in 0.5 s, and %d waited a second in vain",
			got, timedOut.Load())
	}
	stopLoad()
	heavy()
	select {
	case err := <-errs:
		if err != nil {
			t.Errorf("the statement that waited to run alone: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the statement waiting to run alone did not run once nothing else held memory")
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
