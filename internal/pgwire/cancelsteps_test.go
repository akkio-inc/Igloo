package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/workload"
)

// TestCancelAsAStepEnds: the memory watch's cancel of a step of a statement,
// made as the step ends, ends the statement. The watch holds the session's
// lock as it cancels, and the step's end waited for it having already read
// that it was not canceled: the statement went on to its run, which ran to
// its end (a test's, for 30 minutes). A cancel request that comes between
// two steps ends the next, and not the next statement's.
func TestCancelAsAStepEnds(t *testing.T) {
	t.Parallel()
	sess := &session{}
	_, done := sess.cancelable(context.Background(), "SELECT 1")
	sess.mu.Lock()
	ended := make(chan error, 1)
	go func() { ended <- done(nil) }()
	// Long enough for the step's end to wait for the lock, the order that lost
	// the cancel; in any order, the step must report it.
	time.Sleep(50 * time.Millisecond)
	sess.shed, sess.shedAt = true, time.Now()
	sess.running()
	sess.mu.Unlock()
	if err := <-ended; !errors.Is(err, errMemoryShed) {
		t.Errorf("shed as its step ended: %v, want the memory watch's error", err)
	}

	end := sess.steps()
	_, done = sess.cancelable(context.Background(), "SELECT 1")
	if err := done(nil); err != nil {
		t.Fatal(err)
	}
	sess.mu.Lock()
	found := sess.cancelLocked()
	sess.mu.Unlock()
	if !found {
		t.Error("a cancel between two steps found no statement")
	}
	ctx, done := sess.cancelable(context.Background(), "SELECT 1")
	if ctx.Err() == nil {
		t.Error("the step after a cancel between steps started uncanceled")
	}
	if err := done(nil); !errors.Is(err, errCanceledBeforeRun) {
		t.Errorf("the step after a cancel between steps: %v", err)
	}
	end()
	ctx, done = sess.cancelable(context.Background(), "SELECT 2")
	if ctx.Err() != nil {
		t.Error("the next statement started canceled")
	}
	if err := done(nil); err != nil {
		t.Errorf("the next statement: %v", err)
	}
}

// TestCancelWhereNoStepRuns: a statement canceled where none of its steps
// runs, by a cancel request on the wire or the console's cancel, ends with
// 57014 rather than run to its end. Between its steps, where it waits for the
// catalog's lock, the cancel found nothing to end. After it ran out of
// memory, before it runs again alone, the run again started uncanceled. (A
// cancel while it is prepared or readied, while its rows stream and while it
// runs in the second DuckDB: TestCancelWhilePreparing, TestShedMemoryBeforeRun,
// TestStreamedResultCanceled and TestHeavyStatementCanceledInBackground.)
func TestCancelWhereNoStepRuns(t *testing.T) {
	t.Parallel()
	ts := startServer(t, func(o *Options) { o.Workload = workload.NewController(1 << 30) })
	ctx := context.Background()
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	// long would run for hours.
	long := func(marker string) string {
		return fmt.Sprintf("SELECT count(*) AS %s FROM range(10000000000) a(i), range(1000) b(j) WHERE i + j < 0", marker)
	}
	ends := func(what, query string) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, query)
			done <- err
		}()
		select {
		case err := <-done:
			if pgCode(err) != "57014" {
				t.Errorf("%s: %v, want 57014", what, err)
			}
		case <-time.After(time.Minute):
			t.Fatalf("%s: still running a minute after", what)
		}
	}

	var found atomic.Bool
	ts.srv.beforeRun = func(sess *session, query string) {
		switch {
		case strings.Contains(query, "wire_marker"):
			found.Store(true)
			ts.srv.sessions.cancel(sess.pid, sess.secret)
		case strings.Contains(query, "console_marker"):
			found.Store(ts.srv.CancelStatement(sess.pid, "test"))
		}
	}
	t.Cleanup(func() { ts.srv.beforeRun = nil })
	ends("between its steps, a cancel request", long("wire_marker"))
	ends("between its steps, the console's cancel", long("console_marker"))
	if !found.Load() {
		t.Error("the console's cancel found no statement between its steps")
	}

	var runs atomic.Int32
	ts.srv.failLocal = func(ctx context.Context, st *statement) error {
		if !strings.Contains(st.query, "alone_marker") {
			return nil
		}
		if runs.Add(1) > 1 {
			return nil
		}
		sess := sessionFrom(ctx)
		ts.srv.sessions.cancel(sess.pid, sess.secret)
		return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to allocate data of size 4.0 KiB (1.0 GiB/1.0 GiB used)"}
	}
	t.Cleanup(func() { ts.srv.failLocal = nil })
	ends("out of memory, then a cancel request", long("alone_marker"))
	if n := runs.Load(); n != 1 {
		t.Errorf("canceled as it ran out of memory, it ran %d times, want once", n)
	}

	if got := count(t, conn, "SELECT 5"); got != 5 {
		t.Errorf("the next statement: %d", got)
	}
}

// TestHeavyStatementCanceledInBackground: a cancel request ends a statement
// that runs in the second DuckDB, on its connection there.
func TestHeavyStatementCanceledInBackground(t *testing.T) {
	t.Parallel()
	r, admin, metrics := backgroundReplica(t, "200000", time.Nanosecond)
	ctx := context.Background()
	query := func(n string) string {
		return "SELECT count(*) FROM lake.public.visits a, range(" + n + ") b WHERE a.v + b.range < 0"
	}
	// Known to be heavy from its first run.
	if got := count(t, admin, query("10")); got != 0 {
		t.Fatalf("got %d", got)
	}
	done := make(chan error, 1)
	go func() {
		_, err := admin.Exec(ctx, query("10000000000"))
		done <- err
	}()
	inBackground := func() bool {
		r.srv.sessions.mu.Lock()
		defer r.srv.sessions.mu.Unlock()
		for _, sess := range r.srv.sessions.byPID {
			sess.mu.Lock()
			in := sess.inBackground
			sess.mu.Unlock()
			if in {
				return true
			}
		}
		return false
	}
	for deadline := time.Now().Add(time.Minute); !inBackground(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the statement did not start in the second DuckDB")
		}
	}
	if err := admin.PgConn().CancelRequest(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if pgCode(err) != "57014" {
			t.Errorf("canceled in the second DuckDB: %v, want 57014", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("canceled in the second DuckDB, still running a minute after")
	}
	if n := backgrounds(metrics); n != 1 {
		t.Errorf("%v statements ran in the second DuckDB, want 1", n)
	}
}
