package pgwire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestShedMemory checks that ShedMemory cancels the statement that has run
// longest, which fails with SQLSTATE 53200, and leaves the others running.
func TestShedMemory(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	first, second := ts.admin(t, pgx.QueryExecModeSimpleProtocol), ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	const long = "SELECT sum(i) FROM range(100000000000) t(i)"
	running := func(n int) []SessionInfo {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var got []SessionInfo
			for _, s := range ts.srv.Sessions() {
				if s.Statement != "" {
					got = append(got, s)
				}
			}
			if len(got) == n || time.Now().After(deadline) {
				return got
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	run := func(conn *pgx.Conn) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, long)
			done <- err
		}()
		return done
	}
	firstDone := run(first)
	if got := running(1); len(got) != 1 {
		t.Fatalf("running: %+v", got)
	}
	time.Sleep(50 * time.Millisecond)
	secondDone := run(second)
	both := running(2)
	if len(both) != 2 {
		t.Fatalf("running: %+v", both)
	}

	if !ts.srv.ShedMemory(100 << 20) {
		t.Fatal("ShedMemory canceled nothing")
	}
	select {
	case err := <-firstDone:
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "53200" {
			t.Fatalf("the longest-running statement: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the longest-running statement was not canceled")
	}
	select {
	case err := <-secondDone:
		t.Fatalf("the other statement ended: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	rest := running(1)
	if len(rest) != 1 || !ts.srv.CancelStatement(rest[0].PID, "test") {
		t.Fatalf("still running: %+v", rest)
	}
	var pg *pgconn.PgError
	if err := <-secondDone; !errors.As(err, &pg) || pg.Code != "57014" {
		t.Errorf("the other statement, canceled as a client would: %v", err)
	}
	if ts.srv.ShedMemory(100 << 20) {
		t.Error("ShedMemory canceled something with nothing running")
	}
}

// TestShedMemoryBeforeRun checks that what Igloo does for a statement
// before it runs (checking it, preparing it again: a report call reads
// bitmap indexes then) is ended as its run is, by the memory watch and by a
// cancel request.
func TestShedMemoryBeforeRun(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	const query = "SELECT 42 AS before_run"
	ts.srv.beforeRefresh = func(ctx context.Context) {
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute):
		}
	}
	running := func() []SessionInfo {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			var got []SessionInfo
			for _, s := range ts.srv.Sessions() {
				if s.Statement == query {
					got = append(got, s)
				}
			}
			if len(got) == 1 || time.Now().After(deadline) {
				return got
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	run := func() chan error {
		done := make(chan error, 1)
		go func() {
			_, err := conn.Exec(ctx, query)
			done <- err
		}()
		return done
	}
	wait := func(what string, done chan error, code string) {
		t.Helper()
		select {
		case err := <-done:
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != code {
				t.Errorf("%s: %v, want %s", what, err, code)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: not ended", what)
		}
	}

	done := run()
	if got := running(); len(got) != 1 {
		t.Fatalf("running: %+v", got)
	}
	if !ts.srv.ShedMemory(100 << 20) {
		t.Fatal("ShedMemory canceled nothing")
	}
	wait("before its run, the memory watch", done, "53200")

	done = run()
	got := running()
	if len(got) != 1 || !ts.srv.CancelStatement(got[0].PID, "test") {
		t.Fatalf("running: %+v", got)
	}
	wait("before its run, a cancel", done, "57014")

	ts.srv.beforeRefresh = nil
	if v := texts(t, conn, query); len(v) != 1 || v[0] != "42" {
		t.Errorf("after: %q", v)
	}
}
