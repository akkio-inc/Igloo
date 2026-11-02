package pgwire

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/workload"
)

// goneWithin is how long a statement may run on once its client has gone: a look at
// its socket a second, and however long DuckDB takes to notice its cancellation.
const goneWithin = 10 * time.Second

// statementRunning reports whether a session of srv is running a statement that
// contains marker.
func statementRunning(srv *Server, marker string) bool {
	r := srv.sessions
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sess := range r.byPID {
		sess.mu.Lock()
		running := sess.running != nil && strings.Contains(sess.statement, marker)
		sess.mu.Unlock()
		if running {
			return true
		}
	}
	return false
}

// comesTrue waits up to d for cond, and reports whether it came true.
func comesTrue(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// killable connects as a client that can be killed: pgx sends a cancel request
// whenever its connection fails, which a killed client does not, so this one can
// connect only once.
func killable(t *testing.T, ts *testServer, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	return killableAt(t, ts.url("igloo", adminPassword), mode)
}

func killableAt(t *testing.T, url string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	dial := cfg.DialFunc
	var dialed atomic.Bool
	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dialed.Swap(true) {
			return nil, errors.New("this client was killed")
		}
		return dial(ctx, network, addr)
	}
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// killClient runs query on conn and, once the server runs it, closes the socket
// under conn as a killed client's closes, sending nothing first. It returns when the
// client has noticed.
func killClient(t *testing.T, srv *Server, conn *pgx.Conn, query, marker string) {
	t.Helper()
	killClientWhen(t, conn, query, func() bool { return statementRunning(srv, marker) })
}

// killClientWhen is killClient once started says so.
func killClientWhen(t *testing.T, conn *pgx.Conn, query string, started func() bool) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), query)
		done <- err
	}()
	if !comesTrue(goneWithin, started) {
		_ = conn.PgConn().Conn().Close()
		t.Fatalf("the statement did not start: %v", <-done)
	}
	_ = conn.PgConn().Conn().Close()
	if err := <-done; err == nil {
		t.Error("the statement returned to a client that went away")
	}
}

// stopped checks that the statement of the session with pid, whose client is gone,
// stops: the session ends only once it has.
func stopped(t *testing.T, srv *Server, pid uint32) {
	t.Helper()
	start := time.Now()
	r := srv.sessions
	if !comesTrue(goneWithin, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		_, there := r.byPID[int32(pid)]
		return !there
	}) {
		t.Errorf("the statement still ran %s after its client went away", time.Since(start))
	}
}

// TestClientGoneStopsStatement checks that a statement stops when its client goes
// away without a word, under each protocol and with its rows streaming; that the
// history records it as canceled; and that the next statement runs at once.
func TestClientGoneStopsStatement(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := killable(t, cs.testServer, mode)
			marker := "gone_" + name
			query := "SELECT count(*) AS " + marker + " FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"
			pid := conn.PgConn().PID()
			killClient(t, cs.srv, conn, query, marker)
			stopped(t, cs.srv, pid)
			other := cs.testServer.admin(t, mode)
			start := time.Now()
			if got := count(t, other, "SELECT 1"); got != 1 || time.Since(start) > 5*time.Second {
				t.Errorf("the next statement: %d after %s", got, time.Since(start))
			}
			if !comesTrue(goneWithin, func() bool {
				qs, err := cs.history.Queries(ctx, audit.Viewer{All: true}, audit.Page{})
				if err != nil {
					t.Fatal(err)
				}
				for _, q := range qs {
					if strings.Contains(q.Text, marker) {
						return q.Outcome == audit.OutcomeCanceled
					}
				}
				return false
			}) {
				t.Error("the history does not say the statement was canceled")
			}
		})
	}
	t.Run("streaming", func(t *testing.T) {
		conn := killable(t, cs.testServer, pgx.QueryExecModeSimpleProtocol)
		rows, err := conn.Query(ctx, "SELECT i, repeat('x', 100) AS gone_streaming FROM range(1000000000) t(i)")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000 && rows.Next(); i++ {
		}
		if !statementRunning(cs.srv, "gone_streaming") {
			t.Fatal("the rows have not started to stream")
		}
		pid := conn.PgConn().PID()
		_ = conn.PgConn().Conn().Close()
		rows.Close()
		stopped(t, cs.srv, pid)
	})
	t.Run("several statements", func(t *testing.T) {
		conn := killable(t, cs.testServer, pgx.QueryExecModeSimpleProtocol)
		const slow = "SELECT count(*) AS gone_several FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range"
		pid := conn.PgConn().PID()
		killClient(t, cs.srv, conn, slow+"; "+slow, "gone_several")
		stopped(t, cs.srv, pid)
	})
}

// TestClientGoneBehindTLS checks a client's going away where the connection is
// wrapped, in TLS and in the listener's gate for it, and where it says goodbye.
func TestClientGoneBehindTLS(t *testing.T) {
	t.Parallel()
	ts := startServer(t, func(o *Options) {
		o.TLS = selfSignedTLS(t)
		o.RequireTLS = true
	})
	conn := killableAt(t, strings.Replace(ts.url("igloo", adminPassword), "sslmode=disable", "sslmode=require", 1), pgx.QueryExecModeSimpleProtocol)
	pid := conn.PgConn().PID()
	killClient(t, ts.srv, conn, "SELECT count(*) AS gone_tls FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range", "gone_tls")
	stopped(t, ts.srv, pid)
}

// TestClientGoneInLine checks that a statement waiting to start leaves the line
// when its client goes away.
func TestClientGoneInLine(t *testing.T) {
	t.Parallel()
	ctl := workload.NewController(0)
	ctl.LimitReplica(1)
	cs := startCatalogServer(t, func(o *Options) { o.Workload = ctl })
	holder := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, holder, "SET statement_timeout = '60s'")
	held := make(chan struct{})
	go func() {
		defer close(held)
		_, _ = holder.Exec(context.Background(), "SELECT count(*) AS gone_holder FROM range(10000000000) a, range(10) b WHERE a.range % 7 = b.range")
	}()
	if !comesTrue(goneWithin, func() bool { return statementRunning(cs.srv, "gone_holder") }) {
		t.Fatal("the statement holding the replica's slot did not start")
	}
	waiting := ctl.Metrics()[0]
	conn := killable(t, cs.testServer, pgx.QueryExecModeSimpleProtocol)
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "SELECT 1 AS gone_waiter")
		done <- err
	}()
	if !comesTrue(goneWithin, func() bool { return testutil.ToFloat64(waiting) == 1 }) {
		t.Fatal("the statement does not wait in line")
	}
	_ = conn.PgConn().Conn().Close()
	<-done
	if !comesTrue(goneWithin, func() bool { return testutil.ToFloat64(waiting) == 0 }) {
		t.Error("a statement whose client went away still waits in line")
	}
	_ = holder.PgConn().CancelRequest(context.Background())
	<-held
}

// TestClientGoneOnWorkers checks that a statement running on workers, and its tasks,
// stop when its client goes away.
func TestClientGoneOnWorkers(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	release := ds.gate.hold()
	defer release()
	conn := killable(t, ds.testServer, pgx.QueryExecModeSimpleProtocol)
	pid := conn.PgConn().PID()
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, conn, "SET distributed_execution = on")
	done := make(chan error, 1)
	go func() {
		_, err := conn.Exec(context.Background(), "SELECT amount, count(*) AS gone_workers FROM orders GROUP BY amount")
		done <- err
	}()
	if !comesTrue(goneWithin, func() bool {
		waiting, _ := ds.gate.counts()
		return waiting > 0
	}) {
		t.Fatal("no task started on the workers")
	}
	_ = conn.PgConn().Conn().Close()
	<-done
	if !comesTrue(goneWithin, func() bool {
		waiting, canceled := ds.gate.counts()
		return waiting == 0 && canceled > 0
	}) {
		waiting, canceled := ds.gate.counts()
		t.Errorf("%d tasks still run on workers, %d were canceled", waiting, canceled)
	}
	stopped(t, ds.srv, pid)
	if got := count(t, ds.admin, "SELECT 1"); got != 1 {
		t.Errorf("the next statement: %d", got)
	}
}

// TestClientGoneInBackground checks that a statement running in the second DuckDB
// stops when its client goes away.
func TestClientGoneInBackground(t *testing.T) {
	t.Parallel()
	r, admin, m := backgroundReplica(t, "200000", time.Nanosecond)
	// Heavy, by its first run, whatever else it takes.
	mustExec(t, admin, "SELECT sum(hash(x.id + r.i)) AS gone_second FROM lake.public.visits x, range(2) r(i)")
	before := backgrounds(m)
	conn := killable(t, r.testServer, pgx.QueryExecModeSimpleProtocol)
	pid := conn.PgConn().PID()
	killClientWhen(t, conn, "SELECT sum(hash(x.id + r.i)) AS gone_second FROM lake.public.visits x, range(200000000) r(i)", func() bool {
		r := r.srv.sessions
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, sess := range r.byPID {
			sess.mu.Lock()
			there := sess.inBackground
			sess.mu.Unlock()
			if there {
				return true
			}
		}
		return false
	})
	stopped(t, r.srv, pid)
	if backgrounds(m) != before+1 {
		t.Errorf("%v statements ran in the second DuckDB, want 1", backgrounds(m)-before)
	}
	if got := count(t, admin, "SELECT 1"); got != 1 {
		t.Errorf("the next statement: %d", got)
	}
}
