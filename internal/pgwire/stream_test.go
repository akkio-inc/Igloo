package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/prefetch"
)

// TestResultsStream sends a statement's rows as DuckDB computes them, not
// once it has computed them all: a statement that fails three quarters of
// the way through its rows has sent the client rows before the error. Read
// whole first, as they were, a sort of hundreds of millions of rows ran the
// server out of memory before its client saw one.
func TestResultsStream(t *testing.T) {
	ts := startServer(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			ctx := context.Background()
			rows, err := conn.Query(ctx, `SELECT CASE WHEN i < 3000000 THEN i ELSE error('a late failure') END AS i FROM range(4000000) t(i)`)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rows.Next() {
				n++
			}
			rows.Close()
			// DuckDB's error, or what is left of it (see afterFalseInterrupt),
			// after rows went out. Nothing canceled it.
			err = rows.Err()
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code == "57014" ||
				!strings.Contains(pgErr.Message, "a late failure") && !(pgErr.Code == "XX000" && strings.Contains(pgErr.Message, "was not canceled")) {
				t.Fatalf("after %d rows: %v, want the late failure", n, err)
			}
			if n == 0 {
				t.Fatal("no row reached the client before the failure: the result was computed whole first")
			}
			// The session goes on.
			var one int
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("after the failure: %d, %v", one, err)
			}
		})
	}
}

// falseInterrupt interrupts DuckDB on a session's connection, as a thread's
// failure does, once a statement whose text holds marker has streamed at
// rows, on its first times runs: its context lasts.
type falseInterrupt struct {
	marker string
	at     uint32
	times  atomic.Int32
	// report has the stream report the false interrupt itself, for what DuckDB
	// does not stream, and an interrupt would not reach.
	report bool
}

var falseInterrupts sync.Map

func interruptStreams(t *testing.T, marker string, at uint32, times int32) {
	t.Helper()
	f := &falseInterrupt{marker: marker, at: at}
	f.times.Store(times)
	falseInterrupts.Store(marker, f)
	t.Cleanup(func() { falseInterrupts.Delete(marker) })
}

func init() {
	wrapStream = func(ctx context.Context, st *statement, w wire.DataWriter, rows driver.Rows) driver.Rows {
		var found *falseInterrupt
		falseInterrupts.Range(func(k, v any) bool {
			if strings.Contains(st.query, k.(string)) {
				found = v.(*falseInterrupt)
			}
			return found == nil
		})
		if found == nil || found.times.Add(-1) < 0 {
			return rows
		}
		return &interruptedRows{Rows: rows, ctx: ctx, w: w, at: found.at, report: found.report}
	}
}

// interruptedRows are rows whose query is interrupted once w has sent at.
type interruptedRows struct {
	driver.Rows
	ctx  context.Context
	w    wire.DataWriter
	at   uint32
	done bool
	// report says to return the error rather than interrupt the query.
	report bool
}

func (r *interruptedRows) Next(dest []driver.Value) error {
	if !r.done && r.w.Written() >= r.at {
		r.done = true
		if r.report {
			return duckdb.ErrFalseInterrupt
		}
		sessionFrom(r.ctx).conn.Interrupt()
	}
	return r.Rows.Next(dest)
}

// TestStreamFailsBeforeRows checks a statement that fails before a row went
// out, DuckDB reporting an interrupt for the thread's failure, when nothing
// asked for one: the client gets the real error. So it does however the
// threads race, and one that fails no more gets its rows.
func TestStreamFailsBeforeRows(t *testing.T) {
	ts := startServer(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			ctx := context.Background()
			mustFail := func(q string) {
				t.Helper()
				n, err := drainRows(conn, q)
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || !strings.Contains(pgErr.Message, "an early failure") || pgErr.Code == "57014" || n != 0 {
					t.Fatalf("after %d rows: %v, want the failure", n, err)
				}
			}
			const failing = `SELECT CASE WHEN i < 10 THEN i ELSE error('an early failure') END AS %s FROM range(100000) t(i)`
			for range 25 {
				mustFail(fmt.Sprintf(failing, "i"))
			}
			marker := "failing_" + name
			interruptStreams(t, marker, 0, 1)
			mustFail(fmt.Sprintf(failing, marker))

			// It does not fail again: every row, once.
			marker = "passing_" + name
			interruptStreams(t, marker, 0, 1)
			rows, err := conn.Query(ctx, "SELECT i AS "+marker+" FROM range(5000) t(i) ORDER BY i")
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					t.Fatal(err)
				}
				if vals[0].(int64) != int64(n) {
					t.Fatalf("row %d: %v", n, vals)
				}
				n++
			}
			if err := rows.Err(); err != nil || n != 5000 {
				t.Fatalf("after %d rows: %v, want 5000 rows", n, err)
			}
		})
	}
}

// TestStreamFailsAfterRows checks a statement that fails after rows went out,
// DuckDB reporting an interrupt, when nothing asked for one: an internal
// error says so, and not that it was canceled, and the session goes on. So
// does one that may write, which must not run twice, and one in a transaction
// block, which the failure aborted, before a row went out.
func TestStreamFailsAfterRows(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	ts := startServer(t, func(o *Options) { o.Metrics = metrics })
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	wantFailed := func(q string, atLeast int) {
		t.Helper()
		n, err := drainRows(conn, q)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "XX000" || !strings.Contains(pgErr.Message, "was not canceled") {
			t.Fatalf("after %d rows: %v, want the internal error", n, err)
		}
		if n < atLeast {
			t.Fatalf("%d rows reached the client, want at least %d", n, atLeast)
		}
	}
	one := func() {
		t.Helper()
		var v int
		if err := conn.QueryRow(ctx, "SELECT 1").Scan(&v); err != nil || v != 1 {
			t.Fatalf("after the failure: %d, %v", v, err)
		}
	}

	interruptStreams(t, "mid_stream", 1000, 1)
	wantFailed("SELECT i AS mid_stream FROM range(10000000) t(i)", 1000)
	one()
	if got := testutil.ToFloat64(metrics.queries.WithLabelValues("canceled")); got != 0 {
		t.Errorf("counted as canceled %v times", got)
	}
	if got := testutil.ToFloat64(metrics.queries.WithLabelValues("error")); got != 1 {
		t.Errorf("counted as failed %v times, want once", got)
	}

	// A statement that writes runs once.
	mustExec(t, conn, "CREATE TEMP TABLE stream_written (i BIGINT)")
	interruptStreams(t, "written_stream", 0, 1)
	f, _ := falseInterrupts.Load("written_stream")
	f.(*falseInterrupt).report = true
	wantFailed("INSERT INTO stream_written SELECT i FROM range(5000) t(i) RETURNING i AS written_stream", 0)
	one()
	var n int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM stream_written").Scan(&n); err != nil || n != 5000 {
		t.Errorf("after the failure, the table holds %d rows, want those of one run: %v", n, err)
	}

	// One that advances a sequence runs once too.
	mustExec(t, conn, "CREATE TEMP SEQUENCE stream_sequence")
	interruptStreams(t, "sequence_stream", 0, 1)
	wantFailed("SELECT nextval('stream_sequence') AS sequence_stream FROM range(5000) t(i)", 0)
	one()

	// The block is aborted, as any failure leaves it, and the statement cannot
	// run again in it.
	mustExec(t, conn, "BEGIN")
	interruptStreams(t, "block_stream", 0, 1)
	wantFailed("SELECT i AS block_stream FROM range(5000) t(i)", 0)
	if _, err := conn.Exec(ctx, "SELECT 1"); err == nil {
		t.Error("the transaction block runs on after a statement failed in it")
	}
	mustExec(t, conn, "ROLLBACK")
	one()
}

// drainRows reads q's rows, and returns how many reached the client and how
// they ended.
func drainRows(conn *pgx.Conn, q string) (int, error) {
	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		return 0, err
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	return n, rows.Err()
}

// TestStreamInterruptsAskedFor checks that what does end a statement as its
// rows stream is reported as what it is, as a thread's failure must not be:
// the memory watch, as a statement out of memory. A cancel request and the
// statement timeout are in TestStreamedResultCanceled.
func TestStreamInterruptsAskedFor(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	rows, err := conn.Query(ctx, "SELECT i, repeat('x', 100) AS pad FROM range(1000000000) t(i)")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		if n++; n == 100000 {
			if !ts.srv.ShedMemory(1 << 20) {
				t.Fatal("nothing to shed")
			}
		}
	}
	rows.Close()
	var pgErr *pgconn.PgError
	if !errors.As(rows.Err(), &pgErr) || pgErr.Code != "53200" {
		t.Fatalf("after %d rows of a billion and the memory watch: %v, want out of memory", n, rows.Err())
	}
}

// TestStreamFailureClassified checks that nothing in Igloo that goes by a
// statement's error takes the error of one that failed as its rows streamed
// for anything it is not: not a cancel, not a statement out of memory, which
// would run again elsewhere; not a failure in the data or of a block's read,
// not a commit conflict, not bitmaps that did not fit.
func TestStreamFailureClassified(t *testing.T) {
	err := streamFailed(7)
	if got := pgerror.GetCode(pgError(err)); got != codes.Internal {
		t.Errorf("SQLSTATE %s, want internal", got)
	}
	if got := queryOutcome(err); got != "error" {
		t.Errorf("outcome %s, want error", got)
	}
	sess := &session{}
	if sess.ranOutOfMemory(context.Background(), err) || isOutOfMemory(err) || distributed.OutOfMemory(err) {
		t.Error("taken for running out of memory")
	}
	if distributed.DataError(err) || distributed.IsUnsupported(err) || prefetch.BlockRace(err) || commitConflict(err) || bitmapsDidNotFit(err) {
		t.Error("taken for another failure")
	}
}

// TestParameterInResult checks a statement whose result's columns DuckDB cannot
// tell before it has an untyped parameter's value: it fails with a code that
// says so, where it took the session's statement down with a panic, and the
// session goes on. Cast, it runs.
func TestParameterInResult(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeExec)
	ctx := context.Background()
	rows, err := conn.Query(ctx, "SELECT i, i + $1 AS m FROM range(10) t(i) ORDER BY i", int64(5))
	if err == nil {
		for rows.Next() {
		}
		rows.Close()
		err = rows.Err()
	}
	if code := pgCode(err); code != "42P18" {
		t.Fatalf("%v, want SQLSTATE 42P18", err)
	}
	rows, err = conn.Query(ctx, "SELECT i, i + $1::BIGINT AS m FROM range(10) t(i) ORDER BY i", int64(5))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil || n != 10 {
		t.Fatalf("cast: %d rows, %v", n, err)
	}
}

// TestStreamedResultCanceled ends a statement whose rows stream on a
// cancel request while the client reads them, and on the statement
// timeout, and the session runs the next statement.
func TestStreamedResultCanceled(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	const q = `SELECT i, repeat('x', 100) AS pad FROM range(1000000000) t(i)`

	rows, err := conn.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		if n++; n == 100000 {
			if err := conn.PgConn().CancelRequest(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	rows.Close()
	var canceled *pgconn.PgError
	if err := rows.Err(); !errors.As(err, &canceled) || canceled.Code != "57014" {
		t.Fatalf("after %d rows of a billion and a cancel request: %v, want query_canceled", n, err)
	}

	if _, err := conn.Exec(ctx, "SET statement_timeout = '2s'"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rows, err = conn.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	rows.Close()
	var pgErr *pgconn.PgError
	if err := rows.Err(); !errors.As(err, &pgErr) || !strings.Contains(pgErr.Message, "statement timeout") {
		t.Fatalf("after %s: %v, want the statement timeout", time.Since(start), err)
	}
	if took := time.Since(start); took > 30*time.Second {
		t.Errorf("the statement timeout ended the rows' stream after %s", took)
	}
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		t.Fatal(err)
	}
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("after the timeout: %d, %v", one, err)
	}
}

// TestLimitedPortalBetweenStatements reads a portal a few rows at a time
// with another statement run between its reads, in a transaction block,
// as JDBC's fetch size does: the portal's statement is suspended in the
// middle of its rows while the other runs on the session's connection,
// which ends a result DuckDB streams, so a portal with a row limit reads
// its statement's rows whole first, as before.
func TestLimitedPortalBetweenStatements(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	h, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Conn.Close() //nolint:errcheck
	_ = h.Conn.SetDeadline(time.Now().Add(time.Minute))
	fe := h.Frontend
	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	// receive reads messages up to one of the type named until, counting
	// data rows, and fails on an error.
	receive := func(until string) int {
		t.Helper()
		rows := 0
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				rows++
			case *pgproto3.ErrorResponse:
				t.Fatalf("error: %s", m.Message)
			}
			if fmt.Sprintf("%T", msg) == "*pgproto3."+until {
				return rows
			}
		}
	}

	send(&pgproto3.Query{String: "BEGIN"})
	receive("ReadyForQuery")
	send(&pgproto3.Parse{Name: "big", Query: "SELECT i FROM range(100000) t(i) ORDER BY i DESC"},
		&pgproto3.Bind{PreparedStatement: "big", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p", MaxRows: 10},
		&pgproto3.Flush{})
	got := receive("PortalSuspended")
	if got != 10 {
		t.Fatalf("the first read of the portal sent %d rows, want 10", got)
	}
	// Another statement between the portal's reads.
	send(&pgproto3.Parse{Name: "other", Query: "SELECT 42"},
		&pgproto3.Bind{PreparedStatement: "other"},
		&pgproto3.Execute{},
		&pgproto3.Flush{})
	if got := receive("CommandComplete"); got != 1 {
		t.Fatalf("the statement between the portal's reads sent %d rows, want 1", got)
	}
	send(&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
	rest := receive("CommandComplete")
	receive("ReadyForQuery")
	if got+rest != 100000 {
		t.Fatalf("the portal sent %d rows in all, want 100000", got+rest)
	}
	send(&pgproto3.Query{String: "COMMIT"})
	receive("ReadyForQuery")
}

// TestPortalReadsAcrossThreads reads a portal a row at a time, an Execute
// for each, while other sessions run statements, so the session's commands
// run on whichever threads Go has free. A portal resumes in each read the
// coroutine its first read made, and Go ends the process when a switch to
// one is on another thread, or under other thread locks, than it was made
// with: when each command ran locked to its thread, JDBC's fetch size ended
// the process.
func TestPortalReadsAcrossThreads(t *testing.T) {
	ts := startServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	var busy sync.WaitGroup
	for range 4 {
		other := ts.admin(t, pgx.QueryExecModeExec)
		busy.Go(func() {
			for ctx.Err() == nil {
				_, _ = other.Exec(ctx, "SELECT count(*) FROM range(200000) t(i) WHERE i % 7 = 3")
			}
		})
	}
	defer busy.Wait()
	defer cancel()

	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	h, err := conn.PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Conn.Close() //nolint:errcheck
	_ = h.Conn.SetDeadline(time.Now().Add(2 * time.Minute))
	fe := h.Frontend
	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(until string) int {
		t.Helper()
		rows := 0
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatalf("the session ended: %v", err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				rows++
			case *pgproto3.ErrorResponse:
				t.Fatalf("error: %s", m.Message)
			}
			if fmt.Sprintf("%T", msg) == "*pgproto3."+until {
				return rows
			}
		}
	}

	const reads = 400
	send(&pgproto3.Query{String: "BEGIN"})
	receive("ReadyForQuery")
	send(&pgproto3.Parse{Name: "rows", Query: fmt.Sprintf("SELECT i FROM range(%d) t(i)", reads+1)},
		&pgproto3.Bind{PreparedStatement: "rows", DestinationPortal: "p"})
	n := 0
	for range reads {
		send(&pgproto3.Execute{Portal: "p", MaxRows: 1}, &pgproto3.Flush{})
		n += receive("PortalSuspended")
	}
	send(&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
	n += receive("CommandComplete")
	receive("ReadyForQuery")
	if n != reads+1 {
		t.Fatalf("the portal sent %d rows in all, want %d", n, reads+1)
	}
	send(&pgproto3.Query{String: "COMMIT"})
	receive("ReadyForQuery")
}

// TestSortedRowsFromWorkers runs every row of a table ordered by ORDER BY
// without LIMIT on workers, which sort their shares, and the coordinator
// merges them as the client reads them, through a view of the merge it
// registers on the session's connection: the rows are one machine's, in
// its order, in every protocol, and the session runs on after it.
func TestSortedRowsFromWorkers(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	mustExec(t, ds.catalogServer.admin, "INSERT INTO lake.public.orders SELECT i, i % 97 FROM range(2000, 200000) t(i)")
	query := "SELECT id, amount FROM orders ORDER BY amount DESC, id"
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ds.testServer.admin(t, mode)
			ctx := context.Background()
			read := func() string {
				rows, err := conn.Query(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				var b strings.Builder
				for rows.Next() {
					vals, err := rows.Values()
					if err != nil {
						t.Fatal(err)
					}
					fmt.Fprintln(&b, vals...)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return b.String()
			}
			mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
			mustExec(t, conn, "SET distributed_execution = off")
			local := read()
			mustExec(t, conn, "SET distributed_execution = on")
			if got := read(); got != local {
				t.Fatalf("rows from workers differ from one machine's (%d and %d bytes)", len(got), len(local))
			}
			if q := lastRun(t, ds.history, query); q.Workers != 2 {
				t.Errorf("%d workers ran it, want 2", q.Workers)
			}
			var one int
			if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
				t.Fatalf("after it: %d, %v", one, err)
			}
		})
	}
}

// TestStreamFailsReadingMerge checks a statement whose merge of workers'
// sorted rows fails as it streams, DuckDB reporting an interrupt. Before a row
// went out it runs again on this replica, as a merge cannot be read twice, and
// the client gets every row; that streams too, and fails the same way, and
// then runs again read whole. After rows went out, the internal error.
func TestStreamFailsReadingMerge(t *testing.T) {
	t.Parallel()
	// Cores enough to merge on (see distributed's mergesKeepUp), whatever the
	// machine's.
	ds := startDistServerWith(t, t.TempDir(), 2, func(o *distributed.Options) {
		o.Cores, o.Threads = distributed.MergeCores, distributed.MergeCores
	})
	mustExec(t, ds.catalogServer.admin, "INSERT INTO lake.public.orders SELECT i, i % 97 FROM range(2000, 60000) t(i)")
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	read := func(query string) (string, error) {
		rows, err := conn.Query(ctx, query)
		if err != nil {
			return "", err
		}
		var b strings.Builder
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return "", err
			}
			fmt.Fprintln(&b, vals...)
		}
		return b.String(), rows.Err()
	}
	query := "SELECT id AS merge_stream, amount FROM orders ORDER BY amount DESC, id"
	mustExec(t, conn, "SET distributed_execution = off")
	want, err := read(query)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, "SET distributed_execution = on")
	if got, err := read(query); err != nil || got != want {
		t.Fatalf("on workers: %v, %d bytes, want %d", err, len(got), len(want))
	}
	if q := lastRun(t, ds.history, query); q.Workers != 2 {
		t.Fatalf("%d workers ran it, want 2", q.Workers)
	}
	for _, times := range []int32{1, 2} {
		interruptStreams(t, "merge_stream", 0, times)
		if got, err := read(query); err != nil || got != want {
			t.Fatalf("interrupted %d times: %v, %d bytes, want %d", times, err, len(got), len(want))
		}
	}
	interruptStreams(t, "merge_stream", 1000, 1)
	_, err = read(query)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "XX000" || !strings.Contains(pgErr.Message, "was not canceled") {
		t.Fatalf("after rows went out: %v, want the internal error", err)
	}
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("after it: %d, %v", one, err)
	}
}
