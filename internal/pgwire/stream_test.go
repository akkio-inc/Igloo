package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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
			if err := rows.Err(); err == nil || !strings.Contains(err.Error(), "a late failure") {
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
