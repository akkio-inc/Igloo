package pgwire

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// A statement that runs out of memory runs again rather than fail, first
// alone, then alone on fewer threads.
//
// DuckDB has one memory pool per process and fails whichever statement asks
// for memory the others hold, whatever each would need alone. So the statement
// gives back its place and waits for the replica's whole memory (see
// workload.Controller: a reservation of all of it starts once no other
// reservation is held, and none starts after it meanwhile), then runs again.
//
// Some of DuckDB's operators hold memory for each thread, which they cannot
// spill: an aggregate over grouping sets keeps a hash table for each set on
// each thread. TPC-DS q67's ROLLUP of 8 columns, at scale 10 in 700 MB, ran out
// of memory alone within 3 s on 16, 8 or 4 threads, and finished on 2 in
// 8 s; at scale 100 in 7 GiB it spilled 29 GiB over two minutes before it ran
// out. So a statement that runs out of memory alone runs again on half the
// threads, and so on down to one, DuckDB's threads set lower for the
// replica's DuckDB while it runs.
//
// It runs again only before it sent the client a row, outside a transaction
// block, and when running it again changes nothing it did not already, as for
// a query or a change of a table's rows, which DuckDB commits to the catalog in
// one step at the end, or not at all.

// runAlone is what a statement's run returns to runCancelable when it ran out
// of memory and may run again (see mayRunAlone); cause is the error it ran out
// with, and alone says no other statement ran when it did, so that running it
// alone again on as many threads would only run out again.
type runAlone struct {
	cause error
	alone bool
}

func (r *runAlone) Error() string { return r.cause.Error() }
func (r *runAlone) Unwrap() error { return r.cause }

// mayRunAlone says whether st, whose run ended with err, sent the client rows
// if sent, ran out of memory and may run again (see above).
func (sess *session) mayRunAlone(st *statement, err error, sent bool) bool {
	if err == nil || sent || sess.inTx || st.ai || st.changesIceberg || st.staged != nil {
		return false
	}
	switch st.stmtType {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE,
		duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
	default:
		return false
	}
	// DuckDB's own error, and not the memory watch's cancel (ShedMemory), which
	// is for the process's memory: running the statement again would use it up
	// again.
	sess.mu.Lock()
	shed := sess.shed
	sess.mu.Unlock()
	var de *duckdb.Error
	return !shed && errors.As(err, &de) && de.Type == duckdb.ErrorTypeOutOfMemory
}

// runAgainAlone runs fn, the statement whose run under ctx returned alone,
// again once it holds the replica's whole memory, and then on fewer threads
// for as long as it runs out of memory (see above). outer is the context
// runCancelable ran it under. It returns fn's error, or the one the statement
// first ran out of memory with should it not start again before ctx ends.
func (sess *session) runAgainAlone(outer, ctx context.Context, alone *runAlone, fn func(context.Context) error) error {
	again, cancel := sess.rearm(outer, ctx)
	defer cancel()
	l := sess.wl.limits
	l.Memory, l.Estimated, l.Unseen = math.MaxInt64, false, false
	l.Counted, l.Apart = sess.wl.counted, sess.wl.apart
	done, _, err := sess.wl.controller.Admit(again, l)
	if err != nil {
		return alone.cause
	}
	defer done()
	// What a park lets go of (see Server.parked).
	sess.wl.admitted = func() { done() }
	defer func() { sess.wl.admitted = nil }()
	sess.alone = true
	defer func() { sess.alone = false }()
	if alone.alone {
		// Straight to fewer threads: at scale 100, q67 ran two minutes alone
		// before it ran out.
		err = alone
	} else {
		err = fn(again)
	}
	threads := 0
	for {
		var more *runAlone
		if !errors.As(err, &more) {
			return err
		}
		if threads == 0 {
			if threads = sess.threads(again); threads == 0 {
				return more.cause
			}
		}
		if threads /= 2; threads < 1 {
			return more.cause
		}
		restore, ok := sess.setThreads(again, threads)
		if !ok {
			return more.cause
		}
		err = fn(again)
		restore()
	}
}

// threads is how many threads the session's DuckDB runs statements on, or 0
// if it cannot tell.
func (sess *session) threads(ctx context.Context) int {
	v, err := settingValue(ctx, sess.conn, "threads")
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(v)
	return n
}

// setThreads has the session's DuckDBs, its first and, if it has used it, its
// second (see background.go), run statements on n threads until the function
// it returns puts back how many they ran on; ok is false if neither does.
func (sess *session) setThreads(ctx context.Context, n int) (restore func(), ok bool) {
	conns := []*duckdb.Conn{sess.conn}
	if bc := sess.bgConn.Load(); bc != nil {
		conns = append(conns, bc.conn)
	}
	var restores []func()
	for _, c := range conns {
		was, err := settingValue(ctx, c, "threads")
		if err != nil {
			continue
		}
		if _, err := c.ExecContext(ctx, "SET GLOBAL threads = "+strconv.Itoa(n), nil); err != nil {
			continue
		}
		restores = append(restores, func() {
			// Whatever ended the statement's run.
			rctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = c.ExecContext(rctx, "SET GLOBAL threads = "+was, nil)
		})
	}
	return func() {
		for _, r := range restores {
			r()
		}
	}, len(restores) > 0
}
