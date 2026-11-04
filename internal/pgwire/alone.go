package pgwire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/distributed"
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
	if err == nil || sent || st.ai || st.changesIceberg || st.staged != nil {
		return false
	}
	switch st.stmtType {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE,
		duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
	default:
		return false
	}
	return sess.outOfMemoryHere(err)
}

// outOfMemoryHere says err is DuckDB's Out of Memory error, outside a
// transaction block, and not the memory watch's cancel (ShedMemory), which is
// for the process's memory: running the statement again would use it up
// again.
func (sess *session) outOfMemoryHere(err error) bool {
	if err == nil || sess.inTx {
		return false
	}
	sess.mu.Lock()
	shed := sess.shed
	sess.mu.Unlock()
	var de *duckdb.Error
	return !shed && errors.As(err, &de) && de.Type == duckdb.ErrorTypeOutOfMemory
}

// againAlone is what a run that ran out of memory with err, and may run
// again, returns to runCancelable.
func (s *Server) againAlone(sess *session, err error) error {
	others := s.othersRunning(sess)
	how := "running it again once it can run alone"
	if sess.alone || others == 0 {
		how = "running it again on fewer threads"
	}
	s.log.Warn("a statement ran out of memory; "+how, "user", sess.currentUser().Name, "pid", sess.pid, "job", sess.job, "err", err)
	return &runAlone{cause: err, alone: others == 0}
}

// rerunsAlone has fn, run under runCancelable, run again alone, then on fewer
// threads, when it runs out of memory, as a client's statement does: a job's
// work that, failed, leaves nothing that a run from its start trips on, and
// that holds no lock while it is not running, which the statements it waits
// for may need. A refresh of a materialized view or a dynamic table writes in
// one transaction at its end, makes its temporary tables again and goes in
// full if its table moved, so that running it again is its next scheduled
// refresh, sooner; 9 of 11 refreshes beside a mixed load had failed, out of
// memory at 43.4 of 44.2 GiB. A pipe's COPY INTO that runs out of memory
// loads none of its files and gives back its claim on them.
func (s *Server) rerunsAlone(sess *session, fn func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		err := fn(ctx)
		if sess.outer.Err() == nil && sess.outOfMemoryHere(err) {
			return s.againAlone(sess, err)
		}
		return err
	}
}

// runAgainAlone runs fn, the statement whose run under ctx returned alone,
// again once it holds the replica's whole memory, and then on fewer threads
// for as long as it runs out of memory (see above). outer is the context
// runCancelable ran it under. It returns fn's error, or the one the statement
// first ran out of memory with should it not start again before ctx ends.
func (sess *session) runAgainAlone(outer, ctx context.Context, alone *runAlone, fn func(context.Context) error) error {
	again, cancel := sess.rearm(outer, ctx)
	defer cancel()
	if again.Err() != nil && sess.wasCanceled() {
		// A cancel came as it ran out (see rearm).
		return errCanceledBeforeRun
	}
	l := sess.wl.limits
	l.Memory, l.Estimated, l.Unseen = math.MaxInt64, false, false
	l.Counted, l.Apart = sess.wl.counted, sess.wl.apart
	done, _, err := sess.wl.controller.Admit(again, l)
	if err != nil {
		if sess.wasCanceled() {
			// As it waited.
			return errCanceledBeforeRun
		}
		return alone.cause
	}
	// Its history keeps the larger of what its runs held.
	defer func() { sess.wl.keepPeak(done()) }()
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
			return sess.outOfMemoryAlone(more.cause)
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

// outOfMemoryAlone is the error of a statement that ran out of memory, cause,
// running alone on one thread: it says so, and what would let it finish, which
// are the operator's choices and the session's to make.
func (sess *session) outOfMemoryAlone(cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	limit, err := settingValue(ctx, sess.conn, "memory_limit")
	if err != nil {
		limit = "its"
	}
	finish := "a higher memory limit (IGLOO_MEMORY_LIMIT)"
	if sess.mode() == distributed.ModeOff {
		finish += ", or on workers with distributed_execution 'auto' or 'on' (it is 'off')"
	}
	return pgerror.WithCode(fmt.Errorf("this statement ran out of memory running alone, on one thread, under DuckDB's memory limit of %s; it would finish with %s: %w",
		limit, finish, cause), codes.OutOfMemory)
}
