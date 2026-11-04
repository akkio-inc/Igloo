package pgwire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/workload"
)

// A statement that runs out of memory runs again rather than fail: on workers
// where auto mode may run it there (see afterOutOfMemory), else here beside
// the others once what it needs is free, then alone, then alone on fewer
// threads.
//
// DuckDB has one memory pool per process and fails whichever statement asks
// for memory the others hold, whatever each would need alone. So the statement
// gives back its place and waits for a reservation of what it held and the
// block it failed to get, and runs again beside the others. If it runs out
// again, it waits for the replica's whole memory and runs alone. It once went
// straight to that, waiting in line for all of the memory and holding back
// every statement behind it that reserved any: six that ran out of memory at
// once beside a dashboard's load stopped the replica for 150 s. Now one
// statement at a time waits to run alone, and holds nobody back as it waits
// (see workload.Limits.Alone); another that comes to that step fails, saying
// why, as does the one waiting if the replica has no moment free of others
// for aloneWait.
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
// again on as many threads would only run out again. log says what the steps
// that run it again do.
type runAlone struct {
	cause error
	alone bool
	log   *slog.Logger
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
	return &runAlone{cause: err, alone: others == 0,
		log: s.log.With("user", sess.currentUser().Name, "pid", sess.pid, "job", sess.job, "others_running", others)}
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
		s.captureOutOfMemory(sess, err)
		if sess.outer.Err() == nil && sess.outOfMemoryHere(err) {
			return s.againAlone(sess, err)
		}
		return err
	}
}

// aloneWait is how long a statement waits for a moment free of others to run
// alone (see above) before it fails, saying so.
const aloneWait = 5 * time.Minute

// runAgainAlone runs fn, the statement whose run under ctx returned alone,
// again: beside the others once what it held and the block it failed to get
// are free, then alone, then alone on fewer threads, for as long as it runs
// out of memory (see above). outer is the context runCancelable ran it under.
// It returns fn's error, why it cannot run alone, or the error it last ran
// out of memory with should it not start again before ctx ends.
func (sess *session) runAgainAlone(outer, ctx context.Context, alone *runAlone, fn func(context.Context) error) error {
	again, cancel := sess.rearm(outer, ctx)
	defer cancel()
	if again.Err() != nil && sess.wasCanceled() {
		// A cancel came as it ran out (see rearm).
		return errCanceledBeforeRun
	}
	if need, ok := sess.againReservation(alone.cause); ok && !alone.alone {
		alone.log.Warn("a statement ran out of memory; running it again beside others once what it held and the block it failed to get are free",
			"reserve", memlimit.Format(need), "err", alone.cause)
		err := sess.runReserved(again, need, alone, fn)
		var more *runAlone
		if !errors.As(err, &more) {
			return err
		}
		alone = more
	}
	return sess.runAloneTurn(again, alone, fn)
}

// againReservation is what a statement that ran out of memory with cause
// reserves to run again beside the others: what it held, as sampled or as it
// reserved, and the block it failed to get. ok is false where that is three
// quarters of the replica's memory or more, which only running alone gives,
// or nothing is known of it.
func (sess *session) againReservation(cause error) (need int64, ok bool) {
	held := sess.wl.reserved
	if sess.wl.seen.Sampled {
		held = max(held, sess.wl.seen.Peak)
	}
	need = held + failedAllocation(cause)
	budget := sess.wl.controller.Budget()
	return need, need > 0 && (budget == 0 || float64(need) < 0.75*float64(budget))
}

// failedAllocation is the size of the block DuckDB's Out of Memory error err
// says it failed to get, or 0.
func failedAllocation(err error) int64 {
	m := allocationSize.FindStringSubmatch(err.Error())
	if m == nil {
		return 0
	}
	n, perr := config.ParseSize(m[1])
	if perr != nil {
		return 0
	}
	return n
}

// allocationSize is the size in DuckDB's Out of Memory errors: "failed to
// allocate data of size 8.0 KiB", "could not allocate block of size 2.3 GiB",
// "failed to pin block of size 256.0 KiB".
var allocationSize = regexp.MustCompile(`of size ([0-9.]+ ?[KMGT]?i?B)`)

// runReserved runs fn under ctx once a reservation of need is free, beside
// the others; alone is how its run before ran out.
func (sess *session) runReserved(ctx context.Context, need int64, alone *runAlone, fn func(context.Context) error) error {
	l := sess.wl.limits
	l.Memory, l.Estimated, l.Unseen = need, false, false
	l.Counted, l.Apart = sess.wl.counted, sess.wl.apart
	done, _, err := sess.wl.controller.Admit(ctx, l)
	if err != nil {
		if sess.wasCanceled() {
			return errCanceledBeforeRun
		}
		return alone.cause
	}
	// Its history keeps the larger of what its runs held.
	defer func() { sess.wl.keepPeak(done()) }()
	// What a park lets go of (see Server.parked).
	sess.wl.admitted = func() { done() }
	defer func() { sess.wl.admitted = nil }()
	return fn(ctx)
}

// runAloneTurn runs fn under ctx alone, once the replica runs nothing else
// (see workload.Limits.Alone), and then on fewer threads for as long as it
// runs out of memory; alone is how its run before ran out.
func (sess *session) runAloneTurn(ctx context.Context, alone *runAlone, fn func(context.Context) error) error {
	l := sess.wl.limits
	l.Memory, l.Estimated, l.Unseen, l.Alone = 0, false, false, true
	l.Counted, l.Apart = sess.wl.counted, sess.wl.apart
	wait, stop := context.WithTimeout(ctx, aloneWait)
	done, _, err := sess.wl.controller.Admit(wait, l)
	stop()
	switch {
	case errors.Is(err, workload.ErrAloneTaken):
		alone.log.Warn("a statement ran out of memory, and another waits to run alone; it fails", "err", alone.cause)
		return sess.outOfMemoryError(alone.cause, "beside other statements, and another was already waiting to run alone", true)
	case err != nil && sess.wasCanceled():
		return errCanceledBeforeRun
	case err != nil && ctx.Err() == nil:
		alone.log.Warn("a statement that ran out of memory found no moment to run alone; it fails", "waited", aloneWait, "err", alone.cause)
		return sess.outOfMemoryError(alone.cause,
			fmt.Sprintf("beside other statements, and the replica had no moment free of them in %s to run it alone", aloneWait), true)
	case err != nil:
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
		alone.log.Warn("a statement ran out of memory; running it again alone", "err", alone.cause)
		err = fn(ctx)
	}
	threads := 0
	for {
		var more *runAlone
		if !errors.As(err, &more) {
			return err
		}
		if threads == 0 {
			if threads = sess.threads(ctx); threads == 0 {
				return more.cause
			}
		}
		if threads /= 2; threads < 1 {
			return sess.outOfMemoryError(more.cause, "running alone, on one thread", false)
		}
		more.log.Warn("a statement ran out of memory alone; running it again on fewer threads", "threads", threads, "err", more.cause)
		restore, ok := sess.setThreads(ctx, threads)
		if !ok {
			return more.cause
		}
		err = fn(ctx)
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

// outOfMemoryError is the error of a statement that ran out of memory, cause,
// where it gives up, which how says: running alone on one thread, or beside
// others when it could not run alone (busy). It says so, and what would let
// it finish, which are the operator's choices and the session's to make.
func (sess *session) outOfMemoryError(cause error, how string, busy bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	limit, err := settingValue(ctx, sess.conn, "memory_limit")
	if err != nil {
		limit = "its"
	}
	finish := "a higher memory limit (IGLOO_MEMORY_LIMIT)"
	if busy {
		finish = "fewer statements beside it, or " + finish
	}
	if sess.mode() == distributed.ModeOff {
		finish += ", or on workers with distributed_execution 'auto' or 'on' (it is 'off')"
	}
	return pgerror.WithCode(fmt.Errorf("this statement ran out of memory %s, under DuckDB's memory limit of %s; it would finish with %s: %w",
		how, limit, finish, cause), codes.OutOfMemory)
}
