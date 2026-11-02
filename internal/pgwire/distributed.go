package pgwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/models"
	"github.com/akkio-inc/igloo/internal/snowflake"
	"github.com/akkio-inc/igloo/internal/tsforecast"
)

// Distributed execution (docs/distributed-execution.md): a SELECT that
// reads a large table may run in parts on workers. The statement is
// authorized as always before it gets here, and the parts read only the
// tables the statement reads, resolved the way DuckDB resolves them in
// this session; the rest of the statement, which combines the parts'
// results, runs on the session's own connection. Anything that cannot run
// distributed, and any failure of the workers, runs here as usual.

var (
	setDistributed   = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+)?distributed_execution\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetDistributed = regexp.MustCompile(`(?is)^\s*RESET\s+distributed_execution\s*;?\s*$`)
	showDistributed  = regexp.MustCompile(`(?is)^\s*SHOW\s+distributed_execution\s*;?\s*$`)
	// explainDistributed is EXPLAIN DISTRIBUTED and the query it explains.
	explainDistributed = regexp.MustCompile(`(?is)^\s*EXPLAIN\s+DISTRIBUTED\s+(.+?)\s*;?\s*$`)
)

// distributedCommand handles SET, RESET and SHOW distributed_execution,
// which says whether the session's statements may run on workers: auto
// (the default), on (whenever they can, for testing and benchmarks) or off.
func (s *Server) distributedCommand(sess *session, query string) (wire.PreparedStatements, bool) {
	respond := func(fn wire.PreparedStatementFn, opts ...wire.PreparedOptionFn) (wire.PreparedStatements, bool) {
		return wire.Prepared(wire.NewStatement(s.audited(sess, query, fn), opts...)), true
	}
	if m := setDistributed.FindStringSubmatch(query); m != nil {
		mode, ok := distributed.ParseMode(strings.Trim(m[1], `'"`))
		if strings.EqualFold(strings.Trim(m[1], `'"`), "default") {
			mode, ok = distributed.ModeAuto, true
		}
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if !ok {
				return pgerror.WithCode(fmt.Errorf("distributed_execution must be auto, on or off, not %s", m[1]), codes.InvalidParameterValue)
			}
			sess.distMode = mode
			return w.Complete("SET")
		})
	}
	if resetDistributed.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			sess.distMode = distributed.ModeAuto
			return w.Complete("RESET")
		})
	}
	if m := explainDistributed.FindStringSubmatch(query); m != nil {
		return respond(func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			// A cancel request and the session's statement timeout end it,
			// as they end any statement, and nothing else does.
			ctx, done := sess.cancelable(ctx, query)
			rows, err := s.explainDistributed(ctx, sess, m[1])
			if err = done(err); err != nil {
				return err
			}
			for _, r := range rows {
				if err := w.Row([]any{r[0], r[1]}); err != nil {
					return err
				}
			}
			return w.Complete("EXPLAIN")
		}, wire.WithColumns(wire.Columns{{Name: "explain_key", Oid: pgtype.TextOID, Width: -1}, {Name: "explain_value", Oid: pgtype.TextOID, Width: -1}}))
	}
	if showDistributed.MatchString(query) {
		return respond(func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			if err := w.Row([]any{string(sess.mode())}); err != nil {
				return err
			}
			return w.Complete("SHOW")
		}, wire.WithColumns(wire.Columns{{Name: "distributed_execution", Oid: pgtype.TextOID, Width: -1}}))
	}
	return nil, false
}

// explainDistributed says how query would run, without running it (see
// distributed.Coordinator.Explain): checked as the statement would be, by
// the session's user, and with the reason it runs on this replica when it
// cannot run on workers.
func (s *Server) explainDistributed(ctx context.Context, sess *session, query string) ([][2]string, error) {
	// Preparing several statements runs all but the last.
	if n, err := s.engine.CountStatements(query); err != nil || n != 1 {
		return nil, pgerror.WithCode(errors.New("EXPLAIN DISTRIBUTED explains one statement"), codes.Syntax)
	}
	user := sess.currentUser()
	st, err := s.prepareChecked(ctx, sess, user, query, nil)
	if err != nil {
		return nil, err
	}
	defer st.close() //nolint:errcheck
	return s.explainStatement(ctx, sess, user, st)
}

// explainStatement says where st, prepared by user, would run, as running
// it decides: readied as its run readies it (readyStatement: its bitmaps'
// variables set, and prepared again through ID domains' ids and bitmap
// indexes), with the session's setting for counting in roaring bitmaps as
// its run has it, past the checks runDistributed makes, in the session's
// mode; then planned and checked as Plan and Execute do. A statement
// whose EXPLAIN DISTRIBUTED said workers ran on this replica, its keywords
// table read in full with a list column whose nesting the planner was not
// told: workers refused it once they had their tasks. It changes nothing
// in the session that its caller could see: the variables it set are
// dropped again, those set before it kept.
func (s *Server) explainStatement(ctx context.Context, sess *session, user auth.User, st *statement) ([][2]string, error) {
	here := func(reason string) ([][2]string, error) {
		return [][2]string{{"runs on", "this replica"}, {"reason", reason}}, nil
	}
	mode := sessionMode(sess, user)
	switch {
	case s.dist == nil:
		return here("this replica has no workers")
	case st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows:
		return here("only queries run on workers")
	case sess.inTx:
		return here("it is in a transaction, which reads its own snapshots")
	case mode == distributed.ModeOff:
		return here("distributed_execution is off")
	}
	set := sess.setBitmapVariables()
	defer sess.resetBitmapVariablesExcept(ctx, set)
	restore := sess.setBitmaps(ctx, st.bitmaps)
	defer func() { restore() }()
	bitmaps := st.bitmaps
	if err := s.readyStatement(ctx, sess, st); err != nil {
		return nil, err
	}
	if st.bitmaps != bitmaps {
		restore()
		restore = sess.setBitmaps(ctx, st.bitmaps)
	}
	if reason := policyReason(st); reason != "" {
		return here(reason)
	}
	return s.dist.Explain(ctx, st.query, &sessionEnv{s: s, sess: sess, user: user, st: st}, mode)
}

func (sess *session) mode() distributed.Mode {
	if sess.distMode == "" {
		return distributed.ModeAuto
	}
	return sess.distMode
}

// errWorkerMemory is what a statement fails with when a worker, about to
// run out of memory, canceled its part (distributed.ErrWorkerMemory).
var errWorkerMemory = pgerror.WithCode(errors.New("canceling statement: a worker was running out of memory, "+
	"and this statement's part had run the longest there; run it again, or on less data at a time"), codes.OutOfMemory)

// distParts is what running a statement in parts took, for the history:
// how many workers ran them, how many stages, and how many exchanges the
// stages wrote. key is the statement's plan key (distributed.Plan.Key)
// when it ran on workers or auto mode chose one machine for it, so that
// how long it took informs that choice next time. since, if set, is when
// it began to run on workers after running out of memory here.
type distParts struct {
	workers, stages, exchanges int
	// large says the large worker ran them; placement is where auto mode,
	// or the session's mode, ran the statement and why, for the history.
	large     bool
	placement string
	key       string
	since     time.Time
	// release lets the workers count as idle again: the statement holds
	// them while it runs on this replica after their tasks failed, or
	// where auto mode chose it, for the guard (see guardLocal).
	release func()
	// local is how long auto mode estimated this replica would take the
	// statement, when it chose to run it here though workers could run it,
	// alt how long it estimated the workers would, and budget, if set, how
	// long it may take all the same (see guardAfter).
	local, alt, budget time.Duration
}

// sessionMode is the mode user's statements run distributed in: the
// session's, but only admins start workers at will, or take every running
// one.
func sessionMode(sess *session, user auth.User) distributed.Mode {
	mode := sess.mode()
	if mode == distributed.ModeOn && user.Role != auth.RoleAdmin {
		mode = distributed.ModeOnRunning
	}
	return mode
}

// logHere says why a statement stays on this replica: for all to see where the session
// asked for workers, which is the one time that surprises anyone.
func (s *Server) logHere(mode distributed.Mode, msg string, args ...any) {
	if mode == distributed.ModeOn || mode == distributed.ModeOnRunning {
		s.log.Info(msg, args...)
		return
	}
	s.log.Debug(msg, args...)
}

// runDistributed runs st on workers if it qualifies in mode and writes its
// rows to w. done is false when st did not run, and should run on this
// replica; parts is what running it took.
func (s *Server) runDistributed(ctx context.Context, sess *session, user auth.User, st *statement, args []driver.NamedValue, w *tagWriter, mode distributed.Mode) (parts distParts, done bool, err error) {
	r, parts, done, err := s.startDistributed(ctx, sess, user, st, mode)
	if r == nil {
		return parts, done, err
	}
	defer r.end()
	if err := st.runFinal(ctx, sess.conn, r.ex.SQL, r.ex.Merged(), args, w); err != nil {
		if w.Written() > 0 || ctx.Err() != nil || distributed.DataError(err) || bitmapsDidNotFit(err) {
			// Combining the workers' bitmaps did not fit either: the
			// session runs the statement again with the hash.
			return r.parts, true, err
		}
		s.dist.Metrics().Fallback("combine_error")
		s.log.Warn("could not combine a distributed statement's results; running it here", "user", user.Name, "err", err)
		return distParts{release: s.dist.Keep()}, false, nil
	}
	return r.parts, true, nil
}

// distRun is a statement's parts as they ran on workers: ex holds their
// results, which ex.SQL, run on this replica, combines into the
// statement's; parts is how they ran, and end lets go of the results and
// of the workers.
type distRun struct {
	ex    *distributed.Execution
	parts distParts
	end   func()
}

// startDistributed runs st's parts on workers if it qualifies in mode, and
// returns their results to combine (see runDistributed). Without them,
// done false says st runs on this replica, and parts why, for the guard;
// done true that it must not, and err why.
func (s *Server) startDistributed(ctx context.Context, sess *session, user auth.User, st *statement, mode distributed.Mode) (run *distRun, parts distParts, done bool, err error) {
	if s.dist == nil || st.stmtType != duckdb.STATEMENT_TYPE_SELECT || st.kind != returnsRows || sess.inTx || mode == distributed.ModeOff {
		return nil, parts, false, nil
	}
	if reason := policyReason(st); reason != "" {
		s.logHere(mode, "statement runs on this replica", "user", user.Name, "reason", reason)
		return nil, parts, false, nil
	}
	env := &sessionEnv{s: s, sess: sess, user: user, st: st}
	plan, err := s.dist.Plan(ctx, st.query, env, mode)
	if err != nil {
		if ctx.Err() != nil {
			return nil, parts, true, ctx.Err()
		}
		parts.key = distributed.PlanKey(err)
		if parts.key != "" {
			// Auto mode chose this replica.
			parts.placement = "this replica: " + strings.TrimPrefix(err.Error(), "cannot distribute: ")
		}
		if est, ok := distributed.LocalEstimate(err); ok {
			// Workers stay up while it runs here, as an idle replica would
			// stop them, so that the guard can run it on them.
			parts.local, parts.release = est, s.dist.Keep()
			parts.alt, _ = distributed.WorkersEstimate(err)
			parts.budget, _ = distributed.LocalBudget(err)
		}
		if !distributed.IsUnsupported(err) {
			s.log.Warn("could not plan a distributed statement; running it here", "user", user.Name, "err", err)
		} else {
			s.logHere(mode, "statement runs on this replica", "user", user.Name, "reason", err)
		}
		return nil, parts, false, nil
	}
	settings, err := s.distributedSettings(ctx, sess)
	if err != nil {
		s.logHere(mode, "statement runs on this replica", "user", user.Name, "reason", err)
		return nil, parts, false, nil
	}
	if st.writer != nil && plan.WorkersWrite() {
		// Its tasks write the table's files (see tableWrite).
		target, err := st.writer.begin(ctx)
		if err != nil {
			s.log.Warn("workers will not write a table's files; this replica writes them", "user", user.Name, "err", err)
		} else {
			plan.Write(target)
		}
	}
	// The workers stay up while the statement runs, here too.
	release := s.dist.Keep()
	start := time.Now()
	if s.streamMerged {
		// A merge of the workers' sorted rows reaches the client as it is
		// made (see runFinal).
		plan.StreamMerged()
	}
	ex, err := s.dist.Execute(ctx, plan, settings)
	if err != nil {
		// Released now if the statement is done; otherwise once it has run
		// here (parts.release).
		defer func() {
			if done {
				release()
			} else {
				parts.release = release
			}
		}()
		if ctx.Err() == nil && !distributed.DataError(err) && !distributed.BitmapMemory(err) && !distributed.IsUnsupported(err) &&
			!distributed.WriteDenied(err) && !distributed.WriteRefused(err) {
			// How long it ran there before it failed, so that auto mode
			// takes that way to take longer for a while.
			s.dist.ObserveFailure(plan.Key, plan.Workers(), plan.Large(), time.Since(start))
		}
		switch {
		case ctx.Err() != nil:
			return nil, parts, true, ctx.Err()
		case distributed.DataError(err):
			return nil, parts, true, err
		case distributed.BitmapMemory(err):
			// A task's bitmaps did not fit: the session runs the statement
			// again with DuckDB's hash (see rerunWithoutBitmaps).
			return nil, parts, true, err
		case distributed.OutOfMemory(err) && st.fill && plan.FitsHere():
			// A fill's tasks hold what writing Parquet takes, which is not what
			// a read's query does: this replica may well write it, and has what
			// its plan holds. It has had the retries with smaller tasks.
			s.dist.Metrics().Fallback("worker_memory")
			s.log.Warn("workers ran out of memory on a fill, which this replica has the room for; running it here", "user", user.Name, "err", err)
			return nil, parts, false, nil
		case distributed.OutOfMemory(err) && st.writer != nil && plan.WorkersWrite():
			// Too much for this replica's memory: its tasks may fare better
			// with this replica writing the table from their results.
			s.dist.Metrics().Fallback("worker_memory")
			s.log.Warn("workers ran out of memory writing a table's files; this replica writes them", "user", user.Name, "err", err)
			st.writer = nil
			r, p, ran, err := s.startDistributed(ctx, sess, user, st, mode)
			if r != nil {
				return r, r.parts, true, nil
			}
			if ran {
				return nil, p, true, err
			}
			if p.release != nil {
				p.release()
			}
			return nil, p, false, nil
		case distributed.OutOfMemory(err):
			// Another worker, or this replica, would run out the same way.
			// The query history records where it failed, for the replicas
			// that read it when they start.
			s.log.Warn("a worker ran out of memory running part of a statement; the statement fails rather than run again",
				"user", user.Name, "err", err)
			return nil, distParts{workers: plan.Workers(), large: plan.Large(), placement: plan.Placement(), key: plan.Key}, true, errWorkerMemory
		case distributed.Behind(err) && !st.behindOnce:
			// A worker knows of a later deletion than this coordinator did as the
			// statement began. It knows now, and nothing ran: planned again, the
			// statement begins after it, and reads tables as they are then.
			st.behindOnce = true
			s.log.Info("a worker was purged for a later erasure request than this replica had heard of; planning the statement again",
				"user", user.Name, "err", err)
			r, p, ran, err := s.startDistributed(ctx, sess, user, st, mode)
			if r != nil {
				return r, r.parts, true, nil
			}
			if ran {
				return nil, p, true, err
			}
			if p.release != nil {
				p.release()
			}
			return nil, p, false, nil
		case distributed.IsUnsupported(err):
			// The workers found what planning could not, such as data
			// files without field IDs.
			s.logHere(mode, "statement runs on this replica", "user", user.Name, "reason", err)
			return nil, parts, false, nil
		case distributed.WriteDenied(err) && st.writer != nil:
			// Workers may not write the table's files: they run the query, and
			// this replica writes the table from their results.
			s.log.Warn("workers may not write a table's files; this replica writes them", "user", user.Name, "err", err)
			st.writer = nil
			r, p, ran, err := s.startDistributed(ctx, sess, user, st, mode)
			if r != nil {
				return r, r.parts, true, nil
			}
			if ran {
				return nil, p, true, err
			}
			if p.release != nil {
				// This statement's own keeps the workers up.
				p.release()
			}
			return nil, p, false, nil
		case distributed.WriteRefused(err):
			// A worker said it wrote others: nothing of it is committed, and
			// this replica runs the statement.
			s.log.Error("a worker said it wrote files outside the statement's directory; running the statement here", "user", user.Name, "err", err)
			return nil, parts, false, nil
		}
		if plan.Large() && !plan.FitsHere() {
			// The large worker failed, and this replica would not hold the
			// statement: the fleet runs it, as a statement forced onto the
			// workers does.
			s.log.Warn("the large worker could not run a statement, which this replica would not hold; running it on the workers",
				"user", user.Name, "err", err)
			r, p, ran, err := s.startDistributed(ctx, sess, user, st, distributed.ModeOn)
			if r != nil {
				r.parts.placement = "after the large worker failed: " + r.parts.placement
				return r, r.parts, true, nil
			}
			if ran {
				return nil, p, true, err
			}
			if p.release != nil {
				p.release()
			}
		}
		if !plan.FitsHere() {
			// It would run out of memory here, or spill for far longer than
			// the workers took, after taking as long as it does on one
			// machine: fail now.
			s.log.Warn("workers could not run a statement, which would not fit in this replica's memory; it fails",
				"user", user.Name, "err", err)
			return nil, parts, true, fmt.Errorf("workers could not run the statement, and it needs more memory than this replica has: %w", err)
		}
		reason := "worker_error"
		if errors.Is(err, distributed.ErrNoWorkers) {
			reason = "no_workers"
		}
		s.dist.Metrics().Fallback(reason)
		s.log.Warn("workers could not run a statement; running it here", "user", user.Name, "err", err)
		return nil, parts, false, nil
	}
	parts = distParts{workers: ex.Workers, large: ex.Large, placement: plan.Placement(), stages: ex.Stages, exchanges: ex.Exchanges, key: plan.Key}
	return &distRun{ex: ex, parts: parts, end: func() { ex.Close(); release() }}, parts, true, nil
}

// guardFactor, guardAlternative and guardMinimum say when a statement auto
// mode chose to run on this replica, though workers could run it, has run
// long enough there to run it on workers instead (see guardAfter).
// Variables, for tests.
var (
	guardFactor      = 3.0
	guardAlternative = 1.0
	guardMinimum     = 15 * time.Second
	// guardShared is guardFactor while other statements run beside it.
	guardShared = 1.0
)

// guardUnknown is when the guard fires for a statement whose workers'
// estimate is not known, as the guard fired before it weighed them.
const guardUnknown = 90 * time.Second

// guardAfter is how long a statement auto mode estimated at est on this
// replica, and at alt on workers, their start included, runs here before
// the guard runs it on workers instead: once it has run guardFactor times
// its estimate, or, while others run beside it (others), its estimate
// once, as it takes their cores; and once the workers, from scratch, are
// estimated to take no longer than it has run here (guardAlternative times
// alt), so that running it there is clearly the better bet; never before
// guardMinimum, nor budget, what a run on workers took when auto mode tries
// this replica after one (see distributed.LocalBudget). The guard weighs it
// again every guardCheck, as statements come and go beside it.
//
// It once waited as many times longer as statements ran when one started,
// as they shared the replica's cores: at ten times the data, beside 16
// users' dashboards, a report estimated at 65 s here ran 577 s, the
// dashboards' median rising from 4 ms to 23 s, and the guard never fired.
//
// Moving when the run here has taken what the workers would bounds the
// loss at about twice the faster side's time, as long as their estimate
// holds: either the statement finishes here before, or it pays its time
// here and then theirs. A fixed floor of 90 s let a report estimated at 13
// s here and 30 s on workers, which took 58 to 97 s here and 50 s on 16
// workers at ten times the data, run 90 s before moving: 140 s. It now
// moves at 39 s. A statement whose workers are estimated far slower than
// its run here is never moved there, however long it runs, and one that
// ran on workers after the guard moved it runs there from then on, so the
// guard cannot send a statement back and forth.
func guardAfter(est, alt, budget time.Duration, others int) time.Duration {
	factor := guardFactor
	if others > 0 {
		factor = min(factor, guardShared)
	}
	after := time.Duration(factor * float64(est))
	if alt <= 0 {
		after = max(after, guardUnknown)
	}
	return max(after, time.Duration(guardAlternative*float64(alt)), guardMinimum, budget)
}

// guardCheck is how often the guard weighs a statement again.
var guardCheck = time.Second

// othersRunning counts the statements running on this replica beside
// sess's, which share its cores: its other sessions' that are past waiting
// for their limits.
func (s *Server) othersRunning(sess *session) int {
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	for _, other := range r.byPID {
		if other != sess {
			all = append(all, other)
		}
	}
	r.mu.Unlock()
	n := 0
	for _, other := range all {
		other.mu.Lock()
		if other.statement != "" && !other.queued {
			n++
		}
		other.mu.Unlock()
	}
	return n
}

// localGuard cancels a statement's run on this replica once it has run
// past its time (see guardAfter), unless it has sent the client output.
type localGuard struct {
	ctx    context.Context
	cancel context.CancelFunc
	gate   rowGate
	start  time.Time
	fired  atomic.Bool
	// done ends the guard's checks, once, when it stops.
	done     chan struct{}
	stopOnce sync.Once
}

// guardLocal arms a guard over a run on this replica, which writes to tw,
// that fires after d (see guardAfter): run it under the guard's ctx, and
// stop the guard after.
func guardLocal(ctx context.Context, d time.Duration, tw *tagWriter) *localGuard {
	return guardLocalBy(ctx, func() time.Duration { return d }, tw)
}

// guardLocalBy is guardLocal of a time that after says again every
// guardCheck, as the statements beside the run come and go.
func guardLocalBy(ctx context.Context, after func() time.Duration, tw *tagWriter) *localGuard {
	g := &localGuard{start: time.Now(), done: make(chan struct{})}
	g.ctx, g.cancel = context.WithCancel(ctx)
	tw.gate = &g.gate
	fire := func() bool {
		if g.gate.fire() {
			g.fired.Store(true)
			g.cancel()
			return true
		}
		return false
	}
	every := min(guardCheck, max(after()/4, 10*time.Millisecond))
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-g.done:
				return
			case <-g.ctx.Done():
				return
			case <-t.C:
				if time.Since(g.start) >= after() && fire() {
					return
				}
			}
		}
	}()
	return g
}

// stop disarms the guard, and says whether it canceled the run and how
// long the run had taken.
func (g *localGuard) stop(tw *tagWriter) (fired bool, ran time.Duration) {
	g.stopOnce.Do(func() { close(g.done) })
	g.cancel()
	tw.gate = nil
	return g.fired.Load(), time.Since(g.start)
}

// afterGuard runs st on workers after its run on this replica, where auto
// mode had chosen to run it, passed its time (see guardAfter) before it
// sent the client output: the client gets the rows as if it had run there,
// and the history marks it so that it runs on workers next time. If
// workers cannot run it after all, it runs here again, to the end.
func (s *Server) afterGuard(ctx context.Context, sess *session, st *statement, args []driver.NamedValue,
	w *tagWriter, parts *distParts, ran time.Duration) error {
	user := sess.currentUser()
	s.dist.ObserveGuard(parts.key, ran)
	s.log.Warn("a statement ran on this replica far past its estimate; running it on workers instead",
		"user", user.Name, "key", parts.key, "seconds", math.Round(ran.Seconds()), "estimate_seconds", math.Round(parts.local.Seconds()),
		"workers_seconds", math.Round(parts.alt.Seconds()))
	since := time.Now()
	// As an admin's forced statement runs, starting stopped workers: auto
	// mode chose this replica for it, and nothing else would run it.
	p, done, err := s.runDistributed(ctx, sess, user, st, args, w, distributed.ModeOn)
	if !done {
		if p.release != nil {
			defer p.release()
		}
		s.log.Warn("workers could not run a statement the guard stopped here; running it here again", "user", user.Name, "key", parts.key)
		return s.runLocal(ctx, sess, st, args, w)
	}
	p.since = since
	p.placement = s.dist.GuardPlacement(ran, parts.local, p.placement)
	*parts = p
	return err
}

// ranOutOfMemory says whether err, from running a statement on this
// replica under ctx (see runCancelable), is its running out of memory:
// DuckDB's own error, or the memory watch canceling it (ShedMemory).
func (sess *session) ranOutOfMemory(ctx context.Context, err error) bool {
	var de *duckdb.Error
	if errors.As(err, &de) && de.Type == duckdb.ErrorTypeOutOfMemory {
		return true
	}
	if err == nil || !errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.shed
}

// afterOutOfMemory runs st on workers after it ran out of memory on this
// replica, where auto mode had chosen to run it (parts.key), before it
// sent the client a row: the client gets the rows as if it had run there,
// and the history marks it so that it runs on workers next time. It
// returns cause, the error here, if workers cannot run it. outer is the
// context runCancelable ran st under, which a shed does not end; ctx the
// one it gave st.
func (s *Server) afterOutOfMemory(outer, ctx context.Context, sess *session, st *statement, args []driver.NamedValue,
	w *tagWriter, parts *distParts, cause error) error {
	user := sess.currentUser()
	s.dist.ObserveOutOfMemory(parts.key)
	retry, release := sess.rearm(outer, ctx)
	defer release()
	s.log.Warn("a statement ran out of memory on this replica; running it on workers", "user", user.Name, "key", parts.key, "err", cause)
	since := time.Now()
	// As an admin's forced statement runs: auto mode chose this replica
	// for it, and nothing else would run it.
	p, done, err := s.runDistributed(retry, sess, user, st, args, w, distributed.ModeOn)
	if !done {
		if p.release != nil {
			p.release()
		}
		return cause
	}
	if errors.Is(retry.Err(), context.DeadlineExceeded) {
		return errStatementTimeout
	}
	p.since = since
	p.placement = "moved after it ran out of memory on this replica; " + p.placement
	*parts = p
	return err
}

// policyReason says why masking and row access policies keep st on this
// replica, or is empty. Workers read table files directly and know nothing
// of the requesting user, so they cannot apply policies: a statement that
// reads a protected table, directly or through a view, is not distributed.
//
// Which tables a statement reads is what the rewriter found resolving its
// names (touched, applied), not what names it spells: a statement is
// analyzed when any word of it, or of a view it reads, spells a protected
// table's name, as a column named like one does, or one named like an ID
// domain, whose dictionary, Igloo's own and protected, is named after it.
// Analyzed and found to read none, it runs on workers as the text the
// rewriter left, views it expanded included. sessionEnv.Table refuses
// protected tables as well, should a statement reach the planner without
// the rewriter having seen one.
func policyReason(st *statement) string {
	g := st.guard
	switch {
	case g == nil && st.original != "":
		return "masking or row access policies rewrote it"
	case g == nil, g.hiddenOnly:
		// Only ID domains' columns of ids left out: nothing a worker could
		// fail to apply.
		return ""
	case len(g.applied) > 0:
		return "masking or row access policies apply to what it reads"
	case g.touched:
		return "masking or row access policies, or a clean room, concern a table it reads"
	}
	return ""
}

// runFinal runs the part of a distributed statement that combines its
// parts' results, in st's place: it must have st's columns and parameters.
// It reads merged, the workers' sorted rows merged as it reads them (see
// distributed.MergedFunction).
func (st *statement) runFinal(ctx context.Context, conn *duckdb.Conn, query string, merged []*distributed.MergedView, args []driver.NamedValue, w wire.DataWriter) error {
	ctx = distributed.WithMerged(ctx, merged)
	fin, err := prepare(ctx, conn, query, st.clientOIDs)
	if err != nil {
		return err
	}
	defer fin.close() //nolint:errcheck
	if fin.kind != returnsRows || !slices.Equal(fin.params, st.params) ||
		!slices.EqualFunc(fin.columns, st.columns, func(a, b wire.Column) bool { return a.Name == b.Name && a.Oid == b.Oid }) {
		return errors.New("the combined result does not match the statement's columns")
	}
	fin.stmtType, fin.query, fin.names, fin.readsOnce, fin.streams = st.stmtType, st.query, st.names, len(merged) > 0, st.streams
	return fin.run(ctx, args, w)
}

// registerMerged registers distributed.MergedFunction on the engine's
// database, and reports whether it did. Without it, statements read the
// merge of workers' sorted rows from a file once it is made.
func (s *Server) registerMerged() bool {
	r, ok := s.engine.(interface {
		RegisterFunctions(ctx context.Context, register func(*sql.Conn) error) error
	})
	if !ok {
		return false
	}
	if err := r.RegisterFunctions(context.Background(), distributed.RegisterMerged); err != nil {
		s.log.Warn("could not register the function that reads merges of workers' sorted rows; statements read them once merged", "err", err)
		return false
	}
	return true
}

// distributedHarmless are settings a session may change that do not change
// what a statement computes.
var distributedHarmless = []string{
	"search_path", "schema", "explain_output", "errors_as_json", "enable_progress_bar",
	"enable_progress_bar_print", "progress_bar_time", "enable_profiling", "profiling_mode",
	"profiling_output", "profile_output", "custom_profiling_settings", "profiling_coverage",
}

// distributedSettings returns the session's values of the settings workers
// must share, and fails if the session changed a setting that may change
// what statements compute but that workers do not take over.
func (s *Server) distributedSettings(ctx context.Context, sess *session) ([]distributed.Setting, error) {
	mine, err := settingValues(ctx, sess.conn)
	if err != nil {
		return nil, err
	}
	fresh, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	base, err := settingValues(ctx, fresh)
	fresh.Close() //nolint:errcheck
	if err != nil {
		return nil, err
	}
	var out []distributed.Setting
	for name, value := range mine {
		known := slices.ContainsFunc(distributed.SettingNames, func(n string) bool { return strings.EqualFold(n, name) })
		switch {
		case known:
			out = append(out, distributed.Setting{Name: name, Value: value})
		case slices.ContainsFunc(distributed.ChangedSettingNames, func(n string) bool { return strings.EqualFold(n, name) }):
			// The rb extension's, sent only where the session changed
			// them, so that a worker needs the extension only for the
			// statements that count in bitmaps.
			if base[name] != value {
				out = append(out, distributed.Setting{Name: name, Value: value})
			}
		case base[name] != value && !slices.Contains(distributedHarmless, strings.ToLower(name)):
			return nil, fmt.Errorf("the session changed %s", name)
		}
	}
	slices.SortFunc(out, func(a, b distributed.Setting) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func settingValues(ctx context.Context, conn *duckdb.Conn) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT name, value::VARCHAR FROM duckdb_settings()", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]string{}
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		value, _ := dest[1].(string)
		out[name] = value
	}
}

// sessionEnv is the session a statement runs in, for the planner.
type sessionEnv struct {
	s    *Server
	sess *session
	user auth.User
	st   *statement

	resolver *resolver
	userFns  map[string]bool
}

// Table resolves a name to a catalog table as DuckDB would in this
// session. Only a name that can mean exactly one catalog table qualifies.
// For users who read only what grants allow, the table must be one the
// statement was checked to read.
func (e *sessionEnv) Table(ctx context.Context, parts []string) (*distributed.Table, bool, error) {
	o, ok, err := e.catalogTable(ctx, parts)
	if err != nil || !ok {
		return nil, false, err
	}
	catalogName := e.s.catalogName()
	obj := auth.Object{Schema: o.schema, Table: o.name}
	if rank[e.user.Role] < rank[auth.RoleReader] {
		if e.st.access == nil || !slices.ContainsFunc(e.st.access.read, obj.Equal) {
			return nil, false, nil
		}
	}
	if e.s.Protected(ctx, o.schema, o.name) {
		return &distributed.Table{Schema: o.schema, Name: o.name, Unsupported: "masking or row access policies, or a clean room, apply to it"}, true, nil
	}
	t, err := e.s.dist.Table(ctx, o.schema, o.name)
	if err != nil {
		return nil, false, err
	}
	// Workers must read the columns as DuckDB does here. The statement was
	// bound, so DuckDB has loaded them. Listing them with duckdb_columns()
	// walks the catalog's in-memory set; hold the exclusive side for that one
	// scan so it does not run while a table is renamed or dropped on this
	// instance (the read's run holds nothing; see catalogRunLock, holdCatalog).
	release := e.s.holdCatalog(e.sess)
	cols, err := tableColumns(ctx, e.sess, catalogName, o.schema, o.name)
	release()
	if err != nil {
		return nil, false, err
	}
	if len(cols) != len(t.Columns) && t.Unsupported == "" {
		t.Unsupported = "its columns differ between DuckDB and the catalog"
	}
	for i, c := range cols {
		if i >= len(t.Columns) {
			break
		}
		switch {
		case t.Unsupported != "":
		case !strings.EqualFold(t.Columns[i].Name, c.Name):
			t.Unsupported = "its columns differ between DuckDB and the catalog"
		case t.Columns[i].Type != c.Type:
			t.Unsupported = fmt.Sprintf("DuckDB reads column %s as %s, not %s", c.Name, c.Type, t.Columns[i].Type)
		}
	}
	return t, true, nil
}

// catalogTable resolves a name to a table of Igloo's catalog as DuckDB
// would in this session; ok is false unless it means exactly one such
// table.
func (e *sessionEnv) catalogTable(ctx context.Context, parts []string) (o object, ok bool, err error) {
	catalogName := e.s.catalogName()
	if catalogName == "" {
		return o, false, nil
	}
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return o, false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return o, false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return o, false, nil
	}
	o = res.first[0]
	if o.view || !strings.EqualFold(o.db, catalogName) {
		return o, false, nil
	}
	return o, true, nil
}

// Computes vouches for a statement's reads of bitmap indexes' tables when
// Igloo wrote them, to answer from the indexes (see bitmaprewrite.go): the
// references its rewrite wrote, in full, to the indexes the statement's
// pins name (see ownIndexRead), which no user's text may name
// (indexReferences) nor reach through the search path (iglooOnPath).
// Igloo's own text reads them at the snapshots it pinned, and a row access
// policy of Igloo's own keeps everyone else, workers among them, from
// reading them. The coordinator computes such a read for workers.
func (e *sessionEnv) Computes(ctx context.Context, parts []string) (bool, error) {
	if !e.st.readsIndexes() || !indexReference(parts, e.s.catalogName(), indexNames(e.st.indexPins)) {
		return false, nil
	}
	o, ok, err := e.catalogTable(ctx, parts)
	return ok && strings.EqualFold(o.schema, iddomain.IndexSchema), err
}

// Value runs query, a SELECT of one value, on the session's connection, as
// the statement runs there: a read of Igloo's own that Computes vouched
// for, which the policy rewrite leaves alone in the statement too.
func (e *sessionEnv) Value(ctx context.Context, query string) (any, error) {
	rows, err := e.sess.conn.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return nil, err
	}
	return dest[0], nil
}

// View reports whether a name may mean a view in this session: a view of
// any database, or a name that does not resolve to exactly one relation.
func (e *sessionEnv) View(ctx context.Context, parts []string) (bool, error) {
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return true, nil
	}
	return res.first[0].view, nil
}

// ViewDefinition returns the query of the view a name means in this
// session, if it means exactly one: a view of the catalog's (a local copy
// of one), or a temporary view of the session's. Names in the query are
// resolved as DuckDB resolves them in the view's body. The grant check
// expanded the view the same way for users who read only what grants
// allow, so the tables it reads are among those it allowed (see Table).
func (e *sessionEnv) ViewDefinition(ctx context.Context, parts []string) (*distributed.ViewDefinition, bool, error) {
	if e.resolver == nil {
		r, err := e.s.newResolver(ctx, e.sess)
		if err != nil {
			return nil, false, err
		}
		e.resolver = r
	}
	res, err := e.resolver.resolve(tableRef{parts: parts}, nil)
	if err != nil {
		return nil, false, err
	}
	if len(res.all) != 1 || len(res.first) != 1 {
		return nil, false, nil
	}
	o := res.first[0]
	if !o.view || o.internal || o.external || !e.resolver.ownDatabase(o.db) {
		return nil, false, nil
	}
	body := viewBody(o.sql)
	if body == "" {
		return nil, false, nil
	}
	return &distributed.ViewDefinition{
		Query: body,
		Resolve: func(_ context.Context, parts []string) ([]string, bool, error) {
			res, err := e.resolver.resolve(tableRef{parts: parts}, &o)
			if err != nil || len(res.all) != 1 || len(res.first) != 1 {
				return nil, false, err
			}
			t := res.first[0]
			return []string{t.db, t.schema, t.name}, true, nil
		},
	}, true, nil
}

// tableColumns lists a table's columns as DuckDB has them. Names match as
// DuckDB matches them, whatever their case: a statement in Snowflake's
// dialect names lower-case tables in upper case.
func tableColumns(ctx context.Context, sess *session, db, schema, table string) ([]distributed.Column, error) {
	rows, err := sess.conn.QueryContext(ctx, `SELECT column_name, data_type FROM duckdb_columns()
		WHERE lower(database_name) = lower($1) AND lower(schema_name) = lower($2) AND lower(table_name) = lower($3)
		ORDER BY column_index`,
		[]driver.NamedValue{{Ordinal: 1, Value: db}, {Ordinal: 2, Value: schema}, {Ordinal: 3, Value: table}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []distributed.Column
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, distributed.Column{Name: name, Type: typ})
	}
}

// Setting returns the session's value of a DuckDB setting.
// Fill says the statement planned is the query a statement fills a table
// from, which this replica writes from its rows (distributed.Fills).
func (e *sessionEnv) Fill() bool { return e.st.fill }

// FillWrites says workers could write the table the statement fills.
func (e *sessionEnv) FillWrites() bool { return e.st.writer != nil }

// FillPartitions counts the partitions of the table the statement fills among
// the first rows of query, as a task's COPY computes them (distributed.PartitionedFills).
func (e *sessionEnv) FillPartitions(ctx context.Context, query string, rows int64) (half, all, sampled int64, err error) {
	w := e.st.writer
	if w == nil || len(w.partitions) == 0 {
		return 0, 0, 0, nil
	}
	first := fmt.Sprintf("SELECT * FROM (%s) LIMIT %d", query, rows)
	source := strings.Replace(w.sel, distributed.WriteSource, first, 1)
	res, err := e.sess.conn.QueryContext(ctx, fmt.Sprintf("SELECT count(DISTINCT p) FILTER (WHERE n <= %d), count(DISTINCT p), count(*) FROM "+
		"(SELECT row_number() OVER () AS n, hash(%s) AS p FROM (%s))", rows/2, strings.Join(w.partitions, ", "), source), nil)
	if err != nil {
		return 0, 0, 0, err
	}
	defer res.Close() //nolint:errcheck
	dest := make([]driver.Value, 3)
	if err := res.Next(dest); err != nil {
		return 0, 0, 0, err
	}
	half, _ = dest[0].(int64)
	all, _ = dest[1].(int64)
	sampled, _ = dest[2].(int64)
	return half, all, sampled, nil
}

// Others counts the statements running on this replica beside the
// session's, for auto mode's choice (distributed.Loads).
func (e *sessionEnv) Others() int { return e.s.othersRunning(e.sess) }

// Footprint is what the latest statement of the shape of the one planned
// held on this replica, in a run it completed (see
// distributed.Footprints); ok is false when none has, or the latest run
// learned from ended before its end.
func (e *sessionEnv) Footprint() (allocated int64, ok bool) {
	// The session's path is read already, for the statement's shape.
	return e.s.workload.Completed(e.s.workloadShape(context.Background(), e.sess, e.st.prepared))
}

// LocalRun is how long statements of the shape of the one planned took on
// this replica, in runs they completed (see distributed.LocalRuns).
func (e *sessionEnv) LocalRun() (time.Duration, bool) {
	return e.s.workload.Took(e.s.workloadShape(context.Background(), e.sess, e.st.prepared))
}

func (e *sessionEnv) Setting(ctx context.Context, name string) (string, error) {
	return settingValue(ctx, e.sess.conn, name)
}

// Explain returns DuckDB's plan of query as the session would run it,
// EXPLAIN (FORMAT JSON), its parameters bound to NULL, for auto mode's
// estimate of what it holds (see distributed.Explainer).
func (e *sessionEnv) Explain(ctx context.Context, query string) (string, error) {
	ds, err := e.sess.conn.PrepareContext(ctx, "EXPLAIN (FORMAT JSON) "+query)
	if err != nil {
		return "", err
	}
	stmt := ds.(*duckdb.Stmt)
	defer stmt.Close() //nolint:errcheck
	args := make([]driver.NamedValue, stmt.NumInput())
	for i := range args {
		args[i] = driver.NamedValue{Ordinal: i + 1}
	}
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(dest); err != nil || len(dest) < 2 {
		return "", errors.Join(errors.New("EXPLAIN returned no plan"), err)
	}
	plan, _ := dest[1].(string)
	return plan, nil
}

// Aggregates returns the names of the session's aggregate functions, those
// of the extensions the server loaded included.
func (e *sessionEnv) Aggregates(ctx context.Context) ([]string, error) {
	rows, err := e.sess.conn.QueryContext(ctx, distributed.AggregatesQuery, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var names []string
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return names, nil
			}
			return nil, err
		}
		if name, ok := dest[0].(string); ok {
			names = append(names, name)
		}
	}
}

// Describe binds query on the session's connection.
// Parameters are bound to NULL, as for the grant check's plans: the result
// columns must match the statement's, which runFinal checks.
func (e *sessionEnv) Describe(ctx context.Context, query string) ([]distributed.Column, error) {
	ds, err := e.sess.conn.PrepareContext(ctx, "DESCRIBE "+query)
	if err != nil {
		return nil, err
	}
	stmt := ds.(*duckdb.Stmt)
	defer stmt.Close() //nolint:errcheck
	args := make([]driver.NamedValue, stmt.NumInput())
	for i := range args {
		args[i] = driver.NamedValue{Ordinal: i + 1}
	}
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []distributed.Column
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, distributed.Column{Name: name, Type: typ})
	}
}

// UserFunction reports whether name is a function or macro someone
// defined, rather than one built into DuckDB or its extensions. Igloo's
// own macros for Snowflake's functions (snowflake.Macros), which workers
// define too, count as built in while the session's are exactly those:
// redefining any of them makes them all the session's own. So does the
// function that scores models (models.PredictFunction), which workers
// define too, reading models' blobs from the warehouse.
// GroupFunction reports whether name is a function that forecasts series
// (tsforecast.Functions), which workers run over whole series, partition
// by partition (see distributed.GroupFunctions).
func (e *sessionEnv) GroupFunction(name string) bool {
	return slices.ContainsFunc(tsforecast.Functions, func(f string) bool { return strings.EqualFold(name, f) })
}

func (e *sessionEnv) UserFunction(ctx context.Context, name string) (bool, error) {
	if strings.EqualFold(name, models.PredictFunction) || slices.ContainsFunc(tsforecast.Functions, func(f string) bool { return strings.EqualFold(name, f) }) {
		return false, nil
	}
	if e.userFns == nil {
		defs, err := macroDefinitions(ctx, e.sess.conn, `NOT internal`)
		if err != nil {
			return false, err
		}
		standard, err := e.s.snowflakeMacros(ctx)
		if err != nil {
			return false, err
		}
		var sf []string
		e.userFns = map[string]bool{}
		for _, d := range defs {
			e.userFns[d.name] = true
			if strings.HasPrefix(d.name, "sf_") {
				sf = append(sf, d.key)
			}
		}
		slices.Sort(sf)
		if slices.Equal(sf, standard) {
			for n := range e.userFns {
				if strings.HasPrefix(n, "sf_") {
					delete(e.userFns, n)
				}
			}
		}
	}
	return e.userFns[strings.ToLower(name)], nil
}

// macroDefinition is a function or macro overload as duckdb_functions()
// lists it.
type macroDefinition struct {
	// name is lower case; key is everything that defines the overload.
	name, key string
}

// macroDefinitions lists the functions and macros duckdb_functions() has
// where cond holds.
func macroDefinitions(ctx context.Context, conn *duckdb.Conn, cond string) ([]macroDefinition, error) {
	rows, err := conn.QueryContext(ctx, `SELECT lower(function_name), function_type || chr(31) || database_name || chr(31) ||
		coalesce(schema_name, '') || chr(31) || function_name || chr(31) || coalesce(parameters::VARCHAR, '') || chr(31) ||
		coalesce(macro_definition, '') FROM duckdb_functions() WHERE `+cond, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []macroDefinition
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		key, _ := dest[1].(string)
		out = append(out, macroDefinition{name: name, key: key})
	}
}

var (
	sfMacrosMu sync.Mutex
	// sfMacros are the keys of Igloo's macros for Snowflake's functions as
	// a session defines them, sorted.
	sfMacros []string
)

// snowflakeMacros returns the keys (see macroDefinitions) of the macros
// snowflake.MacrosFor defines, as a fresh connection lists them.
func (s *Server) snowflakeMacros(ctx context.Context) ([]string, error) {
	sfMacrosMu.Lock()
	defer sfMacrosMu.Unlock()
	if sfMacros != nil {
		return sfMacros, nil
	}
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	for _, stmt := range snowflake.MacrosFor(s.engine.BitmapsLoaded()) {
		if _, err := conn.ExecContext(ctx, stmt, nil); err != nil {
			return nil, err
		}
	}
	defs, err := macroDefinitions(ctx, conn, `NOT internal AND lower(function_name) LIKE 'sf\_%' ESCAPE '\'`)
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for _, d := range defs {
		keys = append(keys, d.key)
	}
	slices.Sort(keys)
	sfMacros = keys
	return keys, nil
}
