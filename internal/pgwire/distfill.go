package pgwire

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/distributed"
)

// Fills on workers. A statement that fills a table from a query, CREATE
// [OR REPLACE] TABLE ... AS (dbt's table models' too), INSERT INTO ...
// SELECT and a materialized view's or dynamic table's full refresh, runs
// its query where auto mode would run the query as a read: on workers by
// the same estimates, history, grants and policies, the coordinator then
// writing the table from their results, INSERT INTO the table the SQL that
// combines them, as a read sends the client what it combines; or where its
// tasks hold its rows, as a copy's do, they write the table's files, which
// this replica commits (see tableWrite). A count(DISTINCT) of a billion IDs
// into a table, whose thousand rows the replica writes in a moment, held the
// replica's memory at its limit while it ran here and was canceled; on
// workers, the replica holds only its thousand rows.
//
// The fill runs on workers only as a read of its query by its user would:
// not where masking or row access policies apply to, or concern, what the
// query reads (see policyReason), nor over tables the user's grants were
// not checked for. Erasure is as before: workers read table files as for
// any read, and their read caches are purged for erasure requests as for
// reads; the table written is a table like any other.

// fillSource prepares query, the query st fills a table from, as a read of
// it for the planner, with st's grants and policies, which were checked
// for st as a whole. It is nil where no fill runs on workers: without
// them, in a transaction block, in mode off, or where query does not
// prepare as a query of rows without parameters.
func (s *Server) fillSource(ctx context.Context, sess *session, st *statement, query string) *statement {
	if s.dist == nil || st == nil || sess.inTx || sessionMode(sess, sess.currentUser()) == distributed.ModeOff {
		if s.dist != nil && sess.inTx {
			s.noWorkerWrite("a fill", "it is in a transaction block")
		}
		return nil
	}
	var src *statement
	err := s.runBound(sess, nil, func() error {
		var err error
		src, err = prepare(ctx, sess.conn, query, nil)
		return err
	})
	if err != nil {
		return nil
	}
	if src.stmtType != duckdb.STATEMENT_TYPE_SELECT || src.kind != returnsRows || len(src.params) > 0 {
		_ = src.close()
		return nil
	}
	src.access, src.guard, src.original, src.fill = st.access, st.guard, st.original, true
	return src
}

// fillOn runs a fill of a table from src's query (see fillSource): on
// workers where auto mode, or the session's mode, runs the query as a
// read, writing the table from their results with from; otherwise with
// here, under the guard where auto mode chose this replica though workers
// could run the query, and on workers after the guard fires or after it
// runs out of memory here, as a read does (see afterGuard,
// afterOutOfMemory). f gives the context: after the memory watch's stop it
// goes on under what is left of the statement's time (see
// stagedFill.again). Both write functions run under the context they are
// given and return the rows written; neither may leave rows when it fails.
// again says the fill ran out of memory here, and workers could not run
// it: it may go on in passes under f.ctx. parts is where it ran, for the
// histories.
func (s *Server) fillOn(f *stagedFill, src *statement, here func(context.Context) (int64, error),
	from func(ctx context.Context, source string) (int64, error)) (rows int64, parts distParts, again bool, err error) {
	if src == nil {
		rows, err = here(f.ctx)
		return rows, parts, err != nil && f.again(err), err
	}
	defer src.close() //nolint:errcheck
	if fail := s.failFill; fail != nil {
		run := here
		here = func(ctx context.Context) (int64, error) {
			if err := fail(ctx, src.query); err != nil {
				return 0, err
			}
			return run(ctx)
		}
	}
	sess, user := f.sess, f.sess.currentUser()
	n, p, done, err := s.fillOnWorkers(f.ctx, sess, src, sessionMode(sess, user), from)
	if done {
		return n, p, false, err
	}
	if p.release != nil {
		// The workers stay up while it runs here, for the guard.
		defer p.release()
	}
	ctx := f.ctx
	var guard *localGuard
	tw := &tagWriter{}
	if p.local > 0 {
		guard = guardLocalBy(ctx, func() time.Duration { return guardAfter(p.local, p.alt, p.budget, s.othersRunning(sess)) }, tw)
		ctx = guard.ctx
	}
	rows, err = here(ctx)
	if guard != nil {
		if fired, ran := guard.stop(tw); fired && f.ctx.Err() == nil {
			s.dist.ObserveGuard(p.key, ran)
			s.log.Warn("a fill ran on this replica far past its estimate; running its query on workers instead",
				"user", user.Name, "key", p.key, "seconds", math.Round(ran.Seconds()), "estimate_seconds", math.Round(p.local.Seconds()),
				"workers_seconds", math.Round(p.alt.Seconds()))
			since := time.Now()
			n, q, done, werr := s.fillOnWorkers(f.ctx, sess, src, distributed.ModeOn, from)
			if done {
				q.since = since
				q.placement = fmt.Sprintf("moved by the guard after %.0f s on this replica, estimated at %.0f s; %s", ran.Seconds(),
					p.local.Seconds(), q.placement)
				return n, q, false, werr
			}
			if q.release != nil {
				defer q.release()
			}
			s.log.Warn("workers could not run a fill the guard stopped here; running it here again", "user", user.Name, "key", p.key)
			rows, err = here(f.ctx)
			return rows, p, err != nil && f.again(err), err
		}
	}
	if err == nil || !f.again(err) {
		return rows, p, false, err
	}
	if p.key == "" {
		return rows, p, true, err
	}
	// It ran out of memory here, where auto mode chose to run it: on
	// workers, under what is left of the statement's time.
	s.dist.ObserveOutOfMemory(p.key)
	s.log.Warn("a fill ran out of memory on this replica; running its query on workers", "user", user.Name, "key", p.key, "err", err)
	since := time.Now()
	n, q, done, werr := s.fillOnWorkers(f.ctx, sess, src, distributed.ModeOn, from)
	if done {
		q.since = since
		q.placement = "moved after it ran out of memory on this replica; " + q.placement
		return n, q, false, werr
	}
	if q.release != nil {
		q.release()
	}
	return rows, p, true, err
}

// fillOnWorkers runs src's query on workers if it qualifies in mode, and
// writes the table from their results with from (see fillOn). done false
// says it runs on this replica, and parts why.
func (s *Server) fillOnWorkers(ctx context.Context, sess *session, src *statement, mode distributed.Mode,
	from func(ctx context.Context, source string) (int64, error)) (rows int64, parts distParts, done bool, err error) {
	user := sess.currentUser()
	writer := src.writer
	if writer != nil {
		// What its tasks wrote, and no commit holds, goes.
		defer writer.end(ctx)
	}
	r, parts, done, err := s.startDistributed(ctx, sess, user, src, mode)
	if r == nil {
		return 0, parts, done, err
	}
	defer r.end()
	if r.ex.Wrote() {
		// An erasure request deleted files since the statement began: what
		// its tasks read of them is not to be committed. Here, it reads the
		// tables as they are now.
		if r.ex.ErasedSince() {
			s.log.Info("an erasure request deleted files during a fill; its query runs again here", "user", user.Name)
			return 0, distParts{release: s.dist.Keep()}, false, nil
		}
		rows, err = writer.commit(ctx, r.ex.Written())
		if err != nil && writer.left {
			// Whether a commit holds its rows cannot be told: written again
			// here, they could be there twice.
			return 0, r.parts, true, err
		}
	} else {
		rows, err = s.fillFrom(ctx, sess, src, r, from)
	}
	if err == nil {
		return rows, r.parts, true, nil
	}
	if ctx.Err() != nil || distributed.DataError(err) || bitmapsDidNotFit(err) {
		return 0, r.parts, true, err
	}
	s.dist.Metrics().Fallback("combine_error")
	s.log.Warn("could not fill a table from a distributed query's results; filling it here", "user", user.Name, "err", err)
	return 0, distParts{release: s.dist.Keep()}, false, nil
}

// errFillColumns says the SQL combining workers' results does not have
// the columns of the query a table is filled from.
var errFillColumns = errors.New("the combined result does not match the query's columns")

// fillFrom writes a table from r, the results of src's query's parts on
// workers, with from, given the SQL that combines them, which must have
// the query's columns: their names and types, in order.
func (s *Server) fillFrom(ctx context.Context, sess *session, src *statement, r *distRun,
	from func(ctx context.Context, source string) (int64, error)) (int64, error) {
	ctx = distributed.WithMerged(ctx, r.ex.Merged())
	var fin *statement
	err := s.runBound(sess, nil, func() error {
		var err error
		fin, err = prepare(ctx, sess.conn, r.ex.SQL, nil)
		return err
	})
	if err != nil {
		return 0, err
	}
	same := fin.kind == returnsRows && slices.EqualFunc(fin.columns, src.columns, func(a, b wire.Column) bool {
		return a.Name == b.Name && a.Oid == b.Oid
	})
	_ = fin.close()
	if !same {
		return 0, errFillColumns
	}
	var rows int64
	err = s.runBound(sess, nil, func() error {
		var err error
		rows, err = from(ctx, r.ex.SQL)
		return err
	})
	return rows, err
}

// insertParsed reads an INSERT of a query's rows, with a list of columns or
// without.
func insertParsed(text string) (parsedInsert, bool) {
	if p, ok := parseInsert(text); ok {
		return p, true
	}
	sc := &scanner{s: text}
	if !sc.keywords("INSERT", "INTO") {
		return parsedInsert{}, false
	}
	sc.skip()
	start := sc.i
	if _, named := sc.name(); !named {
		return parsedInsert{}, false
	}
	p := parsedInsert{target: text[start:sc.i]}
	sc.skip()
	p.source = strings.TrimRight(sc.rest(), "; \t\r\n")
	for _, t := range topWords(p.source) {
		if t.word == "RETURNING" || t.word == "CONFLICT" {
			return parsedInsert{}, false
		}
	}
	return p, true
}

// insertFill is the query an INSERT writes rows from, source, and
// head, st's text before it, which writes the target from another query
// of its columns: for an INSERT INTO a table of no member columns (see
// idassign.go) of a query's rows, VALUES not included, outside a
// transaction block.
func insertFill(sess *session, st *statement, args int) (head, source string, p parsedInsert, ok bool) {
	if st.stmtType != duckdb.STATEMENT_TYPE_INSERT || st.ids != nil || sess.inTx || args > 0 || st.kind != command {
		return "", "", p, false
	}
	p, ok = insertParsed(st.query)
	if !ok || p.source == "" {
		return "", "", p, false
	}
	switch (&scanner{s: p.source}).word() {
	case "SELECT", "WITH", "FROM":
	default:
		if p.source[0] != '(' {
			return "", "", p, false
		}
	}
	text := strings.TrimRight(st.query, "; \t\r\n")
	if !strings.HasSuffix(text, p.source) {
		return "", "", p, false
	}
	return text[:len(text)-len(p.source)], p.source, p, true
}

// runInsertFill runs an INSERT INTO ... SELECT of st as a fill (see
// fillOn): its query on workers where auto mode runs it there as a read.
// done is false when st is no such INSERT, or its query does not prepare
// as a read: it then runs as usual.
func (s *Server) runInsertFill(ctx context.Context, sess *session, st *statement, args int, w wire.DataWriter,
	run func(ctx context.Context) error) (done bool, err error) {
	head, source, p, ok := insertFill(sess, st, args)
	if !ok {
		if st.stmtType == duckdb.STATEMENT_TYPE_INSERT && (st.ids != nil || sess.inTx) {
			s.noWorkerWrite("an INSERT", "it fills an ID domain's ids, or is in a transaction block")
		}
		return false, nil
	}
	src := s.fillSource(ctx, sess, st, source)
	if src == nil {
		return false, nil
	}
	src.writer = s.insertWriter(ctx, sess, p, src)
	f := &stagedFill{s: s, sess: sess, ctx: ctx}
	defer f.close()
	ranHere := false
	here := func(ctx context.Context) (int64, error) {
		ranHere = true
		return -1, run(ctx)
	}
	from := func(ctx context.Context, source string) (int64, error) {
		var rows int64
		err := runWithCommitRetry(ctx, sess, st, func() error {
			res, err := sess.conn.QueryContext(ctx, head+source, nil)
			if err == nil {
				rows, err = countOf(res)
			}
			return err
		})
		return rows, err
	}
	rows, parts, _, err := s.fillOn(f, src, here, from)
	st.filled = &parts
	if errors.Is(f.ctx.Err(), context.DeadlineExceeded) && f.rearmed() && err != nil {
		err = errStatementTimeout
	}
	if err != nil || ranHere && rows < 0 {
		return true, err
	}
	return true, w.Complete(commandTag(st.stmtType, st.query, rows))
}
