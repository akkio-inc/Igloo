package pgwire

import (
	"context"
	"database/sql/driver"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/lockorder"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Heavy statements in the background. On a replica with no workers, one
// session's long reports took the cores from every dashboard beside them
// (see engine's background.go for what was measured). DuckDB cannot run one
// statement at a lower priority than another, but the operating system can
// run one DuckDB at a lower priority than another: reads that are heavy run
// in a second DuckDB of this process whose threads give way to the first's.
// Alone, they run as fast as they would have in the first.
//
// Whose heavy statements run there (see backgroundFor): a job's, as a task's,
// always, and a client's only with IGLOO_HEAVY_IN_BACKGROUND. A dashboard's own
// heavy statement is part of what its user waits for: at ten times its data on 16
// cores, the dashboard's heavy FLATTEN statement ran 22 s alone in the first
// DuckDB and 57 to 78 s in the second, beside its tiles, and the burst waited
// for it.
//
// A statement is heavy if statements of its shape, its text with the
// literals masked, lately took heavySeconds of all the machine's cores and
// heavyFactor times what the replica's others of some size do (see
// heavyHistory). One not yet seen runs in the first DuckDB; if it has run long
// enough to be heavy by what it can have used beside the others running, and has
// sent the client nothing, it starts again in the second, once, within
// heavyMoveWithin. So a report's first run costs it at most a few seconds
// more, and dashboards see its later ones only as the cores they leave idle.
//
// Only what would give the same rows there runs there: a read outside a
// transaction block, of what the catalog holds, with no temporary objects
// or settings of the session's that the other DuckDB has not got, and no
// function tied to the session. Masking and row access policies apply in
// the statement's text, which runs there as here; a clean room's tables
// stay here (see policyReason). Anything else that the
// second DuckDB cannot bind, as Igloo's functions over its own state, a
// connection's tables or a view not yet copied there, fails as it is
// prepared, and runs in the first.
//
// All this is for short statements beside heavy ones. Where most of what runs or
// waits to in the first DuckDB is of some size itself (see besideIsHeavy), there are none to
// give way to, and a heavy statement stays in the first: under a dashboard's burst
// at ten times its data, with every statement a minute or more, those sent to the
// second had a tenth of what the others left of the cores, and ran past 15 minutes
// until they timed out, the two DuckDBs each spilling what the other wanted, and
// those moved there threw away what they had done.
//
// A write of the catalog's tables known to be heavy (INSERT, UPDATE, DELETE,
// MERGE and the fill of CREATE TABLE ... AS) runs there too, under the same
// conditions and through its own proxy, with the same guard. It commits as a
// replica would, and the first DuckDB finds the new snapshot as it finds
// another replica's. A write is never started again, so one not yet known to
// be heavy runs to its end in the first.

// heavySeconds is how long of all the machine's cores makes a statement
// heavy, at the least. A dashboard's statement takes a fraction of that: on 12
// cores, one over a billion rows used 30 to 40 s of CPU, and a report over them
// 700 to 12,000.
const heavySeconds = 5

// heavyFactor is how many times what the replica's other statements of some
// size use a heavy one uses. Dashboards of a few seconds each over more rows
// are not heavy beside each other: in the second DuckDB they would only wait
// behind the reports.
const heavyFactor = 8

const (
	// heavyRuns is how many of a shape's latest runs count. What DuckDB
	// measures as CPU is the time threads spend in operators, preempted or
	// waiting for the disk or not: a dashboard's statement of 40 s measured 430 s
	// beside a report that took the read cache's memory. So it counts only ever
	// more than it used, and the least of the latest runs is close to it.
	heavyRuns = 3
	// heavyAtOnce is how many times over the line one run in the first DuckDB
	// makes a shape heavy with no other.
	heavyAtOnce = 5
	// heavyRecent is how lately a shape has run for what it uses to count for
	// what is heavy beside it: this afternoon's reports are not heavy by this
	// morning's dashboards.
	heavyRecent = 10 * time.Minute
	// heavyProbeEvery is how often a shape known to be heavy runs in the first
	// DuckDB again, where it soon moves if it is still heavy, as it has only the
	// second DuckDB's measures, which count its waiting for the cores.
	heavyProbeEvery = 8
)

// maxHeavyShapes bounds the shapes remembered; they are forgotten all at
// once, and learned again.
const maxHeavyShapes = 4096

// heavyMoveWithin is how long a statement that turned out heavy may have run
// and still start again in the background. Past it, starting again would
// cost it more than it is likely to cost the others.
var heavyMoveWithin = 60 * time.Second

// besideFor is how long another statement must have run for a statement in the
// first DuckDB to count as running beside it (see runPlaced).
const besideFor = time.Second

// heavyHistory is what statements of each shape took.
type heavyHistory struct {
	mu     lockorder.Mutex
	shapes map[string]*shapeRuns
	// cpu is how much CPU time at the least makes a statement heavy, factor
	// how many times the others' (see line), and after how long it starts again
	// in the background, per statement sharing the cores.
	cpu    time.Duration
	factor time.Duration
	after  time.Duration
	now    func() time.Time
}

// shapeRuns is a shape's latest runs.
type shapeRuns struct {
	cpu   [heavyRuns]time.Duration
	runs  int
	heavy bool
	// probe counts down to its next run in the first DuckDB.
	probe int
	// last is when it last ended.
	last time.Time
	// moved says its last run started again in the background: the next starts
	// there, as a run that gets far in the first takes the memory of its caches,
	// and one that fails there says nothing else.
	moved bool
}

// sorted returns the CPU of the latest runs, from the least.
func (r *shapeRuns) sorted() []time.Duration {
	out := slices.Clone(r.cpu[:min(r.runs, heavyRuns)])
	slices.Sort(out)
	return out
}

func newHeavyHistory() *heavyHistory {
	return &heavyHistory{shapes: map[string]*shapeRuns{}, cpu: heavySeconds * time.Second * time.Duration(runtime.GOMAXPROCS(0)),
		factor: heavyFactor, after: heavySeconds * time.Second, now: time.Now}
}

// line is how much CPU makes shape heavy: at least h.cpu, and heavyFactor times
// what the other statements of some size use, as many of them as use less
// as use more, once there are a few that ran lately. h.mu is held.
func (h *heavyHistory) line(shape string) time.Duration {
	var others []time.Duration
	lately := h.now().Add(-heavyRecent)
	for name, r := range h.shapes {
		if name == shape || r.runs == 0 || r.last.Before(lately) {
			continue
		}
		if low := r.sorted()[0]; low >= h.cpu/10 {
			others = append(others, low)
		}
	}
	if len(others) < 2 {
		return h.cpu
	}
	slices.Sort(others)
	return max(h.cpu, h.factor*others[(len(others)-1)/2])
}

// observe learns what a run of shape took. All of its latest runs are over the
// line for it to be heavy, and one run alone only far over it, in the first
// DuckDB, whose measure is less far from what it used.
func (h *heavyHistory) observe(shape string, cpu time.Duration, inBackground bool) {
	if shape == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.shapes) >= maxHeavyShapes {
		h.shapes = map[string]*shapeRuns{}
	}
	r := h.shapes[shape]
	if r == nil {
		r = &shapeRuns{}
		h.shapes[shape] = r
	}
	r.cpu[r.runs%heavyRuns] = cpu
	r.runs++
	r.last = h.now()
	line := h.line(shape)
	was := r.heavy
	switch {
	case r.sorted()[0] < line:
		r.heavy = false
	case r.runs >= 2:
		r.heavy = true
	default:
		r.heavy = !inBackground && cpu >= heavyAtOnce*line
	}
	if r.heavy && !was {
		r.probe = heavyProbeEvery
	}
}

// known says whether a statement of shape has run.
func (h *heavyHistory) known(shape string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.shapes[shape]
	return r != nil && r.runs > 0
}

// heavy says whether statements of shape are known to be heavy.
func (h *heavyHistory) heavy(shape string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.shapes[shape]
	return r != nil && r.heavy
}

// place says where a read of shape runs: in the background, or in the first
// DuckDB, where it may start again in the background if it runs long, unless
// its shape is known not to.
func (h *heavyHistory) place(shape string) (background, mayMove bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.shapes[shape]
	switch {
	case r == nil:
		return false, true
	case r.moved:
		r.moved = false
		return true, false
	case r.heavy:
		if r.probe--; r.probe > 0 {
			return true, false
		}
		r.probe = heavyProbeEvery
		return false, true
	case r.runs == 0:
		return false, true
	}
	runs := r.sorted()
	return false, runs[len(runs)/2] >= h.line(shape)
}

// moved says a run of shape started again in the background.
func (h *heavyHistory) moved(shape string) {
	if shape == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shapes[shape] == nil {
		h.shapes[shape] = &shapeRuns{}
	}
	h.shapes[shape].moved = true
}

// moveAfter is how long a statement not known to be heavy runs before it
// could have used that much, sharing the cores with others.
func (h *heavyHistory) moveAfter(others int) time.Duration {
	return h.after * time.Duration(1+min(others, 7))
}

// aStatement is a statement running in the first DuckDB, as besideIsHeavy sees it.
type aStatement struct {
	// expect is how long statements of its shape take alone (see
	// workload.Controller.Expect), zero for a shape not seen yet, and ran how long
	// it has run.
	expect, ran time.Duration
	// waiting says it waits in line, for what is held: what makes a heavy statement
	// give way, the cores, is not what it waits for.
	waiting bool
}

// mostOfSize says whether most of beside are of some size: waiting in line, or
// running for half as long as a statement not known to be heavy would before it could
// have used that much, and not known to take less alone than makes a statement heavy
// at the least. A statement known to be short counts as short however long it has
// run: a heavy statement beside dashboards slows them, and were that to make them of
// some size, it would stay beside them. Their p95 went from 9 s to 119 s. And what a
// statement is known to take alone counts only once it has run that long: their first
// runs on a replica, beside the heavy statement, measured many times what they take,
// which kept it in the first with them, where it ran out of memory, and them with it.
func (h *heavyHistory) mostOfSize(beside []aStatement) bool {
	long := h.moveAfter(len(beside)) / 2
	sized := 0
	for _, b := range beside {
		if b.waiting || b.ran >= long && (b.expect == 0 || b.expect >= h.after) {
			sized++
		}
	}
	return sized*2 > len(beside)
}

// besideIsHeavy says whether most of what runs in the first DuckDB beside sess, or
// waits in line to, is of some size itself, so that a heavy statement would give way
// to nothing (see the top of this file). Those waiting count: alone in the first with
// 16 in line, a heavy statement went to the second, where it had none of the first's
// caches and restarted, and took half as long again, as the others waited for it.
func (s *Server) besideIsHeavy(sess *session) bool {
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	for _, other := range r.byPID {
		if other != sess {
			all = append(all, other)
		}
	}
	r.mu.Unlock()
	now := time.Now()
	beside := make([]aStatement, 0, len(all))
	for _, other := range all {
		other.mu.Lock()
		running, waiting, shape, started := other.statement != "" && !other.inBackground && !other.elsewhere, other.queued, other.runShape, other.started
		other.mu.Unlock()
		if running {
			beside = append(beside, aStatement{expect: s.workload.Expect(shape), ran: now.Sub(started), waiting: waiting})
		}
	}
	return s.heavy.mostOfSize(beside)
}

// backgroundLane is the second DuckDB as this server uses it.
type backgroundLane struct {
	once lockorder.Mutex
	eng  *engine.Engine
	// views keeps the local copies of the catalog's views there.
	views *ddl.Manager
	stop  context.CancelFunc
	wg    sync.WaitGroup
}

// backgroundOpener is the engine that can open the second DuckDB.
type backgroundOpener interface {
	Background(ctx context.Context) (*engine.Engine, error)
	UsingBackground(ctx context.Context) func()
	ForegroundRead()
}

// backgroundEngine returns the second DuckDB, opening it and starting its
// copies of the catalog's views if this is its first use.
func (s *Server) backgroundEngine(ctx context.Context) (*engine.Engine, backgroundOpener, error) {
	opener, ok := s.engine.(backgroundOpener)
	if !ok {
		return nil, nil, engine.ErrNoBackground
	}
	l := &s.lane
	l.once.Lock()
	defer l.once.Unlock()
	if l.eng != nil {
		return l.eng, opener, nil
	}
	eng, err := opener.Background(ctx)
	if err != nil {
		return nil, nil, err
	}
	if eng.LocalDatabase() != s.engine.LocalDatabase() {
		// Statements may name the first's local database.
		return nil, nil, engine.ErrNoBackground
	}
	// Its writes reach the catalog through a proxy of its own, under the
	// same rules.
	eng.SetCatalogGuard(s)
	if s.ddl != nil {
		views, err := s.ddl.For(ctx, eng)
		if err != nil {
			return nil, nil, err
		}
		runCtx, stop := context.WithCancel(context.Background())
		l.views, l.stop = views, stop
		l.wg.Go(func() { views.Run(runCtx) })
	}
	l.eng = eng
	return eng, opener, nil
}

// closeBackground stops what keeps the second DuckDB's views up to date, so
// that the engine can close it.
func (s *Server) closeBackground() error {
	l := &s.lane
	l.once.Lock()
	defer l.once.Unlock()
	if l.stop != nil {
		l.stop()
		l.wg.Wait()
		l.stop = nil
	}
	if l.views != nil {
		err := l.views.Close()
		l.views = nil
		return err
	}
	return nil
}

// backgroundConn is a session's connection to the second DuckDB.
type backgroundConn struct {
	conn  *duckdb.Conn
	meter *engine.Meter
}

// closeBackgroundConn closes the session's connection to the second DuckDB,
// if it opened one.
func (sess *session) closeBackgroundConn() error {
	if bc := sess.bgConn.Swap(nil); bc != nil {
		return bc.meter.Close()
	}
	return nil
}

// interruptBackground interrupts what the session runs in the second DuckDB.
func (sess *session) interruptBackground() {
	if bc := sess.bgConn.Load(); bc != nil {
		bc.conn.Interrupt()
	}
}

// backgroundConnOf returns the session's connection to eng, with the
// session's settings and search path, opening it if need be, with what a
// statement may call in any session.
func (s *Server) backgroundConnOf(ctx context.Context, sess *session, eng *engine.Engine) (*backgroundConn, error) {
	bc := sess.bgConn.Load()
	if bc == nil {
		conn, meter, err := eng.ConnectMetered(ctx)
		if err != nil {
			return nil, err
		}
		conn.SetBindHold(eng.CatalogShare)
		// A C++ exception that escapes DuckDB in one of its calls ends the
		// session, as on the session's own connection (see openSession).
		conn.SetCallGuard(sess.callGuard)
		for _, stmt := range sessionCatalogSetup(sess.user.Name, false, s.version, eng.BitmapsLoaded()) {
			// The macros. Views of Igloo's own relations have no place here,
			// and one macro that cannot be defined fails only the statements
			// that call it, which then run in the first DuckDB.
			if strings.Contains(stmt[:min(len(stmt), 40)], " MACRO ") {
				_, _ = conn.ExecContext(ctx, stmt, nil)
			}
		}
		bc = &backgroundConn{conn: conn, meter: meter}
		sess.bgConn.Store(bc)
	}
	settings, err := s.distributedSettings(ctx, sess)
	if err != nil {
		return nil, err
	}
	for _, set := range settings {
		if _, err := bc.conn.ExecContext(ctx, "SET SESSION "+quoteIdent(set.Name)+" = "+quoteLiteral(set.Value), nil); err != nil {
			return nil, err
		}
	}
	path, err := settingValue(ctx, sess.conn, "search_path")
	if err != nil {
		return nil, err
	}
	if _, err := bc.conn.ExecContext(ctx, "SET SESSION search_path = "+quoteLiteral(path), nil); err != nil {
		return nil, err
	}
	return bc, nil
}

// mayRunInBackground says whether st could give the same rows in the second
// DuckDB, as far as a look at it can tell (see above); what it does not
// tell, preparing it there does.
func (s *Server) mayRunInBackground(sess *session, st *statement, tw *tagWriter, parts *distParts) bool {
	return s.readInBackground(sess, st) && parts.local == 0 && tw.Written() == 0
}

// backgroundFor says whether sess's heavy statements may run in the second
// DuckDB: a job's, and with IGLOO_HEAVY_IN_BACKGROUND a client's too (see the
// top of this file).
func (s *Server) backgroundFor(sess *session) bool {
	return s.heavy != nil && (sess.job != "" || s.heavyClients)
}

// readInBackground says whether st is a read that could run in the second
// DuckDB, whatever comes of its turn.
func (s *Server) readInBackground(sess *session, st *statement) bool {
	return s.backgroundFor(sess) && sess.wl.shape != "" && st.stmtType == duckdb.STATEMENT_TYPE_SELECT && st.kind == returnsRows &&
		!sess.inTx && len(st.loads) > 0 && !st.ai && len(st.indexPins) == 0 && st.idCounts == nil && policyReason(st) == ""
}

// runsApart says st is known to be heavy and will run in the second DuckDB (see
// workload.Limits.Apart).
func (s *Server) runsApart(sess *session, st *statement) bool {
	return s.readInBackground(sess, st) && s.heavy.heavy(sess.wl.shape) && !s.besideIsHeavy(sess)
}

// sameInBackground looks closer, as only a statement that will run there
// need: at the functions it calls and the session's temporary objects, which
// a name in it might mean.
func (s *Server) sameInBackground(ctx context.Context, sess *session, st *statement) bool {
	for _, w := range sqllex.CodeWords(st.prepared) {
		if distributed.SessionFunction(strings.ToLower(w)) {
			return false
		}
	}
	if sess.ddlCount == 0 {
		// Only DDL makes temporary objects (see names).
		return true
	}
	release := s.shareCatalog(sess)
	defer release()
	temp, err := queryStrings(ctx, sess, `SELECT system.main.lower(table_name) FROM system.main.duckdb_tables() WHERE temporary AND NOT internal
		UNION ALL SELECT system.main.lower(view_name) FROM system.main.duckdb_views() WHERE temporary AND NOT internal`)
	if err != nil {
		return false
	}
	for _, t := range temp {
		if !setupTempViews[t] {
			return false
		}
	}
	return true
}

// runPlaced runs st on this replica, in whichever of its DuckDBs it belongs.
func (s *Server) runPlaced(ctx, outer context.Context, sess *session, st *statement, args []driver.NamedValue, tw *tagWriter, parts *distParts) error {
	if !s.mayRunInBackground(sess, st, tw, parts) {
		s.foregroundRead(st)
		return s.runHere(ctx, sess, st, args, tw)
	}
	background, mayMove := s.heavy.place(sess.wl.shape)
	if background && s.besideIsHeavy(sess) {
		background, mayMove = false, true
	}
	if background {
		if ran, err := s.runInBackground(ctx, sess, st, args, tw, parts); ran {
			return err
		}
		s.foregroundRead(st)
		return s.runHere(ctx, sess, st, args, tw)
	}
	s.foregroundRead(st)
	if !mayMove {
		return s.runHere(ctx, sess, st, args, tw)
	}
	// It may turn out heavy, beside others: those that have run for a while,
	// and so hold cores or memory worth giving way to. A monitoring poll of a
	// few milliseconds, once a second, does not: beside one, TPC-DS q67 alone
	// started again in the background after 23.5 s, and threw that away.
	start := time.Now()
	guard := guardLocalBy(ctx, func() time.Duration {
		others := s.othersRunningFor(sess, besideFor)
		if others == 0 || time.Since(start) > heavyMoveWithin || s.besideIsHeavy(sess) {
			return time.Hour
		}
		return s.heavy.moveAfter(others)
	}, tw)
	err := s.runHere(guard.ctx, sess, st, args, tw)
	fired, ran := guard.stop(tw)
	if !fired || ctx.Err() != nil || outer.Err() != nil {
		return err
	}
	s.heavy.moved(sess.wl.shape)
	s.log.Debug("a statement turned out heavy beside others; starting it again in the background", "ran", ran.Round(time.Millisecond))
	if bgRan, err := s.runInBackground(ctx, sess, st, args, tw, parts); bgRan {
		return err
	}
	return s.runHere(ctx, sess, st, args, tw)
}

// foregroundRead tells the engine a statement that reads tables runs in the
// first DuckDB, whose caches it wants (see engine's background.go).
func (s *Server) foregroundRead(st *statement) {
	if s.heavy != nil && len(st.loads) > 0 {
		if o, ok := s.engine.(backgroundOpener); ok {
			o.ForegroundRead()
		}
	}
}

// writesInBackground says whether st, a write or the fill of one, runs in
// the second DuckDB: if statements of its shape are known to be heavy, and it
// would write and read the same there.
func (s *Server) writesInBackground(ctx context.Context, sess *session, st *statement) bool {
	return s.backgroundFor(sess) && sess.wl.shape != "" && s.heavy.heavy(sess.wl.shape) && !sess.inTx && st.ids == nil && !st.ai &&
		policyReason(st) == "" && s.sameInBackground(ctx, sess, st)
}

// runWriteInBackground runs st, if it is a write that belongs there, in the
// second DuckDB. ran false says it did not, and nothing was written.
func (s *Server) runWriteInBackground(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) (ran bool, err error) {
	switch st.stmtType {
	case duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
	default:
		return false, nil
	}
	if st.kind != command || !s.writesInBackground(ctx, sess, st) {
		return false, nil
	}
	fin, bc, end := s.prepareInBackground(ctx, sess, st.query, st.clientOIDs)
	if fin == nil {
		return false, nil
	}
	defer end()
	defer fin.close() //nolint:errcheck
	if !sameShape(fin, st, st.original == "") {
		return false, nil
	}
	fin.stmtType, fin.query = st.stmtType, st.query
	if err := fin.run(ctx, args, w); err != nil {
		return true, err
	}
	sess.wl.backgroundUsed = bc.meter.Last(fin.prepared)
	return true, nil
}

// fillInBackground runs q, an INSERT that fills a table for f's statement,
// in the second DuckDB. ran false says it did not, and nothing was written.
func (f *stagedFill) fillInBackground(ctx context.Context, q string) (n int64, ran bool, err error) {
	fin, bc, end := f.s.prepareInBackground(ctx, f.sess, q, nil)
	if fin == nil {
		return 0, false, nil
	}
	defer end()
	defer fin.close() //nolint:errcheck
	res, err := fin.stmt.QueryContext(ctx, nil)
	if err == nil {
		n, err = countOf(res)
	}
	if used := bc.meter.Last(fin.prepared); used != nil && err == nil {
		f.cpu += used.CPUTime
	}
	return n, true, err
}

// prepareInBackground prepares query in the second DuckDB, opening it and
// the session's connection to it if need be, and gives the statement its
// room; end says it has ended. The statement is nil if it cannot run there.
func (s *Server) prepareInBackground(ctx context.Context, sess *session, query string, oids []uint32) (fin *statement, bc *backgroundConn, end func()) {
	eng, opener, err := s.backgroundEngine(ctx)
	if err != nil {
		return nil, nil, nil
	}
	if bc, err = s.backgroundConnOf(ctx, sess, eng); err != nil {
		s.log.Debug("a heavy statement runs in the first DuckDB", "err", err)
		return nil, nil, nil
	}
	if fin, err = prepare(ctx, bc.conn, query, oids); err != nil {
		s.log.Debug("a heavy statement runs in the first DuckDB", "err", err)
		return nil, nil, nil
	}
	s.metrics.backgroundStatement()
	return fin, bc, opener.UsingBackground(ctx)
}

// setInBackground says whether the session's statement runs in the second DuckDB.
func (sess *session) setInBackground(on bool) {
	sess.mu.Lock()
	sess.inBackground = on
	sess.mu.Unlock()
}

// outOfSpillSpace says DuckDB could write no more to its spill files.
func outOfSpillSpace(err error) bool {
	return err != nil && strings.Contains(err.Error(), "failed to offload data block")
}

// runInBackground runs st in the second DuckDB. ran false says it did not,
// and nothing was sent: it runs in the first.
func (s *Server) runInBackground(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, tw *tagWriter, parts *distParts) (ran bool, err error) {
	if !s.sameInBackground(ctx, sess, st) {
		return false, nil
	}
	fin, bc, end := s.prepareInBackground(ctx, sess, st.query, st.clientOIDs)
	if fin == nil {
		return false, nil
	}
	defer end()
	defer fin.close() //nolint:errcheck
	if !sameShape(fin, st, st.original == "") {
		s.log.Debug("a heavy statement runs in the first DuckDB: its result differs in the second")
		return false, nil
	}
	fin.stmtType, fin.query = st.stmtType, st.query
	sess.setInBackground(true)
	defer sess.setInBackground(false)
	// On this thread, which works on the statement's tasks too, at the
	// default priority: one core's worth of it. The client's writer cannot
	// be called from another goroutine.
	err = fin.run(ctx, args, tw)
	if err == nil {
		sess.wl.backgroundUsed = bc.meter.Last(fin.prepared)
		parts.placement = "this replica, in the background: it is heavy, and gives way to the other statements here"
		return true, nil
	}
	// Whatever failed there, unless the statement was canceled, the first
	// DuckDB may do better, if the client has seen nothing yet. Not with its spill
	// files where the disk is full: run again in the first, a report that had filled
	// the disk from the second took the dashboards' p95 from 8 s to 108 s, and failed.
	if ctx.Err() != nil || tw.Written() > 0 || distributed.DataError(err) || outOfSpillSpace(err) {
		return true, err
	}
	s.log.Debug("a heavy statement failed in the second DuckDB; running it in the first", "err", err)
	return false, nil
}
