package pgwire

import (
	"container/list"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Result cache. A dashboard that several people open at once, or that asks
// the same question in several of its charts, sends the same query again
// and again over data that has not changed. Igloo keeps the result of such
// a query and sends it again, without running the query, to the next
// statement that is the same text, but for its comments and spacing, with
// the same parameters and settings, as long as none of the tables it reads
// has changed since (see catalogproxy's TablesVersion): writes to other
// tables, as a pipe's loads into raw tables, leave it be. A statement that
// no policy changes gives every user who may run it the same rows, and
// shares its result with them; one that a policy changes, or of a session
// with temporary objects of its own, keeps its result to its user (or
// session) and roles. A statement that arrives while the same one runs
// waits for that run's result rather than running too.
//
// Only results that running the statement again would give are kept: those
// of queries whose plan reads nothing but tables of the catalog and the
// data the statement itself holds, that call no function whose value can
// differ from one run or session to the next (now(), random(),
// current_setting(), getvariable(), and the macros that call one), outside
// transaction blocks. A session that may have temporary objects of its own
// shares no results with others: the same name may stand for something
// else in another session. Kept results take at most
// IGLOO_RESULT_CACHE_SIZE of memory, the least recently used going first,
// and none more than a sixteenth of it.
type resultCache struct {
	max, entryMax int64
	metrics       *Metrics

	mu      sync.Mutex
	used    int64
	lru     *list.List // of *cachedResult, the most recently used first
	entries map[resultKey]*list.Element
	flights map[resultKey]*resultFlight
	// verdicts say whether statements' results may be kept (see
	// keepable).
	verdicts map[resultKey]bool
	// memos are the statements answered before they are prepared, by
	// statementKey (see memoStatement).
	memos map[resultKey]memo
	// gen goes up when the cache is emptied (see purge): no run begun
	// before keeps its result.
	gen uint64
}

// memo is what answering a statement before it is prepared takes: the key
// of its result, the columns it describes, and the loads of the catalog's
// tables it reads, to make again so that a change made through another
// replica shows (see catalogproxy's Reload).
type memo struct {
	key     resultKey
	columns wire.Columns
	loads   []catalogproxy.Load
}

// maxVerdicts bounds the verdicts kept; they are forgotten all at once.
const maxVerdicts = 10000

// resultKey identifies a statement as the cache tells statements apart.
type resultKey [sha256.Size]byte

// cachedResult is a kept result: the rows a statement sent, as its
// encoders made them, and its command tag, at a data version.
type cachedResult struct {
	key     resultKey
	version uint64
	rows    [][]any
	tag     string
	size    int64
}

// resultFlight is a run of a statement whose result others wait for; res
// is set, before done closes, if the run's result was kept. version is the
// version of the tables it reads when it began (see dataStamp), and gen
// the cache's (see purge).
type resultFlight struct {
	version, gen uint64
	done         chan struct{}
	res          *cachedResult
}

// resultFill takes the rows of a statement's run, for the cache to keep if
// the run succeeds with no table changed meanwhile.
type resultFill struct {
	flight *resultFlight
	key    resultKey
	// loads are the tables the statement reads, whose version says
	// whether any changed while it ran (see dataStamp).
	loads []catalogproxy.Load
	max   int64
	rows  [][]any
	size  int64
	// over says the result grew past max, so is not kept.
	over bool
	tag  string
	// finished says finish has ended the run.
	finished bool
}

func newResultCache(size int64, metrics *Metrics) *resultCache {
	if size <= 0 {
		return nil
	}
	return &resultCache{
		max: size, entryMax: size / 16, metrics: metrics, lru: list.New(),
		entries: map[resultKey]*list.Element{}, flights: map[resultKey]*resultFlight{}, verdicts: map[resultKey]bool{},
		memos: map[resultKey]memo{},
	}
}

// add takes a row the run sent. The values stay as they are; the slice
// holding them is the writer's, so it is copied.
func (f *resultFill) add(values []any) {
	if f == nil || f.over {
		return
	}
	f.size += 24 + 16*int64(len(values))
	for _, v := range values {
		f.size += valueSize(v)
	}
	if f.size > f.max {
		f.over, f.rows = true, nil
		return
	}
	f.rows = append(f.rows, slices.Clone(values))
}

// valueSize estimates the memory a value an encoder made holds beyond its
// slot in the row.
func valueSize(v any) int64 {
	switch v := v.(type) {
	case nil, bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, int, uint, float32, float64:
		return 0
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v)) + 24
	case time.Time:
		return 24
	case pgtype.Numeric:
		if v.Int != nil {
			return 48 + int64(len(v.Int.Bits()))*8
		}
		return 48
	case *big.Int:
		return 32 + int64(len(v.Bits()))*8
	}
	return 64
}

// lookup returns key's kept result at version, if there is one; else the
// run of key under way at version, whose result to wait for; else a fill
// for the caller's own run, which others will wait for.
func (c *resultCache) lookup(key resultKey, version uint64, loads []catalogproxy.Load, max int64) (*cachedResult, *resultFlight, *resultFill) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		res := e.Value.(*cachedResult)
		if res.version == version {
			c.lru.MoveToFront(e)
			return res, nil, nil
		}
		c.drop(e)
	}
	if f, ok := c.flights[key]; ok && f.version == version {
		return nil, f, nil
	}
	f := &resultFlight{version: version, gen: c.gen, done: make(chan struct{})}
	c.flights[key] = f
	return nil, nil, &resultFill{flight: f, key: key, loads: loads, max: max}
}

// finish ends fill's run: it keeps the result if the run succeeded, sent
// it whole, no table it reads changed meanwhile (version is their version
// now) and the cache was not emptied, and lets those waiting for it go.
func (c *resultCache) finish(fill *resultFill, err error, version uint64) {
	if c == nil || fill == nil || fill.finished {
		return
	}
	fill.finished = true
	f := fill.flight
	keep := err == nil && !fill.over && fill.tag != "" && version == f.version
	c.mu.Lock()
	keep = keep && c.gen == f.gen
	if c.flights[fill.key] == f {
		delete(c.flights, fill.key)
	}
	if keep {
		res := &cachedResult{key: fill.key, version: f.version, rows: fill.rows, tag: fill.tag, size: fill.size}
		if e, ok := c.entries[fill.key]; ok {
			c.drop(e)
		}
		c.entries[fill.key] = c.lru.PushFront(res)
		c.used += res.size
		for c.used > c.max && c.lru.Len() > 0 {
			c.drop(c.lru.Back())
		}
		f.res = res
	}
	used := c.used
	c.mu.Unlock()
	close(f.done)
	if keep {
		c.metrics.resultCached("kept")
	}
	c.metrics.resultCacheBytes(used)
}

// purge empties the cache: every result it keeps, every statement it
// answers before preparing, and the result of every run under way, which
// it keeps none of. Erasure requests have every replica purge it, with its
// read cache (see engine.Engine.EmptyCache), once they deleted rows.
func (c *resultCache) purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.gen++
	c.lru.Init()
	clear(c.entries)
	clear(c.memos)
	clear(c.verdicts)
	c.used = 0
	c.mu.Unlock()
	c.metrics.resultCacheBytes(0)
}

// drop forgets a kept result. The caller holds c.mu.
func (c *resultCache) drop(e *list.Element) {
	res := c.lru.Remove(e).(*cachedResult)
	delete(c.entries, res.key)
	c.used -= res.size
}

// replay sends a kept result, as its run sent it.
func (res *cachedResult) replay(w wire.DataWriter) error {
	for i := 0; i < len(res.rows); i += rowBatch {
		if err := wire.WriteRows(w, res.rows[i:min(i+rowBatch, len(res.rows))]); err != nil {
			return err
		}
	}
	return w.Complete(res.tag)
}

// fromCache sends st's result from the cache, kept or from a run of the
// same statement that another session has under way, and reports whether
// it did. Otherwise it returns the fill st's own run is to give its rows
// to, nil if its result is not to be kept. The caller has checked that the
// cache may answer st (resultCandidate). key is st's result key when
// st's result is one the cache keeps.
func (s *Server) fromCache(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w *tagWriter) (bool, *resultFill, *resultKey, error) {
	c := s.results
	settings, err := s.resultSettings(ctx, sess)
	if err != nil {
		return false, nil, nil, nil
	}
	if ok, err := s.keepable(ctx, sess, st, settings); !ok {
		return false, nil, nil, err
	}
	if st.loadsGen != s.catalogGeneration() {
		// DDL ran since it was prepared: DuckDB binds it again as it runs,
		// perhaps to other tables than its loads, which its result is tied
		// to (see resultGeneration).
		return false, nil, nil, nil
	}
	key, err := s.resultKey(ctx, sess, st, args, settings)
	if err != nil {
		return false, nil, nil, nil
	}
	served, fill, err := c.answer(ctx, key, s.dataStamp(st.loads), st.loads, w)
	return served, fill, &key, err
}

// dataStamp is the version of the tables loads loaded, which changes when
// any of them may have changed: of the whole catalog when they are none
// known.
func (s *Server) dataStamp(loads []catalogproxy.Load) uint64 {
	if len(loads) == 0 {
		return s.engine.DataVersion() ^ 0x9e3779b97f4a7c15
	}
	return s.engine.TablesVersion(loads)
}

// answer sends key's result, kept at version or from the run of it under
// way, and reports whether it did. Otherwise it returns the fill for the
// caller's own run, which reads the tables loads loaded, nil if its result
// is not to be kept.
func (c *resultCache) answer(ctx context.Context, key resultKey, version uint64, loads []catalogproxy.Load, w *tagWriter) (bool, *resultFill, error) {
	res, flight, fill := c.lookup(key, version, loads, c.entryMax)
	if flight != nil {
		select {
		case <-flight.done:
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
		if res = flight.res; res == nil {
			// Its run kept nothing; this one runs, and keeps nothing either.
			c.metrics.resultCached("missed")
			return false, nil, nil
		}
		c.metrics.resultCached("joined")
		return true, nil, res.replay(w)
	}
	if res != nil {
		c.metrics.resultCached("hit")
		return true, nil, res.replay(w)
	}
	c.metrics.resultCached("missed")
	return false, fill, nil
}

// has reports whether key's result is kept at version, or a run of it is
// under way at version.
func (c *resultCache) has(key resultKey, version uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok && e.Value.(*cachedResult).version == version {
		return true
	}
	f, ok := c.flights[key]
	return ok && f.version == version
}

// Answering before preparing. Preparing a statement holds the catalog lock
// exclusively (see prepareChecked), so the statements of a dashboard that
// several charts or viewers ask at once were prepared one at a time, each
// waiting for those before it, before the cache answered any of them. A
// query whose result the cache keeps is also known by its text
// (statementKey), when preparing it checks nothing that could come out
// otherwise the next time: an admin's, with no parameters, no policy on
// what it reads and nothing read from a connection. The same text from the
// same user, roles, dialect and settings, with the catalog's tables, views
// and policies as they were, is answered from the kept result, or the run
// of it under way, without being prepared.

// maxMemos bounds the memos kept; they are forgotten all at once.
const maxMemos = 10000

// statementKey tells text apart as memoStatement does: by the text, as
// DuckDB is to prepare it, the dialect, the user with their built-in and
// custom roles, the session's settings, and the generations of the
// catalog, its views and the policies; and by the session itself when it
// may have temporary objects. ok is false for a statement it does not
// answer so.
func (s *Server) statementKey(ctx context.Context, sess *session, user auth.User, text string) (resultKey, bool) {
	// A statement with parameters is never noted (see noteStatement), and
	// Snowflake's session variables are translated into the text, or read
	// with getvariable(), whose results are never kept.
	if s.results == nil || sess.resultCacheOff || sess.inTx || user.Role != auth.RoleAdmin || isIglooSQL(ctx) {
		return resultKey{}, false
	}
	settings, err := s.resultSettings(ctx, sess)
	if err != nil {
		return resultKey{}, false
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return resultKey{}, false
	}
	// Policies changed elsewhere show in their generation, as they do when
	// a statement is prepared.
	s.gov.snapshot(ctx)
	h := sha256.New()
	writeField(h, "statement")
	writeField(h, normalized(text))
	writeField(h, string(sess.sqlDialect()))
	writeField(h, user.Name)
	writeField(h, string(user.Role))
	if privs != nil {
		for _, r := range slices.Sorted(slices.Values(privs.Roles)) {
			writeField(h, "role "+r)
		}
	}
	h.Write(settings[:])
	var views uint64
	if s.ddl != nil {
		views = s.ddl.Generation()
	}
	_ = binary.Write(h, binary.LittleEndian, [3]uint64{s.catalogGeneration(), s.gov.gen.Load(), views})
	if sess.temps {
		_ = binary.Write(h, binary.LittleEndian, sess.resultSalt)
	}
	var k resultKey
	h.Sum(k[:0])
	return k, true
}

// noteStatement has the cache answer text, from user, before it is
// prepared from now on, with the result of key and columns, when what
// preparing it checked could not come out otherwise (see statementKey).
func (s *Server) noteStatement(ctx context.Context, sess *session, user auth.User, text string, st *statement, key resultKey,
	columns wire.Columns) {
	if !st.inert() || st.access != nil || len(st.params) > 0 || len(st.loads) == 0 {
		return
	}
	mk, ok := s.statementKey(ctx, sess, user, text)
	if !ok {
		return
	}
	c := s.results
	c.mu.Lock()
	if len(c.memos) >= maxMemos {
		clear(c.memos)
	}
	c.memos[mk] = memo{key: key, columns: columns, loads: st.loads}
	c.mu.Unlock()
}

// memoStatement answers q, a query in DuckDB's dialect as translated by tr
// if at all, from the cache without preparing it, if a statement of the
// same text was answered or kept so (see statementKey). If the result is
// gone by the time the statement runs, it is prepared and run as any
// other, and fails if its columns are not those it described.
func (s *Server) memoStatement(ctx context.Context, sess *session, user auth.User, q wire.Query, tr *translation) (wire.PreparedStatements, bool) {
	if len(q.ParameterOIDs) > 0 {
		return nil, false
	}
	mk, ok := s.statementKey(ctx, sess, user, q.Query)
	if !ok {
		return nil, false
	}
	c := s.results
	c.mu.Lock()
	m, ok := c.memos[mk]
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	// Loading its tables again, as preparing it would, shows a change made
	// through another replica or engine meanwhile; those loads run at once,
	// holding nothing.
	if err := s.engine.ReloadTables(ctx, m.loads); err != nil || !c.has(m.key, s.dataStamp(m.loads)) {
		return nil, false
	}
	ps := wire.NewStatement(func(ctx context.Context, w wire.DataWriter, params []wire.Parameter) error {
		start := time.Now()
		tw := &tagWriter{DataWriter: w}
		served := false
		var err error
		if arrowWriterOf(w) == nil {
			cctx, cached := sess.cancelable(ctx, q.Query)
			var fill *resultFill
			served, fill, err = c.answer(cctx, m.key, s.dataStamp(m.loads), m.loads, tw)
			err = cached(err)
			// Not answered: the caller's own run below keeps its result.
			s.finishResult(fill, errUnfinished)
		}
		if served || err != nil {
			var used *audit.Resources
			if err == nil {
				used = &audit.Resources{}
				c.metrics.resultCached("unprepared")
			}
			s.metrics.query(start, err)
			s.recordQuery(sess, queryRecord{text: q.Query, start: start, err: err, rows: tw.rows(), used: used,
				stmtType: duckdb.STATEMENT_TYPE_SELECT})
			return pgError(err)
		}
		// Gone meanwhile: prepared and run as any other statement.
		ps, err := s.preparedQuery(ctx, sess, user, q, tr)
		if err != nil {
			return err
		}
		defer sess.release(ps[0]) //nolint:errcheck
		if !sameColumns(ps[0].Columns(), m.columns) {
			return pgerror.WithCode(errors.New("cached plan must not change result type"), codes.FeatureNotSupported)
		}
		return ps[0].Run(ctx, w, params)
	}, wire.WithColumns(m.columns))
	return wire.Prepared(ps), true
}

// sameColumns reports whether a and b describe the same columns.
func sameColumns(a, b wire.Columns) bool {
	return slices.EqualFunc(a, b, func(x, y wire.Column) bool { return x.Name == y.Name && x.Oid == y.Oid })
}

// errUnfinished ends a run whose statement's handling stopped before it
// could say how the run went.
var errUnfinished = errors.New("the run did not finish")

// finishResult ends the run fill took rows of (see resultCache.finish).
func (s *Server) finishResult(fill *resultFill, err error) {
	if fill != nil {
		s.results.finish(fill, err, s.dataStamp(fill.loads))
	}
}

// resultCandidate reports whether st is a statement whose result the cache
// may keep, before anything is looked up: a query run outside a
// transaction block, sent as rows, not one Igloo runs its own way (bitmaps,
// bitmap indexes, counts through ids, report calls, catalog listings).
func resultCandidate(sess *session, st *statement, w *tagWriter) bool {
	return st.kind == returnsRows && st.stmtType == duckdb.STATEMENT_TYPE_SELECT && !sess.inTx &&
		!st.bitmaps && len(st.indexPins) == 0 && st.idCounts == nil && !st.reportCall && !st.listsCatalog &&
		arrowWriterOf(w) == nil
}

// resultSettings returns a digest of the session's DuckDB settings, those
// that can change a statement's result, as of its last SET.
func (s *Server) resultSettings(ctx context.Context, sess *session) ([sha256.Size]byte, error) {
	if c := sess.resultSettings; c != nil && c.gen == sess.settingGen {
		return c.sum, nil
	}
	rows, err := queryStrings(ctx, sess, `SELECT name || chr(9) || coalesce(value, '') FROM duckdb_settings() ORDER BY name`)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	h := sha256.New()
	for _, r := range rows {
		if name, _, _ := strings.Cut(r, "\t"); !resultIgnoredSettings[name] {
			writeField(h, r)
		}
	}
	c := &settingsDigest{gen: sess.settingGen}
	h.Sum(c.sum[:0])
	sess.resultSettings = c
	return c.sum, nil
}

// settingsDigest is resultSettings' digest, for the session's settings at
// generation gen.
type settingsDigest struct {
	gen uint64
	sum [sha256.Size]byte
}

// resultIgnoredSettings cannot change a statement's result, and sessions
// may set them differently.
var resultIgnoredSettings = map[string]bool{
	"custom_profiling_settings": true, "enable_profiling": true, "profiling_output": true, "profile_output": true,
	"profiling_mode": true, "profiling_coverage": true, "enable_progress_bar": true, "enable_progress_bar_print": true,
	"progress_bar_time": true, "enable_http_logging": true, "http_logging_output": true, "streaming_buffer_size": true,
	"explain_output": true,
}

// resultKey tells st apart, with args, as the cache does: by the text
// DuckDB prepared, but for its comments and spacing (see normalized), its
// parameters' values, the dialect, the session's settings, and the
// generations of the catalog and of the policies; by the user with their
// built-in and custom roles when a policy changed it (see
// statement.inert); and by the session itself when it may have temporary
// objects. A statement no policy changes gives the same rows to every user
// who may run it, as preparing it checked: it reads only tables of the
// catalog (see keepable), and calls nothing that says who runs it.
func (s *Server) resultKey(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, settings [sha256.Size]byte) (resultKey, error) {
	h := sha256.New()
	writeField(h, normalized(st.prepared))
	writeField(h, string(sess.sqlDialect()))
	if !st.inert() {
		user := sess.currentUser()
		privs, err := s.privileges(ctx, sess, user)
		if err != nil {
			return resultKey{}, err
		}
		writeField(h, "user "+user.Name)
		writeField(h, string(user.Role))
		if privs != nil {
			roles := slices.Sorted(slices.Values(privs.Roles))
			for _, r := range roles {
				writeField(h, "role "+r)
			}
		}
	}
	h.Write(settings[:])
	_ = binary.Write(h, binary.LittleEndian, [2]uint64{s.resultGeneration(), s.gov.gen.Load()})
	if sess.temps {
		if sess.resultSalt == 0 {
			var b [8]byte
			_, _ = rand.Read(b[:])
			sess.resultSalt = binary.LittleEndian.Uint64(b[:]) | 1
		}
		_ = binary.Write(h, binary.LittleEndian, sess.resultSalt)
	}
	for _, a := range args {
		writeField(h, fmt.Sprintf("%d %T %v", a.Ordinal, a.Value, a.Value))
	}
	var k resultKey
	h.Sum(k[:0])
	return k, nil
}

// normalized is text as the cache tells statements apart: without its
// comments, and with each run of white space in its SQL code as one space,
// none at its ends nor before a final semicolon. Neither changes what a
// statement does, and BI tools vary them from one run of a query to the
// next (a comment with the run's ID, say). String literals and quoted
// identifiers stay as written: a literal's spelling can change its type.
func normalized(text string) string {
	var b strings.Builder
	space := false
	put := func(r rune) {
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	for _, seg := range sqllex.Segments(text) {
		switch seg.Kind {
		case sqllex.Comment:
			// A comment parts the words around it, as a space does.
			space = true
		case sqllex.Code:
			for _, r := range seg.Text {
				if unicode.IsSpace(r) {
					space = true
					continue
				}
				put(r)
			}
		default:
			for i, r := range seg.Text {
				if i == 0 {
					put(r)
					continue
				}
				b.WriteRune(r)
			}
		}
	}
	return strings.TrimRight(b.String(), "; ")
}

// writeField writes a length and then s to h, so that no two lists of
// fields write the same bytes.
func writeField(h hash.Hash, s string) {
	_ = binary.Write(h, binary.LittleEndian, uint64(len(s)))
	h.Write([]byte(s))
}

// keepable reports whether st's result may be kept: it calls no unstable
// function (see sessionFunctions) and its plan reads nothing but tables of
// the catalog (see catalogReadsOnly). The verdict holds for the statement's
// text while the catalog and the session's settings stay as they are.
func (s *Server) keepable(ctx context.Context, sess *session, st *statement, settings [sha256.Size]byte) (bool, error) {
	c := s.results
	gen := s.catalogGeneration()
	h := sha256.New()
	writeField(h, st.prepared)
	h.Write(settings[:])
	_ = binary.Write(h, binary.LittleEndian, gen)
	var vk resultKey
	h.Sum(vk[:0])
	c.mu.Lock()
	v, ok := c.verdicts[vk]
	c.mu.Unlock()
	if ok {
		return v, nil
	}
	v, err := s.judgeResult(ctx, sess, st)
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	if len(c.verdicts) >= maxVerdicts {
		clear(c.verdicts)
	}
	c.verdicts[vk] = v
	c.mu.Unlock()
	return v, nil
}

// judgeResult works out keepable's verdict. Explaining st binds its table
// functions again on its connection, after which DuckDB can fail st as it
// was prepared (see reprepare), so st is prepared again; if that fails, so
// does st.
func (s *Server) judgeResult(ctx context.Context, sess *session, st *statement) (bool, error) {
	f, err := s.functionSets(ctx, sess)
	if err != nil {
		return false, nil
	}
	for _, w := range sqllex.CodeWords(st.prepared) {
		if f.unstable[strings.ToLower(w)] {
			return false, nil
		}
	}
	release := s.shareCatalog(sess)
	defer release()
	plan, err := explainLogical(ctx, sess.conn, st.prepared)
	if rerr := st.reprepare(ctx, sess.conn); rerr != nil {
		return false, rerr
	}
	return err == nil && catalogReadsOnly(plan), nil
}

// sessionFunctions read the session's state or the clock, though DuckDB
// counts some of them as consistent. With DuckDB's volatile functions and
// those consistent only within a query (but error(), volatile only so that
// DuckDB does not move it, which gives the same result every time), and the
// macros that call one, directly or through other macros (a macro counts as
// calling a function when its definition has the function's name as a
// word, which errs toward unstable), they are the unstable functions, whose
// calls keep a result from being kept (see functionSets).
var sessionFunctions = map[string]bool{
	"current_setting": true, "getvariable": true, "current_user": true, "current_role": true, "session_user": true,
	"user": true, "current_catalog": true, "current_database": true, "current_schema": true, "current_schemas": true,
	"current_query": true, "current_time": true, "current_timestamp": true, "current_localtime": true,
	"current_localtimestamp": true, "localtime": true, "localtimestamp": true, "get_current_time": true,
	"get_current_timestamp": true, "now": true, "today": true, "current_date": true, "transaction_timestamp": true,
	"txid_current": true, "pg_backend_pid": true, "current_connection_id": true, "current_transaction_id": true,
	"current_query_id": true,
}

// resultOperators are the operators of a plan that read nothing of their
// own, and resultLeaves its leaves that read no table. A plan with any
// other operator or leaf (a scan of DuckDB's own tables, of files, of
// connections, a table function, a sample) is not kept.
var (
	resultOperators = map[string]bool{
		"projection": true, "filter": true, "aggregate": true, "window": true, "unnest": true, "limit": true,
		"order_by": true, "top_n": true, "distinct": true, "pivot": true, "comparison_join": true, "any_join": true,
		"cross_product": true, "positional_join": true, "asof_join": true, "dependent_join": true, "delim_join": true,
		"union": true, "except": true, "intersect": true, "cte": true, "materialized_cte": true, "rec_cte": true,
		"recursive_cte": true, "expression_get": true,
	}
	resultLeaves = map[string]bool{
		"dummy_scan": true, "cte_scan": true, "cte_ref": true, "delim_get": true, "delim_scan": true,
		"empty_result": true, "chunk_get": true, "column_data_get": true, "range": true, "generate_series": true,
	}
)

// catalogReadsOnly reports whether a statement's unoptimized plan (see
// explainLogical) reads tables of the catalog, through Iceberg scans, and
// nothing else.
func catalogReadsOnly(plan any) bool {
	scans, ok := 0, true
	var visit func(n any)
	visit = func(n any) {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				visit(c)
			}
		case map[string]any:
			name, _ := n["name"].(string)
			switch name = strings.ToLower(strings.TrimSpace(name)); {
			case name == "iceberg_scan" && isLeaf(n):
				scans++
			case isLeaf(n) && resultLeaves[name], !isLeaf(n) && resultOperators[name]:
			default:
				ok = false
			}
			visit(n["children"])
		}
	}
	visit(plan)
	return ok && scans > 0
}

// resultCacheOff is the session switch igloo_result_cache, which
// Snowflake's dialect sets as USE_CACHED_RESULT (see switches.go).
func resultCacheOff(sess *session) *bool { return &sess.resultCacheOff }

// resultCacheOn reports whether the session's statements may use kept
// results.
func resultCacheOn(s *Server, sess *session) bool { return s.results != nil && !sess.resultCacheOff }
