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

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Result cache. A dashboard that several people open at once, or that asks
// the same question in several of its charts, sends the same query again
// and again over data that has not changed. Igloo keeps the result of such
// a query and sends it again, without running the query, to the next
// statement that is the same text with the same parameters, from the same
// user with the same roles and settings, as long as no table of the
// catalog has changed since (see catalogproxy's DataVersion). A statement
// that arrives while the same one runs waits for that run's result rather
// than running too.
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
	// keepable); unstable are the functions whose calls keep a result from
	// being kept, as of catalog generation unstableGen.
	verdicts    map[resultKey]bool
	unstable    map[string]bool
	unstableGen uint64
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
// is set, before done closes, if the run's result was kept.
type resultFlight struct {
	version uint64
	done    chan struct{}
	res     *cachedResult
}

// resultFill takes the rows of a statement's run, for the cache to keep if
// the run succeeds with no table changed meanwhile.
type resultFill struct {
	flight *resultFlight
	key    resultKey
	max    int64
	rows   [][]any
	size   int64
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
func (c *resultCache) lookup(key resultKey, version uint64, max int64) (*cachedResult, *resultFlight, *resultFill) {
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
	f := &resultFlight{version: version, done: make(chan struct{})}
	c.flights[key] = f
	return nil, nil, &resultFill{flight: f, key: key, max: max}
}

// finish ends fill's run: it keeps the result if the run succeeded, sent
// it whole, and no table changed meanwhile (version is the data version
// now), and lets those waiting for it go.
func (c *resultCache) finish(fill *resultFill, err error, version uint64) {
	if c == nil || fill == nil || fill.finished {
		return
	}
	fill.finished = true
	f := fill.flight
	keep := err == nil && !fill.over && fill.tag != "" && version == f.version
	c.mu.Lock()
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
// to, nil if its result is not to be kept.
func (s *Server) fromCache(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w *tagWriter) (bool, *resultFill, error) {
	c := s.results
	if c == nil || sess.resultCacheOff || !resultCandidate(sess, st, w) {
		return false, nil, nil
	}
	settings, err := s.resultSettings(ctx, sess)
	if err != nil {
		return false, nil, nil
	}
	if !s.keepable(ctx, sess, st, settings) {
		return false, nil, nil
	}
	key, err := s.resultKey(ctx, sess, st, args, settings)
	if err != nil {
		return false, nil, nil
	}
	res, flight, fill := c.lookup(key, s.engine.DataVersion(), c.entryMax)
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

// errUnfinished ends a run whose statement's handling stopped before it
// could say how the run went.
var errUnfinished = errors.New("the run did not finish")

// finishResult ends the run fill took rows of (see resultCache.finish).
func (s *Server) finishResult(fill *resultFill, err error) {
	if fill != nil {
		s.results.finish(fill, err, s.engine.DataVersion())
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
// DuckDB prepared, its parameters' values, the user with their built-in and
// custom roles, the session's settings, and the generations of the catalog
// and of the policies; and by the session itself when it may have
// temporary objects.
func (s *Server) resultKey(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, settings [sha256.Size]byte) (resultKey, error) {
	user := sess.currentUser()
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return resultKey{}, err
	}
	h := sha256.New()
	writeField(h, st.prepared)
	writeField(h, user.Name)
	writeField(h, string(user.Role))
	if privs != nil {
		roles := slices.Sorted(slices.Values(privs.Roles))
		for _, r := range roles {
			writeField(h, "role "+r)
		}
	}
	h.Write(settings[:])
	_ = binary.Write(h, binary.LittleEndian, [2]uint64{s.catalogGeneration(), s.gov.gen.Load()})
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

// writeField writes a length and then s to h, so that no two lists of
// fields write the same bytes.
func writeField(h hash.Hash, s string) {
	_ = binary.Write(h, binary.LittleEndian, uint64(len(s)))
	h.Write([]byte(s))
}

// keepable reports whether st's result may be kept: it calls no function
// in unstableFunctions and its plan reads nothing but tables of the
// catalog (see catalogReadsOnly). The verdict holds for the statement's
// text while the catalog and the session's settings stay as they are.
func (s *Server) keepable(ctx context.Context, sess *session, st *statement, settings [sha256.Size]byte) bool {
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
		return v
	}
	v = s.judgeResult(ctx, sess, st, gen)
	c.mu.Lock()
	if len(c.verdicts) >= maxVerdicts {
		clear(c.verdicts)
	}
	c.verdicts[vk] = v
	c.mu.Unlock()
	return v
}

// judgeResult works out keepable's verdict.
func (s *Server) judgeResult(ctx context.Context, sess *session, st *statement, gen uint64) bool {
	unstable, err := s.unstableFunctions(ctx, sess, gen)
	if err != nil {
		return false
	}
	for _, w := range sqllex.CodeWords(st.prepared) {
		if unstable[strings.ToLower(w)] {
			return false
		}
	}
	release := s.shareCatalog(sess)
	plan, err := explainLogical(ctx, sess.conn, st.prepared)
	release()
	return err == nil && catalogReadsOnly(plan)
}

// unstableFunctions returns the functions whose calls keep a result from
// being kept: DuckDB's volatile ones and those consistent only within a
// query, the ones that read the session's state, and the macros that call
// one, directly or through other macros (a macro counts as calling a
// function when its definition has the function's name as a word, which
// errs toward unstable). error() is volatile only so that DuckDB does not
// move it; it gives the same result every time.
func (s *Server) unstableFunctions(ctx context.Context, sess *session, gen uint64) (map[string]bool, error) {
	c := s.results
	c.mu.Lock()
	if c.unstable != nil && c.unstableGen == gen {
		defer c.mu.Unlock()
		return c.unstable, nil
	}
	c.mu.Unlock()
	// Listing them walks the catalog (see catalogRunLock).
	release := s.holdCatalog(sess)
	rows, err := queryStrings(ctx, sess, `SELECT system.main.lower(function_name) || chr(9) || coalesce(stability, '') ||
		chr(9) || coalesce(macro_definition, '') FROM system.main.duckdb_functions()`)
	release()
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for k := range sessionFunctions {
		out[k] = true
	}
	macros := map[string][]string{}
	for _, r := range rows {
		name, rest, _ := strings.Cut(r, "\t")
		stability, def, _ := strings.Cut(rest, "\t")
		switch {
		case (stability == "VOLATILE" || stability == "CONSISTENT_WITHIN_QUERY") && name != "error":
			out[name] = true
		case def != "":
			for _, w := range sqllex.CodeWords(def) {
				macros[name] = append(macros[name], strings.ToLower(w))
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for name, words := range macros {
			if out[name] {
				continue
			}
			for _, w := range words {
				if out[w] {
					out[name], changed = true, true
					break
				}
			}
		}
	}
	c.mu.Lock()
	c.unstable, c.unstableGen = out, gen
	c.mu.Unlock()
	return out, nil
}

// sessionFunctions read the session's state or the clock, though DuckDB
// counts some of them as consistent.
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
