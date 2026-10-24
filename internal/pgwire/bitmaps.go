package pgwire

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
)

// Counting distinct IDs in roaring bitmaps. The rb extension counts
// count(DISTINCT x) of a 32-bit unsigned x into a bitmap per group, a few
// bytes an ID, where DuckDB's hash keeps every (group, value) pair: on
// Acme's heaviest reports, where the hash spilled tens of gigabytes or ran
// out of temporary space, bitmaps took a third to a fifth of the time with
// no spill. Where the hash fits in memory it stays a little faster, so
// Igloo picks the statements, as DuckDB cannot: its estimates of the rows
// reaching these aggregates were 20-34x under on Iceberg tables.
//
// A statement counts in bitmaps when it counts the integer IDs of an ID
// domain, which Igloo writes count(DISTINCT CAST(t._igloo_id_<column> AS
// UINTEGER)), and it reads at least Options.BitmapMinRows rows (10 million
// by default): the rows, as the catalog's metadata has them, of the
// catalog tables it reads, views expanded. A join or an unnest multiplies
// them before they reach a count (Acme's reports read 39 and 192 million
// rows and counted up to 94 and 251 million), and a filter cuts them, so
// they say how large a statement is, not what one count takes. That is
// enough: on every report measured, bitmaps took a third to a fifth of the
// hash's time, or finished where it ran out of temporary space, down to
// counts of 11 million rows, while statements whose hash fits in memory
// lose little to them (1.3x on 30 million rows in 100,000 groups, 0.06 s;
// 3x for 1,000 groups of a small integer key), and below 10 million rows
// read the counts take well under a second either way. The statement is
// prepared and run with the rb extension's rb_count_distinct setting at
// 'always', under which its optimizer rule rewrites those counts and
// Igloo's distributed planner combines workers' partial counts as bitmaps.
//
// Bitmaps take memory DuckDB counts, up to rb_memory_budget when set. A
// statement whose bitmaps do not fit fails with an error that starts
// "rb: bitmap memory budget exceeded", before it sends a row; Igloo then
// prepares it again with bitmaps off and runs it once more, on the hash,
// which spills to disk rather than fail (see rerunWithoutBitmaps).

// bitmapSetting is the rb extension's setting that its rule and the
// distributed planner read.
const bitmapSetting = "rb_count_distinct"

// bitmapBudgetError starts the error of a statement whose bitmaps did not
// fit (the rb extension's BUDGET_EXCEEDED). DuckDB lets an extension
// choose no error code of its own.
const bitmapBudgetError = "rb: bitmap memory budget exceeded"

var (
	// countsDistinct finds a count of distinct values, and domainID a cast
	// of an ID domain's integer IDs to UINTEGER, as Igloo writes them. Both
	// must appear: the pair-count rewrite (paircount.go) moves the cast
	// into an inner select whose column the count reads by another name.
	countsDistinct = regexp.MustCompile(`(?i)\bcount\s*\(\s*distinct\b`)
	domainID       = regexp.MustCompile(`(?i)\bcast\s*\(\s*(?:"?\w+"?\s*\.\s*)*"?_igloo_id_\w*"?\s+as\s+uinteger\s*\)`)
)

// bitmapsFor decides whether query, as it is about to be prepared, counts
// distinct IDs in bitmaps.
func (s *Server) bitmapsFor(ctx context.Context, sess *session, query string) bool {
	if !s.bitmaps || !countsDistinct.MatchString(query) || !domainID.MatchString(query) {
		return false
	}
	if s.bitmapMinRows <= 0 {
		return true
	}
	rows, ok := s.statementRows(ctx, sess, query)
	if !ok {
		return false
	}
	s.log.Debug("a statement counting ID domains' IDs reads this many rows", "rows", rows, "bitmaps", rows >= s.bitmapMinRows)
	return rows >= s.bitmapMinRows
}

// statementRows estimates the rows query reads: the rows of each catalog
// table it reads, views expanded, as the catalog's metadata has them
// (refreshed every half minute or so). ok is false when it cannot tell.
func (s *Server) statementRows(ctx context.Context, sess *session, query string) (int64, bool) {
	if s.meta == nil || s.catalogName() == "" {
		return 0, false
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return 0, false
	}
	r.lenient = true
	if inner := explainedStatement(query); inner != "" {
		query = inner
	}
	reads, err := r.analyze(query, nil, 0)
	if err != nil || len(reads.tables) == 0 {
		return 0, false
	}
	if err := s.meta.RefreshMetadata(ctx); err != nil {
		s.log.Debug("could not read the catalog's metadata to estimate a statement's rows", "err", err)
	}
	return tableRows(s.meta.Metadata(), reads.tables)
}

// tableRows sums the rows md gives the tables; ok is false when it knows
// the rows of none of them.
func tableRows(md *catalog.Metadata, tables []auth.Object) (int64, bool) {
	if md == nil {
		return 0, false
	}
	rows := make(map[[2]string]int64, len(md.Tables))
	for _, t := range md.Tables {
		if !t.View && t.Catalog == "" && t.Provider == "" && t.Rows >= 0 {
			rows[[2]string{strings.ToLower(t.Schema), strings.ToLower(t.Name)}] = t.Rows
		}
	}
	var total int64
	known := false
	for _, t := range tables {
		if n, ok := rows[[2]string{strings.ToLower(t.Schema), strings.ToLower(t.Table)}]; ok {
			total += n
			known = true
		}
	}
	return total, known
}

// setBitmaps sets rb_count_distinct to 'always' on the session's connection
// when on, and returns the func that sets it back. It leaves a session that
// set it to 'always' itself alone.
func (sess *session) setBitmaps(ctx context.Context, on bool) func() {
	if !on {
		return func() {}
	}
	got, err := queryStrings(ctx, sess, "SELECT current_setting('"+bitmapSetting+"')")
	if err != nil || len(got) != 1 || strings.EqualFold(got[0], "always") {
		return func() {}
	}
	was := got[0]
	if _, err := sess.conn.ExecContext(ctx, "SET "+bitmapSetting+" = 'always'", nil); err != nil {
		return func() {}
	}
	return func() {
		_, _ = sess.conn.ExecContext(context.WithoutCancel(ctx), "SET "+bitmapSetting+" = "+quoteLiteral(was), nil)
	}
}

// bitmapsDidNotFit reports whether err is a statement's bitmaps running out
// of the memory they may take.
func bitmapsDidNotFit(err error) bool {
	return err != nil && strings.Contains(err.Error(), bitmapBudgetError)
}

// rerunWithoutIndexes runs st again from the tables, after the bitmaps of
// the indexes it read did not fit in the memory DuckDB has for them. It is
// prepared again from its text without them, and may count in bitmaps of
// its own, which rerunWithoutBitmaps then sees to.
func (s *Server) rerunWithoutIndexes(ctx context.Context, sess *session, st *statement, cause error, run func() error) error {
	s.log.Warn("a statement's bitmap indexes did not fit in memory; running it again from the tables",
		"user", sess.currentUser().Name, "err", cause)
	s.metrics.bitmapRerun()
	s.bitmapReruns.Add(1)
	sess.noIndexes = true
	fresh, err := s.prepareChecked(ctx, sess, sess.currentUser(), st.text, st.clientOIDs)
	sess.noIndexes = false
	if err != nil {
		return errors.Join(cause, err)
	}
	if !sameShape(fresh, st, false) {
		_ = fresh.close()
		return errors.Join(cause, errors.New("a statement answered from bitmap indexes changed its result's columns when prepared again"))
	}
	old := st.stmt
	*st = *fresh
	_ = old.Close()
	if sess.bitmapsRestore != nil {
		sess.bitmapsRestore()
	}
	sess.bitmapsRestore = sess.setBitmaps(ctx, st.bitmaps)
	return run()
}

// rerunWithoutBitmaps runs st again, with bitmaps off, after its bitmaps
// did not fit (cause): it prepares st again with rb_count_distinct off, so
// that DuckDB plans the hash, and calls run. A second failure is the
// statement's error; st keeps the hash for later runs.
func (s *Server) rerunWithoutBitmaps(ctx context.Context, sess *session, st *statement, cause error, run func() error) error {
	s.log.Warn("a statement's bitmaps did not fit in memory; running it again counting with DuckDB's hash",
		"user", sess.currentUser().Name, "err", cause)
	s.metrics.bitmapRerun()
	s.bitmapReruns.Add(1)
	st.bitmaps = false
	if sess.bitmapsRestore != nil {
		sess.bitmapsRestore()
		sess.bitmapsRestore = nil
	}
	release := s.holdCatalog(sess)
	err := st.reprepare(ctx, sess.conn)
	release()
	if err != nil {
		return errors.Join(cause, err)
	}
	return run()
}
