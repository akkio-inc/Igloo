package pgwire

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestResultCache checks which results the cache sends again and that it
// sends none that running the statement again would not give: after a
// change through this replica or another one, of a statement that calls a
// function whose value changes, that reads DuckDB's own tables, that other
// parameters send, or that a policy changes for another user. It sends a
// result again whatever the comments and spacing of the statement, to
// other users when no policy changes it, and after changes to other
// tables.
func TestResultCache(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		o.ResultCacheSize = 64 << 20
		o.Metrics = metrics
	})
	ctx := context.Background()
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.sales AS SELECT i % 7 AS k, i AS v FROM range(1000) r(i)")
	mustExec(t, admin, "CREATE TABLE memory.main.local AS SELECT 1 AS n")
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	outcome := func(o string) float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues(o)) }
	// same runs a query twice, and reports whether the second run was a
	// hit and gave the first one's rows.
	same := func(conn *pgx.Conn, sql string, args ...any) bool {
		t.Helper()
		first := texts(t, conn, sql, args...)
		hits := outcome("hit")
		again := texts(t, conn, sql, args...)
		if !slices.Equal(first, again) {
			t.Errorf("%s: %v, then %v", sql, first, again)
		}
		return outcome("hit") > hits
	}
	const q = "SELECT k, count(*), sum(v)::BIGINT FROM lake.public.sales GROUP BY k ORDER BY k"
	if !same(admin, q) {
		t.Errorf("%s ran again rather than being sent from the cache", q)
	}
	if outcome("kept") < 1 {
		t.Errorf("kept %v results", outcome("kept"))
	}

	// A change through this replica, and one through another.
	before := texts(t, admin, q)
	mustExec(t, admin, "INSERT INTO lake.public.sales VALUES (0, 5000)")
	if after := texts(t, admin, q); slices.Equal(before, after) || after[0] != "0|144|76071" {
		t.Errorf("after an insert here: %v, then %v", before, after)
	}
	other := vc.replica(t).admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, other, "INSERT INTO lake.public.sales VALUES (1, 7000)")
	if n := count(t, admin, "SELECT count(*) FROM lake.public.sales WHERE k = 1"); n != 144 {
		t.Errorf("after an insert through another replica: %d rows of k = 1, want 144", n)
	}
	if got := texts(t, admin, q); got[1] != "1|144|78214" {
		t.Errorf("after an insert through another replica: %v", got)
	}

	// A statement that calls pragma_table_info on an Iceberg table runs as
	// it was prepared after the cache explained it (see judgeResult).
	if got := texts(t, admin, "SELECT string_agg(name, '|' ORDER BY cid) FROM pragma_table_info('lake.public.sales')"); !slices.Equal(got, []string{"k|v"}) {
		t.Errorf("describing a table: %v", got)
	}

	// Statements whose result can change, or that read what the data
	// version does not follow, run every time.
	for _, sql := range []string{
		"SELECT count(*), max(random()) < 2 FROM lake.public.sales",
		"SELECT count(*), max(now()) <= now() FROM lake.public.sales",
		"SELECT count(*), current_setting('timezone') <> '' FROM lake.public.sales",
		"SELECT count(*) FROM lake.public.sales, memory.main.local",
		"SELECT 1",
	} {
		if same(admin, sql) {
			t.Errorf("%s was sent from the cache", sql)
		}
	}
	// A session's temporary view is its own, and so are the results of a
	// session that may have one.
	for i, def := range []string{"SELECT k FROM lake.public.sales WHERE k = 2", "SELECT k FROM lake.public.sales WHERE k = 3"} {
		session := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, session, "CREATE TEMP VIEW mine AS "+def)
		hits := outcome("hit")
		if got := texts(t, session, "SELECT count(*), min(k) FROM mine"); !slices.Equal(got, []string{fmt.Sprintf("143|%d", i+2)}) {
			t.Errorf("session %d: %v", i, got)
		}
		if outcome("hit") > hits {
			t.Errorf("session %d was sent another session's result for its temporary view", i)
		}
		if !same(session, "SELECT min(k) FROM mine") {
			t.Errorf("session %d: a statement ran again in the same session", i)
		}
	}

	// Changes to other tables leave it be; comments and spacing do not
	// matter.
	mustExec(t, admin, "CREATE TABLE lake.public.raw_events AS SELECT 1 AS n")
	texts(t, admin, q)
	mustExec(t, admin, "INSERT INTO lake.public.raw_events VALUES (2)")
	mustExec(t, other, "INSERT INTO lake.public.raw_events VALUES (3)")
	hits := outcome("hit")
	if got := texts(t, admin, "-- dashboard 7, run 81f3\n"+strings.ReplaceAll(q, " ", "\n   ")+" /* chart 2 */;"); !slices.Equal(got, texts(t, admin, q)) ||
		outcome("hit") < hits+2 {
		t.Errorf("after changes to another table, with comments: %v, %v hits", got, outcome("hit")-hits)
	}

	// Another user is sent the result while no policy changes it; one of
	// other parameters is its own.
	rita := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	hits = outcome("hit")
	if got := texts(t, rita, q); !slices.Equal(got, texts(t, admin, q)) || outcome("hit") < hits+1 {
		t.Errorf("another user: %v, from the cache %v", got, outcome("hit") > hits)
	}
	// Once a policy changes what rita reads, her result is her own.
	mustExec(t, admin, "CREATE ROW ACCESS POLICY low_keys AS (k BIGINT) RETURNS BOOLEAN -> is_role_in_session('admin') OR k < 3")
	mustExec(t, admin, "ALTER TABLE lake.public.sales ADD ROW ACCESS POLICY low_keys ON (k)")
	texts(t, admin, q)
	if got := texts(t, rita, q); len(got) != 3 {
		t.Errorf("under a row access policy, rita read %v", got)
	}
	mustExec(t, admin, "ALTER TABLE lake.public.sales DROP ROW ACCESS POLICY low_keys")
	extended := r.admin(t, pgx.QueryExecModeExec)
	const byK = "SELECT count(*) FROM lake.public.sales WHERE k = $1"
	if n, m := count(t, extended, byK, 2), count(t, extended, byK, 3); n != 143 || m != 143 {
		t.Errorf("counts by parameter: %d, %d", n, m)
	}
	hits = outcome("hit")
	if n := count(t, extended, byK, 4); n != 143 || outcome("hit") > hits {
		t.Errorf("a new parameter: %d rows, from the cache %v", n, outcome("hit") > hits)
	}
	if !same(extended, byK, 2) {
		t.Error("a statement with parameters ran again")
	}

	// A session that turns the cache off runs its statements.
	for _, set := range []string{"SET igloo_result_cache = off", "ALTER SESSION SET USE_CACHED_RESULT = FALSE"} {
		off := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, off, set)
		if got := texts(t, off, "SHOW igloo_result_cache"); !slices.Equal(got, []string{"off"}) {
			t.Errorf("%s: SHOW says %v", set, got)
		}
		if same(off, q) {
			t.Errorf("after %s, a statement was sent from the cache", set)
		}
		mustExec(t, off, "RESET igloo_result_cache")
		if !same(off, q) {
			t.Errorf("after %s and RESET, a statement ran again", set)
		}
	}

	// A statement sent while the same one runs waits for its result.
	const slow = "SELECT count(*) FROM lake.public.sales a, lake.public.sales b, range(300) c WHERE a.v + b.v + c.range < 0"
	joined := outcome("joined")
	var wg sync.WaitGroup
	results := make([]int64, 2)
	for i := range results {
		conn := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		wg.Go(func() {
			time.Sleep(time.Duration(i) * 200 * time.Millisecond)
			if err := conn.QueryRow(ctx, slow).Scan(&results[i]); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if results[0] != 0 || results[1] != 0 || outcome("joined") <= joined {
		t.Errorf("concurrent runs: %v, joined %v", results, outcome("joined")-joined)
	}
}

// TestResultCacheBeforePreparing checks that a query the cache keeps is
// answered without being prepared the next time its text comes, from any
// session of the same user, in either dialect and protocol, while nothing
// it reads has changed, here or through another replica; and that a user
// whose statements preparing checks is not answered so.
func TestResultCacheBeforePreparing(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		o.ResultCacheSize = 64 << 20
		o.Metrics = metrics
	})
	var mu sync.Mutex
	prepared := map[string]int{}
	r.srv.beforePrepare = func(_ context.Context, text string) {
		mu.Lock()
		prepared[text]++
		mu.Unlock()
	}
	preparedTimes := func(text string) int {
		mu.Lock()
		defer mu.Unlock()
		return prepared[text]
	}
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.visits AS SELECT i % 5 AS k, i AS v FROM range(500) r(i)")
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	unprepared := func() float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues("unprepared")) }

	// A JSON path's $ is no parameter.
	const q = "SELECT k, count(*), sum(v)::BIGINT, json_extract_string('{\"a\": 1}', '$.a') FROM lake.public.visits GROUP BY k ORDER BY k"
	want := texts(t, admin, q)
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeDescribeExec} {
		before, times := unprepared(), preparedTimes(q)
		if got := texts(t, r.admin(t, mode), q); !slices.Equal(got, want) {
			t.Errorf("mode %v: %v, want %v", mode, got, want)
		}
		if unprepared() != before+1 || preparedTimes(q) != times {
			t.Errorf("mode %v: the query was prepared again", mode)
		}
	}

	// A change through another replica shows: the tables it reads are
	// loaded again first.
	mustExec(t, vc.replica(t).admin(t, pgx.QueryExecModeSimpleProtocol), "INSERT INTO lake.public.visits VALUES (0, 10000)")
	before := unprepared()
	if got := texts(t, admin, q); got[0] != "0|101|34750|1" {
		t.Errorf("after an insert through another replica: %v", got)
	}
	if unprepared() != before {
		t.Error("answered from before the insert")
	}
	if texts(t, admin, q); unprepared() != before+1 {
		t.Error("the new result was not answered before preparing")
	}

	// A user other than an admin, whose statements preparing checks.
	rita := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	before = unprepared()
	texts(t, rita, q)
	texts(t, rita, q)
	if unprepared() != before {
		t.Error("a reader's query was answered before preparing")
	}

	// Snowflake's dialect.
	sf := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	const sq = "select K, count(*) as N from lake.public.visits group by K order by K"
	first := texts(t, sf, sq)
	before = unprepared()
	if got := texts(t, sf, sq); !slices.Equal(got, first) || unprepared() != before+1 {
		t.Errorf("Snowflake's dialect: %v then %v, %v answered before preparing", first, got, unprepared()-before)
	}
}

// TestResultCacheDashboard measures how often a dashboard's queries are
// answered from the cache: three users open a dashboard of five charts
// three times each, its BI tool puts the run's ID in a comment, a pipe
// loads a raw table between each opening, and a load into the dashboard's
// own table comes once. Then an erasure request empties the cache.
func TestResultCacheDashboard(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		o.ResultCacheSize = 64 << 20
		o.Metrics = metrics
	})
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.spend AS SELECT DATE '2026-09-01' + (i % 30)::INTEGER AS day, 'c' || (i % 12) AS client, i % 97 AS amount FROM range(20000) r(i)",
		"CREATE TABLE lake.public.clients AS SELECT 'c' || i AS client, ['north', 'south', 'west'][1 + i % 3] AS region FROM range(12) r(i)",
		"CREATE TABLE lake.public.raw_drops (line VARCHAR)",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
		"CREATE USER sam WITH PASSWORD 'sam-password' ROLE reader",
	} {
		mustExec(t, admin, sql)
	}
	charts := []string{
		"SELECT day, sum(amount) AS spend FROM lake.public.spend GROUP BY day ORDER BY day",
		"SELECT c.region, sum(s.amount) AS spend FROM lake.public.spend s JOIN lake.public.clients c USING (client) GROUP BY c.region ORDER BY 1",
		"SELECT client, count(*) AS days FROM lake.public.spend GROUP BY client ORDER BY 2 DESC, 1 LIMIT 5",
		"SELECT count(DISTINCT client) AS clients, sum(amount) AS total FROM lake.public.spend",
		"SELECT region, count(*) FROM lake.public.clients GROUP BY region ORDER BY region",
	}
	users := []*pgx.Conn{
		admin,
		connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol),
		connect(t, r.url("sam", "sam-password"), pgx.QueryExecModeSimpleProtocol),
	}
	outcome := func(o string) float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues(o)) }
	// What running each chart gives, without the cache.
	plain := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, plain, "SET igloo_result_cache = off")
	rng := rand.New(rand.NewPCG(1, 2))
	hits0, missed0 := outcome("hit"), outcome("missed")
	for round := range 3 {
		for u, conn := range users {
			for i, chart := range charts {
				// As Looker writes its context into each query.
				text := fmt.Sprintf("-- BI context {\"user\":%d,\"chart\":%d,\"run\":\"%x\"}\n%s", u, i, rng.Uint64(), chart)
				if got, want := texts(t, conn, text), texts(t, plain, chart); !slices.Equal(got, want) {
					t.Errorf("round %d, user %d, chart %d: %q, want %q", round, u, i, got, want)
				}
			}
			// A pipe's load into a table the dashboard does not read.
			mustExec(t, admin, fmt.Sprintf("INSERT INTO lake.public.raw_drops VALUES ('drop %d %d')", round, u))
		}
		if round == 1 {
			mustExec(t, admin, "INSERT INTO lake.public.spend VALUES (DATE '2026-10-01', 'c1', 5)")
		}
	}
	hits, missed := outcome("hit")-hits0, outcome("missed")-missed0
	t.Logf("dashboard: %v of %v chart queries answered from the cache (%.0f%%)", hits, hits+missed, 100*hits/(hits+missed))
	if hits < 0.8*(hits+missed) {
		t.Errorf("only %v of %v chart queries were answered from the cache", hits, hits+missed)
	}

	// An erasure request empties the cache, with the read cache.
	if err := r.eng.EmptyCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := outcome("hit")
	texts(t, admin, charts[0])
	if outcome("hit") > before {
		t.Error("a result kept before the cache was emptied was sent again")
	}
}

// TestNormalized checks that the cache tells statements apart but for
// their comments and spacing, and by everything else.
func TestNormalized(t *testing.T) {
	for in, want := range map[string]string{
		"SELECT  a,\n\tb   FROM t;":                           "SELECT a, b FROM t",
		"-- Looker run 81f3\nSELECT a FROM t /* chart 2 */ ;": "SELECT a FROM t",
		"SELECT a/**/b FROM t":                                "SELECT a b FROM t",
		"SELECT 'two  spaces',  \"Quoted  Name\" FROM t":      "SELECT 'two  spaces', \"Quoted  Name\" FROM t",
		"SELECT x FROM t WHERE s = '--not a comment'":         "SELECT x FROM t WHERE s = '--not a comment'",
		"SELECT x\n  FROM t\n  WHERE y = $1":                  "SELECT x FROM t WHERE y = $1",
	} {
		if got := normalized(in); got != want {
			t.Errorf("normalized(%q) = %q, want %q", in, got, want)
		}
	}
	for _, pair := range [][2]string{
		{"SELECT x FROM t WHERE s = 'a'", "SELECT x FROM t WHERE s = 'A'"},
		{"SELECT x FROM t WHERE n = 5", "SELECT x FROM t WHERE n = '5'"},
		{"SELECT \"X\" FROM t", "SELECT \"x\" FROM t"},
		{"SELECT a - -1 FROM t", "SELECT a --1 FROM t"},
	} {
		if normalized(pair[0]) == normalized(pair[1]) {
			t.Errorf("%q and %q are not told apart", pair[0], pair[1])
		}
	}
}
