package pgwire

import (
	"context"
	"fmt"
	"slices"
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
// function whose value changes, that reads DuckDB's own tables, or that
// another user or other parameters send.
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

	// Another user's result is its own, and so is one of other parameters.
	rita := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	hits := outcome("hit")
	texts(t, rita, q)
	if outcome("hit") > hits {
		t.Error("another user was sent admin's result")
	}
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
