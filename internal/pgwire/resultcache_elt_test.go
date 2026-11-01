package pgwire

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestResultCacheKeepsThroughOtherWrites checks that the result cache
// keeps answering a dashboard over tables that ELT does not write while ELT
// runs over others: each kind of ELT statement, run on its own, leaves the
// dashboard's results in the cache, and so does ELT running alongside the
// dashboard, whose statements are prepared and bound at the same time. A
// CREATE OR REPLACE TABLE, a materialized view's refresh and CREATE SCHEMA
// IF NOT EXISTS emptied the whole cache.
func TestResultCacheKeepsThroughOtherWrites(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	env := startRefreshServer(t, func(o *Options) {
		o.ResultCacheSize = 64 << 20
		o.Metrics = metrics
	})
	admin := env.admin
	for _, q := range []string{
		"CREATE TABLE clients AS SELECT i AS client_id, 'region_' || (i % 3) AS region FROM range(20) r(i)",
		"CREATE TABLE spend AS SELECT i % 20 AS client_id, i % 97 AS amount FROM range(20000) r(i)",
		"CREATE TABLE events (client_id INTEGER, n INTEGER)",
		"INSERT INTO events SELECT i % 20, i FROM range(1000) r(i)",
		"CREATE TABLE event_stats (client_id INTEGER, n BIGINT)",
		"CREATE MATERIALIZED VIEW event_reach AS SELECT client_id, count(DISTINCT n) AS reach FROM events GROUP BY client_id",
	} {
		mustExec(t, admin, q)
	}
	charts := []string{
		"SELECT c.region, sum(s.amount) AS spend FROM spend s JOIN clients c USING (client_id) GROUP BY 1 ORDER BY 1",
		"SELECT client_id, count(*) AS n FROM spend GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 5",
	}
	elt := []string{
		"INSERT INTO events SELECT i % 20, 1000 + i FROM range(100) r(i)",
		"CREATE OR REPLACE TABLE event_daily AS SELECT client_id, count(*) AS n FROM events GROUP BY 1",
		"INSERT INTO event_stats SELECT client_id, count(*) FROM events GROUP BY 1",
		"MERGE INTO event_stats t USING (SELECT client_id, count(*) AS n FROM events GROUP BY 1) u ON t.client_id = u.client_id " +
			"WHEN MATCHED THEN UPDATE SET n = u.n WHEN NOT MATCHED THEN INSERT VALUES (u.client_id, u.n)",
		"REFRESH MATERIALIZED VIEW event_reach",
		"CREATE SCHEMA IF NOT EXISTS public",
	}
	outcome := func(o string) float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues(o)) }
	dash := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, c := range charts {
		texts(t, dash, c)
	}
	// Each ELT statement on its own.
	for _, q := range elt {
		mustExec(t, admin, q)
		hits := outcome("hit")
		for _, c := range charts {
			texts(t, dash, c)
		}
		if got := outcome("hit") - hits; got != float64(len(charts)) {
			t.Errorf("after %.40s...: %v of %d charts from the cache", q, got, len(charts))
		}
	}

	// ELT alongside the dashboard.
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var ran atomic.Int64
	for s := range 1 {
		conn := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := s; ctx.Err() == nil; i++ {
				if _, err := conn.Exec(ctx, elt[i%len(elt)]); err != nil && ctx.Err() == nil {
					t.Errorf("ELT %.40s...: %v", elt[i%len(elt)], err)
					return
				}
				ran.Add(1)
			}
		}()
	}
	hits0, missed0 := outcome("hit"), outcome("missed")
	deadline := time.Now().Add(time.Minute)
	for round := 0; ran.Load() < int64(3*len(elt)) && time.Now().Before(deadline); round++ {
		for _, c := range charts {
			texts(t, dash, fmt.Sprintf("/* tile %d */ %s", round%3, c))
		}
	}
	stop()
	wg.Wait()
	hits, missed := outcome("hit")-hits0, outcome("missed")-missed0
	t.Logf("with %d ELT statements alongside: %v of %v charts from the cache", ran.Load(), hits, hits+missed)
	if hits < 0.9*(hits+missed) {
		t.Errorf("only %v of %v charts came from the cache while ELT wrote other tables", hits, hits+missed)
	}
}

// TestResultCacheFollowsShadowingTables checks that a statement prepared
// before a table was made that its name now binds to gets that table's
// rows, not the result kept of the table it bound to before: making a
// table no longer empties the cache (see resultGeneration), and DuckDB
// binds a prepared statement again as it runs.
func TestResultCacheFollowsShadowingTables(t *testing.T) {
	env := startRefreshServer(t, func(o *Options) { o.ResultCacheSize = 64 << 20 })
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA lake.other",
		"CREATE TABLE lake.other.shadowed AS SELECT range AS n FROM range(3)",
	} {
		mustExec(t, env.admin, q)
	}
	conn := env.testServer.admin(t, pgx.QueryExecModeCacheStatement)
	mustExec(t, conn, "SET search_path = 'lake.public,lake.other'")
	count := func() int64 {
		t.Helper()
		var n int64
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM shadowed WHERE n >= $1", 0).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if a, b := count(), count(); a != 3 || b != 3 {
		t.Fatalf("before: %d, %d rows, want 3", a, b)
	}
	mustExec(t, env.admin, "CREATE TABLE lake.public.shadowed AS SELECT range AS n FROM range(5)")
	if n := count(); n != 5 {
		t.Errorf("after a table of the name was made earlier on the search path: %d rows, want 5", n)
	}
}
