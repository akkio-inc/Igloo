package pgwire

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/distributed"
)

// newDistE2Test is newE2Test on a replica with workers, which read the
// warehouse's Iceberg tables as production's do (distributed.NewIceberg),
// ID domains' columns of ids included.
func newDistE2Test(t *testing.T, workers int, opts ...func(*Options)) *idTest {
	t.Helper()
	needRB(t)
	vc := newViewCatalog(t)
	dir := vc.cfg.Storage.Location
	gate := newTaskGate()
	var addrs []string
	for i := range workers {
		addr, _, _ := startTestWorkerRB(t, dir, i, gate, os.Getenv("IGLOO_TEST_RB_EXTENSION"))
		addrs = append(addrs, addr)
	}
	cat, err := catalog.Open(context.Background(), vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: distributed.NewIceberg(cat), Secret: testWorkerSecret, Version: "test", Cores: 1,
		// Partial results where the replica's DuckDB may read them: under the
		// warehouse, which here is a local directory, as exchanges are.
		TempDir: filepath.Join(dir, "coordinator"), Warehouse: dir + "/",
		Storage: distributed.NewStorage(icebergio.LocalFS{}), Pool: distributed.PoolOptions{Addresses: addrs},
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	return e2Fixture(newIndexTestOn(t, vc, append([]func(*Options){func(o *Options) { o.Distributed = co }}, opts...)...))
}

// TestReportCallsOnWorkers runs report API calls whose cohorts come from
// bitmap indexes (path index+scan) on workers: the coordinator computes the
// cohorts' bitmaps, every task lists their members and joins them to its
// share of the source, which EXPLAIN DISTRIBUTED shows split among the
// workers. Their rows are those of the calls on one machine, and of calls
// through ids from the cohorts' tables, which run on workers too.
func TestReportCallsOnWorkers(t *testing.T) {
	t.Parallel()
	history := audit.NewMemoryStore(1000, 1000)
	it := newDistE2Test(t, 2, func(o *Options) {
		o.Bitmaps, o.BitmapMinRows = true, 0
		o.Audit = audit.New(history, audit.Options{}, slog.New(slog.DiscardHandler))
	})
	srv := it.r.srv
	conn := it.admin
	mustExec(t, conn, "SET search_path = 'lake.public'")
	it.exec(
		"CREATE TABLE lake.public.plays AS SELECT 'p' || (i % 200) AS person_id, CASE i % 5 WHEN 0 THEN 'drama' WHEN 1 THEN 'news' WHEN 2 THEN '' WHEN 3 THEN NULL ELSE 'sport' END AS genre, "+
			"CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i AS INTEGER) END AS seconds, DATE '2026-01-01' + CAST(i % 40 AS INTEGER) AS day FROM range(1500) r(i)",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(context.Background())
	cohorts := []any{
		map[string]any{"label": "AUDIENCE", "table": "audience_members", "member": "person_id", "where": map[string]any{"audience_id": "a1"},
			"version": map[string]any{"column": "ver", "latest_from": map[string]any{"table": "audience_revisions", "where": map[string]any{"audience_id": "a1"}}}},
		map[string]any{"label": "BASE_POPULATION", "table": "v_attributes", "member": "person_id"},
	}
	plays := func(metrics ...any) map[string]any {
		return map[string]any{"version": 1, "cohorts": cohorts, "totals": true, "extent": "day",
			"source": map[string]any{"table": "plays", "member": "person_id"},
			"dimensions": []any{map[string]any{"name": "GENRE", "column": "genre", "skip_blank": true},
				map[string]any{"name": "REACH", "constant": "ANY_PLAY", "metric": "seconds"}},
			"metrics": metrics}
	}
	seconds := map[string]any{"name": "seconds", "column": "seconds", "coalesce": 0}
	days := map[string]any{"name": "days", "distinct_with": "day"}
	call := func(spec map[string]any) string {
		return "SELECT * EXCLUDE (path) FROM igloo.audience_profile(" + specLiteral(t, spec) + ") ORDER BY ALL"
	}
	onWorkers := func(what, path string, spec map[string]any) {
		t.Helper()
		q := call(spec)
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, q)
		if got := texts(t, conn, "SELECT DISTINCT path FROM igloo.audience_profile("+specLiteral(t, spec)+")"); !slices.Equal(got, []string{path}) {
			t.Errorf("%s: paths %q, want %s", what, got, path)
		}
		mustExec(t, conn, "SET distributed_execution = on")
		explained := strings.Join(texts(t, conn, "EXPLAIN DISTRIBUTED "+q), "\n")
		if !strings.HasPrefix(explained, "runs on|workers") || !strings.Contains(explained, "split public.plays") {
			t.Errorf("%s: the scan of plays is not split among workers:\n%s", what, explained)
		}
		spread := texts(t, conn, q)
		if len(local) == 0 || !slices.Equal(local, spread) {
			t.Errorf("%s: rows on workers\n%q\non one machine\n%q", what, spread, local)
		}
		if w := lastWorkers(t, history, q); w == 0 {
			t.Errorf("%s: ran on no worker", what)
		}
	}
	onWorkers("cohorts from their bitmaps", "index+scan", plays(seconds))
	// Pairs of days in bitmaps a day, which workers' partial bitmaps
	// combine into.
	srv.splitPairs = true
	onWorkers("cohorts from their bitmaps, with pairs of days", "index+scan", plays(seconds, days))
	srv.splitPairs = false
	// Through ids, the cohorts from their tables.
	mustExec(t, conn, "SET igloo_bitmap_indexes = off")
	onWorkers("cohorts from their tables", "scan", plays(seconds))
	mustExec(t, conn, "RESET igloo_bitmap_indexes")
}

// TestReportRecognitionKeepsWhatOnlyRunsOnWorkersAsWritten: with
// distributed_execution = on, a report that runs on workers as written but
// whose recognized call would run on this replica (one that counts active
// days, distinct_with pairs, which rb_count_distinct counts and the planner
// cannot divide) stays as written, and runs on workers; one whose call runs
// on workers too is recognized. With distributed execution off, both are.
func TestReportRecognitionKeepsWhatOnlyRunsOnWorkersAsWritten(t *testing.T) {
	t.Parallel()
	history := audit.NewMemoryStore(1000, 1000)
	var mu sync.Mutex
	var outcomes []reportOutcome
	it := newDistE2Test(t, 2, func(o *Options) {
		o.Bitmaps, o.BitmapMinRows = true, 0
		o.Audit = audit.New(history, audit.Options{}, slog.New(slog.DiscardHandler))
		o.ReportRewrite = true
		o.reportOutcomes = func(got []reportOutcome) {
			mu.Lock()
			defer mu.Unlock()
			outcomes = append(outcomes, got...)
		}
	})
	srv := it.r.srv
	conn := it.admin
	mustExec(t, conn, "SET search_path = 'lake.public'")
	it.exec(
		"CREATE TABLE lake.public.plays AS SELECT 'p' || (i % 200) AS person_id, CASE i % 5 WHEN 0 THEN 'drama' WHEN 1 THEN 'news' WHEN 2 THEN '' WHEN 3 THEN NULL ELSE 'sport' END AS genre, "+
			"DATE '2026-01-01' + CAST(i % 40 AS INTEGER) AS day FROM range(1500) r(i)",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(context.Background())
	report := func(measures string) string {
		return `WITH AUDIENCE AS (SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = (SELECT max(ver) FROM audience_revisions WHERE audience_id = 'a1')),
GENRES AS (SELECT 'GENRE' AS dimension, P.genre AS genre, count(DISTINCT P.person_id) AS people` + measures + `
  FROM plays AS P JOIN AUDIENCE AS A ON A.person_id = P.person_id WHERE trim(P.genre) <> '' GROUP BY P.genre)
SELECT * FROM GENRES ORDER BY genre`
	}
	days := report(", count(DISTINCT CASE WHEN P.person_id IS NULL THEN NULL WHEN P.day IS NULL THEN NULL ELSE (P.person_id, P.day) END) AS active_days")
	people := report("")
	run := func(sql, mode string) (rows []string, recognized int, why string, workers int) {
		t.Helper()
		mustExec(t, conn, "SET distributed_execution = "+mode)
		mu.Lock()
		outcomes = nil
		mu.Unlock()
		rows = texts(t, conn, sql)
		mu.Lock()
		defer mu.Unlock()
		for _, o := range outcomes {
			if o.Spec != "" {
				recognized++
			} else {
				why = o.Why
			}
		}
		return rows, recognized, why, lastWorkers(t, history, sql)
	}
	for _, c := range []struct {
		name, sql string
		keep      bool // as written on workers, recognized on this replica
	}{{"active days", days, true}, {"people", people, false}} {
		local, n, why, w := run(c.sql, "off")
		if n != 1 || w != 0 {
			t.Errorf("%s, distributed execution off: %d CTEs recognized (%s), on %d workers; want 1, on none", c.name, n, why, w)
		}
		spread, n, why, w := run(c.sql, "on")
		switch {
		case c.keep && (n != 0 || !strings.Contains(why, "runs on workers")):
			t.Errorf("%s: %d CTEs recognized, why %q; want it as written", c.name, n, why)
		case !c.keep && n != 1:
			t.Errorf("%s: %d CTEs recognized (%s), want 1", c.name, n, why)
		}
		if w != 2 {
			t.Errorf("%s, distributed execution on: ran on %d workers, want 2", c.name, w)
		}
		if len(local) == 0 || !slices.Equal(local, spread) {
			t.Errorf("%s: rows on workers\n%q\non one machine\n%q", c.name, spread, local)
		}
	}
}
