package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// estimatedScanRows is DuckDB's estimate of the rows the one scan of query's
// plan returns.
func estimatedScanRows(t *testing.T, conn *pgx.Conn, query string) string {
	t.Helper()
	var key, plan string
	if err := conn.QueryRow(context.Background(), "EXPLAIN (FORMAT JSON) "+query).Scan(&key, &plan); err != nil {
		t.Fatal(err)
	}
	var doc any
	if err := json.Unmarshal([]byte(plan), &doc); err != nil {
		t.Fatal(err)
	}
	found := ""
	walkTree(doc, func(m map[string]any) {
		if extra := asMap(m["extra_info"]); extra != nil && strings.HasSuffix(str(m["name"]), "_SCAN") {
			found = str(extra["Estimated Cardinality"])
		}
	})
	if found == "" {
		t.Fatalf("no scan in the %s: %.1500s", key, strings.Join(strings.Fields(plan), " "))
	}
	return found
}

// TestDistinctCountsReachThePlanner checks that the distinct counts Igloo
// hands to DuckDB (igloo_set_distinct_counts, in the rb extension) are what
// DuckDB estimates an equality filter on an Iceberg table by, rather than a
// fifth of the table.
func TestDistinctCountsReachThePlanner(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	admin := vc.replica(t).admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.members AS SELECT i % 100 AS audience, i AS person FROM range(100000) r(i)")
	const q = "SELECT person FROM lake.public.members WHERE audience = 7"
	if got := estimatedScanRows(t, admin, q); got != "20000" {
		t.Fatalf("estimate without counts: %s rows, want 20000", got)
	}
	mustExec(t, admin, "SELECT system.main.igloo_set_distinct_counts('lake.public.members' || chr(9) || 'audience' || chr(9) || '100')")
	if got := estimatedScanRows(t, admin, q); got != "1000" {
		t.Errorf("estimate with counts: %s rows, want 1000", got)
	}
	if n := count(t, admin, "SELECT count(*) FROM ("+q+")"); n != 1000 {
		t.Errorf("%d rows", n)
	}
	// Another table's scans are as before, and so are this one's once the
	// counts are replaced.
	mustExec(t, admin, "SELECT system.main.igloo_set_distinct_counts('lake.public.other' || chr(9) || 'x' || chr(9) || '5')")
	if got := estimatedScanRows(t, admin, q); got != "20000" {
		t.Errorf("estimate once the counts are gone: %s rows, want 20000", got)
	}
}

// TestAnalyzeKeepsDistinctCounts checks that ANALYZE counts a table's
// distinct values and keeps them in the table, where the planner of this
// replica and of one that starts later finds them; that ANALYZE of named
// columns keeps the others' counts; and that only admins may ANALYZE.
func TestAnalyzeKeepsDistinctCounts(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	first := vc.replica(t)
	admin := first.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.visits AS SELECT i % 50 AS site, i % 1000 AS person, 'x' || i AS note FROM range(100000) r(i)")
	const bySite = "SELECT person FROM lake.public.visits WHERE site = 7"
	const byPerson = "SELECT site FROM lake.public.visits WHERE person = 7"
	// approx_count_distinct is off by up to some 25% on so few values.
	near := func(got string, want int) bool {
		var n int
		_, err := fmt.Sscan(got, &n)
		return err == nil && n >= want*7/10 && n <= want*13/10
	}
	if got := estimatedScanRows(t, admin, bySite); got != "20000" {
		t.Fatalf("estimate before ANALYZE: %s rows, want 20000", got)
	}
	mustExec(t, admin, "ANALYZE lake.public.visits (site)")
	count(t, admin, "SELECT count(*) FROM lake.public.visits")
	if got := estimatedScanRows(t, admin, bySite); !near(got, 2000) {
		t.Errorf("estimate after ANALYZE (site): %s rows, want about 2000", got)
	}
	if got := estimatedScanRows(t, admin, byPerson); got != "20000" {
		t.Errorf("estimate of a column not analyzed: %s rows, want 20000", got)
	}
	mustExec(t, admin, "ANALYZE lake.public.visits (person)")
	later := vc.replica(t).admin(t, pgx.QueryExecModeSimpleProtocol)
	count(t, later, "SELECT count(*) FROM lake.public.visits")
	if got := estimatedScanRows(t, later, bySite); !near(got, 2000) {
		t.Errorf("a later replica's estimate by site: %s rows, want about 2000", got)
	}
	if got := estimatedScanRows(t, later, byPerson); !near(got, 100) {
		t.Errorf("a later replica's estimate by person: %s rows, want about 100", got)
	}

	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	rita := connect(t, first.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	if _, err := rita.Exec(context.Background(), "ANALYZE lake.public.visits"); err == nil || !strings.Contains(err.Error(), "only admins") {
		t.Errorf("a reader's ANALYZE: %v", err)
	}
}

// countsEngine is recordingEngine with distinct counts to hand to DuckDB.
type countsEngine struct{ recordingEngine }

func (e *countsEngine) DistinctCounts() (uint64, map[string]map[string]int64) {
	return 1, map[string]map[string]int64{"lake.public.members": {"audience": 100}}
}

// TestDistinctCountsAfterTheCatalogLock checks that a session handing
// DuckDB the distinct counts takes the catalog lock before countsMu, as a
// session preparing a statement does (prepareChecked holds the lock's
// exclusive side when it hands them over): one that held countsMu while it
// waited for the lock as DuckDB bound the call deadlocked with such a
// session.
func TestDistinctCountsAfterTheCatalogLock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	eng, err := engine.Open(ctx, config.Config{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close() //nolint:errcheck
	conn, err := eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	e := &countsEngine{}
	s := &Server{engine: e, catalog: "lake", log: slog.New(slog.DiscardHandler)}
	sess := &session{conn: conn}
	// Another session is handing them over.
	s.countsMu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.registerDistinctCounts(ctx, sess)
	}()
	shared := false
	for deadline := time.Now().Add(5 * time.Second); !shared && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if e.mu.TryLock() {
			e.mu.Unlock()
		} else {
			shared = true
		}
	}
	s.countsMu.Unlock()
	<-done
	if !shared {
		t.Fatal("it waited for countsMu before it took the catalog lock")
	}
	if !slices.Equal(e.events, []string{"rlock", "runlock"}) {
		t.Errorf("catalog lock events %v, want rlock and runlock", e.events)
	}
	if s.countsGen.Load() != 1 {
		t.Errorf("counts generation %d after handing them over, want 1", s.countsGen.Load())
	}
}
