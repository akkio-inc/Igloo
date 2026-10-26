package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
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
