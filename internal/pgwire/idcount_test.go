package pgwire

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestIDCounts counts member columns through ids where it may, at a
// snapshot that has every row's, with the counts the values give, and
// counts the values otherwise.
func TestIDCounts(t *testing.T) {
	t.Parallel()
	it := newIDTest(t)
	ctx := context.Background()
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.segs AS SELECT 'p' || (i % 37) AS person_id, 'seg' || (i % 3) AS segment, i AS n FROM range(500) r(i)",
		"CREATE TABLE lake.public.plain AS SELECT * FROM lake.public.segs",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.segs ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.segs ALTER COLUMN person_id SET ID DOMAIN people",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
	)
	srv := it.r.srv
	srv.MapIDDomains(ctx)
	var mu sync.Mutex
	var seen []string
	srv.rewriteHook = func(q string) string {
		mu.Lock()
		seen = append(seen, q)
		mu.Unlock()
		return q
	}
	last := func() string {
		mu.Lock()
		defer mu.Unlock()
		if len(seen) == 0 {
			return ""
		}
		return seen[len(seen)-1]
	}
	through := regexp.MustCompile(`(?i)count\(DISTINCT CAST\("?s"?\."_igloo_id_person_id" AS UINTEGER\)\).* AT \(VERSION => \d+\)`)
	rita := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)

	query := "SELECT s.segment, count(DISTINCT s.person_id) AS n FROM lake.public.segs AS s GROUP BY s.segment ORDER BY s.segment"
	want := rowsOf(t, it.admin, strings.ReplaceAll(query, "lake.public.segs", "lake.public.plain"))
	for _, c := range []*pgx.Conn{it.admin, rita} {
		if got := rowsOf(t, c, query); got != want {
			t.Errorf("counts through ids:\n%s\nwant\n%s", got, want)
		}
		if !through.MatchString(last()) {
			t.Errorf("not counted through ids: %s", last())
		}
	}
	// The result has the statement's columns, not the column of ids.
	rows, err := it.admin.Query(ctx, "SELECT count(DISTINCT person_id) AS people FROM lake.public.segs")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	rows.Close()
	if strings.Join(names, ",") != "people" || !strings.Contains(last(), `CAST("segs"."_igloo_id_person_id" AS UINTEGER)`) {
		t.Errorf("columns %v of %s", names, last())
	}

	// Snowflake's COUNT(DISTINCT a, b) beside COUNT(DISTINCT a): the pair
	// counts' rewrite still applies, over the ids.
	sf := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	// (The pair-count rewrite takes a SELECT without ORDER BY of its own.)
	pairs := "SELECT * FROM (SELECT s.segment AS segment, COUNT(DISTINCT s.person_id) AS people, COUNT(DISTINCT s.person_id, s.n) AS person_ns " +
		"FROM lake.public.segs AS s GROUP BY s.segment) ORDER BY segment"
	wantPairs := rowsOf(t, sf, strings.ReplaceAll(pairs, "lake.public.segs", "lake.public.plain"))
	if got := rowsOf(t, sf, pairs); got != wantPairs {
		t.Errorf("pair counts through ids:\n%s\nwant\n%s", got, wantPairs)
	}
	if q := last(); !strings.Contains(q, pairCountPrefix) || !strings.Contains(q, "_igloo_id_person_id") {
		t.Errorf("pair counts not rewritten over ids: %s", q)
	}

	// Left as written where it could show the ids, or where they stand for
	// what a policy hides.
	for _, q := range []string{
		"SELECT *, count(DISTINCT s.person_id) OVER () AS n FROM lake.public.segs AS s",
		"SELECT s, count(DISTINCT s.person_id) OVER () AS n FROM lake.public.segs AS s",
		"SELECT s.*, count(DISTINCT s.person_id) OVER () AS n FROM lake.public.segs AS s",
		"SELECT count(DISTINCT s.person_id) FROM lake.public.segs AS s AT (VERSION => 1)",
	} {
		_, _ = it.admin.Exec(ctx, q)
		if strings.Contains(last(), "_igloo_id_") {
			t.Errorf("%s: counted through ids: %s", q, last())
		}
	}
	it.exec("CREATE MASKING POLICY hide AS (v VARCHAR) RETURNS VARCHAR -> '***'",
		"ALTER TABLE lake.public.segs ALTER COLUMN person_id SET MASKING POLICY hide")
	_ = rowsOf(t, it.admin, query)
	if strings.Contains(last(), "_igloo_id_") {
		t.Errorf("counted through ids under a masking policy: %s", last())
	}
	it.exec("ALTER TABLE lake.public.segs ALTER COLUMN person_id UNSET MASKING POLICY")

	// A prepared statement reads the new snapshot after a write, and counts
	// the values while it lacks ids.
	prepared := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeCacheStatement)
	if got := rowsOf(t, prepared, query); got != want {
		t.Errorf("prepared:\n%s\nwant\n%s", got, want)
	}
	it.exec("INSERT INTO lake.public.segs VALUES ('new1', 'seg0', 1000)", "INSERT INTO lake.public.plain VALUES ('new1', 'seg0', 1000)")
	want = rowsOf(t, it.admin, strings.ReplaceAll(query, "lake.public.segs", "lake.public.plain"))
	if got := rowsOf(t, prepared, query); got != want {
		t.Errorf("prepared, after a write:\n%s\nwant\n%s", got, want)
	}
	if !through.MatchString(last()) {
		t.Errorf("not counted through ids after a write: %s", last())
	}
	it.internal("INSERT INTO lake.public.segs (person_id, segment, n) VALUES ('raw1', 'seg1', 1001)")
	it.exec("INSERT INTO lake.public.plain VALUES ('raw1', 'seg1', 1001)")
	want = rowsOf(t, it.admin, strings.ReplaceAll(query, "lake.public.segs", "lake.public.plain"))
	if got := rowsOf(t, prepared, query); got != want {
		t.Errorf("prepared, with a row without its id:\n%s\nwant\n%s", got, want)
	}
	if strings.Contains(last(), "_igloo_id_") {
		t.Errorf("counted through ids with a row without its id: %s", last())
	}
	srv.MapIDDomains(ctx)
	if got := rowsOf(t, prepared, query); got != want {
		t.Errorf("prepared, mapped again:\n%s\nwant\n%s", got, want)
	}
	if !through.MatchString(last()) {
		t.Errorf("not counted through ids once mapped again: %s", last())
	}

	// No row access policy for anyone.
	it.exec("CREATE ROW ACCESS POLICY all_rows AS (seg VARCHAR) RETURNS BOOLEAN -> true",
		"ALTER TABLE lake.public.segs ADD ROW ACCESS POLICY all_rows ON (segment)")
	_ = rowsOf(t, it.admin, query)
	if strings.Contains(last(), "_igloo_id_") {
		t.Errorf("counted through ids under a row access policy: %s", last())
	}
}
