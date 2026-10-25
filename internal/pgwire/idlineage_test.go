package pgwire

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestIDCountsAlongLineage counts a member column through ids where it
// reaches the count through CTEs, subqueries and a view of its table
// unchanged, with the counts the values give, and counts the values where
// the lineage breaks or its ids could show.
func TestIDCountsAlongLineage(t *testing.T) {
	t.Parallel()
	it := newIDTest(t)
	ctx := context.Background()
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.visits AS SELECT 'p' || (i % 53) AS person_id, DATE '2026-01-01' + CAST(i % 9 AS INTEGER) AS day, "+
			"CASE i % 4 WHEN 0 THEN ['cars', 'news'] WHEN 1 THEN ['news'] WHEN 2 THEN NULL ELSE ['cars', 'cars', 'golf'] END AS keywords, i AS n FROM range(700) r(i)",
		"CREATE TABLE lake.public.plain_visits AS SELECT * FROM lake.public.visits",
		"CREATE TABLE lake.public.everyone AS SELECT 'p' || i AS person_id FROM range(40) r(i)",
		"CREATE VIEW lake.public.v_visits AS SELECT * FROM lake.public.visits",
		"CREATE VIEW lake.public.v_plain AS SELECT * FROM lake.public.plain_visits",
		// Views that name their columns, by position.
		"CREATE VIEW lake.public.n_visits (who, day, keywords, n) AS SELECT * FROM lake.public.visits",
		"CREATE VIEW lake.public.n_plain (who, day, keywords, n) AS SELECT * FROM lake.public.plain_visits",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.visits ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.visits ALTER COLUMN person_id SET ID DOMAIN people",
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
	mustExec(t, it.admin, "SET search_path = 'lake.public'")
	rita := connect(t, it.r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, rita, "SET search_path = 'lake.public'")

	// check runs sql over the member table (or its view) and over a copy
	// without ids, and checks the rows agree and whether the count went
	// through ids.
	check := func(name, sql string, through bool) {
		t.Helper()
		plain := strings.NewReplacer("{visits}", "plain_visits", "{view}", "v_plain", "{named}", "n_plain").Replace(sql)
		member := strings.NewReplacer("{visits}", "visits", "{view}", "v_visits", "{named}", "n_visits").Replace(sql)
		want := rowsOf(t, it.admin, plain)
		for _, c := range []*pgx.Conn{it.admin, rita} {
			if got := rowsOf(t, c, member); got != want {
				t.Errorf("%s: rows\n%s\nwant\n%s", name, got, want)
			}
			if got := strings.Contains(last(), lineagePrefix+"t"); got != through {
				t.Errorf("%s: counted through ids %v, want %v: %s", name, got, through, last())
			}
		}
	}
	// A report's shape: people from a table joined to everyone, with a
	// window over another column, expanded into keywords through stars and
	// an unnest.
	report := `WITH BASE AS (SELECT v.person_id AS person_id, v.day AS day, v.keywords AS keywords, min(v.day) OVER () AS since
  FROM %s AS v JOIN everyone AS e ON e.person_id = v.person_id),
EXPANDED AS (SELECT b.person_id AS person_id, b.kw AS kw, b.since AS since
  FROM (SELECT b.*, unnest(b.keywords) AS kw FROM (SELECT b.*, k.i FROM BASE AS b CROSS JOIN range(2) AS k(i)) AS b) AS b)
SELECT kw, min(since) AS since, count(DISTINCT EXPANDED.person_id) AS people, count(*) AS n FROM EXPANDED GROUP BY kw ORDER BY kw`
	check("a report's CTEs", strings.Replace(report, "%s", "{visits}", 1), true)
	check("a report's CTEs over a view", strings.Replace(report, "%s", "{view}", 1), true)
	check("a star over the table", "WITH B AS (SELECT v.* FROM {visits} AS v WHERE v.n > 10) SELECT count(DISTINCT B.person_id) AS people FROM B", true)
	check("a subquery", "SELECT x.day, count(DISTINCT x.person_id) AS people FROM (SELECT v.person_id AS person_id, v.day FROM {visits} AS v) AS x GROUP BY x.day ORDER BY x.day", true)
	check("the view itself", "SELECT count(DISTINCT w.person_id) AS people FROM {view} AS w", true)
	named := strings.NewReplacer("v.person_id", "v.who", "e.person_id = v.who", "e.person_id = v.who").Replace(strings.Replace(report, "%s", "{named}", 1))
	check("a report's CTEs over a view that names its columns", named, true)

	// The lineage breaks: as written.
	check("transformed", "WITH B AS (SELECT upper(v.person_id) AS person_id FROM {visits} AS v) SELECT count(DISTINCT B.person_id) AS people FROM B", false)
	check("aggregated", "WITH B AS (SELECT v.person_id, count(*) AS c FROM {visits} AS v GROUP BY v.person_id) SELECT count(DISTINCT B.person_id) AS people FROM B", false)
	check("unioned", "WITH B AS (SELECT v.person_id FROM {visits} AS v UNION ALL SELECT e.person_id FROM everyone AS e) SELECT count(DISTINCT B.person_id) AS people FROM B", false)
	check("renamed by position", "WITH B(p) AS (SELECT v.person_id FROM {visits} AS v) SELECT count(DISTINCT B.p) AS people FROM B", false)
	check("not a member column", "WITH B AS (SELECT v.day FROM {visits} AS v) SELECT count(DISTINCT B.day) AS days FROM B", false)
	// The ids could show: a star elsewhere over a CTE on the way, or a
	// statement that names a column like the ones that carry them.
	check("a star elsewhere", "WITH B AS (SELECT v.person_id FROM {visits} AS v) SELECT (SELECT count(DISTINCT B.person_id) FROM B) AS people, (SELECT count(*) FROM (SELECT * FROM B)) AS n", false)
	check("a whole row elsewhere", "WITH B AS (SELECT v.person_id FROM {visits} AS v) SELECT (SELECT count(DISTINCT B.person_id) FROM B) AS people, (SELECT max(B) FROM B) AS r", false)
	check("a lineage column's name", `WITH B AS (SELECT v.person_id, 1 AS "_igloo_lineage_1" FROM {visits} AS v) SELECT count(DISTINCT B.person_id) AS people, max(B."_igloo_lineage_1") AS one FROM B`, false)

	// A session that turns counting through ids off: as written.
	mustExec(t, it.admin, "SET igloo_id_counts = off")
	if v := texts(t, it.admin, "SHOW igloo_id_counts"); len(v) != 1 || v[0] != "off" {
		t.Errorf("SHOW igloo_id_counts: %q", v)
	}
	_ = rowsOf(t, it.admin, strings.Replace(report, "%s", "visits", 1))
	if strings.Contains(last(), lineagePrefix+"t") {
		t.Errorf("counted through ids with igloo_id_counts off: %s", last())
	}
	mustExec(t, it.admin, "RESET igloo_id_counts")

	// A masking policy on the member column: as written.
	it.exec("CREATE MASKING POLICY hide AS (v VARCHAR) RETURNS VARCHAR -> '***'",
		"ALTER TABLE lake.public.visits ALTER COLUMN person_id SET MASKING POLICY hide")
	_ = rowsOf(t, it.admin, strings.Replace(report, "%s", "visits", 1))
	if strings.Contains(last(), lineagePrefix+"t") {
		t.Errorf("counted through ids under a masking policy: %s", last())
	}
}
