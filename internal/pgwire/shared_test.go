package pgwire

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// sharedVolatile returns the volatile names of db as shareSubqueries reads
// them.
func sharedVolatile(t *testing.T, db *sql.DB) func() (map[string]bool, error) {
	return func() (map[string]bool, error) {
		rows, err := db.Query(volatileFunctionsQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				return nil, err
			}
			out = append(out, s)
		}
		return volatileSet(out), rows.Err()
	}
}

// planCTEs returns the names of the common table expressions DuckDB's plan
// of q materializes, and how many times it scans t.
func planCTEs(t *testing.T, db *sql.DB, q string) (ctes []string, scans int) {
	t.Helper()
	rows, err := db.Query("EXPLAIN (FORMAT json) " + q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		plan = v
	}
	for _, f := range strings.Split(plan, "\n") {
		f = strings.TrimSpace(f)
		if name, ok := strings.CutPrefix(f, `"CTE Name": "`); ok {
			ctes = append(ctes, strings.TrimSuffix(strings.TrimSuffix(name, ","), `"`))
		}
		if table, ok := strings.CutPrefix(strings.TrimSuffix(f, ","), `"Table": "`); ok && (table == `t"` || strings.HasSuffix(table, `.t"`)) {
			scans++
		}
	}
	return ctes, scans
}

func TestSharedSubqueries(t *testing.T) {
	db, parse := wholeCountDB(t)
	vol := sharedVolatile(t, db)
	if _, err := db.Exec("CREATE MACRO noisy(a) AS a + random()"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE MACRO noisier(a) AS noisy(a) * 2"); err != nil {
		t.Fatal(err)
	}
	report := "(SELECT x, count(*) AS n, sum(id) AS total FROM t WHERE id > 10 GROUP BY x ORDER BY x)"
	// The shape generated reports have: a top and a bottom selection of
	// one report, each over a copy of it.
	topBottom := "SELECT * FROM (SELECT * FROM " + report + " AS r WHERE n > 6 ORDER BY total DESC LIMIT 3) AS a " +
		"UNION SELECT * FROM (SELECT * FROM " + report + " AS r WHERE n <= 6 ORDER BY total LIMIT 2) AS b ORDER BY total"
	shared := []string{
		topBottom,
		// Copies written differently: case, white space, comments.
		"SELECT a.x, a.n, b.n, c.total, d.total FROM (SELECT x, count(*) AS n FROM t GROUP BY x) AS a " +
			"JOIN (select x,   COUNT(*) as n FROM t /* the same */ group by x) AS b ON a.x = b.x " +
			"JOIN (SELECT x, sum(id) AS total FROM t GROUP BY x) AS c ON c.x = a.x " +
			"JOIN (SELECT x, sum(id) AS total FROM t GROUP BY x) AS d ON d.x = c.x ORDER BY a.x",
		// Copies with common table expressions of their own, in a statement
		// with some of its own.
		"WITH k AS (SELECT 1 AS one) SELECT * FROM (WITH c AS (SELECT x, id FROM t) SELECT x, max(id) AS m FROM c GROUP BY x) AS p " +
			"JOIN (WITH c AS (SELECT x, id FROM t) SELECT x, max(id) AS m FROM c GROUP BY x) AS q USING (x), k ORDER BY x",
		// Copies inside the copies of a bigger one.
		"SELECT * FROM (SELECT p.x, p.n + q.n AS n FROM (SELECT x, count(*) AS n FROM u GROUP BY x) AS p JOIN " +
			"(SELECT x, count(*) AS n FROM u GROUP BY x) AS q USING (x) ORDER BY x) AS a UNION ALL SELECT * FROM " +
			"(SELECT p.x, p.n + q.n AS n FROM (SELECT x, count(*) AS n FROM u GROUP BY x) AS p JOIN " +
			"(SELECT x, count(*) AS n FROM u GROUP BY x) AS q USING (x) ORDER BY x) AS b ORDER BY x, n",
		// Grouped instead of ordered.
		"SELECT a.x, sum(a.n + b.n) FROM (SELECT x, count(*) AS n FROM t GROUP BY x) AS a " +
			"JOIN (SELECT x, count(*) AS n FROM t GROUP BY x) AS b USING (x) GROUP BY a.x",
		// Repeated in a common table expression and in the query.
		"WITH c AS (SELECT * FROM (SELECT x, min(id) AS m FROM t GROUP BY x) AS a ORDER BY x) " +
			"SELECT * FROM c JOIN (SELECT x, min(id) AS m FROM t GROUP BY x) AS b USING (x, m) ORDER BY x",
		"EXPLAIN ANALYZE " + topBottom,
	}
	for _, q := range shared {
		got, err := shareRepeated(q, parse, vol)
		if err != nil || got == "" || !strings.Contains(got, sharedPrefix+"1 AS MATERIALIZED") {
			t.Errorf("not shared: %s\n  -> %q %v", q, got, err)
			continue
		}
		t.Logf("%s\n  -> %s", q, got)
		if strings.HasPrefix(q, "EXPLAIN") {
			if !strings.HasPrefix(got, "EXPLAIN ANALYZE WITH ") {
				t.Errorf("EXPLAIN: %s", got)
			}
			continue
		}
		wantCols, wantRows := resultOf(t, db, q)
		gotCols, gotRows := resultOf(t, db, got)
		if !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
			t.Errorf("%s\n  -> %s\n got %q %q\nwant %q %q", q, got, gotCols, gotRows, wantCols, wantRows)
		}
	}
	// DuckDB computes a report that has a common table expression of its
	// own once per copy; shared, it is computed once.
	withCTE := strings.ReplaceAll(topBottom, report, "(WITH c AS (SELECT x, id FROM t WHERE id > 10) "+
		"SELECT c.x, count(*) AS n, sum(c.id) AS total FROM c JOIN c AS d USING (id) GROUP BY c.x ORDER BY c.x)")
	if _, scans := planCTEs(t, db, withCTE); scans != 2 {
		t.Errorf("as written: %d scans of t, want 2 (does DuckDB share them itself now?)", scans)
	}
	got, _ := shareRepeated(withCTE, parse, vol)
	if ctes, scans := planCTEs(t, db, got); !slices.Contains(ctes, sharedPrefix+"1") || scans != 1 {
		t.Errorf("shared plan: CTEs %q, %d scans of t", ctes, scans)
	}
	wantCols, wantRows := resultOf(t, db, withCTE)
	if gotCols, gotRows := resultOf(t, db, got); !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
		t.Errorf("%s\n got %q %q\nwant %q %q", got, gotCols, gotRows, wantCols, wantRows)
	}

	kept := []string{
		// Nothing repeats.
		"SELECT * FROM (SELECT x FROM t) a JOIN (SELECT x FROM u) b USING (x) ORDER BY x",
		// Volatile, directly or through macros, or sampled.
		"SELECT * FROM (SELECT x, random() AS r FROM t) a JOIN (SELECT x, random() AS r FROM t) b USING (x) ORDER BY x",
		"SELECT * FROM (SELECT x, noisier(x) AS r FROM t) a JOIN (SELECT x, noisier(x) AS r FROM t) b USING (x) ORDER BY x",
		"SELECT * FROM (SELECT x FROM t USING SAMPLE 10) a JOIN (SELECT x FROM t USING SAMPLE 10) b USING (x) ORDER BY x",
		// Names a common table expression of the statement's.
		"WITH c AS (SELECT * FROM t WHERE x < 3) SELECT * FROM (SELECT x FROM c) a JOIN (SELECT x FROM c) b USING (x) ORDER BY x",
		// Or one only some copies see.
		"SELECT * FROM (WITH t AS (SELECT 1 AS x) SELECT * FROM (SELECT x FROM t) a) a JOIN (SELECT x FROM t) b USING (x) ORDER BY x",
		// The order of the copies' rows shows.
		"SELECT * FROM (SELECT x FROM t ORDER BY x) a UNION ALL SELECT * FROM (SELECT x FROM t ORDER BY x) b",
		"SELECT * FROM (SELECT x FROM t ORDER BY x) a UNION ALL SELECT * FROM (SELECT x FROM t ORDER BY x) b LIMIT 5",
		"SELECT row_number() OVER () AS rn, * FROM (SELECT x FROM t ORDER BY x) a JOIN (SELECT x FROM t ORDER BY x) b USING (x) ORDER BY rn",
		"SELECT * FROM (SELECT x FROM t ORDER BY x) a UNION ALL SELECT x FROM u GROUP BY x UNION ALL SELECT * FROM (SELECT x FROM t ORDER BY x) b",
		// Ordered again only further out.
		"SELECT x, sum(n) FROM (SELECT * FROM (SELECT x, count(*) AS n FROM t GROUP BY x) AS a UNION ALL " +
			"SELECT * FROM (SELECT x, count(*) AS n FROM t GROUP BY x) AS b) GROUP BY x",
		// Not derived tables.
		"SELECT (SELECT max(x) FROM t), (SELECT max(x) FROM t)",
		"SELECT * FROM t WHERE x IN (SELECT x FROM u) OR id IN (SELECT x FROM u) ORDER BY id",
		"SELECT * FROM t JOIN LATERAL (SELECT u.x FROM u WHERE u.id = t.id) a ON true " +
			"JOIN LATERAL (SELECT u.x FROM u WHERE u.id = t.id) b ON true ORDER BY t.id",
		// ? parameters, numbered as they come.
		"SELECT * FROM (SELECT x FROM t WHERE id > ?) a JOIN (SELECT x FROM t WHERE id > ?) b USING (x) ORDER BY x",
		// Not a query.
		"INSERT INTO u SELECT * FROM (SELECT id, x, 'v' FROM t) a UNION ALL SELECT * FROM (SELECT id, x, 'v' FROM t) b ORDER BY 1",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) " +
			"SELECT * FROM (SELECT n FROM r) a JOIN (SELECT n FROM r) b USING (n) ORDER BY n",
	}
	for _, q := range kept {
		if got, err := shareRepeated(q, parse, vol); got != "" || err != nil {
			t.Errorf("shared: %s\n  -> %s %v", q, got, err)
		}
	}

	// A copy that reads the query around it is shared, and DuckDB cannot
	// prepare that: Igloo then runs the statement as written.
	correlated := "SELECT * FROM t JOIN (SELECT max(u.id) AS m FROM u WHERE u.x = t.x) a ON true " +
		"JOIN (SELECT max(u.id) AS m FROM u WHERE u.x = t.x) b ON true ORDER BY t.id"
	resultOf(t, db, correlated)
	got, err := shareRepeated(correlated, parse, vol)
	if got == "" || err != nil {
		t.Fatalf("correlated: %q %v", got, err)
	}
	if _, err := db.Prepare(got); err == nil {
		t.Errorf("DuckDB prepares a shared copy that reads the query around it: %s", got)
	}
}

// TestSharedSubqueriesThroughSessions runs a statement that repeats a
// subquery as an admin and as users whose reads policies change, in both
// dialects: the subquery is computed once, and policies apply to it.
func TestSharedSubqueriesThroughSessions(t *testing.T) {
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.crm TO acme_team")
	report := "(SELECT client, id, email, CAST(sum(spend) AS INTEGER) AS spend FROM lake.crm.contacts GROUP BY client, id, email)"
	q := "SELECT * FROM (SELECT * FROM " + report + " AS r WHERE spend > 15 ORDER BY spend DESC LIMIT 2) AS a " +
		"UNION ALL SELECT * FROM (SELECT * FROM " + report + " AS r WHERE spend <= 15 ORDER BY spend LIMIT 1) AS b ORDER BY spend"
	// E-mail addresses are masked for admins too.
	all := []string{"acme|1|***|10", "globex|3|***|30", "globex|4|***|40"}
	expectRows(t, cs.admin, q, all...)
	plan := strings.Join(texts(t, cs.admin, "EXPLAIN (FORMAT json) "+q), "\n")
	if strings.Count(plan, `"CTE Name": "`+sharedPrefix+"1") != 1 {
		t.Errorf("not shared:\n%s", plan)
	}
	mustExec(t, cs.admin, "SET sql_dialect = 'snowflake'")
	expectRows(t, cs.admin, strings.ToUpper(q), all...)
	mustExec(t, cs.admin, "SET sql_dialect = 'duckdb'")
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ali := cs.user(t, "ali_"+name, auth.RoleNone, mode)
			mustExec(t, cs.admin, "GRANT acme_team TO ali_"+name)
			for _, dialect := range []string{"duckdb", "snowflake"} {
				mustExec(t, ali, "SET sql_dialect = '"+dialect+"'")
				// Only acme's rows.
				expectRows(t, ali, q, "acme|1|***|10", "acme|2|***|20")
			}
		})
	}
	// A copy that reads the query around it runs as written.
	correlated := "SELECT c.id, a.n, b.n FROM lake.crm.contacts c " +
		"JOIN (SELECT count(*) AS n FROM lake.public.campaigns p WHERE p.client = c.client) a ON true " +
		"JOIN (SELECT count(*) AS n FROM lake.public.campaigns p WHERE p.client = c.client) b ON true ORDER BY c.id"
	expectRows(t, cs.admin, correlated, "1|1|1", "2|1|1", "3|1|1", "4|1|1")
	// History keeps the statement as sent.
	var n int
	if err := cs.admin.QueryRow(context.Background(), "SELECT count(*) FROM igloo.query_history WHERE query_text = $1", q).Scan(&n); err != nil || n == 0 {
		t.Errorf("history: %d %v", n, err)
	}
	if err := cs.admin.QueryRow(context.Background(), "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%"+sharedPrefix+"%'").Scan(&n); err != nil || n != 0 {
		t.Errorf("history has the rewritten statement: %d %v", n, err)
	}
}

// TestSharedSubqueriesDistributed runs a statement that repeats a
// subquery with distributed execution on: workers compute the shared
// subquery, and the result is the one-machine result.
func TestSharedSubqueriesDistributed(t *testing.T) {
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	report := "(SELECT amount, count(*) AS n, CAST(sum(id) AS BIGINT) AS s FROM lake.public.orders WHERE id > 10 GROUP BY amount ORDER BY amount)"
	// The shape generated reports have: the set operation in a FROM.
	q := "SELECT * FROM (SELECT * FROM (SELECT * FROM " + report + " AS r WHERE n > 280 ORDER BY s DESC LIMIT 3) AS a " +
		"UNION ALL SELECT * FROM (SELECT * FROM " + report + " AS r WHERE n <= 285 ORDER BY s LIMIT 2) AS b) AS u ORDER BY s, amount"
	mustExec(t, conn, "SET distributed_execution = off")
	local := texts(t, conn, q)
	if len(local) != 5 {
		t.Fatalf("local result: %q", local)
	}
	mustExec(t, conn, "SET distributed_execution = on")
	if got := texts(t, conn, q); !slices.Equal(got, local) {
		t.Errorf("distributed result differs:\n%q\nlocal:\n%q", got, local)
	}
	// The shared subquery, a common table expression now, is what the
	// workers compute.
	if n := lastWorkers(t, ds.history, q); n != 2 {
		t.Errorf("history shows %d workers, want 2", n)
	}
	plan := strings.Join(texts(t, conn, "EXPLAIN (FORMAT json) "+q), "\n")
	if !strings.Contains(plan, sharedPrefix+"1") {
		t.Errorf("not shared:\n%s", plan)
	}
}
