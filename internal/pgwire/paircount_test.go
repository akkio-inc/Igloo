package pgwire

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// pairCountDB is a DuckDB database with the tables the pair-count tests
// read, and a parse function as Igloo's. p has NULLs in its key k and in a
// and b, each row twice (so pairs repeat), a group whose a is always NULL
// and one whose b is; q joins it.
func pairCountDB(t *testing.T) (*sql.DB, func(string) (any, error)) {
	t.Helper()
	db, parse := wholeCountDB(t)
	for _, s := range []string{
		`CREATE TABLE p AS
		 SELECT i AS id, CASE WHEN i % 11 = 0 THEN NULL ELSE 'k' || (i % 5) END AS k,
		        CASE WHEN i % 7 = 0 THEN NULL ELSE i % 13 END AS a,
		        CASE WHEN i % 9 = 0 THEN NULL ELSE DATE '2026-01-01' + CAST(i % 4 AS INTEGER) END AS b,
		        CAST(i * 1.5 AS DECIMAL(10, 2)) AS v, 'row ' || i AS s
		 FROM range(1, 301) r(i), range(2) d(copy)`,
		`INSERT INTO p VALUES (1001, 'only a null', NULL, DATE '2026-02-01', 1.00, 'x'),
		                     (1002, 'only a null', NULL, DATE '2026-02-02', 2.00, 'y'),
		                     (1003, 'only b null', 5, NULL, 3.00, 'z')`,
		"CREATE TABLE q AS SELECT i AS id, i % 3 AS x, CASE WHEN i % 4 = 0 THEN NULL ELSE i % 6 END AS y FROM range(1, 201) r(i)",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return db, parse
}

// typedResultOf runs a query and returns its columns' names and types and
// its rows, sorted, as text.
func typedResultOf(t *testing.T, db *sql.DB, q string) ([]string, []string) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	types, _ := rows.ColumnTypes()
	var cols []string
	for _, c := range types {
		cols = append(cols, c.Name()+" "+c.DatabaseTypeName())
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(vals...))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	slices.Sort(out)
	return cols, out
}

func TestPairCounts(t *testing.T) {
	db, parse := pairCountDB(t)
	pair := "count(DISTINCT CASE WHEN (a) IS NOT NULL AND (b) IS NOT NULL THEN (a, b) END)"
	rewritten := []string{
		// NULLs in the key, in a and in b; repeated pairs; groups whose
		// pairs are all NULL count 0.
		"SELECT k AS k, count(DISTINCT a) AS people, " + pair + " AS pairs FROM p GROUP BY k",
		// Every aggregate the rewrite knows, a constant and a ratio, as the
		// translation of a report has them.
		"SELECT 'X' AS c, CAST(NULL AS DOUBLE) AS d, k AS k, min(v) AS lo, max(s) AS hi, count(*) AS n, count(b) AS nb, count(DISTINCT p.a) AS people, " +
			"count(DISTINCT CASE WHEN p.a IS NOT NULL AND p.b IS NOT NULL THEN (p.a, p.b) END) / NULLIF(count(DISTINCT p.a), 0) AS per " +
			"FROM p WHERE id > 20 GROUP BY k",
		// A key expression, an alias named like a column, the WHEN in
		// another order than the tuple, a count of the other value.
		"SELECT upper(k) AS a, count(DISTINCT CASE WHEN b IS NOT NULL AND a IS NOT NULL THEN (a, b) END) AS pairs, count(DISTINCT b) AS days " +
			"FROM p GROUP BY upper(k)",
		"SELECT k || '!' AS b, " + pair + " AS a FROM p GROUP BY k || '!'",
		// Two keys, one NULL in places, over a join.
		"SELECT p.k AS k, q.x AS x, count(DISTINCT CASE WHEN p.a IS NOT NULL AND q.y IS NOT NULL THEN (p.a, q.y) END) AS pairs, " +
			"coalesce(max(q.y), -1) AS top FROM p JOIN q ON q.id = p.id GROUP BY p.k, q.x",
		// In a common table expression, beside a set operation.
		"WITH g AS (SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY k) SELECT * FROM g UNION ALL SELECT 'total', count(*) FROM p",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY k UNION ALL SELECT s AS k, " + pair + " AS pairs FROM p GROUP BY s",
		// No rows at all.
		"SELECT k AS k, " + pair + " AS pairs, count(*) AS n FROM p WHERE id < 0 GROUP BY k",
		// Three values.
		"SELECT k AS k, count(DISTINCT CASE WHEN a IS NOT NULL AND b IS NOT NULL AND s IS NOT NULL THEN (a, b, s) END) AS triples FROM p GROUP BY k",
		// Comments.
		"SELECT k AS k, -- the key\n" + pair + " AS pairs FROM p -- the rows\nWHERE id > 3 -- some\nGROUP BY k -- by key\n",
	}
	kept := []string{
		"SELECT k AS k, count(DISTINCT a) AS people FROM p GROUP BY k",
		"SELECT k, " + pair + " AS pairs FROM p GROUP BY k",
		"SELECT k AS k, sum(v) AS total, " + pair + " AS pairs FROM p GROUP BY k",
		"SELECT k AS k, avg(a) AS mean, " + pair + " AS pairs FROM p GROUP BY k",
		"SELECT k AS k, count(DISTINCT s) AS names, " + pair + " AS pairs FROM p GROUP BY k",
		"SELECT k AS k, " + pair + " AS pairs, count(DISTINCT CASE WHEN a IS NOT NULL AND s IS NOT NULL THEN (a, s) END) AS other FROM p GROUP BY k",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY k HAVING count(*) > 3",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY k ORDER BY k",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY k LIMIT 2",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY ALL",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY 1",
		"SELECT k AS k, " + pair + " AS pairs FROM p GROUP BY ROLLUP (k)",
		"SELECT k AS k, " + pair + " FILTER (WHERE id > 3) AS pairs FROM p GROUP BY k",
		"SELECT k AS k, " + pair + " + id AS pairs FROM p GROUP BY k, id",
		"SELECT k AS k, " + pair + " AS pairs, (SELECT max(x) FROM q) AS m FROM p GROUP BY k",
		"SELECT k AS k, count(DISTINCT CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN (a, b) ELSE (0, b) END) AS pairs FROM p GROUP BY k",
		"SELECT k AS k, count(DISTINCT CASE WHEN a + 1 IS NOT NULL AND b IS NOT NULL THEN (a + 1, b) END) AS pairs FROM p GROUP BY k",
		"SELECT DISTINCT k AS k, " + pair + " AS pairs FROM p GROUP BY k",
		"SELECT k AS k, " + pair + " AS pairs, row_number() OVER () AS rn FROM p GROUP BY k",
		"SELECT (SELECT " + pair + " FROM p) AS z",
		"SELECT k AS k, " + pair + " AS pairs FROM p WHERE id > ? GROUP BY k",
		"SELECT k AS k, " + pair + " AS pairs FROM p",
	}
	for _, q := range rewritten {
		got := pairCounts(q, parse)
		if got == q || strings.Contains(strings.ToUpper(maskCode(got)), "DISTINCT CASE") {
			t.Errorf("not rewritten: %s\n  -> %s", q, got)
			continue
		}
		t.Logf("%s\n  -> %s", q, got)
		wantCols, wantRows := typedResultOf(t, db, q)
		gotCols, gotRows := typedResultOf(t, db, got)
		if !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
			t.Errorf("%s\n  -> %s\n got %q %q\nwant %q %q", q, got, gotCols, gotRows, wantCols, wantRows)
		}
	}
	for _, q := range kept {
		if got := pairCounts(q, parse); got != q {
			t.Errorf("rewritten: %s\n  -> %s", q, got)
		}
	}
	// Groups whose pairs are all NULL are there, with a count of 0.
	q := "SELECT k AS k, " + pair + " AS pairs, count(DISTINCT a) AS people FROM p WHERE k LIKE 'only%' GROUP BY k ORDER BY k"
	got := pairCounts(strings.TrimSuffix(q, " ORDER BY k"), parse)
	rows, err := db.Query("SELECT k, pairs, people FROM (" + got + ") ORDER BY k")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var seen []string
	for rows.Next() {
		var k string
		var pairs, people int64
		if err := rows.Scan(&k, &pairs, &people); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, fmt.Sprintf("%s|%d|%d", k, pairs, people))
	}
	if want := []string{"only a null|0|0", "only b null|0|1"}; !slices.Equal(seen, want) {
		t.Errorf("all-NULL groups: %q, want %q\n%s", seen, want, got)
	}
}

// TestPairCountsThroughSessions runs COUNT(DISTINCT a, b) beside COUNT(DISTINCT a) in both dialects; the counts
// must be the distinct pairs without a NULL.
func TestPairCountsThroughSessions(t *testing.T) {
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, "CREATE TABLE lake.public.visits AS SELECT i AS id, 'site' || (i % 5) AS site, "+
		"CASE WHEN i % 7 = 0 THEN NULL ELSE i % 40 END AS person, DATE '2026-01-01' + CAST(i % 9 AS INTEGER) AS day FROM range(2000) r(i)")
	want := texts(t, cs.admin, "SELECT site, count(DISTINCT person), count(DISTINCT CAST(person AS VARCHAR) || '|' || CAST(day AS VARCHAR)) "+
		"FROM lake.public.visits GROUP BY site ORDER BY site")
	if len(want) != 5 {
		t.Fatalf("expected 5 sites: %q", want)
	}
	for dialect, q := range map[string]string{
		"duckdb": "WITH s AS (SELECT site AS site, count(DISTINCT person) AS people, count(DISTINCT CASE WHEN person IS NOT NULL AND day IS NOT NULL " +
			"THEN (person, day) END) AS person_days FROM lake.public.visits GROUP BY site) SELECT * FROM s ORDER BY site",
		"snowflake": "WITH S AS (SELECT SITE AS SITE, COUNT(DISTINCT PERSON) AS PEOPLE, COUNT(DISTINCT PERSON, DAY) AS PERSON_DAYS " +
			"FROM LAKE.PUBLIC.VISITS GROUP BY SITE) SELECT * FROM S ORDER BY SITE",
	} {
		mustExec(t, cs.admin, "SET sql_dialect = '"+dialect+"'")
		got := texts(t, cs.admin, q)
		mustExec(t, cs.admin, "SET sql_dialect = 'duckdb'")
		if !slices.Equal(got, want) {
			t.Errorf("%s:\n got %q\nwant %q", dialect, got, want)
		}
	}
}
