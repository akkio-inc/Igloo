package pgwire

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// wholeCountDB is a DuckDB database with the tables the whole-count tests
// read, and a parse function as Igloo's.
func wholeCountDB(t *testing.T) (*sql.DB, func(string) (any, error)) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	// Igloo's macros, which translations from Snowflake's dialect call.
	for _, s := range append(snowflake.Macros(),
		"CREATE TABLE t AS SELECT i AS id, i % 13 AS x, 'row ' || i AS s FROM range(1, 101) r(i)",
		"CREATE TABLE u AS SELECT i * 2 AS id, i % 7 AS x, 'u' || i AS v FROM range(1, 41) r(i)",
	) {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	parse := func(q string) (any, error) {
		var raw string
		if err := db.QueryRow("SELECT json_serialize_sql($1::VARCHAR, skip_null := true, skip_empty := true)::VARCHAR", q).Scan(&raw); err != nil {
			return nil, err
		}
		return decodeParse(raw)
	}
	return db, parse
}

// resultOf runs a query and returns its column names and its rows, sorted,
// as text.
func resultOf(t *testing.T, db *sql.DB, q string) ([]string, []string) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
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

func TestWholeCounts(t *testing.T) {
	db, parse := wholeCountDB(t)
	rewritten := []string{
		"SELECT *, COUNT(*) OVER () AS total FROM t",
		"SELECT id, count(*) over () FROM t WHERE x > 5 ORDER BY id LIMIT 3",
		"SELECT a.id, b.v, count_star() OVER () AS n FROM t a JOIN u b USING (id) WHERE a.x % 2 = 0",
		"SELECT *, count(*) OVER () n FROM t WHERE x IN (SELECT x FROM u WHERE v LIKE 'u1%')",
		"WITH c AS (SELECT * FROM t WHERE x < 5) SELECT id, count(*) OVER () AS n FROM c",
		"SELECT id, count(*) over() AS n FROM t -- a comment\nWHERE x > 10 -- another\n",
		"SELECT id, count(*) over () AS n FROM t WHERE x < 3 UNION ALL SELECT id, count(*) over () FROM u",
		"SELECT n, count(*) OVER () AS m, id FROM (SELECT id, count(*) OVER () AS n FROM t WHERE x > 3) s WHERE id > 50",
		"SELECT id, 'over ()' AS s, count(*) /* over () */ OVER ( ) AS n, x + 1 AS k, CAST(x AS VARCHAR) || '!' FROM t",
		"SELECT count(*) OVER () AS n, id FROM t AS \"FROM\" WHERE \"FROM\".x = 1",
		"SELECT id, count(*) OVER () AS n FROM t WHERE x = 1 AND s IN ('row 1', 'row 14') ORDER BY id",
		"SELECT t.id, count(*) OVER () AS n FROM t, u WHERE t.id = u.id;",
		// Other aggregates over the whole result.
		"SELECT id, min(x) OVER () AS lo, max(x) OVER () AS hi FROM t WHERE id > 10",
		"SELECT id, MAX ( s ) over ( ) AS hi, count(t.x) OVER () AS n FROM t",
		// Snowflake's division, as its translation writes it.
		"SELECT id, sf_div(id, x) AS r, count(*) OVER () AS n FROM t WHERE x > 0",
		"SELECT a.id, min(a.x) OVER () AS lo, count(*) OVER () AS n FROM t AS a, u WHERE a.id = u.id ORDER BY a.id LIMIT 4",
		// NULLs count and compare as in the window: none, and none at all.
		"SELECT id, count(CASE WHEN x > 5 THEN x END) OVER () AS n, min(CASE WHEN x > 50 THEN s END) OVER () AS m FROM t",
		// A report's period: a join with a common table expression.
		"WITH a AS (SELECT DISTINCT id FROM t WHERE x < 5), m AS (SELECT u.id, u.v, min(u.x) OVER () AS first_x, " +
			"max(u.x) OVER () AS last_x FROM u INNER JOIN a ON a.id = u.id) SELECT * FROM m",
		// A common table expression with a window function that the FROM
		// does not read does not keep the statement as written.
		"WITH r AS (SELECT id, row_number() OVER (ORDER BY x, id) AS rn FROM t), " +
			"b AS (SELECT u.id, min(u.x) OVER () AS lo, max(u.v) OVER () AS hi FROM u JOIN t ON t.id = u.id) " +
			"SELECT * FROM b JOIN r USING (id) WHERE rn < 30",
	}
	kept := []string{
		"SELECT x, count(*) OVER () FROM t GROUP BY x",
		"SELECT sum(x), count(*) OVER () FROM t",
		"SELECT DISTINCT x, count(*) OVER () FROM t",
		"SELECT id, count(*) OVER (), row_number() OVER () FROM t",
		"SELECT id, count(*) OVER (PARTITION BY x) FROM t",
		"SELECT id, count(*) OVER (ORDER BY id) FROM t",
		"SELECT id, count(x) OVER () FROM t",
		"SELECT id, count(*) FILTER (WHERE x > 1) OVER () FROM t",
		"SELECT id, count(*) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
		"SELECT id, count(*) OVER () FROM t USING SAMPLE 10",
		"SELECT id, count(*) OVER () FROM t TABLESAMPLE 10%",
		"SELECT id, count(*) OVER () FROM t WHERE random() < 0.5",
		"SELECT id, count(*) OVER () FROM (SELECT * FROM t LIMIT 10)",
		"SELECT id, count(*) OVER () FROM (SELECT *, row_number() OVER () AS rn FROM t) WHERE rn < 5",
		"WITH c AS (SELECT * FROM t USING SAMPLE 5) SELECT id, count(*) OVER () FROM c",
		"SELECT id, count(*) OVER () FROM t QUALIFY id > 3",
		"SELECT x + 1 AS y, count(*) OVER () FROM t WHERE y > 3",
		"FROM t SELECT id, count(*) OVER ()",
		"SELECT count(*) OVER ()",
		"SELECT (count(*) OVER ()) AS n FROM t",
		"SELECT upper(s), count(*) OVER () FROM t",
		"SELECT sf_div(sum(x), 2), count(*) OVER () FROM t",
		"SELECT (SELECT max(x) FROM u), count(*) OVER () FROM t",
		"WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n, count(*) OVER () FROM r",
		"SELECT 'count(*) over ()' FROM t",
		// Sums and averages, which may differ in their last digits with the
		// order rows are added in; aggregates without an alias, whose column
		// name would change; DISTINCT, FILTER, a partition, a function, a
		// select item's alias or another query's column in the argument.
		"SELECT id, sum(x) OVER () AS total FROM t",
		"SELECT id, avg(x) OVER () AS mean FROM t",
		"SELECT id, min(x) OVER () FROM t",
		"SELECT id, min(DISTINCT x) OVER () AS m FROM t",
		"SELECT id, max(x) FILTER (WHERE x > 3) OVER () AS m FROM t",
		"SELECT id, min(x) OVER (PARTITION BY s) AS m FROM t",
		"SELECT id, min(upper(s)) OVER () AS m FROM t",
		"SELECT x + 1 AS y, max(y) OVER () AS m FROM t",
		"SELECT (SELECT max(u.x) OVER () AS m FROM t LIMIT 1) AS z FROM u",
		// A FROM that reads a common table expression with a window
		// function, which may number ties either way in each copy.
		"WITH r AS (SELECT id, row_number() OVER (ORDER BY x) AS rn FROM t) SELECT id, min(rn) OVER () AS lo FROM r",
	}
	for _, q := range rewritten {
		got := wholeCounts(q, parse)
		if got == q || strings.Contains(strings.ToLower(maskCode(got)), "over ()") || strings.Contains(maskCode(got), "OVER ( )") {
			t.Errorf("not rewritten: %s\n  -> %s", q, got)
			continue
		}
		t.Logf("%s\n  -> %s", q, got)
		wantCols, wantRows := resultOf(t, db, q)
		gotCols, gotRows := resultOf(t, db, got)
		if !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
			t.Errorf("%s\n  -> %s\n got %q %q\nwant %q %q", q, got, gotCols, gotRows, wantCols, wantRows)
		}
	}
	for _, q := range kept {
		if got := wholeCounts(q, parse); got != q {
			t.Errorf("rewritten: %s\n  -> %s", q, got)
		}
	}
	// The statement Acme previews a table with, in DuckDB's dialect.
	preview := `SELECT * FROM (SELECT *, COUNT(*) OVER () AS "_PREVIEW_ROW_TOTAL" FROM t) TABLESAMPLE reservoir(50 ROWS)`
	want := `SELECT * FROM (SELECT *, (SELECT count(*) FROM t` + "\n" + `) AS "_PREVIEW_ROW_TOTAL" FROM t) TABLESAMPLE reservoir(50 ROWS)`
	if got := wholeCounts(preview, parse); got != want {
		t.Errorf("preview:\n got %s\nwant %s", got, want)
	}
}

// TestWholeCountsThroughSessions runs Acme's table preview in both
// dialects, and a whole count of a table with a row access policy, which
// must count only the rows the user sees.
func TestWholeCountsThroughSessions(t *testing.T) {
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, "CREATE TABLE lake.public.activity AS SELECT i AS id, 'site' || (i % 50) AS site, i * 1.5 AS spend FROM range(5000) r(i)")
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.crm TO acme_team")
	for dialect, q := range map[string]string{
		"duckdb":    `SELECT * FROM (SELECT *, COUNT(*) OVER () AS "_PREVIEW_ROW_TOTAL" FROM lake.public.activity) TABLESAMPLE reservoir(50 ROWS)`,
		"snowflake": `SELECT * FROM (SELECT *, COUNT(*) OVER () AS _PREVIEW_ROW_TOTAL FROM LAKE.PUBLIC.ACTIVITY) SAMPLE (50 ROWS)`,
	} {
		mustExec(t, cs.admin, "SET sql_dialect = '"+dialect+"'")
		rows := texts(t, cs.admin, q)
		mustExec(t, cs.admin, "SET sql_dialect = 'duckdb'")
		if len(rows) != 50 {
			t.Fatalf("%s: %d rows", dialect, len(rows))
		}
		for _, r := range rows {
			if !strings.HasSuffix(r, "|5000") {
				t.Errorf("%s: %s", dialect, r)
			}
		}
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ali := cs.user(t, "ali_"+name, auth.RoleNone, mode)
			mustExec(t, cs.admin, "GRANT acme_team TO ali_"+name)
			for _, dialect := range []string{"duckdb", "snowflake"} {
				mustExec(t, ali, "SET sql_dialect = '"+dialect+"'")
				expectRows(t, ali, "SELECT id, email, COUNT(*) OVER () AS n FROM lake.crm.contacts ORDER BY id", "1|***|2", "2|***|2")
			}
		})
	}
}
