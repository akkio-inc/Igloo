package pgwire

import (
	"os"
	"slices"
	"testing"

	"github.com/akkio-inc/igloo/internal/snowflake"
)

func TestApproxCounts(t *testing.T) {
	t.Parallel()
	db, parse := wholeCountDB(t)
	if _, err := db.Exec(`CREATE TABLE k AS SELECT 1 AS "name", 2 AS "Mixed Case"`); err != nil {
		t.Fatal(err)
	}
	deparse := func(q string) (string, error) {
		var out string
		err := db.QueryRow("SELECT json_deserialize_sql(json_serialize_sql($1::VARCHAR))", q).Scan(&out)
		return out, err
	}
	sk := snowflake.SketchCount
	rewritten := []struct{ in, want string }{
		// A column profile as data tools send it.
		{`SELECT APPROX_COUNT_DISTINCT("x") AS X_D, APPROX_COUNT_DISTINCT("s") AS S_D FROM t`,
			`SELECT CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN "x" IS NOT NULL THEN hash("x") END))), 0) AS BIGINT) AS X_D, ` +
				`CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN "s" IS NOT NULL THEN hash("s") END))), 0) AS BIGINT) AS S_D FROM t`},
		// In any case; an expression next to IS NOT NULL is in parentheses.
		{`select Approx_Count_Distinct ( x + 1 ) as n from t`, `select ` + sk("", "(x + 1)") + ` as n from t`},
		// A call alone without an alias keeps DuckDB's name for its column.
		{`SELECT approx_count_distinct(x), approx_count_distinct(t."s") FROM t`,
			`SELECT ` + sk("", "x") + ` AS "approx_count_distinct(x)", ` + sk("", `t."s"`) + ` AS "approx_count_distinct(t.s)" FROM t`},
		{`SELECT approx_count_distinct("name"), approx_count_distinct(DISTINCT k."Mixed Case") FROM k`,
			`SELECT ` + sk("", `"name"`) + ` AS "approx_count_distinct(""name"")", ` +
				sk("", `k."Mixed Case"`) + ` AS "approx_count_distinct(DISTINCT k.""Mixed Case"")" FROM k`},
		// DISTINCT changes nothing; a FILTER, with or without WHERE, becomes a
		// condition.
		{`SELECT approx_count_distinct(DISTINCT x) AS a, approx_count_distinct(x) FILTER (WHERE id > 50) AS b, approx_count_distinct(x) FILTER (id < 10) AS c FROM t`,
			`SELECT ` + sk("", "x") + ` AS a, ` + sk("(id > 50)", "x") + ` AS b, ` + sk("(id < 10)", "x") + ` AS c FROM t`},
		// A comment at the end of the value is left behind.
		{"SELECT approx_count_distinct(x -- the key\n) AS n FROM t", `SELECT ` + sk("", "x") + ` AS n FROM t`},
		// The name in a string or a comment stays.
		{`SELECT 'approx_count_distinct(x)' AS s, /* approx_count_distinct(x) */ approx_count_distinct(x) AS n FROM t`,
			`SELECT 'approx_count_distinct(x)' AS s, /* approx_count_distinct(x) */ ` + sk("", "x") + ` AS n FROM t`},
		// Grouped, exactly, with its FILTER, in HAVING and ORDER BY too.
		{`SELECT x, approx_count_distinct(id) AS n, approx_count_distinct(id) FILTER (WHERE id > 5) AS m FROM t GROUP BY x HAVING approx_count_distinct(id) > 3 ORDER BY approx_count_distinct(id)`,
			`SELECT x, count(DISTINCT id) AS n, count(DISTINCT id) FILTER (WHERE id > 5) AS m FROM t GROUP BY x HAVING count(DISTINCT id) > 3 ORDER BY count(DISTINCT id)`},
		{`SELECT x, approx_count_distinct(id) AS n FROM t GROUP BY ALL`, `SELECT x, count(DISTINCT id) AS n FROM t GROUP BY ALL`},
		{`SELECT x, approx_count_distinct(id) FROM t GROUP BY ROLLUP (x)`,
			`SELECT x, count(DISTINCT id) AS "approx_count_distinct(id)" FROM t GROUP BY ROLLUP (x)`},
		// As a window function, exactly, with its FILTER and window.
		{`SELECT id, approx_count_distinct(x) OVER (PARTITION BY id % 2) AS n, approx_count_distinct(x) FILTER (WHERE id > 3) OVER w AS m FROM t WINDOW w AS (ORDER BY id)`,
			`SELECT id, count(DISTINCT x) OVER (PARTITION BY id % 2) AS n, count(DISTINCT x) FILTER (WHERE id > 3) OVER w AS m FROM t WINDOW w AS (ORDER BY id)`},
		// A subquery is a select of its own: ungrouped in a grouped select,
		// grouped in an ungrouped one, in FROM or in a common table
		// expression.
		{`SELECT x, approx_count_distinct(id) AS n, (SELECT approx_count_distinct(v) FROM u) AS m FROM t GROUP BY x`,
			`SELECT x, count(DISTINCT id) AS n, (SELECT ` + sk("", "v") + ` AS "approx_count_distinct(v)" FROM u) AS m FROM t GROUP BY x`},
		{`SELECT approx_count_distinct(n) AS m FROM (SELECT x, approx_count_distinct(v) AS n FROM u GROUP BY x) AS g`,
			`SELECT ` + sk("", "n") + ` AS m FROM (SELECT x, count(DISTINCT v) AS n FROM u GROUP BY x) AS g`},
		{`WITH c AS (SELECT x, approx_count_distinct(id) AS n FROM t GROUP BY x) SELECT approx_count_distinct(n) AS m FROM c`,
			`WITH c AS (SELECT x, count(DISTINCT id) AS n FROM t GROUP BY x) SELECT ` + sk("", "n") + ` AS m FROM c`},
		{`SELECT approx_count_distinct(x) AS n FROM t UNION ALL SELECT approx_count_distinct(v) FROM u GROUP BY x`,
			`SELECT ` + sk("", "x") + ` AS n FROM t UNION ALL SELECT count(DISTINCT v) AS "approx_count_distinct(v)" FROM u GROUP BY x`},
	}
	kept := []string{
		"SELECT count(DISTINCT x) FROM t",
		"SELECT 'approx_count_distinct(x)' FROM t",
		"SELECT x /* approx_count_distinct(x) */ FROM t",
		// Not DuckDB's function of one value: DuckDB refuses the first, and
		// a call qualified with a schema may be another function.
		"SELECT approx_count_distinct(x, id) AS n FROM t",
		"SELECT main.approx_count_distinct(x) AS n FROM t",
		"SELECT approx_count_distinct(x ORDER BY id) AS n FROM t",
		"SELECT approx_count_distinct(x ORDER BY id) OVER () AS n FROM t",
		"SELECT approx_count_distinct(x) EXPORT_STATE AS n FROM t",
		// A value the CASE would read twice.
		"SELECT approx_count_distinct(x + $1) AS n FROM t",
		"SELECT approx_count_distinct(random()) AS n FROM t",
		// A select item without an alias, whose column's name would change.
		"SELECT approx_count_distinct(x) + 1 FROM t",
		"SELECT approx_count_distinct(x + 1) FROM t",
		"SELECT approx_count_distinct(x) FILTER (WHERE id > 1) FROM t",
		"SELECT approx_count_distinct(x) OVER () FROM t",
		"SELECT (approx_count_distinct(x)) FROM t",
		"SELECT (SELECT approx_count_distinct(v) AS n FROM u) FROM t",
		// Not one query.
		"CREATE VIEW w AS SELECT approx_count_distinct(x) AS n FROM t",
		"SELECT approx_count_distinct(x) AS n FROM t; SELECT 1",
		"SELECT approx_count_distinct(x AS n FROM t",
	}
	// With the extension, each statement runs as rewritten, and names its
	// columns as before.
	sketches := os.Getenv("IGLOO_TEST_EXTENSION_DIR") != ""
	if sketches {
		for _, s := range []string{"SET extension_directory = '" + os.Getenv("IGLOO_TEST_EXTENSION_DIR") + "'", "LOAD datasketches"} {
			if _, err := db.Exec(s); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range rewritten {
		if !approxCall.MatchString(maskCode(c.in)) {
			t.Errorf("not found: %s", c.in)
		}
		got := approxCounts(c.in, parse, deparse)
		if got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.in, got, c.want)
			continue
		}
		// What it wrote has nothing left to rewrite.
		if again := approxCounts(got, parse, deparse); again != got {
			t.Errorf("rewritten again: %s\n  -> %s", got, again)
		}
		if sketches {
			wantCols, _ := resultOf(t, db, c.in)
			if gotCols, _ := resultOf(t, db, got); !slices.Equal(gotCols, wantCols) {
				t.Errorf("%s: columns %q, want %q", got, gotCols, wantCols)
			}
		}
	}
	for _, q := range kept {
		if got := approxCounts(q, parse, deparse); got != q {
			t.Errorf("rewritten: %s\n  -> %s", q, got)
		}
	}
	for _, q := range []string{"SELECT 'approx_count_distinct(x)' FROM t", "SELECT x /* approx_count_distinct(x) */ FROM t", "SELECT x FROM t"} {
		if approxCall.MatchString(maskCode(q)) {
			t.Errorf("found in %s", q)
		}
	}

	// Snowflake's translations have nothing to rewrite, grouped or not.
	for _, q := range []string{"SELECT APPROX_COUNT_DISTINCT(x) AS n, HLL(x, id) AS m FROM t",
		"SELECT x, APPROX_COUNT_DISTINCT(id) AS n FROM t GROUP BY x"} {
		res, err := snowflake.Translate(q, snowflake.Options{Sketches: true})
		if err != nil {
			t.Fatal(err)
		}
		if approxCall.MatchString(maskCode(res.SQL)) || approxCounts(res.SQL, parse, deparse) != res.SQL {
			t.Errorf("translation of %s rewritten: %s", q, res.SQL)
		}
	}
}
