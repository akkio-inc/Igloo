package pgwire

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// unionDB returns a database with tables for union tests, and the tools
// factorUnionText needs over it.
func unionDB(t *testing.T) (*sql.DB, unionTools) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	for _, s := range []string{
		"CREATE TABLE t AS SELECT 'id' || i AS id, (i % 4)::DECIMAL(1,0) AS a, (i % 3)::DECIMAL(1,0) AS b, " +
			"((i * 7) % 4)::VARCHAR AS c, ((i * 5) % 4)::VARCHAR AS d, i AS n, i / 3 AS h, " +
			"DATE '2026-01-01' + (i % 30)::INTEGER AS day, TIMESTAMP '2026-01-01 12:00:00' + to_hours(i % 50) AS ts " +
			"FROM range(1, 2001) r(i)",
		"CREATE TABLE s AS SELECT i AS resp_id, 'survey ' || i AS label FROM range(0, 700) r(i)",
		"CREATE VIEW v AS SELECT t.*, s.label FROM t LEFT JOIN s ON t.h = s.resp_id",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	tools := unionTools{
		serialize: func(q string) (string, error) {
			var raw string
			err := db.QueryRow("SELECT json_serialize_sql($1::VARCHAR)::VARCHAR", q).Scan(&raw)
			return raw, err
		},
		deparse: func(doc string) (string, error) {
			var out string
			err := db.QueryRow("SELECT json_deserialize_sql($1::VARCHAR::JSON)", doc).Scan(&out)
			return out, err
		},
		describe: func(q string) ([]string, error) {
			rows, err := db.Query("DESCRIBE " + q)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			var out []string
			for rows.Next() {
				dest := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range dest {
					ptrs[i] = &dest[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					return nil, err
				}
				out = append(out, fmt.Sprint(dest[0])+" "+fmt.Sprint(dest[1]))
			}
			return out, rows.Err()
		},
		volatile: sharedVolatile(t, db),
	}
	return db, tools
}

// typedResult runs a query and returns its columns' names and types and
// its rows, sorted, as text.
func typedResult(t *testing.T, db *sql.DB, q string) ([]string, []string) {
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
		dest := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprint(dest...))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return cols, out
}

// e1Shape is a report of the shape that compares measures of the same rows
// as a UNION ALL: n branches over one join, aggregated by branch.
func e1Shape(n int) string {
	styles := []string{"a", "b"}
	chans := []string{"c", "d"}
	var branches []string
	for i := range n {
		st, ch := styles[i%2], chans[(i/2)%2]
		cond := "IN ('1', '2')"
		name := fmt.Sprintf("'%s_%d'", st, i)
		if i == 0 {
			branches = append(branches, fmt.Sprintf("SELECT V.id AS id, %s AS style_name, '%s' AS channel_name, "+
				"V.%s IN (1, 2) AS style_strong, V.%s %s AS channel_strong "+
				"FROM v AS V INNER JOIN g AS G ON G.id = V.id", name, ch, st, ch, cond))
			continue
		}
		branches = append(branches, fmt.Sprintf("SELECT V.id AS id, %s AS %s, '%s' AS %s, V.%s IN (1, 2) AS _col_3, "+
			"V.%s %s AS _col_4 FROM v AS V INNER JOIN g AS G ON G.id = V.id", name, st, ch, ch, st, ch, cond))
	}
	return "SELECT * FROM (WITH g AS (SELECT id FROM v) SELECT V.style_name AS style_name, V.channel_name AS channel_name, " +
		"count(CASE WHEN V.style_strong AND V.channel_strong THEN V.id END) AS strong_count, count(V.id) AS total_count, " +
		"round(100.0 * count(CASE WHEN V.style_strong AND V.channel_strong THEN V.id END) / nullif(count(V.id), 0), 2) AS strong_pct " +
		"FROM (" + strings.Join(branches, " UNION ALL ") + ") AS V GROUP BY V.style_name, V.channel_name " +
		"ORDER BY style_name, channel_name) AS t0 ORDER BY t0.style_name, t0.channel_name LIMIT 10000"
}

// TestFactorUnions checks which UNION ALLs are computed as one SELECT, and
// that the statement returns the same columns, types and rows either way.
func TestFactorUnions(t *testing.T) {
	db, tools := unionDB(t)
	agg := func(union string) string {
		return "SELECT k, count(*) AS n, count(x) AS nx, sum(n) AS total FROM (" + union + ") AS u GROUP BY k"
	}
	factored := []struct{ name, q string }{
		{"the report shape", e1Shape(16)},
		{"two branches", agg("SELECT 'one' AS k, a AS x, n FROM t WHERE n > 10 UNION ALL SELECT 'two', b, n FROM t WHERE n > 10")},
		{"comments and quoted names", agg("SELECT /* first */ 'one' AS \"k\", a AS x, n -- the number\n FROM t WHERE n > 10\n" +
			"UNION ALL SELECT 'two' k, b \"x\", n FROM t -- again\n WHERE n > 10")},
		{"integer and decimal constants", agg("SELECT 'one' AS k, 1 AS x, n FROM t UNION ALL SELECT 'two', 1.5, n FROM t " +
			"UNION ALL SELECT 'three', 2, n FROM t")},
		{"a NULL branch", agg("SELECT 'one' AS k, NULL AS x, n FROM t UNION ALL SELECT 'two', c, n FROM t")},
		{"a NULL first", agg("SELECT NULL AS k, a AS x, n FROM t UNION ALL SELECT 'two', b, n FROM t")},
		{"a date and a timestamp", "SELECT k, min(x) AS lo, max(x) AS hi, count(*) AS n FROM (" +
			"SELECT 'day' AS k, day AS x FROM t UNION ALL SELECT 'ts', ts FROM t) AS u GROUP BY k"},
		{"columns that differ by name", agg("SELECT 'one' AS k, a AS x, n FROM t UNION ALL SELECT 'two', b, n FROM t")},
		{"an aggregate without GROUP BY", "SELECT count(*) AS n, sum(x) AS s FROM (SELECT a AS x FROM t UNION ALL SELECT b FROM t) AS u"},
		{"a join in a CTE", "WITH j AS (SELECT t.id, t.a, t.b, s.label FROM t JOIN s ON t.h = s.resp_id) " +
			"SELECT k, count(*) AS n, count(DISTINCT x) AS nx FROM (SELECT 'a' AS k, j.a AS x FROM j " +
			"UNION ALL SELECT 'b', j.b FROM j) AS u GROUP BY k"},
	}
	for _, c := range factored {
		out, err := factorUnionText(c.q, tools)
		if err != nil || out == "" {
			t.Errorf("%s: not factored (%v)", c.name, err)
			continue
		}
		if !strings.Contains(out, "CROSS JOIN range(") || strings.Contains(strings.ToUpper(out), "UNION ALL") {
			t.Errorf("%s: factored as %s", c.name, out)
		}
		wantCols, want := typedResult(t, db, c.q)
		gotCols, got := typedResult(t, db, out)
		if !slices.Equal(wantCols, gotCols) || !slices.Equal(want, got) {
			t.Errorf("%s: columns %v and rows %v, want %v and %v\n%s", c.name, gotCols, got, wantCols, want, out)
		}
	}
	// Sixteen branches over a join scan its table once instead of sixteen
	// times (and twice for the CTE that reads it again).
	out, _ := factorUnionText(e1Shape(16), tools)
	_, before := planCTEs(t, db, e1Shape(16))
	if _, after := planCTEs(t, db, out); after > 2 || after >= before {
		t.Errorf("the factored report scans t %d times, as written %d:\n%s", after, before, out)
	}

	asWritten := []struct{ name, q string }{
		{"a branch with its own WHERE", agg("SELECT 'one' AS k, a AS x, n FROM t WHERE n > 10 UNION ALL SELECT 'two', b, n FROM t WHERE n > 20")},
		{"a branch without the WHERE", agg("SELECT 'one' AS k, a AS x, n FROM t WHERE n > 10 UNION ALL SELECT 'two', b, n FROM t")},
		{"another alias", agg("SELECT 'one' AS k, a AS x, n FROM t UNION ALL SELECT 'two', b, n FROM t AS t2")},
		{"rows whose order shows", "SELECT * FROM (SELECT 'one' AS k, a AS x FROM t UNION ALL SELECT 'two', b FROM t) AS u"},
		{"an aggregate of their order", "SELECT k, string_agg(x::VARCHAR, ',') AS xs FROM (SELECT 'one' AS k, a AS x FROM t " +
			"UNION ALL SELECT 'two', b FROM t) AS u GROUP BY k"},
		{"a window over them", "SELECT k, count(*) OVER () AS n FROM (SELECT 'one' AS k, a AS x FROM t UNION ALL SELECT 'two', b FROM t) AS u GROUP BY k"},
		{"a function in a branch", agg("SELECT 'one' AS k, upper(c) AS x, n FROM t UNION ALL SELECT 'two', d, n FROM t")},
		{"a subquery in a branch", agg("SELECT 'one' AS k, (SELECT max(a) FROM t) AS x, n FROM t UNION ALL SELECT 'two', b, n FROM t")},
		{"a volatile WHERE", agg("SELECT 'one' AS k, a AS x, n FROM t WHERE random() < 2 UNION ALL SELECT 'two', b, n FROM t WHERE random() < 2")},
		{"a LIMIT in the FROM", agg("SELECT 'one' AS k, a AS x, n FROM (SELECT * FROM t LIMIT 5) AS t UNION ALL " +
			"SELECT 'two', b, n FROM (SELECT * FROM t LIMIT 5) AS t")},
		{"a DISTINCT branch", agg("SELECT DISTINCT 'one' AS k, a AS x, n FROM t UNION ALL SELECT 'two', b, n FROM t")},
		{"UNION without ALL", agg("SELECT 'one' AS k, a AS x, n FROM t UNION SELECT 'two', b, n FROM t")},
		{"an unnamed expression that differs", agg("SELECT 'one' AS k, a + 1, n FROM t UNION ALL SELECT 'two', b + 1, n FROM t")},
		{"a branch in parentheses", agg("(SELECT 'one' AS k, a AS x, n FROM t) UNION ALL (SELECT 'two', b, n FROM t)")},
		{"a star", "SELECT count(*) AS n FROM (SELECT * FROM t UNION ALL SELECT * FROM t) AS u"},
	}
	for _, c := range asWritten {
		if out, err := factorUnionText(c.q, tools); out != "" {
			t.Errorf("%s: factored as %s", c.name, out)
		} else if err != nil {
			t.Logf("%s: %v", c.name, err)
		}
	}

	// A union whose CASE would have another type than the union's stays:
	// UNION ALL makes 1 and 'text' strings, CASE integers.
	if out, err := factorUnionText(agg("SELECT 'one' AS k, 1 AS x, n FROM t UNION ALL SELECT 'two', 'text', n FROM t"), tools); out != "" || err == nil {
		t.Errorf("an integer and a string: factored as %q (%v)", out, err)
	}
	// Parameters' numbering follows the text.
	if out, _ := factorUnionText(agg("SELECT 'one' AS k, a AS x, n FROM t WHERE n > $1 UNION ALL SELECT 'two', b, n FROM t WHERE n > $1"), tools); out != "" {
		t.Errorf("a statement with parameters: factored as %s", out)
	}
}
