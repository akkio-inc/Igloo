package snowflake

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

// TestOutputInSessionTimeZone checks that TO_VARCHAR and TO_CHAR write a
// timestamp with a time zone in the session's time zone, with the offset
// as Snowflake writes it.
func TestOutputInSessionTimeZone(t *testing.T) {
	conn := session(t)
	const at = `'2026-10-07 15:58:37.862 +00:00'::timestamp_ltz`
	for _, c := range []runCase{
		{sql: `alter session set timezone = 'America/Los_Angeles'`},
		// What Snowflake returned for such a column, in its default time zone.
		{name: "los angeles", sql: `select to_varchar(` + at + `), to_varchar('2026-01-07 15:58:37.862 +00:00'::timestamp_ltz), to_char(` + at + `, 'HH24:MI TZH:TZM'), ` + at + `::string`,
			want: []string{"2026-10-07 08:58:37.862 -0700|2026-01-07 07:58:37.862 -0800|08:58 -07:00|2026-10-07 08:58:37.862 -0700"}},
		{sql: `alter session set timezone = 'Asia/Kolkata'`},
		{name: "kolkata", sql: `select to_varchar(` + at + `), to_char(` + at + `, 'TZHTZM')`,
			want: []string{"2026-10-07 21:28:37.862 +0530|+0530"}},
		{sql: `alter session set timezone = 'America/St_Johns'`},
		{name: "st johns", sql: `select to_varchar(` + at + `), to_char(` + at + `, 'TZH:TZM')`,
			want: []string{"2026-10-07 13:28:37.862 -0230|-02:30"}},
		// A TIMESTAMP_NTZ has no time zone to show.
		{name: "ntz", sql: `select to_varchar('2026-10-07 15:58:37.862'::timestamp_ntz)`, want: []string{"2026-10-07 15:58:37.862"}},
	} {
		run(t, conn, c)
	}
}

// TestApproxCountDistinctSketches counts with the datasketches extension,
// as Igloo's sessions do (Options.Sketches), columns of the cardinalities
// of a real table whose distinct values Snowflake's
// APPROX_COUNT_DISTINCT counted within 0.3% to 1.9% and DuckDB's
// approx_count_distinct within 1.1% to 21.6%.
func TestApproxCountDistinctSketches(t *testing.T) {
	dir := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if dir == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set")
	}
	conn := session(t)
	ctx := context.Background()
	for _, s := range []string{
		"SET extension_directory = '" + dir + "'",
		"LOAD datasketches",
		`CREATE TABLE t AS SELECT 'v' || (i % 1099) AS c1, i % 7246 AS c2, DATE '2000-01-01' + CAST(i % 41356 AS INTEGER) AS c3,
			(i % 251690) / 4.0 AS c4, CASE WHEN i % 3 > 0 THEN i % 10 END AS c5, i % 2 AS g FROM range(1252858) r(i)`,
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	translate := func(sql string) string {
		t.Helper()
		res, err := Translate(sql, Options{Sketches: true})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return res.SQL
	}

	// Each count within 3% (about two of Snowflake's standard errors) of
	// the exact one, of whatever type the values are.
	sql := translate(`select approx_count_distinct(c1), approx_count_distinct(c2), hll(c3), approx_count_distinct(c4),
		approx_count_distinct(c5), approx_count_distinct(c1, c5), approx_count_distinct(c3::timestamp),
		approx_count_distinct(to_variant(c2)), approx_count_distinct(c4::number(20, 2)) from t`)
	exact := []float64{1099, 7246, 41356, 251690, 10, 0, 41356, 7246, 251690}
	if err := conn.QueryRowContext(ctx, "SELECT count(DISTINCT (c1, c5)) FROM t WHERE c5 IS NOT NULL").Scan(&exact[5]); err != nil {
		t.Fatal(err)
	}
	got := make([]int64, len(exact))
	ptrs := make([]any, len(got))
	for i := range got {
		ptrs[i] = &got[i]
	}
	if err := conn.QueryRowContext(ctx, sql).Scan(ptrs...); err != nil {
		t.Fatalf("%v\n%s", err, sql)
	}
	for i, n := range got {
		if e := math.Abs(float64(n)/exact[i] - 1); e > 0.03 {
			t.Errorf("count %d: %d, exact %.0f (%.1f%% off)", i+1, n, exact[i], 100*e)
		}
	}
	// Small counts are exact.
	if got[4] != 10 {
		t.Errorf("approx_count_distinct of 10 values: %d", got[4])
	}

	// Nothing to count is 0, as in Snowflake; NULLs do not count.
	for q, want := range map[string]int64{
		`select approx_count_distinct(c1) from t where false`:                                0,
		`select hll(c5) from t where c5 is null`:                                             0,
		`select approx_count_distinct(x) from (values (1), (null), (1), (2)) v(x)`:           2,
		`select approx_count_distinct(x, y) from (values (1, 1), (1, null), (2, 1)) v(x, y)`: 2,
	} {
		var n int64
		if err := conn.QueryRowContext(ctx, translate(q)).Scan(&n); err != nil || n != want {
			t.Errorf("%s: %d %v, want %d", q, n, err, want)
		}
	}

	// As a window function, per partition, exactly.
	rows, err := conn.QueryContext(ctx, translate(`select distinct g, approx_count_distinct(c2) over (partition by g) from t order by g`))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var g, n int64
		if err := rows.Scan(&g, &n); err != nil {
			t.Fatal(err)
		}
		// i % 7246 of even and of odd i: 3,623 values each.
		if n != 3623 {
			t.Errorf("partition %d: %d, exact 3623", g, n)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestApproxCountDistinctGroupedCountsExactly checks that APPROX_COUNT_DISTINCT
// and HLL count exactly in selects with GROUP BY and as window functions,
// where a sketch per group is slower than COUNT(DISTINCT), and with the
// sketch elsewhere, a subquery of a grouped select included.
func TestApproxCountDistinctGroupedCountsExactly(t *testing.T) {
	for _, c := range []struct {
		sql           string
		exact, sketch int
	}{
		{`select approx_count_distinct(x) from t`, 0, 1},
		{`select g, approx_count_distinct(x) from t group by g`, 1, 0},
		{`select g, hll(x, y) from t group by all`, 1, 0},
		{`select g, round(approx_count_distinct(x) / 2) from t group by rollup (g) having hll(x) > 1`, 2, 0},
		{`select approx_count_distinct(x) over (partition by g) from t`, 1, 0},
		{`select g, (select approx_count_distinct(x) from t) from t group by g`, 0, 1},
		{`select approx_count_distinct(n) from (select g, approx_count_distinct(x) as n from t group by g)`, 1, 1},
		{`with c as (select g, hll(x) as n from t group by g) select approx_count_distinct(n) from c`, 1, 1},
		{`select approx_count_distinct(x) from t union all select approx_count_distinct(x) from t group by g`, 1, 1},
		{`select listagg(x) within group (order by x), approx_count_distinct(x) from t`, 0, 1},
	} {
		res, err := Translate(c.sql, Options{Sketches: true})
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if exact, sketch := strings.Count(res.SQL, "count(DISTINCT "), strings.Count(res.SQL, "datasketch_hll("); exact != c.exact || sketch != c.sketch {
			t.Errorf("%s: %d exact and %d sketched, want %d and %d\n%s", c.sql, exact, sketch, c.exact, c.sketch, res.SQL)
		}
	}

	conn := session(t)
	ctx := context.Background()
	for q, want := range map[string]string{
		`select g, approx_count_distinct(x), hll(x, y) from (values (1, 1, 1), (1, 1, 2), (1, 2, null), (2, null, 1)) v(g, x, y) group by g order by g`: "1 2 2;2 0 0",
		`select distinct g, approx_count_distinct(x) over (partition by g) from (values (1, 1), (1, 2), (1, 2), (2, 3)) v(g, x) order by g`:             "1 2;2 1",
	} {
		res, err := Translate(q, Options{Sketches: true})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		rows, err := conn.QueryContext(ctx, res.SQL)
		if err != nil {
			t.Fatalf("%s: %v", res.SQL, err)
		}
		cols, _ := rows.Columns()
		var got []string
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			got = append(got, strings.TrimSuffix(strings.TrimPrefix(fmt.Sprint(vals), "["), "]"))
		}
		rows.Close()
		if strings.Join(got, ";") != want {
			t.Errorf("%s: %q, want %q", q, strings.Join(got, ";"), want)
		}
	}
}
