package cleanroom

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// openDuckDB opens an in-memory DuckDB and returns a connection to it.
func openDuckDB(t *testing.T) *duckdb.Conn {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = connector.Close()
	})
	return c.(*duckdb.Conn)
}

// testDatasets are an agency's CRM audience and a TV maker's viewing log.
func testDatasets() []*Dataset {
	return []*Dataset{
		{Name: "crm", Collaborator: "agency", Schema: "marts", Table: "crm_audience", Kind: SourceTable,
			Policy: Policy{Join: []string{"email"}, Group: []string{"segment", "region"}, Threshold: 50, RunsPerDay: 100}},
		{Name: "views", Collaborator: "tvco", Schema: "partners", Table: "tvco_views", Kind: SourceExternal,
			Policy: Policy{Join: []string{"email", "device"}, Group: []string{"genre", "day", "grp"},
				Aggregates: []Aggregate{{"sum", "minutes"}}, Threshold: 50, RunsPerDay: 100}},
	}
}

var dateParams = []Parameter{{Name: "start_day", Type: "date"}, {Name: "end_day", Type: "DATE"}}

func TestTemplatesAccepted(t *testing.T) {
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	for _, c := range []struct {
		sql    string
		params []Parameter
	}{
		{sql: `SELECT l.segment, count(DISTINCT l.email) AS audience, count(DISTINCT r.email) AS overlap,
			round(100.0 * count(DISTINCT r.email) / count(DISTINCT l.email), 1) AS overlap_percent
			FROM crm AS l LEFT JOIN views AS r ON l.email = r.email GROUP BY ALL ORDER BY overlap DESC, 1 LIMIT 20`},
		{sql: `SELECT v.genre, CAST(v.day AS DATE) AS day, count(*) AS impressions, count(DISTINCT v.device) AS reach
			FROM views v WHERE CAST(v.day AS DATE) BETWEEN $start_day AND $end_day
			GROUP BY v.genre, CAST(v.day AS DATE) ORDER BY genre NULLS FIRST, day`, params: dateParams},
		{sql: `SELECT datasketch_hll_estimate(datasketch_hll(12, c.email)) AS reach,
			approx_count_distinct(c.email) AS approx FROM crm c`},
		{sql: `SELECT v.genre, count(DISTINCT c.email) FILTER (WHERE v.grp = 'exposed') AS exposed,
			count(DISTINCT c.email) FILTER (WHERE v.grp = 'control') AS control
			FROM views v JOIN crm c ON v.email = c.email AND v.day >= DATE '2026-10-01' AND v.day < v.day + INTERVAL 7 DAY
			GROUP BY v.genre`},
		{sql: `SELECT upper(c.region) AS region, count(*) AS n, count(c.email) AS emails FROM crm c
			WHERE c.region IN ('east', 'west') AND c.segment LIKE 'a%' OR c.region IS NULL GROUP BY 1`},
		{sql: `SELECT count(*) AS n FROM crm a JOIN crm b ON a.email = b.email`},
		{sql: `SELECT c.segment FROM crm c GROUP BY c.segment`},
		{sql: `SELECT v.genre, sum(v.minutes) AS minutes FROM views v GROUP BY 1`},
		{sql: `SELECT c.segment, count(*) AS n FROM crm c GROUP BY GROUPING SETS ((c.segment))`},
		{sql: `SELECT count(*) AS n FROM crm c JOIN views v ON c.email = v.email JOIN views w ON v.device = w.device`},
		{sql: `SELECT v.genre, count(*) AS n FROM views v WHERE v.day >= $start_day AND v.day <= $end_day AND v.grp = 'exposed'
			GROUP BY 1`, params: dateParams},
	} {
		if _, err := p.Check(ctx, Template{SQL: c.sql, Parameters: c.params}, testDatasets()); err != nil {
			t.Errorf("%s: %v", c.sql, err)
		}
	}
}

func TestTemplatesRefused(t *testing.T) {
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	one := []Parameter{{Name: "p", Type: "VARCHAR"}}
	for _, c := range []struct {
		why    string
		sql    string
		params []Parameter
	}{
		// An identifier never reaches the result.
		{"identifier in the result", `SELECT l.email, count(*) AS n FROM crm l GROUP BY ALL`, nil},
		{"identifier renamed", `SELECT l.email AS segment, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"quoted, in another case", `SELECT "l"."EMAIL" AS e, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"substring of an identifier", `SELECT substring(l.email, 1, 3) AS p, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"left of an identifier", `SELECT left(l.email, 2) AS p, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"hash of an identifier", `SELECT hash(l.email) AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"md5 of an identifier", `SELECT md5(l.email) AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"lower of an identifier", `SELECT lower(l.email) AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"identifier concatenated", `SELECT l.email || '' AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"identifier cast", `SELECT CAST(l.email AS BLOB) AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"identifier coalesced", `SELECT coalesce(l.email, 'x') AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"identifier in a CASE", `SELECT CASE WHEN l.email = 'x' THEN 1 ELSE 0 END AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"identifier tested for NULL", `SELECT l.email IS NULL AS h, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"list of identifiers", `SELECT list(l.email) AS ids FROM crm l`, nil},
		{"array_agg of identifiers", `SELECT array_agg(l.email) AS ids FROM crm l`, nil},
		{"string_agg of identifiers", `SELECT string_agg(l.email, ',') AS ids FROM crm l`, nil},
		{"min of an identifier", `SELECT min(l.email) AS m FROM crm l`, nil},
		{"max of an identifier", `SELECT l.segment, max(l.email) AS m FROM crm l GROUP BY 1`, nil},
		{"any_value of an identifier", `SELECT any_value(l.email) AS m FROM crm l`, nil},
		{"first of an identifier", `SELECT first(l.email) AS m FROM crm l`, nil},
		{"arg_min of an identifier", `SELECT arg_min(l.email, l.segment) AS m FROM crm l`, nil},
		{"mode of an identifier", `SELECT mode(l.email) AS m FROM crm l`, nil},
		{"histogram of identifiers", `SELECT histogram(l.email) AS m FROM crm l`, nil},
		{"median of an identifier", `SELECT median(l.email) AS m FROM crm l`, nil},
		{"identifier in a list function", `SELECT list_transform([l.email], x -> x) AS m, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"a bare sketch", `SELECT datasketch_theta(12, l.email) AS s FROM crm l`, nil},
		{"a sketch of an expression of an identifier", `SELECT datasketch_hll_estimate(datasketch_hll(12, l.email || 'x')) AS s FROM crm l`, nil},
		{"a counted expression of an identifier", `SELECT count(DISTINCT CASE WHEN l.segment = 'a' THEN l.email END) AS n FROM crm l`, nil},
		{"a counted function of an identifier", `SELECT count(DISTINCT substring(l.email, 1, 1)) AS n FROM crm l`, nil},
		{"grouping of an identifier", `SELECT grouping(l.email) AS g FROM crm l GROUP BY ROLLUP (l.segment)`, nil},
		{"identifier in GROUP BY", `SELECT count(*) AS n FROM crm l GROUP BY l.email`, nil},
		{"function of an identifier in GROUP BY", `SELECT count(*) AS n FROM crm l GROUP BY substring(l.email, 1, 1)`, nil},
		{"identifier in ROLLUP", `SELECT count(*) AS n FROM crm l GROUP BY ROLLUP (l.segment, l.email)`, nil},
		// Aggregates take a bare column: a CASE inside one picks out a part
		// of a group that no threshold counts.
		{"a CASE in a count", `SELECT count(CASE WHEN c.segment = 'vip' THEN 1 END) AS n FROM crm c JOIN views v ON c.email = v.email`, nil},
		{"a CASE in max", `SELECT count(DISTINCT v.email) AS reach, max(CASE WHEN c.segment = 'vip' THEN v.genre END) AS g
			FROM crm c JOIN views v ON c.email = v.email`, nil},
		{"a CASE in a measure's sum", `SELECT sum(CASE WHEN c.segment = 'vip' THEN v.minutes END) AS m FROM crm c JOIN views v ON c.email = v.email`, nil},
		{"nullif in a count", `SELECT count(nullif(c.segment, 'big')) AS n FROM crm c`, nil},
		{"arithmetic in sum", `SELECT sum(v.minutes * 0 + 1) AS n FROM views v`, nil},
		{"sum of a constant", `SELECT sum(1) AS n FROM crm c`, nil},
		{"max of a dimension", `SELECT count(*) AS n, max(v.genre) AS g FROM views v`, nil},
		{"min of a dimension", `SELECT c.region, min(c.segment) AS s, count(*) AS n FROM crm c GROUP BY 1`, nil},
		{"max of dimensions of both sides", `SELECT count(DISTINCT v.email) AS reach,
			max(c.segment || ':' || v.genre || ':' || CAST(v.day AS VARCHAR)) AS m FROM crm c JOIN views v ON c.email = v.email`, nil},
		{"a count of a dimension", `SELECT count(c.segment) AS n FROM crm c`, nil},
		{"a distinct count of a dimension", `SELECT count(DISTINCT v.genre) AS n FROM views v`, nil},
		{"a sketch of a dimension", `SELECT datasketch_hll_estimate(datasketch_hll(12, v.genre)) AS n FROM views v`, nil},
		{"a sketch too small", `SELECT datasketch_hll_estimate(datasketch_hll(4, v.email)) AS n FROM views v`, nil},
		{"a sketch too large", `SELECT datasketch_theta_estimate(datasketch_theta(26, v.email)) AS n FROM views v`, nil},
		{"too many FILTERs", `SELECT count(*) FILTER (WHERE v.day = DATE '2026-10-01') AS a, count(*) FILTER (WHERE v.day = DATE '2026-10-02') AS b,
			count(*) FILTER (WHERE v.day = DATE '2026-10-03') AS c, count(*) FILTER (WHERE v.day = DATE '2026-10-04') AS d,
			count(*) FILTER (WHERE v.day = DATE '2026-10-05') AS e FROM views v`, nil},
		// Groups are disjoint: a total less the groups shown is the groups
		// left out.
		{"ROLLUP", `SELECT c.segment, count(*) AS n FROM crm c GROUP BY ROLLUP (c.segment)`, nil},
		{"CUBE", `SELECT c.segment, c.region, count(*) AS n FROM crm c GROUP BY CUBE (c.segment, c.region)`, nil},
		{"GROUPING SETS", `SELECT c.segment, count(*) AS n FROM crm c GROUP BY GROUPING SETS ((c.segment), ())`, nil},
		{"grouping()", `SELECT c.segment, grouping(c.segment) AS g, count(*) AS n FROM crm c GROUP BY c.segment`, nil},
		{"identifier in WHERE", `SELECT count(*) AS n FROM crm l WHERE l.email = 'e1'`, nil},
		{"identifier compared to a parameter", `SELECT count(*) AS n FROM crm l WHERE l.email = $p`, one},
		{"identifier in LIKE", `SELECT count(*) AS n FROM crm l WHERE l.email LIKE 'a%'`, nil},
		{"identifier IS NOT NULL", `SELECT count(*) AS n FROM crm l WHERE l.email IS NOT NULL`, nil},
		{"identifier in FILTER", `SELECT count(*) FILTER (WHERE l.email < 'm') AS n FROM crm l`, nil},
		{"identifier in ORDER BY", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 ORDER BY l.email`, nil},
		// Joins equate identifiers of two datasets, and nothing else of them.
		{"identifier joined to a dimension", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = r.genre`, nil},
		{"identifier joined to a constant", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = r.email AND l.email = 'x'`, nil},
		{"identifier compared in a join", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = r.email AND l.email > 'm'`, nil},
		{"function of identifiers in a join", `SELECT count(*) AS n FROM crm l JOIN views r ON substring(l.email, 1, 1) = substring(r.email, 1, 1)`, nil},
		{"an ON that does not join its own sides", `SELECT w.genre, count(*) AS n FROM crm a JOIN views v ON a.email = v.email
			JOIN views w ON a.email = v.email GROUP BY 1`, nil},
		{"join with OR", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = r.email OR true`, nil},
		{"join without identifiers", `SELECT count(*) AS n FROM crm l JOIN views r ON l.segment = r.genre`, nil},
		{"join of a dataset with itself", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = l.email`, nil},
		{"comma join", `SELECT count(*) AS n FROM crm l, views r WHERE l.email = r.email`, nil},
		{"cross join", `SELECT count(*) AS n FROM crm l CROSS JOIN views r`, nil},
		{"natural join", `SELECT count(*) AS n FROM crm l NATURAL JOIN views r`, nil},
		{"join using", `SELECT count(*) AS n FROM crm l JOIN views r USING (email)`, nil},
		{"asof join", `SELECT count(*) AS n FROM crm l ASOF JOIN views r ON l.email = r.email AND l.region >= r.genre`, nil},
		{"positional join", `SELECT count(*) AS n FROM crm l POSITIONAL JOIN views r`, nil},
		{"semi join", `SELECT count(*) AS n FROM crm l SEMI JOIN views r ON l.email = r.email`, nil},
		{"anti join", `SELECT count(*) AS n FROM crm l ANTI JOIN views r ON l.email = r.email`, nil},
		{"lateral", `SELECT count(*) AS n FROM crm l, LATERAL (SELECT r.email FROM views r WHERE r.email = l.email) x`, nil},
		// Only datasets, by their names in the room.
		{"a table by its name", `SELECT count(*) AS n FROM marts.crm_audience`, nil},
		{"a qualified table", `SELECT count(*) AS n FROM lake.marts.crm_audience`, nil},
		{"not a dataset", `SELECT count(*) AS n FROM crm_audience`, nil},
		{"a file", `SELECT count(*) AS n FROM 'crm.parquet'`, nil},
		{"a table function", `SELECT count(*) AS n FROM read_parquet('crm.parquet')`, nil},
		{"a subquery in FROM", `SELECT count(*) AS n FROM (SELECT * FROM crm) c`, nil},
		{"a subquery in the result", `SELECT (SELECT count(*) FROM crm) AS n FROM crm l`, nil},
		{"a subquery in WHERE", `SELECT count(*) AS n FROM crm l WHERE l.segment IN (SELECT email FROM crm)`, nil},
		{"EXISTS", `SELECT count(*) AS n FROM crm l WHERE EXISTS (SELECT 1 FROM views r WHERE r.email = 'x')`, nil},
		{"a CTE", `WITH x AS (SELECT * FROM crm) SELECT count(*) AS n FROM x`, nil},
		{"UNION", `SELECT count(*) AS n FROM crm l UNION ALL SELECT count(*) FROM views r`, nil},
		{"VALUES", `VALUES (1)`, nil},
		{"two statements", `SELECT count(*) AS n FROM crm l; SELECT 1`, nil},
		{"not a query", `DELETE FROM crm`, nil},
		{"time travel", `SELECT count(*) AS n FROM crm AT (VERSION => 1)`, nil},
		{"a sample", `SELECT count(*) AS n FROM crm l USING SAMPLE 10%`, nil},
		{"renamed columns", `SELECT count(*) AS n FROM crm AS l(a, b, c)`, nil},
		// Columns are written alias.column and are in the policy.
		{"an unqualified column", `SELECT segment, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"a column by the table's name", `SELECT crm.segment, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"a struct field", `SELECT l.segment.x AS s, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"star", `SELECT * FROM crm`, nil},
		{"a table's star", `SELECT l.* FROM crm l`, nil},
		{"COLUMNS", `SELECT COLUMNS('e.*') FROM crm l`, nil},
		{"a column outside the policy", `SELECT l.name, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"a positional reference", `SELECT #1 AS x, count(*) AS n FROM crm l GROUP BY 1`, nil},
		// Other columns only in their policy's aggregates.
		{"a measure in the result", `SELECT r.minutes, count(*) AS n FROM views r GROUP BY 1`, nil},
		{"a measure in WHERE", `SELECT count(*) AS n FROM views r WHERE r.minutes > 100`, nil},
		{"an aggregate the policy does not allow", `SELECT avg(r.minutes) AS m FROM views r`, nil},
		{"max of a measure", `SELECT max(r.minutes) AS m FROM views r`, nil},
		{"an expression of a measure", `SELECT sum(r.minutes * 2) AS m FROM views r`, nil},
		{"DISTINCT in sum", `SELECT sum(DISTINCT r.minutes) AS m FROM views r`, nil},
		// Results are aggregated, named, and nothing more.
		{"not aggregated", `SELECT l.segment FROM crm l`, nil},
		{"an unnamed column", `SELECT l.segment, count(*) FROM crm l GROUP BY 1`, nil},
		{"two columns of one name", `SELECT l.segment AS n, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"a hidden column's name", `SELECT count(*) AS __igloo_k_0 FROM crm l`, nil},
		{"DISTINCT", `SELECT DISTINCT l.segment FROM crm l`, nil},
		{"HAVING", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 HAVING count(*) > 1`, nil},
		{"QUALIFY", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 QUALIFY n > 1`, nil},
		{"a window function", `SELECT l.segment, count(*) OVER () AS n FROM crm l GROUP BY 1`, nil},
		{"row_number", `SELECT row_number() OVER () AS n, count(*) AS c FROM crm l`, nil},
		{"ORDER BY an expression", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 ORDER BY count(DISTINCT l.email)`, nil},
		{"ORDER BY ALL", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 ORDER BY ALL`, nil},
		{"OFFSET", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 LIMIT 5 OFFSET 2`, nil},
		{"a LIMIT parameter", `SELECT l.segment, count(*) AS n FROM crm l GROUP BY 1 LIMIT $p`, one},
		{"an aggregate in WHERE", `SELECT count(*) AS n FROM crm l WHERE count(*) > 1`, nil},
		{"an ORDER BY in an aggregate", `SELECT count(DISTINCT l.email ORDER BY l.segment) AS n FROM crm l`, nil},
		// Parameters are declared, named, typed, and in WHERE only.
		{"a parameter in the result", `SELECT $p AS x, count(*) AS n FROM crm l`, one},
		{"a parameter in GROUP BY", `SELECT count(*) AS n FROM crm l GROUP BY l.segment = $p`, one},
		{"a parameter in a join", `SELECT count(*) AS n FROM crm l JOIN views r ON l.email = r.email AND r.genre = $p`, one},
		{"a parameter in FILTER", `SELECT count(*) FILTER (WHERE l.segment = $p) AS n FROM crm l`, one},
		{"a positional parameter", `SELECT count(*) AS n FROM crm l WHERE l.segment = $1`, nil},
		{"an undeclared parameter", `SELECT count(*) AS n FROM crm l WHERE l.segment = $q`, one},
		{"an unused parameter", `SELECT count(*) AS n FROM crm l`, one},
		{"a parameter of an unsafe type", `SELECT count(*) AS n FROM crm l WHERE l.segment = $p`, []Parameter{{Name: "p", Type: "VARCHAR); DROP TABLE x; --"}}},
		{"a parameter of a list type", `SELECT count(*) AS n FROM crm l WHERE l.segment = $p`, []Parameter{{Name: "p", Type: "VARCHAR[]"}}},
		{"a parameter's condition on two datasets", `SELECT count(*) AS n FROM crm c JOIN views v ON c.email = v.email
			WHERE c.segment = $p OR v.genre = $p`, one},
		{"a parameter's condition on no dataset", `SELECT count(*) AS n FROM crm c WHERE $p = 'x'`, one},
		// Only functions that read nothing but their arguments.
		{"random", `SELECT l.segment, random() AS r, count(*) AS n FROM crm l GROUP BY 1, 2`, nil},
		{"current_setting", `SELECT count(*) AS n FROM crm l WHERE l.segment = current_setting('search_path')`, nil},
		{"getvariable", `SELECT count(*) AS n FROM crm l WHERE l.segment = getvariable('x')`, nil},
		{"query", `SELECT count(*) AS n FROM crm l WHERE l.segment IN (SELECT * FROM query('SELECT email FROM crm'))`, nil},
		{"read_text", `SELECT count(*) AS n FROM crm l WHERE l.segment = (read_text('/etc/passwd')).content`, nil},
		{"a schema-qualified function", `SELECT main.count(*) AS n FROM crm l`, nil},
		{"a lambda", `SELECT list_filter([l.segment], x -> true) AS s, count(*) AS n FROM crm l GROUP BY 1`, nil},
		{"list extraction", `SELECT [l.segment][1] AS s, count(*) AS n FROM crm l GROUP BY 1`, nil},
	} {
		_, err := p.Check(ctx, Template{SQL: c.sql, Parameters: c.params}, testDatasets())
		if !IsInvalid(err) {
			t.Errorf("%s: %s: want refused, got %v", c.why, c.sql, err)
		}
	}
}

func TestPolicyCheck(t *testing.T) {
	cols := []string{"Email", "segment", "spend", "region"}
	p := Policy{Join: []string{"email"}, Group: []string{"SEGMENT"}, Aggregates: []Aggregate{{"SUM", "spend"}, {"sum", "spend"}}}
	if err := p.Check(cols); err != nil {
		t.Fatal(err)
	}
	if p.Join[0] != "Email" || p.Group[0] != "segment" || len(p.Aggregates) != 1 || p.Aggregates[0].Function != "sum" ||
		p.Threshold != DefaultThreshold || p.RunsPerDay != DefaultRunsPerDay {
		t.Errorf("normalized: %+v", p)
	}
	for _, bad := range []Policy{
		{},
		{Join: []string{"nope"}},
		{Join: []string{"email"}, Group: []string{"email"}},
		{Join: []string{"email", "EMAIL"}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "email"}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"list", "spend"}}},
		{Join: []string{"email"}, Threshold: 5},
		{Join: []string{"email"}, MaxRows: MaxResultRows + 1},
		{Join: []string{"email"}, Noise: -1},
		{Join: []string{"email"}, RunsPerDay: -1},
	} {
		if err := bad.Check(cols); !IsInvalid(err) {
			t.Errorf("%+v: want refused, got %v", bad, err)
		}
	}
}

// runPlan runs a plan as a run does, on conn, and returns the result's
// rows as text, or the population check's refusal.
func runPlan(t *testing.T, conn *duckdb.Conn, pl *Plan) ([]string, error) {
	t.Helper()
	ctx := context.Background()
	args := make([]driver.NamedValue, len(pl.Args))
	for i, a := range pl.Args {
		args[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	if pl.Check != "" {
		if err := pl.Narrowing(checkCounts(t, conn, pl, args)); err != nil {
			return nil, err
		}
	}
	if pl.Seed != "" {
		args = append(args, driver.NamedValue{Ordinal: len(args) + 1, Value: pl.Seed})
	}
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE TEMP TABLE groups AS "+pl.Groups, args); err != nil {
		t.Fatalf("%s: %v", pl.Groups, err)
	}
	return queryRows(t, conn, pl.Keep("groups"), nil), nil
}

// checkCounts runs a plan's population check.
func checkCounts(t *testing.T, conn *duckdb.Conn, pl *Plan, args []driver.NamedValue) []int64 {
	t.Helper()
	var n []int64
	for _, s := range strings.Split(queryRows(t, conn, pl.Check, args)[0], "|") {
		var v int64
		fmt.Sscan(s, &v)
		n = append(n, v)
	}
	return n
}

// mustRun runs a plan that the population check must let through.
func mustRun(t *testing.T, conn *duckdb.Conn, pl *Plan) []string {
	t.Helper()
	rows, err := runPlan(t, conn, pl)
	if err != nil {
		t.Fatalf("population check: %v", err)
	}
	return rows
}

func queryRows(t *testing.T, conn *duckdb.Conn, query string, args []driver.NamedValue) []string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), query, args)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	dest := make([]driver.Value, len(rows.Columns()))
	var out []string
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatal(err)
		}
		parts := make([]string, len(dest))
		for i, v := range dest {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
}

// fixture fills an in-memory DuckDB with the datasets of testDatasets:
// 400 people in the agency's CRM, in segments of 200, 120, 60 and 20, and
// a TV maker that saw the first 300 of them.
func fixture(t *testing.T) *duckdb.Conn {
	conn := openDuckDB(t)
	for _, q := range []string{
		"CREATE SCHEMA marts",
		"CREATE SCHEMA partners",
		`CREATE TABLE marts.crm_audience AS SELECT 'e' || i AS email,
			CASE WHEN i < 200 THEN 'big' WHEN i < 320 THEN 'medium' WHEN i < 380 THEN 'small' ELSE 'tiny' END AS segment,
			CASE WHEN i % 2 = 0 THEN 'east' ELSE 'west' END AS region, 'name' || i AS name
			FROM range(400) r(i)`,
		`CREATE TABLE partners.tvco_views AS SELECT 'e' || i AS email, 'd' || i AS device,
			CASE WHEN i % 3 = 0 THEN 'news' ELSE 'sports' END AS genre, DATE '2026-10-01' + CAST(i % 10 AS INTEGER) AS day,
			CASE WHEN i % 4 = 0 THEN 'control' ELSE 'exposed' END AS grp, i % 7 AS minutes
			FROM range(300) r(i)`,
	} {
		if _, err := conn.ExecContext(context.Background(), q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	return conn
}

func TestThresholdSuppression(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	overlap := Template{SQL: `SELECT l.segment, count(DISTINCT l.email) AS audience, count(DISTINCT r.email) AS overlap
		FROM crm l LEFT JOIN views r ON l.email = r.email GROUP BY ALL ORDER BY segment`}
	pl, err := p.Plan(ctx, overlap, testDatasets(), Databases{"memory", "memory"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// big: 200 seen; medium: 100 of 120 seen; small: none of 60 seen;
	// tiny: 20 people. The thresholds are 50: small has no overlap to
	// count, tiny too few people.
	if rows := mustRun(t, conn, pl); strings.Join(rows, ",") != "big|200|200,medium|120|100" {
		t.Errorf("overlap: %v", rows)
	}
	if strings.Contains(pl.Groups, "tvco_views") == false || !strings.Contains(pl.Groups, `"system".main.count`) {
		t.Errorf("the run reads the dataset's table with DuckDB's own functions: %s", pl.Groups)
	}

	// A threshold applies within each FILTER too: the control group of
	// news is too small.
	lift := Template{SQL: `SELECT v.genre, count(DISTINCT v.email) FILTER (WHERE v.grp = 'exposed') AS exposed,
		count(DISTINCT v.email) FILTER (WHERE v.grp = 'control') AS control
		FROM views v GROUP BY v.genre ORDER BY 1`}
	datasets := testDatasets()
	datasets[1].Policy.Threshold = 30
	pl, err = p.Plan(ctx, lift, datasets, Databases{"memory", "memory"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// news: 100 people, 25 of them control; sports: 200, 50 control.
	if rows := mustRun(t, conn, pl); strings.Join(rows, ",") != "sports|150|50" {
		t.Errorf("lift: %v", rows)
	}

	// MAX ROWS cuts a result short, and says so with one more row.
	datasets = testDatasets()
	datasets[0].Policy.Threshold, datasets[0].Policy.MaxRows = 10, 1
	pl, err = p.Plan(ctx, Template{SQL: `SELECT c.region, count(*) AS n FROM crm c GROUP BY 1 ORDER BY 1`}, datasets, Databases{"memory", "memory"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows := mustRun(t, conn, pl); len(rows) != 2 || pl.Cap != 1 {
		t.Errorf("a capped result has one row more than its cap, to tell: %v", rows)
	}
}

func TestNarrowingAndParameters(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	reach := Template{SQL: `SELECT v.genre, count(DISTINCT v.device) AS reach FROM views v
		WHERE v.day BETWEEN $start_day AND $end_day GROUP BY 1 ORDER BY 1`, Parameters: dateParams}
	pl, err := p.Plan(ctx, reach, testDatasets(), Databases{"memory", "memory"}, map[string]any{"start_day": "2026-10-01", "end_day": "2026-10-05"})
	if err != nil {
		t.Fatal(err)
	}
	if rows := mustRun(t, conn, pl); strings.Join(rows, ",") != "news|50,sports|100" {
		t.Errorf("reach: %v", rows)
	}
	for _, c := range []struct {
		start, end string
	}{
		// One day of ten keeps 30 people of 300: too few.
		{"2026-10-01", "2026-10-01"},
		// Nine days of ten leave out 30: too few.
		{"2026-10-01", "2026-10-09"},
	} {
		pl, err := p.Plan(ctx, reach, testDatasets(), Databases{"memory", "memory"}, map[string]any{"start_day": c.start, "end_day": c.end})
		if err != nil {
			t.Fatal(err)
		}
		args := []driver.NamedValue{{Ordinal: 1, Value: pl.Args[0]}, {Ordinal: 2, Value: pl.Args[1]}}
		var counts []int64
		for _, s := range strings.Split(queryRows(t, conn, pl.Check, args)[0], "|") {
			var v int64
			fmt.Sscan(s, &v)
			counts = append(counts, v)
		}
		var refused *RefusedError
		if err := pl.Narrowing(counts); !errors.As(err, &refused) {
			t.Errorf("%s to %s (%v): want refused, got %v", c.start, c.end, counts, err)
		}
	}
	// Parameters are values, never SQL: this one matches no segment.
	bySegment := Template{SQL: `SELECT c.region, count(*) AS n FROM crm c WHERE c.segment = $segment GROUP BY 1`,
		Parameters: []Parameter{{Name: "segment", Type: "VARCHAR"}}}
	pl, err = p.Plan(ctx, bySegment, testDatasets(), Databases{"memory", "memory"}, map[string]any{"segment": "big' OR 'a' = 'a"})
	if err != nil {
		t.Fatal(err)
	}
	if got := queryRows(t, conn, pl.Check, []driver.NamedValue{{Ordinal: 1, Value: pl.Args[0]}}); len(got) != 1 || got[0] != "400|0" {
		t.Errorf("a value with SQL in it: %v", got)
	}
	if rows := mustRun(t, conn, pl); len(rows) != 0 {
		t.Errorf("a value with SQL in it: %v", rows)
	}
	if _, err := p.Plan(ctx, reach, testDatasets(), Databases{"memory", "memory"}, map[string]any{"start_day": "2026-10-01"}); !IsInvalid(err) {
		t.Errorf("a missing parameter: %v", err)
	}
	if _, err := p.Plan(ctx, reach, testDatasets(), Databases{"memory", "memory"}, map[string]any{"start_day": "x", "end_day": "y", "other": 1}); !IsInvalid(err) {
		t.Errorf("an unknown parameter: %v", err)
	}
}

func TestNoise(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	datasets := testDatasets()
	datasets[0].Policy.Noise = 0.5
	plan := func() *Plan {
		pl, err := p.Plan(ctx, Template{SQL: `SELECT c.segment, count(DISTINCT c.email) AS people,
			round(count(*) / count(DISTINCT c.email), 3) AS ratio FROM crm c GROUP BY 1 ORDER BY 1`}, datasets, Databases{"memory", "memory"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return pl
	}
	pl := plan()
	// Two different counts, one of them twice; the seed is a parameter, in
	// no statement's text.
	if pl.Noise != 0.5 || !strings.Contains(pl.Groups, "sha256") || len(pl.Seed) != 64 || strings.Contains(pl.Groups, pl.Seed) || pl.Cost != 2 {
		t.Fatalf("noise: %v, cost %d, seed %q: %s", pl.Noise, pl.Cost, pl.Seed, pl.Groups)
	}
	exact := map[string]int{"big": 200, "medium": 120, "small": 60}
	differs := false
	// Each plan has a seed of its own, as each run does.
	for range 5 {
		rows := mustRun(t, conn, plan())
		if len(rows) != 3 {
			t.Fatalf("rows: %v", rows)
		}
		for _, r := range rows {
			parts := strings.Split(r, "|")
			var n int
			fmt.Sscan(parts[1], &n)
			// Laplace noise of scale 2 is within 40 but for one draw in
			// 485 million.
			if want := exact[parts[0]]; n < want-40 || n > want+40 {
				t.Errorf("%s: %d people, %d exactly", parts[0], n, want)
			}
			differs = differs || n != exact[parts[0]]
		}
	}
	if !differs {
		t.Error("noisy counts are exact")
	}
}
