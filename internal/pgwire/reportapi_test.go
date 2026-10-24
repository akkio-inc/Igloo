package pgwire

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// The report API's tests, and its parity harness: each spec's result is
// compared, through igloo.audience_profile_check, with SQL written the way
// Acme's reports count, independently of the API's own SQL.

var reportFixture = []string{
	"CREATE TABLE lake.public.audience_members AS SELECT 'p' || (i * 7 % 97) AS person_id, CASE WHEN i % 3 = 0 THEN 'a1' ELSE 'a2' END AS audience_id, CAST(1 + i % 2 AS INTEGER) AS ver FROM range(400) r(i)",
	"INSERT INTO lake.public.audience_members VALUES (NULL, 'a1', 2)",
	"CREATE TABLE lake.public.audience_revisions AS SELECT * FROM (VALUES ('a1', 1), ('a1', 2), ('a2', 1)) v(audience_id, ver)",
	"CREATE TABLE lake.public.attributes AS SELECT 'p' || i AS person_id, CAST(i % 40 AS VARCHAR) AS household_id, " +
		"CASE i % 5 WHEN 0 THEN 'NY' WHEN 1 THEN 'CA' WHEN 2 THEN '' WHEN 3 THEN NULL ELSE ' ' END AS state, CAST(i % 3 AS DECIMAL(3,0)) AS techaffinity, " +
		"CASE WHEN i % 4 = 0 THEN NULL ELSE (i % 7) - 2 END AS movie, i % 2 AS music FROM range(80) r(i)",
	"CREATE TABLE lake.public.survey AS SELECT CAST(i % 10 AS BIGINT) AS resp_id, 'answer ' || i AS answer FROM range(20) r(i)",
	"CREATE VIEW lake.public.v_attributes (person_id, household_id, state, techaffinity, movie, music, survey_resp_id, survey_answer) AS " +
		"SELECT s.*, sv.resp_id, sv.answer FROM lake.public.attributes AS s LEFT JOIN lake.public.survey AS sv ON CAST(s.household_id AS BIGINT) = sv.resp_id",
	"CREATE TABLE lake.public.media AS SELECT 'p' || (i % 120) AS person_id, CASE i % 6 WHEN 0 THEN 'drama' WHEN 1 THEN 'news' WHEN 2 THEN '' WHEN 3 THEN NULL ELSE 'sport' END AS genre, " +
		"CASE WHEN i % 5 = 0 THEN NULL ELSE i % 2 = 0 END AS live, CASE WHEN i % 9 = 0 THEN NULL ELSE CAST(i * 3 AS INTEGER) END AS total_seconds, " +
		"DATE '2026-01-01' + CAST(i % 50 AS INTEGER) AS viewed_date FROM range(900) r(i)",
	"CREATE TABLE lake.public.browsing AS SELECT 'p' || (i % 90) AS person_id, CASE i % 4 WHEN 0 THEN ['cars', 'news'] WHEN 1 THEN ['', 'cars', 'cars'] WHEN 2 THEN NULL ELSE [] END AS keywords, " +
		"CASE WHEN i % 7 = 0 THEN NULL ELSE DATE '2026-02-01' + CAST(i % 9 AS INTEGER) END AS visited_date FROM range(600) r(i)",
	"CREATE TABLE lake.public.segments AS SELECT 'p' || (i % 110) AS person_id, CASE WHEN i % 3 = 0 THEN 'SEG_HH_' || (i % 5) ELSE 'SEGXHH_' || (i % 4) END AS segment_name FROM range(1500) r(i)",
}

// reportCohortsSQL are the reports' cohorts: an audience's latest version
// and the general population, through the attributes view.
const reportCohortsSQL = `AUDIENCE AS (SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = (SELECT max(ver) FROM audience_revisions WHERE audience_id = 'a1')),
BASE_POPULATION AS (SELECT person_id FROM v_attributes),
COHORT_MEMBERS AS (SELECT 'AUDIENCE' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM AUDIENCE WHERE person_id IS NOT NULL)
  UNION ALL SELECT 'BASE_POPULATION' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM BASE_POPULATION WHERE person_id IS NOT NULL)),
COHORT_TOTALS AS (SELECT cohort, count(DISTINCT person_id) AS cohort_size FROM COHORT_MEMBERS GROUP BY cohort)`

func reportCohorts() []any {
	return []any{
		map[string]any{"label": "AUDIENCE", "table": "audience_members", "member": "person_id", "where": map[string]any{"audience_id": "a1"},
			"version": map[string]any{"column": "ver", "latest_from": map[string]any{"table": "audience_revisions", "where": map[string]any{"audience_id": "a1"}}}},
		map[string]any{"label": "BASE_POPULATION", "table": "v_attributes", "member": "person_id"},
	}
}

func specLiteral(t *testing.T, spec map[string]any) string {
	t.Helper()
	b, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return quoteLiteral(string(b))
}

func startReportServer(t *testing.T) *catalogServer {
	t.Helper()
	cs := startCatalogServer(t)
	for _, q := range reportFixture {
		mustExec(t, cs.admin, q)
	}
	mustExec(t, cs.admin, "SET search_path = 'lake.public'")
	return cs
}

// reportParity checks that spec's result is sql's, and that a result that
// is not, sql's counts each one more, is told apart.
func reportParity(t *testing.T, conn *pgx.Conn, name string, spec map[string]any, sql string) {
	t.Helper()
	lit := specLiteral(t, spec)
	if n := texts(t, conn, "SELECT count(*) FROM igloo.audience_profile("+lit+")"); len(n) != 1 || n[0] == "0" {
		t.Errorf("%s: the API returned %v rows", name, n)
	}
	if diff := texts(t, conn, "SELECT * FROM igloo.audience_profile_check("+lit+", "+quoteLiteral(sql)+")"); len(diff) != 0 {
		api := texts(t, conn, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
		t.Errorf("%s: the API and the SQL differ:\n%s\nthe API's rows:\n%s", name, strings.Join(diff, "\n"), strings.Join(api, "\n"))
	}
	wrong := "SELECT dimension, value, cohort, people + 1 AS people FROM (\n" + sql + "\n)"
	if diff := texts(t, conn, "SELECT count(*) FROM igloo.audience_profile_check("+lit+", "+quoteLiteral(wrong)+")"); len(diff) != 1 || diff[0] == "0" {
		t.Errorf("%s: the check did not tell apart counts one more: %v", name, diff)
	}
	// The API's SQL, run as written, gives the same.
	text := texts(t, conn, "SELECT igloo.audience_profile_sql("+lit+")")
	if len(text) != 1 {
		t.Fatalf("%s: audience_profile_sql: %v", name, text)
	}
	viaSQL := texts(t, conn, "SELECT * FROM ("+text[0]+") ORDER BY ALL")
	viaAPI := texts(t, conn, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
	if !slices.Equal(viaSQL, viaAPI) {
		t.Errorf("%s: the API's rows are not those of its SQL:\n%q\n%q", name, viaAPI, viaSQL)
	}
}

// TestReportAPIParity counts as the segments, signals and behaviors reports
// do, and gets the rows their SQL gets.
func TestReportAPIParity(t *testing.T) {
	cs := startReportServer(t)
	conn := cs.admin

	reportParity(t, conn, "segments", map[string]any{
		"version": 1, "cohorts": reportCohorts(), "totals": true,
		"source":     map[string]any{"table": "segments", "member": "person_id", "where": map[string]any{"segment_name": map[string]any{"like": "SEG_HH_%"}}},
		"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}},
	}, `WITH `+reportCohortsSQL+`
SELECT 'SEGMENT' AS dimension, S.segment_name AS value, M.cohort AS cohort, false AS is_total, count(DISTINCT S.person_id) AS people, count(*) AS "rows", Z.cohort_size AS cohort_size
FROM segments AS S JOIN COHORT_MEMBERS AS M ON M.person_id = S.person_id JOIN COHORT_TOTALS AS Z ON Z.cohort = M.cohort WHERE S.segment_name LIKE 'SEG_HH_%' GROUP BY ALL
UNION ALL
SELECT 'SEGMENT', NULL, M.cohort, true, count(DISTINCT S.person_id), count(*), Z.cohort_size
FROM segments AS S JOIN COHORT_MEMBERS AS M ON M.person_id = S.person_id JOIN COHORT_TOTALS AS Z ON Z.cohort = M.cohort WHERE S.segment_name LIKE 'SEG_HH_%' GROUP BY ALL`)

	media := `WITH ` + reportCohortsSQL + `,
B AS (SELECT md.*, cm.cohort FROM media AS md JOIN COHORT_MEMBERS AS cm ON cm.person_id = md.person_id),
E AS (SELECT person_id, cohort, total_seconds, 'GENRE' AS d, CAST(genre AS VARCHAR) AS v FROM B
  UNION ALL SELECT person_id, cohort, total_seconds, 'VIEWING_MODE', CASE WHEN live IS NULL THEN NULL WHEN live THEN 'LIVE' ELSE 'ON_DEMAND' END FROM B
  UNION ALL SELECT person_id, cohort, total_seconds, 'MEDIA_REACH', 'ANY_VIEWING' FROM B)
SELECT d AS dimension, v AS value, cohort, count(DISTINCT person_id) AS people, count(*) AS "rows",
  sum(COALESCE(total_seconds, 0)) AS seconds_sum, avg(COALESCE(total_seconds, 0)) AS seconds_avg,
  (SELECT min(viewed_date) FROM B) AS extent_min, (SELECT max(viewed_date) FROM B) AS extent_max
FROM E WHERE v IS NOT NULL AND trim(v) <> '' GROUP BY d, v, cohort`
	reportParity(t, conn, "media signals", map[string]any{
		"version": 1, "cohorts": reportCohorts(), "extent": "viewed_date",
		"source": map[string]any{"table": "media", "member": "person_id"},
		"dimensions": []any{
			map[string]any{"name": "GENRE", "column": "genre", "skip_blank": true},
			map[string]any{"name": "VIEWING_MODE", "column": "live", "map": map[string]any{"true": "LIVE", "false": "ON_DEMAND"}, "skip_blank": true},
			map[string]any{"name": "MEDIA_REACH", "constant": "ANY_VIEWING", "skip_blank": true},
		},
		"metrics": []any{map[string]any{"name": "seconds", "column": "total_seconds", "coalesce": 0}},
	}, media)

	reportParity(t, conn, "browsing signals", map[string]any{
		"version": 1, "cohorts": reportCohorts(),
		"source": map[string]any{"table": "browsing", "member": "person_id"},
		"dimensions": []any{
			map[string]any{"name": "KEYWORDS", "elements": "keywords", "skip_blank": true},
			map[string]any{"name": "BROWSING_REACH", "constant": "ANY_BROWSING"},
		},
		"metrics": []any{map[string]any{"name": "days", "distinct_with": "visited_date"}},
	}, `WITH `+reportCohortsSQL+`
SELECT 'KEYWORDS' AS dimension, k AS value, M.cohort AS cohort, count(DISTINCT b.person_id) AS people, count(*) AS "rows",
  count(DISTINCT CASE WHEN b.visited_date IS NULL THEN NULL ELSE (b.person_id, b.visited_date) END) AS days_distinct
FROM browsing AS b JOIN COHORT_MEMBERS AS M ON M.person_id = b.person_id, unnest(b.keywords) AS u(k) WHERE k IS NOT NULL AND trim(k) <> '' GROUP BY ALL
UNION ALL
SELECT 'BROWSING_REACH', 'ANY_BROWSING', M.cohort, count(DISTINCT b.person_id), count(*),
  count(DISTINCT CASE WHEN b.visited_date IS NULL THEN NULL ELSE (b.person_id, b.visited_date) END)
FROM browsing AS b JOIN COHORT_MEMBERS AS M ON M.person_id = b.person_id GROUP BY ALL`)

	reportParity(t, conn, "browsing signals, with totals and the extent", map[string]any{
		"version": 1, "cohorts": reportCohorts(), "totals": true, "extent": "visited_date",
		"source": map[string]any{"table": "browsing", "member": "person_id"},
		"dimensions": []any{
			map[string]any{"name": "KEYWORDS", "elements": "keywords", "skip_blank": true},
			map[string]any{"name": "BROWSING_REACH", "constant": "ANY_BROWSING"},
		},
		"metrics": []any{map[string]any{"name": "days", "distinct_with": "visited_date"}},
	}, `WITH `+reportCohortsSQL+`,
B AS (SELECT br.*, cm.cohort FROM browsing AS br JOIN COHORT_MEMBERS AS cm ON cm.person_id = br.person_id),
E AS (SELECT person_id, cohort, visited_date, 'KEYWORDS' AS d, k AS v FROM B, unnest(B.keywords) AS u(k) WHERE k IS NOT NULL AND trim(k) <> ''
  UNION ALL SELECT person_id, cohort, visited_date, 'BROWSING_REACH', 'ANY_BROWSING' FROM B)
SELECT d AS dimension, v AS value, cohort, false AS is_total, count(DISTINCT person_id) AS people, count(*) AS "rows",
  count(DISTINCT CASE WHEN visited_date IS NULL THEN NULL ELSE (person_id, visited_date) END) AS days_distinct,
  (SELECT min(visited_date) FROM B) AS extent_min, (SELECT max(visited_date) FROM B) AS extent_max FROM E GROUP BY d, v, cohort
UNION ALL
SELECT d, NULL, cohort, true, count(DISTINCT person_id), count(*), count(DISTINCT CASE WHEN visited_date IS NULL THEN NULL ELSE (person_id, visited_date) END),
  (SELECT min(visited_date) FROM B), (SELECT max(visited_date) FROM B) FROM E GROUP BY d, cohort`)

	// Through the view; entries of one name, and two of one (name, group,
	// value), which count together.
	interest := func(name, constant, metric string) map[string]any {
		return map[string]any{"name": name, "group": "INTEREST", "constant": constant, "metric": metric, "metric_where": map[string]any{"gt": 0}}
	}
	reportParity(t, conn, "attribute signals", map[string]any{
		"version": 1, "cohorts": reportCohorts(),
		"source": map[string]any{"table": "v_attributes", "member": "person_id"},
		"dimensions": []any{
			map[string]any{"name": "STATE", "group": "REACH_CONTEXT", "column": "state", "skip_blank": true},
			interest("TECHNOLOGY", "TECH_AFFINITY", "techaffinity"),
			interest("ENTERTAINMENT", "MOVIES", "movie"),
			interest("ENTERTAINMENT", "MUSIC", "music"),
			interest("ENTERTAINMENT", "MUSIC", "movie"),
		},
	}, `WITH `+reportCohortsSQL+`,
A AS (SELECT a.*, M.cohort FROM v_attributes AS a JOIN COHORT_MEMBERS AS M ON M.person_id = a.person_id),
E AS (SELECT person_id, cohort, 'STATE' AS d, 'REACH_CONTEXT' AS g, CAST(state AS VARCHAR) AS v, CAST(NULL AS DOUBLE) AS m, false AS h FROM A WHERE state IS NOT NULL AND trim(state) <> ''
  UNION ALL SELECT person_id, cohort, 'TECHNOLOGY', 'INTEREST', 'TECH_AFFINITY', CAST(techaffinity AS DOUBLE), true FROM A WHERE CAST(techaffinity AS DOUBLE) > 0
  UNION ALL SELECT person_id, cohort, 'ENTERTAINMENT', 'INTEREST', 'MOVIES', CAST(movie AS DOUBLE), true FROM A WHERE CAST(movie AS DOUBLE) > 0
  UNION ALL SELECT person_id, cohort, 'ENTERTAINMENT', 'INTEREST', 'MUSIC', CAST(music AS DOUBLE), true FROM A WHERE CAST(music AS DOUBLE) > 0
  UNION ALL SELECT person_id, cohort, 'ENTERTAINMENT', 'INTEREST', 'MUSIC', CAST(movie AS DOUBLE), true FROM A WHERE CAST(movie AS DOUBLE) > 0)
SELECT d AS dimension, g AS dim_group, v AS value, cohort, count(DISTINCT person_id) AS people, count(*) AS "rows",
  sum(m) AS metric_sum, CASE WHEN bool_or(h) THEN count(m) END AS metric_count, avg(m) AS metric_avg FROM E GROUP BY ALL`)

	reportParity(t, conn, "cohort sizes", map[string]any{"version": 1, "cohorts": reportCohorts(), "dimensions": []any{}},
		`WITH `+reportCohortsSQL+`
SELECT CAST(NULL AS VARCHAR) AS dimension, CAST(NULL AS VARCHAR) AS value, cohort, true AS is_total, cohort_size AS people, cohort_size FROM COHORT_TOTALS`)

	reportParity(t, conn, "a cohort given as SQL", map[string]any{
		"version": 1, "cohorts": []any{map[string]any{"label": "NY", "sql": "SELECT person_id FROM attributes WHERE state = 'NY' -- New York"}},
		"source":     map[string]any{"table": "segments", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}},
	}, `SELECT 'SEGMENT' AS dimension, s.segment_name AS value, 'NY' AS cohort, count(DISTINCT s.person_id) AS people
FROM segments AS s WHERE s.person_id IN (SELECT person_id FROM attributes WHERE state = 'NY') GROUP BY ALL`)
}

// TestReportAPIForms: the versions, the call in a common table expression
// and under DESCRIBE, and specs the API refuses.
func TestReportAPIForms(t *testing.T) {
	cs := startReportServer(t)
	conn := cs.admin
	ctx := context.Background()
	if got := texts(t, conn, "SELECT igloo.report_api_versions()"); len(got) != 1 || got[0] != "[1]" {
		t.Errorf("report_api_versions: %q", got)
	}
	spec := map[string]any{"version": 1, "cohorts": reportCohorts(), "source": map[string]any{"table": "segments", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}}
	lit := specLiteral(t, spec)
	all := texts(t, conn, "SELECT count(*) FROM igloo.audience_profile("+lit+")")
	if got := texts(t, conn, "WITH S AS (SELECT * FROM igloo.audience_profile("+lit+")), T AS (SELECT 1 AS x) SELECT count(*) FROM S, T"); !slices.Equal(got, all) {
		t.Errorf("in a CTE: %q, want %q", got, all)
	}
	// In Snowflake's dialect too, as a table function.
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	if got := texts(t, conn, "SELECT count(*) FROM TABLE(igloo.audience_profile("+lit+"))"); !slices.Equal(got, all) {
		t.Errorf("in Snowflake's dialect: %q, want %q", got, all)
	}
	mustExec(t, conn, "SET sql_dialect = 'duckdb'")
	var cols []string
	for _, r := range texts(t, conn, "DESCRIBE SELECT * FROM igloo.audience_profile("+lit+") AS p") {
		f := strings.Split(r, "|")
		cols = append(cols, f[0]+" "+f[1])
	}
	want := []string{"dimension VARCHAR", "dim_group VARCHAR", "value VARCHAR", "cohort VARCHAR", "is_total BOOLEAN", "people BIGINT", "rows BIGINT",
		"cohort_size BIGINT", "metric_sum DOUBLE", "metric_count BIGINT", "metric_avg DOUBLE", "path VARCHAR"}
	if !slices.Equal(cols, want) {
		t.Errorf("DESCRIBE:\n%q\nwant\n%q", cols, want)
	}

	many := make([]any, 9)
	for i := range many {
		many[i] = map[string]any{"label": strings.Repeat("c", i+1), "table": "segments", "member": "person_id"}
	}
	for _, c := range []struct{ name, call string }{
		{"no version", "igloo.audience_profile('{\"cohorts\": []}')"},
		{"version 2", "igloo.audience_profile('{\"version\": 2}')"},
		{"an unknown field", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "cohorts": reportCohorts(), "dimensions": []any{}, "colour": "blue"}) + ")"},
		{"nine cohorts", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "cohorts": many, "dimensions": []any{}}) + ")"},
		{"a spec over 64 KiB", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "cohorts": reportCohorts(), "dimensions": []any{}, "pad": strings.Repeat("x", 70000)}) + ")"},
		{"a metric whose columns are the result's", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "cohorts": reportCohorts(),
			"source": map[string]any{"table": "media", "member": "person_id"}, "dimensions": []any{map[string]any{"name": "G", "column": "genre"}},
			"metrics": []any{map[string]any{"name": "metric", "column": "total_seconds"}}}) + ")"},
		{"a cohort of two columns", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "dimensions": []any{},
			"cohorts": []any{map[string]any{"label": "x", "sql": "SELECT person_id, state FROM attributes"}}}) + ")"},
		{"a cohort that is no SELECT", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "dimensions": []any{},
			"cohorts": []any{map[string]any{"label": "x", "sql": "DELETE FROM attributes"}}}) + ")"},
		{"a spec that is no literal", "igloo.audience_profile(lower('{}'))"},
		{"an unknown operator", "igloo.audience_profile(" + specLiteral(t, map[string]any{"version": 1, "cohorts": []any{
			map[string]any{"label": "x", "table": "segments", "member": "person_id", "where": map[string]any{"segment_name": map[string]any{"near": 1}}}}, "dimensions": []any{}}) + ")"},
	} {
		_, err := conn.Exec(ctx, "SELECT * FROM "+c.call)
		if pgCode(err) != "22023" {
			t.Errorf("%s: %v, want 22023", c.name, err)
		}
	}
}

// TestReportAPIGovernance: a call is allowed and refused exactly as the SQL
// it stands for, with the same error, and policies apply to it as to that
// SQL.
func TestReportAPIGovernance(t *testing.T) {
	cs := startReportServer(t)
	ctx := context.Background()
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	for _, tbl := range []string{"segments", "audience_members", "audience_revisions", "media"} {
		mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public."+tbl+" TO nina")
	}
	audience := reportCohorts()[:1]
	specs := map[string]map[string]any{
		"granted tables": {"version": 1, "cohorts": audience, "totals": true, "source": map[string]any{"table": "segments", "member": "person_id"},
			"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}},
		"a view not granted": {"version": 1, "cohorts": reportCohorts(), "source": map[string]any{"table": "segments", "member": "person_id"},
			"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}},
		"a cohort's SQL reading a table not granted": {"version": 1, "dimensions": []any{},
			"cohorts": []any{map[string]any{"label": "x", "sql": "SELECT person_id FROM attributes"}}},
		"a source not granted": {"version": 1, "cohorts": audience, "source": map[string]any{"table": "browsing", "member": "person_id"},
			"dimensions": []any{map[string]any{"name": "K", "elements": "keywords"}}},
		"the other catalog's secrets": {"version": 1, "cohorts": []any{map[string]any{"label": "x", "table": "secrets", "member": "code"}}, "dimensions": []any{}},
		"files":                       {"version": 1, "cohorts": []any{map[string]any{"label": "x", "sql": "SELECT column0 FROM read_csv('/etc/passwd')"}}, "dimensions": []any{}},
	}
	same := func(who string, conn *pgx.Conn) {
		t.Helper()
		for name, spec := range specs {
			lit := specLiteral(t, spec)
			text := texts(t, conn, "SELECT igloo.audience_profile_sql("+lit+")")
			if len(text) != 1 {
				t.Fatalf("%s, %s: audience_profile_sql: %q", who, name, text)
			}
			_, errAPI := conn.Exec(ctx, "SELECT * FROM igloo.audience_profile("+lit+")")
			_, errSQL := conn.Exec(ctx, text[0])
			if pgCode(errAPI) != pgCode(errSQL) || (errAPI == nil) != (errSQL == nil) {
				t.Errorf("%s, %s: the API: %v; its SQL: %v", who, name, errAPI, errSQL)
				continue
			}
			if errAPI == nil {
				api := texts(t, conn, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
				sql := texts(t, conn, "SELECT * FROM ("+text[0]+") ORDER BY ALL")
				if !slices.Equal(api, sql) {
					t.Errorf("%s, %s: rows differ:\n%q\n%q", who, name, api, sql)
				}
			}
		}
	}
	same("nina", nina)
	if _, err := nina.Exec(ctx, "SELECT * FROM igloo.audience_profile("+specLiteral(t, specs["a view not granted"])+")"); pgCode(err) != "42501" {
		t.Errorf("nina's call reading a view she may not: %v", err)
	}

	// Row access and masking policies apply to the call as to its SQL.
	for _, q := range []string{
		"CREATE ROW ACCESS POLICY some_segments AS (s VARCHAR) RETURNS BOOLEAN -> starts_with(s, 'SEG_')",
		"ALTER TABLE lake.public.segments ADD ROW ACCESS POLICY some_segments ON (segment_name)",
		"CREATE MASKING POLICY first_letter AS (v VARCHAR) RETURNS VARCHAR -> left(v, 1)",
		"ALTER TABLE lake.public.audience_members ALTER COLUMN person_id SET MASKING POLICY first_letter",
	} {
		mustExec(t, cs.admin, q)
	}
	same("nina, under policies", nina)
	same("the admin, under policies", cs.admin)
}

// TestReportAPICheck: the parity check runs the caller's SQL as the caller
// would: one read-only SELECT, refused what the caller may not read.
func TestReportAPICheck(t *testing.T) {
	cs := startReportServer(t)
	ctx := context.Background()
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.segments TO nina")
	spec := specLiteral(t, map[string]any{"version": 1, "cohorts": []any{map[string]any{"label": "x", "table": "segments", "member": "person_id"}},
		"source": map[string]any{"table": "segments", "member": "person_id"}, "dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}})
	check := func(sql string) error {
		_, err := nina.Exec(ctx, "SELECT * FROM igloo.audience_profile_check("+spec+", "+quoteLiteral(sql)+")")
		return err
	}
	for _, c := range []struct{ name, sql, code string }{
		{"two statements", "SELECT 'a' AS dimension, 'b' AS value, 'x' AS cohort; DELETE FROM segments", "22023"},
		{"a DELETE", "DELETE FROM segments", "22023"},
		{"an INSERT", "INSERT INTO segments VALUES ('p', 's')", "22023"},
		{"a star", "SELECT * FROM segments", "22023"},
		{"no cohort column", "SELECT 'a' AS dimension, 'b' AS value", "22023"},
	} {
		if err := check(c.sql); pgCode(err) != c.code {
			t.Errorf("%s: %v, want %s", c.name, err, c.code)
		}
	}
	// What nina may not read directly, she may not read through the check,
	// with the same error.
	for _, sql := range []string{
		"SELECT 'a' AS dimension, code AS value, 'x' AS cohort FROM secrets",
		"SELECT 'a' AS dimension, column0 AS value, 'x' AS cohort FROM read_csv('/etc/passwd')",
		"SELECT 'a' AS dimension, 'b' AS value, 'x' AS cohort FROM igloo_bitmaps.anything",
		"SELECT 'a' AS dimension, segment_name AS value, 'x' AS cohort FROM segments WHERE _igloo_id_person_id = 1",
	} {
		_, direct := nina.Exec(ctx, sql)
		through := check(sql)
		if direct == nil || through == nil || pgCode(direct) != pgCode(through) {
			t.Errorf("%s: directly %v, through the check %v", sql, direct, through)
		}
	}
	if err := check("SELECT 'SEGMENT' AS dimension, segment_name AS value, 'x' AS cohort, count(DISTINCT person_id) AS people FROM segments GROUP BY ALL"); err != nil {
		t.Errorf("a check nina may run: %v", err)
	}
}

// TestReportAPIThroughIDs counts through ID domains' ids where the tables
// allow, with the rows the values give, and by value elsewhere.
func TestReportAPIThroughIDs(t *testing.T) {
	it := newIndexTest(t)
	ctx := context.Background()
	srv := it.r.srv
	it.exec(
		"CREATE TABLE lake.public.audience_members AS SELECT 'p' || (i * 7 % 97) AS person_id, CASE WHEN i % 3 = 0 THEN 'a1' ELSE 'a2' END AS audience_id, CAST(1 + i % 2 AS INTEGER) AS ver FROM range(400) r(i)",
		"CREATE TABLE lake.public.audience_revisions AS SELECT * FROM (VALUES ('a1', 1), ('a1', 2), ('a2', 1)) v(audience_id, ver)",
		"CREATE TABLE lake.public.attributes AS SELECT 'p' || i AS person_id, CAST(i % 40 AS VARCHAR) AS household_id FROM range(250) r(i)",
		"CREATE TABLE lake.public.survey AS SELECT CAST(i % 10 AS BIGINT) AS resp_id, 'answer ' || i AS answer FROM range(20) r(i)",
		"CREATE VIEW lake.public.v_attributes (person_id, household_id, survey_resp_id) AS SELECT s.*, sv.resp_id FROM lake.public.attributes AS s LEFT JOIN lake.public.survey AS sv ON CAST(s.household_id AS BIGINT) = sv.resp_id",
		"CREATE VIEW lake.public.v_segments AS SELECT person_id, segment_name FROM lake.public.segments",
	)
	for _, tbl := range []string{"audience_members", "attributes"} {
		it.exec("ALTER TABLE lake.public."+tbl+" ALTER COLUMN person_id SET TAG pii = 'person_id'",
			"ALTER TABLE lake.public."+tbl+" ALTER COLUMN person_id SET ID DOMAIN people")
	}
	srv.MapIDDomains(ctx)
	conn := it.admin
	mustExec(t, conn, "SET search_path = 'lake.public'")

	segment := []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}
	spec := func(source string, dims []any, cohorts []any) map[string]any {
		return map[string]any{"version": 1, "cohorts": cohorts, "totals": true, "source": map[string]any{"table": source, "member": "person_id"}, "dimensions": dims}
	}
	// counted runs spec as conn and checks its rows are its SQL's, and
	// whether it counted through ids.
	counted := func(conn *pgx.Conn, what string, spec map[string]any, throughIDs bool) {
		t.Helper()
		lit := specLiteral(t, spec)
		before := srv.reportThroughIDs.Load()
		api := texts(t, conn, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
		if got := srv.reportThroughIDs.Load() > before; got != throughIDs {
			t.Errorf("%s: through ids %v, want %v", what, got, throughIDs)
		}
		text := texts(t, conn, "SELECT igloo.audience_profile_sql("+lit+")")
		if len(text) != 1 {
			t.Fatalf("%s: %q", what, text)
		}
		if sql := texts(t, conn, "SELECT * FROM ("+text[0]+") ORDER BY ALL"); len(api) == 0 || !slices.Equal(api, sql) {
			t.Errorf("%s: rows\n%q\nwant\n%q", what, api, sql)
		}
	}
	counted(conn, "tables, and the population through its LEFT JOIN view", spec("segments", segment, reportCohorts()), true)
	counted(conn, "a source through a view of its table alone", spec("v_segments", segment, reportCohorts()), true)
	// A view that may repeat its table's rows: its rows count as the view's.
	counted(conn, "a source through a LEFT JOIN view", spec("v_attributes", []any{map[string]any{"name": "HH", "column": "household_id"}}, reportCohorts()), false)
	counted(conn, "a cohort given as SQL", spec("segments", segment, []any{map[string]any{"label": "x", "sql": "SELECT person_id FROM attributes"}}), false)

	// Users whose reads grants decide read views as views; tables, through
	// ids all the same.
	it.exec("CREATE USER nina WITH PASSWORD 'nina-password' ROLE none", "GRANT SELECT ON SCHEMA lake.public TO nina")
	nina := connect(t, it.r.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, nina, "SET search_path = 'lake.public'")
	counted(nina, "nina, with a view", spec("segments", segment, reportCohorts()), false)
	counted(nina, "nina, with tables", spec("segments", segment, reportCohorts()[:1]), true)

	// A prepared call reads the tables' new snapshots after a write.
	cached := it.r.admin(t, pgx.QueryExecModeCacheStatement)
	mustExec(t, cached, "SET search_path = 'lake.public'")
	lit := specLiteral(t, spec("segments", segment, reportCohorts()))
	first := texts(t, cached, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
	it.exec("INSERT INTO lake.public.segments SELECT 'p' || i, 'seg_new', 0 FROM range(0, 40) r(i)")
	before := srv.reportThroughIDs.Load()
	again := texts(t, cached, "SELECT * FROM igloo.audience_profile("+lit+") ORDER BY ALL")
	if slices.Equal(first, again) {
		t.Error("a prepared call did not see a write")
	}
	if srv.reportThroughIDs.Load() == before {
		t.Error("a prepared call was not written again for the tables' new snapshots")
	}
	text := texts(t, conn, "SELECT igloo.audience_profile_sql("+lit+")")
	if sql := texts(t, conn, "SELECT * FROM ("+text[0]+") ORDER BY ALL"); !slices.Equal(again, sql) {
		t.Errorf("after a write: rows\n%q\nwant\n%q", again, sql)
	}
}

// TestReportAPIFromIndexes answers calls from bitmap indexes, with the rows
// the tables give, where the indexes hold all the call counts, and from the
// tables elsewhere.
func TestReportAPIFromIndexes(t *testing.T) {
	it := newE2Test(t)
	ctx := context.Background()
	srv := it.r.srv
	conn := it.admin
	mustExec(t, conn, "SET search_path = 'lake.public'")
	cohorts := []any{
		map[string]any{"label": "AUDIENCE", "table": "audience_members", "member": "person_id", "where": map[string]any{"audience_id": "a1"},
			"version": map[string]any{"column": "ver", "latest_from": map[string]any{"table": "audience_revisions", "where": map[string]any{"audience_id": "a1"}}}},
		map[string]any{"label": "BASE_POPULATION", "table": "v_attributes", "member": "person_id"},
	}
	segments := map[string]any{"version": 1, "cohorts": cohorts, "totals": true,
		"source":     map[string]any{"table": "person_segments", "member": "person_id", "where": map[string]any{"segment_name": map[string]any{"like": "SEG_%"}}},
		"dimensions": []any{map[string]any{"name": "SEGMENT", "column": "segment_name"}}}
	everyone := map[string]any{"version": 1, "cohorts": cohorts[:1], "totals": true,
		"source":     map[string]any{"table": "attributes", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "REACH", "constant": "ANYONE"}}}
	// answered runs spec and checks its rows are its SQL's, and whether
	// bitmap indexes answered it.
	answered := func(what string, spec map[string]any, want string) {
		t.Helper()
		lit := specLiteral(t, spec)
		before := srv.reportFromIndexes.Load()
		api := texts(t, conn, "SELECT * EXCLUDE (path) FROM igloo.audience_profile("+lit+") ORDER BY ALL")
		if got := srv.reportFromIndexes.Load() > before; got != (want != "scan") {
			t.Errorf("%s: from indexes %v, want %s", what, got, want)
		}
		paths := texts(t, conn, "SELECT DISTINCT path FROM igloo.audience_profile("+lit+")")
		if len(paths) != 1 || paths[0] != want {
			t.Errorf("%s: paths %q, want %s", what, paths, want)
		}
		text := texts(t, conn, "SELECT igloo.audience_profile_sql("+lit+")")
		if len(text) != 1 {
			t.Fatalf("%s: %q", what, text)
		}
		if sql := texts(t, conn, "SELECT * EXCLUDE (path) FROM ("+text[0]+") ORDER BY ALL"); len(api) == 0 || !slices.Equal(api, sql) {
			t.Errorf("%s: rows\n%q\nwant\n%q", what, api, sql)
		}
	}
	answered("segments", segments, "index")
	answered("a constant, over a table of one row a member", everyone, "index")
	// Values of several groups: a constant over the segments a condition
	// keeps (E2's totals), values a map merges, and entries that merge.
	answered("a constant over some segments", map[string]any{"version": 1, "cohorts": cohorts, "totals": true,
		"source":     map[string]any{"table": "person_segments", "member": "person_id", "where": map[string]any{"segment_name": map[string]any{"like": "SEG_%"}}},
		"dimensions": []any{map[string]any{"name": "TOTAL", "constant": "ANY"}}}, "index")
	answered("a map that merges values", map[string]any{"version": 1, "cohorts": cohorts, "totals": true,
		"source": map[string]any{"table": "person_segments", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "KIND", "column": "segment_name", "map": map[string]any{"SEG_1": "low", "SEG_2": "low", "SEG_3": "high"}},
			map[string]any{"name": "KIND", "column": "segment_name", "map": map[string]any{"SEG_4": "low"}, "skip_blank": true}}}, "index")

	// A list's elements, from an index of them: once each a member, from
	// the bitmaps; repeated in a member's lists, from the tables.
	it.exec(
		"CREATE TABLE lake.public.interests AS SELECT 'p' || i AS person_id, CASE i % 3 WHEN 0 THEN ['cars', 'news'] WHEN 1 THEN ['news', ''] ELSE NULL END AS topics FROM range(300) r(i)",
		"ALTER TABLE lake.public.interests ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.interests ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(ctx)
	it.exec("ALTER TABLE lake.public.interests ADD BITMAP INDEX topics ON (ELEMENTS(topics)) FOR person_id")
	srv.waitIndexBuilds(t)
	topics := map[string]any{"version": 1, "cohorts": cohorts, "totals": true,
		"source":     map[string]any{"table": "interests", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "TOPIC", "elements": "topics", "skip_blank": true}}}
	answered("a list's elements", topics, "index")
	it.internal("INSERT INTO lake.public.interests (person_id, topics, _igloo_id_person_id) SELECT value, ['cars'], id FROM lake.igloo_domains.people WHERE value = 'p0'")
	srv.indexesChanged("public", "interests")
	srv.waitIndexBuilds(t)
	srv.MaintainBitmapIndexes(ctx)
	answered("a list's elements, one repeated in a member's lists", topics, "index+scan")

	// Metrics and the extent: people from the bitmaps, the rest from a scan
	// of the source that keeps the cohorts' rows by their bitmaps.
	it.exec(
		"CREATE TABLE lake.public.plays AS SELECT 'p' || (i % 200) AS person_id, CASE i % 5 WHEN 0 THEN 'drama' WHEN 1 THEN 'news' WHEN 2 THEN '' WHEN 3 THEN NULL ELSE 'sport' END AS genre, "+
			"CASE WHEN i % 7 = 0 THEN NULL ELSE CAST(i AS INTEGER) END AS seconds, DATE '2026-01-01' + CAST(i % 40 AS INTEGER) AS day FROM range(1500) r(i)",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"ALTER TABLE lake.public.plays ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(ctx)
	it.exec("ALTER TABLE lake.public.plays ADD BITMAP INDEX plays_genre ON (genre) FOR person_id",
		"ALTER TABLE lake.public.plays ADD BITMAP INDEX plays_all ON () FOR person_id")
	srv.waitIndexBuilds(t)
	answered("plays, with metrics and the extent", map[string]any{"version": 1, "cohorts": cohorts, "totals": true, "extent": "day",
		"source": map[string]any{"table": "plays", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "GENRE", "column": "genre", "skip_blank": true},
			map[string]any{"name": "REACH", "constant": "ANY_PLAY", "metric": "seconds"}},
		"metrics": []any{map[string]any{"name": "seconds", "column": "seconds", "coalesce": 0}, map[string]any{"name": "days", "distinct_with": "day"}}}, "index+scan")
	// A condition on a metric, which bitmaps cannot keep: the tables.
	answered("plays, with a metric's condition", map[string]any{"version": 1, "cohorts": cohorts,
		"source":     map[string]any{"table": "plays", "member": "person_id"},
		"dimensions": []any{map[string]any{"name": "LONG", "constant": "LONG_PLAY", "metric": "seconds", "metric_where": map[string]any{"gt": 1000}}}}, "scan")

	// A prepared call counted from the tables while an index it needs is
	// missing reads the bitmaps once the index is built.
	it.exec("ALTER TABLE lake.public.attributes DROP BITMAP INDEX population")
	cached := it.r.admin(t, pgx.QueryExecModeCacheStatement)
	mustExec(t, cached, "SET search_path = 'lake.public'")
	call := "SELECT * EXCLUDE (path) FROM igloo.audience_profile(" + specLiteral(t, segments) + ") ORDER BY ALL"
	fromTables := texts(t, cached, call)
	it.exec("ALTER TABLE lake.public.attributes ADD BITMAP INDEX population ON () FOR person_id")
	srv.waitIndexBuilds(t)
	srv.MaintainBitmapIndexes(ctx)
	before := srv.reportFromIndexes.Load()
	if got := texts(t, cached, call); !slices.Equal(got, fromTables) {
		t.Errorf("a prepared call, from the bitmaps: rows\n%q\nwant\n%q", got, fromTables)
	}
	if srv.reportFromIndexes.Load() == before {
		t.Error("a prepared call counted from the tables did not go to the bitmaps once its index was built")
	}

	// A masking policy on a column read: the tables.
	it.exec("CREATE MASKING POLICY same AS (v VARCHAR) RETURNS VARCHAR -> v",
		"ALTER TABLE lake.public.person_segments ALTER COLUMN segment_name SET MASKING POLICY same")
	answered("segments under a mask", segments, "scan")
	it.exec("ALTER TABLE lake.public.person_segments ALTER COLUMN segment_name UNSET MASKING POLICY")

	// A member with two rows in a group: its rows are not its people, so
	// the tables count them.
	it.exec("INSERT INTO lake.public.person_segments VALUES ('p1', 'SEG_1'), ('p1', 'SEG_1')")
	srv.waitIndexBuilds(t)
	srv.MaintainBitmapIndexes(ctx)
	answered("segments, with a member's row repeated", segments, "index+scan")
}
