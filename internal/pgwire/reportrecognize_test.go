package pgwire

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Recognizing reports (IGLOO_REPORT_REWRITE) is tested by running reports
// written the way audience reports count, natively and in Snowflake's
// dialect, with recognition on and off: the rows must be the same, and the
// CTEs recognized those expected. The tables are reportapi_test.go's.

// reportCountsSQL are the reports' counting CTEs over reportCohortsSQL:
// people and intensities by media genre, live viewing (a map of a boolean)
// and reach (a constant), by browsing keyword (a list's elements) and
// activity (a literal list), and by state and interest metrics, filtered
// on the metric, with the cohorts' sizes.
const reportCountsSQL = `WITH ` + reportCohortsSQL + `,
MEDIA_COUNTS AS (
  SELECT 'MEDIA' AS category, D.dim AS dimension, CAST(D.name AS VARCHAR) AS value, M.cohort AS cohort,
    count(DISTINCT md.person_id) AS people,
    sum(coalesce(md.total_seconds, 0)) / nullif(count(DISTINCT md.person_id), 0) AS intensity
  FROM media AS md JOIN COHORT_MEMBERS AS M ON M.person_id = md.person_id
  CROSS JOIN UNNEST([{'dim': 'GENRE', 'name': md.genre},
    {'dim': 'VIEWING_MODE', 'name': CASE WHEN md.live IS NULL THEN NULL ELSE CASE WHEN md.live THEN 'LIVE' ELSE 'ON_DEMAND' END END},
    {'dim': 'MEDIA_REACH', 'name': 'ANY_VIEWING'}]) AS MD(D)
  WHERE NOT CAST(D.name AS VARCHAR) IS NULL AND trim(CAST(D.name AS VARCHAR)) <> ''
  GROUP BY D.dim, CAST(D.name AS VARCHAR), M.cohort),
BROWSING_COUNTS AS (
  SELECT 'DIGITAL' AS category, D.dim AS dimension, CAST(I.item AS VARCHAR) AS value, M.cohort AS cohort,
    count(DISTINCT b.person_id) AS people,
    count(DISTINCT CASE WHEN b.person_id IS NULL THEN NULL WHEN b.visited_date IS NULL THEN NULL ELSE (b.person_id, b.visited_date) END)
      / nullif(count(DISTINCT b.person_id), 0) AS intensity
  FROM browsing AS b JOIN COHORT_MEMBERS AS M ON M.person_id = b.person_id
  CROSS JOIN UNNEST([{'dim': 'KEYWORD', 'items': b.keywords}, {'dim': 'ACTIVITY', 'items': ['ANY_BROWSING']}]) AS BD(D)
  CROSS JOIN UNNEST(D.items) AS I(item)
  WHERE NOT CAST(I.item AS VARCHAR) IS NULL AND trim(CAST(I.item AS VARCHAR)) <> ''
  GROUP BY D.dim, CAST(I.item AS VARCHAR), M.cohort),
ATTRIBUTE_COUNTS AS (
  SELECT CASE WHEN D.grp = 'REACH' THEN 'REACH' ELSE 'INTERESTS' END AS category, D.dim AS dimension, CAST(D.name AS VARCHAR) AS value,
    M.cohort AS cohort, count(DISTINCT a.person_id) AS people, avg(CAST(D.metric AS DOUBLE)) AS intensity
  FROM attributes AS a JOIN COHORT_MEMBERS AS M ON M.person_id = a.person_id
  CROSS JOIN UNNEST([{'grp': 'REACH', 'dim': 'STATE', 'name': a.state, 'metric': NULL},
    {'grp': 'INTEREST', 'dim': 'TECHNOLOGY', 'name': 'TECH_AFFINITY', 'metric': a.techaffinity},
    {'grp': 'INTEREST', 'dim': 'ENTERTAINMENT', 'name': 'MOVIES', 'metric': a.movie},
    {'grp': 'INTEREST', 'dim': 'ENTERTAINMENT', 'name': 'MUSIC', 'metric': a.music}]) AS ATTR(D)
  WHERE NOT CAST(D.name AS VARCHAR) IS NULL AND trim(CAST(D.name AS VARCHAR)) <> ''
    AND (D.grp = 'REACH' OR (D.grp = 'INTEREST' AND CAST(D.metric AS DOUBLE) > 0))
  GROUP BY ALL),
ALL_COUNTS AS (SELECT * FROM MEDIA_COUNTS UNION ALL SELECT * FROM BROWSING_COUNTS UNION ALL SELECT * FROM ATTRIBUTE_COUNTS)
SELECT S.*, Z.cohort_size, S.people / nullif(Z.cohort_size, 0) AS penetration
FROM ALL_COUNTS AS S JOIN COHORT_TOTALS AS Z ON Z.cohort = S.cohort`

// reportSegmentsSQL counts segments per cohort, each cohort apart, with
// the totals of the segments counted.
const reportSegmentsSQL = `WITH AUDIENCE AS (
  SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = (SELECT max(ver) FROM audience_revisions WHERE audience_id = 'a1')
), BASE_POPULATION AS (SELECT person_id FROM v_attributes),
AUD_SEGMENTS AS (
  SELECT S.segment_name AS segment_name, count(DISTINCT S.person_id) AS audience_people
  FROM segments AS S INNER JOIN AUDIENCE AS A ON A.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_HH_%' GROUP BY S.segment_name
), BASEPOP_SEGMENTS AS (
  SELECT S.segment_name AS segment_name, count(DISTINCT S.person_id) AS basepop_people
  FROM segments AS S INNER JOIN BASE_POPULATION AS G ON G.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_HH_%' GROUP BY S.segment_name
), AUD_TOTAL AS (
  SELECT count(DISTINCT S.person_id) AS total_audience
  FROM segments AS S INNER JOIN AUDIENCE AS A ON A.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_HH_%'
), BASEPOP_TOTAL AS (
  SELECT count(DISTINCT S.person_id) AS total_basepop
  FROM segments AS S INNER JOIN BASE_POPULATION AS G ON G.person_id = S.person_id WHERE S.segment_name LIKE 'SEG_HH_%'
)
SELECT A.segment_name, A.audience_people, G.basepop_people, TA.total_audience, TG.total_basepop,
  round(A.audience_people / nullif(TA.total_audience, 0) / nullif(G.basepop_people / nullif(TG.total_basepop, 0), 0), 4) AS index_ratio
FROM AUD_SEGMENTS AS A JOIN BASEPOP_SEGMENTS AS G ON A.segment_name = G.segment_name
CROSS JOIN AUD_TOTAL AS TA CROSS JOIN BASEPOP_TOTAL AS TG`

// reportRecognitionCases are reports and how many of their CTEs
// recognition answers through the report API.
var reportRecognitionCases = []struct {
	name, sql string
	want      int
}{
	{"signals", reportCountsSQL, 4},
	{"segments", reportSegmentsSQL, 4},
	{"keywords", `WITH AUDIENCE AS (SELECT person_id FROM audience_members WHERE audience_id = 'a2' AND ver = 2),
KEYWORDS AS (
  SELECT 'KEYWORD' AS dimension, K.kw AS keyword, count(DISTINCT B.person_id) AS people,
    count(DISTINCT CASE WHEN B.person_id IS NULL THEN NULL WHEN B.visited_date IS NULL THEN NULL ELSE (B.person_id, B.visited_date) END) AS active_days
  FROM browsing AS B JOIN AUDIENCE AS A ON A.person_id = B.person_id CROSS JOIN UNNEST(B.keywords) AS K(kw)
  WHERE trim(K.kw) <> '' GROUP BY K.kw)
SELECT * FROM KEYWORDS ORDER BY keyword`, 1},
	{"genres of a cohort given by IN", `WITH COUNTS AS (
  SELECT M.genre, count(DISTINCT M.person_id) AS people, count(*) AS views, sum(M.total_seconds) AS seconds, avg(M.total_seconds) AS average_seconds
  FROM media AS M
  WHERE M.person_id IN (SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = 2)
    AND M.viewed_date BETWEEN DATE '2026-01-05' AND DATE '2026-01-30'
  GROUP BY M.genre)
SELECT * FROM COUNTS ORDER BY genre NULLS FIRST`, 1},
	{"cohort sizes", `WITH ` + reportCohortsSQL + ` SELECT * FROM COHORT_TOTALS`, 1},
	{"live viewing grouped by its boolean", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT M.live, count(DISTINCT M.person_id) AS people, count(*) AS views
  FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id GROUP BY M.live)
SELECT * FROM C`, 1},

	{"a range of a column", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT M.genre, count(DISTINCT M.person_id) AS people, sum(M.total_seconds) AS seconds
  FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id WHERE M.total_seconds > 100 AND M.total_seconds <= 2000 AND M.genre IN ('drama', 'news', '')
  GROUP BY M.genre)
SELECT * FROM C`, 1},

	// Shapes recognition leaves as written: a window over the joined rows
	// (as behaviors reports have), a tuple count DuckDB counts with NULLs,
	// a sum over a cohort that may repeat members, value IS NOT NULL
	// without the blank check, a cohort filtered by its label, and a
	// value of a type whose text is not one per value.
	{"a window", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM v_attributes WHERE person_id IS NOT NULL),
BASE AS (SELECT M.person_id, M.genre, min(M.viewed_date) OVER () AS period_start FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id),
C AS (SELECT genre, min(period_start) AS period_start, count(DISTINCT person_id) AS people FROM BASE GROUP BY genre)
SELECT * FROM C`, 0},
	{"a tuple count", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT B.visited_date, count(DISTINCT (B.person_id, B.visited_date)) AS days
  FROM browsing AS B JOIN AUDIENCE AS A ON A.person_id = B.person_id GROUP BY B.visited_date)
SELECT * FROM C`, 0},
	{"a sum over a cohort that repeats members", `WITH AUDIENCE AS (SELECT person_id FROM audience_members WHERE audience_id = 'a1'),
C AS (SELECT M.genre, sum(M.total_seconds) AS seconds FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id GROUP BY M.genre)
SELECT * FROM C`, 0},
	{"IS NOT NULL alone", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT M.genre, count(DISTINCT M.person_id) AS people FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id
  WHERE M.genre IS NOT NULL GROUP BY M.genre)
SELECT * FROM C`, 0},
	{"a cohort filtered by its label", `WITH ` + reportCohortsSQL + `,
C AS (SELECT S.segment_name, M.cohort, count(DISTINCT S.person_id) AS people
  FROM segments AS S JOIN COHORT_MEMBERS AS M ON M.person_id = S.person_id WHERE M.cohort = 'AUDIENCE' GROUP BY ALL)
SELECT * FROM C`, 1}, // COHORT_TOTALS, which nothing reads
	{"two lower bounds of a column", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT M.genre, count(DISTINCT M.person_id) AS people FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id
  WHERE M.total_seconds > 100 AND M.total_seconds > 300 GROUP BY M.genre)
SELECT * FROM C`, 0},
	{"GROUP BY a name of an input and an output", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT upper(M.genre) AS genre, count(DISTINCT M.person_id) AS people FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id GROUP BY genre)
SELECT * FROM C`, 0},
	{"a timestamp grouped by as it is", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT CAST(M.viewed_date AS TIMESTAMP) AS day, count(DISTINCT M.person_id) AS people
  FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id GROUP BY 1)
SELECT * FROM C`, 0},
}

// reportRecognitionServer is a report server with recognition on, and
// what recognition did with the last statement.
type reportRecognitionServer struct {
	*catalogServer
	mu       sync.Mutex
	outcomes []reportOutcome
}

func startReportRecognitionServer(t *testing.T) *reportRecognitionServer {
	t.Helper()
	rs := &reportRecognitionServer{}
	rs.catalogServer = startCatalogServer(t, func(o *Options) {
		o.ReportRewrite = true
		o.reportOutcomes = func(outcomes []reportOutcome) {
			rs.mu.Lock()
			defer rs.mu.Unlock()
			rs.outcomes = append(rs.outcomes, outcomes...)
		}
		o.reportFallback = func(recognized string, err error) {
			t.Logf("a recognized statement failed: %v\n%s", err, recognized)
		}
	})
	for _, q := range reportFixture {
		mustExec(t, rs.admin, q)
	}
	mustExec(t, rs.admin, "SET search_path = 'lake.public'")
	return rs
}

// run runs sql with recognition on or off, and returns its rows, sorted,
// with doubles to 9 significant digits, the CTEs recognition answered
// through the API, and those it left, with why.
func (rs *reportRecognitionServer) run(t *testing.T, conn *pgx.Conn, sql string, on bool) (rows []string, recognized, left []string, err error) {
	t.Helper()
	rs.srv.reportRewrite.Store(on)
	defer rs.srv.reportRewrite.Store(true)
	rs.mu.Lock()
	rs.outcomes = nil
	rs.mu.Unlock()
	rows, err = recognitionRows(conn, sql)
	rs.mu.Lock()
	defer rs.mu.Unlock()
	for _, o := range rs.outcomes {
		if o.Spec != "" {
			recognized = append(recognized, o.CTE)
		} else {
			left = append(left, o.CTE+": "+o.Why)
		}
	}
	return rows, recognized, left, err
}

func recognitionRows(conn *pgx.Conn, sql string) ([]string, error) {
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				parts[i] = "NULL"
			case float64:
				parts[i] = strconv.FormatFloat(x, 'g', 9, 64)
			case float32:
				parts[i] = strconv.FormatFloat(float64(x), 'g', 6, 32)
			default:
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// checkRecognized runs sql with recognition on and off and compares.
func (rs *reportRecognitionServer) checkRecognized(t *testing.T, conn *pgx.Conn, name, sql string, want int) {
	t.Helper()
	before, counted := rs.srv.reportUnrecognized.Load(), rs.srv.reportRecognized.Load()
	on, recognized, left, errOn := rs.run(t, conn, sql, true)
	if rs.srv.reportUnrecognized.Load() != before {
		t.Errorf("%s: the recognized statement failed and ran as written", name)
	}
	if n := rs.srv.reportRecognized.Load() - counted; n != int64(len(recognized)) {
		t.Errorf("%s: counted %d CTEs recognized, heard of %d", name, n, len(recognized))
	}
	off, _, _, errOff := rs.run(t, conn, sql, false)
	if errOn != nil || errOff != nil {
		t.Errorf("%s: %v; as written: %v", name, errOn, errOff)
		return
	}
	if len(recognized) != want {
		t.Errorf("%s: recognized %q, want %d; left %q", name, recognized, want, left)
	}
	if len(on) == 0 {
		t.Errorf("%s: no rows", name)
	}
	if !slices.Equal(on, off) {
		t.Errorf("%s: rows differ:\nrecognized:\n%s\nas written:\n%s", name, strings.Join(on, "\n"), strings.Join(off, "\n"))
	}
}

func TestReportRecognition(t *testing.T) {
	rs := startReportRecognitionServer(t)
	for _, c := range reportRecognitionCases {
		rs.checkRecognized(t, rs.admin, c.name, c.sql, c.want)
	}
	// Prepared and run with parameters elsewhere in the statement too.
	conn := connect(t, rs.url("igloo", adminPassword), pgx.QueryExecModeCacheStatement)
	mustExec(t, conn, "SET search_path = 'lake.public'")
	rs.checkRecognized(t, conn, "segments, prepared", reportSegmentsSQL, 4)
}

// TestReportRecognitionSnowflake: reports in Snowflake's dialect are
// recognized in the translation Igloo makes of them.
func TestReportRecognitionSnowflake(t *testing.T) {
	rs := startReportRecognitionServer(t)
	conn := rs.admin
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	cohorts := `AUDIENCE AS (SELECT PERSON_ID FROM AUDIENCE_MEMBERS WHERE audience_id = 'a1' AND ver = (SELECT MAX(ver) FROM AUDIENCE_REVISIONS WHERE audience_id = 'a1')),
BASE_POPULATION AS (SELECT "PERSON_ID" AS "PERSON_ID", "STATE" FROM V_ATTRIBUTES),
AUD_COHORT AS (SELECT DISTINCT PERSON_ID AS PERSON_ID FROM AUDIENCE AS AUDIENCE WHERE NOT PERSON_ID IS NULL),
BASE_POPULATION_COHORT AS (SELECT DISTINCT PERSON_ID AS PERSON_ID FROM BASE_POPULATION AS BASE_POPULATION WHERE NOT PERSON_ID IS NULL),
COHORT_MEMBERS AS (SELECT 'AUDIENCE' AS COHORT, AUD_COHORT.PERSON_ID AS PERSON_ID FROM AUD_COHORT AS AUD_COHORT
  UNION ALL SELECT 'BASE_POPULATION' AS COHORT, BASE_POPULATION_COHORT.PERSON_ID AS PERSON_ID FROM BASE_POPULATION_COHORT AS BASE_POPULATION_COHORT),
COHORT_TOTALS AS (SELECT COHORT_MEMBERS.COHORT AS COHORT, COUNT(DISTINCT COHORT_MEMBERS.PERSON_ID) AS COHORT_SIZE FROM COHORT_MEMBERS AS COHORT_MEMBERS
  GROUP BY COHORT_MEMBERS.COHORT)`
	signals := `SELECT * FROM (WITH ` + cohorts + `,
MEDIA_COUNTS AS (SELECT 'MEDIA' AS ITEM_CATEGORY, CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_DIMENSION') AS TEXT) AS ITEM_DIMENSION,
  CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_NAME') AS TEXT) AS ITEM_NAME, COHORT.COHORT AS COHORT, COUNT(DISTINCT MEDIA_ROW.PERSON_ID) AS DISTINCT_PEOPLE,
  SUM(COALESCE(MEDIA_ROW.TOTAL_SECONDS, 0)) / NULLIF(COUNT(DISTINCT MEDIA_ROW.PERSON_ID), 0) AS INTENSITY_MEASURE
  FROM MEDIA AS MEDIA_ROW INNER JOIN COHORT_MEMBERS AS COHORT ON COHORT.PERSON_ID = MEDIA_ROW.PERSON_ID
  CROSS JOIN LATERAL FLATTEN(INPUT => [OBJECT_CONSTRUCT('ITEM_DIMENSION', 'GENRE', 'ITEM_NAME', MEDIA_ROW.GENRE),
    OBJECT_CONSTRUCT('ITEM_DIMENSION', 'VIEWING_MODE', 'ITEM_NAME', IFF(MEDIA_ROW.LIVE IS NULL, NULL, IFF(MEDIA_ROW.LIVE, 'LIVE', 'ON_DEMAND'))),
    OBJECT_CONSTRUCT('ITEM_DIMENSION', 'MEDIA_REACH', 'ITEM_NAME', 'ANY_VIEWING')]) AS MEDIA_ITEM(SEQ, KEY, PATH, INDEX, VALUE, THIS)
  WHERE NOT CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_NAME') AS TEXT) IS NULL AND TRIM(CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_NAME') AS TEXT)) <> ''
  GROUP BY CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_DIMENSION') AS TEXT), CAST(GET_PATH(MEDIA_ITEM.VALUE, 'ITEM_NAME') AS TEXT), COHORT.COHORT),
ATTRIBUTE_COUNTS AS (SELECT IFF(CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_GROUP') AS TEXT) = 'REACH', 'REACH', 'INTERESTS') AS ITEM_CATEGORY,
  CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_DIMENSION') AS TEXT) AS ITEM_DIMENSION, CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_NAME') AS TEXT) AS ITEM_NAME,
  COHORT.COHORT AS COHORT, COUNT(DISTINCT ATTR.PERSON_ID) AS DISTINCT_PEOPLE, AVG(CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'METRIC_VALUE') AS FLOAT)) AS INTENSITY_MEASURE
  FROM ATTRIBUTES AS ATTR INNER JOIN COHORT_MEMBERS AS COHORT ON COHORT.PERSON_ID = ATTR.PERSON_ID
  CROSS JOIN LATERAL FLATTEN(INPUT => [OBJECT_CONSTRUCT('ITEM_GROUP', 'REACH', 'ITEM_DIMENSION', 'STATE', 'ITEM_NAME', ATTR.STATE),
    OBJECT_CONSTRUCT('ITEM_GROUP', 'INTEREST', 'ITEM_DIMENSION', 'TECHNOLOGY', 'ITEM_NAME', 'TECH_AFFINITY', 'METRIC_VALUE', ATTR.TECHAFFINITY),
    OBJECT_CONSTRUCT('ITEM_GROUP', 'INTEREST', 'ITEM_DIMENSION', 'ENTERTAINMENT', 'ITEM_NAME', 'MOVIES', 'METRIC_VALUE', ATTR.MOVIE),
    OBJECT_CONSTRUCT('ITEM_GROUP', 'INTEREST', 'ITEM_DIMENSION', 'ENTERTAINMENT', 'ITEM_NAME', 'MUSIC', 'METRIC_VALUE', ATTR.MUSIC)])
    AS ATTRIBUTE_ITEM(SEQ, KEY, PATH, INDEX, VALUE, THIS)
  WHERE NOT CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_NAME') AS TEXT) IS NULL AND TRIM(CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_NAME') AS TEXT)) <> ''
    AND (CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_GROUP') AS TEXT) = 'REACH'
      OR (CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'ITEM_GROUP') AS TEXT) = 'INTEREST' AND CAST(GET_PATH(ATTRIBUTE_ITEM.VALUE, 'METRIC_VALUE') AS FLOAT) > 0))
  GROUP BY 1, 2, 3, 4),
ALL_COUNTS AS (SELECT * FROM MEDIA_COUNTS UNION ALL SELECT * FROM ATTRIBUTE_COUNTS)
SELECT S.*, Z.COHORT_SIZE FROM ALL_COUNTS AS S INNER JOIN COHORT_TOTALS AS Z ON UPPER(Z.COHORT) = UPPER(S.COHORT)) AS T`
	rs.checkRecognized(t, conn, "signals in Snowflake's dialect", signals, 3)
	segments := `WITH AUDIENCE AS (SELECT PERSON_ID FROM AUDIENCE_MEMBERS WHERE audience_id = 'a1' AND ver = (SELECT MAX(ver) FROM AUDIENCE_REVISIONS WHERE audience_id = 'a1')),
AUD_SEGMENTS AS (SELECT S.SEGMENT_NAME, COUNT(DISTINCT S.PERSON_ID) AS PEOPLE FROM SEGMENTS AS S INNER JOIN AUDIENCE AS A ON A.PERSON_ID = S.PERSON_ID
  WHERE S.SEGMENT_NAME LIKE 'SEG_HH_%' GROUP BY S.SEGMENT_NAME),
AUD_TOTAL AS (SELECT COUNT(DISTINCT S.PERSON_ID) AS TOTAL FROM SEGMENTS AS S INNER JOIN AUDIENCE AS A ON A.PERSON_ID = S.PERSON_ID WHERE S.SEGMENT_NAME LIKE 'SEG_HH_%')
SELECT A.SEGMENT_NAME, A.PEOPLE, T.TOTAL, A.PEOPLE / NULLIF(T.TOTAL, 0) AS SHARE FROM AUD_SEGMENTS AS A CROSS JOIN AUD_TOTAL AS T`
	rs.checkRecognized(t, conn, "segments in Snowflake's dialect", segments, 2)
}

// TestReportRecognitionGovernance: a recognized statement is allowed and
// refused as written, and policies apply to it as to the statement
// written.
func TestReportRecognitionGovernance(t *testing.T) {
	rs := startReportRecognitionServer(t)
	mustExec(t, rs.admin, "CREATE TABLE lake.public.audience_versions AS SELECT * FROM lake.public.audience_revisions")
	nina := rs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	for _, tbl := range []string{"segments", "audience_members", "audience_revisions", "media"} {
		mustExec(t, rs.admin, "GRANT SELECT ON TABLE lake.public."+tbl+" TO nina")
	}
	audienceOnly := `WITH AUDIENCE AS (
  SELECT person_id FROM audience_members WHERE audience_id = 'a1' AND ver = (SELECT max(ver) FROM audience_revisions WHERE audience_id = 'a1')),
C AS (SELECT S.segment_name, count(DISTINCT S.person_id) AS people FROM segments AS S JOIN AUDIENCE AS A ON A.person_id = S.person_id GROUP BY S.segment_name),
T AS (SELECT count(DISTINCT M.person_id) AS viewers FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id)
SELECT * FROM C CROSS JOIN T`
	same := func(who string, conn *pgx.Conn) {
		t.Helper()
		for _, c := range []struct {
			name, sql string
			want      int // CTEs recognized when allowed
		}{
			{"granted tables", audienceOnly, 2},
			{"a cohort of a view not granted", reportSegmentsSQL, -1},
			{"a source not granted", reportRecognitionCases[2].sql, -1},
			{"a cohort's table not granted", strings.ReplaceAll(audienceOnly, "FROM audience_revisions", "FROM audience_versions"), -1},
		} {
			on, recognized, left, errOn := rs.run(t, conn, c.sql, true)
			off, _, _, errOff := rs.run(t, conn, c.sql, false)
			if pgCode(errOn) != pgCode(errOff) || (errOn == nil) != (errOff == nil) {
				t.Errorf("%s, %s: recognized: %v; as written: %v", who, c.name, errOn, errOff)
				continue
			}
			if !slices.Equal(on, off) {
				t.Errorf("%s, %s: rows differ:\n%q\n%q", who, c.name, on, off)
			}
			if c.want >= 0 && errOn == nil && len(recognized) != c.want {
				t.Errorf("%s, %s: recognized %q, want %d; left %q", who, c.name, recognized, c.want, left)
			}
			if c.want < 0 && who == "nina" && pgCode(errOn) != "42501" {
				t.Errorf("%s, %s: %v, want 42501", who, c.name, errOn)
			}
		}
	}
	same("nina", nina)
	for _, q := range []string{
		"CREATE ROW ACCESS POLICY some_segments AS (s VARCHAR) RETURNS BOOLEAN -> starts_with(s, 'SEG_')",
		"ALTER TABLE lake.public.segments ADD ROW ACCESS POLICY some_segments ON (segment_name)",
		"CREATE MASKING POLICY first_letter AS (v VARCHAR) RETURNS VARCHAR -> left(v, 1)",
		"ALTER TABLE lake.public.media ALTER COLUMN genre SET MASKING POLICY first_letter",
	} {
		mustExec(t, rs.admin, q)
	}
	same("nina, under policies", nina)
	same("the admin, under policies", rs.admin)
	rs.checkRecognized(t, nina, "genres under a masking policy", `WITH AUDIENCE AS (SELECT DISTINCT person_id FROM audience_members WHERE audience_id = 'a1' AND person_id IS NOT NULL),
C AS (SELECT M.genre, count(DISTINCT M.person_id) AS people, count(*) AS views FROM media AS M JOIN AUDIENCE AS A ON A.person_id = M.person_id GROUP BY M.genre)
SELECT * FROM C`, 1)
}

// TestReportRecognitionFallback: a statement whose recognized form fails,
// here because the API takes at most eight cohorts, runs as written.
func TestReportRecognitionFallback(t *testing.T) {
	rs := startReportRecognitionServer(t)
	var branches []string
	for i := range 9 {
		branches = append(branches, fmt.Sprintf("SELECT 'c%d' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM segments WHERE segment_name = 'SEGXHH_%d')", i, i%4))
	}
	sql := `WITH COHORTS AS (` + strings.Join(branches, " UNION ALL ") + `),
C AS (SELECT M.genre, C.cohort, count(DISTINCT M.person_id) AS people FROM media AS M JOIN COHORTS AS C ON C.person_id = M.person_id GROUP BY ALL)
SELECT * FROM C`
	before := rs.srv.reportUnrecognized.Load()
	on, recognized, left, err := rs.run(t, rs.admin, sql, true)
	if err != nil || len(recognized) != 1 {
		t.Fatalf("recognized %q (left %q): %v", recognized, left, err)
	}
	if rs.srv.reportUnrecognized.Load() != before+1 {
		t.Errorf("the recognized statement did not fail")
	}
	off, _, _, err := rs.run(t, rs.admin, sql, false)
	if err != nil || len(on) == 0 || !slices.Equal(on, off) {
		t.Errorf("rows differ (%v):\n%q\n%q", err, on, off)
	}
}

// TestReportRecognitionInTransaction: a recognized statement that fails
// leaves an open transaction as the statement as written does.
func TestReportRecognitionInTransaction(t *testing.T) {
	rs := startReportRecognitionServer(t)
	var branches []string
	for i := range 9 {
		branches = append(branches, fmt.Sprintf("SELECT 'c%d' AS cohort, person_id FROM (SELECT DISTINCT person_id FROM segments WHERE segment_name = 'SEGXHH_%d')", i, i%4))
	}
	sql := `WITH COHORTS AS (` + strings.Join(branches, " UNION ALL ") + `),
C AS (SELECT M.genre, C.cohort, count(DISTINCT M.person_id) AS people FROM media AS M JOIN COHORTS AS C ON C.person_id = M.person_id GROUP BY ALL)
SELECT * FROM C`
	mustExec(t, rs.admin, "BEGIN")
	if _, err := recognitionRows(rs.admin, sql); err != nil {
		t.Fatal(err)
	}
	if got := texts(t, rs.admin, "SELECT count(*) FROM media"); len(got) != 1 {
		t.Errorf("after the statement, the transaction: %q", got)
	}
	mustExec(t, rs.admin, "COMMIT")
}
