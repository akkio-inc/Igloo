package snowflake

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func translate(t *testing.T, sql string) Result {
	t.Helper()
	res, err := Translate(sql, Options{IsSetting: func(n string) bool { return n == "search_path" || n == "timezone" }})
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res
}

// TestTranslate checks the DuckDB SQL each construct becomes. Results are
// checked against Snowflake's in TestAgainstSnowflake.
func TestTranslate(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// Conditional expressions.
		{`SELECT IFF(a > 0, 'pos', 'neg') AS sign FROM t`, `SELECT CASE WHEN (a > 0) THEN 'pos' ELSE 'neg' END AS "SIGN" FROM t`},
		{`SELECT NVL(a, 0) n, NVL2(a, 1, 2) n2, ZEROIFNULL(a) z, NULLIFZERO(a) nz, EQUAL_NULL(a, b) e FROM t`,
			`SELECT coalesce(a, 0) "N", CASE WHEN a IS NOT NULL THEN 1 ELSE 2 END "N2", coalesce(a, 0) "Z", nullif(a, 0) "NZ", (a IS NOT DISTINCT FROM b) "E" FROM t`},
		{`SELECT DECODE(x, 1, 'one', NULL, 'none', 'other') AS d FROM t`,
			`SELECT CASE WHEN x IS NOT DISTINCT FROM 1 THEN 'one' WHEN x IS NOT DISTINCT FROM NULL THEN 'none' ELSE 'other' END AS "D" FROM t`},
		{`SELECT DIV0(a, b) AS q FROM t`, `SELECT CASE WHEN b = 0 THEN 0 ELSE sf_div(a, b) END AS "Q" FROM t`},
		// Division: the dividend is the operands that *, / and % before it
		// join, from the left, with their unary signs.
		{`SELECT a / b, a * b / c % d / e AS m, x - a / -b + 1 AS s, x * -a / b AS u, -a / 2 AS n, 1 / 3 * 3 AS t FROM t ORDER BY a / b DESC`,
			`SELECT sf_div(a, b) AS "A / B", sf_div(sf_div(a * b, c) % d, e) AS "M", x - sf_div(a, -b) + 1 AS "S", sf_div(x * -a, b) AS "U", sf_div(-a, 2) AS "N", sf_div(1, 3) * 3 AS "T" FROM t ORDER BY sf_div(a, b) DESC NULLS FIRST`},
		{`SELECT sum(x) / nullif(count(DISTINCT y), 0) AS a, sum(x) OVER (PARTITION BY g) / count(*) OVER () AS b, t.a / u."B" AS c, CASE WHEN a THEN 1 ELSE 2 END / CASE WHEN b THEN 3 END AS d, percentile_cont(0.5) WITHIN GROUP (ORDER BY x) / 2 AS e, (a + b) / 2 AS f FROM t, u`,
			`SELECT sf_div(sum(x), nullif(count(DISTINCT y), 0)) AS "A", sf_div(sum(x) OVER (PARTITION BY g), count(*) OVER ()) AS "B", sf_div(t.a, u."B") AS "C", sf_div(CASE WHEN a THEN 1 ELSE 2 END, CASE WHEN b THEN 3 END) AS "D", sf_div(percentile_cont(0.5) WITHIN GROUP (ORDER BY x NULLS LAST), 2) AS "E", sf_div((a + b), 2) AS "F" FROM t, u`},
		// A subquery or a volatile function is computed once.
		{`SELECT count(*) / (SELECT count(*) FROM u) AS a, RANDOM() / 2 AS b FROM t`,
			`SELECT sf_div_once(count(*), (SELECT count(*) FROM u)) AS "A", sf_div_once(CAST((random() - 0.5) * 18446744073709549568.0 AS BIGINT), 2) AS "B" FROM t`},
		{`SELECT GREATEST(a, b) AS g FROM t`, `SELECT CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE greatest(a, b) END AS "G" FROM t`},
		// Dates and times.
		{`SELECT DATEADD(day, 1, d) AS a, DATEADD('month', -1, d) AS b, TIMESTAMPADD(hour, 2, ts) AS c FROM t`,
			`SELECT sf_add_days(d, 1, 1) AS "A", sf_add_months(d, -1, 1) AS "B", sf_add_micros(ts, 2, 3600000000) AS "C" FROM t`},
		{`SELECT DATEDIFF(day, a, b) AS d, DATEDIFF('week', a, b) AS w FROM t`,
			`SELECT date_diff('day', a, b) AS "D", (date_diff('day', date_trunc('week', a), date_trunc('week', b)) // 7) AS "W" FROM t`},
		{`SELECT DATE_TRUNC('MONTH', ts) AS m, TRUNC(d, 'YEAR') AS y FROM t`,
			`SELECT sf_date_trunc('month', ts) AS "M", sf_date_trunc('year', d) AS "Y" FROM t`},
		{`SELECT LAST_DAY(d) AS l, DAYNAME(d) AS n FROM t`, `SELECT last_day(d) AS "L", strftime(d, '%a') AS "N" FROM t`},
		{`SELECT DATE_PART(epoch_second, ts) AS e, EXTRACT(dayofweek FROM d) AS w FROM t`,
			`SELECT CAST(floor(date_part('epoch', ts)) AS BIGINT) AS "E", date_part('dow', d) AS "W" FROM t`},
		{`SELECT TO_DATE(s, 'MM/DD/YYYY') AS d FROM t`, `SELECT CAST(strptime(s, '%m/%d/%Y') AS DATE) AS "D" FROM t`},
		{`SELECT TO_TIMESTAMP_LTZ(ms, 3) AS ts FROM t`, `SELECT to_timestamp(ms / 1e3) AS "TS" FROM t`},
		{`SELECT TO_CHAR(d, 'YYYY-MM') AS m FROM t`, `SELECT strftime(d, '%Y-%m') AS "M" FROM t`},
		{`SELECT CONVERT_TIMEZONE('UTC', 'America/New_York', ts) AS ny FROM t`,
			`SELECT timezone('America/New_York', timezone('UTC', CAST(ts AS TIMESTAMP))) AS "NY" FROM t`},
		{`SELECT TIME_SLICE(ts, 15, 'MINUTE') AS s FROM t`, `SELECT sf_time_slice(ts, to_minutes(CAST(15 AS BIGINT)), '1970-01-01') AS "S" FROM t`},
		// Aggregates.
		{`SELECT LISTAGG(DISTINCT x, ',') WITHIN GROUP (ORDER BY x) AS l FROM t`,
			`SELECT coalesce(string_agg(DISTINCT x, ',' ORDER BY x NULLS LAST), '') AS "L" FROM t`},
		{`SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY y DESC) AS a FROM t`,
			`SELECT coalesce(to_json(list(sf_to_json(x) ORDER BY y DESC NULLS FIRST) FILTER (WHERE sf_to_json(x) IS NOT NULL)), CAST('[]' AS JSON)) AS "A" FROM t`},
		{`SELECT x, RATIO_TO_REPORT(x) OVER (PARTITION BY g) AS r FROM t`,
			`SELECT x, sf_div(x, nullif(sum(x) OVER (PARTITION BY g), 0)) AS "R" FROM t`},
		{`SELECT lag(x) IGNORE NULLS OVER (ORDER BY o) AS l FROM t`, `SELECT lag(x IGNORE NULLS) OVER (ORDER BY o NULLS LAST) AS "L" FROM t`},
		// Semi-structured data.
		{`SELECT v:a.b[0]::string AS s, v:c AS c FROM t WHERE v:type = 'click' AND v:n > 5`,
			`SELECT sf_get_string(v, '$.a.b[0]') AS "S", sf_get(v, '$.c') AS "C" FROM t WHERE sf_get_string(v, '$.type') = 'click' AND CAST(sf_get_string(v, '$.n') AS DOUBLE) > 5`},
		{`SELECT v['a b']:"C" AS x FROM t`, `SELECT sf_get(v, '$."a b".C') AS "X" FROM t`},
		{`SELECT GET(v, 'a') AS a, GET_PATH(v, 'a.b[0]') AS b FROM t`, `SELECT sf_get(v, '$.a') AS "A", sf_get(v, '$.a.b[0]') AS "B" FROM t`},
		{`SELECT PARSE_JSON(s) AS j FROM t`, `SELECT json(s) AS "J" FROM t`},
		{`SELECT ARRAY_CONSTRUCT(1, 2) AS a, [1, 2] AS b, {'k': 1} AS o FROM t`,
			`SELECT json_array(1, 2) AS "A", json_array(1, 2) AS "B", to_json(map_from_entries(list_sort(list_filter([{'k': CAST('k' AS VARCHAR), 'v': to_json(1)}], lambda e: e.v IS NOT NULL)))) AS "O" FROM t`},
		{`SELECT SPLIT(s, ',')[0] AS first FROM t`, `SELECT json_extract(to_json(string_split(s, ',')), '$[0]') AS "FIRST" FROM t`},
		// Strings.
		{`SELECT SPLIT_PART(s, ',', 0) AS p, CHARINDEX('b', s) AS i, LEN(s) AS n FROM t`,
			`SELECT split_part(s, ',', 1) AS "P", strpos(s, 'b') AS "I", length(s) AS "N" FROM t`},
		{`SELECT REGEXP_SUBSTR(s, '[0-9]+', 1, 2) AS r, REGEXP_LIKE(s, 'a.*', 'i') AS l FROM t`,
			`SELECT regexp_extract_all(s, '[0-9]+', 0)[CAST(2 AS BIGINT)] AS "R", regexp_full_match(s, '(?i)a.*') AS "L" FROM t`},
		{`SELECT * FROM t WHERE s RLIKE 'b+' AND s NOT RLIKE 'c'`, `SELECT * FROM t WHERE s SIMILAR TO 'b+' AND s NOT SIMILAR TO 'c'`},
		{`SELECT * FROM t WHERE s ILIKE ANY ('a%', 'b%')`, `SELECT * FROM t WHERE (s ILIKE 'a%' OR s ILIKE 'b%')`},
		{`SELECT CONCAT(a, b) AS c FROM t`, `SELECT (CAST(a AS VARCHAR) || CAST(b AS VARCHAR)) AS "C" FROM t`},
		// Literals, comments and variables.
		{`SELECT 'it\'s' AS a, 'a\\b' AS b, $$raw\n$$ AS c // a comment`, `SELECT 'it''s' AS "A", 'a\b' AS "B", 'raw\n' AS "C" -- a comment`},
		{`SELECT /* a /* b */ 1 AS x`, `SELECT /* a / * b */ 1 AS "X"`},
		{`SELECT $x AS x`, `SELECT getvariable('x') AS "X"`},
		{`SET Limit_Rows = 10`, `SET VARIABLE "limit_rows" = 10`},
		{`SET search_path = 'lake.sales'`, `SET search_path = 'lake.sales'`},
		{`UNSET x`, `RESET VARIABLE "x"`},
		// Types.
		{`SELECT x::number(10,2) AS a, y::int AS b, z::float AS c, w::variant AS d, CAST(q AS TIMESTAMP_LTZ) AS e FROM t`,
			`SELECT CAST(x AS DECIMAL(10,2)) AS "A", CAST(y AS BIGINT) AS "B", CAST(z AS DOUBLE) AS "C", sf_to_json(w) AS "D", CAST(q AS TIMESTAMPTZ) AS "E" FROM t`},
		{`SELECT TIMESTAMP_TZ '2024-01-01 10:00 +02:00' AS ts`, `SELECT CAST('2024-01-01 10:00 +02:00' AS TIMESTAMPTZ) AS "TS"`},
		// Clauses.
		{`SELECT a FROM t MINUS SELECT a FROM u`, `SELECT a FROM t EXCEPT SELECT a FROM u`},
		{`SELECT TOP 10 * FROM t ORDER BY a`, `SELECT * FROM t ORDER BY a NULLS LAST LIMIT 10`},
		{`SELECT a FROM t ORDER BY a DESC, b`, `SELECT a FROM t ORDER BY a DESC NULLS FIRST, b NULLS LAST`},
		{`SELECT a FROM t ORDER BY a DESC NULLS LAST`, `SELECT a FROM t ORDER BY a DESC NULLS LAST`},
		{`SELECT * FROM t SAMPLE (10) SEED (42)`, `SELECT * FROM t TABLESAMPLE bernoulli(10%) REPEATABLE (42)`},
		{`SELECT * FROM t AS x TABLESAMPLE BERNOULLI (5 ROWS)`, `SELECT * FROM t AS x TABLESAMPLE reservoir(5 ROWS)`},
		{`SELECT * FROM VALUES (1, 'a'), (2, 'b')`, `SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS _sf_values(column1, column2)`},
		{`SELECT * FROM (VALUES (1)) v`, `SELECT * FROM (VALUES (1)) v(column1)`},
		{`WITH r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r`,
			`WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r`},
		{`WITH r AS (SELECT 1 AS n UNION ALL SELECT r.n + 1 FROM t JOIN r ON r.n = t.n) SELECT n FROM r`,
			`WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT r.n + 1 FROM t JOIN r ON r.n = t.n) SELECT n FROM r`},
		{`WITH r AS (SELECT 1 AS n UNION ALL SELECT r.n + 1 FROM t, r WHERE r.n = t.n) SELECT n FROM r`,
			`WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT r.n + 1 FROM t, r WHERE r.n = t.n) SELECT n FROM r`},
		// A column named like its common table expression is no reference
		// to it.
		{`WITH tags AS (SELECT t.tags, tags AS n FROM t GROUP BY t.tags, tags) SELECT * FROM tags`,
			`WITH tags AS (SELECT t.tags, tags AS n FROM t GROUP BY t.tags, tags) SELECT * FROM tags`},
		{`SELECT * FROM IDENTIFIER('db.sch.tbl')`, `SELECT * FROM db.sch.tbl`},
		{`SELECT * FROM IDENTIFIER('"My Table"')`, `SELECT * FROM "My Table"`},
		{`SELECT * FROM t PIVOT (SUM(v) FOR k IN ('a', 'b'))`, `SELECT * FROM t PIVOT (SUM(v) FOR k IN ('a' AS "'a'", 'b' AS "'b'"))`},
		{`SELECT * FROM t UNPIVOT (val FOR name IN (col_a, "Col_B"))`, `SELECT * FROM t UNPIVOT (val FOR name IN (col_a AS 'COL_A', "Col_B" AS 'Col_B'))`},
		// Statements.
		{`ALTER SESSION SET TIMEZONE = 'UTC'`, `SET TimeZone = 'UTC'`},
		{`ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 60`, `SET statement_timeout = '60s'`},
		{`USE SCHEMA analytics.marts`, `USE analytics.marts`},
		{`DESC TABLE t`, `DESCRIBE t`},
		{`GRANT SELECT ON TABLE t TO ROLE analyst`, `GRANT SELECT ON TABLE t TO analyst`},
		{`GRANT ROLE analyst TO USER ana`, `GRANT analyst TO ana`},
		{`CREATE OR REPLACE TRANSIENT TABLE t (id INT, name STRING, amount NUMBER(10,2), ts TIMESTAMP_NTZ, v VARIANT) CLUSTER BY (id) COPY GRANTS`,
			`CREATE OR REPLACE TABLE t (id BIGINT, name VARCHAR, amount DECIMAL(10,2), ts TIMESTAMP, v JSON)`},
		{`CREATE TEMPORARY TABLE t AS SELECT 1 AS a`, `CREATE TEMPORARY TABLE t AS SELECT 1 AS a`},
		{`CREATE OR REPLACE VIEW v COPY GRANTS AS SELECT count(*) FROM t`, `CREATE OR REPLACE VIEW v AS SELECT count(*) AS "COUNT(*)" FROM t`},
		{`ALTER TABLE t ADD COLUMN c TIMESTAMP_LTZ`, `ALTER TABLE t ADD COLUMN c TIMESTAMPTZ`},
		// Statements Igloo carries out itself keep their syntax; their SQL is
		// translated.
		{`CREATE TASK nightly SCHEDULE '0 6 * * *' AS INSERT INTO t SELECT IFF(a, 1, 2) FROM u`,
			`CREATE TASK nightly SCHEDULE '0 6 * * *' AS INSERT INTO t SELECT CASE WHEN a THEN 1 ELSE 2 END FROM u`},
		{`ALTER SESSION SET QUERY_TAG = 'team:\'x\''`, `ALTER SESSION SET QUERY_TAG = 'team:''x'''`},
		// PostgreSQL catalog queries pass as written.
		{`SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname ~ E'^t\\d$'`,
			`SELECT c.relname FROM pg_catalog.pg_class c WHERE c.relname ~ E'^t\\d$'`},
	} {
		if got := translate(t, c.in).SQL; got != c.want {
			t.Errorf("%s\n got: %s\nwant: %s", c.in, got, c.want)
		}
	}
}

// TestFlattenHoistedUnderItsTableName: a FLATTEN of objects is hoisted
// into its FROM item's select list when the item's alias is its table's
// name, as it is under another alias.
func TestFlattenHoistedUnderItsTableName(t *testing.T) {
	for _, from := range []string{"t as t", "t t", "T AS t"} {
		res, err := Translate(`select cast(get_path(g.value, 'kind') as text), cast(v.value as text), count(distinct t.id) from `+from+`
			cross join lateral flatten(input => [object_construct('kind', 'a', 'items', t.a), object_construct('kind', 'b', 'items', t.b)]) as g(seq, key, path, index, value, this)
			cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)
			where not cast(v.value as text) is null and trim(cast(v.value as text)) <> '' group by 1, 2`, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.SQL, "AS _sf_t FROM "+from) {
			t.Errorf("from %s, not hoisted:\n%s", from, res.SQL)
		}
	}
}

func TestTranslateNoticesAndCommands(t *testing.T) {
	for _, c := range []struct {
		in, command string
		notices     []string
	}{
		{`ALTER SESSION SET USE_CACHED_RESULT = FALSE`, "ALTER SESSION", nil},
		{`ALTER SESSION SET WEEK_START = 0, WEEK_OF_YEAR_POLICY = 0`, "ALTER SESSION", nil},
		{`ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM', TIMESTAMP_LTZ_OUTPUT_FORMAT = '', BINARY_OUTPUT_FORMAT = 'hex'`, "ALTER SESSION", nil},
		{`USE WAREHOUSE wh`, "USE", []string{"USE WAREHOUSE changes nothing"}},
		{`USE ROLE analyst`, "USE", []string{"USE ROLE changes nothing"}},
		{`GRANT USAGE ON DATABASE db TO ROLE r`, "GRANT", []string{"GRANT USAGE on a database changes nothing"}},
		{`ALTER TABLE t CLUSTER BY (to_date(ts))`, "ALTER TABLE", []string{"columns, not expressions"}},
		{`ALTER TABLE t RECLUSTER`, "ALTER TABLE", []string{"table maintenance clusters"}},
		{`ALTER TABLE t SUSPEND RECLUSTER`, "ALTER TABLE", []string{"table maintenance clusters"}},
		{`ALTER TABLE t SET DATA_RETENTION_TIME_IN_DAYS = 0`, "ALTER TABLE", []string{"DATA_RETENTION_TIME_IN_DAYS is ignored"}},
		{`CREATE TABLE t (a INT) CLUSTER BY (a + 1)`, "", []string{"CLUSTER BY is ignored: Igloo clusters tables by columns"}},
		{`CREATE TEMPORARY TABLE t (a INT) CLUSTER BY (a)`, "", []string{"CLUSTER BY is ignored for a temporary table"}},
		{`CREATE TABLE IF NOT EXISTS t (a INT) CLUSTER BY (a)`, "", []string{"CLUSTER BY is ignored with IF NOT EXISTS"}},
		{`CREATE TABLE t (a INT COMMENT 'x')`, "", []string{"column comments"}},
		{`CREATE SECURE VIEW v AS SELECT 1 AS a`, "", []string{"SECURE is ignored"}},
		{`SELECT HASH(a) FROM t`, "", []string{"HASH computes DuckDB's hash"}},
		{`SELECT RANDOM(42)`, "", []string{"seed is ignored"}},
	} {
		res := translate(t, c.in)
		if res.Command != c.command {
			t.Errorf("%s: command %q, want %q", c.in, res.Command, c.command)
		}
		for _, n := range c.notices {
			if !slices.ContainsFunc(res.Notices, func(got string) bool { return strings.Contains(got, n) }) {
				t.Errorf("%s: notices %q, want one with %q", c.in, res.Notices, n)
			}
		}
	}
}

// TestRefusals checks that constructs Igloo cannot translate faithfully
// are refused with SQLSTATE 0A000 (or 42601 for what is not valid SQL),
// naming the construct.
func TestRefusals(t *testing.T) {
	for _, c := range []struct{ in, code, says string }{
		{`SET (a, b) = (1, 2)`, CodeUnsupported, "SET (a, b)"},
		{`ALTER SESSION SET WEEK_START = 1`, CodeUnsupported, "WEEK_START = 1"},
		{`ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = TIMESTAMP_LTZ`, CodeUnsupported, "TIMESTAMP_TYPE_MAPPING"},
		{`ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT = 'YYYY-MM-DD'`, CodeUnsupported, "TIMESTAMP_NTZ_OUTPUT_FORMAT"},
		{`ALTER SESSION SET BINARY_OUTPUT_FORMAT = 'BASE64'`, CodeUnsupported, "BINARY_OUTPUT_FORMAT"},
		{`ALTER SESSION SET NO_SUCH_PARAMETER = 1`, CodeUnsupported, "NO_SUCH_PARAMETER"},
		{`INSERT OVERWRITE INTO t SELECT 1`, CodeUnsupported, "INSERT OVERWRITE"},
		{`INSERT ALL INTO t VALUES (1) SELECT 1`, CodeUnsupported, "INSERT ALL"},
		{`BEGIN LET x := 1; END`, CodeUnsupported, "Snowflake Scripting"},
		{`CALL my_procedure()`, CodeUnsupported, "stored procedures"},
		{`PUT file:///tmp/x @stage`, CodeUnsupported, "stages"},
		{`CREATE STAGE s`, CodeUnsupported, "CREATE STAGE"},
		{`CREATE FUNCTION f(x INT) RETURNS INT AS 'x + 1'`, CodeUnsupported, "CREATE FUNCTION"},
		{`CREATE TABLE t (id INT AUTOINCREMENT)`, CodeUnsupported, "AUTOINCREMENT"},
		{`ALTER TABLE a SWAP WITH b`, CodeUnsupported, "SWAP WITH"},
		{`SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))`, CodeUnsupported, "RESULT_SCAN"},
		{`SELECT CONDITIONAL_CHANGE_EVENT(x) OVER (ORDER BY y) FROM t`, CodeUnsupported, "CONDITIONAL_CHANGE_EVENT"},
		{`SELECT REGEXP_REPLACE(s, 'a', 'b', 1, 2) FROM t`, CodeUnsupported, "REGEXP_REPLACE"},
		{`SELECT TO_CHAR(n, 'S999') FROM t`, CodeUnsupported, "format"},
		{`SELECT SHA2(s, 512) FROM t`, CodeUnsupported, "256 bits"},
		{`SELECT UUID_STRING('ns', 'name')`, CodeUnsupported, "UUID_STRING"},
		{`SELECT x::VECTOR(FLOAT, 3) FROM t`, CodeUnsupported, "VECTOR"},
		{`SELECT * FROM t SAMPLE (n)`, CodeUnsupported, "SAMPLE"},
		{`SELECT TO_CHAR(d, fmt) FROM t`, CodeUnsupported, "computed format"},
		{`SELECT IDENTIFIER(a || b) FROM t`, CodeUnsupported, "IDENTIFIER"},
		{`SELECT 'unterminated`, CodeSyntax, "unterminated"},
		{`SELECT (1`, CodeSyntax, "unclosed"},
		{`SELECT IFF(a, b)`, CodeSyntax, "IFF takes 3 arguments"},
		{`SELECT 'a\0b'`, CodeSyntax, "NUL"},
		{`SELECT 'a\x00b'`, CodeSyntax, "NUL"},
		{`SELECT 'a\u0000b'`, CodeSyntax, "NUL"},
	} {
		_, err := Translate(c.in, Options{})
		var e *Error
		if !errors.As(err, &e) || e.Code != c.code || !strings.Contains(e.Message, c.says) {
			t.Errorf("%s: got %v, want %s mentioning %q", c.in, err, c.code, c.says)
		}
		if e != nil && e.Code == CodeUnsupported && !strings.Contains(e.Message, "is not supported") {
			t.Errorf("%s: message %q", c.in, e.Message)
		}
	}
}

func TestSplit(t *testing.T) {
	got := Split("SELECT 'a;b'; SELECT $$c;d$$ // e;f\n; -- only a comment;\nSELECT \"g;h\";")
	want := []string{"SELECT 'a;b'", "SELECT $$c;d$$ // e;f", "-- only a comment;\nSELECT \"g;h\""}
	if !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestColumnName(t *testing.T) {
	quoted := map[string]bool{"keep": true}
	for name, want := range map[string]string{
		"o_orderkey": "O_ORDERKEY", "keep": "keep", "MixedCase": "MixedCase", "count_star()": "COUNT_STAR()", "X": "X",
	} {
		if got := ColumnName(name, quoted); got != want {
			t.Errorf("%s: %s, want %s", name, got, want)
		}
	}
}

// TestLexerAgreesWithDuckDB checks that what the translation writes reads
// the same in DuckDB's lexer: every string the translation emits is a
// standard string, which reads backslashes as themselves, so a value
// cannot end early or swallow the SQL after it.
func TestTranslatedStringsStayStrings(t *testing.T) {
	for _, in := range []string{
		`SELECT 'a\'; DROP TABLE x; --'`,
		`SELECT 'a\\'`,
		`SELECT $$'; DROP TABLE x; --$$`,
		`SELECT 'tab\there\\', 'x'`,
	} {
		out := translate(t, in).SQL
		// One statement, as DuckDB's lexer (and so Igloo's checks) reads it.
		if parts := Split(strings.ReplaceAll(out, `\`, `\\`)); len(parts) != 1 {
			t.Errorf("%s became %s", in, out)
		}
		if strings.Contains(out, "DROP TABLE x;") && !strings.Contains(out, "'a''; DROP TABLE x; --'") &&
			!strings.Contains(out, "'''; DROP TABLE x; --'") {
			t.Errorf("%s became %s", in, out)
		}
	}
}

// TestTranslateClusterBy checks the translations of a clustering key: the
// table's sort order, set by ALTER TABLE ... SET SORTED BY, after CREATE
// TABLE has created the table.
func TestTranslateClusterBy(t *testing.T) {
	for _, c := range []struct {
		in, want string
		after    []string
	}{
		{`ALTER TABLE t CLUSTER BY (a, "Mixed")`, `ALTER TABLE t SET SORTED BY (a, "Mixed")`, nil},
		{`ALTER TABLE IF EXISTS db.s.t CLUSTER BY (day)`, `ALTER TABLE IF EXISTS db.s.t SET SORTED BY (day)`, nil},
		{`ALTER TABLE s.t DROP CLUSTERING KEY`, `ALTER TABLE s.t RESET SORTED BY`, nil},
		{`CREATE TABLE db.s.t (a INT, b STRING) CLUSTER BY (b, a)`, `CREATE TABLE db.s.t (a BIGINT, b VARCHAR)`,
			[]string{`ALTER TABLE db.s.t SET SORTED BY (b, a)`}},
		{`CREATE OR REPLACE TABLE "My Table" CLUSTER BY (a) AS SELECT 1 AS a`, `CREATE OR REPLACE TABLE "My Table" AS SELECT 1 AS a`,
			[]string{`ALTER TABLE "My Table" SET SORTED BY (a)`}},
		{`CREATE TABLE t (a INT) CLUSTER BY (a + 1)`, `CREATE TABLE t (a BIGINT)`, nil},
	} {
		res := translate(t, c.in)
		if res.SQL != c.want || !slices.Equal(res.After, c.after) || res.Command != "" {
			t.Errorf("%s\n got: %s %q %q\nwant: %s %q", c.in, res.SQL, res.After, res.Command, c.want, c.after)
		}
	}
}

// TestImplicitConversions checks which operands Options.Implicit converts:
// those of comparisons, BETWEEN and LIKE that stand alone, not literals,
// not an UPDATE's assignments, and not an operand of a tighter operator.
func TestImplicitConversions(t *testing.T) {
	for _, c := range []struct{ in, has, hasNot string }{
		{`SELECT * FROM t WHERE a < b`, `sf_as(a, b) < sf_as(b, a)`, ""},
		{`SELECT * FROM t WHERE x.a >= y.b AND c = d`, `sf_as(x.a, y.b) >= sf_as(y.b, x.a) AND sf_as(c, d) = sf_as(d, c)`, ""},
		{`SELECT * FROM t WHERE ts NOT BETWEEN lo AND hi`, `sf_as(sf_as(ts, lo), hi) NOT BETWEEN sf_as(sf_as(lo, ts), hi) AND sf_as(sf_as(hi, ts), lo)`, ""},
		{`SELECT * FROM t WHERE ts BETWEEN '2024-01-01' AND hi`, `sf_as(ts, hi) BETWEEN '2024-01-01' AND sf_as(hi, ts)`, ""},
		{`SELECT * FROM t WHERE ts BETWEEN lo AND '2024-01-01'`, `sf_as(ts, lo) BETWEEN sf_as(lo, ts) AND '2024-01-01'`, ""},
		{`SELECT * FROM t WHERE n LIKE p`, `sf_str(n) LIKE sf_str(p)`, ""},
		{`SELECT upper(n), left(n, 2) FROM t`, `upper(sf_str(n))`, "sf_str(2)"},
		{`SELECT upper('a') FROM t WHERE a < 5 AND s = 'x' AND b IS NULL`, "", "sf_"},
		{`SELECT a + b < c, a < b + c FROM t`, "", "sf_as"},
		{`UPDATE t SET a = b, c = d WHERE e = f`, `SET a = b, c = d WHERE sf_as(e, f) = sf_as(f, e)`, ""},
		// A WHERE that a FLATTEN's join takes as its ON (see whereOn).
		{`SELECT f.value FROM t AS b CROSS JOIN r, LATERAL FLATTEN(INPUT => b.tags) AS f WHERE b.ts BETWEEN r.lo AND r.hi AND b.n = r.m`,
			`ON (sf_as(sf_as(b.ts, r.lo), r.hi) BETWEEN sf_as(sf_as(r.lo, b.ts), r.hi) AND sf_as(sf_as(r.hi, b.ts), r.lo) AND sf_as(b.n, r.m) = sf_as(r.m, b.n))`, ""},
	} {
		res, err := Translate(c.in, Options{Implicit: true})
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if c.has != "" && !strings.Contains(res.SQL, c.has) {
			t.Errorf("%s:\n  got  %s\n  want %s in it", c.in, res.SQL, c.has)
		}
		if c.hasNot != "" && strings.Contains(res.SQL, c.hasNot) {
			t.Errorf("%s:\n  got  %s\n  want no %s in it", c.in, res.SQL, c.hasNot)
		}
		plain, err := Translate(c.in, Options{})
		if err != nil || strings.Contains(plain.SQL, "sf_as(") || strings.Contains(plain.SQL, "sf_str(") {
			t.Errorf("%s without Implicit: %s %v", c.in, plain.SQL, err)
		}
	}
}

func TestTemporaryNames(t *testing.T) {
	known := func(schema, name string) bool { return schema == "s" && name == "m__dbt_tmp" }
	for _, c := range []struct {
		in, has, hasNot  string
		known            bool
		created, dropped *TempName
	}{
		{in: `create or replace temporary table db.s.m__dbt_tmp as (select 1 as a)`, has: `temporary table temp.main.m__dbt_tmp as`,
			created: &TempName{"s", "m__dbt_tmp"}},
		{in: `CREATE TEMP VIEW "S"."V__DBT_TMP" ("A" COMMENT $$x$$) AS SELECT 1 AS a`, has: `VIEW temp.main."V__DBT_TMP"`,
			created: &TempName{"s", "v__dbt_tmp"}},
		{in: `create temporary table t as select 1 as a`, has: `temporary table t as`},
		{in: `create table db.s.m__dbt_tmp as select 1 as a`, has: `table db.s.m__dbt_tmp as`},
		{in: `delete from db.s.m where id in (select id from db.s.m__dbt_tmp)`, known: true,
			has: `delete from db.s.m where id in (select id from temp.main.m__dbt_tmp)`},
		{in: `drop table if exists s.m__dbt_tmp`, known: true, has: `drop table if exists temp.main.m__dbt_tmp`,
			dropped: &TempName{"s", "m__dbt_tmp"}},
		{in: `select s.m__dbt_tmp(1), v:s.m__dbt_tmp from db.s.m__dbt_tmp`, has: `from db.s.m__dbt_tmp`},
		{in: `select s.m__dbt_tmp(1) from t`, known: true, hasNot: "temp.main"},
	} {
		opts := Options{}
		if c.known {
			opts.Temporary = known
		}
		res, err := Translate(c.in, opts)
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if c.has != "" && !strings.Contains(res.SQL, c.has) {
			t.Errorf("%s:\n  got  %s\n  want %s in it", c.in, res.SQL, c.has)
		}
		if c.hasNot != "" && strings.Contains(res.SQL, c.hasNot) {
			t.Errorf("%s:\n  got  %s\n  want no %s in it", c.in, res.SQL, c.hasNot)
		}
		if !sameTemp(res.Temporary, c.created) || !sameTemp(res.Dropped, c.dropped) {
			t.Errorf("%s: created %v dropped %v, want %v %v", c.in, res.Temporary, res.Dropped, c.created, c.dropped)
		}
	}
}

func sameTemp(a, b *TempName) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
