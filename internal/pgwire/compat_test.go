package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestIgnoredCommand(t *testing.T) {
	t.Parallel()
	ignored := []string{
		"SET application_name = 'psql'",
		"set Application_Name to 'x'",
		"SET SESSION extra_float_digits = 3",
		"set jit = off",
		`SET "DateStyle" = 'ISO'`,
		"-- driver setup\nSET client_encoding = 'UTF8'",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
	}
	for _, q := range ignored {
		if tag, ok := ignoredCommand(q); !ok || tag != "SET" {
			t.Errorf("%q should be ignored", q)
		}
	}

	passedThrough := []string{
		"SET threads = 4",
		"SET search_path = 'lake.main'",
		"SELECT 'SET application_name = 1'",
		"SET application_names = 1",
	}
	for _, q := range passedThrough {
		if _, ok := ignoredCommand(q); ok {
			t.Errorf("%q should reach DuckDB", q)
		}
	}
}

func TestCommandTag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		t     duckdb.StmtType
		query string
		n     int64
		want  string
	}{
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT 1", 1, "SELECT 1"},
		{duckdb.STATEMENT_TYPE_INSERT, "INSERT INTO t VALUES (1)", 1, "INSERT 0 1"},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE OR REPLACE TEMP VIEW v AS SELECT 1", 0, "CREATE VIEW"},
		{duckdb.STATEMENT_TYPE_CREATE, "/* dbt */ create schema if not exists s", 0, "CREATE SCHEMA"},
		{duckdb.STATEMENT_TYPE_DROP, "DROP TABLE IF EXISTS t", 0, "DROP TABLE"},
		{duckdb.STATEMENT_TYPE_TRANSACTION, "begin", 0, "BEGIN"},
		{duckdb.STATEMENT_TYPE_TRANSACTION, "END", 0, "COMMIT"},
		{duckdb.STATEMENT_TYPE_TRANSACTION, "ABORT", 0, "ROLLBACK"},
		{duckdb.STATEMENT_TYPE_ATTACH, "ATTACH 'x.db'", 0, "ATTACH"},
	}
	for _, c := range cases {
		if got := commandTag(c.t, c.query, c.n); got != c.want {
			t.Errorf("%q: got %q, want %q", c.query, got, c.want)
		}
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	cases := []struct {
		t       duckdb.StmtType
		columns []string
		want    kind
	}{
		{duckdb.STATEMENT_TYPE_SELECT, []string{"a"}, returnsRows},
		{duckdb.STATEMENT_TYPE_INSERT, []string{"Count"}, command},
		{duckdb.STATEMENT_TYPE_INSERT, []string{"id"}, returnsRows},
		{duckdb.STATEMENT_TYPE_CREATE, []string{"Count"}, command},
		{duckdb.STATEMENT_TYPE_PRAGMA, nil, command},
	}
	for _, c := range cases {
		if got := classify(c.t, c.columns); got != c.want {
			t.Errorf("classify(%v, %v) = %v, want %v", c.t, c.columns, got, c.want)
		}
	}
}

func TestRewriteCatalogQuery(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		// Queries without pg_ names or reg* types are never touched, nor
		// are literals, quoted identifiers and comments in queries that
		// have them.
		"SELECT x::text FROM t":     "SELECT x::text FROM t",
		"SELECT x::regclass FROM t": "SELECT x::VARCHAR FROM t",
		"SELECT 'OPERATOR(pg_catalog.~)', E'it\\'s pg_catalog.f(', $$pg_catalog.g($$ -- pg_catalog.h(\nFROM pg_catalog.pg_class": "SELECT 'OPERATOR(pg_catalog.~)', E'it\\'s pg_catalog.f(', $$pg_catalog.g($$ -- pg_catalog.h(\nFROM pg_class",
		`SELECT "pg_catalog.x(" FROM pg_catalog.pg_class /* pg_catalog.y( */`:                                                    `SELECT "pg_catalog.x(" FROM pg_class /* pg_catalog.y( */`,

		"WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default":                   "WHERE c.relname ~ '^(t)$'",
		"SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c":                    "SELECT pg_get_userbyid(c.relowner) FROM pg_class c",
		"SELECT c.reloftype::pg_catalog.regtype::pg_catalog.text FROM pg_catalog.pg_namespace":        "SELECT c.reloftype::VARCHAR::text FROM pg_namespace",
		"SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d":        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d",
		"FROM pg_catalog.generate_series(0, pg_catalog.array_upper(a, 1)) s, pg_catalog.pg_attribute": "FROM generate_series(0, array_upper(a, 1)) AS s(s), pg_attribute",
		// pgjdbc's getTables join on comments.
		"LEFT JOIN pg_catalog.pg_description d ON (d.classoid = 'pg_class'::regclass)": "LEFT JOIN pg_description d ON (d.classoid = 1259)",
		// dbt-postgres's view dependencies.
		"join pg_depend d on d.classid = 'pg_rewrite'::regclass and d.refclassid = 'pg_class' :: pg_catalog.regclass": "join pg_depend d on d.classid = 2618 and d.refclassid = 1259",
		// SQLAlchemy's reflection.
		"ON d.classoid = CAST($2 AS REGCLASS) WHERE x = CAST(CAST(a.attrelid AS pg_catalog.regclass) AS TEXT)": "ON d.classoid = pg_regclass($2::VARCHAR) WHERE x = CAST(pg_regclass(a.attrelid) AS TEXT)",
		"SELECT CAST(f('a)', ')') AS regclass), 'CAST(x AS regclass)' FROM pg_catalog.pg_class":                "SELECT pg_regclass(f('a)', ')')), 'CAST(x AS regclass)' FROM pg_class",
		"SELECT pg_catalog.json_build_object('a', 1)":                                                          "SELECT json_object('a', 1)",
		// information_schema.columns in any quoting, as dbt, Npgsql and
		// Metabase write it, and PostgreSQL's format().
		`SELECT FORMAT('%I.%I', "c"."table_schema", c.table_name), format('{}%', 1) FROM "information_schema"."columns" AS "c", information_schema.tables`: `SELECT printf('%s.%s', "c"."table_schema", c.table_name), format('{}%', 1) FROM pg_information_schema_columns AS "c", information_schema.tables`,
		"SELECT column_name FROM information_schema . columns WHERE table_name = 'columns'":                                                                "SELECT column_name FROM pg_information_schema_columns WHERE table_name = 'columns'",
		// SQLAlchemy 1.4's indexes (Superset).
		"FROM pg_class t LEFT JOIN pg_attribute a ON t.oid = a.attrelid AND a.attnum = ANY(ix.indkey) WHERE x = ANY($1)": "FROM pg_class t LEFT JOIN pg_attribute a ON t.oid = a.attrelid AND list_contains(ix.indkey, a.attnum) WHERE x = ANY($1)",
		// Metabase quotes the qualifier.
		`SELECT * FROM "pg_catalog"."pg_class" AS "c" JOIN "pg_catalog"."pg_tables" t ON true, "pg_catalog".format_type(1, 2), '"pg_catalog"."pg_type"'`: `SELECT * FROM "pg_class" AS "c" JOIN "pg_catalog"."pg_tables" t ON true, format_type(1, 2), '"pg_catalog"."pg_type"'`,
	}
	for in, want := range cases {
		if got := rewriteCatalogQuery(in); got != want {
			t.Errorf("rewriteCatalogQuery(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestParseStatementTimeout(t *testing.T) {
	t.Parallel()
	cases := map[string]time.Duration{
		"0": 0, "1500": 1500 * time.Millisecond, "'30s'": 30 * time.Second,
		"'5min'": 5 * time.Minute, "'2h'": 2 * time.Hour, "'250 ms'": 250 * time.Millisecond,
	}
	for in, want := range cases {
		got, isDefault, err := parseStatementTimeout(in)
		if err != nil || isDefault || got != want {
			t.Errorf("%s: got %s %v %v, want %s", in, got, isDefault, err, want)
		}
	}
	if _, isDefault, err := parseStatementTimeout("DEFAULT"); err != nil || !isDefault {
		t.Errorf("DEFAULT: %v %v", isDefault, err)
	}
	for _, bad := range []string{"'soon'", "-1", "'1y'"} {
		if _, _, err := parseStatementTimeout(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

func TestRewriteSessionUser(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"SELECT current_user":                     `SELECT 'ana' AS "current_user"`,
		"select CURRENT_USER, session_user()":     `select 'ana' AS "current_user", 'ana' AS "session_user"`,
		"SELECT 'current_user', \"current_user\"": "SELECT 'current_user', \"current_user\"",
		"SELECT current_users FROM t":             "SELECT current_users FROM t",
		// Whole items of a select list keep PostgreSQL's column names.
		"SELECT a, current_role() FROM t WHERE b = current_user": `SELECT a, 'ana' AS "current_role" FROM t WHERE b = 'ana'`,
		"SELECT DISTINCT current_role\n-- who\nFROM t":           `SELECT DISTINCT 'ana' AS "current_role"` + "\n-- who\nFROM t",
		"SELECT * FROM (SELECT current_user) s":                  `SELECT * FROM (SELECT 'ana' AS "current_user") s`,
		"SELECT 1 UNION ALL SELECT session_user;":                `SELECT 1 UNION ALL SELECT 'ana' AS "session_user";`,
		"CREATE TABLE t AS SELECT current_user ORDER BY 1":       `CREATE TABLE t AS SELECT 'ana' AS "current_user" ORDER BY 1`,
		// Others are left as they were: with an alias, in an expression,
		// an argument or a list, and outside select lists.
		"SELECT current_user AS who, current_user u":             "SELECT 'ana' AS who, 'ana' u",
		`SELECT current_user() AS "CURRENT_USER()"`:              `SELECT 'ana' AS "CURRENT_USER()"`,
		"SELECT upper(current_user), current_user = 'ana'":       "SELECT upper('ana'), 'ana' = 'ana'",
		"SELECT coalesce(a, current_user), [1, current_user]":    "SELECT coalesce(a, 'ana'), [1, 'ana']",
		"SELECT current_user::VARCHAR, (current_user)":           "SELECT 'ana'::VARCHAR, ('ana')",
		"INSERT INTO t VALUES (current_user)":                    "INSERT INTO t VALUES ('ana')",
		"SELECT x FROM t GROUP BY current_user, x":               "SELECT x FROM t GROUP BY 'ana', x",
		"SELECT CASE WHEN a THEN current_user END, current_user": `SELECT CASE WHEN a THEN 'ana' END, 'ana' AS "current_user"`,
	}
	for in, want := range cases {
		if got := rewriteSessionUser(in, "ana"); got != want {
			t.Errorf("rewriteSessionUser(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRewriteSystemSchemaRefs(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]string{
		// dbt's get_columns_in_relation
		`select column_name from "lake".information_schema.columns where table_name = 'm'`: `select column_name from information_schema.columns where table_name = 'm'`,
		"SELECT * FROM LAKE . pg_catalog.pg_class, lake.information_schema.tables":         "SELECT * FROM pg_catalog.pg_class, information_schema.tables",
		"SELECT * FROM lake.public.columns":                                                "SELECT * FROM lake.public.columns",
		"SELECT * FROM other.information_schema.columns":                                   "SELECT * FROM other.information_schema.columns",
		"SELECT 'lake.information_schema.columns'":                                         "SELECT 'lake.information_schema.columns'",
		"SELECT x.lake.information_schema.columns":                                         "SELECT x.lake.information_schema.columns",
	} {
		if got := rewriteSystemSchemaRefs(q, "lake"); got != want {
			t.Errorf("rewriteSystemSchemaRefs(%q) = %q, want %q", q, got, want)
		}
	}
}

func TestDBTCatalogQueries(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	// dbt-postgres lists relations from pg_tables, pg_views and pg_matviews.
	mustExec(t, conn, "CREATE TABLE facts (i INTEGER)")
	rows, err := conn.Query(context.Background(), `select tablename from pg_tables where schemaname ilike 'main'
		union all select viewname from pg_views where schemaname ilike 'main'
		union all select matviewname from pg_catalog.pg_matviews where schemaname ilike 'main'`)
	if err != nil {
		t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || !slices.Contains(names, "facts") {
		t.Errorf("relations: %q %v", names, err)
	}
}

func TestShowPostgresSettings(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	for name, mode := range modes {
		conn := ts.admin(t, mode)
		for sql, want := range map[string]string{
			// SQLAlchemy and JDBC read the isolation level when they connect.
			"show transaction isolation level": "repeatable read",
			"SHOW standard_conforming_strings": "on",
			"SHOW is_superuser":                "on",
			"SHOW server_version_num;":         "150000",
		} {
			var got string
			if err := conn.QueryRow(context.Background(), sql).Scan(&got); err != nil || got != want {
				t.Errorf("%s: %s: %q %v, want %q", name, sql, got, err, want)
			}
		}
		// DuckDB's own setting, which defaults to the machine's zone.
		var zone string
		if err := conn.QueryRow(context.Background(), `SHOW "TimeZone"`).Scan(&zone); err != nil || !strings.HasSuffix(zone, "UTC") {
			t.Errorf("%s: SHOW TimeZone: %q %v", name, zone, err)
		}
		// current_setting, of a literal or (as SQLAlchemy sends it) a
		// parameter.
		var version, method, zone2 string
		err := conn.QueryRow(context.Background(), "SELECT current_setting('server_version_num'), current_setting($1), current_setting('TimeZone')",
			"default_table_access_method").Scan(&version, &method, &zone2)
		if err != nil || version != "150000" || method != "heap" || zone2 == "" {
			t.Errorf("%s: current_setting: %q %q %q %v", name, version, method, zone2, err)
		}
		// DuckDB's own SHOW still describes tables.
		if _, err := conn.Exec(context.Background(), "SHOW TABLES"); err != nil {
			t.Errorf("%s: SHOW TABLES: %v", name, err)
		}
	}
}

// TestDriverCatalogQueries runs the catalog lookups of JDBC's key metadata
// and SQLAlchemy's reflection that need more than the views above.
func TestDriverCatalogQueries(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		conn := ts.admin(t, mode)
		var setting string
		var position int64
		err := conn.QueryRow(ctx, `SELECT setting, (SELECT max(n) FROM (SELECT (information_schema._pg_expandarray([5, 7])).n AS n))
			FROM pg_catalog.pg_settings WHERE name = 'max_index_keys'`).Scan(&setting, &position)
		if err != nil || setting != "32" || position != 2 {
			t.Errorf("%s: JDBC's max_index_keys and _pg_expandarray: %q %d %v", name, setting, position, err)
		}
		// DuckDB binds json_object of a parameter only with the value, as
		// SQLAlchemy's column reflection sends it; the result is described
		// all the same.
		rows, err := conn.Query(ctx, "SELECT json_build_object($1::VARCHAR, 1) AS options, 2 AS n FROM pg_catalog.pg_class LIMIT 1", "always")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		fields := rows.FieldDescriptions()
		rows.Close()
		if len(fields) != 2 || fields[0].Name != "options" || fields[1].Name != "n" || fields[1].DataTypeOID != pgtype.Int4OID || rows.Err() != nil {
			t.Errorf("%s: json_object of a parameter: %+v %v", name, fields, rows.Err())
		}
		var oid int64
		if err := conn.QueryRow(ctx, "SELECT CAST($1 AS REGCLASS)", "pg_catalog.pg_constraint").Scan(&oid); err != nil || oid != 2606 {
			t.Errorf("%s: a catalog name cast to regclass: %d %v", name, oid, err)
		}
	}
}

func TestToChar(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	var got []string
	err := conn.QueryRow(context.Background(), `SELECT [
		to_char(DATE_TRUNC('month', DATE '2026-10-05'), 'YYYY-MM'),
		to_char(TIMESTAMP '2026-10-05 14:07:09', 'YYYY-MM-DD HH24:MI:SS'),
		to_char(DATE '2026-10-05', 'FMMonth DD, YYYY (Dy)'),
		to_char(TIMESTAMP '2026-10-05 14:07:09', 'HH12:MI AM, 100%')]`).Scan(&got)
	want := []string{"2026-10", "2026-10-05 14:07:09", "October 05, 2026 (Mon)", "02:07 PM, 100%"}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("got %q %v, want %q", got, err, want)
	}
}

// TestTimestamptzText checks that timestamps with time zone reach clients
// in text as PostgreSQL writes them: psycopg2 cannot parse pgx's "Z".
func TestTimestamptzText(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	res := conn.PgConn().ExecParams(context.Background(),
		"SELECT TIMESTAMPTZ '2026-10-14 12:00:00.25+02', TIMESTAMPTZ '2026-10-14 12:00:00+00'",
		nil, nil, nil, []int16{pgtype.TextFormatCode}).Read()
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	got := []string{string(res.Rows[0][0]), string(res.Rows[0][1])}
	if !slices.Equal(got, []string{"2026-10-14 10:00:00.25+00", "2026-10-14 12:00:00+00"}) {
		t.Errorf("got %q", got)
	}
}

// TestMissingNameCodes checks that statements naming what does not exist
// fail with PostgreSQL's SQLSTATE for what is missing, a function, a type,
// a table or a setting, for admins and for other users.
func TestMissingNameCodes(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	codes := map[string]string{
		"SELECT no_such_function(1)":             "42883",
		"SELECT * FROM no_such_table_function()": "42883",
		"SELECT 1::NUMBER":                       "42704",
		"SELECT CAST(1 AS no_such_type)":         "42704",
		"SELECT * FROM no_such_table":            "42P01",
		"SELECT * FROM no_such_schema.t":         "42P01",
		"SELECT nextval('no_such_sequence')":     "42P01",
		"SELECT no_such_column FROM (SELECT 1)":  "42703",
		"SHOW no_such_setting":                   "42704",
		"SET no_such_setting = 1":                "42704",
		"SET SESSION no_such_setting TO 1":       "42704",
		`SET "No_Such_Setting" = 1`:              "42704",
		"RESET no_such_setting":                  "42704",
	}
	for name, mode := range modes {
		rita := connect(t, ts.url("rita", "rita-password"), mode)
		for _, conn := range []*pgx.Conn{rita, ts.admin(t, mode)} {
			user := userName(t, conn)
			for sql, want := range codes {
				_, err := conn.Exec(ctx, sql)
				if got := pgCode(err); got != want {
					t.Errorf("%s, %s: %s: %s (%v), want %s", name, user, sql, got, err, want)
				}
			}
			// PostgreSQL's message for a setting it does not have.
			var pgErr *pgconn.PgError
			if _, err := conn.Exec(ctx, "SHOW no_such_setting"); !asPgError(err, &pgErr) ||
				pgErr.Message != `unrecognized configuration parameter "no_such_setting"` || pgErr.Hint != "" {
				t.Errorf("%s, %s: SHOW no_such_setting: %v", name, user, err)
			}
			// DuckDB's own setting, which SHOW does not read: a hint says
			// how to.
			if _, err := conn.Exec(ctx, "SHOW threads"); !asPgError(err, &pgErr) || pgErr.Code != "42704" ||
				!strings.Contains(pgErr.Hint, "current_setting('threads')") {
				t.Errorf("%s, %s: SHOW threads: %v", name, user, err)
			}
			// SHOW of a table still describes it, as in DuckDB.
			mustExec(t, conn, "SHOW information_schema.tables")
		}
		// A setting that exists is still only for admins, and the dialect
		// and the session's own settings are for anyone.
		if _, err := rita.Exec(ctx, "SET threads = 1"); pgCode(err) != "42501" {
			t.Errorf("%s: SET threads: %v", name, err)
		}
		if _, err := rita.Exec(ctx, "SET no_such_setting = 1"); err == nil || !strings.Contains(err.Error(), `unrecognized configuration parameter "no_such_setting"`) {
			t.Errorf("%s: SET of an unknown setting: %v", name, err)
		}
		for _, sql := range []string{"SET sql_dialect = 'snowflake'", "RESET sql_dialect", "SET search_path = 'main'", "RESET search_path",
			"SET TimeZone = 'UTC'", "SET application_name = 'x'"} {
			mustExec(t, rita, sql)
		}
		if _, err := rita.Exec(ctx, "CREATE TABLE no_such_schema.t (x INTEGER)"); err == nil {
			t.Errorf("%s: a reader created a table", name)
		}
		if _, err := ts.admin(t, mode).Exec(ctx, "CREATE TABLE no_such_schema.t (x INTEGER)"); pgCode(err) != "3F000" {
			t.Errorf("%s: a table in a schema that does not exist: %v", name, err)
		}
	}
}

// TestSessionUserColumns checks the names of the columns of current_user,
// session_user and current_role: PostgreSQL's, which clients read them
// by, and in the Snowflake dialect Snowflake's.
func TestSessionUserColumns(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	for name, mode := range modes {
		rita := connect(t, ts.url("rita", "rita-password"), mode)
		for sql, want := range map[string]string{
			"SELECT current_user":   "current_user",
			"SELECT session_user":   "session_user",
			"SELECT current_role":   "current_role",
			"SELECT current_role()": "current_role",
			"SELECT 1 AS n, current_user, session_user FROM range(1)":    "n,current_user,session_user",
			"SELECT * FROM (SELECT current_user) t":                      "current_user",
			"SELECT current_user AS who":                                 "who",
			"SELECT current_user FROM range(2) WHERE current_user <> ''": "current_user",
		} {
			if got := strings.Join(resultColumns(t, rita, sql), ","); got != want {
				t.Errorf("%s: %s: columns %q, want %q", name, sql, got, want)
			}
		}
		var user, session, role string
		if err := rita.QueryRow(ctx, "SELECT current_user, session_user, current_role").Scan(&user, &session, &role); err != nil ||
			user != "rita" || session != "rita" || role != "rita" {
			t.Errorf("%s: %q %q %q %v", name, user, session, role, err)
		}
		// Snowflake names CURRENT_USER() after its text, and an unquoted
		// name in upper case.
		mustExec(t, rita, "SET sql_dialect = 'snowflake'")
		for sql, want := range map[string]string{
			"SELECT CURRENT_USER(), CURRENT_ROLE()": "CURRENT_USER(),CURRENT_ROLE()",
			"SELECT CURRENT_USER() AS who":          "WHO",
			"SELECT CURRENT_USER":                   "CURRENT_USER",
		} {
			if got := strings.Join(resultColumns(t, rita, sql), ","); got != want {
				t.Errorf("%s: snowflake: %s: columns %q, want %q", name, sql, got, want)
			}
		}
		if err := rita.QueryRow(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil || user != "rita" {
			t.Errorf("%s: snowflake: CURRENT_USER(): %q %v", name, user, err)
		}
		mustExec(t, rita, "RESET sql_dialect")
	}
	// A table made from it has the column too.
	mustExec(t, admin, "CREATE TEMP TABLE who AS SELECT current_user")
	var user string
	if err := admin.QueryRow(ctx, `SELECT "current_user" FROM who`).Scan(&user); err != nil || user != "igloo" {
		t.Errorf("CREATE TABLE AS: %q %v", user, err)
	}
}
