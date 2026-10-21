package pgwire

import (
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestRequiredRole(t *testing.T) {
	cases := []struct {
		t     duckdb.StmtType
		query string
		want  auth.Role
	}{
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT 1", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM duckdb_secrets()", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, `SELECT * FROM "DuckDB_Secrets"()`, auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT which_secret('s3://b/x', 's3')", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT 'duckdb_secrets' AS note", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM set_iceberg_table_properties(t, {'history.expire.max-snapshot-age-ms': '1'})", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_SELECT, `SELECT * FROM "remove_iceberg_table_properties"(t, ['x'])`, auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM set_iceberg_schema_properties('s', {'a': 'b'})", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM iceberg_table_properties(t)", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM iceberg_to_ducklake('lake', 'dl')", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM dbgen(sf = 1, catalog = 'lake')", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM enable_logging()", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT message FROM duckdb_logs", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM tpch_queries()", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM query_table('lake.public.t')", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, `SELECT * FROM "Query_Table"(['t'])`, auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT cache_httpfs_clear_cache()", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM cache_httpfs_cache_status_query()", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, `SELECT "CACHE_HTTPFS_get_profile"()`, auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT current_setting('cache_httpfs_type')", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "SET cache_httpfs_profile_type = 'temp'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET SESSION cache_httpfs_type = 'noop'", auth.RoleAdmin},
		{statementTypeMergeInto, "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_TRANSACTION, "BEGIN", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN SELECT 1", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN ANALYZE SELECT 1", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN /*x*/ ANALYZE INSERT INTO t VALUES (3)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN (ANALYSE) DELETE FROM t", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN ANALYZE SET memory_limit = '1GB'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_PRAGMA, "PRAGMA table_info('t')", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_PRAGMA, "PRAGMA tpch(1)", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_PRAGMA, "PRAGMA threads=4", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_PRAGMA, "PRAGMA enable_profiling", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_PRAGMA, "PRAGMA /*table_info*/ enable_profiling", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_CALL, "CALL pragma_table_info('t')", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_CALL, "CALL dbgen(sf = 1)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_CALL, "CALL /*pragma_x*/ dbgen(sf = 1)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET search_path = 'x'", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "SET SESSION TimeZone = 'UTC'", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "RESET schema", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "USE lake.sales", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "/* c */ use\nsales", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_SET, "SET GLOBAL search_path = 'x'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET LOCAL search_path = 'x'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET /*x*/ search_path = 'x'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET threads = 4", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_SET, "SET allowed_directories = ['/']", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_VARIABLE_SET, "SET VARIABLE x = 42", auth.RoleReader},
		{duckdb.STATEMENT_TYPE_INSERT, "INSERT INTO t VALUES (1)", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_COPY, "COPY t FROM 's3://landing/x.csv'", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE TABLE t (i INT)", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_CREATE, `CREATE TABLE "secret" (i INT)`, auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE TABLE secret (i INT)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE OR REPLACE PERSISTENT SECRET s (TYPE s3)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE /*x*/ SECRET s3x (TYPE s3)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_CREATE, "CREATE OR REPLACE -- c\n SECRET igloo_storage (TYPE s3)", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_DROP, "DROP SECRET s", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_DROP, "DROP /*x*/ SECRET igloo_storage", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_DROP, "DROP TABLE t", auth.RoleWriter},
		{duckdb.STATEMENT_TYPE_ATTACH, "ATTACH 'x.db'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_LOAD, "LOAD spatial", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_EXTENSION, "INSTALL spatial", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_EXPORT, "EXPORT DATABASE 'x'", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_PREPARE, "PREPARE p AS DELETE FROM t", auth.RoleAdmin},
		{duckdb.STATEMENT_TYPE_INVALID, "WHATEVER", auth.RoleAdmin},
	}
	for _, c := range cases {
		if got := requiredRole(c.t, c.query); got != c.want {
			t.Errorf("%q: got %s, want %s", c.query, got, c.want)
		}
	}
}

// TestForbidden covers statements no one may run: SQL run from strings,
// and moving the read cache or changing what it caches.
func TestForbidden(t *testing.T) {
	refused := []string{
		"SELECT * FROM query('SELECT * FROM duckdb_secrets()')",
		"FROM query('SELECT 1')",
		"SELECT * FROM t, query ( 'SELECT 1' )",
		"SELECT * FROM QUERY/* c */('SELECT 1')",
		`SELECT * FROM "query"('SELECT 1')`,
		`SELECT * FROM "Query"('SELECT 1')`,
		"SELECT * FROM system.main.query('SELECT 1')",
		`SELECT * FROM "system"."main"."query"('SELECT 1')`,
		"SELECT * FROM t JOIN query('SELECT 1') q ON true",
		"SELECT * FROM (SELECT 1) WHERE EXISTS (SELECT * FROM query('SELECT 1'))",
		"MERGE INTO t USING query('SELECT 1') s ON t.a = s.a WHEN MATCHED THEN DELETE",
		"CREATE VIEW v AS SELECT * FROM query('SELECT 1')",
		"WITH a AS (SELECT 1), query (x) AS (SELECT 2) SELECT * FROM query",
		"query('SELECT 1')",
		"SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))",
		"PRAGMA json_execute_serialized_sql('{}')",
		`SELECT * FROM "JSON_EXECUTE_SERIALIZED_SQL"('{}')`,
		"SET cache_httpfs_cache_directory = '/tmp/elsewhere'",
		"SET GLOBAL cache_httpfs_cache_directory = '/tmp/elsewhere'",
		"RESET cache_httpfs_cache_directory",
		"SET cache_httpfs_cache_directories_config = '[]'",
		`SET "cache_httpfs_cache_directory" = '/tmp/x'`,
		"PRAGMA cache_httpfs_cache_directory = '/tmp/x'",
		"SELECT cache_httpfs_wrap_cache_filesystem('LocalFileSystem')",
		"SELECT cache_httpfs_add_exclusion_regex('.*')",
		"SELECT cache_httpfs_reset_exclusion_regex()",
		// query_table with a name built at run time, which could resolve to
		// a GDAL dataset without a check of the text seeing it.
		"SELECT * FROM query_table('lake.public.' || 't')",
		"SELECT * FROM query_table(getvariable('f'))",
		"SELECT * FROM query_table(name) ",
		"SELECT * FROM main.query_table('a.' || 'gpkg')",
		`SELECT * FROM "query_table"('a' || 'b')`,
		"SELECT * FROM query_table((SELECT 'lake.public.t'))",
		"SELECT * FROM query_table(['a.' || 'fgb'])",
		"SELECT * FROM t, query_table(format('{}', x))",
	}
	for _, q := range refused {
		for _, role := range []auth.Role{auth.RoleAdmin, auth.RoleReader} {
			if err := authorize(role, duckdb.STATEMENT_TYPE_SELECT, q); err == nil {
				t.Errorf("%s may run %q", role, q)
			}
		}
		if forbidden(q) == nil {
			t.Errorf("not forbidden: %q", q)
		}
	}
	allowed := []string{
		"SELECT query FROM log",
		"SELECT query, query_text FROM igloo.query_history",
		"SELECT current_query()",
		"INSERT INTO query (a, b) VALUES (1, 2)",
		"INSERT INTO main.query (a) VALUES (1)",
		"CREATE TABLE query (a INT)",
		"CREATE TABLE IF NOT EXISTS query (a INT)",
		"CREATE TABLE t (a INT REFERENCES query (a))",
		"CREATE VIEW query (a) AS SELECT 1",
		"WITH query (a) AS (SELECT 1) SELECT * FROM query",
		"WITH RECURSIVE query (a) AS (SELECT 1) SELECT * FROM query",
		"SELECT * FROM t AS query (a, b)",
		"COPY query (a) FROM 'x.csv'",
		"CREATE INDEX i ON query (a)",
		"CREATE MACRO query(x) AS x + 1",
		"SELECT 'query(1)' AS s",
		`SELECT "query" FROM t`,
		"SELECT json_serialize_sql('SELECT 1')",
		"SELECT current_setting('cache_httpfs_cache_directory')",
		"SET cache_httpfs_profile_type = 'temp'",
		"SELECT cache_httpfs_clear_cache()",
		// query_table with a name written out as literals is checked by the
		// text (gdalUse) and admin-gated; it is not forbidden outright.
		"SELECT * FROM query_table('lake.public.t')",
		"SELECT * FROM query_table(['lake.a', 'lake.b'])",
		`SELECT * FROM "query_table"('lake.public.t')`,
		"SELECT query_table FROM t",
		"WITH query_table AS (SELECT 1) SELECT * FROM query_table",
	}
	for _, q := range allowed {
		if err := forbidden(q); err != nil {
			t.Errorf("%q: %v", q, err)
		}
	}
}

func TestStartsQuery(t *testing.T) {
	for q, want := range map[string]bool{
		"SELECT * FROM (PIVOT t ON c USING sum(n))":        true,
		"  /* c */ -- d\n pivot t ON c":                    true,
		"FROM (PIVOT t ON c)":                              true,
		"(PIVOT t ON c) UNION ALL (PIVOT t ON c)":          true,
		"UNPIVOT (PIVOT t ON c) ON COLUMNS(*) INTO NAME k": true,
		"PIVOT_WIDER t ON c":                               true,
		"WITH p AS (PIVOT t ON c) SELECT * FROM p":         false,
		"WITH p AS (PIVOT t ON c) INSERT INTO x FROM p":    false,
		"CREATE TABLE x AS PIVOT t ON c":                   false,
		"INSERT INTO x PIVOT t ON c":                       false,
		"EXPLAIN PIVOT t ON c":                             false,
		"DESCRIBE PIVOT t ON c":                            false,
		`"select" FROM t`:                                  false,
		"":                                                 false,
	} {
		if got := startsQuery(q); got != want {
			t.Errorf("startsQuery(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestSessionScoped(t *testing.T) {
	cases := map[string]string{
		"SET default_order = 'desc'":    "SET SESSION default_order = 'desc'",
		"  -- c\nset TimeZone TO 'UTC'": "set SESSION TimeZone TO 'UTC'",
		"RESET calendar":                "RESET SESSION calendar",
		"SET SESSION search_path = 'x'": "SET SESSION search_path = 'x'",
		"SET VARIABLE x = 1":            "SET VARIABLE x = 1",
		"SET threads = 2":               "SET threads = 2",
		"SELECT 1":                      "SELECT 1",
	}
	for in, want := range cases {
		if got := sessionScoped(in); got != want {
			t.Errorf("sessionScoped(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestAuthorizeRanksRoles(t *testing.T) {
	insert := duckdb.STATEMENT_TYPE_INSERT
	if authorize(auth.RoleReader, insert, "INSERT INTO t VALUES (1)") == nil {
		t.Error("reader may not insert")
	}
	for _, r := range []auth.Role{auth.RoleWriter, auth.RoleAdmin} {
		if err := authorize(r, insert, "INSERT INTO t VALUES (1)"); err != nil {
			t.Errorf("%s may insert: %v", r, err)
		}
	}
	// The none role, like an unknown one, allows nothing by itself.
	for _, r := range []auth.Role{auth.RoleNone, "superuser"} {
		if authorize(r, duckdb.STATEMENT_TYPE_SELECT, "SELECT 1") == nil {
			t.Errorf("%s may select without grants", r)
		}
	}
}

func TestParseUserCommand(t *testing.T) {
	ok := []struct {
		query string
		check func(userCommand) bool
	}{
		{"SHOW USERS", func(c userCommand) bool { return c.verb == "SHOW" }},
		{"create user Ana with password 'it''s long' role WRITER;", func(c userCommand) bool {
			return c.verb == "CREATE" && c.name == "ana" && *c.password == "it's long" && *c.role == auth.RoleWriter
		}},
		{`ALTER USER "ana" PASSWORD 'SCRAM-SHA-256$x'`, func(c userCommand) bool {
			return c.verb == "ALTER" && c.name == "ana" && *c.password == "SCRAM-SHA-256$x" && c.role == nil
		}},
		{"ALTER USER current_user WITH PASSWORD 'p'", func(c userCommand) bool {
			return c.currentUser && c.name == ""
		}},
		{"/* drop */ DROP USER IF EXISTS bob", func(c userCommand) bool {
			return c.verb == "DROP" && c.ifExists && c.name == "bob"
		}},
	}
	for _, c := range ok {
		cmds, matched, err := parseUserCommands(c.query)
		if !matched || err != nil || len(cmds) != 1 || !c.check(cmds[0]) {
			var cmd userCommand
			if len(cmds) == 1 {
				cmd = cmds[0]
			}
			t.Errorf("%q: matched=%v err=%v cmd=%+v", c.query, matched, err, cmd)
		}
	}

	cmds, matched, err := parseUserCommands("CREATE USER a PASSWORD 'a-password'; CREATE USER b PASSWORD 'b-password';")
	if !matched || err != nil || len(cmds) != 2 || cmds[1].name != "b" {
		t.Errorf("two user commands: %v %v %+v", matched, err, cmds)
	}

	malformed := []string{
		"CREATE USER ana",
		"CREATE USER ana PASSWORD unquoted",
		"CREATE USER ana PASSWORD 'p' ROLE superuser",
		"ALTER USER ana",
		"DROP USER ana CASCADE",
		`CREATE USER "Ana" PASSWORD 'p'`,
		"CREATE USER ana PASSWORD 'ana-password'; SELECT 1",
	}
	for _, q := range malformed {
		if _, matched, err := parseUserCommands(q); matched && err == nil {
			t.Errorf("%q should be rejected", q)
		}
	}

	notUserCommands := []string{"SELECT 1", "CREATE TABLE users (i INT)", "SHOW TABLES", "DROP TABLE user"}
	for _, q := range notUserCommands {
		if _, matched, _ := parseUserCommands(q); matched {
			t.Errorf("%q is not a user command", q)
		}
	}
}
