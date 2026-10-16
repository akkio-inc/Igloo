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
