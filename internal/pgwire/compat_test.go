package pgwire

import (
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

func TestIgnoredCommand(t *testing.T) {
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
	cases := map[string]string{
		// Queries without pg_catalog are never touched, nor are literals,
		// quoted identifiers and comments in queries that have it.
		"SELECT x::regclass FROM t": "SELECT x::regclass FROM t",
		"SELECT 'OPERATOR(pg_catalog.~)', E'it\\'s pg_catalog.f(', $$pg_catalog.g($$ -- pg_catalog.h(\nFROM pg_catalog.pg_class": "SELECT 'OPERATOR(pg_catalog.~)', E'it\\'s pg_catalog.f(', $$pg_catalog.g($$ -- pg_catalog.h(\nFROM pg_class",
		`SELECT "pg_catalog.x(" FROM pg_catalog.pg_class /* pg_catalog.y( */`:                                                    `SELECT "pg_catalog.x(" FROM pg_class /* pg_catalog.y( */`,

		"WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default":                   "WHERE c.relname ~ '^(t)$'",
		"SELECT pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c":                    "SELECT pg_get_userbyid(c.relowner) FROM pg_class c",
		"SELECT c.reloftype::pg_catalog.regtype::pg_catalog.text FROM pg_catalog.pg_namespace":        "SELECT c.reloftype::VARCHAR::text FROM pg_namespace",
		"SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d":        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_catalog.pg_attrdef d",
		"FROM pg_catalog.generate_series(0, pg_catalog.array_upper(a, 1)) s, pg_catalog.pg_attribute": "FROM generate_series(0, array_upper(a, 1)) AS s(s), pg_attribute",
		// pgjdbc's getTables join on comments.
		"LEFT JOIN pg_catalog.pg_description d ON (d.classoid = 'pg_class'::regclass)": "LEFT JOIN pg_description d ON (d.classoid = 1259)",
	}
	for in, want := range cases {
		if got := rewriteCatalogQuery(in); got != want {
			t.Errorf("rewriteCatalogQuery(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestParseStatementTimeout(t *testing.T) {
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
	cases := map[string]string{
		"SELECT current_user":                     "SELECT 'ana'",
		"select CURRENT_USER, session_user()":     "select 'ana', 'ana'",
		"SELECT 'current_user', \"current_user\"": "SELECT 'current_user', \"current_user\"",
		"SELECT current_users FROM t":             "SELECT current_users FROM t",
	}
	for in, want := range cases {
		if got := rewriteSessionUser(in, "ana"); got != want {
			t.Errorf("rewriteSessionUser(%q) = %q, want %q", in, got, want)
		}
	}
}
