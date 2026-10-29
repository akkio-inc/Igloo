package pgwire

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// sessionChanges change every setting a session may change for itself,
// the dialect last: what follows it is Snowflake's. An admin's SET of
// default_order with no scope is DuckDB's global one, for every session,
// which no session's reset undoes; non-admins' are made SESSION.
var sessionChanges = []string{
	"USE lake.sales",
	"SET TimeZone = 'Asia/Tokyo'",
	"SET SESSION default_order = 'DESC'",
	"SET integer_division = true",
	"SET explain_output = 'all'",
	"SET statement_timeout = '5min'",
	"SET statement_queued_timeout = '1min'",
	"SET query_tag = 'client:synthetic'",
	"SET distributed_execution = off",
	"SET igloo_prefetch = off",
	"SET igloo_report_rewrite = off",
	"SET igloo_bitmap_indexes = off",
	"SET igloo_id_counts = off",
	"SET igloo_result_cache = off",
	"SET ai_integration = 'synthetic_ai'",
	"SET ai_max_calls = 5",
	"SET ai_decision_threshold = 0.7",
	"SET application_name = 'synthetic-app'",
	"SET VARIABLE synthetic_limit = 42",
	"SET sql_dialect = 'snowflake'",
}

// sessionTemps make the temporary objects, cursor and prepared statement
// that DISCARD ALL drops, and replace one of the objects every session
// starts with.
var sessionTemps = []string{
	"CREATE TEMP TABLE scratch_rows AS SELECT 1 AS n",
	"CREATE TEMP VIEW scratch_view AS SELECT 2 AS n",
	"CREATE TEMP MACRO scratch_add(x) AS x + 1",
	"CREATE TEMP MACRO scratch_rows_of(n) AS TABLE SELECT * FROM range(n)",
	"CREATE TEMP SEQUENCE scratch_seq",
	"CREATE OR REPLACE TEMP MACRO version() AS 'replaced'",
	"DECLARE scratch_cursor CURSOR WITH HOLD FOR SELECT 1",
}

// sessionState describes what a session's statements run under, in
// DuckDB's dialect.
func sessionState(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	ctx := context.Background()
	var b strings.Builder
	value := func(sql string) {
		var v string
		if err := conn.QueryRow(ctx, sql).Scan(&v); err != nil {
			v = "error: " + err.Error()
		}
		fmt.Fprintf(&b, "%s: %s\n", sql, v)
	}
	value("SELECT current_database() || '.' || current_schema() || ', search_path ' || current_setting('search_path')")
	for _, name := range slices.Sorted(maps.Keys(sessionSettings)) {
		value("SELECT current_setting('" + name + "')::VARCHAR")
	}
	for _, name := range []string{"statement_timeout", "statement_queued_timeout", "query_tag", "sql_dialect",
		"distributed_execution", "igloo_prefetch", "igloo_report_rewrite", "igloo_bitmap_indexes", "igloo_id_counts",
		"igloo_result_cache", "ai_integration", "ai_max_calls", "ai_max_cost_usd", "ai_decision_threshold", "application_name"} {
		value("SHOW " + name)
	}
	value("SELECT coalesce(string_agg(name, ',' ORDER BY name), '') FROM duckdb_variables()")
	value(`SELECT count(*)::VARCHAR FROM (
		SELECT table_name FROM duckdb_tables() WHERE database_name = 'temp'
		UNION ALL SELECT view_name FROM duckdb_views() WHERE database_name = 'temp'
		UNION ALL SELECT DISTINCT function_name FROM duckdb_functions() WHERE database_name = 'temp'
		UNION ALL SELECT sequence_name FROM duckdb_sequences() WHERE database_name = 'temp')`)
	value("SELECT version()")
	return b.String()
}

// TestSessionResetMatchesNewSession checks that RESET ALL and DISCARD ALL
// leave a session as it started, an admin's and a non-admin's, in either
// protocol and from Snowflake's dialect: on the catalog's default schema,
// with every setting as it was, and after DISCARD ALL with only the
// temporary objects every session starts with, and no cursors or prepared
// statements. DuckDB refuses RESET ALL and does not parse DISCARD.
func TestSessionResetMatchesNewSession(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	admin := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA lake.sales")
	mustExec(t, admin, "CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer")
	writer := connect(t, env.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)

	if a, w := sessionState(t, admin), sessionState(t, writer); a != w {
		t.Fatalf("an admin's session starts as\n%s\na writer's as\n%s", a, w)
	}
	for name, conn := range map[string]*pgx.Conn{"admin": admin, "writer": writer} {
		start := sessionState(t, conn)
		for _, reset := range []string{"RESET ALL", "DISCARD ALL"} {
			for _, extended := range []bool{false, true} {
				what := fmt.Sprintf("%s: %s", name, reset)
				if extended {
					what += " (extended protocol)"
				}
				if reset == "DISCARD ALL" {
					for _, sql := range sessionTemps {
						mustExec(t, conn, sql)
					}
					if _, err := conn.PgConn().Prepare(ctx, "scratch_stmt", "SELECT 1", nil); err != nil {
						t.Fatal(err)
					}
				}
				for _, sql := range sessionChanges {
					mustExec(t, conn, sql)
				}
				var err error
				if extended {
					err = conn.PgConn().ExecParams(ctx, reset, nil, nil, nil, nil).Read().Err
				} else {
					_, err = conn.Exec(ctx, reset)
				}
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				if got := sessionState(t, conn); got != start {
					t.Errorf("%s: the session is at\n%s\nwant as it started\n%s", what, got, start)
				}
				if reset != "DISCARD ALL" {
					continue
				}
				if _, err := conn.Exec(ctx, "FETCH scratch_cursor"); err == nil {
					t.Errorf("%s: the cursor is still open", what)
				}
				if err := conn.PgConn().ExecPrepared(ctx, "scratch_stmt", nil, nil, nil).Read().Err; err == nil {
					t.Errorf("%s: the prepared statement is still there", what)
				}
			}
		}
	}

	// An unqualified name after RESET ALL means the catalog's default
	// schema again.
	mustExec(t, admin, "USE lake.sales")
	mustExec(t, admin, "RESET ALL")
	mustExec(t, admin, "CREATE SCHEMA after_reset")
	if n := count(t, admin, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'lake' AND schema_name = 'after_reset'"); n != 1 {
		t.Errorf("CREATE SCHEMA after RESET ALL made %d schemas in lake, want 1", n)
	}
}

// TestDiscardTemp checks DISCARD TEMP, which keeps the session's settings,
// DISCARD PLANS and DISCARD SEQUENCES, which change nothing, and that
// DISCARD ALL runs only outside a transaction block, as in PostgreSQL.
func TestDiscardTemp(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	start := sessionState(t, conn)

	mustExec(t, conn, "CREATE TEMP TABLE kept AS SELECT 7 AS n")
	mustExec(t, conn, "SET query_tag = 'client:synthetic'")
	for _, sql := range []string{"DISCARD PLANS", "DISCARD SEQUENCES"} {
		mustExec(t, conn, sql)
		if n := count(t, conn, "SELECT n FROM kept"); n != 7 {
			t.Errorf("after %s the temporary table holds %d, want 7", sql, n)
		}
	}

	// Rolled back, it drops nothing.
	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "DISCARD TEMP")
	mustExec(t, conn, "ROLLBACK")
	if n := count(t, conn, "SELECT n FROM kept"); n != 7 {
		t.Errorf("after a rolled-back DISCARD TEMP the temporary table holds %d, want 7", n)
	}

	mustExec(t, conn, "CREATE OR REPLACE TEMP MACRO version() AS 'replaced'")
	mustExec(t, conn, "DISCARD TEMPORARY")
	if _, err := conn.Exec(ctx, "SELECT n FROM kept"); err == nil {
		t.Error("DISCARD TEMP left the temporary table")
	}
	var tag string
	if err := conn.QueryRow(ctx, "SHOW query_tag").Scan(&tag); err != nil || tag != "client:synthetic" {
		t.Errorf("DISCARD TEMP changed the query tag: %q %v", tag, err)
	}
	mustExec(t, conn, "RESET query_tag")
	if got := sessionState(t, conn); got != start {
		t.Errorf("after DISCARD TEMP the session is at\n%s\nwant as it started\n%s", got, start)
	}

	// A query of several statements is a transaction block too.
	mustExec(t, conn, "BEGIN")
	if _, err := conn.Exec(ctx, "DISCARD ALL"); pgCode(err) != "25001" {
		t.Errorf("DISCARD ALL after BEGIN: want 25001, got %v", err)
	}
	mustExec(t, conn, "ROLLBACK")
	if _, err := conn.Exec(ctx, "SELECT 1; DISCARD ALL"); pgCode(err) != "25001" {
		t.Errorf("DISCARD ALL after another statement in one query: want 25001, got %v", err)
	}
}

// TestResetAllRestoresLoginSettings checks that RESET ALL takes a session
// back to the settings it had at login: its user's and roles' settings
// (ALTER ROLE ... SET), and the dialect and application_name its
// connection gave.
func TestResetAllRestoresLoginSettings(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, "ALTER ROLE reader SET query_tag = 'client:acme'")
	mustExec(t, cs.admin, "ALTER ROLE reader SET statement_timeout = '1h'")
	cs.user(t, "rhea", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	conn := connect(t, cs.url("rhea", "rhea-password")+"&application_name=synthetic-client&sql_dialect=snowflake",
		pgx.QueryExecModeSimpleProtocol)

	show := func(name string) string {
		t.Helper()
		var v string
		if err := conn.QueryRow(ctx, "SHOW "+name).Scan(&v); err != nil {
			t.Fatalf("SHOW %s: %v", name, err)
		}
		return v
	}
	want := map[string]string{
		"query_tag": "client:acme", "statement_timeout": "1h", "sql_dialect": "snowflake", "application_name": "synthetic-client",
	}
	names := slices.Sorted(maps.Keys(want))
	for _, name := range names {
		if got := show(name); got != want[name] {
			t.Fatalf("%s at login: %q, want %q", name, got, want[name])
		}
	}
	for _, sql := range []string{"SET query_tag = 'client:other'", "SET statement_timeout = '5min'",
		"SET application_name = 'other-client'", "SET sql_dialect = 'duckdb'"} {
		mustExec(t, conn, sql)
	}
	mustExec(t, conn, "RESET ALL")
	for _, name := range names {
		if got := show(name); got != want[name] {
			t.Errorf("%s after RESET ALL: %q, want %q as at login", name, got, want[name])
		}
	}
}

// asyncpgPoolReset is the query asyncpg's connection pool sends, as one
// simple query, to clean a connection it takes back.
const asyncpgPoolReset = "SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL;"

// TestAsyncpgPoolReset checks that asyncpg's pool can clean a session, an
// admin's and a non-admin's, from either dialect: its reset query succeeds
// and leaves the session's settings as they started and no cursor open.
// Igloo has no advisory locks and no LISTEN, so pg_advisory_unlock_all()
// and UNLISTEN have nothing to undo, and LISTEN is refused as not
// supported. DuckDB has no pg_advisory_unlock_all() and parses neither
// LISTEN nor UNLISTEN.
func TestAsyncpgPoolReset(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	admin := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA lake.sales")
	mustExec(t, admin, "CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer")
	writer := connect(t, env.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)

	// sessionChanges end in Snowflake's dialect; without the last, the
	// reset is sent in DuckDB's.
	dialects := map[string][]string{"Snowflake": sessionChanges, "DuckDB": sessionChanges[:len(sessionChanges)-1]}
	for name, conn := range map[string]*pgx.Conn{"admin": admin, "writer": writer} {
		start := sessionState(t, conn)
		for dialect, changes := range dialects {
			what := fmt.Sprintf("%s, in %s's dialect", name, dialect)
			mustExec(t, conn, "DECLARE scratch_cursor CURSOR WITH HOLD FOR SELECT 1")
			for _, sql := range changes {
				mustExec(t, conn, sql)
			}
			results, err := conn.PgConn().Exec(ctx, asyncpgPoolReset).ReadAll()
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			var tags []string
			for _, r := range results {
				tags = append(tags, r.CommandTag.String())
			}
			if got, want := strings.Join(tags, ", "), "SELECT 1, CLOSE CURSOR ALL, UNLISTEN, RESET"; got != want {
				t.Errorf("%s: the statements completed as %s, want %s", what, got, want)
			}
			if got := sessionState(t, conn); got != start {
				t.Errorf("%s: the session is at\n%s\nwant as it started\n%s", what, got, start)
			}
			if _, err := conn.Exec(ctx, "FETCH scratch_cursor"); err == nil {
				t.Errorf("%s: the cursor is still open", what)
			}
		}
	}

	for _, dialect := range []string{"duckdb", "snowflake"} {
		mustExec(t, writer, "SET sql_dialect = '"+dialect+"'")
		for _, sql := range []string{"UNLISTEN synthetic_channel", `UNLISTEN "Synthetic Channel"`} {
			mustExec(t, writer, sql)
		}
		if _, err := writer.Exec(ctx, "LISTEN synthetic_channel"); pgCode(err) != "0A000" {
			t.Errorf("LISTEN in the %s dialect: want 0A000, got %v", dialect, err)
		}
	}
}
