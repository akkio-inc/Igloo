package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestResetSearchPathRestoresDefault checks that the SET and RESET that put
// the search path back to its default leave a session as it started, on
// the catalog's default schema with its views' schema next to it. DuckDB's
// own default is its own database with an empty path: an admin's
// unqualified CREATE SCHEMA landed there, and a non-admin's RESET was
// refused for making it the current database.
func TestResetSearchPathRestoresDefault(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	admin := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA lake.sales")
	mustExec(t, admin, "CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer")
	writer := connect(t, env.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)

	state := func(conn *pgx.Conn) string {
		t.Helper()
		var path, db, schema string
		err := conn.QueryRow(ctx, "SELECT current_setting('search_path')::VARCHAR, current_database(), current_schema()").
			Scan(&path, &db, &schema)
		if err != nil {
			t.Fatal(err)
		}
		return db + "." + schema + ", search_path " + path
	}
	start := state(writer)
	if got := state(admin); got != start {
		t.Fatalf("an admin's session starts at %s, a writer's at %s", got, start)
	}
	for name, conn := range map[string]*pgx.Conn{"admin": admin, "writer": writer} {
		for _, sql := range []string{
			"RESET search_path",
			"RESET SESSION search_path",
			"RESET schema",
			"SET search_path TO DEFAULT",
			"SET search_path = DEFAULT",
			"SET SESSION search_path TO DEFAULT",
			"SET schema TO DEFAULT",
		} {
			mustExec(t, conn, "SET search_path = 'lake.sales'")
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Errorf("%s: %s: %v", name, sql, err)
				continue
			}
			if got := state(conn); got != start {
				t.Errorf("%s: after %s the session is at %s, want %s as it started", name, sql, got, start)
			}
		}
	}

	// An unqualified name after the RESET means the catalog's default
	// schema, as it did when the session started.
	mustExec(t, admin, "SET search_path = 'lake.sales'")
	mustExec(t, admin, "RESET search_path")
	mustExec(t, admin, "CREATE SCHEMA after_reset")
	if n := count(t, admin, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'lake' AND schema_name = 'after_reset'"); n != 1 {
		t.Errorf("CREATE SCHEMA after RESET search_path made %d schemas in lake, want 1", n)
	}
}
