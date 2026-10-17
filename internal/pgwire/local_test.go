package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// TestLocalDatabaseIsAdminsOnly checks that non-admins cannot change DuckDB's
// own database, where each replica keeps its copies of the catalog's views,
// by naming it or through the search path.
func TestLocalDatabaseIsAdminsOnly(t *testing.T) {
	ctx := context.Background()
	cs := startCatalogServer(t)
	// Stand-ins for a view's local copy and an admin's own table.
	mustExec(t, cs.admin, "CREATE VIEW memory.main.revenue AS SELECT 1 AS dept, 10 AS amount")
	mustExec(t, cs.admin, "CREATE TABLE memory.main.adm (x INTEGER)")
	// cs.user puts lake.public first on the path, then memory.main.
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)

	for name, conn := range map[string]*pgx.Conn{"writer": walt, "reader": rita} {
		for _, sql := range []string{
			// Making the local database the current one.
			"SET search_path = 'memory.main,lake.public'",
			"SET schema = 'memory.main'",
			// Through the search path, from the catalog's current database.
			"CREATE OR REPLACE VIEW main.revenue AS SELECT 1 AS dept, 999999 AS amount",
			"CREATE TABLE main.t AS SELECT 1 AS x",
			"DROP VIEW revenue",
			"DROP VIEW IF EXISTS main.revenue",
			"INSERT INTO adm VALUES (1)",
			"DELETE FROM adm",
			"DROP TABLE adm",
			"ALTER TABLE adm ADD COLUMN y INTEGER",
			// ALTER's modifiers do not hide the target.
			"ALTER TABLE ONLY adm ADD COLUMN y INTEGER",
			"ALTER TABLE IF EXISTS ONLY memory.main.adm ADD COLUMN y INTEGER",
			"ALTER TABLE /* x */ ONLY (memory.main.adm) ADD COLUMN y INTEGER",
			"ALTER TABLE memory.main.adm * ADD COLUMN y INTEGER",
			// By name.
			"CREATE OR REPLACE VIEW memory.main.revenue AS SELECT 1 AS dept, 999999 AS amount",
			"CREATE TABLE memory.t AS SELECT 1 AS x",
			"INSERT INTO memory.main.adm VALUES (1)",
		} {
			if _, err := conn.Exec(ctx, sql); pgCode(err) != "42501" {
				t.Errorf("%s: %s: %v, want 42501", name, sql, err)
			}
		}
		var db string
		if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil || db != "lake" {
			t.Errorf("%s: a refused SET was not undone: current database %q, %v", name, db, err)
		}
	}
	// A session whose current database is the local one (here, as the test
	// server starts sessions) still cannot change it unqualified.
	raw := connect(t, cs.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"CREATE OR REPLACE VIEW revenue AS SELECT 1 AS dept, 999999 AS amount",
		"DROP VIEW revenue",
		"CREATE TABLE t AS SELECT 1 AS x",
		"CREATE SCHEMA s",
	} {
		if _, err := raw.Exec(ctx, sql); pgCode(err) != "42501" {
			t.Errorf("from the local database: %s: %v, want 42501", sql, err)
		}
	}
	var amount, rows int
	if err := cs.admin.QueryRow(ctx, "SELECT amount, (SELECT count(*) FROM memory.main.adm) FROM memory.main.revenue").Scan(&amount, &rows); err != nil ||
		amount != 10 || rows != 0 {
		t.Errorf("the local database changed: amount %d, %d rows, %v", amount, rows, err)
	}

	// What stays allowed: the catalog's tables, temporary objects, and the
	// local database on the path behind the catalog.
	for _, sql := range []string{
		"CREATE TABLE t AS SELECT 1 AS x",
		"CREATE TABLE public.u AS SELECT * FROM t",
		"INSERT INTO t VALUES (2)",
		"CREATE TEMP TABLE scratch AS SELECT 1 AS x",
		"DROP TABLE scratch",
		"SET search_path = 'lake.public,memory.main'",
		"DROP TABLE u",
	} {
		mustExec(t, walt, sql)
	}
	// Admins may do as they please.
	mustExec(t, cs.admin, "SET search_path = 'memory.main'")
	mustExec(t, cs.admin, "DROP TABLE adm")
}

// TestNulRefused checks that SQL with a NUL in it is refused: DuckDB would
// read only up to the NUL. (The wire protocol ends a query at a NUL; the
// web console and Flight SQL take SQL as it comes.)
func TestNulRefused(t *testing.T) {
	srv, store := newEmbeddedServer(t, nil)
	es := openAs(t, srv, store, "igloo")
	if err := es.Run(context.Background(), "SELECT 1\x00; DROP TABLE x", &collected{}); codeOf(err) != "22021" {
		t.Errorf("console: %v, want 22021", err)
	}
}
