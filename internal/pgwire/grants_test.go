package pgwire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// catalogServer is a server whose catalog, the one grants apply to, is an
// in-memory DuckDB database named lake standing in for Iceberg. Its scans
// name their tables, unlike Iceberg's, so the e2e test covers Iceberg; the
// analysis is otherwise the same.
type catalogServer struct {
	*testServer
	grants  *auth.MemoryGrants
	history *audit.MemoryStore
	admin   *pgx.Conn
}

func startCatalogServer(t *testing.T, configure ...func(*Options)) *catalogServer {
	t.Helper()
	return startCatalogServerWithConfig(t, config.Config{}, configure...)
}

func startCatalogServerWithConfig(t *testing.T, cfg config.Config, configure ...func(*Options)) *catalogServer {
	t.Helper()
	cs := &catalogServer{history: audit.NewMemoryStore(1000, 1000)}
	setup := func(o *Options) {
		cs.grants = auth.NewMemoryGrants(o.Users)
		o.Grants = cs.grants
		o.Catalog = "lake"
		o.Audit = audit.New(cs.history, audit.Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		// Production sets these from the same config the engine's sandbox
		// uses; mirror that so the file checks and the sandbox agree.
		o.AllowedLocations = cfg.Storage.AllowedLocations
	}
	cs.testServer = startServerWithConfig(t, cfg, append([]func(*Options){setup}, configure...)...)
	cs.admin = cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"ATTACH ':memory:' AS lake",
		"CREATE SCHEMA lake.public",
		"CREATE SCHEMA lake.sales",
		"CREATE TABLE lake.public.orders (id INTEGER, amount INTEGER)",
		"INSERT INTO lake.public.orders VALUES (1, 10), (2, 20)",
		"CREATE TABLE lake.public.secrets (id INTEGER, code VARCHAR)",
		"INSERT INTO lake.public.secrets VALUES (1, 'x')",
		"CREATE TABLE lake.sales.leads (id INTEGER)",
		"INSERT INTO lake.sales.leads VALUES (1), (2), (3)",
	} {
		mustExec(t, cs.admin, sql)
	}
	return cs
}

// user creates a user with role and connects as them, with search_path set
// to the stand-in catalog as Igloo sets it for Iceberg.
func (cs *catalogServer) user(t *testing.T, name string, role auth.Role, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	mustExec(t, cs.admin, "CREATE USER "+name+" WITH PASSWORD '"+name+"-password' ROLE "+string(role))
	conn := connect(t, cs.url(name, name+"-password"), mode)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	return conn
}

func expectDenied(t *testing.T, conn *pgx.Conn, sql string, mentions ...string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	if pgCode(err) != "42501" {
		t.Errorf("%s: want 42501, got %v", sql, err)
		return
	}
	for _, m := range mentions {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("%s: error %q does not mention %q", sql, err, m)
		}
	}
}

func expectAllowed(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql); err != nil {
		t.Errorf("%s: %v", sql, err)
	}
}

func count(t *testing.T, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Errorf("%s: %v", sql, err)
		return -1
	}
	return n
}

func TestNoneRoleReadsOnlyGrantedTables(t *testing.T) {
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			cs := startCatalogServer(t)
			nina := cs.user(t, "nina", auth.RoleNone, mode)

			// Nothing is granted yet: only statements that read no table.
			expectDenied(t, nina, "SELECT * FROM lake.public.orders", "SELECT on table public.orders")
			expectAllowed(t, nina, "SELECT 1")
			expectAllowed(t, nina, "SELECT count(*) FROM information_schema.tables")
			expectAllowed(t, nina, "SELECT relname FROM pg_catalog.pg_class")
			expectAllowed(t, nina, "SHOW USERS")
			expectAllowed(t, nina, "BEGIN")
			expectAllowed(t, nina, "COMMIT")

			mustExec(t, cs.admin, "CREATE ROLE analyst")
			mustExec(t, cs.admin, "GRANT analyst TO nina")
			mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO analyst")

			for _, sql := range []string{
				"SELECT * FROM lake.public.orders",
				"SELECT * FROM orders",
				"SELECT * FROM public.orders",
				"SELECT o.id FROM orders o JOIN orders p USING (id)",
				"WITH c AS (SELECT * FROM orders) SELECT * FROM c a, c b",
				"WITH secrets AS (SELECT * FROM orders) SELECT * FROM secrets",
				"WITH RECURSIVE r(n) AS (SELECT min(id) FROM orders UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
				"SELECT (SELECT max(amount) FROM orders) AS m",
				"SELECT * FROM orders WHERE id IN (SELECT id FROM orders)",
				"SUMMARIZE orders",
				"DESCRIBE secrets",
				"EXPLAIN SELECT * FROM orders",
				"SET VARIABLE top = (SELECT max(amount) FROM orders)",
				"CALL pragma_table_info('orders')",
			} {
				expectAllowed(t, nina, sql)
			}
			if n := count(t, nina, "SELECT count(*) FROM orders WHERE amount > $1", 5); n != 2 {
				t.Errorf("parameterized query counted %d", n)
			}

			for _, sql := range []string{
				"SELECT * FROM secrets",
				"SELECT * FROM orders, secrets",
				"SELECT * FROM orders WHERE id IN (SELECT id FROM secrets)",
				"SELECT (SELECT max(code) FROM lake.public.secrets)",
				"WITH c AS (SELECT * FROM secrets) SELECT * FROM c",
				"SELECT * FROM orders UNION ALL SELECT id, 0 FROM secrets",
				"SELECT * FROM lake.public.orders o JOIN \"lake\".\"public\".\"secrets\" s USING (id)",
				"EXPLAIN SELECT * FROM secrets",
				"SET VARIABLE x = (SELECT max(code) FROM secrets)",
				"SUMMARIZE secrets",
			} {
				expectDenied(t, nina, sql, "secrets")
			}
			expectDenied(t, nina, "SELECT * FROM sales.leads", "sales.leads")
		})
	}
}

func TestNoneRoleCannotReadFilesOrBypassAnalysis(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.public TO nina")
	expectAllowed(t, nina, "SELECT * FROM orders")
	// Describing tables is allowed. (With Iceberg tables these failed until
	// statements were prepared again after the check: see reprepare.)
	expectAllowed(t, nina, "SELECT * FROM pragma_table_info('orders')")
	expectAllowed(t, nina, "CALL pragma_table_info('orders')")
	expectDenied(t, nina, "CALL range(3)", "admin")

	// Functions that take a table's name or a query read what their call
	// does not name. (The Iceberg functions are in the e2e test: this
	// engine does not load the extension. Files are in
	// TestNoneRoleCannotReadAllowedFiles.)
	for sql, reason := range map[string]string{
		"SELECT * FROM query_table('lake.sales.leads')":         "requires the admin role",
		"SELECT * FROM query('SELECT * FROM lake.sales.leads')": "query()",
		"SELECT * FROM orders, query_table('lake.sales.leads')": "requires the admin role",
		"SELECT * FROM read_parquet('s3://bucket/x.parquet')":   "",
		"SELECT * FROM duckdb_secrets()":                        "requires the admin role",
		"PRAGMA table_info('orders')":                           "PRAGMA",
		// s3://bucket/ is not an allowed location, so the file guard refuses
		// the write before the role check names it; both deny with 42501.
		"COPY orders TO 's3://bucket/out.parquet'":                  "",
		"SELECT 1; SELECT * FROM lake.sales.leads":                  "sales.leads",
		"SELECT * FROM orders, (FROM duckdb_tables()), sales.leads": "sales.leads",
	} {
		if reason == "" {
			expectDenied(t, nina, sql)
		} else {
			expectDenied(t, nina, sql, reason)
		}
	}

	// A macro an admin defines may read anything; its callers need the
	// reader role.
	mustExec(t, cs.admin, "CREATE MACRO memory.main.leads_count() AS (SELECT count(*) FROM lake.sales.leads)")
	expectDenied(t, nina, "SELECT leads_count() FROM orders", "macro leads_count")
	mustExec(t, cs.admin, "CREATE MACRO memory.main.leads_table() AS TABLE SELECT * FROM lake.sales.leads")
	expectDenied(t, nina, "SELECT * FROM leads_table()", "leads_table")
}

// TestNoneRoleCannotReadAllowedFiles reads files the sandbox allows, which
// readers may read and users with the none role may not: the files could be
// any table's.
func TestNoneRoleCannotReadAllowedFiles(t *testing.T) {
	dir := t.TempDir()
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}})
	parquet, csv := dir+"/orders.parquet", dir+"/orders.csv"
	mustExec(t, cs.admin, "COPY lake.public.orders TO '"+parquet+"' (FORMAT parquet)")
	mustExec(t, cs.admin, "COPY lake.public.orders TO '"+csv+"' (FORMAT csv)")

	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	if n := count(t, rita, "SELECT count(*) FROM read_parquet('"+parquet+"')"); n != 2 {
		t.Fatalf("a reader reads %d rows of the file", n)
	}

	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT ALL ON SCHEMA lake.public TO nina")
	for sql, fn := range map[string]string{
		"SELECT * FROM read_parquet('" + parquet + "')":                      "read_parquet",
		"SELECT * FROM '" + parquet + "'":                                    "parquet_scan",
		"SELECT * FROM orders WHERE id IN (SELECT id FROM '" + csv + "')":    "read_csv",
		"SELECT * FROM read_csv('" + csv + "')":                              "read_csv",
		"SELECT * FROM read_text('" + csv + "')":                             "read_text",
		"SELECT * FROM read_blob('" + csv + "')":                             "read_blob",
		"SELECT * FROM glob('" + dir + "/*')":                                "glob",
		"SELECT * FROM parquet_metadata('" + parquet + "')":                  "parquet_metadata",
		"SELECT * FROM parquet_schema('" + parquet + "')":                    "parquet_schema",
		"INSERT INTO orders SELECT * FROM read_parquet('" + parquet + "')":   "read_parquet",
		"CREATE TABLE lake.public.c AS SELECT * FROM '" + parquet + "'":      "parquet_scan",
		"SET VARIABLE n = (SELECT count(*) FROM read_csv('" + csv + "'))":    "read_csv",
		"SELECT (SELECT count(*) FROM read_parquet('" + parquet + "')) AS n": "read_parquet",
	} {
		expectDenied(t, nina, sql, fn, "reads data that grants do not cover")
	}
	expectDenied(t, nina, "COPY orders FROM '"+csv+"'", "COPY FROM reads files")
	expectAllowed(t, nina, "SELECT * FROM orders")
}

func TestSchemaGrantsCoverFutureTables(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.sales TO nina")
	if n := count(t, nina, "SELECT count(*) FROM lake.sales.leads"); n != 3 {
		t.Errorf("leads: %d", n)
	}
	mustExec(t, cs.admin, "CREATE TABLE lake.sales.later (i INTEGER)")
	expectAllowed(t, nina, "SELECT * FROM lake.sales.later")
	expectDenied(t, nina, "SELECT * FROM lake.public.orders")

	// ALL TABLES IN SCHEMA covers the tables there now, as in PostgreSQL.
	mustExec(t, cs.admin, "GRANT SELECT ON ALL TABLES IN SCHEMA lake.public TO nina")
	expectAllowed(t, nina, "SELECT * FROM lake.public.orders")
	mustExec(t, cs.admin, "CREATE TABLE lake.public.newer (i INTEGER)")
	expectDenied(t, nina, "SELECT * FROM lake.public.newer", "public.newer")
}

func TestViewsReadWithTheQueryingUsersGrants(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, cs.admin, "CREATE VIEW memory.main.big_orders AS SELECT * FROM lake.public.orders WHERE amount > 15")
	mustExec(t, cs.admin, "CREATE VIEW memory.main.order_codes AS SELECT o.id, s.code FROM lake.public.orders o JOIN lake.public.secrets s USING (id)")
	mustExec(t, cs.admin, "CREATE VIEW memory.main.nested AS SELECT * FROM memory.main.big_orders")

	if n := count(t, nina, "SELECT count(*) FROM big_orders"); n != 1 {
		t.Errorf("big_orders: %d", n)
	}
	expectAllowed(t, nina, "SELECT * FROM nested")
	expectDenied(t, nina, "SELECT * FROM order_codes", "public.secrets")
	expectDenied(t, nina, "SELECT * FROM big_orders, order_codes", "public.secrets")
}

func TestGrantsAllowWrites(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeCacheStatement)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)

	// A reader without any grants keeps the message they always got.
	_, err := rita.Exec(context.Background(), "INSERT INTO orders VALUES (3, 30)")
	if pgCode(err) != "42501" || !strings.Contains(err.Error(), "INSERT requires the writer role") {
		t.Errorf("reader insert without grants: %v", err)
	}

	mustExec(t, cs.admin, "GRANT INSERT, UPDATE ON TABLE lake.public.orders TO rita")
	expectAllowed(t, rita, "INSERT INTO orders VALUES (3, 30)")
	expectAllowed(t, rita, "UPDATE orders SET amount = amount + 1 WHERE id = 3")
	expectAllowed(t, rita, "INSERT INTO orders SELECT id + 10, 0 FROM lake.public.secrets")
	expectDenied(t, rita, "DELETE FROM orders", "DELETE on table public.orders")
	expectDenied(t, rita, "INSERT INTO secrets VALUES (2, 'y')", "INSERT on table public.secrets")
	expectDenied(t, rita, "CREATE TABLE lake.public.t (i INTEGER)", "schema public")
	expectDenied(t, rita, "SET threads = 1")

	// The none role also needs SELECT on what it reads, the target of
	// UPDATE and DELETE included.
	mustExec(t, cs.admin, "GRANT INSERT, UPDATE, DELETE ON lake.public.orders TO nina")
	expectAllowed(t, nina, "INSERT INTO orders VALUES (4, 40)")
	expectDenied(t, nina, "UPDATE orders SET amount = 0 WHERE id = 4", "SELECT on table public.orders")
	expectDenied(t, nina, "INSERT INTO orders SELECT id, 0 FROM secrets", "secrets")
	mustExec(t, cs.admin, "GRANT SELECT ON lake.public.orders TO nina")
	expectAllowed(t, nina, "UPDATE orders SET amount = 0 WHERE id = 4")
	expectAllowed(t, nina, "DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE amount = 0)")
	expectDenied(t, nina, "DELETE FROM orders WHERE id IN (SELECT id FROM secrets)", "secrets")
	expectDenied(t, nina, "UPDATE orders SET amount = (SELECT count(*) FROM secrets)", "secrets")
	expectDenied(t, nina, "COPY orders FROM 's3://bucket/x.csv'")
	expectDenied(t, nina, "DELETE FROM secrets")
}

func TestCreateOnSchemaAllowsDDL(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT CREATE ON SCHEMA lake.sales TO nina")
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")

	expectAllowed(t, nina, "CREATE TABLE lake.sales.scratch (i INTEGER)")
	expectAllowed(t, nina, "ALTER TABLE lake.sales.scratch ADD COLUMN j INTEGER")
	expectAllowed(t, nina, "CREATE TABLE lake.sales.copy AS SELECT * FROM lake.public.orders")
	expectDenied(t, nina, "CREATE TABLE lake.sales.stolen AS SELECT * FROM lake.public.secrets", "secrets")
	expectDenied(t, nina, "CREATE TABLE lake.public.nope (i INTEGER)", "schema public")
	expectDenied(t, nina, "DROP TABLE lake.public.secrets", "schema public")
	expectDenied(t, nina, "CREATE TEMP TABLE scratch (i INTEGER)")
	expectDenied(t, nina, "CREATE SCHEMA lake.mine")
	// CREATE does not grant reading what one creates.
	expectDenied(t, nina, "SELECT * FROM lake.sales.copy", "SELECT on table sales.copy")
	expectAllowed(t, nina, "DROP TABLE lake.sales.copy")
	expectAllowed(t, nina, "DROP TABLE lake.sales.scratch")
}

func TestRevokesAndRoleChangesReachSessions(t *testing.T) {
	cs := startCatalogServer(t, func(o *Options) { o.RoleRefresh = 300 * time.Millisecond })
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "CREATE ROLE analyst")
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.public TO analyst")
	mustExec(t, cs.admin, "GRANT analyst TO nina")
	expectAllowed(t, nina, "SELECT * FROM orders")

	// On the replica that made the change, at once.
	mustExec(t, cs.admin, "REVOKE analyst FROM nina")
	expectDenied(t, nina, "SELECT * FROM orders")
	mustExec(t, cs.admin, "GRANT analyst TO nina")
	expectAllowed(t, nina, "SELECT * FROM orders")

	// A change another replica made (here, straight in the store) arrives
	// within the refresh interval, even for a statement prepared before.
	ctx := context.Background()
	if err := cs.grants.RemoveMember(ctx, "analyst", "nina"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	expectDenied(t, nina, "SELECT * FROM orders")

	mustExec(t, cs.admin, "GRANT analyst TO nina")
	mustExec(t, cs.admin, "DROP ROLE analyst")
	expectDenied(t, nina, "SELECT * FROM orders")
}

// TestPreparedStatementsAreCheckedAgain checks that a statement prepared
// while allowed cannot read more later: DuckDB binds it again when a view
// it reads changes, or with a new search path.
func TestPreparedStatementsAreCheckedAgain(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, cs.admin, "CREATE TABLE lake.sales.orders (id INTEGER, amount INTEGER)")
	mustExec(t, cs.admin, "CREATE VIEW memory.main.pv AS SELECT id, amount FROM lake.public.orders")

	const viaView = "SELECT count(*) FROM pv WHERE id > $1"
	if n := count(t, nina, viaView, 0); n != 2 {
		t.Fatalf("through the view: %d", n)
	}
	mustExec(t, cs.admin, "CREATE OR REPLACE VIEW memory.main.pv AS SELECT id, length(code)::INTEGER AS amount FROM lake.public.secrets")
	if _, err := nina.Exec(ctx, viaView, 0); pgCode(err) != "42501" {
		t.Errorf("a prepared statement read a replaced view: %v", err)
	}

	const unqualified = "SELECT count(*) FROM orders WHERE id > $1"
	if n := count(t, nina, unqualified, 0); n != 2 {
		t.Fatalf("unqualified: %d", n)
	}
	mustExec(t, nina, "SET search_path = 'lake.sales,lake.public'")
	if _, err := nina.Exec(ctx, unqualified, 0); pgCode(err) != "42501" {
		t.Errorf("a prepared statement read another table after SET search_path: %v", err)
	}

	// Checked again, a statement is prepared again; one whose columns
	// changed meanwhile fails rather than send rows the client was not
	// told about.
	const star = "SELECT * FROM lake.public.orders WHERE id > $1"
	if _, err := nina.Exec(ctx, star, 0); err != nil {
		t.Fatalf("select *: %v", err)
	}
	mustExec(t, cs.admin, "UPDATE lake.public.orders SET amount = amount + 1")
	mustExec(t, cs.admin, "CREATE OR REPLACE VIEW memory.main.unrelated AS SELECT 1 AS x")
	if n := count(t, nina, "SELECT sum(amount) FROM lake.public.orders WHERE id > $1", 0); n != 32 {
		t.Errorf("after the catalog changed: sum %d", n)
	}
	mustExec(t, cs.admin, "ALTER TABLE lake.public.orders ADD COLUMN note VARCHAR")
	if _, err := nina.Exec(ctx, star, 0); pgCode(err) != "0A000" {
		t.Errorf("a prepared statement whose columns changed: %v", err)
	}
}

// TestPivot runs PIVOTs that take their columns from the data, which DuckDB
// turns into several statements.
func TestPivot(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, "CREATE TABLE lake.public.sales (region VARCHAR, quarter VARCHAR, amount INTEGER)")
	mustExec(t, cs.admin, "INSERT INTO lake.public.sales VALUES ('east', 'q1', 1), ('east', 'q2', 2), ('west', 'q1', 4)")

	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement} {
		rita := cs.user(t, fmt.Sprintf("rita%d", mode), auth.RoleReader, mode)
		var q1, q2 int64
		err := rita.QueryRow(ctx, "SELECT q1, q2 FROM (PIVOT sales ON quarter USING sum(amount)) WHERE region = 'east'").Scan(&q1, &q2)
		if err != nil || q1 != 1 || q2 != 2 {
			t.Errorf("%v: pivot: %d %d %v", mode, q1, q2, err)
		}
		// UNPIVOT leaves out west's q2, which is NULL.
		if n := count(t, rita, "SELECT count(*) FROM (UNPIVOT (PIVOT sales ON quarter USING sum(amount)) ON q1, q2 INTO NAME quarter VALUE amount)"); n != 3 {
			t.Errorf("%v: unpivot of a pivot: %d rows", mode, n)
		}
		// The query that finds the columns is checked first.
		const types = "SELECT count(*) FROM duckdb_types() WHERE type_name LIKE '__pivot_enum_%'"
		before := count(t, rita, types)
		expectDenied(t, rita, "PIVOT (SELECT * FROM duckdb_secrets()) ON name USING count(*)", "admin")
		expectDenied(t, rita, "PIVOT (SELECT * FROM query('SELECT 1 AS a')) ON a USING count(*)", "query()")
		if n := count(t, rita, types); before < 1 || n != before {
			t.Errorf("%v: %d pivot types, then %d: a refused PIVOT ran", mode, before, n)
		}
	}

	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	// Each statement of a query is checked on its own.
	expectDenied(t, rita, "PIVOT sales ON quarter USING sum(amount); DELETE FROM sales", "DELETE requires the writer role")
	// Outside a query, DuckDB would run the statement itself while
	// preparing it.
	for _, sql := range []string{
		"CREATE TABLE lake.public.by_quarter AS PIVOT sales ON quarter USING sum(amount)",
		"INSERT INTO lake.public.orders SELECT 9, q1 FROM (PIVOT sales ON quarter USING sum(amount))",
		"WITH p AS (PIVOT sales ON quarter USING sum(amount)) INSERT INTO lake.public.orders SELECT 9, q1 FROM p",
		"EXPLAIN PIVOT sales ON quarter USING sum(amount)",
	} {
		expectDenied(t, rita, sql, "IN (")
	}
	mustExec(t, rita, "BEGIN")
	if n := count(t, rita, "SELECT count(*) FROM (PIVOT sales ON quarter USING sum(amount))"); n != 2 {
		t.Errorf("pivot in a transaction: %d rows", n)
	}
	mustExec(t, rita, "COMMIT")
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, walt, "CREATE TABLE lake.public.by_quarter AS PIVOT sales ON quarter USING sum(amount)", "IN (")
	mustExec(t, walt, "BEGIN")
	expectDenied(t, walt, "INSERT INTO lake.public.orders SELECT 9, q1 FROM (PIVOT sales ON quarter USING sum(amount))", "IN (")
	mustExec(t, walt, "ROLLBACK")
	expectAllowed(t, walt, "CREATE TABLE lake.public.by_quarter AS PIVOT sales ON quarter IN ('q1', 'q2') USING sum(amount)")
	if n := count(t, cs.admin, "SELECT count(*) FROM lake.public.orders WHERE id = 9"); n != 0 {
		t.Errorf("a refused PIVOT inserted %d rows", n)
	}
	if n := count(t, cs.admin, "SELECT count(*) FROM lake.public.by_quarter"); n != 2 {
		t.Errorf("created from a pivot: %d rows", n)
	}

	// Users with the none role list the columns.
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.sales TO nina")
	expectDenied(t, nina, "PIVOT sales ON quarter USING sum(amount)", "IN (")
	if n := count(t, nina, "SELECT count(*) FROM (PIVOT sales ON quarter IN ('q1', 'q2') USING sum(amount))"); n != 2 {
		t.Errorf("none role, static pivot: %d rows", n)
	}
	expectDenied(t, nina, "SELECT * FROM (PIVOT secrets ON code IN ('x') USING count(*))", "secrets")
}

func TestDroppedTablesLoseTheirGrants(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.sales.leads TO nina")
	expectAllowed(t, nina, "SELECT * FROM lake.public.orders")

	mustExec(t, cs.admin, "DROP TABLE lake.public.orders")
	mustExec(t, cs.admin, "CREATE TABLE lake.public.orders (id INTEGER, card VARCHAR)")
	expectDenied(t, nina, "SELECT * FROM lake.public.orders")

	mustExec(t, cs.admin, "ALTER TABLE lake.sales.leads RENAME TO prospects")
	expectAllowed(t, nina, "SELECT * FROM lake.sales.prospects")
}

func TestRoleAndGrantStatements(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)

	for _, c := range []struct{ sql, code string }{
		{"CREATE ROLE reader", "42939"},
		{"CREATE ROLE nina", "42710"},
		{"CREATE ROLE \"Bad Name\"", "42601"},
		{"GRANT SELECT ON TABLE lake.public.nope TO nina", "42P01"},
		{"GRANT SELECT ON SCHEMA lake.nope TO nina", "3F000"},
		{"GRANT SELECT ON TABLE lake.public.orders TO ghost", "42704"},
		{"GRANT CREATE ON TABLE lake.public.orders TO nina", "0LP01"},
		{"GRANT TRUNCATE ON TABLE lake.public.orders TO nina", "42601"},
		{"GRANT ghost TO nina", "42704"},
		{"GRANT reader TO nina", "0LP01"},
		{"DROP ROLE ghost", "42704"},
	} {
		if _, err := cs.admin.Exec(ctx, c.sql); pgCode(err) != c.code {
			t.Errorf("%s: want %s, got %v", c.sql, c.code, err)
		}
	}
	mustExec(t, cs.admin, "DROP ROLE IF EXISTS ghost")
	mustExec(t, cs.admin, "CREATE ROLE analyst")
	if _, err := cs.admin.Exec(ctx, "CREATE USER analyst PASSWORD 'long-enough'"); pgCode(err) != "42710" {
		t.Errorf("a user named like a role: %v", err)
	}
	mustExec(t, cs.admin, "GRANT analyst TO nina")
	mustExec(t, cs.admin, "GRANT SELECT, INSERT ON public.orders TO analyst")
	mustExec(t, cs.admin, "GRANT ALL PRIVILEGES ON SCHEMA lake.sales TO nina")
	mustExec(t, cs.admin, "SET search_path = 'lake.public'")
	mustExec(t, cs.admin, "REVOKE INSERT ON TABLE orders FROM analyst")

	type grantRow struct {
		Grantee, GranteeType, Privilege, ObjectType, ObjectName, GrantedBy string
		GrantedAt                                                          time.Time
	}
	grants := func(conn *pgx.Conn, sql string) []string {
		t.Helper()
		rows, _ := conn.Query(ctx, sql)
		got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[grantRow])
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		var out []string
		for _, g := range got {
			out = append(out, g.Grantee+" "+g.Privilege+" "+g.ObjectType+" "+g.ObjectName)
		}
		slices.Sort(out)
		return out
	}
	want := []string{
		"analyst SELECT table public.orders",
		"nina CREATE schema sales", "nina DELETE schema sales", "nina INSERT schema sales",
		"nina SELECT schema sales", "nina UPDATE schema sales",
	}
	if got := grants(cs.admin, "SHOW GRANTS"); !slices.Equal(got, want) {
		t.Errorf("SHOW GRANTS:\n got %q\nwant %q", got, want)
	}
	if got := grants(nina, "SHOW GRANTS TO nina"); !slices.Equal(got, want) {
		t.Errorf("SHOW GRANTS TO nina:\n got %q\nwant %q", got, want)
	}
	if got := grants(cs.admin, "SHOW GRANTS ON TABLE lake.public.orders"); !slices.Equal(got, want[:1]) {
		t.Errorf("SHOW GRANTS ON TABLE: %q", got)
	}
	if got := grants(cs.admin, "SHOW GRANTS ON SCHEMA sales"); len(got) != 5 {
		t.Errorf("SHOW GRANTS ON SCHEMA: %q", got)
	}

	// Non-admins see what applies to them and change nothing.
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	if got := grants(rita, "SHOW GRANTS"); len(got) != 0 {
		t.Errorf("rita sees %q", got)
	}
	expectDenied(t, rita, "SHOW GRANTS TO nina")
	expectDenied(t, rita, "GRANT SELECT ON TABLE lake.public.orders TO rita")
	expectDenied(t, rita, "CREATE ROLE mine")

	var name, members string
	var created time.Time
	if err := nina.QueryRow(ctx, "SHOW ROLES").Scan(&name, &members, &created); err != nil || name != "analyst" || members != "nina" {
		t.Errorf("SHOW ROLES as nina: %s %s %v", name, members, err)
	}

	// Dropping a user removes their grants.
	mustExec(t, cs.admin, "DROP USER nina")
	if got := grants(cs.admin, "SHOW GRANTS"); !slices.Equal(got, want[:1]) {
		t.Errorf("after DROP USER: %q", got)
	}
}

func TestNoneRoleCursors(t *testing.T) {
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, nina, "BEGIN")
	expectAllowed(t, nina, "DECLARE c CURSOR FOR SELECT * FROM orders")
	expectDenied(t, nina, "DECLARE d CURSOR FOR SELECT * FROM secrets", "secrets")
	mustExec(t, nina, "ROLLBACK")
}

func TestQueryAndLoginHistory(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nina")
	mustExec(t, cs.admin, "ALTER USER nina PASSWORD 'another-secret-password'")
	expectAllowed(t, nina, "SELECT * FROM orders")
	expectDenied(t, nina, "SELECT * FROM secrets")
	if _, err := tryConnect(cs.url("nina", "wrong-password"), pgx.QueryExecModeSimpleProtocol); err == nil {
		t.Fatal("wrong password accepted")
	}

	type row struct {
		User, Outcome, SQLState, Kind, Text string
		Rows                                *int64
		Read                                []string
	}
	history := func(conn *pgx.Conn) []row {
		t.Helper()
		rows, err := conn.Query(ctx, `SELECT user_name, outcome, coalesce(sqlstate, ''), statement_type, query_text,
			row_count, tables_read FROM igloo.query_history ORDER BY start_time`)
		if err != nil {
			t.Fatal(err)
		}
		var out []row
		for rows.Next() {
			var r row
			var read any
			if err := rows.Scan(&r.User, &r.Outcome, &r.SQLState, &r.Kind, &r.Text, &r.Rows, &read); err != nil {
				t.Fatal(err)
			}
			if l, ok := read.([]any); ok {
				for _, v := range l {
					r.Read = append(r.Read, v.(string))
				}
			}
			out = append(out, r)
		}
		return out
	}

	mine := history(nina)
	for _, r := range mine {
		if r.User != "nina" {
			t.Errorf("nina sees another user's statement: %+v", r)
		}
	}
	find := func(rows []row, text string) *row {
		for i := range rows {
			if rows[i].Text == text {
				return &rows[i]
			}
		}
		return nil
	}
	if r := find(mine, "SELECT * FROM orders"); r == nil || r.Outcome != "ok" || r.Kind != "SELECT" ||
		r.Rows == nil || *r.Rows != 2 || !slices.Equal(r.Read, []string{"lake.public.orders"}) {
		t.Errorf("nina's select: %+v", r)
	}
	if r := find(mine, "SELECT * FROM secrets"); r == nil || r.Outcome != "denied" || r.SQLState != "42501" {
		t.Errorf("nina's denied select: %+v", r)
	}

	all := history(cs.admin)
	if find(all, "SELECT * FROM orders") == nil || find(all, "GRANT SELECT ON TABLE lake.public.orders TO nina") == nil {
		t.Error("the admin does not see everyone's statements")
	}
	for _, r := range all {
		if strings.Contains(r.Text, "password'") && !strings.Contains(r.Text, "'***'") ||
			strings.Contains(r.Text, "another-secret-password") || strings.Contains(r.Text, "nina-password") {
			t.Errorf("history kept a password: %q", r.Text)
		}
	}
	if r := find(all, "ALTER USER nina PASSWORD '***'"); r == nil || r.Kind != "ALTER USER" {
		t.Errorf("redacted ALTER USER missing: %+v", r)
	}

	// Calling the function directly shows no more.
	if n := count(t, nina, "SELECT count(*) FROM igloo_query_history() WHERE user_name <> 'nina'"); n != 0 {
		t.Errorf("nina read %d of other users' statements through the function", n)
	}
	var failures int64
	err := cs.admin.QueryRow(ctx, `SELECT count(*) FROM igloo.login_history
		WHERE user_name = 'nina' AND result = 'failure' AND reason = 'wrong password'`).Scan(&failures)
	if err != nil || failures != 1 {
		t.Errorf("failed logins: %d %v", failures, err)
	}
	if n := count(t, nina, "SELECT count(*) FROM igloo.login_history WHERE user_name <> 'nina'"); n != 0 {
		t.Errorf("nina sees %d logins of others", n)
	}
}
