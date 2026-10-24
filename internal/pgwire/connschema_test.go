package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/federation"
)

// TestSchemaNamedLikeAConnections checks names qualified with a schema that
// both the catalog and a connection have, as the catalog's media_demo and
// a Snowflake connection's MEDIA_DEMO: DuckDB binds media_demo.t to the
// catalog's table unless the connection's schema is on the search path, and
// the checks of reads, writes, DDL and policies must say the same, for
// users with the none role and for readers. Once on a DuckDB stand-in for
// the catalog, whose scans name their tables, and once on an Iceberg
// catalog, whose scans do not.
func TestSchemaNamedLikeAConnections(t *testing.T) {
	t.Parallel()
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	t.Run("duckdb lake", func(t *testing.T) {
		testSchemaNamedLikeAConnections(t, pg, func(store *federation.MemoryStore) *catalogServer {
			return startFedServer(t, store).catalogServer
		})
	})
	t.Run("iceberg lake", func(t *testing.T) {
		testSchemaNamedLikeAConnections(t, pg, func(store *federation.MemoryStore) *catalogServer {
			return startIcebergServer(t, store, nil, true).catalogServer
		})
	})
}

// testSchemaNamedLikeAConnections runs the test on the server start starts
// with the connections of store.
func testSchemaNamedLikeAConnections(t *testing.T, pg remoteDatabase, start func(*federation.MemoryStore) *catalogServer) {
	ctx := context.Background()
	schema := uniqueName("media_demo_")
	remote, err := pgx.Connect(ctx, pg.url)
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".audit_events (id int, note text)",
		"INSERT INTO " + schema + ".audit_events SELECT g, 'remote' FROM generate_series(1, 5) g",
		"CREATE TABLE " + schema + ".leads (id int)",
		"INSERT INTO " + schema + ".leads VALUES (1), (4)",
		"CREATE TABLE " + schema + ".scratch (id int)",
	} {
		if _, err := remote.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	_ = remote.Close(ctx)
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), pg.url)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = c.Close(context.Background())
		}
	})

	// sf, as Igloo attaches connections when it starts.
	store := federation.NewMemoryStore()
	c := federation.New("sf", federation.Postgres)
	c.TLS = federation.TLSDisable // the test databases have no TLS
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	cs := start(store)
	admin := cs.admin

	audit := schema + `."AUDIT_EVENTS"`
	remoteAudit, leads := "sf."+schema+".audit_events", "sf."+schema+".leads"
	for _, sql := range []string{
		"CREATE SCHEMA lake." + schema,
		"CREATE TABLE lake." + audit + " (id INTEGER, note VARCHAR)",
		"INSERT INTO lake." + audit + " VALUES (1, 'lake'), (2, 'lake'), (3, 'lake')",
		"CREATE TABLE lake." + schema + ".scratch (id INTEGER)",
		"CREATE USER nina WITH PASSWORD 'nina-password' ROLE none",
		"GRANT SELECT ON SCHEMA lake." + schema + " TO nina",
		"CREATE USER nora WITH PASSWORD 'nora-password' ROLE none",
		"GRANT SELECT ON TABLE lake." + audit + " TO nora",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
		"GRANT SELECT ON TABLE " + leads + " TO rita",
		"CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE none",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lake." + audit + " TO wanda",
		"GRANT CREATE, SELECT ON SCHEMA lake." + schema + " TO wanda",
		"SET search_path = 'lake." + schema + "'",
	} {
		mustExec(t, admin, sql)
	}
	session := func(name string, mode pgx.QueryExecMode) *pgx.Conn {
		conn := connect(t, cs.url(name, name+"-password"), mode)
		mustExec(t, conn, "SET search_path = 'lake."+schema+"'")
		return conn
	}
	nina := session("nina", pgx.QueryExecModeSimpleProtocol)
	nora := session("nora", pgx.QueryExecModeCacheStatement)
	rita := session("rita", pgx.QueryExecModeDescribeExec)
	wanda := session("wanda", pgx.QueryExecModeSimpleProtocol)
	want := func(conn *pgx.Conn, sql string, n int64) {
		t.Helper()
		if got := count(t, conn, sql); got != n {
			t.Errorf("%s, %s: %d rows, want %d", userName(t, conn), sql, got, n)
		}
	}

	// The catalog's schema is on the path: the name means the catalog's
	// table, written in any case, as with no schema or the catalog's name.
	for _, conn := range []*pgx.Conn{nina, nora, rita} {
		for _, sql := range []string{
			"SELECT count(*) FROM " + audit,
			"SELECT count(*) FROM " + strings.ToUpper(schema) + `."AUDIT_EVENTS"`,
			`SELECT count(*) FROM "AUDIT_EVENTS"`,
			"SELECT count(*) FROM lake." + audit,
			"SELECT count(*) FROM (SELECT * FROM " + audit + ") a JOIN " + audit + " b USING (id)",
		} {
			want(conn, sql, 3)
		}
		expectAllowed(t, conn, "EXPLAIN SELECT count(*) FROM "+audit)
		// The connection's table still needs a grant on it, and a table
		// only the connection has is not found by the catalog's schema.
		expectDenied(t, conn, "SELECT count(*) FROM "+remoteAudit, "sf."+schema+".audit_events")
		if _, err := conn.Exec(ctx, "SELECT count(*) FROM "+schema+".leads"); pgCode(err) != "42P01" {
			t.Errorf("%s: a table only the connection has: %v", userName(t, conn), err)
		}
	}
	// A reader may join it with the connection's tables granted to them.
	want(rita, "SELECT count(*) FROM "+audit+" a JOIN "+leads+" l USING (id)", 1)
	// With the schema not on the path, the name is looked for in the current
	// database, the catalog.
	mustExec(t, nina, "SET search_path = 'lake.public'")
	want(nina, "SELECT count(*) FROM "+audit, 3)
	// With the connection's schema first on the path, the name means the
	// connection's table.
	for _, conn := range []*pgx.Conn{nina, rita} {
		mustExec(t, conn, "SET search_path = 'sf."+schema+",lake."+schema+"'")
		expectDenied(t, conn, "SELECT count(*) FROM "+audit, "sf."+schema+".audit_events")
	}
	// With it after the catalog's, the catalog's table, which readers read.
	// Either could be read once the other is dropped, so users with the
	// none role need grants on both, as for a name with no schema.
	mustExec(t, nina, "SET search_path = 'lake."+schema+",sf."+schema+"'")
	mustExec(t, rita, "SET search_path = 'lake."+schema+",sf."+schema+"'")
	expectDenied(t, nina, "SELECT count(*) FROM "+audit, "sf."+schema+".audit_events")
	expectDenied(t, nina, `SELECT count(*) FROM "AUDIT_EVENTS"`, "sf."+schema+".audit_events")
	want(rita, "SELECT count(*) FROM "+audit, 3)
	for _, conn := range []*pgx.Conn{nina, rita} {
		mustExec(t, conn, "SET search_path = 'lake."+schema+"'")
	}

	// Writes and DDL by grants.
	for _, sql := range []string{
		"INSERT INTO " + audit + " VALUES (4, 'new')",
		"UPDATE " + audit + " SET note = 'changed' WHERE id = 4",
		"DELETE FROM " + audit + " WHERE id = 4",
		"CREATE TABLE " + schema + ".made AS SELECT * FROM " + audit,
		"DROP TABLE " + schema + ".made",
		"DROP TABLE " + schema + ".scratch",
	} {
		expectAllowed(t, wanda, sql)
	}
	want(nina, "SELECT count(*) FROM "+audit, 3)
	want(admin, "SELECT count(*) FROM "+remoteAudit, 5)

	// Policies: one on the connection's table leaves the catalog's alone,
	// and one on the catalog's table applies however it is named. A
	// policy's own query resolves names the same way.
	for _, sql := range []string{
		"CREATE ROW ACCESS POLICY no_rows AS (i INTEGER) RETURNS BOOLEAN -> false",
		"ALTER TABLE " + remoteAudit + " ADD ROW ACCESS POLICY no_rows ON (id)",
	} {
		mustExec(t, admin, sql)
	}
	want(admin, "SELECT count(*) FROM "+remoteAudit, 0)
	for _, conn := range []*pgx.Conn{nina, rita} {
		want(conn, "SELECT count(*) FROM "+audit, 3)
	}
	for _, sql := range []string{
		"CREATE ROW ACCESS POLICY low_ids AS (i INTEGER) RETURNS BOOLEAN -> i <= 2",
		"ALTER TABLE lake." + audit + " ADD ROW ACCESS POLICY low_ids ON (id)",
		"CREATE ROW ACCESS POLICY audited AS (i INTEGER) RETURNS BOOLEAN -> EXISTS (SELECT 1 FROM " + audit + " a WHERE a.id = i)",
		"ALTER TABLE " + leads + " ADD ROW ACCESS POLICY audited ON (id)",
	} {
		mustExec(t, admin, sql)
	}
	for _, conn := range []*pgx.Conn{nina, nora, rita} {
		want(conn, "SELECT count(*) FROM "+audit, 2)
		want(conn, `SELECT count(*) FROM "AUDIT_EVENTS"`, 2)
	}
	// The policy on leads reads the catalog's table, which has ids 1 to 3,
	// not the connection's, which has 1 to 5.
	want(rita, "SELECT count(*) FROM "+leads, 1)
}
