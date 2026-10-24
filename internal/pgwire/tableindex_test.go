package pgwire

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// TestTableIndexSeesNewTables checks that the index of tables the checks
// resolve names with (indexedTables) is built again when a table is made:
// a table made after a user's statements filled the index, earlier on
// their path than the one they may read, is the one their next statement
// reads, and needs a grant. Temporary tables, the session's own, are seen
// as before.
func TestTableIndexSeesNewTables(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.sales TO nina")
	mustExec(t, nina, "SET search_path = 'lake.public,lake.sales,memory.main'")
	for range 2 {
		if got := count(t, nina, "SELECT count(*) FROM leads"); got != 3 {
			t.Fatalf("leads: %d", got)
		}
	}
	cs.srv.tables.mu.Lock()
	built := cs.srv.tables.byName != nil
	cs.srv.tables.mu.Unlock()
	if !built {
		t.Fatal("the checks did not use the index")
	}

	mustExec(t, cs.admin, "CREATE TABLE lake.public.leads AS SELECT 9 AS id")
	expectCode(t, nina, "SELECT count(*) FROM leads", "42501", "public.leads")

	// A temporary table of the session's, first on every path.
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.sales TO walt")
	mustExec(t, walt, "SET search_path = 'lake.sales,memory.main'")
	if got := count(t, walt, "SELECT count(*) FROM leads"); got != 3 {
		t.Fatalf("walt's leads: %d", got)
	}
	mustExec(t, walt, "CREATE TEMP TABLE leads AS SELECT 1 AS id")
	if got := count(t, walt, "SELECT count(*) FROM leads"); got != 1 {
		t.Errorf("walt's temporary leads: %d", got)
	}
}

func TestMayCreateTemporary(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]bool{
		"CREATE TEMP TABLE t AS SELECT 1":           true,
		"create temporary view v as select 1":       true,
		"CREATE TABLE temp.main.t (i INTEGER)":      true,
		"CREATE OR REPLACE TEMP VIEW v AS SELECT 1": true,
		"CREATE TABLE lake.public.t AS SELECT 1":    false,
		"SELECT temp FROM weather":                  false,
		"CREATE TABLE t AS SELECT 'temp' AS label":  false,
		"-- create temp\nSELECT 1":                  false,
	} {
		if got := mayCreateTemporary(q); got != want {
			t.Errorf("%q: %v, want %v", q, got, want)
		}
	}
}

// TestTableIndexSeesIcebergTablesMadeElsewhere is TestTableIndexSeesNewTables
// for tables of an Iceberg catalog made around this replica (by another
// replica, or another engine): DuckDB's plan does not name the Iceberg
// tables it scans, so only the index stands between a user and a table
// made earlier on their path than the one they may read. The table is made
// while the catalog proxy keeps the listing that lacks it, and the index
// was built from that listing; DuckDB loads the name from the catalog,
// which tells the proxy, so the statement's checks see the table it bound.
func TestTableIndexSeesIcebergTablesMadeElsewhere(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, vc.cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	var grants *auth.MemoryGrants
	srv := startServerOn(t, eng, func(o *Options) {
		grants = auth.NewMemoryGrants(o.Users)
		o.Grants, o.Catalog = grants, "lake"
	})
	admin := srv.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE SCHEMA lake.sales",
		"CREATE TABLE lake.sales.leads AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3",
		"CREATE USER nina WITH PASSWORD 'nina-password' ROLE none",
		"GRANT SELECT ON SCHEMA lake.sales TO nina",
	} {
		mustExec(t, admin, q)
	}
	nina := connect(t, srv.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, nina, "SET search_path = 'lake.public,lake.sales,memory.main'")
	for range 2 {
		if got := count(t, nina, "SELECT count(*) FROM leads"); got != 3 {
			t.Fatalf("leads: %d", got)
		}
	}

	req, err := http.NewRequest(http.MethodPost, vc.fake.URL+"/v1/"+icebergtest.Prefix+"/namespaces/public/tables",
		strings.NewReader(`{"name": "leads", "schema": {"type": "struct", "schema-id": 0,
			"fields": [{"id": 1, "name": "id", "required": false, "type": "int"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("make public.leads around the replica: %d", resp.StatusCode)
	}
	expectCode(t, nina, "SELECT count(*) FROM leads", "42501", "public.leads")
}

// TestSchemasLoad checks that a replica that has just started lists every
// Iceberg table's real columns, not DuckDB's placeholder, both once
// WarmSchemas has run in the background and at the first statement that
// lists columns (LoadSchemas), which load tables side by side.
func TestSchemasLoad(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	a := vc.replica(t)
	admin := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	const tables = 40
	for i := range tables {
		mustExec(t, admin, fmt.Sprintf("CREATE TABLE lake.public.t%d AS SELECT %d AS id, 'x' AS label", i, i))
	}
	placeholders := func(eng *engine.Engine) int64 {
		t.Helper()
		conn, err := eng.Connect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		rows, err := conn.QueryContext(context.Background(),
			`SELECT count(*) FROM duckdb_columns() WHERE database_name = 'lake' AND column_name = '__'`, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close() //nolint:errcheck
		dest := make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Fatal(err)
		}
		n, _ := dest[0].(int64)
		return n
	}

	b := vc.replica(t)
	if n := placeholders(b.eng); n == 0 {
		t.Skip("DuckDB loaded every schema at attach; nothing to test")
	}
	if err := b.eng.WarmSchemas(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := placeholders(b.eng); n != 0 {
		t.Errorf("after WarmSchemas: %d tables still have a placeholder", n)
	}

	c := vc.replica(t)
	got := count(t, c.admin(t, pgx.QueryExecModeSimpleProtocol),
		"SELECT count(*) FROM information_schema.columns WHERE table_catalog = 'lake' AND table_schema = 'public' AND table_name LIKE 't%'")
	if got != 2*tables {
		t.Errorf("columns listed by a replica that just started: %d, want %d", got, 2*tables)
	}
}
