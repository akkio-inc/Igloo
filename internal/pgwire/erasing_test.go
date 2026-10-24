package pgwire

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/policy"
)

// A table an erasure request that is not over erases from keeps its name
// and its format version, whoever asks, admins included: it may be neither
// dropped nor replaced nor renamed, its schema not dropped, no other table
// renamed to its name, and no table of format version 3 made under its
// name, or a tagged table's, however the statement says so: Igloo checks
// every change DuckDB makes to the catalog (see catalogguard.go). Once an
// admin closes the request, which only an admin may, the table is anyone's
// again.
func TestTablesAnErasureErasesFrom(t *testing.T) {
	t.Parallel()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	cfg := config.Config{
		Catalog: config.Catalog{URI: fake.URL, Warehouse: "test", Name: "lake", Schema: "public"},
		Storage: config.Storage{Location: dir, S3: config.S3{Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused"}},
		DuckDB:  config.DuckDB{ExtensionDir: ext, TempDir: t.TempDir()},
	}
	cat, err := catalog.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	store, policies := erasure.NewMemoryStore(), policy.NewMemoryStore()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connector.Close() })
	eraser := erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: policies.Load, Engine: duckdbConnector{connector},
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Warehouse: dir + "/"})
	ts := startServerWithEngine(t, cfg, nil, func(o *Options) { o.Erasure, o.Policies = eraser, policies })
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"CREATE TABLE people (id INTEGER, email VARCHAR)",
		"CREATE TABLE copy (id INTEGER, email VARCHAR)",
		"CREATE TABLE other (id INTEGER, email VARCHAR)",
		"CREATE TABLE tagged (id INTEGER, email VARCHAR)",
		"CREATE TAG pii ALLOWED_VALUES 'email'",
		"ALTER TABLE tagged ALTER COLUMN email SET TAG pii = 'email'",
		"CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer",
		"CREATE USER pat WITH PASSWORD 'pat-password' ROLE none",
		"CREATE ROLE privacy_officer",
		"GRANT ERASE ON ACCOUNT TO privacy_officer",
		"GRANT privacy_officer TO pat",
	} {
		mustExec(t, admin, sql)
	}
	// A request blocked on public.people, set aside, with public.copy.
	r, err := eraser.Submit(ctx, "pat", []erasure.Wanted{{Tag: "pii", Value: "email", Identifiers: []string{"ann@example.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	r.Status, r.Step, r.DeleteStarted = erasure.StatusBlocked, erasure.StepRewrite, true
	r.Error = "the request went through every step with its other tables, but set aside public.people"
	r.Tables = []erasure.Table{{Name: "public.people", Ident: []string{"public", "people"}, Reason: "tagged", Blocked: "format version 3", BlockedAt: erasure.StepRewrite},
		{Name: "public.copy", Ident: []string{"public", "copy"}, Reason: "a clone of public.people"}}
	if err := store.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	writer := connect(t, ts.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)
	for _, conn := range []*pgx.Conn{writer, admin} {
		for _, sql := range []string{
			"DROP TABLE people",
			"DROP TABLE IF EXISTS lake.public.copy",
			"ALTER TABLE people RENAME TO folks",
			"ALTER TABLE other RENAME TO copy",
			"CREATE TABLE IF NOT EXISTS people (id INTEGER) WITH ('format-version' = '3')",
			"CREATE OR REPLACE TABLE people AS SELECT * FROM people",
			"SELECT 1; DROP TABLE people",
			"SELECT * FROM set_iceberg_table_properties/**/(lake.public.people, MAP {'format' || '-version': '3'})",
			"SELECT * FROM set_iceberg_table_properties(lake.public.people, MAP {'format' || '-version': '3'})",
			"SELECT * FROM set_iceberg_table_properties(lake.public.copy, {'format-version': 3})",
		} {
			expectDenied(t, conn, sql, "erasure request")
		}
	}
	expectDenied(t, admin, "DROP SCHEMA public CASCADE", "erasure request")
	// EXPLAIN, which only admins may run, runs what it explains.
	expectDenied(t, admin, "EXPLAIN ANALYZE DROP TABLE people", "erasure request")
	expectDenied(t, admin, "EXPLAIN ANALYZE ALTER TABLE people RENAME TO folks", "erasure request")
	// A tagged table may be dropped, but no table of version 3 made under
	// its name, as the tags stay with it, nor the table upgraded, by a key
	// computed or bound as a parameter included.
	expectDenied(t, admin, "SELECT * FROM set_iceberg_table_properties(lake.public.tagged, MAP {concat('format', chr(45), 'version'): '3'})", "format version 2")
	param := ts.admin(t, pgx.QueryExecModeExec)
	if _, err := param.Exec(ctx, "SELECT * FROM set_iceberg_table_properties(lake.public.tagged, MAP {$1::VARCHAR: '3'})", "format-version"); pgCode(err) != "42501" {
		t.Errorf("a key bound as a parameter: %v", err)
	}
	mustExec(t, writer, "DROP TABLE tagged")
	expectDenied(t, writer, "CREATE TABLE tagged (id INTEGER, email VARCHAR) WITH ('format-version' = '3')", "tags")
	// No one claims a table reads another's files, which only CLONE does.
	for _, conn := range []*pgx.Conn{writer, admin} {
		expectDenied(t, conn, "SELECT * FROM set_iceberg_table_properties(lake.public.other, {'igloo.clone.source': 'public.people'})",
			"Igloo's own property")
	}
	// Other tables, and other properties, are as before.
	mustExec(t, admin, "SELECT * FROM set_iceberg_table_properties(lake.public.people, {'history.expire.max-snapshot-age-ms': '86400000'})")
	mustExec(t, writer, "DROP TABLE other")

	// Only an admin closes the request.
	pat := connect(t, ts.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, writer, "CLOSE ERASURE REQUEST '"+r.ID+"'")
	expectDenied(t, pat, "CLOSE ERASURE REQUEST '"+r.ID+"'", "admin")
	if _, err := pat.Exec(ctx, "CANCEL ERASURE REQUEST '"+r.ID+"'"); pgCode(err) != "22023" {
		t.Errorf("a blocked request was canceled: %v", err)
	}
	if got := texts(t, admin, "CLOSE ERASURE REQUEST '"+r.ID+"'"); len(got) != 1 || got[0] != r.ID+"|closed" {
		t.Errorf("CLOSE: %q", got)
	}
	closed, err := store.Get(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.ClosedBy != "igloo" || closed.Tables[0].Left != "format version 3" || !strings.Contains(closed.Tables[1].Left, "closed at its rewrite step") {
		t.Errorf("closed: by %q, left %q and %q", closed.ClosedBy, closed.Tables[0].Left, closed.Tables[1].Left)
	}
	mustExec(t, writer, "DROP TABLE people")
}
