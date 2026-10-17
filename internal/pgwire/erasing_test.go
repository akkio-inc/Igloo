package pgwire

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/policy"
)

// A table an erasure request that is not over erases from keeps its name
// and its format version, whoever asks, admins included: it may be neither
// dropped nor renamed, its schema not dropped, no other table renamed to
// its name, and no table of format version 3 made under its name, or a
// tagged table's; and properties of such tables are set by literal keys
// and values only, none of them format-version. Once an admin closes the
// request, which only an admin may, the table is anyone's again.
func TestTablesAnErasureErasesFrom(t *testing.T) {
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
	store := erasure.NewMemoryStore()
	r := &erasure.Request{ID: "01blocked", Requester: "pat", Status: erasure.StatusBlocked, Step: erasure.StepRewrite, Created: time.Now(),
		DeleteStarted: true, Error: "the request went through every step with its other tables, but set aside public.people",
		Tables: []erasure.Table{{Name: "public.people", Ident: []string{"public", "people"}, Reason: "tagged", Blocked: "format version 3", BlockedAt: erasure.StepRewrite},
			{Name: "public.copy", Ident: []string{"public", "copy"}, Reason: "a clone of public.people"}}}
	if err := store.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	eraser := erasure.New(erasure.Options{Store: store, Catalog: cat})
	ts := startServerWithEngine(t, cfg, nil, func(o *Options) { o.Erasure, o.Policies = eraser, policy.NewMemoryStore() })
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
	writer := connect(t, ts.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)
	for _, conn := range []*pgx.Conn{writer, admin} {
		for _, sql := range []string{
			"DROP TABLE people",
			"DROP TABLE IF EXISTS lake.public.copy",
			"ALTER TABLE people RENAME TO folks",
			"ALTER TABLE other RENAME TO copy",
			"CREATE TABLE IF NOT EXISTS people (id INTEGER) WITH ('format-version' = '3')",
			"SELECT * FROM set_iceberg_table_properties(lake.public.people, MAP {'format' || '-version': '3'})",
			"SELECT * FROM set_iceberg_table_properties(lake.public.copy, {'format-version': 3})",
		} {
			expectDenied(t, conn, sql, "erasure request")
		}
	}
	expectDenied(t, admin, "DROP SCHEMA public CASCADE", "erasure request")
	// A tagged table may be dropped, but no table of version 3 made under
	// its name, as the tags stay with it; nor its properties set by a key
	// computed, or bound as a parameter.
	expectDenied(t, admin, "SELECT * FROM set_iceberg_table_properties(lake.public.tagged, MAP {concat('format', chr(45), 'version'): '3'})", "literals")
	param := ts.admin(t, pgx.QueryExecModeExec)
	if _, err := param.Exec(ctx, "SELECT * FROM set_iceberg_table_properties(lake.public.tagged, MAP {$1::VARCHAR: '3'})", "format-version"); pgCode(err) != "42501" {
		t.Errorf("a key bound as a parameter: %v", err)
	}
	mustExec(t, writer, "DROP TABLE tagged")
	expectDenied(t, writer, "CREATE TABLE tagged (id INTEGER, email VARCHAR) WITH ('format-version' = '3')", "tags")
	// Other tables, and other properties, are as before.
	mustExec(t, admin, "SELECT * FROM set_iceberg_table_properties(lake.public.people, {'history.expire.max-snapshot-age-ms': '86400000'})")
	mustExec(t, writer, "DROP TABLE other")

	// Only an admin closes the request.
	pat := connect(t, ts.url("pat", "pat-password"), pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, writer, "CLOSE ERASURE REQUEST '01blocked'")
	expectDenied(t, pat, "CLOSE ERASURE REQUEST '01blocked'", "admin")
	if _, err := pat.Exec(ctx, "CANCEL ERASURE REQUEST '01blocked'"); pgCode(err) != "22023" {
		t.Errorf("a blocked request was canceled: %v", err)
	}
	if got := texts(t, admin, "CLOSE ERASURE REQUEST '01blocked'"); len(got) != 1 || got[0] != "01blocked|closed" {
		t.Errorf("CLOSE: %q", got)
	}
	closed, err := store.Get(ctx, "01blocked")
	if err != nil {
		t.Fatal(err)
	}
	if closed.ClosedBy != "igloo" || closed.Tables[0].Left != "format version 3" || !strings.Contains(closed.Tables[1].Left, "closed at its rewrite step") {
		t.Errorf("closed: by %q, left %q and %q", closed.ClosedBy, closed.Tables[0].Left, closed.Tables[1].Left)
	}
	mustExec(t, writer, "DROP TABLE people")
}
