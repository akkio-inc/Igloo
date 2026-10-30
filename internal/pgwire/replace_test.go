package pgwire

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// withObjects gives env a server that keeps its own view of the catalog
// (ddl.Manager), as Igloo on a catalog does, on the same catalog behind the
// same faults. swapIn asks it what a step that failed did.
func (env *catalogEnv) withObjects(t *testing.T, opts ...engine.Option) {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)
	cfg := env.direct
	cfg.Catalog.URI = env.faults.URL
	cfg.DuckDB.TempDir = t.TempDir()
	eng, err := engine.Open(ctx, cfg, log, opts...)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(ctx, cfg)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	objects, err := ddl.New(ctx, cat, eng, log)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	env.testServer = startServerOn(t, eng, func(o *Options) { o.DDL = objects })
}

// tablesNamed lists the catalog's tables whose names start with prefix,
// as the catalog itself has them, without faults: DuckDB keeps listing a
// table it renamed while it heard that the rename failed.
func (env *catalogEnv) tablesNamed(t *testing.T, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Open(ctx, env.direct)
	if err != nil {
		t.Fatal(err)
	}
	idents, err := cat.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, ident := range idents {
		if name := icecat.TableNameFromIdent(ident); strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// TestReplaceKeepsTheNewTableWhenTheCatalogFails checks CREATE OR REPLACE
// TABLE when the catalog applies the drop of the old table or the rename of
// the new one into place but DuckDB hears that it failed. After such a
// drop the new table was dropped too, and after such a rename the table in
// place, the new one, was dropped as if another session had made it: both
// copies were gone, and the error said the new table was complete under
// its temporary name.
func TestReplaceKeepsTheNewTableWhenTheCatalogFails(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	if env.fake == nil {
		t.Skip("needs icebergtest's catalog")
	}
	env.withObjects(t)
	// A repeat of the rename is refused, as by a catalog without
	// Idempotency-Key support, so DuckDB hears that it failed.
	env.fake.IgnoreIdempotencyKeys()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)

	for _, c := range []struct {
		name string
		arm  func()
	}{
		{"drop", func() { env.faults.ArmDrops(icebergtest.FailAfterApply) }},
		{"rename", func() { env.faults.ArmRenames(icebergtest.FailAfterApply) }},
	} {
		tbl := env.table + "_" + c.name
		mustExec(t, conn, "CREATE TABLE "+tbl+" AS SELECT 1 AS n")
		c.arm()
		if err := runQuery(conn, "CREATE OR REPLACE TABLE "+tbl+" AS SELECT range AS n FROM range(5)"); err != nil {
			t.Errorf("%s applied but failed: %v", c.name, err)
		}
		if got := env.tablesNamed(t, tbl); !slices.Equal(got, []string{tbl}) {
			t.Errorf("%s applied but failed: the catalog has %v, want only %s", c.name, got, tbl)
		}
		var n int64
		if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM "+tbl).Scan(&n); err != nil || n != 5 {
			t.Errorf("%s applied but failed: %d rows (%v), want the new table's 5", c.name, n, err)
		}
	}
}

// TestReplaceWithUnknownDropKeepsTheNewTable checks CREATE OR REPLACE
// TABLE when the catalog stops answering once it has dropped the old
// table: whether the old table is gone is unknown, so the new one is kept
// under its temporary name, which the error gives.
func TestReplaceWithUnknownDropKeepsTheNewTable(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	env.withObjects(t, engine.CatalogProxy(func(o *catalogproxy.Options) {
		o.AttemptTimeout = time.Second
		o.Patience = 2 * time.Second
	}))
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	tbl := env.table
	mustExec(t, conn, "CREATE TABLE "+tbl+" AS SELECT 1 AS n")

	env.faults.ArmDrops(icebergtest.DownAfterApply)
	err := runQuery(conn, "CREATE OR REPLACE TABLE "+tbl+" AS SELECT range AS n FROM range(5)")
	env.faults.Up()
	if err == nil {
		t.Fatal("the replace succeeded while the catalog was down")
	}
	got := env.tablesNamed(t, tbl)
	if len(got) != 1 || !strings.HasPrefix(got[0], tbl+"__igloo_new_") {
		t.Fatalf("the catalog has %v, want the new table under its temporary name (the replace's error: %v)", got, err)
	}
	if !strings.Contains(err.Error(), got[0]) {
		t.Errorf("the error does not say where the new table is, %s: %v", got[0], err)
	}
	if n := count(t, conn, `SELECT count(*) FROM "`+got[0]+`"`); n != 5 {
		t.Errorf("the new table, %s, has %d rows, want 5", got[0], n)
	}
}
