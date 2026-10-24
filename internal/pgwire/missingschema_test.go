package pgwire

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// TestSchemaMadeAfterALookupMissedIt checks that a schema created after a
// statement looked for it, by this replica or another, can be used: DuckDB
// otherwise goes on taking it for missing, in every session, until it
// stops (see Engine.ForgetMissingSchema).
func TestSchemaMadeAfterALookupMissedIt(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, vc.cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	srv := startServerOn(t, eng, func(o *Options) { o.Catalog = "lake" })
	admin := srv.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustFail := func(q string) {
		t.Helper()
		if _, err := admin.Exec(ctx, q); err == nil || !strings.Contains(err.Error(), "c1") {
			t.Fatalf("%s: %v, want an error about the missing schema", q, err)
		}
	}

	// Made here.
	mustFail("CREATE TABLE lake.c10.t AS SELECT 1 AS x")
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.c10")
	mustExec(t, admin, "CREATE TABLE lake.c10.t AS SELECT 1 AS x")
	other := srv.admin(t, pgx.QueryExecModeSimpleProtocol)
	if n := count(t, other, "SELECT count(*) FROM lake.c10.t"); n != 1 {
		t.Fatalf("c10.t: %d rows", n)
	}

	// Made by another replica, straight in the catalog.
	mustFail("SELECT * FROM lake.c11.t")
	res, err := http.Post(vc.fake.URL+"/v1/"+icebergtest.Prefix+"/namespaces", "application/json",
		strings.NewReader(`{"namespace": ["c11"]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create c11: %s", res.Status)
	}
	mustExec(t, admin, "CREATE TABLE lake.c11.t AS SELECT 2 AS x")
	if n := count(t, admin, "SELECT sum(x) FROM lake.c11.t"); n != 2 {
		t.Fatalf("c11.t: %d", n)
	}

	// Forgetting never drops: the schemas and their tables are all there.
	commits := vc.fake.Commits()
	for _, schema := range []string{"c10", "c11"} {
		if !eng.ForgetMissingSchema(ctx, schema) {
			t.Fatalf("%s: not forgotten", schema)
		}
	}
	if eng.ForgetMissingSchema(ctx, "c12") {
		t.Fatal("c12, which the catalog does not have, forgotten")
	}
	if got := vc.fake.Commits(); got != commits {
		t.Fatalf("forgetting made %d commits", got-commits)
	}
	for _, q := range []string{"SELECT count(*) FROM lake.c10.t", "SELECT count(*) FROM lake.c11.t"} {
		if n := count(t, other, q); n != 1 {
			t.Fatalf("%s: %d", q, n)
		}
	}
}

func TestMissingSchemaOf(t *testing.T) {
	t.Parallel()
	for msg, want := range map[string]string{
		`Catalog Error: Schema with name "c10" not found`:                                            "c10",
		`Catalog Error: Table with name "c10.t" does not exist because schema "c10" does not exist.`: "c10",
		`Invalid Input Error: Schema with name "a.b" does not exist`:                                 "a.b",
		`Catalog Error: Iceberg namespace by the name of 'sales' does not exist`:                     "sales",
		`Catalog Error: Table with name t does not exist!`:                                           "",
		`Binder Error: Referenced column "schema" not found in FROM clause`:                          "",
	} {
		if got := missingSchemaOf(msg); got != want {
			t.Errorf("missingSchemaOf(%q) = %q, want %q", msg, got, want)
		}
	}
}
