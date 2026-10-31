package pgwire

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/federation"
)

// TestRefreshLeavesTheCatalog checks that a refresh of a materialized view
// holds the catalog lock only to plan and to change the catalog: while a
// full refresh reads a slow table of a connection, another session runs
// statements, lists the catalog and signs in. The refresh held the lock
// for its whole run, and every other session waited.
func TestRefreshLeavesTheCatalog(t *testing.T) {
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	ctx := context.Background()
	remote, err := pgx.Connect(ctx, pg.url)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close(ctx) //nolint:errcheck
	if _, err := remote.Exec(ctx, "CREATE VIEW "+schema+".slow_counts AS SELECT pg_sleep(4)::text AS x, 1 AS n"); err != nil {
		t.Fatal(err)
	}
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.TLS = federation.TLSDisable // the test databases have no TLS
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	fed := federation.NewManager(store, federation.Options{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	env := startRefreshServerWith(t, []engine.Option{engine.BeforeLockdown(fed.Attach)})
	admin := env.admin
	mustExec(t, admin, "CREATE MATERIALIZED VIEW slow_view AS SELECT n FROM crm."+schema+".slow_counts")
	other := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := admin.Exec(ctx, "REFRESH MATERIALIZED VIEW slow_view")
		done <- err
	}()
	time.Sleep(time.Second)
	for _, q := range []string{"SELECT 42", "SELECT count(*) FROM information_schema.tables"} {
		qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var n int64
		err := other.QueryRow(qctx, q).Scan(&n)
		cancel()
		if err != nil {
			t.Errorf("%s while the refresh reads: %v", q, err)
		}
	}
	lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	login, err := pgx.Connect(lctx, env.testServer.url("igloo", adminPassword))
	cancel()
	if err != nil {
		t.Errorf("signing in while the refresh reads: %v", err)
	} else {
		_ = login.Close(ctx)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 4*time.Second {
		t.Errorf("the refresh took %s: the source was not slow", took)
	}
	if got := sortedTexts(t, admin, "SELECT n FROM slow_view"); len(got) != 1 || got[0] != "1" {
		t.Errorf("the view's rows: %v", got)
	}
	if action, _, _, _ := viewRefresh(t, admin, "public.slow_view"); action != "FULL" {
		t.Errorf("last refresh: %s", action)
	}
}
