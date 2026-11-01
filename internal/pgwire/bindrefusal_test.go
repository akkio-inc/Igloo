package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestRunsBindAgainWhenRefused checks that a run whose bind the catalog
// proxy refuses, because the bind listed a schema's tables while another
// bind held the catalog lock shared, binds again under the exclusive lock
// rather than fail with the refusal: a statement prepared over a table that
// was dropped since lists the schema as DuckDB binds it to run, to suggest
// another name, and gets DuckDB's own error.
func TestRunsBindAgainWhenRefused(t *testing.T) {
	env := startRefreshServer(t)
	ctx := context.Background()
	mustExec(t, env.admin, "CREATE TABLE gone_soon (k INTEGER)")
	conn := env.testServer.admin(t, pgx.QueryExecModeCacheStatement)
	q := "SELECT count(*) FROM gone_soon WHERE k > $1"
	var n int64
	if err := conn.QueryRow(ctx, q, 0).Scan(&n); err != nil {
		t.Fatal(err)
	}
	mustExec(t, env.admin, "DROP TABLE gone_soon")

	// Another bind holds the lock shared for a moment.
	release := env.srv.engine.CatalogShare()
	go func() {
		time.Sleep(300 * time.Millisecond)
		release()
	}()
	refusals := env.srv.engine.SharedRefusals()
	err := conn.QueryRow(ctx, q, 0).Scan(&n)
	if err == nil || !strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Locked") {
		t.Fatalf("a run of a statement over a dropped table: %v", err)
	}
	if env.srv.engine.SharedRefusals() == refusals {
		t.Fatal("the proxy refused nothing: the test no longer makes a run's bind list the schema")
	}
}
