package pgwire

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/engine"
)

// TestPreparedCreateRunsAgain checks that a prepared CREATE TABLE ... AS a
// client runs again makes the table it says: DuckDB 1.5.6 makes it with no
// columns from the second run on (see statement.freshForRun).
func TestPreparedCreateRunsAgain(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, vc.cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	srv := startServerOn(t, eng, func(o *Options) { o.Catalog = "lake" })
	conn := srv.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE SCHEMA IF NOT EXISTS lake.public")

	sums := func(c *pgx.Conn, table string) (n, sum int64) {
		t.Helper()
		if err := c.QueryRow(ctx, "SELECT count(*), sum(b) FROM (SELECT 0 + b AS b FROM "+table+")").Scan(&n, &sum); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		return n, sum
	}
	for _, table := range []string{"_seed0", "lake.public.seed"} {
		create := "CREATE OR REPLACE TEMP TABLE _seed0 AS SELECT range AS b FROM range(1000)"
		if table != "_seed0" {
			create = "CREATE OR REPLACE TABLE lake.public.seed AS SELECT range AS b FROM range(1000)"
		}
		// A named prepared statement run again, as psycopg runs one.
		if _, err := conn.Prepare(ctx, "create_"+table, create); err != nil {
			t.Fatal(err)
		}
		for run := 1; run <= 3; run++ {
			if _, err := conn.Exec(ctx, "create_"+table); err != nil {
				t.Fatalf("%s, run %d: %v", table, run, err)
			}
			if n, sum := sums(conn, table); n != 1000 || sum != 499500 {
				t.Fatalf("%s after run %d: %d rows summing to %d", table, run, n, sum)
			}
		}
	}

	// With a parameter, each run its own value; and through pgx's cache of
	// statements, which prepares a text once and runs it again.
	cached := srv.admin(t, pgx.QueryExecModeCacheStatement)
	for run := int64(1); run <= 3; run++ {
		if _, err := cached.Exec(ctx, "CREATE OR REPLACE TEMP TABLE p AS SELECT range + $1::BIGINT AS b FROM range(10)", run); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if n, sum := sums(cached, "p"); n != 10 || sum != 45+10*run {
			t.Fatalf("p after run %d: %d rows summing to %d", run, n, sum)
		}
	}
}
