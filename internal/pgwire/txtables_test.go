package pgwire

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Transaction blocks hold back maintenance's rewrites until they end, by
// COMMIT, ROLLBACK, the end of a script's implicit block or the end of the
// connection: dbt's incremental models failed at COMMIT when compaction
// committed in the middle of their blocks. A block of a user who may write
// holds every table from its BEGIN, as DuckDB reads each as of the block's
// start; a reader's, the tables it read.
func TestTransactionsHoldMaintenance(t *testing.T) {
	env := startRefreshServer(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE events (id INTEGER)")
	mustExec(t, admin, "CREATE TABLE other (id INTEGER)")
	mustExec(t, admin, "INSERT INTO events VALUES (1)")
	mustExec(t, admin, "CREATE USER rhea WITH PASSWORD 'rhea-password' ROLE reader")
	reader := connect(t, env.url("rhea", "rhea-password"), pgx.QueryExecModeSimpleProtocol)
	held := func(want ...string) {
		t.Helper()
		for _, table := range []string{"public.events", "PUBLIC.Other"} {
			wanted := false
			for _, w := range want {
				wanted = wanted || w == table
			}
			if got := env.srv.InTransaction(table); got != wanted {
				t.Fatalf("InTransaction(%q) = %v", table, got)
			}
		}
	}
	held()

	mustExec(t, admin, "BEGIN")
	held("public.events", "PUBLIC.Other")
	mustExec(t, admin, "DELETE FROM events WHERE id = 1")
	mustExec(t, admin, "INSERT INTO events VALUES (2)")
	mustExec(t, admin, "COMMIT")
	held()

	// A reader's block holds what it read: a block commits on top of what
	// it read.
	mustExec(t, reader, "BEGIN")
	held()
	texts(t, reader, "SELECT count(*) FROM other")
	held("PUBLIC.Other")
	mustExec(t, reader, "ROLLBACK")
	held()

	mustExec(t, admin, "INSERT INTO events VALUES (3); INSERT INTO other VALUES (3)")
	held()

	conn := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "INSERT INTO other VALUES (4)")
	held("public.events", "PUBLIC.Other")
	if err := conn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); env.srv.InTransaction("public.other"); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("a closed connection's block still holds maintenance back")
		}
	}
	if got := texts(t, admin, "SELECT count(*) FROM events"); len(got) != 1 || got[0] != "2" {
		t.Errorf("events: %v", got)
	}
}
