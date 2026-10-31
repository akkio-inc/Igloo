package pgwire

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The tables a transaction block reads or writes hold back maintenance's
// rewrites until it ends, by COMMIT, ROLLBACK, the end of a script's
// implicit block or the end of the connection: dbt's incremental models
// failed at COMMIT when compaction committed in the middle of their blocks.
func TestTransactionsHoldMaintenance(t *testing.T) {
	env := startRefreshServer(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE events (id INTEGER)")
	mustExec(t, admin, "CREATE TABLE other (id INTEGER)")
	mustExec(t, admin, "INSERT INTO events VALUES (1)")
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
	mustExec(t, admin, "DELETE FROM events WHERE id = 1")
	held("public.events")
	mustExec(t, admin, "INSERT INTO events VALUES (2)")
	mustExec(t, admin, "COMMIT")
	held()

	// Reading is enough: the block commits on top of what it read.
	mustExec(t, admin, "BEGIN")
	texts(t, admin, "SELECT count(*) FROM other")
	held("PUBLIC.Other")
	mustExec(t, admin, "ROLLBACK")
	held()

	mustExec(t, admin, "INSERT INTO events VALUES (3); INSERT INTO other VALUES (3)")
	held()

	conn := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "INSERT INTO other VALUES (4)")
	held("PUBLIC.Other")
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
