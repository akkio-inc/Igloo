package pgwire

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/config"
)

// TestDataGenOneAtATime calls dbgen from sessions at once, which crashed the
// process, this test's among them, when two of them ran. One at a time runs, the
// others are refused with a code that says so, and the server answers after.
// A macro or a prepared statement cannot keep it for later.
func TestDataGenOneAtATime(t *testing.T) {
	dir := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if dir == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the tpch extension is not here")
	}
	ts := startServerWithConfig(t, config.Config{DuckDB: config.DuckDB{ExtensionDir: dir}})
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "LOAD tpch")

	const sessions = 8
	conns := make([]*pgx.Conn, sessions)
	for i := range conns {
		conns[i] = ts.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, conns[i], fmt.Sprintf("CREATE SCHEMA memory.gen%d", i))
	}
	ran, refused := 0, 0
	for range 5 {
		var mu sync.Mutex
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, conn := range conns {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := conn.Exec(ctx, fmt.Sprintf("CALL dbgen(sf = 0.01, catalog = 'memory', schema = 'gen%d', overwrite = true)", i))
				mu.Lock()
				defer mu.Unlock()
				switch code := pgCode(err); {
				case err == nil:
					ran++
				case code == "55P03":
					refused++
				default:
					t.Errorf("session %d: %v", i, err)
				}
			}()
		}
		close(start)
		wg.Wait()
	}
	if ran == 0 || refused == 0 {
		t.Errorf("%d ran and %d were refused, want some of each", ran, refused)
	}
	var one int
	if err := admin.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("after them: %d, %v", one, err)
	}

	// Called by other ways, one at a time, and kept for later by none.
	if _, err := admin.Exec(ctx, "SELECT * FROM dbgen(sf = 0.01, catalog = 'memory', schema = 'gen0', overwrite = true)"); err != nil {
		t.Errorf("in a SELECT: %v", err)
	}
	for _, q := range []string{
		"CREATE MACRO memory.main.gen_later() AS TABLE SELECT * FROM dbgen(sf = 0.01)",
		`CREATE MACRO memory.main.gen_later() AS TABLE SELECT * FROM "dbgen"(sf = 0.01)`,
		"CREATE VIEW memory.main.gen_later AS SELECT * FROM dbgen(sf = 0.01)",
		"PREPARE gen_later AS SELECT * FROM dbgen(sf = 0.01)",
	} {
		if _, err := admin.Exec(ctx, q); pgCode(err) != "0A000" {
			t.Errorf("%s: %v, want SQLSTATE 0A000", q, err)
		}
	}
	// Words that only look like it.
	var text string
	if err := admin.QueryRow(ctx, "SELECT 'dbgen' /* dsdgen */").Scan(&text); err != nil || text != "dbgen" {
		t.Errorf("a literal: %q, %v", text, err)
	}
}
