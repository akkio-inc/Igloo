package pgwire

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestTransactionsAsInPostgres checks the transaction behavior that
// psycopg2 (and so dbt) and JDBC rely on, where DuckDB's differs.
func TestSavepoints(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		conn := ts.admin(t, mode)
		mustExec(t, conn, "CREATE TABLE IF NOT EXISTS marks (i INTEGER)")
		before := count(t, conn, "SELECT count(*) FROM marks")
		code := func(sql string) string {
			_, err := conn.Exec(ctx, sql)
			return pgCode(err)
		}
		if c := code("SAVEPOINT a"); c != "25P01" {
			t.Errorf("%s: SAVEPOINT outside a block: %q", name, c)
		}

		// psycopg's transaction() blocks and SQLAlchemy's first connect.
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, `SAVEPOINT "_pg3_1"`)
		mustExec(t, conn, "SELECT oid FROM pg_type WHERE oid = to_regtype('hstore')")
		mustExec(t, conn, `RELEASE "_pg3_1"`)
		// A failed query since the savepoint is recovered from, whether
		// DuckDB failed it while preparing or while running it.
		for _, failing := range []string{"SELECT * FROM missing", "SELECT 'x'::INTEGER"} {
			mustExec(t, conn, "SAVEPOINT a")
			if code(failing) == "" {
				t.Fatalf("%s: %s succeeded", name, failing)
			}
			if c := code("ROLLBACK TO SAVEPOINT a"); c != "" || conn.PgConn().TxStatus() != 'T' {
				t.Errorf("%s: ROLLBACK TO after %s: %q, status %c", name, failing, c, conn.PgConn().TxStatus())
			}
		}
		mustExec(t, conn, "INSERT INTO marks VALUES (1)")
		// As in PostgreSQL, a name used twice is released newest first.
		mustExec(t, conn, "RELEASE SAVEPOINT a")
		mustExec(t, conn, "RELEASE a")
		if c := code("RELEASE a"); c != "3B001" {
			t.Errorf("%s: releasing a released savepoint: %q", name, c)
		}
		// A change since the savepoint cannot be undone.
		mustExec(t, conn, "SAVEPOINT b")
		mustExec(t, conn, "INSERT INTO marks VALUES (2)")
		if c := code("ROLLBACK TO b"); c != "0A000" || conn.PgConn().TxStatus() != 'E' {
			t.Errorf("%s: ROLLBACK TO after a change: %q, status %c", name, c, conn.PgConn().TxStatus())
		}
		mustExec(t, conn, "ROLLBACK")
		if n := count(t, conn, "SELECT count(*) FROM marks"); n != before {
			t.Errorf("%s: %d rows after ROLLBACK, want %d", name, n, before)
		}
	}
}

func TestTransactionsAsInPostgres(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE TABLE ledger (i INTEGER)")
	mustExec(t, admin, "CREATE USER walt WITH PASSWORD 'walt-password' ROLE writer")

	for name, mode := range modes {
		cfg, err := pgx.ParseConfig(ts.url("walt", "walt-password"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.DefaultQueryExecMode = mode
		var (
			mu      sync.Mutex
			notices []string
		)
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			mu.Lock()
			notices = append(notices, n.Severity+" "+n.Code+" "+n.Message)
			mu.Unlock()
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(ctx) })
		exec := func(sql string) (string, error) {
			tag, err := conn.Exec(ctx, sql)
			return tag.String(), err
		}
		lastNotice := func() string {
			mu.Lock()
			defer mu.Unlock()
			if len(notices) == 0 {
				return ""
			}
			return notices[len(notices)-1]
		}
		before := count(t, conn, "SELECT count(*) FROM ledger")

		// psycopg2 opens a transaction before dbt's own BEGIN.
		mustExec(t, conn, "BEGIN")
		if tag, err := exec("BEGIN"); err != nil || tag != "BEGIN" ||
			lastNotice() != "WARNING 25001 there is already a transaction in progress" {
			t.Errorf("%s: BEGIN in a block: %q %v, notice %q", name, tag, err, lastNotice())
		}
		mustExec(t, conn, "INSERT INTO ledger VALUES (1)")
		mustExec(t, conn, "COMMIT")
		if tag, err := exec("COMMIT"); err != nil || tag != "COMMIT" ||
			lastNotice() != "WARNING 25P01 there is no transaction in progress" {
			t.Errorf("%s: COMMIT outside a block: %q %v, notice %q", name, tag, err, lastNotice())
		}
		if tag, err := exec("ROLLBACK"); err != nil || tag != "ROLLBACK" {
			t.Errorf("%s: ROLLBACK outside a block: %q %v", name, tag, err)
		}

		// After a failure, the block refuses everything until it ends, even
		// statements prepared before, and COMMIT rolls it back.
		mustExec(t, conn, "BEGIN")
		mustExec(t, conn, "INSERT INTO ledger VALUES (2)")
		if _, err := exec("INSERT INTO missing VALUES (3)"); pgCode(err) != "42P01" {
			t.Fatalf("%s: %v", name, err)
		}
		for _, sql := range []string{"INSERT INTO ledger VALUES (2)", "SELECT 1", "SET statement_timeout = '1min'", "DEALLOCATE ALL"} {
			if _, err := exec(sql); pgCode(err) != "25P02" {
				t.Errorf("%s: %s in a failed block: %v", name, sql, err)
			}
		}
		if tag, err := exec("COMMIT"); err != nil || tag != "ROLLBACK" || conn.PgConn().TxStatus() != 'I' {
			t.Errorf("%s: COMMIT of a failed block: %q %v, status %c", name, tag, err, conn.PgConn().TxStatus())
		}
		if n := count(t, conn, "SELECT count(*) FROM ledger"); n != before+1 {
			t.Errorf("%s: %d rows, want %d", name, n, before+1)
		}
	}
}
