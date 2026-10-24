package pgwire

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/federation"
)

// TestCreateTableAsInSteps checks CREATE TABLE ... AS of an Iceberg table,
// which Igloo carries out in steps (see stagedCreate): every form of query
// fills the table, a table that exists gives DuckDB's error or, with IF NOT
// EXISTS, is left alone, and a query that fails part way leaves nothing
// behind.
func TestCreateTableAsInSteps(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	tbl := env.table
	for _, c := range []struct {
		query string
		want  int64
	}{
		{"SELECT range AS n FROM range(3)", 3},
		{"(SELECT range AS n FROM range(4))", 4},
		{"WITH a AS (SELECT range AS n FROM range(5)) SELECT * FROM a", 5},
		{"VALUES (1), (2)", 2},
		{"FROM range(6)", 6},
		{"SELECT range AS n FROM range(7) -- a comment at the end", 7},
		{"SELECT range AS n FROM range(8) ORDER BY n DESC", 8},
	} {
		tag, err := conn.Exec(ctx, "CREATE TABLE "+tbl+" AS "+c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		if tag.String() != "CREATE TABLE" {
			t.Errorf("%s: tag %q", c.query, tag)
		}
		if n := count(t, conn, "SELECT count(*) FROM "+tbl); n != c.want {
			t.Errorf("%s: %d rows, want %d", c.query, n, c.want)
		}
		mustExec(t, conn, "DROP TABLE "+tbl)
	}

	mustExec(t, conn, "CREATE TABLE "+tbl+" AS SELECT range AS n FROM range(3)")
	if err := runQuery(conn, "CREATE TABLE "+tbl+" AS SELECT 1 AS n"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("CREATE TABLE of a table that exists: %v", err)
	}
	mustExec(t, conn, "CREATE TABLE IF NOT EXISTS "+tbl+" AS SELECT 1 AS n")
	mustExec(t, conn, "CREATE OR REPLACE TABLE "+tbl+" AS SELECT range AS n FROM range(9)")
	if n := count(t, conn, "SELECT count(*) FROM "+tbl); n != 9 {
		t.Errorf("after the replace: %d rows, want 9", n)
	}

	failing := "SELECT CASE WHEN range = 5 THEN error('boom') ELSE range END AS n FROM range(10)"
	if err := runQuery(conn, "CREATE TABLE "+tbl+"_failed AS "+failing); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("a query that fails: %v", err)
	}
	if err := runQuery(conn, "CREATE OR REPLACE TABLE "+tbl+" AS "+failing); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("a replace whose query fails: %v", err)
	}
	if n := count(t, conn, "SELECT count(*) FROM "+tbl); n != 9 {
		t.Errorf("after a failed replace: %d rows, want the 9 of before", n)
	}
	if n := count(t, conn, "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '"+tbl+"%'"); n != 1 {
		t.Errorf("%d tables named %s..., want only %s", n, tbl, tbl)
	}
}

// TestCreateTableAsDoesNotStopOtherSessions checks that CREATE TABLE ... AS
// of an Iceberg table holds the catalog lock only while it changes the
// catalog: while it reads a slow table of a connection, another session
// prepares and runs statements and lists the catalog. As one DuckDB
// statement it held the lock for as long as it read, and every other
// session waited.
func TestCreateTableAsDoesNotStopOtherSessions(t *testing.T) {
	t.Parallel()
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	ctx := context.Background()
	remote, err := pgx.Connect(ctx, pg.url)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close(ctx) //nolint:errcheck
	if _, err := remote.Exec(ctx, "CREATE VIEW "+schema+".slow AS SELECT pg_sleep(4)::text AS x, 1 AS n"); err != nil {
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
	env := newCatalogEnv(t, engine.BeforeLockdown(fed.Attach))
	writer := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	other := env.admin(t, pgx.QueryExecModeSimpleProtocol)

	for _, create := range []string{"CREATE TABLE ", "CREATE OR REPLACE TABLE "} {
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			_, err := writer.Exec(ctx, create+env.table+" AS SELECT n FROM crm."+schema+".slow")
			done <- err
		}()
		time.Sleep(time.Second)
		for _, q := range []string{"SELECT 42", "SELECT count(*) FROM information_schema.tables"} {
			qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var n int64
			err := other.QueryRow(qctx, q).Scan(&n)
			cancel()
			if err != nil {
				t.Errorf("%s while %s... reads: %v", q, create, err)
			}
		}
		if err := <-done; err != nil {
			t.Fatalf("%s: %v", create, err)
		}
		if took := time.Since(start); took < 4*time.Second {
			t.Errorf("%s took %s: the source was not slow", create, took)
		}
		if n := count(t, other, "SELECT count(*) FROM "+env.table); n != 1 {
			t.Errorf("%s: %d rows, want 1", create, n)
		}
	}
}
