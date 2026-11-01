package pgwire

import (
	"context"
	"io"
	"log/slog"
	"slices"
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
// EXISTS, is left alone, a query that fails part way leaves nothing
// behind, and a table partitioned by bucket, as Snowflake's CLUSTER BY
// (BUCKET(n, column)) makes one, gets its partition spec.
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

	mustExec(t, conn, "CREATE OR REPLACE TABLE "+tbl+" PARTITIONED BY (bucket(4, n)) AS SELECT range AS n FROM range(100)")
	if n := count(t, conn, "SELECT count(*) FROM "+tbl); n != 100 {
		t.Errorf("bucketed: %d rows, want 100", n)
	}
	if env.fake != nil {
		if spec := env.fake.Metadata("public", tbl).PartitionSpec(); !strings.Contains(spec.String(), "bucket[4]") {
			t.Errorf("bucketed: partition spec %s", spec.String())
		}
	}
	mustExec(t, conn, "CREATE OR REPLACE TABLE "+tbl+" AS SELECT range AS n FROM range(9)")

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

	// Temporary tables, in steps too (see tempCreate), as DuckDB makes them.
	mustExec(t, conn, "CREATE TEMP TABLE steps_tmp AS SELECT range AS n FROM range(5)")
	if n := count(t, conn, "SELECT count(*) FROM steps_tmp"); n != 5 {
		t.Errorf("a temporary table: %d rows, want 5", n)
	}
	if err := runQuery(conn, "CREATE TEMP TABLE steps_tmp AS SELECT 1 AS n"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("CREATE TEMP TABLE of a table that exists: %v", err)
	}
	mustExec(t, conn, "CREATE OR REPLACE TEMP TABLE steps_tmp AS SELECT range AS n, 'x' AS s FROM range(7)")
	if got := texts(t, conn, "SELECT count(*), min(s) FROM steps_tmp"); len(got) != 1 || got[0] != "7|x" {
		t.Errorf("a replaced temporary table: %v", got)
	}
	if err := runQuery(conn, "CREATE OR REPLACE TEMP TABLE steps_tmp AS "+failing); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("a temporary replace whose query fails: %v", err)
	}
	if err := runQuery(conn, "CREATE TEMP TABLE steps_failed AS "+failing); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("a temporary table whose query fails: %v", err)
	}
	if got := texts(t, conn, "SELECT table_name FROM duckdb_tables() WHERE temporary ORDER BY 1"); !slices.Equal(got, []string{"steps_tmp"}) {
		t.Errorf("temporary tables %v, want only steps_tmp", got)
	}
	if n := count(t, conn, "SELECT count(*) FROM steps_tmp"); n != 7 {
		t.Errorf("after a failed temporary replace: %d rows, want the 7 of before", n)
	}
}

// TestCreateTableAsDoesNotStopOtherSessions checks that CREATE TABLE ... AS
// holds the catalog lock only while it changes the catalog: while it reads
// a slow table of a connection, another session prepares and runs
// statements, lists the catalog and signs in. As one DuckDB statement it
// held the lock for as long as it read, and every other session waited: of
// an Iceberg table, in a transaction block too, and of a temporary table,
// as dbt makes one for each incremental model, and of a table partitioned
// by bucket, which ran as one statement for 35 minutes on a large table.
// So did EXECUTE of a query prepared in SQL.
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
	if _, err := remote.Exec(ctx, "CREATE VIEW "+schema+".slow AS SELECT pg_sleep(6)::text AS x, 1 AS n"); err != nil {
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

	slow := "SELECT n FROM crm." + schema + ".slow"
	for _, c := range []struct {
		name, setup, stmt, after, rows string
	}{
		{"CREATE TABLE ... AS", "", "CREATE TABLE " + env.table + " AS " + slow, "", env.table},
		{"CREATE OR REPLACE TABLE ... AS", "", "CREATE OR REPLACE TABLE " + env.table + " AS " + slow, "", env.table},
		{"a bucketed one", "", "CREATE OR REPLACE TABLE " + env.table + " PARTITIONED BY (bucket(4, n)) AS " + slow, "", env.table},
		{"one in a transaction block", "BEGIN", "CREATE OR REPLACE TABLE " + env.table + " AS " + slow, "COMMIT", env.table},
		{"a temporary one", "", "CREATE TEMP TABLE slow_copy AS " + slow, "", "slow_copy"},
		{"a temporary one replaced", "", "CREATE OR REPLACE TEMP TABLE slow_copy AS " + slow, "", "slow_copy"},
		{"a temporary one in a block, as dbt makes them", "BEGIN", "CREATE TEMPORARY TABLE slow_tmp AS (" + slow + ")", "COMMIT", "slow_tmp"},
		{"EXECUTE of a query prepared in SQL", "PREPARE slow_read AS " + slow, "EXECUTE slow_read", "DEALLOCATE slow_read", ""},
	} {
		if c.setup != "" {
			mustExec(t, writer, c.setup)
		}
		start := time.Now()
		ended := alongside(t, other, env.url("igloo", adminPassword), func() error {
			_, err := writer.Exec(ctx, c.stmt)
			return err
		}, c.name)
		if took := ended.Sub(start); took < 6*time.Second {
			t.Errorf("%s took %s: the source was not slow", c.name, took)
		}
		if c.after != "" {
			mustExec(t, writer, c.after)
		}
		if c.rows != "" {
			if n := count(t, writer, "SELECT count(*) FROM "+c.rows); n != 1 {
				t.Errorf("%s: %d rows, want 1", c.name, n)
			}
		}
	}
}

// alongside runs run, a statement that reads a slow source, and a second
// in: on other, a statement and a listing of the catalog, and a login at
// url. Each must end before run does: had run held the catalog lock, they
// would have waited for it. It returns when run ended, and fails the test
// if run failed.
func alongside(t *testing.T, other *pgx.Conn, url string, run func() error, what string) time.Time {
	t.Helper()
	ctx := context.Background()
	type result struct {
		err error
		at  time.Time
	}
	done := make(chan result, 1)
	go func() {
		err := run()
		done <- result{err, time.Now()}
	}()
	time.Sleep(time.Second)
	var names []string
	var ends []time.Time
	for _, q := range []string{"SELECT 42", "SELECT count(*) FROM information_schema.tables"} {
		qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var n int64
		if err := other.QueryRow(qctx, q).Scan(&n); err != nil {
			t.Errorf("%s while %s reads: %v", q, what, err)
		}
		cancel()
		names, ends = append(names, q), append(ends, time.Now())
	}
	lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	login, err := pgx.Connect(lctx, url)
	cancel()
	if err != nil {
		t.Errorf("signing in while %s reads: %v", what, err)
	} else {
		_ = login.Close(ctx)
	}
	names, ends = append(names, "signing in"), append(ends, time.Now())
	r := <-done
	if r.err != nil {
		t.Fatalf("%s: %v", what, r.err)
	}
	for i, end := range ends {
		if !end.Before(r.at) {
			t.Errorf("%s while %s reads waited for it to end", names[i], what)
		}
	}
	return r.at
}
