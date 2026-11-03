package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
)

// startFillServer starts a server whose tables auto mode weighs as 300 MB,
// on which statements past a second are worth workers, of which it has
// two of 8 cores each: an aggregate over its tables runs on them.
func startFillServer(t *testing.T) (*distServer, *pgx.Conn) {
	t.Helper()
	ds := startDistServerCores(t, t.TempDir(), 2, 8, "", func(o *distributed.Options) { o.Threshold = time.Second })
	ds.source.size = 300 << 20
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	return ds, conn
}

// local returns query's rows as this replica alone computes them.
func local(t *testing.T, conn *pgx.Conn, query string) string {
	t.Helper()
	mustExec(t, conn, "SET distributed_execution = off")
	defer mustExec(t, conn, "RESET distributed_execution")
	return strings.Join(texts(t, conn, query), "\n")
}

// TestFillOnWorkers checks that a fill of a table from an aggregate runs
// the aggregate on workers under auto mode, as a read of it would, and
// writes the rows this replica computes alone: CREATE TABLE ... AS,
// CREATE OR REPLACE, dbt's CREATE TABLE ... AS (query) and INSERT INTO ...
// SELECT; and that a copy, whose rows are as many as it reads and which
// this replica writes either way, stays here.
func TestFillOnWorkers(t *testing.T) {
	t.Parallel()
	ds, conn := startFillServer(t)
	ctx := context.Background()
	aggregate := "SELECT amount, count(*) AS n, sum(id) AS s FROM orders GROUP BY amount"
	for _, c := range []struct{ name, fill, table, query string }{
		{"create", "CREATE TABLE fill_agg AS " + aggregate, "fill_agg", aggregate},
		{"replace", "CREATE OR REPLACE TABLE fill_agg AS SELECT amount, max(id) AS m FROM orders GROUP BY amount", "fill_agg",
			"SELECT amount, max(id) AS m FROM orders GROUP BY amount"},
		{"dbt", `create table "lake"."public"."fill_dbt__dbt_tmp" as (select amount, min(id) as m from orders group by amount)`,
			"fill_dbt__dbt_tmp", "select amount, min(id) as m from orders group by amount"},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := local(t, conn, c.query+" ORDER BY ALL")
			if _, err := conn.Exec(ctx, c.fill); err != nil {
				t.Fatal(err)
			}
			if q := lastRun(t, ds.history, c.fill); q.Workers == 0 {
				t.Errorf("ran here: %q", q.Placement)
			}
			if got := local(t, conn, "SELECT * FROM "+c.table+" ORDER BY ALL"); got != want {
				t.Errorf("filled:\n%s\nwant:\n%s", got, want)
			}
		})
	}
	insert := "INSERT INTO fill_agg SELECT amount + 10, max(id) FROM orders GROUP BY amount"
	tag, err := conn.Exec(ctx, insert)
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT 0 " + local(t, conn, "SELECT count(DISTINCT amount) FROM orders"); tag.String() != want {
		t.Errorf("insert's tag %q, want %q", tag, want)
	}
	if q := lastRun(t, ds.history, insert); q.Workers == 0 {
		t.Errorf("insert ran here: %q", q.Placement)
	}
	if got := local(t, conn, "SELECT count(*), sum(m) FROM fill_agg"); got != local(t, conn,
		"SELECT 2 * count(*), 2 * sum(m) FROM (SELECT amount, max(id) AS m FROM orders GROUP BY amount)") {
		t.Errorf("after the insert: %s", got)
	}

	// A materialized view's full refresh: a column renamed in FROM, it is
	// refreshed in full.
	mv := "SELECT amount, count(*) AS n FROM lake.public.orders AS o(i) GROUP BY amount"
	mustExec(t, conn, "CREATE MATERIALIZED VIEW fill_mv AS "+mv)
	mustExec(t, conn, "REFRESH MATERIALIZED VIEW fill_mv")
	if action, detail, _, _ := viewRefresh(t, conn, "public.fill_mv"); !strings.Contains(detail, "its query ran on") {
		t.Errorf("the view's refresh: %s: %s", action, detail)
	}
	if got, want := local(t, conn, "SELECT * FROM fill_mv ORDER BY ALL"), local(t, conn, mv+" ORDER BY ALL"); got != want {
		t.Errorf("materialized view:\n%s\nwant:\n%s", got, want)
	}

	// A copy: as many rows as it reads, which this replica writes wherever
	// it runs, and then from the workers' files.
	copyFill := "CREATE TABLE fill_copy AS SELECT * FROM orders"
	if _, err := conn.Exec(ctx, copyFill); err != nil {
		t.Fatal(err)
	}
	if q := lastRun(t, ds.history, copyFill); q.Workers != 0 {
		t.Errorf("the copy ran on %d workers: %q", q.Workers, q.Placement)
	}
	if got, want := local(t, conn, "SELECT count(*), sum(id) FROM fill_copy"), local(t, conn, "SELECT count(*), sum(id) FROM orders"); got != want {
		t.Errorf("copy: %s, want %s", got, want)
	}
}

// TestFillPolicies checks that a fill from a table masking or row access
// policies apply to runs on workers as reads of it do, through the
// policies, and holds what its user may see: a row copied as it is, which
// tasks write themselves, and an aggregate the coordinator writes.
func TestFillPolicies(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	for _, sql := range policyFixture {
		mustExec(t, ds.admin, sql)
	}
	mustExec(t, ds.admin, `INSERT INTO lake.crm.contacts SELECT i, CASE WHEN i % 2 = 0 THEN 'acme' ELSE 'globex' END,
		'user' || i || '@acme.com', 'dev-' || i, i FROM range(5, 3000) t(i)`)
	rita := ds.user(t, "rita", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ds.admin, "GRANT acme_team TO rita")
	for _, conn := range []*pgx.Conn{ds.admin, rita} {
		mustExec(t, conn, "SET distributed_execution = on")
	}
	ctx := context.Background()
	for _, fill := range []struct {
		conn       *pgx.Conn
		user, text string
	}{
		{ds.admin, "igloo", "CREATE TABLE lake.public.fill_contacts AS SELECT client, count(*) AS n, max(email) AS e FROM lake.crm.contacts GROUP BY client"},
		{ds.admin, "igloo", "INSERT INTO lake.public.fill_contacts SELECT client, count(*), min(email) FROM lake.crm.contacts GROUP BY client"},
		{rita, "rita", "CREATE TABLE lake.public.fill_rita AS SELECT id, client, email FROM lake.crm.contacts"},
		{rita, "rita", "INSERT INTO lake.public.fill_contacts SELECT client, count(*), max(email) FROM lake.crm.contacts GROUP BY client"},
	} {
		if _, err := fill.conn.Exec(ctx, fill.text); err != nil {
			t.Fatal(err)
		}
		if q := lastRun(t, ds.history, fill.text); q.Workers == 0 || len(q.Policies) == 0 {
			t.Errorf("%s: ran on %d workers, through %q: %q", fill.text, q.Workers, q.Policies, q.Placement)
		}
	}
	for _, row := range texts(t, ds.admin, "SELECT e FROM lake.public.fill_contacts UNION ALL SELECT email FROM lake.public.fill_rita") {
		if row != "***" {
			t.Errorf("a raw value was written: %s", row)
		}
	}
	// acme's rows alone: ids 1 and 2, and the even ids from 6 to 2998.
	if got := texts(t, ds.admin, "SELECT client, count(*) FROM lake.public.fill_rita GROUP BY 1"); !slices.Equal(got, []string{"acme|1499"}) {
		t.Errorf("rita's copy holds %q", got)
	}
	if got := texts(t, ds.admin, "SELECT n FROM lake.public.fill_contacts WHERE client = 'globex'"); !slices.Equal(got, []string{"1500", "1500"}) {
		t.Errorf("globex's counts: %q, want the admin's two alone", got)
	}
	// A fill from a table without policies, forced, runs on workers.
	plain := "CREATE TABLE lake.public.fill_orders AS SELECT amount, count(*) AS n FROM lake.public.orders GROUP BY amount"
	if _, err := ds.admin.Exec(ctx, plain); err != nil {
		t.Fatal(err)
	}
	if q := lastRun(t, ds.history, plain); q.Workers == 0 {
		t.Errorf("a fill without policies ran here: %q", q.Placement)
	}
}

// TestFillAfterLocal checks a fill auto mode runs here: once the guard
// fires, and after it runs out of memory here, its query runs on workers,
// into the table its run here left empty, and the table holds the rows
// this replica computes alone.
func TestFillAfterLocal(t *testing.T) {
	ds := startDistServer(t, 2)
	// Estimated at about 45 s here, under the minute workers are worth.
	ds.source.size = 300 << 20
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	defer func(f, a float64, m time.Duration) { guardFactor, guardAlternative, guardMinimum = f, a, m }(guardFactor, guardAlternative, guardMinimum)
	guardFactor, guardAlternative, guardMinimum = 0, 0, 300*time.Millisecond
	ctx := context.Background()
	for _, c := range []struct {
		name, table, query, placement string
		fail                          func(ctx context.Context) error
	}{
		{"guard", "fill_guarded", "SELECT amount, count(*) AS n FROM orders GROUP BY amount", "moved by the guard",
			func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
		{"out of memory", "fill_oom", "SELECT amount, sum(id) AS s FROM orders GROUP BY amount", "moved after it ran out of memory",
			func(context.Context) error {
				return &duckdb.Error{Type: duckdb.ErrorTypeOutOfMemory, Msg: "Out of Memory Error: failed to pin block of size 256.0 KiB"}
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := local(t, conn, c.query+" ORDER BY ALL")
			ds.srv.failFill = func(ctx context.Context, query string) error {
				if strings.Contains(query, c.query) {
					return c.fail(ctx)
				}
				return nil
			}
			defer func() { ds.srv.failFill = nil }()
			fill := "CREATE TABLE " + c.table + " AS " + c.query
			if _, err := conn.Exec(ctx, fill); err != nil {
				t.Fatal(err)
			}
			if q := lastRun(t, ds.history, fill); q.Workers == 0 || !strings.Contains(q.Placement, c.placement) {
				t.Errorf("%d workers, placement %q", q.Workers, q.Placement)
			}
			if got := local(t, conn, "SELECT * FROM "+c.table+" ORDER BY ALL"); got != want {
				t.Errorf("filled:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}
