package pgwire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// TestDropSchemaCascade drops a schema of the Iceberg catalog with what is in
// it: tables, whose files go as DROP TABLE's do, a view, a materialized view
// with its state and a dynamic table. A schema with a materialized view the
// user does not own is refused before anything goes; and dbt's way, IF EXISTS
// in a transaction block, works too.
func TestDropSchemaCascade(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	sched := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	purge := func(context.Context, string, []string, string, bool) clone.PurgeVerdict {
		return clone.PurgeVerdict{Purge: true, Release: func() {}}
	}
	r := vc.replica(t, func(o *Options) { o.Tasks, o.ClonePurge = sched, purge })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA lake.sales",
		"CREATE TABLE lake.sales.orders AS SELECT i AS id, i % 3 AS region FROM range(100) AS r(i)",
		"CREATE TABLE lake.sales.empty (id INTEGER)",
		"CREATE VIEW lake.sales.recent AS SELECT * FROM lake.sales.orders WHERE id > 50",
		"CREATE MATERIALIZED VIEW lake.sales.by_region AS SELECT region, count(*) AS n FROM lake.sales.orders GROUP BY region",
		"REFRESH MATERIALIZED VIEW lake.sales.by_region",
		"CREATE DYNAMIC TABLE lake.sales.big TARGET_LAG = '1 hour' WAREHOUSE = wh AS SELECT id FROM lake.sales.orders WHERE id > 90",
		"ALTER DYNAMIC TABLE lake.sales.big REFRESH",
		"CREATE SCHEMA lake.other",
		"CREATE TABLE lake.other.kept AS SELECT 1 AS id",
	} {
		mustExec(t, admin, q)
	}
	state := texts(t, admin, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'igloo_refresh'")
	if len(state) == 0 {
		t.Fatal("the materialized view keeps no state: the test shows nothing")
	}
	orders, err := r.objects.LoadTable(ctx, "sales", "orders")
	if err != nil {
		t.Fatal(err)
	}
	ordersData := filepath.Join(strings.TrimPrefix(orders.Location(), "file://"), "data")

	// Another user's materialized view: refused, and nothing goes.
	walt := r.dbt(t)
	mustExec(t, walt, "CREATE MATERIALIZED VIEW lake.sales.mine AS SELECT id FROM lake.sales.orders")
	if _, err := admin.Exec(ctx, "CREATE USER wanda WITH PASSWORD 'wanda-password' ROLE writer"); err != nil {
		t.Fatal(err)
	}
	wanda := connect(t, r.url("wanda", "wanda-password"), pgx.QueryExecModeSimpleProtocol)
	if _, err := wanda.Exec(ctx, "DROP SCHEMA lake.sales CASCADE"); pgCode(err) != "42501" || !strings.Contains(err.Error(), "belongs to") {
		t.Errorf("a schema with another's materialized view: %v", err)
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.sales.orders"); n != 100 {
		t.Errorf("after the refusal: %d rows", n)
	}

	mustExec(t, admin, "DROP SCHEMA lake.sales CASCADE")
	if n := count(t, admin, "SELECT count(*) FROM information_schema.schemata WHERE catalog_name = 'lake' AND schema_name = 'sales'"); n != 0 {
		t.Errorf("the schema is still there")
	}
	if n := count(t, admin, "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = 'sales'"); n != 0 {
		t.Errorf("%d tables or views are still there", n)
	}
	for _, kind := range []string{tasks.KindMaterializedView, tasks.KindDynamicTable} {
		jobs, err := sched.Store().Jobs(ctx, kind)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) != 0 {
			t.Errorf("%ss left: %v", kind, jobs)
		}
	}
	if left := texts(t, admin, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'igloo_refresh'"); len(left) != 0 {
		t.Errorf("state left: %q", left)
	}
	if files, _ := filepath.Glob(filepath.Join(ordersData, "*.parquet")); len(files) != 0 {
		t.Errorf("%d files of a dropped table left", len(files))
	}
	if _, err := os.Stat(ordersData); err == nil {
		if entries, _ := os.ReadDir(ordersData); len(entries) != 0 {
			t.Errorf("the dropped table's data directory holds %d entries", len(entries))
		}
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.other.kept"); n != 1 {
		t.Errorf("another schema's table: %d rows", n)
	}

	// dbt drops a schema in a transaction block, with IF EXISTS.
	mustExec(t, admin, "CREATE SCHEMA lake.dbt_tmp")
	mustExec(t, admin, "CREATE TABLE lake.dbt_tmp.model AS SELECT 1 AS id")
	mustExec(t, admin, "BEGIN")
	mustExec(t, admin, `drop schema if exists "lake"."dbt_tmp" cascade`)
	mustExec(t, admin, "COMMIT")
	if n := count(t, admin, "SELECT count(*) FROM information_schema.schemata WHERE catalog_name = 'lake' AND schema_name = 'dbt_tmp'"); n != 0 {
		t.Errorf("dbt's schema is still there")
	}
	mustExec(t, admin, "DROP SCHEMA IF EXISTS lake.never_made CASCADE")
	if _, err := admin.Exec(ctx, "DROP SCHEMA lake.never_made CASCADE"); err == nil {
		t.Error("DROP SCHEMA of a schema that is not there went through")
	}
}
