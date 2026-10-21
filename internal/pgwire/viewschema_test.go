package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// TestViewNamesResolveInTheViewsSchema checks that the tables and views a
// view names without a schema are those of the view's own schema, as in
// Snowflake and as the view's Iceberg default namespace says, wherever the
// view is loaded: on the replica that made it and on another, whatever the
// creating session's search path was. A CTE named like a table stays the
// CTE; a view stored with bare names before Igloo qualified them loads too;
// and a view whose local copy cannot be made fails with why when read, and
// loads once it can.
func TestViewNamesResolveInTheViewsSchema(t *testing.T) {
	ctx := context.Background()
	vc := newViewCatalog(t)
	a, b := vc.replica(t), vc.replica(t)
	w := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		`create schema if not exists lake.sales`,
		`create table lake.sales.orders as select 1 as id`,
		`create table lake.public.orders as select 2 as id`,
		// From the default schema: orders is still sales's, the view's.
		`create view lake.sales.v as select id from orders`,
		// From a session in sales, as a migration from Snowflake makes them.
		`use lake.sales`,
		`create view lake.sales.w as select id from orders`,
		`create view lake.sales.vw as select id from w`,
		`create view lake.sales.c as with orders as (select 3 as id) select id from orders`,
		`use lake.public`,
	} {
		mustExec(t, w, q)
	}

	// Views stored with bare names, as Igloo stored them before, or by
	// another engine; one reads a table that does not exist yet.
	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := []catalog.Column{{Name: "id", Type: "INTEGER"}}
	for _, v := range []catalog.View{
		{Schema: "sales", Name: "old", SQL: "select id from orders", Columns: id},
		{Schema: "sales", Name: "broken", SQL: "select id from missing_t", Columns: id},
	} {
		if err := cat.PutView(ctx, v, false); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := b.objects.SyncViews(ctx); err == nil || !strings.Contains(err.Error(), "missing_t") {
		t.Errorf("syncing a view of a missing table: %v", err)
	}
	if _, err := a.objects.SyncViews(ctx); err == nil {
		t.Error("syncing a view of a missing table succeeded")
	}
	want := map[string]int64{"v": 1, "w": 1, "vw": 1, "c": 3, "old": 1}
	for _, r := range []*viewReplica{a, b} {
		conn := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		for view, n := range want {
			if got := count(t, conn, "SELECT id FROM lake.sales."+view); got != n {
				t.Errorf("view %s: id %d, want %d", view, got, n)
			}
		}
		err := runQuery(conn, "SELECT id FROM lake.sales.broken")
		if err == nil || !strings.Contains(err.Error(), "could not be loaded") || !strings.Contains(err.Error(), "missing_t") {
			t.Errorf("reading a view that could not be loaded: %v", err)
		}
	}

	mustExec(t, w, `create table lake.sales.missing_t as select 5 as id`)
	if _, err := b.objects.SyncViews(ctx); err != nil {
		t.Fatal(err)
	}
	if got := count(t, b.admin(t, pgx.QueryExecModeSimpleProtocol), "SELECT id FROM lake.sales.broken"); got != 5 {
		t.Errorf("the view once its table exists: id %d, want 5", got)
	}
	mustExec(t, w, `drop view lake.sales.vw`)
	if _, err := b.objects.SyncViews(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runQuery(b.admin(t, pgx.QueryExecModeSimpleProtocol), "SELECT id FROM lake.sales.vw"); err == nil {
		t.Error("a dropped view still reads")
	}
}
