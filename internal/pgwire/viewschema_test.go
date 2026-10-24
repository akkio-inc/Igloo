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
	t.Parallel()
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

// TestUseKeepsViewsOnPath checks that a view resolves by its bare name
// after USE makes its schema the current one, as after SET search_path:
// Snowflake sessions name their schema with USE SCHEMA and then the views
// in it without one. In DuckDB's dialect and Snowflake's, for an admin and
// a writer; and a USE of another schema leaves the view behind.
func TestUseKeepsViewsOnPath(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t)
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		`create schema if not exists lake.sales`,
		`create table lake.sales.orders as select 1 as id`,
		`create view lake.sales.v as select id + 1 as id from orders`,
	} {
		mustExec(t, admin, q)
	}
	for _, who := range []struct {
		name string
		conn *pgx.Conn
	}{{"admin", admin}, {"writer", r.dbt(t)}} {
		for _, use := range [][]string{
			{"use lake.sales"},
			{"use sales"},
			{"set sql_dialect = 'snowflake'", "use schema sales", "reset sql_dialect"},
			{"set sql_dialect = 'snowflake'", "use schema lake.sales", "reset sql_dialect"},
		} {
			mustExec(t, who.conn, "use lake.public")
			for _, q := range use {
				mustExec(t, who.conn, q)
			}
			if got := count(t, who.conn, "SELECT id FROM v"); got != 2 {
				t.Errorf("%s after %q: id %d, want 2", who.name, use, got)
			}
			if got := count(t, who.conn, "SELECT id FROM orders"); got != 1 {
				t.Errorf("%s after %q: table id %d, want 1", who.name, use, got)
			}
		}
		mustExec(t, who.conn, "use lake.public")
		if err := runQuery(who.conn, "SELECT id FROM v"); err == nil {
			t.Errorf("%s: sales's view resolved by its bare name after use lake.public", who.name)
		}
	}
	// As with SET search_path, only admins may make the local copies'
	// database the current one.
	walt := r.dbt(t)
	if err := runQuery(walt, "use "+r.eng.LocalDatabase()+".sales"); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Errorf("a writer's USE of the local database: %v", err)
	}
}
