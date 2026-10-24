package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/tasks"
)

// A task that runs CREATE OR REPLACE TABLE ... AS records the rows it
// wrote, as a materialized view's refresh does: on an Iceberg table, which
// Igloo replaces itself (see replace.go), as on any other.
func TestTaskCreateTableAsCountsRows(t *testing.T) {
	t.Parallel()
	env := newCatalogEnv(t)
	ctx := context.Background()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	run := func(stmt string) int64 {
		t.Helper()
		n, err := env.srv.RunJob(ctx, tasks.Job{Kind: tasks.KindTask, Name: "build", Owner: "igloo", Statement: stmt})
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		return n
	}
	for _, c := range []struct {
		stmt string
		want int64
	}{
		{"CREATE TABLE " + env.table + " AS SELECT range AS n FROM range(7)", 7},
		{"CREATE OR REPLACE TABLE " + env.table + " AS SELECT range AS n FROM range(12)", 12},
		{"CREATE OR REPLACE TABLE " + env.table + " AS SELECT range AS n FROM range(0)", 0},
		{"INSERT INTO " + env.table + " SELECT range FROM range(4)", 4},
	} {
		if got := run(c.stmt); got != c.want {
			t.Errorf("%s: %d rows, want %d", c.stmt, got, c.want)
		}
	}
	if n := count(t, conn, "SELECT count(*) FROM "+env.table); n != 4 {
		t.Errorf("the table has %d rows, want 4", n)
	}
}

// createsTableAs tells CREATE TABLE ... AS, whose rows the history counts,
// from the other CREATE statements.
func TestCreatesTableAs(t *testing.T) {
	t.Parallel()
	for query, want := range map[string]bool{
		"CREATE TABLE t AS SELECT 1":                                 true,
		"create or replace table lake.s.t as select 1":               true,
		"CREATE TEMP TABLE t AS SELECT 1":                            true,
		"CREATE TABLE IF NOT EXISTS \"My t\" AS (SELECT 1)":          true,
		"/* dbt */ CREATE TABLE t\nAS\nWITH x AS (SELECT 1) TABLE x": true,
		"CREATE TABLE t (a INTEGER)":                                 false,
		"CREATE VIEW v AS SELECT 1":                                  false,
		"CREATE MATERIALIZED VIEW v AS SELECT 1":                     false,
		"CREATE TASK t SCHEDULE '1 hour' AS SELECT 1":                false,
		"INSERT INTO t SELECT 1":                                     false,
	} {
		if got := createsTableAs(query); got != want {
			t.Errorf("createsTableAs(%q) = %v, want %v", query, got, want)
		}
	}
}
