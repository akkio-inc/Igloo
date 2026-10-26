package pgwire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestAggregatePasses checks that CREATE TABLE ... AS of an aggregate whose
// list, string and DISTINCT aggregates would not fit in memory fills the
// table in passes over its groups, with the rows one run gives, and that
// statements whose rows do not each come from one group run as one.
func TestAggregatePasses(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		// So little that every such aggregate here runs in passes.
		o.QueryMemory = 1 << 20
		o.Metrics = metrics
	})
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, `CREATE TABLE lake.public.events AS SELECT DATE '2025-01-01' + CAST(i % 10 AS INTEGER) AS d,
		'u' || (i % 5003) AS uid, CASE WHEN i % 13 = 0 THEN NULL ELSE 'n' || (i % 7) END AS net, 't' || (i % 31) AS title
		FROM range(200000) r(i)`)
	passes := func() float64 { return testutil.ToFloat64(metrics.aggPasses) }
	// want is what one run of the aggregate gives, in a form that compares.
	const want = `SELECT d, uid, list_sort(list(DISTINCT net)), list_sort(list(DISTINCT title)) FROM lake.public.events GROUP BY d, uid`
	same := func(table string) {
		t.Helper()
		if n := count(t, admin, "SELECT count(*) FROM "+table); n != 50030 {
			t.Errorf("%s: %d rows, want 50030", table, n)
		}
		got := "SELECT d, uid, list_sort(nets), list_sort(string_split(nullif(titles, ''), '|')) FROM " + table
		for _, q := range []string{got + " EXCEPT ALL " + want, want + " EXCEPT ALL " + got} {
			if n := count(t, admin, "SELECT count(*) FROM ("+q+")"); n != 0 {
				t.Errorf("%s: %d rows differ from one run's", table, n)
			}
		}
	}

	before := passes()
	mustExec(t, admin, `CREATE TABLE lake.public.summary AS
		WITH s AS (SELECT e.d AS d, e.uid AS uid, list(DISTINCT e.net) AS nets, coalesce(string_agg(DISTINCT e.title, '|'), '') AS titles
			FROM lake.public.events AS e WHERE e.d >= DATE '2025-01-01' GROUP BY e.d, e.uid)
		SELECT s.d, s.uid, s.nets, s.titles FROM s AS s WHERE s.uid IS NOT NULL`)
	if passes() <= before {
		t.Error("the aggregate ran in one")
	}
	same("lake.public.summary")

	// Groups by position, through a derived table.
	before = passes()
	mustExec(t, admin, `CREATE TABLE lake.public.by_position AS SELECT * FROM (
		SELECT d, uid, list(DISTINCT net) AS nets, string_agg(DISTINCT title, '|') AS titles FROM lake.public.events GROUP BY 1, 2) AS x`)
	if passes() <= before {
		t.Error("the aggregate grouped by position ran in one")
	}
	same("lake.public.by_position")

	// Snowflake's dialect, as dbt's models write it; ARRAY_UNIQUE_AGG leaves
	// out NULLs.
	sf := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	before = passes()
	// ARRAY_FLATTEN's translation calls error() for what is not an array of
	// arrays.
	mustExec(t, sf, `create or replace table lake.public.sf_summary as (
		with detail as (select *, split(title, 't') as parts from lake.public.events),
		summary as (select ji.d as d, ji.uid as uid, array_unique_agg(ji.net) as nets, listagg(distinct ji.title, '|') as titles,
			array_unique_agg(ji.parts) as parts from detail as ji group by ji.d, ji.uid)
		select su.d, su.uid, su.nets, su.titles, array_distinct(array_flatten(su.parts)) as parts from summary as su)`)
	if passes() <= before {
		t.Error("the Snowflake model ran in one")
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.public.sf_summary"); n != 50030 {
		t.Errorf("sf_summary: %d rows, want 50030", n)
	}
	if n := count(t, admin, `SELECT count(*) FROM lake.public.sf_summary s JOIN (`+want+`) w(d, uid, nets, titles) USING (d, uid)
		WHERE json_array_length(s.nets) <> len(list_filter(w.nets, x -> x IS NOT NULL)) OR len(string_split(s.titles, '|')) <> len(w.titles)`); n != 0 {
		t.Errorf("sf_summary: %d rows differ from one run's", n)
	}

	// Rows that come from more than one group, or from elsewhere too, run as
	// one, and right.
	before = passes()
	mustExec(t, admin, `CREATE TABLE lake.public.counted AS WITH s AS (
		SELECT d, uid, list(DISTINCT net) AS nets FROM lake.public.events GROUP BY d, uid) SELECT count(*) AS n FROM s`)
	mustExec(t, admin, `CREATE TABLE lake.public.unioned AS
		SELECT d, uid, list(DISTINCT net) AS nets FROM lake.public.events GROUP BY d, uid
		UNION ALL SELECT DATE '2000-01-01', 'x', []`)
	// now() would differ from one pass to the next.
	mustExec(t, admin, `CREATE TABLE lake.public.stamped AS
		SELECT d, uid, list(DISTINCT net) AS nets, now() AS at FROM lake.public.events GROUP BY d, uid`)
	if passes() != before {
		t.Error("an aggregate whose rows do not each come from one group, or that calls now(), ran in passes")
	}
	if n := count(t, admin, "SELECT n FROM lake.public.counted"); n != 50030 {
		t.Errorf("counted: %d", n)
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.public.unioned"); n != 50031 {
		t.Errorf("unioned: %d rows", n)
	}
}

// TestAggregatePassesSplit checks that a pass that runs out of memory runs
// again as the two passes of twice as many that its groups hash to, as do
// the passes after it, while those done stay done, and that the passes stop
// splitting at maxAggPasses.
func TestAggregatePassesSplit(t *testing.T) {
	t.Parallel()
	s := &Server{log: slog.New(slog.DiscardHandler), metrics: NewMetrics(prometheus.NewRegistry())}
	ap := &aggPasses{passes: 2, query: func(pass, n int) (string, error) { return fmt.Sprintf("%d/%d", pass, n), nil }}
	oom := errors.New("Out of Memory Error: could not allocate block of size 256.0 KiB")
	run := func(tooBig func(pass string) bool) ([]string, int64, error) {
		var ran []string
		fill := func(q string) (int64, error) {
			pass := strings.TrimPrefix(q, `INSERT INTO "t"`+"\n")
			if tooBig(pass) {
				ran = append(ran, pass+" out of memory")
				return 0, oom
			}
			ran = append(ran, pass)
			return 1, nil
		}
		rows, err := s.fillInPasses(context.Background(), fill, []string{"t"}, ap)
		return ran, rows, err
	}

	ran, rows, err := run(func(pass string) bool { return pass == "1/2" || pass == "1/4" })
	if want := []string{"0/2", "1/2 out of memory", "1/4 out of memory", "1/8", "5/8", "3/8", "7/8"}; err != nil || rows != 5 || !slices.Equal(ran, want) {
		t.Errorf("ran %q, %d rows, %v; want %q, 5 rows", ran, rows, err, want)
	}

	ran, _, err = run(func(pass string) bool { return strings.HasPrefix(pass, "0/") })
	if !errors.Is(err, oom) || ran[len(ran)-1] != fmt.Sprintf("0/%d out of memory", maxAggPasses) {
		t.Errorf("ran %q, %v; want the last pass 0 of %d to run out of memory", ran, err, maxAggPasses)
	}
}
