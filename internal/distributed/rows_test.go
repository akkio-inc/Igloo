package distributed

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestSelectiveRowsAndSubqueries checks shapes that ran on one machine for
// a reason that need not hold: a scan that keeps few of the divided
// table's rows (a column equal to a constant, in a list, or equal to a
// subquery's value), as the statement or as a subquery or common table
// expression the coordinator reads; and scalar subqueries that stand on
// their own in an aggregation's select list, HAVING and ORDER BY, which
// the coordinator computes over the combined groups. They run on workers
// with one machine's rows; a scan that keeps rows no estimate says are
// few, and a correlated subquery, still run on one machine.
func TestSelectiveRowsAndSubqueries(t *testing.T) {
	h := newHarness(t, 0.02, 4, 3)
	for _, tc := range []struct {
		query   string
		workers bool
	}{
		{`SELECT l_orderkey, l_linenumber, l_comment FROM lineitem WHERE l_suppkey = 7`, true},
		{`SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_partkey IN (11, 12, 13) AND l_quantity > 5 ORDER BY 1, 2`, true},
		{`SELECT o_orderkey, o_totalprice FROM orders WHERE o_custkey = (SELECT min(c_custkey) FROM customer WHERE c_nationkey = 3)`, true},
		// A subquery the coordinator reads, under a part workers cannot run.
		{`SELECT x.l_orderkey, count(*) OVER () AS n FROM (SELECT l_orderkey FROM lineitem WHERE l_suppkey = 3) AS x ORDER BY 1`, true},
		{`WITH s AS (SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_partkey IN (1, 2, 3))
			SELECT l_orderkey, l_extendedprice, row_number() OVER (ORDER BY l_extendedprice DESC, l_orderkey) AS r FROM s ORDER BY r`, true},
		// Scalar subqueries over the combined groups.
		{`SELECT l_returnflag, count(*) * 1.0 / (SELECT count(*) FROM orders) AS per_order FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT l_shipmode, sum(l_quantity) AS q FROM lineitem GROUP BY 1 HAVING sum(l_quantity) > (SELECT avg(o_totalprice) / 200 FROM orders) ORDER BY 1`, true},
		{`SELECT l_suppkey % 10 AS b, count(*) AS n FROM lineitem GROUP BY 1 ORDER BY abs(count(*) - (SELECT count(*) / 10 FROM lineitem)), 1`, true},
		// A subquery the coordinator reads every row of, for a window over
		// all of them, whose filters or columns keep less than the table.
		{`SELECT x.l_orderkey, x.l_comment, count(*) OVER () AS n FROM (SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity > 45) AS x ORDER BY 1, 2`, true},
		{`SELECT x.l_orderkey, x.l_shipmode, rank() OVER (ORDER BY x.l_orderkey) AS r FROM (SELECT l_orderkey, l_shipmode FROM lineitem) AS x ORDER BY 3, 2 LIMIT 30`, true},
		// What still runs on one machine: a subquery of every row and column.
		{`SELECT x.l_orderkey, count(*) OVER () AS n FROM (SELECT * FROM lineitem) AS x ORDER BY 1 LIMIT 3`, false},
		{`SELECT l_orderkey, l_comment FROM lineitem WHERE l_comment LIKE '%furious%'`, false},
		{`SELECT l_returnflag, count(*) FROM lineitem l GROUP BY 1 HAVING count(*) > (SELECT count(*) FROM orders WHERE o_orderstatus = l_returnflag) ORDER BY 1`, false},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if got := h.check(tc.query); got != tc.workers {
				t.Errorf("ran distributed: %v, want %v", got, tc.workers)
			}
		})
	}
	// The coordinator keeps the subquery, which it computes once.
	h.t = t
	q := `SELECT l_returnflag, count(*) * 1.0 / (SELECT count(*) FROM orders) AS per_order FROM lineitem GROUP BY 1 ORDER BY 1`
	if final, _ := h.co.parser.print(h.planOf(q).final); !strings.Contains(final, "FROM orders") {
		t.Errorf("the coordinator does not compute the subquery: %s", final)
	}
	// But not one that reads more than a statement worth workers does, as
	// through a common table expression: the coordinator would read it all.
	ctx := context.Background()
	ws := h.co.pool.known(ctx)
	for _, tc := range []struct {
		query string
		ok    bool
	}{
		{q, false},
		{`WITH o AS (SELECT * FROM orders) SELECT l_returnflag, count(*) * 1.0 / (SELECT count(*) FROM o) FROM lineitem GROUP BY 1`, false},
		{`SELECT l_returnflag, count(*) * 1.0 / (SELECT count(*) FROM nation) FROM lineitem GROUP BY 1`, true},
	} {
		stmt, err := h.co.parser.parse(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		opts := planOptions{force: true, now: time.Now(), maxBroadcast: defaultBroadcast, minBytes: 100_000,
			taskMemory: taskMemory(ws), workers: len(ws)}
		_, err = newPlanner(ctx, harnessEnv{h}, h.co.parser, h.co.aggregates(ctx, harnessEnv{h}), opts).plan(stmt)
		if (err == nil) != tc.ok || err != nil && !strings.Contains(err.Error(), "more than the coordinator should read") {
			t.Errorf("%s: planned with the coordinator reading 100 KB at most: %v, want %v", short(tc.query), err, tc.ok)
		}
	}
}
