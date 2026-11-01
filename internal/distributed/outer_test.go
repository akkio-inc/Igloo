package distributed

import (
	"testing"
)

// TestOuterJoinsByPartition checks LEFT, RIGHT and FULL joins whose side
// that supplies NULLs (or either side, for FULL) is too large for every
// worker to read whole: both sides are read by partition, by the key the
// join's condition makes equal, and the rows are one machine's, the rows
// no match kept once, with their NULLs; groups and filters by the NULL
// side's columns, which its partitions do not hold whole, too.
func TestOuterJoinsByPartition(t *testing.T) {
	h := newHarness(t, 0.02, 4, 3)
	// orders and lineitem are too large to read whole; customer is not.
	h.co.maxBroadcast = h.tables["customer"].Bytes + 1
	for _, tc := range []struct {
		query   string
		workers bool
	}{
		{`SELECT c_custkey, c_name, count(o_orderkey) AS orders, sum(o_totalprice) AS spent FROM customer
			LEFT JOIN orders ON o_custkey = c_custkey GROUP BY 1, 2 ORDER BY 3, 1 LIMIT 50`, true},
		{`SELECT count(*) AS n, count(o_orderkey) AS matched FROM customer LEFT JOIN orders ON o_custkey = c_custkey AND o_orderstatus = 'F'`, true},
		// A filter on the side that supplies NULLs holds after the join.
		{`SELECT count(*) AS n FROM customer LEFT JOIN orders ON o_custkey = c_custkey WHERE o_orderkey IS NULL`, true},
		{`SELECT c_mktsegment, count(*) AS n FROM customer LEFT JOIN orders ON o_custkey = c_custkey WHERE o_totalprice > 100000 OR o_orderkey IS NULL
			GROUP BY 1 ORDER BY 1`, true},
		// Grouped by the NULL side's key, whose NULL group every partition holds a share of.
		{`SELECT o_orderstatus, count(*) AS n, count(DISTINCT c_custkey) AS customers FROM customer LEFT JOIN orders ON o_custkey = c_custkey
			GROUP BY 1 ORDER BY 1`, true},
		{`SELECT count(*) AS n, count(c_custkey) AS customers FROM orders RIGHT JOIN customer ON o_custkey = c_custkey`, true},
		{`SELECT count(*) AS n, count(l_orderkey) AS lines FROM part LEFT JOIN lineitem ON p_partkey = l_partkey`, true},
		{`SELECT o_orderpriority, count(*) AS n, count(l_orderkey) AS shipped FROM orders
			FULL JOIN (SELECT * FROM lineitem WHERE l_shipmode = 'AIR') AS l ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`, true},
		{`SELECT count(*) AS n, count(a.k) AS a, count(b.k) AS b FROM (SELECT o_orderkey AS k FROM orders WHERE o_orderkey % 3 = 0) AS a
			FULL JOIN (SELECT l_orderkey AS k FROM lineitem WHERE l_orderkey % 5 = 0) AS b ON a.k = b.k`, true},
		// Not on equal columns: no key to partition by.
		{`SELECT count(*) FROM customer LEFT JOIN orders ON o_custkey < c_custkey AND o_orderkey = c_custkey * 7`, false},
		{`SELECT count(*) FROM customer LEFT JOIN orders ON o_custkey + 0 = c_custkey`, false},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if got := h.check(tc.query); got != tc.workers {
				t.Errorf("ran distributed: %v, want %v", got, tc.workers)
			}
		})
	}
	// customer, small enough to read whole, is partitioned to meet orders,
	// rather than read whole beside its partitions.
	h.t = t
	q := `SELECT count(*) AS n FROM customer LEFT JOIN orders ON o_custkey = c_custkey WHERE o_orderkey IS NULL`
	if p := h.planOf(q); p.exchanges("join") != 2 {
		t.Errorf("%d tables partitioned to meet, want customer and orders:\n%s", p.exchanges("join"), p.describe())
	}
}
