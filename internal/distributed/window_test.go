package distributed

import (
	"strings"
	"testing"
)

// TestWindowsOverPartitions checks window functions that partition their
// rows, computed by workers from inputs read by partition, keyed by the
// windows' columns: in the statement, under QUALIFY, in a subquery the
// coordinator reads, and in relations later stages read, over a table
// and a join; with frames, lag and running sums. The coordinator computes
// no window. Windows over all rows, or partitioned by different columns,
// run on one machine, and so do those that rank groups cut to the first
// few, which the coordinator ranks.
func TestWindowsOverPartitions(t *testing.T) {
	h := newHarness(t, 0.01, 4, 3)
	for _, tc := range []struct {
		query string
		// workers says the windows run on workers; distributed that the
		// statement runs distributed at all.
		workers, distributed bool
	}{
		// Each order's largest line.
		{`SELECT l_orderkey, l_linenumber, l_quantity, row_number() OVER (PARTITION BY l_orderkey ORDER BY l_quantity DESC, l_linenumber) AS rn
			FROM lineitem QUALIFY rn = 1 ORDER BY l_orderkey`, true, true},
		{`SELECT l_suppkey, l_orderkey, l_linenumber, sum(l_quantity) OVER (PARTITION BY l_suppkey ORDER BY l_orderkey, l_linenumber
				ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running,
			lag(l_shipdate) OVER (PARTITION BY l_suppkey ORDER BY l_orderkey, l_linenumber) AS prev
			FROM lineitem WHERE l_quantity > 40 QUALIFY running > 140 ORDER BY 1, 2, 3`, true, true},
		// Every row would reach the coordinator: one machine.
		{`SELECT l_orderkey, sum(l_quantity) OVER (PARTITION BY l_orderkey) FROM lineitem WHERE l_quantity > 45`, false, false},
		// A subquery the coordinator reads, and a relation a later stage
		// groups.
		{`SELECT l_orderkey, l_linenumber FROM (SELECT l_orderkey, l_linenumber, rank() OVER (PARTITION BY l_orderkey ORDER BY l_extendedprice DESC) AS r
			FROM lineitem) AS x WHERE r <= 2 ORDER BY 1, 2`, true, true},
		{`WITH ranked AS (SELECT l_partkey, l_extendedprice, dense_rank() OVER (PARTITION BY l_partkey ORDER BY l_extendedprice DESC) AS r FROM lineitem)
			SELECT l_partkey, count(*) AS n, sum(l_extendedprice) AS s FROM ranked WHERE r <= 2 GROUP BY 1 ORDER BY 1`, true, true},
		// Over a join on the column the windows partition by.
		{`SELECT o.o_orderkey, l.l_linenumber, o.o_orderdate, count(*) OVER (PARTITION BY o.o_orderkey) AS lines
			FROM orders o JOIN lineitem l ON l.l_orderkey = o.o_orderkey WHERE l.l_quantity > 40 QUALIFY lines > 1 ORDER BY 1, 2`, true, true},
		// Over every row, or partitioned by different columns: one machine.
		{`SELECT l_orderkey, row_number() OVER (ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber) AS rn FROM lineitem QUALIFY rn <= 5 ORDER BY 2`, false, false},
		{`SELECT l_orderkey, rank() OVER (PARTITION BY l_orderkey ORDER BY l_quantity) AS a, rank() OVER (PARTITION BY l_suppkey ORDER BY l_quantity) AS b
			FROM lineitem QUALIFY a = 1 AND b = 1 ORDER BY 1, 2, 3`, false, false},
		// A ranking of an aggregation's groups cut to the first few: the
		// stages keep those, and the coordinator ranks them.
		{`WITH agg AS (SELECT l_returnflag AS f, l_partkey % 50 AS v, count(*) AS n FROM lineitem GROUP BY 1, 2),
			ranked AS (SELECT f, v, n, row_number() OVER (PARTITION BY f ORDER BY n DESC, v) AS r FROM agg)
			SELECT f, v, n, r FROM ranked WHERE r <= 3 ORDER BY f, r`, false, true},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if got := h.check(tc.query); got != tc.distributed {
				t.Fatalf("ran distributed: %v, want %v", got, tc.distributed)
			}
			if !tc.distributed {
				return
			}
			p := h.planOf(tc.query)
			final, err := h.co.parser.print(p.final)
			if err != nil {
				t.Fatal(err)
			}
			// The statement's own windows, not a QUALIFY of the first
			// ranks (rank() OVER) a stage keeps (see markTopK).
			own := func(sql string) bool {
				return strings.Count(sql, " OVER (") > strings.Count(sql, " OVER (PARTITION BY __g")
			}
			stages := own(p.describe())
			if stages != tc.workers || own(final) == tc.workers {
				t.Errorf("windows on workers: %v, want %v\n%s\nthe coordinator: %s", stages, tc.workers, p.describe(), final)
			}
		})
	}
}
