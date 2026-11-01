package distributed

import (
	"context"
	"strings"
	"testing"
)

// A statement that ranks an aggregation's groups and keeps those within
// the first k has the stage that completes the groups keep only rows that
// may rank so (QUALIFY), and returns what one machine does: with ties,
// which RANK() keeps all of at the kth place and ROW_NUMBER() chooses
// among; with NULLs first or last; with k past a partition's size; and
// through a UNION ALL of two aggregations, read through projections, with
// a rank computed under a CASE and a condition that keeps whole
// partitions. Groups are all kept where ranks would come out otherwise
// over the rows kept: with two windows, or rows kept by a column the
// window does not partition by; and where the statement reads the groups
// elsewhere too.
func TestTopK(t *testing.T) {
	h := newHarness(t, 0.01, 3, 3)
	agg := `WITH agg AS (SELECT l_returnflag AS f, l_shipmode AS m, l_partkey % 50 AS v, count(DISTINCT l_orderkey) AS n, count(*) AS c,
		max(CASE WHEN l_quantity > 45 THEN l_extendedprice END) AS x FROM lineitem GROUP BY 1, 2, 3)`
	cases := []struct {
		name, query string
		pushed      bool
	}{
		{"row_number", agg + `, ranked AS (SELECT f, m, v, n, c, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY n DESC NULLS LAST, c DESC, v) AS r FROM agg)
			SELECT f, m, v, n, c, r FROM ranked WHERE r <= 3 ORDER BY f, m, r`, true},
		// Ties at the kth place: RANK() keeps them all; ROW_NUMBER() any of
		// them, so only the counts compare.
		{"rank ties", agg + `, ranked AS (SELECT f, m, v, n, rank() OVER (PARTITION BY f ORDER BY n DESC) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE r <= 2 ORDER BY f, r, m, v`, true},
		{"dense_rank ties", agg + `, ranked AS (SELECT f, m, v, n, dense_rank() OVER (PARTITION BY m ORDER BY n DESC) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE 3 >= r ORDER BY m, r, f, v`, true},
		{"row_number ties", agg + `, ranked AS (SELECT f, m, n, ROW_NUMBER() OVER (PARTITION BY f ORDER BY n DESC) AS r FROM agg)
			SELECT f, n, r FROM ranked WHERE r < 4 ORDER BY f, r`, true},
		{"nulls first", agg + `, ranked AS (SELECT f, m, v, x, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY x DESC NULLS FIRST, v) AS r FROM agg)
			SELECT f, m, v, x, r FROM ranked WHERE r <= 2 ORDER BY f, m, r`, true},
		{"nulls last", agg + `, ranked AS (SELECT f, m, v, x, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY x ASC NULLS LAST, v) AS r FROM agg)
			SELECT f, m, v, x, r FROM ranked WHERE r <= 2 ORDER BY f, m, r`, true},
		{"k past the partition", agg + `, ranked AS (SELECT f, m, v, n, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY n DESC, v) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE r <= 100000 ORDER BY f, m, r`, true},
		{"two windows", agg + `, ranked AS (SELECT f, m, v, n, c, ROW_NUMBER() OVER (PARTITION BY f ORDER BY n DESC, v, m) AS r1,
			ROW_NUMBER() OVER (PARTITION BY m ORDER BY c, v, f) AS r2 FROM agg)
			SELECT f, m, v, n, c, r1, r2 FROM ranked WHERE r1 <= 2 OR r2 <= 1 ORDER BY f, m, v`, false},
		{"another window read", agg + `, ranked AS (SELECT f, m, v, n, c, ROW_NUMBER() OVER (PARTITION BY f ORDER BY n DESC, v, m) AS r1,
			ROW_NUMBER() OVER (PARTITION BY m ORDER BY c, v, f) AS r2 FROM agg)
			SELECT f, m, v, n, c, r1, r2 FROM ranked WHERE r1 <= 2 ORDER BY f, m, v`, false},
		{"rows kept by another column", agg + `, ranked AS (SELECT f, m, v, n, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY n DESC, v) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE v < 3 OR r <= 2 ORDER BY f, m, v`, false},
		{"rows kept by a partition column", agg + `, ranked AS (SELECT f, m, v, n, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY n DESC, v) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE m = 'AIR' OR r <= 2 ORDER BY f, m, v`, true},
		{"union under a case", `WITH a1 AS (SELECT 'ship' AS src, l_shipmode AS kind, l_partkey % 40 AS v, count(DISTINCT l_orderkey) AS n FROM lineitem GROUP BY 2, 3),
			a2 AS (SELECT 'flag' AS src, l_returnflag AS kind, l_suppkey % 40 AS v, count(DISTINCT l_orderkey) AS n FROM lineitem GROUP BY 2, 3),
			allr AS (SELECT a1.src AS src, a1.kind AS kind, a1.v AS v, a1.n AS n FROM a1 UNION ALL SELECT a2.src AS s2, a2.kind AS k2, a2.v AS v2, a2.n AS n2 FROM a2),
			ranked AS (SELECT src, kind, v, n, CASE WHEN upper(kind) = 'AIR' THEN 0 ELSE ROW_NUMBER() OVER (PARTITION BY src, kind ORDER BY n DESC NULLS LAST, v) END AS r FROM allr)
			SELECT src, kind, v, n, r FROM ranked WHERE upper(kind) = 'AIR' OR r <= 2 ORDER BY src, kind, r, v`, true},
		{"read twice", agg + `, ranked AS (SELECT f, m, v, n, ROW_NUMBER() OVER (PARTITION BY f, m ORDER BY n DESC, v) AS r FROM agg)
			SELECT f, m, v, n, r FROM ranked WHERE r <= 1 UNION ALL SELECT f, m, v, n, 0 FROM agg WHERE n > 25 ORDER BY 1, 2, 3, 5`, false},
		// A union's branches as SELECT * of the aggregations they read.
		{"union of stars", `WITH a1 AS (SELECT l_shipmode AS kind, l_partkey % 40 AS v, count(DISTINCT l_orderkey) AS n FROM lineitem GROUP BY 1, 2),
			a2 AS (SELECT l_returnflag AS kind, l_suppkey % 40 AS v, count(DISTINCT l_orderkey) AS n FROM lineitem GROUP BY 1, 2),
			allr AS (SELECT * FROM a1 UNION ALL SELECT * FROM a2),
			ranked AS (SELECT kind, v, n, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY n DESC NULLS LAST, v) AS r FROM allr)
			SELECT kind, v, n, r FROM ranked WHERE r <= 2 ORDER BY kind, r`, true},
		// The aggregation's own QUALIFY, by an alias and inline; beside
		// another window, every group.
		{"own qualify", `SELECT l_returnflag AS f, l_partkey % 50 AS v, count(DISTINCT l_orderkey) AS n, row_number() OVER (PARTITION BY f ORDER BY n DESC, v) AS r
			FROM lineitem GROUP BY 1, 2 QUALIFY r <= 3 ORDER BY f, r`, true},
		{"own qualify inline", `SELECT l_shipmode AS m, l_partkey % 50 AS v, count(*) AS c FROM lineitem GROUP BY 1, 2
			QUALIFY rank() OVER (PARTITION BY m ORDER BY count(*) DESC) <= 2 ORDER BY m, c DESC, v`, true},
		{"own qualify beside a window", `SELECT l_shipmode AS m, l_partkey % 50 AS v, count(*) AS c, sum(count(*)) OVER (PARTITION BY l_shipmode) AS t
			FROM lineitem GROUP BY 1, 2 QUALIFY rank() OVER (PARTITION BY m ORDER BY count(*) DESC) <= 2 ORDER BY m, c DESC, v`, false},
	}
	for _, mode := range []string{aggByGroup, ""} {
		h.co.aggregation = mode
		for _, tc := range cases {
			t.Run(mode+" "+tc.name, func(t *testing.T) {
				h.t = t
				if !h.check(tc.query) {
					t.Fatal("did not run distributed")
				}
				plan, err := h.co.Plan(context.Background(), tc.query, harnessEnv{h}, ModeOn)
				if err != nil {
					t.Fatal(err)
				}
				pushed := strings.Contains(plan.describe(), "QUALIFY")
				if mode == aggByGroup && pushed != tc.pushed {
					t.Errorf("ranked rows kept early: %v, want %v\n%s", pushed, tc.pushed, plan.describe())
				}
			})
		}
	}
	h.co.aggregation = ""

	// Groups whole in each partition of a partitioned join.
	h.co.maxBroadcast = 1
	defer func() { h.co.maxBroadcast = 0 }()
	h.t = t
	q := `WITH agg AS (SELECT o_orderpriority AS p, o_orderkey AS k, count(*) AS n, sum(l_quantity) AS q FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1, 2),
		ranked AS (SELECT p, k, n, q, ROW_NUMBER() OVER (PARTITION BY p ORDER BY n DESC, q DESC, k) AS r FROM agg)
		SELECT p, k, n, q, r FROM ranked WHERE r <= 3 ORDER BY p, r`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	plan, err := h.co.Plan(context.Background(), q, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.describe(), "QUALIFY") {
		t.Errorf("whole groups: ranked rows not kept early\n%s", plan.describe())
	}
}
