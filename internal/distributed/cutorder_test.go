package distributed

import "testing"

// TestCutsComeAfterHaving checks that a stage keeps a ranking's first groups, or
// ORDER BY ... LIMIT's, only from the groups HAVING keeps: SQL filters groups by
// HAVING before it ranks them, orders them or limits them. A stage that cut first
// and left HAVING to the coordinator, because it reads a subquery, returned no rows
// where one machine returned 10. With whole groups in each partition the stage never
// applied HAVING at all.
func TestCutsComeAfterHaving(t *testing.T) {
	h := newHarness(t, 0.01, 6, 3)
	h.addTable("topk_having", "(SELECT i % 1000 AS g, (i % 1000)::DOUBLE AS v, i % 7 AS k FROM range(300000) r(i))", 6)
	queries := []string{
		// A ranking over groups HAVING keeps by a subquery.
		`SELECT count(*) FROM (SELECT g, rank() OVER (ORDER BY a) AS rnk FROM (SELECT g, avg(v) AS a FROM topk_having GROUP BY g
			HAVING avg(v) > (SELECT avg(v) FROM topk_having WHERE k = 0)) x) y WHERE rnk <= 10`,
		`SELECT g, rnk FROM (SELECT g, row_number() OVER (ORDER BY a DESC) AS rnk FROM (SELECT g, avg(v) AS a FROM topk_having GROUP BY g
			HAVING avg(v) < (SELECT avg(v) FROM topk_having WHERE k = 1)) x) y WHERE rnk <= 5 ORDER BY rnk`,
		// The aggregation's own QUALIFY.
		`SELECT g, avg(v) AS a FROM topk_having GROUP BY g HAVING avg(v) > (SELECT avg(v) FROM topk_having WHERE k = 0)
			QUALIFY rank() OVER (ORDER BY a) <= 10 ORDER BY g`,
		// ORDER BY ... LIMIT over groups HAVING keeps by a subquery.
		`SELECT g, avg(v) AS a FROM topk_having GROUP BY g HAVING avg(v) > (SELECT avg(v) FROM topk_having WHERE k = 0) ORDER BY a LIMIT 10`,
		`SELECT g, count(*) AS n FROM topk_having GROUP BY g HAVING count(*) >= (SELECT max(k) FROM topk_having) * 43 ORDER BY g DESC LIMIT 3`,
		// Groups whole in each partition: grouped by the key a join partitions by.
		`SELECT o_custkey, count(*) AS n FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY o_custkey
			HAVING count(*) > 20 ORDER BY o_custkey LIMIT 5`,
		`SELECT o_custkey, n FROM (SELECT o_custkey, rank() OVER (ORDER BY n, o_custkey) AS r, n FROM (SELECT o_custkey, count(*) AS n
			FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY o_custkey HAVING count(*) > 20) x) y WHERE r <= 5 ORDER BY o_custkey`,
		`SELECT o_custkey, count(*) AS n FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY o_custkey
			HAVING count(*) > (SELECT count(*) / 1000 FROM orders) ORDER BY n DESC, o_custkey LIMIT 5`,
	}
	for _, broadcast := range []int64{0, 1} {
		h.co.maxBroadcast = broadcast
		for _, q := range queries {
			h.check(q)
		}
	}
}
