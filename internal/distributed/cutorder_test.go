package distributed

import "testing"

// TestCutsComeAfterHaving checks that a stage keeps a ranking's first groups, or
// ORDER BY ... LIMIT's, only from the groups HAVING keeps: SQL filters groups by
// HAVING before it ranks them, orders them or limits them. A stage that cut first
// and left HAVING to the coordinator, because it reads a subquery, returned no rows
// where one machine returned 10. With whole groups in each partition the stage never
// applied HAVING at all. Nor may a stage cut groups that a window function sees
// before the ranking: row_number() OVER (ORDER BY g) numbered the five kept 1 to 5.
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
		// ORDER BY a name that is both the select list's alias and a column of
		// FROM orders by the alias.
		`SELECT o_custkey, sum(o_totalprice) AS o_totalprice FROM orders JOIN customer ON o_custkey = c_custkey GROUP BY o_custkey, o_totalprice
			ORDER BY o_totalprice DESC, o_custkey LIMIT 5`,
		`WITH a AS (SELECT o_custkey, sum(o_totalprice) total FROM orders GROUP BY o_custkey), b AS (SELECT c_custkey AS o_custkey, sum(c_acctbal) total
			FROM customer GROUP BY c_custkey) SELECT o_custkey, sum(total) total FROM (SELECT * FROM a UNION ALL SELECT * FROM b) t GROUP BY o_custkey
			ORDER BY total, o_custkey LIMIT 10`,
		// Windows over the groups a ranking keeps the first of, which see every group.
		`SELECT g, n, rn, tot FROM (SELECT g, n, rn, tot, rank() OVER (ORDER BY n DESC, g) AS r FROM (SELECT g, count(*) AS n, row_number() OVER (ORDER BY g) AS rn,
			sum(count(*)) OVER () AS tot FROM topk_having WHERE k < 3 GROUP BY g) x) y WHERE r <= 5 ORDER BY g`,
		`SELECT g, n, c FROM (SELECT g, n, c, row_number() OVER (ORDER BY n, g) AS r FROM (SELECT g, count(*) AS n, count(*) OVER (PARTITION BY g % 10) AS c
			FROM topk_having WHERE k < 3 GROUP BY g) x) y WHERE r <= 5 ORDER BY g`,
		`SELECT g, count(*) AS n, sum(count(*)) OVER () AS tot FROM topk_having WHERE k < 3 GROUP BY g
			QUALIFY rank() OVER (ORDER BY n DESC, g) <= 5 ORDER BY g`,
		// A window of a projection between them.
		`SELECT g, n, rn FROM (SELECT g, n, rn, rank() OVER (ORDER BY n DESC, g) AS r FROM (SELECT g, n, row_number() OVER (ORDER BY g) AS rn
			FROM (SELECT g, count(*) AS n FROM topk_having WHERE k < 3 GROUP BY g) a) x) y WHERE r <= 5 ORDER BY g`,
	}
	for _, broadcast := range []int64{0, 1} {
		h.co.maxBroadcast = broadcast
		for _, q := range queries {
			h.check(q)
		}
	}
}
