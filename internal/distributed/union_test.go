package distributed

import (
	"slices"
	"strings"
	"testing"
)

// TestUnionRelations checks a UNION of distinct rows as a relation workers
// compute: its branches write their rows, partitioned by all of their
// columns, and a last stage keeps each partition's distinct rows, NULLs
// equal to NULLs, as one machine's UNION does, nested in UNION ALL or
// around one.
func TestUnionRelations(t *testing.T) {
	h := newHarness(t, 0.02, 4, 3)
	h.co.maxBroadcast = 1
	for _, q := range []string{
		`SELECT kind, count(*) AS n FROM (SELECT l_shipmode AS kind, l_orderkey FROM lineitem UNION SELECT o_orderpriority, o_orderkey FROM orders) GROUP BY 1 ORDER BY 1`,
		`SELECT f, s, count(*) AS n FROM (SELECT CASE WHEN l_quantity > 40 THEN NULL ELSE l_returnflag END AS f, l_linestatus AS s FROM lineitem
			UNION SELECT NULL, o_orderstatus FROM orders UNION SELECT 'R', 'F' FROM supplier) GROUP BY 1, 2 ORDER BY 1, 2`,
		`SELECT count(*) AS n, count(DISTINCT k) AS keys FROM (SELECT l_suppkey % 10 AS k, l_returnflag AS f FROM lineitem
			UNION SELECT s_suppkey % 10, 'R' FROM supplier UNION ALL SELECT o_custkey % 10, o_orderstatus FROM orders)`,
		`WITH u AS (SELECT l_orderkey AS id, l_shipdate AS day FROM lineitem WHERE l_quantity < 5 UNION SELECT o_orderkey, o_orderdate FROM orders WHERE o_totalprice < 5000)
		SELECT date_trunc('month', day) AS m, count(*) AS n FROM u JOIN orders ON o_orderkey = u.id GROUP BY 1 ORDER BY 1`,
	} {
		if !h.check(q) {
			t.Errorf("ran on one machine: %s", short(q))
			continue
		}
		p := h.planOf(q)
		if !slices.ContainsFunc(p.Stages, func(s *Stage) bool { return strings.Contains(s.SQL, "SELECT DISTINCT * FROM __igloo_r") }) {
			t.Errorf("no stage keeps the distinct rows: %s\n%s", short(q), p.describe())
		}
	}
}
