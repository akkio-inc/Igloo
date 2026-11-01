package distributed

import (
	"context"
	"strings"
	"testing"
)

// TestShapesOnWorkers checks statements of the shapes that stayed on one
// machine, or left their heaviest part to the coordinator, over tables too large
// for every worker to read: workers compute them, by partition, and they return
// the rows of one machine. want is in the stages their plans list.
func TestShapesOnWorkers(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	// Orders of no line, lines of no order, NULL keys on both sides.
	h.addTable("olines", "(SELECT * REPLACE (CASE WHEN l_linenumber = 7 THEN NULL WHEN l_linenumber = 6 THEN l_orderkey + 100000000 ELSE l_orderkey END AS l_orderkey) FROM lineitem)", 2)
	h.addTable("lorders", "(SELECT * REPLACE (CASE WHEN o_orderkey % 11 = 0 THEN NULL ELSE o_orderkey END AS o_orderkey) FROM orders)", 2)
	h.co.maxBroadcast = 1
	for _, tc := range []struct{ name, query, want string }{
		{"not exists", `SELECT o_orderpriority, count(*) AS n, count(o_orderkey) AS k FROM lorders o WHERE NOT EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 40) GROUP BY 1 ORDER BY 1`, "ANTI JOIN"},
		{"exists", `SELECT o_orderpriority, count(*) AS n FROM lorders o WHERE EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 40) GROUP BY 1 ORDER BY 1`, "SEMI JOIN"},
		{"not exists on two keys, unqualified", `SELECT count(*) AS n FROM lorders WHERE NOT EXISTS (SELECT 1 FROM olines WHERE l_orderkey = o_orderkey AND l_linenumber = o_shippriority AND l_quantity > 40) AND o_orderstatus = 'F'`, "ANTI JOIN"},
		{"exists with no filter of its own", `SELECT count(*) AS n FROM lorders o WHERE EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey)`, "SEMI JOIN"},
		{"correlated scalar in WHERE", `SELECT o_orderpriority, count(*) AS n FROM lorders o WHERE o_totalprice > (SELECT avg(l_extendedprice) * 3 FROM olines l WHERE l.l_orderkey = o.o_orderkey) GROUP BY 1 ORDER BY 1`, "LEFT JOIN"},
		{"correlated scalar in the select list", `SELECT o_orderkey, (SELECT max(l_quantity) FROM olines l WHERE l.l_orderkey = o.o_orderkey) FROM lorders o WHERE o_orderkey % 100 = 1 ORDER BY 1 LIMIT 50`, "LEFT JOIN"},
		{"correlated count", `SELECT o_orderkey, (SELECT count(*) FROM olines l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 45) AS lines FROM lorders o ORDER BY 2 DESC, 1 LIMIT 50`, "LEFT JOIN"},
		{"correlated scalar over a filter", `SELECT count(*) AS n FROM lorders o WHERE (SELECT sum(l_quantity) FROM olines l WHERE l.l_orderkey = o.o_orderkey AND l_returnflag = 'R') IS NULL`, "LEFT JOIN"},
		{"a join in a subquery on another column", `SELECT t.o_orderpriority, sum(s.q) AS q FROM (SELECT l.l_partkey AS k, l.l_quantity AS q FROM lineitem l JOIN orders o ON o.o_orderkey = l.l_orderkey) s JOIN orders t ON t.o_orderkey = s.k GROUP BY 1 ORDER BY 1`, "(relation)"},
		{"distinct", `SELECT DISTINCT l_orderkey FROM olines`, "GROUP BY"},
		{"distinct in order", `SELECT DISTINCT l_returnflag, l_orderkey FROM olines ORDER BY 1, 2 LIMIT 40`, "GROUP BY"},
		{"distinct of an expression", `SELECT DISTINCT l_orderkey % 1000 AS m FROM olines ORDER BY 1`, "GROUP BY"},
		{"intersect", `SELECT l_orderkey FROM olines WHERE l_quantity > 45 INTERSECT SELECT o_orderkey FROM lorders WHERE o_orderstatus = 'F' ORDER BY 1`, "INTERSECT"},
		{"intersect all", `SELECT l_orderkey % 100 AS k FROM olines INTERSECT ALL SELECT o_orderkey % 100 FROM lorders ORDER BY 1`, "INTERSECT ALL"},
		{"except", `SELECT l_orderkey FROM olines WHERE l_quantity > 45 EXCEPT SELECT o_orderkey FROM lorders WHERE o_orderstatus = 'F' ORDER BY 1`, "EXCEPT"},
		{"except all", `SELECT l_orderkey % 100 AS k FROM olines EXCEPT ALL SELECT o_orderkey % 100 FROM lorders ORDER BY 1`, "EXCEPT ALL"},
		{"except without order", `SELECT l_orderkey FROM olines EXCEPT SELECT o_orderkey FROM lorders`, "EXCEPT"},
		{"union", `SELECT l_orderkey FROM olines WHERE l_quantity > 10 UNION SELECT o_orderkey FROM lorders ORDER BY 1`, "DISTINCT"},
		{"intersect in a subquery", `SELECT count(*) AS n FROM (SELECT l_orderkey FROM olines INTERSECT SELECT o_orderkey FROM lorders) x`, "INTERSECT"},
		{"except of unions", `(SELECT l_orderkey FROM olines UNION SELECT l_suppkey FROM olines) EXCEPT SELECT o_orderkey FROM lorders ORDER BY 1`, "EXCEPT"},
		{"left join using", `SELECT o_orderpriority, count(l_quantity) AS n FROM lorders LEFT JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l USING (o_orderkey) GROUP BY 1 ORDER BY 1`, "(relation)"},
		{"natural left join", `SELECT o_orderpriority, count(l_quantity) AS n FROM lorders NATURAL LEFT JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l GROUP BY 1 ORDER BY 1`, "(relation)"},
		{"full join using", `SELECT count(*) AS n, count(l_quantity) AS q, count(o_orderkey) AS k, count(o_orderstatus) AS s FROM lorders FULL JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l USING (o_orderkey)`, "(relation)"},
		{"right join using", `SELECT count(*) AS n, count(o_orderstatus) AS s FROM lorders RIGHT JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l USING (o_orderkey)`, "(relation)"},
		{"inner join using", `SELECT o_orderpriority, count(*) AS n FROM lorders JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l USING (o_orderkey) GROUP BY 1 ORDER BY 1`, "(relation)"},
		{"natural inner join", `SELECT o_orderpriority, count(*) AS n FROM lorders NATURAL JOIN (SELECT l_orderkey AS o_orderkey, l_quantity FROM olines) l GROUP BY 1 ORDER BY 1`, "(relation)"},
		{"a search of text", `SELECT o_orderkey, o_comment FROM lorders WHERE o_comment LIKE '%special%requests%'`, "~~"},
		{"contains", `SELECT o_orderkey, o_comment FROM lorders WHERE contains(o_comment, 'furious')`, "contains"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(tc.query)
			if !strings.Contains(p.describe(), tc.want) {
				t.Errorf("no %q in its stages:\n%s", tc.want, p.describe())
			}
			rows, err := h.co.Explain(context.Background(), tc.query, harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rows {
				if r[0] == "stays on the coordinator" {
					t.Errorf("stays on the coordinator: %s\n%s", r[1], p.describe())
				}
			}
		})
	}

	// Left rows few enough for every task to read: an anti join reads them by
	// partition all the same, or each task would keep those the others match.
	h.t = t
	h.co.maxBroadcast = h.tables["olines"].Bytes - 1
	antiSmall := `SELECT o_orderpriority, count(*) AS n FROM lorders o WHERE NOT EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey) GROUP BY 1 ORDER BY 1`
	if !h.check(antiSmall) {
		t.Error("an anti join of few left rows did not run distributed")
	}
	h.co.maxBroadcast = 1

	// An anti join after another of them, on left rows of two items, stays.
	two := `SELECT count(*) AS n FROM lorders o WHERE NOT EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey) AND NOT EXISTS (SELECT 1 FROM lineitem l WHERE l.l_suppkey = o.o_custkey)`
	h.check(two)

	// What stays as it was: a subquery whose rows cannot be told by a join,
	// NOT IN, whose NULLs no partition can know, and a search of a character.
	for _, tc := range []struct{ name, query, not string }{
		{"correlated through an expression", `SELECT count(*) AS n FROM lorders o WHERE o_orderkey < 2000 AND NOT EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey + 1)`, "ANTI JOIN"},
		{"correlated by less than equal", `SELECT count(*) AS n FROM lorders o WHERE NOT EXISTS (SELECT 1 FROM olines l WHERE l.l_orderkey < o.o_orderkey AND l_quantity > 49.5 AND o_orderkey < 200)`, "ANTI JOIN"},
		{"a count in arithmetic", `SELECT count(*) AS n FROM lorders o WHERE o_orderkey % 100 = 1 AND (SELECT count(*) + 1 FROM olines l WHERE l.l_orderkey = o.o_orderkey) = 1`, "LEFT JOIN"},
		{"coalesce over no rows", `SELECT count(*) AS n FROM lorders o WHERE o_orderkey % 100 = 1 AND (SELECT coalesce(max(l_quantity), 0) FROM olines l WHERE l.l_orderkey = o.o_orderkey) = 0`, "LEFT JOIN"},
		{"not in", `SELECT count(*) AS n FROM lorders o WHERE o_orderkey NOT IN (SELECT l_orderkey FROM olines WHERE l_quantity > 40 AND l_orderkey IS NOT NULL)`, "ANTI JOIN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.t = t
			h.check(tc.query)
			if p, err := h.co.Plan(context.Background(), tc.query, harnessEnv{h}, ModeOn); err == nil && strings.Contains(p.describe(), tc.not) {
				t.Errorf("%q in its stages:\n%s", tc.not, p.describe())
			}
		})
	}
	parser, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer parser.close() //nolint:errcheck
	for q, want := range map[string]bool{
		`SELECT 1 FROM orders WHERE o_comment LIKE '%word%'`:     true,
		`SELECT 1 FROM orders WHERE contains(o_comment, 'wo')`:   false,
		`SELECT 1 FROM orders WHERE o_comment LIKE '%a%'`:        false,
		`SELECT 1 FROM orders WHERE o_comment NOT LIKE '%word%'`: false,
	} {
		n, err := parser.parse(q)
		if err != nil {
			t.Fatal(err)
		}
		if got := searchesText(child(n, "where_clause")); got != want {
			t.Errorf("%s: %v, want %v", q, got, want)
		}
	}
}
