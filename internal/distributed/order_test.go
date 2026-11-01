package distributed

import (
	"context"
	"testing"
)

// TestDropOrders checks which ORDER BY clauses dropOrders removes: those of
// subqueries and common table expressions whose readers sort again,
// aggregate, or take distinct rows, and not those whose order reaches the
// client, a LIMIT without ORDER BY, DISTINCT ON, or a call that sees its
// rows in order, nor any with a LIMIT of its own.
func TestDropOrders(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	ctx := context.Background()
	pl := newPlanner(ctx, harnessEnv{h}, h.co.parser, h.co.aggregates(ctx, harnessEnv{h}), planOptions{})
	for _, tc := range []struct {
		query string
		kept  int
	}{
		// Sorted again, aggregated, distinct or a UNION: dropped.
		{`SELECT * FROM (SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity) AS s WHERE l_quantity > 3 ORDER BY l_orderkey LIMIT 5`, 1},
		{`SELECT count(*), sum(l_quantity) FROM (SELECT l_quantity FROM lineitem ORDER BY 1) AS s`, 0},
		{`SELECT l_returnflag, max(l_quantity) FROM (SELECT * FROM lineitem ORDER BY l_orderkey) AS s GROUP BY 1`, 0},
		{`SELECT DISTINCT l_returnflag FROM (SELECT * FROM lineitem ORDER BY l_orderkey) AS s`, 0},
		{`WITH c AS (SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity) SELECT count(*) FROM c JOIN orders ON o_orderkey = c.l_orderkey`, 0},
		{`SELECT count(*) FROM (SELECT * FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS a UNION ALL SELECT o_orderkey FROM orders) AS u`, 0},
		{`SELECT * FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS a UNION SELECT o_orderkey FROM orders`, 0},
		{`SELECT * FROM (SELECT l_orderkey FROM (SELECT * FROM lineitem ORDER BY l_quantity) AS i ORDER BY 1) AS o ORDER BY 1 DESC`, 1},
		// Passed on to the client, or to what takes its first rows: kept.
		{`SELECT * FROM (SELECT l_orderkey, l_quantity FROM lineitem ORDER BY l_quantity) AS s WHERE l_quantity > 3`, 1},
		{`SELECT * FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS s LIMIT 5`, 1},
		{`WITH c AS (SELECT l_orderkey FROM lineitem ORDER BY 1) SELECT * FROM c`, 1},
		{`WITH c AS (SELECT l_orderkey FROM lineitem ORDER BY 1) SELECT count(*) FROM c UNION ALL SELECT l_orderkey FROM c`, 1},
		{`SELECT * FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS a UNION ALL SELECT o_orderkey FROM orders`, 1},
		{`SELECT l_orderkey FROM (SELECT * FROM lineitem ORDER BY l_quantity) AS s JOIN orders ON o_orderkey = l_orderkey`, 1},
		// Read in order by what it calls: kept.
		{`SELECT list(l_orderkey) FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS s`, 1},
		{`SELECT first(l_orderkey), count(*) FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS s`, 1},
		{`SELECT l_orderkey, row_number() OVER () FROM (SELECT l_orderkey FROM lineitem ORDER BY 1) AS s ORDER BY 2`, 2},
		{`SELECT DISTINCT ON (l_returnflag) l_returnflag, l_quantity FROM (SELECT * FROM lineitem ORDER BY l_quantity DESC) AS s`, 1},
		// Its own LIMIT: kept.
		{`SELECT count(*) FROM (SELECT l_orderkey FROM lineitem ORDER BY 1 LIMIT 10) AS s`, 1},
		{`SELECT count(*) FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM (SELECT o_orderkey FROM orders ORDER BY o_totalprice) AS s LIMIT 3)`, 1},
		// Ordered by its call itself: dropped. (DuckDB writes list(x ORDER
		// BY x) as list_sort(list(x)), whose list() keeps it.)
		{`SELECT list(l_orderkey ORDER BY l_orderkey) FROM (SELECT l_orderkey FROM lineitem ORDER BY 1 DESC) AS s`, 1},
		{`SELECT l_orderkey, row_number() OVER (ORDER BY l_orderkey) FROM (SELECT l_orderkey FROM lineitem ORDER BY 1 DESC) AS s ORDER BY 1`, 1},
	} {
		stmt, err := h.co.parser.parse(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		pl.dropOrders(stmt)
		n := 0
		walk(stmt, func(m node) bool {
			for _, mod := range list(m, "modifiers") {
				if md, ok := mod.(node); ok && str(md["type"]) == "ORDER_MODIFIER" {
					n++
				}
			}
			return true
		})
		if n != tc.kept {
			out, _ := h.co.parser.print(stmt)
			t.Errorf("%s\nkept %d ORDER BY clauses, want %d: %s", tc.query, n, tc.kept, out)
		}
	}
	// The rows of a report that sorts in its last SELECT, read by a query
	// that sorts again, are one machine's, on workers.
	h.t = t
	if !h.check(`SELECT * FROM (WITH g AS (SELECT l_returnflag, l_linestatus, count(*) AS n, sum(l_quantity) AS q FROM lineitem GROUP BY 1, 2)
		SELECT * FROM g WHERE n > 1 ORDER BY q DESC) AS t0 WHERE t0.q > 0 ORDER BY t0.n DESC, t0.l_returnflag, t0.l_linestatus LIMIT 100`) {
		t.Error("ran on one machine")
	}
}
