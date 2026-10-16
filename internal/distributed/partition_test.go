package distributed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// partitioned runs query distributed and reports whether a job of it
// partitioned two tables, after comparing its result with one machine's.
func (h *harness) partitioned(query string) bool {
	h.t.Helper()
	if !h.check(query) {
		return false
	}
	plan, err := h.co.Plan(context.Background(), query, harnessEnv{h}, ModeOn)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, j := range plan.Jobs {
		if j.Partition != nil {
			return true
		}
	}
	return false
}

// TestPartitionedJoins makes orders too large for every worker to read, so
// joins of lineitem and orders partition both by the order key.
func TestPartitionedJoins(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["orders"].Bytes - 1
	for _, q := range []string{
		`SELECT o_orderpriority, count(*), sum(l_quantity) FROM lineitem JOIN orders ON l_orderkey = o_orderkey
			WHERE o_orderdate < DATE '1995-01-01' GROUP BY 1 ORDER BY 1`,
		`SELECT count(DISTINCT o_custkey), count(*) FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_shipmode = 'AIR'`,
		`SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem
			JOIN orders ON o_orderkey = l_orderkey JOIN customer ON c_custkey = o_custkey JOIN nation ON n_nationkey = c_nationkey
			GROUP BY n_name ORDER BY revenue DESC`,
		`SELECT l_orderkey, l_linenumber, o_orderdate FROM lineitem JOIN orders ON l_orderkey = o_orderkey
			WHERE l_quantity = 50 ORDER BY 1, 2 LIMIT 10`,
		`SELECT y, count(*) FROM (SELECT extract(year FROM o_orderdate) AS y FROM orders o, lineitem l
			WHERE o.o_orderkey = l.l_orderkey AND l.l_discount > 0.05) GROUP BY y ORDER BY y`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.partitioned(q) {
				t.Error("did not partition the join")
			}
		})
	}
	if len(h.removed) == 0 {
		t.Error("no partitioned rows were removed")
	}
	for _, dir := range h.removed {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s is still there", dir)
		}
	}

	// A composite key, between tables of different shapes.
	h.co.maxBroadcast = h.tables["partsupp"].Bytes - 1
	h.t = t
	q := `SELECT ps_availqty > 5000 AS plenty, count(*), sum(l_quantity * ps_supplycost) FROM lineitem
		JOIN partsupp ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey GROUP BY 1 ORDER BY 1`
	if !h.partitioned(q) {
		t.Error("did not partition the join on two columns")
	}
}

// TestTPCHPartitioned runs TPC-H with orders and partsupp too large to
// read in full, comparing every result with one machine's.
func TestTPCHPartitioned(t *testing.T) {
	h := newHarness(t, 0.05, 6, 4)
	h.co.maxBroadcast = min(h.tables["orders"].Bytes, h.tables["partsupp"].Bytes) - 1
	var parted []string
	for q := 1; q <= 22; q++ {
		b, err := os.ReadFile(filepath.Join("..", "..", "bench", "queries", "tpch", fmt.Sprintf("q%02d.sql", q)))
		if err != nil {
			t.Fatal(err)
		}
		t.Run(fmt.Sprintf("q%02d", q), func(t *testing.T) {
			h.t = t
			if h.partitioned(string(b)) {
				parted = append(parted, fmt.Sprintf("q%02d", q))
			}
		})
	}
	t.Logf("partitioned: %s", strings.Join(parted, " "))
	if len(parted) < 4 {
		t.Errorf("only %d TPC-H queries partitioned a join", len(parted))
	}
}
