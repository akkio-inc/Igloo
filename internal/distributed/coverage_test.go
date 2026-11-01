package distributed

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestCoverageShapes runs EXPLAIN DISTRIBUTED, forced on, over a synthetic
// workload of the shapes that kept a real workload's data queries on one
// machine, grouped by the reason they had, and tabulates where each group
// runs: TABLESAMPLE, HLL sketches, rows of a scan that keeps few, a
// subquery without aggregation the coordinator reads, and a scalar
// subquery outside WHERE. Each shape listed runs on workers; those that
// still stay are listed apart, with their reasons.
func TestCoverageShapes(t *testing.T) {
	sketchesHere := os.Getenv("IGLOO_TEST_EXTENSION_DIR") != ""
	var exts []string
	if sketchesHere {
		exts = []string{"datasketches"}
	}
	h := newHarnessWith(t, 0.02, 4, 3, "", exts...)
	approx := func(arg string) string {
		return "CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN " + arg + " IS NOT NULL THEN hash(" + arg + ") END))), 0) AS BIGINT)"
	}
	type shape struct {
		group, query string
		workers      bool
	}
	shapes := []shape{
		{"TABLESAMPLE", `SELECT count(*), sum(l_quantity) FROM lineitem TABLESAMPLE bernoulli(10%) REPEATABLE (1)`, true},
		{"TABLESAMPLE", `SELECT l_shipmode, avg(l_extendedprice) FROM lineitem TABLESAMPLE system(25%) GROUP BY 1`, true},
		{"TABLESAMPLE", `SELECT n_name, count(*) FROM lineitem TABLESAMPLE bernoulli(5%) REPEATABLE (2) JOIN supplier ON l_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey GROUP BY 1`, true},
		{"TABLESAMPLE", `SELECT s.f, count(*) FROM (SELECT l_returnflag AS f FROM lineitem WHERE l_quantity > 3) AS s TABLESAMPLE bernoulli(20%) GROUP BY 1`, true},
		{"TABLESAMPLE", `SELECT count(*) FROM lineitem TABLESAMPLE reservoir(500 ROWS)`, false},
		{"rows of a scan", `SELECT l_orderkey, l_comment FROM lineitem WHERE l_suppkey = 11`, true},
		{"rows of a scan", `SELECT o_orderkey, o_orderdate FROM orders WHERE o_custkey IN (5, 6, 7) ORDER BY 1`, true},
		{"rows of a scan", `SELECT l_orderkey FROM lineitem WHERE l_comment LIKE '%regular%'`, true},
		{"subquery without aggregation", `SELECT x.l_orderkey, count(*) OVER () FROM (SELECT l_orderkey FROM lineitem WHERE l_partkey = 9) AS x`, true},
		{"subquery without aggregation", `WITH s AS (SELECT l_orderkey, l_extendedprice FROM lineitem WHERE l_suppkey IN (1, 2))
			SELECT *, rank() OVER (ORDER BY l_extendedprice DESC) FROM s`, true},
		{"subquery outside WHERE", `SELECT l_returnflag, count(*) * 1.0 / (SELECT count(*) FROM orders) FROM lineitem GROUP BY 1`, true},
		{"subquery outside WHERE", `SELECT l_shipmode, sum(l_quantity) FROM lineitem GROUP BY 1 HAVING sum(l_quantity) > (SELECT count(*) FROM orders) / 10`, true},
		{"subquery outside WHERE", `SELECT l_returnflag, count(*) FROM lineitem l GROUP BY 1 HAVING count(*) > (SELECT count(*) FROM orders WHERE o_orderstatus = l.l_returnflag)`, false},
	}
	if sketchesHere {
		shapes = append(shapes,
			shape{"HLL sketches", `SELECT ` + approx("l_partkey") + ` FROM lineitem`, true},
			shape{"HLL sketches", `SELECT l_shipmode, datasketch_hll_estimate(datasketch_hll(12, hash(l_orderkey))) FROM lineitem GROUP BY 1`, true},
			shape{"HLL sketches", `WITH d AS (SELECT l_shipdate AS day, datasketch_hll(12, hash(l_orderkey)) AS s FROM lineitem GROUP BY 1)
				SELECT datasketch_hll_estimate(datasketch_hll_union(12, s)) FROM d`, true})
	}
	type tally struct{ workers, total int }
	groups := map[string]*tally{}
	var stays []string
	for _, s := range shapes {
		rows, err := h.co.Explain(context.Background(), s.query, harnessEnv{h}, ModeOn)
		if err != nil {
			t.Fatalf("%s: %v", short(s.query), err)
		}
		where, reason := "", ""
		for _, r := range rows {
			switch r[0] {
			case "runs on":
				where = r[1]
			case "reason":
				reason = r[1]
			}
		}
		g := groups[s.group]
		if g == nil {
			g = &tally{}
			groups[s.group] = g
		}
		g.total++
		onWorkers := where == "workers"
		if onWorkers {
			g.workers++
		} else {
			stays = append(stays, fmt.Sprintf("%s: %s: %s", s.group, short(s.query), reason))
		}
		if onWorkers != s.workers {
			t.Errorf("%s runs on %s (%s), want on workers %v", short(s.query), where, reason, s.workers)
		}
	}
	var names []string
	for n := range groups {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("\n| Reason the shapes had | On workers | Shapes |\n|---|---|---|\n")
	for _, n := range names {
		fmt.Fprintf(&b, "| %s | %d | %d |\n", n, groups[n].workers, groups[n].total)
	}
	b.WriteString("\nStill on one machine:\n")
	for _, s := range stays {
		b.WriteString("- " + s + "\n")
	}
	t.Log(b.String())
}
