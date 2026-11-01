package distributed

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestSamples checks TABLESAMPLE of a percentage of the table workers
// divide: of all its rows, the rows one machine returns; of a part, about
// that part, by row (bernoulli) and by vector (system), the same rows
// again with a seed, in aggregations and joins to tables every task reads
// whole, and by bucket. A sample of a table every task reads whole, or of a
// number of rows, runs on one machine.
func TestSamples(t *testing.T) {
	h := newHarness(t, 0.1, 6, 3)
	h.addBucketedTable("lineitem8", "lineitem", "l_orderkey", 8)
	ctx := context.Background()
	var total int64
	if err := h.session.QueryRowContext(ctx, "SELECT count(*) FROM lineitem").Scan(&total); err != nil {
		t.Fatal(err)
	}
	// Of every row: one machine's rows.
	for _, q := range []string{
		`SELECT count(*), sum(l_quantity) FROM lineitem TABLESAMPLE bernoulli(100%)`,
		`SELECT l_returnflag, count(*) FROM lineitem TABLESAMPLE system(100%) REPEATABLE (3) GROUP BY 1 ORDER BY 1`,
	} {
		if !h.check(q) {
			t.Errorf("%s ran on one machine", q)
		}
	}
	count := func(q string) int64 {
		t.Helper()
		rows, _, _, _, err := h.run(ctx, q, ModeOn)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var n int64
		for _, r := range rows {
			n += r[len(r)-1].(int64)
		}
		return n
	}
	for _, tc := range []struct {
		query  string
		lo, hi float64
	}{
		{`SELECT count(*) FROM lineitem TABLESAMPLE bernoulli(10%) REPEATABLE (42)`, 0.09, 0.11},
		{`SELECT count(*) FROM lineitem TABLESAMPLE system(20%) REPEATABLE (42)`, 0.08, 0.35},
		{`SELECT n_name, count(*) FROM lineitem TABLESAMPLE bernoulli(50%) REPEATABLE (7) JOIN supplier ON l_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey GROUP BY 1 ORDER BY 1`, 0.48, 0.52},
		{`SELECT l_orderkey, count(*) FROM lineitem8 TABLESAMPLE bernoulli(30%) REPEATABLE (5) GROUP BY 1 HAVING count(*) > 0 ORDER BY 1`, 0.28, 0.32},
		// A subquery passing the divided table's rows, sampled where they
		// are made.
		{`SELECT s.f, count(*) FROM (SELECT l_returnflag AS f, l_quantity FROM lineitem WHERE l_quantity > 0) AS s TABLESAMPLE bernoulli(40%) REPEATABLE (9)
			GROUP BY 1 ORDER BY 1`, 0.38, 0.42},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			first, again := count(tc.query), count(tc.query)
			if first != again {
				t.Errorf("a seeded sample gave %d rows, then %d", first, again)
			}
			if share := float64(first) / float64(total); share < tc.lo || share > tc.hi {
				t.Errorf("a sample of %.3f of the rows, want %.2f to %.2f", share, tc.lo, tc.hi)
			}
			p := h.planOf(tc.query)
			if !strings.Contains(p.describe(), "TABLESAMPLE") {
				t.Errorf("no stage samples:\n%s", p.describe())
			}
			if strings.Contains(tc.query, "lineitem8") && !strings.Contains(p.describe(), "buckets of main.lineitem8") {
				t.Errorf("not read by bucket:\n%s", p.describe())
			}
		})
	}
	// Without a seed, a sample still is one.
	h.t = t
	if n := count(`SELECT count(*) FROM lineitem TABLESAMPLE bernoulli(25%)`); float64(n) < 0.23*float64(total) || float64(n) > 0.27*float64(total) {
		t.Errorf("an unseeded sample of %d of %d rows", n, total)
	}
	for _, q := range []string{
		`SELECT count(*) FROM lineitem, nation TABLESAMPLE bernoulli(50%) WHERE l_suppkey % 25 = n_nationkey`,
		`SELECT count(*) FROM lineitem TABLESAMPLE reservoir(1000 ROWS)`,
	} {
		if _, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn); !IsUnsupported(err) {
			t.Errorf("%s: planned, %v", q, err)
		} else if !strings.Contains(err.Error(), "TABLESAMPLE") {
			t.Errorf("%s: %v", q, err)
		}
	}
	// A sample of a relation's rows: the coordinator samples what the
	// workers computed.
	// So does a sample of a subquery whose rows every task would make, or of
	// a number of rows, which workers send it.
	for _, q := range []string{
		`WITH s AS (SELECT l_orderkey, count(*) AS n FROM lineitem GROUP BY 1) SELECT count(*) > 0, sum(n) > 0 FROM s TABLESAMPLE bernoulli(50%)`,
		`SELECT count(*) > 0 FROM lineitem, (SELECT n_nationkey FROM nation) AS n TABLESAMPLE bernoulli(50%) WHERE l_suppkey % 25 = n.n_nationkey`,
		`SELECT count(*) FROM (SELECT l_orderkey FROM lineitem) AS s TABLESAMPLE reservoir(10 ROWS)`,
	} {
		h.check(q)
		if p := h.planOf(q); strings.Contains(p.describe(), "TABLESAMPLE") {
			t.Errorf("workers sample a relation:\n%s", p.describe())
		} else if final, _ := h.co.parser.print(p.final); !strings.Contains(final, "TABLESAMPLE") {
			t.Errorf("the coordinator does not sample: %s", final)
		}
	}
	_ = fmt.Sprint
}
