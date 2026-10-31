package distributed

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
)

// TestHLLSketches checks HLL sketches of the datasketches extension, as
// APPROX_COUNT_DISTINCT and HLL_ACCUMULATE count with them: workers sketch
// their shares, and the coordinator or a stage that completes the groups
// merges the sketches (datasketch_hll_union), over every row, by group, and
// sketches of a relation's groups merged after. Estimates are one machine's
// within a sketch's error (one machine merges its threads' sketches too),
// and the exact counts' within it as well. A sketch of a size that is not
// a constant runs on one machine.
func TestHLLSketches(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the datasketches extension is not here")
	}
	h := newHarnessWith(t, 0.05, 6, 3, "", "datasketches")
	ctx := context.Background()
	approx := func(arg string) string {
		return "CAST(coalesce(round(datasketch_hll_estimate(datasketch_hll(12, CASE WHEN " + arg + " IS NOT NULL THEN hash(" + arg + ") END))), 0) AS BIGINT)"
	}
	near := func(a, b, within float64) bool { return math.Abs(a-b) <= within*math.Max(math.Abs(a), math.Abs(b))+1 }
	values := func(rows [][]any) []float64 {
		var out []float64
		for _, r := range rows {
			switch v := r[len(r)-1].(type) {
			case int64:
				out = append(out, float64(v))
			case float64:
				out = append(out, v)
			}
		}
		return out
	}
	for _, tc := range []struct {
		query, exact string
	}{
		{`SELECT ` + approx("l_partkey") + ` FROM lineitem`, `SELECT count(DISTINCT l_partkey) FROM lineitem`},
		{`SELECT l_returnflag, ` + approx("l_orderkey") + ` FROM lineitem GROUP BY 1 ORDER BY 1`,
			`SELECT l_returnflag, count(DISTINCT l_orderkey) FROM lineitem GROUP BY 1 ORDER BY 1`},
		{`SELECT l_suppkey % 40 AS b, datasketch_hll_estimate(datasketch_hll(12, hash(l_partkey))) FROM lineitem GROUP BY 1 ORDER BY 1`,
			`SELECT l_suppkey % 40 AS b, count(DISTINCT l_partkey) FROM lineitem GROUP BY 1 ORDER BY 1`},
		// HLL_ACCUMULATE by day, HLL_COMBINE after.
		{`WITH d AS (SELECT l_shipdate AS day, datasketch_hll(12, hash(l_orderkey)) AS s FROM lineitem GROUP BY 1)
			SELECT datasketch_hll_estimate(datasketch_hll_union(12, s)) FROM d`, `SELECT count(DISTINCT l_orderkey) FROM lineitem`},
	} {
		for _, mode := range []string{aggGather, aggByGroup, ""} {
			t.Run(mode+" "+short(tc.query), func(t *testing.T) {
				h.t = t
				h.co.aggregation = mode
				defer func() { h.co.aggregation = "" }()
				want, _, err := h.query(ctx, tc.query)
				if err != nil {
					t.Fatal(err)
				}
				exact, _, err := h.query(ctx, tc.exact)
				if err != nil {
					t.Fatal(err)
				}
				got, _, plan, _, err := h.run(ctx, tc.query, ModeOn)
				if err != nil {
					t.Fatalf("distributed: %v", err)
				}
				g, w, x := values(got), values(want), values(exact)
				if len(g) != len(w) || len(g) != len(x) || len(g) == 0 {
					t.Fatalf("%d rows, want %d", len(g), len(w))
				}
				for i := range g {
					if !near(g[i], w[i], 0.03) || !near(g[i], x[i], 0.05) {
						t.Errorf("row %d: %.0f on workers, %.0f on one machine, %.0f exactly", i, g[i], w[i], x[i])
					}
				}
				if d := plan.describe(); !strings.Contains(d, "datasketch_hll(") {
					t.Errorf("workers do not sketch:\n%s", d)
				}
			})
		}
	}
	h.t = t
	q := `SELECT datasketch_hll_estimate(datasketch_hll(CAST(l_linenumber AS INTEGER) + 9, hash(l_partkey))) FROM lineitem`
	if _, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn); !IsUnsupported(err) {
		t.Errorf("a sketch of a computed size: %v", err)
	}
	_ = fmt.Sprint
}
