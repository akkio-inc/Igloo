package distributed

import (
	"os"
	"strconv"
	"testing"
)

// TestExchangeLoop runs statements that two stages reading one table
// concurrently feed exchanges for, IGLOO_LOOP times, to look for results
// that depend on timing.
func TestExchangeLoop(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("IGLOO_LOOP"))
	if n == 0 {
		t.Skip("IGLOO_LOOP not set")
	}
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["customer"].Bytes
	q := `SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`
	q18 := `SELECT o_orderpriority, count(*) FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 250) GROUP BY 1 ORDER BY 1`
	for i := range n {
		if !h.check(q) || !h.check(q18) {
			t.Fatal("did not run distributed")
		}
		if t.Failed() {
			t.Fatalf("failed on iteration %d", i)
		}
	}
}
