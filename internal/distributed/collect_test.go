package distributed

import (
	"context"
	"strings"
	"testing"
)

// list() and array_agg() combine from partial lists, whose order is
// arbitrary: the queries sort them.
func TestListAggregates(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	queries := []string{
		`SELECT l_returnflag, list_sort(list(l_linenumber)) AS lines, len(array_agg(l_partkey)) AS n FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT l_shipmode, list_sort(list(l_quantity) FILTER (WHERE l_quantity > 49)) AS q, count(*) FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT len(list(l_comment)), list_sort(list(DISTINCT l_returnflag)) FROM lineitem`,
		// No row passes the filter: NULL, as on one machine.
		`SELECT l_linestatus, list(l_quantity) FILTER (WHERE l_quantity < 0) AS none FROM lineitem GROUP BY 1 ORDER BY 1`,
	}
	for _, mode := range []string{aggGather, aggByGroup} {
		h.co.aggregation = mode
		for _, q := range queries {
			t.Run(mode+" "+short(q), func(t *testing.T) {
				h.t = t
				if !h.check(q) {
					t.Error("ran on one machine")
				}
			})
		}
	}
	h.co.aggregation = ""
	h.t = t
	// ORDER BY inside list() needs every value in order.
	if _, err := h.co.Plan(context.Background(), `SELECT l_returnflag, list(l_linenumber ORDER BY l_orderkey) FROM lineitem GROUP BY 1`,
		harnessEnv{h}, ModeOn); !IsUnsupported(err) || !strings.Contains(err.Error(), "ORDER BY inside list()") {
		t.Errorf("ORDER BY in list(): %v", err)
	}
}

// A group function runs where its groups are whole: the second stage of an
// aggregation by group computes the select items that call it, and the
// lists only they read stay on the workers.
func TestGroupFunctions(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.groupFunctions = map[string]bool{"list_sort": true}
	q := `SELECT l_returnflag AS flag, list_sort(list(l_linenumber))[1:3] AS first, count(*) AS n FROM lineitem GROUP BY 1 ORDER BY 1`
	if n := h.exchanged(q, "aggregate"); n != 1 {
		t.Fatalf("%d stages wrote partial results to an exchange", n)
	}
	_, _, plan, ex, err := h.run(context.Background(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	second := plan.Stages[len(plan.Stages)-1].SQL
	if !strings.Contains(second, "list_sort(") || !strings.Contains(second, "__s1") {
		t.Errorf("the second stage does not sort: %s", second)
	}
	// list() is the first aggregate (__f0), count(*) the second (__f1).
	if strings.Contains(ex.SQL, "list_sort(") || strings.Contains(second, "__f0") || !strings.Contains(second, "__f1") {
		t.Errorf("the coordinator sorts, or reads the lists: %s\n%s", ex.SQL, second)
	}
	// A group function the coordinator's HAVING reads too: the lists cross.
	h.check(`SELECT l_returnflag, list_sort(list(l_linenumber))[1] AS first FROM lineitem GROUP BY 1 HAVING len(list(l_linenumber)) > 10 ORDER BY 1`)
	// Without one, the coordinator completes few groups.
	h.groupFunctions = nil
	if n := h.exchanged(q, "aggregate"); n != 0 {
		t.Errorf("without group functions, %d stages wrote partial results to an exchange", n)
	}
}
