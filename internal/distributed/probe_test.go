package distributed

import (
	"strings"
	"testing"
)

// TestUncertainRelations checks which relations count as of uncertain
// size: one filtered by a constant, read by partition with a large table
// partitioned to meet it; not one without a filter, nor one only its own
// stage reads by partition while the table is read in full.
func TestUncertainRelations(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	plan := func(q string, broadcast int64) *Plan {
		t.Helper()
		h.co.maxBroadcast = broadcast
		p, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeOn)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	filtered := `WITH aud AS (SELECT DISTINCT l_orderkey AS k FROM lineitem WHERE l_quantity = 1)
		SELECT o.o_orderpriority, count(*) FROM orders AS o JOIN aud ON aud.k = o.o_orderkey GROUP BY 1`
	if p := plan(filtered, 1); len(p.uncertain) != 1 || p.uncertain[0].name != "aud" {
		t.Errorf("a filtered relation partitioned with orders: %v\n%s", p.uncertain, p.describe())
	}
	byScalar := strings.Replace(filtered, "l_quantity = 1", "l_quantity = (SELECT max(l_quantity) FROM lineitem)", 1)
	if p := plan(byScalar, 1); len(p.uncertain) != 1 {
		t.Errorf("filtered by a scalar subquery: %v\n%s", p.uncertain, p.describe())
	}
	whole := strings.Replace(filtered, " WHERE l_quantity = 1", "", 1)
	if p := plan(whole, 1); len(p.uncertain) != 0 {
		t.Errorf("an unfiltered relation: %v", p.uncertain)
	}
	if p := plan(filtered, h.tables["orders"].Bytes); len(p.uncertain) != 0 {
		t.Errorf("orders read in full: %v\n%s", p.uncertain, p.describe())
	}
}

// TestKeepUncertainLocal checks auto mode's choice for a statement that
// partitions large tables to meet a relation of uncertain size: one
// machine, unless workers ran it faster or one machine ran out of memory.
func TestKeepUncertainLocal(t *testing.T) {
	u := []uncertain{{name: "aud"}}
	if on, _, reason := keepUncertainLocal(u, runs{}, false, true, false, "estimated"); on || !strings.Contains(reason, "aud") {
		t.Errorf("no history: on workers %v (%s)", on, reason)
	}
	if on, explore, _ := keepUncertainLocal(u, runs{local: 30}, true, true, true, "trying workers"); on || explore {
		t.Error("explored workers")
	}
	if on, _, _ := keepUncertainLocal(u, runs{local: 300, dist: 100}, true, true, false, "faster"); !on {
		t.Error("kept local though workers ran it faster")
	}
	if on, _, _ := keepUncertainLocal(u, runs{outOfMemory: true}, true, true, false, "ran out"); !on {
		t.Error("kept local though one machine ran out of memory")
	}
	if on, _, _ := keepUncertainLocal(nil, runs{}, false, true, false, "estimated"); !on {
		t.Error("kept local with nothing uncertain")
	}
}
