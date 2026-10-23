package distributed

import (
	"strings"
	"testing"
)

// TestExplain checks what EXPLAIN DISTRIBUTED says of a partitioned join:
// where it runs and why, its estimate and partitions, each stage with its
// SQL and what the coordinator runs, and that explaining runs no task and
// leaves the history alone.
func TestExplain(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.co.maxBroadcast = 1
	q := `SELECT o.o_orderpriority, count(*) FROM lineitem AS l JOIN orders AS o ON o.o_orderkey = l.l_orderkey GROUP BY 1`
	rows, err := h.co.Explain(t.Context(), q, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	stages := 0
	for _, r := range rows {
		got[r[0]] = r[1]
		if strings.HasPrefix(r[0], "stage ") {
			stages++
			if r[1] == "" {
				t.Errorf("%s has no SQL", r[0])
			}
		}
	}
	if p := h.planOf(q); stages != len(p.Stages) || stages < 3 {
		t.Errorf("%d stages explained, plan has %d:\n%v", stages, len(p.Stages), rows)
	}
	if got["runs on"] != "workers" || got["reason"] != "distributed_execution is on" || got["partitions"] == "" ||
		!strings.Contains(got["estimate"], "levels") || !strings.Contains(got["the coordinator"], "SELECT") {
		t.Errorf("explained as %v", rows)
	}
	// Auto mode, which keeps a table this small on one machine.
	rows, err = h.co.Explain(t.Context(), q, harnessEnv{h}, ModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0] != [2]string{"runs on", "this replica"} || rows[1][1] == "" {
		t.Errorf("auto: %v", rows)
	}
	if _, found := h.co.history.get(h.planOf(q).Key); found {
		t.Error("explaining wrote the history")
	}
}
