package distributed

import (
	"slices"
	"testing"
)

// TestPrunedBranches checks that a filter on a union's constant column, followed
// through a GROUP BY of it, a window partitioned by it and common table
// expressions read once, leaves the union's branches it rejects unplanned, and
// the statement's rows as one machine's; and that branches stay where the filter
// cannot be followed so far.
func TestPrunedBranches(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	h.tables["lineitem"].Bytes = 1 << 34
	h.tables["orders"].Bytes = 1 << 32
	h.tables["part"].Bytes = 1 << 33
	// report is shaped like a profile report: sections of a union, each named
	// by a constant, grouped, ranked and filtered from outside; the first
	// branch's section is what is pruned first, so the union's names must stay.
	report := func(filter, window string, twice bool) string {
		other := ""
		if twice {
			other = ", (SELECT count(*) AS c FROM agg) AS t1"
		}
		return `WITH sel AS (SELECT DISTINCT o_custkey AS k FROM orders WHERE o_orderkey < 100000),
			items AS (SELECT CAST('line items' AS VARCHAR) AS section, l_quantity AS v FROM lineitem JOIN sel ON l_partkey = sel.k
				UNION ALL SELECT CAST('parts' AS VARCHAR) AS sec, p_retailprice AS v FROM part JOIN sel ON p_partkey = sel.k
				UNION ALL SELECT CAST('orders' AS VARCHAR), o_totalprice FROM orders JOIN sel ON o_orderkey = sel.k),
			agg AS (SELECT section, count(*) AS n, avg(v) AS a FROM items GROUP BY section),
			ranked AS (SELECT section, n, a, row_number() OVER (` + window + ` ORDER BY n DESC) AS r FROM agg)
			SELECT t0.* FROM (SELECT section, n, a, r FROM ranked ORDER BY section) AS t0` + other + ` WHERE ` + filter + ` AND t0.r = 1`
	}
	reads := func(p *Plan, table string) bool {
		return slices.ContainsFunc(p.Stages, func(s *Stage) bool {
			return slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Table != nil && in.Table.Name == table })
		})
	}
	for _, c := range []struct {
		name, query string
		// lineitem and part say whether the plan reads them, and pruned is how
		// many branches it leaves out.
		lineitem, part bool
		pruned         int
	}{
		{"one section kept", report("upper(t0.section) IN ('PARTS')", "PARTITION BY section", false), false, true, 2},
		{"two sections kept", report("upper(t0.section) IN ('PARTS', 'LINE ITEMS')", "PARTITION BY section", false), true, true, 1},
		{"a window over every section", report("upper(t0.section) IN ('PARTS')", "", false), true, true, 0},
		{"a section read twice", report("upper(t0.section) IN ('PARTS')", "PARTITION BY section", true), true, true, 0},
	} {
		want, _, err := h.query(t.Context(), c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got, _, plan, _, err := h.run(t.Context(), c.query, ModeOn)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if msg := compareRows(want, got, false); msg != "" {
			t.Errorf("%s: %s\n%s", c.name, msg, plan.describe())
		}
		if reads(plan, "lineitem") != c.lineitem || reads(plan, "part") != c.part || plan.pruned != c.pruned {
			t.Errorf("%s: reads lineitem %v, part %v; left out %d branches\n%s", c.name, reads(plan, "lineitem"), reads(plan, "part"), plan.pruned, plan.describe())
		}
	}
}

// TestBranchesStayUnderDistinctOn checks that a filter on a union's constant
// column leaves its branches be under a DISTINCT ON of other columns, which picks
// a row of each group that the filter may then reject: one machine returns none
// of the groups, as their first rows are the left-out section's.
func TestBranchesStayUnderDistinctOn(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	h.tables["lineitem"].Bytes = 1 << 34
	h.tables["orders"].Bytes = 1 << 32
	q := `SELECT * FROM (SELECT DISTINCT ON (k) sec, k FROM (SELECT 'a' AS sec, l_partkey % 7 AS k FROM lineitem
		UNION ALL SELECT 'b', o_orderkey % 7 FROM orders) AS u ORDER BY k, sec) AS t WHERE t.sec = 'b'`
	want, _, err := h.query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	got, _, plan, _, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, false); msg != "" || plan.pruned != 0 {
		t.Errorf("%s; left out %d\n%s", msg, plan.pruned, plan.describe())
	}
}
