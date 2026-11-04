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
		// read are the tables the plan reads of lineitem and part.
		lineitem, part bool
	}{
		{"one section kept", report("upper(t0.section) IN ('PARTS')", "PARTITION BY section", false), false, true},
		{"two sections kept", report("upper(t0.section) IN ('PARTS', 'LINE ITEMS')", "PARTITION BY section", false), true, true},
		{"a window over every section", report("upper(t0.section) IN ('PARTS')", "", false), true, true},
		{"a section read twice", report("upper(t0.section) IN ('PARTS')", "PARTITION BY section", true), true, true},
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
		if reads(plan, "lineitem") != c.lineitem || reads(plan, "part") != c.part {
			t.Errorf("%s: reads lineitem %v, part %v\n%s", c.name, reads(plan, "lineitem"), reads(plan, "part"), plan.describe())
		}
	}
}
