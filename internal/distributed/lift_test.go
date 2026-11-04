package distributed

import (
	"slices"
	"testing"
)

// TestLiftedScalars checks that a statement whose columns are aggregates of a
// large table in scalar subqueries, over a FROM of a row of a small one, as
// TPC-DS q09's are, runs on workers with the rows one machine returns, the large
// table read by stages; that one of a small table stays on the coordinator;
// and that one whose subquery may have no row or several stays on one machine.
func TestLiftedScalars(t *testing.T) {
	h := newHarness(t, 0.01, 2, 2)
	h.tables["lineitem"].Bytes = 1 << 34
	reads := func(p *Plan, table string) bool {
		return slices.ContainsFunc(p.Stages, func(s *Stage) bool {
			return slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Table != nil && in.Table.Name == table })
		})
	}
	q := `SELECT CASE WHEN (SELECT count(*) FROM lineitem WHERE l_quantity BETWEEN 1 AND 20) > 1000
			THEN (SELECT avg(l_extendedprice) FROM lineitem WHERE l_quantity BETWEEN 1 AND 20)
			ELSE (SELECT avg(l_discount) FROM lineitem WHERE l_quantity BETWEEN 1 AND 20) END AS bucket1,
		(SELECT max(l_shipdate) FROM lineitem WHERE l_quantity > 45) AS last, (SELECT sum(l_tax) FROM lineitem WHERE l_tax < 0) AS none, r_name
		FROM region WHERE r_regionkey = 1`
	want, _, err := h.query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	got, _, plan, _, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, false); msg != "" || !reads(plan, "lineitem") {
		t.Errorf("%s; reads lineitem %v\n%s", msg, reads(plan, "lineitem"), plan.describe())
	}
	// One the coordinator may read itself stays there, without a stage.
	q = `SELECT x.c, (SELECT count(*) FROM region) AS r FROM (SELECT count(*) AS c FROM lineitem) AS x`
	if !h.check(q) {
		t.Error("did not run distributed")
	}
	if plan, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeOn); err != nil || reads(plan, "region") {
		t.Errorf("a small subquery: %v, want it read by the coordinator", err)
	}
	// A subquery of rows, not of an aggregate, may have none or several.
	q = `SELECT (SELECT l_orderkey FROM lineitem WHERE l_orderkey < 0) AS k, r_name FROM region`
	if _, _, _, _, err := h.run(t.Context(), q, ModeOn); !IsUnsupported(err) {
		t.Errorf("a subquery of rows: %v, want it on one machine", err)
	}
}
