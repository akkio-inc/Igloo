package distributed

import (
	"context"
	"slices"
	"testing"
)

// TestExpressionJoinKeys checks joins of inputs too large for every worker
// to read on the same expression of a column on each side, as reports
// join metrics on upper(...) of their names: each input is partitioned by
// the expression's value, rows that differ but for case meet, NULLs meet
// nothing, and the rows are one machine's. Different expressions on the
// two sides, or columns of different types, are no such join.
func TestExpressionJoinKeys(t *testing.T) {
	h := newHarness(t, 0.02, 4, 3)
	h.co.maxBroadcast = 1
	ctx := context.Background()
	for _, q := range []string{
		`WITH a AS (SELECT CASE WHEN c_custkey % 50 = 0 THEN NULL ELSE lower(c_mktsegment) END AS k, count(*) AS n FROM customer GROUP BY 1),
			b AS (SELECT CASE WHEN c_custkey % 70 = 0 THEN NULL ELSE upper(c_mktsegment) END AS k, sum(c_acctbal) AS s, count(*) AS m FROM customer GROUP BY 1)
		SELECT upper(a.k) AS k, sum(a.n * b.m) AS nm, count(*) AS pairs FROM a JOIN b ON upper(a.k) = upper(b.k) GROUP BY 1 ORDER BY 1`,
		`WITH a AS (SELECT ' ' || o_orderpriority AS p, o_orderstatus AS st, count(*) AS n FROM orders GROUP BY 1, 2),
			b AS (SELECT o_orderpriority || '  ' AS p, lower(o_orderstatus) AS st, sum(o_totalprice) AS s FROM orders GROUP BY 1, 2)
		SELECT DISTINCT a.p, a.st, a.n, round(b.s, 2) AS s FROM a JOIN b ON trim(a.p) = trim(b.p) AND upper(a.st) = upper(b.st) ORDER BY 1, 2`,
		`SELECT count(*), count(DISTINCT l_orderkey) FROM lineitem JOIN orders ON upper(l_comment) = upper(o_comment)`,
	} {
		if !h.check(q) {
			t.Errorf("ran on one machine: %s", short(q))
			continue
		}
		p := h.planOf(q)
		if !slices.ContainsFunc(p.Stages, func(s *Stage) bool {
			return s.Out != nil && slices.ContainsFunc(s.Out.Casts, keyExpression)
		}) {
			t.Errorf("no exchange partitioned by an expression of its key: %s\n%s", short(q), p.describe())
		}
	}
	for _, q := range []string{
		`SELECT count(*) FROM lineitem JOIN orders ON upper(l_comment) = lower(o_comment)`,
		`SELECT count(*) FROM lineitem JOIN orders ON upper(CAST(l_orderkey AS VARCHAR)) = upper(o_comment)`,
	} {
		if _, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn); err == nil || !IsUnsupported(err) {
			t.Errorf("%s: planned (%v), want it on one machine", short(q), err)
		}
	}
}
