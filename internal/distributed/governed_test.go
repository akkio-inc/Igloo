package distributed

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
)

// governedEnv is the harness's session with a statement that masking and
// row access policies rewrote, as pgwire's rewrite writes them, with mark:
// customer is the protected table, and its row access policy reads nation.
type governedEnv struct {
	harnessEnv
	mark string
}

func (e governedEnv) RewriteMark() string { return e.mark }

func (e governedEnv) Table(ctx context.Context, parts []string) (*Table, bool, error) {
	t, ok, err := e.harnessEnv.Table(ctx, parts)
	if ok && t.Name == "customer" {
		c := *t
		c.Governed = true
		t = &c
	}
	return t, ok, err
}

// governedCustomer is customer as the rewrite reads it: its phone masked,
// the rows of one segment and of the nations of the last two regions hidden.
const governedCustomer = `(SELECT CASE WHEN "__igloo_t_m1_1"."__igloo_ok_m1" THEN "__igloo_t_m1_1".c_custkey END AS c_custkey,
	CASE WHEN "__igloo_t_m1_1"."__igloo_ok_m1" THEN CAST((CASE WHEN false THEN "__igloo_t_m1_1".c_phone ELSE '***' END) AS VARCHAR) END AS c_phone,
	CASE WHEN "__igloo_t_m1_1"."__igloo_ok_m1" THEN "__igloo_t_m1_1".c_nationkey END AS c_nationkey,
	CASE WHEN "__igloo_t_m1_1"."__igloo_ok_m1" THEN "__igloo_t_m1_1".c_acctbal END AS c_acctbal,
	CASE WHEN "__igloo_t_m1_1"."__igloo_ok_m1" THEN "__igloo_t_m1_1".c_mktsegment END AS c_mktsegment
	FROM (SELECT "__igloo_t_m1_1".c_custkey, "__igloo_t_m1_1".c_phone, "__igloo_t_m1_1".c_nationkey, "__igloo_t_m1_1".c_acctbal,
		"__igloo_t_m1_1".c_mktsegment, ("__igloo_t_m1_1".c_mktsegment <> 'BUILDING' AND EXISTS (SELECT 1 FROM
			(SELECT * FROM main.nation AS "__igloo_p_m1_2") AS n WHERE n.n_nationkey = "__igloo_t_m1_1".c_nationkey AND n.n_regionkey < 3)) AS "__igloo_ok_m1"
		FROM main.customer AS "__igloo_t_m1_1") AS "__igloo_t_m1_1" WHERE "__igloo_t_m1_1"."__igloo_ok_m1")`

// TestGovernedStages checks statements over a table read through its
// policies: they run on workers, give what they give on one machine, and
// every stage reads the table, and the table its row access policy reads,
// only through the policies' subquery as written, from the tables' files,
// however the statement joins, partitions, windows or aggregates them; and
// that the check of the stages finds every other read.
func TestGovernedStages(t *testing.T) {
	t.Parallel()
	h := newHarness(t, 0.01, 4, 2)
	ctx := context.Background()
	env := governedEnv{harnessEnv{h}, "m1"}
	g := governedCustomer
	for _, q := range []string{
		"SELECT c.c_mktsegment, count(*), sum(c.c_acctbal) FROM " + g + " AS c GROUP BY 1",
		"SELECT c_phone, count(DISTINCT c_custkey), min(c_phone) FROM " + g + " AS c GROUP BY 1",
		"SELECT count(*), sum(o.o_totalprice) FROM orders o JOIN " + g + " AS c ON o.o_custkey = c.c_custkey",
		"SELECT c.c_phone, count(*) FROM orders o JOIN " + g + " AS c ON o.o_custkey = c.c_custkey GROUP BY 1",
		"SELECT count(*) FROM " + g + " AS a JOIN " + g + " AS b ON a.c_custkey = b.c_custkey AND a.c_phone = b.c_phone",
		"SELECT count(*) FROM orders WHERE o_custkey IN (SELECT c_custkey FROM " + g + " AS c WHERE c_acctbal > 0)",
		"SELECT c_mktsegment, c_custkey, c_phone FROM " + g + " AS c QUALIFY row_number() OVER (PARTITION BY c_mktsegment ORDER BY c_acctbal DESC, c_custkey) <= 2",
		"WITH c AS (SELECT * FROM " + g + " AS c WHERE c_nationkey < 20) SELECT c_nationkey, count(*) FROM c GROUP BY 1",
	} {
		for _, broadcast := range []int64{0, 1 << 15} {
			h.co.SetMaxBroadcast(broadcast)
			want, _, err := h.query(ctx, q)
			if err != nil {
				t.Fatalf("%s: %v", short(q), err)
			}
			plan, err := h.co.Plan(ctx, q, env, ModeOn)
			if err != nil {
				t.Errorf("broadcast %d, %s: %v", broadcast, short(q), err)
				continue
			}
			ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
			if err != nil {
				t.Fatalf("%s: %v", short(q), err)
			}
			got, _, err := h.query(ctx, ex.SQL)
			ex.Close()
			if err != nil {
				t.Fatalf("%s: %v", short(q), err)
			}
			if msg := compareRows(want, got, false); msg != "" {
				t.Errorf("broadcast %d, %s: %s\n%s", broadcast, short(q), msg, plan.describe())
			}
			if broadcast > 0 && strings.Contains(q, " JOIN ") && plan.exchanges("join")+plan.exchanges("relation") == 0 {
				t.Errorf("%s partitions no rows: the test shows nothing\n%s", short(q), plan.describe())
			}
		}
	}
	h.co.SetMaxBroadcast(0)

	// The check itself, of stages as the planner could write them.
	stmt, err := h.co.parser.parse("SELECT count(*) FROM " + g + " AS c")
	if err != nil {
		t.Fatal(err)
	}
	pl := newPlanner(ctx, env, h.co.parser, h.co.aggregates(ctx, env), planOptions{})
	pl.gov = pl.newGovernance(stmt)
	customer, _, _ := env.Table(ctx, []string{"main", "customer"})
	nation, _, _ := env.Table(ctx, []string{"main", "nation"})
	inner := strings.NewReplacer("main.customer", "__igloo_split", "main.nation", "__igloo_t0")
	stage := func(sql string, inputs ...Input) *Stage {
		f, err := h.co.parser.parse(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return &Stage{fragment: f, Inputs: inputs}
	}
	split := Input{View: "__igloo_split", Table: customer, Split: true}
	whole := Input{View: "__igloo_t0", Table: nation}
	keyed := Input{View: "__igloo_k0", Exchange: &Exchange{ID: 1}, Partitioned: true}
	for _, c := range []struct {
		name string
		s    *Stage
		bad  bool
	}{
		{"the subquery as written", stage("SELECT count(*) FROM "+inner.Replace(g)+" AS c", split, whole), false},
		{"a relation of its rows", stage("SELECT c_custkey, c_phone FROM "+inner.Replace(g)+" AS r", split, whole), false},
		{"the table's rows as they are", stage(`SELECT "__igloo_t_m1_1".c_phone, "__igloo_t_m1_1".c_custkey FROM __igloo_split AS "__igloo_t_m1_1"`, split), true},
		{"under another alias", stage("SELECT c_phone FROM __igloo_split AS c", split), true},
		{"without its row access policy", stage("SELECT count(*) FROM "+strings.Replace(inner.Replace(g), `WHERE "__igloo_t_m1_1"."__igloo_ok_m1"`, "", 1)+" AS c", split, whole), true},
		{"over rows another stage wrote", stage("SELECT count(*) FROM "+strings.Replace(inner.Replace(g), "__igloo_split", "__igloo_k0", 1)+" AS c", keyed, whole), true},
		{"the policy's table outside it", stage(`SELECT n_nationkey FROM __igloo_t0 AS "__igloo_p_m1_2"`, whole), true},
		{"the filter without the masks", stage(`SELECT * FROM (SELECT "__igloo_t_m1_1".c_custkey, "__igloo_t_m1_1".c_phone, true AS "__igloo_ok_m1" FROM __igloo_split AS "__igloo_t_m1_1") AS "__igloo_t_m1_1"`, split), true},
	} {
		if bad := pl.governedReads([]*Stage{c.s}); (len(bad) > 0) != c.bad {
			t.Errorf("%s: flagged %q, want flagged %v", c.name, bad, c.bad)
		}
	}
	if bad := pl.governedReads([]*Stage{stage("SELECT count(*) FROM __igloo_split AS o", Input{View: "__igloo_split", Table: h.tables["orders"], Split: true})}); len(bad) > 0 {
		t.Errorf("a table without policies flagged: %q", bad)
	}
	if pl.gov.roots["__igloo_t_m1_1"] == "" {
		t.Errorf("the protected table's subquery was not found: %v", slices.Collect(maps.Keys(pl.gov.roots)))
	}
}
