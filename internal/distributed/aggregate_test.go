package distributed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// exchanged runs query distributed, compares its result with one
// machine's, and returns how many stages of its plan wrote an exchange for
// purpose.
func (h *harness) exchanged(query, purpose string) int {
	h.t.Helper()
	if !h.check(query) {
		h.t.Fatal("did not run distributed")
	}
	plan, err := h.co.Plan(context.Background(), query, harnessEnv{h}, ModeOn)
	if err != nil {
		h.t.Fatal(err)
	}
	return plan.exchanges(purpose)
}

// TestPartitionedAggregation checks aggregations whose groups workers
// complete, partition by partition, rather than the coordinator: those
// over DISTINCT values, by the values or by group, and the top groups of
// many.
func TestPartitionedAggregation(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	for _, tc := range []struct {
		query string
		// exchange says whether the partial results go through an exchange.
		exchange bool
	}{
		// By value: count, sum, min, max and avg over DISTINCT values add up
		// over partitions that keep each value apart.
		{`SELECT l_returnflag, count(DISTINCT l_partkey) AS parts, count(*) AS n FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT count(DISTINCT l_partkey) FROM lineitem`, true},
		{`SELECT count(DISTINCT l_suppkey), avg(DISTINCT l_suppkey), sum(DISTINCT l_suppkey), max(DISTINCT l_suppkey) FROM lineitem WHERE l_quantity > 1000`, true},
		{`SELECT l_shipmode, count(DISTINCT l_partkey) FROM lineitem WHERE l_quantity > 1000 GROUP BY 1`, true},
		{`SELECT l_shipmode AS m, sum(DISTINCT l_suppkey) AS s, avg(DISTINCT l_suppkey) AS a, min(DISTINCT l_suppkey), sum(l_quantity), avg(l_discount), count(*) FILTER (WHERE l_tax > 0.04)
			FROM lineitem GROUP BY ALL HAVING count(DISTINCT l_suppkey) > 10 ORDER BY m`, true},
		// A FILTER on a DISTINCT aggregate that leaves NULLs out drops values
		// as CASE WHEN would.
		{`SELECT l_shipmode, count(DISTINCT l_partkey) FILTER (WHERE l_quantity > 25) AS parts, count(*) FILTER (WHERE l_quantity > 25) AS n,
			sum(l_quantity) FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT avg(DISTINCT l_suppkey) FILTER (WHERE l_tax > 0.04), max(DISTINCT l_suppkey) FILTER (WHERE l_tax > 0.04) FROM lineitem`, true},
		// Arguments that one of them fixes: partitioned by that one.
		{`SELECT l_shipmode, count(DISTINCT l_orderkey) AS orders, count(DISTINCT CASE WHEN l_orderkey IS NOT NULL AND l_linenumber IS NOT NULL
			THEN (l_orderkey, l_linenumber) END) AS lines FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT l_returnflag, count(DISTINCT l_partkey) AS n, rank() OVER (ORDER BY count(DISTINCT l_partkey) DESC) AS r FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		// By group: aggregates over DISTINCT values that do not add up, or
		// over arguments none of which fixes the others.
		{`SELECT l_returnflag, median(DISTINCT l_quantity) AS m, count(*) FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT l_linestatus, count(DISTINCT l_partkey) AS p, count(DISTINCT l_suppkey) AS s, sum(l_quantity) FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		// The top groups of many: each partition keeps its first groups.
		{`SELECT l_orderkey, sum(l_extendedprice) AS revenue FROM lineitem GROUP BY 1 ORDER BY revenue DESC, l_orderkey LIMIT 10`, true},
		{`SELECT l_orderkey, l_suppkey, count(*) AS n, max(l_shipdate) AS last FROM lineitem GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 5 OFFSET 3`, true},
		{`SELECT o.l_orderkey AS k, sum(o.l_quantity) FROM lineitem o JOIN supplier s ON o.l_suppkey = s.s_suppkey GROUP BY k ORDER BY 2 DESC, k LIMIT 7`, true},
		// Grouping sets are applied by the coordinator, over every worker's
		// finest groups.
		{`SELECT l_returnflag, l_linestatus, count(DISTINCT l_partkey) FROM lineitem GROUP BY ROLLUP (1, 2) ORDER BY 1, 2`, false},
		{`SELECT l_returnflag, l_linestatus, GROUPING(l_linestatus) AS total, count(DISTINCT l_partkey) FILTER (WHERE l_discount > 0.05) AS parts
			FROM lineitem GROUP BY GROUPING SETS ((l_returnflag, l_linestatus), ()) ORDER BY 1, 2`, false},
		{`SELECT l_returnflag, sum(l_quantity) FROM lineitem GROUP BY 1 ORDER BY 1 LIMIT 2`, true},
		// Groups of the tables every worker reads in full are few: the
		// coordinator combines them.
		{`SELECT n_name, count(*) FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY 1 ORDER BY 2 DESC LIMIT 3`, false},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if n := h.exchanged(tc.query, "aggregate"); (n > 0) != tc.exchange {
				t.Errorf("%d stages wrote partial results to an exchange, want exchange %v", n, tc.exchange)
			}
		})
	}
}

// TestAggregationModes runs the same aggregations with their groups
// completed by the coordinator, by group and by value, which must agree.
func TestAggregationModes(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	queries := []string{
		`SELECT l_returnflag, l_linestatus, count(*), sum(l_quantity), avg(l_discount), min(l_shipdate), max(l_comment), count(l_comment) FROM lineitem GROUP BY ALL ORDER BY ALL`,
		`SELECT l_shipmode, count(DISTINCT l_orderkey) AS o, sum(l_quantity) AS q FROM lineitem GROUP BY 1 HAVING sum(l_quantity) > 100 ORDER BY 1`,
		`SELECT l_orderkey, sum(l_quantity) AS q FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 250 ORDER BY 1`,
		`SELECT l_orderkey % 7 AS k, count(*) AS n FROM lineitem GROUP BY k QUALIFY rank() OVER (ORDER BY n DESC) <= 3 ORDER BY k`,
		// A ranking of many groups, whose first the partitions keep, and
		// one beside another window, which needs every group.
		`SELECT l_partkey % 50 AS b, l_suppkey, count(*) AS n FROM lineitem GROUP BY 1, 2
			QUALIFY dense_rank() OVER (PARTITION BY b ORDER BY n DESC) <= 2 ORDER BY 1, 3 DESC, 2`,
		`SELECT l_suppkey, count(*) AS n, sum(count(*)) OVER () AS total FROM lineitem GROUP BY 1
			QUALIFY row_number() OVER (ORDER BY n DESC, l_suppkey) <= 5 ORDER BY 2 DESC, 1`,
		`SELECT DISTINCT l_returnflag, count(*) > 0 AS any FROM lineitem GROUP BY 1 ORDER BY 1`,
		// GROUPING() of one grouping set, which the coordinator projects
		// whole groups of: 0.
		`SELECT l_returnflag, l_linestatus, GROUPING(l_linestatus) AS g, GROUPING(l_returnflag, l_linestatus) AS gg, count(DISTINCT l_suppkey) AS s
			FROM lineitem GROUP BY 1, 2 HAVING GROUPING(l_returnflag) = 0 ORDER BY 1, 2`,
		`SELECT l_partkey, count(*) AS n, bool_or(l_quantity > 45), bit_xor(l_linenumber), any_value(l_returnflag) IS NOT NULL FROM lineitem GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 20`,
		// Partial sums of BIGINT are HUGEINT, which Parquet would make DOUBLE.
		`SELECT l_partkey % 5 AS k, sum(l_orderkey * 100000000000 + l_linenumber) AS big, count(DISTINCT l_suppkey) FROM lineitem GROUP BY 1 ORDER BY 1`,
	}
	for _, mode := range []string{aggGather, aggByGroup, ""} {
		h.co.aggregation = mode
		for _, q := range queries {
			t.Run(mode+" "+short(q), func(t *testing.T) {
				h.t = t
				h.exchanged(q, "aggregate")
			})
		}
	}
	h.co.aggregation = ""

	// Under a collation, 'A' and 'a' are one group but hash apart: the
	// coordinator completes the groups.
	h.t = t
	h.exec("SET default_collation = 'nocase'")
	defer h.exec("RESET default_collation")
	// Which spelling stands for a group is arbitrary, so the result leaves
	// it out.
	q := `SELECT count(DISTINCT l_partkey) AS n, count(*) AS rows FROM lineitem
		GROUP BY CASE WHEN l_linenumber % 2 = 0 THEN lower(l_returnflag) ELSE l_returnflag END ORDER BY 1, 2`
	if n := h.exchanged(q, "aggregate"); n > 0 {
		t.Errorf("under a collation, %d stages wrote partial results to an exchange", n)
	}
}

// TestItemOf checks which select item a subquery's column comes from,
// around a star.
func TestItemOf(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer p.close() //nolint:errcheck
	for _, tc := range []struct {
		query string
		cols  []string
		// want are the items' positions, -1 for none.
		want []int
	}{
		{`SELECT a, b + 1 AS c FROM t`, []string{"a", "c"}, []int{0, 1}},
		// A star of three columns, then two items.
		{`SELECT s.*, CAST(s.id AS UINTEGER) AS member, keep FROM t AS s`, []string{"id", "x", "y", "member", "keep"}, []int{-1, -1, -1, 1, 2}},
		{`SELECT CAST(id AS UINTEGER) AS member, * FROM t`, []string{"member", "id", "x"}, []int{0, -1, -1}},
		// Two stars: no telling.
		{`SELECT a.*, b.*, 1 AS one FROM a, b`, []string{"x", "y", "z", "w", "one"}, []int{-1, -1, -1, -1, -1}},
		// Named otherwise than the item at its place.
		{`SELECT *, 1 AS one FROM t`, []string{"x", "y", "other"}, []int{-1, -1, -1}},
	} {
		n, err := p.parse(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		items := list(n, "select_list")
		for i, name := range tc.cols {
			got := itemOf(items, i, len(tc.cols), name)
			at := slices.IndexFunc(items, func(it any) bool {
				return got != nil && reflect.ValueOf(it.(node)).Pointer() == reflect.ValueOf(got).Pointer()
			})
			if at != tc.want[i] {
				t.Errorf("%s, column %d (%s): item %d, want %d", tc.query, i, name, at, tc.want[i])
			}
		}
	}
}

// TestDetermined checks which columns a DISTINCT argument's value fixes.
func TestDetermined(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	it := &fromItem{name: "t", columns: []string{"a", "b", "c"}}
	sc := &scope{items: []*fromItem{it}}
	for _, tc := range []struct {
		expr string
		want []string
	}{
		{"a", []string{"a"}},
		{"t.b", []string{"b"}},
		{"(a, b)", []string{"a", "b"}},
		{"struct_pack(x := a, y := (b, c))", []string{"a", "b", "c"}},
		{"CASE WHEN a IS NOT NULL AND b IS NOT NULL THEN (a, b) END", []string{"a", "b"}},
		{"CASE WHEN c > 0 THEN (a, b) WHEN c < 0 THEN (a, b) END", []string{"a", "b"}},
		{"CASE WHEN c > 0 THEN (a, b) WHEN c < 0 THEN (b, a) END", nil},
		{"CASE WHEN c > 0 THEN (a, b) ELSE (a, c) END", nil},
		{"CAST(a AS INTEGER)", nil},
		{"a + 1", nil},
	} {
		e, err := p.expr(tc.expr)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, ic := range determined(namedRows(e), sc) {
			got = append(got, ic.name)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s fixes %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// TestTPCHByGroup runs TPC-H with every aggregation completing its groups
// on workers, partition by partition.
func TestTPCHByGroup(t *testing.T) {
	h := newHarness(t, 0.05, 6, 3)
	h.co.aggregation = aggByGroup
	var exchanged []string
	for q := 1; q <= 22; q++ {
		b, err := os.ReadFile(filepath.Join("..", "..", "bench", "queries", "tpch", fmt.Sprintf("q%02d.sql", q)))
		if err != nil {
			t.Fatal(err)
		}
		t.Run(fmt.Sprintf("q%02d", q), func(t *testing.T) {
			h.t = t
			if !h.check(string(b)) {
				return
			}
			plan, err := h.co.Plan(t.Context(), string(b), harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			if plan.exchanges("aggregate") > 0 {
				exchanged = append(exchanged, fmt.Sprintf("q%02d", q))
			}
		})
	}
	t.Logf("completed groups on workers: %v", exchanged)
	if len(exchanged) < 8 {
		t.Errorf("only %d TPC-H queries completed their groups on workers", len(exchanged))
	}
}

// TestDistinctInPlace checks aggregates over DISTINCT values in a
// partitioned join on those values: each partition holds every row of
// each value, so the stage computes them and the coordinator adds them up,
// with no exchange of partial results.
func TestDistinctInPlace(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	h.co.maxBroadcast = h.tables["orders"].Bytes - 1
	for _, tc := range []struct {
		query   string
		inPlace bool
	}{
		{`SELECT o_orderpriority, count(DISTINCT o_orderkey) AS orders, count(DISTINCT l_orderkey) AS o2, count(*) AS n, sum(l_quantity) AS q
			FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`, true},
		{`SELECT count(DISTINCT o_orderkey), avg(DISTINCT l_orderkey), max(DISTINCT o_orderkey) FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_shipmode = 'AIR'`, true},
		{`SELECT l_shipmode, count(DISTINCT CASE WHEN o_orderkey IS NOT NULL AND l_linenumber IS NOT NULL THEN (o_orderkey, l_linenumber) END) AS lines,
			count(DISTINCT o_orderkey) FROM lineitem JOIN orders ON o_orderkey = l_orderkey GROUP BY 1 ORDER BY 1`, true},
		// Values the join key does not fix go through an exchange.
		{`SELECT o_orderpriority, count(DISTINCT o_custkey) FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`, false},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if !h.partitioned(tc.query) {
				t.Fatal("did not partition the join")
			}
			n := h.exchanged(tc.query, "aggregate")
			if (n == 0) != tc.inPlace {
				t.Errorf("%d stages wrote partial results to an exchange; want in place %v", n, tc.inPlace)
			}
		})
	}
}

// TestBitmapDistinct checks the plan of a count(DISTINCT) of 32-bit
// unsigned IDs with roaring bitmaps on: workers send a bitmap of their
// values a group (rb_agg), no value, and the coordinator counts the union;
// off, or over another type, the values cross as before.
func TestBitmapDistinct(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	q := `SELECT l_shipmode, count(DISTINCT CAST(l_orderkey AS UINTEGER)) AS o, sum(l_quantity) AS q FROM lineitem GROUP BY 1 ORDER BY 1`
	plain := `SELECT l_shipmode, count(DISTINCT l_orderkey) AS o FROM lineitem GROUP BY 1 ORDER BY 1`
	sql := func(p *Plan) string {
		var b strings.Builder
		for _, s := range p.Stages {
			b.WriteString(s.SQL + "\n")
		}
		if f, err := h.co.parser.print(p.final); err == nil {
			b.WriteString(f)
		}
		return b.String()
	}
	on, off := true, false
	for _, mode := range []string{aggGather, aggByGroup, ""} {
		h.co.aggregation = mode
		h.co.bitmaps = &on
		got := sql(h.planOf(q))
		if !strings.Contains(got, "rb_agg(") || !strings.Contains(got, "rb_cardinality(rb_or_agg(") || strings.Contains(got, "__dk") {
			t.Errorf("%q, bitmaps on:\n%s", mode, got)
		}
		if got := sql(h.planOf(plain)); strings.Contains(got, "rb_agg(") {
			t.Errorf("%q, a BIGINT counted distinct with bitmaps:\n%s", mode, got)
		}
		h.co.bitmaps = &off
		if got := sql(h.planOf(q)); strings.Contains(got, "rb_") || !strings.Contains(got, "__dk") {
			t.Errorf("%q, bitmaps off:\n%s", mode, got)
		}
	}
	h.co.aggregation, h.co.bitmaps = "", nil
	// Without the extension, bitmaps are off whatever the session says.
	if got := sql(h.planOf(q)); strings.Contains(got, "rb_") {
		t.Errorf("without the extension:\n%s", got)
	}
}

// TestBitmapDistinctResults checks that with roaring bitmaps on, the
// workers' bitmaps (rb_agg), sent to the coordinator, through an exchange to
// a second stage, or both, count what one machine does, beside aggregates
// over DISTINCT values that cross as values.
func TestBitmapDistinctResults(t *testing.T) {
	rb := os.Getenv("IGLOO_TEST_RB_EXTENSION")
	if rb == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	h := newHarnessRB(t, 0.02, 4, 3, rb)
	h.exec("SET rb_count_distinct = 'always'")
	queries := []string{
		`SELECT l_shipmode, count(DISTINCT CAST(l_orderkey AS UINTEGER)) AS o, sum(l_quantity) AS q FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT count(DISTINCT CAST(l_partkey AS UINTEGER)) AS p, count(DISTINCT CAST(l_suppkey AS UINTEGER)) AS s, count(*) FROM lineitem`,
		`SELECT l_returnflag, count(DISTINCT CAST(CASE WHEN l_quantity > 25 THEN l_orderkey END AS UINTEGER)) AS big,
			count(DISTINCT l_suppkey) AS s FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT o.o_orderpriority, count(DISTINCT CAST(l.l_partkey AS UINTEGER)) AS parts FROM lineitem AS l
			JOIN orders AS o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
		// A FILTER, written as a CASE, and an ID through the common table
		// expressions that compute it, as a report counts its members.
		`SELECT l_shipmode, count(DISTINCT CAST(l_orderkey AS UINTEGER)) FILTER (WHERE l_quantity > 25) AS big, count(*) AS n
			FROM lineitem GROUP BY 1 ORDER BY 1`,
		`WITH r AS (SELECT l_shipmode AS mode, CAST(l_orderkey AS UINTEGER) AS member, l_quantity > 25 AS keep FROM lineitem),
			v AS (SELECT mode, member, keep FROM r)
			SELECT mode, count(DISTINCT member) FILTER (WHERE keep) AS people, count(DISTINCT member) AS everyone FROM v GROUP BY 1 ORDER BY 1`,
		// The ID past a star, as a report selects its source's rows with
		// their members.
		`WITH m AS (SELECT l.*, CAST(l.l_orderkey AS UINTEGER) AS member, l.l_quantity > 25 AS keep FROM lineitem AS l),
			v AS (SELECT l_shipmode AS mode, member, keep FROM m)
			SELECT mode, count(DISTINCT CASE WHEN keep THEN member END) AS people, count(DISTINCT member) AS everyone FROM v GROUP BY 1 ORDER BY 1`,
	}
	h.co.maxBroadcast = 1
	for _, mode := range []string{aggGather, aggByGroup, ""} {
		h.co.aggregation = mode
		for _, q := range queries {
			t.Run(mode+" "+short(q), func(t *testing.T) {
				h.t = t
				if !h.check(q) {
					t.Fatal("did not run distributed")
				}
				if p := h.planOf(q); !strings.Contains(p.describe(), "rb_agg(") {
					t.Errorf("no bitmaps:\n%s", p.describe())
				}
			})
		}
	}
}

// TestExtensionAggregates checks that the planner knows the aggregates of
// the extensions sessions load, which its own DuckDB does not: rb's rb_agg,
// rb_or_agg and rb_count_distinct of IDs combine as unions of the workers'
// bitmaps, written before any exchange, at one level and at two, over
// every row and through a relation another statement joins to, and return
// what one machine does. rb_count_distinct(id, key), which has no
// partials, stays on one machine, rather than have workers complete its
// groups from every row, through a common table expression too.
func TestExtensionAggregates(t *testing.T) {
	rb := os.Getenv("IGLOO_TEST_RB_EXTENSION")
	if rb == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	h := newHarnessRB(t, 0.02, 4, 3, rb)
	h.co.maxBroadcast = 1
	defer func() { h.co.maxBroadcast = 0 }()
	cases := []struct {
		query string
		// partials says the stage that reads lineitem aggregates into
		// bitmaps, and a later step unions them.
		partials bool
	}{
		{`SELECT rb_cardinality(rb_agg(CAST(l_orderkey AS UINTEGER))) AS n FROM lineitem`, true},
		{`SELECT l_shipmode, rb_cardinality(rb_agg(CAST(l_orderkey AS UINTEGER))) AS n, count(*) AS c FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		{`SELECT l_returnflag, rb_cardinality(rb_agg(CASE WHEN l_quantity > 25 THEN CAST(l_orderkey AS UINTEGER) END)) AS big,
			count(*) FILTER (WHERE l_quantity > 25) AS n FROM lineitem GROUP BY 1 ORDER BY 1`, true},
		// Two levels, as a report counts distinct (member, day) pairs: a
		// bitmap of each group's members a day, then the days' sum and the
		// union's members.
		{`WITH d AS (SELECT l_shipmode AS m, l_shipdate AS day, rb_agg(CAST(l_orderkey AS UINTEGER)) AS bm FROM lineitem GROUP BY 1, 2)
			SELECT m, CAST(sum(rb_cardinality(bm)) AS BIGINT) AS pairs, rb_cardinality(rb_or_agg(bm)) AS orders FROM d GROUP BY 1 ORDER BY 1`, true},
		// A bitmap over every row of a table another joins to: one row.
		{`WITH b AS (SELECT rb_agg(CAST(o_orderkey AS UINTEGER)) AS bm FROM orders WHERE o_orderpriority = '1-URGENT')
			SELECT l_shipmode, count(*) AS n FROM lineitem, b WHERE rb_contains(b.bm, CAST(l_orderkey AS UINTEGER)) GROUP BY 1 ORDER BY 1`, false},
		{`SELECT l_returnflag, rb_count_distinct(CAST(l_partkey AS UINTEGER)) AS parts, rb_count_distinct(CAST(l_suppkey AS UINTEGER)) FILTER (WHERE l_quantity > 25) AS s
			FROM lineitem GROUP BY 1 ORDER BY 1`, true},
	}
	for _, mode := range []string{aggGather, aggByGroup, ""} {
		h.co.aggregation = mode
		for _, tc := range cases {
			t.Run(mode+" "+short(tc.query), func(t *testing.T) {
				h.t = t
				if !h.check(tc.query) {
					t.Fatal("did not run distributed")
				}
				if !tc.partials {
					return
				}
				p := h.planOf(tc.query)
				first := ""
				for _, s := range p.Stages {
					if in := s.split(); in != nil && in.Table.Name == "lineitem" {
						first = s.SQL
					}
				}
				final, _ := h.co.parser.print(p.final)
				if !strings.Contains(first, "rb_agg(") || !strings.Contains(p.describe()+final, "rb_or_agg(") {
					t.Errorf("no bitmaps before the exchange:\n%s", p.describe())
				}
			})
		}
	}
	h.co.aggregation = ""
	for _, q := range []string{
		`SELECT l_shipmode, rb_count_distinct(CAST(l_orderkey AS UINTEGER), l_shipdate) AS pairs FROM lineitem GROUP BY 1 ORDER BY 1`,
		`WITH v AS (SELECT l_shipmode AS m, CAST(l_orderkey AS UINTEGER) AS id, l_shipdate AS day FROM lineitem, unnest([1, 2]))
			SELECT m, rb_count_distinct(id, day) AS pairs, count(*) AS n FROM v GROUP BY 1 ORDER BY 1`,
	} {
		if _, err := h.co.Plan(context.Background(), q, harnessEnv{h}, ModeOn); err == nil || !strings.Contains(err.Error(), "rb_count_distinct(id, key)") {
			t.Errorf("%s: planned on workers, or for another reason: %v", short(q), err)
		}
	}
}
