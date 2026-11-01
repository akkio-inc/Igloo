package distributed

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Workers complete an aggregation's groups, so that the coordinator only
// projects, filters and orders final rows: its memory and disk follow the
// result, not the groups times the tasks that computed them. These tests
// run statements over behaviors, a table of people's activity whose values
// are many, at two sizes, against one machine's results.

// behaviorsSource is n rows of behaviors: a person (UINTEGER, as Igloo
// counts people by their integer IDs), a source of four, a type of five a
// source, a value of about n/10, a day and an amount.
func behaviorsSource(n int) string {
	return fmt.Sprintf(`(SELECT CAST(i %% 60000 AS UINTEGER) AS person,
		['media', 'purchase', 'place', 'browse'][1 + CAST(i %% 4 AS INTEGER)] AS source,
		'type ' || CAST((i // 4) %% 5 AS VARCHAR) AS btype,
		'value ' || CAST(hash(i // 7) %% %d AS VARCHAR) AS bvalue,
		DATE '2026-07-01' + CAST(i %% 92 AS INTEGER) AS day,
		CAST(i %% 1000 AS DOUBLE) / 10 AS amount
		FROM range(%d) AS r(i))`, n/10, n)
}

// behaviorsHarness is a harness with behaviors (400,000 rows) and
// behaviors4 (four times as many), whose session and workers load the rb
// extension where the tests have it.
func behaviorsHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarnessRB(t, 0.001, 2, 3, os.Getenv("IGLOO_TEST_RB_EXTENSION"))
	h.addTable("behaviors", behaviorsSource(400_000), 8)
	h.addTable("behaviors4", behaviorsSource(1_600_000), 8)
	return h
}

// f3Like is a report shaped like the one that did not finish at a hundred
// times the data on 16 workers: four sources' behaviors, each grouped by
// type and value with the distinct people of a cohort and their activity,
// ranked within each source and type, the first ten kept.
func f3Like(table string) string {
	branch := func(source, activity, total string) string {
		return `SELECT b.source, b.btype, b.bvalue, count(DISTINCT b.person) AS members, ` + activity + ` AS activity, ` + total + ` AS total
			FROM ` + table + ` AS b JOIN cohort AS c ON c.person = b.person WHERE b.source = '` + source + `' GROUP BY b.source, b.btype, b.bvalue`
	}
	return `WITH cohort AS (SELECT DISTINCT person FROM ` + table + ` WHERE person % 3 <> 0),
		media AS (` + branch("media", "count(*)", "CAST(NULL AS DOUBLE)") + `),
		purchase AS (` + branch("purchase", "count(*)", "sum(b.amount)") + `),
		place AS (` + branch("place", "count(*)", "CAST(NULL AS DOUBLE)") + `),
		browse AS (` + branch("browse", "count(DISTINCT (b.person, b.day))", "CAST(NULL AS DOUBLE)") + `),
		everything AS (SELECT source, btype, bvalue, members, activity, total FROM media
			UNION ALL SELECT source, btype, bvalue, members, activity, total FROM purchase
			UNION ALL SELECT source, btype, bvalue, members, activity, total FROM place
			UNION ALL SELECT source, btype, bvalue, members, activity, total FROM browse),
		ranked AS (SELECT source, btype, bvalue, members, activity, total,
			row_number() OVER (PARTITION BY source, btype ORDER BY members DESC NULLS LAST, activity DESC NULLS LAST, bvalue) AS r
			FROM everything)
		SELECT source, btype, bvalue, members, activity, total, r FROM ranked WHERE r <= 10 ORDER BY source, btype, r`
}

// received runs query distributed, checks its result against one
// machine's, and returns the bytes of partial results the coordinator
// received, the SQL it ran over them and the result's rows.
func (h *harness) received(query string) (int64, string, int) {
	h.t.Helper()
	ctx := context.Background()
	want, _, err := h.query(ctx, query)
	if err != nil {
		h.t.Fatalf("%s\nlocal: %v", short(query), err)
	}
	plan, err := h.co.Plan(ctx, query, harnessEnv{h}, ModeOn)
	if err != nil {
		h.t.Fatalf("%s\nplan: %v", short(query), err)
	}
	ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
	if err != nil {
		h.t.Fatalf("%s\ndistributed: %v\n%s", short(query), err, plan.describe())
	}
	defer ex.Close()
	got, _, err := h.query(ctx, ex.SQL)
	if err != nil {
		h.t.Fatalf("%s\ncoordinator: %v\n%s", short(query), err, ex.SQL)
	}
	if msg := compareRows(want, got, hasOrderBy(query)); msg != "" {
		h.t.Errorf("%s\n%s\n%s", short(query), msg, plan.describe())
	}
	return ex.Received, ex.SQL, len(want)
}

// TestF3LikeReport runs the report at two sizes: the cohort's people
// partition the four sources' behaviors, each partition counts its people
// in place and completes its groups of values, keeping those that may rank
// within the first ten, and the coordinator ranks what arrives, about as
// much at four times the data.
func TestF3LikeReport(t *testing.T) {
	h := behaviorsHarness(t)
	h.co.maxBroadcast = 1
	small, coordinator, _ := h.received(f3Like("behaviors"))
	large, _, _ := h.received(f3Like("behaviors4"))
	t.Logf("received %d bytes at 1x, %d at 4x", small, large)
	if strings.Contains(coordinator, "GROUP BY") {
		t.Errorf("the coordinator groups: %s", coordinator)
	}
	// Twenty rankings of ten, from each of the partitions.
	if small > 64<<10 || large > 4*small {
		t.Errorf("the coordinator received %d bytes at 1x and %d at 4x", small, large)
	}
	// The union's branches as SELECT * of the sources' groups: the same.
	stars := f3Like("behaviors")
	for _, source := range []string{"media", "purchase", "place", "browse"} {
		stars = strings.Replace(stars, "SELECT source, btype, bvalue, members, activity, total FROM "+source, "SELECT * FROM "+source, 1)
	}
	if !strings.Contains(stars, "SELECT * FROM browse") {
		t.Fatal("no stars")
	}
	if n, _, _ := h.received(stars); n > 64<<10 {
		t.Errorf("with stars, the coordinator received %d bytes", n)
	}
}

// TestGroupsByDistinctCounts checks when workers complete the groups of
// the divided table's columns: always where ANALYZE did not count their
// values, which may be many; otherwise when their combinations may be
// more than maxGatheredGroups, as the counts and dates' fields say.
func TestGroupsByDistinctCounts(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	cases := []struct {
		query string
		// unknown and counted are how many stages write partial results to
		// an exchange without and with the distinct counts.
		unknown, counted int
	}{
		{`SELECT l_returnflag, l_linestatus, count(*), sum(l_quantity) FROM lineitem GROUP BY 1, 2 ORDER BY 1, 2`, 1, 0},
		{`SELECT date_trunc('month', l_shipdate) AS m, l_returnflag, count(*) FROM lineitem GROUP BY 1, 2 ORDER BY 1, 2`, 1, 0},
		{`SELECT CASE WHEN l_quantity > 25 THEN 'big' ELSE 'small' END AS size, l_shipmode, count(*) FROM lineitem GROUP BY 1, 2 ORDER BY 1, 2`, 1, 0},
		{`SELECT l_shipmode, count(DISTINCT l_partkey) AS parts FROM lineitem GROUP BY 1 ORDER BY 1`, 2, 1},
		{`SELECT l_orderkey, count(*) AS n FROM lineitem GROUP BY 1 HAVING count(*) > 6 ORDER BY 1`, 1, 1},
		{`SELECT l_shipmode, l_suppkey, count(*) AS n FROM lineitem GROUP BY 1, 2 HAVING count(*) > 40 ORDER BY 1, 2`, 1, 1},
	}
	for _, counted := range []bool{false, true} {
		if counted {
			h.tables["lineitem"].Distinct = map[string]int64{"l_returnflag": 3, "l_linestatus": 2, "l_shipmode": 7, "l_orderkey": 15000, "l_suppkey": 2000}
		}
		for _, tc := range cases {
			t.Run(fmt.Sprint(counted, " ", short(tc.query)), func(t *testing.T) {
				h.t = t
				want := tc.unknown
				if counted {
					want = tc.counted
				}
				if n := h.exchanged(tc.query, "aggregate"); n != want {
					t.Errorf("%d stages wrote partial results to an exchange, want %d", n, want)
				}
			})
		}
	}
}

// TestFinalCombine checks shapes whose results are small but whose groups
// are many: HAVING, ORDER BY ... LIMIT and a ranking of the groups, over
// aggregates of rows and over DISTINCT values, in bitmaps too. Completed by
// workers, the coordinator receives the result's rows, a few, and as many
// at four times the data for a result of the first rows; combining every
// worker's partial results it received the groups times the tasks, four
// times as many at four times the data. Under a spool of a few hundred
// kilobytes, which those overflow, they run on workers all the same.
func TestFinalCombine(t *testing.T) {
	h := behaviorsHarness(t)
	type shape struct {
		query string
		// first says the result is the first rows, as many whatever the
		// data.
		first bool
	}
	shapes := []shape{
		{`SELECT bvalue, count(*) AS n, sum(amount) AS total FROM %s GROUP BY 1 HAVING count(*) > 24 ORDER BY n DESC, bvalue`, false},
		{`SELECT bvalue, count(DISTINCT person) AS people, count(*) AS n FROM %s GROUP BY 1 ORDER BY people DESC, n DESC, bvalue LIMIT 20`, true},
		{`SELECT source, bvalue, count(DISTINCT person) AS people, count(*) AS n FROM %s GROUP BY 1, 2
			QUALIFY row_number() OVER (PARTITION BY source ORDER BY people DESC, n DESC, bvalue) <= 5 ORDER BY 1, 3 DESC, 4 DESC, 2`, true},
		{`SELECT btype, bvalue, count(*) AS n, count(DISTINCT person) AS people FROM %s WHERE day >= DATE '2026-08-01'
			GROUP BY ALL HAVING count(DISTINCT person) >= 15 ORDER BY 1, 2`, false},
		{`SELECT bvalue, max(day) AS last, avg(amount) AS a, count(*) AS n FROM %s GROUP BY bvalue ORDER BY last DESC, n DESC, bvalue LIMIT 10`, true},
	}
	bitmaps := os.Getenv("IGLOO_TEST_RB_EXTENSION") != ""
	if bitmaps {
		shapes = append(shapes, shape{`SELECT bvalue, count(DISTINCT person) AS people FROM %s GROUP BY 1 HAVING count(DISTINCT person) > 20 ORDER BY 2 DESC, 1`, false})
	}
	for i, sh := range shapes {
		q := sh.query
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if bitmaps && i == len(shapes)-1 {
				h.exec("SET rb_count_distinct = 'always'")
				defer h.exec("RESET rb_count_distinct")
				if p := h.planOf(fmt.Sprintf(q, "behaviors")); !strings.Contains(p.describe(), "rb_agg(") {
					t.Errorf("no bitmaps:\n%s", p.describe())
				}
			}
			h.co.aggregation = ""
			small, coordinator, rows := h.received(fmt.Sprintf(q, "behaviors"))
			large, _, rows4 := h.received(fmt.Sprintf(q, "behaviors4"))
			if strings.Contains(coordinator, "GROUP BY") {
				t.Errorf("the coordinator groups: %s", coordinator)
			}
			h.co.aggregation = aggGather
			gathered, _, _ := h.received(fmt.Sprintf(q, "behaviors"))
			gathered4, _, _ := h.received(fmt.Sprintf(q, "behaviors4"))
			h.co.aggregation = ""
			t.Logf("received %d and %d bytes for %d and %d rows at 1x and 4x; gathered, %d and %d", small, large, rows, rows4, gathered, gathered4)
			// A file a task, of the rows it keeps.
			if bound := int64(64<<10 + 256*rows); small > bound {
				t.Errorf("the coordinator received %d bytes for %d rows at 1x", small, rows)
			}
			if bound := int64(64<<10 + 256*rows4); large > bound {
				t.Errorf("the coordinator received %d bytes for %d rows at 4x", large, rows4)
			}
			if sh.first && large > 2*small+(16<<10) {
				t.Errorf("the coordinator received %d bytes at 1x and %d at 4x for the first rows", small, large)
			}
			if gathered < 8*small || gathered4 < 3*gathered {
				t.Errorf("gathered, the coordinator received %d bytes at 1x and %d at 4x, against %d completed", gathered, gathered4, small)
			}
			// Under a spool those would overflow, it runs on workers.
			h.co.opts.MaxSpoolBytes = 256 << 10
			defer func() { h.co.opts.MaxSpoolBytes = 0 }()
			h.received(fmt.Sprintf(q, "behaviors4"))
			h.co.aggregation = aggGather
			defer func() { h.co.aggregation = "" }()
			plan, err := h.co.Plan(context.Background(), fmt.Sprintf(q, "behaviors4"), harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.co.Execute(context.Background(), plan, h.settings(context.Background())); !IsUnsupported(err) {
				t.Errorf("gathered under a spool of 256 KiB: %v", err)
			}
		})
	}
}
