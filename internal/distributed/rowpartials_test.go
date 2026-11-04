package distributed

import (
	"context"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// TestRowPartials checks that stages that write an aggregation's partial results
// for a stage that completes its groups may write them a row at a time, that the
// statement's rows are one machine's either way, and that a stage starts a row at
// a time when the distinct counts of the one table it reads say its groups are
// about as many as its rows.
func TestRowPartials(t *testing.T) {
	h := newHarness(t, 0.01, 4, 3)
	li := h.tables["lineitem"]
	li.Rows = rowCount(h.files[li.key()])
	li.Distinct = map[string]int64{"l_orderkey": li.Rows / 4, "l_linenumber": 7}
	ctx := context.Background()
	for _, c := range []struct {
		name, query string
		// partitioned joins by partition on the part, so that groups by the
		// line item's key are whole in no partition.
		partitioned bool
		// first says the stage starts a row at a time.
		first bool
	}{
		{"a row a group", `SELECT l_orderkey, l_linenumber, min(p_retailprice) AS lo, sum(l_quantity) AS q, count(*) AS n,
			count(l_comment) FILTER (WHERE l_quantity > 10) AS big, count_if(l_tax > 0.04) AS taxed
			FROM lineitem JOIN part ON l_partkey = p_partkey GROUP BY l_orderkey, l_linenumber`, true, false},
		{"rows a group", `SELECT l_shipmode, max(p_retailprice) AS hi, avg(l_quantity) AS q, bool_or(l_tax > 0.07) AS high
			FROM lineitem JOIN part ON l_partkey = p_partkey GROUP BY l_shipmode`, true, false},
		{"a row a group of one table", "SELECT l_orderkey, l_linenumber, sum(l_extendedprice) AS v FROM lineitem GROUP BY ALL", false, true},
		{"rows a group of one table", "SELECT l_orderkey, max(l_shipdate) AS last FROM lineitem GROUP BY ALL", false, false},
	} {
		want, wantCols, err := h.query(ctx, c.query)
		if err != nil {
			t.Fatal(err)
		}
		for _, partials := range []string{"", partialsByRow} {
			h.co.maxBroadcast, h.co.aggregation, h.co.partialRows = 0, aggByGroup, partials
			if c.partitioned {
				h.co.maxBroadcast = 1
			}
			got, gotCols, plan, _, err := h.run(ctx, c.query, ModeOn)
			if err != nil {
				t.Fatalf("%s, %q: %v", c.name, partials, err)
			}
			if msg := compareRows(slices.Clone(want), got, false); msg != "" || !slices.Equal(wantCols, gotCols) {
				t.Errorf("%s, %q: %s, columns %v, want %v\n%s", c.name, partials, msg, gotCols, wantCols, plan.describe())
			}
			i := slices.IndexFunc(plan.Stages, func(s *Stage) bool { return s.rowSQL != "" })
			if i < 0 {
				t.Errorf("%s, %q: no stage may write its partial results a row at a time\n%s", c.name, partials, plan.describe())
				continue
			}
			if first := plan.Stages[i].rows.first; first != (c.first || partials == partialsByRow) {
				t.Errorf("%s, %q: starts a row at a time: %v", c.name, partials, first)
			}
		}
	}
	h.co.maxBroadcast, h.co.aggregation, h.co.partialRows = 0, "", ""
}

// TestWorkerRowPartials checks that a worker runs the next tasks of a stage a row
// at a time once one of its tasks found each of its groups to be one row, runs
// them grouped while their groups hold several, and runs a task a row at a time
// from the start when told to.
func TestWorkerRowPartials(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	w := h.workers[0]
	ctx := t.Context()
	li := h.tables["lineitem"]
	view := viewDef{Name: "__igloo_split", Columns: li.Columns, Files: []string{h.files[li.key()][0].Path}}
	// A row at a time, its groups count 2 rows, so that the counts it wrote
	// tell which ran.
	task := func(query string, n int, group string, first bool) *taskRequest {
		x := func(q string) string {
			return "SELECT *, CAST(hash(__g0) % 4 AS INTEGER) AS __b FROM (" + q + ") AS __igloo_x"
		}
		return &taskRequest{Query: query, Task: 5*tasksPerStage + n, Version: "test", Views: []viewDef{view}, Partitions: 4,
			SQL:       x("SELECT " + group + " AS __g0, min(l_quantity) AS __a0, count(*) AS " + groupRowsColumn + " FROM __igloo_split GROUP BY 1"),
			RowSQL:    x("SELECT " + group + " AS __g0, l_quantity AS __a0, CAST(2 AS BIGINT) AS " + groupRowsColumn + " FROM __igloo_split"),
			RowsFirst: first,
			Shuffle:   filepath.Join(h.dir, "warehouse", config.ShuffleDir, query, strconv.Itoa(n))}
	}
	run := func(req *taskRequest) int64 {
		t.Helper()
		_, res, err := w.run(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return res.groupRows
	}
	unique := "l_orderkey * 8 + l_linenumber"
	if n := run(task("q-unique", 0, unique, false)); n != 1 {
		t.Errorf("a task grouping each row on its own: groups of up to %d rows", n)
	}
	if n := run(task("q-unique", 1, unique, false)); n != 2 {
		t.Errorf("the stage's next task ran grouped (groups of up to %d rows), not a row at a time", n)
	}
	if run(task("q-several", 0, "l_orderkey % 10", false)) < 2 || run(task("q-several", 1, "l_orderkey % 10", false)) == 2 {
		t.Error("a stage whose groups hold several rows ran a row at a time")
	}
	if n := run(task("q-first", 0, "l_orderkey % 10", true)); n != 2 {
		t.Errorf("a task told to start a row at a time ran grouped (groups of up to %d rows)", n)
	}

	// With inputs large enough to probe, the first task of a stage probes a
	// sample: groups of one row each have it, and the stage's other tasks, write
	// a row at a time from the start; groups of several leave them grouped. The
	// tasks read the whole of their input after the probe.
	minRows := probeMinRows
	defer func() { probeMinRows = minRows }()
	probeMinRows = 1000
	view.Rows = h.files[li.key()][0].Rows
	probed := func(query string, n int, group string) *taskResult {
		t.Helper()
		_, res, err := w.run(ctx, task(query, n, group, false))
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	for n := range 2 {
		if res := probed("q-probe-unique", n, unique); res.groupRows != 2 || res.Rows != view.Rows {
			t.Errorf("task %d of a stage of single-row groups: groups of up to %d rows, %d rows of %d", n, res.groupRows, res.Rows, view.Rows)
		}
	}
	if res := probed("q-probe-several", 0, "l_orderkey % 10"); res.groupRows <= 2 || res.Rows != 10 {
		t.Errorf("a stage of groups of several rows: groups of up to %d rows, %d groups of 10", res.groupRows, res.Rows)
	}
}

// TestStatMax checks reading the largest value of a column from the statistics
// a COPY returns of a file it wrote.
func TestStatMax(t *testing.T) {
	// As the driver returns MAPs, and as a plain map.
	var col, other, stats duckdb.OrderedMap
	col.Set("min", "1")
	col.Set("max", "3")
	other.Set("max", "9")
	stats.Set("x", other)
	stats.Set(quoteIdent(groupRowsColumn), col)
	if n, ok := statMax(stats, groupRowsColumn); !ok || n != 3 {
		t.Errorf("statMax: %d, %v", n, ok)
	}
	if n, ok := statMax(duckdb.Map{groupRowsColumn: duckdb.Map{"max": "1"}}, groupRowsColumn); !ok || n != 1 {
		t.Errorf("statMax of a map: %d, %v", n, ok)
	}
	for _, s := range []any{nil, "x", duckdb.Map{}, duckdb.Map{groupRowsColumn: duckdb.Map{"max": "a"}}} {
		if _, ok := statMax(s, groupRowsColumn); ok {
			t.Errorf("statMax(%v) said", s)
		}
	}
}
