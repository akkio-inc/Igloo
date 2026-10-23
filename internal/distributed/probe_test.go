package distributed

import (
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
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

// TestProbedRelations checks a statement that partitions a table to meet
// a relation of uncertain size: the relation is written and measured
// first. Small, or empty, it is read whole beside orders' shares, which
// are not partitioned; large, the statement runs as planned, reading what
// the first stages wrote, each stage once. Either way it returns what one
// machine does.
func TestProbedRelations(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	rec := &stageTasks{tasks: map[string]int64{}}
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator2"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2, Pool: PoolOptions{Addresses: []string{
			h.lns[0].Addr().String(), h.lns[1].Addr().String(), h.lns[2].Addr().String()}},
		CacheBytes: func() int64 { return 1 << 40 },
		Storage:    recordingStorage{NewStorage(icebergio.LocalFS{}), h},
	}, slog.New(rec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	h.co = co
	query := func(filter string) string {
		return `WITH aud AS (SELECT DISTINCT l_orderkey AS k FROM lineitem WHERE ` + filter + `)
			SELECT o.o_orderpriority, count(*), sum(o.o_totalprice) FROM orders AS o JOIN aud ON aud.k = o.o_orderkey GROUP BY 1 ORDER BY 1`
	}
	// run runs q, checks it against one machine, which runs it again, and
	// returns the first run's plan, execution and stages.
	run := func(q string) (*Plan, *Execution, []string) {
		t.Helper()
		rec.mu.Lock()
		clear(rec.tasks)
		rec.mu.Unlock()
		_, _, plan, ex, err := h.run(t.Context(), q, ModeOn)
		if err != nil {
			t.Fatal(err)
		}
		rec.mu.Lock()
		stages := slices.Collect(maps.Keys(rec.tasks))
		rec.mu.Unlock()
		if !h.check(q) {
			t.Fatal("did not run distributed")
		}
		return plan, ex, stages
	}

	// Every worker may read what is under orders' size: the audience, a
	// fiftieth of the orders, is read whole.
	co.maxBroadcast = h.tables["orders"].Bytes / 2
	for _, filter := range []string{"l_quantity = 1", "l_quantity = -1"} {
		plan, ex, stages := run(query(filter))
		if len(plan.uncertain) != 1 || len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "read whole") {
			t.Errorf("%s: probes %v of %v\n%s", filter, ex.Probes, plan.uncertain, plan.describe())
		}
		if slices.ContainsFunc(stages, func(s string) bool { return strings.Contains(s, "split main.orders to exchange") }) {
			t.Errorf("%s: orders partitioned, in %v", filter, stages)
		}
	}
	// Nothing is small enough: as planned, each stage run once.
	co.maxBroadcast = 1
	plan, ex, stages := run(query("l_quantity = 1"))
	if len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "as planned") || len(stages) != len(plan.Stages) {
		t.Errorf("large: probes %v, stages %v run of %d\n%s", ex.Probes, stages, len(plan.Stages), plan.describe())
	}
	// Measured large, it is planned as large the next time, without
	// measuring it first; a day on, it is measured again.
	if plan, ex, _ := run(query("l_quantity = 1")); len(ex.Probes) != 0 || len(plan.uncertain) != 0 {
		t.Errorf("large again: probes %v of %v", ex.Probes, plan.uncertain)
	}
	co.outcomes.mu.Lock()
	for k, o := range co.outcomes.byKey {
		o.at = o.at.Add(-exploreEvery - time.Hour)
		co.outcomes.byKey[k] = o
	}
	co.outcomes.mu.Unlock()
	if _, ex, _ := run(query("l_quantity = 1")); len(ex.Probes) != 1 {
		t.Errorf("a day on: probes %v", ex.Probes)
	}
	// Measured small, it is measured every time.
	co.maxBroadcast = h.tables["orders"].Bytes / 2
	for range 2 {
		if _, ex, _ := run(query("l_quantity = 2")); len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "read whole") {
			t.Errorf("small again: probes %v", ex.Probes)
		}
	}
}

// TestMeasuredSize checks the size a measured relation is planned with:
// its bytes, unless its rows would not fit a task's memory in a hash
// table, when it is past what every worker may read.
func TestMeasuredSize(t *testing.T) {
	cols := []Column{{Name: "id", Type: "VARCHAR"}}
	pl := &planner{opts: planOptions{maxBroadcast: 1 << 30, taskMemory: 1 << 30,
		sizes: map[string]knownSize{"small": {bytes: 5 << 20, rows: 1e6}, "many": {bytes: 50 << 20, rows: 1e8}}}}
	if b, known := pl.measured("small", 1<<40, cols); !known || b != 5<<20 {
		t.Errorf("small: %d %v", b, known)
	}
	if b, known := pl.measured("many", 1<<40, cols); !known || b <= pl.opts.maxBroadcast {
		t.Errorf("a hundred million rows: %d %v", b, known)
	}
	if b, known := pl.measured("other", 1<<40, cols); known || b != 1<<40 {
		t.Errorf("unmeasured: %d %v", b, known)
	}
}
