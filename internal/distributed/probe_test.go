package distributed

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/config"
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
		Storage: recordingStorage{NewStorage(icebergio.LocalFS{}), h},
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
		rec.seq = nil
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
	// leftover lists the files left under the exchanges' directory once
	// the statements' exchanges are removed.
	leftover := func() []string {
		t.Helper()
		co.removals.Wait()
		var files []string
		err := filepath.WalkDir(filepath.Join(h.dir, "warehouse", config.ShuffleDir), func(path string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				files = append(files, path)
			}
			return err
		})
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
		return files
	}
	for _, filter := range []string{"l_quantity = 1", "l_quantity = -1"} {
		plan, ex, stages := run(query(filter))
		if len(plan.uncertain) != 1 || len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "read whole") {
			t.Errorf("%s: probes %v of %v\n%s", filter, ex.Probes, plan.uncertain, plan.describe())
		}
		// Orders' partitioning may have started beside the relation, and
		// been canceled: the new plan reads orders' shares instead, and
		// nothing read the relation by partition.
		if !slices.ContainsFunc(stages, func(s string) bool { return strings.Contains(s, "split main.orders to the coordinator") }) ||
			slices.ContainsFunc(stages, func(s string) bool { return strings.Contains(s, "partitions of exchange 0 partitions of") }) {
			t.Errorf("%s: orders partitioned, in %v", filter, stages)
		}
		// What the canceled stages wrote is removed with the rest.
		if files := leftover(); len(files) > 0 {
			t.Errorf("%s: left %v", filter, files)
		}
	}
	// Nothing is small enough: as planned, each stage run once, orders'
	// partitioning beside the relation rather than after it. (A relation
	// measured small lately is measured first: this one is new.)
	co.maxBroadcast = 1
	plan, ex, stages := run(query("l_quantity = 3"))
	if len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "as planned") || len(stages) != len(plan.Stages) {
		t.Errorf("large: probes %v, stages %v run of %d\n%s", ex.Probes, stages, len(plan.Stages), plan.describe())
	}
	rec.mu.Lock()
	seq := slices.Clone(rec.seq)
	rec.mu.Unlock()
	orders := slices.IndexFunc(seq, func(s string) bool { return strings.Contains(s, "split main.orders to exchange") })
	if measured := slices.Index(seq, "measured"); orders < 0 || measured < 0 || orders > measured {
		t.Errorf("large: orders partitioned after the relation was measured: %v", seq)
	}
	// Measured large, it is planned as large the next time, without
	// measuring it first; a day on, it is measured again.
	if plan, ex, _ := run(query("l_quantity = 3")); len(ex.Probes) != 0 || len(plan.uncertain) != 0 {
		t.Errorf("large again: probes %v of %v", ex.Probes, plan.uncertain)
	}
	co.outcomes.mu.Lock()
	for k, o := range co.outcomes.byKey {
		o.at = o.at.Add(-exploreEvery - time.Hour)
		co.outcomes.byKey[k] = o
	}
	co.outcomes.mu.Unlock()
	if _, ex, _ := run(query("l_quantity = 3")); len(ex.Probes) != 1 {
		t.Errorf("a day on: probes %v", ex.Probes)
	}
	// Measured small, it is measured every time, and once it was, with
	// the workers to itself: orders' partitioning does not start beside it.
	co.maxBroadcast = h.tables["orders"].Bytes / 2
	for i := range 2 {
		_, ex, stages := run(query("l_quantity = 2"))
		if len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "read whole") {
			t.Errorf("small again: probes %v", ex.Probes)
		}
		if i == 1 && slices.ContainsFunc(stages, func(s string) bool { return strings.Contains(s, "split main.orders to exchange") }) {
			t.Errorf("measured small before: orders' partitioning started, in %v", stages)
		}
	}
}

// A relation measured once, which the plan made with its size read whole
// in one branch and by partition in another, is planned with that size the
// next time, without measuring it: the statement gets the plan it got once
// measured, rather than the one made without its size.
func TestRememberedSize(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	// Every worker may read the audience, but not part, whose fewer rows
	// than the audience's make partitioning both cheaper than every task
	// reading the audience whole; orders' many rows make that dearer.
	h.co.maxBroadcast = h.tables["part"].Bytes / 2
	for _, name := range []string{"orders", "part"} {
		h.tables[name].Rows = rowCount(h.files[h.tables[name].key()])
	}
	q := `WITH aud AS (SELECT DISTINCT l_orderkey AS k FROM lineitem WHERE l_quantity IN (1, 2, 3))
		SELECT 'orders' AS side, count(*) AS n FROM orders AS o JOIN aud ON aud.k = o.o_orderkey
		UNION ALL SELECT 'parts', count(*) FROM part AS p JOIN aud ON aud.k = p.p_partkey`
	_, _, first, ex, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.uncertain) == 0 || len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "by partition in others, planned again") {
		t.Fatalf("first run: probes %v of %v\n%s", ex.Probes, first.uncertain, first.describe())
	}
	_, _, again, ex2, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.uncertain) != 0 || len(ex2.Probes) != 0 || len(again.Stages) != ex.Stages || ex2.Stages != ex.Stages {
		t.Errorf("again: probes %v of %v, %d stages, where the first run's second plan had %d\n%s",
			ex2.Probes, again.uncertain, len(again.Stages), ex.Stages, again.describe())
	}
	if !h.check(q) {
		t.Error("did not run distributed")
	}
}

// TestMeasuredSize checks the size a measured relation is planned with:
// its bytes, unless its rows would not fit a quarter of a task's memory in
// a hash table, when it is past what every worker may read.
func TestMeasuredSize(t *testing.T) {
	cols := []Column{{Name: "id", Type: "VARCHAR"}}
	pl := &planner{opts: planOptions{maxBroadcast: 1 << 30, taskMemory: 1 << 30,
		sizes: map[string]knownSize{"small": {bytes: 5 << 20, rows: 1e6}, "many": {bytes: 50 << 20, rows: 1e8}}}}
	if b, rows, known := pl.measured("small", 1<<40, cols); !known || b != 5<<20 || rows != 1e6 {
		t.Errorf("small: %d %d %v", b, rows, known)
	}
	if b, _, known := pl.measured("many", 1<<40, cols); !known || b <= pl.opts.maxBroadcast {
		t.Errorf("a hundred million rows: %d %v", b, known)
	}
	if b, rows, known := pl.measured("other", 1<<40, cols); known || b != 1<<40 || rows != 0 {
		t.Errorf("unmeasured: %d %d %v", b, rows, known)
	}
	// Few bytes for what every worker may read, but 2.5 KB a row of
	// strings: in a hash table, far more than a task's memory.
	pl.opts.sizes["long"] = knownSize{bytes: 500 << 20, rows: 200_000}
	if b, _, known := pl.measured("long", 1<<40, cols); !known || b <= pl.opts.maxBroadcast {
		t.Errorf("long strings: %d %v", b, known)
	}
	// Measured lately and read by partition somewhere: the size measured
	// then, while the tables it reads stay within a factor of two.
	pl.opts.large = map[string]outcome{"seen": {size: knownSize{bytes: 5 << 20, rows: 1e6}, estimate: 1 << 40, large: true}}
	if b, rows, known := pl.measured("seen", 1<<40, cols); !known || b != 5<<20 || rows != 1e6 {
		t.Errorf("remembered: %d %d %v", b, rows, known)
	}
	if _, _, known := pl.measured("seen", 1<<42, cols); known {
		t.Error("remembered over tables four times as large")
	}
}

// TestWholeRelations checks which measured relations every task reads
// whole, with the sizes the probe measured at ten times the data on 16
// workers of 24.6 GiB and 8 cores, two tasks at a time, beside a table of
// 1.67 billion segment rows. An audience of 45.9 million IDs, read whole
// by every task, took a report from 84 s to 608 s and another from 22 s to
// 73 s, and a cohort of 10 million took two from 176 and 188 s to 260 and
// 253 s; the 189,400 people of another report, read whole, took it from
// 517 s to 161 s.
// (DuckDB 1.5.6 built a join's hash table of 36-character IDs in 72 bytes
// a row, and peaked at 110 bytes a row in all; rowMemory counts 128.)
func TestWholeRelations(t *testing.T) {
	var ws []*worker
	for range 16 {
		ws = append(ws, &worker{status: workerStatus{MemoryBytes: 26_400_000_000, Cores: 8}})
	}
	ids := []Column{{Name: "PERSON_ID", Type: "VARCHAR"}}
	segments := &tableUse{splittable: true, table: &Table{Name: "segments", Rows: 1_665_200_815, Bytes: 40e9}}
	pl := &planner{opts: planOptions{maxBroadcast: 26_400_000_000 / 8, taskMemory: taskMemory(ws), workers: len(ws), sizes: map[string]knownSize{
		"audience": {bytes: 453_000_000, rows: 45_889_560},
		"cohort":   {bytes: 315_700_000, rows: 10_030_730},
		"people":   {bytes: 6_100_000, rows: 189_400},
		"few":      {bytes: 1_000_000, rows: 30_730},
	}}}
	whole := func(key string, table *tableUse) bool {
		bytes, rows, known := pl.measured(key, 1<<40, ids)
		r := &relUse{key: key, known: known, bytes: bytes, rows: rows}
		c := &candidate{sel: node{}, uses: []*tableUse{table}, rels: []*relUse{r}}
		return known && bytes <= pl.opts.maxBroadcast && pl.wholeCheaper(r, c)
	}
	for key, want := range map[string]bool{"audience": false, "cohort": false, "people": true, "few": true} {
		if got := whole(key, segments); got != want {
			t.Errorf("%s beside the segments: read whole %v, want %v", key, got, want)
		}
	}
	// Beside a table of a million rows, building the 189,400 people's
	// table in each of 16 tasks costs more than partitioning the table.
	small := &tableUse{splittable: true, table: &Table{Name: "small", Rows: 1_000_000, Bytes: 25e6}}
	if whole("people", small) {
		t.Error("people beside a small table: read whole")
	}
	if !whole("few", small) {
		t.Error("30,730 beside a small table: partitioned")
	}
}

// TestProbeGuards checks what keeps a probe from reading what is not
// there: a relation whose exchange no stage writes, fused into its reader,
// is not measured; a table a new plan reads is listed before its stages
// run; and a stage never reads a table whose files were not listed as
// empty.
func TestProbeGuards(t *testing.T) {
	x, y := &Exchange{ID: 0}, &Exchange{ID: 1}
	stages := []*Stage{{ID: 0, Out: x}}
	if got := written([]uncertain{{name: "a", out: x}, {name: "b", out: y}}, stages); len(got) != 1 || got[0].name != "a" {
		t.Errorf("written: %v", got)
	}

	e := &execution{files: map[string][]File{}}
	s := &Stage{ID: 3, Inputs: []Input{{View: "t", Table: &Table{Schema: "main", Name: "unlisted"}, Split: true}}}
	if err := e.stage(t.Context(), s); err == nil || !strings.Contains(err.Error(), "not listed") {
		t.Errorf("a table not listed: %v", err)
	}

	h := newHarness(t, 0.01, 2, 3)
	p := h.planOf(`SELECT o_orderpriority, count(*) FROM orders GROUP BY 1`)
	tables, files := map[string]*Table{}, map[string][]File{}
	if err := h.co.loadMore(t.Context(), p.Stages, tables, files); err != nil {
		t.Fatal(err)
	}
	if fs, ok := files["main.orders"]; !ok || len(fs) == 0 || tables["main.orders"] == nil {
		t.Errorf("listed %v", files)
	}
}

// TestProbeRetry checks a statement whose task runs out of memory after
// its relation was measured and the statement planned again: it runs
// again as planned with the relation's size, in smaller tasks, without
// measuring it again, and returns what one machine does.
func TestProbeRetry(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	var fails, tried atomic.Int32
	for _, w := range h.workers {
		w.opts.Engine = oomEngine{w.opts.Engine, &fails, &tried}
	}
	rec := &armOnMeasure{fails: &fails}
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator2"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2, Pool: PoolOptions{Addresses: []string{
			h.lns[0].Addr().String(), h.lns[1].Addr().String(), h.lns[2].Addr().String()}},
		Storage: recordingStorage{NewStorage(icebergio.LocalFS{}), h},
	}, slog.New(rec))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	h.co = co
	co.maxBroadcast = h.tables["orders"].Bytes / 2
	q := `WITH aud AS (SELECT DISTINCT l_orderkey AS k FROM lineitem WHERE l_quantity = 1)
		SELECT o.o_orderpriority, count(*), sum(o.o_totalprice) FROM orders AS o JOIN aud ON aud.k = o.o_orderkey GROUP BY 1 ORDER BY 1`
	_, _, _, ex, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if n := rec.measured.Load(); n != 1 || fails.Load() != 0 || len(ex.Probes) != 1 || !strings.Contains(ex.Probes[0], "read whole") {
		t.Errorf("measured %d times, %d failures left, probes %v", n, fails.Load(), ex.Probes)
	}
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
}

// armOnMeasure makes the next task fail as out of memory once relations
// of uncertain size are measured, and counts the times they are.
type armOnMeasure struct {
	fails    *atomic.Int32
	measured atomic.Int32
}

func (a *armOnMeasure) Enabled(context.Context, slog.Level) bool { return true }
func (a *armOnMeasure) WithAttrs([]slog.Attr) slog.Handler       { return a }
func (a *armOnMeasure) WithGroup(string) slog.Handler            { return a }
func (a *armOnMeasure) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "measured relations of uncertain size" && a.measured.Add(1) == 1 {
		a.fails.Store(1)
	}
	return nil
}
