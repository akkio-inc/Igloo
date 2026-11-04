package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// workingSetPlan is DuckDB's EXPLAIN (FORMAT JSON) of a statement that
// joins events to people, counts people distinct per day and sorts the
// days, over an Iceberg scan of people with no estimate of its own.
const workingSetPlan = `[{"name": "ORDER_BY", "children": [
  {"name": "HASH_GROUP_BY", "children": [
    {"name": "PROJECTION", "children": [
      {"name": "HASH_JOIN", "children": [
        {"name": "READ_PARQUET", "children": [], "extra_info": {"Projections": ["person", "day"], "Estimated Cardinality": "100000000"}},
        {"name": "ICEBERG_SCAN", "children": [], "extra_info": {"Projections": ["id", "country"], "Filters": "country='x'"}}
      ], "extra_info": {"Join Type": "INNER", "Conditions": "person = id", "Estimated Cardinality": "50000000"}}
    ], "extra_info": {"Projections": ["day", "person"], "Estimated Cardinality": "50000000"}}
  ], "extra_info": {"Groups": "#0", "Aggregates": ["count(DISTINCT #1)"], "Estimated Cardinality": "40000000"}}
], "extra_info": {"Order By": "day ASC"}}]`

// TestWorkingSetOf checks what a plan holds: the join's build side, the
// rows of a scan DuckDB has no estimate for from its table, the groups no
// more than their keys' distinct count allows, each thread's share of
// them, the distinct pairs, and the sort; and a join's rows from its keys'
// distinct counts.
func TestWorkingSetOf(t *testing.T) {
	people := &Table{Name: "people", Rows: 10_000_000, Columns: []Column{{Name: "id", Type: "BIGINT"}, {Name: "country", Type: "VARCHAR"}},
		Distinct: map[string]int64{"id": 10_000_000}}
	events := &Table{Name: "events", Rows: 100_000_000, Columns: []Column{{Name: "person", Type: "BIGINT"}, {Name: "day", Type: "DATE"}},
		Distinct: map[string]int64{"day": 365, "person": 8_000_000}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: people}, {Table: events}}}}, 8)
	set, parts, err := workingSetOf(workingSetPlan, st)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]heldPart{}
	for _, p := range parts {
		got[p.op] = p
	}
	// People filtered: 10 million rows a fifth of them, 2 million, of an id
	// and a country (8 + 32 bytes) and a join's 20.
	if p := got["HASH_JOIN"]; p.rows != 2e6 || math.Abs(p.bytes-2e6*60*heldFactors["join"]) > 1 {
		t.Errorf("join: %+v", p)
	}
	// 100 million events of 8 million people joined to 2 million ids: 25
	// million rows, by the larger distinct count of the keys; 365 days,
	// held by each of 8 threads; every joined row of the distinct pairs.
	if p := got["HASH_GROUP_BY"]; p.rows != 365*8 {
		t.Errorf("grouping: %+v", p)
	}
	if p := got["HASH_GROUP_BY DISTINCT"]; p.rows != 1e8*2e6/8e6 {
		t.Errorf("distinct pairs: %+v", p)
	}
	if p := got["ORDER_BY"]; p.rows != 365 {
		t.Errorf("sort: %+v", p)
	}
	var sum float64
	for _, p := range parts {
		sum += p.bytes
	}
	if set != sum {
		t.Errorf("set %.0f, parts add to %.0f", set, sum)
	}
	// A scan of nothing the statement's tables have: no estimate.
	if _, _, err := workingSetOf(strings.Replace(workingSetPlan, `["id", "country"]`, `["other"]`, 1), st); err == nil {
		t.Error("an unknown scan gave a working set")
	}
	if _, _, err := workingSetOf("not json", st); err == nil {
		t.Error("a plan that is not JSON gave a working set")
	}
}

// op is an operator of a plan as DuckDB's EXPLAIN (FORMAT JSON) writes it,
// for the plans the tests make up; extra alternates keys and values.
func op(name string, children []map[string]any, extra ...any) map[string]any {
	info := map[string]any{}
	for i := 0; i+1 < len(extra); i += 2 {
		info[extra[i].(string)] = extra[i+1]
	}
	if children == nil {
		children = []map[string]any{}
	}
	return map[string]any{"name": name, "children": children, "extra_info": info}
}

func planJSON(t *testing.T, root map[string]any) string {
	t.Helper()
	b, err := json.Marshal([]map[string]any{root})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestPlanShapes checks the plan estimates of statements shaped like the
// scale runs' reports whose estimates ran far off.
func TestPlanShapes(t *testing.T) {
	// E1-like: people joined to themselves 16 times, in branches of a
	// UNION, each through a view joining a survey on a computed key; 400
	// million people, 18 million survey rows. DuckDB took the plan to hold
	// 16 hash tables of 400 million rows, 793 GB; it held 33 GB, 14 GB in
	// hash tables, one branch's at a time; and its stages read 77 GB of the
	// people's 470 columns where DuckDB's scans read 9.6 GB.
	people := &Table{Name: "people", Rows: 400e6, Bytes: 77e9, Columns: []Column{{Name: "pid", Type: "VARCHAR"}, {Name: "hh", Type: "VARCHAR"}}}
	for i := range 468 {
		people.Columns = append(people.Columns, Column{Name: fmt.Sprint("trait", i), Type: "VARCHAR"})
	}
	survey := &Table{Name: "survey", Rows: 18e6, Bytes: 1e8, Columns: []Column{{Name: "survey_key", Type: "DECIMAL(38,0)"}}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: people}, {Table: survey}}}}, 24)
	view := func(trait string) map[string]any {
		return op("PROJECTION", []map[string]any{
			op("HASH_JOIN", []map[string]any{
				op("ICEBERG_SCAN", nil, "Projections", []string{"pid", "hh", trait}, "Estimated Cardinality", "400000000"),
				op("ICEBERG_SCAN", nil, "Projections", "survey_key", "Estimated Cardinality", "18000000"),
			}, "Join Type", "LEFT", "Conditions", "TRY_CAST(hh AS DECIMAL(38,0)) = survey_key", "Estimated Cardinality", "400000000"),
		}, "Projections", []string{"pid", trait}, "Estimated Cardinality", "400000000")
	}
	var branches []map[string]any
	for i := range 16 {
		branches = append(branches, op("PROJECTION", []map[string]any{
			op("HASH_JOIN", []map[string]any{view(fmt.Sprint("trait", i)), op("CTE_SCAN", nil, "CTE Index", "0", "Estimated Cardinality", "400000000")},
				"Join Type", "INNER", "Conditions", "pid = pid", "Estimated Cardinality", "400000000"),
		}, "Projections", []string{"pid", "style"}, "Estimated Cardinality", "400000000"))
	}
	e1 := op("CTE", []map[string]any{
		op("PROJECTION", []map[string]any{view("trait0")}, "Projections", "pid", "Estimated Cardinality", "400000000"),
		op("HASH_GROUP_BY", []map[string]any{op("UNION", branches)}, "Groups", []string{"#0", "#1"}, "Aggregates", []string{"count(#2)"},
			"Estimated Cardinality", "16"),
	}, "CTE Name", "everyone", "Table Index", "0", "Estimated Cardinality", "0")
	p, err := planOf(planJSON(t, e1), st)
	if err != nil {
		t.Fatal(err)
	}
	// The same from the tables alone, as a replica estimates a statement before
	// its first run.
	if set, ok := WorkingSet(planJSON(t, e1), []*Table{people, survey}, 24); !ok || set != int64(p.set) {
		t.Errorf("WorkingSet: %d, %v; want %d", set, ok, int64(p.set))
	}
	if _, ok := WorkingSet(planJSON(t, op("ICEBERG_SCAN", nil, "Projections", "pid")), nil, 24); ok {
		t.Error("WorkingSet said what a scan of a table it was not given holds")
	}
	// One branch: 400 million ids of 32 bytes and a join's 20, and its
	// view's survey; and the view the common table expression reads.
	one := 400e6*52*heldFactors["join"] + 18e6*36*heldFactors["join"]
	if p.set < one || p.set > 1.2*one {
		t.Errorf("E1-like: %.1f GB, want about one branch's %.1f GB", p.set/1e9, one/1e9)
	}
	// Two columns and the 16 traits of 470, about a 26th of the table.
	if want := int64(77e9) / 470 * 18; p.bytes < want-1e8 || p.bytes > want+2e8 {
		t.Errorf("E1-like reads %.1f GB, want %.1f GB", float64(p.bytes)/1e9, float64(want)/1e9)
	}
	// 16 joins of 400 million rows to 400 million and 17 to 18 million, and
	// their scans: 3,295 core-seconds by the operators' costs; it took 2,166.
	if took := 2166.0; p.work < took/1.6 || p.work > took*1.6 {
		t.Errorf("E1-like: %.0f core-seconds; it took %.0f", p.work, took)
	}

	// F3-like: 157 million events joined to an audience of everyone, 2
	// million, which DuckDB took to keep a fifth of them; the window over
	// them all (OVER ()); FLATTEN's cross product with six objects; the
	// people counted distinct per value. It held 51 GB in hash tables and
	// spilled 55 GB, and was estimated at 25 GB.
	events := &Table{Name: "events", Rows: 157e6, Bytes: 5e9, Columns: []Column{{Name: "id", Type: "VARCHAR"}, {Name: "day", Type: "DATE"},
		{Name: "genres", Type: "VARCHAR[]"}, {Name: "titles", Type: "VARCHAR[]"}}}
	audience := &Table{Name: "audience", Rows: 2e6, Bytes: 5e7, Columns: []Column{{Name: "member", Type: "VARCHAR"}}}
	st = newPlanStats([]*Stage{{Inputs: []Input{{Table: events}, {Table: audience}}}}, 24)
	f3 := op("CTE", []map[string]any{
		op("HASH_GROUP_BY", []map[string]any{
			op("ICEBERG_SCAN", nil, "Projections", "member", "Filters", "(NOT (member IS NULL))", "Estimated Cardinality", "2000000"),
		}, "Groups", "#0", "Aggregates", "", "Estimated Cardinality", "2000000"),
		op("HASH_GROUP_BY", []map[string]any{
			op("UNNEST", []map[string]any{
				op("BLOCKWISE_NL_JOIN", []map[string]any{
					op("WINDOW", []map[string]any{
						op("HASH_JOIN", []map[string]any{
							op("ICEBERG_SCAN", nil, "Projections", []string{"day", "id", "genres", "titles"}, "Estimated Cardinality", "157000000"),
							op("CTE_SCAN", nil, "CTE Index", "1", "Estimated Cardinality", "2000000"),
						}, "Join Type", "INNER", "Conditions", "id = member", "Estimated Cardinality", "31400000"),
					}, "Projections", []string{"min(day) OVER ()", "max(day) OVER ()"}),
					op("RANGE", nil, "Function", "RANGE", "Estimated Cardinality", "6"),
				}, "Join Type", "INNER", "Condition", "(CASE WHEN (i = 0) THEN genres ELSE titles END IS NOT NULL)"),
			}),
		}, "Groups", []string{"#0", "#1"}, "Aggregates", []string{"count(DISTINCT #2)", "count_star()"}, "Estimated Cardinality", "31400000"),
	}, "CTE Name", "chosen", "Table Index", "1", "Estimated Cardinality", "0")
	p, err = planOf(planJSON(t, f3), st)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]heldPart{}
	for _, part := range p.parts {
		got[part.op] = part
	}
	// An event's id, day and lists, and its member's id.
	if w := got["WINDOW"]; w.rows != 157e6 || math.Abs(w.bytes-157e6*(32+4+64+64+32)*heldFactors["constant window"]) > 1 {
		t.Errorf("F3-like window: %+v, want every event, buffered once", w)
	}
	if d := got["HASH_GROUP_BY DISTINCT"]; d.rows != 6*unnestValues*157e6 {
		t.Errorf("F3-like distinct pairs: %+v, want six objects of what an unnest makes for each event", d)
	}
	if g := got["HASH_GROUP_BY"]; g.rows > 31.4e6*24 {
		t.Errorf("F3-like groups: %+v, no more than DuckDB's for each thread", g)
	}
	// Within four times what it held and spilled, 106 GB.
	if p.set < 100e9 || p.set > 4*106e9 {
		t.Errorf("F3-like: %.0f GB", p.set/1e9)
	}

	// F4-like: an audience lookup of every version, joined to its latest
	// (a scalar subquery's one row), which DuckDB took to keep all 283
	// million rows; then 157 million events joined to the members. It held
	// 9.5 GB and was estimated at 439 GB.
	lookup := &Table{Name: "lookup", Rows: 283e6, Bytes: 4e9, Columns: []Column{{Name: "ver", Type: "INTEGER"}, {Name: "member", Type: "VARCHAR"}}}
	versions := &Table{Name: "versions", Rows: 934, Bytes: 1e5, Columns: []Column{{Name: "version", Type: "INTEGER"}}}
	st = newPlanStats([]*Stage{{Inputs: []Input{{Table: events}, {Table: lookup}, {Table: versions}}}}, 24)
	f4 := op("CTE", []map[string]any{
		op("HASH_GROUP_BY", []map[string]any{
			op("HASH_JOIN", []map[string]any{
				op("ICEBERG_SCAN", nil, "Projections", []string{"ver", "member"}, "Estimated Cardinality", "283000000"),
				op("PROJECTION", []map[string]any{
					op("UNGROUPED_AGGREGATE", []map[string]any{
						op("ICEBERG_SCAN", nil, "Projections", "version", "Estimated Cardinality", "934"),
					}, "Aggregates", "max(#0)"),
				}, "Projections", "#0", "Estimated Cardinality", "1"),
			}, "Join Type", "INNER", "Conditions", "ver = SUBQUERY", "Estimated Cardinality", "283000000"),
		}, "Groups", "#0", "Aggregates", "", "Estimated Cardinality", "283000000"),
		op("HASH_JOIN", []map[string]any{
			op("ICEBERG_SCAN", nil, "Projections", []string{"day", "id", "genres"}, "Estimated Cardinality", "157000000"),
			op("CTE_SCAN", nil, "CTE Index", "2", "Estimated Cardinality", "283000000"),
		}, "Join Type", "INNER", "Conditions", "id = member", "Estimated Cardinality", "283000000"),
	}, "CTE Name", "chosen", "Table Index", "2", "Estimated Cardinality", "0")
	p, err = planOf(planJSON(t, f4), st)
	if err != nil {
		t.Fatal(err)
	}
	if p.set > 20e9 {
		t.Errorf("F4-like: %.0f GB", p.set/1e9)
	}
	// The lookup's columns a hundredth of them, as the latest version's
	// row groups; the events' three columns.
	lookupBytes := int64(float64(columnSet{"ver": true, "member": true}.bytes(lookup)) * filterSelectivity)
	eventBytes := columnSet{"day": true, "id": true, "genres": true}.bytes(events)
	if p.bytes != lookupBytes+eventBytes+versions.Bytes {
		t.Errorf("F4-like reads %d, want %d of the lookup and %d of the events", p.bytes, lookupBytes, eventBytes)
	}
}

// TestSketchedDistinct checks that Iceberg's theta sketches give the
// distinct counts ANALYZE did not, of the current snapshot or the latest.
func TestSketchedDistinct(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "Id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "day", Type: iceberg.PrimitiveTypes.Date})
	blob := func(field int32, ndv string) table.BlobMetadata {
		return table.BlobMetadata{Type: table.BlobTypeApacheDatasketchesThetaV1, Fields: []int32{field}, Properties: map[string]string{"ndv": ndv}}
	}
	files := []table.StatisticsFile{
		{SnapshotID: 1, BlobMetadata: []table.BlobMetadata{blob(1, "10"), blob(2, "5")}},
		{SnapshotID: 2, BlobMetadata: []table.BlobMetadata{blob(1, "1000"), blob(2, "365")}},
	}
	seq := func(yield func(table.StatisticsFile) bool) {
		for _, f := range files {
			if !yield(f) {
				return
			}
		}
	}
	got := withSketchedDistinct(map[string]int64{"day": 400}, seq, schema, &table.Snapshot{SnapshotID: 1})
	if got["id"] != 10 || got["day"] != 400 {
		t.Errorf("snapshot 1: %v", got)
	}
	if got := withSketchedDistinct(nil, seq, schema, &table.Snapshot{SnapshotID: 9}); got["id"] != 1000 || got["day"] != 365 {
		t.Errorf("no statistics of the snapshot, the latest: %v", got)
	}
}

// TestMemoryScale checks the correction of plans' working sets by peaks:
// none until a few statements, within a factor of four, and none from
// small statements or those at the memory limit's share.
func TestMemoryScale(t *testing.T) {
	var m memoryScale
	for range scaleAfter - 1 {
		m.observe(1e9, 2e9)
	}
	if m.scale() != 1 {
		t.Errorf("after %d statements: %v", scaleAfter-1, m.scale())
	}
	for range 100 {
		m.observe(1e9, 2e9)
		m.observe(1e6, 1e9) // small: not taken
	}
	if s := m.scale(); math.Abs(s-2) > 0.01 {
		t.Errorf("statements holding twice their estimate: %v", s)
	}
	for range 100 {
		m.observe(1e9, 1e12)
	}
	if s := m.scale(); math.Abs(s-scaleBound) > 1e-9 {
		t.Errorf("bounded: %v", s)
	}
	co := &Coordinator{history: newHistory(), opts: Options{MemoryBytes: 10e9}}
	co.history.update("k", func(r *runs) { r.planSet = 1e9 })
	for range 10 {
		co.ObservePeak("k", 8e9) // at the limit's share: maybe spilled
	}
	if co.memory.count != 0 {
		t.Error("took in a peak at the memory limit")
	}
	for range 10 {
		co.ObservePeak("k", 3e9)
	}
	if s := co.memory.scale(); s < 2 || s > 3 {
		t.Errorf("after peaks of 3 GB on 1 GB: %v", s)
	}
}

// TestRepeatPlacedByItsPeak checks that a statement that ran to its end on
// the replica is placed, run again, by the most it held then, not by its
// plan's estimate; and not by a peak at the memory's share, which may be
// one spilled.
func TestRepeatPlacedByItsPeak(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	h.co.opts.MemoryBytes = 4 << 30
	for _, name := range []string{"orders", "lineitem"} {
		h.tables[name].Bytes = 1 << 34
	}
	q := `SELECT o_orderpriority, count(*) FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`
	env := harnessEnv{h}
	set := func() (string, float64) {
		t.Helper()
		plan, d, err := h.co.plan(context.Background(), q, env, ModeAuto, false, nil)
		if err != nil || d == nil {
			t.Fatalf("plan: %v", err)
		}
		return plan.Key, d.e.local.set
	}
	key, first := set()
	if first <= 0 {
		t.Fatalf("no working set by the plan: %g", first)
	}
	h.co.ObservePeak(key, int64(0.9*memoryShare*float64(h.co.opts.MemoryBytes))+1<<30)
	if _, again := set(); again != first {
		t.Errorf("after a peak at the memory's share: %g, want the plan's %g", again, first)
	}
	h.co.ObservePeak(key, 300<<20)
	h.co.ObservePeak(key, 200<<20)
	if _, again := set(); again != 300<<20 {
		t.Errorf("run again: %g, want the most it held, %d", again, 300<<20)
	}
}

// TestGroupEntries checks what groupings hold against what DuckDB was seen to
// hold, on 16 threads.
func TestGroupEntries(t *testing.T) {
	for _, c := range []struct{ in, groups, min, max float64 }{
		{16e6, 2000, 0, 2e6},
		{64e6, 10_000, 0, 2e6},
		{16e6, 50_000, 13e6, 16e6},
		{64e6, 50_000, 14e6, 17e6},
		{64e6, 200_000, 19e6, 22e6},
		{16e6, 200_000, 15e6, 16e6},
		{64e6, 500_000, 55e6, 64e6},
		{64e6, 4e6, 55e6, 64e6},
		{1e6, 500_000, 1e6, 1e6},
	} {
		if got := groupEntries(c.in, c.groups, 16); got < c.min || got > c.max {
			t.Errorf("%.0f rows of %.0f groups: %.1f million held, want %.1f to %.1f", c.in, c.groups, got/1e6, c.min/1e6, c.max/1e6)
		}
	}
}

// TestWorkingSetCalibration checks the working sets DuckDB's plans give
// against what DuckDB was seen holding, sampled as the server samples it,
// over joins, groupings, distinct counts, sorts and windows of 16 million
// rows, with distinct counts as ANALYZE keeps them: within a factor of two
// where they hold 300 MB or more, and nearer than six times what the
// stages read, which counts nothing for a join of a table every worker
// reads whole and as much for 1,000 groups as for 16 million.
func TestWorkingSetCalibration(t *testing.T) {
	if testing.Short() {
		t.Skip("writes and reads 20 million rows")
	}
	h := newHarness(t, 0.01, 2, 1)
	ctx := context.Background()
	h.addTable("facts", `(SELECT i AS id, i % 4000000 AS k, i % 1000 AS g, (i * 7919 % 1000003)::DOUBLE AS v, 'name-' || (i % 200000) AS s
		FROM range(16000000) t(i))`, 8)
	h.addTable("dims", `(SELECT i AS k, 'dim-' || i AS name, i % 50 AS cat FROM range(4000000) t(i))`, 4)
	ndv := map[string]map[string]int64{"facts": {"id": 16_000_000, "k": 4_000_000, "g": 1000, "v": 1_000_003, "s": 200_000},
		"dims": {"k": 4_000_000, "name": 4_000_000, "cat": 50}}
	var stages []*Stage
	for name, tb := range h.tables {
		tb.Distinct, tb.Rows = ndv[name], rowCount(h.files[tb.key()])
		stages = append(stages, &Stage{Inputs: []Input{{Table: tb}}})
	}
	st := newPlanStats(stages, runtime.GOMAXPROCS(0))
	var newErr, oldErr float64
	n := 0
	for _, q := range []string{
		`SELECT f.g, sum(f.v) FROM facts f JOIN dims d ON d.k = f.k GROUP BY 1`,
		`SELECT k, sum(v), count(*) FROM facts GROUP BY k ORDER BY 2 DESC LIMIT 5`,
		`SELECT id, s, max(v) FROM facts GROUP BY ALL ORDER BY 3 DESC LIMIT 5`,
		`SELECT g, count(DISTINCT k) FROM facts GROUP BY g ORDER BY 2 DESC LIMIT 5`,
		`SELECT count(DISTINCT s) FROM facts`,
		`SELECT max(r) FROM (SELECT row_number() OVER (PARTITION BY g ORDER BY v) AS r FROM facts)`,
		`SELECT sum(x) FROM (SELECT k, v, s FROM facts ORDER BY v, s) t(k, x, s)`,
		`SELECT g, count(*) FROM facts GROUP BY g`,
		`SELECT s, sum(v) FROM facts GROUP BY s`,
	} {
		plan, err := harnessEnv{h}.Explain(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		est, _, err := workingSetOf(plan, st)
		if err != nil {
			t.Fatal(err)
		}
		old := 0.0
		if p, _, err := h.co.plan(ctx, q, harnessEnv{h}, ModeOn, true, nil); err == nil {
			old = planFeatures(p.Stages).set()
		}
		peak := heldPeak(t, h, q)
		t.Logf("held %4.0f MB, by DuckDB's plan %4.0f MB (%.2f), by what the stages read %4.0f MB: %s", peak/1e6, est/1e6, est/peak, old/1e6, q)
		if peak < 300e6 {
			continue
		}
		if est > 2*peak || est < peak/2 {
			t.Errorf("held %.0f MB, estimated %.0f: %s", peak/1e6, est/1e6, q)
		}
		newErr += math.Abs(math.Log(max(est, 1e6) / peak))
		oldErr += math.Abs(math.Log(max(old, 1e6) / peak))
		n++
	}
	if n < 5 || newErr >= oldErr {
		t.Errorf("over %d statements: by DuckDB's plans %.2f times off at the geometric mean, by what the stages read %.2f", n,
			math.Exp(newErr/float64(n)), math.Exp(oldErr/float64(n)))
	}
}

// TestWorkingSetFlatten checks a statement shaped like F3-like against
// what DuckDB holds: 3 million events of lists joined to an audience of
// everyone, a window over them all, FLATTEN's cross product with four
// lists and their values unnested, people counted distinct per value.
// DuckDB's plan took the join to keep a fifth of the events and each to
// make one row, a working set of a tenth of what it held. Corrected, and
// counting about what the unnest makes, the estimate is within a third of it.
func TestWorkingSetFlatten(t *testing.T) {
	if testing.Short() {
		t.Skip("writes and reads 4 million rows")
	}
	h := newHarness(t, 0.01, 2, 1)
	const n = 1_000_000
	h.addTable("aud", fmt.Sprintf(`(SELECT md5(i::VARCHAR) AS pid, i %% 90 AS age FROM range(%d) t(i))`, n), 4)
	list := func(prefix string, k, mod int) string {
		var items []string
		for j := range k {
			items = append(items, fmt.Sprintf("'%s-' || ((i * %d + %d) %% %d)", prefix, j+1, j, mod))
		}
		return "[" + strings.Join(items, ", ") + "]"
	}
	h.addTable("media", fmt.Sprintf(`(SELECT md5((i %% %d)::VARCHAR) AS pid, DATE '2026-01-01' + (i %% 60)::INTEGER AS d,
		%s AS genre, %s AS title, %s AS network, %s AS app FROM range(%d) t(i))`, n, list("genre", 3, 40), list("title", 6, 5000),
		list("net", 2, 80), list("app", 2, 30), 3*n), 8)
	var stages []*Stage
	for _, tb := range h.tables {
		tb.Rows = rowCount(h.files[tb.key()])
		stages = append(stages, &Stage{Inputs: []Input{{Table: tb}}})
	}
	q := `WITH sel AS (SELECT DISTINCT a.pid AS pid FROM aud AS a WHERE a.pid IS NOT NULL),
		base AS (SELECT m.pid, m.d, m.genre, m.title, m.network, m.app, min(m.d) OVER () AS ps, max(m.d) OVER () AS pe
			FROM media AS m INNER JOIN sel AS a ON a.pid = m.pid),
		expanded AS (SELECT 'media' AS source, b.ps, b.pe,
			CASE r.i WHEN 0 THEN 'genre' WHEN 1 THEN 'title' WHEN 2 THEN 'network' ELSE 'app' END AS btype,
			unnest(CASE r.i WHEN 0 THEN b.genre WHEN 1 THEN b.title WHEN 2 THEN b.network ELSE b.app END) AS bvalue, b.pid, b.d
			FROM base AS b JOIN range(4) AS r(i) ON (CASE r.i WHEN 0 THEN b.genre WHEN 1 THEN b.title WHEN 2 THEN b.network ELSE b.app END) IS NOT NULL)
		SELECT source, min(ps), max(pe), btype, bvalue, count(DISTINCT pid) AS members, count(*) AS activity
		FROM expanded GROUP BY source, btype, bvalue`
	plan, err := harnessEnv{h}.Explain(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	est, _, err := workingSetOf(plan, newPlanStats(stages, runtime.GOMAXPROCS(0)))
	if err != nil {
		t.Fatal(err)
	}
	peak := heldPeak(t, h, q)
	t.Logf("held %.0f MB, estimated %.0f MB", peak/1e6, est/1e6)
	if est < peak*2/3 || est > peak*4/3 {
		t.Errorf("held %.0f MB, estimated %.0f MB", peak/1e6, est/1e6)
	}
}

// heldPeak runs query on the harness's session and returns the most memory
// DuckDB held for it, as the server samples it (engine's queryMemory),
// every 5 ms, over what it held before.
func heldPeak(t *testing.T, h *harness, query string) float64 {
	const held = `SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory()
		WHERE tag IN ('HASH_TABLE', 'ORDER_BY', 'WINDOW', 'COLUMN_DATA', 'ALLOCATOR', 'OVERFLOW_STRINGS',
			'PARQUET_READER', 'CSV_READER', 'TRANSACTION')`
	ctx := context.Background()
	conn, err := h.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var base float64
	if err := conn.QueryRowContext(ctx, held).Scan(&base); err != nil {
		t.Fatal(err)
	}
	var peak atomic.Int64
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			var v float64
			if conn.QueryRowContext(ctx, held).Scan(&v) == nil && int64(v-base) > peak.Load() {
				peak.Store(int64(v - base))
			}
		}
	}()
	rows, err := h.session.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
	}
	if err := rows.Close(); err != nil {
		t.Fatal(fmt.Errorf("%s: %w", query, err))
	}
	close(done)
	<-stopped
	return float64(max(peak.Load(), 1))
}

// TestWorkingSetOfUnion checks a report that joins its events, made a row for
// each of seven channels, to a union of an audience, one version's rows of a
// lookup, and everyone: the union's rows are its branches', what its filters
// keep the most any branch keeps, so the join keeps every event, and the
// operators above the cross product count its seven once. The cohorts' sizes,
// grouped by a key nothing knows and one group by DuckDB's estimate, are not a
// scalar subquery's one row, whose scans would read a hundredth; the audience's
// version is. It was estimated at 5.6 GB, reading 0.4 GB, and 15 s on one
// machine, where its like took 714 s and spilled 235 GB.
func TestWorkingSetOfUnion(t *testing.T) {
	cols := func(names ...string) []Column {
		var out []Column
		for _, n := range names {
			out = append(out, Column{Name: n, Type: "VARCHAR"})
		}
		return out
	}
	lookup := &Table{Name: "lookup", Rows: 1_400_000_000, Bytes: 75e9, Columns: cols("audience", "version", "person")}
	versions := &Table{Name: "versions", Rows: 2000, Bytes: 1e5, Columns: cols("audience", "ver_no")}
	people := &Table{Name: "people", Rows: 10_000_000, Bytes: 1e9, Columns: cols("person", "age")}
	events := &Table{Name: "events", Rows: 360_000_000, Bytes: 12e9, Columns: cols("person", "seconds", "channel", "title")}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: lookup}, {Table: versions}, {Table: people}, {Table: events}}}}, 16)

	audience := op("ICEBERG_SCAN", nil, "Projections", []string{"version", "person"}, "Filters", "audience='a'", "Estimated Cardinality", "1500000")
	latest := op("PROJECTION", []map[string]any{op("UNGROUPED_AGGREGATE", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"ver_no"}, "Filters", "audience='a'", "Estimated Cardinality", "186")}, "Aggregates", []string{"max(#0)"})},
		"Projections", []string{"#0"}, "Estimated Cardinality", "1")
	inAudience := op("HASH_GROUP_BY", []map[string]any{op("HASH_JOIN", []map[string]any{audience, latest},
		"Join Type", "INNER", "Conditions", "version = SUBQUERY", "Estimated Cardinality", "1500000")},
		"Groups", []string{"#1"}, "Aggregates", []string{}, "Estimated Cardinality", "1500000")
	everyone := op("HASH_GROUP_BY", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"person"}, "Filters", "(NOT (person IS NULL))",
		"Estimated Cardinality", "2000000")}, "Groups", []string{"#0"}, "Aggregates", []string{}, "Estimated Cardinality", "2000000")
	cohort := func(card string) map[string]any {
		return op("CTE_SCAN", nil, "CTE Index", "6", "Estimated Cardinality", card)
	}
	channels := op("FILTER", []map[string]any{op("PROJECTION", []map[string]any{op("CROSS_PRODUCT", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"person", "seconds", "channel", "title"}, "Estimated Cardinality", "360000000"),
		op("RANGE", nil, "Estimated Cardinality", "7")})},
		"Projections", []string{"person", "seconds", "#2"}, "Estimated Cardinality", "2520000000")}, "Estimated Cardinality", "2520000000")
	signals := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{op("HASH_JOIN", []map[string]any{channels, cohort("3500000")},
		"Join Type", "INNER", "Conditions", "person = person", "Estimated Cardinality", "890000000")},
		"Projections", []string{"#2", "#0", "#1"}, "Estimated Cardinality", "890000000")},
		"Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)", "sum(#2)"}, "Estimated Cardinality", "330000000")
	traits := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{op("HASH_JOIN", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"person", "age"}, "Estimated Cardinality", "10000000"), cohort("3500000")},
		"Join Type", "INNER", "Conditions", "person = person", "Estimated Cardinality", "3500000")},
		"Projections", []string{"#1", "#0"}, "Estimated Cardinality", "3500000")},
		"Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)"}, "Estimated Cardinality", "100")
	sizes := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{cohort("3500000")}, "Projections", []string{"#0", "#1"},
		"Estimated Cardinality", "3500000")}, "Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)"}, "Estimated Cardinality", "1")
	report := op("CTE", []map[string]any{
		op("UNION", []map[string]any{inAudience, everyone}),
		op("HASH_JOIN", []map[string]any{op("UNION", []map[string]any{signals, traits}), sizes},
			"Join Type", "INNER", "Conditions", "upper(cohort) = upper(cohort)", "Estimated Cardinality", "53"),
	}, "CTE Name", "cohorts", "Table Index", "6", "Estimated Cardinality", "0")

	p, err := planOf(planJSON(t, report), st)
	if err != nil {
		t.Fatal(err)
	}
	var grouped, distinct float64
	for _, h := range p.parts {
		switch h.op {
		case "HASH_GROUP_BY":
			grouped = max(grouped, h.rows)
		case "HASH_GROUP_BY DISTINCT":
			distinct = max(distinct, h.rows)
		}
	}
	// Seven rows for each of 360 million events, all of them joined, once.
	if distinct != 2.52e9 || grouped != 2.52e9 {
		t.Errorf("the events' grouping holds %g rows and its distinct pairs %g, want 2.52e9: %+v", grouped, distinct, p.parts)
	}
	// The events' columns and everyone's in full, and of the lookup, read for
	// one version of an audience, a hundredth.
	wantBytes := int64(float64(events.Bytes) + float64(people.Bytes) + float64(lookup.Bytes)*2/3*filterSelectivity)
	if p.bytes < wantBytes/10*9 || p.bytes > wantBytes/10*11 {
		t.Errorf("reads %d bytes, want about %d", p.bytes, wantBytes)
	}
	if p.set < 200e9 || p.work < 500 {
		t.Errorf("working set %g, hash tables' work %g core-seconds", p.set, p.work)
	}

	x := features{bytes: 12e9, exchanged: 50e9, held: 12e9, rows: 2e10, levels: 11, stages: 24, planned: true, planSet: p.set, planBytes: p.bytes,
		planWork: p.work, planKeeps: p.keeps, planFields: p.fields, planSorted: p.sorted, planJoins: p.joins}
	m := machine{cores: 16, threads: 16, memory: 47 << 30}
	e := x.estimate(m, fleet{cores: 16, threads: 16, running: 4, most: 4, memory: 26 << 30})
	if !e.spills || e.oneMachine < 200 {
		t.Errorf("one machine %.0f s, spills %v, %s", e.oneMachine, e.spills, e.local)
	}
	now := time.Now()
	if workers, explore, reason := choose(e, runs{}, false, DefaultThreshold, now); !workers || explore {
		t.Errorf("its first run: on workers %v, trying %v: %s", workers, explore, reason)
	}
	if workers, explore, reason := choose(e, runs{dist: 200}, true, DefaultThreshold, now); !workers || explore {
		t.Errorf("after 200 s on workers: on workers %v, trying %v: %s", workers, explore, reason)
	}

	// One row by what the plan says of its keys is one row still.
	st.distinct["age"] = 1
	one := op("HASH_GROUP_BY", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"age"}, "Estimated Cardinality", "1000")},
		"Groups", []string{"#0"}, "Aggregates", []string{}, "Estimated Cardinality", "1")
	joined := op("HASH_JOIN", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"person", "seconds", "channel", "title"}, "Estimated Cardinality", "360000000"), one},
		"Join Type", "INNER", "Conditions", "title = age", "Estimated Cardinality", "360000000")
	if p, err = planOf(planJSON(t, joined), st); err != nil {
		t.Fatal(err)
	}
	if want := int64(float64(events.Bytes)*filterSelectivity + float64(people.Bytes)/2); p.bytes != want {
		t.Errorf("joined to a key of one value: reads %d bytes, want the events' hundredth and a column of people, %d", p.bytes, want)
	}
}

// TestWorkingSetOfGuesses checks a report whose plan, at a hundred times the
// data, compounds DuckDB's guesses: groups of a tenth of their input, by keys it
// computes, which were thousands; their metrics joined to the cohorts' sizes and
// to themselves, each taken for more than its inputs, and each corrected as its
// inputs were; and windows over that. It was estimated at 37.9 TB, 25 TB of it a
// window, so that every way on workers should run out of memory, and ran on one
// machine; 16 workers took 521 to 825 s. What its aggregates hold of their input
// stands; a join does not multiply the rows of the side with more without its
// keys' distinct counts, and a grouping by computed keys keeps a hundredth.
func TestWorkingSetOfGuesses(t *testing.T) {
	cols := func(names ...string) []Column {
		var out []Column
		for _, n := range names {
			out = append(out, Column{Name: n, Type: "VARCHAR"})
		}
		return out
	}
	lookup := &Table{Name: "lookup", Rows: 14_000_000_000, Bytes: 750e9, Columns: cols("audience", "version", "person")}
	people := &Table{Name: "people", Rows: 100_000_000, Bytes: 10e9, Columns: cols("person", "age")}
	events := &Table{Name: "events", Rows: 3_600_000_000, Bytes: 120e9, Columns: cols("person", "seconds", "channel", "title")}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: lookup}, {Table: people}, {Table: events}}}}, 16)
	cohorts := op("HASH_GROUP_BY", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"person"}, "Filters", "(NOT (person IS NULL))",
		"Estimated Cardinality", "2000000000")}, "Groups", []string{"#0"}, "Aggregates", []string{}, "Estimated Cardinality", "2000000000")
	cohort := op("CTE_SCAN", nil, "CTE Index", "6", "Estimated Cardinality", "2000000000")
	signals := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{op("HASH_JOIN", []map[string]any{
		op("FILTER", []map[string]any{op("PROJECTION", []map[string]any{op("CROSS_PRODUCT", []map[string]any{
			op("ICEBERG_SCAN", nil, "Projections", []string{"person", "seconds", "channel", "title"}, "Estimated Cardinality", "3600000000"),
			op("RANGE", nil, "Estimated Cardinality", "7")})},
			"Projections", []string{"person", "struct_extract(x, 'dimension')", "struct_extract(x, 'value')"}, "Estimated Cardinality", "25200000000")}, "Estimated Cardinality", "25200000000"),
		cohort}, "Join Type", "INNER", "Conditions", "person = person", "Estimated Cardinality", "20000000000")},
		"Projections", []string{"#1", "#2", "#0"}, "Estimated Cardinality", "20000000000")},
		"Groups", []string{"#0", "#1"}, "Aggregates", []string{"count(DISTINCT #2)"}, "Estimated Cardinality", "3600000000")
	traits := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{op("HASH_JOIN", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"person", "age"}, "Estimated Cardinality", "100000000"), cohort},
		"Join Type", "INNER", "Conditions", "person = person", "Estimated Cardinality", "100000000000")},
		"Projections", []string{"upper(#1)", "#0"}, "Estimated Cardinality", "100000000000")},
		"Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)"}, "Estimated Cardinality", "100000000")
	sizes := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{cohort}, "Projections", []string{"upper(person)", "#0"},
		"Estimated Cardinality", "2000000000")}, "Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)"}, "Estimated Cardinality", "1")
	metrics := op("PROJECTION", []map[string]any{op("HASH_JOIN", []map[string]any{op("UNION", []map[string]any{signals, traits}), sizes},
		"Join Type", "INNER", "Conditions", "upper(cohort) = upper(cohort)", "Estimated Cardinality", "15000000000")},
		"Projections", []string{"#0", "#1", "#2", "#3"}, "Estimated Cardinality", "15000000000")
	metric := op("CTE_SCAN", nil, "CTE Index", "12", "Estimated Cardinality", "15000000000")
	ranked := op("ORDER_BY", []map[string]any{op("WINDOW", []map[string]any{op("HASH_JOIN", []map[string]any{metric, metric},
		"Join Type", "INNER", "Conditions", []string{"upper(a) = upper(a)", "upper(b) = upper(b)"}, "Estimated Cardinality", "31000000000")},
		"Projections", []string{"rank() OVER (PARTITION BY #0 ORDER BY #1)"})})
	report := op("CTE", []map[string]any{cohorts, op("CTE", []map[string]any{metrics, op("TOP_N", []map[string]any{ranked}, "Top", "10000")},
		"CTE Name", "metrics", "Table Index", "12", "Estimated Cardinality", "31000000000")},
		"CTE Name", "cohorts", "Table Index", "6", "Estimated Cardinality", "31000000000")

	p, err := planOf(planJSON(t, report), st)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]heldPart{}
	for _, h := range p.parts {
		if h.rows > got[h.op].rows {
			got[h.op] = h
		}
	}
	// Seven rows for each of 3.6 billion events, on their way to their groups.
	if g := got["HASH_GROUP_BY DISTINCT"]; g.rows != 2.52e10 {
		t.Errorf("distinct pairs: %+v", g)
	}
	// A hundredth of them and of what the people came to, for what is above.
	if w := got["WINDOW"]; w.rows > 3e8 {
		t.Errorf("window: %+v, in a working set of %.0f GB", w, p.set/1e9)
	}
	if p.sorted > 100e9 {
		t.Errorf("sorts hold %.0f GB", p.sorted/1e9)
	}
	if p.set < 2e12 || p.set > 6e12 {
		t.Errorf("working set %.0f GB", p.set/1e9)
	}
	// The result is what TOP_N keeps, for the client.
	if p.fields > 1e6 {
		t.Errorf("%g fields of a result of 10,000 rows", p.fields)
	}
	x := features{bytes: 120e9, exchanged: 600e9, held: 120e9, rows: 1.9e11, levels: 11, stages: 25, planned: true, planSet: p.set, planBytes: p.bytes,
		planWork: p.work, planKeeps: p.keeps, planFields: p.fields, planSorted: p.sorted, planJoins: p.joins}
	m := machine{cores: 16, threads: 16, memory: 44e9}
	e := x.estimate(m, fleet{cores: 16, threads: 16, running: 16, most: 16, memory: 26 << 30})
	if e.workers != 16 || math.IsInf(e.onWorkers, 1) || !e.spills || !e.local.oom {
		t.Errorf("%d workers, %.0f s; one machine %.0f s, spills %v, %s", e.workers, e.onWorkers, e.oneMachine, e.spills, e.local)
	}
	if workers, explore, reason := choose(e, runs{}, false, DefaultThreshold, time.Now()); !workers || explore {
		t.Errorf("on workers %v, trying %v: %s", workers, explore, reason)
	}
	// As it was: a sort beyond every way on workers, and still the most of them.
	x.planSet, x.planSorted = 37.9e12, 25e12
	e = x.estimate(m, fleet{cores: 16, threads: 16, running: 4, most: 16, memory: 26 << 30})
	workers, explore, reason := choose(e, runs{}, false, DefaultThreshold, time.Now())
	if !workers || explore || e.workers != 16 || !strings.Contains(e.fleet, "short of memory") {
		t.Errorf("every way short of memory: on workers %v on %d, trying %v: %s; %s", workers, e.workers, explore, reason, e.fleet)
	}
	if e = x.estimate(m, fleet{}); math.IsInf(e.oneMachine, 1) {
		t.Errorf("no workers: one machine %.0f s", e.oneMachine)
	}
	if workers, _, reason := choose(e, runs{}, false, DefaultThreshold, time.Now()); workers || !strings.Contains(reason, "no workers") {
		t.Errorf("no workers: on workers %v: %s", workers, reason)
	}
}

// TestLatestVersionOfAnAudience checks a report that reads one audience's latest
// version: DuckDB's rows of the lookup are one audience's already, its filter
// counted, which is not a filter's hundredth again; so of a report that took 216 s
// on one machine and spilled 36 GB, estimated at 52 s with a working set of 0.2 GB.
// The members keep a row each of what is joined to them, not a fifth of it. Of a
// lookup not filtered so, the latest version still counts as a filter does (see
// F4-like in TestPlanShapes).
func TestLatestVersionOfAnAudience(t *testing.T) {
	lookup := &Table{Name: "lookup", Rows: 6_300_000_000, Bytes: 128e9, Columns: []Column{{Name: "audience", Type: "VARCHAR"}, {Name: "version", Type: "INTEGER"},
		{Name: "member", Type: "VARCHAR"}}}
	versions := &Table{Name: "versions", Rows: 2000, Bytes: 1e5, Columns: []Column{{Name: "audience", Type: "VARCHAR"}, {Name: "ver_no", Type: "INTEGER"}}}
	people := &Table{Name: "people", Rows: 400_000_000, Bytes: 8e9, Columns: []Column{{Name: "id", Type: "VARCHAR"}, {Name: "day", Type: "DATE"}, {Name: "interests", Type: "VARCHAR[]"}}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: lookup}, {Table: versions}, {Table: people}}}}, 16)
	latest := op("PROJECTION", []map[string]any{op("UNGROUPED_AGGREGATE", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"ver_no"}, "Filters", "audience='a'", "Estimated Cardinality", "22")}, "Aggregates", []string{"max(#0)"})},
		"Projections", []string{"#0"}, "Estimated Cardinality", "1")
	members := op("HASH_GROUP_BY", []map[string]any{op("HASH_JOIN", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"version", "member"}, "Filters", "audience='a'", "Estimated Cardinality", "57800000"), latest},
		"Join Type", "INNER", "Conditions", "version = SUBQUERY", "Estimated Cardinality", "57800000")},
		"Groups", []string{"#1"}, "Aggregates", []string{}, "Estimated Cardinality", "57800000")
	chosen := op("HASH_JOIN", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"id", "day", "interests"}, "Filters", "day>='2000-01-01'::DATE",
		"Estimated Cardinality", "80000000"), members}, "Join Type", "SEMI", "Conditions", "id = #0", "Estimated Cardinality", "16000000")
	p, err := planOf(planJSON(t, chosen), st)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]float64{}
	for _, h := range p.parts {
		held[h.op] = max(held[h.op], h.rows)
	}
	if held["HASH_JOIN"] != 57.8e6 {
		t.Errorf("the join to the members holds %g rows, want all of DuckDB's for one audience: %+v", held["HASH_JOIN"], p.parts)
	}
	grouped := op("HASH_GROUP_BY", []map[string]any{chosen}, "Groups", []string{"#2"}, "Aggregates", []string{"count_star()"}, "Estimated Cardinality", "16000000")
	if p, err = planOf(planJSON(t, grouped), st); err != nil {
		t.Fatal(err)
	}
	for _, h := range p.parts {
		held[h.op] = max(held[h.op], h.rows)
	}
	// As many as the members, where DuckDB took a fifth of what it read.
	if held["HASH_GROUP_BY"] < 57.8e6 {
		t.Errorf("what was joined to the members: %g rows: %+v", held["HASH_GROUP_BY"], p.parts)
	}
	// Reading the lookup for one audience's latest version reads a hundredth.
	if want := int64(float64(columnSet{"version": true, "member": true}.bytes(lookup)) * filterSelectivity); p.bytes < want || p.bytes > want+8e9+1e5 {
		t.Errorf("reads %d bytes, want %d of the lookup and all of the people", p.bytes, want)
	}
	x := features{bytes: 7e9, exchanged: 83e9, held: 7e9, rows: 5.75e9, levels: 5, stages: 9, planned: true, planSet: p.set, planBytes: p.bytes, planWork: p.work,
		planKeeps: p.keeps, planJoins: p.joins}
	e := x.estimate(machine{cores: 16, threads: 16, memory: 44e9}, fleet{cores: 16, threads: 16, running: 16, most: 16, memory: 26 << 30})
	if !e.memory || e.onWorkers > e.oneMachine {
		t.Fatalf("one machine %.0f s, short of memory %v; workers %.0f s", e.oneMachine, e.memory, e.onWorkers)
	}
	if workers, explore, reason := choose(e, runs{}, false, DefaultThreshold, time.Now()); !workers || explore {
		t.Errorf("its first run: on workers %v, trying %v: %s", workers, explore, reason)
	}
}

// TestPlanKeeps checks the share of the rows a plan's scans read that its
// result keeps, which a fill writes (see fillCost): all of them for a
// copy, a filter's share for a filtered one, and no more than a hundredth
// for an aggregate, whose groups DuckDB takes for all its input rows.
func TestPlanKeeps(t *testing.T) {
	events := &Table{Name: "events", Rows: 1e9, Bytes: 5e10, Columns: []Column{{Name: "id", Type: "BIGINT"}, {Name: "day", Type: "DATE"},
		{Name: "grp", Type: "INTEGER"}}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: events}}}}, 16)
	scan := func(card string, extra ...any) map[string]any {
		return op("ICEBERG_SCAN", nil, append([]any{"Projections", []string{"id", "day", "grp"}, "Estimated Cardinality", card}, extra...)...)
	}
	for _, c := range []struct {
		name string
		plan map[string]any
		want float64
	}{
		{"copy", op("PROJECTION", []map[string]any{scan("1000000000")}, "Projections", []string{"id", "day", "grp"}), 1},
		{"filtered copy", op("PROJECTION", []map[string]any{scan("40000000", "Filters", "grp<2")}, "Projections", []string{"id", "day", "grp"}), 0.04},
		{"aggregate", op("ORDER_BY", []map[string]any{op("HASH_GROUP_BY", []map[string]any{scan("1000000000")},
			"Groups", []string{"#0", "#1"}, "Aggregates", []string{"count(DISTINCT #2)"}, "Estimated Cardinality", "1000000000")}), aggregateKeeps},
	} {
		p, err := planOf(planJSON(t, c.plan), st)
		if err != nil {
			t.Fatal(err)
		}
		if math.Abs(p.keeps-c.want) > c.want/100 {
			t.Errorf("%s: keeps %g, want %g", c.name, p.keeps, c.want)
		}
	}
}

// TestFilterShares checks what filters are taken to keep (see filtered):
// near all of a table for filters that a column is set, not empty or
// recent, which DuckDB takes for a fifth; ranges of one column together;
// an OR as its branches; DuckDB's own estimate for an equality of a column
// nothing here knows; and of a join to a filtered dimension, the share of
// the keys the facts have that the filter keeps.
func TestFilterShares(t *testing.T) {
	people := &Table{Name: "people", Rows: 1e7, Bytes: 1e9, Columns: []Column{{Name: "id", Type: "VARCHAR"}, {Name: "segment", Type: "VARCHAR"},
		{Name: "born", Type: "INTEGER"}, {Name: "seen", Type: "TIMESTAMP"}, {Name: "kind", Type: "INTEGER"}, {Name: "city", Type: "VARCHAR"}},
		Bounds:   map[string][2]float64{"born": {1900, 2099}, "seen": {0, 864e9}},
		Distinct: map[string]int64{"kind": 10, "city": 50}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: people}}}}, 16)
	scan := func(card string, filters ...string) map[string]any {
		extra := []any{"Projections", []string{"id", "segment", "born", "seen", "kind", "city"}, "Estimated Cardinality", card}
		if len(filters) > 0 {
			extra = append(extra, "Filters", filters)
		}
		return op("ICEBERG_SCAN", nil, extra...)
	}
	for _, c := range []struct {
		name string
		plan map[string]any
		want float64
	}{
		{"set, not empty, seen in the last nine of ten days",
			scan("2000000", "(NOT (id IS NULL))", "segment != ''", "seen>='1970-01-02 00:00:00'::TIMESTAMP"), 0.9},
		{"born 1999 to 2001", scan("2000000", "born>=1999", "born<=2001"), 3.0 / 200},
		{"a kind or a city", scan("2000000", "((kind = 1) OR (city = 'x'))"), 0.1 + 0.02},
		{"optional", scan("2000000", "optional: kind IN (1, 2)"), 1},
		{"a segment", scan("3300", "segment='a'"), 3300 / 1e7},
		{"a segment, set", scan("660", "segment='a'", "(NOT (id IS NULL))"), 3300 / 1e7},
		{"seen in a filter", op("FILTER", []map[string]any{scan("10000000")}, "Expression", "(seen >= '1970-01-02 00:00:00'::TIMESTAMP)",
			"Estimated Cardinality", "2000000"), 0.9},
	} {
		p, err := planOf(planJSON(t, c.plan), st)
		if err != nil {
			t.Fatal(err)
		}
		if got := p.rows / 1e7; math.Abs(got-c.want) > c.want/100 {
			t.Errorf("%s: keeps %g, want %g", c.name, got, c.want)
		}
	}

	// Two centuries of days, of which a year is kept, joined to five years
	// of sales: a fifth of them, which DuckDB took for 500,000.
	days := &Table{Name: "days", Rows: 73049, Bytes: 1e6, Columns: []Column{{Name: "day_key", Type: "INTEGER"}, {Name: "year", Type: "INTEGER"}},
		Bounds: map[string][2]float64{"day_key": {0, 73048}, "year": {1900, 2099}}}
	sales := &Table{Name: "sales", Rows: 1e8, Bytes: 1e9, Columns: []Column{{Name: "sold", Type: "INTEGER"}, {Name: "amount", Type: "BIGINT"}},
		Bounds: map[string][2]float64{"sold": {36500, 38325}}}
	st = newPlanStats([]*Stage{{Inputs: []Input{{Table: days}, {Table: sales}}}}, 16)
	join := op("HASH_JOIN", []map[string]any{
		op("ICEBERG_SCAN", nil, "Projections", []string{"sold", "amount"}, "Estimated Cardinality", "100000000"),
		op("ICEBERG_SCAN", nil, "Projections", []string{"day_key"}, "Filters", "year=2001", "Estimated Cardinality", "14610")},
		"Join Type", "INNER", "Conditions", "sold = day_key", "Estimated Cardinality", "500000")
	p, err := planOf(planJSON(t, join), st)
	if err != nil {
		t.Fatal(err)
	}
	if p.rows < 1.9e7 || p.rows > 2.1e7 {
		t.Errorf("a year of five years' sales: %g rows", p.rows)
	}
}

// TestPlanResult checks the fields of a plan's result, which a read sends
// its client, and what its largest sort holds.
func TestPlanResult(t *testing.T) {
	events := &Table{Name: "events", Rows: 1e9, Bytes: 5e10, Columns: []Column{{Name: "id", Type: "BIGINT"}, {Name: "day", Type: "DATE"},
		{Name: "grp", Type: "INTEGER"}, {Name: "amt", Type: "BIGINT"}}}
	st := newPlanStats([]*Stage{{Inputs: []Input{{Table: events}}}}, 16)
	plan := op("ORDER_BY", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"id", "day", "grp", "amt"},
		"Estimated Cardinality", "1000000000")}, "Order By", []string{"id ASC"})
	p, err := planOf(planJSON(t, plan), st)
	if err != nil {
		t.Fatal(err)
	}
	// The sort is the working set, but for what the scan's readers hold.
	if p.fields != 4e9 || p.sorted < 1e9*24 || p.set-p.sorted > 0.02*p.set {
		t.Errorf("fields %g, sorted %g of %g", p.fields, p.sorted, p.set)
	}

	// An aggregate's result, by a table's column whose distinct count nothing
	// knows and whose groups DuckDB takes for most of its input: no more than
	// aggregateKeeps of its rows reach the client, so that the time to take them
	// does not hide what more workers save. At a hundred times the data, a report
	// that took 25 s on 16 workers was estimated at 3,375 s there and 3,999 s on
	// 2, took 2 as within a fifth of the fastest, and took 449 to 619 s.
	facts := &Table{Name: "facts", Rows: 3_600_000_000, Bytes: 36e9, Columns: []Column{{Name: "genre", Type: "VARCHAR"}, {Name: "viewer", Type: "VARCHAR"},
		{Name: "seconds", Type: "BIGINT"}}}
	st = newPlanStats([]*Stage{{Inputs: []Input{{Table: facts}}}}, 16)
	byGenre := op("HASH_GROUP_BY", []map[string]any{op("PROJECTION", []map[string]any{op("ICEBERG_SCAN", nil, "Projections", []string{"genre", "viewer", "seconds"},
		"Estimated Cardinality", "3600000000")}, "Projections", []string{"genre", "viewer", "seconds"}, "Estimated Cardinality", "3600000000")},
		"Groups", []string{"#0"}, "Aggregates", []string{"count(DISTINCT #1)", "sum(#2)", "count_star()"}, "Estimated Cardinality", "2280000000")
	if p, err = planOf(planJSON(t, byGenre), st); err != nil {
		t.Fatal(err)
	}
	if want := aggregateKeeps * 3.6e9 * 4; p.fields != want {
		t.Errorf("an aggregate's fields %g, want %g", p.fields, want)
	}
	x := features{bytes: 36e9, exchanged: 36e9, held: 36e9, rows: 1.45e10, levels: 3, stages: 3, planned: true, planSet: p.set, planBytes: p.bytes, planWork: p.work,
		planKeeps: p.keeps, planFields: p.fields, planJoins: p.joins}
	e := x.estimate(machine{cores: 16, threads: 16, memory: 44e9}, fleet{cores: 16, threads: 16, running: 16, most: 16, memory: 26 << 30})
	if e.workers < 8 || e.large {
		t.Errorf("%d workers, %.0f s: %s", e.workers, e.onWorkers, e.fleet)
	}
	if workers, explore, reason := choose(e, runs{}, false, DefaultThreshold, time.Now()); !workers || explore {
		t.Errorf("on workers %v, trying %v: %s", workers, explore, reason)
	}
}
