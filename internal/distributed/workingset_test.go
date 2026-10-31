package distributed

import (
	"context"
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
	// and a country (8 + 32 bytes) and a join's 32, times 1.5.
	if p := got["HASH_JOIN"]; p.rows != 2e6 || math.Abs(p.bytes-2e6*72*heldFactors["join"]) > 1 {
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
