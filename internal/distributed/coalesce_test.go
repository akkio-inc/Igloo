package distributed

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

// TestGroupPartitions checks how a stage's partitions are grouped into
// tasks: within the budget, never fewer than one a worker, a large
// partition alone and never split, and each partition in one group.
func TestGroupPartitions(t *testing.T) {
	const mb = 1 << 20
	for _, c := range []struct {
		name   string
		mem    []int64
		budget int64
		tasks  int
		want   int // groups
	}{
		{"small partitions, one task a worker", []int64{mb, mb, mb, mb, mb, mb, mb, mb}, 100 * mb, 3, 3},
		{"no budget: a task a partition", []int64{mb, mb, mb, mb}, 0, 2, 4},
		{"as many as the budget needs", []int64{40 * mb, 40 * mb, 40 * mb, 40 * mb, 40 * mb, 40 * mb}, 100 * mb, 2, 3},
		{"partitions over budget stay alone", []int64{300 * mb, 300 * mb, 10 * mb, 10 * mb}, 100 * mb, 1, 3},
		{"fewer partitions than workers", []int64{mb, mb}, 100 * mb, 3, 2},
		{"empty partitions", []int64{0, 0, 0, 0, 0, 0}, 100 * mb, 3, 3},
	} {
		groups := groupPartitions(c.mem, c.budget, c.tasks)
		if len(groups) != c.want {
			t.Errorf("%s: %d groups %v, want %d", c.name, len(groups), groups, c.want)
		}
		var seen []int
		for _, g := range groups {
			if len(g) == 0 {
				t.Errorf("%s: an empty group in %v", c.name, groups)
			}
			var load int64
			for _, p := range g {
				load += c.mem[p]
			}
			if c.budget > 0 && len(g) > 1 && load > c.budget {
				t.Errorf("%s: group %v holds %d, over %d", c.name, g, load, c.budget)
			}
			seen = append(seen, g...)
		}
		slices.Sort(seen)
		for p := range c.mem {
			if p >= len(seen) || seen[p] != p {
				t.Errorf("%s: partitions %v, want each of %d once", c.name, seen, len(c.mem))
				break
			}
		}
	}

	// A row's memory: at least 64 bytes, more for wider rows.
	if m := rowMemory(nil); m != 64 {
		t.Errorf("an empty row takes %d", m)
	}
	if narrow, wide := rowMemory([]Column{{Type: "BIGINT"}}), rowMemory([]Column{{Type: "VARCHAR"}, {Type: "VARCHAR[]"}}); narrow >= wide || narrow < 64 {
		t.Errorf("a BIGINT row takes %d, two strings %d", narrow, wide)
	}
}

// TestPartitionsFor checks the partitions exchanges need for a task's rows
// to fit its memory: a billion rows of two BIGINTs, 96 bytes each in hash
// tables, need 90 partitions of 1 GiB, and an exchange fed by it as many.
func TestPartitionsFor(t *testing.T) {
	big := &Table{Name: "events", Rows: 1e9, Columns: []Column{{Name: "a", Type: "BIGINT"}, {Name: "b", Type: "BIGINT"}}}
	x, y := &Exchange{ID: 0}, &Exchange{ID: 1}
	stages := []*Stage{
		{ID: 0, Inputs: []Input{{View: "t", Table: big, Split: true}}, Out: x},
		{ID: 1, Inputs: []Input{{View: "k", Exchange: x, Partitioned: true}}, Out: y},
		{ID: 2, Inputs: []Input{{View: "k", Exchange: y, Partitioned: true}}},
	}
	if n := partitionsFor(stages, 1<<30); n != 90 {
		t.Errorf("%d partitions, want 90", n)
	}
	if n := partitionsFor(stages, 0); n != 0 {
		t.Errorf("%d partitions without a budget", n)
	}
	big.Rows = 1000
	if n := partitionsFor(stages, 1<<30); n != 1 {
		t.Errorf("%d partitions for a thousand rows", n)
	}
}

// TestShuffleRowGroup checks the row groups a shuffle task writes: as many
// rows as keep a group of every partition within a quarter of the task's
// memory, between 2,048 and DuckDB's 122,880.
func TestShuffleRowGroup(t *testing.T) {
	wide := []Column{{Type: "BIGINT"}, {Type: "VARCHAR"}, {Type: "VARCHAR"}, {Type: "VARCHAR"}, {Type: "VARCHAR"}, {Type: "VARCHAR"},
		{Type: "VARCHAR"}, {Type: "VARCHAR"}, {Type: "VARCHAR"}, {Type: "INTEGER"}}
	for _, c := range []struct {
		memory      int64
		slots, part int
		cols        []Column
		want        int64
	}{
		{24 << 30, 3, 128, wide, 62_601},
		{24 << 30, 3, 8, wide, 122_880},
		{500 << 20, 1, 256, wide, 2_048},
		{0, 1, 128, wide, 122_880},
		{3 << 30, 1, 0, wide, 11_737},
	} {
		if got := shuffleRowGroup(c.memory, c.slots, c.part, c.cols); got != c.want {
			t.Errorf("%d bytes over %d slots, %d partitions: %d rows a group, want %d", c.memory, c.slots, c.part, got, c.want)
		}
	}
}

// TestWriterPartitions checks the cap on partitions that exchanges' writers
// set: none to speak of for a few narrow columns, as a report's exchanges
// of IDs and flags, on workers of 24.5 GiB; 28 for rows of 400 strings on
// a worker of 3 GB, which wrote them in 76 s within 1.3 GB, where 32 and 64
// partitions took 123 and 194 s at its limit and 128 ran out of memory.
// Twice the partitions, as running again after running out of memory
// gives, halve the row groups, down to the fewest rows.
func TestWriterPartitions(t *testing.T) {
	stagesOf := func(cols []Column) []*Stage {
		tbl := &Table{Name: "t", Rows: 4e9, Columns: cols}
		return []*Stage{{ID: 0, Inputs: []Input{{View: "t", Table: tbl, Split: true}}, Out: &Exchange{ID: 0}}}
	}
	narrow := []Column{{Name: "id", Type: "VARCHAR"}, {Name: "style", Type: "VARCHAR"}, {Name: "channel", Type: "VARCHAR"},
		{Name: "a", Type: "BOOLEAN"}, {Name: "b", Type: "BOOLEAN"}}
	var wide []Column
	for i := range 400 {
		wide = append(wide, Column{Name: "c" + strconv.Itoa(i), Type: "VARCHAR"})
	}
	workers := func(n int, memory int64, slots int) []*worker {
		var ws []*worker
		for range n {
			ws = append(ws, &worker{status: workerStatus{MemoryBytes: memory, Slots: slots, Cores: 16}})
		}
		return ws
	}
	if most := writerPartitions(stagesOf(narrow), workers(16, 24_500<<20, 3)); most < maxPartitions {
		t.Errorf("narrow rows on 24.5 GiB workers: at most %d partitions", most)
	}
	if most := writerPartitions(stagesOf(wide), workers(1, 3e9, 1)); most != 28 {
		t.Errorf("400 strings on a 3 GB worker: at most %d partitions, want 28", most)
	}
	if n := exchangePartitions(stagesOf(wide), workers(1, 3e9, 1), 1); n != 28 {
		t.Errorf("400 strings: %d partitions", n)
	}
	if n := exchangePartitions(stagesOf(wide), workers(16, 3e9, 1), 16); n < 16 {
		t.Errorf("%d partitions for 16 workers", n)
	}
	cols := []Column{{Type: "BIGINT"}, {Type: "VARCHAR"}}
	one, two := shuffleRowGroup(1<<30, 1, 64, cols), shuffleRowGroup(1<<30, 1, 128, cols)
	if one >= 122_880 || two < one/2-1 || two > one/2+1 || shuffleRowGroup(1<<30, 1, 1<<20, cols) != minShuffleRowGroup {
		t.Errorf("row groups of %d rows over 64 partitions, %d over 128", one, two)
	}
	if f := shuffleFlush(3e9, 1, 16, wide); f != 16_384 {
		t.Errorf("400 strings on 16 threads flush every %d rows", f)
	}
	// Narrow rows on 64 threads flush at two thirds of DuckDB's 524,288,
	// which held 3.3 GB of a task's 8.2 GB.
	if f := shuffleFlush(24_500<<20, 3, 64, narrow); f != 341_333 {
		t.Errorf("narrow rows flush every %d rows", f)
	}
}

// stageTasks records how many tasks each stage ran, from the coordinator's
// log.
type stageTasks struct {
	mu    sync.Mutex
	tasks map[string]int64
}

func (s *stageTasks) Enabled(context.Context, slog.Level) bool { return true }
func (s *stageTasks) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *stageTasks) WithGroup(string) slog.Handler            { return s }
func (s *stageTasks) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "running a stage" {
		return nil
	}
	var stage string
	var tasks int64
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "stage":
			stage = a.Value.String()
		case "tasks":
			tasks = a.Value.Int64()
		}
		return true
	})
	s.mu.Lock()
	s.tasks[stage] = tasks
	s.mu.Unlock()
	return nil
}

// TestCoalescedPartitions checks a report's shape whose stage reads a
// small relation by partition and a table in full: an audience, estimated
// at the size of the table it filters, joined to a table every task reads.
// The stage runs a task a worker, each over several partitions, rather than
// a task a partition reading the table again, and returns what one machine
// does.
func TestCoalescedPartitions(t *testing.T) {
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
	// The audience is a relation keyed by order, as the table it filters is
	// too large for every worker; orders is read in full.
	co.maxBroadcast = h.tables["orders"].Bytes
	q := `WITH aud AS (SELECT DISTINCT l_orderkey AS k FROM lineitem WHERE l_quantity = 1)
		SELECT o.o_orderpriority, count(*), sum(o.o_totalprice) FROM orders AS o JOIN aud ON aud.k = o.o_orderkey GROUP BY 1 ORDER BY 1`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	p := h.planOf(q)
	found := false
	for _, s := range p.Stages {
		if !s.partitioned() || !slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Table != nil && !in.Split }) {
			continue
		}
		found = true
		rec.mu.Lock()
		tasks := rec.tasks[s.String()]
		rec.mu.Unlock()
		if tasks != 3 {
			t.Errorf("%s ran %d tasks, want one a worker:\n%s", s, tasks, p.describe())
		}
	}
	if !found {
		t.Errorf("no stage reads partitions and a table in full:\n%s", p.describe())
	}
	if !strings.Contains(p.describe(), "partitions of exchange") {
		t.Errorf("nothing read by partition:\n%s", p.describe())
	}
}
