package distributed

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
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
