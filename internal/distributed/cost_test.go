package distributed

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestChoose checks where auto mode runs a statement, from its estimates
// and what it took before.
func TestChoose(t *testing.T) {
	now := time.Now()
	slow := estimate{oneMachine: 600, onWorkers: 120, workers: 4}  // workers estimated 5 times faster
	short := estimate{oneMachine: 20, onWorkers: 5, workers: 4}    // under the threshold
	close := estimate{oneMachine: 600, onWorkers: 500, workers: 4} // not 1.5 times faster
	memory := estimate{oneMachine: 30, onWorkers: 10, workers: 4, memory: true}
	none := estimate{oneMachine: 600, onWorkers: 120, workers: 0}
	for _, c := range []struct {
		name    string
		e       estimate
		r       runs
		found   bool
		workers bool
		explore bool
	}{
		{"estimated faster on workers", slow, runs{}, false, true, false},
		{"under the threshold", short, runs{}, false, false, false},
		{"not faster enough", close, runs{}, false, false, false},
		{"short of memory", memory, runs{}, false, true, false},
		{"no workers to run on", none, runs{}, false, false, false},
		{"faster on workers before", close, runs{local: 300, dist: 100}, true, true, false},
		{"faster on one machine before", slow, runs{local: 100, dist: 300}, true, false, false},
		{"ran on one machine within the threshold", slow, runs{local: 30}, true, false, false},
		{"ran on one machine well within its estimate", slow, runs{local: 250}, true, false, false},
		{"ran long on one machine, workers estimated faster", estimate{oneMachine: 100, onWorkers: 60, workers: 4}, runs{local: 120}, true, true, false},
		{"ran long on one machine: try workers", close, runs{local: 400}, true, true, true},
		{"ran long on one machine, tried workers today", close, runs{local: 400, explored: now.Add(-time.Hour)}, true, false, false},
		{"ran on workers within their estimate", close, runs{dist: 200}, true, true, false},
		{"ran on workers slower than one machine's estimate", slow, runs{dist: 500}, true, false, false},
	} {
		workers, explore, reason := choose(c.e, c.r, c.found, DefaultThreshold, now)
		if workers != c.workers || explore != c.explore || reason == "" {
			t.Errorf("%s: workers %v, explore %v (%s); want %v, %v", c.name, workers, explore, reason, c.workers, c.explore)
		}
	}
}

// TestEstimateTimes checks the estimates of a plan: one machine's from
// the coordinator's threads, the workers' from theirs, the rounds of tasks
// and starting workers, and the fewest workers within a fifth of the
// fastest.
func TestEstimateTimes(t *testing.T) {
	big := &Table{Schema: "main", Name: "big", Bytes: 300e9}
	small := &Table{Schema: "main", Name: "small", Bytes: 1e6}
	x, y := &Exchange{ID: 0}, &Exchange{ID: 1}
	stages := []*Stage{
		{Inputs: []Input{{Table: big, Split: true}}, Out: x},
		{Inputs: []Input{{Exchange: x, Partitioned: true}, {Table: small}}, Out: y},
		{Inputs: []Input{{Exchange: y, Partitioned: true}}},
	}
	e := estimateTimes(stages, 16, 50e9, fleet{threads: 8, running: 2, most: 16, start: 30 * time.Second})
	if e.levels != 3 || e.bytes != 300e9+1e6 || e.exchanged != 300e9+1e6 || !e.memory {
		t.Errorf("levels %d, bytes %d, exchanged %d, memory %v", e.levels, e.bytes, e.exchanged, e.memory)
	}
	if want := (300e9 + 1e6) / (16 * localRate); e.oneMachine != want {
		t.Errorf("one machine: %.1f s, want %.1f", e.oneMachine, want)
	}
	// 16 workers: 12 + 300e9/(16*8*50e6) + 30 = 89 s, starting more than
	// the 2 running; the fewest within a fifth of that: 12, at 104.5 s.
	if e.workers != 12 || e.onWorkers > 89*1.2 {
		t.Errorf("%d workers, %.1f s", e.workers, e.onWorkers)
	}
	if e := estimateTimes(stages, 16, 0, fleet{}); e.workers != 0 || e.memory {
		t.Errorf("no workers: %d workers, memory %v", e.workers, e.memory)
	}
}

// TestPlanKey checks that a statement's plan key leaves out its literals
// other than integers, and tells tables' sizes apart by factors of two.
func TestPlanKey(t *testing.T) {
	h := newHarness(t, 0.01, 2, 2)
	q := func(day string, limit int) string {
		return fmt.Sprintf(`SELECT l_returnflag, count(*) FROM lineitem WHERE l_shipdate >= DATE '%s' GROUP BY 1 ORDER BY 1 LIMIT %d`, day, limit)
	}
	key := func(query string) string {
		stmt, err := h.co.parser.parse(query)
		if err != nil {
			t.Fatal(err)
		}
		return planKey(stmt, []*Stage{{Inputs: []Input{{Table: h.tables["lineitem"], Split: true}}}})
	}
	a := key(q("1995-01-01", 10))
	if b := key(q("1996-06-30", 10)); a != b {
		t.Error("the key depends on a date literal")
	}
	if b := key(q("1995-01-01", 20)); a == b {
		t.Error("the key leaves out an integer")
	}
	li := h.tables["lineitem"]
	defer func(b int64) { li.Bytes = b }(li.Bytes)
	li.Bytes = li.Bytes*2 + 1
	if b := key(q("1995-01-01", 10)); a == b {
		t.Error("the key is the same for a table twice the size")
	}
}

// TestHistory checks the runs a history keeps: the last run weighs as much
// as the earlier ones, and the keys used longest ago go first.
func TestHistory(t *testing.T) {
	h := newHistory()
	h.observe("a", false, 100)
	h.observe("a", false, 50)
	h.observe("a", true, 30)
	if r, ok := h.get("a"); !ok || r.local != 75 || r.dist != 30 {
		t.Errorf("runs of a: %+v", r)
	}
	for i := range maxHistory {
		h.observe(fmt.Sprint("k", i), false, 1)
	}
	if _, ok := h.get("a"); ok {
		t.Error("kept the key used longest ago")
	}
	if _, ok := h.get("k1"); !ok {
		t.Error("lost a key used since")
	}
}

// TestAutoMode checks auto mode's choice in the harness: a statement as
// large as the 1x reports runs on one machine; one that ran faster on
// workers before runs there, and one that ran faster on one machine stays
// there though workers are estimated faster.
func TestAutoMode(t *testing.T) {
	h := newHarness(t, 0.01, 3, 3)
	li := h.tables["lineitem"]
	li.Bytes = 8 << 30
	q := `SELECT l_returnflag, count(*), sum(l_quantity) FROM lineitem GROUP BY 1`
	join := `SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`
	h.co.maxBroadcast = h.tables["customer"].Bytes
	for _, query := range []string{q, join} {
		_, err := h.co.Plan(t.Context(), query, harnessEnv{h}, ModeAuto)
		if !IsUnsupported(err) || PlanKey(err) == "" || !strings.Contains(err.Error(), "one machine is faster") {
			t.Fatalf("%s: want one machine, got %v", short(query), err)
		}
	}
	_, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeAuto)
	key := PlanKey(err)
	h.co.Observe(key, 0, 2*time.Minute)
	h.co.Observe(key, 3, 30*time.Second)
	plan, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeAuto)
	if err != nil || plan.Key != key {
		t.Fatalf("faster on workers before: %v", err)
	}
	ex, err := h.co.Execute(t.Context(), plan, h.settings(t.Context()))
	if err != nil {
		t.Fatal(err)
	}
	ex.Close()

	// Workers estimated faster: a table of a terabyte, a coordinator of
	// one thread.
	h.co.opts.Threads = 1
	li.Bytes = 1 << 40
	if _, err := h.co.Plan(t.Context(), join, harnessEnv{h}, ModeAuto); err != nil {
		t.Fatalf("estimated faster on workers: %v", err)
	}
	plan, err = h.co.Plan(t.Context(), join, harnessEnv{h}, ModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	h.co.Observe(plan.Key, 0, 20*time.Second)
	h.co.Observe(plan.Key, 3, 50*time.Second)
	if _, err := h.co.Plan(t.Context(), join, harnessEnv{h}, ModeAuto); !IsUnsupported(err) {
		t.Fatalf("faster on one machine before: %v", err)
	}
	// Forced, it runs on workers whatever the history.
	if _, err := h.co.Plan(t.Context(), join, harnessEnv{h}, ModeOn); err != nil {
		t.Fatalf("forced: %v", err)
	}
}
