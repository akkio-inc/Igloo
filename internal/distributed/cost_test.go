package distributed

import (
	"fmt"
	"math"
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
	memory := estimate{oneMachine: 30, onWorkers: 10, workers: 4, spills: true}
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
		{"faster on one machine than on a quarter of the workers: try them all once", estimate{oneMachine: 100, onWorkers: 60, workers: 13},
			runs{local: 178, dist: 230, distWorkers: 4}, true, true, true},
		{"faster on one machine than on a quarter of the workers, tried today", estimate{oneMachine: 100, onWorkers: 60, workers: 13},
			runs{local: 178, dist: 230, distWorkers: 4, explored: now.Add(-time.Hour)}, true, false, false},
		{"faster on one machine than on as many workers", estimate{oneMachine: 100, onWorkers: 60, workers: 13},
			runs{local: 178, dist: 230, distWorkers: 12}, true, false, false},
		{"ran on one machine within the threshold", slow, runs{local: 30}, true, false, false},
		{"ran on one machine for minutes: try workers once", slow, runs{local: 250}, true, true, true},
		{"ran on one machine, tried workers today, well within its estimate", slow, runs{local: 250, explored: now.Add(-time.Hour)}, true, false, false},
		{"ran long on one machine, tried workers today, workers estimated faster", estimate{oneMachine: 100, onWorkers: 60, workers: 4},
			runs{local: 120, explored: now.Add(-time.Hour)}, true, true, false},
		{"ran long on one machine: try workers", close, runs{local: 400}, true, true, true},
		{"ran long on one machine, tried workers today", close, runs{local: 400, explored: now.Add(-time.Hour)}, true, false, false},
		{"ran for an hour on one machine, workers estimated faster", close, runs{local: 3600, explored: now.Add(-time.Hour)}, true, true, false},
		{"ran on workers for minutes, one machine estimated close: try it once", estimate{oneMachine: 300, onWorkers: 250, workers: 4},
			runs{dist: 200}, true, false, true},
		{"ran on workers for minutes, one machine estimated three times slower", close, runs{dist: 200}, true, true, false},
		// At ten times the data, two reports that took 57 s and 25 s on
		// workers were tried on one machine, estimated at 176 s and 180 s
		// and short of memory: one took 251 s and spilled 8.4 GB.
		{"large-table scan at 10x, after 57 s on workers", estimate{oneMachine: 176, onWorkers: 42, workers: 16, memory: true, spills: true},
			runs{dist: 57.2}, true, true, false},
		{"large join at 10x, after 25 s on workers", estimate{oneMachine: 180, onWorkers: 40, workers: 16, memory: true, spills: true},
			runs{dist: 25.4}, true, true, false},
		{"close, but one machine should spill", estimate{oneMachine: 70, onWorkers: 50, workers: 4, spills: true},
			runs{dist: 40}, true, true, false},
		// The auto pass at ten times the data: after a run on workers, two
		// reports went to one machine, estimated at 157 s and short of
		// memory, and took 809 s and 610 s; another took 177.5 s against
		// 103 s. Two that ran long on one machine went to workers and took
		// a fraction.
		{"F1 at 10x, after 146 s on workers", estimate{oneMachine: 157, onWorkers: 87, workers: 16, memory: true, spills: true},
			runs{dist: 145.6}, true, true, false},
		{"F2 at 10x, after 141 s on workers", estimate{oneMachine: 157, onWorkers: 87, workers: 16, memory: true, spills: true},
			runs{dist: 140.6}, true, true, false},
		{"E2 at 10x, after 103 s on workers", estimate{oneMachine: 193, onWorkers: 83, workers: 16, memory: true, spills: true},
			runs{dist: 103.3}, true, true, false},
		{"E1 at 10x, after 67 s on one machine, estimated at 13 s", estimate{oneMachine: 13, onWorkers: 30, workers: 16},
			runs{local: 67.4}, true, true, true},
		{"E3 at 10x, after 220 s on one machine, estimated at 56 s", estimate{oneMachine: 56, onWorkers: 58, workers: 16, memory: true, spills: true},
			runs{local: 219.5}, true, true, true},
		// The auto pass at a hundred times the data: after runs on workers
		// of 277 s, 499 s and 248 s, three reports were tried on one
		// machine, estimated at 1290 s, 991 s and 1324 s and short of
		// memory: 1154 s, out of memory after about 840 s, and 282 s.
		{"E1 at 100x, after 277 s on workers", estimate{oneMachine: 1290, onWorkers: 125, workers: 16, memory: true, spills: true},
			runs{dist: 277}, true, true, false},
		{"E3 at 100x, after 499 s on workers", estimate{oneMachine: 991, onWorkers: 121, workers: 16, memory: true, spills: true},
			runs{dist: 499}, true, true, false},
		{"F5 at 100x, after 248 s on workers", estimate{oneMachine: 1324, onWorkers: 307, workers: 16, memory: true, spills: true},
			runs{dist: 248}, true, true, false},
		// First runs at ten times the data of reports one machine should
		// spill: workers within half again one machine's estimate run them
		// (E3 took 220 s on one machine and 66 s on workers); where workers
		// are estimated at 1.7 or 2 times (F5, F4) one machine runs them,
		// and was five times as fast. F3's 3.3 times stays a miss.
		{"E3 at 10x, first", estimate{oneMachine: 56, onWorkers: 58, workers: 16, memory: true, spills: true}, runs{}, false, true, false},
		{"F4 at 10x, first", estimate{oneMachine: 83, onWorkers: 169, workers: 16, memory: true, spills: true}, runs{}, false, false, false},
		{"F5 at 10x, first", estimate{oneMachine: 96, onWorkers: 164, workers: 16, memory: true, spills: true}, runs{}, false, false, false},
		{"F3 at 10x, first", estimate{oneMachine: 51, onWorkers: 168, workers: 16, spills: true}, runs{}, false, false, false},
		{"ran on one machine for minutes, workers estimated far slower", estimate{oneMachine: 100, onWorkers: 600, workers: 4},
			runs{local: 250}, true, false, false},
		{"ran on workers within their estimate, tried one machine today", close, runs{dist: 200, explored: now.Add(-time.Hour)}, true, true, false},
		{"ran on workers slower than one machine's estimate, tried today", slow, runs{dist: 500, explored: now.Add(-time.Hour)}, true, false, false},
		{"ran on workers for an hour, not estimated faster there", close, runs{dist: 3600}, true, false, false},
		{"ran out of memory on one machine", short, runs{local: 10, outOfMemory: true}, true, true, false},
		{"stopped by the guard on one machine, at 153 s", estimate{oneMachine: 51, onWorkers: 168, workers: 16, spills: true},
			runs{local: 153, dist: 362, guarded: true}, true, true, false},
		// Measured on workers to exchange more than one machine's memory
		// holds, though one machine should not spill: kept on workers, not
		// tried on one machine.
		{"ran on workers, exchanging more than one machine holds", estimate{oneMachine: 120, onWorkers: 100, workers: 4},
			runs{dist: 90, heavy: true}, true, true, false},
		{"ran both ways, faster on one machine, exchanging much on workers", estimate{oneMachine: 120, onWorkers: 100, workers: 4},
			runs{local: 38, dist: 228, heavy: true}, true, false, false},
		{"ran out of memory, no workers", none, runs{outOfMemory: true}, true, false, false},
		// A dashboard's statement at ten times the data, estimated at 14 s
		// on one machine and short of memory by what it exchanged, after
		// 1,312 s split over two workers: tried on one machine once, not kept
		// on workers for good.
		{"ran on workers 90 times one machine's estimate, short of memory", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true},
			runs{dist: 1312}, true, false, true},
		{"ran on workers 90 times one machine's estimate, tried today", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true},
			runs{dist: 1312, explored: now.Add(-time.Hour)}, true, true, false},
		{"ran on workers 9 times one machine's estimate, short of memory", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true},
			runs{dist: 126}, true, true, false},
		{"ran on workers 90 times one machine's estimate, exchanging more than it holds", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true},
			runs{dist: 1312, heavy: true}, true, true, false},
		// Short of memory by what it exchanges, and workers no slower: a report
		// estimated at 52 s took 174 s by way of the guard, and 68 to 87 s on
		// workers. Not where workers are slower, as a report's that took 10 s
		// here and 66 s there.
		{"short of memory, workers no slower", estimate{oneMachine: 52, onWorkers: 45, workers: 16, memory: true}, runs{}, false, true, false},
		{"short of memory, workers slower", estimate{oneMachine: 30, onWorkers: 79, workers: 8, memory: true}, runs{}, false, false, false},
		{"fits, workers no slower", estimate{oneMachine: 52, onWorkers: 45, workers: 16}, runs{}, false, false, false},
		// Far beyond its memory, whatever workers should take: not just past
		// its room, where their time decides.
		{"far beyond its memory, workers slower", estimate{oneMachine: 100, onWorkers: 400, workers: 16, memory: true, spills: true, farBeyond: true}, runs{}, false, true, false},
		{"past its room, workers slower", estimate{oneMachine: 100, onWorkers: 400, workers: 16, memory: true, spills: true}, runs{}, false, false, false},
		{"far beyond its memory, no workers", estimate{oneMachine: 100, onWorkers: math.Inf(1), memory: true, spills: true, farBeyond: true}, runs{}, false, false, false},
		// Far over an estimate that says it spills: the estimate is wrong, as
		// a report's of 15 s, after 200 s on workers, that took 714 s there.
		{"ran on workers 90 times one machine's estimate, which says it spills", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true, spills: true},
			runs{dist: 1312}, true, true, false},
		{"ran on workers 90 times one machine's estimate, its working set far beyond it", estimate{oneMachine: 14, onWorkers: 30, workers: 2, memory: true,
			spills: true, farBeyond: true}, runs{dist: 1312}, true, true, false},
	} {
		workers, explore, reason := choose(c.e, c.r, c.found, DefaultThreshold, now)
		if workers != c.workers || explore != c.explore || reason == "" {
			t.Errorf("%s: workers %v, explore %v (%s); want %v, %v", c.name, workers, explore, reason, c.workers, c.explore)
		}
	}
}

// TestEstimateTimes checks the estimates of a plan: one machine's from
// the coordinator's cores and threads, reading cold, longer short of
// memory; the workers' from their start, their stages and their cores and
// threads, starting more; and the cheapest within a fifth of the fastest,
// as the first estimates counted them.
func TestEstimateTimes(t *testing.T) {
	coordinator := machine{cores: 16, threads: 32, memory: 50e9}
	big := &Table{Schema: "main", Name: "big", Bytes: 300e9, Columns: []Column{{Name: "k", Type: "VARCHAR"}}}
	small := &Table{Schema: "main", Name: "small", Bytes: 1e6}
	x, y := &Exchange{ID: 0}, &Exchange{ID: 1}
	stages := []*Stage{
		{Inputs: []Input{{Table: big, Split: true}}, Out: x},
		{Inputs: []Input{{Exchange: x, Partitioned: true}, {Table: small}}, Out: y},
		{Inputs: []Input{{Exchange: y, Partitioned: true}}},
	}
	f := fleet{cores: 8, running: 2, most: 16, start: 30 * time.Second}
	e := estimateTimes(stages, coordinator, f)
	if e.levels != 3 || e.stages != 3 || e.bytes != 300e9+1e6 || e.exchanged != 300e9+1e6 || !e.memory || !e.spills {
		t.Errorf("levels %d, stages %d, bytes %d, exchanged %d, memory %v, spills %v", e.levels, e.stages, e.bytes, e.exchanged, e.memory, e.spills)
	}
	// Short of memory: spilling, as its working set, six times what its
	// exchanges' stages read, is over its share of memory, to the power of
	// spillExponent; and over ten times, out of memory.
	ratio := 6 * (300e9 + 1e6) / (memoryShare * 50e9)
	if want := (300e9 + 1e6) / (16 * coldRateAt(2)) * math.Pow(ratio, spillExponent); math.Abs(e.oneMachine-want) > 1 || !e.local.oom {
		t.Errorf("one machine: %.1f s, out of memory %v; want %.1f s and out of memory", e.oneMachine, e.local.oom, want)
	}
	// All 16, starting 14 more than the 2 running: 8 + 3*5 +
	// 300e9/(16*8*16e6*0.4) + 30 = 419.2 s; 8 would take 785 s.
	if e.workers != 16 || e.large || math.Abs(e.onWorkers-419.2) > 0.5 {
		t.Errorf("%d workers, %.1f s", e.workers, e.onWorkers)
	}
	// Workers of fewer threads a core read slower.
	f.threads = 12
	if slower := estimateTimes(stages, coordinator, f); slower.workers != 16 || slower.onWorkers <= e.onWorkers {
		t.Errorf("at 1.5 threads a core: %d workers, %.1f s", slower.workers, slower.onWorkers)
	}
	if e := estimateTimes(stages, machine{cores: 16}, fleet{}); e.workers != 0 || e.memory || !math.IsInf(e.onWorkers, 1) {
		t.Errorf("no workers: %d workers, memory %v, %.0f s", e.workers, e.memory, e.onWorkers)
	}
	// A table its stage filters selectively, by a constant or a subquery's
	// value, counts a hundredth of its bytes, and is short of memory only
	// as it falls back.
	for _, value := range []node{{"class": "CONSTANT"}, {"class": "SUBQUERY"}} {
		stages[0].fragment = node{"where_clause": node{"type": "COMPARE_EQUAL",
			"left": node{"class": "COLUMN_REF", "column_names": []any{"k"}}, "right": value}}
		e := estimateTimes(stages, coordinator, f)
		if e.bytes != 3e9+1e6 || e.exchanged != 300e9+1e6 || !e.memory || e.spills || e.workers == 0 {
			t.Errorf("filtered by %s: bytes %d, exchanged %d, memory %v, spills %v, %d workers", value["class"], e.bytes, e.exchanged, e.memory, e.spills, e.workers)
		}
	}
	if r := coldRateAt(4); r != coldRate {
		t.Errorf("cold rate at 4 threads a core: %.0f", r)
	}
	if r := coldRateAt(2) / 1e6; r < 10 || r > 12 {
		t.Errorf("cold rate at 2 threads a core: %.1f MB/s", r)
	}
}

// TestScaleReports checks auto mode's first choice for the scale
// environment's reports at 1x, 10x and 100x, as the planner estimates
// them (bytes as selective filters leave them, and in full; the rows
// their stages go through, and as the first estimates counted them; their
// levels and stages), against how long they took: on a coordinator of 16
// cores, 24 threads and a 49.3 GiB limit (one), and on 16 workers of 8
// cores and 12 threads (workers), zero where not measured. The side that
// was faster by half again or more is chosen, but for those listed in
// misses, whose first run, on the side the estimates pick, sends the next
// to the faster side, and a small-group scan at 10x, 24 s on one machine and 12 on
// workers, which stays under the threshold.
func TestScaleReports(t *testing.T) {
	coordinator := machine{cores: 16, threads: 24, memory: 52_935_471_923}
	workers := fleet{cores: 8, threads: 12, running: 16, most: 16}
	misses := map[string]string{
		// FLATTEN's arrays and maps cost far more a row than the rows say.
		"E3 report-b at 10x":  "one",
		"map flatten at 100x": "one",
		// Under DefaultThreshold on one machine.
		"small-group scan at 10x": "one",
	}
	now := time.Now()
	for _, c := range []struct {
		name         string
		x            features
		one, workers float64
		faster       string
	}{
		{"E2 report-a at 1x", features{bytes: 1.349e+09, exchanged: 8.999e+09, held: 1.349e+09, rows: 1.2e+10, levels: 2, stages: 10}, 9, 0, "one"},
		{"E3 report-b at 1x", features{bytes: 1.044e+08, exchanged: 7.755e+09, held: 1.044e+08, rows: 4.82e+08, levels: 5, stages: 7}, 11, 0, "one"},
		{"F1 report-c at 1x", features{bytes: 1.186e+09, exchanged: 5.402e+09, held: 9.051e+08, rows: 5.68e+08, levels: 4, stages: 20}, 14, 0, "one"},
		{"F2 report-d at 1x", features{bytes: 1.186e+09, exchanged: 5.402e+09, held: 9.051e+08, rows: 2.84e+08, levels: 4, stages: 10}, 13, 0, "one"},
		{"F3 report-e at 1x", features{bytes: 4.669e+08, exchanged: 4.669e+08, held: 4.669e+08, rows: 7.23e+08, levels: 7, stages: 24}, 13, 0, "one"},
		{"F4 report-f at 1x", features{bytes: 4.934e+08, exchanged: 3.491e+09, held: 4.934e+08, rows: 7.49e+08, levels: 7, stages: 22}, 4, 0, "one"},
		{"F5 report-g at 1x", features{bytes: 8.144e+08, exchanged: 3.812e+09, held: 8.144e+08, rows: 9.34e+08, levels: 7, stages: 23}, 2, 0, "one"},
		{"small-group scan at 1x", features{bytes: 3.636e+08, exchanged: 3.636e+08, held: 3.636e+08, rows: 1.45e+08, levels: 2, stages: 2}, 3, 17, "one"},
		{"cohort join at 1x", features{bytes: 2.726e+08, exchanged: 3.27e+09, held: 2.726e+08, rows: 1.5e+08, levels: 2, stages: 3}, 1, 10, "one"},
		{"large-table scan at 1x", features{bytes: 1.248e+09, exchanged: 1.248e+09, held: 1.248e+09, rows: 3.49e+09, levels: 2, stages: 3}, 22, 87, "one"},
		{"large join at 1x", features{bytes: 1.277e+09, exchanged: 6.377e+09, held: 1.277e+09, rows: 3.36e+09, levels: 2, stages: 3}, 2, 49, "one"},
		{"list flatten at 1x", features{bytes: 1.111e+08, exchanged: 1.111e+08, held: 1.111e+08, rows: 6.98e+07, levels: 2, stages: 2}, 2, 27, "one"},
		{"wide list flatten at 1x", features{bytes: 6.059e+08, exchanged: 6.059e+08, held: 6.059e+08, rows: 1.81e+08, levels: 2, stages: 2}, 2, 26, "one"},
		{"list flatten joined at 1x", features{bytes: 1.565e+08, exchanged: 4.653e+09, held: 1.565e+08, rows: 9.23e+07, levels: 2, stages: 2}, 1, 8, "one"},
		{"map flatten at 1x", features{bytes: 1.808e+07, exchanged: 0, held: 0, rows: 1.2e+08, levels: 1, stages: 1}, 7, 9, "tie"},
		{"E2 report-a at 10x", features{bytes: 1.349e+10, exchanged: 8.999e+10, held: 1.349e+10, rows: 1.2e+11, levels: 2, stages: 10}, 172, 84, "workers"},
		{"E3 report-b at 10x", features{bytes: 1.044e+09, exchanged: 7.755e+10, held: 1.044e+09, rows: 4.82e+09, levels: 5, stages: 7}, 251, 113, "workers"},
		{"F1 report-c at 10x", features{bytes: 1.186e+10, exchanged: 5.683e+10, held: 1.186e+10, rows: 5.83e+09, levels: 3, stages: 22}, 691, 170, "workers"},
		{"F2 report-d at 10x", features{bytes: 1.186e+10, exchanged: 5.683e+10, held: 1.186e+10, rows: 2.92e+09, levels: 3, stages: 11}, 621, 188, "workers"},
		{"F4 report-f at 10x", features{bytes: 4.934e+09, exchanged: 3.491e+10, held: 4.934e+09, rows: 7.87e+09, levels: 7, stages: 26}, 40, 183, "one"},
		// By DuckDB's plan, its working set of 31 GB fits; by six times what
		// its stages read, it would spill.
		{"F5 report-g at 10x", features{bytes: 8.144e+09, exchanged: 3.812e+10, held: 8.144e+09, rows: 1.01e+10, levels: 7, stages: 33,
			planned: true, planSet: 3.1e10}, 25, 161, "one"},
		{"small-group scan at 10x", features{bytes: 3.636e+09, exchanged: 3.636e+09, held: 3.636e+09, rows: 1.45e+09, levels: 2, stages: 2}, 24, 12, "workers"},
		{"cohort join at 10x", features{bytes: 2.726e+09, exchanged: 3.27e+10, held: 2.726e+09, rows: 1.5e+09, levels: 2, stages: 3}, 9, 22, "one"},
		{"large-table scan at 10x", features{bytes: 1.248e+10, exchanged: 1.248e+10, held: 1.248e+10, rows: 3.49e+10, levels: 2, stages: 3}, 259, 40, "workers"},
		{"large join at 10x", features{bytes: 1.277e+10, exchanged: 6.377e+10, held: 1.277e+10, rows: 3.36e+10, levels: 2, stages: 3}, 30, 22, "tie"},
		{"list flatten at 10x", features{bytes: 1.111e+09, exchanged: 1.111e+09, held: 1.111e+09, rows: 6.98e+08, levels: 2, stages: 2}, 11, 35, "one"},
		{"wide list flatten at 10x", features{bytes: 6.059e+09, exchanged: 6.059e+09, held: 6.059e+09, rows: 1.81e+09, levels: 2, stages: 2}, 34, 36, "tie"},
		{"list flatten joined at 10x", features{bytes: 1.565e+09, exchanged: 4.653e+10, held: 1.565e+09, rows: 7.69e+08, levels: 2, stages: 3}, 9, 39, "one"},
		{"map flatten at 10x", features{bytes: 1.808e+08, exchanged: 0, held: 0, rows: 1.2e+09, levels: 1, stages: 1}, 22, 31, "tie"},
		{"F1 report-c at 100x", features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 5.83e+10, levels: 3, stages: 22}, 0, 722, "workers"},
		{"F2 report-d at 100x", features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 2.92e+10, levels: 3, stages: 11}, 0, 685, "workers"},
		{"F4 report-f at 100x", features{bytes: 4.934e+10, exchanged: 3.491e+11, held: 4.934e+10, rows: 7.87e+10, levels: 7, stages: 26}, 2223, 1263, "workers"},
		{"F5 report-g at 100x", features{bytes: 8.144e+10, exchanged: 3.812e+11, held: 8.144e+10, rows: 1.01e+11, levels: 7, stages: 34}, 311, 257, "tie"},
		{"small-group scan at 100x", features{bytes: 3.636e+10, exchanged: 3.636e+10, held: 3.636e+10, rows: 1.45e+10, levels: 2, stages: 2}, 501, 72, "workers"},
		{"cohort join at 100x", features{bytes: 2.726e+10, exchanged: 3.27e+11, held: 2.726e+10, rows: 1.5e+10, levels: 2, stages: 3}, 167, 41, "workers"},
		{"large-table scan at 100x", features{bytes: 1.248e+11, exchanged: 1.248e+11, held: 1.248e+11, rows: 3.49e+11, levels: 2, stages: 3}, 0, 262, "workers"},
		{"large join at 100x", features{bytes: 1.277e+11, exchanged: 6.377e+11, held: 1.277e+11, rows: 3.36e+11, levels: 2, stages: 3}, 0, 119, "workers"},
		{"list flatten at 100x", features{bytes: 1.111e+10, exchanged: 1.111e+10, held: 1.111e+10, rows: 6.98e+09, levels: 2, stages: 2}, 363, 146, "workers"},
		{"wide list flatten at 100x", features{bytes: 6.059e+10, exchanged: 6.059e+10, held: 6.059e+10, rows: 1.81e+10, levels: 2, stages: 2}, 0, 376, "workers"},
		{"list flatten joined at 100x", features{bytes: 1.565e+10, exchanged: 4.653e+11, held: 1.565e+10, rows: 7.69e+09, levels: 2, stages: 3}, 281, 43, "workers"},
		{"map flatten at 100x", features{bytes: 1.808e+09, exchanged: 0, held: 0, rows: 1.2e+10, levels: 1, stages: 1}, 379, 30, "workers"},
	} {
		e := c.x.estimate(coordinator, workers)
		got, _, reason := choose(e, runs{}, false, DefaultThreshold, now)
		side := map[bool]string{true: "workers", false: "one"}[got]
		t.Logf("%s: %s (%s)", c.name, side, reason)
		if miss, ok := misses[c.name]; ok {
			if side != miss {
				t.Errorf("%s: %s first, the miss listed is %s (%s)", c.name, side, miss, reason)
			}
			if c.name == "small-group scan at 10x" {
				continue
			}
			// One run there, and the next goes to the faster side.
			r := runs{local: c.one}
			if got {
				r = runs{dist: c.workers}
			}
			if again, _, reason := choose(e, r, true, DefaultThreshold, now); again == got {
				t.Errorf("%s: %s again after a run there (%s)", c.name, side, reason)
			}
			continue
		}
		if c.faster != "tie" && side != c.faster {
			t.Errorf("%s: %s, but %s was faster: %.0f s on one machine, %.0f s on workers (%s)", c.name, side, c.faster, c.one, c.workers, reason)
		}
	}
}

// TestColumnsRead checks what a stage reads of a table: the columns its
// fragment names, under any qualifier and through a view's star, weighed
// by their types, and the whole table for a star in its select list.
func TestColumnsRead(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	wide := &Table{Name: "wide", Bytes: 400e9, Columns: []Column{{Name: "id", Type: "VARCHAR"}}}
	for i := range 399 {
		wide.Columns = append(wide.Columns, Column{Name: fmt.Sprint("c", i), Type: "VARCHAR"})
	}
	for _, c := range []struct {
		sql  string
		want int64
	}{
		{`SELECT v.id, v.c1, c2 FROM (SELECT s.* FROM __igloo_split AS s) AS v`, 3e9},
		{`SELECT count(*) FROM __igloo_split`, 1e9},
		{`SELECT * FROM __igloo_split WHERE c3 = 'x'`, 400e9},
	} {
		f, err := h.co.parser.parse(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		if got := columnsRead(f, wide).bytes(wide); got != c.want {
			t.Errorf("%s: %d bytes, want %d", c.sql, got, c.want)
		}
	}
	if typeBytes("BOOLEAN") >= typeBytes("VARCHAR") || typeBytes("DECIMAL(38,0)") != 16 || typeBytes("VARCHAR[]") != 32 {
		t.Error("type weights")
	}
}

// TestRowWork checks the rows a plan's stages go through: a table's rows,
// a hundredth of them when filtered by a constant, once for the scan and
// again for each join and grouping, and an exchange's as the rows that fed
// it.
func TestRowWork(t *testing.T) {
	h := newHarness(t, 0.01, 1, 1)
	parse := func(sql string) node {
		f, err := h.co.parser.parse(sql)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	people := &Table{Name: "people", Rows: 1e9, Columns: []Column{{Name: "id", Type: "BIGINT"}, {Name: "audience", Type: "VARCHAR"}}}
	events := &Table{Name: "events", Rows: 4e9, Columns: []Column{{Name: "person", Type: "BIGINT"}, {Name: "kind", Type: "VARCHAR"}}}
	x := &Exchange{ID: 0}
	stages := []*Stage{
		{fragment: parse(`SELECT id FROM __igloo_t0 WHERE audience = 'a'`), Inputs: []Input{{Table: people}}, Out: x},
		{fragment: parse(`SELECT e.kind, count(*) FROM __igloo_split AS e JOIN __igloo_k0 AS p ON p.id = e.person GROUP BY 1`),
			Inputs: []Input{{Table: events, Split: true}, {Exchange: x, Partitioned: true}}},
	}
	// 1e7 people for the filter; (4e9 + 1e7) rows, scanned, joined and
	// grouped.
	for _, filters := range []func(node, *Table) bool{filtersByConstant, filtersSelectively} {
		if got, want := rowWork(stages, filters), 1e7+3*(4e9+1e7); got != want {
			t.Errorf("%.3g rows, want %.3g", got, want)
		}
	}
	// The latest version, a subquery's value: selective for auto mode's
	// estimates, not for partitions' (see exchangeEstimates).
	stages[0].fragment = parse(`SELECT id FROM __igloo_t0 WHERE audience = (SELECT max(audience) FROM __igloo_t1)`)
	if got, want := rowWork(stages, filtersSelectively), 1e7+3*(4e9+1e7); got != want {
		t.Errorf("filtered by a subquery's value: %.3g rows, want %.3g", got, want)
	}
	if got, want := rowWork(stages, filtersByConstant), 1e9+3*(4e9+1e9); got != want {
		t.Errorf("filtered by a subquery's value, for partitions: %.3g rows, want %.3g", got, want)
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
		return planKey(stmt, []*Stage{{Inputs: []Input{{Table: h.tables["lineitem"], Split: true}}}}, "")
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
	h.observe("a", 0, false, 100, 0)
	h.observe("a", 0, false, 50, 0)
	h.observe("a", 4, false, 30, 0)
	if r, ok := h.get("a"); !ok || r.local != 75 || r.dist != 30 || r.distWorkers != 4 {
		t.Errorf("runs of a: %+v", r)
	}
	// On about as many workers, runs average; on four times as many, the
	// last stands alone.
	h.observe("a", 5, false, 20, 0)
	if r, _ := h.get("a"); r.dist != 25 || r.distWorkers != 5 {
		t.Errorf("on 5 workers: %+v", r)
	}
	h.observe("a", 16, false, 8, 0)
	if r, _ := h.get("a"); r.dist != 8 || r.distWorkers != 16 {
		t.Errorf("on 16 workers: %+v", r)
	}
	for i := range maxHistory {
		h.observe(fmt.Sprint("k", i), 0, false, 1, 0)
	}
	if _, ok := h.get("a"); ok {
		t.Error("kept the key used longest ago")
	}
	if _, ok := h.get("k1"); !ok {
		t.Error("lost a key used since")
	}

	// What the guard learned, from the placement the query history keeps, on
	// a replica of about this one's memory.
	guardedBy := &Coordinator{history: newHistory(), opts: Options{MemoryBytes: 47e9}}
	placement := guardedBy.GuardPlacement(88*time.Second, 47*time.Second, "8 workers: it ran on one machine")
	ran, memory, ok := GuardedRun(placement)
	if !ok || ran != 88*time.Second || memory != 47e9 {
		t.Errorf("guarded run from %q: %s, %d, %v", placement, ran, memory, ok)
	}
	for _, other := range []string{"8 workers: one machine should take 88 s", "moved by the guard after 88 s on this replica, estimated at 47 s; 8 workers: x", ""} {
		if _, _, ok := GuardedRun(other); ok {
			t.Errorf("guarded run from %q", other)
		}
	}
	guardedBy.Seed([]Run{{Key: "g", Workers: 8, Duration: 100 * time.Second, Memory: 6e9}, {Key: "g", Guarded: true, Duration: ran, Memory: memory},
		{Key: "g", Guarded: true, Duration: ran}})
	r, _ := guardedBy.history.get("g")
	if !r.guarded || r.local != 88 || r.dist != 100 {
		t.Errorf("seeded runs of g: %+v", r)
	}
	if r = comparable(r, 47e9, 6e9, 0); !r.guarded {
		t.Errorf("on a replica of the memory it was guarded on: %+v", r)
	}
	if r = comparable(r, 200e9, 6e9, 0); r.guarded || r.local != 0 {
		t.Errorf("on a far larger replica: %+v", r)
	}
	if e := (estimate{oneMachine: 47, onWorkers: 60, workers: 8}); true {
		r, _ := guardedBy.history.get("g")
		if workers, explore, why := choose(e, comparable(r, 47e9, 6e9, 0), true, DefaultThreshold, time.Now()); !workers || explore {
			t.Errorf("seeded guarded: on workers %v, trying %v (%s)", workers, explore, why)
		}
	}

	// Out of memory on one machine until it runs there again.
	co := &Coordinator{history: newHistory()}
	co.Seed([]Run{{Key: "m", Workers: 0, Duration: time.Minute, Memory: 1 << 30}, {Key: "m", OutOfMemory: true, Memory: 1 << 30}})
	if r, _ := co.history.get("m"); !r.outOfMemory || r.local != 60 {
		t.Errorf("runs of m: %+v", r)
	}
	co.Observe("m", 4, 30*time.Second)
	if r, _ := co.history.get("m"); !r.outOfMemory {
		t.Error("a run on workers forgot one machine ran out of memory")
	}
	co.Observe("m", 0, 50*time.Second)
	if r, _ := co.history.get("m"); r.outOfMemory {
		t.Error("a run on one machine left it out of memory")
	}
}

// TestAutoMode checks auto mode's choice in the harness: a statement as
// large as the 1x reports runs on one machine; one that ran faster on
// workers before runs there, and one that ran faster on one machine stays
// there though workers are estimated faster.
func TestAutoMode(t *testing.T) {
	h := newHarness(t, 0.01, 3, 3)
	li := h.tables["lineitem"]
	// Large enough to distribute, but read cold by two cores in seconds.
	li.Bytes = 1 << 30
	q := `SELECT l_returnflag, count(*), sum(l_quantity) FROM lineitem GROUP BY 1`
	join := `SELECT count(*), sum(a.l_quantity - b.l_quantity) FROM lineitem a JOIN lineitem b ON a.l_orderkey = b.l_orderkey AND a.l_linenumber < b.l_linenumber`
	h.co.maxBroadcast = h.tables["customer"].Bytes
	for _, query := range []string{q, join} {
		_, err := h.co.Plan(t.Context(), query, harnessEnv{h}, ModeAuto)
		if !IsUnsupported(err) || PlanKey(err) == "" || !strings.Contains(err.Error(), "auto mode chose one machine") {
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
	// one core.
	h.co.opts.Cores = 1
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

// TestRanHere checks that a statement of a shape that ran to its end on
// this machine is not short of memory there, whatever its estimate says,
// and spills only if what it allocated over its run is over its share.
func TestRanHere(t *testing.T) {
	const gb = 1 << 30
	// What a dashboard's statement at ten times the data exchanges: short of
	// memory and spilling on a machine of 22 GB, by the estimate.
	x := features{bytes: 3e9, exchanged: 2e10, held: 1e10, rows: 2e9, levels: 2, stages: 3}
	m := machine{cores: 8, threads: 8, memory: 22 * gb}
	f := fleet{cores: 8, threads: 8, running: 2, most: 2}
	before := x.estimate(m, f)
	if !before.memory || !before.spills {
		t.Fatalf("by the estimate alone: memory %v, spills %v; want both", before.memory, before.spills)
	}
	m.ranHere, m.allocated = true, 4*gb
	if e := x.estimate(m, f); e.memory || e.spills || e.oneMachine >= before.oneMachine {
		t.Errorf("after a run here of 4 GB: memory %v, spills %v, one machine %.0f s (before %.0f)", e.memory, e.spills, e.oneMachine, before.oneMachine)
	}
	m.allocated = 40 * gb
	if e := x.estimate(m, f); e.memory || !e.spills {
		t.Errorf("after a run here of 40 GB: memory %v, spills %v; want spills alone", e.memory, e.spills)
	}
}

// TestComparable checks that runs measured under other memory, the
// replica's or the workers', are left out, and that unknown memory is not
// held against a run.
func TestComparable(t *testing.T) {
	const gb = int64(1) << 30
	r := runs{local: 14, localMemory: 14 * gb, outOfMemory: true, dist: 1312, distWorkers: 2, distMemory: 6 * gb, exchangeBytes: gb}
	if got := comparable(r, 22*gb, 22*gb, 0); got.local != 0 || got.outOfMemory || got.dist != 0 || got.distWorkers != 0 || got.exchangeBytes != 0 {
		t.Errorf("on 22 GB with workers of 22 GB: %+v", got)
	}
	if got := comparable(r, 15*gb, 7*gb, 0); got.local != 14 || !got.outOfMemory || got.dist != 1312 {
		t.Errorf("within a quarter: %+v", got)
	}
	if got := comparable(runs{local: 5, dist: 9}, 22*gb, 0, 0); got.local != 5 || got.dist != 9 {
		t.Errorf("runs whose memory is not known: %+v", got)
	}
	// Seeded runs that do not say what memory they had are left out.
	co := &Coordinator{history: newHistory()}
	co.Seed([]Run{{Key: "old", Duration: time.Second}, {Key: "new", Duration: time.Second, Memory: 22 * gb}})
	if _, ok := co.history.get("old"); ok {
		t.Error("seeded a run that does not say what memory it had")
	}
	if r, ok := co.history.get("new"); !ok || r.local != 1 || r.localMemory != 22*gb {
		t.Errorf("seeded run: %+v %v", r, ok)
	}
	// A run under other memory stands alone rather than average with them.
	co.history.observe("new", 0, false, 9, 14*gb)
	if r, _ := co.history.get("new"); r.local != 9 || r.localMemory != 14*gb {
		t.Errorf("after a run on 14 GB: %+v", r)
	}
}
