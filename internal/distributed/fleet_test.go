package distributed

import (
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
)

// The scale environment's machines, as the estimates see them: a
// coordinator of 16 cores, 24 threads and a 49.3 GiB limit; workers of 8
// cores, 12 threads and 24.6 GiB at $0.48 an hour (16 for $7.69); and a
// large worker of 64 cores and a 440 GiB limit, as on a machine of 512 GiB,
// at $4.35 an hour.
var (
	scaleCoordinator = machine{cores: 16, threads: 24, memory: 52_935_471_923}
	scaleWorkers     = fleet{cores: 8, threads: 12, running: 16, most: 16, memory: 26_414_048_870, price: 0.48,
		start: 30 * time.Second, tail: 2 * time.Minute}
	scaleLarge = largeWorker{cores: 64, threads: 128, memory: 440 << 30, price: 4.35, start: time.Minute, tail: 3 * time.Minute}
)

// Reports of the scale runs, as the planner measures them (see
// TestScaleReports).
var (
	f1At100 = features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 5.83e+10, levels: 3, stages: 22}
	f2At100 = features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 2.92e+10, levels: 3, stages: 11}
	f4At10x = features{bytes: 4.934e+09, exchanged: 3.491e+10, held: 4.934e+09, rows: 7.87e+09, levels: 7, stages: 26}
	f4At100 = features{bytes: 4.934e+10, exchanged: 3.491e+11, held: 4.934e+10, rows: 7.87e+10, levels: 7, stages: 26}
	e2At100 = features{bytes: 1.349e+11, exchanged: 8.999e+11, held: 1.349e+11, rows: 1.2e+12, levels: 2, stages: 10}
)

// TestSpillOn checks what a machine does with a working set: nothing
// within memoryShare of its memory or with its memory unknown; spilling,
// longer by the ratio to spillExponent; out of memory past oomRatio.
func TestSpillOn(t *testing.T) {
	const gb = 1e9
	if s := spillOn(30*gb, 50e9); s.factor != 1 || s.oom || !strings.Contains(s.String(), "within") {
		t.Errorf("fits: %+v %s", s, s)
	}
	if s := spillOn(1e15, 0); s.factor != 1 || s.oom {
		t.Errorf("memory unknown: %+v", s)
	}
	// Within half again its room (35 GB of 50e9): fits, as estimates run
	// high; past it, spills.
	if s := spillOn(1.45*35*gb, 50e9); s.factor != 1 || s.oom || !strings.Contains(s.String(), "within half again") {
		t.Errorf("1.45 times its room: %+v %s", s, s)
	}
	if s := spillOn(1.55*35*gb, 50e9); s.factor <= 1 || s.oom {
		t.Errorf("1.55 times its room: %+v %s", s, s)
	}
	s := spillOn(70*gb, 50e9)
	if want := math.Pow(2, spillExponent); math.Abs(s.factor-want) > 1e-9 || s.oom || !strings.Contains(s.String(), "spills about 35.0 GB") {
		t.Errorf("twice its room: %+v %s, want factor %.2f", s, s, want)
	}
	if s := spillOn(13*35*gb, 50e9); !s.oom || !strings.Contains(s.String(), "runs out of memory") {
		t.Errorf("13 times its room: %+v %s", s, s)
	}
}

// TestSpillCalibration checks spillExponent and oomRatio against the scale
// runs on the coordinator of 49.3 GiB (a room of 37 GB): each report's
// working set as it reached it, what it held at the memory limit (53 GB)
// and what it spilled at the peak, and how many times longer it took than
// its time where it did not spill, scaled from a tenth of the data. The
// curve is within a factor of two of every one but F3-like at 10x, whose
// distinct counts through a view's computed keys re-read their spilled
// partitions many times over, 11 times as long at 6.6 times the room.
func TestSpillCalibration(t *testing.T) {
	const room = memoryShare * 52_935_471_923
	for _, c := range []struct {
		name                string
		held, spilled, took float64 // GB, GB, seconds
		alone               float64 // seconds without spilling, scaled
		outlier             bool
	}{
		{"E1-like at 100x", 53, 330, 1086, 585, false},
		{"E2-like at 10x", 53, 21, 174, 75, false},
		{"E3-like at 10x", 53, 40, 220, 138, false},
		{"F1-like at 10x", 53, 190, 590, 228, false},
		{"F2-like at 10x", 53, 238, 587, 206, false},
		{"F3-like at 10x", 53, 192, 1843, 168, true},
		{"F4-like at 100x", 53, 71, 778, 480, false},
		{"F5-like at 100x", 53, 51, 248, 200, false},
		{"P1-like at 100x", 53, 37, 318, 240, false},
		{"P5-like at 100x", 53, 128, 363, 130, false},
		{"P7-like at 100x", 53, 81, 281, 120, false},
	} {
		s := spillOn((c.held+c.spilled)*1e9, 52_935_471_923)
		took := c.took / c.alone
		off := max(s.factor/took, took/s.factor)
		t.Logf("%s: working set %.1f times the room, %.2f times as long, estimated %.2f", c.name, s.set/room, took, s.factor)
		if s.oom || off > 2 != c.outlier {
			t.Errorf("%s: estimated %.2f times as long (out of memory %v), took %.2f times", c.name, s.factor, s.oom, took)
		}
	}
	// Out of memory: reports that reached 12 to 23 times the room ran out
	// on that machine (E3-like and F2-like at 100x past 400 GB of spill,
	// F1-like and F3-like past 770 GB); E1-like finished at 10.3.
	for _, set := range []float64{400 + 53, 410 + 53, 786 + 53, 767 + 53} {
		if s := spillOn(set*1e9, 52_935_471_923); !s.oom {
			t.Errorf("a working set of %.0f GB finishes, by the estimate", set)
		}
	}
}

// The second scale runs at ten times the data, of the build auto mode
// chose 4 workers in: a coordinator of 16 cores (m7gd.4xlarge, $0.8542 an
// hour) and 16 workers of 8 (m7gd.2xlarge, $0.4271), as the planner
// measured two reports. F1-like took 678, 368, 200, 112 and 127 s on 1, 2,
// 4, 8 and 16 workers; E2-like 552, 265, 120, 75 and 44 s.
var (
	benchWorkers = fleet{cores: 8, threads: 12, running: 16, most: 16, memory: 26_414_048_870, price: 0.4271,
		replicaPrice: 0.8542, replicaCores: 16, alone: true, start: 30 * time.Second, tail: 2 * time.Minute}
	f1At10x = features{bytes: 1.1838e10, exchanged: 5.6784e10, held: 1.1838e10, rows: 1.9476e10, levels: 11, stages: 24}
	e2At10x = features{bytes: 1.3691e10, exchanged: 9.0162e10, held: 1.3691e10, rows: 2.912e11, levels: 4, stages: 13}
)

// TestFleetChoice checks how many workers, or the large worker, auto mode
// takes: the cheapest way within a fifth of the fastest, the replica's
// time counted, every running worker's where idle ones do not stop alone;
// measured times where it has them; a size estimated near the fastest
// measured tried once to measure it.
func TestFleetChoice(t *testing.T) {
	now := time.Now()
	decide := func(x features, f fleet, r runs, found bool) estimate {
		t.Helper()
		e := x.estimate(scaleCoordinator, f)
		if found {
			e.pick(r, f)
		}
		return e
	}
	explain := func(e estimate) string { return e.fleet + "; " + describeOptions(e.options, e.dollars) }

	// F1-like: 16 workers first, 8 estimated 30% slower. After 127 s on
	// 16, 8 are corrected to 164 s, within fleetExploreWithin: tried once.
	// After 112 s on 8, 8: 16 are within a fifth but cost twice as much,
	// and 4, corrected to 163 s, are not tried. Auto mode had taken 4, 200
	// s, for 194 s estimated within a fifth of 16's 162 s.
	e := decide(f1At10x, benchWorkers, runs{}, false)
	if e.workers != 16 || e.large {
		t.Errorf("F1-like, first: %d workers (%s)", e.workers, explain(e))
	}
	r := runs{dist: 127, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 127}}
	e = decide(f1At10x, benchWorkers, r, true)
	if e.workers != 16 {
		t.Errorf("F1-like after 127 s on 16: %d workers (%s)", e.workers, explain(e))
	}
	if o, ok := exploreFleet(e.options, e.picked, r, now); !ok || o.workers != 8 {
		t.Errorf("F1-like after 127 s on 16: tries %s, %v (%s)", o, ok, explain(e))
	}
	if _, ok := exploreFleet(e.options, e.picked, runs{fleets: r.fleets, fleetExplored: now.Add(-time.Hour)}, now); ok {
		t.Error("F1-like: tried another size again within a day")
	}
	r.fleets[fleetBucket(8)] = 112
	e = decide(f1At10x, benchWorkers, r, true)
	if e.workers != 8 || !strings.Contains(e.fleet, "as it took before") {
		t.Errorf("F1-like after 112 s on 8: %d workers (%s)", e.workers, explain(e))
	}
	if o, ok := exploreFleet(e.options, e.picked, r, now); ok {
		t.Errorf("F1-like after 112 s on 8: tries %s (%s)", o, explain(e))
	}

	// E2-like: 16, whose 44 s correct 8 to 67 s, past fleetExploreWithin
	// of it (8 took 75 s).
	e = decide(e2At10x, benchWorkers, runs{}, false)
	if e.workers != 16 {
		t.Errorf("E2-like, first: %d workers (%s)", e.workers, explain(e))
	}
	r = runs{dist: 44, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 44}}
	e = decide(e2At10x, benchWorkers, r, true)
	if o, ok := exploreFleet(e.options, e.picked, r, now); e.workers != 16 || ok {
		t.Errorf("E2-like after 44 s on 16: %d workers, tries %s %v (%s)", e.workers, o, ok, explain(e))
	}

	// Where idle workers do not stop alone, all 16 bill while any works:
	// fewer cost no less, and none slower is tried.
	held := benchWorkers
	held.alone = false
	r = runs{dist: 127, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 127}}
	e = decide(f1At10x, held, r, true)
	if o, ok := exploreFleet(e.options, e.picked, r, now); e.workers != 16 || ok {
		t.Errorf("F1-like, idle workers kept up: %d workers, tries %s %v (%s)", e.workers, o, ok, explain(e))
	}
	for _, o := range e.options {
		if want := (0.8542 + 16*0.4271) * o.seconds / 3600; !o.large && math.Abs(o.cost-want) > 1e-9 {
			t.Errorf("idle workers kept up: %s costs $%.4f, want $%.4f", o, o.cost, want)
		}
	}
	// The replica counts: with it, 8 at 112 s are cheaper than 4 at 200 s,
	// as the runs' bills had them ($0.133 against $0.143).
	four := option{workers: 4, seconds: 200}.costOn(benchWorkers)
	eight := option{workers: 8, seconds: 112}.costOn(benchWorkers)
	if math.Abs(eight-0.1329) > 0.0005 || math.Abs(four-0.1424) > 0.0005 {
		t.Errorf("8 workers for 112 s $%.4f, 4 for 200 s $%.4f", eight, four)
	}
	// Workers stopped: their start, and their idle tail after, count.
	stopped := benchWorkers
	stopped.running = 0
	e = decide(f1At10x, stopped, runs{}, false)
	for _, o := range e.options {
		if o.started != o.workers {
			t.Errorf("workers stopped: %s starts %d", o, o.started)
		}
	}

	// E1-like at ten times the data, by DuckDB's plan: a working set of 46
	// GB, 1.24 times the replica's room, which fits; its joins' hash tables
	// take 2,033 core-seconds, 127 s on 16 cores, and its stages read 77 GB
	// where its scans read 3: one machine, which held 33 GB and took 98 s,
	// where 16 workers are estimated at 198 s. F3-like: 215 GB, 5.8 times
	// the room, spilling: workers.
	e1 := features{bytes: 7.744e10, exchanged: 7.744e10, held: 7.744e10, rows: 6.688e9, levels: 4, stages: 5,
		planned: true, planSet: 46e9, planBytes: 3e9, planWork: 2033}
	f3 := features{bytes: 6.683e9, exchanged: 6.683e9, held: 6.683e9, rows: 1.273e10, levels: 8, stages: 28,
		planned: true, planSet: 215e9, planBytes: 6.7e9, planWork: 153}
	e = decide(e1, benchWorkers, runs{}, false)
	if onWorkers, _, reason := choose(e, runs{}, false, DefaultThreshold, now); onWorkers || e.spills || e.oneMachine < 100 || e.oneMachine > 160 {
		t.Errorf("E1-like at 10x: workers %v, one machine %.0f s (%s; %s)", onWorkers, e.oneMachine, reason, e.local)
	}
	e = decide(f3, benchWorkers, runs{}, false)
	if onWorkers, _, reason := choose(e, runs{}, false, DefaultThreshold, now); !onWorkers || !e.spills {
		t.Errorf("F3-like at 10x: workers %v, one machine %.0f s (%s; %s)", onWorkers, e.oneMachine, reason, e.local)
	}

	// One machine keeps what it runs faster.
	for name, x := range map[string]features{"F4-like at 10x": f4At10x} {
		e := decide(x, scaleWorkers, runs{}, false)
		if onWorkers, _, reason := choose(e, runs{}, false, DefaultThreshold, now); onWorkers {
			t.Errorf("%s: on workers (%s)", name, reason)
		}
	}
	// F1-like and F2-like at 100x: all 16, whose stages' reading dwarfs
	// their start; one machine would run out of memory.
	for name, x := range map[string]features{"F1-like at 100x": f1At100, "F2-like at 100x": f2At100} {
		e := decide(x, scaleWorkers, runs{}, false)
		onWorkers, _, reason := choose(e, runs{}, false, DefaultThreshold, now)
		if !onWorkers || e.workers != 16 || e.large {
			t.Errorf("%s: workers %v on %d (%s; %s)", name, onWorkers, e.workers, reason, explain(e))
		}
	}
	if e := decide(e2At100, scaleWorkers, runs{}, false); !e.local.oom {
		t.Errorf("E2-like at 100x: one machine %s", e.local)
	}

	// F4-like at 100x fits the large worker of 440 GiB: faster than 16
	// workers and cheaper, whether all are stopped or running.
	withLarge := stopped
	withLarge.large = &scaleLarge
	e = decide(f4At100, withLarge, runs{}, false)
	if onWorkers, _, reason := choose(e, runs{}, false, DefaultThreshold, now); !onWorkers || !e.large || e.workers != 1 ||
		e.options[len(e.options)-1].spill.factor != 1 || !strings.Contains(e.fleet, "the large worker") {
		t.Errorf("F4-like at 100x, all stopped: workers %v, large %v (%s; %s)", onWorkers, e.large, reason, explain(e))
	}
	running := benchWorkers
	l := scaleLarge
	l.running = true
	running.large = &l
	if e := decide(f4At100, running, runs{}, false); !e.large {
		t.Errorf("F4-like at 100x, all running: %d workers (%s)", e.workers, explain(e))
	}
	// F1-like at 100x spills on it, 1.5 times as long, 342 s against 16
	// workers' 314 s: not before it ran there (see option.unproven). Once
	// it took 342 s there: within a fifth, for 79% of their cost.
	if e := decide(f1At100, withLarge, runs{}, false); e.large || e.workers != 16 || !strings.Contains(e.fleet, "not the large worker") {
		t.Errorf("F1-like at 100x with the large worker: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
	if e := decide(f1At100, withLarge, runs{large: 342}, true); !e.large || !strings.Contains(e.fleet, "within a fifth of the fastest, 16 workers") {
		t.Errorf("F1-like at 100x after 342 s on the large worker: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
	// What it took on the large worker corrects the large worker alone:
	// after 900 s there and 700 s on 16 workers, the fleet.
	r = runs{large: 900, dist: 700, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 700}}
	if e := decide(f4At100, withLarge, r, true); e.large || e.workers < 8 {
		t.Errorf("F4-like at 100x after 900 s on the large worker and 700 s on 16: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
}

// TestUnprovenLarge checks that the large worker does not run a statement
// whose working set would spill there before it ran there, while the small
// workers can: F3-like at a hundred times the data, its working set 1.6 TB
// by its plan, 5.3 times the large worker's room, was estimated at 217 s
// there, spilling 2.5 times as long, against 1,536 s on 16 workers, which
// it had taken; it ran past 1,800 s there, and 16 workers ran it in 729 s.
func TestUnprovenLarge(t *testing.T) {
	now := time.Now()
	f3 := features{bytes: 6.81e10, exchanged: 6.81e10, held: 6.81e10, rows: 1.259e11, levels: 7, stages: 35,
		planned: true, planSet: 1.585e12, planBytes: 6.75e10, planWork: 1791}
	// The large worker as it ran: 197 DuckDB threads, a limit of 425 GB.
	l := scaleLarge
	l.threads, l.memory, l.running = 197, 424_987_013_939, true
	f := scaleWorkers
	f.large = &l
	decide := func(f fleet, r runs, found bool) estimate {
		t.Helper()
		e := f3.estimate(scaleCoordinator, f)
		if found {
			e.pick(r, f)
		}
		return e
	}
	explain := func(e estimate) string { return e.fleet + "; " + describeOptions(e.options, e.dollars) }
	r := runs{dist: 1536, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 1536}, explored: now}
	e := decide(f, r, true)
	large := e.options[len(e.options)-1]
	if !large.large || large.spill.factor < 2.4 || large.spill.factor > 2.6 || math.Abs(large.seconds-217) > 25 {
		t.Fatalf("the large worker: %s %.0f s, %s", large, large.seconds, large.spill)
	}
	if e.large || e.workers != 16 || !strings.Contains(e.fleet, "not the large worker, estimated at") ||
		!strings.Contains(describeOptions(e.options, true), "spills 2.5x, not run on") {
		t.Errorf("after 1,536 s on 16 workers: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
	if workers, _, reason := choose(e, r, true, DefaultThreshold, now); !workers || e.large {
		t.Errorf("after 1,536 s on 16 workers: workers %v (%s)", workers, reason)
	}
	// Nor the first time.
	if e := decide(f, runs{}, false); e.large || e.workers == 0 {
		t.Errorf("first: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
	// Once it ran there, what it took stands.
	r.large = 900
	if e := decide(f, r, true); !e.large {
		t.Errorf("after 900 s on the large worker: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
	// Without small workers, the large worker is the way on workers.
	alone := fleet{large: &l, replicaPrice: 0.8542, replicaCores: 16}
	if e := decide(alone, runs{}, false); !e.large || e.workers != 1 {
		t.Errorf("the large worker alone: large %v on %d (%s)", e.large, e.workers, explain(e))
	}
}

// TestSpillingNotProbed checks that auto mode never tries one machine, to
// measure it, where its working set says it would spill. F3-like at ten
// times the data took 108 s on 16 workers; an earlier run on the replica,
// which the guard stopped, was learned at all of the replica's 53 GB
// (the query history's memory for a run cut short), which auto mode took
// for proof that the statement fit: estimated at 47 s, it was tried there
// once, held 52 GB, spilled 54 GB and was moved after 145 s. Its plan's
// working set, about three times that, says it spills.
func TestSpillingNotProbed(t *testing.T) {
	now := time.Now()
	f3 := features{bytes: 6.683e9, exchanged: 6.683e9, held: 6.683e9, rows: 1.273e10, levels: 8, stages: 28,
		planned: true, planSet: 150e9, planBytes: 6.635e9, planWork: 179}
	r := runs{dist: 108, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 108}}
	here := scaleCoordinator
	here.ranHere, here.allocated = true, here.memory
	e := f3.estimate(here, benchWorkers)
	e.pick(r, benchWorkers)
	if e.spills || math.Abs(e.oneMachine-47) > 3 {
		t.Errorf("as on the benchmark: one machine %.0f s, %s", e.oneMachine, e.local)
	}
	// A run cut short is no footprint (see workload.Controller.Completed):
	// by the plan, it spills, and is not tried there.
	e = f3.estimate(scaleCoordinator, benchWorkers)
	e.pick(r, benchWorkers)
	workers, explore, reason := choose(e, r, true, DefaultThreshold, now)
	if !e.spills || !workers || explore || !strings.Contains(reason, "one machine should run short of memory") {
		t.Errorf("by its plan: spills %v, workers %v, explore %v (%s; %s)", e.spills, workers, explore, reason, e.local)
	}
}

// TestChooseShared checks that statements running beside one on the
// replica send a heavy statement to workers rather than take their cores:
// F5-like at ten times the data, estimated at 65 s here and 73 s on 16
// workers, runs here alone and on workers beside three others; one that
// ran in 25 s here and 66 s on 16 workers alone, as F5-like did, runs here
// alone and on workers beside three (it ran 577 s here beside 16 users'
// dashboards), and so does one that ran in 25 s here alone, never on
// workers; a dashboard's statement of 3 s stays here beside ten.
func TestChooseShared(t *testing.T) {
	now := time.Now()
	f5 := features{bytes: 9.2562e9, exchanged: 3.922e10, held: 9.2562e9, rows: 1.3863e10, levels: 9, stages: 28, planned: true, planSet: 3.1e10}
	decide := func(x features, others int, r runs, found bool) (bool, string) {
		t.Helper()
		e := x.estimate(scaleCoordinator, benchWorkers)
		e.others = others
		if found {
			e.pick(r, benchWorkers)
		}
		workers, _, reason := choose(e, r, found, DefaultThreshold, now)
		return workers, reason
	}
	if workers, reason := decide(f5, 0, runs{}, false); workers {
		t.Errorf("F5-like alone: workers (%s)", reason)
	}
	if workers, reason := decide(f5, 3, runs{}, false); !workers || !strings.Contains(reason, "3 other statements run on this replica") {
		t.Errorf("F5-like beside three: workers %v (%s)", workers, reason)
	}
	r := runs{local: 25, dist: 66, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 66}, explored: now}
	if workers, reason := decide(f5, 0, r, true); workers {
		t.Errorf("F5-like alone after 25 s here and 66 s on 16: workers (%s)", reason)
	}
	if workers, reason := decide(f5, 3, r, true); !workers {
		t.Errorf("F5-like beside three after 25 s here and 66 s on 16: here (%s)", reason)
	}
	// Ran here in 25 s alone, never on workers: beside three, 100 s by
	// that, and the workers estimated at 73 s.
	if workers, reason := decide(f5, 3, runs{local: 25, explored: now}, true); !workers {
		t.Errorf("F5-like beside three after 25 s here: here (%s)", reason)
	}
	dash := features{bytes: 4e8, held: 4e8, rows: 1e9, levels: 2, stages: 3}
	if workers, reason := decide(dash, 10, runs{}, false); workers {
		t.Errorf("a dashboard's statement beside ten: workers (%s)", reason)
	}
}

// TestFleetCalibration checks the workers' estimates against the second
// scale runs at ten times the data: seven reports on 16 workers, and two
// on 1, 2, 4 and 8, within a factor of 1.35 at the root mean square of
// the logarithms, where a cost a stage and one machine's rate were within
// a factor of 2.05.
func TestFleetCalibration(t *testing.T) {
	reports := map[string]features{
		"E2-like": e2At10x,
		"E3-like": {bytes: 7.0418e9, exchanged: 8.3513e10, rows: 5.745e9, levels: 5, stages: 8},
		"F1-like": f1At10x,
		"F2-like": {bytes: 1.1838e10, exchanged: 5.6784e10, rows: 1.7653e10, levels: 11, stages: 23},
		"F3-like": {bytes: 6.6826e9, exchanged: 6.6826e9, rows: 1.273e10, levels: 8, stages: 28},
		"F4-like": {bytes: 8.5154e9, exchanged: 3.8479e10, rows: 1.3168e10, levels: 7, stages: 29},
		"F5-like": {bytes: 9.2562e9, exchanged: 3.922e10, rows: 1.3863e10, levels: 9, stages: 28},
	}
	runs := []struct {
		report  string
		workers int
		took    float64
	}{
		{"E2-like", 16, 44.0}, {"E3-like", 16, 55.6}, {"F1-like", 16, 126.8}, {"F2-like", 16, 121.1}, {"F3-like", 16, 80.7},
		{"F4-like", 16, 28.0}, {"F5-like", 16, 66.5}, {"E2-like", 8, 74.6}, {"F1-like", 8, 111.8}, {"E2-like", 4, 120.0},
		{"F1-like", 4, 200.3}, {"E2-like", 2, 265.4}, {"F1-like", 2, 367.5}, {"E2-like", 1, 552.1}, {"F1-like", 1, 677.9},
	}
	var sum float64
	for _, r := range runs {
		o := reports[r.report].onFleet(r.workers, benchWorkers)
		d := math.Log(o.estimate / r.took)
		sum += d * d
		t.Logf("%s on %d: took %.0f s, estimated %.0f s", r.report, r.workers, r.took, o.estimate)
	}
	if rms := math.Exp(math.Sqrt(sum / float64(len(runs)))); rms > 1.4 {
		t.Errorf("estimates within a factor of %.2f at the root mean square", rms)
	}
}

// TestFailedRuns checks that a statement's failures on a number of workers
// steer how many it runs on: F3-like at a hundred times the data took 729
// s on 16 workers, failed there after 2,078 s, a worker out of memory, and
// auto mode kept taking 16 as if it took 729 s.
func TestFailedRuns(t *testing.T) {
	now := time.Now()
	options := func() []option {
		return []option{{workers: 8, estimate: 1100}, {workers: 16, estimate: 600}, {workers: 1, large: true, estimate: 500}}
	}
	run := func(r runs) (option, string) {
		t.Helper()
		opts := options()
		correctAt(opts, r, benchWorkers, now)
		best, why, ok := pick(opts, true)
		if !ok {
			t.Fatal("nothing picked")
		}
		return best, why + "; " + describeOptions(opts, true)
	}
	b16 := fleetBucket(16)
	// 729 s on 16, the large worker 900 s: 16.
	r := runs{fleets: map[int]float64{b16: 729}, large: 900}
	if best, why := run(r); best.workers != 16 || best.large {
		t.Errorf("after 729 s on 16: %s (%s)", best, why)
	}
	// Failed on 16 an hour ago after 2,078 s: 16 takes 1.5 times that, and
	// the large worker's 900 s is faster.
	r.failures = map[int]failure{b16: {seconds: 2078, at: now.Add(-time.Hour)}}
	if best, why := run(r); !best.large || !strings.Contains(why, "16 workers 3117 s (failed after 2078 s)") {
		t.Errorf("after failing on 16: %s (%s)", best, why)
	}
	// A day on, the failure counts no more.
	r.failures[b16] = failure{seconds: 2078, at: now.Add(-exploreEvery - time.Hour)}
	if best, why := run(r); best.workers != 16 || best.large {
		t.Errorf("a day after failing on 16: %s (%s)", best, why)
	}
	// Failed on the large worker after 700 s: 16 again.
	r.failures = nil
	r.largeFailure = failure{seconds: 700, at: now.Add(-time.Minute)}
	r.large = 0
	if best, why := run(r); best.large || !strings.Contains(why, "the large worker 1050 s (failed after 700 s)") {
		t.Errorf("after failing on the large worker: %s (%s)", best, why)
	}

	// The history keeps failures by the workers' bucket, apart from the
	// large worker's, and for workers of the memory they failed with.
	h := newHistory()
	h.observe("a", 16, false, 729, 24<<30)
	h.observeFailure("a", 16, false, 2078, 24<<30, now)
	h.observeFailure("a", 1, true, 300, 440<<30, now)
	got, _ := h.get("a")
	if f := got.failures[b16]; f.seconds != 2078 || got.fleets[b16] != 729 || got.largeFailure.seconds != 300 {
		t.Errorf("history: %+v", got)
	}
	if c := comparable(got, 0, 48<<30, 440<<30); c.failures != nil || c.largeFailure.seconds != 300 {
		t.Errorf("workers of other memory: %+v", c)
	}
	// Seeded from the query history: a run that failed on workers, then.
	co := &Coordinator{history: newHistory()}
	co.Seed([]Run{{Key: "b", Workers: 16, Duration: 2078 * time.Second, Memory: 24 << 30, Failed: true, At: now.Add(-2 * time.Hour)}})
	if r, _ := co.history.get("b"); r.failures[b16].seconds != 2078 || !r.failures[b16].at.Equal(now.Add(-2*time.Hour)) || r.outOfMemory {
		t.Errorf("seeded: %+v", r)
	}
}

// TestFitsHere checks which statements the coordinator would run itself
// should their workers fail: not one whose working set by DuckDB's plan
// is four or more times its room, whatever a run of its shape allocated
// there at a tenth of the data; one that held little there still does.
func TestFitsHere(t *testing.T) {
	room := memoryShare * float64(scaleCoordinator.memory)
	f3 := features{bytes: 1e11, exchanged: 1e11, held: 1e11, rows: 1.3e11, levels: 7, stages: 35, planned: true, planSet: 250e9}
	e := f3.estimate(scaleCoordinator, scaleWorkers)
	if !e.farBeyond || e.local.factor <= 1 {
		t.Errorf("F3-like at 100x, 250 GB: far beyond %v, %s", e.farBeyond, e.local)
	}
	// A run of its shape allocated 53 GB here, at a tenth of the data: the
	// plan's 250 GB, more than three times that, stands.
	here := scaleCoordinator
	here.ranHere, here.allocated = true, 52_935_471_923
	if e := f3.estimate(here, scaleWorkers); !e.farBeyond || e.local.set != 250e9 {
		t.Errorf("F3-like at 100x, a run of its shape here: far beyond %v, %s", e.farBeyond, e.local)
	}
	// A dashboard's statement estimated at 10 GB that allocated 2 GB here:
	// what it allocated.
	dash := features{bytes: 1e9, held: 1e9, rows: 1e8, levels: 2, stages: 3, planned: true, planSet: 10e9}
	here.allocated = 2e9
	if e := dash.estimate(here, scaleWorkers); e.farBeyond || e.local.set != 2e9 {
		t.Errorf("a dashboard's statement: far beyond %v, %s", e.farBeyond, e.local)
	}
	if e := (features{planned: true, planSet: 3.9 * room}).estimate(scaleCoordinator, scaleWorkers); e.farBeyond {
		t.Error("3.9 times the room: far beyond")
	}
	p := &Plan{farBeyond: true}
	if p.FitsHere() || !(&Plan{}).FitsHere() || (&Plan{shortOfMemory: true}).FitsHere() {
		t.Error("FitsHere")
	}
}

// TestFleetSizes checks the numbers of workers weighed: powers of two up
// to the most, and the running workers.
func TestFleetSizes(t *testing.T) {
	for _, c := range []struct {
		running, most int
		want          string
	}{
		{0, 16, "[1 2 4 8 16]"},
		{3, 13, "[1 2 3 4 8 13]"},
		{16, 16, "[1 2 4 8 16]"},
		{0, 1, "[1]"},
	} {
		if got := fmt.Sprint(fleetSizes(c.running, c.most)); got != c.want {
			t.Errorf("running %d, most %d: %s, want %s", c.running, c.most, got, c.want)
		}
	}
}

// TestFleetHistory checks that runs on workers are kept by how many ran
// them, and that a run on the large worker is kept apart.
func TestFleetHistory(t *testing.T) {
	h := newHistory()
	h.observe("a", 16, false, 50, 0)
	h.observe("a", 4, false, 60, 0)
	h.observe("a", 5, false, 50, 0)
	h.observe("a", 1, true, 40, 0)
	r, _ := h.get("a")
	if r.fleets[fleetBucket(16)] != 50 || r.fleets[fleetBucket(4)] != 55 || r.dist != 55 || r.distWorkers != 5 || r.large != 40 || len(r.fleets) != 2 {
		t.Errorf("runs: %+v", r)
	}
	// A copy: changing it changes nothing kept.
	r.fleets[fleetBucket(16)] = 1
	if again, _ := h.get("a"); again.fleets[fleetBucket(16)] != 50 {
		t.Error("the runs read shared their fleets with the history")
	}
	// Workers of other memory: their runs go; the large worker's stay.
	const gb = int64(1) << 30
	h.observe("b", 16, false, 50, 24*gb)
	h.observe("b", 1, true, 40, 440*gb)
	r, _ = h.get("b")
	if c := comparable(r, 0, 60*gb, 440*gb); c.fleets != nil || c.dist != 0 || c.large != 40 {
		t.Errorf("workers of 60 GB: %+v", c)
	}
	if c := comparable(r, 0, 24*gb, 120*gb); c.fleets[fleetBucket(16)] != 50 || c.large != 0 {
		t.Errorf("a large worker of 120 GB: %+v", c)
	}
	// Seeded from the query history, by the placement it records.
	co := &Coordinator{history: newHistory()}
	co.Seed([]Run{{Key: "c", Workers: 1, Large: OnLarge(LargePlacement + ": one machine should take ..."), Duration: time.Minute, Memory: 440 * gb},
		{Key: "c", Workers: 8, Duration: 2 * time.Minute, Memory: 24 * gb}})
	if r, _ := co.history.get("c"); r.large != 60 || r.fleets[fleetBucket(8)] != 120 || r.fleets[fleetBucket(1)] != 0 {
		t.Errorf("seeded: %+v", r)
	}
}

// TestPick checks the choice among ways: the cheapest within a fifth of
// the fastest, none that runs out of memory, and the reason.
func TestPick(t *testing.T) {
	opts := []option{
		{workers: 1, seconds: 100, cost: 1},
		{workers: 4, seconds: 40, cost: 1.6},
		{workers: 16, seconds: 35, cost: 5.6},
		{workers: 1, large: true, seconds: 30, cost: 9, spill: spill{factor: 1, oom: true}},
	}
	best, why, ok := pick(opts, true)
	if !ok || best.workers != 4 || best.large || !strings.Contains(why, "within a fifth of the fastest, 16 workers at 35 s, for 29% of its cost") {
		t.Errorf("picked %s: %s", best, why)
	}
	if _, why, ok := pick(opts[3:], true); ok || why == "" {
		t.Errorf("only a way that runs out of memory: %v %s", ok, why)
	}
	opts[1].seconds = 43
	if best, _, _ := pick(opts, true); best.workers != 16 {
		t.Errorf("4 workers past a fifth of the fastest: picked %s", best)
	}
	// No way picked is beaten on time, and on cost by more than costTie,
	// by another that finishes.
	rnd := rand.New(rand.NewPCG(1, 2))
	for range 1000 {
		var opts []option
		for _, n := range []int{1, 2, 4, 8, 16} {
			opts = append(opts, option{workers: n, seconds: 10 + 500*rnd.Float64(), cost: rnd.Float64(), spill: spill{oom: rnd.IntN(8) == 0}})
		}
		best, _, ok := pick(opts, true)
		for _, o := range opts {
			if ok && !o.spill.oom && o.seconds < best.seconds && o.cost*(1+costTie) < best.cost {
				t.Fatalf("picked %s (%.0f s, %.3f) over %s (%.0f s, %.3f)", best, best.seconds, best.cost, o, o.seconds, o.cost)
			}
		}
	}
}

// A statement runs on as many of the running workers as auto mode chose,
// those with the fewest tasks, and on the large worker alone when it chose
// that; EXPLAIN DISTRIBUTED and the plan's placement say so.
func TestFleetOnWorkers(t *testing.T) {
	h := newHarness(t, 0.01, 6, 4)
	// Workers 0 to 2 are the fleet, worker 3 the large worker.
	addr := func(i int) string { return h.lns[i].Addr().String() }
	co, err := NewCoordinator(Options{
		Source: fileSource{h}, Secret: testSecret, Version: "test", TempDir: filepath.Join(h.dir, "coordinator-fleet"),
		Warehouse: filepath.Join(h.dir, "warehouse") + "/", Cores: 2,
		Pool:    PoolOptions{Addresses: []string{addr(0), addr(1), addr(2)}},
		Large:   PoolOptions{Addresses: []string{addr(3)}},
		Storage: recordingStorage{NewStorage(icebergio.LocalFS{}), h},
	}, h.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { co.Close() })
	h.co = co
	var tasks [4]atomic.Int32
	for i, w := range h.workers {
		w.beforeTask = func(http.ResponseWriter, *http.Request) bool {
			tasks[i].Add(1)
			return true
		}
	}
	h.tables["lineitem"].Bytes = 1 << 30
	q := `SELECT l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM lineitem GROUP BY 1 ORDER BY 1`
	ctx := t.Context()
	want, _, err := h.query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	_, err = co.Plan(ctx, q, harnessEnv{h}, ModeAuto)
	key := PlanKey(err)
	if key == "" {
		t.Fatalf("auto mode's first choice: %v", err)
	}

	// It took 200 s here, 5 s on one worker and 6 s on all three: one
	// worker, though three run.
	co.Observe(key, 0, 200*time.Second)
	co.Observe(key, 1, 5*time.Second)
	co.Observe(key, 3, 6*time.Second)
	got, _, plan, ex, err := h.run(ctx, q, ModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("on one worker: %s", msg)
	}
	if plan.workers != 1 || plan.large || ex.Workers != 1 || ex.Large || tasks[3].Load() != 0 || !strings.HasPrefix(plan.Placement(), "1 workers: ") {
		t.Errorf("on one worker: planned %d (large %v), ran on %d (large %v), the large worker ran %d tasks; placement %q",
			plan.workers, plan.large, ex.Workers, ex.Large, tasks[3].Load(), plan.Placement())
	}
	rows, err := co.Explain(ctx, q, harnessEnv{h}, ModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	explained := map[string]string{}
	for _, r := range rows {
		explained[r[0]] += r[1] + "\n"
	}
	if !strings.Contains(explained["fleet"], "1 worker, 5 s as it took before, the fastest") || !strings.Contains(explained["options"], "3 workers 6 s (measured)") ||
		!strings.Contains(explained["spill"], "this replica: working set") || !strings.Contains(explained["spill"], "the large worker: working set") {
		t.Errorf("EXPLAIN DISTRIBUTED: %q", rows)
	}

	// It took 2 s on the large worker: there, alone.
	co.ObserveOn(key, 1, true, 2*time.Second)
	for i := range tasks {
		tasks[i].Store(0)
	}
	got, _, plan, ex, err = h.run(ctx, q, ModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("on the large worker: %s", msg)
	}
	if !plan.large || !ex.Large || ex.Workers != 1 || tasks[3].Load() == 0 || tasks[0].Load()+tasks[1].Load()+tasks[2].Load() != 0 || !OnLarge(plan.Placement()) {
		t.Errorf("on the large worker: planned large %v, ran on %d (large %v), tasks %d %d %d %d; placement %q", plan.large, ex.Workers, ex.Large,
			tasks[0].Load(), tasks[1].Load(), tasks[2].Load(), tasks[3].Load(), plan.Placement())
	}
	if rows, err := co.Explain(ctx, q, harnessEnv{h}, ModeAuto); err != nil || rows[0][1] != LargePlacement {
		t.Errorf("EXPLAIN DISTRIBUTED on the large worker: %q, %v", rows, err)
	}
	// Forced, a statement runs on every worker of the fleet, never on the
	// large worker.
	for i := range tasks {
		tasks[i].Store(0)
	}
	if _, _, plan, ex, err := h.run(ctx, q, ModeOn); err != nil || plan.large || ex.Large || ex.Workers != 3 || tasks[3].Load() != 0 {
		t.Errorf("forced: %v; large %v, %d workers, the large worker ran %d tasks", err, ex != nil && ex.Large, ex.Workers, tasks[3].Load())
	}
	// A statement the server runs itself keeps the fleet up, for the
	// guard, and never the large worker, which nothing else runs on.
	release := co.Keep()
	co.pool.mu.Lock()
	co.large.mu.Lock()
	busy, largeBusy := co.pool.busy, co.large.busy
	co.large.mu.Unlock()
	co.pool.mu.Unlock()
	release()
	if busy != 1 || largeBusy != 0 {
		t.Errorf("kept: the fleet %d, the large worker %d", busy, largeBusy)
	}
}

// leastBusy takes the workers running the fewest tasks.
func TestLeastBusy(t *testing.T) {
	ws := []*worker{
		{status: workerStatus{ID: "a", Running: 3}}, {status: workerStatus{ID: "b", Running: 0}},
		{status: workerStatus{ID: "c", Running: 1}}, {status: workerStatus{ID: "d", Running: 0}},
	}
	var ids []string
	for _, w := range leastBusy(ws, 3) {
		ids = append(ids, w.status.ID)
	}
	if strings.Join(ids, ",") != "b,c,d" || len(leastBusy(ws, 9)) != 4 {
		t.Errorf("least busy: %v", ids)
	}
}

// TestListPrices checks what a worker is taken to cost where no price is
// set: the list price of its instance type, as told or as the worker
// reports it, on Spot or not; a set price stands.
func TestListPrices(t *testing.T) {
	co := &Coordinator{opts: Options{Region: "us-east-1"}}
	reports := []*worker{{status: workerStatus{InstanceType: "m7gd.2xlarge", Spot: true, Region: "eu-west-1"}}}
	for _, c := range []struct {
		name string
		set  float64
		typ  string
		spot bool
		ws   []*worker
		want float64
	}{
		{"set", 0.31, "m7gd.2xlarge", false, reports, 0.31},
		{"told", 0, "r7gd.16xlarge", false, reports, 4.3546},
		{"told, Spot", 0, "m7gd.2xlarge", true, nil, 0.4271 * 0.35},
		{"as the worker reports", 0, "", false, reports, 0.4271 * 0.35},
		{"unknown", 0, "", false, nil, 0},
	} {
		if got := co.price(c.set, c.typ, c.spot, c.ws); math.Abs(got-c.want) > 0.002 {
			t.Errorf("%s: %.4f, want %.4f", c.name, got, c.want)
		}
	}
	// With list prices alone, a statement within a fifth of the fastest on
	// a large worker of 64 cores and 16 workers of 8 runs on the cheaper:
	// the large worker, at its r7gd.16xlarge's $4.35 against 16
	// m7gd.2xlarge's $6.83 an hour.
	f := scaleWorkers
	f.price = co.price(0, "m7gd.2xlarge", false, nil)
	l := scaleLarge
	l.running, l.price = true, co.price(0, "r7gd.16xlarge", false, nil)
	f.large = &l
	mid := features{bytes: 4e10, held: 1e10, rows: 1e10, levels: 3, stages: 10}
	if e := mid.estimate(scaleCoordinator, f); !e.large || !e.dollars {
		t.Errorf("by list prices: %d workers, large %v, dollars %v (%s)", e.workers, e.large, e.dollars, e.fleet)
	}
}

// TestFillCost checks what writing a fill's rows adds to its estimates: a
// copy of a table of 50 GB of files at ten times the data, whose rows this
// replica writes wherever the query runs, and after fetching them from the
// workers where it runs there, stays here, where a read of the same query
// runs on workers; an aggregate of it, whose result keeps a hundredth of
// its rows at most, runs on workers either way.
func TestFillCost(t *testing.T) {
	now := time.Now()
	copyOf := features{bytes: 5e10, rows: 1e9, levels: 1, stages: 1, planned: true, planSet: 1e9, planBytes: 5e10, planKeeps: 1}
	decide := func(x features) (bool, string) {
		t.Helper()
		e := x.estimate(scaleCoordinator, benchWorkers)
		workers, _, reason := choose(e, runs{}, false, DefaultThreshold, now)
		return workers, reason
	}
	if workers, reason := decide(copyOf); !workers {
		t.Errorf("a read of the copy's query: here (%s)", reason)
	}
	copyOf.fill = true
	if workers, reason := decide(copyOf); workers {
		t.Errorf("the copy: on workers (%s)", reason)
	}
	aggregate := copyOf
	aggregate.planKeeps = aggregateKeeps
	if workers, reason := decide(aggregate); !workers {
		t.Errorf("an aggregate fill: here (%s)", reason)
	}
}

// TestOrderWithoutLimit checks a read whose result is its every row,
// ordered: at ten times the data, 361 million rows of four columns took
// 516 s to reach the client from one machine, estimated at 30 s, which
// counted none of the sending, and came within 2 GB of the memory watch,
// its sort held near the replica's room. The estimates count the client's
// taking the rows, either way; a sort that holds half the room or more
// runs on workers where they take no more than a quarter longer; and why
// one machine runs a statement names the rule that decided.
func TestOrderWithoutLimit(t *testing.T) {
	now := time.Now()
	ordered := features{bytes: 4e9, rows: 3.61e8, levels: 1, stages: 1, planned: true, planSet: 21e9, planBytes: 4e9,
		planFields: 3.61e8 * 4, planSorted: 20.8e9}
	e := ordered.estimate(scaleCoordinator, benchWorkers)
	if e.stream < 500 || e.stream > 530 || e.oneMachine < e.stream || !e.risky {
		t.Errorf("sending %.0f s, one machine %.0f s, risky %v", e.stream, e.oneMachine, e.risky)
	}
	workers, _, reason := choose(e, runs{}, false, DefaultThreshold, now)
	if !workers || !strings.Contains(reason, "its sort holds 20.8 GB, near the 37.1 GB one machine has for it") {
		t.Errorf("ordered rows: workers %v (%s)", workers, reason)
	}
	// After it ran 516 s on one machine: workers, at about as long.
	if workers, _, reason := choose(e, runs{local: 516, explored: now}, true, DefaultThreshold, now); !workers {
		t.Errorf("after 516 s on one machine: here (%s)", reason)
	}
	// A sort of a tenth of the room is no risk: one machine, not clearly
	// slower.
	small := ordered
	small.planSorted = 3.7e9
	e = small.estimate(scaleCoordinator, benchWorkers)
	if workers, _, reason := choose(e, runs{}, false, DefaultThreshold, now); workers || e.risky ||
		!strings.Contains(reason, "times as fast as one machine's") {
		t.Errorf("a small sort: workers %v, risky %v (%s)", workers, e.risky, reason)
	}
	// Why one machine: the threshold, where it decided.
	for _, c := range []struct {
		e    estimate
		want string
	}{
		{estimate{oneMachine: 30, onWorkers: 22, workers: 16}, "one machine should take 30 s, within the 60 s a statement must take for workers to be worth it"},
		{estimate{oneMachine: 90, onWorkers: 70, workers: 16}, "workers should take 70 s, not 1.5 times as fast as one machine's 90 s"},
		{estimate{oneMachine: 90, onWorkers: math.Inf(1)}, "no way on workers would finish"},
	} {
		if workers, _, reason := choose(c.e, runs{}, false, DefaultThreshold, now); workers || !strings.Contains(reason, c.want) {
			t.Errorf("%+v: workers %v (%s), want %q", c.e, workers, reason, c.want)
		}
	}
}

// TestProbeOrdered checks the ways on workers auto mode weighs, and tries,
// for an ordered result of 361 million rows whose sort holds 41.6 GB, at
// ten times the data on a replica with 34 GB for it: auto mode ran it on
// workers, then tried one worker to measure it, which took 621 s against
// 462 to 476 s on 16 and 509 to 522 s on one machine. One worker's share
// of the sort spills there; a first run takes the best way, not a probe;
// and a probe of a statement of minutes is within a fifth of the best
// way's time.
func TestProbeOrdered(t *testing.T) {
	now := time.Now()
	replica := scaleCoordinator
	replica.memory = 48_571_428_571 // 34 GB for a statement
	ordered := features{bytes: 4e9, rows: 3.61e8, levels: 1, stages: 1, planned: true, planSet: 43e9, planBytes: 4e9,
		planFields: 3.61e8 * 4, planSorted: 41.6e9}
	e := ordered.estimate(replica, benchWorkers)
	explain := describeOptions(e.options, e.dollars)
	for _, o := range e.options {
		share := ordered.planSorted / float64(o.workers)
		if spills := share > slightSpill*memoryShare*float64(benchWorkers.memory); spills != (o.spill.factor > 1) {
			t.Errorf("%s, a sort share of %.1f GB: spills %.1fx (%s)", o, share/1e9, o.spill.factor, explain)
		}
	}
	if one := e.options[0]; one.workers != 1 || one.spill.factor < 1.5 {
		t.Errorf("one worker: %s, spills %.1fx (%s)", one, one.spill.factor, explain)
	}
	workers, _, reason := choose(e, runs{}, false, DefaultThreshold, now)
	if !workers || !e.risky {
		t.Errorf("first run: workers %v, risky %v (%s)", workers, e.risky, reason)
	}
	if o, probe := exploreFleet(e.options, e.picked, runs{}, now); probe {
		t.Errorf("first run: probes %s (%s)", o, explain)
	}
	// After 470 s on 16 workers: no probe beyond a fifth of that, nor of
	// one worker.
	r := runs{dist: 470, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 470}}
	e.pick(r, benchWorkers)
	explain = describeOptions(e.options, e.dollars)
	if e.picked.seconds > 470*fleetMargin {
		t.Errorf("after 470 s on 16: picked %s at %.0f s (%s)", e.picked, e.picked.seconds, explain)
	}
	if o, probe := exploreFleet(e.options, e.picked, r, now); probe && (o.workers == 1 || o.seconds > exploreLongWithin*min(470, e.picked.seconds)) {
		t.Errorf("after 470 s on 16: probes %s at %.0f s (%s)", o, o.seconds, explain)
	}
	// A statement of seconds still probes within fleetExploreWithin.
	short := []option{{workers: 4, estimate: 50, seconds: 50, cost: 1}, {workers: 16, estimate: 40, seconds: 40, measured: true, cost: 2}}
	if o, probe := exploreFleet(short, short[1], runs{}, now); !probe || o.workers != 4 {
		t.Errorf("a statement of 40 s: probes %v %s", probe, o)
	}
}
