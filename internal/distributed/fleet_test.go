package distributed

import (
	"fmt"
	"math"
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
// TestScaleReports). E1-like at ten times the data reads 1.9 GB and goes
// through some 3e10 rows in 3 stages: estimated at 13 s on one machine, it
// took 58 to 97 s there, and 50 s on 16 workers.
var (
	e1At10x = features{bytes: 1.9e9, exchanged: 1.9e10, held: 1.9e9, rows: 3.1e10, levels: 2, stages: 3}
	f1At100 = features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 5.83e+10, levels: 3, stages: 22}
	f2At100 = features{bytes: 1.186e+11, exchanged: 5.683e+11, held: 1.186e+11, rows: 2.92e+10, levels: 3, stages: 11}
	f4At10x = features{bytes: 4.934e+09, exchanged: 3.491e+10, held: 4.934e+09, rows: 7.87e+09, levels: 7, stages: 26}
	f5At10x = features{bytes: 8.144e+09, exchanged: 3.812e+10, held: 8.144e+09, rows: 1.01e+10, levels: 7, stages: 33}
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

// TestFleetChoice checks how many workers, or the large worker, auto mode
// takes for the scale runs' reports, and that one machine keeps those it
// runs faster.
func TestFleetChoice(t *testing.T) {
	now := time.Now()
	decide := func(x features, f fleet, r runs, found bool) (estimate, bool, string) {
		t.Helper()
		e := x.estimate(scaleCoordinator, f)
		if found {
			e.pick(r, f)
		}
		onWorkers, _, reason := choose(e, r, found, DefaultThreshold, now)
		return e, onWorkers, reason
	}

	// E1-like at 10x: on 16 workers running, 4 are within a fifth of the
	// fastest at a quarter of the worker-seconds; with workers stopped,
	// their start and idle tail count too, and 4 are still within a fifth.
	e, _, _ := decide(e1At10x, scaleWorkers, runs{}, false)
	if e.workers != 4 || e.large || !strings.Contains(e.fleet, "within a fifth of the fastest, 16 workers") {
		t.Errorf("E1-like at 10x, 16 running: %d workers (%s)", e.workers, e.fleet)
	}
	stopped := scaleWorkers
	stopped.running = 0
	if e, _, _ := decide(e1At10x, stopped, runs{}, false); e.workers != 4 || !strings.Contains(e.fleet, "for 26% of its cost") {
		t.Errorf("E1-like at 10x, workers stopped: %d workers (%s)", e.workers, e.fleet)
	}
	// Its first run, estimated at 13 s here, stays here (the guard moves
	// it, see the server's guardAfter); after 97 s here, it runs on 4.
	if _, onWorkers, reason := decide(e1At10x, scaleWorkers, runs{}, false); onWorkers {
		t.Errorf("E1-like at 10x, first: on workers (%s)", reason)
	}
	e, onWorkers, reason := decide(e1At10x, scaleWorkers, runs{local: 97}, true)
	if !onWorkers || e.workers != 4 {
		t.Errorf("E1-like at 10x after 97 s here: workers %v on %d (%s; %s)", onWorkers, e.workers, reason, e.fleet)
	}
	// After 50 s on 16 workers, 4 are estimated at 59 s, within a fifth of
	// 50: it stays on 4, and after 55 s there, too.
	r := runs{local: 97, dist: 50, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 50}}
	if e, onWorkers, reason := decide(e1At10x, scaleWorkers, r, true); !onWorkers || e.workers != 4 || !strings.Contains(describeOptions(e.options, e.dollars), "(estimated") {
		t.Errorf("E1-like at 10x after 50 s on 16: workers %v on %d (%s; %s)", onWorkers, e.workers, reason, e.fleet)
	}
	r.fleets[fleetBucket(4)] = 55
	if e, _, _ := decide(e1At10x, scaleWorkers, r, true); e.workers != 4 || !strings.Contains(e.fleet, "as it took before") {
		t.Errorf("E1-like at 10x after 55 s on 4: %d workers (%s)", e.workers, e.fleet)
	}
	// Far slower on 4 than its estimate: 8 then, which the run on 16
	// corrects to 53 s.
	r.fleets[fleetBucket(4)] = 90
	if e, _, _ := decide(e1At10x, scaleWorkers, r, true); e.workers != 8 {
		t.Errorf("E1-like at 10x after 90 s on 4: %d workers (%s)", e.workers, e.fleet)
	}

	// F1-like and F2-like at 100x: all 16, whose stages' reading dwarfs
	// their start; one machine would run out of memory.
	for name, x := range map[string]features{"F1-like at 100x": f1At100, "F2-like at 100x": f2At100} {
		e, onWorkers, reason := decide(x, scaleWorkers, runs{}, false)
		if !onWorkers || e.workers != 16 || e.large {
			t.Errorf("%s: workers %v on %d (%s; %s)", name, onWorkers, e.workers, reason, e.fleet)
		}
	}
	// E2-like at 100x: out of memory on one machine.
	if e, _, _ := decide(e2At100, scaleWorkers, runs{}, false); !e.local.oom {
		t.Errorf("E2-like at 100x: one machine %s", e.local)
	}

	// F4-like and F5-like at 10x: one machine, 48 s and 15 s there against
	// 228 s and 159 s on 16 workers.
	for name, x := range map[string]features{"F4-like at 10x": f4At10x, "F5-like at 10x": f5At10x} {
		if _, onWorkers, reason := decide(x, scaleWorkers, runs{}, false); onWorkers {
			t.Errorf("%s: on workers (%s)", name, reason)
		}
	}

	// F4-like at 100x fits the large worker of 440 GiB: faster than 16
	// workers and cheaper, whether all are stopped or running.
	withLarge := stopped
	withLarge.large = &scaleLarge
	e, onWorkers, reason = decide(f4At100, withLarge, runs{}, false)
	if !onWorkers || !e.large || e.workers != 1 || e.options[len(e.options)-1].spill.factor != 1 || !strings.Contains(e.fleet, "the large worker") {
		t.Errorf("F4-like at 100x, all stopped: workers %v, large %v (%s; %s)", onWorkers, e.large, reason, e.fleet)
	}
	running := scaleWorkers
	l := scaleLarge
	l.running = true
	running.large = &l
	if e, _, _ := decide(f4At100, running, runs{}, false); !e.large {
		t.Errorf("F4-like at 100x, all running: %d workers (%s)", e.workers, e.fleet)
	}
	// At four times the price, still far faster than 16 workers: the large
	// worker, whatever it costs, as no way within a fifth of it is.
	pricey := l
	pricey.price = 4 * l.price
	running.large = &pricey
	if e, _, _ := decide(f4At100, running, runs{}, false); !e.large {
		t.Errorf("F4-like at 100x, the large worker at four times the price: %d workers (%s)", e.workers, e.fleet)
	}
	// A statement the large worker runs in 87 s and 16 workers in 103 s,
	// within a fifth: the large worker at its price, $0.11 against $0.22;
	// at four times it, 16 workers.
	mid := features{bytes: 4e10, held: 1e10, rows: 1e10, levels: 3, stages: 10}
	running.large = &l
	if e, _, _ := decide(mid, running, runs{}, false); !e.large || !strings.Contains(e.fleet, "the fastest, for 48% of the cost of 16 workers") {
		t.Errorf("within a fifth, the large worker cheaper: %d workers, large %v (%s)", e.workers, e.large, e.fleet)
	}
	running.large = &pricey
	if e, _, _ := decide(mid, running, runs{}, false); e.large || e.workers != 16 {
		t.Errorf("within a fifth, the large worker dearer: large %v on %d (%s)", e.large, e.workers, e.fleet)
	}
	// F1-like at 100x spills on it, 1.5 times as long: 16 workers.
	if e, _, _ := decide(f1At100, withLarge, runs{}, false); e.large || e.workers != 16 {
		t.Errorf("F1-like at 100x with the large worker: large %v on %d (%s)", e.large, e.workers, e.fleet)
	}
	// E2-like at 100x, which runs out of memory on one machine of 49.3
	// GiB, spills on the large worker, 1.6 times as long: 16 workers.
	if e, _, _ := decide(e2At100, withLarge, runs{}, false); e.large || e.options[len(e.options)-1].spill.factor < 1.5 {
		t.Errorf("E2-like at 100x with the large worker: large %v (%s)", e.large, e.fleet)
	}
	// What it took on the large worker corrects the large worker alone:
	// after 900 s there and 700 s on 16 workers, the fleet.
	r = runs{large: 900, dist: 700, distWorkers: 16, fleets: map[int]float64{fleetBucket(16): 700}}
	if e, _, _ := decide(f4At100, withLarge, r, true); e.large || e.workers < 8 {
		t.Errorf("F4-like at 100x after 900 s on the large worker and 700 s on 16: large %v on %d (%s)", e.large, e.workers, e.fleet)
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
