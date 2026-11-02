package pgwire

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// backgroundReplica is a replica on an Iceberg catalog, with no workers,
// that runs heavy statements in the background, whatever took more than cpu
// of CPU being heavy; and a table of rows rows to read.
func backgroundReplica(t *testing.T, rows string, cpu time.Duration) (*viewReplica, *pgx.Conn, *Metrics) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("threads have priorities of their own only on Linux")
	}
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		o.HeavyInBackground = true
		o.Metrics = metrics
		o.QueryMemory = 1 << 40
	})
	r.srv.heavy.cpu, r.srv.heavy.factor = cpu, 0
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.visits AS SELECT range AS id, CAST(hash(range) % 1000000 AS BIGINT) AS grp, range % 97 AS v FROM range("+rows+")")
	return r, admin, metrics
}

// backgrounds counts the statements that ran in the second DuckDB.
func backgrounds(m *Metrics) float64 { return testutil.ToFloat64(m.backgrounds) }

// TestHeavyStatementsRunInBackground checks where statements run and that
// what they return does not change: a heavy one runs in the background from
// its second run on, in the session's settings and on its search path, and
// its threads run at the lower priority; one that a name in it might mean
// otherwise there, or that is tied to the session, stays in the first DuckDB;
// and a statement one DuckDB can read the other can, after a write.
func TestHeavyStatementsRunInBackground(t *testing.T) {
	t.Parallel()
	r, admin, m := backgroundReplica(t, "200000", time.Nanosecond)
	const q = "SELECT v, count(*) AS n, sum(id) AS s FROM lake.public.visits GROUP BY v ORDER BY v"
	first := sortedTexts(t, admin, q)
	if n := backgrounds(m); n != 0 {
		t.Fatalf("%v statements ran in the background on first sight", n)
	}
	if got := sortedTexts(t, admin, q); !slices.Equal(got, first) {
		t.Errorf("in the background: %q, want %q", got[:3], first[:3])
	}
	if n := backgrounds(m); n != 1 {
		t.Fatalf("%v statements ran in the background, want 1", n)
	}
	if n := niceThreads(t); n == 0 {
		t.Error("no thread of the process has a lower priority")
	}

	// Its history is as any statement's.
	if got := texts(t, admin, "SELECT count(*) > 0 FROM duckdb_settings()"); len(got) != 1 {
		t.Errorf("a statement after it: %q", got)
	}

	// With a parameter, the session's time zone and its search path.
	mustExec(t, admin, "SET TimeZone = 'Asia/Tokyo'")
	mustExec(t, admin, "SET search_path = 'lake.public'")
	const bare = "SELECT strftime(TIMESTAMPTZ '2026-01-01 00:00:00+00', '%H') AS h, count(*) AS n FROM visits GROUP BY 1"
	want := sortedTexts(t, admin, bare)
	before := backgrounds(m)
	if got := sortedTexts(t, admin, bare); !slices.Equal(got, want) || backgrounds(m) != before+1 {
		t.Errorf("in the session's settings: %q, want %q, %v more ran in the background", got, want, backgrounds(m)-before)
	}
	ctx := context.Background()
	var n int64
	const param = "SELECT count(*) FROM lake.public.visits WHERE v = $1"
	ext := r.admin(t, pgx.QueryExecModeExec)
	for range 2 {
		if err := ext.QueryRow(ctx, param, 5).Scan(&n); err != nil || n != 200000/97+1 {
			t.Errorf("with a parameter: %d, %v", n, err)
		}
	}

	// What a write commits, the other DuckDB reads.
	mustExec(t, admin, "INSERT INTO lake.public.visits VALUES (999999999, 1, 500)")
	if got := texts(t, admin, "SELECT v, count(*) FROM lake.public.visits WHERE v = 500 GROUP BY v"); len(got) != 1 || got[0] != "500|1" {
		t.Errorf("after a write: %q", got)
	}
	if got := texts(t, admin, "SELECT v, count(*) FROM lake.public.visits WHERE v = 500 GROUP BY v"); len(got) != 1 || got[0] != "500|1" {
		t.Errorf("after a write, in the background: %q", got)
	}

	// A function tied to the session stays in the first.
	const session = "SELECT current_setting('TimeZone'), count(*) FROM lake.public.visits"
	sortedTexts(t, admin, session)
	before = backgrounds(m)
	if got := texts(t, admin, session); len(got) != 1 || !strings.Contains(got[0], "Asia/Tokyo") || backgrounds(m) != before {
		t.Errorf("tied to the session: %q, %v ran in the background", got, backgrounds(m)-before)
	}

	// A temporary table of the name is what the name means, though the
	// catalog has a table of it.
	mustExec(t, admin, "CREATE TABLE lake.public.marks AS SELECT 96 AS v")
	own := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, own, "SET search_path = 'lake.public'")
	mustExec(t, own, "CREATE TEMP TABLE marks AS SELECT 7 AS v")
	const temp = "SELECT m.v FROM marks m JOIN lake.public.visits x ON x.v = m.v GROUP BY 1"
	before = backgrounds(m)
	for range 2 {
		if got := texts(t, own, temp); len(got) != 1 || got[0] != "7" {
			t.Errorf("a temporary table's name: %q", got)
		}
	}
	if n := backgrounds(m) - before; n != 0 {
		t.Errorf("%v statements that read a temporary table ran in the background", n)
	}

	// Writes run there once known to be heavy, and the first DuckDB reads
	// what they commit; a write is measured, in both, for the next like it.
	const copyTo = "CREATE OR REPLACE TABLE lake.public.visit_copy AS SELECT id, v FROM lake.public.visits WHERE id < 1000"
	mustExec(t, admin, copyTo)
	before = backgrounds(m)
	mustExec(t, admin, copyTo)
	if n := backgrounds(m) - before; n < 1 {
		t.Errorf("%v fills of CREATE TABLE ... AS ran in the background", n)
	}
	const inserted = "INSERT INTO lake.public.visit_copy SELECT id + 1000000, v FROM lake.public.visits WHERE v = 1"
	mustExec(t, admin, inserted)
	before = backgrounds(m)
	mustExec(t, admin, inserted)
	if n := backgrounds(m) - before; n != 1 {
		t.Errorf("%v INSERTs ran in the background", n)
	}
	const merged = "MERGE INTO lake.public.visit_copy t USING (SELECT id, v + 1000 AS v FROM lake.public.visits WHERE id < 10) s ON t.id = s.id " +
		"WHEN MATCHED THEN UPDATE SET v = s.v"
	mustExec(t, admin, merged)
	before = backgrounds(m)
	mustExec(t, admin, merged)
	if n := backgrounds(m) - before; n != 1 {
		t.Errorf("%v MERGEs ran in the background", n)
	}
	wantRows := []string{"1000|1009"}
	if got := texts(t, admin, "SELECT count(*) FILTER (WHERE id < 1000), max(v) FROM lake.public.visit_copy"); !slices.Equal(got, wantRows) {
		t.Errorf("what the writes left: %q, want %q", got, wantRows)
	}

	// A view of the catalog's reads the same there, once copied.
	mustExec(t, admin, "CREATE VIEW lake.public.visits_of_five AS SELECT id FROM lake.public.visits WHERE v = 5")
	if _, err := r.srv.lane.views.SyncViews(ctx); err != nil {
		t.Fatal(err)
	}
	const view = "SELECT count(*) FROM lake.public.visits_of_five"
	sawView := texts(t, admin, view)
	before = backgrounds(m)
	if got := texts(t, admin, view); !slices.Equal(got, sawView) || backgrounds(m) != before+1 {
		t.Errorf("a view: %q, want %q, %v ran in the background", got, sawView, backgrounds(m)-before)
	}

	// What the second DuckDB does not have, the first answers.
	const own2 = "SELECT count(*) >= 0 FROM igloo_comments(), lake.public.visits LIMIT 1"
	for range 2 {
		if _, err := admin.Exec(ctx, own2); err != nil && !strings.Contains(err.Error(), "igloo_comments") {
			t.Errorf("Igloo's own function: %v", err)
		}
	}

	// An erasure request's emptying of the caches reaches both.
	if err := r.eng.EmptyCache(ctx); err != nil {
		t.Errorf("emptying the caches: %v", err)
	}
}

// niceThreads counts the process's threads at a nice value above the
// default.
func niceThreads(t *testing.T) int {
	t.Helper()
	dirs, err := filepath.Glob("/proc/self/task/*/stat")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range dirs {
		b, err := os.ReadFile(d)
		if err != nil {
			continue
		}
		// After the name, in parentheses, the 17th field is the nice value.
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		if len(f) > 16 && f[16] != "0" {
			n++
		}
	}
	return n
}

// TestShortQueryBesideHeavyStatement checks, with no workers and no settings,
// that a short query beside a heavy statement that wants every core takes
// about what it takes alone: DuckDB's threads stay with a pipeline until it
// ends, so while they shared them the short query had its own thread and a
// share of a core. Alone, a heavy statement takes about what it does in the
// first DuckDB.
func TestShortQueryBesideHeavyStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("times statements of seconds")
	}
	r, admin, m := backgroundReplica(t, "20000000", 2*time.Second)
	// One long pipeline, in little memory.
	const heavy = "SELECT sum(hash(x.id + r.i)) FROM lake.public.visits x, range(50) r(i)"
	const short = "SELECT sum(i % 7) FROM range(30000000) t(i)"
	best := func(q string, n int) time.Duration {
		d := time.Hour
		for range n {
			start := time.Now()
			mustExec(t, admin, q)
			d = min(d, time.Since(start))
		}
		return d
	}
	// The history learns it is heavy, from a run in the first DuckDB.
	learned := best(heavy, 1)
	alone := best(heavy, 3)
	if backgrounds(m) == 0 {
		t.Fatalf("the heavy statement, of %s, did not run in the background", learned)
	}
	first := r.srv.heavy
	r.srv.heavy = nil
	inFirst := best(heavy, 3)
	r.srv.heavy = first
	if float64(alone) > 1.5*float64(inFirst) {
		t.Errorf("a heavy statement alone took %s in the background and %s in the first DuckDB", alone, inFirst)
	}
	idle := make([]time.Duration, 0, 7)
	for range 7 {
		start := time.Now()
		mustExec(t, admin, short)
		idle = append(idle, time.Since(start))
	}
	slices.Sort(idle)

	// Beside the heavy statement, over and over.
	other := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	besideHeavy := func() []time.Duration {
		var stop atomic.Bool
		heavyDone := make(chan error, 1)
		go func() {
			for !stop.Load() {
				if _, err := other.Exec(context.Background(), heavy); err != nil {
					heavyDone <- err
					return
				}
			}
			heavyDone <- nil
		}()
		time.Sleep(time.Second)
		beside := make([]time.Duration, 0, 7)
		for range 7 {
			start := time.Now()
			mustExec(t, admin, short)
			beside = append(beside, time.Since(start))
		}
		slices.Sort(beside)
		stop.Store(true)
		if err := <-heavyDone; err != nil {
			t.Fatal(err)
		}
		return beside
	}
	beside := besideHeavy()
	// What it was without the second DuckDB, for the log.
	heavies := r.srv.heavy
	r.srv.heavy = nil
	shared := besideHeavy()
	r.srv.heavy = heavies
	t.Logf("short query alone %s to %s, beside the heavy statement %s to %s (sharing DuckDB's threads %s to %s); the heavy statement alone %s, in the first DuckDB %s",
		idle[0].Round(time.Millisecond), idle[6].Round(time.Millisecond), beside[0].Round(time.Millisecond), beside[6].Round(time.Millisecond),
		shared[0].Round(time.Millisecond), shared[6].Round(time.Millisecond), alone.Round(time.Millisecond), inFirst.Round(time.Millisecond))
	// The median, against the slowest alone, with room for the heavy
	// statement's one thread at the default priority and its tenth of each
	// core.
	if limit := 2 * idle[6]; beside[3] > limit {
		t.Errorf("a short query took %s beside a heavy statement and %s to %s alone", beside[3], idle[0], idle[6])
	}
}

// TestHeavyHistory checks what makes a shape heavy. CPU as DuckDB measures it
// counts threads that wait: a dashboard's statement beside a report once measured
// ten times what it uses, which does not make it heavy, nor keep it so; and what is
// heavy goes by what else the replica runs.
func TestHeavyHistory(t *testing.T) {
	const line = 60 * time.Second
	h := newHeavyHistory()
	h.cpu = line

	// One far over the line in the first DuckDB is enough.
	h.observe("report", 700*time.Second, false)
	if !h.heavy("report") {
		t.Error("a report's first run did not make it heavy")
	}
	// It stays so, measured in the second, but for a run in the first now and then.
	first := 0
	for range heavyProbeEvery * 2 {
		background, mayMove := h.place("report")
		if !background {
			first++
			if !mayMove {
				t.Error("a report's run in the first DuckDB may not move")
			}
		}
		h.observe("report", 900*time.Second, true)
	}
	if first < 1 || first > 3 {
		t.Errorf("%d of %d runs in the first DuckDB", first, heavyProbeEvery*2)
	}
	if !h.heavy("report") {
		t.Error("a report stopped being heavy")
	}

	// One measure over the line in the second, as the first DuckDB's guard moved
	// it, says nothing yet: it runs in the first, and may move.
	h.observe("moved", 430*time.Second, true)
	if background, mayMove := h.place("moved"); h.heavy("moved") || background || !mayMove {
		t.Errorf("one run moved to the second: heavy %v, background %v, may move %v", h.heavy("moved"), background, mayMove)
	}
	// One that started again in the background starts there next time, once.
	h.moved("unseen")
	if background, _ := h.place("unseen"); !background {
		t.Error("the next run of one moved does not start in the second DuckDB")
	}
	if background, mayMove := h.place("unseen"); background || !mayMove {
		t.Errorf("the run after that: background %v, may move %v", background, mayMove)
	}
	// In the first it takes what it takes: light, and it stays where it is.
	h.observe("moved", 40*time.Second, false)
	h.observe("moved", 45*time.Second, false)
	if background, mayMove := h.place("moved"); h.heavy("moved") || background || mayMove {
		t.Errorf("light again: heavy %v, background %v, may move %v", h.heavy("moved"), background, mayMove)
	}

	// A light statement that one run measures far over the line stays light, and
	// is not moved for it.
	for range 3 {
		h.observe("tile", 40*time.Second, false)
	}
	h.observe("tile", 430*time.Second, false)
	if background, mayMove := h.place("tile"); h.heavy("tile") || background || mayMove {
		t.Errorf("a tile measured high once: heavy %v, background %v, may move %v", h.heavy("tile"), background, mayMove)
	}
	// A shape that all its latest runs measure over the line is heavy, and one that
	// comes back under it is not: its run in the first DuckDB says so.
	for range heavyRuns {
		h.observe("tile", 400*time.Second, true)
	}
	if !h.heavy("tile") {
		t.Error("a shape over the line in all its latest runs is not heavy")
	}
	h.observe("tile", 40*time.Second, false)
	if h.heavy("tile") {
		t.Error("a shape that took 40 s in the first DuckDB stayed heavy")
	}
	// What is heavy goes by what else runs: over the least line, but not many
	// times what the others use.
	h = newHeavyHistory()
	h.cpu = line
	for range heavyRuns {
		for _, tile := range []string{"a", "b", "c"} {
			h.observe(tile, 80*time.Second, false)
		}
	}
	for _, tile := range []string{"a", "b", "c"} {
		if h.heavy(tile) {
			t.Errorf("%s is heavy beside what uses as much", tile)
		}
	}
	for range heavyRuns {
		h.observe("big", 300*time.Second, false)
	}
	if h.heavy("big") {
		t.Error("300 s is heavy beside statements of 80 s")
	}
	for range heavyRuns {
		h.observe("report", 1000*time.Second, false)
	}
	if !h.heavy("report") {
		t.Error("1000 s is not heavy beside statements of 80 s")
	}
	// What ran long ago says nothing of it.
	at := time.Now()
	h.now = func() time.Time { return at }
	at = at.Add(heavyRecent + time.Minute)
	for range heavyRuns {
		h.observe("big", 300*time.Second, false)
	}
	if !h.heavy("big") {
		t.Error("300 s is not heavy once the statements of 80 s have stopped")
	}
	// Small statements say nothing of it.
	h = newHeavyHistory()
	h.cpu = line
	for i := range 20 {
		h.observe(string(rune('a'+i)), time.Second, false)
	}
	h.observe("report", 1000*time.Second, false)
	if !h.heavy("report") {
		t.Error("a report is not heavy beside small statements")
	}
}

// TestMostOfSize checks when what runs beside a heavy statement gives it nothing
// to give way to: most of it known to take as much as makes a statement heavy at
// the least, or running long enough to have used that much.
func TestMostOfSize(t *testing.T) {
	t.Parallel()
	h := newHeavyHistory()
	h.after = 5 * time.Second
	short := aStatement{expect: time.Second, ran: 2 * time.Second}
	unseen := aStatement{ran: 2 * time.Second}
	sized := aStatement{expect: 30 * time.Second, ran: time.Second}
	long := aStatement{ran: time.Minute}
	for _, c := range []struct {
		beside []aStatement
		want   bool
	}{
		{nil, false},
		{[]aStatement{short}, false},
		{[]aStatement{unseen, unseen, unseen}, false},
		{[]aStatement{sized}, true},
		{[]aStatement{long}, true},
		{[]aStatement{sized, short}, false},
		{[]aStatement{sized, long, short}, true},
		{[]aStatement{sized, short, short, short, long}, false},
		{[]aStatement{sized, sized, sized, sized, long, long, short}, true},
	} {
		if got := h.mostOfSize(c.beside); got != c.want {
			t.Errorf("beside %v: %v, want %v", c.beside, got, c.want)
		}
	}
}

// TestHeavyStaysBesideHeavy checks that a heavy statement stays in the first DuckDB
// beside a statement of some size, and goes to the second once that has ended.
func TestHeavyStaysBesideHeavy(t *testing.T) {
	r, admin, m := backgroundReplica(t, "200000", time.Nanosecond)
	const q = "SELECT v, count(*) AS n, sum(id) AS s FROM lake.public.visits GROUP BY v ORDER BY v"
	want := sortedTexts(t, admin, q)
	if got := sortedTexts(t, admin, q); !slices.Equal(got, want) || backgrounds(m) != 1 {
		t.Fatalf("alone: %q, %v ran in the background", got[:1], backgrounds(m))
	}
	// Whatever runs is of some size.
	after := r.srv.heavy.after
	r.srv.heavy.after = 0
	defer func() { r.srv.heavy.after = after }()
	other := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = other.Exec(ctx, "SELECT count(*) FROM range(100000000000) x WHERE hash(x.range) = 1")
	}()
	deadline := time.Now().Add(20 * time.Second)
	for !r.srv.besideIsHeavy(nil) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sortedTexts(t, admin, q); !slices.Equal(got, want) {
		t.Errorf("beside it: %q", got[:1])
	}
	if n := backgrounds(m); n != 1 {
		t.Errorf("%v ran in the background, beside a statement of some size", n)
	}
	cancel()
	<-done
	for r.srv.besideIsHeavy(nil) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sortedTexts(t, admin, q); !slices.Equal(got, want) {
		t.Errorf("after it: %q", got[:1])
	}
	if n := backgrounds(m); n != 2 {
		t.Errorf("%v ran in the background, with nothing beside it", n)
	}
}
