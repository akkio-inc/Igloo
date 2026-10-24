package pgwire

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
)

func TestBitmapEligibility(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		query string
		want  bool
	}{
		{`SELECT k, count(DISTINCT CAST(t._igloo_id_person_id AS UINTEGER)) FROM t GROUP BY k`, true},
		{`SELECT COUNT( distinct cast("T"."_igloo_id_person_id" as uinteger) ) FROM lake.public.t AS "T"`, true},
		// as the pair-count rewrite leaves it: the cast in the inner select
		{`SELECT _igloo_g1 AS k, count(DISTINCT _igloo_d1) AS people FROM (SELECT k AS _igloo_g1,
			CAST(m._igloo_id_person_id AS UINTEGER) AS _igloo_d1, d AS _igloo_d2 FROM m GROUP BY k, _igloo_d1, _igloo_d2) AS _igloo_pairs GROUP BY _igloo_g1`, true},
		{`SELECT count(DISTINCT person_id) FROM t`, false},
		{`SELECT count(DISTINCT CAST(_igloo_id_person_id AS BIGINT)) FROM t`, false},
		{`SELECT sum(CAST(_igloo_id_person_id AS UINTEGER)) FROM t`, false},
		{`SELECT count(DISTINCT CAST(person AS UINTEGER)) FROM t`, false},
	} {
		got := countsDistinct.MatchString(c.query) && domainID.MatchString(c.query)
		if got != c.want {
			t.Errorf("%s: eligible %v, want %v", c.query, got, c.want)
		}
	}
}

func TestTableRows(t *testing.T) {
	t.Parallel()
	md := &catalog.Metadata{Tables: []catalog.TableMeta{
		{Schema: "public", Name: "media", Rows: 36_000_000},
		{Schema: "public", Name: "AUDIENCE_MEMBERS", Rows: 141_000_000},
		{Schema: "public", Name: "v", View: true, Rows: 5},
		{Schema: "public", Name: "unknown", Rows: -1},
		{Schema: "public", Name: "elsewhere", Catalog: "pg", Rows: 7},
	}}
	tables := func(names ...string) []auth.Object {
		var out []auth.Object
		for _, n := range names {
			out = append(out, auth.Object{Schema: "PUBLIC", Table: n})
		}
		return out
	}
	if n, ok := tableRows(md, tables("media", "audience_members", "v", "missing")); !ok || n != 177_000_000 {
		t.Errorf("rows %d, %v", n, ok)
	}
	if _, ok := tableRows(md, tables("unknown", "elsewhere")); ok {
		t.Error("rows known for tables the metadata does not count")
	}
	if _, ok := tableRows(nil, tables("media")); ok {
		t.Error("rows known without metadata")
	}
}

// lockedBuffer is a buffer the server's goroutines may log to.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// personID stands in tests' statements for the column of an ID domain's
// ids, which no statement a user writes may name; Igloo writes it in
// (withIDColumns), as it will in counts through the domain.
const personID = "person_id_column"

// withIDColumns makes Igloo write the column of ids in place of personID.
func withIDColumns(s *Server) {
	s.rewriteHook = func(q string) string { return strings.ReplaceAll(q, personID, "_igloo_id_person") }
}

// bitmapServer starts a server on an engine with the rb extension, which
// CI builds (IGLOO_TEST_RB_EXTENSION), logging to the returned buffer.
func bitmapServer(t *testing.T, configure ...func(*Options)) (*testServer, *lockedBuffer) {
	t.Helper()
	if os.Getenv("IGLOO_TEST_RB_EXTENSION") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	eng, err := engine.Open(context.Background(), config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if !eng.BitmapsLoaded() {
		t.Fatal("the engine did not load the rb extension")
	}
	logs := &lockedBuffer{}
	ts := startServerLogged(t, eng, slog.New(slog.NewTextHandler(logs, nil)), configure...)
	withIDColumns(ts.srv)
	return ts, logs
}

// countPeople counts distinct IDs by group as Igloo writes counts through ID
// domains: 2 million IDs spread over UINTEGER in 4 groups, whose bitmaps
// take far more than 4 MB.
const countPeople = `SELECT g, count(DISTINCT CAST(` + personID + ` AS UINTEGER)) AS n
	FROM (SELECT i % 4 AS g, hash(i) % 4294967295 AS ` + personID + ` FROM range(2000000) r(i)) AS t GROUP BY g ORDER BY g`

// TestBitmapsRerunOnTheHash runs a statement whose bitmaps outgrow
// rb_memory_budget: Igloo recognizes the error, logs it, counts it and runs
// the statement once more with bitmaps off, through both protocols, and a
// statement that fails the same way again is not run a third time.
func TestBitmapsRerunOnTheHash(t *testing.T) {
	t.Parallel()
	ts, logs := bitmapServer(t, func(o *Options) { o.Bitmaps, o.BitmapMinRows = true, 0 })
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec, pgx.QueryExecModeCacheStatement} {
		t.Run(mode.String(), func(t *testing.T) {
			conn := ts.admin(t, mode)
			want := texts(t, conn, strings.Replace(countPeople, "CAST("+personID+" AS UINTEGER)", personID, 1))

			// It fits: bitmaps count it, and the setting is back as it was.
			plan := strings.Join(texts(t, conn, "EXPLAIN "+countPeople), "\n")
			if !strings.Contains(plan, "rb_count_distinct") {
				t.Errorf("the plan does not count in bitmaps:\n%s", plan)
			}
			if got := texts(t, conn, "SELECT current_setting('rb_count_distinct')"); len(got) != 1 || got[0] != "off" {
				t.Errorf("rb_count_distinct after the statement: %q", got)
			}

			mustExec(t, conn, "SET rb_memory_budget = '4MB'")
			before := ts.srv.bitmapReruns.Load()
			got := texts(t, conn, countPeople)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("rows %q, want %q", got, want)
			}
			if n := ts.srv.bitmapReruns.Load() - before; n != 1 {
				t.Errorf("%d reruns, want 1", n)
			}
			// Prepared once and run again, as clients reuse statements:
			// the hash from then on.
			if mode == pgx.QueryExecModeCacheStatement {
				if got := texts(t, conn, countPeople); strings.Join(got, ",") != strings.Join(want, ",") {
					t.Errorf("rows of the cached statement %q, want %q", got, want)
				}
				if n := ts.srv.bitmapReruns.Load() - before; n != 1 {
					t.Errorf("%d reruns after running the cached statement again, want 1", n)
				}
			}

			// Bitmaps the statement asks for itself fail again: one rerun,
			// then the error.
			before = ts.srv.bitmapReruns.Load()
			_, err := conn.Exec(context.Background(), strings.Replace(countPeople, " AS n", " AS n, rb_count_distinct(CAST("+personID+" AS UINTEGER)) AS m", 1))
			if err == nil || !strings.Contains(err.Error(), bitmapBudgetError) {
				t.Errorf("a statement whose own bitmaps do not fit: %v", err)
			}
			if n := ts.srv.bitmapReruns.Load() - before; n != 1 {
				t.Errorf("%d reruns of a statement that failed again, want 1", n)
			}
			mustExec(t, conn, "RESET rb_memory_budget")
			if got := texts(t, conn, "SELECT current_setting('rb_count_distinct')"); len(got) != 1 || got[0] != "off" {
				t.Errorf("rb_count_distinct after the reruns: %q", got)
			}
		})
	}
	if !strings.Contains(logs.String(), "bitmaps did not fit in memory") {
		t.Errorf("no log of the reruns:\n%s", logs)
	}
}

// TestBitmapsNeedRows leaves statements to the hash when Igloo cannot tell
// that they read enough rows, and when bitmaps are off.
func TestBitmapsNeedRows(t *testing.T) {
	t.Parallel()
	for name, configure := range map[string]func(*Options){
		"threshold":  func(o *Options) { o.Bitmaps, o.BitmapMinRows = true, 1_000_000 },
		"off":        func(o *Options) { o.Bitmaps, o.BitmapMinRows = false, 0 },
		"no setting": func(*Options) {},
	} {
		t.Run(name, func(t *testing.T) {
			ts, _ := bitmapServer(t, configure)
			conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
			plan := strings.Join(texts(t, conn, "EXPLAIN "+countPeople), "\n")
			if strings.Contains(plan, "rb_count_distinct") || !strings.Contains(plan, "count(DISTINCT") {
				t.Errorf("the plan counts in bitmaps:\n%s", plan)
			}
		})
	}
}

// TestBitmapsByTableRows counts in bitmaps the statements that read at least
// BitmapMinRows rows of the catalog's tables, as its metadata counts them.
func TestBitmapsByTableRows(t *testing.T) {
	t.Parallel()
	if os.Getenv("IGLOO_TEST_RB_EXTENSION") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	vc := newViewCatalog(t)
	query := "SELECT k, count(DISTINCT CAST(v." + personID + " AS UINTEGER)) AS n FROM lake.public.visits AS v GROUP BY k ORDER BY k"
	for _, c := range []struct {
		min  int64
		want bool
	}{{150_000, true}, {250_000, false}} {
		r := vc.replica(t, func(o *Options) { o.Bitmaps, o.BitmapMinRows = true, c.min })
		withIDColumns(r.srv)
		admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
		mustExec(t, admin, "CREATE TABLE IF NOT EXISTS lake.public.visits AS SELECT i % 7 AS k, i AS "+personID+" FROM range(200000) r(i)")
		plan := strings.Join(texts(t, admin, "EXPLAIN "+query), "\n")
		if got := strings.Contains(plan, "rb_count_distinct"); got != c.want {
			t.Errorf("at least %d rows: bitmaps %v, want %v:\n%s", c.min, got, c.want, plan)
		}
		if got := texts(t, admin, query); len(got) != 7 || got[0] != "0|28572" {
			t.Errorf("at least %d rows: counts %q", c.min, got)
		}
	}
}

// TestDistributedBitmapsRerunOnTheHash runs a count on workers whose tasks'
// bitmaps outgrow a tiny rb_memory_budget, which the session's setting
// carries to them: the statement runs once more counting with DuckDB's
// hash, on workers, with no smaller tasks in between and no run here, and
// the rows are right. A plain statement runs on workers that load no rb
// extension while this replica has it.
func TestDistributedBitmapsRerunOnTheHash(t *testing.T) {
	t.Parallel()
	rb := os.Getenv("IGLOO_TEST_RB_EXTENSION")
	if rb == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("IGLOO_TEST_RB_EXTENSION is not set: CI builds the rb extension (extensions/rb/build.sh)")
		}
		t.Skip("IGLOO_TEST_RB_EXTENSION is not set; build the rb extension with extensions/rb/build.sh")
	}
	reg := prometheus.NewRegistry()
	ds := startDistServerRB(t, t.TempDir(), 2, rb, func(o *distributed.Options) { o.Metrics = distributed.NewMetrics(reg) },
		func(o *Options) { o.Bitmaps, o.BitmapMinRows = true, 0 })
	// outcomes counts the statements whose parts ran on workers, by outcome.
	outcomes := func() map[string]float64 {
		t.Helper()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, mf := range mfs {
			if mf.GetName() != "igloo_distributed_queries_total" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "outcome" {
						out[l.GetValue()] = m.GetCounter().GetValue()
					}
				}
			}
		}
		return out
	}
	withIDColumns(ds.srv)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE lake.public.visits AS SELECT i % 4 AS g, hash(i) % 4294967295 AS "+personID+" FROM range(2000000) r(i)")
	query := "SELECT g, count(DISTINCT CAST(v." + personID + " AS UINTEGER)) AS n FROM lake.public.visits AS v GROUP BY g ORDER BY g"
	want := texts(t, conn, "SELECT g, count(DISTINCT v."+personID+") AS n FROM lake.public.visits AS v GROUP BY g ORDER BY g")
	if len(want) != 4 {
		t.Fatalf("expected counts: %q", want)
	}

	var mu sync.Mutex
	local := 0
	ds.srv.failLocal = func(_ context.Context, st *statement) error {
		if strings.Contains(st.query, "visits") {
			mu.Lock()
			local++
			mu.Unlock()
		}
		return nil
	}
	defer func() { ds.srv.failLocal = nil }()
	mustExec(t, conn, "SET distributed_execution = on")

	// Within the budget: bitmaps on workers.
	if got := texts(t, conn, query); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows %q, want %q", got, want)
	}
	if n := ds.srv.bitmapReruns.Load(); n != 0 {
		t.Errorf("%d reruns within the budget", n)
	}

	// Past it on the workers, in tasks of any size: once more, with the
	// hash.
	mustExec(t, conn, "SET rb_memory_budget = '64KB'")
	if got := texts(t, conn, query); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows after the workers' bitmaps did not fit %q, want %q", got, want)
	}
	if n := ds.srv.bitmapReruns.Load(); n != 1 {
		t.Errorf("%d reruns, want 1", n)
	}
	// The workers' tasks failed it, at once, in tasks of the first size:
	// one statement on workers ran out of bitmap memory, and two ran.
	if got := outcomes(); got["bitmap_memory"] != 1 || got["ok"] != 2 || got["out_of_memory"] != 0 {
		t.Errorf("statements on workers by outcome: %v", got)
	}
	if q := lastRun(t, ds.history, query); q.Workers == 0 || q.Outcome != audit.OutcomeOK {
		t.Errorf("history: %d workers, %s %s", q.Workers, q.Outcome, q.SQLState)
	}
	mu.Lock()
	if local != 0 {
		t.Errorf("ran here %d times", local)
	}
	mu.Unlock()
	if got := texts(t, conn, "SELECT current_setting('rb_count_distinct')"); len(got) != 1 || got[0] != "off" {
		t.Errorf("rb_count_distinct after the rerun: %q", got)
	}
}

// TestDistributedWithoutRBOnWorkers runs statements on workers that load no
// rb extension while this replica does: the rb settings, unchanged, are not
// sent to them.
func TestDistributedWithoutRBOnWorkers(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET distributed_execution = on")
	query := "SELECT amount, count(*) AS n FROM lake.public.orders GROUP BY amount ORDER BY amount"
	if got := texts(t, conn, query); len(got) != 9 {
		t.Errorf("rows %q", got)
	}
	if q := lastRun(t, ds.history, query); q.Workers == 0 || q.Outcome != audit.OutcomeOK {
		t.Errorf("history: %d workers, %s %s", q.Workers, q.Outcome, q.SQLState)
	}
}
