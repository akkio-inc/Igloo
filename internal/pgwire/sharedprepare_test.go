package pgwire

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// TestSharedPrepares checks which statements are prepared under the shared
// catalog lock: queries, by every role that may read, through either
// protocol, with their rows; not other statements, nor queries in a
// transaction. A query naming a table that does not exist has DuckDB's
// suggestion of similar names refused under the shared lock, and is
// prepared again under the exclusive one, which gives the suggestion.
func TestSharedPrepares(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) { o.Metrics = metrics })
	ctx := context.Background()
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.orders AS SELECT i AS id, i % 3 AS region FROM range(10) r(i)")
	mustExec(t, admin, "CREATE VIEW lake.public.by_region AS SELECT region, count(*) AS n FROM lake.public.orders GROUP BY region")
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	count := func(lock string) float64 { return testutil.ToFloat64(metrics.prepares.WithLabelValues(lock)) }

	for _, c := range []struct {
		name string
		conn *pgx.Conn
	}{
		{"admin", admin},
		{"admin, extended protocol", r.admin(t, pgx.QueryExecModeExec)},
		{"writer", r.dbt(t)},
		{"reader", connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)},
	} {
		shared, refusals := count("shared"), r.eng.SharedRefusals()
		if got := rawTexts(t, c.conn, "SELECT region, n FROM lake.public.by_region ORDER BY region"); !slices.Equal(got, []string{"0|4", "1|3", "2|3"}) {
			t.Errorf("%s: the view gave %q", c.name, got)
		}
		if got := rawTexts(t, c.conn, "WITH o AS (SELECT * FROM orders) SELECT count(*) FROM o"); !slices.Equal(got, []string{"10"}) {
			t.Errorf("%s: the CTE gave %q", c.name, got)
		}
		if n := count("shared") - shared; n != 2 || r.eng.SharedRefusals() != refusals {
			t.Errorf("%s: %v queries prepared shared, %d refusals, want 2 and none", c.name, n,
				r.eng.SharedRefusals()-refusals)
		}
	}

	retried, refusals := count("retried"), r.eng.SharedRefusals()
	if _, err := admin.Exec(ctx, "SELECT * FROM lake.public.ordrs"); err == nil || !strings.Contains(err.Error(), `"orders"`) {
		t.Errorf("a missing table: %v, want DuckDB's suggestion of orders", err)
	}
	if count("retried") != retried+1 || r.eng.SharedRefusals() == refusals {
		t.Errorf("a missing table: %v retried, %d refusals, want 1 and some", count("retried")-retried,
			r.eng.SharedRefusals()-refusals)
	}

	shared := count("shared")
	mustExec(t, admin, "INSERT INTO lake.public.orders VALUES (10, 1)")
	mustExec(t, admin, "BEGIN")
	if got := rawTexts(t, admin, "SELECT count(*) FROM lake.public.orders"); !slices.Equal(got, []string{"11"}) {
		t.Errorf("in a transaction: %q", got)
	}
	mustExec(t, admin, "COMMIT")
	rawTexts(t, admin, "SELECT count(*) FROM information_schema.tables")
	if count("shared") != shared {
		t.Errorf("%v statements other than queries outside a transaction prepared shared", count("shared")-shared)
	}

	// IGLOO_SHARED_PREPARES=false.
	off := NewMetrics(prometheus.NewRegistry())
	alone := vc.replica(t, func(o *Options) { o.Metrics, o.ExclusivePrepares = off, true })
	rawTexts(t, alone.admin(t, pgx.QueryExecModeSimpleProtocol), "SELECT count(*) FROM lake.public.orders")
	if n := testutil.ToFloat64(off.prepares.WithLabelValues("shared")); n != 0 {
		t.Errorf("%v queries prepared shared with shared prepares off", n)
	}
}

// TestSharedHoldRefusesWalks checks the guard shared prepares rely on, in
// DuckDB itself: while the catalog lock is held shared, a listing of the
// catalog's tables, a missing table's suggestion of similar names and a
// drop fail with the catalog proxy's refusal, rather than walk
// DuckDB-Iceberg's set of tables or erase from it; a table still loads, and
// the rest go through as usual once the lock is let go of.
func TestSharedHoldRefusesWalks(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t)
	mustExec(t, r.admin(t, pgx.QueryExecModeSimpleProtocol), "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, r.admin(t, pgx.QueryExecModeSimpleProtocol), "CREATE TABLE lake.public.kept AS SELECT 1 AS n")
	ctx := context.Background()
	conn, err := r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	exec := func(q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	walks := []string{
		"SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'",
		"SELECT * FROM lake.public.kep",
		"DROP TABLE lake.public.kept",
	}

	release := r.eng.CatalogShare()
	refusals := r.eng.SharedRefusals()
	for _, q := range walks {
		if err := exec(q); err == nil || !strings.Contains(err.Error(), "423") {
			t.Errorf("%s under the shared lock: %v, want the proxy's refusal", q, err)
		}
	}
	if err := exec("SELECT n FROM lake.public.kept"); err != nil {
		t.Errorf("loading a table under the shared lock: %v", err)
	}
	if n := r.eng.SharedRefusals() - refusals; n < uint64(len(walks)) {
		t.Errorf("%d refusals, want at least %d", n, len(walks))
	}
	release()

	if err := exec(walks[0]); err != nil {
		t.Errorf("listing: %v", err)
	}
	if err := exec(walks[1]); err == nil || !strings.Contains(err.Error(), `"kept"`) {
		t.Errorf("a missing table: %v, want DuckDB's suggestion of kept", err)
	}
	if err := exec(walks[2]); err != nil {
		t.Errorf("dropping: %v", err)
	}
}

// TestListingSubqueriesHoldTheCatalog checks that a query whose listing is a
// subquery, SELECT ... FROM (SHOW TABLES ...) or (DESCRIBE ...), holds the
// catalog lock exclusively, as SHOW and DESCRIBE statements do (see
// listsCatalog), while other holders take its shared side over and over, as
// other sessions' binds do: it waits its turn, rather than list alongside
// them and have the listing refused.
func TestListingSubqueriesHoldTheCatalog(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t)
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.crm")
	mustExec(t, admin, "CREATE TABLE lake.crm.visits AS SELECT 1 AS id")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			release := r.eng.CatalogShare()
			time.Sleep(20 * time.Millisecond)
			release()
		}
	})
	for q, want := range map[string]string{
		"SELECT name FROM (SHOW TABLES FROM lake.crm) ORDER BY 1": "visits",
		"SELECT column_name FROM (DESCRIBE lake.crm.visits)":      "id",
	} {
		for range 5 {
			if got := rawTexts(t, admin, q); !slices.Equal(got, []string{want}) {
				t.Errorf("%s: %q, want %q", q, got, want)
			}
		}
	}
	close(stop)
	wg.Wait()
}

// sharedPrepareChildEnv, when set, makes TestSharedPreparesSurviveConcurrentDDL
// run the stress itself rather than spawn a subprocess to.
const sharedPrepareChildEnv = "IGLOO_SHARED_PREPARE_CHILD"

// TestSharedPreparesSurviveConcurrentDDL runs sharedPrepareStress in a
// subprocess, as TestCatalogListingSurvivesConcurrentDDL does, so that a
// crash or a data race (under -race) fails this test rather than taking the
// test binary down.
func TestSharedPreparesSurviveConcurrentDDL(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	if os.Getenv(sharedPrepareChildEnv) != "" {
		sharedPrepareStress(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSharedPreparesSurviveConcurrentDDL$", "-test.v", "-test.timeout=4m")
	cmd.Env = append(os.Environ(), sharedPrepareChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("shared prepares alongside listings and table swaps did not exit cleanly: %v\n%s", err, out)
	}
}

// sharedPrepareStress prepares queries alongside one another, each new to the
// replica, by an admin and a reader through both protocols, over tables that
// stay put: every one must succeed. Alongside them, readers list the catalog
// and read the table a writer swaps dbt-style (rename and drop), a table
// that does not exist, and tables another engine drops behind the replica's
// back, which may fail but must not crash it; and the background view sync
// polls. Queries must have been prepared shared, and some prepared again.
func sharedPrepareStress(t *testing.T) {
	const builds = 60
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) { o.Metrics = metrics })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	writer := r.dbt(t)
	mustExec(t, writer, `create schema if not exists "lake"."analytics"`)
	for i := range 8 {
		mustExec(t, writer, fmt.Sprintf(`create table "lake"."analytics"."other_%d" as select %d as run`, i, i))
	}
	mustExec(t, writer, `create view "lake"."analytics"."v" as select run from "lake"."analytics"."other_0"`)
	syncing(t, func(ctx context.Context) (bool, error) {
		defer r.eng.CatalogLock()()
		return r.objects.PollViews(ctx)
	})
	ctx := context.Background()
	run := func(conn *pgx.Conn, q string) error {
		rows, err := conn.Query(ctx, q)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		rows.Close()
		return rows.Err()
	}

	stop := make(chan struct{})
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for i := range 6 {
		mode := pgx.QueryExecModeSimpleProtocol
		if i%2 == 1 {
			mode = pgx.QueryExecModeExec
		}
		conn := r.admin(t, mode)
		if i >= 3 {
			conn = connect(t, r.url("rita", "rita-password"), mode)
		}
		wg.Go(func() {
			for n := 0; !stopped(); n++ {
				q := fmt.Sprintf(`SELECT %d AS n, count(*) FROM "lake"."analytics"."other_%d" a, "lake"."analytics"."v" b WHERE a.run >= 0`,
					i*1_000_000+n, n%8)
				if err := run(conn, q); err != nil {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("%s: %v", q, err))
					mu.Unlock()
				}
			}
		})
	}
	// Another engine creates tables and drops them through the catalog,
	// which the replica learns of only when it next loads them.
	gone := make(chan string, builds)
	other := r.dbt(t)
	wg.Go(func() {
		for n := 0; !stopped(); n++ {
			name := fmt.Sprintf("ext_%d", n)
			if err := run(other, fmt.Sprintf(`create table "lake"."analytics"."%s" as select 1 as run`, name)); err != nil {
				continue
			}
			req, _ := http.NewRequest(http.MethodDelete, vc.fake.URL+"/v1/"+icebergtest.Prefix+"/namespaces/analytics/tables/"+name, nil)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				_ = resp.Body.Close()
			}
			select {
			case gone <- name:
			default:
			}
		}
	})
	for range 3 {
		conn := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
		wg.Go(func() {
			last := "ext_0"
			for n := 0; !stopped(); n++ {
				select {
				case last = <-gone:
				default:
				}
				queries := append(slices.Clone(readerQueries),
					fmt.Sprintf(`SELECT %d, count(*) FROM "lake"."analytics"."nothing_here"`, n),
					fmt.Sprintf(`SELECT %d, count(*) FROM "lake"."analytics"."%s"`, n, last),
					fmt.Sprintf(`SELECT %d, run FROM "lake"."analytics"."mart"`, n))
				_ = run(conn, queries[n%len(queries)])
			}
		})
	}

	mustExec(t, writer, `create table "lake"."analytics"."log" (run INTEGER)`)
	for i := 1; i <= builds; i++ {
		if err := dbtRun(writer, "mart", dbtTable("mart", fmt.Sprintf("select %d as run", i), i > 1, false)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("build %d: %v", i, err)
		}
		if _, err := writer.Exec(ctx, fmt.Sprintf(`INSERT INTO "lake"."analytics"."log" VALUES (%d)`, i)); err != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("build %d: insert: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	for _, f := range failures {
		t.Errorf("a query over tables that stay put failed: %s", f)
	}
	shared, retried := testutil.ToFloat64(metrics.prepares.WithLabelValues("shared")), testutil.ToFloat64(metrics.prepares.WithLabelValues("retried"))
	if shared == 0 || retried == 0 {
		t.Errorf("%v queries prepared shared, %v prepared again; want some of each", shared, retried)
	}
	t.Logf("%v queries prepared shared, %v prepared again, %d catalog requests refused", shared, retried, r.eng.SharedRefusals())
	if mart := martTables(t, connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)); !slices.Equal(mart, []string{"mart"}) {
		t.Errorf("a listing shows %v, want [mart]", mart)
	}
}
