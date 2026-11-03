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

// TestSharedPrepares checks that statements are prepared under the shared
// catalog lock: queries, by every role that may read, through either
// protocol, with their rows, and DDL, listings and queries in a transaction
// too; a query naming a table that does not exist gets DuckDB's suggestion of
// similar names; and IGLOO_SHARED_PREPARES=false prepares each on its own.
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
	count := func() float64 { return testutil.ToFloat64(metrics.prepares.WithLabelValues("shared")) }

	for _, c := range []struct {
		name string
		conn *pgx.Conn
	}{
		{"admin", admin},
		{"admin, extended protocol", r.admin(t, pgx.QueryExecModeExec)},
		{"writer", r.dbt(t)},
		{"reader", connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)},
	} {
		shared := count()
		if got := rawTexts(t, c.conn, "SELECT region, n FROM lake.public.by_region ORDER BY region"); !slices.Equal(got, []string{"0|4", "1|3", "2|3"}) {
			t.Errorf("%s: the view gave %q", c.name, got)
		}
		if got := rawTexts(t, c.conn, "WITH o AS (SELECT * FROM orders) SELECT count(*) FROM o"); !slices.Equal(got, []string{"10"}) {
			t.Errorf("%s: the CTE gave %q", c.name, got)
		}
		if n := count() - shared; n != 2 {
			t.Errorf("%s: %v queries prepared shared, want 2", c.name, n)
		}
	}

	if _, err := admin.Exec(ctx, "SELECT * FROM lake.public.ordrs"); err == nil || !strings.Contains(err.Error(), `"orders"`) {
		t.Errorf("a missing table: %v, want DuckDB's suggestion of orders", err)
	}

	shared := count()
	mustExec(t, admin, "INSERT INTO lake.public.orders VALUES (10, 1)")
	mustExec(t, admin, "BEGIN")
	if got := rawTexts(t, admin, "SELECT count(*) FROM lake.public.orders"); !slices.Equal(got, []string{"11"}) {
		t.Errorf("in a transaction: %q", got)
	}
	mustExec(t, admin, "COMMIT")
	rawTexts(t, admin, "SELECT count(*) FROM information_schema.tables")
	mustExec(t, admin, "CREATE TABLE lake.public.regions AS SELECT 1 AS region")
	if n := count() - shared; n < 4 {
		t.Errorf("%v statements other than queries outside a transaction prepared shared, want 4 or more", n)
	}

	// IGLOO_SHARED_PREPARES=false.
	off := NewMetrics(prometheus.NewRegistry())
	alone := vc.replica(t, func(o *Options) { o.Metrics, o.ExclusivePrepares = off, true })
	rawTexts(t, alone.admin(t, pgx.QueryExecModeSimpleProtocol), "SELECT count(*) FROM lake.public.orders")
	if n := testutil.ToFloat64(off.prepares.WithLabelValues("shared")); n != 0 {
		t.Errorf("%v queries prepared shared with shared prepares off", n)
	}
}

// TestQueriesWithoutPoliciesHoldNoExclusiveLock checks that with no policies a
// query is prepared and run while another session holds the catalog lock
// shared: taking it exclusively for policies there are none of would wait for
// that session, as every prepare in a dashboard's burst waited for the others.
func TestQueriesWithoutPoliciesHoldNoExclusiveLock(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t)
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TABLE lake.public.visits AS SELECT i AS id FROM range(10) r(i)")
	release := r.eng.CatalogShare()
	defer release()
	done := make(chan []string, 1)
	go func() { done <- rawTexts(t, admin, "SELECT count(*) FROM lake.public.visits") }()
	select {
	case got := <-done:
		if !slices.Equal(got, []string{"10"}) {
			t.Errorf("got %q", got)
		}
	case <-time.After(30 * time.Second):
		t.Error("the query waited for a shared hold of the catalog lock")
	}
}

// TestListingSubqueriesHoldTheCatalog checks that a query whose listing is a
// subquery, SELECT ... FROM (SHOW TABLES ...) or (DESCRIBE ...), lists the
// catalog as SHOW and DESCRIBE statements do (see listsCatalog), alongside
// other holders that take the lock shared over and over, as other sessions'
// binds do.
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
// polls. Queries must have been prepared shared.
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
	shared := testutil.ToFloat64(metrics.prepares.WithLabelValues("shared"))
	if shared == 0 {
		t.Error("no query was prepared shared")
	}
	t.Logf("%v statements prepared shared", shared)
	if mart := martTables(t, connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)); !slices.Equal(mart, []string{"mart"}) {
		t.Errorf("a listing shows %v, want [mart]", mart)
	}
}
