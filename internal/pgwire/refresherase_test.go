package pgwire

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// TestErasureReplacesRefreshedTables: an erasure request on a table that
// materialized views and dynamic tables read, a projection that copies the
// person's email, an aggregate that keeps a bitmap of their id in its
// state and one that reads another, completes only once it has replaced
// them: no row of theirs holds the person's email, no snapshot of their
// tables or their state is older than the request's views step, their
// next refresh is full, and the results equal their queries'. A view of
// another table stays as it was. A view whose owner may no longer refresh
// it blocks the request until they may again.
func TestErasureReplacesRefreshedTables(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	sched := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	// A table dropped, or replaced, has its files deleted with it, as
	// Igloo has the catalog do for tables no clone reads.
	purge := func(context.Context, string, []string, string, bool) clone.PurgeVerdict {
		return clone.PurgeVerdict{Purge: true, Release: func() {}}
	}
	r := vc.replica(t, func(o *Options) { o.Tasks, o.ClonePurge = sched, purge })
	r.srv.refreshFraction = 1000
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		`CREATE TABLE lake.public.visits AS SELECT 'u' || (i % 20) || '@example.com' AS email, i % 20 AS person, ['north', 'south'][1 + i % 2] AS region,
			i AS spend FROM range(200) AS r(i)`,
		"CREATE TABLE lake.public.other AS SELECT i AS k FROM range(10) AS r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.visits ALTER COLUMN email SET TAG pii = 'email'",
	} {
		mustExec(t, admin, q)
	}
	walt := r.dbt(t)
	for _, q := range []string{
		"CREATE MATERIALIZED VIEW lake.public.big_visits AS SELECT email, spend FROM lake.public.visits WHERE spend > 50",
		"CREATE MATERIALIZED VIEW lake.public.reach AS SELECT region, count(DISTINCT person) AS people, sum(spend) AS spend FROM lake.public.visits GROUP BY region",
		"CREATE DYNAMIC TABLE lake.public.top_regions TARGET_LAG = '1 day' WAREHOUSE = wh AS SELECT region, people FROM lake.public.reach WHERE people > 1",
		"CREATE MATERIALIZED VIEW lake.public.other_totals AS SELECT count(*) AS n FROM lake.public.other",
	} {
		mustExec(t, walt, q)
	}
	// Refreshed twice: the second is incremental, and the tables have
	// older snapshots.
	mustExec(t, admin, "INSERT INTO lake.public.visits VALUES ('u3@example.com', 3, 'north', 1000)")
	for range 2 {
		for _, q := range []string{"REFRESH MATERIALIZED VIEW lake.public.big_visits", "REFRESH MATERIALIZED VIEW lake.public.reach",
			"ALTER DYNAMIC TABLE lake.public.top_regions REFRESH", "REFRESH MATERIALIZED VIEW lake.public.other_totals"} {
			mustExec(t, walt, q)
		}
		mustExec(t, admin, "INSERT INTO lake.public.visits VALUES ('u7@example.com', 7, 'south', 70)")
	}
	const person = "u3@example.com"
	if n := count(t, admin, "SELECT count(*) FROM lake.public.big_visits WHERE email = '"+person+"'"); n == 0 {
		t.Fatal("the projection does not hold the person's rows: the test shows nothing")
	}
	reach, err := sched.Store().Job(ctx, tasks.KindMaterializedView, "public.reach")
	if err != nil || decodeRefresh(reach.Refresh).State == "" {
		t.Fatalf("reach keeps no state: %+v %v", reach, err)
	}
	otherBefore, err := r.objects.LoadTable(ctx, "public", "other_totals")
	if err != nil {
		t.Fatal(err)
	}

	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := erasure.NewMemoryStore()
	views := &ViewRebuilder{}
	views.Bind(r.srv)
	svc := erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: r.srv.gov.store.Load, Engine: r.eng, Jobs: sched,
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Schema: "public", Warehouse: vc.cfg.Storage.Location + "/",
		Views: views, Purger: erasure.NewPurger(store, r.eng, erasure.MemoryCache+"replica", "replica", slog.New(slog.DiscardHandler))})
	run := func(id string, until ...string) *erasure.Request {
		t.Helper()
		var got *erasure.Request
		for range 10 {
			_ = svc.Run(ctx, id)
			if got, err = store.Get(ctx, id); err != nil {
				t.Fatal(err)
			}
			if slices.Contains(until, got.Status) {
				return got
			}
		}
		t.Fatalf("request %s: status %s at %s: %s", id, got.Status, got.Step, got.Error)
		return nil
	}
	submit := func(email string) string {
		t.Helper()
		req, err := svc.Submit(ctx, "privacy", []erasure.Wanted{{Tag: "pii", Value: "email", Identifiers: []string{email}}})
		if err != nil {
			t.Fatal(err)
		}
		return req.ID
	}

	start := time.Now()
	got := run(submit(person), erasure.StatusCompleted)
	if got.ViewsRebuilt != 3 || got.Verification == nil || got.Verification.Views != 0 {
		t.Errorf("replaced %d views, verification %+v", got.ViewsRebuilt, got.Verification)
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.public.big_visits WHERE email = '"+person+"'"); n != 0 {
		t.Errorf("the projection still holds %d of the person's rows", n)
	}
	for _, name := range []string{"big_visits", "reach", "top_regions"} {
		tbl, err := r.objects.LoadTable(ctx, "public", name)
		if err != nil {
			t.Fatal(err)
		}
		for _, snap := range tbl.Metadata().Snapshots() {
			if time.UnixMilli(snap.TimestampMs).Before(start) {
				t.Errorf("%s has a snapshot from before the request: %v", name, time.UnixMilli(snap.TimestampMs))
			}
		}
	}
	if _, err := r.objects.LoadTable(ctx, refreshSchema, decodeRefresh(reach.Refresh).State); err == nil {
		t.Errorf("reach's old state is still there")
	}
	if other, err := r.objects.LoadTable(ctx, "public", "other_totals"); err != nil || other.MetadataLocation() != otherBefore.MetadataLocation() {
		t.Errorf("a view of another table changed: %v", err)
	}
	for _, q := range []struct{ view, query string }{
		{"big_visits", "SELECT email, spend FROM lake.public.visits WHERE spend > 50"},
		{"reach", "SELECT region, count(DISTINCT person), sum(spend) FROM lake.public.visits GROUP BY region"},
	} {
		if got, want := sortedTexts(t, admin, "SELECT * FROM lake.public."+q.view), sortedTexts(t, admin, q.query); !slices.Equal(got, want) {
			t.Errorf("%s after the request: %q, want %q", q.view, got, want)
		}
	}
	// Their next refresh is full, and rebuilds the state.
	mustExec(t, walt, "REFRESH MATERIALIZED VIEW lake.public.reach")
	if action, detail, _, _ := viewRefresh(t, admin, "public.reach"); action != "FULL" {
		t.Errorf("reach's refresh after the request: %s, %s", action, detail)
	}

	// A view whose owner may no longer refresh it blocks the next request
	// until they may again.
	mustExec(t, admin, "INSERT INTO lake.public.visits VALUES ('u5@example.com', 5, 'north', 500)")
	mustExec(t, walt, "REFRESH MATERIALIZED VIEW lake.public.big_visits")
	mustExec(t, admin, "ALTER USER walt ROLE none")
	id := submit("u5@example.com")
	blocked := run(id, erasure.StatusBlocked)
	if blocked.Step != erasure.StepViews || !strings.Contains(blocked.Error, "public.big_visits: permission denied") {
		t.Errorf("blocked at %s: %s", blocked.Step, blocked.Error)
	}
	mustExec(t, admin, "ALTER USER walt ROLE writer")
	if got := run(id, erasure.StatusCompleted); got.Verification.Views != 0 {
		t.Errorf("after walt was enabled: %+v", got.Verification)
	}
	if n := count(t, admin, "SELECT count(*) FROM lake.public.big_visits WHERE email = 'u5@example.com'"); n != 0 {
		t.Errorf("the projection still holds %d of the second person's rows", n)
	}
}
