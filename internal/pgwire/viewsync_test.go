package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// Every replica keeps a local copy of each of the catalog's views in
// DuckDB (see package ddl), and syncs them with the catalog in the
// background and whenever a statement names a relation DuckDB does not
// have. dbt materializes a model by creating it under a temporary name and
// renaming it into place, statement after statement on one connection. A
// sync that runs in between must not undo what the session has just done
// on its own replica. These tests run dbt's statements, as dbt-core 1.12.6
// with dbt-postgres 1.11.0 (the versions scripts/e2e.sh pins) sends them
// through psycopg2, while the replicas sync their views without pause and
// the catalog is slow to list views, so that syncs often hold a list made
// before the session's latest change.

// viewCatalog is icebergtest's catalog, for replicas of Igloo to share.
type viewCatalog struct {
	fake *icebergtest.Catalog
	cfg  config.Config
}

func newViewCatalog(t *testing.T) *viewCatalog {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	return &viewCatalog{fake: fake, cfg: config.Config{
		Catalog: config.Catalog{URI: fake.URL, Warehouse: "test", Name: "lake", Schema: "public"},
		// Keys only so the storage secret can be made; the files are local.
		Storage: config.Storage{Location: dir, S3: config.S3{
			Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused",
		}},
		DuckDB: config.DuckDB{ExtensionDir: ext},
	}}
}

// slowViewLists makes every list of a namespace's views reach the replicas
// up to max late, as it was when the catalog made it.
func (vc *viewCatalog) slowViewLists(max time.Duration) {
	vc.fake.HoldViewLists(func(string) { time.Sleep(rand.N(max)) })
}

// onNextViewList runs fn while the catalog holds the next list of the
// views of namespace ns it makes, before sending it; later lists go out at
// once.
func (vc *viewCatalog) onNextViewList(ns string, fn func()) {
	var done atomic.Bool
	vc.fake.HoldViewLists(func(listed string) {
		if listed == ns && done.CompareAndSwap(false, true) {
			fn()
		}
	})
}

// viewReplica is a replica of Igloo on a viewCatalog, with a writer, walt.
// Its views sync only when a test syncs them.
type viewReplica struct {
	*testServer
	objects *ddl.Manager
	eng     *engine.Engine
}

func (vc *viewCatalog) replica(t *testing.T) *viewReplica {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.DiscardHandler)
	cfg := vc.cfg
	cfg.DuckDB.TempDir = t.TempDir()
	eng, err := engine.Open(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(ctx, cfg)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	objects, err := ddl.New(ctx, cat, eng, log)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	r := &viewReplica{testServer: startServerOn(t, eng, func(o *Options) { o.DDL = objects }), objects: objects, eng: eng}
	t.Cleanup(func() { _ = objects.Close() })
	mustExec(t, r.admin(t, pgx.QueryExecModeSimpleProtocol), "CREATE USER walt WITH PASSWORD 'walt-password' ROLE writer")
	return r
}

// dbt connects as dbt does: walt, through the simple query protocol, as
// psycopg2 talks.
func (r *viewReplica) dbt(t *testing.T) *pgx.Conn {
	t.Helper()
	return connect(t, r.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol)
}

// syncing runs a replica's PollViews, as its background sync does, or its
// SyncViews, as statements that miss a relation do, over and over until
// the test ends. A sync may fail for a while, as when it lists a view in
// the middle of dbt's swap of another it reads; checkViews syncs once more
// when all is quiet.
func syncing(t *testing.T, views func(context.Context) (bool, error)) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			_, _ = views(ctx)
		}
	})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

// localViews returns r's local copies of views in schema, by name, with
// the query each runs.
func (r *viewReplica) localViews(t *testing.T, schema string) map[string]string {
	t.Helper()
	ctx := context.Background()
	conn, err := r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	rows, err := conn.QueryContext(ctx, "SELECT view_name, sql FROM duckdb_views() WHERE database_name = $1 AND schema_name = $2 AND NOT internal",
		[]driver.NamedValue{{Ordinal: 1, Value: r.eng.LocalDatabase()}, {Ordinal: 2, Value: schema}})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]string{}
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			return out
		}
		out[dest[0].(string)] = dest[1].(string)
	}
}

// dbtComment is the comment dbt puts before each statement of a model.
func dbtComment(model string) string {
	return `/* {"app": "dbt", "dbt_version": "1.12.6", "profile_name": "igloo", "target_name": "igloo", "node_id": "model.agency_reporting.` +
		model + `"} */` + "\n"
}

// dbtRun runs a model's statements on conn, each as dbt sends it, and
// stops at the first that fails.
func dbtRun(conn *pgx.Conn, model string, stmts []string) error {
	for _, s := range stmts {
		if s != "BEGIN" && s != "COMMIT" {
			s = dbtComment(model) + s
		}
		if _, err := conn.Exec(context.Background(), s); err != nil {
			return fmt.Errorf("model %s: %s: %w", model, s, err)
		}
	}
	return nil
}

// inBlock puts dbt's transaction statements around stmts if tx: psycopg2
// opens a block before the first statement after a commit.
func inBlock(tx bool, stmts ...string) []string {
	if !tx {
		return stmts
	}
	return append(append([]string{"BEGIN"}, stmts...), "COMMIT")
}

// relation is how dbt-postgres names a model in analytics.
func relation(name string) string { return `"lake"."analytics"."` + name + `"` }

// dbtView is dbt's view materialization (dbt-adapters' view.sql) of model
// as query, whose columns are run and label: with persist_docs, as the e2e
// project has it, and with or without psycopg2's transaction blocks (tx).
// exists is whether the model's view exists already.
func dbtView(model, query string, exists, tx bool) []string {
	main := []string{
		"\n  create view " + relation(model+"__dbt_tmp") + "\n    \n    \n  as (\n    " + query + "\n  );",
	}
	if exists {
		main = append(main, "alter table "+relation(model)+` rename to "`+model+`__dbt_backup"`)
	}
	main = append(main,
		"alter table "+relation(model+"__dbt_tmp")+` rename to "`+model+`"`,
		"comment on view "+relation(model)+" is $dbt_comment_literal_block$One row per run.$dbt_comment_literal_block$;",
		`select
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

  from "lake".INFORMATION_SCHEMA.columns
  where table_name = '`+model+`'

    and table_schema = 'analytics'

  order by ordinal_position`,
		// Both comments in one query, as dbt runs them.
		"\n    comment on column "+relation(model)+".run is $dbt_comment_literal_block$The run.$dbt_comment_literal_block$;\n  \n"+
			"    comment on column "+relation(model)+".label is $dbt_comment_literal_block$Its label.$dbt_comment_literal_block$;\n  ",
	)
	return append(inBlock(tx, main...), inBlock(tx, "drop view if exists "+relation(model+"__dbt_backup")+" cascade")...)
}

// dbtTable is dbt's table materialization (table.sql) of model as query.
func dbtTable(model, query string, exists, tx bool) []string {
	main := []string{"\n  \n    \n\n  create  table " + relation(model+"__dbt_tmp") + "\n  \n  \n    as\n  \n  (\n    " + query + "\n  );"}
	if exists {
		main = append(main, "alter table "+relation(model)+` rename to "`+model+`__dbt_backup"`)
	}
	main = append(main,
		"alter table "+relation(model+"__dbt_tmp")+` rename to "`+model+`"`,
		"comment on table "+relation(model)+" is $dbt_comment_literal_block$Runs, counted.$dbt_comment_literal_block$;")
	return append(inBlock(tx, main...), inBlock(tx, "drop table if exists "+relation(model+"__dbt_backup")+" cascade")...)
}

// dbtAppend is dbt's incremental materialization (incremental.sql) of
// model as query with the append strategy, once the table exists: the new
// rows go through a temporary table.
func dbtAppend(model, query string, tx bool) []string {
	tmp := `"` + model + `__dbt_tmp` + fmt.Sprintf("%012d", rand.N(1_000_000_000_000)) + `"`
	return inBlock(tx,
		"\n  \n    \n\n  create temporary table "+tmp+"\n  \n  \n    as\n  \n  (\n    "+query+"\n  );",
		"\n      insert into "+relation(model)+` ("run", "label")`+"\n    (\n        select \"run\", \"label\"\n        from "+tmp+"\n    )\n\n  ")
}

// dbtBuild materializes a small project once, as `dbt run` would for the
// i-th time: views stg_a and stg_b, a table that reads them, and an
// incremental model that appends to itself.
func dbtBuild(conn *pgx.Conn, i int, tx bool) error {
	first := i == 1
	steps := []struct {
		model string
		stmts []string
	}{
		{"stg_a", dbtView("stg_a", fmt.Sprintf("select %d as run, 'a' as label", i), !first, tx)},
		{"stg_b", dbtView("stg_b", fmt.Sprintf("select run, label || 'b' as label from %s", relation("stg_a")), !first, tx)},
		{"mart", dbtTable("mart", "select a.run, b.label from "+relation("stg_a")+" a join "+relation("stg_b")+" b using (run)", !first, tx)},
	}
	log := dbtAppend("log", "select run, label from "+relation("stg_a"), tx)
	if first {
		// The first run creates the table in place.
		log = inBlock(tx, "\n  \n    \n\n  create  table "+relation("log")+"\n  \n  \n    as\n  \n  (\n    select run, label from "+relation("stg_a")+"\n  );")
	}
	steps = append(steps, struct {
		model string
		stmts []string
	}{"log", log})
	for _, s := range steps {
		if err := dbtRun(conn, s.model, s.stmts); err != nil {
			return err
		}
	}
	return nil
}

// checkBuild checks what the i-th build made, as the session that ran it
// sees it.
func checkBuild(t *testing.T, conn *pgx.Conn, i int) {
	t.Helper()
	ctx := context.Background()
	var run int
	var label string
	if err := conn.QueryRow(ctx, "SELECT run, label FROM "+relation("mart")).Scan(&run, &label); err != nil {
		t.Fatalf("build %d: read mart: %v", i, err)
	}
	if run != i || label != "ab" {
		t.Fatalf("build %d: mart has run %d, label %q, from out-of-date views", i, run, label)
	}
	var n, sum int
	if err := conn.QueryRow(ctx, "SELECT count(*), sum(run) FROM "+relation("log")).Scan(&n, &sum); err != nil {
		t.Fatalf("build %d: read log: %v", i, err)
	}
	if n != i || sum != i*(i+1)/2 {
		t.Fatalf("build %d: log has %d rows summing to %d, want %d rows summing to %d", i, n, sum, i, i*(i+1)/2)
	}
	var leftovers []string
	rows, err := conn.Query(ctx, `SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'analytics' AND (table_name LIKE '%dbt_tmp%' OR table_name LIKE '%dbt_backup%')`)
	if err != nil {
		t.Fatalf("build %d: list relations: %v", i, err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		leftovers = append(leftovers, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(leftovers) > 0 {
		t.Fatalf("build %d left intermediate relations: %v", i, leftovers)
	}
}

// checkViews checks that the catalog has exactly the views want, by
// schema.name, and that each replica, once synced, has a local copy of
// each, of its current definition, and no other.
func checkViews(t *testing.T, vc *viewCatalog, want map[string]string, replicas ...*viewReplica) {
	t.Helper()
	got := vc.fake.Views()
	slices.Sort(got)
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	slices.Sort(names)
	if !slices.Equal(got, names) {
		t.Errorf("the catalog has views %v, want %v", got, names)
	}
	for i, r := range replicas {
		if _, err := r.objects.SyncViews(context.Background()); err != nil {
			t.Errorf("replica %d: sync views: %v", i, err)
		}
		local := r.localViews(t, "analytics")
		var have []string
		for name, sql := range local {
			have = append(have, "analytics."+name)
			if !strings.Contains(sql, want["analytics."+name]) {
				t.Errorf("replica %d: the local copy of %s is out of date: %s, want %s", i, name, sql, want["analytics."+name])
			}
		}
		slices.Sort(have)
		if !slices.Equal(have, names) {
			t.Errorf("replica %d has local copies %v, want %v", i, have, names)
		}
	}
}

// TestViewSyncKeepsASessionsChanges is the e2e's failure, made certain: the
// background sync gets the catalog's views listed before dbt creates the
// model's view under its temporary name, and applies that list once it
// has. It must not drop the session's new view, so the renames that follow
// work.
func TestViewSyncKeepsASessionsChanges(t *testing.T) {
	for _, tx := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "transaction"}[tx], func(t *testing.T) {
			vc := newViewCatalog(t)
			r := vc.replica(t)
			conn := r.dbt(t)
			mustExec(t, conn, `create schema if not exists "lake"."analytics"`)
			// The model's view as the previous run left it.
			if err := dbtRun(conn, "m", dbtView("m", "select 1 as run, 'a' as label", false, tx)); err != nil {
				t.Fatal(err)
			}

			stmts := dbtView("m", "select 2 as run, 'b' as label", true, tx)
			create := slices.IndexFunc(stmts, func(s string) bool { return strings.Contains(s, "create view") })
			listed, release := make(chan struct{}), make(chan struct{})
			vc.onNextViewList("analytics", func() {
				close(listed)
				<-release
			})
			synced := make(chan error, 1)
			go func() {
				_, err := r.objects.PollViews(context.Background())
				synced <- err
			}()
			<-listed
			if err := dbtRun(conn, "m", stmts[:create+1]); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-synced; err != nil {
				t.Fatal(err)
			}
			if err := dbtRun(conn, "m", stmts[create+1:]); err != nil {
				t.Fatal(err)
			}
			var run int
			if err := conn.QueryRow(context.Background(), "SELECT run FROM "+relation("m")).Scan(&run); err != nil || run != 2 {
				t.Errorf("the model reads run %d (%v), want 2", run, err)
			}
			checkViews(t, vc, map[string]string{"analytics.m": "2 AS run"}, r)
		})
	}
}

// TestOlderViewListIsNotApplied: a replica's sync that listed the
// catalog's views before another replica created one must not drop the
// local copy a sync that listed them later has made.
func TestOlderViewListIsNotApplied(t *testing.T) {
	vc := newViewCatalog(t)
	a, b := vc.replica(t), vc.replica(t)
	mustExec(t, a.dbt(t), `create schema if not exists "lake"."analytics"`)
	listed, release := make(chan struct{}), make(chan struct{})
	vc.onNextViewList("analytics", func() {
		close(listed)
		<-release
	})
	older := make(chan error, 1)
	go func() {
		_, err := b.objects.PollViews(context.Background())
		older <- err
	}()
	<-listed
	mustExec(t, a.dbt(t), `create view "lake"."analytics"."v" as select 1 as run`)
	if changed, err := b.objects.SyncViews(context.Background()); err != nil || !changed {
		t.Fatalf("the later sync: changed %v, %v", changed, err)
	}
	close(release)
	if err := <-older; err != nil {
		t.Fatal(err)
	}
	if _, ok := b.localViews(t, "analytics")["v"]; !ok {
		t.Error("the earlier sync's list dropped the view the later one brought")
	}
}

// TestStatementFindsViewAnotherSyncBrought: a statement that names a view
// another replica created syncs before it fails; when another sync, which
// began later, brings the view first, its own changes nothing, and the
// statement must still find the view.
func TestStatementFindsViewAnotherSyncBrought(t *testing.T) {
	vc := newViewCatalog(t)
	a, b := vc.replica(t), vc.replica(t)
	mustExec(t, a.dbt(t), `create schema if not exists "lake"."analytics"`)
	mustExec(t, a.dbt(t), `create view "lake"."analytics"."v" as select 7 as run`)
	reader := b.dbt(t)
	// The statement's sync lists the views; before it gets the list, the
	// background sync begins, lists them and makes the view.
	vc.onNextViewList("analytics", func() {
		if _, err := b.objects.PollViews(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var run int
	if err := reader.QueryRow(context.Background(), "SELECT run FROM "+relation("v")).Scan(&run); err != nil || run != 7 {
		t.Errorf("read run %d (%v), want 7", run, err)
	}
}

// TestDbtRunsWhileViewsSync runs dbt's statements for a small project over
// and over, with and without transaction blocks, while the replica polls
// its views all the time and a second replica, which syncs its own as
// statements that miss a view do, reads them. Every build must succeed,
// leave nothing behind and see what it built, and once synced, both
// replicas must have one up-to-date copy of each view.
//
// Nothing on the first replica syncs as SyncViews does while dbt runs:
// that lists the tables of the schema whose tables dbt swaps, which
// DuckDB-Iceberg does not keep in step (see PollViews).
func TestDbtRunsWhileViewsSync(t *testing.T) {
	const builds = 12
	for _, tx := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "transaction"}[tx], func(t *testing.T) {
			vc := newViewCatalog(t)
			a, b := vc.replica(t), vc.replica(t)
			conn := a.dbt(t)
			mustExec(t, conn, `create schema if not exists "lake"."analytics"`)
			vc.slowViewLists(20 * time.Millisecond)
			syncing(t, a.objects.PollViews)
			syncing(t, b.objects.PollViews)
			syncing(t, b.objects.SyncViews)

			reader := b.dbt(t)
			for i := 1; i <= builds; i++ {
				if err := dbtBuild(conn, i, tx); err != nil {
					t.Fatalf("build %d: %v", i, err)
				}
				checkBuild(t, conn, i)
				// The other replica may not have caught up yet, but it
				// never sees a build that is not done.
				var run int
				if err := reader.QueryRow(context.Background(), "SELECT run FROM "+relation("stg_b")).Scan(&run); err != nil || run > i {
					t.Errorf("build %d: the other replica reads run %d (%v)", i, run, err)
				}
			}
			checkViews(t, vc, map[string]string{
				"analytics.stg_a": fmt.Sprintf("%d AS run", builds),
				"analytics.stg_b": "stg_a",
			}, a, b)
		})
	}
}

// TestTablesSwapWhileViewsPoll runs dbt's table materialization over and
// over while the replica polls its views without pause, as the background
// sync does every 30 seconds through a dbt run. Polling must not make
// DuckDB list the schema's tables: DuckDB-Iceberg, listing them while dbt
// drops the model's __dbt_backup table, can go on listing that table after
// the catalog dropped it, or crash.
func TestTablesSwapWhileViewsPoll(t *testing.T) {
	const builds = 60
	vc := newViewCatalog(t)
	r := vc.replica(t)
	conn := r.dbt(t)
	mustExec(t, conn, `create schema if not exists "lake"."analytics"`)
	mustExec(t, conn, `create view "lake"."analytics"."v" as select 1 as run`)
	syncing(t, r.objects.PollViews)
	for i := 1; i <= builds; i++ {
		if err := dbtRun(conn, "mart", dbtTable("mart", fmt.Sprintf("select %d as run", i), i > 1, false)); err != nil {
			t.Fatalf("build %d: %v", i, err)
		}
		var listed []string
		rows, err := conn.Query(context.Background(), `SELECT table_name FROM information_schema.tables
			WHERE table_schema = 'analytics' AND table_name LIKE 'mart%' ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			listed = append(listed, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(listed, []string{"mart"}) {
			tables := vc.fake.Tables()
			slices.Sort(tables)
			t.Fatalf("build %d: DuckDB lists %v; the catalog has %v", i, listed, tables)
		}
	}
}

// TestSessionsChangeViewsWhileTheySync is TestDbtRunsWhileViewsSync with
// two sessions on each of two replicas, each building its own view in the
// same schema at once, while both replicas poll and sync their views.
func TestSessionsChangeViewsWhileTheySync(t *testing.T) {
	const builds = 8
	vc := newViewCatalog(t)
	replicas := []*viewReplica{vc.replica(t), vc.replica(t)}
	mustExec(t, replicas[0].dbt(t), `create schema if not exists "lake"."analytics"`)
	vc.slowViewLists(20 * time.Millisecond)
	for _, r := range replicas {
		syncing(t, r.objects.PollViews)
		syncing(t, r.objects.SyncViews)
	}

	want := map[string]string{}
	var wg sync.WaitGroup
	for ri, r := range replicas {
		for s := range 2 {
			model := fmt.Sprintf("v_%d_%d", ri, s)
			want["analytics."+model] = fmt.Sprintf("%d AS run", builds)
			conn := r.dbt(t)
			tx := s == 1
			wg.Go(func() {
				for i := 1; i <= builds; i++ {
					if err := dbtRun(conn, model, dbtView(model, fmt.Sprintf("select %d as run, 'x' as label", i), i > 1, tx)); err != nil {
						t.Errorf("build %d: %v", i, err)
						return
					}
					var run int
					if err := conn.QueryRow(context.Background(), "SELECT run FROM "+relation(model)).Scan(&run); err != nil || run != i {
						t.Errorf("build %d of %s reads run %d (%v)", i, model, run, err)
						return
					}
				}
			})
		}
	}
	wg.Wait()
	checkViews(t, vc, want, replicas...)
}
