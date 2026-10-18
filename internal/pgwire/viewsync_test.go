package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"

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
// have. These tests run dbt's statements, as dbt-core 1.12.6 with
// dbt-postgres 1.11.0 (the versions scripts/e2e.sh pins) sends them through
// psycopg2, while the replicas sync their views without pause.

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
// the middle of dbt's swap of another it reads.
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
