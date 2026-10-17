package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	iofs "io/fs"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// These tests run Igloo on an Iceberg catalog with faults injected between
// DuckDB and the catalog, and check that every commit lands exactly once
// and that tables never reference files that are gone.
//
// They use icebergtest's catalog, with tables in a local directory. With
// IGLOO_TEST_CATALOG=env they use the catalog and object store configured
// as for Igloo itself (IGLOO_CATALOG_URI, IGLOO_WAREHOUSE_LOCATION,
// IGLOO_S3_*, ...), such as the Lakekeeper of the Compose stack.

// catalogEnv is a server on a catalog behind a fault injector.
type catalogEnv struct {
	*testServer
	faults *icebergtest.Faults
	// direct reaches the catalog without faults.
	direct config.Config
	// fake is icebergtest's catalog, or nil with a real one.
	fake *icebergtest.Catalog
	// table is a fresh table name for this test.
	table string
}

func newCatalogEnv(t *testing.T, opts ...engine.Option) *catalogEnv {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	env := &catalogEnv{}

	var cfg config.Config
	if os.Getenv("IGLOO_TEST_CATALOG") == "env" {
		var err error
		if cfg, err = config.Load(os.LookupEnv); err != nil {
			t.Fatal(err)
		}
		setupCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if err := catalog.Setup(setupCtx, cfg, &http.Client{Timeout: 10 * time.Second}, slog.New(slog.DiscardHandler)); err != nil {
			t.Fatal(err)
		}
	} else {
		dir := t.TempDir()
		env.fake = icebergtest.NewCatalog(dir)
		t.Cleanup(env.fake.Close)
		cfg = config.Config{
			Catalog: config.Catalog{URI: env.fake.URL, Warehouse: "test", Name: "lake", Schema: "public"},
			// Keys only so the storage secret can be made; the files are
			// local.
			Storage: config.Storage{Location: dir, S3: config.S3{
				Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused",
			}},
		}
	}
	cfg.DuckDB = config.DuckDB{ExtensionDir: ext, TempDir: t.TempDir()}
	env.direct = cfg

	faults, err := icebergtest.NewFaults(cfg.Catalog.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(faults.Close)
	env.faults = faults
	cfg.Catalog.URI = faults.URL
	env.testServer = startServerWithEngine(t, cfg, opts)

	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	env.table = "commits_" + hex.EncodeToString(suffix)
	return env
}

// loadTable loads the table from the catalog, without faults.
func (env *catalogEnv) loadTable(t *testing.T) (*table.Table, iceio.IO) {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Open(ctx, env.direct)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := cat.LoadTable(ctx, catalog.Ident(env.direct.Catalog.Schema, env.table))
	if err != nil {
		t.Fatal(err)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return tbl, fs
}

// files checks the table's current snapshot: it returns the rows its data
// files hold, by their metadata, and the files that are missing.
func (env *catalogEnv) files(t *testing.T) (rows int64, missing []string) {
	t.Helper()
	tbl, fs := env.loadTable(t)
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, nil
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df.ContentType() == iceberg.EntryContentData {
				rows += df.Count()
			}
			f, err := fs.Open(df.FilePath())
			if err != nil {
				missing = append(missing, df.FilePath())
				continue
			}
			_ = f.Close()
		}
	}
	return rows, missing
}

// dataFiles lists the Parquet files in the table's data directory.
func (env *catalogEnv) dataFiles(t *testing.T) []string {
	t.Helper()
	tbl, fs := env.loadTable(t)
	list, ok := fs.(iceio.ListableIO)
	if !ok {
		t.Fatalf("cannot list %s", tbl.Location())
	}
	var files []string
	err := list.WalkDir(tbl.Location()+"/data", func(path string, d iofs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func (env *catalogEnv) query(t *testing.T, conn *pgx.Conn, sql string) string {
	t.Helper()
	var got string
	if err := conn.QueryRow(context.Background(), sql).Scan(&got); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return got
}

// checkOnce checks that the table holds the rows 1, 2 and 3 once and that
// every file it references is there.
func (env *catalogEnv) checkOnce(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	rows, missing := env.files(t)
	if len(missing) > 0 {
		t.Errorf("the table references %d missing files: %v", len(missing), missing)
	}
	if rows != 3 {
		t.Errorf("the table's snapshot holds %d rows, want 3", rows)
	}
	if len(missing) == 0 {
		if got := env.query(t, conn, "SELECT string_agg(n::VARCHAR, ',' ORDER BY n) FROM "+env.table); got != "1,2,3" {
			t.Errorf("rows %s, want 1,2,3", got)
		}
	}
}

// TestCommitAppliedButAnsweredWithErrorLandsOnce is the failure seen on an
// overloaded host: the catalog applied a commit but answered 503. DuckDB
// sent the commit again, the catalog refused that (the table had moved
// on, by this very commit), DuckDB deleted the commit's data file and
// Igloo, seeing a conflict, ran the INSERT again.
func TestCommitAppliedButAnsweredWithErrorLandsOnce(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	env.faults.Arm(icebergtest.FailAfterApply)
	if _, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2), (3)"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	env.checkOnce(t, conn)
}

// TestCommitConnectionLostAfterApplyLandsOnce is the same with the answer
// lost altogether, as when a request times out.
func TestCommitConnectionLostAfterApplyLandsOnce(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	env.faults.Arm(icebergtest.DropAfterApply)
	if _, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2), (3)"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	env.checkOnce(t, conn)
}

// TestCommitAppliedButAnsweredWithErrorWithoutIdempotencyKeys is the first
// failure on a catalog that ignores Idempotency-Key headers: the commit's
// repeat is refused, and the proxy finds the commit's snapshot in the
// table instead.
func TestCommitAppliedButAnsweredWithErrorWithoutIdempotencyKeys(t *testing.T) {
	env := newCatalogEnv(t)
	if env.fake == nil {
		t.Skip("needs icebergtest's catalog")
	}
	env.fake.IgnoreIdempotencyKeys()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	env.faults.Arm(icebergtest.FailAfterApply)
	if _, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2), (3)"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	env.checkOnce(t, conn)
}

// TestCommitWithUnknownOutcomeKeepsItsFiles checks a commit the catalog
// applied before it stopped answering: the client hears that the outcome
// is unknown (SQLSTATE 40003, not a conflict to retry), and the commit's
// files stay, since it landed.
func TestCommitWithUnknownOutcomeKeepsItsFiles(t *testing.T) {
	env := newCatalogEnv(t, engine.CatalogProxy(func(o *catalogproxy.Options) {
		o.AttemptTimeout = time.Second
		o.Patience = 2 * time.Second
	}))
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	env.faults.Arm(icebergtest.DownAfterApply)
	_, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2), (3)")
	if got := pgCode(err); got != "40003" {
		t.Errorf("INSERT: code %q, want 40003 (%v)", got, err)
	}
	if err != nil && !strings.Contains(err.Error(), env.faults.URL+"/v1/") {
		t.Errorf("the error names the catalog proxy rather than the catalog: %v", err)
	}
	env.faults.Up()
	env.checkOnce(t, conn)
}

// TestCommitInTransactionAppliedButAnsweredWithError checks COMMIT of an
// explicit transaction, which Igloo does not retry: the client must hear
// that it committed.
func TestCommitInTransactionAppliedButAnsweredWithError(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "INSERT INTO "+env.table+" VALUES (1), (2)")
	mustExec(t, conn, "INSERT INTO "+env.table+" VALUES (3)")
	env.faults.Arm(icebergtest.FailAfterApply)
	if _, err := conn.Exec(context.Background(), "COMMIT"); err != nil {
		t.Errorf("COMMIT: %v", err)
	}
	env.checkOnce(t, conn)
}

// TestCommitRefusedBeforeApplyIsSentAgain checks a commit the catalog
// answered 503 without applying it: it is sent again, and lands once.
func TestCommitRefusedBeforeApplyIsSentAgain(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	before := len(env.faults.Commits())
	env.faults.Arm(icebergtest.FailBeforeApply)
	if _, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2), (3)"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	if got := len(env.faults.Commits()) - before; got != 2 {
		t.Errorf("the commit was sent %d times, want 2", got)
	}
	env.checkOnce(t, conn)
}

// TestConflictingCommitIsRetried checks a real conflict: another writer
// commits to the table first. The catalog refuses the commit, Igloo runs
// the statement again on the new snapshot, and the refused commit's data
// files are deleted.
func TestConflictingCommitIsRetried(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	other := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")

	env.faults.Before(func() {
		if _, err := other.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (3)"); err != nil {
			t.Errorf("the other writer's INSERT: %v", err)
		}
	})
	before := len(env.faults.Commits())
	if _, err := conn.Exec(context.Background(), "INSERT INTO "+env.table+" VALUES (1), (2)"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	// The other writer's commit, the refused one and the one after the
	// statement ran again.
	if got := len(env.faults.Commits()) - before; got != 3 {
		t.Errorf("%d commits, want 3", got)
	}
	env.checkOnce(t, conn)

	if got := len(env.dataFiles(t)); got != 2 {
		t.Errorf("%d data files, want 2: the refused commit's should be gone", got)
	}
}

// TestCommitConflictErrorIsKept checks that a conflict inside an explicit
// transaction still reaches the client as one, SQLSTATE 40001, and leaves
// the table as the other writer made it.
func TestCommitConflictErrorIsKept(t *testing.T) {
	env := newCatalogEnv(t)
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	other := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "CREATE TABLE "+env.table+" (n INTEGER)")
	mustExec(t, conn, "INSERT INTO "+env.table+" VALUES (1), (2)")

	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "INSERT INTO "+env.table+" VALUES (4)")
	mustExec(t, other, "INSERT INTO "+env.table+" VALUES (3)")
	_, err := conn.Exec(context.Background(), "COMMIT")
	if got := pgCode(err); got != "40001" {
		t.Errorf("COMMIT: code %q, want 40001 (%v)", got, err)
	}
	if err == nil {
		_, _ = conn.Exec(context.Background(), "ROLLBACK")
	}
	env.checkOnce(t, conn)
}
