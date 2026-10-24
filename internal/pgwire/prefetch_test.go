package pgwire

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/prefetch"
)

// localObject reads a data file of the test catalog's local warehouse.
type localObject struct{ *os.File }

func (localObject) ETag() string { return "" }

// TestPrefetchStatements reads ahead, as a query runs, the blocks of the
// files of the tables it reads, and not for a session that turned it off.
func TestPrefetchStatements(t *testing.T) {
	vc := newViewCatalog(t)
	ctx := context.Background()
	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := distributed.NewScanner(distributed.NewIceberg(cat))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	dir := t.TempDir()
	pf, err := prefetch.New(prefetch.Options{Dir: dir, Open: func(_ context.Context, path string) (prefetch.Object, error) {
		f, err := os.Open(strings.TrimPrefix(path, "file://"))
		return localObject{f}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pf.Close)
	r := vc.replica(t, func(o *Options) { o.Scanner, o.Prefetcher = scanner, pf })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.visits AS SELECT md5(CAST(i AS VARCHAR)) AS person, i AS n, 'site' || (i % 5) AS site FROM range(200000) r(i)",
		"CREATE TABLE lake.public.others AS SELECT md5(CAST(i AS VARCHAR)) AS person FROM range(200000) r(i)",
	} {
		mustExec(t, admin, q)
	}
	files := func(table string) map[string]bool {
		t.Helper()
		out := map[string]bool{}
		meta := vc.fake.Metadata("public", table)
		root := strings.TrimPrefix(meta.Location(), "file://")
		_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err == nil && strings.HasSuffix(p, ".parquet") {
				out[filepath.Base(p)] = true
			}
			return nil
		})
		return out
	}
	blocksOf := func(table string) int {
		t.Helper()
		names := files(table)
		entries, _ := os.ReadDir(dir)
		n := 0
		for _, e := range entries {
			parts := strings.Split(e.Name(), "-")
			if len(parts) > 3 && names[strings.Join(parts[1:len(parts)-2], "-")] {
				n++
			}
		}
		return n
	}
	if got := texts(t, admin, "SHOW igloo_prefetch"); len(got) != 1 || got[0] != "on" {
		t.Errorf("SHOW igloo_prefetch: %q", got)
	}
	expectRows(t, admin, "SELECT count(DISTINCT person) FROM lake.public.visits WHERE n >= 0", "200000")
	deadline := time.Now().Add(10 * time.Second)
	for blocksOf("visits") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if blocksOf("visits") == 0 {
		t.Fatalf("no blocks of visits' files were read ahead: %v", files("visits"))
	}

	// A session that turned it off reads nothing ahead.
	mustExec(t, admin, "SET igloo_prefetch = off")
	if got := texts(t, admin, "SHOW igloo_prefetch"); len(got) != 1 || got[0] != "off" {
		t.Errorf("SHOW igloo_prefetch after SET: %q", got)
	}
	expectRows(t, admin, "SELECT count(DISTINCT person) FROM lake.public.others", "200000")
	time.Sleep(300 * time.Millisecond)
	if n := blocksOf("others"); n != 0 {
		t.Errorf("%d blocks read ahead for a session with igloo_prefetch off", n)
	}
	if _, err := admin.Exec(ctx, "SET igloo_prefetch = sometimes"); pgCode(err) != "22023" {
		t.Errorf("an invalid value: %v", err)
	}
	mustExec(t, admin, "RESET igloo_prefetch")
	// CREATE TABLE ... AS reads ahead what its query reads.
	mustExec(t, admin, "CREATE TABLE lake.public.copy AS SELECT person FROM lake.public.others")
	deadline = time.Now().Add(10 * time.Second)
	for blocksOf("others") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if blocksOf("others") == 0 {
		t.Error("no blocks read ahead for CREATE TABLE ... AS")
	}
}
