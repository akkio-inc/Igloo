package pgwire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/lockorder"
	"github.com/akkio-inc/igloo/internal/prefetch"
)

// TestMain checks the lock order in every test of the package: the catalog lock
// before the server's mutexes (see package lockorder).
func TestMain(m *testing.M) {
	lockorder.Enable()
	code := m.Run()
	if v := lockorder.Violations(); len(v) > 0 {
		fmt.Fprintf(os.Stderr, "the catalog lock was taken %d times while holding a server mutex:\n%s\n", len(v), strings.Join(v, "\n"))
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

// TestCatalogLoadTogether runs dashboards, read ahead, beside ingest, DDL that
// replaces tables and views, and listings of the catalog, all at once for a while,
// and fails if any statement takes more than 10 s. A session that held the catalog
// lock waited for a mutex of the server's held by one waiting for the catalog lock,
// and a replica froze for 16 minutes, refusing every login, 2 s into such a load.
func TestCatalogLoadTogether(t *testing.T) {
	t.Parallel()
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
	pf, err := prefetch.New(prefetch.Options{Dir: t.TempDir(), Open: func(_ context.Context, path string) (prefetch.Object, error) {
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
		"CREATE TABLE lake.public.load_facts AS SELECT i AS id, i % 50 AS g, i * 0.5 AS v FROM range(20000) r(i)",
		"CREATE TABLE lake.public.load_daily AS SELECT g, sum(v) AS v FROM lake.public.load_facts GROUP BY g",
		"CREATE VIEW lake.public.load_view AS SELECT g, count(*) AS n FROM lake.public.load_facts GROUP BY g",
	} {
		mustExec(t, admin, q)
	}

	streams := map[string][]string{
		"dashboard": {
			"SELECT g, count(*), sum(v) FROM lake.public.load_facts WHERE g < 10 GROUP BY g ORDER BY g",
			"SELECT g, v FROM lake.public.load_daily ORDER BY v DESC LIMIT 5",
			"SELECT * FROM lake.public.load_view ORDER BY g LIMIT 10",
			"SELECT count(*) FROM lake.public.load_facts f JOIN lake.public.load_daily d ON f.g = d.g WHERE f.id % 7 = 0",
		},
		"ingest": {
			"INSERT INTO lake.public.load_facts SELECT 100000 + i, i % 50, i FROM range(200) r(i)",
		},
		"ddl": {
			"CREATE OR REPLACE TABLE lake.public.load_daily AS SELECT g, sum(v) AS v FROM lake.public.load_facts GROUP BY g",
			"CREATE OR REPLACE VIEW lake.public.load_view AS SELECT g, count(*) AS n FROM lake.public.load_facts GROUP BY g",
			"CREATE TABLE IF NOT EXISTS lake.public.load_scratch (id INTEGER)",
			"DROP TABLE IF EXISTS lake.public.load_scratch",
		},
		"listing": {
			"SELECT count(*) FROM information_schema.tables",
			"SELECT count(*) FROM duckdb_columns() WHERE table_name LIKE 'load_%'",
			"SHOW TABLES FROM lake.public",
		},
	}
	sessions := map[string]int{"dashboard": 6, "ingest": 1, "ddl": 1, "listing": 2}
	const window, most = 8 * time.Second, 10 * time.Second
	stop := time.Now().Add(window)
	var ran atomic.Int64
	var mu sync.Mutex
	var slow []string
	var wg sync.WaitGroup
	for kind, n := range sessions {
		for i := range n {
			conn := r.admin(t, []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeCacheStatement}[i%2])
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := i; time.Now().Before(stop); j++ {
					q := streams[kind][j%len(streams[kind])]
					qctx, cancel := context.WithTimeout(ctx, most)
					start := time.Now()
					rows, err := conn.Query(qctx, q)
					if err == nil {
						for rows.Next() {
						}
						rows.Close()
						err = rows.Err()
					}
					cancel()
					ran.Add(1)
					if took := time.Since(start); took >= most || errors.Is(err, context.DeadlineExceeded) {
						mu.Lock()
						slow = append(slow, fmt.Sprintf("%s: %s took %s: %v", kind, q, took.Round(time.Millisecond), err))
						mu.Unlock()
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	t.Logf("%d statements in %s", ran.Load(), window)
	for _, s := range slow {
		t.Error(s)
	}
	// And a login still gets in, as none could on the frozen replica.
	lctx, cancel := context.WithTimeout(ctx, most)
	defer cancel()
	var n int
	if err := r.admin(t, pgx.QueryExecModeSimpleProtocol).QueryRow(lctx, "SELECT count(*) FROM lake.public.load_daily").Scan(&n); err != nil {
		t.Errorf("after the load: %v", err)
	}
}
