package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// processThreads counts the threads of this process, Go's and DuckDB's.
func processThreads(t *testing.T) int {
	t.Helper()
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	return len(tasks)
}

// TestReadCacheStartsNoThreads reads a table of wide row groups through the real read
// cache from a stand-in for object storage, in sessions at once, cold and warm, and
// counts the process's threads meanwhile: reads of the cache start none of their own.
// By default the extension starts a thread for each block of each read, 80 for a
// column of these row groups, cold or warm: under dashboards, ELT and reports beside
// each other, a replica of 16 cores reached 20,394 threads, a load of 240, and no
// statement finished for 20 minutes.
func TestReadCacheStartsNoThreads(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	if runtime.GOOS != "linux" {
		t.Skip("threads are counted in /proc")
	}
	local := filepath.Join(t.TempDir(), "wide.parquet")
	gen, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer gen.Close() //nolint:errcheck
	if _, err := gen.Exec(fmt.Sprintf(`COPY (SELECT i, repeat(md5(i::VARCHAR), 20) AS wide FROM range(250000) t(i)) TO '%s' (FORMAT parquet)`, local)); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(local)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"1234-5678"`)
		w.Header().Set("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
		http.ServeFile(w, r, local)
	}))
	defer srv.Close()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	db.SetMaxOpenConns(1)
	setup := []string{
		"SET extension_directory = '" + ext + "'", "LOAD httpfs", "LOAD cache_httpfs",
		"SET GLOBAL s3_endpoint = '" + strings.TrimPrefix(srv.URL, "http://") + "'", "SET GLOBAL s3_use_ssl = false",
		"SET GLOBAL s3_url_style = 'path'", "SET GLOBAL s3_region = 'us-east-1'", "SET GLOBAL s3_access_key_id = 'x'",
		"SET GLOBAL s3_secret_access_key = 'y'",
	}
	const threads, sessions = 8, 8
	setup = append(setup, fmt.Sprintf("SET GLOBAL threads = %d", threads))
	setup = append(setup, cacheStatements(t.TempDir(), "s3://bucket/", 1<<20, true)...)
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	db.SetMaxOpenConns(sessions)
	baseline := processThreads(t)

	stop := make(chan struct{})
	peak := make(chan int, 1)
	go func() {
		most := 0
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				peak <- most
				return
			case <-tick.C:
				most = max(most, processThreads(t))
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 3 {
				var n int64
				if err := db.QueryRowContext(ctx, "SELECT count(DISTINCT wide) FROM read_parquet('s3://bucket/t/data/wide.parquet')").Scan(&n); err != nil {
					errs <- err
					return
				}
				if n != 250000 {
					errs <- fmt.Errorf("%d distinct rows", n)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	most := <-peak
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// What may start: a thread of Go's for each session in DuckDB, a few more of its
	// own, and DuckDB's threads where they had not started yet.
	limit := baseline + threads + 2*sessions + 8
	t.Logf("threads: %d before, %d at most while they read, limit %d", baseline, most, limit)
	if most > limit {
		t.Errorf("the process reached %d threads reading %d sessions at once, from %d; at most %d should do", most, sessions, baseline, limit)
	}
}
