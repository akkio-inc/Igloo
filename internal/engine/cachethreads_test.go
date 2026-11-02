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

	"github.com/akkio-inc/igloo/internal/config"
)

// The DuckDB threads and the sessions of cacheReadPeak.
const (
	peakThreads  = 8
	peakSessions = 8
)

// peakOthers is what may start beside the read cache's threads: a thread of Go's
// for each session in DuckDB, a few more of its own, DuckDB's threads where they had
// not started yet, and a thread for each of Go's processors besides. Run with the rest
// of the package, a worker's reads usually add 111 to 125 threads; once they added
// 166, 6 past the limit without that last allowance. A storm adds thousands.
func peakOthers() int { return peakThreads + 2*peakSessions + 8 + runtime.GOMAXPROCS(0) }

// processThreads counts the threads of this process, Go's and DuckDB's.
func processThreads(t *testing.T) int {
	t.Helper()
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	return len(tasks)
}

// cacheReadPeak reads a table of wide row groups through the real read cache, set up
// with readThreads threads a read as an engine's is, from a stand-in for object storage
// that answers each request a little later, in sessions at once, cold and warm. It
// returns the process's threads before the reads, and the most while they ran.
func cacheReadPeak(t *testing.T, readThreads int) (before, most int) {
	t.Helper()
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
		// Long enough for reads' threads to be there at once.
		time.Sleep(5 * time.Millisecond)
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
		"SET GLOBAL s3_secret_access_key = 'y'", fmt.Sprintf("SET GLOBAL threads = %d", peakThreads),
	}
	setup = append(setup, cacheStatements(t.TempDir(), "s3://bucket/", 1<<20, true, readThreads)...)
	for _, q := range setup {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	db.SetMaxOpenConns(peakSessions)
	before = processThreads(t)

	stop := make(chan struct{})
	peak := make(chan int, 1)
	go func() {
		seen := 0
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				peak <- seen
				return
			case <-tick.C:
				seen = max(seen, processThreads(t))
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, peakSessions)
	for range peakSessions {
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
	most = <-peak
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	return before, most
}

// TestReadCacheStartsNoThreads checks that, as on a replica, reads of the cache start
// no threads of their own. By default the extension starts a thread for each block of
// each read, 80 for a column of these row groups, cold or warm: under dashboards, ELT
// and reports beside each other, a replica of 16 cores reached 20,394 threads, a load
// of 240, and no statement finished for 20 minutes.
func TestReadCacheStartsNoThreads(t *testing.T) {
	before, most := cacheReadPeak(t, 0)
	limit := before + peakOthers()
	t.Logf("threads: %d before, %d at most while they read, limit %d", before, most, limit)
	if most > limit {
		t.Errorf("the process reached %d threads reading %d sessions at once, from %d; at most %d should do", most, peakSessions, before, limit)
	}
}

// TestWorkerReadThreadsStayBounded checks that, as on a worker, a read of the cache
// starts at most its few threads, so that the process's threads are bounded by the
// reads there can be at once: one on each of DuckDB's threads and each session's.
func TestWorkerReadThreadsStayBounded(t *testing.T) {
	n := config.DefaultWorkerCacheReadThreads
	before, most := cacheReadPeak(t, n)
	limit := before + (peakThreads+peakSessions)*n + peakOthers()
	t.Logf("threads: %d before, %d at most while they read, limit %d", before, most, limit)
	if most > limit {
		t.Errorf("the process reached %d threads reading %d sessions at once, from %d, at %d threads a read; at most %d should do", most, peakSessions, before, n, limit)
	}
}
