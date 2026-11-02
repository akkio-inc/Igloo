package prefetch

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestWriteBlockAlone writes one block from several writers at once, as
// statements reading the same files ahead do, while a reader reads it: the
// reader always finds it whole. Two writes of one temporary file, the
// second truncating what the first had renamed into place, left DuckDB
// reading a block short.
func TestWriteBlockAlone(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, BlockName("s3://b/t/f.parquet", 0, 1<<20))
	data := bytes.Repeat([]byte("igloo"), 200<<10)
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for range 4 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = writeBlock(dst, "s3://b/t/f.parquet", `"v1"`, data)
			}
		}()
	}
	short := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := os.ReadFile(dst)
		if err == nil && !bytes.Equal(got, data) {
			short++
		}
	}
	close(stop)
	writers.Wait()
	if short > 0 {
		t.Errorf("read the block %d times not whole", short)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*"+tmpSuffix)); len(left) > 0 {
		t.Errorf("temporary files left: %v", left)
	}
}

// TestNoUnversionedBlocks reads nothing ahead of a file whose object has
// no ETag: cache_httpfs would take such a block for stale and delete it.
func TestNoUnversionedBlocks(t *testing.T) {
	local := testFile(t, 100000, 10000)
	st, _ := os.Stat(local)
	dir := t.TempDir()
	p, err := New(Options{Dir: dir, BlockSize: 64 << 10, Concurrency: 4, Open: func(ctx context.Context, _ string) (Object, error) {
		f, err := os.Open(local)
		return &localObject{File: f}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: "s3://b/t/f.parquet", Size: st.Size()}}, Whole: true}})
	time.Sleep(300 * time.Millisecond)
	stop()
	if entries, _ := os.ReadDir(dir); len(entries) > 0 {
		t.Errorf("%d files read ahead of an object without an ETag", len(entries))
	}
}

// TestNoRoomNoBlocks reads nothing ahead, not even a file's footer, while
// the cache has no room, where cache_httpfs evicts a block on each miss and
// a block read ahead takes another's place; once there is room, it reads
// ahead.
func TestNoRoomNoBlocks(t *testing.T) {
	local := testFile(t, 100000, 10000)
	st, _ := os.Stat(local)
	for name, mode := range map[string]tracking{"access times": byAccessTime, "opens": byOpens} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			var room atomic.Int64
			p, err := New(Options{Dir: dir, BlockSize: 64 << 10, Concurrency: 4, Room: room.Load,
				Open: func(ctx context.Context, _ string) (Object, error) {
					f, err := os.Open(local)
					return &localObject{File: f, etag: `"v1"`}, err
				}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			withTracking(t, p, mode)
			stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: "s3://b/t/f.parquet", Size: st.Size()}}, Whole: true}})
			defer stop()
			count := func() int {
				entries, _ := os.ReadDir(dir)
				return len(entries)
			}
			time.Sleep(300 * time.Millisecond)
			if n := count(); n > 0 {
				t.Fatalf("%d files read ahead with no room in the cache", n)
			}
			room.Store(1 << 30)
			waitFor(t, "blocks once there is room", func() bool { return count() > 0 })
		})
	}
}

// TestBlockRace tells DuckDB's I/O errors over a block's file of the read
// cache, which a read a moment later does not meet, from others.
func TestBlockRace(t *testing.T) {
	block := "/var/lib/igloo/cache/blocks/" + BlockName("s3://bucket/t/data/f.parquet", 1048576, 524288)
	for _, c := range []struct {
		msg  string
		race bool
	}{
		{`IO Error: Could not remove file "` + block + `": No such file or directory`, true},
		{`task 3 failed: IO Error: Could not read enough bytes from file "` + block + `": attempted to read 524288 bytes from location 0`, true},
		{`IO Error: Could not remove file "/var/lib/igloo/tmp/x.parquet": No such file or directory`, false},
		{`HTTP Error: Request returned HTTP 404 for HTTP GET to '` + block + `'`, false},
		{`Conversion Error: Could not convert string "` + block + `" to INT32`, false},
	} {
		if got := BlockRace(errors.New(c.msg)); got != c.race {
			t.Errorf("%q: race %v, want %v", c.msg, got, c.race)
		}
	}
	if BlockRace(nil) {
		t.Error("no error taken for a race")
	}
}

// TestCacheHTTPFSReadAheadUnderEviction reads a file with DuckDB through
// cache_httpfs from several connections while several statements read the
// same blocks ahead, and cache_httpfs, its disk short of the free space it
// keeps, evicts a block on each miss, as on a worker whose disk the cache
// and the exchanges filled. Every read succeeds, at most after running
// again a read that raced another over a block (BlockRace), as the worker
// runs a task again; none reads a block short. Read ahead from an object
// without an ETag, which cache_httpfs would delete, nothing lands, and
// with room as the engine tells it, the prefetcher writes nothing past the
// extension's floor.
func TestCacheHTTPFSReadAheadUnderEviction(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	local := testFile(t, 300000, 20000)
	st, _ := os.Stat(local)
	const etag = `"1234-5678"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
		http.ServeFile(w, r, local)
	}))
	defer srv.Close()
	const path = "s3://bucket/t/data/f.parquet"
	// floor is above the disk's free space: cache_httpfs stores nothing and
	// evicts a block on each miss.
	const floor = int64(1) << 60
	for _, c := range []struct {
		name string
		// objectTag is the ETag the prefetcher's object reports; live has
		// it ask for the room as the engine tells it.
		objectTag string
		live      bool
	}{
		{"unbounded room", etag, false},
		{"no etag", "", false},
		{"room as the engine tells it", etag, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Dir: dir, Concurrency: 32, Open: func(ctx context.Context, _ string) (Object, error) {
				f, err := os.Open(local)
				return &localObject{File: f, etag: c.objectTag}, err
			}}
			if c.live {
				opts.Room = func() int64 {
					var fs syscall.Statfs_t
					if err := syscall.Statfs(dir, &fs); err != nil {
						return 0
					}
					return int64(fs.Bavail)*int64(fs.Bsize) - floor
				}
			}
			p, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			host := strings.TrimPrefix(srv.URL, "http://")
			for _, q := range []string{
				"SET extension_directory = '" + ext + "'", "LOAD httpfs", "LOAD cache_httpfs", "SET GLOBAL threads = 8",
				"SET GLOBAL s3_endpoint = '" + host + "'", "SET GLOBAL s3_use_ssl = false", "SET GLOBAL s3_url_style = 'path'",
				"SET GLOBAL s3_region = 'us-east-1'", "SET GLOBAL s3_access_key_id = 'x'", "SET GLOBAL s3_secret_access_key = 'y'",
				// As engine.cacheStatements sets the cache up.
				"SET GLOBAL cache_httpfs_enable_glob_cache = false", "SET GLOBAL cache_httpfs_enable_metadata_cache = false",
				"SET GLOBAL cache_httpfs_enable_file_handle_cache = false", "SET GLOBAL cache_httpfs_enable_cache_validation = true",
				"SET GLOBAL cache_httpfs_cache_directory = '" + dir + "'", "SET GLOBAL cache_httpfs_evict_policy = 'lru_sp'",
				"SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = 1152921504606846976",
				"SET GLOBAL cache_httpfs_disk_cache_reader_enable_memory_cache = false",
				"SET GLOBAL enable_external_file_cache = false", "SET GLOBAL cache_httpfs_parallel_read_mode = 'duckdb_task_scheduler'",
				"SET GLOBAL cache_httpfs_type = 'on_disk'",
			} {
				if _, err := db.Exec(q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			db.SetMaxOpenConns(8)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for range 3 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for ctx.Err() == nil {
						stop := p.Start(ctx, []Scan{{Files: []File{{Path: path, Size: st.Size()}}, Whole: true}})
						time.Sleep(10 * time.Millisecond)
						stop()
					}
				}()
			}
			var reads, raced atomic.Int64
			var mu sync.Mutex
			var failures []string
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for ctx.Err() == nil {
						var err error
						for again := 0; again <= 3; again++ {
							var n int64
							err = db.QueryRowContext(context.Background(), "SELECT count(DISTINCT title) FROM read_parquet('"+path+"')").Scan(&n)
							if err == nil && n != 300000 {
								err = errors.New("wrong count")
							}
							if !BlockRace(err) {
								break
							}
							raced.Add(1)
							if strings.Contains(err.Error(), "Could not read enough bytes") {
								mu.Lock()
								failures = append(failures, "short block: "+err.Error())
								mu.Unlock()
							}
						}
						reads.Add(1)
						if err != nil {
							mu.Lock()
							failures = append(failures, err.Error())
							mu.Unlock()
						}
					}
				}()
			}
			wg.Wait()
			t.Logf("%d reads, %d run again after a race over a block", reads.Load(), raced.Load())
			if len(failures) > 0 {
				t.Errorf("%d reads failed; first: %s", len(failures), failures[0])
			}
			if c.objectTag == "" || c.live {
				if n := testutil.ToFloat64(p.blocks); n > 0 {
					t.Errorf("the prefetcher wrote %v blocks", n)
				}
			}
		})
	}
}
