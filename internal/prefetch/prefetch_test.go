package prefetch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/sys/unix"
)

// localObject reads a local file as an object.
type localObject struct {
	*os.File
	etag  string
	delay time.Duration
	reads *atomic.Int64
}

func (o *localObject) ReadAt(p []byte, off int64) (int, error) {
	if o.delay > 0 {
		time.Sleep(o.delay)
	}
	if o.reads != nil {
		o.reads.Add(1)
	}
	return o.File.ReadAt(p, off)
}

func (o *localObject) ETag() string { return o.etag }

// testFile writes a Parquet file of four columns, with field IDs 1 to 4,
// in row groups of group rows, and returns its path.
func testFile(t *testing.T, rows, group int) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "data.parquet")
	_, err = db.Exec(fmt.Sprintf(`COPY (SELECT md5(CAST(i AS VARCHAR)) AS id, i * 1.5 AS secs, 'genre_' || CAST(i %% 40 AS VARCHAR) AS genre,
		md5(CAST(i * 7 AS VARCHAR)) || md5(CAST(i * 11 AS VARCHAR)) AS title
		FROM range(%d) r(i)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE %d, FIELD_IDS {id: 1, secs: 2, genre: 3, title: 4})`, rows, path, group))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// chunkRanges returns, from DuckDB's parquet_metadata, the byte ranges of
// the column chunks of column name in the file at path.
func chunkRanges(t *testing.T, path, name string) [][2]int64 {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT coalesce(nullif(dictionary_page_offset, 0), data_page_offset), total_compressed_size
		FROM parquet_metadata(?) WHERE path_in_schema = ?`, path, name)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var start, n int64
		if err := rows.Scan(&start, &n); err != nil {
			t.Fatal(err)
		}
		out = append(out, [2]int64{start, start + n})
	}
	return out
}

func TestBlockName(t *testing.T) {
	sum := sha256.Sum256([]byte("s3://bucket/t/data/x.parquet"))
	if got, want := BlockName("s3://bucket/t/data/x.parquet", 524288, 1000), hex.EncodeToString(sum[:])+"-x.parquet-524288-1000"; got != want {
		t.Errorf("BlockName: %s, want %s", got, want)
	}
	p := &Prefetcher{opts: Options{BlockSize: 100}}
	f := &openFile{File: File{Path: "s3://b/f", Size: 350}}
	var got []string
	for _, b := range p.blocksOf(f, [][2]int64{{250, 320}, {10, 20}, {90, 110}, {340, 350}, {5, 5}}) {
		got = append(got, fmt.Sprintf("%d+%d", b.offset, b.length))
	}
	if strings.Join(got, " ") != "0+100 100+100 200+100 300+50" {
		t.Errorf("blocks: %v", got)
	}
}

// blockFiles lists the blocks in dir, as offset+length, and checks each is
// marked as cache_httpfs marks its own.
func blockFiles(t *testing.T, dir, path, etag string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	prefix := BlockName(path, 0, 0)
	prefix = prefix[:len(prefix)-len("0-0")]
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), tmpSuffix) {
			// Being written.
			continue
		}
		rest, ok := strings.CutPrefix(e.Name(), prefix)
		if !ok {
			t.Errorf("a file of another name: %s", e.Name())
			continue
		}
		buf := make([]byte, 256)
		n, err := unix.Getxattr(filepath.Join(dir, e.Name()), pathAttr, buf)
		if err != nil || string(buf[:n]) != path {
			t.Errorf("%s: path attribute %q, %v", e.Name(), buf[:max(n, 0)], err)
		}
		n, err = unix.Getxattr(filepath.Join(dir, e.Name()), versionAttr, buf)
		if err != nil || string(buf[:n]) != etag {
			t.Errorf("%s: version attribute %q, %v", e.Name(), buf[:max(n, 0)], err)
		}
		out = append(out, strings.Replace(rest, "-", "+", 1))
	}
	slices.SortFunc(out, func(a, b string) int {
		x, _ := strconv.ParseInt(strings.Split(a, "+")[0], 10, 64)
		y, _ := strconv.ParseInt(strings.Split(b, "+")[0], 10, 64)
		return int(x - y)
	})
	return out
}

func expectedBlocks(size, block int64, ranges [][2]int64) []string {
	p := &Prefetcher{opts: Options{BlockSize: block}}
	var out []string
	for _, b := range p.blocksOf(&openFile{File: File{Size: size}}, ranges) {
		out = append(out, fmt.Sprintf("%d+%d", b.offset, b.length))
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestColumns reads ahead the blocks of one column of a file, and its
// footer, and nothing else, each marked as cache_httpfs marks its blocks.
func TestColumns(t *testing.T) {
	local := testFile(t, 200000, 10000)
	st, _ := os.Stat(local)
	const path, etag, block = "s3://bucket/t/data/f.parquet", `"v1"`, 64 << 10
	dir := t.TempDir()
	p, err := New(Options{Dir: dir, BlockSize: block, Concurrency: 8, Open: func(ctx context.Context, p string) (Object, error) {
		if p != path {
			return nil, fmt.Errorf("opened %s", p)
		}
		f, err := os.Open(local)
		return &localObject{File: f, etag: etag}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ranges := append(chunkRanges(t, local, "secs"), [2]int64{st.Size() - footerGuess, st.Size()})
	want := expectedBlocks(st.Size(), block, ranges)
	stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}, FieldIDs: []int32{2}}})
	waitFor(t, "the blocks", func() bool { return len(blockFiles(t, dir, path, etag)) >= len(want) })
	stop()
	time.Sleep(100 * time.Millisecond)
	if got := blockFiles(t, dir, path, etag); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("blocks\n got %v\nwant %v", got, want)
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*"+tmpSuffix)); len(left) > 0 {
		t.Errorf("blocks left half written: %v", left)
	}
	// A scan of no column, as of COUNT(*), reads the footer alone.
	footerOnly := t.TempDir()
	p3, err := New(Options{Dir: footerOnly, BlockSize: block, Open: func(ctx context.Context, _ string) (Object, error) {
		f, err := os.Open(local)
		return &localObject{File: f, etag: etag}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p3.Close()
	stop = p3.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}}})
	footer := expectedBlocks(st.Size(), block, [][2]int64{{st.Size() - footerGuess, st.Size()}})
	waitFor(t, "the footer", func() bool { return len(blockFiles(t, footerOnly, path, etag)) >= len(footer) })
	time.Sleep(200 * time.Millisecond)
	stop()
	if got := blockFiles(t, footerOnly, path, etag); strings.Join(got, " ") != strings.Join(footer, " ") {
		t.Errorf("blocks of a scan of no column\n got %v\nwant %v", got, footer)
	}

	// What is in the cache already is not fetched again.
	var reads atomic.Int64
	p2, err := New(Options{Dir: dir, BlockSize: block, Open: func(ctx context.Context, _ string) (Object, error) {
		f, err := os.Open(local)
		return &localObject{File: f, etag: etag, reads: &reads}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	stop = p2.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}, FieldIDs: []int32{2}}})
	time.Sleep(300 * time.Millisecond)
	stop()
	if reads.Load() != 0 {
		t.Errorf("%d blocks read again", reads.Load())
	}
}

// TestReadAheadOnce checks that a file a statement read ahead is not looked at
// again for the same columns, that it is for others, and that it is after Hold,
// which empties the cache.
func TestReadAheadOnce(t *testing.T) {
	local := testFile(t, 200000, 10000)
	st, _ := os.Stat(local)
	const path, etag, block = "s3://bucket/t/data/f.parquet", `"v1"`, 64 << 10
	dir := t.TempDir()
	var opens atomic.Int64
	p, err := New(Options{Dir: dir, BlockSize: block, Concurrency: 8, Open: func(ctx context.Context, _ string) (Object, error) {
		opens.Add(1)
		f, err := os.Open(local)
		return &localObject{File: f, etag: etag}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	scan := func(ids ...int32) Scan { return Scan{Files: []File{{Path: path, Size: st.Size()}}, FieldIDs: ids} }
	start := func(sc Scan) {
		t.Helper()
		stop := p.Start(context.Background(), []Scan{sc})
		key := path + "\x00" + readColumns(sc)
		waitFor(t, "the file read ahead", func() bool { return p.alreadyRead(key) })
		stop()
	}
	start(scan(2))
	if opens.Load() == 0 {
		t.Fatal("the first statement fetched nothing")
	}
	looked := testutil.ToFloat64(p.present) + testutil.ToFloat64(p.blocks)
	stop := p.Start(context.Background(), []Scan{scan(2)})
	time.Sleep(100 * time.Millisecond)
	stop()
	if got := testutil.ToFloat64(p.present) + testutil.ToFloat64(p.blocks); got != looked {
		t.Errorf("the next statement of the same columns looked at %v blocks", got-looked)
	}
	start(scan(3))
	if got := testutil.ToFloat64(p.present) + testutil.ToFloat64(p.blocks); got == looked {
		t.Error("a statement of other columns looked at no block")
	}
	looked = testutil.ToFloat64(p.present) + testutil.ToFloat64(p.blocks)
	if err := p.Hold(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	start(scan(2))
	if got := testutil.ToFloat64(p.present) + testutil.ToFloat64(p.blocks); got == looked {
		t.Error("after Hold, a statement looked at no block")
	}
}

// withTracking makes p tell the blocks DuckDB read as mode says, or skips
// the test where the platform or the file system cannot.
func withTracking(t *testing.T, p *Prefetcher, mode tracking) {
	t.Helper()
	switch mode {
	case byAccessTime:
		if p.track != byAccessTime {
			t.Skip("the test directory's file system keeps no access times")
		}
	case byOpens:
		if p.watch == nil {
			w, err := newOpenWatch(p.opts.Dir)
			if err != nil {
				t.Skipf("no file events: %v", err)
			}
			p.watch = w
		}
		p.track = byOpens
	case bounded:
		p.track = bounded
	}
}

// TestAhead keeps a statement's blocks that DuckDB has not read within its
// share of the cache's room, and goes on as DuckDB reads them; those it
// skips count as overfetch. It tells the blocks DuckDB read by their access
// times, and by the events of their opens, as on a file system mounted
// noatime.
func TestAhead(t *testing.T) {
	local := testFile(t, 400000, 10000)
	st, _ := os.Stat(local)
	const path, block = "s3://bucket/t/data/f.parquet", 64 << 10
	saved := minAhead
	minAhead = 3 * block
	defer func() { minAhead = saved }()
	for name, mode := range map[string]tracking{"access times": byAccessTime, "opens": byOpens} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			// A sliver of room: minAhead's worth ahead.
			p, err := New(Options{Dir: dir, BlockSize: block, Concurrency: 4, Room: func() int64 { return 1 },
				Open: func(ctx context.Context, _ string) (Object, error) {
					f, err := os.Open(local)
					return &localObject{File: f, etag: `"v1"`}, err
				}})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			withTracking(t, p, mode)
			stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}, Whole: true}})
			defer stop()
			count := func() int {
				entries, _ := os.ReadDir(dir)
				return len(entries)
			}
			// The footer's blocks, then three ahead.
			waitFor(t, "the first blocks", func() bool { return count() >= 3 })
			time.Sleep(300 * time.Millisecond)
			first := count()
			if first > 5 {
				t.Fatalf("%d blocks ahead of a scan that read none", first)
			}
			// DuckDB reads them all; the prefetcher goes on.
			entries, _ := os.ReadDir(dir)
			for _, e := range entries {
				if _, err := os.ReadFile(filepath.Join(dir, e.Name())); err != nil {
					t.Fatal(err)
				}
			}
			waitFor(t, "more blocks", func() bool { return count() > first })
		})
	}
}

// TestBounded reads ahead, where the prefetcher cannot tell which blocks
// DuckDB read, at most boundedAhead for a statement however much DuckDB
// reads, and nothing past its share of the cache's free room, so reading
// ahead evicts nothing: with no room, not even a file's footer.
func TestBounded(t *testing.T) {
	local := testFile(t, 400000, 10000)
	st, _ := os.Stat(local)
	const path, block = "s3://bucket/t/data/f.parquet", 64 << 10
	saved := boundedAhead
	boundedAhead = 4 * block
	defer func() { boundedAhead = saved }()
	footer := len(expectedBlocks(st.Size(), block, [][2]int64{{st.Size() - footerGuess, st.Size()}}))
	for _, c := range []struct {
		room int64
		want int
	}{{1 << 40, footer + 4}, {0, 0}} {
		dir := t.TempDir()
		p, err := New(Options{Dir: dir, BlockSize: block, Concurrency: 4, Room: func() int64 { return c.room },
			Open: func(ctx context.Context, _ string) (Object, error) {
				f, err := os.Open(local)
				return &localObject{File: f, etag: `"v1"`}, err
			}})
		if err != nil {
			t.Fatal(err)
		}
		withTracking(t, p, bounded)
		stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}, Whole: true}})
		count := func() int {
			entries, _ := os.ReadDir(dir)
			return len(entries)
		}
		waitFor(t, "the first blocks", func() bool { return count() >= c.want })
		if c.want == 0 {
			time.Sleep(300 * time.Millisecond)
		}
		// DuckDB reads them all, which changes nothing.
		for range 3 {
			entries, _ := os.ReadDir(dir)
			for _, e := range entries {
				_, _ = os.ReadFile(filepath.Join(dir, e.Name()))
			}
			time.Sleep(150 * time.Millisecond)
		}
		if n := count(); n != c.want {
			t.Errorf("room %d: %d blocks read ahead, want %d (with room, the footer's %d and at most 4 more)", c.room, n, c.want, footer)
		}
		stop()
		p.Close()
	}
}

// TestSharing lets concurrent statements take the requests in flight in
// turn.
func TestSharing(t *testing.T) {
	local := testFile(t, 400000, 10000)
	st, _ := os.Stat(local)
	dir := t.TempDir()
	var mu sync.Mutex
	var order []string
	p, err := New(Options{Dir: dir, BlockSize: 64 << 10, Concurrency: 1, Open: func(ctx context.Context, path string) (Object, error) {
		f, err := os.Open(local)
		return &recordingObject{localObject: localObject{File: f, etag: `"v1"`}, path: path, mu: &mu, order: &order}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stopA := p.Start(context.Background(), []Scan{{Files: []File{{Path: "s3://b/a/f.parquet", Size: st.Size()}}, Whole: true}})
	stopB := p.Start(context.Background(), []Scan{{Files: []File{{Path: "s3://b/b/f.parquet", Size: st.Size()}}, Whole: true}})
	waitFor(t, "reads of both", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) >= 20
	})
	stopA()
	stopB()
	mu.Lock()
	defer mu.Unlock()
	a := 0
	for _, o := range order[:20] {
		if strings.Contains(o, "/a/") {
			a++
		}
	}
	if a < 6 || a > 14 {
		t.Errorf("of the first 20 reads, %d were statement A's: %v", a, order[:20])
	}
}

type recordingObject struct {
	localObject
	path  string
	mu    *sync.Mutex
	order *[]string
}

func (o *recordingObject) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(2 * time.Millisecond)
	o.mu.Lock()
	*o.order = append(*o.order, o.path)
	o.mu.Unlock()
	return o.File.ReadAt(p, off)
}

// TestCacheHTTPFSReadsBlocks checks the naming and marking against the
// cache_httpfs extension Igloo bakes into its image: after the prefetcher
// read a column ahead, DuckDB reads it without a request to object
// storage, with the cache set up as Igloo sets it up. A new version of the
// extension that names or marks blocks otherwise fails here.
func TestCacheHTTPFSReadsBlocks(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	local := testFile(t, 300000, 100000)
	st, _ := os.Stat(local)
	const etag = `"1234-5678"`
	var gets atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket/t/data/f.parquet" && r.URL.Path != "/bucket/t/data/other.parquet" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", st.ModTime().UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		http.ServeFile(w, r, local)
	}))
	defer srv.Close()
	const path = "s3://bucket/t/data/f.parquet"
	dir := t.TempDir()
	p, err := New(Options{Dir: dir, Open: func(ctx context.Context, _ string) (Object, error) {
		f, err := os.Open(local)
		return &localObject{File: f, etag: etag}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stop := p.Start(context.Background(), []Scan{{Files: []File{{Path: path, Size: st.Size()}}, FieldIDs: []int32{1}}})
	want := expectedBlocks(st.Size(), DefaultBlockSize, append(chunkRanges(t, local, "id"), [2]int64{st.Size() - footerGuess, st.Size()}))
	waitFor(t, "the blocks", func() bool { return len(blockFiles(t, dir, path, etag)) >= len(want) })
	stop()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	host := strings.TrimPrefix(srv.URL, "http://")
	for _, q := range []string{
		"SET extension_directory = '" + ext + "'", "LOAD httpfs", "LOAD cache_httpfs",
		"SET s3_endpoint = '" + host + "'", "SET s3_use_ssl = false", "SET s3_url_style = 'path'", "SET s3_region = 'us-east-1'",
		"SET s3_access_key_id = 'x'", "SET s3_secret_access_key = 'y'",
		// As engine.cacheStatements sets the cache up.
		"SET GLOBAL cache_httpfs_enable_glob_cache = false", "SET GLOBAL cache_httpfs_enable_metadata_cache = false",
		"SET GLOBAL cache_httpfs_enable_file_handle_cache = false", "SET GLOBAL cache_httpfs_enable_cache_validation = true",
		"SET GLOBAL cache_httpfs_cache_directory = '" + dir + "'", "SET GLOBAL cache_httpfs_evict_policy = 'lru_sp'",
		"SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = 1", "SET GLOBAL cache_httpfs_disk_cache_reader_enable_memory_cache = false",
		"SET GLOBAL cache_httpfs_type = 'on_disk'",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	// Where file events tell reads (Linux), they show each block DuckDB
	// reads, as a file system mounted noatime needs (see openWatch).
	watch, werr := newOpenWatch(dir)
	if werr == nil {
		defer watch.close()
		for _, b := range blockFiles(t, dir, path, etag) {
			off, length, _ := strings.Cut(b, "+")
			o, _ := strconv.ParseInt(off, 10, 64)
			l, _ := strconv.ParseInt(length, 10, 64)
			watch.track(BlockName(path, o, l))
		}
	}
	var n int64
	if err := db.QueryRow("SELECT count(DISTINCT id) FROM read_parquet('" + path + "')").Scan(&n); err != nil || n != 300000 {
		t.Fatalf("count: %d, %v", n, err)
	}
	if g := gets.Load(); g != 0 {
		t.Errorf("DuckDB made %d GET requests for blocks the prefetcher had read ahead", g)
	}
	if werr == nil {
		for _, b := range blockFiles(t, dir, path, etag) {
			off, length, _ := strings.Cut(b, "+")
			o, _ := strconv.ParseInt(off, 10, 64)
			l, _ := strconv.ParseInt(length, 10, 64)
			if read, ok := watch.readSince(BlockName(path, o, l)); ok && !read {
				t.Errorf("no open of block %s seen as DuckDB read the column", b)
			}
		}
	}
	// Where reads move access times, those tell the blocks DuckDB read
	// too, though cache_httpfs sets a block's access and modification
	// times both to the time it reads it.
	if atime, _ := probe(dir); atime {
		for _, b := range blockFiles(t, dir, path, etag) {
			off, length, _ := strings.Cut(b, "+")
			o, _ := strconv.ParseInt(off, 10, 64)
			l, _ := strconv.ParseInt(length, 10, 64)
			if read, ok := readSince(filepath.Join(dir, BlockName(path, o, l))); ok && !read {
				t.Errorf("block %s does not show as read by its access time after DuckDB read the column", b)
			}
		}
	}
	// A column it did not read ahead is fetched as usual.
	if err := db.QueryRow("SELECT count(DISTINCT title) FROM read_parquet('" + path + "')").Scan(&n); err != nil || n != 300000 {
		t.Fatalf("count of titles: %d, %v", n, err)
	}
	if gets.Load() == 0 {
		t.Error("DuckDB found the other column's blocks too, so the test proves nothing")
	}
	// cache_httpfs evicts the blocks the prefetcher wrote as it evicts its
	// own: with a free-space floor above the disk's, every block it adds
	// makes it drop the least recently used.
	before := len(blockFiles(t, dir, path, etag))
	if _, err := db.Exec("SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = 1000000000000000000"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT count(DISTINCT title) FROM read_parquet('s3://bucket/t/data/other.parquet')"); err != nil {
		t.Fatal(err)
	}
	if after := len(blockFiles(t, dir, path, etag)); after >= before {
		t.Errorf("cache_httpfs evicted none of the prefetched blocks: %d before, %d after", before, after)
	}
}

// An open that the end of its statement cuts short is not an error: a
// worker's task ends, and with it its reading ahead, while opens are in
// flight. One that fails while the statement runs is.
func TestFetchCanceled(t *testing.T) {
	opened := make(chan struct{}, 1)
	p, err := New(Options{Dir: t.TempDir(), Open: func(ctx context.Context, path string) (Object, error) {
		if strings.HasSuffix(path, "missing.parquet") {
			return nil, errors.New("no such object")
		}
		opened <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- p.fetch(ctx, block{file: &openFile{File: File{Path: "s3://b/t/f.parquet", Size: 100}}, length: 10, name: "a"})
	}()
	<-opened
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("fetch: %v", err)
	}
	if n := testutil.ToFloat64(p.failed); n != 0 {
		t.Errorf("an open the statement's end cut short counted as %v errors", n)
	}
	err = p.fetch(context.Background(), block{file: &openFile{File: File{Path: "s3://b/t/missing.parquet", Size: 100}}, length: 10, name: "b"})
	if err == nil || testutil.ToFloat64(p.failed) != 1 {
		t.Errorf("a failed open: %v, %v errors", err, testutil.ToFloat64(p.failed))
	}
}
