package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

func TestRestoreReads(t *testing.T) {
	m := manifest{
		Files: []manifestFile{{Path: "s3://b/w/a.parquet", Size: 100 << 20}, {Path: "s3://b/w/b.parquet", Size: 10 << 20}},
		Ranges: [][3]int64{
			{1, 0, 1 << 20},                  // first used: b's head
			{0, 50 << 20, 1 << 20},           // a, far from the rest of a
			{0, 0, 1 << 20},                  // a's head
			{0, 1<<20 + 100, 1 << 20},        // close after a's head: same request
			{1, 9 << 20, 2 << 20},            // past b's end: dropped
			{0, 2<<20 + 200, restoreRequest}, // would make a's head request too long
		},
	}
	reads := restoreReads(&m, 1<<40)
	type got struct {
		path       string
		start, end int64
		ranges     int
	}
	var out []got
	for _, r := range reads {
		out = append(out, got{r.file.Path, r.start, r.end, len(r.ranges)})
	}
	want := []got{
		{"s3://b/w/b.parquet", 0, 1 << 20, 1},
		{"s3://b/w/a.parquet", 50 << 20, 51 << 20, 1},
		{"s3://b/w/a.parquet", 0, 2<<20 + 100, 2},
		{"s3://b/w/a.parquet", 2<<20 + 200, 2<<20 + 200 + restoreRequest, 1},
	}
	if !slices.Equal(out, want) {
		t.Fatalf("reads\n got %v\nwant %v", out, want)
	}

	// A budget takes ranges in the manifest's order.
	reads = restoreReads(&m, 2<<20)
	if len(reads) != 2 || reads[0].file.Path != "s3://b/w/b.parquet" || reads[1].start != 50<<20 {
		t.Fatalf("with a budget of two ranges: %+v", reads)
	}
}

func TestManifestOrder(t *testing.T) {
	f := &fileCache{first: map[rangeKey]int64{}}
	a := manifestFile{Path: "s3://b/w/a.parquet", Size: 10}
	b := manifestFile{Path: "s3://b/w/b.parquet", Size: 10, Version: []byte(`"e"`)}
	f.number([]cachedRange{{file: b, offset: 4, length: 2}})
	ranges := []cachedRange{{file: a, offset: 0, length: 4}, {file: b, offset: 0, length: 4}, {file: b, offset: 4, length: 2}}
	f.number(ranges)
	m := f.manifestOf(ranges)
	if len(m.Files) != 2 || m.Files[0].Path != b.Path || string(m.Files[0].Version) != `"e"` || m.Files[1].Path != a.Path {
		t.Fatalf("files %+v", m.Files)
	}
	want := [][3]int64{{0, 4, 2}, {1, 0, 4}, {0, 0, 4}}
	if !slices.Equal(m.Ranges, want) {
		t.Fatalf("ranges %v, want %v", m.Ranges, want)
	}
}

func TestReadManifest(t *testing.T) {
	dir := t.TempDir()
	f := &fileCache{path: filepath.Join(dir, fileCacheManifest)}
	if m, err := f.readManifest(); err != nil || len(m.Ranges) != 0 {
		t.Fatalf("no manifest: %+v, %v", m, err)
	}
	if err := os.WriteFile(f.path, []byte(`{"files":[{"path":"s3://b/w/a.parquet","size":10}],"ranges":[[1,0,4]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.readManifest(); err == nil {
		t.Fatal("a range of a file the manifest does not list was read")
	}
	if err := os.WriteFile(f.path, []byte(`{"files":[{"path":"s3://b/w/a.parquet","size":10}],"ranges":[[0,0,4]]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m, err := f.readManifest()
	if err != nil || len(m.Ranges) != 1 || m.Files[0].Path != "s3://b/w/a.parquet" {
		t.Fatalf("read %+v, %v", m, err)
	}
	if err := f.forget(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.path); !os.IsNotExist(err) {
		t.Fatalf("the manifest survived forget: %v", err)
	}
}

func TestSameETag(t *testing.T) {
	if !sameETag(`"abc"`, "abc") || !sameETag("abc", `"abc"`) || sameETag("", "") || sameETag(`"abc"`, `"abd"`) {
		t.Fatal("sameETag")
	}
}

// TestExternalFileCacheGivesWay fills DuckDB's external file cache with
// files read from a stand-in for object storage, to most of DuckDB's memory
// limit, and then runs statements that need that memory: one large hash
// table, a single block of 512 MiB, and many groups, many small blocks.
// DuckDB drops the cached ranges for each, as it did for a single block of 2
// GiB with 5.3 of 6 GB cached, alone and beside scans filling it again.
func TestExternalFileCacheGivesWay(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	exec := func(q string) error {
		_, err := conn.ExecContext(ctx, q, nil)
		return err
	}
	dir := t.TempDir()
	for i := range 4 {
		f := filepath.Join(dir, fmt.Sprintf("t%d.parquet", i))
		q := fmt.Sprintf("COPY (SELECT range AS a, hash(range) AS b, hash(range + 1) AS c FROM range(%d, %d)) TO %s (FORMAT parquet)",
			i*20_000_000, (i+1)*20_000_000, literal(f))
		if err := exec(q); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	defer srv.Close()
	for _, q := range []string{
		"SET extension_directory = " + literal(ext), "LOAD httpfs", "SET threads = 4", "SET memory_limit = '1200MB'",
		"SET enable_external_file_cache = true",
	} {
		if err := exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cached := func() int64 {
		t.Helper()
		v, err := queryString(ctx, conn, "SELECT coalesce(sum(memory_usage_bytes), 0)::VARCHAR FROM duckdb_memory() WHERE tag = 'EXTERNAL_FILE_CACHE'")
		if err != nil {
			t.Fatal(err)
		}
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	fill := func() {
		t.Helper()
		for i := range 4 {
			if err := exec(fmt.Sprintf("SELECT sum(a), sum(b), sum(c) FROM read_parquet('%s/t%d.parquet')", srv.URL, i)); err != nil {
				t.Fatal(err)
			}
		}
		if n := cached(); n < 800<<20 {
			t.Fatalf("the cache holds %d MiB, want most of 1,200", n>>20)
		}
	}
	for _, s := range []struct{ name, q string }{
		{"a large hash table", "SELECT count(*) FROM range(25000000) a(i) JOIN range(25000000) b(j) ON i = j"},
		{"many groups", "SELECT count(*) FROM (SELECT i % 10000000 AS k, sum(i) FROM range(30000000) t(i) GROUP BY 1)"},
	} {
		fill()
		if err := exec(s.q); err != nil {
			t.Errorf("%s, beside %d MiB cached: %v", s.name, cached()>>20, err)
		}
	}
}

// The rb extension lists DuckDB's external file cache and puts ranges back
// into it, where queries read them: a file in a stand-in for object storage
// is read, its ranges listed, the cache emptied and the ranges put back, and
// then the store serves zeros under the same ETag, which DuckDB checks the
// file by, and the query still sees the rows.
func TestRBFileCacheRoundTrip(t *testing.T) {
	rb := testRB(t)
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	connector, err := duckdb.NewConnector(unsignedDSN("", []extensionFile{{name: "rb", path: rb}}), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	exec := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	local := filepath.Join(t.TempDir(), "t.parquet")
	exec("COPY (SELECT range AS a, range * 2 AS b FROM range(300000)) TO " + literal(local) + " (FORMAT parquet, ROW_GROUP_SIZE 50000)")
	data, err := os.ReadFile(local)
	if err != nil {
		t.Fatal(err)
	}
	var serve atomic.Pointer[[]byte]
	serve.Store(&data)
	modified := time.Now().Add(-time.Hour)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"1234-5678"`)
		http.ServeContent(w, r, "t.parquet", modified, bytes.NewReader(*serve.Load()))
	}))
	defer srv.Close()
	for _, q := range []string{
		"SET extension_directory = " + literal(ext), "LOAD httpfs", "LOAD " + literal(rb),
		"SET s3_endpoint = " + literal(strings.TrimPrefix(srv.URL, "http://")), "SET s3_use_ssl = false",
		"SET s3_url_style = 'path'", "SET s3_region = 'us-east-1'", "SET s3_access_key_id = 'x'",
		"SET s3_secret_access_key = 'y'", "SET enable_external_file_cache = true",
	} {
		exec(q)
	}
	path := "s3://bucket/t/data/t.parquet"
	sum := func() string {
		t.Helper()
		v, err := queryString(ctx, conn, "SELECT (sum(a) + sum(b))::VARCHAR FROM read_parquet("+literal(path)+")")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	want := sum()

	e := &Engine{connector: connector}
	ranges, err := e.fileCacheRanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var cached int64
	for _, r := range ranges {
		if r.file.Path != path || r.file.Size != int64(len(data)) || string(r.file.Version) == "" {
			t.Fatalf("range %+v of a file of %d bytes", r, len(data))
		}
		cached += r.length
	}
	if cached < int64(len(data))/2 {
		t.Fatalf("the query left %d bytes of %d in DuckDB's cache", cached, len(data))
	}

	exec("SET enable_external_file_cache = false")
	exec("SET enable_external_file_cache = true")
	if left, err := e.fileCacheRanges(ctx); err != nil || len(left) != 0 {
		t.Fatalf("after emptying the cache: %d ranges, %v", len(left), err)
	}
	for _, r := range ranges {
		status, err := putRange(ctx, conn, &r.file, r.offset, data[r.offset:r.offset+r.length])
		if err != nil || status != putDone {
			t.Fatalf("put %+v: %d, %v", r, status, err)
		}
		if status, err := putRange(ctx, conn, &r.file, r.offset, data[r.offset:r.offset+r.length]); err != nil || status != putSkipped {
			t.Fatalf("put again: %d, %v", status, err)
		}
	}
	// A file DuckDB holds under another version takes nothing.
	other := ranges[0].file
	other.Version = []byte(`"another"`)
	if status, err := putRange(ctx, conn, &other, 0, data[:4]); err != nil || status != putSkipped {
		t.Fatalf("put of another version: %d, %v", status, err)
	}
	// Nor does a range past the file's end.
	if status, err := putRange(ctx, conn, &ranges[0].file, int64(len(data))-2, data[:4]); err != nil || status != putSkipped {
		t.Fatalf("put past the end: %d, %v", status, err)
	}

	zeros := make([]byte, len(data))
	serve.Store(&zeros)
	if got := sum(); got != want {
		t.Fatalf("read %s from the cache, want %s", got, want)
	}
}
