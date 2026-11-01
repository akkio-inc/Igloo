package prefetch

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// filterFile writes a Parquet file of rows rows, numbered from first, in
// row groups of 10,000, in the order order names (an expression of i): k
// is the row's number, r a hash of it and genre one of 40 values, with
// field IDs 1 to 3, and payload, field ID 4.
func filterFile(t *testing.T, first, rows int, order string) string {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "data.parquet")
	_, err = db.Exec(fmt.Sprintf(`COPY (SELECT i AS k, hash(i) %% 1000000 AS r, 'genre_' || CAST(i %% 40 AS VARCHAR) AS genre,
		md5(CAST(i AS VARCHAR)) AS payload FROM range(%d, %d) t(i) ORDER BY %s)
		TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 10000, FIELD_IDS {k: 1, r: 2, genre: 3, payload: 4})`, first, first+rows, order, path))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// readAhead runs scan, whose files' paths are s3://bucket/t/data/<n>.parquet
// for the local files locals, until its reading ahead has gone quiet, and
// returns each file's blocks in the cache, by path.
func readAhead(t *testing.T, locals []string, scan Scan) map[string][]string {
	t.Helper()
	const block = 64 << 10
	dir := t.TempDir()
	byPath := map[string]string{}
	for i, l := range locals {
		st, err := os.Stat(l)
		if err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("s3://bucket/t/data/%d.parquet", i)
		byPath[path] = l
		scan.Files = append(scan.Files, File{Path: path, Size: st.Size()})
	}
	p, err := New(Options{Dir: dir, BlockSize: block, Concurrency: 8, Open: func(ctx context.Context, path string) (Object, error) {
		f, err := os.Open(byPath[path])
		return &localObject{File: f, etag: `"v1"`}, err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	stop := p.Start(context.Background(), []Scan{scan})
	// Quiet: no new block for a while.
	last, since := -1, time.Now()
	for time.Since(since) < 400*time.Millisecond {
		entries, _ := os.ReadDir(dir)
		if len(entries) != last {
			last, since = len(entries), time.Now()
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	// Each file's blocks moved to a directory of their own, for blockFiles.
	out := map[string][]string{}
	entries, _ := os.ReadDir(dir)
	for path := range byPath {
		own := t.TempDir()
		prefix := BlockName(path, 0, 0)
		prefix = prefix[:len(prefix)-len("0-0")]
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), prefix) {
				if err := os.Rename(filepath.Join(dir, e.Name()), filepath.Join(own, e.Name())); err != nil {
					t.Fatal(err)
				}
			}
		}
		out[path] = blockFiles(t, own, path, `"v1"`)
	}
	return out
}

// TestFilteredScans reads ahead a filtered scan's file in full where no
// filter on the columns it names could skip a row group, and only its
// footer where one could: row groups of narrow ranges of a filtered
// column, files of narrow ranges of it within the scan's, a Bloom filter,
// or no column known.
func TestFilteredScans(t *testing.T) {
	const block = 64 << 10
	sorted := filterFile(t, 0, 200000, "i")
	st, _ := os.Stat(sorted)
	footer := strings.Join(expectedBlocks(st.Size(), block, [][2]int64{{st.Size() - footerGuess, st.Size()}}), " ")
	whole := strings.Join(expectedBlocks(st.Size(), block, append(chunkRanges(t, sorted, "payload"), [2]int64{st.Size() - footerGuess, st.Size()})), " ")
	const path0, path1 = "s3://bucket/t/data/0.parquet", "s3://bucket/t/data/1.parquet"

	// r is in no order: every row group holds values from across its range.
	got := readAhead(t, []string{sorted}, Scan{FieldIDs: []int32{4}, Filtered: true, Filters: []int32{2}})
	if g := strings.Join(got[path0], " "); g != whole {
		t.Errorf("filtered by a column in no order: blocks\n got %s\nwant %s", g, whole)
	}
	// k is the file's order: a filter on it skips row groups.
	got = readAhead(t, []string{sorted}, Scan{FieldIDs: []int32{4}, Filtered: true, Filters: []int32{1, 2}})
	if g := strings.Join(got[path0], " "); g != footer {
		t.Errorf("filtered by the file's order: blocks\n got %s\nwant the footer's %s", g, footer)
	}
	// Columns not known: footers only.
	got = readAhead(t, []string{sorted}, Scan{FieldIDs: []int32{4}, Filtered: true})
	if g := strings.Join(got[path0], " "); g != footer {
		t.Errorf("filtered by columns not known: blocks\n got %s\nwant the footer's %s", g, footer)
	}
	// Not filtered: in full.
	got = readAhead(t, []string{sorted}, Scan{FieldIDs: []int32{4}})
	if g := strings.Join(got[path0], " "); g != whole {
		t.Errorf("not filtered: blocks\n got %s\nwant %s", g, whole)
	}

	// Two files, each of half the values of k in no order within it: a
	// filter on k skips whole files, so neither is read ahead past its
	// footer, the second's read for the decision on the first.
	a, b := filterFile(t, 0, 100000, "hash(i)"), filterFile(t, 100000, 100000, "hash(i)")
	got = readAhead(t, []string{a, b}, Scan{FieldIDs: []int32{4}, Filtered: true, Filters: []int32{1}})
	for i, l := range []string{a, b} {
		st, _ := os.Stat(l)
		want := strings.Join(expectedBlocks(st.Size(), block, [][2]int64{{st.Size() - footerGuess, st.Size()}}), " ")
		if g := strings.Join(got[[]string{path0, path1}[i]], " "); g != want {
			t.Errorf("files dividing the filtered column, file %d: blocks\n got %s\nwant the footer's %s", i, g, want)
		}
	}
	// The same files filtered by r, in no order in either: both in full.
	got = readAhead(t, []string{a, b}, Scan{FieldIDs: []int32{4}, Filtered: true, Filters: []int32{2}})
	for i, l := range []string{a, b} {
		st, _ := os.Stat(l)
		want := strings.Join(expectedBlocks(st.Size(), block, append(chunkRanges(t, l, "payload"), [2]int64{st.Size() - footerGuess, st.Size()})), " ")
		if g := strings.Join(got[[]string{path0, path1}[i]], " "); g != want {
			t.Errorf("files filtered by a column in no order, file %d: blocks\n got %s\nwant %s", i, g, want)
		}
	}

	// A Bloom filter on the filtered column, where DuckDB writes one (for
	// columns it writes with a dictionary), lets an equality skip row
	// groups whatever their ranges.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var blooms int
	if err := db.QueryRow(`SELECT count(*) FROM parquet_metadata(?) WHERE path_in_schema = 'genre' AND bloom_filter_offset > 0`, sorted).Scan(&blooms); err != nil {
		t.Fatal(err)
	}
	if blooms > 0 {
		got = readAhead(t, []string{sorted}, Scan{FieldIDs: []int32{4}, Filtered: true, Filters: []int32{3}})
		if g := strings.Join(got[path0], " "); g != footer {
			t.Errorf("filtered by a column with a Bloom filter: blocks\n got %s\nwant the footer's %s", g, footer)
		}
	} else {
		t.Log("DuckDB wrote no Bloom filter for genre; not checked")
	}
}

// TestSpread compares ranges of order-preserving keys.
func TestSpread(t *testing.T) {
	for _, c := range []struct {
		lo, hi, a, b []byte
		want         float64
	}{
		{intKey(0), intKey(100), intKey(0), intKey(100), 1},
		{intKey(0), intKey(100), intKey(10), intKey(35), 0.25},
		{intKey(-50), intKey(50), intKey(-50), intKey(0), 0.5},
		{floatKey(-1), floatKey(1), floatKey(-1), floatKey(1), 1},
		{[]byte("segment_a"), []byte("segment_z"), []byte("segment_a"), []byte("segment_z"), 1},
		{[]byte("aa"), []byte("aa"), []byte("aa"), []byte("aa"), 1},
	} {
		if got := spread(c.lo, c.hi, c.a, c.b); got < c.want-1e-9 || got > c.want+1e-9 {
			t.Errorf("spread(%x, %x, %x, %x) = %v, want %v", c.lo, c.hi, c.a, c.b, got, c.want)
		}
	}
	if s := spread(floatKey(-1), floatKey(1), floatKey(0.5), floatKey(0.75)); s <= 0 || s >= 0.5 {
		t.Errorf("a quarter of a float range spreads %v", s)
	}
	if s := spread([]byte("segment_a"), []byte("segment_z"), []byte("segment_m"), []byte("segment_n")); s <= 0 || s >= 0.1 {
		t.Errorf("a narrow range of strings spreads %v", s)
	}
}
