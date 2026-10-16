package sharing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

func dataFileAt(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content, path, iceberg.ParquetFile, nil, nil, nil, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	return b.NullValueCounts(map[int]int64{1: 2}).
		LowerBoundValues(map[int][]byte{1: bound(t, iceberg.Int64Literal(1))}).
		UpperBoundValues(map[int][]byte{1: bound(t, iceberg.Int64Literal(9))}).Build()
}

func TestCopyPaths(t *testing.T) {
	m := newMaterializer(iceio.LocalFS{}, "s3://b/w/")
	data := dataFileAt(t, "s3://b/w/t/data/1.parquet", iceberg.EntryContentData)
	del1 := dataFileAt(t, "s3://b/w/t/data/d1.parquet", iceberg.EntryContentPosDeletes)
	del2 := dataFileAt(t, "s3://b/w/t/data/d2.parquet", iceberg.EntryContentPosDeletes)

	a := m.copyPath("uuid", table.FileScanTask{File: data, DeleteFiles: []iceberg.DataFile{del1, del2}})
	b := m.copyPath("uuid", table.FileScanTask{File: data, DeleteFiles: []iceberg.DataFile{del2, del1}})
	c := m.copyPath("uuid", table.FileScanTask{File: data, DeleteFiles: []iceberg.DataFile{del1}})
	if a != b || a == c || !strings.HasPrefix(a, "s3://b/w/.igloo/sharing/uuid/") || !strings.HasSuffix(a, ".parquet") {
		t.Errorf("copy paths %q %q %q", a, b, c)
	}

	// A copy keeps the original's bounds, which still hold, with its own
	// row count, and no null counts, which may have changed.
	s := iceberg.NewSchema(0, field(1, "id", iceberg.PrimitiveTypes.Int64, false))
	var st map[string]any
	if err := json.Unmarshal([]byte(copyStats(data, s, 4)), &st); err != nil {
		t.Fatal(err)
	}
	if st["numRecords"] != 4.0 || st["nullCount"] != nil || st["minValues"].(map[string]any)["id"] != 1.0 {
		t.Errorf("copy stats %v", st)
	}
}

func TestCollectCopies(t *testing.T) {
	dir := t.TempDir()
	m := newMaterializer(iceio.LocalFS{}, dir+"/")
	root := filepath.Join(dir, ".igloo", "sharing", "uuid")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * materializedRetention)
	for name, mtime := range map[string]time.Time{"needed.parquet": old, "stale.parquet": old, "recent.parquet": time.Now()} {
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	n, err := m.collect(map[string]bool{filepath.Join(root, "needed.parquet"): true}, time.Now())
	if err != nil || n != 1 {
		t.Fatalf("collect = %d, %v", n, err)
	}
	for name, want := range map[string]bool{"needed.parquet": true, "stale.parquet": false, "recent.parquet": true} {
		if _, err := os.Stat(filepath.Join(root, name)); (err == nil) != want {
			t.Errorf("%s exists: %v, want %v", name, err == nil, want)
		}
	}
	// No sharing area yet is not an error.
	if _, err := newMaterializer(iceio.LocalFS{}, t.TempDir()+"/").collect(nil, time.Now()); err != nil {
		t.Errorf("collect without copies: %v", err)
	}
}
