package deletefiles

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// A file is one file however a table's metadata, a listing or a position
// delete file spells it; two files are two.
func TestPaths(t *testing.T) {
	s := Paths{}
	s.Add("s3a://bucket/wh/t/data/a.parquet")
	s.Add("file:///tmp/wh/t/data/b.parquet")
	for _, p := range []string{"s3://bucket/wh/t/data/a.parquet", "s3://bucket//wh/t/data/a.parquet", "/tmp/wh/t/data/b.parquet",
		"file://localhost/tmp/wh/t/data/b.parquet", "/tmp/wh/t/data/%62.parquet"} {
		if !s.Has(p) {
			t.Errorf("%s is not in the set", p)
		}
	}
	for _, p := range []string{"s3://bucket/wh/u/data/a.parquet", "/tmp/wh/t/data/c.parquet"} {
		if s.Has(p) {
			t.Errorf("%s is in the set", p)
		}
	}
}

// posDelete is a position delete file at path whose manifest bounds say it
// deletes rows of target alone, or, with target "", says nothing.
func posDelete(t *testing.T, path, target string) iceberg.DataFile {
	t.Helper()
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, path, iceberg.ParquetFile,
		nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if target != "" {
		b.LowerBoundValues(map[int][]byte{filePathID: []byte(target)}).UpperBoundValues(map[int][]byte{filePathID: []byte(target)})
	}
	return b.Build()
}

// dataFile is a data file at path.
func dataFile(t *testing.T, path string) iceberg.DataFile {
	t.Helper()
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path, iceberg.ParquetFile, nil, nil, nil, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	return b.Build()
}

// A delete file's target counts as replaced only when the rewrite replaces
// it as the delete file spells it: s3a:// and s3:// may name one object or,
// with other endpoints configured, two, and taking a kept file for a
// replaced one would drop deletes that still apply to it. Live, in any
// spelling, keeps the delete file. The files a rewrite replaces are its
// tasks', as their manifests spell them, as the delete file's writer did.
func TestRemovableMatchesReplacedFilesExactly(t *testing.T) {
	ctx := context.Background()
	kept := "s3a://bucket/wh/t/data/a.parquet"
	d := posDelete(t, "s3://bucket/wh/t/data/delete.parquet", kept)
	live := Paths{}
	live.Add(kept)
	for _, c := range []struct {
		rewritten string
		want      bool
	}{
		{"s3://bucket/wh/t/data/a.parquet", false},
		{"s3a://bucket//wh/t/data/a.parquet", false},
		{kept, true},
	} {
		tasks := []table.FileScanTask{{File: dataFile(t, c.rewritten), DeleteFiles: []iceberg.DataFile{d}}}
		got, err := NewTargets().Removable(ctx, iceio.LocalFS{}, tasks, live)
		if err != nil {
			t.Fatal(err)
		}
		if (len(got) == 1) != c.want {
			t.Errorf("rewriting %s: removable %v, want %v", c.rewritten, len(got) == 1, c.want)
		}
	}
	// A target no longer live, however spelled, holds no delete back.
	gone := Paths{}
	gone.Add("s3://bucket/wh/t/data/other.parquet")
	tasks := []table.FileScanTask{{File: dataFile(t, "s3://bucket/wh/t/data/other.parquet"), DeleteFiles: []iceberg.DataFile{d}}}
	if got, err := NewTargets().Removable(ctx, iceio.LocalFS{}, tasks, gone); err != nil || len(got) != 1 {
		t.Errorf("a delete of a file gone: %v, %v", got, err)
	}
}

// What a delete file deletes rows of is read from its manifest bounds when
// they name one file, from its row groups' statistics when they do, and
// otherwise from its file_path column alone, a batch at a time.
func TestTargetsReadOnlyWhatTheyNeed(t *testing.T) {
	ctx := context.Background()
	// Bounds that name one file: the file is not even opened.
	missing := filepath.Join(t.TempDir(), "missing.parquet")
	got, err := NewTargets().Of(ctx, iceio.LocalFS{}, posDelete(t, missing, "s3://b/t/a.parquet"))
	if err != nil || !equal(got, "s3://b/t/a.parquet") {
		t.Errorf("by bounds: %v, %v", got, err)
	}

	// Row groups of two rows: one names a alone, one b and c, one d alone.
	path := filepath.Join(t.TempDir(), "deletes.parquet")
	schema := arrow.NewSchema([]arrow.Field{{Name: "file_path", Type: arrow.BinaryTypes.String}, {Name: "pos", Type: arrow.PrimitiveTypes.Int64},
		{Name: "row", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i, p := range []string{"a", "a", "b", "c", "d", "d"} {
		b.Field(0).(*array.StringBuilder).Append(p)
		b.Field(1).(*array.Int64Builder).Append(int64(i))
		b.Field(2).(*array.StringBuilder).Append("a whole row that is not read")
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2), parquet.WithStats(true)),
		pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = NewTargets().Of(ctx, iceio.LocalFS{}, posDelete(t, path, ""))
	if err != nil || !equal(got, "a", "b", "c", "d") {
		t.Errorf("by statistics and the column: %v, %v", got, err)
	}
	// Only the middle row group needs reading.
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close() //nolint:errcheck
	col := rdr.MetaData().Schema.ColumnIndexByName("file_path")
	for i, want := range []string{"a", "", "d"} {
		if one, ok := single(rdr.MetaData().RowGroup(i), col); one != want || ok != (want != "") {
			t.Errorf("row group %d: %q, %v", i, one, ok)
		}
	}
}

func equal(got map[string]bool, want ...string) bool {
	var keys []string
	for k := range got {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return slices.Equal(keys, want)
}
