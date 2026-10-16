package maintenance

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Compaction writes data files through iceberg-go's WriteRecords. They must
// be laid out the way DuckDB writes Parquet, or compacting a table makes
// queries slower (see third_party/iceberg-go/IGLOO_PATCHES.md).
func TestCompactedFilesMatchDuckDB(t *testing.T) {
	ctx := context.Background()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "price", Type: iceberg.DecimalTypeOf(15, 2)},
		iceberg.NestedField{ID: 3, Name: "quantity", Type: iceberg.DecimalTypeOf(15, 2)},
		iceberg.NestedField{ID: 4, Name: "rate", Type: iceberg.DecimalTypeOf(7, 4)},
		iceberg.NestedField{ID: 5, Name: "status", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 6, Name: "comment", Type: iceberg.PrimitiveTypes.String},
	)
	dir := t.TempDir()
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "file://"+dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New([]string{"t"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)

	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	// More than a row group, in two batches, so the writer starts while
	// batches are still arriving.
	const rows = 150_000
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	for i := range int64(rows) {
		b.Field(0).(*array.Int64Builder).Append(i)
		b.Field(1).(*array.Decimal128Builder).Append(decimal128.FromI64(i * 37))
		b.Field(2).(*array.Decimal128Builder).Append(decimal128.FromI64(i % 50 * 100))
		b.Field(3).(*array.Decimal128Builder).Append(decimal128.FromI64(i % 9))
		b.Field(4).(*array.StringBuilder).Append([]string{"open", "shipped", "returned"}[i%3])
		b.Field(5).(*array.StringBuilder).Append("comment " + strconv.FormatInt(i, 10))
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	records := func(yield func(arrow.RecordBatch, error) bool) {
		_ = yield(rec.NewSlice(0, 100_000), nil) && yield(rec.NewSlice(100_000, rows), nil)
	}

	var paths []string
	for df, err := range table.WriteRecords(ctx, tbl, arrowSchema, records, table.WithClusteredWrite()) {
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, strings.TrimPrefix(df.FilePath(), "file://"))
	}
	if len(paths) != 1 || !strings.HasPrefix(paths[0], filepath.Clean(dir)) {
		t.Fatalf("data files: %v", paths)
	}

	rdr, err := file.OpenParquetFile(paths[0], false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	want := map[string]struct {
		physical parquet.Type
		dict     bool
	}{
		// Decimals as integers, not FIXED_LEN_BYTE_ARRAY.
		"price":    {parquet.Types.Int64, false},
		"quantity": {parquet.Types.Int64, true},
		"rate":     {parquet.Types.Int32, true},
		// A dictionary only where at most a fifth of the values are distinct.
		"id":      {parquet.Types.Int64, false},
		"status":  {parquet.Types.ByteArray, true},
		"comment": {parquet.Types.ByteArray, false},
	}
	rg := rdr.MetaData().RowGroup(0)
	for i := range rdr.MetaData().Schema.NumColumns() {
		col := rdr.MetaData().Schema.Column(i)
		chunk, err := rg.ColumnChunk(i)
		if err != nil {
			t.Fatal(err)
		}
		w, ok := want[col.Name()]
		if !ok {
			t.Fatalf("unexpected column %s", col.Name())
		}
		if col.PhysicalType() != w.physical {
			t.Errorf("%s stored as %s, want %s", col.Name(), col.PhysicalType(), w.physical)
		}
		if chunk.HasDictionaryPage() != w.dict {
			t.Errorf("%s: dictionary %v, want %v", col.Name(), chunk.HasDictionaryPage(), w.dict)
		}
		if chunk.Compression() != compress.Codecs.Snappy {
			t.Errorf("%s compressed with %s, want snappy", col.Name(), chunk.Compression())
		}
	}
	if n := rdr.NumRows(); n != rows {
		t.Errorf("%d rows, want %d", n, rows)
	}
}
