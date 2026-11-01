package maintenance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// TestCompactionKeepsBuckets checks that compacting a table partitioned by
// bucket[4] of a column rewrites each bucket's small files into files of
// that bucket alone, under the same partition value: distributed execution
// reads such a table by bucket, every row of a key in its bucket's files.
func TestCompactionKeepsBuckets(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket",
		Transform: iceberg.BucketTransform{NumBuckets: 4}})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, t.TempDir(), iceberg.Properties{})
	if err != nil {
		t.Fatal(err)
	}
	cat := &hookCatalog{}
	cat.tbl = table.New(table.Identifier{"crm", "people"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	id := int64(0)
	for c := range 8 {
		var rows []string
		for i := range 40 {
			rows = append(rows, fmt.Sprintf("p%d-%d@example.com", c, i))
		}
		appendEmails(t, ctx, cat, &id, rows...)
	}
	files := func(tbl *table.Table) int {
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(tasks)
	}
	before, rows := files(cat.tbl), emails(t, cat.tbl)
	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour}, log)
	committed, err := m.compact(ctx, cat.tbl, log)
	if err != nil {
		t.Fatal(err)
	}
	if after := files(committed); after >= before || after < 4 {
		t.Errorf("%d files compacted into %d, want one or more a bucket", before, after)
	}
	if got := emails(t, committed); len(got) != len(rows) {
		t.Errorf("rows %d -> %d", len(rows), len(got))
	}
	bucket := iceberg.BucketTransform{NumBuckets: 4}.Transformer(iceberg.PrimitiveTypes.Int64)
	tasks, err := committed.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		want, ok := task.File.Partition()[1000].(int32)
		if !ok || task.File.SpecID() != int32(spec.ID()) {
			t.Fatalf("%s: partition %v of spec %d", task.File.FilePath(), task.File.Partition(), task.File.SpecID())
		}
		pf, err := file.OpenParquetFile(task.File.FilePath(), false)
		if err != nil {
			t.Fatal(err)
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := fr.ReadTable(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range tbl.Column(0).Data().Chunks() {
			ids := chunk.(*array.Int64)
			for i := range ids.Len() {
				if b := bucket(ids.Value(i)); !b.Valid || b.Val != want {
					t.Errorf("%s, of bucket %d, holds id %d of bucket %v", task.File.FilePath(), want, ids.Value(i), b)
				}
			}
		}
		tbl.Release()
		pf.Close() //nolint:errcheck
	}
}
