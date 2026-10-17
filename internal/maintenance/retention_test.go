package maintenance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// readEmails reads the email column of a data file, deleted rows and all.
func readEmails(ctx context.Context, path string) ([]string, error) {
	f, err := iceio.LocalFS{}.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer rdr.Close() //nolint:errcheck
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	tbl, err := fr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()
	var out []string
	for _, chunk := range tbl.Column(tbl.Schema().FieldIndices("email")[0]).Data().Chunks() {
		col := chunk.(*array.String)
		for i := range col.Len() {
			out = append(out, col.Value(i))
		}
	}
	return out, nil
}

// localCatalog commits one table's metadata in memory.
type localCatalog struct{ tbl *table.Table }

func (c *localCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

func (c *localCatalog) CommitTable(_ context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.tbl.Metadata()); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	b, err := table.MetadataBuilderFromBase(c.tbl.Metadata(), "")
	if err != nil {
		return nil, "", err
	}
	for _, u := range updates {
		if err := u.Apply(b); err != nil {
			return nil, "", err
		}
	}
	meta, err := b.Build()
	if err != nil {
		return nil, "", err
	}
	c.tbl = table.New(id, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c)
	return meta, "", nil
}

// A table with a data retention keeps no older version of its rows: not in
// snapshots, whatever its own properties keep, nor deleted rows in files.
func TestDataRetention(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(), iceberg.Properties{
		table.WriteDeleteModeKey: table.WriteModeMergeOnRead, minSnapshotsToKeepProperty: "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	cat := &localCatalog{}
	cat.tbl = table.New(table.Identifier{"crm", "people"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]string{{"ann@example.com", "bo@example.com"}, {"cy@example.com"}} {
		b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		for i, email := range rows {
			b.Field(0).(*array.Int64Builder).Append(int64(i))
			b.Field(1).(*array.StringBuilder).Append(email)
		}
		rec := b.NewRecordBatch()
		tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		if _, err := cat.tbl.AppendTable(ctx, tbl, 100, nil); err != nil {
			t.Fatal(err)
		}
		tbl.Release()
		rec.Release()
		b.Release()
	}
	if _, err := cat.tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("email"), "ann@example.com"), nil); err != nil {
		t.Fatal(err)
	}
	// The data files, and those that rows were deleted from.
	files := func() (all, deleted []string) {
		tasks, err := cat.tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range tasks {
			all = append(all, task.File.FilePath())
			if len(task.DeleteFiles) > 0 {
				deleted = append(deleted, task.File.FilePath())
			}
		}
		// Planning lists files in no particular order.
		slices.Sort(all)
		slices.Sort(deleted)
		return all, deleted
	}
	before, withDeletes := files()
	if len(before) != 2 || len(withDeletes) == 0 {
		t.Fatalf("files %v, with deletes %v", before, withDeletes)
	}

	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour}, log)
	if _, err := m.purgeDeleted(ctx, cat.tbl, time.Hour, log); err != nil {
		t.Fatal(err)
	}
	if after, _ := files(); !slices.Equal(after, before) {
		t.Fatalf("files with deletes newer than the retention were rewritten: %v", after)
	}
	if _, err := m.purgeDeleted(ctx, cat.tbl, 0, log); err != nil {
		t.Fatal(err)
	}
	// Only the file the row was deleted from is rewritten, although
	// planning attaches the delete file to both.
	after, _ := files()
	if len(after) != 2 || !slices.Contains(after, before[0]) && !slices.Contains(after, before[1]) {
		t.Errorf("files after rewriting: %v, before %v", after, before)
	}
	for _, p := range after {
		rows, err := readEmails(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(rows, "ann@example.com") {
			t.Errorf("%s still holds the deleted row", p)
		}
	}
	snapshots := len(cat.tbl.Metadata().Snapshots())
	if _, err := m.purgeDeleted(ctx, cat.tbl, 0, log); err != nil || len(cat.tbl.Metadata().Snapshots()) != snapshots {
		t.Errorf("a second pass rewrote again: %v", err)
	}
	rows, err := cat.tbl.Scan().ToArrowTable(ctx)
	if err != nil || rows.NumRows() != 2 {
		t.Fatalf("rows after rewriting: %v, %v", rows, err)
	}
	rows.Release()

	if _, _, err := m.expire(ctx, cat.tbl, log, 0, true); err != nil {
		t.Fatal(err)
	}
	if after := len(cat.tbl.Metadata().Snapshots()); snapshots < 4 || after != 1 {
		t.Errorf("snapshots: %d before, %d after, want 1 despite min-snapshots-to-keep", snapshots, after)
	}
}

// A data retention expires old snapshots, but not branches and tags,
// which keep the snapshots they point at: a tag is someone's reference
// point, such as an audit's.
func TestDataRetentionKeepsRefs(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &localCatalog{}
	cat.tbl = table.New(table.Identifier{"fin", "ledger"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for i := range 3 {
		b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		rec := b.NewRecordBatch()
		tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
		if _, err := cat.tbl.AppendTable(ctx, tbl, 100, nil); err != nil {
			t.Fatal(err)
		}
		tbl.Release()
		rec.Release()
		b.Release()
		ids = append(ids, cat.tbl.CurrentSnapshot().SnapshotID)
	}
	if _, _, err := cat.CommitTable(ctx, cat.tbl.Identifier(), nil, []table.Update{
		table.NewSetSnapshotRefUpdate("audit-2026-09", ids[0], table.TagRef, 0, 0, 0),
		table.NewSetSnapshotRefUpdate("staging", ids[1], table.BranchRef, 0, 0, 0),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour}, log)
	if _, _, err := m.expire(ctx, cat.tbl, log, time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	var refs []string
	for name, ref := range cat.tbl.Metadata().Refs() {
		refs = append(refs, name)
		if cat.tbl.Metadata().SnapshotByID(ref.SnapshotID) == nil {
			t.Errorf("%s points at an expired snapshot", name)
		}
	}
	slices.Sort(refs)
	if !slices.Equal(refs, []string{"audit-2026-09", "main", "staging"}) {
		t.Errorf("refs: %v", refs)
	}
}
