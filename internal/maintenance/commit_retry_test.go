package maintenance

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// conflictCatalog always rejects a commit with ErrCommitFailed and counts the
// attempts, so a test can see how many times a commit is retried.
type conflictCatalog struct {
	tbl            *table.Table
	commits, loads int
}

func (c *conflictCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	c.loads++
	return c.tbl, nil
}

func (c *conflictCatalog) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	c.commits++
	return nil, "", table.ErrCommitFailed
}

// TestMaintenanceCommitsDoNotRetry proves the fix for the duplicate-rows
// exposure: a table that sets commit.retry.num-retries would, on a conflict,
// make iceberg-go rebuild a rewrite's manifest list from the fresh parent and
// reintroduce the files the rewrite replaced (see
// third_party/iceberg-go/IGLOO_PATCHES.md). Igloo's maintenance commits with
// CommitWithoutRetry, which never takes that path: a conflict fails at once,
// to be retried next pass. A plain Commit still honours the property.
func TestMaintenanceCommitsDoNotRetry(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	props := iceberg.Properties{
		"commit.retry.num-retries":      "3",
		"commit.retry.min-wait-ms":      "1",
		"commit.retry.max-wait-ms":      "1",
		"commit.retry.total-timeout-ms": "60000",
	}
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "file://"+t.TempDir(), props)
	if err != nil {
		t.Fatal(err)
	}
	cat := &conflictCatalog{}
	tbl := table.New([]string{"t"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	cat.tbl = tbl

	// A plain Commit of an append honours the table's num-retries: 1
	// initial + 3 retries.
	tx := tbl.NewTransaction()
	if err := tx.AppendTable(context.Background(), ids(t, tbl, 1, 2), 10, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("Commit: want ErrCommitFailed, got %v", err)
	}
	if cat.commits != 4 {
		t.Errorf("Commit attempted %d times, want 4 (1 + num-retries)", cat.commits)
	}

	// A plain Commit that adds no snapshot, as one that sets properties,
	// expires snapshots or moves a ref, is not retried: a retry moves its
	// requirements on refs to the new head, a state it never saw.
	cat.commits, cat.loads = 0, 0
	tx = tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{"igloo.test": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("Commit of properties: want ErrCommitFailed, got %v", err)
	}
	if cat.commits != 1 || cat.loads != 0 {
		t.Errorf("Commit of properties attempted %d times, refreshed %d; want 1, 0", cat.commits, cat.loads)
	}

	// Nor is an append to a table that merges manifests on append, whose
	// own manifests can carry the parent's entries.
	merging := table.New([]string{"t"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	cat.tbl = merging
	tx = merging.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{table.ManifestMergeEnabledKey: "true"}); err != nil {
		t.Fatal(err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatal(err)
	}
	merging = table.New([]string{"t"}, staged.Metadata(), "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	cat.tbl, cat.commits, cat.loads = merging, 0, 0
	tx = merging.NewTransaction()
	if err := tx.AppendTable(context.Background(), ids(t, merging, 3), 10, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("Commit of a merging append: want ErrCommitFailed, got %v", err)
	}
	if cat.commits != 1 {
		t.Errorf("Commit of a merging append attempted %d times, want 1", cat.commits)
	}
	cat.tbl = tbl

	// CommitWithoutRetry makes exactly one attempt and never refreshes, so the
	// manifest-list rebuild that reintroduces replaced files is never reached.
	cat.commits, cat.loads = 0, 0
	tx = tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{"igloo.test": "y"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CommitWithoutRetry(context.Background()); !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("CommitWithoutRetry: want ErrCommitFailed, got %v", err)
	}
	if cat.commits != 1 {
		t.Errorf("CommitWithoutRetry attempted %d times, want 1", cat.commits)
	}
	if cat.loads != 0 {
		t.Errorf("CommitWithoutRetry refreshed %d times, want 0", cat.loads)
	}
}

// ids is an Arrow table of tbl's one column of ids.
func ids(t *testing.T, tbl *table.Table, values ...int64) arrow.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(values, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	out := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	t.Cleanup(out.Release)
	return out
}
