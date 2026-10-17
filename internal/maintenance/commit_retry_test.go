package maintenance

import (
	"context"
	"errors"
	"testing"

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

	// A plain Commit honours the table's num-retries: 1 initial + 3 retries.
	tx := tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{"igloo.test": "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("Commit: want ErrCommitFailed, got %v", err)
	}
	if cat.commits != 4 {
		t.Errorf("Commit attempted %d times, want 4 (1 + num-retries)", cat.commits)
	}

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
