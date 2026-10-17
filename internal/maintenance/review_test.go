package maintenance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// hookCatalog commits one table's metadata in memory, calling before, if
// set, ahead of each commit.
type hookCatalog struct {
	tbl    *table.Table
	before func(updates []table.Update)
}

func (c *hookCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.tbl, nil
}

func (c *hookCatalog) CommitTable(_ context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if c.before != nil {
		c.before(updates)
	}
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

// people is a table of emails, one data file per commit of rows.
func people(t *testing.T, props iceberg.Properties, commits ...[]string) *hookCatalog {
	t.Helper()
	ctx := context.Background()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String})
	props[table.WriteDeleteModeKey] = table.WriteModeMergeOnRead
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(), props)
	if err != nil {
		t.Fatal(err)
	}
	cat := &hookCatalog{}
	cat.tbl = table.New(table.Identifier{"crm", "people"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	id := int64(0)
	for _, rows := range commits {
		appendEmails(t, ctx, cat, &id, rows...)
	}
	return cat
}

func appendEmails(t *testing.T, ctx context.Context, cat *hookCatalog, id *int64, emails ...string) {
	t.Helper()
	arrowSchema, err := table.SchemaToArrowSchema(cat.tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	for _, email := range emails {
		*id++
		b.Field(0).(*array.Int64Builder).Append(*id)
		b.Field(1).(*array.StringBuilder).Append(email)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer tbl.Release()
	if _, err := cat.tbl.AppendTable(ctx, tbl, 100, nil); err != nil {
		t.Fatal(err)
	}
}

func emails(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	rows, err := tbl.Scan().ToArrowTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Release()
	var out []string
	for _, chunk := range rows.Column(1).Data().Chunks() {
		col := chunk.(*array.String)
		for i := range col.Len() {
			out = append(out, col.Value(i))
		}
	}
	slices.Sort(out)
	return out
}

// Ordinary compaction, on any table: it rewrites the small files of a
// partition and keeps a large one, and one position delete file
// (partition-scoped, as iceberg-go, DuckDB and Spark before Iceberg 1.6
// write them) deletes a row of a small file and one of the large file.
// iceberg-go's RewriteDataFiles removed the delete file with the small
// files, and the large file's deleted row came back (3004 rows became
// 3005). The review's proof of concept.
func TestCompactionKeepsDeletesOfFilesItKeeps(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var big []string
	for i := range 3000 {
		big = append(big, fmt.Sprintf("big-%04d@example.com", i))
	}
	commits := [][]string{big}
	for i := range 6 {
		commits = append(commits, []string{fmt.Sprintf("small-%d@example.com", i)})
	}
	cat := people(t, iceberg.Properties{}, commits...)
	// The target size makes the large file no candidate, and the small
	// ones candidates.
	var bigSize int64
	tasks, err := cat.tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		bigSize = max(bigSize, task.File.FileSizeBytes())
	}
	if _, _, err := cat.CommitTable(ctx, cat.tbl.Identifier(), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{
		table.WriteTargetFileSizeBytesKey: strconv.FormatInt(bigSize, 10)})}); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.tbl.Delete(ctx, iceberg.IsIn(iceberg.Reference("email"), "big-0007@example.com", "small-2@example.com"), nil); err != nil {
		t.Fatal(err)
	}
	before := emails(t, cat.tbl)
	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour}, log)
	committed, err := m.compact(ctx, cat.tbl, log)
	if err != nil {
		t.Fatal(err)
	}
	if committed.CurrentSnapshot().Summary.Operation != table.OpReplace {
		t.Fatalf("compaction did not run: %s", committed.CurrentSnapshot().Summary.Operation)
	}
	after := emails(t, committed)
	if slices.Contains(after, "big-0007@example.com") {
		t.Errorf("compaction brought back a row deleted from the file it kept")
	}
	if len(after) != len(before) {
		t.Errorf("rows %d -> %d", len(before), len(after))
	}
}

// A compaction that loses a race is not retried by iceberg-go, whatever
// commit.retry.num-retries says: a retry added the new head's manifests,
// which still list the replaced files live, and their rows came back
// twice. Maintenance tries again on its next run.
func TestCompactionIsNotRetried(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var commits [][]string
	for i := range 6 {
		commits = append(commits, []string{fmt.Sprintf("p%d@example.com", i)})
	}
	cat := people(t, iceberg.Properties{table.CommitNumRetriesKey: "3", table.CommitMinRetryWaitMsKey: "1"}, commits...)
	if _, err := cat.tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("email"), "p0@example.com"), nil); err != nil {
		t.Fatal(err)
	}
	id, raced := int64(100), false
	cat.before = func(updates []table.Update) {
		if raced || !slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == table.UpdateAddSnapshot }) {
			return
		}
		raced = true
		appendEmails(t, ctx, cat, &id, "zed@example.com")
	}
	m := New(nil, "", Options{Retention: 7 * 24 * time.Hour}, log)
	_, err := m.compact(ctx, cat.tbl, log)
	if !raced || !errors.Is(err, table.ErrCommitFailed) {
		t.Fatalf("raced %v, err %v", raced, err)
	}
	want := []string{"p1@example.com", "p2@example.com", "p3@example.com", "p4@example.com", "p5@example.com", "zed@example.com"}
	if got := emails(t, cat.tbl); !slices.Equal(got, want) {
		t.Errorf("rows: %v", got)
	}
}

// Appends are retried as before: a retry of one adds what it wrote to the
// new head's files, which is right.
func TestAppendsAreStillRetried(t *testing.T) {
	ctx := context.Background()
	cat := people(t, iceberg.Properties{table.CommitNumRetriesKey: "3", table.CommitMinRetryWaitMsKey: "1"}, []string{"ann@example.com"})
	id, raced := int64(100), false
	cat.before = func(updates []table.Update) {
		if raced {
			return
		}
		raced = true
		appendEmails(t, ctx, cat, &id, "zed@example.com")
	}
	appendEmails(t, ctx, cat, &id, "bo@example.com")
	if got := emails(t, cat.tbl); !raced || !slices.Equal(got, []string{"ann@example.com", "bo@example.com", "zed@example.com"}) {
		t.Errorf("raced %v, rows %v", raced, got)
	}
}

// Java's Iceberg writes Long.MAX_VALUE milliseconds for "forever", which
// overflowed a duration into a negative retention: every snapshot older
// than now expired.
func TestRetentionForever(t *testing.T) {
	cat := people(t, iceberg.Properties{maxSnapshotAgeProperty: "9223372036854775807"}, []string{"a"})
	m := New(nil, "", Options{Retention: time.Hour}, slog.New(slog.DiscardHandler))
	if age, _ := m.retention(cat.tbl); age <= 0 {
		t.Errorf("retention of Long.MAX_VALUE ms: %v", age)
	}
	if millis(9223372036854775807) <= 0 || millis(1500) != 1500*time.Millisecond {
		t.Errorf("millis")
	}
}
