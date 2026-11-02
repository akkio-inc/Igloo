package maintenance

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/filerewrite"
)

// TestSizeRowGroups checks the row group size maintenance sets for the
// inserts into a table Igloo created: from its bytes a row, within the
// memory of a row group on every thread, and never over a size a user set.
func TestSizeRowGroups(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 4, rows: 700_000, keys: 500}
	cat := u.table(t, db, dir, table.UnsortedSortOrder)
	set := func(props iceberg.Properties) {
		t.Helper()
		tx := cat.tbl.NewTransaction()
		if err := tx.SetProperties(props); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	m := New(nil, dir+"/", Options{Memory: 1 << 30, Threads: 4}, quiet)
	size := func() string {
		t.Helper()
		if err := m.sizeRowGroups(context.Background(), cat.tbl, quiet); err != nil {
			t.Fatal(err)
		}
		return cat.tbl.Properties()[duckDBRowGroups]
	}

	// A table Igloo did not create keeps DuckDB's own.
	if got := size(); got != "" {
		t.Errorf("a table without the marker got %q", got)
	}
	set(iceberg.Properties{rowGroupsProperty: "auto"})
	summary := cat.tbl.CurrentSnapshot().Summary.Properties
	bytes, _ := strconv.ParseInt(summary["total-files-size"], 10, 64)
	rows, _ := strconv.ParseInt(summary["total-records"], 10, 64)
	want := filerewrite.TargetRows(bytes, rows)
	// DuckDB's commits leave the files' size out of the summary: the
	// files say it.
	delete(summary, "total-files-size")
	if got := size(); got != strconv.FormatInt(want, 10) || cat.tbl.Properties()[rowGroupsProperty] != "auto:"+got {
		t.Errorf("row groups of %q rows, marked %q; want %d", got, cat.tbl.Properties()[rowGroupsProperty], want)
	}
	if want <= filerewrite.DefaultRowGroupRows {
		t.Errorf("rows of %d bytes each aim at %d rows a group", bytes/rows, want)
	}

	// A size near the one in force is not worth a commit; one far from it
	// is.
	near := strconv.FormatInt(want*95/100, 10)
	set(iceberg.Properties{duckDBRowGroups: near, rowGroupsProperty: "auto:" + near})
	if got := size(); got != near {
		t.Errorf("%s rows in force became %q, near enough to %d", near, got, want)
	}
	far := strconv.FormatInt(want/2, 10)
	set(iceberg.Properties{duckDBRowGroups: far, rowGroupsProperty: "auto:" + far})
	if got := size(); got != strconv.FormatInt(want, 10) {
		t.Errorf("%s rows in force became %q; want %d", far, got, want)
	}

	// Little memory a thread: as many rows as fit, but no fewer than
	// DuckDB's own.
	m.opts.Memory, m.opts.Threads = 64<<20, 64
	if got := size(); got != strconv.Itoa(filerewrite.DefaultRowGroupRows) {
		t.Errorf("with 1 MiB a thread: %q rows", got)
	}

	// A size a user set stays.
	set(iceberg.Properties{duckDBRowGroups: "1000"})
	m.opts.Memory, m.opts.Threads = 1<<30, 4
	if got := size(); got != "1000" {
		t.Errorf("a user's size became %q", got)
	}
}

// TestSplitRowGroups checks that a pass rewrites a small table Igloo created whose row
// groups are too few for its size, once, with its rows as they were; and leaves alone
// one whose row groups are fine, a user's, and one too large to be small.
func TestSplitRowGroups(t *testing.T) {
	db := openDuckDB(t)
	ctx := context.Background()
	m := New(nil, t.TempDir()+"/", Options{Memory: 1 << 30, Threads: 4}, quiet)
	groups := func(tbl *table.Table) (files, groups int, rows int64) {
		t.Helper()
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fs, err := tbl.FS(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range tasks {
			n, err := filerewrite.RowGroups(fs, task.File.FilePath())
			if err != nil {
				t.Fatal(err)
			}
			groups += n
			rows += task.File.Count()
		}
		return len(tasks), groups, rows
	}
	mark := func(cat listedCatalog, props iceberg.Properties) {
		t.Helper()
		tx := cat.tbl.NewTransaction()
		if err := tx.SetProperties(props); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Two files of a row group each, of 400,000 rows.
	u := unsortedTable{files: 2, rows: 400_000, keys: 500, rowGroups: 524_288}
	cat := u.table(t, db, t.TempDir(), table.UnsortedSortOrder)
	if f, g, rows := groups(cat.tbl); f != 2 || g > 8 || rows != 800_000 {
		t.Fatalf("the table to split: %d files, %d row groups, %d rows", f, g, rows)
	}
	// One Igloo did not create stays as it is.
	committed, err := m.splitRowGroups(ctx, cat.tbl, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if committed.CurrentSnapshot().SnapshotID != cat.tbl.CurrentSnapshot().SnapshotID {
		t.Error("a table without the marker was rewritten")
	}
	mark(cat, iceberg.Properties{rowGroupsProperty: "auto"})
	committed, err = m.splitRowGroups(ctx, cat.tbl, quiet)
	if err != nil {
		t.Fatal(err)
	}
	f, g, rows := groups(committed)
	if rows != 800_000 || g < 6 {
		t.Errorf("after the split: %d files, %d row groups, %d rows", f, g, rows)
	}
	// Once is enough, and checked once.
	snap := committed.CurrentSnapshot().SnapshotID
	cat.tbl = committed
	if again, err := m.splitRowGroups(ctx, cat.tbl, quiet); err != nil || again.CurrentSnapshot().SnapshotID != snap {
		t.Errorf("a second pass: %v", err)
	}
	delete(m.splitChecked, name(cat.tbl.Identifier()))
	if again, err := m.splitRowGroups(ctx, cat.tbl, quiet); err != nil || again.CurrentSnapshot().SnapshotID != snap {
		t.Errorf("a table with enough row groups was rewritten: %v", err)
	}

	// Row groups as small as they aim at stay as they are.
	fine := unsortedTable{files: 8, rows: 100_000, keys: 500}
	cat = fine.table(t, db, t.TempDir(), table.UnsortedSortOrder)
	mark(cat, iceberg.Properties{rowGroupsProperty: "auto"})
	before := cat.tbl.CurrentSnapshot().SnapshotID
	if got, err := m.splitRowGroups(ctx, cat.tbl, quiet); err != nil || got.CurrentSnapshot().SnapshotID != before {
		t.Errorf("a table of 8 files of 100,000 rows was rewritten: %v", err)
	}
}

// TestOpenPartitions checks how many partitions' files, each holding a row
// group, an insert into a table holds open: its buckets', and as many as
// DuckDB keeps open for other transforms or more buckets.
func TestOpenPartitions(t *testing.T) {
	bucket := func(id, n int) iceberg.PartitionField {
		return iceberg.PartitionField{SourceIDs: []int{id}, FieldID: 999 + id, Name: "b" + strconv.Itoa(id),
			Transform: iceberg.BucketTransform{NumBuckets: n}}
	}
	day := iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1010, Name: "d", Transform: iceberg.DayTransform{}}
	for _, c := range []struct {
		fields []iceberg.PartitionField
		want   int64
	}{
		{nil, 1},
		{[]iceberg.PartitionField{bucket(1, 16)}, 16},
		{[]iceberg.PartitionField{bucket(1, 16), bucket(2, 8)}, 128},
		{[]iceberg.PartitionField{bucket(1, 4096)}, openPartitionFiles},
		{[]iceberg.PartitionField{bucket(1, 4), day}, openPartitionFiles},
	} {
		if got := openPartitions(iceberg.NewPartitionSpec(c.fields...)); got != c.want {
			t.Errorf("%v: %d, want %d", c.fields, got, c.want)
		}
	}
}
