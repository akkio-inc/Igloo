package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/memlimit"
)

// unsortedTable is a table of DuckDB-written files whose rows come in no
// order: each file holds keys from the whole range, as a table's inserts
// leave them, and every file's range overlaps every other's.
type unsortedTable struct {
	files, rows int
	// keys is how many distinct keys there are; one in nullEvery rows has
	// a NULL key (0 for none).
	keys, nullEvery int
}

func (u unsortedTable) schema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "audience", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "ver", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
}

// write writes count files of rows starting at row first, as Igloo's
// writes do, into dir.
func (u unsortedTable) write(t testing.TB, db *sql.DB, dir string, first, count int) []string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("md5(((i * 7919) %% %d)::VARCHAR)", u.keys)
	if u.nullEvery > 0 {
		key = fmt.Sprintf("CASE WHEN i %% %d = 0 THEN NULL ELSE %s END", u.nullEvery, key)
	}
	var paths []string
	for f := first; f < first+count; f++ {
		path := filepath.Join(dir, fmt.Sprintf("duckdb-%03d.parquet", f))
		stmt := fmt.Sprintf(`COPY (SELECT i AS id, %s AS audience, CAST(i %% 3 AS INTEGER) AS ver,
			'payload-' || md5(i::VARCHAR) || '-' || md5((i + 1)::VARCHAR) AS payload
			FROM range(%d, %d) t(i) ORDER BY random()) TO '%s' (FORMAT parquet)`, key, f*u.rows, (f+1)*u.rows, path)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

// table registers the files as the Iceberg table mirror.audiences, sorted
// by order, in dir.
func (u unsortedTable) table(t testing.TB, db *sql.DB, dir string, order table.SortOrder) listedCatalog {
	t.Helper()
	ctx := context.Background()
	meta, err := table.NewMetadata(u.schema(), iceberg.UnpartitionedSpec, order, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &localCatalog{}
	cat.tbl = table.New(table.Identifier{"mirror", "audiences"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := cat.tbl.NewTransaction()
	if err := tx.AddFiles(ctx, u.write(t, db, filepath.Join(dir, "data"), 0, u.files), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return listedCatalog{cat}
}

// byAudience is the sort order audience ASC NULLS FIRST, ver DESC.
func byAudience(t testing.TB) table.SortOrder {
	t.Helper()
	order, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst},
		{SourceIDs: []int{3}, Transform: iceberg.IdentityTransform{}, Direction: table.SortDESC, NullOrder: table.NullsLast},
	})
	if err != nil {
		t.Fatal(err)
	}
	return order
}

// digest sums up the table's rows, to compare them before and after.
func (u unsortedTable) digest(t testing.TB, db *sql.DB, files []string) string {
	t.Helper()
	var got string
	err := db.QueryRow(fmt.Sprintf(`SELECT count(*)::VARCHAR || ' ' || sum(id)::VARCHAR || ' ' || sum(hash(id, audience, ver, payload))::VARCHAR
		|| ' ' || count(audience)::VARCHAR FROM read_parquet(['%s'])`, strings.Join(files, "', '"))).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// overlaps counts the files of tbl whose ranges of the sort order's first
// column overlap another's.
func overlaps(t testing.TB, tbl *table.Table) int {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, first, ok := clusterFields(tbl)
	if !ok {
		t.Fatal("the table has no sort order to cluster by")
	}
	return countTrue(overlapping(spans(tasks, tbl.Schema(), first)))
}

// unsortedRows counts the rows of each file that come before a row they
// should follow, by audience (NULLs first) and then ver descending.
func unsortedRows(t testing.TB, db *sql.DB, files []string) int {
	t.Helper()
	if _, err := db.Exec("SET threads = 1"); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range files {
		var bad int
		err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM (
			SELECT audience, ver, lag(audience) OVER () AS pa, lag(ver) OVER () AS pv, row_number() OVER () AS rn FROM read_parquet('%s'))
			WHERE rn > 1 AND (pa IS NOT NULL AND audience IS NULL OR pa > audience OR pa IS NOT DISTINCT FROM audience AND pv < ver)`, f)).Scan(&bad)
		if err != nil {
			t.Fatal(err)
		}
		n += bad
	}
	return n
}

// TestClusteringMakesRangesDisjoint clusters a table of files written in no
// order, larger in Arrow than the memory clustering may use, so that it
// sorts range by range and spills: afterwards no two files' ranges of the
// first column overlap, every file is sorted by the whole order, the rows
// are the same, the memory stayed within the budget and nothing is left in
// the spill directory. A second pass rewrites nothing.
func TestClusteringMakesRangesDisjoint(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 6, rows: 80_000, keys: 3000, nullEvery: 97}
	cat := u.table(t, db, dir, byAudience(t))
	before := dataFiles(t, cat.tbl)
	want := u.digest(t, db, before)
	const budget = 48 << 20
	// The rows take more memory in Arrow than the budget allows.
	if size := arrowSize(t, cat.tbl); size <= budget {
		t.Fatalf("the table takes %s in Arrow, no more than the budget", memlimit.Format(size))
	}
	if n := overlaps(t, cat.tbl); n != len(before) {
		t.Fatalf("%d of %d files overlap before clustering", n, len(before))
	}

	spill := t.TempDir()
	outer := unmetered()
	ctx := compute.WithAllocator(context.Background(), outer)
	// A small target size, so that the rows make several files.
	m := New(nil, dir+"/", Options{Memory: budget, SpillDir: spill}, quiet)
	tbl := withTarget(t, cat, 4<<20)
	if _, err := m.compactOrCluster(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	after := dataFiles(t, cat.tbl)
	if len(after) < 2 {
		t.Fatalf("clustering wrote %d files", len(after))
	}
	if got := u.digest(t, db, after); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d of %d files still overlap", n, len(after))
	}
	if n := unsortedRows(t, db, after); n != 0 {
		t.Errorf("%d rows out of order", n)
	}
	if peak := outer.Peak(); peak > budget {
		t.Errorf("clustering held %s at once, more than its %s", memlimit.Format(peak), memlimit.Format(budget))
	} else {
		t.Logf("clustered %d files into %d holding at most %s of %s", len(before), len(after), memlimit.Format(peak), memlimit.Format(budget))
	}
	if outer.Live() != 0 {
		t.Errorf("%d bytes still allocated after clustering", outer.Live())
	}
	if left, _ := os.ReadDir(spill); len(left) != 0 {
		t.Errorf("left in the spill directory: %v", left)
	}

	// Clustered: the next pass leaves it.
	snap := cat.tbl.CurrentSnapshot().SnapshotID
	if _, err := m.compactOrCluster(ctx, cat.tbl); err != nil {
		t.Fatal(err)
	}
	if cat.tbl.CurrentSnapshot().SnapshotID != snap {
		t.Error("a clustered table was rewritten again")
	}

	// New rows, written unsorted, overlap the clustered files; the next pass
	// clusters them in, and touches only the files they overlap.
	tx := cat.tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), u.write(t, db, filepath.Join(dir, "data"), u.files, 1), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	want = u.digest(t, db, dataFiles(t, cat.tbl))
	if _, err := m.compactOrCluster(ctx, cat.tbl); err != nil {
		t.Fatal(err)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("after an insert: %d files overlap", n)
	}
	if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
		t.Errorf("after an insert: rows changed: %s, want %s", got, want)
	}
}

// arrowSize reads tbl's rows and returns how much memory they take in Arrow.
func arrowSize(t testing.TB, tbl *table.Table) int64 {
	t.Helper()
	_, recs, err := tbl.Scan().ToArrowRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for rec, err := range recs {
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range rec.Columns() {
			for _, b := range c.Data().Buffers() {
				if b != nil {
					n += int64(b.Len())
				}
			}
		}
		rec.Release()
	}
	return n
}

// withTarget gives the catalog's table a target file size of target bytes.
func withTarget(t testing.TB, cat listedCatalog, target int64) *table.Table {
	t.Helper()
	tx := cat.tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{table.WriteTargetFileSizeBytesKey: fmt.Sprint(target)}); err != nil {
		t.Fatal(err)
	}
	tbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// compactOrCluster runs what maintenance runs to rewrite a table's files:
// clustering for a table with a sort order, compaction otherwise.
func (m *Maintainer) compactOrCluster(ctx context.Context, tbl *table.Table) (*table.Table, error) {
	if fields, first, ok := clusterFields(tbl); ok {
		return m.cluster(ctx, tbl, fields, first, quiet)
	}
	return m.compact(ctx, tbl, quiet)
}

// TestClusteringMergesSmallNeighbors checks that a clustered table's small
// files next to each other in the order are merged, and that a lone small
// file, such as the last of a sorted rewrite, is left alone.
func TestClusteringMergesSmallNeighbors(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	order, err := table.NewSortOrder(1, []table.SortField{
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortASC, NullOrder: table.NullsFirst}})
	if err != nil {
		t.Fatal(err)
	}
	// Files of ids 0-999, 1000-1999, ...: disjoint, each written in order.
	u := unsortedTable{files: 4, rows: 1000, keys: 50}
	cat := u.table(t, db, dir, order)
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Fatalf("%d files overlap", n)
	}
	m := New(nil, dir+"/", Options{SpillDir: t.TempDir()}, quiet)
	want := u.digest(t, db, dataFiles(t, cat.tbl))
	if _, err := m.compactOrCluster(context.Background(), cat.tbl); err != nil {
		t.Fatal(err)
	}
	if n := len(dataFiles(t, cat.tbl)); n != 1 {
		t.Errorf("four small neighbors became %d files", n)
	}
	if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	snap := cat.tbl.CurrentSnapshot().SnapshotID
	if _, err := m.compactOrCluster(context.Background(), cat.tbl); err != nil {
		t.Fatal(err)
	}
	if cat.tbl.CurrentSnapshot().SnapshotID != snap {
		t.Error("a lone small file was rewritten")
	}
}

// TestClusterFields checks which sort orders maintenance clusters by.
func TestClusterFields(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "tags", Type: &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String}},
	)
	for _, c := range []struct {
		name   string
		fields []table.SortField
		ok     bool
	}{
		{"identity of a column", []table.SortField{{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: table.SortDESC, NullOrder: table.NullsLast}}, true},
		{"bucket", []table.SortField{{SourceIDs: []int{1}, Transform: iceberg.BucketTransform{NumBuckets: 4}, Direction: table.SortASC, NullOrder: table.NullsFirst}}, false},
		{"unsorted", nil, false},
	} {
		order := table.UnsortedSortOrder
		if c.fields != nil {
			var err error
			if order, err = table.NewSortOrder(1, c.fields); err != nil {
				t.Fatal(err)
			}
		}
		meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, order, t.TempDir(), nil)
		if err != nil {
			t.Fatal(err)
		}
		tbl := table.New(table.Identifier{"s", "t"}, meta, "", nil, nil)
		fields, first, ok := clusterFields(tbl)
		if ok != c.ok || ok && (first != 1 || len(fields) != 1 || fields[0].Name != "id" || !fields[0].Descending || fields[0].NullsFirst) {
			t.Errorf("%s: %v %d %+v", c.name, ok, first, fields)
		}
	}
}

// TestClusteringBacklogTakesPasses clusters a table larger than a pass may
// rewrite: each pass rewrites no more than its limit, and passes converge
// on files whose ranges do not overlap, with the same rows.
func TestClusteringBacklogTakesPasses(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 8, rows: 20_000, keys: 2000}
	cat := u.table(t, db, dir, byAudience(t))
	files := dataFiles(t, cat.tbl)
	want := u.digest(t, db, files)
	var size int64
	tasks, err := cat.tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		size += task.File.FileSizeBytes()
	}
	m := New(nil, dir+"/", Options{SpillDir: t.TempDir()}, quiet)
	m.maxRewrite = size / 3
	withTarget(t, cat, size/16)
	passes := 0
	for ; passes < 20; passes++ {
		before := cat.tbl.CurrentSnapshot()
		prev := map[string]bool{}
		for _, f := range dataFiles(t, cat.tbl) {
			prev[f] = true
		}
		if _, err := m.compactOrCluster(context.Background(), cat.tbl); err != nil {
			t.Fatal(err)
		}
		if cat.tbl.CurrentSnapshot().SnapshotID == before.SnapshotID {
			break
		}
		// What the pass replaced is within its limit, but for the file
		// that crossed it, or the two files any pass merges.
		var replaced, largest int64
		n := 0
		now := map[string]bool{}
		for _, f := range dataFiles(t, cat.tbl) {
			now[f] = true
		}
		for _, task := range tasks {
			if prev[task.File.FilePath()] && !now[task.File.FilePath()] {
				replaced += task.File.FileSizeBytes()
				largest = max(largest, task.File.FileSizeBytes())
				n++
			}
		}
		if n > 2 && replaced > m.maxRewrite+largest {
			t.Errorf("pass %d replaced %d files, %d bytes, more than its %d", passes+1, n, replaced, m.maxRewrite)
		}
		if tasks, err = cat.tbl.Scan().PlanFiles(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if passes < 2 || passes >= 20 {
		t.Errorf("clustered in %d passes", passes)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d files overlap after %d passes", n, passes)
	}
	if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	t.Logf("clustered in %d passes into %d files", passes, len(dataFiles(t, cat.tbl)))
}
