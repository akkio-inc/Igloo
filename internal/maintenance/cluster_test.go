package maintenance

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/filerewrite"
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
	if _, err := m.compactOrCluster(ctx, tbl, quiet); err != nil {
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
	if _, err := m.compactOrCluster(ctx, cat.tbl, quiet); err != nil {
		t.Fatal(err)
	}
	if cat.tbl.CurrentSnapshot().SnapshotID != snap {
		t.Error("a clustered table was rewritten again")
	}

	// New rows, written unsorted, overlap every clustered file: a sixth of
	// the table, enough to sort at once, they are sorted into a run of their
	// own, and the clustered files of full size are left as they are (a
	// small one, the last of the sorted rewrite, is sorted with them).
	var clustered []string
	tasks, err := cat.tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.File.FileSizeBytes() >= (4<<20)*3/4 {
			clustered = append(clustered, task.File.FilePath())
		}
	}
	tx := cat.tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), u.write(t, db, filepath.Join(dir, "data"), u.files, 1), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	want = u.digest(t, db, dataFiles(t, cat.tbl))
	if _, err := m.compactOrCluster(ctx, cat.tbl, quiet); err != nil {
		t.Fatal(err)
	}
	now := dataFiles(t, cat.tbl)
	for _, f := range clustered {
		if !slices.Contains(now, f) {
			t.Errorf("after an insert: clustered file %s was rewritten", filepath.Base(f))
		}
	}
	if n := unsortedRows(t, db, now); n != 0 {
		t.Errorf("after an insert: %d rows out of order", n)
	}
	if got := u.digest(t, db, now); got != want {
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

// withLayout gives the catalog's table a target file size of target bytes
// and row groups of at most rowGroup rows, so that the writer can cut small
// files near the target, as it cuts files of a real table.
func withLayout(t testing.TB, cat listedCatalog, target int64, rowGroup int) *table.Table {
	t.Helper()
	tx := cat.tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{table.WriteTargetFileSizeBytesKey: fmt.Sprint(target),
		table.ParquetRowGroupLimitKey: fmt.Sprint(rowGroup)}); err != nil {
		t.Fatal(err)
	}
	tbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tbl
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
	if _, err := m.compactOrCluster(context.Background(), cat.tbl, quiet); err != nil {
		t.Fatal(err)
	}
	if n := len(dataFiles(t, cat.tbl)); n != 1 {
		t.Errorf("four small neighbors became %d files", n)
	}
	if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	snap := cat.tbl.CurrentSnapshot().SnapshotID
	if _, err := m.compactOrCluster(context.Background(), cat.tbl, quiet); err != nil {
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
	withLayout(t, cat, size/16, 1000)
	passes := 0
	for ; passes < 20; passes++ {
		before := cat.tbl.CurrentSnapshot()
		prev := map[string]bool{}
		for _, f := range dataFiles(t, cat.tbl) {
			prev[f] = true
		}
		if _, err := m.compactOrCluster(context.Background(), cat.tbl, quiet); err != nil {
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

// TestClusteringSplitsHeavyKeys clusters a table whose rows all have one
// audience and one of three versions: each value of the whole sort order has
// more rows than a range may hold, so their rows are dealt out among several
// ranges. Clustering stays within its memory, every file is sorted, and the
// files follow each other in the order: no two files' version ranges
// overlap, and their audience ranges only touch.
func TestClusteringSplitsHeavyKeys(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 6, rows: 80_000, keys: 1}
	cat := u.table(t, db, dir, byAudience(t))
	want := u.digest(t, db, dataFiles(t, cat.tbl))
	const budget = 48 << 20
	if size := arrowSize(t, cat.tbl); size <= budget {
		t.Fatalf("the table takes %s in Arrow, no more than the budget", memlimit.Format(size))
	}
	outer := unmetered()
	ctx := compute.WithAllocator(context.Background(), outer)
	m := New(nil, dir+"/", Options{Memory: budget, SpillDir: t.TempDir()}, quiet)
	tbl := withTarget(t, cat, 2<<20)
	if _, err := m.compactOrCluster(ctx, tbl, quiet); err != nil {
		t.Fatal(err)
	}
	if len(m.clusterFailed) != 0 {
		t.Fatalf("clustering needed more memory than %s", memlimit.Format(budget))
	}
	after := dataFiles(t, cat.tbl)
	if len(after) < 3 {
		t.Fatalf("clustering wrote %d files", len(after))
	}
	if got := u.digest(t, db, after); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d of %d files overlap", n, len(after))
	}
	if n := unsortedRows(t, db, after); n != 0 {
		t.Errorf("%d rows out of order", n)
	}
	type span struct{ lo, hi int }
	var vers []span
	for _, f := range after {
		var s span
		if err := db.QueryRow(fmt.Sprintf("SELECT min(ver), max(ver) FROM read_parquet('%s')", f)).Scan(&s.lo, &s.hi); err != nil {
			t.Fatal(err)
		}
		vers = append(vers, s)
	}
	for i, a := range vers {
		for _, b := range vers[i+1:] {
			if a.lo < b.hi && b.lo < a.hi {
				t.Errorf("files with versions %d-%d and %d-%d overlap", a.lo, a.hi, b.lo, b.hi)
			}
		}
	}
	if peak := outer.Peak(); peak > budget {
		t.Errorf("clustering held %s at once, more than its %s", memlimit.Format(peak), memlimit.Format(budget))
	} else {
		t.Logf("clustered %d rows of one audience into %d files holding at most %s of %s", 6*80_000, len(after), memlimit.Format(peak), memlimit.Format(budget))
	}
}

// TestClusteringFallsBackToCompaction makes clustering need more memory than
// maintenance may use: the table is compacted unsorted instead, with a
// warning, so its small files are still merged. Clustering is not tried
// again while the table stays as that pass left it, and is tried again once
// it changes, or clusterRetry later; once it fits, the table is clustered.
func TestClusteringFallsBackToCompaction(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 6, rows: 2000, keys: 50}
	cat := u.table(t, db, dir, byAudience(t))
	m := New(nil, dir+"/", Options{SpillDir: t.TempDir()}, quiet)
	tries := 0
	m.sorted = func([]filerewrite.SortField) groupWriter {
		return func(context.Context, *table.Table, iceio.IO, table.CompactionTaskGroup, int64) ([]iceberg.DataFile, filerewrite.Layout, error) {
			tries++
			return nil, filerewrite.Layout{}, fmt.Errorf("sort a range: %w", errOverBudget)
		}
	}
	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, nil))
	ctx := context.Background()
	pass := func(wantTries, wantFiles int, what string) {
		t.Helper()
		want := u.digest(t, db, dataFiles(t, cat.tbl))
		if _, err := m.compactOrCluster(ctx, cat.tbl, log); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if tries != wantTries {
			t.Errorf("%s: clustering tried %d times, want %d", what, tries, wantTries)
		}
		files := dataFiles(t, cat.tbl)
		if len(files) != wantFiles {
			t.Errorf("%s: %d files, want %d", what, len(files), wantFiles)
		}
		if got := u.digest(t, db, files); got != want {
			t.Errorf("%s: rows changed: %s, want %s", what, got, want)
		}
	}
	insert := func(n int) {
		t.Helper()
		tx := cat.tbl.NewTransaction()
		if err := tx.AddFiles(ctx, u.write(t, db, filepath.Join(dir, "data"), 100+10*tries, n), nil, false); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expire := func() {
		f := m.clusterFailed[name(cat.tbl.Identifier())]
		f.at = f.at.Add(-clusterRetry - time.Minute)
		m.clusterFailed[name(cat.tbl.Identifier())] = f
	}

	// Six small files, overlapping: compaction merges them.
	pass(1, 1, "first pass")
	if !strings.Contains(logged.String(), "compacting it unsorted") {
		t.Errorf("no warning logged:\n%s", logged.String())
	}
	snap := cat.tbl.CurrentSnapshot().SnapshotID
	pass(1, 1, "unchanged")
	if cat.tbl.CurrentSnapshot().SnapshotID != snap {
		t.Error("an unchanged, compacted table was rewritten")
	}
	// Two more, too few for compaction: they stay, overlapping.
	insert(2)
	pass(2, 3, "after an insert")
	pass(2, 3, "unchanged again")
	expire()
	pass(3, 3, "after the backoff")

	// With the memory it needs, the table is clustered.
	m.sorted = nil
	expire()
	if _, err := m.compactOrCluster(ctx, cat.tbl, log); err != nil {
		t.Fatal(err)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d files overlap once clustering fits", n)
	}
	if len(m.clusterFailed) != 0 {
		t.Error("the table is still taken for one clustering does not fit")
	}
}

// TestClusterFieldsFromProperty checks that a table's clustering key in its
// table property, with no Iceberg sort order, is the order clustering uses.
func TestClusterFieldsFromProperty(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "site", Type: iceberg.PrimitiveTypes.String},
	)
	key := `[{"source-id":2,"transform":"identity","direction":"desc","null-order":"nulls-last"},{"source-id":1,"transform":"identity","direction":"asc","null-order":"nulls-first"}]`
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(),
		iceberg.Properties{catalog.ClusteringKeyProperty: key})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(table.Identifier{"s", "t"}, meta, "", nil, nil)
	fields, first, ok := clusterFields(tbl)
	if !ok || first != 2 || len(fields) != 2 || fields[0].Name != "site" || !fields[0].Descending || fields[1].Name != "id" {
		t.Errorf("%v %d %+v", ok, first, fields)
	}
	if got := catalog.ClusteringKey(meta); got != "LINEAR(site DESC, id)" {
		t.Errorf("clustering key %q", got)
	}
}

// TestClusteringDefersScatteredInserts inserts into a clustered table small
// files whose keys are spread over the whole table. One is left as it is,
// though it overlaps every file, rather than having the table rewritten
// around it; once inserts take freshShare of the table they are sorted
// together, alone, into runs, without rewriting the clustered files; once
// the runs have grown to a quarter of what they overlap, passes merge
// everything, and no files overlap. An insert of one audience is clustered
// in at once. The depth gauges follow.
func TestClusteringDefersScatteredInserts(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 8, rows: 6000, keys: 6000}
	cat := u.table(t, db, dir, byAudience(t))
	var size int64
	for _, f := range dataFiles(t, cat.tbl) {
		st, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		size += st.Size()
	}
	withLayout(t, cat, size/24, 250)
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	m := New(nil, dir+"/", Options{SpillDir: t.TempDir(), Metrics: metrics}, quiet)
	ctx := context.Background()
	pass := func() []string {
		t.Helper()
		before := dataFiles(t, cat.tbl)
		if _, err := m.compactOrCluster(ctx, cat.tbl, quiet); err != nil {
			t.Fatal(err)
		}
		var replaced []string
		now := dataFiles(t, cat.tbl)
		for _, f := range before {
			if !slices.Contains(now, f) {
				replaced = append(replaced, f)
			}
		}
		return replaced
	}
	depth := func() float64 {
		return testutil.ToFloat64(metrics.depth.WithLabelValues(name(cat.tbl.Identifier())))
	}
	for range 20 {
		if len(pass()) == 0 {
			break
		}
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Fatalf("%d files overlap once clustered", n)
	}
	pass()
	if d := depth(); d != 1 {
		t.Errorf("a clustered table's depth is %v", d)
	}
	// The clustered files of full size: small ones, such as the last of a
	// sorted rewrite, are sorted again with fresh files that overlap them.
	var body []string
	tasks, err := cat.tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.File.FileSizeBytes() >= size/24*3/4 {
			body = append(body, task.File.FilePath())
		}
	}

	// Inserts of 600 rows, a 1/80 of the table each, keys from all of it.
	next := 100
	insert := func() {
		t.Helper()
		small := unsortedTable{rows: 600, keys: 6000}
		tx := cat.tbl.NewTransaction()
		if err := tx.AddFiles(ctx, small.write(t, db, filepath.Join(dir, "data"), next, 1), nil, false); err != nil {
			t.Fatal(err)
		}
		next++
		if _, err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	insert()
	if replaced := pass(); len(replaced) != 0 {
		t.Fatalf("one small insert had %d files rewritten", len(replaced))
	}
	if d := depth(); d != 2 {
		t.Errorf("with one insert over every file, the depth is %v, want 2", d)
	}

	// More, until they are sorted into runs and the runs are merged.
	merged, runs := false, 0
	for round := 0; round < 60 && !merged; round++ {
		insert()
		want := u.digest(t, db, dataFiles(t, cat.tbl))
		replaced := pass()
		touchedBody := false
		for _, f := range replaced {
			touchedBody = touchedBody || slices.Contains(body, f)
		}
		switch {
		case len(replaced) == 0:
		case !touchedBody:
			runs++
		default:
			// The merge: passes until nothing overlaps.
			for range 20 {
				if len(pass()) == 0 {
					break
				}
			}
			merged = true
			if runs < 2 {
				t.Errorf("merged after %d runs", runs)
			}
		}
		if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
			t.Fatalf("round %d: rows changed: %s, want %s", round, got, want)
		}
	}
	if !merged {
		t.Fatalf("runs were never merged; %d made", runs)
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d files overlap after the merge", n)
	}
	if n := unsortedRows(t, db, dataFiles(t, cat.tbl)); n != 0 {
		t.Errorf("%d rows out of order", n)
	}
	t.Logf("%d runs, then merged", runs)

	// An insert of one audience falls within a file or two, and is
	// clustered in at once.
	path := filepath.Join(dir, "data", "one-audience.parquet")
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT 1000000 + i AS id, md5('4321') AS audience, 9 AS ver, 'payload' AS payload
		FROM range(300) t(i)) TO '%s' (FORMAT parquet)`, path)); err != nil {
		t.Fatal(err)
	}
	tx := cat.tbl.NewTransaction()
	if err := tx.AddFiles(ctx, []string{path}, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if replaced := pass(); len(replaced) < 2 || len(replaced) > 3 {
		t.Errorf("an insert of one audience had %d files rewritten, want it and a file or two", len(replaced))
	}
	if n := overlaps(t, cat.tbl); n != 0 {
		t.Errorf("%d files overlap after an insert of one audience", n)
	}
}

// TestClusteringTiersRuns inserts into a clustered table, 48 times, four
// small files whose keys span the whole table, a pass after each. Fresh
// files are sorted into runs of level 0 once they stack deeper than
// freshMaxDepth, tierFanIn runs of a level are sorted into one of the next,
// and the table stays shallow, with the same rows, without its clustered
// files rewritten until the runs grow to a quarter of the table.
func TestClusteringTiersRuns(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	u := unsortedTable{files: 20, rows: 6000, keys: 6000}
	cat := u.table(t, db, dir, byAudience(t))
	var size int64
	for _, f := range dataFiles(t, cat.tbl) {
		st, err := os.Stat(f)
		if err != nil {
			t.Fatal(err)
		}
		size += st.Size()
	}
	withLayout(t, cat, size/60, 250)
	m := New(nil, dir+"/", Options{SpillDir: t.TempDir()}, quiet)
	ctx := context.Background()
	pass := func() (replaced []string) {
		t.Helper()
		before := dataFiles(t, cat.tbl)
		if _, err := m.compactOrCluster(ctx, cat.tbl, quiet); err != nil {
			t.Fatal(err)
		}
		now := dataFiles(t, cat.tbl)
		for _, f := range before {
			if !slices.Contains(now, f) {
				replaced = append(replaced, f)
			}
		}
		return replaced
	}
	for range 20 {
		if len(pass()) == 0 {
			break
		}
	}
	clustered := dataFiles(t, cat.tbl)
	fields, first, _ := clusterFields(cat.tbl)
	depth := func() int {
		tasks, err := cat.tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s := spans(tasks, cat.tbl.Schema(), first)
		in := make([]bool, len(s))
		for i := range in {
			in[i] = true
		}
		_, top := depthOf(s, in)
		return top
	}
	small := unsortedTable{rows: 75, keys: 6000}
	deepest, levels := 0, map[int]bool{}
	for k := range 48 {
		tx := cat.tbl.NewTransaction()
		if err := tx.AddFiles(ctx, small.write(t, db, filepath.Join(dir, "data"), 1000+4*k, 4), nil, false); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		want := u.digest(t, db, dataFiles(t, cat.tbl))
		deepest = max(deepest, depth())
		for _, f := range pass() {
			if slices.Contains(clustered, f) {
				t.Fatalf("insert %d: a clustered file was rewritten while the runs are small", k+1)
			}
		}
		for _, f := range dataFiles(t, cat.tbl) {
			if level, _, ok := filerewrite.RunOf(f, fields); ok {
				levels[level] = true
			}
		}
		if got := u.digest(t, db, dataFiles(t, cat.tbl)); got != want {
			t.Fatalf("insert %d: rows changed: %s, want %s", k+1, got, want)
		}
	}
	if !levels[0] || !levels[1] {
		t.Errorf("run levels seen: %v, want 0 and 1", levels)
	}
	// Fresh files up to freshMaxDepth and the four of one insert, a
	// clustered file, and tierFanIn-1 runs of each of two levels.
	if limit := freshMaxDepth + 4 + 1 + 2*(tierFanIn-1); deepest > limit {
		t.Errorf("the table stacked %d deep, more than %d", deepest, limit)
	}
	if n := unsortedRows(t, db, dataFiles(t, cat.tbl)); n != 0 {
		t.Errorf("%d rows out of order", n)
	}
	t.Logf("deepest %d, run levels %v", deepest, levels)
}
