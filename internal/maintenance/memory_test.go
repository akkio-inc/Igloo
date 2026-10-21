package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/filerewrite"
	"github.com/akkio-inc/igloo/internal/memlimit"
)

// Compaction used to read a group's files all at once, one per core, in
// batches of 131,072 rows, and queue 64 batches for the writer, so its
// memory grew with the width of the rows and the number of files: on
// 2026-10-20 a pass over tables mirrored from Snowflake grew Igloo to 30 GB
// and the kernel killed it, again after every restart. These tests compact
// tables of DuckDB-written files, whose repetitive strings take a few bytes
// in Parquet and dozens in Arrow, and check that compaction stays within
// Options.Memory.

// wideTable describes a table of DuckDB-written Parquet files like those of
// a table mirrored from Snowflake: numbers, repetitive strings and hashes.
type wideTable struct {
	files, rows      int // files, and rows per file
	ints, reps, uniq int // BIGINT, repetitive VARCHAR and unique VARCHAR columns
}

func (w wideTable) schema() *iceberg.Schema {
	fields := []iceberg.NestedField{{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}}
	add := func(prefix string, n int, typ iceberg.Type) {
		for i := range n {
			fields = append(fields, iceberg.NestedField{ID: len(fields) + 1, Name: fmt.Sprintf("%s%d", prefix, i), Type: typ})
		}
	}
	add("n", w.ints, iceberg.PrimitiveTypes.Int64)
	add("s", w.reps, iceberg.PrimitiveTypes.String)
	add("h", w.uniq, iceberg.PrimitiveTypes.String)
	return iceberg.NewSchema(0, fields...)
}

// write writes the files into dir with DuckDB, as Igloo's writes do.
func (w wideTable) write(t testing.TB, db *sql.DB, dir string) []string {
	t.Helper()
	cols := []string{"i AS id"}
	for i := range w.ints {
		cols = append(cols, fmt.Sprintf("(i * %d) %% %d AS n%d", i+7, 1000*(i+1), i))
	}
	for i := range w.reps {
		cols = append(cols, fmt.Sprintf("'campaign-%d-' || ((i // 1000 + %d) %% 40) || '-awareness-prospecting' AS s%d", i, i, i))
	}
	for i := range w.uniq {
		cols = append(cols, fmt.Sprintf("md5((i + %d)::VARCHAR) AS h%d", i, i))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for f := range w.files {
		path := filepath.Join(dir, fmt.Sprintf("duckdb-%03d.parquet", f))
		stmt := fmt.Sprintf("COPY (SELECT %s FROM range(%d, %d) t(i)) TO '%s' (FORMAT parquet)",
			strings.Join(cols, ", "), f*w.rows, (f+1)*w.rows, path)
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

// listedCatalog is a localCatalog that lists its one table.
type listedCatalog struct{ *localCatalog }

func (c listedCatalog) Tables(context.Context) ([]table.Identifier, error) {
	return []table.Identifier{c.tbl.Identifier()}, nil
}

// table registers the files as the Iceberg table mirror.wide, in dir.
func (w wideTable) table(t testing.TB, db *sql.DB, dir string) listedCatalog {
	t.Helper()
	ctx := context.Background()
	meta, err := table.NewMetadata(w.schema(), iceberg.UnpartitionedSpec, table.UnsortedSortOrder, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	cat := &localCatalog{}
	cat.tbl = table.New(table.Identifier{"mirror", "wide"}, meta, "", func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, cat)
	tx := cat.tbl.NewTransaction()
	if err := tx.AddFiles(ctx, w.write(t, db, filepath.Join(dir, "data")), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return listedCatalog{cat}
}

func openDuckDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// dataFiles are the paths of the table's data files.
func dataFiles(t testing.TB, tbl *table.Table) []string {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, task := range tasks {
		paths = append(paths, task.File.FilePath())
	}
	return paths
}

// digest sums up the rows of files with DuckDB, to compare a table's rows
// before and after compaction.
func (w wideTable) digest(t testing.TB, db *sql.DB, files []string) string {
	t.Helper()
	var cols []string
	for _, f := range w.schema().Fields() {
		cols = append(cols, f.Name)
	}
	var got string
	err := db.QueryRow(fmt.Sprintf("SELECT count(*)::VARCHAR || ' ' || sum(id)::VARCHAR || ' ' || sum(hash(%s))::VARCHAR FROM read_parquet(['%s'])",
		strings.Join(cols, ", "), strings.Join(files, "', '"))).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// unmetered counts what is allocated through it without limiting it.
func unmetered() *filerewrite.Meter {
	return filerewrite.NewMeter(memory.NewGoAllocator(), math.MaxInt64, nil)
}

func TestPlan(t *testing.T) {
	const budget = 1 << 30
	fits := func(lay filerewrite.Layout, rowBytes, fixed int64) {
		t.Helper()
		if used := fixed + 8*rowBytes*lay.RowGroupRows; used > budget || lay.BatchRows > lay.RowGroupRows/4 || lay.BatchRows < 1 {
			t.Errorf("%+v of %d-byte rows takes %d", lay, rowBytes, used)
		}
	}
	// Narrow rows: DuckDB's row groups.
	lay, err := filerewrite.Plan(budget, 100, 10<<20, filerewrite.DefaultRowGroupRows)
	if err != nil || lay != (filerewrite.Layout{BatchRows: filerewrite.DefaultRowGroupRows / 4, RowGroupRows: filerewrite.DefaultRowGroupRows}) {
		t.Errorf("narrow rows: %+v, %v", lay, err)
	}
	// Wide rows (thousands of columns): smaller row groups.
	lay, err = filerewrite.Plan(budget, 10_000, 64<<20, filerewrite.DefaultRowGroupRows)
	if err != nil || lay.RowGroupRows >= filerewrite.DefaultRowGroupRows || lay.RowGroupRows < filerewrite.MinRowGroupRows {
		t.Errorf("wide rows: %+v, %v", lay, err)
	}
	fits(lay, 10_000, 64<<20)
	// A table's own smaller row groups are kept.
	if lay, err := filerewrite.Plan(budget, 100, 0, 5000); err != nil || lay.RowGroupRows != 5000 {
		t.Errorf("row-group-limit 5000: %+v, %v", lay, err)
	}
	// Rows too wide for the smallest row group, or reading that alone takes
	// the budget: no compaction.
	for _, c := range [][2]int64{{100_000, 0}, {100, budget}} {
		if _, err := filerewrite.Plan(budget, c[0], c[1], filerewrite.DefaultRowGroupRows); !errors.Is(err, errOverBudget) {
			t.Errorf("rows of %d bytes, %d fixed: %v", c[0], c[1], err)
		}
	}
}

// TestCompactionStaysWithinMemory compacts six DuckDB-written files of 50,000
// rows, about 400 MB in Arrow, with 160 MiB. Before compaction read files one
// at a time in batches sized to the budget, it held all of them at once.
func TestCompactionStaysWithinMemory(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	w := wideTable{files: 6, rows: 50_000, ints: 10, reps: 30, uniq: 2}
	cat := w.table(t, db, dir)
	before := dataFiles(t, cat.tbl)
	want := w.digest(t, db, before)

	const budget = 160 << 20
	outer := unmetered()
	ctx := compute.WithAllocator(context.Background(), outer)
	m := New(nil, dir+"/", Options{Memory: budget}, quiet)
	if _, err := m.compact(ctx, cat.tbl, quiet); err != nil {
		t.Fatal(err)
	}
	after := dataFiles(t, cat.tbl)
	if len(after) != 1 {
		t.Fatalf("compacted %d files into %d", len(before), len(after))
	}
	if got := w.digest(t, db, after); got != want {
		t.Errorf("rows changed: %s, want %s", got, want)
	}
	if peak := outer.Peak(); peak > budget {
		t.Errorf("compaction held %s at once, more than its %s", memlimit.Format(peak), memlimit.Format(budget))
	} else {
		t.Logf("compaction held at most %s of %s", memlimit.Format(peak), memlimit.Format(budget))
	}
	if outer.Live() != 0 {
		t.Errorf("%d bytes still allocated after compaction", outer.Live())
	}

	// The rows are too wide for 160 MiB to hold DuckDB's row groups of
	// 122,880 rows: smaller ones fit.
	f, err := iceio.LocalFS{}.Open(after[0])
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close() //nolint:errcheck
	for i := range rdr.NumRowGroups() {
		if n := rdr.MetaData().RowGroup(i).NumRows(); n > filerewrite.DefaultRowGroupRows/2 || (n < filerewrite.MinRowGroupRows && i < rdr.NumRowGroups()-1) {
			t.Errorf("row group %d has %d rows", i, n)
		}
	}
}

// A table that cannot be compacted within the memory compaction may use is
// left as it is, the failure is reported, and the table is not tried again
// until it changes.
func TestCompactionOverBudgetLeavesTheTable(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	cat := wideTable{files: 6, rows: 20_000, ints: 4, reps: 10, uniq: 1}.table(t, db, dir)
	before := dataFiles(t, cat.tbl)
	var failures []string
	// The warehouse is elsewhere, so the pass does not look for orphan files.
	m := New(cat, "s3://elsewhere/", Options{Interval: time.Hour, Retention: time.Hour, Memory: 4 << 20,
		Failed: func(table string, err error) { failures = append(failures, table+": "+err.Error()) }}, quiet)

	m.RunOnce(context.Background())
	if len(failures) != 1 || !strings.Contains(failures[0], "mirror.wide: compact: rewriting the files needs more memory than IGLOO_MAINTENANCE_MEMORY allows") {
		t.Fatalf("failures: %q", failures)
	}
	if after := dataFiles(t, cat.tbl); len(after) != len(before) {
		t.Errorf("%d files became %d", len(before), len(after))
	}
	m.RunOnce(context.Background())
	if len(failures) != 1 {
		t.Errorf("tried again though the table did not change: %q", failures[1:])
	}

	// With enough memory it is compacted.
	m.opts.Memory = 0
	m.tooBig = map[string]int64{}
	m.RunOnce(context.Background())
	if after := dataFiles(t, cat.tbl); len(failures) != 1 || len(after) != 1 {
		t.Errorf("default memory: %d files, failures %q", len(after), failures)
	}
}

// If Igloo dies while it rewrites a table's files, as when the kernel kills
// it for its memory, a replica that starts again does not rewrite them
// again, which would kill it again, until interruptedBackoff has passed.
func TestInterruptedRewriteIsNotRepeated(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	cat := wideTable{files: 6, rows: 1000, ints: 2, reps: 2, uniq: 1}.table(t, db, dir)
	sched := fakeSchedule{rewriteStartedJob + "mirror.wide": time.Now().Add(-time.Hour)}
	var failures []string
	opts := Options{Interval: time.Hour, Retention: time.Hour, Schedule: sched,
		Failed: func(table string, err error) { failures = append(failures, table+": "+err.Error()) }}

	New(cat, "s3://elsewhere/", opts, quiet).RunOnce(context.Background())
	if len(failures) != 1 || !strings.Contains(failures[0], "never ended") {
		t.Fatalf("failures: %q", failures)
	}
	if n := len(dataFiles(t, cat.tbl)); n != 6 {
		t.Fatalf("rewritten after an interrupted rewrite: %d files", n)
	}

	// A day later it is rewritten, and the end is recorded.
	sched[rewriteStartedJob+"mirror.wide"] = time.Now().Add(-interruptedBackoff - time.Minute)
	New(cat, "s3://elsewhere/", opts, quiet).RunOnce(context.Background())
	if n := len(dataFiles(t, cat.tbl)); len(failures) != 1 || n != 1 {
		t.Fatalf("after the backoff: %d files, failures %q", n, failures)
	}
	started, ended := sched[rewriteStartedJob+"mirror.wide"], sched[rewriteEndedJob+"mirror.wide"]
	if time.Since(started) > time.Minute || ended.Before(started) {
		t.Errorf("recorded start %v, end %v", started, ended)
	}
}

// heapPeak samples the Go heap until stop is called, which returns the
// largest heap seen.
func heapPeak() (stop func() uint64) {
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	var peak uint64
	done, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		start := time.Now()
		for {
			metrics.Read(samples)
			if v := samples[0].Value.Uint64(); v > peak {
				// Progress, in case the process is killed for it.
				if v>>29 > peak>>29 {
					fmt.Fprintf(os.Stderr, "%6.1fs: Go heap reached %d MiB\n", time.Since(start).Seconds(), v>>20)
				}
				peak = v
			}
			select {
			case <-done:
				return
			case <-tick.C:
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-finished
		return peak
	}
}

// TestCompactionMemoryRepro compacts a table of large DuckDB-written files
// and reports the memory compaction took, to reproduce the 2026-10-20 outage
// at a size that fits a laptop. It runs only with IGLOO_TEST_COMPACTION_REPRO
// set to files,rows,ints,reps,uniq (wideTable's fields), e.g.
// 14,250000,20,20,4; IGLOO_TEST_COMPACTION_MEMORY sets Options.Memory in
// bytes. Run it in a container with a memory limit (docker run --memory 6g).
func TestCompactionMemoryRepro(t *testing.T) {
	spec := os.Getenv("IGLOO_TEST_COMPACTION_REPRO")
	if spec == "" {
		t.Skip("IGLOO_TEST_COMPACTION_REPRO not set")
	}
	var n [5]int
	for i, s := range strings.Split(spec, ",") {
		n[i], _ = strconv.Atoi(s)
	}
	w := wideTable{files: n[0], rows: n[1], ints: n[2], reps: n[3], uniq: n[4]}
	dir := t.TempDir()
	start := time.Now()
	db := openDuckDB(t)
	cat := w.table(t, db, dir)
	_ = db.Close()
	var size int64
	tasks, err := cat.tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		size += task.File.FileSizeBytes()
	}
	t.Logf("wrote %d files, %d rows, %d columns, %s of Parquet in %v",
		len(tasks), w.files*w.rows, len(w.schema().Fields()), memlimit.Format(size), time.Since(start).Round(time.Second))

	var budget int64
	if v := os.Getenv("IGLOO_TEST_COMPACTION_MEMORY"); v != "" {
		budget, _ = strconv.ParseInt(v, 10, 64)
	}
	runtime.GC()
	outer := unmetered()
	ctx := compute.WithAllocator(context.Background(), outer)
	m := New(nil, dir+"/", Options{Interval: time.Hour, Retention: time.Hour, Memory: budget}, quiet)
	stop := heapPeak()
	start = time.Now()
	_, err = m.compact(ctx, cat.tbl, quiet)
	heap := stop()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compacted %d files into %d in %v: peak Arrow memory %s, peak Go heap %s (GOMAXPROCS %d, memory limit %s)",
		len(tasks), len(dataFiles(t, cat.tbl)), time.Since(start).Round(time.Second), memlimit.Format(outer.Peak()),
		memlimit.Format(int64(heap)), runtime.GOMAXPROCS(0), memlimit.Format(m.memory()))
}
