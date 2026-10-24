package maintenance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"

	"github.com/akkio-inc/igloo/internal/memlimit"
)

// TestCompactionAppliesLargeDeletesAFileAtATime compacts a table whose one
// position delete file covers half of every file's rows, as an UPDATE of
// every row leaves one, within a budget that holds one file's deletes but
// not all of them: the rewrite applies them a file at a time rather than
// refusing the table.
func TestCompactionAppliesLargeDeletesAFileAtATime(t *testing.T) {
	db := openDuckDB(t)
	dir := t.TempDir()
	w := wideTable{files: 6, rows: 200_000, ints: 2, reps: 2}
	cat := w.table(t, db, dir)
	files := dataFiles(t, cat.tbl)

	// The even positions of every file, sorted by path, as writers sort
	// position deletes.
	var parts []string
	for _, f := range files {
		parts = append(parts, fmt.Sprintf("SELECT '%s' AS file_path, CAST(2 * i AS BIGINT) AS pos FROM range(%d) r(i)", f, w.rows/2))
	}
	deletes := filepath.Join(dir, "data", "deletes.parquet")
	if _, err := db.Exec("COPY (" + strings.Join(parts, " UNION ALL ") + " ORDER BY file_path, pos) TO '" + deletes + "' (FORMAT parquet, ROW_GROUP_SIZE 50000)"); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(deletes)
	if err != nil {
		t.Fatal(err)
	}
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, deletes, iceberg.ParquetFile,
		nil, nil, nil, int64(len(files)*w.rows/2), st.Size())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx := cat.tbl.NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(b.Build()).Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// What the table holds: the odd positions of each file.
	var cols []string
	for _, f := range w.schema().Fields() {
		cols = append(cols, f.Name)
	}
	var want string
	if err := db.QueryRow(fmt.Sprintf("SELECT count(*)::VARCHAR || ' ' || sum(id)::VARCHAR || ' ' || sum(hash(%s))::VARCHAR FROM read_parquet(['%s'], file_row_number = true) WHERE file_row_number %% 2 = 1",
		strings.Join(cols, ", "), strings.Join(files, "', '"))).Scan(&want); err != nil {
		t.Fatal(err)
	}

	// 600,000 deleted positions take 37 MiB as iceberg-go loads them; one
	// file's 100,000, under 2.
	const budget = 20 << 20
	outer := unmetered()
	m := New(nil, dir+"/", Options{Memory: budget}, quiet)
	if _, err := m.compact(compute.WithAllocator(ctx, outer), cat.tbl, quiet); err != nil {
		t.Fatal(err)
	}
	after := dataFiles(t, cat.tbl)
	if len(after) > len(files) || cat.tbl.CurrentSnapshot().Summary.Operation != "replace" {
		t.Fatalf("not compacted: %d files, then %d, %v", len(files), len(after), cat.tbl.CurrentSnapshot().Summary)
	}
	if got := w.digest(t, db, after); got != want {
		t.Errorf("rows: %s, want %s", got, want)
	}
	tasks, err := cat.tbl.Scan().PlanFiles(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if len(task.DeleteFiles) > 0 {
			t.Errorf("%s still has deletes", task.File.FilePath())
		}
	}
	if peak := outer.Peak(); peak > budget {
		t.Errorf("compaction held %s at once, more than its %s", memlimit.Format(peak), memlimit.Format(budget))
	} else {
		t.Logf("compaction held at most %s of %s", memlimit.Format(peak), memlimit.Format(budget))
	}
}
