package distributed

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// largestRowGroup returns how many row groups the Parquet file at path has,
// and the most rows and uncompressed bytes one of them holds.
func largestRowGroup(t *testing.T, db *sql.DB, path string) (groups int, rows, bytes int64) {
	t.Helper()
	err := db.QueryRow(`SELECT count(*), max(n), max(b) FROM (SELECT row_group_id, max(row_group_num_rows) AS n,
		sum(total_uncompressed_size) AS b FROM parquet_metadata('`+path+`') GROUP BY ALL)`).Scan(&groups, &rows, &bytes)
	if err != nil {
		t.Fatal(err)
	}
	return groups, rows, bytes
}

// TestTaskResultRowGroups checks the row groups of a task's result of rows
// of 2 KiB: about RowGroupBytes each where the rows have no order to keep,
// where DuckDB's 122,880 rows put them all in one; and, where the task sorts
// them, rows kept in order, in groups of RowGroupRows of their types' width.
func TestTaskResultRowGroups(t *testing.T) {
	h := newHarness(t, 0.01, 2, 1)
	w := h.workers[0]
	li := h.tables["lineitem"]
	view := viewDef{Name: "__igloo_split", Columns: li.Columns, Files: []string{h.files[li.key()][0].Path}}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	const width = 64 * 32
	wide := "SELECT l_orderkey * 8 + l_linenumber AS k, CAST(repeat(md5(CAST(l_orderkey * 8 + l_linenumber AS VARCHAR)), 64) AS BLOB) AS b FROM __igloo_split"

	path, res, err := w.run(t.Context(), &taskRequest{Query: "q-wide", Version: "test", Views: []viewDef{view}, SQL: wide})
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path) //nolint:errcheck
	groups, rows, bytes := largestRowGroup(t, db, path)
	// DuckDB ends a row group at the first vector of rows, up to 2,048,
	// past the bytes.
	if most := int64(RowGroupBytes + 2048*(width+64)); groups < 2 || bytes > most {
		t.Errorf("%d rows of %d bytes in %d row groups, the largest of %d rows and %d bytes; want at most %d", res.Rows, width, groups, rows, bytes, most)
	}

	path, res, err = w.run(t.Context(), &taskRequest{Query: "q-wide-sorted", Version: "test", Views: []viewDef{view}, SQL: wide + " ORDER BY k DESC",
		Sorted: true})
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path) //nolint:errcheck
	if _, rows, _ := largestRowGroup(t, db, path); rows > max(RowGroupRows(rowWidth(res.Columns)), 2048) {
		t.Errorf("sorted rows in row groups of up to %d rows, want %d", rows, RowGroupRows(rowWidth(res.Columns)))
	}
	var out int64
	if err := db.QueryRow(`SELECT count(*) FROM (SELECT k, lag(k) OVER (ORDER BY file_row_number) AS before FROM read_parquet('` + path +
		`', file_row_number = true)) WHERE k > before`).Scan(&out); err != nil || out != 0 {
		t.Errorf("%d of %d sorted rows out of order (%v)", out, res.Rows, err)
	}
}

// TestWriteRowGroups checks that a merge's writer ends its row groups at
// about RowGroupBytes, where rows of 64 KiB made 122,880 of them each, in
// order.
func TestWriteRowGroups(t *testing.T) {
	const rows, width = 1000, 64 << 10
	schema := arrow.NewSchema([]arrow.Field{{Name: "k", Type: arrow.PrimitiveTypes.Int64}, {Name: "b", Type: arrow.BinaryTypes.Binary}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	blob := make([]byte, width)
	for i := range rows {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		blob[0] = byte(i)
		b.Field(1).(*array.BinaryBuilder).Append(blob)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	path := filepath.Join(t.TempDir(), "merged.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(mergeRowGroup)), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRowGroups(w, rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	groups, most, bytes := largestRowGroup(t, db, path)
	if groups < 8 || bytes > RowGroupBytes+2*width {
		t.Errorf("%d rows of %d bytes in %d row groups, the largest of %d rows and %d bytes", rows, width, groups, most, bytes)
	}
	var out int64
	if err := db.QueryRow(`SELECT count(*) FILTER (WHERE k <> file_row_number) FROM read_parquet('` + path + `', file_row_number = true)`).Scan(&out); err != nil || out != 0 {
		t.Errorf("%d rows out of order (%v)", out, err)
	}
}
