package distributed

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// positionDeleteIDs are the field IDs Iceberg gives a position delete
// file's columns.
const positionDeleteIDs = "file_path: 2147483546, pos: 2147483545"

// TestPositionDeletes checks that workers leave out the rows that position
// delete files delete, as UPDATE, DELETE and MERGE write them: a file of
// deletes of one data file, one of a partition's, which names several data
// files, one that spells a data file's path otherwise than the table's
// metadata, and one that names a data file it does not apply to, which
// must delete nothing there.
func TestPositionDeletes(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.addTable("events", `(SELECT i AS id, i % 13 AS kind, 'e' || i AS name FROM range(40000) t(i))`, 5)
	tbl := h.tables["events"]
	fs := h.files[tbl.key()]
	dir := filepath.Join(h.dir, "warehouse", "events")

	type deleted struct {
		file string
		pos  int64
	}
	var gone []deleted
	write := func(name string, rows []deleted, spell func(string) string) string {
		path := filepath.Join(dir, name+".parquet")
		var values []string
		for _, r := range rows {
			values = append(values, fmt.Sprintf("(%s, %d)", quoteLiteral(spell(r.file)), r.pos))
		}
		h.exec(fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) t(file_path, pos)) TO %s (FORMAT parquet, FIELD_IDS {%s})",
			strings.Join(values, ", "), quoteLiteral(path), positionDeleteIDs))
		return path
	}
	same := func(p string) string { return p }
	span := func(file string, from, to, step int64) []deleted {
		var out []deleted
		for p := from; p < to; p += step {
			out = append(out, deleted{file, p})
		}
		return out
	}
	// One data file's deletes.
	one := span(fs[0].Path, 0, 3000, 7)
	d1 := write("d1", one, same)
	// A partition's: rows of files 1 and 2, attached to both.
	part := append(span(fs[1].Path, 5, 2000, 3), span(fs[2].Path, 0, 100, 1)...)
	d2 := write("d2", part, same)
	// File 3's path as another engine might spell it.
	other := span(fs[3].Path, 10, 500, 2)
	d3 := write("d3", other, func(p string) string { return "file://" + strings.ReplaceAll(p, "/", "//") })
	// Rows of file 4, attached to file 0 only: it deletes nothing.
	d4 := write("d4", span(fs[4].Path, 0, 1000, 1), same)
	gone = append(append(append(gone, one...), part...), other...)

	fs[0].Deletes = &Deletes{Positions: []string{d1, d4}}
	fs[1].Deletes = &Deletes{Positions: []string{d2}}
	fs[2].Deletes = &Deletes{Positions: []string{d2}}
	fs[3].Deletes = &Deletes{Positions: []string{d3}}

	var values []string
	for _, g := range gone {
		values = append(values, fmt.Sprintf("(%s, %d)", quoteLiteral(g.file), g.pos))
	}
	h.exec(fmt.Sprintf(`CREATE OR REPLACE VIEW main.events AS SELECT id, kind, name FROM read_parquet([%s], filename = '__f', file_row_number = true) AS d
		ANTI JOIN (VALUES %s) AS x(f, r) ON x.f = d.__f AND x.r = d.file_row_number`, strings.Join(quoted(paths(fs)), ", "), strings.Join(values, ", ")))
	var n int64
	if err := h.session.QueryRowContext(t.Context(), "SELECT count(*) FROM events").Scan(&n); err != nil || n != 40000-int64(len(gone)) {
		t.Fatalf("the table has %d rows (%v), want %d", n, err, 40000-len(gone))
	}

	for _, q := range []string{
		`SELECT count(*), count(DISTINCT id), sum(id), min(name), max(name) FROM events`,
		`SELECT kind, count(*) AS n, sum(id) AS s FROM events GROUP BY kind ORDER BY kind`,
		`SELECT id, name FROM events ORDER BY id LIMIT 50`,
		`SELECT DISTINCT kind FROM events WHERE id % 1000 = 3 ORDER BY 1`,
		`SELECT e.kind, count(*) FROM events e JOIN nation ON e.kind = n_nationkey GROUP BY 1 ORDER BY 1`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t

	// Read in full by every worker, joined to the table divided among them.
	h.addTable("big", `(SELECT i % 40000 AS id, i AS v FROM range(400000) t(i))`, 6)
	if !h.check(`SELECT e.kind, count(*), sum(b.v) FROM big b JOIN events e ON b.id = e.id GROUP BY 1 ORDER BY 1`) {
		t.Error("did not run distributed")
	}

	// A column named like the one rows are matched to their positions by.
	w := h.workers[0]
	v := viewDef{Name: "__igloo_t0", Columns: []Column{{Name: "file_row_number", Type: "BIGINT", ID: 1}}, Files: []string{fs[0].Path},
		Deletes: []fileDeletes{{File: fs[0].Path, Deletes: Deletes{Positions: []string{d1}}}}}
	if _, err := w.viewStatement(v, "__igloo_deleted_0"); !IsUnsupported(err) {
		t.Errorf("a column called file_row_number: %v", err)
	}
	// Delete files outside the warehouse.
	conn, err := w.opts.Engine.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	outside := filepath.Join(t.TempDir(), "d.parquet")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	v.Columns = tbl.Columns
	v.Deletes[0].Positions = []string{outside}
	if err := w.deletedRows(t.Context(), conn, v, "__igloo_deleted_9"); err == nil || !strings.Contains(err.Error(), "outside the warehouse") {
		t.Errorf("a delete file outside the warehouse: %v", err)
	}
}
