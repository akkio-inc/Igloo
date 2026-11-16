package pgwire

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// TestAlterTableKeepsColumnComments checks that ALTER TABLE keeps the
// comments on a table's columns, the Iceberg schema's field docs, in the
// catalog and as igloo_comments() lists them: renaming a column keeps its
// own comment under its new name, and adding, dropping, retyping one or
// dropping its NOT NULL leaves every other column's as it was, alone and in
// a transaction block. Apps keep tags and descriptions there, and every one
// of a table's comments used to go with any such change
// (extensions/iceberg/patches/0009).
func TestAlterTableKeepsColumnComments(t *testing.T) {
	vc := newViewCatalog(t)
	r := vc.replica(t)
	ctx := context.Background()
	conn := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	const table = "tagged"
	name := "lake.public." + table
	mustExec(t, conn, "CREATE TABLE "+name+" (a INTEGER, b VARCHAR, c DOUBLE, d INTEGER, s STRUCT(x INTEGER, y VARCHAR), n INTEGER NOT NULL)")
	// tag gives each column named a comment, as an app keeps its tags
	// there, so that each change below meets a table whose every column
	// has one.
	want := map[string]string{}
	tag := func(cols ...string) {
		t.Helper()
		for _, col := range cols {
			want[col] = "Tags of " + col + ": {\"pii\": false, \"owner\": \"it's\"}"
			mustExec(t, conn, "COMMENT ON COLUMN "+name+"."+col+" IS '"+strings.ReplaceAll(want[col], "'", "''")+"'")
		}
	}
	tag("a", "b", "c", "d", "s", "n")
	docs := func() map[string]string {
		t.Helper()
		tbl, err := cat.LoadTable(ctx, catalog.Ident("public", table))
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, f := range tbl.Schema().Fields() {
			got[f.Name] = f.Doc
		}
		return got
	}
	shown := func() map[string]string {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT column_name, comment FROM igloo_comments() WHERE table_name = '"+table+"' AND column_name IS NOT NULL")
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for rows.Next() {
			var col, doc string
			if err := rows.Scan(&col, &doc); err != nil {
				t.Fatal(err)
			}
			got[col] = doc
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return got
	}
	check := func(step string, cols []string) {
		t.Helper()
		got := docs()
		if !slices.Equal(slices.Sorted(maps.Keys(got)), slices.Sorted(slices.Values(cols))) {
			t.Fatalf("%s: the table's columns are %v, want %v", step, slices.Sorted(maps.Keys(got)), cols)
		}
		for _, col := range cols {
			if got[col] != want[col] {
				t.Errorf("%s: column %s's doc is %q, want %q", step, col, got[col], want[col])
			}
		}
		listed := map[string]string{}
		for col, doc := range want {
			if doc != "" && slices.Contains(cols, col) {
				listed[col] = doc
			}
		}
		if s := shown(); !maps.Equal(s, listed) {
			t.Errorf("%s: igloo_comments() lists %v, want %v", step, s, listed)
		}
	}
	check("after COMMENT ON COLUMN", []string{"a", "b", "c", "d", "s", "n"})

	mustExec(t, conn, "ALTER TABLE "+name+" RENAME COLUMN b TO b2")
	want["b2"] = want["b"]
	check("after RENAME COLUMN", []string{"a", "b2", "c", "d", "s", "n"})

	tag("a", "b2", "c", "d", "s", "n")
	mustExec(t, conn, "ALTER TABLE "+name+" ADD COLUMN e VARCHAR")
	check("after ADD COLUMN", []string{"a", "b2", "c", "d", "s", "n", "e"})

	tag("a", "b2", "c", "d", "s", "n", "e")
	mustExec(t, conn, "ALTER TABLE "+name+" DROP COLUMN c")
	check("after DROP COLUMN", []string{"a", "b2", "d", "s", "e", "n"})

	tag("a", "b2", "d", "s", "e", "n")
	mustExec(t, conn, "ALTER TABLE "+name+" ALTER COLUMN d TYPE BIGINT")
	check("after a change of type", []string{"a", "b2", "d", "s", "e", "n"})

	tag("a", "b2", "d", "s", "e", "n")
	mustExec(t, conn, "ALTER TABLE "+name+" ALTER COLUMN n DROP NOT NULL")
	check("after DROP NOT NULL", []string{"a", "b2", "d", "s", "e", "n"})

	// In one transaction block, as a migration runs.
	tag("a", "b2", "d", "s", "e", "n")
	mustExec(t, conn, "BEGIN")
	mustExec(t, conn, "ALTER TABLE "+name+" RENAME COLUMN a TO a2")
	mustExec(t, conn, "ALTER TABLE "+name+" ADD COLUMN f INTEGER")
	mustExec(t, conn, "COMMIT")
	want["a2"] = want["a"]
	check("after a block that renamed and added", []string{"a2", "b2", "d", "s", "e", "f", "n"})
}
