package iceberg

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// DuckDB plans a write whose filter folds to false without a scan of the
// table. The iceberg extension's UPDATE, and a MERGE's WHEN MATCHED THEN
// UPDATE, then flushed their deletes, of which there are none, without the
// scan's file list, and threw an InternalException as the statement ran:
// DuckDB invalidated its whole database (patches/0008). TestUpdateOfNoRows
// runs each such write in a DuckDB of its own, and checks that it succeeds
// and does what it says, and that the DuckDB serves on.
func TestUpdateOfNoRows(t *testing.T) {
	ext := testExtension(t)
	// DuckDB's iceberg writer creates a relative directory named data.
	t.Chdir(t.TempDir())
	for _, c := range []struct {
		name, stmt string
		// ids are the table's ids after the statement.
		ids []string
	}{
		{"WHERE false", "UPDATE lake.s.m SET id = 2 WHERE false", []string{"1"}},
		{"WHERE NULL", "UPDATE lake.s.m SET id = 2 WHERE NULL", []string{"1"}},
		{"an empty IN list", "UPDATE lake.s.m SET id = 2 WHERE id IN (NULL) AND 1 != 1", []string{"1"}},
		{"MERGE of an empty source", "MERGE INTO lake.s.m USING (SELECT 1 AS id WHERE false) src ON m.id = src.id WHEN MATCHED THEN UPDATE SET id = 2", []string{"1"}},
		{"MERGE ON false", "MERGE INTO lake.s.m USING (SELECT 3 AS id) src ON false WHEN MATCHED THEN UPDATE SET id = 2 WHEN NOT MATCHED THEN INSERT VALUES (src.id)", []string{"1", "3"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			cat := icebergtest.NewCatalog(filepath.Join(t.TempDir(), "warehouse"))
			t.Cleanup(cat.Close)
			db := openLake(t, ext, cat.URL)
			t.Cleanup(func() { _ = db.Close() })
			w := conn(t, db)
			for _, s := range []string{"CREATE SCHEMA lake.s", "CREATE TABLE lake.s.m AS SELECT 1 AS id"} {
				if err := query(ctx, w, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			if err := query(ctx, w, c.stmt); err != nil {
				t.Errorf("%s: %v", c.stmt, err)
			}
			ids, err := strs(ctx, conn(t, db), "SELECT id FROM lake.s.m ORDER BY id")
			if err != nil {
				t.Fatalf("a new connection, after the statement: %v", err)
			}
			if !slices.Equal(ids, c.ids) {
				t.Errorf("after %s the table's ids are %v, want %v", c.stmt, ids, c.ids)
			}
		})
	}
}
