package pgwire

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// The catalog counts a table's rows as queries see them (igloo.tables'
// row_count): after DuckDB's updates and deletes, which leave the old rows
// in their files and delete them with position deletes; after a
// materialized view's refresh, which deletes every row and inserts the new
// ones; after CREATE OR REPLACE TABLE; and after other engines' copy-on-
// write and merge-on-read deletes and overwrites. Equality deletes, whose
// rows only a scan can count, leave it unknown.
func TestCatalogRowCounts(t *testing.T) {
	env := newCatalogEnv(t)
	if env.fake == nil {
		t.Skip("needs icebergtest's catalog")
	}
	ctx := context.Background()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	cat, err := catalog.Open(ctx, env.direct)
	if err != nil {
		t.Fatal(err)
	}
	rows := func(name string) int64 {
		t.Helper()
		m, err := cat.TableMetadata(ctx, "public", name, false)
		if err != nil {
			t.Fatal(err)
		}
		return m.Rows
	}
	check := func(what, name string, want int64) {
		t.Helper()
		if n := count(t, conn, "SELECT count(*) FROM "+name); n != want {
			t.Fatalf("%s: the table has %d rows, want %d", what, n, want)
		}
		if got := rows(name); got != want {
			t.Errorf("%s: the catalog counts %d rows, want %d", what, got, want)
		}
	}

	tbl := env.table
	mustExec(t, conn, "CREATE TABLE "+tbl+" AS SELECT range AS id, range % 10 AS g FROM range(2000)")
	check("created", tbl, 2000)
	mustExec(t, conn, "UPDATE "+tbl+" SET g = g + 1")
	check("every row updated", tbl, 2000)
	mustExec(t, conn, "UPDATE "+tbl+" SET g = g + 1 WHERE id < 500")
	check("updated again", tbl, 2000)
	mustExec(t, conn, "DELETE FROM "+tbl+" WHERE id >= 1900")
	check("deleted", tbl, 1900)
	mustExec(t, conn, "INSERT INTO "+tbl+" SELECT range + 5000, 0 FROM range(5)")
	mustExec(t, conn, "DELETE FROM "+tbl+" WHERE id % 2 = 0")
	check("deleted from several files", tbl, 952)

	mv := tbl + "_mv"
	mustExec(t, conn, "CREATE MATERIALIZED VIEW "+mv+" AS SELECT g, count(*) AS n FROM "+tbl+" GROUP BY g")
	check("a materialized view", mv, 11)
	mustExec(t, conn, "REFRESH MATERIALIZED VIEW "+mv)
	mustExec(t, conn, "REFRESH MATERIALIZED VIEW "+mv)
	check("refreshed twice", mv, 11)

	replaced := tbl + "_replaced"
	mustExec(t, conn, "CREATE TABLE "+replaced+" AS SELECT range AS id FROM range(10)")
	mustExec(t, conn, "DELETE FROM "+replaced+" WHERE id < 5")
	mustExec(t, conn, "CREATE OR REPLACE TABLE "+replaced+" AS SELECT range AS id FROM range(3)")
	check("replaced", replaced, 3)

	// Other engines: iceberg-go's deletes, copy-on-write by default and
	// merge-on-read when the table asks, and an overwrite.
	other := tbl + "_other"
	mustExec(t, conn, "CREATE TABLE "+other+" AS SELECT range AS id FROM range(100)")
	commit := func(what string, change func(*table.Transaction) error) {
		t.Helper()
		ice, err := cat.LoadTable(ctx, catalog.Ident("public", other))
		if err != nil {
			t.Fatal(err)
		}
		tx := ice.NewTransaction()
		if err := change(tx); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	commit("copy-on-write delete", func(tx *table.Transaction) error {
		return tx.Delete(ctx, iceberg.LessThan(iceberg.Reference("id"), int64(10)), nil)
	})
	check("copy-on-write delete", other, 90)
	commit("merge-on-read", func(tx *table.Transaction) error {
		return tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead})
	})
	commit("merge-on-read delete", func(tx *table.Transaction) error {
		return tx.Delete(ctx, iceberg.LessThan(iceberg.Reference("id"), int64(20)), nil)
	})
	check("merge-on-read delete", other, 80)
	commit("equality delete", func(tx *table.Transaction) error {
		ice, err := cat.LoadTable(ctx, catalog.Ident("public", other))
		if err != nil {
			return err
		}
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
			ice.Location()+"/data/eq-deletes.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 100)
		if err != nil {
			return err
		}
		return tx.NewRowDelta(nil).AddDeletes(b.EqualityFieldIDs([]int{1}).Build()).Commit(ctx)
	})
	if got := rows(other); got != -1 {
		t.Errorf("with equality deletes, the catalog counts %d rows, want -1 (unknown)", got)
	}
}
