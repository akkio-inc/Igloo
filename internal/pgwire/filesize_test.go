package pgwire

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/engine"
)

// TestNewTablesWriteLargeFiles checks that a table Igloo creates is written
// in files of the Iceberg default target size, not one per row group, as
// DuckDB's Iceberg writer does when a table has no target: 500,000 rows of
// five columns came to 5 files of 13 MB each.
func TestNewTablesWriteLargeFiles(t *testing.T) {
	vc := newViewCatalog(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, vc.cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	srv := startServerOn(t, eng, func(o *Options) { o.Catalog = "lake" })
	admin := srv.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	mustExec(t, admin, "CREATE TEMP TABLE seed AS SELECT i, md5(i::VARCHAR) AS a, md5((i * 3)::VARCHAR) AS b, "+
		"md5((i * 7)::VARCHAR) AS c, md5((i * 11)::VARCHAR) AS d FROM range(500000) r(i)")

	dataFiles := func() int {
		n := 0
		_ = filepath.WalkDir(vc.cfg.Storage.Location, func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
				n++
			}
			return nil
		})
		return n
	}
	for _, c := range []struct{ name, create, insert string }{
		{"made by CREATE TABLE AS", "CREATE TABLE lake.public.made AS SELECT * FROM seed", ""},
		{"filled by INSERT", "CREATE TABLE lake.public.filled AS SELECT * FROM seed LIMIT 0",
			"INSERT INTO lake.public.filled SELECT * FROM seed"},
	} {
		before := dataFiles()
		mustExec(t, admin, c.create)
		if c.insert != "" {
			mustExec(t, admin, c.insert)
		}
		if n := dataFiles() - before; n != 1 {
			t.Errorf("a table %s: %d data files, want 1", c.name, n)
		}
	}
	for _, name := range []string{"made", "filled"} {
		props := vc.fake.Metadata("public", name).Properties()
		if got := props["write.target-file-size-bytes"]; got != "536870912" {
			t.Errorf("%s: write.target-file-size-bytes %q", name, got)
		}
		if got := props["igloo.row-groups"]; got != "auto" {
			t.Errorf("%s: igloo.row-groups %q", name, got)
		}
	}

	// Maintenance sizes the row groups of the tables Igloo marks with
	// write.parquet.row-group-size, which DuckDB's inserts follow.
	mustExec(t, admin, "CREATE TABLE lake.public.grouped AS SELECT * FROM seed LIMIT 0")
	mustExec(t, admin, "SELECT * FROM set_iceberg_table_properties(lake.public.grouped, {'write.parquet.row-group-size': '400000'})")
	mustExec(t, admin, "INSERT INTO lake.public.grouped SELECT * FROM seed")
	var grouped []string
	loc := strings.TrimPrefix(vc.fake.Metadata("public", "grouped").Location(), "file://")
	_ = filepath.WalkDir(loc, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			grouped = append(grouped, p)
		}
		return nil
	})
	if len(grouped) != 1 {
		t.Fatalf("grouped: data files %v under %s", grouped, loc)
	}
	f, err := os.Open(grouped[0])
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close() //nolint:errcheck
	if n := rdr.MetaData().RowGroup(0).NumRows(); n <= 122_880 {
		t.Errorf("grouped: a row group of %d rows, in %d groups", n, rdr.NumRowGroups())
	}

	// DuckDB refuses to write a partitioned table that has a target file
	// size, unless told to ignore it, as Igloo does.
	mustExec(t, admin, "CREATE TABLE lake.public.parts AS SELECT i % 4 AS k, a FROM seed LIMIT 1000")
	mustExec(t, admin, "ALTER TABLE lake.public.parts SET PARTITIONED BY (k)")
	mustExec(t, admin, "INSERT INTO lake.public.parts SELECT i % 4, a FROM seed LIMIT 1000")
	if n := count(t, admin, "SELECT count(*) FROM lake.public.parts"); n != 2000 {
		t.Errorf("parts: %d rows", n)
	}
}
