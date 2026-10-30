package distributed

import (
	"testing"

	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// restTable creates table s.t, of format version and with fields, in a
// catalog of icebergtest's, and returns a client of the catalog.
func restTable(t *testing.T, version string, fields ...iceberg.NestedField) *catalog.Client {
	t.Helper()
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	cat, err := catalog.Open(t.Context(), config.Config{Catalog: config.Catalog{URI: fake.URL, Warehouse: "test"}, Storage: config.Storage{Location: dir}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateNamespace(t.Context(), table.Identifier{"s"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateTable(t.Context(), table.Identifier{"s", "t"}, iceberg.NewSchema(0, fields...),
		icebergcatalog.WithProperties(iceberg.Properties{"format-version": version})); err != nil {
		t.Fatal(err)
	}
	return cat
}

// alterTable commits a change to the schema of table s.t, as another
// engine would.
func alterTable(t *testing.T, cat *catalog.Client, change func(*table.UpdateSchema) *table.UpdateSchema) {
	t.Helper()
	tbl, err := cat.LoadTable(t.Context(), table.Identifier{"s", "t"})
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err := change(tx.UpdateSchema(true, false)).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// TestRefreshRefusesChangedTypes checks that a table whose column types
// changed between planning and running, by a promotion another engine
// committed, is not refreshed: workers would read the new snapshot's
// files with the old types, FLOAT for DOUBLE values, and the statement
// runs on one machine instead. Nested fields count, and nested types
// printed as DuckDB prints them (see Coordinator.Table) do not.
func TestRefreshRefusesChangedTypes(t *testing.T) {
	fields := []iceberg.NestedField{
		{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Float32},
		{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "f", Type: iceberg.PrimitiveTypes.Int32}}}},
		{ID: 4, Name: "l", Type: &iceberg.ListType{ElementID: 5, Element: iceberg.PrimitiveTypes.Int32}},
	}
	for name, promote := range map[string]struct {
		path []string
		to   iceberg.Type
	}{
		"float to double": {[]string{"x"}, iceberg.PrimitiveTypes.Float64},
		"struct field":    {[]string{"s", "f"}, iceberg.PrimitiveTypes.Int64},
		"list element":    {[]string{"l", "element"}, iceberg.PrimitiveTypes.Int64},
	} {
		t.Run(name, func(t *testing.T) {
			cat := restTable(t, "2", fields...)
			src := NewIceberg(cat)
			tab, err := src.Table(t.Context(), "s", "t")
			if err != nil {
				t.Fatal(err)
			}
			tab.Columns[1].Type = "STRUCT(f INTEGER)"
			if err := src.Refresh(t.Context(), tab); err != nil {
				t.Fatalf("refresh of an unchanged table: %v", err)
			}
			alterTable(t, cat, func(u *table.UpdateSchema) *table.UpdateSchema {
				return u.UpdateColumn(promote.path, table.ColumnUpdate{FieldType: iceberg.Optional[iceberg.Type]{Val: promote.to, Valid: true}})
			})
			if err := src.Refresh(t.Context(), tab); !IsUnsupported(err) {
				t.Errorf("refresh after the promotion: %v, want the statement kept on one machine", err)
			}
		})
	}
}
