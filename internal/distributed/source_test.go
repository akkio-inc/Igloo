package distributed

import (
	"strings"
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

// setSchema makes fields the schema of table s.t, as another engine would.
// (iceberg-go's UpdateSchema leaves out the initial defaults of the
// columns it adds.)
func setSchema(t *testing.T, cat *catalog.Client, fields ...iceberg.NestedField) {
	t.Helper()
	ident := table.Identifier{"s", "t"}
	tbl, err := cat.LoadTable(t.Context(), ident)
	if err != nil {
		t.Fatal(err)
	}
	current := tbl.Metadata().CurrentSchema().ID
	if _, _, err := cat.CommitTable(t.Context(), ident, []table.Requirement{table.AssertCurrentSchemaID(current)},
		[]table.Update{table.NewAddSchemaUpdate(iceberg.NewSchema(current+1, fields...)), table.NewSetCurrentSchemaUpdate(-1)}); err != nil {
		t.Fatal(err)
	}
}

// TestInitialDefaultsAreUnsupported checks that workers do not read a
// table of format version 3 with a column, or a field nested in one, that
// has an initial default: they would read it as NULL from the files
// written before it was added, where it is its default. The statement
// runs on one machine instead.
func TestInitialDefaultsAreUnsupported(t *testing.T) {
	id := iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}
	f := iceberg.NestedField{ID: 3, Name: "f", Type: iceberg.PrimitiveTypes.Int32}
	n := iceberg.NestedField{ID: 4, Name: "n", Type: iceberg.PrimitiveTypes.Int32, InitialDefault: 7}
	s := func(fields ...iceberg.NestedField) iceberg.NestedField {
		return iceberg.NestedField{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: fields}}
	}
	for name, c := range map[string]struct {
		fields []iceberg.NestedField
		column string
	}{
		"column":       {[]iceberg.NestedField{id, s(f), n}, "n"},
		"nested field": {[]iceberg.NestedField{id, s(f, n)}, "s.n"},
	} {
		t.Run(name, func(t *testing.T) {
			cat := restTable(t, "3", id, s(f))
			if tab, err := NewIceberg(cat).Table(t.Context(), "s", "t"); err != nil || tab.Unsupported != "" {
				t.Fatalf("without initial defaults: %v, %v", tab, err)
			}
			setSchema(t, cat, c.fields...)
			tab, err := NewIceberg(cat).Table(t.Context(), "s", "t")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(tab.Unsupported, "column "+c.column+" has an initial default") {
				t.Errorf("unsupported %q, want it to name %s's initial default", tab.Unsupported, c.column)
			}
		})
	}
}
