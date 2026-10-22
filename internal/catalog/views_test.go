package catalog

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go/view"
)

// A view's metadata as the catalog holds it: one version, reading a.
const viewMetadata = `{
	"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
	"format-version": 1,
	"location": "s3://bucket/warehouse/v",
	"current-version-id": 1,
	"properties": {},
	"schemas": [{"schema-id": 0, "type": "struct", "fields": [
		{"id": 1, "name": "id", "required": false, "type": "int"},
		{"id": 2, "name": "name", "required": false, "type": "string"}]}],
	"versions": [{"version-id": 1, "schema-id": 0, "timestamp-ms": 1, "summary": {"engine-name": "igloo"},
		"representations": [{"type": "sql", "sql": "SELECT * FROM a", "dialect": "duckdb"}],
		"default-namespace": ["public"]}],
	"version-log": [{"timestamp-ms": 1, "version-id": 1}]
}`

// TestReplaceView applies the changes replaceView makes the way the
// catalog does, which reuses a schema or version equal to an earlier one:
// a view that goes back to earlier columns must still get a version.
func TestReplaceView(t *testing.T) {
	meta, err := view.ParseMetadataString(viewMetadata)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(meta view.Metadata, v View) view.Metadata {
		t.Helper()
		requirements, updates, err := replaceView(meta, v)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range requirements {
			if err := r.Validate(meta); err != nil {
				t.Fatal(err)
			}
		}
		b, err := view.MetadataBuilderFromBase(meta)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range updates {
			if err := u.Apply(b); err != nil {
				t.Fatalf("%s: %v", u.Action(), err)
			}
		}
		res, err := b.Build()
		if err != nil {
			t.Fatalf("replace with %q: %v", v.SQL, err)
		}
		return res.Metadata
	}
	current := func(meta view.Metadata) (string, int, int64) {
		v := meta.CurrentVersion()
		return v.Representations[0].Sql, v.SchemaID, v.VersionID
	}

	a := View{Schema: "public", Name: "v", SQL: "SELECT * FROM a", Columns: []Column{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "VARCHAR"}}}
	b := View{Schema: "public", Name: "v", SQL: "SELECT * FROM b", Columns: []Column{{Name: "x", Type: "DOUBLE"}, {Name: "d", Type: "DATE"}}}

	meta = apply(meta, b)
	if sql, schema, version := current(meta); sql != b.SQL || schema != 1 || version != 2 {
		t.Errorf("replaced with b: %q, schema %d, version %d", sql, schema, version)
	}
	// Back to a's columns: the catalog reuses schema 0.
	a.SQL = "SELECT id, name FROM a"
	meta = apply(meta, a)
	if sql, schema, version := current(meta); sql != a.SQL || schema != 0 || version != 3 {
		t.Errorf("replaced with a's columns: %q, schema %d, version %d", sql, schema, version)
	}
	// Back to b exactly, then to the same again: each is a new version,
	// as Lakekeeper cannot go back to an earlier one.
	meta = apply(meta, b)
	if sql, schema, version := current(meta); sql != b.SQL || schema != 1 || version != 4 {
		t.Errorf("replaced with b again: %q, schema %d, version %d", sql, schema, version)
	}
	meta = apply(meta, b)
	if sql, schema, version := current(meta); sql != b.SQL || schema != 1 || version != 5 {
		t.Errorf("replaced with the same: %q, schema %d, version %d", sql, schema, version)
	}
	if n := len(meta.VersionLog()); n != 5 {
		t.Errorf("version log has %d entries, want 5", n)
	}
}

// TestNestedViewColumns checks that a view's lists, maps and structs are
// Iceberg's in its schema, with field IDs after the columns', and read
// back as DuckDB's types.
func TestNestedViewColumns(t *testing.T) {
	cols := []Column{{Name: "id", Type: "INTEGER"}, {Name: "tags", Type: "VARCHAR[]"},
		{Name: "attrs", Type: "MAP(VARCHAR, VARCHAR)"},
		{Name: "loc", Type: `STRUCT(city VARCHAR, "Zip Code" DECIMAL(38,0), pts STRUCT(x DOUBLE)[])`},
		{Name: "j", Type: "JSON"}}
	s := icebergSchema(0, cols)
	want := "table {\n\t1: id: optional int\n\t2: tags: optional list<string>\n\t3: attrs: optional map<string, string>\n" +
		"\t4: loc: optional struct<9: city: optional string, 10: Zip Code: optional decimal(38, 0), 11: pts: optional list<struct<13: x: optional double>>>\n" +
		"\t5: j: optional string\n}"
	if got := s.String(); got != want {
		t.Errorf("schema:\n%s\nwant\n%s", got, want)
	}
	var back []string
	for _, f := range s.Fields() {
		back = append(back, duckDBType(f.Type))
	}
	if got := strings.Join(back, ", "); got != `INTEGER, VARCHAR[], MAP(VARCHAR, VARCHAR), STRUCT("city" VARCHAR, "Zip Code" DECIMAL(38,0), "pts" STRUCT("x" DOUBLE)[]), VARCHAR` {
		t.Errorf("DuckDB types: %s", got)
	}
}
