package catalog

import (
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

	a := View{Schema: "public", Name: "v", SQL: "SELECT * FROM a", Columns: []Column{{"id", "INTEGER"}, {"name", "VARCHAR"}}}
	b := View{Schema: "public", Name: "v", SQL: "SELECT * FROM b", Columns: []Column{{"x", "DOUBLE"}, {"d", "DATE"}}}

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
