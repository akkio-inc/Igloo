package pgwire

import (
	"slices"
	"testing"
)

func TestParseViewCommand(t *testing.T) {
	c, ok := parseViewCommand(`CREATE OR REPLACE VIEW lake.public."Top" (a, "B") AS SELECT 1, 2;`)
	if !ok || !c.replace || c.drop || !slices.Equal(c.name, []string{"lake", "public", "Top"}) ||
		!slices.Equal(c.columns, []string{"a", "B"}) || c.query != "SELECT 1, 2" {
		t.Errorf("create or replace: %+v %v", c, ok)
	}
	c, ok = parseViewCommand("create view if not exists v as with x as (select 1) select * from x")
	if !ok || c.replace || !c.ifNotExists || c.query != "with x as (select 1) select * from x" {
		t.Errorf("create: %+v %v", c, ok)
	}
	c, ok = parseViewCommand("DROP VIEW IF EXISTS s.v;")
	if !ok || !c.drop || !c.ifExists || !slices.Equal(c.name, []string{"s", "v"}) {
		t.Errorf("drop: %+v %v", c, ok)
	}
	for _, q := range []string{
		"CREATE TEMP VIEW v AS SELECT 1",
		"CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1",
		"CREATE MATERIALIZED VIEW v AS SELECT 1",
		"DROP VIEW a, b",
		"CREATE VIEW v",
		"CREATE TABLE v AS SELECT 1",
	} {
		if c, ok := parseViewCommand(q); ok {
			t.Errorf("%q parsed as %+v", q, c)
		}
	}
}

func TestParseCommentCommand(t *testing.T) {
	c, ok := parseCommentCommand("COMMENT ON TABLE lake.public.events IS 'Raw events | :pii:false';")
	if !ok || c.kind != "TABLE" || c.text == nil || *c.text != "Raw events | :pii:false" ||
		!slices.Equal(c.name, []string{"lake", "public", "events"}) {
		t.Errorf("table: %+v %v", c, ok)
	}
	c, ok = parseCommentCommand(`comment on column events."User Id" is null`)
	if !ok || c.kind != "COLUMN" || c.text != nil || !slices.Equal(c.name, []string{"events", "User Id"}) {
		t.Errorf("column: %+v %v", c, ok)
	}
	c, ok = parseCommentCommand("COMMENT ON SCHEMA lake.marts IS 'Reporting tables'")
	if !ok || c.kind != "SCHEMA" || c.text == nil || *c.text != "Reporting tables" || !slices.Equal(c.name, []string{"lake", "marts"}) {
		t.Errorf("schema: %+v %v", c, ok)
	}
	for _, q := range []string{
		"COMMENT ON COLUMN events IS 'x'",
		"COMMENT ON SCHEMA lake.marts.x IS 'x'",
		"COMMENT ON TABLE t IS 'x'; DROP TABLE t",
		"COMMENT ON TABLE t IS E'x'",
	} {
		if c, ok := parseCommentCommand(q); ok {
			t.Errorf("%q parsed as %+v", q, c)
		}
	}
}

func TestNamesDatabase(t *testing.T) {
	for q, want := range map[string]bool{
		"DROP VIEW memory.public.v":                     true,
		`CREATE TABLE "Memory"."public".t AS SELECT 1`:  true,
		"ALTER VIEW MEMORY . main.v RENAME TO w":        true,
		"CREATE TABLE t AS SELECT * FROM lake.public.v": false,
		"INSERT INTO t SELECT 'memory.public.v'":        false,
		"DELETE FROM memorytable.x":                     false,
	} {
		if got := namesDatabase(q, "memory"); got != want {
			t.Errorf("namesDatabase(%q) = %v, want %v", q, got, want)
		}
	}
}

func TestParseAlterView(t *testing.T) {
	if name, ok := parseAlterView("ALTER VIEW IF EXISTS s.v RENAME TO w"); !ok || !slices.Equal(name, []string{"s", "v"}) {
		t.Errorf("got %q %v", name, ok)
	}
	if _, ok := parseAlterView("ALTER TABLE t RENAME TO u"); ok {
		t.Error("ALTER TABLE parsed")
	}
}
