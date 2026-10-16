package pgwire

import (
	"slices"
	"strings"
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

func TestParseAlterRelation(t *testing.T) {
	for q, want := range map[string]alterRelation{
		"ALTER VIEW IF EXISTS s.v RENAME TO w": {view: true, ifExists: true, name: []string{"s", "v"}, renameTo: "w"},
		// dbt renames views with ALTER TABLE, as PostgreSQL allows.
		`alter table "lake"."analytics"."stg__dbt_tmp" rename to "stg"`: {name: []string{"lake", "analytics", "stg__dbt_tmp"}, renameTo: "stg"},
		"ALTER VIEW v AS SELECT 1":                                      {view: true, name: []string{"v"}},
		"ALTER TABLE t ADD COLUMN c INTEGER":                            {name: []string{"t"}},
		"ALTER TABLE t RENAME TO other.u":                               {name: []string{"t"}},
		"ALTER TABLE t RENAME TO u; SELECT 1":                           {name: []string{"t"}},
	} {
		got, ok := parseAlterRelation(q)
		if !ok || got.view != want.view || got.ifExists != want.ifExists || !slices.Equal(got.name, want.name) || got.renameTo != want.renameTo {
			t.Errorf("parseAlterRelation(%q) = %+v, %v; want %+v", q, got, ok, want)
		}
	}
	if _, ok := parseAlterRelation("ALTER SCHEMA s RENAME TO t"); ok {
		t.Error("ALTER SCHEMA parsed")
	}
}

func TestParseCommentDollarQuoted(t *testing.T) {
	// dbt's persist_docs quotes comments so.
	c, ok := parseCommentCommand(`comment on column "lake"."analytics"."m".id is $dbt_comment_literal_block$It's the id$dbt_comment_literal_block$;`)
	if !ok || c.kind != "COLUMN" || c.text == nil || *c.text != "It's the id" ||
		!slices.Equal(c.name, []string{"lake", "analytics", "m", "id"}) {
		t.Errorf("got %+v %v", c, ok)
	}
	for _, q := range []string{"COMMENT ON TABLE t IS $a$x$b$", "COMMENT ON TABLE t IS $1", "COMMENT ON TABLE t IS $$x"} {
		if _, ok := parseCommentCommand(q); ok {
			t.Errorf("parsed %q", q)
		}
	}
	if c, ok := parseCommentCommand("COMMENT ON VIEW v IS $$$$"); !ok || c.text == nil || *c.text != "" {
		t.Errorf("empty dollar-quoted comment: %+v %v", c, ok)
	}
}

func TestParseDropViewCascade(t *testing.T) {
	c, ok := parseViewCommand(`drop view if exists "lake"."analytics"."stg__dbt_backup" cascade`)
	if !ok || !c.drop || !c.ifExists || !slices.Equal(c.name, []string{"lake", "analytics", "stg__dbt_backup"}) {
		t.Errorf("got %+v %v", c, ok)
	}
}

func TestParseLocalTarget(t *testing.T) {
	type want struct {
		kind targetKind
		name string
	}
	for query, w := range map[string]want{
		`/* dbt */ create temporary table "t__dbt_tmp" as (select * from "memory"."s"."v")`: {writesNone, ""},
		`CREATE OR REPLACE TABLE "memory".s.v AS SELECT 1`:                                  {writesNew, "memory.s.v"},
		`CREATE OR REPLACE VIEW revenue AS SELECT 1`:                                        {writesNew, "revenue"},
		`CREATE TEMP VIEW v AS SELECT 1`:                                                    {writesNone, ""},
		`create schema if not exists "lake"."analytics"`:                                    {writesSchema, "lake.analytics"},
		`CREATE UNIQUE INDEX i ON s.t (x)`:                                                  {writesRelation, "s.t"},
		`INSERT INTO lake.s.t SELECT * FROM "memory".s.v`:                                   {writesRelation, "lake.s.t"},
		`insert or replace into s.t values (1)`:                                             {writesRelation, "s.t"},
		`WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x`:                                {writesRelation, "t"},
		`UPDATE lake.s.t SET x = 1`:                                                         {writesRelation, "lake.s.t"},
		`DELETE FROM t WHERE x IN (SELECT x FROM "memory".s.v)`:                             {writesRelation, "t"},
		`MERGE INTO lake.s.t USING "memory".s.v AS v ON true WHEN MATCHED THEN DELETE`:      {writesRelation, "lake.s.t"},
		`TRUNCATE revenue`:            {writesRelation, "revenue"},
		`DROP VIEW IF EXISTS revenue`: {writesRelation, "revenue"},
		`drop table if exists "lake"."analytics"."x__dbt_backup" cascade`: {writesRelation, "lake.analytics.x__dbt_backup"},
		`DROP SCHEMA igloo`:                      {writesSchema, "igloo"},
		`DROP MACRO TABLE IF EXISTS m`:           {writesRoutine, "m"},
		`ALTER TABLE lake.s.t RENAME TO u`:       {writesRelation, "lake.s.t"},
		`COMMENT ON COLUMN s.t.c IS 'x'`:         {writesRelation, "s.t"},
		`COPY t (a, b) FROM 's3://bucket/x.csv'`: {writesRelation, "t"},
		`COPY (SELECT 1) TO 's3://bucket/x.csv'`: {writesNone, ""},
		`COPY lake.s.t TO 's3://bucket/x.csv'`:   {writesNone, ""},
		`COPY FROM DATABASE lake TO memory`:      {writesSchema, "memory.main"},
	} {
		got, ok := parseLocalTarget(query)
		if !ok || got.kind != w.kind || strings.Join(got.name, ".") != w.name {
			t.Errorf("parseLocalTarget(%q) = %+v, %v; want %+v", query, got, ok, w)
		}
	}
	for _, query := range []string{"VACUUM", "CHECKPOINT", "CREATE SECRET s (TYPE s3)", "WITH x AS (SELECT 1) SELECT * FROM x"} {
		if got, ok := parseLocalTarget(query); ok {
			t.Errorf("parseLocalTarget(%q) = %+v, want not ok", query, got)
		}
	}
}
