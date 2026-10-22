package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
)

func TestIcebergCast(t *testing.T) {
	for _, c := range []struct{ typ, want string }{
		{"JSON", `CAST("c" AS VARCHAR)`},
		{"VARIANT", `CAST(CAST("c" AS JSON) AS VARCHAR)`},
		{"JSON[]", `CAST("c" AS VARCHAR[])`},
		{"MAP(VARCHAR, JSON)", `CAST("c" AS MAP(VARCHAR, VARCHAR))`},
		{`STRUCT("json" JSON, b INTEGER[], "c d" VARIANT)[]`, `CAST(CAST("c" AS JSON) AS STRUCT("json" VARCHAR, b INTEGER[], "c d" VARCHAR)[])`},
		{"STRUCT(json JSON)", `CAST("c" AS STRUCT(json VARCHAR))`},
		{"VARCHAR", ""},
		{"VARCHAR[]", ""},
		{"DECIMAL(10,2)", ""},
		{"MAP(VARCHAR, VARCHAR)", ""},
		{"STRUCT(a TIMESTAMP WITH TIME ZONE, b DOUBLE)", ""},
		{`ENUM('JSON', 'b')`, ""},
	} {
		if got := icebergCast(`"c"`, c.typ); got != c.want {
			t.Errorf("%s: got %s, want %s", c.typ, got, c.want)
		}
	}
}

func TestIcebergColumnList(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`CREATE TABLE t (json JSON, v variant NOT NULL, l JSON[], m MAP(VARCHAR, JSON) DEFAULT NULL, s STRUCT(json JSON, n INT), d DECIMAL(10, 2), PRIMARY KEY (json))`,
			`CREATE TABLE t (json VARCHAR, v VARCHAR NOT NULL, l VARCHAR[], m MAP(VARCHAR, VARCHAR) DEFAULT NULL, s STRUCT(json VARCHAR, n INT), d DECIMAL(10, 2), PRIMARY KEY (json))`},
		{`CREATE OR REPLACE TABLE IF NOT EXISTS lake."S".t /* JSON */ ("JSON" json, b TIMESTAMP WITH TIME ZONE, c json) `,
			`CREATE OR REPLACE TABLE IF NOT EXISTS lake."S".t /* JSON */ ("JSON" VARCHAR, b TIMESTAMP WITH TIME ZONE, c VARCHAR) `},
		{`CREATE TABLE t (a INT, b VARCHAR DEFAULT 'JSON')`, `CREATE TABLE t (a INT, b VARCHAR DEFAULT 'JSON')`},
	} {
		if got := icebergColumnList(c.in); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
	for _, c := range []struct{ in, want string }{
		{`ALTER TABLE t ADD COLUMN j JSON`, `ALTER TABLE t ADD COLUMN j VARCHAR`},
		{`ALTER TABLE t ADD IF NOT EXISTS j MAP(VARCHAR, JSON)`, `ALTER TABLE t ADD IF NOT EXISTS j MAP(VARCHAR, VARCHAR)`},
		{`ALTER TABLE t ADD j VARCHAR[]`, `ALTER TABLE t ADD j VARCHAR[]`},
	} {
		if got := icebergAddColumn(c.in); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}

// TestSemiStructuredIcebergColumns creates Iceberg tables with JSON,
// VARIANT, lists, maps and structs, in DuckDB's dialect and Snowflake's,
// with column lists, CREATE TABLE ... AS and ALTER TABLE, and checks the
// Iceberg types they get and the values they hold.
func TestSemiStructuredIcebergColumns(t *testing.T) {
	env := newCatalogEnv(t)
	ctx := context.Background()
	conn := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	query := func(conn *pgx.Conn, sql string) string {
		t.Helper()
		return strings.Join(rawTexts(t, conn, sql), "\n")
	}
	types := func(name string) string {
		t.Helper()
		cat, err := catalog.Open(ctx, env.direct)
		if err != nil {
			t.Fatal(err)
		}
		tbl, err := cat.LoadTable(ctx, catalog.Ident(env.direct.Catalog.Schema, name))
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, f := range tbl.Schema().Fields() {
			out = append(out, f.Name+" "+f.Type.String())
		}
		return strings.Join(out, ", ")
	}
	check := func(got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("got\n  %s\nwant\n  %s", got, want)
		}
	}

	// DuckDB's dialect: JSON and VARIANT hold their JSON text.
	mustExec(t, conn, `CREATE TABLE duck_types (id INT, j JSON, v VARIANT, l VARCHAR[], jl JSON[], m MAP(VARCHAR, JSON), s STRUCT(a JSON, b INT))`)
	check(types("duck_types"), "id int, j string, v string, l list<string>, jl list<string>, m map<string, string>, s struct<12: a: optional string, 13: b: optional int>")
	mustExec(t, conn, `INSERT INTO duck_types VALUES (1, '{"a":[1,"x"]}', '{"b":2}', ['x', NULL], ['"s"', '{"k":1}']::JSON[],
		MAP {'k': '{"y":2}'::JSON, 's': '"t"'::JSON}, {'a': '"str"'::JSON, 'b': 3})`)
	check(query(conn, `SELECT j, v, l, jl, m, s FROM duck_types`),
		`{"a":[1,"x"]}|{"b":2}|["x",null]|["\"s\"","{\"k\":1}"]|{"k":"{\"y\":2}","s":"\"t\""}|{"a":"\"str\"","b":3}`)
	mustExec(t, conn, `ALTER TABLE duck_types ADD COLUMN extra JSON`)
	check(types("duck_types"), "id int, j string, v string, l list<string>, jl list<string>, m map<string, string>, s struct<12: a: optional string, 13: b: optional int>, extra string")

	// CREATE TABLE ... AS of JSON values, and CREATE OR REPLACE.
	mustExec(t, conn, `CREATE TABLE duck_ctas AS SELECT 1 AS id, '{"a":1}'::JSON AS j, ['"s"', '2']::JSON[] AS jl -- comment`)
	check(types("duck_ctas"), "id int, j string, jl list<string>")
	check(query(conn, `SELECT j, jl FROM duck_ctas`), `{"a":1}|["\"s\"","2"]`)
	mustExec(t, conn, `CREATE OR REPLACE TABLE duck_ctas AS SELECT '[1]'::JSON AS j`)
	check(types("duck_ctas"), "j string")

	// Snowflake's dialect: ARRAY and OBJECT are lists and maps of strings,
	// VARIANT JSON text, and all read back as Snowflake's values.
	sf := env.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	mustExec(t, sf, `create table sf_types (a array, o object, v variant, s array(varchar), st object(city varchar, zip number), m map(varchar, number))`)
	check(types("sf_types"), "a list<string>, o map<string, string>, v string, s list<string>, "+
		"st struct<10: city: optional string, 11: zip: optional decimal(38, 0)>, m map<string, decimal(38, 0)>")
	mustExec(t, sf, `insert into sf_types select array_construct('x', 1, object_construct('k', 'v')), object_construct('a', 1, 'b', 'c'),
		parse_json('{"z":[1]}'), array_construct('p', 'q'), object_construct('city', 'Paris', 'zip', 75001), object_construct('a', 1)`)
	check(query(sf, `select a[0]::string, a[2]:k::string, o:b::string, v:z[0]::int, st:city::string, m:a::int from sf_types`), "x|v|c|1|Paris|1")
	check(query(sf, `select f.index, f.value::string from sf_types, lateral flatten(input => a) f order by 1`), "0|x\n1|1\n2|{\"k\":\"v\"}")

	// What schema browsers and clients see of them.
	check(query(conn, `SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) FROM information_schema.columns
		WHERE table_name = 'sf_types'`), "a VARCHAR[], o MAP(VARCHAR, VARCHAR), v VARCHAR, s VARCHAR[], "+
		`st STRUCT(city VARCHAR, zip DECIMAL(38,0)), m MAP(VARCHAR, DECIMAL(38,0))`)
	check(query(conn, `SELECT string_agg(attname || ' ' || format_type(atttypid, atttypmod), ', ' ORDER BY attnum) FROM pg_attribute
		WHERE attrelid = 'sf_types'::regclass AND attnum > 0`), "a json, o json, v text, s json, st json, m json")
	check(query(conn, `SELECT a, o, st FROM sf_types`), `["x","1","{\"k\":\"v\"}"]|{"a":"1","b":"c"}|{"city":"Paris","zip":75001}`)
	mustExec(t, sf, `create table sf_ctas as select array_construct('a', 'b') as a, parse_json('{"k":[1]}') as v, object_construct('x', 1) as o`)
	check(types("sf_ctas"), "a list<string>, v string, o map<string, string>")
	check(query(sf, `select a[1]::string, v:k[0]::int, o:x::int from sf_ctas`), "b|1|1")
}

// TestMaskingSemiStructuredColumns masks ARRAY and OBJECT columns, lists
// and maps of strings, with policies written in either dialect.
func TestMaskingSemiStructuredColumns(t *testing.T) {
	cs := startCatalogServer(t)
	for _, sql := range []string{
		"CREATE TABLE lake.public.ev (id INTEGER, tags VARCHAR[], attrs MAP(VARCHAR, VARCHAR))",
		"INSERT INTO lake.public.ev VALUES (1, ['a', 'b'], MAP {'k': 'v'})",
		"CREATE MASKING POLICY hide_tags AS (val VARCHAR[]) RETURNS VARCHAR[] -> ['***']",
		"ALTER TABLE lake.public.ev ALTER COLUMN tags SET MASKING POLICY hide_tags",
	} {
		mustExec(t, cs.admin, sql)
	}
	sf := cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	mustExec(t, sf, "create masking policy hide_attrs as (val object) returns object -> object_construct('masked', 'yes')")
	mustExec(t, sf, "alter table lake.public.ev alter column attrs set masking policy hide_attrs")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	if got := rawTexts(t, rita, "SELECT tags, attrs FROM ev"); !slices.Equal(got, []string{`["***"]|{"masked":"yes"}`}) {
		t.Errorf("masked: %q", got)
	}
	mustExec(t, rita, "SET sql_dialect = 'snowflake'")
	if got := rawTexts(t, rita, "select f.value::string, attrs:masked::string from ev, lateral flatten(input => tags) f"); !slices.Equal(got, []string{`***|yes`}) {
		t.Errorf("masked, in Snowflake's dialect: %q", got)
	}
}

// rawTexts runs sql and returns its rows as the server sent them, values
// separated by "|".
func rawTexts(t *testing.T, conn *pgx.Conn, sql string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var parts []string
		for _, v := range rows.RawValues() {
			if v == nil {
				parts = append(parts, "NULL")
			} else {
				parts = append(parts, string(v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}
