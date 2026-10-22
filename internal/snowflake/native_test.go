package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// nativeRows translates and runs a Snowflake statement and returns its
// rows, values separated by "|" and JSON as JSON text.
func nativeRows(t *testing.T, conn *sql.Conn, snowflake string) string {
	t.Helper()
	res, err := Translate(snowflake, Options{})
	if err != nil {
		t.Fatalf("%s: %v", snowflake, err)
	}
	rows, err := conn.QueryContext(context.Background(), res.SQL)
	if err != nil {
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, snowflake, res.SQL)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		vals := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			parts[i] = format(v, types[i].DatabaseTypeName())
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, snowflake, res.SQL)
	}
	return strings.Join(out, "\n")
}

// TestNativeColumnsReadAsJSON checks that Igloo's ARRAY and OBJECT columns,
// lists and maps of strings, read in Snowflake's dialect exactly as the
// same values held as JSON do (VARIANT columns, and views that parse text):
// FLATTEN in each form and MODE, paths, element access, the functions that
// take or build semi-structured values, and FLATTEN of objects built from
// them, as reports flatten them. Those that read a value read it alike from
// JSON text too. The values are strings, objects and arrays, which the
// lists and maps hold without loss (objects and arrays as their JSON text);
// see TestNativeColumnsHoldText for the others.
func TestNativeColumnsReadAsJSON(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	docs := []struct{ array, object string }{
		{`["a","b c",{"x":1,"y":[2,"z"]},[3,"w"],null,""]`, `{"k":"v","o":{"y":2},"l":[1,"m"],"n":null,"":"e"}`},
		{`[]`, `{}`},
		{`["only"]`, `{"one":"1x"}`},
		{`[{"name":"n1"},{"name":"n2","tags":["t"]}]`, `{"nested":{"a":{"b":"c"}}}`},
	}
	var values []string
	for i, d := range docs {
		values = append(values, fmt.Sprintf("(%d, '%s', '%s')", i+1, d.array, d.object))
	}
	values = append(values, "(9, NULL, NULL)")
	// t_text holds them as JSON text, as tables loaded from text do; t_json
	// as JSON, as VARIANT columns and views that parse text do; t_native as
	// Igloo stores Snowflake's ARRAY and OBJECT.
	for _, stmt := range []string{
		"CREATE TABLE t_text AS SELECT * FROM (VALUES " + strings.Join(values, ", ") + ") v(id, a, o)",
		"CREATE TABLE t_json AS SELECT id, CAST(a AS JSON) AS a, CAST(o AS JSON) AS o FROM t_text",
		"CREATE TABLE t_native AS SELECT id, CAST(a AS VARCHAR[]) AS a, CAST(o AS MAP(VARCHAR, VARCHAR)) AS o FROM t_json",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, c := range []struct {
		q string
		// builds says the statement builds values from the columns, which
		// takes text as a string, so JSON text reads otherwise.
		builds bool
	}{
		{q: `select t.id, f.seq, f.key, f.path, f.index, f.value, f.this from %s t, lateral flatten(input => t.a) f order by 1, 5`},
		{q: `select t.id, f.key, f.path, f.value, f.this from %s t, lateral flatten(input => t.o) f order by 1, 2`},
		{q: `select t.id, f.index, f.value from %s t, lateral flatten(t.a, mode => 'ARRAY') f order by 1, 2`},
		{q: `select t.id, count(*) from %s t, lateral flatten(t.a, mode => 'OBJECT') f group by 1 order by 1`},
		{q: `select t.id, count(*) from %s t, lateral flatten(t.o, mode => 'ARRAY') f group by 1 order by 1`},
		{q: `select f.* from %s t, lateral flatten(input => t.a) f order by t.id, f.index`},
		{q: `select t.id, f.index, f.value from %s t, lateral flatten(input => t.a, outer => true) f order by 1, 2`},
		{q: `select t.id, f.key, f.path, f.value from %s t, lateral flatten(input => t.a, recursive => true) f order by 1, 3`},
		{q: `select t.id, f.index, f.value from %s t, lateral flatten(input => t.a, path => '[2].y') f order by 1, 2`},
		{q: `select t.id, f.value:name::string, f.value:tags[0] from %s t, lateral flatten(input => t.a) f order by 1, f.index`},
		{q: `select id, a[0], a[1]::string, a[2]:x, a[2]:y[1]::string, a[3][0], get(a, 4), o:k, o:o.y, o['l'][1], get_path(o, 'nested.a.b')::string from %s order by id`},
		{q: `select id, array_size(a), array_contains('b c'::variant, a), typeof(a), typeof(o), is_array(a), is_object(o), object_keys(o) from %s order by id`},
		{q: `select id, array_to_string(a, ','), object_insert(o, 'added', 'x'), array_position('only'::variant, a) from %s order by id`},
		{q: `select id, to_json(a), a::string, to_varchar(o), a::variant, to_array(a) from %s order by id`, builds: true},
		{q: `select id, object_construct('k', a, 'o', o), array_construct(a, o), [a, 'x'] from %s order by id`, builds: true},
		{q: `select array_agg(a) within group (order by id), array_agg(o) within group (order by id), object_agg(id::string, a) from %s where id < 3`, builds: true},
		{q: `select t.id, cast(g.value:facet_type as text) as bt, cast(v.value as text) as bv from %s t,
			lateral flatten(input => [object_construct('facet_type', 'all', 'facet_values', ['every']),
				object_construct_keep_null('facet_type', 'array', 'facet_values', t.a)]) g,
			lateral flatten(input => get_path(g.value, 'facet_values')) v order by 1, 2, 3`, builds: true},
		{q: `select t.id, f.key, f.value::string, count(*) over (partition by f.key) from %s t, lateral flatten(input => t.o) f order by 1, 2`},
		{q: `select t.id, f.index, g.key, g.index, g.value, g.this from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value) g order by 1, 2, 3, 4`},
		{q: `select t.id, f.index, g.value from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value:y) g order by 1, 2, 3`},
		{q: `select id, (select count(*) from table(flatten(input => s.a)) f) from %s s order by id`},
	} {
		want := nativeRows(t, conn, fmt.Sprintf(c.q, "t_json"))
		got := nativeRows(t, conn, fmt.Sprintf(c.q, "t_native"))
		if got != want {
			t.Errorf("%s\n got\n%s\nwant (as JSON)\n%s", fmt.Sprintf(c.q, "t_native"), got, want)
		}
		if !c.builds {
			if text := nativeRows(t, conn, fmt.Sprintf(c.q, "t_text")); text != want {
				t.Errorf("%s\n got\n%s\nwant (as JSON)\n%s", fmt.Sprintf(c.q, "t_text"), text, want)
			}
		}
	}
}

// TestNativeColumnsHoldText checks what an ARRAY or OBJECT column loses: an
// element or value that is a number or a boolean is held as its text, as
// Snowflake's ARRAY(VARCHAR) holds it, and reads back as a string, which
// compares and casts as the number would.
func TestNativeColumnsHoldText(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE t AS SELECT CAST(CAST('[1, 2.5, true]' AS JSON) AS VARCHAR[]) AS a,
		CAST(CAST('{"n": 7}' AS JSON) AS MAP(VARCHAR, VARCHAR)) AS o`); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ q, want string }{
		{`select a, o from t`, `["1","2.5","true"]|{"n":"7"}`},
		{`select a[0] + 1, a[1]::number(10,1), a[0] = 1, o:n::int * 2, typeof(a[0]) from t`, `2|2.5|true|14|VARCHAR`},
		{`select sum(f.value) from t, lateral flatten(input => t.a, mode => 'ARRAY') f where f.index < 2`, `3.5`},
	} {
		if got := nativeRows(t, conn, c.q); got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.q, got, c.want)
		}
	}
}

// TestFlattenShapes checks the forms FLATTEN takes (see flattenUnnest): a
// column is unnested where it is read, an array constant from a list of its
// elements, and anything else computed once in a subquery.
func TestFlattenShapes(t *testing.T) {
	for _, c := range []struct{ sql, has, hasNot string }{
		{`select f.value from t, lateral flatten(input => t.a) f`,
			`FROM (SELECT unnest(sf_flatten_values(t.a, 'BOTH')) AS _sf_value`, `_sf_flatten_input`},
		{`select f.value from t, lateral flatten(input => [object_construct('k', t.a), 'x']) f`,
			`unnest([coalesce(to_json(to_json(map_from_entries(`, `sf_flatten_values`},
		{`select f.value from t, lateral flatten(input => array_construct(1, t.b)) f`,
			`unnest([coalesce(to_json(1), CAST('null' AS JSON)), coalesce(to_json(sf_to_json(t.b)), CAST('null' AS JSON))]) AS _sf_value`, `sf_flatten_values`},
		{`select f.value from t, lateral flatten(input => parse_json(t.s)) f`,
			`unnest(sf_flatten_values(_sf_this, 'BOTH')) AS _sf_value`, `sf_flatten_values(json(`},
		{`select f.value from t, lateral flatten(input => [1, 2], mode => 'OBJECT') f`,
			`FROM (SELECT json_array(1, 2) AS _sf_this) AS _sf_flatten_input`, `unnest([`},
	} {
		res, err := Translate(c.sql, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.SQL, c.has) || strings.Contains(res.SQL, c.hasNot) {
			t.Errorf("%s:\n%s\nwant %q and not %q", c.sql, res.SQL, c.has, c.hasNot)
		}
	}
}

// TestSemiStructuredColumnTypes checks the DuckDB types of Snowflake's
// semi-structured and structured types: of columns, and of casts.
func TestSemiStructuredColumnTypes(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`CREATE TABLE t (a ARRAY, o OBJECT, v VARIANT, s ARRAY(VARCHAR), n ARRAY(NUMBER(10,2)), st OBJECT(city VARCHAR, "Zip" NUMBER NOT NULL), m MAP(VARCHAR, NUMBER), aa ARRAY(ARRAY(INT)))`,
			`CREATE TABLE t (a VARCHAR[], o MAP(VARCHAR, VARCHAR), v JSON, s VARCHAR[], n DECIMAL(10,2)[], st STRUCT("city" VARCHAR, "Zip" DECIMAL(38,0)), m MAP(VARCHAR, DECIMAL(38,0)), aa BIGINT[][])`},
		{`ALTER TABLE t ADD COLUMN a ARRAY`, `ALTER TABLE t ADD COLUMN a VARCHAR[]`},
		{`ALTER TABLE t ADD COLUMN o OBJECT(k VARCHAR)`, `ALTER TABLE t ADD COLUMN o STRUCT("k" VARCHAR)`},
		{`SELECT x::ARRAY(VARCHAR) AS a, v:items::ARRAY(NUMBER) AS b, CAST(x AS MAP(VARCHAR, INT)) AS c FROM t`,
			`SELECT CAST(sf_json(x) AS VARCHAR[]) AS "A", CAST(sf_get(v, '$.items') AS DECIMAL(38,0)[]) AS "B", CAST(sf_json(x) AS MAP(VARCHAR, BIGINT)) AS "C" FROM t`},
		{`SELECT x::ARRAY AS a, CAST(x AS OBJECT) AS o, PARSE_JSON(s)::VARIANT AS v FROM t`,
			`SELECT sf_to_json(x) AS "A", sf_to_json(x) AS "O", to_json(json(s)) AS "V" FROM t`},
		{`CREATE TABLE c AS SELECT ARRAY_AGG(x) AS a, OBJECT_CONSTRUCT('k', x) o, PARSE_JSON(s) AS v, [x] AS l, x::ARRAY AS xa, 1 AS n FROM t GROUP BY s`,
			`CREATE TABLE c AS SELECT CAST(coalesce(to_json(list(sf_to_json(x)) FILTER (WHERE sf_to_json(x) IS NOT NULL)), CAST('[]' AS JSON)) AS VARCHAR[]) AS a, ` +
				`CAST(to_json(map_from_entries(list_sort(list_filter([{'k': CAST('k' AS VARCHAR), 'v': to_json(sf_to_json(x))}], lambda e: e.v IS NOT NULL)))) AS MAP(VARCHAR, VARCHAR)) o, ` +
				`json(s) AS v, CAST(json_array(sf_to_json(x)) AS VARCHAR[]) AS l, CAST(sf_to_json(x) AS VARCHAR[]) AS xa, 1 AS n FROM t GROUP BY s`},
		{`CREATE TABLE c AS SELECT ARRAY_CONSTRUCT(1) AS a UNION ALL SELECT ARRAY_CONSTRUCT(2)`,
			`CREATE TABLE c AS SELECT json_array(1) AS a UNION ALL SELECT json_array(2)`},
	} {
		if got := translate(t, c.in).SQL; got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.in, got, c.want)
		}
	}
	for _, bad := range []string{`CREATE TABLE t (a ARRAY(VARCHAR, INT))`, `CREATE TABLE t (m MAP(VARCHAR))`, `CREATE TABLE t (o OBJECT(k))`} {
		if _, err := Translate(bad, Options{}); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}

// TestSemiStructuredColumnsHoldValues creates a table with Snowflake's
// types, inserts semi-structured values, and reads them back.
func TestSemiStructuredColumnsHoldValues(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	for _, q := range []string{
		`create table tt (a array, o object, v variant, s array(varchar), st object(city varchar, zip number), m map(varchar, number))`,
		`insert into tt select array_construct('x', 1, object_construct('k', 'v')), object_construct('a', 1, 'b', 'c'),
			parse_json('{"z":[1]}'), array_construct('p', 'q'), object_construct('city', 'Paris', 'zip', 75001), object_construct('a', 1)`,
	} {
		res, err := Translate(q, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, res.SQL); err != nil {
			t.Fatalf("%v\n%s", err, res.SQL)
		}
	}
	for _, c := range []struct{ q, want string }{
		{`select typeof(a), typeof(o), typeof(v) from tt`, `ARRAY|OBJECT|OBJECT`},
		{`select a[0]::string, a[1]::int + 1, a[2]:k::string, o:a::int, o:b::string, v:z[0], s[1]::string, st:city::string, st:zip::int, m:a::int from tt`,
			`x|2|v|1|c|1|q|Paris|75001|1`},
		{`select f.index, f.value from tt, lateral flatten(a) f order by 1`, "0|\"x\"\n1|\"1\"\n2|{\"k\":\"v\"}"},
		{`select f.key, f.value::string from tt, lateral flatten(st) f order by 1`, "city|Paris\nzip|75001"},
		{`select to_json(s), to_json(st), to_json(m) from tt`, `["p","q"]|{"city":"Paris","zip":75001}|{"a":1}`},
	} {
		if got := nativeRows(t, conn, c.q); got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.q, got, c.want)
		}
	}
}
