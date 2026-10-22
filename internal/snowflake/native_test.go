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
// see TestNativeColumnsHoldText for the others. FLATTENs of objects that
// the translation keeps as structs (see flattenStructs) read as they do
// when it keeps them as JSON, which a bare read of their VALUE makes it do.
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
		// asJSON, when set, says q flattens objects as structs, with %[2]s
		// where asJSON makes it flatten them as JSON instead.
		asJSON string
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
		{q: `select t.id, cast(g.value:facet_type as text) as bt, cast(v.value as text) as bv from %[1]s t,
			lateral flatten(input => [object_construct('facet_type', 'all', 'facet_values', ['every']),
				object_construct_keep_null('facet_type', 'array', 'facet_values', t.a)]) g,
			lateral flatten(input => get_path(g.value, 'facet_values')) v %[2]s order by 1, 2, 3`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select cast(g.value:facet_type as text) as bt, cast(v.value as text) as bv, count(distinct t.id), count(*)
			from %[1]s as t inner join (select 1 as id union all select 2 union all select 4 union all select 9) as u on u.id = t.id
			cross join lateral flatten(input => [object_construct('facet_type', 'all', 'facet_values', ['every']),
				object_construct_keep_null('facet_type', 'array', 'facet_values', t.a),
				object_construct_keep_null('facet_type', 'object', 'facet_values', t.o)]) as g(seq, key, path, index, value, this)
			cross join lateral flatten(input => get_path(g.value, 'facet_values')) as v(seq, key, path, index, value, this)
			%[2]s group by 1, 2 order by 1, 2`, builds: true,
			asJSON: `where g.value is not null`},
		// Reads of each key, by each mode: a key of strings, keys of lists,
		// maps, NULLs left out or kept, numbers and objects, a key some
		// objects lack and one none has, and paths into a key's value.
		{q: `select t.id, g.index, g.value:k, g.value:k::string, get(g.value, 'k') is null, g.value:k = 'b', g.value:l,
				g.value:l::string, g.value:l[1], g.value:l[2]:x, g.value['l'][2]:y[1]::string, g.value:n, get_path(g.value, 'n') is null,
				g.value:n:z, g.value:missing, typeof(g.value:l), array_size(g.value:l), to_json(g.value:l)
			from %[1]s t, lateral flatten(input => [object_construct('k', 'a', 'l', t.a, 'n', null),
				object_construct_keep_null('k', 'b', 'l', t.a, 'n', null),
				object_construct('k', null, 'l', ['x', 'y'], 'n', 1.5),
				object_construct_keep_null('l', t.o, 'n', object_construct('z', true))]) g %[2]s order by 1, 2`, builds: true,
			asJSON: `where g.value is not null`},
		// FLATTEN of a key: of a list, a map, a constant, a string, a key an
		// object lacks; with KEY, INDEX, PATH and THIS; by MODE; and with a
		// path, OUTER and TABLE(FLATTEN(...)).
		{q: `select t.id, g.index, v.key, v.index, v.path, v.value, v.this from %[1]s t,
				lateral flatten(input => [object_construct('l', t.a), object_construct_keep_null('l', t.o),
					object_construct('l', ['p', 'q']), object_construct('l', 'text'), object_construct('m', 1)]) g,
				lateral flatten(input => g.value:l) v %[2]s order by 1, 2, 4, 3`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, g.index, v.key, v.index, v.value from %[1]s t,
				lateral flatten(input => [object_construct('l', t.a), object_construct_keep_null('l', t.o)]) g,
				lateral flatten(input => get_path(g.value, 'l'), mode => 'OBJECT') v %[2]s order by 1, 2, 3`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, g.index, v.index, v.value::string from %[1]s t,
				lateral flatten(input => array_construct(object_construct('l', t.a), object_construct_keep_null('l', t.o)), mode => 'array') g,
				lateral flatten(input => get_path(g.value, 'l'), mode => 'ARRAY') v %[2]s order by 1, 2, 3`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, g.index, g.this, v.index, v.value from %[1]s t,
				table(flatten(input => [object_construct('l', t.a, 'k', 'x'), object_construct('k', 'y')])) g,
				lateral flatten(input => g.value:l, outer => true) v %[2]s order by 1, 2, 4`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, v.index, v.value from %[1]s t, lateral flatten(input => [object_construct('l', t.a)]) g,
				lateral flatten(input => get_path(g.value, 'l[2].y')) v, lateral flatten(input => g.value:l, path => '[3]') w
				%[2]s order by 1, 2`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, f.key, f.value::string, count(*) over (partition by f.key) from %s t, lateral flatten(input => t.o) f order by 1, 2`},
		{q: `select t.id, f.index, g.key, g.index, g.value, g.this from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value) g order by 1, 2, 3, 4`},
		{q: `select t.id, f.index, g.value from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value:y) g order by 1, 2, 3`},
		{q: `select id, (select count(*) from table(flatten(input => s.a)) f) from %s s order by id`},
	} {
		q := func(table string) string { return fmt.Sprintf(c.q, table) }
		if c.asJSON != "" {
			q = func(table string) string { return fmt.Sprintf(c.q, table, "") }
			if sql := translate(t, q("t_native")).SQL; !strings.Contains(sql, "_sf_fields") {
				t.Errorf("%s: flattens no structs:\n%s", q("t_native"), sql)
			}
			asJSON := fmt.Sprintf(c.q, "t_json", c.asJSON)
			if sql := translate(t, asJSON).SQL; strings.Contains(sql, "_sf_fields") {
				t.Errorf("%s: flattens structs:\n%s", asJSON, sql)
			}
			want, got := nativeRows(t, conn, asJSON), nativeRows(t, conn, q("t_json"))
			if got != want {
				t.Errorf("%s\n got\n%s\nwant (objects as JSON)\n%s", q("t_json"), got, want)
			}
		}
		want := nativeRows(t, conn, q("t_json"))
		got := nativeRows(t, conn, q("t_native"))
		if got != want {
			t.Errorf("%s\n got\n%s\nwant (as JSON)\n%s", q("t_native"), got, want)
		}
		if !c.builds {
			if text := nativeRows(t, conn, q("t_text")); text != want {
				t.Errorf("%s\n got\n%s\nwant (as JSON)\n%s", q("t_text"), text, want)
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
		// THIS only where the statement reads it.
		{`select f.value from t, lateral flatten(input => t.a) as f(seq, key, path, index, value, this), lateral flatten(input => f.value) as g(seq, key, path, index, value, this)`,
			`CAST(NULL AS JSON) AS "THIS"`, `sf_flatten_this`},
		{`select f.this from t, lateral flatten(input => t.a) as f(seq, key, path, index, value, this)`,
			`unnest(sf_flatten_this(t.a, 'BOTH')) AS _sf_this`, `CAST(NULL AS JSON) AS "THIS"`},
		{`select f.whole from t, lateral flatten(input => [1, 2]) as f(s, k, p, i, v, whole)`,
			`unnest(repeat([json_array(1, 2)], 2)) AS _sf_this`, `CAST(NULL AS JSON) AS "THIS"`},
		{`select * from t, lateral flatten(input => t.a) f`, `unnest(sf_flatten_this(t.a, 'BOTH'))`, `CAST(NULL AS JSON) AS "THIS"`},
		// A FLATTEN of JSON objects that another reads gets the columns it
		// builds them from as JSON, unless something else reads them.
		{`select v.value from t, lateral flatten(input => [object_construct('k', t.a)]) g, lateral flatten(input => g.value) v`,
			`from (SELECT * REPLACE (sf_to_json(t.a) AS a) FROM t) AS t, LATERAL`, ``},
		{`select v.value from s.tab as m, lateral flatten(input => [object_construct('k', m.a, 'l', m.b), object_construct('k', m.a)]) g, lateral flatten(input => g.value) v`,
			`from (SELECT * REPLACE (sf_to_json(m.a) AS a, sf_to_json(m.b) AS b) FROM s.tab as m) AS m, LATERAL`, ``},
		{`select count(distinct m.id) from tab m inner join c on c.id = m.id cross join lateral flatten(input => [object_construct('k', m.a)]) as g(seq, key, path, index, value, this) cross join lateral flatten(input => g.value) as v(seq, key, path, index, value, this)`,
			`from (SELECT * REPLACE (sf_to_json(m.a) AS a) FROM tab m) AS m inner join c`, ``},
		{`select m.a, v.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g, lateral flatten(input => g.value) v`,
			`from tab m, LATERAL`, `REPLACE`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g, lateral flatten(input => g.value) v where m.a is not null`,
			`from tab m, LATERAL`, `REPLACE`},
		{`select g.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g`, `from tab m, LATERAL`, `REPLACE`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g, lateral flatten(input => g.value) v union all select m.a from tab2 m`,
			`from tab m, LATERAL`, `REPLACE`},
		// Objects of literal keys whose VALUE the statement reads by a key
		// are structs, a key of strings a VARCHAR and any other key a list
		// and a JSON part; a later FLATTEN of a key unnests the struct's,
		// and their columns are not turned into JSON ahead.
		{`select g.value:k::string, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a), object_construct_keep_null('k', 'b', 'l', m.b)]) g, lateral flatten(input => get_path(g.value, 'l')) v`,
			`unnest([{'_sf_k1': CAST('a' AS VARCHAR), '_sf_l2': CAST(sf_list_part(m.a) AS VARCHAR[]), '_sf_j2': CAST(sf_json_part(m.a) AS JSON)}, ` +
				`{'_sf_k1': CAST('b' AS VARCHAR), '_sf_l2': CAST(sf_list_part(m.b) AS VARCHAR[]), '_sf_j2': CAST(sf_json_part_keep_null(m.b) AS JSON)}]) AS _sf_value`, `REPLACE`},
		{`select g.value:k::string, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => get_path(g.value, 'l')) v`,
			`select g._sf_fields._sf_k1 AS "G.VALUE:K::STRING", v.value from tab m, LATERAL`, `json_extract`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`FROM (SELECT unnest(sf_parts_values(g._sf_fields._sf_l1, g._sf_fields._sf_j1, 'BOTH')) AS _sf_value`, `REPLACE`},
		{`select g.value:l, g.value:l[0]::string, g.value:none from tab m, lateral flatten(input => [object_construct('l', ['x', 'y'], 'n', null), object_construct_keep_null('l', null, 'n', 1)]) g`,
			`select sf_parts_json(g._sf_fields._sf_l1, g._sf_fields._sf_j1) AS "G.VALUE:L", ` +
				`json_extract_string(sf_parts_json(g._sf_fields._sf_l1, g._sf_fields._sf_j1), '$[0]') AS "G.VALUE:L[0]::STRING", CAST(NULL AS JSON) AS "G.VALUE:NONE" from`, `json_extract(`},
		{`select g.value:l from tab m, lateral flatten(input => [object_construct('l', ['x', 'y'], 'n', null), object_construct_keep_null('l', null, 'n', 1)]) g`,
			`{'_sf_l1': CAST(['x', 'y'] AS VARCHAR[]), '_sf_j1': CAST(NULL AS JSON), '_sf_l2': CAST(NULL AS VARCHAR[]), '_sf_j2': CAST(NULL AS JSON)}, ` +
				`{'_sf_l1': CAST(NULL AS VARCHAR[]), '_sf_j1': CAST(CAST('null' AS JSON) AS JSON), '_sf_l2': CAST(NULL AS VARCHAR[]), '_sf_j2': CAST(coalesce(to_json(1), CAST('null' AS JSON)) AS JSON)}`, ``},
		{`select g.value:k from tab m, table(flatten(input => array_construct(object_construct('k', 'a')))) as g(seq, key, path, index, value, this)`,
			`select to_json(g._sf_fields._sf_k1) AS "G.VALUE:K" from`, ``},
		// VALUE is JSON, which DuckDB computes only where it is read, and
		// any other read of it, or any other FLATTEN, flattens JSON.
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`,
			`to_json(map_from_entries(list_sort(list_filter([{'k': CAST('k' AS VARCHAR), 'v': CAST(to_json(_sf_flatten._sf_value._sf_k1) AS JSON)}], lambda e: e.v IS NOT NULL)))) AS "VALUE"`, ``},
		{`select g.value from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.value[0] from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select get_path(g.value, '[0]') from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select json_extract_path_text(g.value, 'k') from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select * from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.* from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select value:k from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.val:k from tab m, lateral flatten(input => [object_construct('k', 'a')]) as g(seq, key, path, index, val, this)`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a')], outer => true) g`, `json_each`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a')], mode => 'OBJECT') g`, `_sf_flatten_input`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct(m.k, 'a')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a', 'k', 'b')]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a'), m.o]) g`, `unnest([coalesce(`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a')]) g where exists (select 1 from u as g where g.value:k = 'a')`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.value:k from tab m, lateral flatten(input => [object_construct('k', 'a')]) g, lateral flatten(input => m.a) as g`, `unnest([coalesce(`, `_sf_fields`},
		// So does one at a level that filters its rows, where DuckDB joins it
		// with a dependent join.
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id > 1`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g join lateral flatten(input => g.value:l) v on v.index > 0`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.value from tab m join u on u.id = m.id, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`_sf_fields`, ``},
		{`select v.value, count(*) from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v group by 1 having count(*) > 1 and v.value <> 'x'`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.value, count(*) from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v group by 1 having count(*) > 1 and sum(m.n) < 9 order by 1`,
			`_sf_fields`, ``},
		// A WHERE becomes the ON of a last FLATTEN's join, which DuckDB
		// then unnests where it reads the column.
		{`select f.value from t, lateral flatten(input => t.a) f where t.id > 1 and f.index < 2 group by 1`,
			`) AS "f" ON (t.id > 1 and f.index < 2) group by 1`, `where`},
		{`select f.value from t cross join lateral flatten(input => t.a) f where t.id > 1`,
			`from t  join LATERAL (SELECT`, `cross`},
		{`select count(*) from t inner join u on u.id = t.id, lateral flatten(input => t.a) as f(seq, key, path, index, value, this) where u.k = 'x'`,
			`inner join u on u.id = t.id JOIN LATERAL`, `where`},
		{`select t.id + 1 as n, f.value from t, lateral flatten(input => t.a) f where n > 1`, `where n > 1`, `ON (`},
		{`select f.value from t left join u on u.id = t.id, lateral flatten(input => t.a) f where t.id > 1`, `where t.id > 1`, `ON (`},
		{`select f.value from t, lateral flatten(input => t.a) f where t.id in (select id from u)`, `where t.id in`, `ON (`},
		{`select f.value from t, lateral flatten(input => t.a) f, u where u.id = t.id`, `where u.id = t.id`, `ON (`},
	} {
		res, err := Translate(c.sql, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.SQL, c.has) || c.hasNot != "" && strings.Contains(res.SQL, c.hasNot) {
			t.Errorf("%s:\n%s\nwant %q and not %q", c.sql, res.SQL, c.has, c.hasNot)
		}
	}
}

// TestFlattenStructsPlan checks that DuckDB plans FLATTEN of objects built
// from a row's ARRAY columns, and a FLATTEN of their lists after it, as
// reports write them, without a dependent join, which an earlier FLATTEN's
// JSON VALUE takes (see flattenStructs).
func TestFlattenStructsPlan(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE t AS SELECT * FROM (VALUES (1, ['a', 'b'], ['c']), (2, NULL, [])) v(id, a, b)`); err != nil {
		t.Fatal(err)
	}
	res, err := Translate(`select cast(get_path(g.value, 'kind') as text), cast(v.value as text), count(distinct m.id)
		from t as m inner join (select 1 as id union all select 2) as u on u.id = m.id
		cross join lateral flatten(input => [object_construct('kind', 'all', 'items', ['every']), object_construct_keep_null('kind', 'a', 'items', m.a),
			object_construct_keep_null('kind', 'b', 'items', m.b)]) as g(seq, key, path, index, value, this)
		cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)
		group by 1, 2`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.QueryContext(ctx, "EXPLAIN "+res.SQL)
	if err != nil {
		t.Fatalf("%v\n%s", err, res.SQL)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plan.String(), "DELIM_JOIN") {
		t.Errorf("a dependent join:\n%s\n%s", res.SQL, plan.String())
	}
}

// TestFlattenWhere checks the rows of FLATTENs whose WHERE becomes their
// join's ON (see whereOn), against the rows Snowflake returns.
func TestFlattenWhere(t *testing.T) {
	conn := session(t)
	if _, err := conn.ExecContext(context.Background(), `CREATE TABLE t AS SELECT * FROM (VALUES
		(1, ['a', 'b'], MAP {'k': '1', 'l': '0'}), (2, ['c'], MAP {'k': '0'}), (3, [], MAP {}), (4, NULL, NULL)) v(id, a, o)`); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ q, want string }{
		{`select t.id, f.value::string from t, lateral flatten(input => t.a) f where t.id < 3 and f.index >= 0 order by 1, 2`,
			"1|a\n1|b\n2|c"},
		{`select f.key, sum(f.value::int) from t cross join lateral flatten(input => t.o) f where t.id <> 2 group by 1 order by 1`,
			"k|1\nl|0"},
		{`select t.id, count(*) as n from t, lateral flatten(input => t.a) f where f.value::string <> 'b' group by t.id having count(*) > 0 order by n desc, 1`,
			"1|1\n2|1"},
		{`select t.id, f.value::string as v from t, lateral flatten(input => t.a, outer => true) f where t.id > 2 order by 1`,
			"3|NULL\n4|NULL"},
	} {
		if got := nativeRows(t, conn, c.q); got != c.want {
			t.Errorf("%s\n got %q\nwant %q", c.q, got, c.want)
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
