package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
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
	strs := []string{"'s1'", "''", "NULL", "' s4 '"}
	for i, d := range docs {
		values = append(values, fmt.Sprintf("(%d, '%s', '%s', %s)", i+1, d.array, d.object, strs[i]))
	}
	values = append(values, "(9, NULL, NULL, NULL)")
	// t_text holds them as JSON text, as tables loaded from text do; t_json
	// as JSON, as VARIANT columns and views that parse text do; t_native as
	// Igloo stores Snowflake's ARRAY and OBJECT. Each has a column of plain
	// strings, s, too.
	for _, stmt := range []string{
		"CREATE TABLE t_text AS SELECT * FROM (VALUES " + strings.Join(values, ", ") + ") v(id, a, o, s)",
		"CREATE TABLE t_json AS SELECT id, CAST(a AS JSON) AS a, CAST(o AS JSON) AS o, s FROM t_text",
		"CREATE TABLE t_native AS SELECT id, CAST(a AS VARCHAR[]) AS a, CAST(o AS MAP(VARCHAR, VARCHAR)) AS o, s FROM t_json",
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
				lateral flatten(input => g.value:l, outer => true) v order by 1, 2, 4`, builds: true},
		// Reports' signals: objects of a dimension and a row's string or
		// other value, filtered by those values, with no FLATTEN after;
		// objects of a group, a dimension, a name and a metric, filtered by
		// all of them; and objects of a dimension and a list, whose elements
		// are filtered.
		{q: `select cast(get_path(d.value, 'dim') as text) as dim, cast(get_path(d.value, 'name') as text) as name, count(distinct t.id), d.index, d.path
			from %[1]s as t cross join lateral flatten(input => [object_construct('dim', 'S', 'name', t.s), object_construct('dim', 'FIRST', 'name', t.a[0]::string),
				object_construct('dim', 'ID', 'name', iff(t.id > 2, 'big', null)), object_construct_keep_null('dim', 'KEPT', 'name', t.s),
				object_construct('dim', 'ALL', 'name', 'any')]) as d(seq, key, path, index, value, this)
			where not cast(get_path(d.value, 'name') as text) is null and trim(cast(get_path(d.value, 'name') as text)) <> '' %[2]s
			group by 1, 2, 4, 5 order by 1, 2, 4`, builds: true,
			asJSON: `and d.value is not null`},
		{q: `select cast(get_path(d.value, 'dim') as text) as dim, cast(get_path(d.value, 'name') as text) as name, count(distinct t.id)
			from %[1]s as t cross join lateral flatten(input => [object_construct('dim', 'S', 'name', t.s), object_construct('dim', 'FIRST', 'name', t.a[0]::string),
				object_construct('dim', 'ID', 'name', iff(t.id > 2, 'big', null)), object_construct_keep_null('dim', 'KEPT', 'name', t.s),
				object_construct('dim', 'ALL', 'name', 'any')]) as d(seq, key, path, index, value, this)
			where not cast(get_path(d.value, 'name') as text) is null and trim(cast(get_path(d.value, 'name') as text)) <> '' %[2]s
			group by 1, 2 order by 1, 2`, builds: true,
			asJSON: `and d.value is not null`},
		{q: `select iff(cast(get_path(x.value, 'grp') as text) = 'R', 'R', 'I') as grp, cast(get_path(x.value, 'dim') as text) as dim,
				cast(get_path(x.value, 'name') as text) as name, count(distinct t.id), avg(cast(get_path(x.value, 'metric') as float))
			from %[1]s as t inner join (select 1 as id union all select 3 union all select 4) as u on u.id = t.id
			cross join lateral flatten(input => [object_construct('grp', 'R', 'dim', 'S', 'name', t.s),
				object_construct('grp', 'I', 'dim', 'A', 'name', 'first', 'metric', array_size(t.a)), object_construct('grp', 'I', 'dim', 'B', 'name', 'id', 'metric', t.id - 2)]) as x(seq, key, path, index, value, this)
			where not cast(get_path(x.value, 'name') as text) is null and trim(cast(get_path(x.value, 'name') as text)) <> ''
				and (cast(get_path(x.value, 'grp') as text) = 'R' or (cast(get_path(x.value, 'grp') as text) = 'I' and cast(get_path(x.value, 'metric') as float) > 0)) %[2]s
			group by 1, 2, 3 order by 1, 2, 3`, builds: true,
			asJSON: `and x.value is not null`},
		{q: `select cast(get_path(b.value, 'dim') as text) as dim, cast(i.value as text) as name, count(distinct t.id), count(*)
			from %[1]s as t cross join lateral flatten(input => [object_construct('dim', 'A', 'items', t.a), object_construct('dim', 'O', 'items', t.o),
				object_construct('dim', 'ACT', 'items', ['active', ''])]) as b(seq, key, path, index, value, this)
			cross join lateral flatten(input => get_path(b.value, 'items')) as i(seq, key, path, index, value, this)
			where not cast(i.value as text) is null and trim(cast(i.value as text)) <> '' and t.id <> 3 %[2]s
			group by 1, 2 order by 1, 2`, builds: true,
			asJSON: `and b.value is not null`},
		// Elements that every read casts, which are unnested as text: of
		// lists, maps, JSON of each type and constants, one of which holds
		// an object's text as a string; and elements read otherwise, by
		// MIN and MAX, a path or ORDER BY.
		{q: `select t.id, cast(g.value:k as text) as k, cast(v.value as text) as x, try_cast(v.value as float) as f, v.value::string as y
			from %[1]s t, lateral flatten(input => [object_construct('k', 'a', 'l', t.a), object_construct_keep_null('k', 'o', 'l', t.o),
				object_construct('k', 'j', 'l', parse_json('[1, 2.5, true, null, "x", {"a": [1, "b"]}, " y "]')),
				object_construct('k', 'c', 'l', ['p', '{"q": 1}', '']), object_construct('k', 's', 'l', 'text')]) g,
			lateral flatten(input => g.value:l) v %[2]s order by 1, 2, 3, 4, 5`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select cast(g.value:k as text) as k, min(v.value), max(v.value), max(get_path(v.value, 'name')::string), count(*)
			from %[1]s t, lateral flatten(input => [object_construct('k', 'a', 'l', t.a), object_construct('k', 'c', 'l', ['p', 'q'])]) g,
			lateral flatten(input => g.value:l) v %[2]s group by 1 order by 1`, builds: true,
			asJSON: `where g.value is not null`},
		{q: `select t.id, v.value from %[1]s t, lateral flatten(input => [object_construct('l', t.a)]) g, lateral flatten(input => g.value:l) v
			%[2]s order by v.value, 1`, builds: true,
			asJSON: `where g.value is not null`},
		// Keys that every read casts, by each form of read: strings, lists,
		// maps, NULLs left out or kept, numbers and objects, and a key some
		// objects lack.
		{q: `select t.id, cast(g.value:k as text), g.value:l::string, cast(get_path(g.value, 'n') as text), try_cast(get(g.value, 'n') as float),
				cast(g.value['m'] as text)
			from %[1]s t, lateral flatten(input => [object_construct('k', 'a', 'l', t.a, 'n', null, 'm', t.o),
				object_construct_keep_null('k', 'b', 'l', t.a, 'n', null, 'm', t.s),
				object_construct('k', null, 'l', ['x', 'y'], 'n', 1.5, 'm', parse_json('{"z": [1, "w"]}')),
				object_construct_keep_null('l', t.o, 'n', object_construct('z', true), 'm', t.id)]) g %[2]s order by 1, 2, 3, 4, 5, 6`, builds: true,
			asJSON: `where g.value is not null`},
		// CTEs that each flatten objects of their own keys AS g and v.
		{q: `with a as (select t.id, 'a' as src, g.index as gi, g.value:k::string as k, v.value::string as x from %[1]s t,
					lateral flatten(input => [object_construct('k', 'x', 'l', t.a), object_construct('l', t.o)]) g,
					lateral flatten(input => g.value:l) v %[2]s),
				b as (select t.id, 'b' as src, g.index as gi, g.value:k::string as k, v.value::string as x from %[1]s t,
					lateral flatten(input => [object_construct('l', t.a, 'k', t.id), object_construct_keep_null('k', 'y')]) g,
					lateral flatten(input => get_path(g.value, 'l')) v %[2]s)
				select * from a union all select * from b order by 1, 2, 3, 5`, builds: true,
			asJSON: `where g.value is not null`},
		// A WHERE on the table, on the objects' keys, or both, which the
		// translation keeps out of the joins (see structWhere).
		{q: `select t.id, g.index, v.index, v.value::string from %[1]s t,
				lateral flatten(input => [object_construct('k', 'a', 'l', t.a), object_construct_keep_null('k', 'b', 'l', t.o),
					object_construct('l', ['p', 'q'])]) g,
				lateral flatten(input => g.value:l) v
				where t.id <> 3 and (g.value:k <> 'b' or g.value:k is null) and t.id between 1 and 9 %[2]s order by 1, 2, 3`, builds: true,
			asJSON: `and g.value is not null`},
		{q: `select cast(g.value:facet_type as text) as bt, cast(v.value as text) as bv, count(distinct t.id), count(*)
			from %[1]s as t cross join lateral flatten(input => [object_construct('facet_type', 'all', 'facet_values', ['every']),
				object_construct_keep_null('facet_type', 'array', 'facet_values', t.a)]) as g(seq, key, path, index, value, this)
			cross join lateral flatten(input => get_path(g.value, 'facet_values')) as v(seq, key, path, index, value, this)
			where t.id in (1, 2, 4, 9) and get_path(g.value, 'facet_type')::string <> 'none' %[2]s group by 1, 2 order by 1, 2`, builds: true,
			asJSON: `and g.value is not null`},
		{q: `select t.id, g.index, g.value:l from %[1]s t, lateral flatten(input => [object_construct('l', t.a), object_construct('l', t.o)]) g
				where t.id < 3 %[2]s order by 1, 2`, builds: true,
			asJSON: `and g.value is not null`},
		{q: `select t.id, v.index, v.value from %[1]s t, lateral flatten(input => [object_construct('l', t.a)]) g,
				lateral flatten(input => get_path(g.value, 'l[2].y')) v, lateral flatten(input => g.value:l, path => '[3]') w
				order by 1, 2`, builds: true},
		{q: `select t.id, f.key, f.value::string, count(*) over (partition by f.key) from %s t, lateral flatten(input => t.o) f order by 1, 2`},
		{q: `select t.id, f.index, g.key, g.index, g.value, g.this from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value) g order by 1, 2, 3, 4`},
		{q: `select t.id, f.index, g.value from %s t, lateral flatten(input => t.a) f, lateral flatten(input => f.value:y) g order by 1, 2, 3`},
		{q: `select id, (select count(*) from table(flatten(input => s.a)) f) from %s s order by id`},
	} {
		q := func(table string) string { return fmt.Sprintf(c.q, table) }
		if c.asJSON != "" {
			q = func(table string) string { return fmt.Sprintf(c.q, table, "") }
			if sql := translate(t, q("t_native")).SQL; !strings.Contains(sql, "_sf_fields") && !strings.Contains(sql, "AS _sf_t FROM") {
				t.Errorf("%s: flattens no structs:\n%s", q("t_native"), sql)
			}
			asJSON := fmt.Sprintf(c.q, "t_json", c.asJSON)
			if sql := translate(t, asJSON).SQL; strings.Contains(sql, "_sf_fields") || strings.Contains(sql, "AS _sf_t FROM") {
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
			`from tab m CROSS JOIN LATERAL`, `REPLACE`},
		{`select g.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g`, `from tab m, LATERAL`, `REPLACE`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('k', m.a)]) g, lateral flatten(input => g.value) v union all select m.a from tab2 m`,
			`from tab m, LATERAL`, `REPLACE`},
		// Objects of literal keys whose VALUE the statement reads by a key
		// are structs, a key of strings a VARCHAR and any other key a list
		// and a JSON part; a later FLATTEN of a key unnests the struct's,
		// and their columns are not turned into JSON ahead.
		{`select g.value:k::string, g.index, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a), object_construct_keep_null('k', 'b', 'l', m.b)]) g, lateral flatten(input => get_path(g.value, 'l')) v`,
			`unnest([{'_sf_i': CAST(0 AS BIGINT), '_sf_k1': CAST('a' AS VARCHAR), '_sf_l2': CAST(sf_list_part(m.a) AS VARCHAR[]), ` +
				`'_sf_s2': CAST(sf_string_part(m.a) AS VARCHAR), '_sf_j2': CAST(sf_other_part(m.a) AS JSON)}, {'_sf_i': CAST(1 AS BIGINT), '_sf_k1': CAST('b' AS VARCHAR), ` +
				`'_sf_l2': CAST(sf_list_part(m.b) AS VARCHAR[]), '_sf_s2': CAST(sf_string_part(m.b) AS VARCHAR), '_sf_j2': CAST(sf_other_part_keep_null(m.b) AS JSON)}]) AS _sf_value`,
			`REPLACE`},
		{`select g.value:k::string, g.index, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => get_path(g.value, 'l')) v`,
			`select g._sf_fields._sf_k1 AS "G.VALUE:K::STRING", g.index, v.value from tab m, LATERAL`, `json_extract`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`FROM (SELECT unnest(sf_parts_values(g._sf_fields._sf_l1, g._sf_fields._sf_j1, 'BOTH')) AS _sf_value, CAST(NULL AS VARCHAR) AS _sf_key, CAST(NULL AS BIGINT) AS _sf_index)`,
			`REPLACE`},
		// Objects of one FROM item's columns are unnested in its select list,
		// and a later FLATTEN of one key in a select list around that one, so
		// that no filter DuckDB pushes onto them makes a dependent join.
		{`select g.value:k::string, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a), object_construct_keep_null('k', 'b', 'l', m.b)]) g, lateral flatten(input => get_path(g.value, 'l')) v`,
			`select m._sf_t._sf_k1 AS "G.VALUE:K::STRING", m._sf_e from (SELECT m.*, unnest(sf_parts_values(m._sf_t._sf_l2, m._sf_t._sf_j2, 'BOTH')) AS _sf_e ` +
				`FROM (SELECT m.*, CASE _sf_k.i WHEN 0 THEN {'_sf_i': CAST(0 AS BIGINT), '_sf_k1': CAST('a' AS VARCHAR), '_sf_l2': CAST(sf_list_part(m.a) AS VARCHAR[]), ` +
				`'_sf_s2': CAST(sf_string_part(m.a) AS VARCHAR), '_sf_j2': CAST(sf_other_part(m.a) AS JSON)} WHEN 1 THEN {'_sf_i': CAST(1 AS BIGINT), '_sf_k1': CAST('b' AS VARCHAR), ` +
				`'_sf_l2': CAST(sf_list_part(m.b) AS VARCHAR[]), '_sf_s2': CAST(sf_string_part(m.b) AS VARCHAR), '_sf_j2': CAST(sf_other_part_keep_null(m.b) AS JSON)} END AS _sf_t ` +
				`FROM tab m CROSS JOIN range(2) AS _sf_k(i)) AS m) AS m`, `LATERAL`},
		// Elements that every read casts are unnested as text; one read
		// otherwise keeps them JSON.
		{`select cast(g.value:k as text), cast(v.value as text), v.value::float from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`select m._sf_t._sf_k1 AS "CAST(G.VALUE:K AS TEXT)", m._sf_e AS "CAST(V.VALUE AS TEXT)", CAST(m._sf_e AS DOUBLE) AS "V.VALUE::FLOAT" ` +
				`from (SELECT m.*, unnest(sf_parts_strings(m._sf_t._sf_l2, m._sf_t._sf_j2, 'BOTH')) AS _sf_e FROM`, `json_extract`},
		{`select cast(v.value as text), v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`select json_extract_string(m._sf_e, '$') AS "CAST(V.VALUE AS TEXT)", m._sf_e from (SELECT m.*, unnest(sf_parts_values(`, `sf_parts_strings`},
		{`select v.value:x::string from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`unnest(sf_parts_values(`, `sf_parts_strings`},
		{`select g.value:k::string from tab m, lateral flatten(input => [object_construct('k', ['p', '{"q": 1}'])]) g`,
			`'_sf_l1': CAST(NULL AS VARCHAR[]), '_sf_s1': CAST(sf_string_part(json_array('p', '{"q": 1}')) AS VARCHAR), '_sf_j1': CAST(sf_expr_part(json_array('p', '{"q": 1}')) AS JSON)`, ``},
		{`select g.value:n::string from tab m inner join u on u.id = m.id cross join lateral flatten(input => [object_construct('n', m.s), object_construct('n', 'x')]) as g(seq, key, path, index, value, this) where m.id > 1`,
			`select m._sf_t._sf_t1 AS "G.VALUE:N::STRING" from (SELECT m.*, CASE _sf_k.i WHEN 0 THEN {'_sf_i': CAST(0 AS BIGINT), ` +
				`'_sf_t1': CAST(sf_fields_string(sf_list_part(m.s), sf_string_part(m.s), sf_other_part(m.s)) AS VARCHAR)} ` +
				`WHEN 1 THEN {'_sf_i': CAST(1 AS BIGINT), '_sf_t1': CAST(sf_fields_string(NULL, 'x', NULL) AS VARCHAR)} END AS _sf_t FROM tab m CROSS JOIN range(2) AS _sf_k(i)) AS m ` +
				`inner join u on u.id = m.id where m.id > 1`, `LATERAL`},
		// A key that every read casts is its text alone; one read otherwise
		// keeps its parts.
		{`select cast(get_path(g.value, 'n') as text), try_cast(get(g.value, 'n') as float), g.value['n']::string from tab m, lateral flatten(input => [object_construct('n', m.s)]) g`,
			`select m._sf_t._sf_t1 AS "CAST(GET_PATH(G.VALUE, 'N') AS TEXT)", TRY_CAST(m._sf_t._sf_t1 AS DOUBLE) AS "TRY_CAST(GET(G.VALUE, 'N') AS FLOAT)", m._sf_t._sf_t1 AS`, `_sf_l1`},
		// Their reads in a WHERE go through sf_text_read, which keeps
		// DuckDB's filter on them above the select list that unnests them,
		// a key of string literals' too.
		{`select cast(g.value:k as text) from tab m, lateral flatten(input => [object_construct('k', 'a', 'n', m.s), object_construct('k', 'b', 'n', 'x')]) g where cast(g.value:k as text) = 'a' or (cast(g.value:k as text) = 'b' and cast(g.value:n as text) <> '')`,
			`select m._sf_t._sf_k1 AS "CAST(G.VALUE:K AS TEXT)" from (SELECT m.*, CASE _sf_k.i WHEN 0 THEN`, `json_extract`},
		{`select cast(g.value:k as text) from tab m, lateral flatten(input => [object_construct('k', 'a', 'n', m.s), object_construct('k', 'b', 'n', 'x')]) g where cast(g.value:k as text) = 'a' or (cast(g.value:k as text) = 'b' and cast(g.value:n as text) <> '')`,
			`AS _sf_k(i)) AS m where sf_text_read(m._sf_t._sf_k1) = 'a' or (sf_text_read(m._sf_t._sf_k1) = 'b' and sf_text_read(m._sf_t._sf_t2) <> '')`, ``},
		{`select cast(g.value:n as text), cast(v.value as text) from tab m, lateral flatten(input => [object_construct('n', m.s, 'l', m.a)]) g, lateral flatten(input => g.value:l) v where cast(v.value as text) <> '' and cast(g.value:n as text) <> 'x' group by 1, 2`,
			`select m._sf_t._sf_t1 AS "CAST(G.VALUE:N AS TEXT)", m._sf_e AS "CAST(V.VALUE AS TEXT)" from`,
			`json_extract`},
		{`select cast(g.value:n as text), cast(v.value as text) from tab m, lateral flatten(input => [object_construct('n', m.s, 'l', m.a)]) g, lateral flatten(input => g.value:l) v where cast(v.value as text) <> '' and cast(g.value:n as text) <> 'x' group by 1, 2`,
			`FROM tab m) AS m) AS m where sf_text_read(m._sf_e) <> '' and sf_text_read(m._sf_t._sf_t1) <> 'x' group by 1, 2`, `json_extract`},
		{`select cast(g.value:n as text), g.value:n from tab m, lateral flatten(input => [object_construct('n', m.s)]) g`,
			`'_sf_l1': CAST(sf_list_part(m.s) AS VARCHAR[])`, `_sf_t1`},
		{`select cast(g.value:n.x as text) from tab m, lateral flatten(input => [object_construct('n', m.s)]) g`, `'_sf_l1'`, `_sf_t1`},
		{`select cast(get_path(g.value, 'n.x') as text) from tab m, lateral flatten(input => [object_construct('n', m.s)]) g`, `'_sf_l1'`, `_sf_t1`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v, lateral flatten(input => v.value) w where m.id > 1`,
			`AS _sf_t FROM tab m) AS m) AS m, LATERAL (SELECT`, `"g"`},
		// Not when the objects read two FROM items, or a column unqualified,
		// or two FLATTENs read them, or the select reads other columns.
		{`select v.value from tab m join u on u.id = m.id, lateral flatten(input => [object_construct('l', m.a, 'k', u.k)]) g, lateral flatten(input => g.value:l) v`,
			`AS _sf_fields FROM`, `_sf_t`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', a)]) g, lateral flatten(input => g.value:l) v`, `AS _sf_fields FROM`, `_sf_t`},
		{`select v.value, w.value from tab m, lateral flatten(input => [object_construct('l', m.a, 'k', m.b)]) g, lateral flatten(input => g.value:l) v, lateral flatten(input => g.value:k) w`,
			`AS _sf_fields FROM`, `_sf_t`},
		{`select v.value, g.value:l from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`select m._sf_e, sf_fields_json(m._sf_t._sf_l1, m._sf_t._sf_s1, m._sf_t._sf_j1) AS "G.VALUE:L" from (SELECT m.*, unnest(sf_parts_values(m._sf_t._sf_l1, m._sf_t._sf_j1, 'BOTH')) AS _sf_e FROM`, ``},
		{`select v.value, g.index from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`, `AS _sf_fields FROM`, `_sf_t`},
		// Its KEY, PATH, INDEX or THIS read, the last FLATTEN unnests them too.
		{`select v.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`unnest(sf_parts_values(g._sf_fields._sf_l1, g._sf_fields._sf_j1, 'BOTH')) AS _sf_value, unnest(sf_parts_keys(`, ``},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) as g(seq, key, path, index, value, this), lateral flatten(input => g.value:l) as v(a, b, c, i, value, d) order by v.i`,
			`unnest(sf_parts_indexes(`, ``},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v order by index`,
			`unnest(sf_parts_indexes(`, ``},
		// A column of the same name elsewhere is another's: of a table, or in
		// another select, as reports name an INDEX.
		{`with a as (select v.value as x, u."INDEX", u.key from tab m join u on u.id = m.id, lateral flatten(input => [object_construct('l', m.a)]) as g(seq, key, path, index, value, this),
					lateral flatten(input => g.value:l) as v(seq, key, path, index, value, this) where v.value::string <> ''),
				b as (select r.index, r."INDEX", r.s as index, r.path from ranks r)
				select * from a, b order by index`,
			`AS _sf_t FROM tab m) AS m) AS m join u on u.id = m.id where json_extract_string(m._sf_e, '$') <> ''`, ``},
		// A key's strings, a string's column and any other string a
		// VARCHAR, read as itself.
		{`select g.value:n::string, g.value:n, g.value:n = 'x' from tab m, lateral flatten(input => [object_construct('n', m.s), object_construct('n', iff(m.b, 'y', 'z')), object_construct('n', 'w')]) g`,
			`select sf_fields_string(m._sf_t._sf_l1, m._sf_t._sf_s1, m._sf_t._sf_j1) AS "G.VALUE:N::STRING", sf_fields_json(m._sf_t._sf_l1, m._sf_t._sf_s1, m._sf_t._sf_j1) AS "G.VALUE:N", ` +
				`sf_fields_string(m._sf_t._sf_l1, m._sf_t._sf_s1, m._sf_t._sf_j1) = 'x'`, ``},
		{`select g.value:n from tab m, lateral flatten(input => [object_construct('n', m.s), object_construct('n', iff(m.b, 'y', 'z')), object_construct('n', 'w')]) g`,
			`'_sf_s1': CAST(sf_string_part(CASE WHEN (m.b) THEN 'y' ELSE 'z' END) AS VARCHAR), '_sf_j1': CAST(sf_expr_part(CASE WHEN (m.b) THEN 'y' ELSE 'z' END) AS JSON)} ` +
				`WHEN 2 THEN {'_sf_i': CAST(2 AS BIGINT), '_sf_l1': CAST(NULL AS VARCHAR[]), '_sf_s1': CAST('w' AS VARCHAR), '_sf_j1': CAST(NULL AS JSON)} END AS _sf_t ` +
				`FROM tab m CROSS JOIN range(3) AS _sf_k(i)) AS m`, ``},
		{`select g.value:l, g.value:l[0]::string, g.value:none from tab m, lateral flatten(input => [object_construct('l', ['x', 'y'], 'n', null), object_construct_keep_null('l', null, 'n', 1)]) g`,
			`select sf_fields_json(g._sf_fields._sf_l1, g._sf_fields._sf_s1, g._sf_fields._sf_j1) AS "G.VALUE:L", ` +
				`json_extract_string(sf_fields_json(g._sf_fields._sf_l1, g._sf_fields._sf_s1, g._sf_fields._sf_j1), '$[0]') AS "G.VALUE:L[0]::STRING", CAST(NULL AS JSON) AS "G.VALUE:NONE" from`, `json_extract(`},
		{`select g.value:l from tab m, lateral flatten(input => [object_construct('l', ['x', 'y'], 'n', null), object_construct_keep_null('l', null, 'n', 1)]) g`,
			`{'_sf_i': CAST(0 AS BIGINT), '_sf_l1': CAST(['x', 'y'] AS VARCHAR[]), '_sf_s1': CAST(NULL AS VARCHAR), '_sf_j1': CAST(NULL AS JSON), ` +
				`'_sf_l2': CAST(NULL AS VARCHAR[]), '_sf_s2': CAST(NULL AS VARCHAR), '_sf_j2': CAST(NULL AS JSON)}, {'_sf_i': CAST(1 AS BIGINT), '_sf_l1': CAST(NULL AS VARCHAR[]), ` +
				`'_sf_s1': CAST(NULL AS VARCHAR), '_sf_j1': CAST(CAST('null' AS JSON) AS JSON), '_sf_l2': CAST(NULL AS VARCHAR[]), '_sf_s2': CAST(sf_string_part(1) AS VARCHAR), ` +
				`'_sf_j2': CAST(sf_expr_part_keep_null(1) AS JSON)}`, ``},
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
		// Selects apart, such as CTEs that each flatten objects AS g, are
		// apart, each with the keys of its own objects; a select nested in
		// one that names g again keeps the outer one JSON.
		{`with a as (select v.value as x, g.value:k::string as k from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v),
				b as (select v.value as x, g.value:k as k from tab m, lateral flatten(input => [object_construct('l', m.b, 'k', m.n)]) g, lateral flatten(input => g.value:l) v)
				select * from a union all select * from b`,
			`m._sf_t._sf_k1 as k from (SELECT m.*, unnest(sf_parts_values(m._sf_t._sf_l2,`, `unnest([coalesce(`},
		{`with a as (select v.value as x, g.value:k::string as k from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v),
				b as (select v.value as x, g.value:k as k from tab m, lateral flatten(input => [object_construct('l', m.b, 'k', m.n)]) g, lateral flatten(input => g.value:l) v)
				select * from a union all select * from b`,
			`sf_fields_json(m._sf_t._sf_l2, m._sf_t._sf_s2, m._sf_t._sf_j2) as k from (SELECT m.*, unnest(sf_parts_values(m._sf_t._sf_l1,`, `unnest([coalesce(`},
		{`with a as (select v.value as x, g.value:k::string as k from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v),
				b as (select v.value as x, g.value:k as k from tab m, lateral flatten(input => [object_construct('l', m.b, 'k', m.n)]) g, lateral flatten(input => g.value:l) v)
				select * from a union all select * from b`,
			`'_sf_l1': CAST(sf_list_part(m.b) AS VARCHAR[]), '_sf_s1': CAST(sf_string_part(m.b) AS VARCHAR), '_sf_j1': CAST(sf_other_part(m.b) AS JSON)`, `unnest([coalesce(`},
		{`with a as (select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v),
				b as (select g.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g)
				select * from a, b`,
			`unnest(sf_parts_values(m._sf_t._sf_l1, m._sf_t._sf_j1, 'BOTH')) AS _sf_e FROM (SELECT m.*, {'_sf_i': CAST(0 AS BIGINT), '_sf_l1'`, `CROSS JOIN range(`},
		{`select g.value:k, (select max(g.value:k::string) from table(flatten(input => [object_construct('k', 'b')])) g) from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`,
			`(select max(g._sf_fields._sf_k1) from LATERAL`, ``},
		{`select g.value:k, (select max(g.value:k::string) from table(flatten(input => [object_construct('k', 'b')])) g) from tab m, lateral flatten(input => [object_construct('k', 'a')]) g`,
			`select json_extract(g.value, '$.k') AS "G.VALUE:K"`, ``},
		// A WHERE stays out of the joins, which DuckDB would make dependent
		// ones: a condition on the tables goes into the first FLATTEN's ON,
		// one on its keys into the parts the last FLATTEN unnests.
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id > 1`,
			`AS _sf_t FROM tab m) AS m) AS m where m.id > 1`, `LATERAL`},
		{`select v.value from tab m cross join lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g cross join lateral flatten(input => g.value:l) v where g.value:k = 'a' group by 1`,
			`AS _sf_t FROM tab m) AS m) AS m where sf_text_read(m._sf_t._sf_k1) = 'a' group by 1`, `LATERAL`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id > 1`,
			`) AS "g" ON ((m.id > 1)), LATERAL (SELECT`, `where`},
		{`select g.index, v.value from tab m cross join lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g cross join lateral flatten(input => g.value:l) v where g.value:k = 'a' group by 1, 2`,
			`}], lambda _sf_e: (_sf_e._sf_k1 = 'a'))) AS _sf_value) AS _sf_flatten) AS "g" cross join LATERAL`, `where`},
		{`select g.index, v.value from tab m inner join u on u.id = m.id, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) as g(seq, key, path, index, value, this), lateral flatten(input => get_path(g.value, 'l')) v
				where m.id between 1 and 9 and (g.value:k = 'a' or m.id = 1) and case when m.a is null and m.id > 0 then false else true end order by 1`,
			`) AS "g"(seq, key, path, index, value, this) ON ((m.id between 1 and 9) AND (case when m.a is null and m.id > 0 then false else true end)), LATERAL`,
			`where`},
		{`select g.index, v.value from tab m inner join u on u.id = m.id, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) as g(seq, key, path, index, value, this), lateral flatten(input => get_path(g.value, 'l')) v
				where m.id between 1 and 9 and (g.value:k = 'a' or m.id = 1)`,
			`}], lambda _sf_e: ((_sf_e._sf_k1 = 'a' or m.id = 1)))) AS _sf_value`, `where`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v where g.value:k = 'a' or m.id > 1`,
			`}], lambda _sf_e: (_sf_e._sf_k1 = 'a' or m.id > 1))) AS _sf_value`, `ON (`},
		// Other WHEREs, and other filters, keep the objects JSON: DuckDB
		// joins structs with a dependent join there.
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where v.value::string <> 'x' and m.id > 1`,
			`) AS "g" ON ((m.id > 1)), LATERAL (SELECT CAST(NULL AS BIGINT) AS "SEQ", _sf_flatten._sf_key AS "KEY", coalesce(CASE WHEN _sf_flatten._sf_key IS NULL THEN '[' || _sf_flatten._sf_index || ']' ELSE _sf_flatten._sf_key END, '') AS "PATH", _sf_flatten._sf_index AS "INDEX", _sf_flatten._sf_value AS "VALUE", CAST(NULL AS JSON) AS "THIS" FROM (SELECT ` +
				`unnest(list_filter(sf_parts_values(g._sf_fields._sf_l1, g._sf_fields._sf_j1, 'BOTH'), lambda _sf_e: (json_extract_string(_sf_e, '$') <> 'x'))) AS _sf_value, CAST(NULL AS VARCHAR) AS _sf_key`,
			`where`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('k', 'a', 'l', m.a)]) g, lateral flatten(input => g.value:l) v where trim(v.value::string) <> g.value:k`,
			`lambda _sf_e: (trim(json_extract_string(_sf_e, '$')) <> g._sf_fields._sf_k1))) AS _sf_value`, `where`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id > 1 and v.index > 0`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where v.value::string <> 'x'`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id in (select id from u)`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select m.id + 1 as n, g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where n > 1`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, u, lateral flatten(input => g.value:l) v where u.id = m.id`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v where m.id in (select id from u)`,
			`AS _sf_t FROM tab m) AS m) AS m where m.id in (select id from u)`, ``},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, u, lateral flatten(input => g.value:l) v where u.id = m.id`,
			`AS _sf_t FROM tab m) AS m) AS m, u where u.id = m.id`, ``},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l, outer => true) v where g.value:l is not null`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l[0]) v where g.value:l is not null`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v, lateral flatten(input => v.value) w where g.value:l is not null`,
			`lambda _sf_e: (sf_fields_json(_sf_e._sf_l1, _sf_e._sf_s1, _sf_e._sf_j1) is not null)`, `where`},
		{`select g.index, v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v, lateral flatten(input => v.value) w where m.id > 1`,
			`) AS "g" ON ((m.id > 1)), LATERAL`, `where`},
		{`select g.index, g.value:l from tab m, lateral flatten(input => [object_construct('l', m.a)]) g where m.id > 1`,
			`) AS "g" ON ((m.id > 1))`, `where`},
		{`select g.value:l from tab m, lateral flatten(input => [object_construct('l', m.a)]) g where m.id > 1`,
			`AS _sf_t FROM tab m) AS m where m.id > 1`, `LATERAL`},
		// With no FLATTEN of its keys, any filter leaves the objects
		// structs: a dependent join holds the tables' columns, as for JSON.
		{`select g.index, g.value:l from tab m, lateral flatten(input => [object_construct('l', m.a)]) g where g.value:l is not null`,
			`lambda _sf_e: (sf_fields_json(_sf_e._sf_l1, _sf_e._sf_s1, _sf_e._sf_j1) is not null))) AS _sf_value`, `where`},
		{`select g.value:l from tab m, lateral flatten(input => [object_construct('l', m.a)]) g where g.value:l is not null`,
			`AS _sf_t FROM tab m) AS m where sf_fields_json(m._sf_t._sf_l1, m._sf_t._sf_s1, m._sf_t._sf_j1) is not null`, `LATERAL`},
		{`select g.value:n from tab m, lateral flatten(input => [object_construct('n', m.s)]) g where g.index > 0 and m.id in (select id from u)`,
			`where g.index > 0 and m.id in`, ``},
		{`select g.value:n from tab m, lateral flatten(input => [object_construct('n', m.s)]) g where g.index > 0 and m.id in (select id from u)`,
			`unnest([{'_sf_i'`, ``},
		{`select g.index from tab m join u on g.value:n = u.n, lateral flatten(input => [object_construct('n', m.s)]) g`, `_sf_fields`, ``},
		{`select g.value:n, count(*) from tab m, lateral flatten(input => [object_construct('n', m.s)]) g group by 1 having g.value:n <> 'x'`, `AS _sf_t FROM`, ``},
		{`select v.value from tab m, lateral flatten(input => [object_construct('l', m.a)]) g join lateral flatten(input => g.value:l) v on v.index > 0`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.index, v.value from tab m join u on u.id = m.id, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`_sf_fields`, ``},
		{`select v.value from tab m join u on u.id = m.id, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v`,
			`from (SELECT m.*, unnest(sf_parts_values(`, `LATERAL`},
		{`select g.index, v.value, count(*) from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v group by 1, 2 having count(*) > 1 and v.value <> 'x'`,
			`unnest([coalesce(`, `_sf_fields`},
		{`select g.index, v.value, count(*) from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v group by 1, 2 having count(*) > 1 and sum(m.n) < 9 order by 1`,
			`_sf_fields`, ``},
		{`select v.value, count(*) from tab m, lateral flatten(input => [object_construct('l', m.a)]) g, lateral flatten(input => g.value:l) v group by 1 having count(*) > 1 and v.value <> 'x'`,
			`AS _sf_t FROM`, ``},
		// A WHERE becomes the ON of a last FLATTEN's join, which DuckDB
		// then unnests where it reads the column.
		{`select f.value from t, lateral flatten(input => t.a) f where t.id > 1 and f.index < 2 group by 1`,
			`) AS "f" ON (t.id > 1 and f.index < 2) group by 1`, `where`},
		{`select f.value from t cross join lateral flatten(input => t.a) f where t.id > 1`,
			`from t  join LATERAL (SELECT`, `cross`},
		{`select count(*) from t inner join u on u.id = t.id, lateral flatten(input => t.a) as f(seq, key, path, index, value, this) where u.k = 'x'`,
			`inner join u on u.id = t.id JOIN LATERAL`, `where`},
		{`select v.value from t, lateral flatten(input => t.a) f, lateral flatten(input => f.value) v where t.id > 1`,
			`from t CROSS JOIN LATERAL (SELECT`, `where`},
		{`select v.value from t join u on u.id = t.id, lateral flatten(input => t.a) f, lateral flatten(input => f.value) v where t.id > 1`,
			`from t join u on u.id = t.id CROSS JOIN LATERAL (SELECT`, `where`},
		{`select v.value from t, u join w on w.id = u.id, lateral flatten(input => t.a) f where t.id > 1`, `where t.id > 1`, `ON (`},
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
// JSON VALUE takes (see flattenStructs): with no WHERE, with WHEREs on the
// tables and the objects' keys (see structWhere), and in CTEs that reuse
// the FLATTENs' aliases.
func TestFlattenStructsPlan(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE t AS SELECT * FROM (VALUES (1, ['a', 'b'], ['c']), (2, NULL, [])) v(id, a, b)`); err != nil {
		t.Fatal(err)
	}
	for _, where := range []string{
		"",
		"where m.id > 1",
		"where cast(get_path(g.value, 'kind') as text) <> 'b'",
		"where m.id > 1 and g.value:kind <> 'b' and m.a is not null",
	} {
		res, err := Translate(`select cast(get_path(g.value, 'kind') as text), cast(v.value as text), count(distinct m.id)
			from t as m inner join (select 1 as id union all select 2) as u on u.id = m.id
			cross join lateral flatten(input => [object_construct('kind', 'all', 'items', ['every']), object_construct_keep_null('kind', 'a', 'items', m.a),
				object_construct_keep_null('kind', 'b', 'items', m.b)]) as g(seq, key, path, index, value, this)
			cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)
			`+where+` group by 1, 2`, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if plan := explain(t, conn, res.SQL); strings.Contains(plan, "DELIM_JOIN") {
			t.Errorf("a dependent join:\n%s\n%s", res.SQL, plan)
		}
	}
	res, err := Translate(`select cast(get_path(g.value, 'kind') as text), cast(v.value as text), count(distinct m.id) from t as m
		cross join lateral flatten(input => [object_construct('kind', 'a', 'items', m.a), object_construct('kind', 'b', 'items', m.b)]) as g(seq, key, path, index, value, this)
		cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)
		where not cast(v.value as text) is null and trim(cast(v.value as text)) <> '' and g.value:kind <> 'c' and m.id > 0 group by 1, 2`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "AS _sf_t FROM t as m") {
		t.Errorf("not hoisted:\n%s", res.SQL)
	}
	if plan := explain(t, conn, res.SQL); strings.Contains(plan, "DELIM_JOIN") {
		t.Errorf("a dependent join:\n%s\n%s", res.SQL, plan)
	}
	cte := `select cast(get_path(g.value, 'kind') as text) as kind, cast(v.value as text) as item, m.id from t as m
		cross join lateral flatten(input => [object_construct('kind', 'all', 'items', ['every']), object_construct_keep_null('kind', 'a', 'items', m.a)]) as g(seq, key, path, index, value, this)
		cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)`
	res, err = Translate(`with x as (`+cte+`), y as (`+strings.ReplaceAll(cte, "m.a", "m.b")+`)
		select kind, item, count(distinct id) from (select * from x union all select * from y) group by 1, 2`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(res.SQL, "AS _sf_t FROM") != 2 {
		t.Errorf("not both CTEs' objects are structs:\n%s", res.SQL)
	}
	if plan := explain(t, conn, res.SQL); strings.Contains(plan, "DELIM_JOIN") {
		t.Errorf("a dependent join:\n%s\n%s", res.SQL, plan)
	}
	// An outer query's filter on a key the report groups and ranks by,
	// which DuckDB pushes down onto the FLATTEN, as BI tools filter reports.
	res, err = Translate(`select * from (select kind, item, n, rank from (select kind, item, n,
			row_number() over (partition by kind order by n desc) as rank
			from (select kind, item, count(distinct id) as n from (`+cte+`) as e group by kind, item) as a) as r
			where upper(kind) = 'ALL' or rank <= 10) as "t0"
		where upper("t0"."KIND") <> 'ALL' and "t0"."RANK" <= 3 and "t0"."ITEM" is not null`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if plan := explain(t, conn, res.SQL); strings.Contains(plan, "DELIM_JOIN") {
		t.Errorf("a dependent join:\n%s\n%s", res.SQL, plan)
	}
	// A filter on unnested text, as reports filter their signals' names,
	// leaves a join with a smaller table building its hash table on that
	// table, not on the unnest's rows (see sf_text_read).
	for _, stmt := range []string{
		`CREATE TABLE tb AS SELECT i AS id, ['x' || (i % 7), 'y'] AS a, 'n' || (i % 5) AS s FROM range(1000) r(i)`,
		`CREATE TABLE tc AS SELECT i AS id, 'c' || (i % 3) AS cohort FROM range(500) r(i)`,
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	res, err = Translate(`select cast(get_path(g.value, 'kind') as text), cast(v.value as text), c.cohort, count(distinct m.id)
		from tb as m inner join tc as c on c.id = m.id
		cross join lateral flatten(input => [object_construct('kind', 'a', 'items', m.a), object_construct('kind', 'b', 'items', ['z'])]) as g(seq, key, path, index, value, this)
		cross join lateral flatten(input => get_path(g.value, 'items')) as v(seq, key, path, index, value, this)
		where not cast(v.value as text) is null and trim(cast(v.value as text)) <> '' group by 1, 2, 3`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "sf_parts_strings(") {
		t.Errorf("elements not unnested as text:\n%s", res.SQL)
	}
	if builtOn(t, conn, res.SQL)["tb"] {
		t.Errorf("a join builds its hash table on the objects:\n%s\n%s", res.SQL, explain(t, conn, res.SQL))
	}
	// So does one on a key read only as text, as reports filter their
	// media signals' names.
	res, err = Translate(`select cast(get_path(d.value, 'dim') as text), cast(get_path(d.value, 'name') as text), c.cohort, count(distinct m.id)
		from tb as m inner join tc as c on c.id = m.id
		cross join lateral flatten(input => [object_construct('dim', 'S', 'name', m.s), object_construct('dim', 'T', 'name', 'x')]) as d(seq, key, path, index, value, this)
		where not cast(get_path(d.value, 'name') as text) is null and trim(cast(get_path(d.value, 'name') as text)) <> '' group by 1, 2, 3`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "'_sf_t2'") {
		t.Errorf("the name is not its text alone:\n%s", res.SQL)
	}
	if builtOn(t, conn, res.SQL)["tb"] {
		t.Errorf("a join builds its hash table on the objects:\n%s\n%s", res.SQL, explain(t, conn, res.SQL))
	}
	// And one on a key of string literals in ORs, as reports filter their
	// attribute signals by group, from which DuckDB would derive a condition
	// that joins the table to the range of the objects' indexes and is taken
	// for a third of their rows, fewer than the other table's.
	if _, err := conn.ExecContext(ctx, `CREATE TABLE td AS SELECT i AS id, 'c' || (i % 3) AS cohort FROM range(1500) r(i)`); err != nil {
		t.Fatal(err)
	}
	res, err = Translate(`select cast(get_path(d.value, 'grp') as text), cast(get_path(d.value, 'name') as text), c.cohort, count(distinct m.id)
		from tb as m inner join td as c on c.id = m.id
		cross join lateral flatten(input => [object_construct('grp', 'R', 'name', m.s), object_construct('grp', 'I', 'name', 'x', 'metric', m.id % 3),
			object_construct('grp', 'O', 'name', 'y')]) as d(seq, key, path, index, value, this)
		where cast(get_path(d.value, 'grp') as text) = 'R' or (cast(get_path(d.value, 'grp') as text) = 'I' and cast(get_path(d.value, 'metric') as float) > 0)
		group by 1, 2, 3`, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if plan := explain(t, conn, res.SQL); builtOn(t, conn, res.SQL)["tb"] || strings.Contains(plan, "NL_JOIN") {
		t.Errorf("a join builds its hash table on the objects, or joins them to their indexes' range by a condition:\n%s\n%s", res.SQL, plan)
	}
}

// builtOn returns the tables that the hash joins in DuckDB's plan of a
// statement build their hash tables, on their second children, from.
func builtOn(t *testing.T, conn *sql.Conn, statement string) map[string]bool {
	t.Helper()
	type planNode struct {
		Name      string         `json:"name"`
		ExtraInfo map[string]any `json:"extra_info"`
		Children  []planNode     `json:"children"`
	}
	rows, err := conn.QueryContext(context.Background(), "EXPLAIN (FORMAT JSON) "+statement)
	if err != nil {
		t.Fatalf("%v\n%s", err, statement)
	}
	defer rows.Close()
	var plans []planNode
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatal(err)
		}
		var p []planNode
		if err := json.Unmarshal([]byte(value), &p); err != nil {
			t.Fatalf("%v\n%s", err, value)
		}
		plans = append(plans, p...)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	var scans func(n planNode)
	scans = func(n planNode) {
		if name, ok := n.ExtraInfo["Table"].(string); ok {
			tables[name[strings.LastIndex(name, ".")+1:]] = true // catalog.schema.table
		}
		for _, c := range n.Children {
			scans(c)
		}
	}
	var joins func(n planNode)
	joins = func(n planNode) {
		if n.Name == "HASH_JOIN" && len(n.Children) == 2 {
			scans(n.Children[1])
		}
		for _, c := range n.Children {
			joins(c)
		}
	}
	for _, p := range plans {
		joins(p)
	}
	return tables
}

// explain returns DuckDB's plan of a statement.
func explain(t *testing.T, conn *sql.Conn, statement string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), "EXPLAIN "+statement)
	if err != nil {
		t.Fatalf("%v\n%s", err, statement)
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
	return plan.String()
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
		// A FLATTEN of an earlier one's elements, with a WHERE on the table.
		{`select t.id, f.index, g.value::string from t, lateral flatten(input => [t.a, t.a]) f, lateral flatten(input => f.value) g where t.id <> 2 order by 1, 2, 3`,
			"1|0|a\n1|0|b\n1|1|a\n1|1|b"},
		{`select t.id, v.value::string from t, lateral flatten(input => [object_construct('l', t.a)]) g, lateral flatten(input => g.value:l) v where t.id < 3 order by 1, 2`,
			"1|a\n1|b\n2|c"},
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
