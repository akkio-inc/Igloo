package distributed

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/akkio-inc/igloo/internal/snowflake"
)

// peopleColumns are the field IDs of the people table: a person table
// with lists, maps and structs, as Igloo holds Snowflake's ARRAY
// (VARCHAR[]) and OBJECT (MAP(VARCHAR, VARCHAR)) columns.
var peopleColumns = []Column{
	{Name: "person_id", ID: 1},
	{Name: "segments", ID: 2, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: 10}}}},
	{Name: "attrs", ID: 3, Nested: &Nesting{Kind: "map", Fields: []Field{{Name: "key", ID: 11}, {Name: "value", ID: 12}}}},
	{Name: "home", ID: 4, Nested: &Nesting{Kind: "struct", Fields: []Field{{Name: "city", ID: 13}, {Name: "zip", ID: 14}}}},
	{Name: "visits", ID: 5, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: 15, Nested: &Nesting{Kind: "struct",
		Fields: []Field{{Name: "day", ID: 16}, {Name: "n", ID: 17}}}}}}},
	{Name: "household_id", ID: 6},
}

// addPeople adds the people table, of n people in files files.
func (h *harness) addPeople(n, files int) {
	h.t.Helper()
	h.addNestedTable("people", fmt.Sprintf(`(SELECT i AS person_id,
		CASE WHEN i %% 11 = 0 THEN NULL ELSE ['seg_' || (i %% 13), 'seg_' || (i %% 3), 'seg_' || (i %% 29)][1:(i %% 4)] END AS segments,
		MAP {'gender': CASE WHEN i %% 2 = 0 THEN 'f' ELSE 'm' END, 'age': ((i %% 60) + 18)::VARCHAR} AS attrs,
		{'city': 'city_' || (i %% 17), 'zip': lpad((i %% 1000)::VARCHAR, 5, '0')} AS home,
		[{'day': DATE '2026-01-01' + (i %% 30)::INTEGER, 'n': (i %% 4)::INTEGER}] AS visits,
		(i // 3)::VARCHAR AS household_id
		FROM range(%d) t(i))`, n), files, peopleColumns)
}

// TestNestedColumns checks that workers read lists, maps and structs, with
// their nested field IDs, and statements over them return what one machine
// returns.
func TestNestedColumns(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.addPeople(20000, 6)
	for _, q := range []string{
		`SELECT count(*), count(segments), sum(len(segments)), count(DISTINCT attrs['gender']) FROM people`,
		`SELECT attrs['gender'] AS gender, count(DISTINCT person_id) AS people FROM people GROUP BY 1 ORDER BY 1`,
		`SELECT home.city AS city, count(*) FROM people WHERE list_contains(segments, 'seg_3') GROUP BY 1 ORDER BY 1`,
		`SELECT len(segments) AS n, sum(visits[1].n) AS visits FROM people GROUP BY 1 ORDER BY 1 NULLS FIRST`,
		`SELECT person_id, segments, attrs, home, visits FROM people ORDER BY person_id DESC LIMIT 5`,
		`SELECT DISTINCT segments FROM people ORDER BY 1 NULLS FIRST`,
		`SELECT home, count(*) AS n FROM people GROUP BY home ORDER BY n DESC, home LIMIT 3`,
		`SELECT segments, count(*) AS n FROM people GROUP BY 1 ORDER BY 2 DESC, 1 NULLS FIRST LIMIT 5`,
		`SELECT min(home), max(visits), count(DISTINCT segments) FROM people`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
}

// TestUnnest checks lateral unnests of a split table's lists and maps, in
// DuckDB's forms and as Snowflake's FLATTEN becomes them, which call
// Igloo's sf_ macros: each row's elements are computed from the row alone,
// so they run on workers like a filter.
func TestUnnest(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.addPeople(20000, 6)
	for _, stmt := range snowflake.Macros() {
		h.exec(stmt)
	}
	queries := []string{
		`SELECT seg, count(DISTINCT person_id) AS people FROM people, unnest(segments) AS u(seg) GROUP BY seg ORDER BY seg`,
		`SELECT count(*) FROM people CROSS JOIN LATERAL (SELECT unnest(segments) AS seg) s WHERE s.seg <> 'seg_0'`,
		`SELECT s.seg, count(*) AS n FROM people p JOIN LATERAL (SELECT unnest(p.segments) AS seg) s ON s.seg LIKE 'seg_1%' GROUP BY 1 ORDER BY 1`,
		`SELECT k, count(*) AS n FROM people, LATERAL (SELECT unnest(map_keys(attrs)) AS k) GROUP BY 1 ORDER BY 1`,
		`SELECT j.key, j.value, count(DISTINCT person_id) AS n FROM people, json_each(to_json(attrs)) AS j GROUP BY 1, 2 ORDER BY 1, 2`,
		`SELECT count(*) FROM people p WHERE EXISTS (SELECT 1 FROM unnest(p.segments) AS u(s) WHERE s = 'seg_2')`,
		`SELECT seg, count(*) FROM (SELECT person_id, unnest(segments) AS seg FROM people) GROUP BY 1 ORDER BY 1`,
		`SELECT DISTINCT unnest(segments) AS seg FROM people ORDER BY 1`,
		`SELECT home.city, v.day, sum(v.n) AS n FROM people, unnest(visits) AS u(v) GROUP BY ALL ORDER BY ALL LIMIT 20`,
		`SELECT list_transform(segments, lambda x: upper(x)) AS up, count(*) AS n FROM people GROUP BY 1 ORDER BY 2 DESC, 1 NULLS FIRST LIMIT 5`,
		`SELECT count(*) FROM people WHERE len(list_filter(segments, lambda x: x LIKE 'seg_1%')) > 0`,
		// FLATTENs of lists of structs as Snowflake's translation writes
		// them: unnest in the select list of a subquery over the table, a
		// second one over the first's items, and a common table expression
		// read once as its query beneath them.
		`SELECT x.item, count(DISTINCT x.person_id) AS n FROM (SELECT m.*, unnest(m._sf_t.items) AS item
			FROM (SELECT p.*, unnest(list_transform(p.visits, lambda v: {'day': v.day, 'items': [v.n, v.n + 1]})) AS _sf_t FROM people AS p) AS m) AS x
			GROUP BY 1 ORDER BY 1`,
		`WITH base AS (SELECT person_id, visits FROM people WHERE person_id % 3 <> 0),
			cohorts AS (SELECT DISTINCT person_id, CASE WHEN person_id % 2 = 0 THEN 'even' ELSE 'odd' END AS cohort FROM people)
			SELECT m._sf_t.day AS day, c.cohort, count(DISTINCT m.person_id) AS n
			FROM (SELECT m.*, unnest(list_transform(m.visits, lambda v: {'day': v.day, 'items': [v.n]})) AS _sf_t FROM base AS m) AS m
			JOIN cohorts AS c ON c.person_id = m.person_id GROUP BY 1, 2 ORDER BY 1, 2`,
		// Series of integer literals are the same rows on every worker. An
		// object of a few keys flattens as a CASE per row of range(n),
		// which DuckDB estimates at n times the table's rows.
		`SELECT count(*), sum(i) FROM people, range(3) AS r(i)`,
		`SELECT g, count(DISTINCT person_id) AS n FROM people CROSS JOIN generate_series(1, 3) AS s(g) WHERE person_id % g = 0 GROUP BY 1 ORDER BY 1`,
		`WITH base AS (SELECT person_id, home, attrs FROM people WHERE person_id % 3 <> 0),
			cohorts AS (SELECT DISTINCT person_id, CASE WHEN person_id % 2 = 0 THEN 'even' ELSE 'odd' END AS cohort FROM people)
			SELECT m._sf_t.key AS k, m._sf_t.value AS v, c.cohort, count(DISTINCT m.person_id) AS n
			FROM (SELECT b.*, CASE k.i WHEN 0 THEN {'key': 'city', 'value': b.home.city} WHEN 1 THEN {'key': 'gender', 'value': b.attrs['gender']} END AS _sf_t
				FROM base AS b CROSS JOIN range(2) AS k(i)) AS m
			JOIN cohorts AS c ON c.person_id = m.person_id GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
	}
	for _, q := range []string{
		`SELECT f.value::string AS seg, COUNT(DISTINCT p.person_id) AS people FROM people p, LATERAL FLATTEN(input => p.segments) f GROUP BY 1 ORDER BY 1`,
		`SELECT f.key AS k, f.value::string AS v, COUNT(DISTINCT p.person_id) AS n FROM people p, TABLE(FLATTEN(p.attrs)) f GROUP BY 1, 2 ORDER BY 1, 2`,
		`SELECT COUNT(DISTINCT p.person_id) AS n FROM people p, LATERAL FLATTEN(input => p.segments) f WHERE f.value::string IN ('seg_1', 'seg_2')`,
		`SELECT f.index AS i, f.this IS NULL AS t, COUNT(*) AS n FROM people p, LATERAL FLATTEN(input => p.segments, outer => true) f GROUP BY 1, 2 ORDER BY 1, 2`,
		`SELECT v.value:n::int AS n, COUNT(*) AS c FROM people p, LATERAL FLATTEN(input => p.visits) v GROUP BY 1 ORDER BY 1`,
		`SELECT p.home:city::string AS city, f.value::string AS seg, COUNT(*) AS n FROM people p, LATERAL FLATTEN(p.segments) f
			WHERE p.attrs:gender::string = 'f' GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 10`,
		`SELECT TRY_TO_NUMBER(p.household_id) % 3 AS h, COUNT(*) AS n FROM people p GROUP BY 1 ORDER BY 1`,
		// Objects built with literal keys flatten as structs, through lambdas.
		`SELECT f.value:k::string AS k, COUNT(DISTINCT p.person_id) AS n FROM people p, LATERAL FLATTEN(input =>
			ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('k', p.home:city, 'v', p.person_id), OBJECT_CONSTRUCT('k', 'x', 'v', 1))) f GROUP BY 1 ORDER BY 1`,
		// As reports' signals flatten them: objects of a row's columns, built
		// over the table joined to the range of their indexes, joined to
		// cohorts, and a FLATTEN of a key of them unnested around that, both
		// filtered on their text.
		`SELECT CAST(GET_PATH(g.value, 'k') AS TEXT) AS k, CAST(v.value AS TEXT) AS v, c.c, COUNT(DISTINCT p.person_id) AS n
			FROM people AS p INNER JOIN (SELECT DISTINCT person_id, person_id % 2 AS c FROM people) AS c ON c.person_id = p.person_id
			CROSS JOIN LATERAL FLATTEN(input => [OBJECT_CONSTRUCT('k', 'segments', 'items', p.segments), OBJECT_CONSTRUCT('k', 'all', 'items', ['every'])]) AS g(seq, key, path, index, value, this)
			CROSS JOIN LATERAL FLATTEN(input => GET_PATH(g.value, 'items')) AS v(seq, key, path, index, value, this)
			WHERE NOT CAST(v.value AS TEXT) IS NULL AND TRIM(CAST(v.value AS TEXT)) <> '' GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`,
		`SELECT CAST(GET_PATH(d.value, 'dim') AS TEXT) AS dim, CAST(GET_PATH(d.value, 'name') AS TEXT) AS name, COUNT(DISTINCT p.person_id) AS n
			FROM people AS p CROSS JOIN LATERAL FLATTEN(input => [OBJECT_CONSTRUCT('dim', 'city', 'name', p.home:city::string),
				OBJECT_CONSTRUCT('dim', 'gender', 'name', p.attrs:gender::string), OBJECT_CONSTRUCT('dim', 'all', 'name', 'any')]) AS d(seq, key, path, index, value, this)
			WHERE NOT CAST(GET_PATH(d.value, 'name') AS TEXT) IS NULL AND TRIM(CAST(GET_PATH(d.value, 'name') AS TEXT)) <> '' GROUP BY 1, 2 ORDER BY 1, 2`,
		`SELECT ARRAY_TO_STRING(p.segments, ',') AS s, COUNT(*) AS n FROM people p GROUP BY 1 ORDER BY 1 NULLS FIRST`,
	} {
		r, err := snowflake.Translate(q, snowflake.Options{})
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		queries = append(queries, r.SQL)
	}
	for _, q := range queries {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t
	for _, q := range []string{
		// Computed once on one machine, and once per worker.
		`SELECT count(*) FROM people, (SELECT random() < 2 AS r) x WHERE x.r`,
		// Series too large, or not of literals.
		`SELECT count(*) FROM people, range(1001)`,
		`SELECT count(*) FROM people, range((SELECT 3))`,
		`SELECT count(*) FROM people, range(0, 3, 0)`,
		`SELECT count(*) FROM people, range(TIMESTAMP '2026-01-01', TIMESTAMP '2026-01-03', INTERVAL 1 DAY)`,
		`SELECT count(*) FROM people, LATERAL (SELECT * FROM people AS q WHERE q.person_id = people.person_id)`,
		`SELECT count(*) FROM people, unnest((SELECT list(person_id) FROM people LIMIT 1))`,
		// A lambda parameter named like a select alias, which planning would
		// put in place of the parameter in GROUP BY.
		`SELECT list_transform(segments, lambda seg: upper(seg)) AS x, count(*) AS seg FROM people GROUP BY 1`,
		`SELECT list_transform(segments, lambda current_date: upper(current_date)) AS x, count(*) FROM people GROUP BY 1`,
	} {
		t.Run("local "+short(q), func(t *testing.T) {
			if _, _, _, _, err := h.run(t.Context(), q, ModeOn); !IsUnsupported(err) {
				t.Errorf("want it to run on one machine, got %v", err)
			}
		})
	}
}

// TestIcebergNestedTypes checks that nested Iceberg types are spelled as
// DuckDB spells them, field IDs and all.
func TestIcebergNestedTypes(t *testing.T) {
	p, err := newParser()
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	for _, tc := range []struct {
		typ  iceberg.Type
		want string
	}{
		{&iceberg.ListType{ElementID: 2, Element: iceberg.PrimitiveTypes.String}, "VARCHAR[]"},
		{&iceberg.MapType{KeyID: 2, KeyType: iceberg.PrimitiveTypes.String, ValueID: 3, ValueType: iceberg.PrimitiveTypes.String},
			"MAP(VARCHAR, VARCHAR)"},
		{&iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "Zip", Type: iceberg.PrimitiveTypes.Int32},
			{ID: 3, Name: "type", Type: iceberg.PrimitiveTypes.String},
			{ID: 4, Name: "a b", Type: &iceberg.ListType{ElementID: 5, Element: iceberg.DecimalTypeOf(10, 2)}},
		}}, `STRUCT(Zip INTEGER, "type" VARCHAR, "a b" DECIMAL(10,2)[])`},
		{&iceberg.ListType{ElementID: 2, Element: &iceberg.MapType{KeyID: 3, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 4, ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 5, Name: "x", Type: iceberg.PrimitiveTypes.Int64}}}}},
			"MAP(VARCHAR, STRUCT(x BIGINT))[]"},
	} {
		c, ok := icebergColumn("c", 1, tc.typ)
		if !ok {
			t.Errorf("%s: not supported", tc.typ)
			continue
		}
		got, err := p.typeName(c.Type)
		if err != nil || got != tc.want {
			t.Errorf("%s: %q (%q), %v; want %q", tc.typ, got, c.Type, err, tc.want)
		}
	}
	c, _ := icebergColumn("c", 1, &iceberg.ListType{ElementID: 2, Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 3, Name: "x", Type: iceberg.PrimitiveTypes.Int64}}}})
	want := Column{Name: "c", ID: 1, Type: "STRUCT(\"x\" BIGINT)[]", Nested: &Nesting{Kind: "list", Fields: []Field{
		{Name: "element", ID: 2, Nested: &Nesting{Kind: "struct", Fields: []Field{{Name: "x", ID: 3}}}}}}}
	if !reflect.DeepEqual(c, want) {
		t.Errorf("got %+v, want %+v", c, want)
	}
	if _, ok := icebergColumn("c", 1, &iceberg.ListType{ElementID: 2, Element: iceberg.PrimitiveTypes.TimestampNs}); ok {
		t.Error("a list of a type workers cannot read")
	}
}

// TestNestedEvolution checks files written before a nested column
// changed: a struct field added since reads as NULL, and a field renamed,
// or dropped and added again, keeps the statement on the coordinator, as
// does a file without nested field IDs, since DuckDB reads nested fields
// by name.
func TestNestedEvolution(t *testing.T) {
	h := newHarness(t, 0.01, 2, 2)
	dir := filepath.Join(h.dir, "warehouse", "nested")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, query, ids string) string {
		path := filepath.Join(dir, name+".parquet")
		h.exec(fmt.Sprintf("COPY (%s) TO %s (FORMAT parquet, FIELD_IDS {%s})", query, quoteLiteral(path), ids))
		return path
	}
	// The table: k, s STRUCT(a, b, c) with c added last, tags VARCHAR[].
	cols := []Column{
		{Name: "k", Type: "BIGINT", ID: 1},
		{Name: "s", Type: "STRUCT(a BIGINT, b VARCHAR, c INTEGER)", ID: 2, Nested: &Nesting{Kind: "struct",
			Fields: []Field{{Name: "a", ID: 3}, {Name: "b", ID: 4}, {Name: "c", ID: 5}}}},
		{Name: "tags", Type: "VARCHAR[]", ID: 6, Nested: &Nesting{Kind: "list", Fields: []Field{{Name: "element", ID: 7}}}},
	}
	before := write("before", `SELECT i AS k, {'a': i, 'b': 'b' || i} AS s, ['x' || (i % 3)] AS tags FROM range(1000) t(i)`,
		"k: 1, s: {__duckdb_field_id: 2, a: 3, b: 4}, tags: {__duckdb_field_id: 6, element: 7}")
	after := write("after", `SELECT i AS k, {'a': i, 'b': 'b' || i, 'c': (i % 5)::INTEGER} AS s, ['y' || (i % 2), 'z'] AS tags FROM range(1000, 2500) t(i)`,
		"k: 1, s: {__duckdb_field_id: 2, a: 3, b: 4, c: 5}, tags: {__duckdb_field_id: 6, element: 7}")
	renamed := write("renamed", `SELECT i AS k, {'old_a': i, 'b': 'b' || i, 'c': 1::INTEGER} AS s, ['r'] AS tags FROM range(10) t(i)`,
		"k: 1, s: {__duckdb_field_id: 2, old_a: 3, b: 4, c: 5}, tags: {__duckdb_field_id: 6, element: 7}")
	readded := write("readded", `SELECT i AS k, {'a': i, 'b': 'b' || i, 'c': 1::INTEGER} AS s, ['r'] AS tags FROM range(10) t(i)`,
		"k: 1, s: {__duckdb_field_id: 2, a: 3, b: 4, c: 9}, tags: {__duckdb_field_id: 6, element: 7}")
	element := write("element", `SELECT i AS k, {'a': i, 'b': 'b' || i, 'c': 1::INTEGER} AS s, ['e'] AS tags FROM range(10) t(i)`,
		"k: 1, s: {__duckdb_field_id: 2, a: 3, b: 4, c: 5}, tags: {__duckdb_field_id: 6, element: 8}")
	noIDs := write("no_ids", `SELECT i AS k, {'a': i, 'b': 'b' || i, 'c': 1::INTEGER} AS s, ['n'] AS tags FROM range(10) t(i)`,
		"k: 1, s: 2, tags: 6")
	notStruct := write("not_struct", `SELECT i AS k, 'text' AS s, ['n'] AS tags FROM range(10) t(i)`,
		"k: 1, s: 2, tags: {__duckdb_field_id: 6, element: 7}")

	tbl := &Table{Schema: "main", Name: "nested", Columns: cols}
	var fs []File
	for _, p := range []string{before, after} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		fs = append(fs, File{Path: p, Size: info.Size()})
		tbl.Bytes += info.Size()
	}
	h.tables["nested"], h.files[tbl.key()] = tbl, fs
	h.exec(fmt.Sprintf(`CREATE OR REPLACE VIEW main.nested AS
		SELECT k, {'a': s.a, 'b': s.b, 'c': CAST(NULL AS INTEGER)} AS s, tags FROM read_parquet(%s)
		UNION ALL SELECT k, s, tags FROM read_parquet(%s)`, quoteLiteral(before), quoteLiteral(after)))

	w := h.workers[0]
	conn, err := w.opts.Engine.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		name string
		file string
		ok   bool
	}{
		{"written before a field was added", before, true},
		{"written with every field", after, true},
		{"a field renamed since", renamed, false},
		{"a field dropped and added again", readded, false},
		{"another element field ID", element, false},
		{"no nested field IDs", noIDs, false},
		{"text where the table has a struct", notStruct, false},
	} {
		byName, err := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: cols, Files: []string{before, tc.file}})
		switch {
		case byName:
			t.Errorf("%s: read by name", tc.name)
		case tc.ok && err != nil:
			t.Errorf("%s: %v", tc.name, err)
		case !tc.ok && !IsUnsupported(err):
			t.Errorf("%s: want it refused, got %v", tc.name, err)
		}
	}

	// A nested column whose nesting the task does not give.
	bare := slices.Clone(cols)
	bare[2].Nested = nil
	if _, err := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: bare, Files: []string{after}}); !IsUnsupported(err) {
		t.Errorf("a nested column without its nesting: %v", err)
	}

	for _, q := range []string{
		`SELECT count(*), count(s.c), sum(s.a), min(s.b), max(s.b), sum(s.c) FROM nested`,
		`SELECT s.c, tags, count(*) FROM nested GROUP BY ALL ORDER BY ALL`,
		`SELECT k, s, tags FROM nested WHERE k % 100 = 7 ORDER BY k LIMIT 50`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t

	// A file the workers cannot map keeps the statement on the coordinator.
	info, err := os.Stat(renamed)
	if err != nil {
		t.Fatal(err)
	}
	h.files[tbl.key()] = append(fs, File{Path: renamed, Size: info.Size()})
	if _, _, _, _, err := h.run(t.Context(), `SELECT count(*), sum(s.a) FROM nested`, ModeOn); !IsUnsupported(err) ||
		!strings.Contains(err.Error(), "nests column s") {
		t.Errorf("a file with a renamed struct field: %v", err)
	}
}
