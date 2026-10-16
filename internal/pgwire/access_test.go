package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestDMLAsSelect(t *testing.T) {
	cases := []struct {
		t     duckdb.StmtType
		query string
		want  string
	}{
		{duckdb.STATEMENT_TYPE_DELETE, "DELETE FROM t WHERE id IN (SELECT id FROM s)",
			"SELECT 1 FROM t WHERE id IN (SELECT id FROM s)"},
		{duckdb.STATEMENT_TYPE_DELETE, "delete from t as x using s, u where x.id = s.id;",
			"SELECT 1 FROM t as x, s, u WHERE x.id = s.id"},
		{duckdb.STATEMENT_TYPE_DELETE, "TRUNCATE TABLE lake.public.t", "SELECT 1 FROM lake.public.t"},
		{duckdb.STATEMENT_TYPE_DELETE, "TRUNCATE t", "SELECT 1 FROM t"},
		{duckdb.STATEMENT_TYPE_UPDATE, "UPDATE t SET a = (SELECT max(b) FROM s), c = 'where' WHERE d > 1",
			"SELECT (SELECT max(b) FROM s), 'where' FROM t WHERE d > 1"},
		{duckdb.STATEMENT_TYPE_UPDATE, "UPDATE t AS x SET a = s.b FROM s WHERE x.id = s.id",
			"SELECT s.b FROM t AS x, s WHERE x.id = s.id"},
		{duckdb.STATEMENT_TYPE_UPDATE, "UPDATE t SET (a, b) = (1, 2)", "SELECT (1, 2) FROM t"},
		{statementTypeMergeInto, "MERGE INTO t USING (SELECT * FROM s) src ON t.id = src.id WHEN MATCHED THEN DELETE",
			"SELECT 1 FROM t JOIN (SELECT * FROM s) src ON t.id = src.id"},
		{duckdb.STATEMENT_TYPE_UPDATE, "UPDATE t", ""},
	}
	for _, c := range cases {
		if got := dmlAsSelect(c.t, c.query); got != c.want {
			t.Errorf("dmlAsSelect(%q)\n got %q\nwant %q", c.query, got, c.want)
		}
	}
}

func TestInsertSource(t *testing.T) {
	cases := map[string]string{
		"INSERT INTO t VALUES (1, 'a')":                          "VALUES (1, 'a')",
		"INSERT INTO lake.public.t (a, b) SELECT * FROM s;":      "SELECT * FROM s",
		"INSERT OR REPLACE INTO t BY NAME FROM s":                "FROM s",
		"insert into t (select * from s) returning id":           "(select * from s)",
		"INSERT INTO t AS x DEFAULT VALUES":                      "",
		"INSERT INTO t SELECT 1 ON CONFLICT DO NOTHING":          "SELECT 1",
		"INSERT INTO \"T\" WITH c AS (SELECT 1) SELECT * FROM c": "WITH c AS (SELECT 1) SELECT * FROM c",
	}
	for in, want := range cases {
		if got := insertSource(in); got != want {
			t.Errorf("insertSource(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseCreateTable(t *testing.T) {
	c, ok := parseCreateTable("CREATE OR REPLACE TABLE lake.s.t AS (SELECT 1) WITH NO DATA")
	if !ok || !c.replace || c.temporary || !slices.Equal(c.name, []string{"lake", "s", "t"}) || c.query != "(SELECT 1)" {
		t.Errorf("CTAS: %+v %v", c, ok)
	}
	c, ok = parseCreateTable("create temp table if not exists x (i int)")
	if !ok || !c.temporary || c.query != "" || !slices.Equal(c.name, []string{"x"}) {
		t.Errorf("temp: %+v %v", c, ok)
	}
	if _, ok := parseCreateTable("CREATE VIEW v AS SELECT 1"); ok {
		t.Error("a view is not a table")
	}
}

func TestTopLevelKeyword(t *testing.T) {
	cases := []struct {
		query string
		words []string
		want  int
	}{
		{"a = (SELECT 1 FROM x) FROM y", []string{"FROM"}, 22},
		{"'from' FROM y", []string{"FROM"}, 7},
		{"fromage from y", []string{"FROM"}, 8},
		{"x ON /* c */ CONFLICT", []string{"ON", "CONFLICT"}, 2},
		{"x ON z", []string{"ON", "CONFLICT"}, -1},
	}
	for _, c := range cases {
		if got := topLevelKeyword(c.query, c.words...); got != c.want {
			t.Errorf("topLevelKeyword(%q, %v) = %d, want %d", c.query, c.words, got, c.want)
		}
	}
}

func TestSplitQualified(t *testing.T) {
	cases := map[string][]string{
		`"temp".main.t`:     {"temp", "main", "t"},
		`lake."My ""S""".t`: {"lake", `My "S"`, "t"},
		`memory.main.mt`:    {"memory", "main", "mt"},
		`"a"b`:              nil,
	}
	for in, want := range cases {
		if got := splitQualified(in); !reflect.DeepEqual(got, want) {
			t.Errorf("splitQualified(%q) = %q, want %q", in, got, want)
		}
	}
	if got := splitTopLevelQuoted(`lake.public,"a,b".x`, ','); !slices.Equal(got, []string{"lake.public", `"a,b".x`}) {
		t.Errorf("splitTopLevelQuoted: %q", got)
	}
}

func TestViewBodyAndExplained(t *testing.T) {
	if got := viewBody("CREATE TEMP VIEW tv (a) AS SELECT id FROM t1;"); got != "SELECT id FROM t1" {
		t.Errorf("temp view body: %q", got)
	}
	if got := viewBody("CREATE VIEW public.v1 AS SELECT t1.id FROM lake.public.t1;"); got != "SELECT t1.id FROM lake.public.t1" {
		t.Errorf("view body: %q", got)
	}
	if got := explainedStatement("EXPLAIN (FORMAT json) SELECT 1"); got != "SELECT 1" {
		t.Errorf("explained: %q", got)
	}
	if !selectLike("  (SELECT 1)") || !selectLike("FROM t") || selectLike("INSERT INTO t VALUES (1)") {
		t.Error("selectLike")
	}
}

func TestMergeActions(t *testing.T) {
	got := mergeActions("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1 WHEN NOT MATCHED THEN INSERT VALUES (1)")
	if !slices.Equal(got, []auth.Privilege{auth.PrivilegeUpdate, auth.PrivilegeInsert}) {
		t.Errorf("merge: %v", got)
	}
	if got := mergeActions("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DO NOTHING"); len(got) != 3 {
		t.Errorf("unknown actions need everything: %v", got)
	}
}

func TestGrantable(t *testing.T) {
	cases := []struct {
		t     duckdb.StmtType
		query string
		want  bool
	}{
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM t", true},
		{duckdb.STATEMENT_TYPE_INSERT, "INSERT INTO t VALUES (1)", true},
		{statementTypeMergeInto, "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE", true},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM set_iceberg_table_properties(t, {'a': 'b'})", false},
		{duckdb.STATEMENT_TYPE_SELECT, "SELECT * FROM duckdb_secrets()", false},
		{duckdb.STATEMENT_TYPE_SET, "SET threads = 1", false},
		{duckdb.STATEMENT_TYPE_ATTACH, "ATTACH 'x'", false},
		{duckdb.STATEMENT_TYPE_EXPLAIN, "EXPLAIN ANALYZE SELECT 1", false},
	}
	for _, c := range cases {
		if got := grantable(c.t, c.query); got != c.want {
			t.Errorf("grantable(%q) = %v, want %v", c.query, got, c.want)
		}
	}
}

// TestASTWalk checks which table references the walker takes from
// DuckDB's own parse of queries, and that it follows CTEs as DuckDB plans
// them: each used one once, unused ones not at all.
func TestASTWalk(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)

	parse := func(query string) any {
		t.Helper()
		rows, err := conn.QueryContext(context.Background(),
			"SELECT json_serialize_sql($1::VARCHAR, skip_null := true, skip_empty := true)::VARCHAR",
			[]driver.NamedValue{{Ordinal: 1, Value: query}})
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close() //nolint:errcheck
		dest := make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Fatal(err)
		}
		var doc struct{ Statements []any }
		if err := json.Unmarshal([]byte(dest[0].(string)), &doc); err != nil || len(doc.Statements) != 1 {
			t.Fatalf("%s: %v %s", query, err, dest[0])
		}
		return doc.Statements[0]
	}

	cases := []struct {
		query     string
		refs      []string
		functions []string
	}{
		{"SELECT * FROM a JOIN s.b USING (id) WHERE x IN (SELECT x FROM c.s.d)", []string{"a", "c.s.d", "s.b"}, nil},
		{"WITH unused AS (SELECT * FROM a) SELECT 1", nil, nil},
		{"WITH c AS (SELECT * FROM a) SELECT * FROM c x, c y", []string{"a"}, nil},
		{"WITH c AS (SELECT * FROM a), d AS (SELECT * FROM c) SELECT * FROM d", []string{"a"}, nil},
		{"WITH a AS (SELECT * FROM a) SELECT * FROM main.a, a", []string{"a", "main.a"}, nil},
		{"WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r, b) SELECT * FROM r", []string{"b"}, nil},
		{"SELECT (WITH c AS (SELECT * FROM a) SELECT count(*) FROM c), (SELECT 1 FROM c)", []string{"a", "c"}, nil},
		{"DESCRIBE secret", nil, nil},
		{"SUMMARIZE a", []string{"a"}, nil},
		{"SELECT * FROM range(3), read_parquet('x') WHERE f(1) > 0", nil, []string{"range", "read_parquet"}},
		{"FROM 'file.parquet'", []string{"file.parquet"}, nil},
		{"PIVOT a ON k IN ('x') USING sum(v)", []string{"a"}, nil},
	}
	for _, c := range cases {
		w := &astWalk{}
		w.walk(parse(c.query), nil)
		var refs, functions []string
		for _, r := range w.refs {
			refs = append(refs, strings.Join(r.parts, "."))
		}
		for _, f := range w.tableFunctions {
			functions = append(functions, f.name)
		}
		slices.Sort(refs)
		slices.Sort(functions)
		if !slices.Equal(refs, c.refs) || !slices.Equal(functions, c.functions) {
			t.Errorf("%s:\n refs %q, want %q\n table functions %q, want %q", c.query, refs, c.refs, functions, c.functions)
		}
	}
}
