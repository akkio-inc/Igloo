package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// runCase is a Snowflake statement and what Snowflake returns for it.
type runCase struct {
	name string
	sql  string
	// want is the result, one string per row, values separated by "|", as
	// format writes them; wantErr, if set, is part of the error instead.
	want    []string
	wantErr string
	// types are the DuckDB types of the result's columns, when they matter.
	types []string
	// cols are the result's column names as ColumnName presents them.
	cols []string
	// implicit translates with Options.Implicit.
	implicit bool
}

// session opens a DuckDB connection with the macros defined.
func session(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := conn.ExecContext(ctx, "SET TimeZone = 'UTC'"); err != nil {
		t.Fatal(err)
	}
	for _, m := range Macros() {
		if _, err := conn.ExecContext(ctx, m); err != nil {
			t.Fatalf("%v\n%s", err, m)
		}
	}
	return conn
}

// format writes a value as the tests expect it. duckdb-go decodes JSON
// values, a JSON null to nil, so JSON is written as JSON again.
func format(v any, typ string) string {
	if typ == "JSON" && v != nil {
		b, err := json.Marshal(v)
		if err == nil {
			return string(b)
		}
	}
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case []byte:
		return fmt.Sprintf("\\x%x", v)
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case *big.Int:
		return v.String()
	case duckdb.Decimal:
		s := new(big.Int).Abs(v.Value).String()
		if v.Scale > 0 {
			for len(s) <= int(v.Scale) {
				s = "0" + s
			}
			s = s[:len(s)-int(v.Scale)] + "." + s[len(s)-int(v.Scale):]
		}
		if v.Value.Sign() < 0 {
			s = "-" + s
		}
		return s
	case duckdb.Interval:
		return fmt.Sprintf("%d months %d days %d us", v.Months, v.Days, v.Micros)
	case time.Time:
		switch {
		case typ == "DATE":
			return v.Format("2006-01-02")
		case typ == "TIME":
			return v.Format("15:04:05.999999")
		case strings.HasPrefix(typ, "TIMESTAMP WITH TIME ZONE") || typ == "TIMESTAMPTZ":
			return v.UTC().Format("2006-01-02 15:04:05.999999Z")
		}
		return v.Format("2006-01-02 15:04:05.999999")
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// run translates and runs c.sql, comparing the result with c's.
func run(t *testing.T, conn *sql.Conn, c runCase) {
	t.Helper()
	ctx := context.Background()
	res, err := Translate(c.sql, Options{IsSetting: func(n string) bool { return n == "timezone" }, Implicit: c.implicit})
	if err != nil {
		if c.wantErr != "" && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("translate %s: %v", c.sql, err)
	}
	rows, err := conn.QueryContext(ctx, res.SQL)
	if err != nil {
		if c.wantErr != "" && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, c.sql, res.SQL)
	}
	defer rows.Close()
	if c.wantErr != "" {
		for rows.Next() {
		}
		if err := rows.Err(); err != nil && strings.Contains(err.Error(), c.wantErr) {
			return
		}
		t.Fatalf("want an error with %q\nDuckDB: %s", c.wantErr, res.SQL)
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	names, _ := rows.Columns()
	var got []string
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
		got = append(got, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%v\nSnowflake: %s\nDuckDB:    %s", err, c.sql, res.SQL)
	}
	if strings.Join(got, "\n") != strings.Join(c.want, "\n") {
		t.Errorf("got\n  %s\nwant\n  %s\nSnowflake: %s\nDuckDB:    %s", strings.Join(got, "\n  "),
			strings.Join(c.want, "\n  "), c.sql, res.SQL)
	}
	for i, typ := range c.types {
		if i < len(types) && typ != "" && types[i].DatabaseTypeName() != typ {
			t.Errorf("column %d is %s, want %s\nDuckDB: %s", i+1, types[i].DatabaseTypeName(), typ, res.SQL)
		}
	}
	for i, name := range c.cols {
		if i < len(names) && ColumnName(names[i], res.Quoted) != name {
			t.Errorf("column %d is named %q, want %q\nDuckDB: %s", i+1, ColumnName(names[i], res.Quoted), name, res.SQL)
		}
	}
}

var _ driver.Value // keep database/sql/driver for Scan helpers

// TestAgainstSnowflake runs translated statements on DuckDB and compares
// their results with what Snowflake returned for the same statements
// (recorded from Snowflake; see the comments).
func TestAgainstSnowflake(t *testing.T) {
	conn := session(t)
	for _, c := range snowflakeCases {
		t.Run(c.name, func(t *testing.T) { run(t, conn, c) })
	}
}

// TestWideDecimalCasts checks that casts of text to NUMBER of more than 18
// digits, which go through DECIMAL(18,s) first (see decimalCast), give
// exactly what DuckDB's own cast to the wide DECIMAL gives, value, type and
// error alike, for text that fits 18 digits and text that does not, and
// that other types cast as before.
func TestWideDecimalCasts(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE wide_inputs AS SELECT * FROM (VALUES ('0'), ('42'), (' 42 '), ('-7.5'),
		('1.5'), ('2.5'), ('0.125'), ('999999999999999999'), ('-999999999999999999'), ('1000000000000000000'),
		('12345678901234567890123'), ('99999999999999999999999999999999999999'), ('1e3'), ('1.5e2'), ('+5'), ('.5'),
		('0001'), ('abc'), (''), (NULL)) t(s)`); err != nil {
		t.Fatal(err)
	}
	query := func(q string) (string, error) {
		rows, err := conn.QueryContext(ctx, q)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		types, _ := rows.ColumnTypes()
		var out []string
		for rows.Next() {
			var s, v any
			if err := rows.Scan(&s, &v); err != nil {
				return "", err
			}
			out = append(out, fmt.Sprintf("%v=%v", s, v))
		}
		return types[1].DatabaseTypeName() + " " + strings.Join(out, ","), rows.Err()
	}
	for _, c := range []struct{ snowflake, duckdb string }{
		{"try_cast(s as number)", "TRY_CAST(s AS DECIMAL(38,0))"},
		{"try_to_number(s)", "TRY_CAST(s AS DECIMAL(38,0))"},
		{"try_to_number(s, 38, 2)", "TRY_CAST(s AS DECIMAL(38,2))"},
		{"try_to_decimal(s, 20, 5)", "TRY_CAST(s AS DECIMAL(20,5))"},
		{"try_cast(s as number(38,18))", "TRY_CAST(s AS DECIMAL(38,18))"},
		{"try_cast(length(s) as number)", "TRY_CAST(length(s) AS DECIMAL(38,0))"},
		{"to_number(s)", "CAST(s AS DECIMAL(38,0))"},
		{"s::number(30,3)", "CAST(s AS DECIMAL(30,3))"},
	} {
		for _, where := range []string{"true", "s IS NULL OR TRY_CAST(s AS DECIMAL(38,18)) IS NOT NULL"} {
			res, err := Translate("select s, "+c.snowflake+" from wide_inputs where "+where+" order by s nulls last", Options{})
			if err != nil {
				t.Fatal(err)
			}
			got, gotErr := query(res.SQL)
			want, wantErr := query("select s, " + c.duckdb + " from wide_inputs where " + where + " order by s nulls last")
			if got != want || (gotErr == nil) != (wantErr == nil) {
				t.Errorf("%s where %s:\n got %s (%v)\nwant %s (%v)\nDuckDB: %s", c.snowflake, where, got, gotErr, want, wantErr, res.SQL)
			}
			if !strings.Contains(res.SQL, "DECIMAL(18,") {
				t.Errorf("%s: no narrow cast first: %s", c.snowflake, res.SQL)
			}
		}
	}
}

// TestFlattenMatchesJSONEach checks that FLATTEN, which unnests lists of a
// document's elements (see flattenUnnest), gives the rows DuckDB's json_each
// gives, as the translation did before: keys, paths, indexes, values (JSON
// nulls staying JSON) and THIS, each document's in its order, for arrays,
// objects, nested and empty ones, scalars, NULL and JSON in text, with each
// MODE and with a PATH.
func TestFlattenMatchesJSONEach(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE docs AS SELECT * FROM (VALUES
		(1, '[1, null, "x", {"k": [2, 3]}, [4, null]]'), (2, '{"b": 1, "a": null, "c d": {"e": [5]}, "": "empty key"}'),
		(3, '[]'), (4, '{}'), (5, '5'), (6, '"s"'), (7, 'null'), (8, NULL), (9, '[[], {}, [[1]]]'),
		(10, '{"a": {"b": [7, 8, {"c": 9}]}}')) t(id, doc)`); err != nil {
		t.Fatal(err)
	}
	rows := func(q string) string {
		r, err := conn.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("%v\n%s", err, q)
		}
		defer r.Close()
		var out []string
		for r.Next() {
			var id, key, path, index, value, this any
			if err := r.Scan(&id, &key, &path, &index, &value, &this); err != nil {
				t.Fatal(err)
			}
			out = append(out, fmt.Sprintf("%v|%v|%v|%v|%v|%v", id, key, path, index, value, this))
		}
		if err := r.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(out, "\n")
	}
	for _, c := range []struct{ args, mode, path string }{
		{"input => parse_json(d.doc)", "", ""},
		{"input => d.doc", "", ""},
		{"parse_json(d.doc), mode => 'ARRAY'", "ARRAY", ""},
		{"input => parse_json(d.doc), mode => 'OBJECT'", "OBJECT", ""},
		{"input => parse_json(d.doc), path => 'a.b'", "", "a.b"},
	} {
		res, err := Translate("select d.id as i, f.key as k, f.path as p, f.index as x, f.value as v, f.this as th "+
			"from docs d, lateral flatten("+c.args+") f", Options{})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(res.SQL, "json_each") {
			t.Errorf("FLATTEN(%s) still reads json_each: %s", c.args, res.SQL)
		}
		this := "CAST(doc AS JSON)"
		prefix := "''"
		if c.path != "" {
			this = "json_extract(CAST(doc AS JSON), '$." + c.path + "')"
			prefix = "'" + c.path + "' || CASE WHEN starts_with(rel, '[') THEN '' ELSE '.' END"
		}
		cond := "json_type(j) IN ('ARRAY', 'OBJECT')"
		if c.mode != "" {
			cond += " AND json_type(j) = '" + c.mode + "'"
		}
		want := rows(`SELECT d.id, CASE WHEN json_type(j) = 'OBJECT' THEN e.key END, ` + prefix + ` || rel,
			CASE WHEN json_type(j) = 'ARRAY' THEN CAST(e.key AS BIGINT) END, CAST(json(e.value) AS VARCHAR), CAST(json(j) AS VARCHAR)
			FROM (SELECT id, ` + this + ` AS j FROM docs) d, LATERAL (SELECT *, regexp_replace(substring(fullkey, 2), '^\.', '') AS rel
			FROM json_each(d.j)) e WHERE ` + cond + ` ORDER BY d.id, e.id`)
		got := rows("SELECT i, k, p, x, CAST(json(v) AS VARCHAR), CAST(json(th) AS VARCHAR) FROM (" + res.SQL + ")")
		if perDoc(got) != want {
			t.Errorf("FLATTEN(%s):\n got\n%s\nwant\n%s\nDuckDB: %s", c.args, perDoc(got), want, res.SQL)
		}
	}
}

// perDoc lists each document's rows in the order they came, documents in
// order of their id.
func perDoc(s string) string {
	by := map[int][]string{}
	var ids []int
	for _, l := range strings.Split(s, "\n") {
		f, _, _ := strings.Cut(l, "|")
		id, _ := strconv.Atoi(f)
		if _, ok := by[id]; !ok {
			ids = append(ids, id)
		}
		by[id] = append(by[id], l)
	}
	slices.Sort(ids)
	var out []string
	for _, id := range ids {
		out = append(out, by[id]...)
	}
	return strings.Join(out, "\n")
}

// TestCreateViewColumnComments creates a view as dbt-snowflake does, by its
// qualified name with a comment on each column, and reads it.
func TestCreateViewColumnComments(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, "CREATE SCHEMA analytics"); err != nil {
		t.Fatal(err)
	}
	res, err := Translate("create or replace view memory.analytics.v_spend (\n  \"CLIENT\" COMMENT $$the client$$,\n  \"SPEND\" COMMENT 'in dollars'\n) copy grants as (\n  select 'acme' as client, 1.5 as spend\n)", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.SQL, "COMMENT") || len(res.Notices) == 0 {
		t.Errorf("comments kept or not noticed: %s %q", res.SQL, res.Notices)
	}
	if _, err := conn.ExecContext(ctx, res.SQL); err != nil {
		t.Fatalf("%v\n%s", err, res.SQL)
	}
	var client string
	var spend float64
	if err := conn.QueryRowContext(ctx, `SELECT "CLIENT", "SPEND" FROM memory.analytics.v_spend`).Scan(&client, &spend); err != nil {
		t.Fatal(err)
	}
	if client != "acme" || spend != 1.5 {
		t.Errorf("got %q %v", client, spend)
	}
}

// TestLikeAnyContains checks that LIKE ANY of text anywhere in a value, which it
// translates to one regular expression, is true of the same values as a comparison
// for each pattern: with characters that mean something to a regular expression, on
// more than one line, in other scripts, and for NULL.
func TestLikeAnyContains(t *testing.T) {
	conn := session(t)
	ctx := context.Background()
	values := []string{"NULL", "''", "'plain'", "'has a.b in it'", "'axb'", "'c++ here'", "'cc'", "'(x)'", "'x|y'", "'x'", "'[z]'", "'z'",
		"'line one' || chr(10) || 'needle' || chr(10) || 'line three'", "'Needle'", "'naïve'", "'nai'", "'日本語'", "'本'", "'$^'", "'it''s'", "'{1}'", "'\\d'"}
	patterns := []string{"a.b", "c++", "(x)", "x|y", "[z]", "needle", "ï", "本", "$^", "it''s", "{1}"}
	var quoted, ors []string
	for _, p := range patterns {
		quoted = append(quoted, "'%"+p+"%'")
		ors = append(ors, "(v LIKE '%"+p+"%')")
	}
	res, err := Translate("SELECT v, LOWER(v) LIKE ANY ("+strings.Join(quoted, ", ")+") AS got FROM t", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.SQL, "regexp_matches") {
		t.Fatalf("not one regular expression: %s", res.SQL)
	}
	for _, v := range values {
		table := "(SELECT " + v + " AS v) AS t"
		q := "SELECT coalesce(CAST(got AS VARCHAR), 'NULL'), coalesce(CAST(want AS VARCHAR), 'NULL') FROM (SELECT got, want FROM (" +
			strings.Replace(res.SQL, "FROM t", ", ("+strings.ReplaceAll(strings.Join(ors, " OR "), "(v ", "(lower(v) ")+") AS want FROM "+table, 1) + "))"
		var got, want string
		if err := conn.QueryRowContext(ctx, q).Scan(&got, &want); err != nil {
			t.Fatalf("%s: %v\n%s", v, err, q)
		}
		if got != want {
			t.Errorf("%s: %s, want %s", v, got, want)
		}
	}
}
