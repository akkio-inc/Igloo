package pgwire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/akkio-inc/igloo/internal/audit"
)

// copyText sends data as COPY's input for sql, as psql's \copy does.
func copyText(t *testing.T, conn *pgx.Conn, sql, data string) (int64, error) {
	t.Helper()
	tag, err := conn.PgConn().CopyFrom(context.Background(), strings.NewReader(data), sql)
	return tag.RowsAffected(), err
}

// TestCopyFromStdin loads rows in COPY's text, CSV and binary formats, as
// psql, psycopg and pgx send them, into an Iceberg table, and checks what
// each value becomes, NULLs, escapes and PostgreSQL's own forms of
// booleans, bytea, arrays and intervals included.
func TestCopyFromStdin(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t, func(o *Options) {
		o.Audit = audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, slog.New(slog.DiscardHandler))
	})
	admin := env.admin
	ctx := context.Background()
	mustExec(t, admin, `CREATE TABLE people (id BIGINT, name VARCHAR, ok BOOLEAN, score DECIMAL(10,2), born DATE,
		seen TIMESTAMPTZ, data BLOB, tags VARCHAR[])`)
	const cols = "id, name, ok, score::VARCHAR, born::VARCHAR, seen::VARCHAR, hex(data), tags::VARCHAR"
	check := func(step, where string, want []string) {
		t.Helper()
		got := texts(t, admin, "SELECT "+cols+" FROM people WHERE "+where+" ORDER BY id")
		if !slices.Equal(got, want) {
			t.Errorf("%s:\n got %q\nwant %q", step, got, want)
		}
	}

	// Text: tabs, \N, backslash escapes, and PostgreSQL's forms.
	text := "1\tAnn\\tTab\tt\t1.50\t2026-01-02\t2026-01-02 03:04:05+00\t\\\\x0a0b\t{a,\"b c\",NULL}\n" +
		"2\t\\N\toff\t\\N\t\\N\t\\N\t\\N\t\\N\r\n" +
		"3\tline\\nbreak \\\\ back\\x41\\101\tYES\t-0.25\t2026-02-03\t2026-02-03 04:05:06.5+02\t\\\\001A\t{}\n" +
		"\\.\n"
	n, err := copyText(t, admin, "COPY people FROM STDIN", text)
	if err != nil || n != 3 {
		t.Fatalf("text: %d rows, %v", n, err)
	}
	check("text", "id <= 3", []string{
		"1|Ann\tTab|true|1.50|2026-01-02|2026-01-02 03:04:05+00|0A0B|[a, b c, NULL]",
		"2|NULL|false|NULL|NULL|NULL|NULL|NULL",
		"3|line\nbreak \\ backAA|true|-0.25|2026-02-03|2026-02-03 02:05:06.5+00|0141|[]",
	})

	// CSV with a header: quotes around delimiters and line ends, "" an empty
	// string and nothing a NULL, but where FORCE_NOT_NULL and FORCE_NULL
	// say otherwise; and a column list.
	csv := "id,name,ok,tags\n" +
		"10,\"Smith, \"\"Jr\"\"\",true,\"[\"\"x\"\"]\"\n" +
		"11,\"two\nlines\",,\n" +
		"12,\"\",false,{q}\n"
	n, err = copyText(t, admin, "COPY people (id, name, ok, tags) FROM STDIN WITH (FORMAT csv, HEADER true)", csv)
	if err != nil || n != 3 {
		t.Fatalf("csv: %d rows, %v", n, err)
	}
	check("csv", "id BETWEEN 10 AND 12", []string{
		`10|Smith, "Jr"|true|NULL|NULL|NULL|NULL|[x]`,
		"11|two\nlines|NULL|NULL|NULL|NULL|NULL|NULL",
		"12||false|NULL|NULL|NULL|NULL|[q]",
	})
	if got := texts(t, admin, "SELECT name IS NULL, ok IS NULL FROM people WHERE id IN (11, 12) ORDER BY id"); !slices.Equal(got, []string{"false|true", "false|false"}) {
		t.Errorf("csv NULLs: %q", got)
	}
	n, err = copyText(t, admin, "COPY people (id, name) FROM STDIN (FORMAT csv, FORCE_NOT_NULL (name))", "13,\n")
	if err != nil || n != 1 || texts(t, admin, "SELECT name IS NULL FROM people WHERE id = 13")[0] != "false" {
		t.Errorf("FORCE_NOT_NULL: %d, %v", n, err)
	}
	n, err = copyText(t, admin, "COPY people (id, name) FROM STDIN (FORMAT csv, FORCE_NULL (name))", "14,\"\"\n")
	if err != nil || n != 1 || texts(t, admin, "SELECT name IS NULL FROM people WHERE id = 14")[0] != "true" {
		t.Errorf("FORCE_NULL: %d, %v", n, err)
	}
	// The options as PostgreSQL wrote them before 9.0, and WHERE.
	n, err = copyText(t, admin, "COPY people (id, name) FROM STDIN WITH DELIMITER '|' NULL AS 'none' WHERE id > 15", "15|a\n16|none\n17|b\n")
	if err != nil || n != 2 {
		t.Errorf("legacy options and WHERE: %d, %v", n, err)
	}
	check("WHERE", "id BETWEEN 15 AND 17", []string{"16|NULL|NULL|NULL|NULL|NULL|NULL|NULL", "17|b|NULL|NULL|NULL|NULL|NULL|NULL"})

	// Binary, as pgx's CopyFrom sends it.
	seen := time.Date(2026, 3, 4, 5, 6, 7, 800000, time.UTC)
	rows := [][]any{
		{int64(20), "Bin", true, pgtype.Numeric{Int: big.NewInt(12345), Exp: -2, Valid: true}, time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC), seen,
			[]byte{0, 255}, `["p","q"]`},
		{int64(21), nil, nil, nil, nil, nil, nil, nil},
	}
	got, err := admin.CopyFrom(ctx, pgx.Identifier{"people"}, []string{"id", "name", "ok", "score", "born", "seen", "data", "tags"}, pgx.CopyFromRows(rows))
	if err != nil || got != 2 {
		t.Fatalf("binary: %d, %v", got, err)
	}
	check("binary", "id IN (20, 21)", []string{
		"20|Bin|true|123.45|2026-03-04|2026-03-04 05:06:07.0008+00|00FF|[p, q]",
		"21|NULL|NULL|NULL|NULL|NULL|NULL|NULL",
	})

	// In a transaction block, it commits and rolls back with it.
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Conn().PgConn().CopyFrom(ctx, strings.NewReader("30\n"), "COPY people (id) FROM STDIN"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, admin, "SELECT count(*) FROM people WHERE id = 30"); n != 0 {
		t.Errorf("rolled back, %d rows", n)
	}
	tx, err = admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Conn().PgConn().CopyFrom(ctx, strings.NewReader("31\n"), "COPY people (id) FROM STDIN"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, admin, "SELECT count(*) FROM people WHERE id = 31"); n != 1 {
		t.Errorf("committed, %d rows", n)
	}

	// The history has the COPY, what it wrote and how many rows.
	if got := texts(t, admin, "SELECT row_count, tables_written FROM igloo.query_history WHERE query_text = 'COPY people FROM STDIN'"); len(got) != 1 ||
		!strings.HasPrefix(got[0], "3|") || !strings.Contains(got[0], "people") {
		t.Errorf("history: %q", got)
	}
	if n := count(t, admin, "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE '__igloo_copy%'"); n != 0 {
		t.Errorf("%d staging tables left", n)
	}

	// Into a temporary table, which takes types Iceberg has not got, as
	// intervals, in text and in binary.
	mustExec(t, admin, "CREATE TEMP TABLE spans (id INTEGER, span INTERVAL, start TIME)")
	if _, err := copyText(t, admin, "COPY spans FROM STDIN", "1\t-1 days +02:00:00\t04:05:06\n2\t1 year 2 mons\t\\N\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CopyFrom(ctx, pgx.Identifier{"spans"}, []string{"id", "span", "start"}, pgx.CopyFromRows([][]any{
		{int32(3), pgtype.Interval{Months: 1, Days: 2, Microseconds: 3_000_000, Valid: true}, pgtype.Time{Microseconds: 3_600_000_000, Valid: true}},
	})); err != nil {
		t.Fatal(err)
	}
	if got := texts(t, admin, "SELECT id, span::VARCHAR, start::VARCHAR FROM spans ORDER BY id"); !slices.Equal(got, []string{
		"1|-1 day 02:00:00|04:05:06", "2|1 year 2 months|NULL", "3|1 month 2 days 00:00:03|01:00:00",
	}) {
		t.Errorf("intervals: %q", got)
	}
}

// TestCopyFromStdinRefusals checks COPY's errors: each fails the whole COPY,
// with the line and column at fault, writes nothing, and leaves the session
// ready for its next statement; and a user without INSERT on the table is
// refused before they send anything.
func TestCopyFromStdinRefusals(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	ctx := context.Background()
	mustExec(t, admin, "CREATE TABLE nums (id INTEGER, label VARCHAR, day DATE)")
	mustExec(t, admin, "INSERT INTO nums VALUES (1, 'one', DATE '2026-01-01')")
	for _, c := range []struct{ name, sql, data, code, mentions string }{
		{"a value that does not convert", "COPY nums FROM STDIN", "2\ttwo\t2026-01-02\nx\tthree\t2026-01-03\n", "22P02", "line 2, column id"},
		{"a date that does not convert", "COPY nums FROM STDIN (FORMAT csv)", "2,two,2026-13-45\n", "22P02", "line 1, column day"},
		{"missing data", "COPY nums FROM STDIN", "2\ttwo\n", "22P04", `missing data for column "day"`},
		{"extra data", "COPY nums FROM STDIN", "2\ttwo\t2026-01-02\tmore\n", "22P04", "extra data"},
		{"an unterminated quote", "COPY nums FROM STDIN (FORMAT csv)", "2,\"two\n", "22P04", "unterminated"},
		{"an unknown column", "COPY nums (id, nope) FROM STDIN", "", "42703", "nope"},
		{"a missing table", "COPY nowhere FROM STDIN", "", "42P01", "nowhere"},
		{"an unknown option", "COPY nums FROM STDIN (FORMAT xml)", "", "22023", "xml"},
		{"a header that does not match", "COPY nums FROM STDIN (FORMAT csv, HEADER match)", "id,name,day\n", "22P04", "mismatch"},
	} {
		_, err := copyText(t, admin, c.sql, c.data)
		if pgCode(err) != c.code || !strings.Contains(err.Error(), c.mentions) {
			t.Errorf("%s: %v, want %s mentioning %q", c.name, err, c.code, c.mentions)
		}
		if n := count(t, admin, "SELECT count(*) FROM nums"); n != 1 {
			t.Errorf("%s: the table has %d rows", c.name, n)
		}
	}
	// The client gives up part way, as psql does on ^C.
	_, err := admin.PgConn().CopyFrom(ctx, io.MultiReader(strings.NewReader("2\ttwo\t2026-01-02\n"), failingReader{}), "COPY nums FROM STDIN")
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Errorf("a client that gave up: %v", err)
	}
	if n := count(t, admin, "SELECT count(*) FROM nums"); n != 1 {
		t.Errorf("after the client gave up the table has %d rows", n)
	}

	// INSERT is what COPY needs: a user with none is refused at once.
	mustExec(t, admin, "CREATE USER nina WITH PASSWORD 'nina-password' ROLE none")
	nina := connect(t, env.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	mustExec(t, nina, "SET search_path = 'lake.public'")
	if _, err := copyText(t, nina, "COPY nums FROM STDIN", "5\tfive\t2026-01-05\n"); pgCode(err) != "42501" {
		t.Errorf("without INSERT: %v", err)
	}
	mustExec(t, admin, "GRANT INSERT ON TABLE nums TO nina")
	if n, err := copyText(t, nina, "COPY nums FROM STDIN", "5\tfive\t2026-01-05\n"); err != nil || n != 1 {
		t.Errorf("with INSERT: %d, %v", n, err)
	}
	if n := count(t, admin, "SELECT count(*) FROM nums"); n != 2 {
		t.Errorf("the table has %d rows", n)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("the user stopped") }

// TestPgDumpLoad loads a data-only pg_dump as psql runs it: the settings a
// dump starts with, then COPY ... FROM stdin with each table's rows.
func TestPgDumpLoad(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE accounts (id INTEGER, email VARCHAR, balance DECIMAL(12,2), opened TIMESTAMPTZ, flags BOOLEAN[])")
	for _, q := range []string{
		"SET statement_timeout = 0",
		"SET lock_timeout = 0",
		"SET idle_in_transaction_session_timeout = 0",
		"SET client_encoding = 'UTF8'",
		"SET standard_conforming_strings = on",
		"SELECT pg_catalog.set_config('search_path', '', false)",
		"SET check_function_bodies = false",
		"SET xmloption = content",
		"SET client_min_messages = warning",
		"SET row_security = off",
	} {
		if _, err := admin.Exec(context.Background(), q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
	data := "1\tann@example.com\t10.00\t2026-01-01 00:00:00+00\t{t,f}\n" +
		"2\tbob@example.com\t-3.50\t2026-01-02 12:30:00+00\t{}\n" +
		"3\t\\N\t0.00\t\\N\t{NULL}\n"
	n, err := copyText(t, admin, "COPY public.accounts (id, email, balance, opened, flags) FROM stdin;", data)
	if err != nil || n != 3 {
		t.Fatalf("the dump's COPY: %d, %v", n, err)
	}
	got := texts(t, admin, "SELECT id, email, balance::VARCHAR, opened::VARCHAR, flags::VARCHAR FROM lake.public.accounts ORDER BY id")
	want := []string{
		"1|ann@example.com|10.00|2026-01-01 00:00:00+00|[true, false]",
		"2|bob@example.com|-3.50|2026-01-02 12:30:00+00|[]",
		"3|NULL|0.00|NULL|[NULL]",
	}
	if !slices.Equal(got, want) {
		t.Errorf("loaded:\n got %q\nwant %q", got, want)
	}
}

// TestCopyFromStdinParse checks the forms of the statement it reads.
func TestCopyFromStdinParse(t *testing.T) {
	for _, c := range []struct {
		sql, want string
		err       bool
	}{
		{sql: "COPY t FROM STDIN", want: "text \t \\N 0"},
		{sql: "copy s.t (a, \"B\") from stdin;", want: "text \t \\N 0 a,B"},
		{sql: "COPY t FROM STDIN WITH CSV HEADER", want: "csv , [] 1"},
		{sql: "COPY t FROM STDIN (FORMAT csv, DELIMITER ';', NULL 'NA', HEADER match)", want: "csv ; NA 2"},
		{sql: "COPY t FROM STDIN WITH (DELIMITER E'\\t', NULL '')", want: "text \t [] 0"},
		{sql: "COPY t FROM STDIN BINARY", want: "binary \t \\N 0"},
		{sql: "COPY t (a) FROM STDIN (FORMAT binary)", want: "binary \t \\N 0 a"},
		{sql: "COPY t FROM STDIN (FORMAT text, QUOTE '\"')", err: true},
		{sql: "COPY t FROM STDIN (FORMAT csv, DELIMITER '\"')", err: true},
		{sql: "COPY t FROM STDIN (DEFAULT 'x')", err: true},
		{sql: "COPY t FROM STDIN (FORMAT binary, HEADER)", err: true},
	} {
		got, ok, err := parseCopyFromStdin(c.sql)
		if !ok {
			t.Errorf("%s: not recognized", c.sql)
			continue
		}
		if c.err {
			if err == nil {
				t.Errorf("%s: accepted", c.sql)
			}
			continue
		}
		null := got.null
		if null == "" {
			null = "[]"
		}
		s := fmt.Sprintf("%s %c %s %d", got.format, got.delim, null, got.header)
		if len(got.columns) > 0 {
			s += " " + strings.Join(got.columns, ",")
		}
		if err != nil || s != c.want {
			t.Errorf("%s: %q, %v; want %q", c.sql, s, err, c.want)
		}
	}
	for _, sql := range []string{"COPY t FROM 'file.csv'", "COPY t TO STDOUT", "COPY INTO t FROM 's3://x/'"} {
		if _, ok, _ := parseCopyFromStdin(sql); ok {
			t.Errorf("%s: recognized", sql)
		}
	}
}
