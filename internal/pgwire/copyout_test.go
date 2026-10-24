package pgwire

import (
	"bytes"
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestParseCopyTo(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]copyTo{
		"COPY (SELECT 1) TO STDOUT":                                   {query: "SELECT 1", format: "text", delimiter: "\t", null: `\N`},
		`copy "Sales".orders (id, "Total") to stdout with csv header`: {query: `SELECT id, "Total" FROM "Sales"."orders"`, format: "csv", header: true, delimiter: ",", null: ""},
		// ADBC and connectorx.
		"COPY (SELECT f(x) FROM t) TO STDOUT (FORMAT binary)":                    {query: "SELECT f(x) FROM t", format: "binary", delimiter: "\t", null: `\N`},
		"COPY (SELECT 1) TO STDOUT WITH BINARY":                                  {query: "SELECT 1", format: "binary", delimiter: "\t", null: `\N`},
		"COPY t TO STDOUT (FORMAT csv, HEADER false, DELIMITER ';', NULL 'NA');": {query: `SELECT * FROM "t"`, format: "csv", delimiter: ";", null: "NA"},
	} {
		got, ok, err := parseCopyTo(q)
		if !ok || err != nil || got != want {
			t.Errorf("parseCopyTo(%q) = %+v, %v, %v; want %+v", q, got, ok, err, want)
		}
	}
	for _, q := range []string{"COPY t TO 's3://bucket/t.parquet'", "COPY t FROM STDIN", "SELECT 1"} {
		if _, ok, _ := parseCopyTo(q); ok {
			t.Errorf("parsed %q", q)
		}
	}
	if _, ok, err := parseCopyTo("COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE *)"); !ok || err == nil {
		t.Errorf("an unsupported option: %v %v", ok, err)
	}
}

func TestCopyToStdout(t *testing.T) {
	t.Parallel()
	ts := startServer(t)
	ctx := context.Background()
	admin := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, `CREATE TABLE spend (id INTEGER, name VARCHAR, amount DECIMAL(10,2), paid_at TIMESTAMPTZ)`)
	mustExec(t, admin, `INSERT INTO spend VALUES (1, 'tab	and "quote"', 1.50, TIMESTAMPTZ '2026-10-01 12:00:00+00'), (2, NULL, NULL, NULL)`)
	mustExec(t, admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	reader := connect(t, ts.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)

	copyTo := func(conn *pgx.Conn, sql string) (string, string, error) {
		var buf bytes.Buffer
		tag, err := conn.PgConn().CopyTo(ctx, &buf, sql)
		return buf.String(), tag.String(), err
	}
	// A reader may copy what they may read.
	for sql, want := range map[string]string{
		"COPY (SELECT * FROM spend ORDER BY id) TO STDOUT":          "1\ttab\\tand \"quote\"\t1.50\t2026-10-01 12:00:00+00\n2\t\\N\t\\N\t\\N\n",
		"COPY spend (id, name) TO STDOUT WITH (FORMAT csv, HEADER)": "id,name\n1,\"tab	and \"\"quote\"\"\"\n2,\n",
	} {
		got, tag, err := copyTo(reader, sql)
		if err != nil || got != want || tag != "COPY 2" {
			t.Errorf("%s: %q %q %v, want %q", sql, got, tag, err, want)
		}
	}
	got, _, err := copyTo(reader, "COPY (SELECT id FROM spend WHERE id = 1) TO STDOUT (FORMAT binary)")
	want := "PGCOPY\n\xff\r\n\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00" + // signature, flags, extension
		"\x00\x01\x00\x00\x00\x04\x00\x00\x00\x01" + // one field: int4 1
		"\xff\xff"
	if err != nil || got != want {
		t.Errorf("binary: %q %v", got, err)
	}

	// A failure while copying ends the copy, and the session carries on.
	if _, _, err := copyTo(reader, "COPY (SELECT CASE WHEN id = 2 THEN error('boom') ELSE id END FROM spend ORDER BY id) TO STDOUT"); err == nil {
		t.Error("a failing query copied")
	}
	if n := count(t, reader, "SELECT count(*) FROM spend"); n != 2 {
		t.Errorf("after a failed copy: %d", n)
	}

	// COPY TO STDOUT is a query: a user with role none needs SELECT, and
	// COPY to a file still needs the writer role.
	if err := ts.store.Create(ctx, auth.User{Name: "nina", Role: auth.RoleNone, Verifier: mustVerifier(t, "nina-password")}); err != nil {
		t.Fatal(err)
	}
	nina := connect(t, ts.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	if _, _, err := copyTo(nina, "COPY spend TO STDOUT"); pgCode(err) != "42501" {
		t.Errorf("role none without a grant: %v", err)
	}
	if _, err := reader.Exec(ctx, "COPY spend TO 'spend.csv'"); pgCode(err) != "42501" {
		t.Errorf("COPY to a file as a reader: %v", err)
	}
}

func mustVerifier(t *testing.T, password string) string {
	t.Helper()
	v, err := auth.NewVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
