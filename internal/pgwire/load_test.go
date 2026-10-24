package pgwire

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// loadServer is a catalogServer that may load files from dir.
type loadServer struct {
	*catalogServer
	dir   string
	tasks *tasks.Scheduler
}

func startLoadServer(t *testing.T) *loadServer {
	t.Helper()
	dir := t.TempDir()
	ls := &loadServer{dir: dir, tasks: tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})}
	ls.catalogServer = startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) { o.Tasks = ls.tasks })
	mustExec(t, ls.admin, "CREATE TABLE lake.public.sales (id INTEGER, name VARCHAR, amount DECIMAL(10,2))")
	useLake(t, ls.admin)
	return ls
}

// useLake makes conn resolve names in the stand-in catalog, as sessions
// do in the Iceberg catalog.
func useLake(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	mustExec(t, conn, "USE lake.public")
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
}

// write puts a file at name under the server's directory, gzipped if
// zip is set.
func (ls *loadServer) write(t *testing.T, name, content string, zip bool) string {
	t.Helper()
	path := filepath.Join(ls.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	data := []byte(content)
	if zip {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		_, _ = w.Write(data)
		_ = w.Close()
		data = b.Bytes()
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// copyResult is a row COPY INTO returns.
type copyResult struct {
	file, status string
	rows, errors int64
	firstError   *string
}

func copyInto(t *testing.T, conn *pgx.Conn, sql string) ([]copyResult, string, error) {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		return nil, "", err
	}
	var out []copyResult
	for rows.Next() {
		var r copyResult
		if err := rows.Scan(&r.file, &r.status, &r.rows, &r.errors, &r.firstError); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	rows.Close()
	return out, rows.CommandTag().String(), rows.Err()
}

func mustCopy(t *testing.T, conn *pgx.Conn, sql string) ([]copyResult, string) {
	t.Helper()
	out, tag, err := copyInto(t, conn, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out, tag
}

func TestCopyIntoLoadsEachFileOnce(t *testing.T) {
	t.Parallel()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ls := startLoadServer(t)
			ls.write(t, "drops/a.csv", "id,name,amount\n1,alpha,1.50\n2,beta,2.50\n", false)
			ls.write(t, "drops/2026/10/b.csv.gz", "id,name,amount\n3,gamma,3.00\n", true)
			ls.write(t, "drops/d.json", `[{"id": 4, "name": "delta", "amount": 4}]`, false)
			ls.write(t, "drops/e.ndjson", "{\"id\": 5, \"name\": \"eps\"}\n{\"id\": 6, \"name\": \"zeta\"}\n", false)
			ls.write(t, "drops/notes.txt", "not data", false)
			mustExec(t, ls.admin, "COPY (SELECT 7 AS id, 'eta' AS name, 7.25 AS amount) TO '"+ls.dir+"/drops/c.parquet' (FORMAT parquet)")
			conn := connect(t, ls.url("igloo", adminPassword), mode)
			useLake(t, conn)

			from := "COPY INTO sales FROM '" + ls.dir + "/drops/' (PATTERN '.*[.](csv|csv[.]gz|parquet|json|ndjson)')"
			got, tag := mustCopy(t, conn, from)
			if len(got) != 5 || tag != "COPY 7" {
				t.Fatalf("first load: %v, tag %q", got, tag)
			}
			for _, r := range got {
				if r.status != tasks.LoadLoaded || r.errors != 0 || r.firstError != nil {
					t.Errorf("%s: %+v", r.file, r)
				}
			}
			if n := count(t, conn, "SELECT count(*) FROM lake.public.sales"); n != 7 {
				t.Fatalf("rows after the first load: %d", n)
			}
			if n := count(t, conn, "SELECT sum(amount)::INTEGER FROM lake.public.sales WHERE id IN (4, 7)"); n != 11 {
				t.Errorf("JSON and Parquet amounts: %d", n)
			}

			// Loaded files are not loaded again.
			if got, tag := mustCopy(t, conn, from); len(got) != 0 || tag != "COPY 0" {
				t.Fatalf("second load: %v, tag %q", got, tag)
			}
			// A file that changed is new; so is one that arrived since.
			ls.write(t, "drops/a.csv", "id,name,amount\n8,theta,8.00\n9,iota,9.00\n10,kappa,10.00\n", false)
			ls.write(t, "drops/f.csv", "id,name,amount\n11,lambda,11.00\n", false)
			got, _ = mustCopy(t, conn, from)
			if len(got) != 2 || got[0].rows+got[1].rows != 4 {
				t.Fatalf("changed and new files: %v", got)
			}
			// FORCE loads again what was loaded.
			got, _ = mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/drops/f.csv' (FORCE true)")
			if len(got) != 1 || got[0].rows != 1 {
				t.Fatalf("forced: %v", got)
			}
			if n := count(t, conn, "SELECT count(*) FROM lake.public.sales"); n != 12 {
				t.Fatalf("rows: %d", n)
			}
			if n := count(t, conn, "SELECT count(*) FROM igloo.load_history WHERE status = 'loaded' AND user_name = 'igloo' "+
				"AND table_name = 'lake.public.sales' AND size > 0 AND last_modified IS NOT NULL"); n != 8 {
				t.Errorf("load history rows: %d", n)
			}
		})
	}
}

func TestCopyIntoColumns(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	conn := ls.admin

	// By name, whatever the order and case; extra columns are left out and
	// missing ones are NULL.
	ls.write(t, "a.csv", "Amount,ID,extra\n1.25,1,x\n", false)
	mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/a.csv'")
	var amount float64
	var name *string
	if err := conn.QueryRow(context.Background(), "SELECT amount::DOUBLE, name FROM lake.public.sales WHERE id = 1").Scan(&amount, &name); err != nil ||
		amount != 1.25 || name != nil {
		t.Errorf("by name: %v %v %v", amount, name, err)
	}
	// Files without a header match by position, into a column list.
	ls.write(t, "b.csv", "2,two\n3,three\n", false)
	got, _ := mustCopy(t, conn, "COPY INTO sales (id, name) FROM '"+ls.dir+"/b.csv' (HEADER false)")
	if len(got) != 1 || got[0].rows != 2 {
		t.Fatalf("by position: %v", got)
	}
	if n := count(t, conn, "SELECT count(*) FROM lake.public.sales WHERE name = 'three' AND id = 3"); n != 1 {
		t.Errorf("by position: %d", n)
	}
	// A file whose columns cannot line up fails.
	ls.write(t, "c.csv", "4,four\n", false)
	if _, _, err := copyInto(t, conn, "COPY INTO sales FROM '"+ls.dir+"/c.csv' (HEADER false)"); err == nil ||
		!strings.Contains(err.Error(), "the file has 2 columns and the table 3") {
		t.Errorf("position mismatch: %v", err)
	}
	ls.write(t, "d.csv", "x,y\n1,2\n", false)
	if _, _, err := copyInto(t, conn, "COPY INTO sales FROM '"+ls.dir+"/d.csv'"); err == nil ||
		!strings.Contains(err.Error(), "none of the file's columns (x, y)") {
		t.Errorf("no matching names: %v", err)
	}
	if _, _, err := copyInto(t, conn, "COPY INTO sales (nope) FROM '"+ls.dir+"/b.csv'"); pgCode(err) != "42703" {
		t.Errorf("unknown column: %v", err)
	}
}

func TestCopyIntoFormatsAndCompression(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	conn := ls.admin
	ls.write(t, "a.tsv", "id\tname\n1\tone\n", false)
	ls.write(t, "b", "id;name\n2;two\n", true) // gzipped, without saying so
	ls.write(t, "c.data", "{\"id\": 3, \"name\": \"three\"}\n", false)
	mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/a.tsv'")
	mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/b' (FORMAT csv, DELIMITER ';')")
	mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/c.data' (FORMAT ndjson)")
	if n := count(t, conn, "SELECT count(*) FROM lake.public.sales WHERE id IN (1, 2, 3)"); n != 3 {
		t.Errorf("rows: %d", n)
	}
	if _, _, err := copyInto(t, conn, "COPY INTO sales FROM '"+ls.dir+"/c.data' (FORCE true)"); err == nil ||
		!strings.Contains(err.Error(), "give COPY INTO the FORMAT option") {
		t.Errorf("unknown format: %v", err)
	}
	for sql, want := range map[string]string{
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv' (FORMAT xml)":        "invalid FORMAT",
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv' (ON_ERROR maybe)":    "invalid ON_ERROR",
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv' (COLOR blue)":        "unknown COPY INTO option COLOR",
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv' (PATTERN '[')":       "PATTERN",
		"COPY INTO sales FROM " + ls.dir:                                  "location as a string",
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv'; SELECT 1":           "on their own",
		"COPY INTO sales FROM '" + ls.dir + "/a.tsv' (HEADER true) extra": "unexpected text",
	} {
		if _, _, err := copyInto(t, conn, sql); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", sql, err, want)
		}
	}
}

func TestCopyIntoOnError(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	conn := ls.admin
	ls.write(t, "drop/1.csv", "id,name,amount\n1,one,1.00\n", false)
	ls.write(t, "drop/2.csv", "id,name,amount\nx,two,2.00\n3,three,3.00\n", false)
	ls.write(t, "drop/3.csv", "id,name,amount\n4,four,4.00\n", false)
	from := "COPY INTO sales FROM '" + ls.dir + "/drop/'"

	// abort, the default: nothing is loaded, and the bad file is recorded.
	_, _, err := copyInto(t, conn, from)
	if err == nil || !strings.Contains(err.Error(), "2.csv") || !strings.Contains(err.Error(), "loaded nothing") {
		t.Fatalf("abort: %v", err)
	}
	if n := count(t, conn, "SELECT count(*) FROM lake.public.sales"); n != 0 {
		t.Fatalf("abort loaded %d rows", n)
	}
	if n := count(t, conn, "SELECT count(*) FROM igloo.load_history WHERE status = 'failed' AND file LIKE '%2.csv' AND first_error IS NOT NULL"); n != 1 {
		t.Errorf("abort history: %d", n)
	}

	// skip_file loads the good files and records the bad one.
	got, tag := mustCopy(t, conn, from+" (ON_ERROR skip_file)")
	if len(got) != 3 || tag != "COPY 2" {
		t.Fatalf("skip_file: %v %s", got, tag)
	}
	statuses := map[string]string{}
	for _, r := range got {
		statuses[filepath.Base(r.file)] = r.status
	}
	if statuses["1.csv"] != "loaded" || statuses["2.csv"] != "failed" || statuses["3.csv"] != "loaded" {
		t.Errorf("skip_file statuses: %v", statuses)
	}

	// continue loads what it can of the bad file, and counts the rest.
	got, tag = mustCopy(t, conn, from+" (ON_ERROR continue)")
	if len(got) != 1 || got[0].status != tasks.LoadPartial || got[0].rows != 1 || got[0].errors != 1 || got[0].firstError == nil || tag != "COPY 1" {
		t.Fatalf("continue: %+v %s", got, tag)
	}
	if n := count(t, conn, "SELECT count(*) FROM lake.public.sales"); n != 3 {
		t.Errorf("rows: %d", n)
	}
	// A partly loaded file counts as loaded.
	if got, _ := mustCopy(t, conn, from+" (ON_ERROR continue)"); len(got) != 0 {
		t.Errorf("loaded again: %v", got)
	}

	// Values that do not convert in Parquet and JSON are skipped too.
	ls.write(t, "j.ndjson", "{\"id\": \"five\", \"name\": \"x\"}\n{\"id\": 6, \"name\": \"six\"}\n", false)
	got, _ = mustCopy(t, conn, "COPY INTO sales FROM '"+ls.dir+"/j.ndjson' (ON_ERROR continue)")
	if len(got) != 1 || got[0].rows != 1 || got[0].errors != 1 {
		t.Errorf("JSON continue: %+v", got)
	}
}

func TestCopyIntoCreatesTables(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	ls.write(t, "new/a.csv", "day,clicks,campaign\n2026-10-01,10,fall\n2026-10-02,12,fall\n", false)
	ls.write(t, "new/b.csv", "day,clicks,campaign\n2026-10-03,7,winter\n", false)
	if _, _, err := copyInto(t, ls.admin, "COPY INTO clicks FROM '"+ls.dir+"/new/'"); pgCode(err) != "42P01" {
		t.Fatalf("missing table: %v", err)
	}
	got, _ := mustCopy(t, ls.admin, "COPY INTO clicks FROM '"+ls.dir+"/new/' (CREATE_TABLE true)")
	if len(got) != 2 {
		t.Fatalf("loaded: %v", got)
	}
	var typ string
	if err := ls.admin.QueryRow(context.Background(), `SELECT data_type FROM duckdb_columns()
		WHERE database_name = 'lake' AND table_name = 'clicks' AND column_name = 'day'`).Scan(&typ); err != nil || typ != "DATE" {
		t.Errorf("inferred type of day: %q %v", typ, err)
	}
	if n := count(t, ls.admin, "SELECT sum(clicks) FROM lake.public.clicks"); n != 29 {
		t.Errorf("clicks: %d", n)
	}
}

func TestCopyIntoIsAuthorized(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	ls.write(t, "a.csv", "id,name,amount\n1,one,1.00\n", false)
	from := "COPY INTO sales FROM '" + ls.dir + "/a.csv'"

	rita := ls.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, rita, from, "INSERT requires the writer role")
	mustExec(t, ls.admin, "GRANT INSERT ON TABLE lake.public.sales TO rita")
	expectAllowed(t, rita, from)
	// Refused even when there is nothing new to load.
	mustExec(t, ls.admin, "REVOKE INSERT ON TABLE lake.public.sales FROM rita")
	expectDenied(t, rita, from, "INSERT requires the writer role")
	expectDenied(t, rita, "COPY INTO lake.public.other FROM '"+ls.dir+"/a.csv' (CREATE_TABLE true)", "CREATE TABLE requires the writer role")

	// Reading files needs the reader role, whatever the grants.
	nina := ls.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ls.admin, "GRANT ALL ON SCHEMA lake.public TO nina")
	expectDenied(t, nina, from+" (FORCE true)", "COPY INTO reads files, which needs the reader role")

	// Only the allowed locations: not the server's own disk, nor a way
	// out of the location.
	walt := ls.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, walt, "COPY INTO sales FROM '/etc/'", "COPY INTO loads only from")
	expectDenied(t, walt, "COPY INTO sales FROM '"+ls.dir+"/../'", "COPY INTO loads only from")
	expectDenied(t, walt, "COPY INTO sales FROM 's3://elsewhere/drops/'", "COPY INTO loads only from")
	if _, _, err := copyInto(t, walt, "COPY INTO memory.main.local FROM '"+ls.dir+"/a.csv'"); err == nil {
		t.Error("loaded into the local database")
	}

	// It commits itself, so not inside a transaction.
	mustExec(t, walt, "BEGIN")
	if _, err := walt.Exec(context.Background(), from); pgCode(err) != "25001" {
		t.Errorf("in a transaction: %v", err)
	}
	mustExec(t, walt, "ROLLBACK")

	if n := count(t, ls.admin, "SELECT count(*) FROM igloo.query_history WHERE user_name = 'rita' AND statement_type = 'COPY' "+
		"AND outcome = 'ok' AND tables_written = ['lake.public.sales']"); n != 1 {
		t.Errorf("query history: %d", n)
	}
}

func TestInterruptedLoadIsNotRepeated(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	path := ls.write(t, "a.csv", "id,name,amount\n1,one,1.00\n", false)
	var (
		size     int64
		modified time.Time
	)
	if err := ls.admin.QueryRow(context.Background(), "SELECT size, last_modified FROM read_blob('"+path+"')").Scan(&size, &modified); err != nil {
		t.Fatal(err)
	}
	// Igloo stopped while loading the file: it may or may not be in the
	// table.
	if err := ls.tasks.Store().WriteLoads(context.Background(), []tasks.Load{{
		ID: "x", Table: "lake.public.sales", File: path, Size: size, Modified: modified,
		Status: tasks.LoadLoading, Start: time.Now(), User: "igloo",
	}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := mustCopy(t, ls.admin, "COPY INTO sales FROM '"+path+"'"); len(got) != 0 {
		t.Fatalf("loaded a file whose load was cut short: %v", got)
	}
	if n := count(t, ls.admin, "SELECT count(*) FROM igloo.load_history WHERE status = 'unknown' AND first_error LIKE '%FORCE%'"); n != 1 {
		t.Errorf("unknown loads: %d", n)
	}
	if got, _ := mustCopy(t, ls.admin, "COPY INTO sales FROM '"+path+"' (FORCE true)"); len(got) != 1 {
		t.Errorf("forced: %v", got)
	}
}

func TestDroppingATableForgetsItsLoads(t *testing.T) {
	t.Parallel()
	ls := startLoadServer(t)
	ls.write(t, "a.csv", "id,name,amount\n1,one,1.00\n", false)
	from := "COPY INTO sales FROM '" + ls.dir + "/a.csv'"
	mustCopy(t, ls.admin, from)
	mustExec(t, ls.admin, "ALTER TABLE lake.public.sales RENAME TO sales2")
	if got, _ := mustCopy(t, ls.admin, "COPY INTO sales2 FROM '"+ls.dir+"/a.csv'"); len(got) != 0 {
		t.Errorf("the history did not follow the rename: %v", got)
	}
	mustExec(t, ls.admin, "DROP TABLE lake.public.sales2")
	mustExec(t, ls.admin, "CREATE TABLE lake.public.sales2 (id INTEGER, name VARCHAR, amount DECIMAL(10,2))")
	if got, _ := mustCopy(t, ls.admin, "COPY INTO sales2 FROM '"+ls.dir+"/a.csv'"); len(got) != 1 {
		t.Errorf("a new table with the old name got the old history: %v", got)
	}
}

func TestParseCopyInto(t *testing.T) {
	t.Parallel()
	c, ok, err := parseCopyInto(`copy into lake."My Schema".t (a, "B") from 's3://b/p/' (format csv, header false, delimiter ';',
		pattern '.*[.]csv', match_by position, on_error skip_file, force, compression gzip, create_table = true);`)
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	want := `COPY INTO "lake"."My Schema"."t" ("a", "B") FROM 's3://b/p/' (FORMAT csv, HEADER false, DELIMITER ';', ` +
		`COMPRESSION gzip, PATTERN '.*[.]csv', MATCH_BY position, ON_ERROR skip_file, FORCE true, CREATE_TABLE true)`
	if c.String() != want {
		t.Errorf("got  %s\nwant %s", c, want)
	}
	again, _, err := parseCopyInto(c.String())
	if err != nil || again.String() != want {
		t.Errorf("round trip: %s %v", again, err)
	}
	if _, ok, _ := parseCopyInto("COPY t FROM 'x'"); ok {
		t.Error("plain COPY taken for COPY INTO")
	}
}
