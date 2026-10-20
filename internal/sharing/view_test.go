package sharing

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// mapped is a Delta schema field under column mapping.
func mapped(name string, typ string, id int, phys string) string {
	return fmt.Sprintf(`{"name":%q,"type":%s,"nullable":true,"metadata":{"delta.columnMapping.id":%d,"delta.columnMapping.physicalName":%q}}`,
		name, typ, id, phys)
}

// mappedSchema is a table under column mapping: id, info (a struct with
// x), day.
var mappedSchema = `{"type":"struct","fields":[` + mapped("id", `"long"`, 1, "col-1") + "," +
	mapped("info", `{"type":"struct","fields":[`+mapped("x", `"integer"`, 3, "col-3")+`]}`, 2, "col-2") + "," +
	mapped("day", `"date"`, 4, "col-4") + `]}`

func TestColumns(t *testing.T) {
	snap := &remoteSnapshot{schema: mappedSchema, columnMapping: "name", partitionColumns: []string{"day"}}
	cols, err := snap.columns()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range cols {
		got = append(got, fmt.Sprintf("%s/%s/%v", c.Name, c.Physical, c.Partition))
	}
	if want := "id/col-1/false info/col-2/false day/col-4/true"; strings.Join(got, " ") != want {
		t.Errorf("columns %s, want %s", got, want)
	}
	snap.columnMapping = ""
	if cols, _ := snap.columns(); cols[0].Physical != "id" {
		t.Errorf("without column mapping the physical name is %s", cols[0].Physical)
	}

	for name, s := range map[string]*remoteSnapshot{
		"no physical name":  {schema: `{"type":"struct","fields":[{"name":"a","type":"long","metadata":{}}]}`, columnMapping: "name"},
		"unknown partition": {schema: mappedSchema, partitionColumns: []string{"month"}},
		"no columns":        {schema: `{"type":"struct","fields":[]}`},
		"not a struct":      {schema: `{"type":"array"}`},
	} {
		if _, err := s.columns(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	// The catalog lists the columns in the table's order, partition
	// columns with the type read_parquet gives them once there are files.
	snap = &remoteSnapshot{schema: `{"type":"struct","fields":[{"name":"at","type":"timestamp","metadata":{}},` +
		`{"name":"id","type":"long","metadata":{}}]}`, partitionColumns: []string{"at"}}
	catalogColumns := func() string {
		cols, err := remoteColumns(snap)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, c := range cols {
			out = append(out, c.Name+" "+c.Type)
		}
		return strings.Join(out, ", ")
	}
	if got := catalogColumns(); got != "at TIMESTAMPTZ, id BIGINT" {
		t.Errorf("columns of an empty table: %s", got)
	}
	snap.files = []remoteFile{{ID: "a"}}
	if got := catalogColumns(); got != "at TIMESTAMP, id BIGINT" {
		t.Errorf("columns of a partitioned table: %s", got)
	}
	sql, err := emptyView([]remoteColumn{{Name: "id", Type: "long"}, {Name: "at", Type: "timestamp"}})
	if want := `SELECT CAST(NULL AS BIGINT) AS "id", CAST(NULL AS TIMESTAMPTZ) AS "at" LIMIT 0`; err != nil || sql != want {
		t.Errorf("empty view %s, %v", sql, err)
	}
}

func TestProjection(t *testing.T) {
	prim := &duckShape{}
	structOf := func(names ...string) *duckShape {
		s := &duckShape{kind: 's'}
		for _, n := range names {
			s.fields = append(s.fields, shapeField{name: n, shape: prim})
		}
		return s
	}
	snap := &remoteSnapshot{schema: mappedSchema, partitionColumns: []string{"day"}}
	plain, _ := snap.columns()
	snap.columnMapping = "name"
	cols, _ := snap.columns()

	for _, c := range []struct {
		name    string
		cols    []remoteColumn
		shapes  map[string]*duckShape
		q       string
		mapping bool
		want    string
	}{
		{"as in the files", plain, map[string]*duckShape{"id": prim, "info": structOf("x"), "day": prim}, "", false,
			`"id" AS "id", "info" AS "info", "day" AS "day"`},
		{"a column no file has", plain, map[string]*duckShape{"id": prim, "day": prim}, "", false,
			`"id" AS "id", CAST(NULL AS STRUCT("x" INTEGER)) AS "info", "day" AS "day"`},
		{"files with a field the table dropped", plain, map[string]*duckShape{"id": prim, "info": structOf("x", "old"), "day": prim}, "", false,
			`"id" AS "id", CASE WHEN "info" IS NULL THEN NULL ELSE struct_pack("x" := struct_extract("info", 'x')) END AS "info", "day" AS "day"`},
		{"column mapping", cols, map[string]*duckShape{"col-1": prim, "col-2": structOf("col-3"), "col-4": prim}, "d.", true,
			`d."col-1" AS "id", CASE WHEN d."col-2" IS NULL THEN NULL ELSE struct_pack("x" := struct_extract(d."col-2", 'col-3')) END AS "info", ` +
				`d."col-4" AS "day"`},
		{"column mapping, a field no file has", cols, map[string]*duckShape{"col-1": prim, "col-2": structOf("col-9"), "col-4": prim}, "", true,
			`"col-1" AS "id", CASE WHEN "col-2" IS NULL THEN NULL ELSE struct_pack("x" := CAST(NULL AS INTEGER)) END AS "info", "col-4" AS "day"`},
	} {
		got, err := projection(c.cols, c.shapes, c.q, c.mapping)
		if err != nil || got != c.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", c.name, got, err, c.want)
		}
	}

	// Lists and maps of structs are rebuilt element by element.
	var nested remoteSnapshot
	nested.columnMapping = "name"
	nested.schema = `{"type":"struct","fields":[` +
		mapped("tags", `{"type":"array","elementType":{"type":"struct","fields":[`+mapped("k", `"string"`, 6, "col-6")+`]},"containsNull":true}`, 5, "col-5") + "," +
		mapped("attrs", `{"type":"map","keyType":"string","valueType":{"type":"array","elementType":{"type":"struct","fields":[`+
			mapped("v", `"long"`, 8, "col-8")+`]},"containsNull":true},"valueContainsNull":true}`, 7, "col-7") + `]}`
	ncols, err := nested.columns()
	if err != nil {
		t.Fatal(err)
	}
	shapes := map[string]*duckShape{
		"col-5": {kind: 'l', elem: structOf("col-6")},
		"col-7": {kind: 'm', key: prim, value: &duckShape{kind: 'l', elem: structOf("col-8")}},
	}
	got, err := projection(ncols, shapes, "", true)
	want := `list_transform("col-5", lambda x0: CASE WHEN x0 IS NULL THEN NULL ELSE struct_pack("k" := struct_extract(x0, 'col-6')) END) AS "tags", ` +
		`map_from_entries(list_transform(map_entries("col-7"), lambda x0: struct_pack("key" := struct_extract(x0, 'key'), "value" := ` +
		`list_transform(struct_extract(x0, 'value'), lambda x1: CASE WHEN x1 IS NULL THEN NULL ELSE struct_pack("v" := struct_extract(x1, 'col-8')) END)))) AS "attrs"`
	if err != nil || got != want {
		t.Errorf("nested:\n got %s (%v)\nwant %s", got, err, want)
	}

	// Files that hold another type than the schema's are refused.
	if _, err := projection(plain, map[string]*duckShape{"id": prim, "info": prim, "day": prim}, "", false); err == nil {
		t.Error("a struct column read from a primitive one")
	}
}

// testEngine is DuckDB with httpfs, for views that read through the proxy.
type testEngine struct{ connector *duckdb.Connector }

func newTestEngine(t *testing.T) *testEngine {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	connector, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		for _, stmt := range []string{"SET extension_directory = " + quoteLiteral(ext), "SET autoinstall_known_extensions = false", "LOAD httpfs"} {
			if _, err := execer.ExecContext(context.Background(), stmt, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connector.Close() })
	return &testEngine{connector: connector}
}

func (e *testEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c.(*duckdb.Conn), nil
}
func (e *testEngine) LocalDatabase() string { return "memory" }
func (e *testEngine) Catalog() string       { return "lake" }

// fakeProvider is a Delta Sharing server with one table, t in share s and
// schema sc, that answers in the delta format if delta is set, and serves
// the table's files from dir.
type fakeProvider struct {
	srv *httptest.Server
	dir string

	mu       sync.Mutex
	delta    bool
	version  int
	protocol string
	metadata string
	files    []string // file actions, without the server's URL (see url)
	// downloads counts requests for each file.
	downloads map[string]int
	asked     []string
}

func newFakeProvider(t *testing.T) *fakeProvider {
	p := &fakeProvider{dir: t.TempDir(), downloads: map[string]int{}}
	p.srv = httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.srv.Close)
	return p
}

// url is a file's URL, with a signature that changes with each listing.
func (p *fakeProvider) url(name string) string {
	return fmt.Sprintf("%s/files/%s?sig=v%d", p.srv.URL, name, p.version)
}

func (p *fakeProvider) serve(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if name, ok := strings.CutPrefix(r.URL.Path, "/files/"); ok {
		if r.Header.Get("Authorization") != "" {
			http.Error(w, "a file request carried the bearer token", http.StatusBadRequest)
			return
		}
		p.downloads[name]++
		http.ServeFile(w, r, filepath.Join(p.dir, name))
		return
	}
	if r.Header.Get("Authorization") != "Bearer tok" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	table := "/delta-sharing/shares/s/schemas/sc/tables/t/"
	caps := r.Header.Get(capabilitiesHeader)
	w.Header().Set("Delta-Table-Version", fmt.Sprint(p.version))
	switch strings.TrimPrefix(r.URL.Path, table) {
	case "version":
	case "metadata":
		p.asked = append(p.asked, "metadata "+caps)
		if p.delta {
			w.Header().Set(capabilitiesHeader, "responseformat=delta")
		}
		fmt.Fprintln(w, p.protocol)
		fmt.Fprintln(w, p.metadata)
	case "query":
		p.asked = append(p.asked, "query "+caps)
		if p.delta != (caps == askDelta) {
			http.Error(w, `{"errorCode":"INVALID_PARAMETER_VALUE","message":"wrong format"}`, http.StatusBadRequest)
			return
		}
		fmt.Fprintln(w, p.protocol)
		fmt.Fprintln(w, p.metadata)
		for _, f := range p.files {
			fmt.Fprintln(w, strings.ReplaceAll(f, "$URL/", p.srv.URL+"/files/"))
		}
	default:
		http.NotFound(w, r)
	}
}

// set changes the table, as a new version.
func (p *fakeProvider) set(delta bool, protocol, metadata string, files ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.version++
	p.delta, p.protocol, p.metadata = delta, protocol, metadata
	p.files = nil
	for _, f := range files {
		p.files = append(p.files, strings.ReplaceAll(f, "$SIG", fmt.Sprintf("v%d", p.version)))
	}
}

// deltaFile is a file action in the delta format.
func deltaFile(id, name, day string, size int64, dv string) string {
	add := map[string]any{"path": "$URL/" + name + "?sig=$SIG", "partitionValues": map[string]string{"col-4": day},
		"size": size, "modificationTime": 1, "dataChange": true}
	file := map[string]any{"id": id, "expirationTimestamp": time.Now().Add(time.Hour).UnixMilli(),
		"deltaSingleAction": map[string]any{"add": add}}
	if dv != "" {
		add["deletionVector"] = json.RawMessage(dv)
		// The provider's ID of the file of vectors, as Databricks gives it.
		var d deletionVector
		_ = json.Unmarshal([]byte(dv), &d)
		if path, ok := strings.CutPrefix(d.PathOrInline, "$URL/"); ok {
			file["deletionVectorFileId"] = "dvfile-" + strings.Split(path, "?")[0]
		}
	}
	b, _ := json.Marshal(map[string]any{"file": file})
	return string(b)
}

func TestExternalTableDeltaFormat(t *testing.T) {
	eng := newTestEngine(t)
	ctx := context.Background()
	db := sql.OpenDB(eng.connector)
	defer db.Close() //nolint:errcheck
	p := newFakeProvider(t)
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	size := func(name string) int64 {
		st, err := os.Stat(filepath.Join(p.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}

	// Files of a table under column mapping, as Delta writes them: with
	// physical names, field IDs, a dropped column (col-9) and a row
	// tracking column. f1 was written before info was added.
	ids := `(FIELD_IDS {'col-1': 1, 'col-2': {__duckdb_field_id: 2, 'col-3': 3}})`
	exec(`COPY (SELECT range AS "col-1", {'col-3': range::INTEGER * 10} AS "col-2", 'gone' AS "col-9", range AS "_row-id-col-x" FROM range(10)) TO ` +
		quoteLiteral(filepath.Join(p.dir, "f0.parquet")) + " " + strings.Replace(ids, "}})", "}, 'col-9': 9, '_row-id-col-x': 99})", 1))
	exec(`COPY (SELECT range + 10 AS "col-1" FROM range(10)) TO ` + quoteLiteral(filepath.Join(p.dir, "f1.parquet")) + ` (FIELD_IDS {'col-1': 1})`)
	exec(`COPY (SELECT range + 20 AS "col-1", {'col-3': (range + 20)::INTEGER * 10} AS "col-2" FROM range(10)) TO ` +
		quoteLiteral(filepath.Join(p.dir, "f2.parquet")) + " " + ids)

	// f0 deletes rows 0, 3, 4, 5 and 9 (vector in a file of vectors),
	// f1 rows 1 and 2 (inline).
	v0 := encodeVector([]uint64{0, 3, 4, 5, 9}, true)
	other := encodeVector([]uint64{7}, false)
	vectors, offsets := vectorFile(other, v0)
	if err := os.WriteFile(filepath.Join(p.dir, "dv.bin"), vectors, 0o600); err != nil {
		t.Fatal(err)
	}
	v1 := encodeVector([]uint64{1, 2}, false)
	storedDV := func(file string, offset int64, size, card int) string {
		return fmt.Sprintf(`{"storageType":"p","pathOrInlineDv":"$URL/%s?sig=$SIG","offset":%d,"sizeInBytes":%d,"cardinality":%d}`,
			file, offset, size, card)
	}
	inlineDV := fmt.Sprintf(`{"storageType":"i","pathOrInlineDv":%q,"sizeInBytes":%d,"cardinality":2}`, z85Encode(v1), len(v1))
	protocol := `{"protocol":{"deltaProtocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors","columnMapping"]}}}`
	metadata := func(mode string) string {
		schema, _ := json.Marshal(mappedSchema)
		return `{"metaData":{"deltaMetadata":{"id":"t1","format":{"provider":"parquet","options":{}},"schemaString":` + string(schema) +
			`,"partitionColumns":["day"],"configuration":{"delta.columnMapping.mode":"` + mode + `","delta.enableDeletionVectors":"true"}}}}`
	}
	p.set(true, protocol, metadata("name"),
		deltaFile("f0", "f0.parquet", "2026-10-01", size("f0.parquet"), storedDV("dv.bin", offsets[1], len(v0), 5)),
		deltaFile("f1", "f1.parquet", "2026-10-02", size("f1.parquet"), inlineDV),
		deltaFile("f2", "f2.parquet", "2026-10-01", size("f2.parquet"), ""))

	proxy, err := NewProxy(nil)
	if err != nil {
		t.Fatal(err)
	}
	go proxy.Serve()    //nolint:errcheck
	defer proxy.Close() //nolint:errcheck
	conn, err := eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := &Consumer{proxy: proxy, eng: eng, conn: conn, log: slog.New(slog.DiscardHandler),
		providers: map[string]*client{}, tables: map[string]*external{}}
	defer c.close() //nolint:errcheck
	c.providers["p"] = c.newClient(Provider{Endpoint: p.srv.URL + "/delta-sharing", Token: "tok"})
	tbl := &external{def: ExternalTable{Schema: "partners", Name: "viewing", Provider: "p", Share: "s", RemoteSchema: "sc", RemoteTable: "t"},
		key: "partners.viewing"}
	c.known.Store(&map[string]*external{tbl.key: tbl})
	proxy.refresh = func(ctx context.Context, table string) error { return c.refresh(ctx, tbl, true) }

	rows := func(q string) string {
		t.Helper()
		rs, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rs.Close() //nolint:errcheck
		cols, _ := rs.Columns()
		out := []string{strings.Join(cols, ",")}
		for rs.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rs.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			var parts []string
			for _, v := range vals {
				if tm, ok := v.(time.Time); ok {
					v = tm.Format(time.DateOnly)
				}
				parts = append(parts, fmt.Sprint(v))
			}
			out = append(out, strings.Join(parts, "|"))
		}
		if err := rs.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(out, "\n")
	}

	if err := c.refresh(ctx, tbl, true); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(p.asked, "\n"), "metadata "+offerFormats+"\nquery "+askDelta; got != want {
		t.Errorf("requests:\n%s\nwant\n%s", got, want)
	}
	// The table's columns by their names, without the dropped and row
	// tracking columns; f0's and f1's deleted rows left out.
	want := "id,info,day\n" +
		"1|map[x:10]|2026-10-01\n2|map[x:20]|2026-10-01\n6|map[x:60]|2026-10-01\n7|map[x:70]|2026-10-01\n8|map[x:80]|2026-10-01\n" +
		"10|<nil>|2026-10-02\n13|<nil>|2026-10-02\n14|<nil>|2026-10-02\n15|<nil>|2026-10-02\n16|<nil>|2026-10-02\n" +
		"17|<nil>|2026-10-02\n18|<nil>|2026-10-02\n19|<nil>|2026-10-02\n" +
		"20|map[x:200]|2026-10-01\n21|map[x:210]|2026-10-01\n22|map[x:220]|2026-10-01\n23|map[x:230]|2026-10-01\n" +
		"24|map[x:240]|2026-10-01\n25|map[x:250]|2026-10-01\n26|map[x:260]|2026-10-01\n27|map[x:270]|2026-10-01\n" +
		"28|map[x:280]|2026-10-01\n29|map[x:290]|2026-10-01"
	if got := rows("SELECT * FROM memory.partners.viewing ORDER BY id"); got != want {
		t.Errorf("rows:\n%s\nwant\n%s", got, want)
	}
	if n := tbl.scans.Load(); n != 3 {
		t.Errorf("%d scans, want 3", n)
	}
	// Partition pruning leaves the rows of the remaining files right.
	if got := rows("SELECT count(*) AS n, min(id) AS lo, sum(id) AS s FROM memory.partners.viewing WHERE day = DATE '2026-10-02'"); got != "n,lo,s\n8|10|122" {
		t.Errorf("one partition: %s", got)
	}
	if got := rows("SELECT count(*) FROM memory.partners.viewing WHERE id IN (0, 3, 9, 11, 12, 20)"); got != "count_star()\n1" {
		t.Errorf("deleted rows by key: %s", got)
	}

	// A new version deletes another row of f0, from a new file of
	// vectors; the unchanged inline vector is not read again, nor is
	// dv.bin.
	v0b := encodeVector([]uint64{0, 1, 3, 4, 5, 9}, false)
	vectors2, offsets2 := vectorFile(v0b)
	if err := os.WriteFile(filepath.Join(p.dir, "dv2.bin"), vectors2, 0o600); err != nil {
		t.Fatal(err)
	}
	p.set(true, protocol, metadata("name"),
		deltaFile("f0", "f0.parquet", "2026-10-01", size("f0.parquet"), storedDV("dv2.bin", offsets2[0], len(v0b), 6)),
		deltaFile("f1", "f1.parquet", "2026-10-02", size("f1.parquet"), inlineDV),
		deltaFile("f2", "f2.parquet", "2026-10-01", size("f2.parquet"), ""))
	if err := c.refresh(ctx, tbl, true); err != nil {
		t.Fatal(err)
	}
	if got := rows("SELECT count(*) AS n, min(id) AS lo FROM memory.partners.viewing"); got != "n,lo\n22|2" {
		t.Errorf("after a new deletion: %s", got)
	}
	if p.downloads["dv.bin"] != 1 || p.downloads["dv2.bin"] != 1 {
		t.Errorf("vector downloads %v", p.downloads)
	}

	// Column mapping mode id: the same, as the files' field IDs agree
	// with their names.
	p.set(true, protocol, metadata("id"),
		deltaFile("f0", "f0.parquet", "2026-10-01", size("f0.parquet"), storedDV("dv2.bin", offsets2[0], len(v0b), 6)),
		deltaFile("f2", "f2.parquet", "2026-10-01", size("f2.parquet"), ""))
	if err := c.refresh(ctx, tbl, true); err != nil {
		t.Fatal(err)
	}
	if got := rows("SELECT count(*) AS n, sum(info.x) AS s FROM memory.partners.viewing"); got != "n,s\n14|2680" {
		t.Errorf("mode id: %s", got)
	}
	// A file whose col-1 has info's field ID is refused, and the view
	// stays as it was.
	exec(`COPY (SELECT range + 30 AS "col-1" FROM range(10)) TO ` + quoteLiteral(filepath.Join(p.dir, "f3.parquet")) + ` (FIELD_IDS {'col-1': 2})`)
	p.set(true, protocol, metadata("id"),
		deltaFile("f2", "f2.parquet", "2026-10-01", size("f2.parquet"), ""),
		deltaFile("f3", "f3.parquet", "2026-10-01", size("f3.parquet"), ""))
	if err := c.refresh(ctx, tbl, true); err == nil || !strings.Contains(err.Error(), "field ID") {
		t.Errorf("mismatched field IDs: %v", err)
	}
	if got := rows("SELECT count(*) FROM memory.partners.viewing"); got != "count_star()\n14" {
		t.Errorf("after a refused listing: %s", got)
	}

	// A vector that deletes other rows than listed fails the listing.
	p.set(true, protocol, metadata("name"),
		deltaFile("f0", "f0.parquet", "2026-10-01", size("f0.parquet"), storedDV("dv2.bin", offsets2[0], len(v0b), 7)))
	if err := c.refresh(ctx, tbl, true); err == nil || !strings.Contains(err.Error(), "deletes 6 rows") {
		t.Errorf("a wrong cardinality: %v", err)
	}
	// So does a feature Igloo does not read.
	p.set(true, `{"protocol":{"deltaProtocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors","typeWidening"]}}}`,
		metadata("name"), deltaFile("f2", "f2.parquet", "2026-10-01", size("f2.parquet"), ""))
	if err := c.refresh(ctx, tbl, true); err == nil || !strings.Contains(err.Error(), "typeWidening") {
		t.Errorf("an unknown reader feature: %v", err)
	}

	// The parquet format: plain files, read by name, a column the files
	// add (row tracking's) left out, a column they lack read as NULL.
	exec(`COPY (SELECT range AS id, 'x' || range AS name, range AS "_row-id" FROM range(3)) TO ` + quoteLiteral(filepath.Join(p.dir, "p0.parquet")))
	parquetFile := func(name string) string {
		return fmt.Sprintf(`{"file":{"url":"$URL/%s?sig=$SIG","id":%q,"partitionValues":{},"size":%d}}`, name, name, size(name))
	}
	p.asked = nil
	p.set(false, `{"protocol":{"minReaderVersion":1}}`,
		`{"metaData":{"id":"t1","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[`+
			`{\"name\":\"id\",\"type\":\"long\",\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"metadata\":{}},`+
			`{\"name\":\"added\",\"type\":\"double\",\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}`,
		parquetFile("p0.parquet"))
	if err := c.refresh(ctx, tbl, true); err != nil {
		t.Fatal(err)
	}
	if got := rows("SELECT * FROM memory.partners.viewing ORDER BY id"); got != "id,name,added\n0|x0|<nil>\n1|x1|<nil>\n2|x2|<nil>" {
		t.Errorf("parquet format:\n%s", got)
	}
	if got, want := strings.Join(p.asked, "\n"), "metadata "+offerFormats+"\nquery "+askParquet; got != want {
		t.Errorf("requests:\n%s\nwant\n%s", got, want)
	}
	if n := tbl.scans.Load(); n != 1 {
		t.Errorf("%d scans, want 1", n)
	}
}

// TestDeltaFormatAnswers checks what Igloo accepts of the delta format.
func TestDeltaFormatAnswers(t *testing.T) {
	p := newFakeProvider(t)
	g := newGuardedDialer()
	g.allow(endpointHostPort(p.srv.URL))
	cl := &client{endpoint: p.srv.URL + "/delta-sharing", token: "tok", http: newHTTPClient(g)}
	protocol := `{"protocol":{"deltaProtocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"]}}}`
	metadata := `{"metaData":{"deltaMetadata":{"id":"t1","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[]}",` +
		`"partitionColumns":[],"configuration":{}}}}`
	query := func() (*remoteSnapshot, error) {
		return cl.query(context.Background(), "s", "sc", "t", time.Now())
	}

	p.set(true, protocol, metadata, deltaFile("f0", "f0.parquet", "", 10,
		`{"storageType":"p","pathOrInlineDv":"$URL/dv.bin?sig=$SIG","offset":1,"sizeInBytes":40,"cardinality":3}`))
	snap, err := query()
	if err != nil {
		t.Fatal(err)
	}
	f := snap.files[0]
	if snap.format != formatDelta || snap.version != int64(p.version) || f.ID != "f0" || !strings.HasPrefix(f.URL, p.srv.URL+"/files/f0.parquet?sig=") ||
		f.DV == nil || f.DV.fileID != "dvfile-dv.bin" || f.DV.offset() != 1 || f.DV.Cardinality != 3 || *f.PartitionValues["col-4"] != "" {
		t.Errorf("snapshot %+v, file %+v", snap, f)
	}

	for name, c := range map[string]struct {
		protocol, metadata, file string
		want                     string
	}{
		"reader version 4": {`{"protocol":{"deltaProtocol":{"minReaderVersion":4}}}`, metadata, "", "protocol version 4"},
		"unknown feature": {`{"protocol":{"deltaProtocol":{"minReaderVersion":3,"readerFeatures":["deletionVectors","variantType"]}}}`,
			metadata, "", "variantType"},
		"a removed file": {protocol, metadata, `{"file":{"id":"f0","deltaSingleAction":{"remove":{"path":"x"}}}}`, "other than an added file"},
		"a relative vector": {protocol, metadata, deltaFile("f0", "f0.parquet", "", 10,
			`{"storageType":"u","pathOrInlineDv":"ab^7j0abcdefghijklmn","offset":1,"sizeInBytes":40,"cardinality":3}`), "path in the table's storage"},
		"a file without a URL": {protocol, metadata, `{"file":{"id":"f0","deltaSingleAction":{"add":{"path":"part-0.parquet","size":1}}}}`, "http(s)"},
		"mapping mode": {protocol, strings.Replace(metadata, `"configuration":{}`, `"configuration":{"delta.columnMapping.mode":"pos"}`, 1), "",
			`mode "pos"`},
		"not parquet": {protocol, strings.Replace(metadata, `"provider":"parquet"`, `"provider":"orc"`, 1), "", "orc"},
	} {
		var files []string
		if c.file != "" {
			files = []string{c.file}
		}
		p.set(true, c.protocol, c.metadata, files...)
		if _, err := query(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want an error about %q", name, err, c.want)
		}
	}

	// Column mapping in the parquet format is refused, as are reader
	// versions above 1.
	p.set(false, `{"protocol":{"minReaderVersion":1}}`,
		`{"metaData":{"id":"t1","format":{"provider":"parquet"},"schemaString":"{}","configuration":{"delta.columnMapping.mode":"name"}}}`)
	if _, err := query(); err == nil || !strings.Contains(err.Error(), "column mapping") {
		t.Errorf("column mapping in the parquet format: %v", err)
	}
	p.set(false, `{"protocol":{"minReaderVersion":2}}`, `{"metaData":{"id":"t1","schemaString":"{}"}}`)
	if _, err := query(); err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Errorf("reader version 2 in the parquet format: %v", err)
	}

	// A server without the capabilities header shows the format in its
	// protocol action.
	bare := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, protocol)
	}))
	defer bare.Close()
	g.allow(endpointHostPort(bare.URL))
	if f, err := (&client{endpoint: bare.URL, token: "tok", http: newHTTPClient(g)}).format(context.Background(), "s", "sc", "t"); err != nil || f != formatDelta {
		t.Errorf("format without the header: %q, %v", f, err)
	}
}
