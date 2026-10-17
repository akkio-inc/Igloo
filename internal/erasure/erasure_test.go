package erasure

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// testCatalog keeps Iceberg tables in a local directory, as Lakekeeper
// keeps them in object storage.
type testCatalog struct {
	mu     sync.Mutex
	tables map[string]*table.Table
	// before and after, if set, are called around each commit, outside
	// the lock, so they may commit too.
	before func(id table.Identifier, updates []table.Update)
	after  func(id table.Identifier, meta table.Metadata)
	// io, if set, are the file IOs of tables, by name; others use the
	// local file system.
	io map[string]iceio.IO
}

func localFS(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }

// fs is the file IO of table name.
func (c *testCatalog) fs(name string) func(context.Context) (iceio.IO, error) {
	if fsys, ok := c.io[name]; ok {
		return func(context.Context) (iceio.IO, error) { return fsys, nil }
	}
	return localFS
}

// reopen loads table name again with its file IO.
func (c *testCatalog) reopen(t *testing.T, name string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	tbl := c.tables[name]
	c.tables[name] = table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), c.fs(name), c)
}

func (c *testCatalog) create(t *testing.T, dir, name string, schema *iceberg.Schema) *table.Table {
	t.Helper()
	location := filepath.Join(dir, "warehouse", strings.ReplaceAll(name, ".", "/"))
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident(name), meta, c.write(t, location, meta), localFS, c)
	c.mu.Lock()
	c.tables[name] = tbl
	c.mu.Unlock()
	return tbl
}

// write writes a table's metadata file, as a catalog does at each commit.
func (c *testCatalog) write(t testing.TB, location string, meta table.Metadata) string {
	b, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(location, "metadata", fmt.Sprintf("%05d-%d.metadata.json", meta.LastSequenceNumber(), time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func (c *testCatalog) Tables(context.Context) ([]table.Identifier, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []table.Identifier
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for n := range c.tables {
			if !yield(n) {
				return
			}
		}
	}) {
		out = append(out, ident(name))
	}
	return out, nil
}

func (c *testCatalog) LoadTable(_ context.Context, id table.Identifier) (*table.Table, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tbl, ok := c.tables[tableName(id)]
	if !ok {
		return nil, icecat.ErrNoSuchTable
	}
	return tbl, nil
}

func (c *testCatalog) CommitTable(ctx context.Context, id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if c.before != nil {
		c.before(id, updates)
	}
	meta, loc, err := c.commit(id, reqs, updates)
	if err == nil && c.after != nil {
		c.after(id, meta)
	}
	return meta, loc, err
}

func (c *testCatalog) commit(id table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, ok := c.tables[tableName(id)]
	if !ok {
		return nil, "", icecat.ErrNoSuchTable
	}
	for _, r := range reqs {
		if err := r.Validate(cur.Metadata()); err != nil {
			return nil, "", fmt.Errorf("%w: %v", table.ErrCommitFailed, err)
		}
	}
	b, err := table.MetadataBuilderFromBase(cur.Metadata(), cur.MetadataLocation())
	if err != nil {
		return nil, "", err
	}
	for _, u := range updates {
		if err := u.Apply(b); err != nil {
			return nil, "", err
		}
	}
	meta, err := b.Build()
	if err != nil {
		return nil, "", err
	}
	loc := c.write(nopTB{}, meta.Location(), meta)
	c.tables[tableName(id)] = table.New(id, meta, loc, c.fs(tableName(id)), c)
	return meta, loc, nil
}

type nopTB struct{ testing.TB }

func (nopTB) Fatal(args ...any) { panic(fmt.Sprint(args...)) }

// duckdbEngine opens connections to an in-memory DuckDB.
type duckdbEngine struct{ c *duckdb.Connector }

func (e duckdbEngine) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := e.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c.(*duckdb.Conn), nil
}

func newEngine(t *testing.T) duckdbEngine {
	t.Helper()
	c, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return duckdbEngine{c}
}

func (e duckdbEngine) exec(t *testing.T, stmt string) {
	t.Helper()
	conn, err := e.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(context.Background(), stmt, nil); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

func (e duckdbEngine) values(t *testing.T, stmt string) []string {
	t.Helper()
	conn, err := e.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	var out []string
	err = query(context.Background(), conn, stmt, func(row []driver.Value) error {
		for _, v := range row {
			out = append(out, fmt.Sprint(v))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	return out
}

// testRows deletes and counts rows with iceberg-go, by the raw values the
// test knows, standing in for DuckDB's DELETE on the catalog. A table it
// has no expression for has no rows the DELETE can name, as when a column
// was renamed by another engine.
type testRows map[string]iceberg.BooleanExpression

func (r testRows) expr(tbl *table.Table) iceberg.BooleanExpression {
	if e, ok := r[tableName(tbl.Identifier())]; ok {
		return e
	}
	return iceberg.AlwaysFalse{}
}

func (r testRows) Delete(ctx context.Context, tbl *table.Table, _ string) (int64, error) {
	n, err := r.Count(ctx, tbl, "")
	if err != nil || n == 0 {
		return n, err
	}
	_, err = tbl.Delete(ctx, r.expr(tbl), nil)
	return n, err
}

func (r testRows) Count(ctx context.Context, tbl *table.Table, _ string) (int64, error) {
	if tbl.CurrentSnapshot() == nil {
		return 0, nil
	}
	rows, err := tbl.Scan(table.WithRowFilter(r.expr(tbl))).ToArrowTable(ctx)
	if err != nil {
		return 0, err
	}
	defer rows.Release()
	return rows.NumRows(), nil
}

// readCache counts how often it is emptied.
type readCache struct {
	mu      sync.Mutex
	emptied int
}

func (c *readCache) EmptyCache(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emptied++
	return nil
}

// ident is the identifier of a table the tests name with dots.
func ident(name string) table.Identifier { return strings.Split(name, ".") }

// testKey is the erasure key of the tests.
var testKey = []byte("an erasure key of thirty-two bytes")

var peopleSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 3, Name: "device_id", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String},
)

var audienceSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "segment", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 2, Name: "email_sha256", Type: iceberg.PrimitiveTypes.String},
)

// appendRows appends rows, each as many strings or int64s as the schema
// has columns, in one commit.
func appendRows(t *testing.T, tbl *table.Table, rows ...[]any) *table.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			switch v := v.(type) {
			case int64:
				b.Field(i).(*array.Int64Builder).Append(v)
			case string:
				b.Field(i).(*array.StringBuilder).Append(v)
			}
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	arrTable := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer arrTable.Release()
	out, err := tbl.AppendTable(context.Background(), arrTable, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The identifiers of the person a request erases, as rows hold them, and
// as the request names them.
const (
	storedEmail     = "Subject@Example.COM"
	requestedEmail  = "  subject@example.com"
	storedDevice    = "DEV-SUBJECT-0001"
	requestedDevice = "dev-subject-0001"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestErasure erases a person from two tables, one of which holds hashes of
// emails, and a clone, and then looks for them in every byte of every
// file left: raw, in decompressed Avro manifests, and in Parquet values and
// statistics.
func TestErasure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	warehouse := filepath.Join(dir, "warehouse") + "/"
	eng := newEngine(t)
	cat := &testCatalog{tables: map[string]*table.Table{}}

	people := cat.create(t, dir, "crm.people", peopleSchema)
	// A snapshot from before the person's data arrived, which survives.
	people = appendRows(t, people, []any{int64(1), "ann@other.com", "dev-ann", "keep"}, []any{int64(2), "bob@other.com", "dev-bob", "keep"})
	before := people.CurrentSnapshot().SnapshotID
	people = appendRows(t, people,
		[]any{int64(3), storedEmail, "dev-x", "erase by email"},
		[]any{int64(4), "carl@other.com", storedDevice, "erase by device"},
		[]any{int64(5), "dora@other.com", "dev-dora", "keep"})
	people = appendRows(t, people, []any{int64(6), "eve@other.com", "dev-eve", "keep"})

	audience := cat.create(t, dir, "crm.audience", audienceSchema)
	appendRows(t, audience, []any{"sports", sha256Hex("subject@example.com")}, []any{"sports", sha256Hex("ann@other.com")})

	// A clone: another table with the source's data files.
	clone := cat.create(t, dir, "crm.people_clone", peopleSchema)
	tf, err := readFiles(ctx, people)
	if err != nil {
		t.Fatal(err)
	}
	var shared []iceberg.DataFile
	for _, df := range tf.live(true) {
		shared = append(shared, df)
	}
	tx := clone.NewTransaction()
	if err := tx.AddDataFiles(ctx, shared, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// A Delta Sharing copy of a data file, an old orphan file from a write
	// that failed, and a recent one, which may be a write in progress.
	people, _ = cat.LoadTable(ctx, ident("crm.people"))
	copyPath := filepath.Join(warehouse, sharingDir, people.Metadata().TableUUID().String(), "copy.parquet")
	if err := os.MkdirAll(filepath.Dir(copyPath), 0o755); err != nil {
		t.Fatal(err)
	}
	eng.exec(t, fmt.Sprintf("COPY (SELECT 3 AS id, '%s' AS email) TO '%s' (FORMAT parquet)", storedEmail, copyPath))
	oldOrphan := filepath.Join(people.Location(), "data", "failed-write.parquet")
	eng.exec(t, fmt.Sprintf("COPY (SELECT 9 AS id, '%s' AS email) TO '%s' (FORMAT parquet)", storedEmail, oldOrphan))
	if err := os.Chtimes(oldOrphan, time.Now().Add(-4*24*time.Hour), time.Now().Add(-4*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	newOrphan := filepath.Join(people.Location(), "data", "in-progress.parquet")
	eng.exec(t, fmt.Sprintf("COPY (SELECT 10 AS id, '%s' AS device_id) TO '%s' (FORMAT parquet)", storedDevice, newOrphan))

	policies := policy.NewMemoryStore()
	err = policies.Update(ctx, func(c *policy.Catalog) error {
		now := time.Now()
		if err := c.PutTag(policy.Tag{Name: "pii", AllowedValues: []string{"email", "email:sha256", "device_id"}}, false, now); err != nil {
			return err
		}
		for _, ct := range []policy.ColumnTag{
			{Schema: "crm", Table: "people", Column: "email", Tag: "pii", Value: "email"},
			{Schema: "crm", Table: "people", Column: "device_id", Tag: "pii", Value: "device_id"},
			{Schema: "crm", Table: "audience", Column: "email_sha256", Tag: "pii", Value: "email:sha256"},
		} {
			if err := c.SetColumnTag(ct, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	queries := audit.NewMemoryStore(100, 100)
	now := time.Now()
	_ = queries.WriteQueries(ctx, []audit.Query{
		{ID: "q1", User: "ana", Start: now, Outcome: audit.OutcomeOK, StatementType: "SELECT",
			Text: "SELECT * FROM crm.people WHERE lower(email) = 'subject@example.com'"},
		{ID: "q2", User: "ana", Start: now, Outcome: audit.OutcomeOK, StatementType: "SELECT",
			Text: `SELECT * FROM crm.people WHERE note = 'keep' AND payload = '{"device": "DEV-SUBJECT-0001"}'`},
		{ID: "q3", User: "ana", Start: now, Outcome: audit.OutcomeOK, StatementType: "CREATE TABLE",
			Text: "CREATE TABLE crm.report AS SELECT note, count(*) FROM crm.people GROUP BY note"},
	})
	loads := tasks.NewMemoryStore()
	_ = loads.WriteLoads(ctx, []tasks.Load{{ID: "l1", Table: "lake.crm.people", File: "s3://drops/a.csv", Status: tasks.LoadFailed,
		FirstError: "Could not convert string 'Subject@Example.COM' to INT64", Start: now}})

	// Before, the search finds the person in data files and in manifests,
	// whose bounds hold the device ID whole: it is 16 characters long.
	needles := []string{storedEmail, storedDevice, sha256Hex("subject@example.com"), sha256Hex(storedEmail)}
	found := strings.Join(search(t, eng, dir, needles), "\n")
	for _, want := range []string{".parquet (values): " + storedEmail, ".avro (decompressed): " + storedDevice} {
		if !strings.Contains(found, want) {
			t.Fatalf("before erasing, the search does not find %q:\n%s", want, found)
		}
	}

	store := NewMemoryStore()
	cache := &readCache{}
	svc := New(Options{
		Store: store, Catalog: cat, Policies: policies.Load, Engine: eng, Key: testKey, CatalogName: "lake", Warehouse: warehouse,
		History: []History{queries, loads}, Lineage: queries,
		Purger: NewPurger(store, cache, MemoryCache+"test", "test", slog.New(slog.DiscardHandler)),
		Rows: testRows{
			"crm.people": iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("email"), storedEmail),
				iceberg.EqualTo(iceberg.Reference("device_id"), storedDevice)),
			"crm.people_clone": iceberg.NewOr(iceberg.EqualTo(iceberg.Reference("email"), storedEmail),
				iceberg.EqualTo(iceberg.Reference("device_id"), storedDevice)),
			"crm.audience": iceberg.EqualTo(iceberg.Reference("email_sha256"), sha256Hex("subject@example.com")),
		},
	})

	for _, bad := range [][]Wanted{
		{{Tag: "pii", Value: "phone", Identifiers: []string{"555"}}},
		{{Tag: "nope", Value: "email", Identifiers: []string{"a@b.com"}}},
		{{Tag: "pii", Value: "email", Identifiers: []string{"   "}}},
	} {
		if _, err := svc.Submit(ctx, "privacy", bad); err == nil {
			t.Errorf("Submit(%v) succeeded", bad)
		}
	}
	r, err := svc.Submit(ctx, "privacy", []Wanted{
		{Tag: "pii", Value: "email", Identifiers: []string{requestedEmail, "SUBJECT@example.com"}},
		{Tag: "pii", Value: "device_id", Identifiers: []string{requestedDevice}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Subjects[0].Count != 1 {
		t.Errorf("identifiers that compare equal count once: %+v", r.Subjects[0])
	}
	doc, _ := json.Marshal(r)
	if bytes.Contains(bytes.ToLower(doc), []byte("subject@example")) || bytes.Contains(bytes.ToLower(doc), []byte(requestedDevice)) {
		t.Fatalf("the request's record holds an identifier: %s", doc)
	}

	// The recent orphan holds the person's device ID: the request waits
	// until it is old enough to delete, then goes on.
	run := func() *Request {
		t.Helper()
		if err := svc.Run(ctx, r.ID); err != nil {
			t.Fatalf("run: %v", err)
		}
		got, err := store.Get(ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := run()
	if got.Status != StatusWaiting || got.Step != StepPurge {
		t.Fatalf("with a recent orphan: status %s at %s, want waiting at purge (error %q)", got.Status, got.Step, got.Error)
	}
	if _, err := os.Stat(oldOrphan); !os.IsNotExist(err) {
		t.Errorf("the old orphan is still there: %v", err)
	}
	later := got.WaitUntil.Add(time.Minute)
	svc.now = func() time.Time { return later }
	if got = run(); got.Status != StatusCompleted {
		t.Fatalf("status %s at %s, error %q, verification %+v", got.Status, got.Step, got.Error, got.Verification)
	}

	v := got.Verification
	if v == nil || v.Rows+v.Files+v.Bounds+v.Statistics+v.Storage+v.Orphans+v.Sharing+v.History != 0 || v.Caches != 1 {
		t.Errorf("verification: %+v", v)
	}
	if got.Salt != "" || got.Sealed != "" {
		t.Errorf("a completed request keeps its identifiers or salt: %q %q", got.Salt, got.Sealed)
	}
	if files, _ := store.Files(ctx, got.ID); len(files) > 0 {
		t.Errorf("a completed request keeps the paths of the files it deleted: %+v", files)
	}
	tables := map[string]Table{}
	for _, tt := range got.Tables {
		tables[tt.Name] = tt
	}
	if tables["crm.people"].RowsDeleted != 2 || tables["crm.audience"].RowsDeleted != 1 || tables["crm.people_clone"].RowsDeleted != 2 {
		t.Errorf("rows deleted: %+v", got.Tables)
	}
	if !strings.HasPrefix(tables["crm.people_clone"].Reason, "shares data files") {
		t.Errorf("the clone was not found by its files: %+v", got.Tables)
	}
	for _, name := range []string{"crm.people", "crm.people_clone", "crm.audience"} {
		if tt := tables[name]; tt.FilesRewritten == 0 || tt.SnapshotsExpired == 0 || len(tt.Steps) < 4 {
			t.Errorf("%s was not rewritten and expired: %+v", name, tt)
		}
	}
	if tables["crm.people"].FilesDeleted == 0 || got.HistoryRedacted != 3 {
		t.Errorf("files deleted %d, history records redacted %d", tables["crm.people"].FilesDeleted, got.HistoryRedacted)
	}
	t.Logf("tables: %+v\nsteps: %+v", got.Tables, got.Steps)
	if len(got.Downstream) != 1 || got.Downstream[0].Table != "crm.report" || got.Downstream[0].Erased {
		t.Errorf("downstream: %+v", got.Downstream)
	}

	// Everything else is still there, and so is the snapshot from before.
	for name, want := range map[string]int64{"crm.people": 4, "crm.people_clone": 4, "crm.audience": 1} {
		tbl, _ := cat.LoadTable(ctx, ident(name))
		rows, err := tbl.Scan().ToArrowTable(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if rows.NumRows() != want {
			t.Errorf("%s has %d rows, want %d", name, rows.NumRows(), want)
		}
		rows.Release()
	}
	people, _ = cat.LoadTable(ctx, ident("crm.people"))
	if s := people.SnapshotByID(before); s == nil {
		t.Errorf("the snapshot from before the person's data was expired")
	} else if rows, err := people.Scan(table.WithSnapshotID(before)).ToArrowTable(ctx); err != nil || rows.NumRows() != 2 {
		t.Errorf("time travel to before: %v rows, %v", rows, err)
	}

	// Not a trace in storage. The same search finds a row that stays, so
	// it would find the person's.
	if found := search(t, eng, dir, needles); len(found) > 0 {
		t.Errorf("identifiers left in storage:\n%s", strings.Join(found, "\n"))
	}
	if found := search(t, eng, dir, []string{"dora@other.com", sha256Hex("ann@other.com")}); len(found) < 2 {
		t.Errorf("the search does not find rows that stay: %v", found)
	}
	for _, p := range []string{copyPath, newOrphan} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s is still there: %v", p, err)
		}
	}
	if cache.emptied == 0 {
		t.Errorf("the read cache was not emptied")
	}

	// Nor in history, whose other records stay as they were.
	recorded, _ := queries.Queries(ctx, audit.Viewer{All: true}, audit.Page{})
	for _, q := range recorded {
		lower := strings.ToLower(q.Text)
		if strings.Contains(lower, "subject@example.com") || strings.Contains(lower, "dev-subject") {
			t.Errorf("history still holds an identifier: %s", q.Text)
		}
		if q.ID == "q3" && !strings.HasPrefix(q.Text, "CREATE TABLE crm.report") {
			t.Errorf("an unrelated statement changed: %s", q.Text)
		}
	}
	if l, _ := loads.Loads(ctx, audit.Viewer{All: true}, audit.Page{}); len(l) != 1 || strings.Contains(l[0].FirstError, "Example") {
		t.Errorf("the load history still holds an identifier: %+v", l)
	}

	// A partner's opt-out list holds only hashes: a request for the hashed
	// tag value erases by them, from the columns of that value alone.
	svc.rows.(testRows)["crm.audience"] = iceberg.EqualTo(iceberg.Reference("email_sha256"), sha256Hex("ann@other.com"))
	r2, err := svc.Submit(ctx, "privacy", []Wanted{{Tag: "pii", Value: "email:sha256",
		Identifiers: []string{strings.ToUpper(sha256Hex("ann@other.com"))}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(ctx, r2.ID); err != nil {
		t.Fatal(err)
	}
	// It matches the table's one row left: a request that would empty a
	// table waits for someone other than its requester, whatever the
	// table's size.
	if got, _ := store.Get(ctx, r2.ID); got.Status != StatusHeld {
		t.Fatalf("a request that empties a table: %s", got.Status)
	}
	if _, err := svc.Confirm(ctx, r2.ID, "privacy"); !errors.Is(err, ErrInvalid) {
		t.Errorf("its requester confirmed it: %v", err)
	}
	if _, err := svc.Confirm(ctx, r2.ID, "pat"); err != nil {
		t.Fatal(err)
	}
	if err := svc.Run(ctx, r2.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Get(ctx, r2.ID); got.Status != StatusCompleted || len(got.Tables) != 1 || got.Tables[0].RowsDeleted != 1 {
		t.Errorf("by hash: %s, error %q, tables %+v", got.Status, got.Error, got.Tables)
	}
	if found := search(t, eng, dir, []string{sha256Hex("ann@other.com")}); len(found) > 0 {
		t.Errorf("the hash is still in storage: %v", found)
	}
}

// search returns where in the files under dir any of needles appears,
// compared without regard to case: in raw bytes, in Avro files'
// decompressed blocks, and in Parquet files' values and statistics.
func search(t *testing.T, eng duckdbEngine, dir string, needles []string) []string {
	t.Helper()
	var found []string
	check := func(where string, b []byte) {
		for _, n := range needles {
			if bytes.Contains(bytes.ToLower(b), []byte(strings.ToLower(n))) {
				found = append(found, where+": "+n)
			}
		}
	}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		check(p, b)
		switch {
		case bytes.HasPrefix(b, []byte("Obj\x01")):
			blocks, err := avroBlocks(b)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			for _, blk := range blocks {
				check(p+" (decompressed)", blk)
			}
		case bytes.HasPrefix(b, []byte("PAR1")):
			check(p+" (values)", []byte(strings.Join(eng.values(t, "SELECT * FROM read_parquet('"+p+"')"), "\n")))
			check(p+" (statistics)", []byte(strings.Join(eng.values(t,
				"SELECT stats_min_value, stats_max_value, stats_min, stats_max FROM parquet_metadata('"+p+"')"), "\n")))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// avroBlocks returns an Avro object container file's blocks, decompressed.
func avroBlocks(b []byte) ([][]byte, error) {
	r := bytes.NewReader(b[4:])
	long := func() int64 {
		v, _ := binary.ReadUvarint(r)
		return int64(v>>1) ^ -int64(v&1)
	}
	next := func(n int64) []byte {
		out := make([]byte, n)
		_, _ = io.ReadFull(r, out)
		return out
	}
	meta := map[string][]byte{}
	for {
		n := long()
		if n == 0 {
			break
		}
		if n < 0 {
			n = -n
			long()
		}
		for range n {
			k := next(long())
			meta[string(k)] = next(long())
		}
	}
	sync := next(16)
	var out [][]byte
	for r.Len() > 0 {
		long()
		data := next(long())
		if !bytes.Equal(next(16), sync) {
			return nil, fmt.Errorf("bad sync marker")
		}
		switch codec := string(meta["avro.codec"]); codec {
		case "", "null":
			out = append(out, data)
		case "deflate":
			plain, err := io.ReadAll(flate.NewReader(bytes.NewReader(data)))
			if err != nil {
				return nil, err
			}
			out = append(out, plain)
		default:
			return nil, fmt.Errorf("codec %s", codec)
		}
	}
	return out, nil
}
