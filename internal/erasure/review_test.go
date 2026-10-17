package erasure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// fixture is a catalog of tables in a directory, with policies, and a
// service that erases from them.
type fixture struct {
	t        *testing.T
	ctx      context.Context
	dir      string
	eng      duckdbEngine
	cat      *testCatalog
	policies *policy.MemoryStore
	store    *MemoryStore
	cache    *readCache
	rows     testRows
	metrics  *Metrics
	svc      *Service
}

func newFixture(t *testing.T) *fixture {
	f := &fixture{t: t, ctx: context.Background(), dir: t.TempDir(), eng: newEngine(t), cat: &testCatalog{tables: map[string]*table.Table{}},
		policies: policy.NewMemoryStore(), store: NewMemoryStore(), cache: &readCache{}, rows: testRows{},
		metrics: NewMetrics(prometheus.NewRegistry())}
	f.svc = New(Options{Store: f.store, Catalog: f.cat, Policies: f.policies.Load, Engine: f.eng, Key: testKey, CatalogName: "lake",
		Warehouse: f.warehouse(), Rows: f.rows, Metrics: f.metrics,
		Purger: NewPurger(f.store, f.cache, MemoryCache+"test", "test", slog.New(slog.DiscardHandler))})
	return f
}

func (f *fixture) warehouse() string { return filepath.Join(f.dir, "warehouse") + "/" }

// tag tags columns with pii.
func (f *fixture) tag(cts ...policy.ColumnTag) {
	f.t.Helper()
	err := f.policies.Update(f.ctx, func(c *policy.Catalog) error {
		now := time.Now()
		if _, ok := c.Tag("pii"); !ok {
			if err := c.PutTag(policy.Tag{Name: "pii"}, false, now); err != nil {
				return err
			}
		}
		for _, ct := range cts {
			ct.Tag = "pii"
			if err := c.SetColumnTag(ct, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

// people makes a table of people, with these rows, in one commit each.
func (f *fixture) people(name string, commits ...[][]any) *table.Table {
	f.t.Helper()
	tbl := f.cat.create(f.t, f.dir, name, peopleSchema)
	for _, rows := range commits {
		tbl = appendRows(f.t, tbl, rows...)
	}
	return tbl
}

func (f *fixture) load(name string) *table.Table {
	f.t.Helper()
	tbl, err := f.cat.LoadTable(f.ctx, ident(name))
	if err != nil {
		f.t.Fatal(err)
	}
	return tbl
}

// erase makes a request and runs it until it ends or stops for another
// reason than waiting.
func (f *fixture) erase(wanted ...Wanted) *Request {
	f.t.Helper()
	r, err := f.svc.Submit(f.ctx, "privacy", wanted)
	if err != nil {
		f.t.Fatal(err)
	}
	return f.run(r.ID)
}

func (f *fixture) run(id string) *Request {
	f.t.Helper()
	for range 5 {
		_ = f.svc.Run(f.ctx, id)
		r, err := f.store.Get(f.ctx, id)
		if err != nil {
			f.t.Fatal(err)
		}
		if r.Status != StatusWaiting {
			return r
		}
		later := r.WaitUntil.Add(time.Minute)
		f.svc.now = func() time.Time { return later }
	}
	f.t.Fatalf("request %s still waits", id)
	return nil
}

func (f *fixture) completed(r *Request) {
	f.t.Helper()
	if r.Status != StatusCompleted {
		f.t.Fatalf("status %s at %s, error %q, verification %+v", r.Status, r.Step, r.Error, r.Verification)
	}
}

// ids are the ids of a table's rows at snapshot (0: the current one), and
// their emails, sorted.
func (f *fixture) ids(tbl *table.Table, snapshot int64) []string {
	f.t.Helper()
	var opts []table.ScanOption
	if snapshot != 0 {
		opts = append(opts, table.WithSnapshotID(snapshot))
	}
	rows, err := tbl.Scan(opts...).ToArrowTable(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Release()
	var out []string
	tr := array.NewTableReader(rows, 1000)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		for i := range int(rec.NumRows()) {
			out = append(out, fmt.Sprint(ids.Value(i)))
		}
	}
	slices.Sort(out)
	return out
}

func (f *fixture) clean(needles ...string) {
	f.t.Helper()
	if found := search(f.t, f.eng, f.dir, needles); len(found) > 0 {
		f.t.Errorf("identifiers left in storage:\n%s", strings.Join(found, "\n"))
	}
}

var (
	ann     = []any{int64(1), "ann@other.com", "dev-ann", "keep"}
	subject = []any{int64(2), storedEmail, "dev-x", "erase"}
	carl    = []any{int64(3), "carl@other.com", "dev-carl", "keep"}
	byEmail = Wanted{Tag: "pii", Value: "email", Identifiers: []string{requestedEmail}}
)

// A row the DELETE cannot name, as when it arrived after the delete step
// or its column was renamed by another engine, goes when its file is
// rewritten, in the same commit; and a request whose matching rows keep
// arriving stops after a few passes instead of committing without end.
func TestRewriteRemovesRowsTheDeleteMissed(t *testing.T) {
	t.Run("missed", func(t *testing.T) {
		f := newFixture(t)
		f.people("crm.people", [][]any{ann, subject, carl})
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		r := f.erase(byEmail)
		f.completed(r)
		if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3"}) {
			t.Errorf("rows: %v", got)
		}
		if r.Tables[0].RowsDeleted != 1 {
			t.Errorf("rows deleted: %+v", r.Tables[0])
		}
		f.clean(storedEmail)
	})
	t.Run("renamed by another engine", func(t *testing.T) {
		f := newFixture(t)
		tbl := f.people("crm.people", [][]any{ann, subject, carl})
		tx := tbl.NewTransaction()
		if err := tx.UpdateSchema(true, false).RenameColumn([]string{"email"}, "contact").Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		r := f.erase(byEmail)
		f.completed(r)
		if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3"}) {
			t.Errorf("rows: %v", got)
		}
		f.clean(storedEmail)
	})
	t.Run("keep arriving", func(t *testing.T) {
		f := newFixture(t)
		f.people("crm.people", [][]any{ann, subject, carl})
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		erasing := 0
		f.cat.after = func(id table.Identifier, meta table.Metadata) {
			snap := meta.CurrentSnapshot()
			if tableName(id) != "crm.people" || snap == nil || snap.Summary == nil || snap.Summary.Properties["igloo.operation"] != "erase" {
				return
			}
			erasing++
			// Another event for the person, just after each rewrite.
			appendRows(t, f.load("crm.people"), []any{int64(100 + erasing), storedEmail, "dev-x", "again"})
		}
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		err = f.svc.Run(f.ctx, r.ID)
		if err == nil || !strings.Contains(err.Error(), "keep arriving") {
			t.Fatalf("run: %v", err)
		}
		if erasing > maxPasses+1 {
			t.Errorf("%d rewrites", erasing)
		}
		got, _ := f.store.Get(f.ctx, r.ID)
		if got.Status != StatusRetrying || got.Failures != 1 || strings.Contains(got.Error, "subject@") {
			t.Errorf("request: %s, %d failures, %q", got.Status, got.Failures, got.Error)
		}
	})
}

// Every branch and tag is rewritten, so time travel and unpublished
// branches keep everyone else's rows: a branch's unpublished rows stay,
// and a tag is moved to a snapshot without the person's.
func TestBranchesAndTags(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	audit1 := tbl.CurrentSnapshot().SnapshotID
	commitRef := func(name string, id int64, typ table.RefType) {
		t.Helper()
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
			[]table.Update{table.NewSetSnapshotRefUpdate(name, id, typ, 0, 0, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	commitRef("audit-2026-09", audit1, table.TagRef)
	tbl = appendRows(t, f.load("crm.people"), carl)
	commitRef("wap", tbl.CurrentSnapshot().SnapshotID, table.BranchRef)
	// Unpublished rows on the branch, the person's among them.
	tbl = f.load("crm.people")
	tx := tbl.NewTransactionOnBranch("wap")
	if err := tx.AppendTable(f.ctx, arrowRows(t, tbl, []any{int64(4), "eve@other.com", "dev-eve", "unpublished"},
		[]any{int64(5), storedEmail, "dev-y", "unpublished"}), 1000, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	// While snapshots are expired, someone points the branch at an old
	// snapshot that holds the person's rows: the commit that expires
	// them asserts every branch, so it is made again.
	moved := false
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		if moved || len(updates) == 0 || updates[0].Action() != table.UpdateRemoveSnapshots {
			return
		}
		moved = true
		commitRef("wap", audit1, table.BranchRef)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	if !moved {
		t.Fatal("no snapshot was expired")
	}
	tbl = f.load("crm.people")
	refs := refs(tbl)
	if len(refs) != 3 || refs["wap"].SnapshotRefType != table.BranchRef || refs["audit-2026-09"].SnapshotRefType != table.TagRef {
		t.Fatalf("refs: %v", refs)
	}
	if got := f.ids(tbl, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("main: %v", got)
	}
	if got := f.ids(tbl, refs["wap"].SnapshotID); !slices.Equal(got, []string{"1"}) {
		t.Errorf("wap, moved to the old snapshot: %v", got)
	}
	if got := f.ids(tbl, refs["audit-2026-09"].SnapshotID); !slices.Equal(got, []string{"1"}) {
		t.Errorf("the tag: %v", got)
	}
	if !slices.Contains(r.Tables[0].Refs, "audit-2026-09") {
		t.Errorf("refs rewritten: %v", r.Tables[0].Refs)
	}
	f.clean(storedEmail)
}

// The branch's unpublished rows stay when it is rewritten in place.
func TestBranchKeepsUnpublishedRows(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
		[]table.Update{table.NewSetSnapshotRefUpdate("wap", tbl.CurrentSnapshot().SnapshotID, table.BranchRef, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	tbl = f.load("crm.people")
	tx := tbl.NewTransactionOnBranch("wap")
	if err := tx.AppendTable(f.ctx, arrowRows(t, tbl, []any{int64(4), "eve@other.com", "dev-eve", "unpublished"}), 1000, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	tbl = f.load("crm.people")
	if got := f.ids(tbl, refs(tbl)["wap"].SnapshotID); !slices.Equal(got, []string{"1", "4"}) {
		t.Errorf("wap: %v", got)
	}
	f.clean(storedEmail)
}

// arrowRows are rows for tbl's schema.
func arrowRows(t *testing.T, tbl *table.Table, rows ...[]any) arrow.Table {
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
			case nil:
				b.Field(i).AppendNull()
			}
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	return array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
}

// A table that shares files with a tagged one, in a nested namespace, is
// found and erased from, and loaded by its identifier as it is; one that
// shares them under other column names keeps its files, and the request
// says why it cannot go on.
func TestNestedNamespace(t *testing.T) {
	share := func(f *fixture, schema *iceberg.Schema) {
		t.Helper()
		src := f.people("crm.people", [][]any{ann, subject, carl})
		tf, err := readFiles(f.ctx, src)
		if err != nil {
			t.Fatal(err)
		}
		copy := f.cat.create(t, f.dir, "crm.eu.people_copy", schema)
		tx := copy.NewTransaction()
		if err := tx.AddDataFiles(f.ctx, slices.Collect(func(yield func(iceberg.DataFile) bool) {
			for _, df := range tf.live(true) {
				if !yield(df) {
					return
				}
			}
		}), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	}
	t.Run("found", func(t *testing.T) {
		f := newFixture(t)
		share(f, peopleSchema)
		r := f.erase(byEmail)
		f.completed(r)
		i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == "crm.eu.people_copy" })
		if i < 0 || !slices.Equal(r.Tables[i].Ident, []string{"crm", "eu", "people_copy"}) {
			t.Fatalf("tables: %+v", r.Tables)
		}
		if got := f.ids(f.load("crm.eu.people_copy"), 0); !slices.Equal(got, []string{"1", "3"}) {
			t.Errorf("the copy: %v", got)
		}
		f.clean(storedEmail)
	})
	t.Run("other names", func(t *testing.T) {
		f := newFixture(t)
		share(f, iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "mail", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 3, Name: "device", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String}))
		r := f.erase(byEmail)
		if r.Status != StatusRetrying || !strings.Contains(r.Error, "crm.eu.people_copy references") {
			t.Fatalf("status %s, error %q", r.Status, r.Error)
		}
		if got := f.ids(f.load("crm.eu.people_copy"), 0); !slices.Equal(got, []string{"1", "2", "3"}) {
			t.Errorf("the copy lost its files: %v", got)
		}
	})
}

// A tag names a column, which can be dropped and added again by another
// engine: the values under the old field stay in older files, and are
// found by every field that had the name. A tagged table that is gone
// stops a request rather than letting it complete with nothing erased.
func TestFieldIdentity(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject, carl})
	tx := tbl.NewTransaction()
	if err := tx.UpdateSchema(true, true).DeleteColumn([]string{"email"}).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	tx = f.load("crm.people").NewTransaction()
	if err := tx.UpdateSchema(true, false).AddColumn([]string{"email"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	// The rows stay, without the dropped column's values.
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "2", "3"}) {
		t.Errorf("rows: %v", got)
	}
	if r.Tables[0].FilesRewritten == 0 {
		t.Errorf("no file was rewritten: %+v", r.Tables[0])
	}
	f.clean(storedEmail)

	f.tag(policy.ColumnTag{Schema: "crm", Table: "ghost", Column: "email", Value: "email"})
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); err == nil || !strings.Contains(err.Error(), "crm.ghost is tagged") {
		t.Errorf("a request with a missing tagged table: %v", err)
	}
}

// A table found by a request that is gone by the time it runs fails it.
func TestTableGoneWhileErasing(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.cat.mu.Lock()
	delete(f.cat.tables, "crm.people")
	f.cat.mu.Unlock()
	if got := f.run(r.ID); got.Status == StatusCompleted || !strings.Contains(got.Error, "crm.people") {
		t.Errorf("status %s, error %q", got.Status, got.Error)
	}
}

// Lists, structs and maps hold identifiers inside values, which compare
// as text as a whole: a request does not pretend to erase from them.
func TestNonScalarColumns(t *testing.T) {
	f := newFixture(t)
	f.cat.create(t, f.dir, "crm.lists", iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "emails", Type: &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String}}))
	f.tag(policy.ColumnTag{Schema: "crm", Table: "lists", Column: "emails", Value: "email"})
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "emails") {
		t.Errorf("submit: %v", err)
	}
}

// Placeholders are refused; a dry run counts without making a request; a
// request that would delete much of a table waits for confirmation, and
// can be canceled until it deletes.
func TestSafeguards(t *testing.T) {
	f := newFixture(t)
	var rows [][]any
	for i := range 150 {
		rows = append(rows, []any{int64(i), fmt.Sprintf("p%d@other.com", i), "dev-shared", "keep"})
	}
	f.people("crm.people", append(rows, ann, subject))
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "people", Column: "device_id", Value: "device_id"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("device_id"), "dev-shared")

	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "unknown", "0", "n/a", "-", " NULL "} {
		if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "device_id", Identifiers: []string{id}}}); !errors.Is(err, ErrInvalid) {
			t.Errorf("%q: %v", id, err)
		}
	}

	shared := Wanted{Tag: "pii", Value: "device_id", Identifiers: []string{"DEV-SHARED"}}
	preview, err := f.svc.Preview(f.ctx, []Wanted{shared})
	if err != nil || len(preview.Tables) != 1 || preview.Tables[0].Matched != 150 || preview.Tables[0].Rows != 152 {
		t.Fatalf("preview: %+v, %v", preview, err)
	}
	if all, _ := f.store.List(f.ctx); len(all) != 0 {
		t.Errorf("a preview made a request")
	}

	// Held, then canceled: nothing is deleted.
	r := f.erase(shared)
	if r.Status != StatusHeld || testutil.ToFloat64(f.metrics.held) != 1 {
		t.Fatalf("status %s", r.Status)
	}
	if got := f.run(r.ID); got.Status != StatusHeld {
		t.Errorf("a held request ran: %s", got.Status)
	}
	if _, err := f.svc.Confirm(f.ctx, "nope", "pat"); !errors.Is(err, ErrNotFound) {
		t.Errorf("confirm of a missing request: %v", err)
	}
	if got, err := f.svc.Cancel(f.ctx, r.ID, "pat"); err != nil || got.Status != StatusCanceled || got.Sealed != "" {
		t.Fatalf("cancel: %+v, %v", got, err)
	}
	if got := f.ids(f.load("crm.people"), 0); len(got) != 152 {
		t.Errorf("a canceled request deleted rows: %d left", len(got))
	}

	// Held, then confirmed.
	r = f.erase(shared)
	if _, err := f.svc.Confirm(f.ctx, r.ID, "pat"); err != nil {
		t.Fatal(err)
	}
	r = f.run(r.ID)
	f.completed(r)
	if r.ConfirmedBy != "pat" || r.Tables[0].RowsDeleted != 150 {
		t.Errorf("confirmed: %+v", r)
	}
	if _, err := f.svc.Cancel(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) {
		t.Errorf("cancel of a completed request: %v", err)
	}

	// One that has begun deleting cannot be canceled.
	young := filepath.Join(f.load("crm.people").Location(), "data", "in-progress.parquet")
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT 9 AS id, '%s' AS email) TO '%s' (FORMAT parquet)", storedEmail, young))
	r, err = f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.svc.Run(f.ctx, r.ID)
	if got, _ := f.store.Get(f.ctx, r.ID); got.Status != StatusWaiting {
		t.Fatalf("status %s, error %q", got.Status, got.Error)
	}
	if _, err := f.svc.Cancel(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "begun deleting") {
		t.Errorf("cancel while deleting: %v", err)
	}
}

// Runs do not wait for one another, failed runs wait longer each time,
// and a request that keeps failing fails for good, forgets whose data it
// was, and is counted, for an alert.
func TestFailures(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	lease, ok, err := f.svc.lease(f.ctx)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	start := time.Now()
	if err := f.svc.Run(f.ctx, r.ID); !errors.Is(err, tasks.ErrIdle) || time.Since(start) > time.Second {
		t.Errorf("a run while another holds the lock: %v after %v", err, time.Since(start))
	}
	lease.Release()

	f.svc.rows = failingRows{}
	var alerts []string
	f.svc.opts.Failed = func(id, step, err string) { alerts = append(alerts, id+" "+step+": "+err) }
	now := time.Now()
	f.svc.now = func() time.Time { return now }
	var waits []time.Duration
	for range maxFailures {
		_ = f.svc.Run(f.ctx, r.ID)
		got, _ := f.store.Get(f.ctx, r.ID)
		if got.Status == StatusFailed {
			break
		}
		waits = append(waits, got.WaitUntil.Sub(now))
	}
	if len(waits) != maxFailures-1 || waits[0] != time.Minute || waits[1] != 2*time.Minute || waits[len(waits)-1] != maxBackoff {
		t.Errorf("waits: %v", waits)
	}
	got, _ := f.store.Get(f.ctx, r.ID)
	if got.Status != StatusFailed || got.Sealed != "" || got.Salt != "" || testutil.ToFloat64(f.metrics.failed) != 1 {
		t.Errorf("failed: %+v", got)
	}
	if len(alerts) != 1 || !strings.HasPrefix(alerts[0], r.ID+" delete: ") || strings.Contains(alerts[0], "subject@") {
		t.Errorf("alerts: %q", alerts)
	}
	if err := f.svc.Run(f.ctx, r.ID); err != nil {
		t.Errorf("a run of a failed request: %v", err)
	}
}

type failingRows struct{}

func (failingRows) Delete(context.Context, *table.Table, string) (int64, error) {
	return 0, errors.New("Binder Error: cannot delete")
}
func (failingRows) Count(context.Context, *table.Table, string) (int64, error) { return 0, nil }

// A run that loses its lock, as when its connection to the metadata
// database dropped and another replica may have taken it, stops before
// its next step.
func TestLostLease(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.svc.lease = func(context.Context) (tasks.Lease, bool, error) { return &lostLease{}, true, nil }
	if err := f.svc.Run(f.ctx, r.ID); err == nil || !strings.Contains(err.Error(), "lock is lost") {
		t.Fatalf("run: %v", err)
	}
	got, _ := f.store.Get(f.ctx, r.ID)
	if got.Step != StepDelete {
		t.Errorf("step %s", got.Step)
	}
	if ids := f.ids(f.load("crm.people"), 0); len(ids) != 2 {
		t.Errorf("rows were deleted: %v", ids)
	}
}

// lostLease is held for one check, then lost.
type lostLease struct{ checks int }

func (l *lostLease) Held(context.Context) error {
	if l.checks++; l.checks > 1 {
		return errors.New("the lock is lost")
	}
	return nil
}
func (l *lostLease) Release() {}

// Without the key a request cannot be made, and with another one it
// cannot be carried out.
func TestKey(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	keyless := New(Options{Store: f.store, Catalog: f.cat, Policies: f.policies.Load, Engine: f.eng, Warehouse: f.warehouse(), Rows: f.rows})
	if _, err := keyless.Submit(f.ctx, "privacy", []Wanted{byEmail}); err == nil || !strings.Contains(err.Error(), "IGLOO_ERASURE_KEY") {
		t.Errorf("submit without the key: %v", err)
	}
	if p, err := keyless.Preview(f.ctx, []Wanted{byEmail}); err != nil || p.Tables[0].Matched != 0 {
		t.Errorf("a preview needs no key: %+v, %v", p, err)
	}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	// A replica with another key, as one deployed with a mistyped key, or
	// none, leaves the request as it is, for one with the right key: as
	// often as it runs it, it neither fails it nor forgets its
	// identifiers, and the scheduler is told, for an alert.
	other := New(Options{Store: f.store, Catalog: f.cat, Policies: f.policies.Load, Engine: f.eng, Key: []byte("another key of thirty-two bytes!"),
		Warehouse: f.warehouse(), Rows: f.rows})
	for _, replica := range []*Service{other, keyless, other} {
		for range maxFailures + 1 {
			err := replica.Run(f.ctx, r.ID)
			if err == nil || !strings.Contains(err.Error(), "IGLOO_ERASURE_KEY") {
				t.Fatalf("run with the wrong key: %v", err)
			}
		}
	}
	got, _ := f.store.Get(f.ctx, r.ID)
	if got.Status != StatusRetrying || got.Sealed != r.Sealed || got.Salt == "" || got.Failures != 0 || !strings.Contains(got.Error, "IGLOO_ERASURE_KEY") {
		t.Fatalf("after runs with the wrong key: status %s, failures %d, error %q, sealed kept %v", got.Status, got.Failures, got.Error, got.Sealed == r.Sealed)
	}
	if r = f.run(r.ID); r.Status != StatusCompleted {
		t.Errorf("with the right key: status %s, error %q", r.Status, r.Error)
	}
}

// Equality delete files hold column values: whole rows, as Flink writes
// them, by whatever columns they delete. They are scanned, the ones that
// hold matches go with the files they apply to, and those that apply to
// no file go at once.
func TestEqualityDeletes(t *testing.T) {
	f := newFixture(t)
	tbl := f.cat.create(t, f.dir, "crm.people", peopleSchema)
	if err := os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	eqDelete := func(name, sel string, ids []int) iceberg.DataFile {
		t.Helper()
		p := filepath.Join(tbl.Location(), "data", name)
		f.eng.exec(t, fmt.Sprintf("COPY (SELECT %s) TO '%s' (FIELD_IDS {id: 1, email: 2, device_id: 3, note: 4})", sel, p))
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes, p, iceberg.ParquetFile,
			nil, nil, nil, 1, info.Size())
		if err != nil {
			t.Fatal(err)
		}
		return b.EqualityFieldIDs(ids).Build()
	}
	addDeletes := func(df iceberg.DataFile) {
		t.Helper()
		tx := f.load("crm.people").NewTransaction()
		if err := tx.NewRowDelta(nil).AddDeletes(df).Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Keyed on the email, before any data: it applies to no file.
	addDeletes(eqDelete("dangling.parquet", fmt.Sprintf("NULL::BIGINT AS id, '%s' AS email, NULL AS device_id, NULL AS note", storedEmail), []int{2}))
	appendRows(t, f.load("crm.people"), ann, subject, carl)
	// A whole row, keyed on the id, as change data capture writes them.
	addDeletes(eqDelete("cdc.parquet", fmt.Sprintf("2::BIGINT AS id, '%s' AS email, 'dev-x' AS device_id, 'erase' AS note", storedEmail), []int{1}))
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("rows: %v", got)
	}
	f.clean(storedEmail)
}

// Files outside the warehouse, such as those added from a landing bucket,
// are left where they are, and reported.
func TestFilesOutsideTheWarehouse(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann})
	landing := filepath.Join(f.dir, "landing", "drop.parquet")
	if err := os.MkdirAll(filepath.Dir(landing), 0o755); err != nil {
		t.Fatal(err)
	}
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT 2::BIGINT AS id, '%s' AS email, 'dev-x' AS device_id, 'erase' AS note) TO '%s' "+
		"(FIELD_IDS {id: 1, email: 2, device_id: 3, note: 4})", storedEmail, landing))
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(f.ctx, []string{landing}, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	if _, err := os.Stat(landing); err != nil {
		t.Errorf("the landing file was deleted: %v", err)
	}
	if r.Tables[0].FilesOutside != 1 {
		t.Errorf("files outside: %+v", r.Tables[0])
	}
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1"}) {
		t.Errorf("rows: %v", got)
	}
	if found := search(t, f.eng, f.warehouse(), []string{storedEmail}); len(found) > 0 {
		t.Errorf("identifiers left in the warehouse:\n%s", strings.Join(found, "\n"))
	}
}

// Every replica empties its whole cache once after a request deletes
// files: blocks of files deleted before, which no request knows of, go
// too.
func TestCachesAreEmptied(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	cache := &readCache{}
	p := NewPurger(store, cache, "disk:a", "replica-a", slog.New(slog.DiscardHandler))
	if err := p.Once(ctx); err != nil || cache.emptied != 0 {
		t.Fatalf("with nothing deleted: emptied %d, %v", cache.emptied, err)
	}
	r := &Request{ID: "r1", Status: StatusRunning, DeletedThrough: time.Now()}
	if err := store.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := p.Once(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if cache.emptied != 1 {
		t.Errorf("emptied %d times, want once", cache.emptied)
	}
	svc := New(Options{Store: store})
	emptied, pending, waiting, err := svc.caches(ctx, r)
	if err != nil || emptied != 1 || pending != 0 || len(waiting) != 0 {
		t.Errorf("caches: %d %d %v %v", emptied, pending, waiting, err)
	}
	// Another replica, seen just now, has yet to.
	if err := store.Seen(ctx, "disk:b", "replica-b", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, _, waiting, _ := svc.caches(ctx, r); !slices.Equal(waiting, []string{"replica-b"}) {
		t.Errorf("waiting: %v", waiting)
	}
}

// A column that names the person in someone else's row is set to NULL
// there, and the row stays.
func TestNullColumns(t *testing.T) {
	f := newFixture(t)
	f.cat.create(t, f.dir, "crm.referrals", iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "referrer_email", Type: iceberg.PrimitiveTypes.String}))
	tbl := f.load("crm.referrals")
	tx := tbl.NewTransaction()
	if err := tx.AppendTable(f.ctx, arrowRows(t, tbl,
		[]any{int64(1), storedEmail, "bob@other.com"},
		[]any{int64(2), "eve@other.com", storedEmail},
		[]any{int64(3), "fay@other.com", "gus@other.com"}), 1000, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "referrals", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "referrals", Column: "referrer_email", Value: "email" + nullSuffix})
	r := f.erase(byEmail)
	f.completed(r)
	if got := f.ids(f.load("crm.referrals"), 0); !slices.Equal(got, []string{"2", "3"}) {
		t.Errorf("rows: %v", got)
	}
	if r.Tables[0].RowsDeleted != 1 || r.Tables[0].ValuesNulled != 1 {
		t.Errorf("table: %+v", r.Tables[0])
	}
	f.clean(storedEmail)
}

// Delta Sharing copies under another table's UUID, as those of a table
// since replaced, are looked in too.
func TestSharingCopiesOfReplacedTables(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	old := filepath.Join(f.warehouse(), sharingDir, "0b9c6a8e-0000-4000-8000-000000000000", "copy.parquet")
	if err := os.MkdirAll(filepath.Dir(old), 0o755); err != nil {
		t.Fatal(err)
	}
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT 2 AS id, '%s' AS email) TO '%s' (FORMAT parquet)", storedEmail, old))
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("the old table's copy is still there: %v", err)
	}
}

// The memory history redacts query tags and application names too.
func TestMemoryHistoryRedaction(t *testing.T) {
	ctx := context.Background()
	h := audit.NewMemoryStore(10, 10)
	_ = h.WriteQueries(ctx, []audit.Query{{ID: "q1", Text: "SELECT 1", QueryTag: "for " + storedEmail, ApplicationName: storedEmail},
		{ID: "q2", Text: "COPY t FROM 's3://landing/*.csv'", LocationsRead: []string{"s3://landing/a.csv", "s3://landing/Subject%40Example.com/b.csv"}}})
	_ = h.WriteLogins(ctx, []audit.Login{{ID: "l1", ApplicationName: storedEmail}})
	r := &Request{salt: "s", Subjects: []Subject{testSubject("s", "pii", "email", requestedEmail)}}
	if n, err := h.Redact(ctx, newRedactor(r)); err != nil || n != 3 {
		t.Errorf("redacted %d, %v", n, err)
	}
	q, _ := h.Queries(ctx, audit.Viewer{All: true}, audit.Page{})
	l, _ := h.Logins(ctx, audit.Viewer{All: true}, audit.Page{})
	i := slices.IndexFunc(q, func(q audit.Query) bool { return q.ID == "q1" })
	j := slices.IndexFunc(q, func(q audit.Query) bool { return q.ID == "q2" })
	if q[i].QueryTag != "for ***" || q[i].ApplicationName != "***" || l[0].ApplicationName != "***" ||
		!slices.Equal(q[j].LocationsRead, []string{"s3://landing/a.csv", "s3://landing/***/b.csv"}) {
		t.Errorf("queries %+v, logins %+v", q, l)
	}
}
