package erasure

import (
	"context"
	"errors"
	"fmt"
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
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/policy"
)

// The fourth review's findings, each reproduced by a test it wrote, and
// fixed: what a request cannot finish with certainty it refuses before its
// first DELETE, and checks again on each table just before each DELETE
// from it; what arises after a DELETE blocks it, and never fails it.

// appendValues appends rows of int64, int32, string or nil values, in the
// order of tbl's current schema, in one commit.
func appendValues(t *testing.T, tbl *table.Table, rows ...[]any) *table.Table {
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
			case int32:
				b.Field(i).(*array.Int32Builder).Append(v)
			case string:
				b.Field(i).(*array.StringBuilder).Append(v)
			case nil:
				b.Field(i).AppendNull()
			}
		}
	}
	rec := b.NewRecordBatch()
	defer rec.Release()
	at := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	defer at.Release()
	out, err := tbl.AppendTable(context.Background(), at, 1000, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// rowsAt reads every column of tbl at snapshot as iceberg-go and PyIceberg
// do, with the snapshot's own schema: the rows' values as text by column
// name, by id.
func rowsAt(t *testing.T, f *fixture, tbl *table.Table, snapshot int64) map[string]map[string]string {
	t.Helper()
	rows, err := tbl.Scan(table.WithSnapshotID(snapshot)).ToArrowTable(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Release()
	out := map[string]map[string]string{}
	tr := array.NewTableReader(rows, 1000)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := map[string]string{}
			for c, fl := range rec.Schema().Fields() {
				if col := rec.Column(c); col.IsNull(i) {
					row[fl.Name] = "NULL"
				} else {
					row[fl.Name] = col.ValueStr(i)
				}
			}
			out[row["id"]] = row
		}
	}
	return out
}

// setRef points ref name of table tbl at snapshot id.
func setRef(t *testing.T, f *fixture, tbl, name string, id int64, typ table.RefType) {
	t.Helper()
	if _, _, err := f.cat.CommitTable(f.ctx, ident(tbl), nil, []table.Update{table.NewSetSnapshotRefUpdate(name, id, typ, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
}

// hookRows deletes and counts as testRows do. Each DELETE is recorded with
// the format version the catalog has the table at when it runs, and calls
// before, ahead of it, and after, once it succeeded; fail fails the first
// DELETE of the tables it names, as a lost connection does.
type hookRows struct {
	testRows
	f             *fixture
	before, after func(name string)
	fail          map[string]bool
	mu            sync.Mutex
	deletes       []string
}

func (r *hookRows) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	name := tableName(tbl.Identifier())
	if r.before != nil {
		r.before(name)
	}
	r.mu.Lock()
	r.deletes = append(r.deletes, fmt.Sprintf("%s@v%d", name, r.f.load(name).Metadata().Version()))
	failing := r.fail[name]
	delete(r.fail, name)
	r.mu.Unlock()
	if failing {
		return 0, errors.New("connection reset by peer")
	}
	n, err := r.testRows.Delete(ctx, tbl, cond)
	if err == nil && r.after != nil {
		r.after(name)
	}
	return n, err
}

// ranOn reports whether a DELETE ran on table name at a format version of
// 3 or later.
func (r *hookRows) ranOn(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.deletes, func(d string) bool { return strings.HasPrefix(d, name+"@v") && !strings.HasSuffix(d, "@v2") })
}

// upgradeWithDV upgrades table name to format version 3 and adds a
// deletion vector for its first data file, as Spark's DELETE on it would.
func upgradeWithDV(t *testing.T, f *fixture, name string) {
	t.Helper()
	if _, _, err := f.cat.CommitTable(f.ctx, ident(name), nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
		t.Fatal(err)
	}
	tbl := f.load(name)
	tasks, err := tbl.Scan().PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, tbl.Location()+"/data/spark-dv.puffin",
		iceberg.PuffinFile, nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	df := b.ReferencedDataFile(tasks[0].File.FilePath()).ContentOffset(4).ContentSizeInBytes(42).Build()
	tx := tbl.NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(df).Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// runFor runs request id up to runs times, three hours apart, until it
// ends, is held or is blocked.
func runFor(f *fixture, id string, runs int) *Request {
	f.t.Helper()
	start := time.Now()
	for i := range runs {
		at := start.Add(time.Duration(i+1) * 3 * time.Hour)
		f.svc.now = func() time.Time { return at }
		_ = f.svc.Run(f.ctx, id)
		r, err := f.store.Get(f.ctx, id)
		if err != nil {
			f.t.Fatal(err)
		}
		switch r.Status {
		case StatusCompleted, StatusFailed, StatusHeld, StatusBlocked:
			return r
		}
	}
	r, _ := f.store.Get(f.ctx, id)
	return r
}

// runTimes runs request id n times, an hour apart, whatever its status.
func (f *fixture) runTimes(id string, n int) *Request {
	f.t.Helper()
	start := time.Now()
	for i := range n {
		at := start.Add(time.Duration(i+1) * time.Hour)
		f.svc.now = func() time.Time { return at }
		_ = f.svc.Run(f.ctx, id)
	}
	r, err := f.store.Get(f.ctx, id)
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

// alerts collects the blocked requests' alerts.
func (f *fixture) alerts() *[]string {
	var mu sync.Mutex
	var got []string
	f.svc.opts.Blocked = func(id, step, err string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, id+" "+step+": "+err)
	}
	return &got
}

// blocked checks that r is blocked at step, with its identifiers, having
// alerted, and that it cannot be canceled.
func (f *fixture) blocked(r *Request, step, why string, alerts []string) {
	f.t.Helper()
	if r.Status != StatusBlocked || r.Step != step || !r.DeleteStarted || r.Sealed == "" || r.Salt == "" || !strings.Contains(r.Error, why) {
		f.t.Errorf("status %s at %s, delete started %v, sealed %v, error %q; want blocked at %s by %q",
			r.Status, r.Step, r.DeleteStarted, r.Sealed != "", r.Error, step, why)
	}
	if len(alerts) == 0 || !strings.HasPrefix(alerts[len(alerts)-1], r.ID+" "+step+": ") {
		f.t.Errorf("alerts: %q", alerts)
	}
	if _, err := f.svc.Cancel(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) {
		f.t.Errorf("a blocked request was canceled: %v", err)
	}
}

// ---------------------------------------------- 1: branches' and tags' schemas

// A branch whose head was written before a column was dropped is read with
// its head's schema, by Igloo's AT (BRANCH => ...) as by PyIceberg, and is
// rewritten with it: the column stays in the branch's other rows, and the
// snapshot that replaces its head keeps that schema.
func TestBranchKeepsItsHeadsSchema(t *testing.T) {
	f := newFixture(t)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "device_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 5, Name: "revenue", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := appendValues(t, f.cat.create(t, f.dir, "crm.people", schema),
		[]any{int64(1), "ann@other.com", "dev-ann", "keep", "ann-revenue-100"},
		[]any{int64(2), storedEmail, "dev-x", "erase", "subject-revenue"})
	s1 := tbl.CurrentSnapshot()
	setRef(t, f, "crm.people", "dev", s1.SnapshotID, table.BranchRef)
	tx := f.load("crm.people").NewTransaction()
	if err := tx.UpdateSchema(true, false).DeleteColumn([]string{"revenue"}).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	appendRows(t, f.load("crm.people"), carl)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	tbl = f.load("crm.people")
	head := refs(tbl)["dev"].SnapshotID
	if head == s1.SnapshotID {
		t.Fatal("the branch was not rewritten")
	}
	if got := tbl.SnapshotByID(head).SchemaID; got == nil || *got != *s1.SchemaID {
		t.Errorf("the branch's head has schema %v, want its own, %d", got, *s1.SchemaID)
	}
	rows := rowsAt(t, f, tbl, head)
	if len(rows) != 1 || rows["1"]["revenue"] != "ann-revenue-100" {
		t.Errorf("the branch's rows: %v; want ann's alone, with her revenue", rows)
	}
	if got := f.ids(tbl, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("main: %v", got)
	}
	f.clean(storedEmail)
}

// A tag made before the table was partitioned by a column added since is
// read with its snapshot's schema, which lacks the column: its files are
// written again with their own spec, whose columns the schema has, rather
// than the table's default one, and the request completes.
func TestTagOlderThanItsPartitioning(t *testing.T) {
	f := newFixture(t)
	s1 := f.people("crm.people", [][]any{ann, subject}).CurrentSnapshot()
	setRef(t, f, "crm.people", "q3-audit", s1.SnapshotID, table.TagRef)
	tx := f.load("crm.people").NewTransaction()
	if err := tx.UpdateSchema(true, false).AddColumn([]string{"region"}, iceberg.PrimitiveTypes.String, "", false, nil).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	tx = f.load("crm.people").NewTransaction()
	if err := tx.UpdateSpec(true).AddIdentity("region").Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	appendValues(t, f.load("crm.people"), []any{int64(3), "carl@other.com", "dev-carl", "keep", "eu"})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.completed(f.erase(byEmail))
	tbl := f.load("crm.people")
	tag := refs(tbl)["q3-audit"].SnapshotID
	if tag == s1.SnapshotID {
		t.Fatal("the tag was not rewritten")
	}
	if got := f.ids(tbl, tag); !slices.Equal(got, []string{"1"}) {
		t.Errorf("the tag's rows: %v", got)
	}
	tasks, err := tbl.Scan(table.WithSnapshotID(tag)).PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.File.SpecID() != 0 {
			t.Errorf("the tag's %s has spec %d, which partitions by a column its schema lacks", base(task.File.FilePath()), task.File.SpecID())
		}
	}
	if got := f.ids(tbl, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("main: %v", got)
	}
	f.clean(storedEmail)
}

// partitionedPeople makes a table of people partitioned by note.
func partitionedPeople(t *testing.T, f *fixture, name string, rows ...[]any) *table.Table {
	t.Helper()
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{4}, FieldID: 1000, Name: "note", Transform: iceberg.IdentityTransform{}})
	loc := filepath.Join(f.dir, "warehouse", strings.ReplaceAll(name, ".", "/"))
	meta, err := table.NewMetadata(peopleSchema, &spec, table.UnsortedSortOrder, loc,
		iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident(name), meta, f.cat.write(t, loc, meta), localFS, f.cat)
	f.cat.mu.Lock()
	f.cat.tables[name] = tbl
	f.cat.mu.Unlock()
	return appendRows(t, tbl, rows...)
}

// unwritableTag tags, as another engine could, a snapshot of table name
// that holds its current files but whose schema lacks note, which they are
// partitioned by, and which every spec of the table partitions by: a
// rewrite of the tag could write its files again with no spec.
func unwritableTag(t *testing.T, f *fixture, name, tag string) {
	t.Helper()
	tbl := f.load(name)
	cur := tbl.CurrentSnapshot()
	id := 0
	for _, s := range tbl.Metadata().Schemas() {
		id = max(id, s.ID+1)
	}
	schema := iceberg.NewSchema(id,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "device_id", Type: iceberg.PrimitiveTypes.String})
	parent := cur.SnapshotID
	snap := table.Snapshot{SnapshotID: cur.SnapshotID + 1, ParentSnapshotID: &parent, SequenceNumber: tbl.Metadata().LastSequenceNumber() + 1,
		TimestampMs: time.Now().UnixMilli(), ManifestList: cur.ManifestList, Summary: &table.Summary{Operation: table.OpAppend}, SchemaID: &id}
	if _, _, err := f.cat.CommitTable(f.ctx, ident(name), nil, []table.Update{
		table.NewAddSchemaUpdate(schema), table.NewAddSnapshotUpdate(&snap),
		table.NewSetSnapshotRefUpdate(tag, snap.SnapshotID, table.TagRef, 0, 0, 0),
	}); err != nil {
		t.Fatal(err)
	}
}

// A tag whose files the request could write again with no spec of the
// table, as its schema lacks a column each partitions by, is refused: when
// the request is made; before its first DELETE, when the tag was made
// since, and then it fails, having deleted nothing; and once it has begun
// deleting, it is blocked, keeps its identifiers, alerts, and goes on once
// the tag is dropped.
func TestUnwritableRefs(t *testing.T) {
	t.Run("made before the request", func(t *testing.T) {
		f := newFixture(t)
		partitionedPeople(t, f, "crm.people", ann, subject)
		unwritableTag(t, f, "crm.people", "legacy")
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "tag legacy") {
			t.Errorf("submit: %v", err)
		}
	})
	t.Run("made before its first DELETE", func(t *testing.T) {
		f := newFixture(t)
		partitionedPeople(t, f, "crm.people", ann, subject)
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		unwritableTag(t, f, "crm.people", "legacy")
		got := runFor(f, r.ID, 1)
		if got.Status != StatusFailed || got.DeleteStarted || !strings.Contains(got.Error, "tag legacy") {
			t.Errorf("status %s, delete started %v, error %q", got.Status, got.DeleteStarted, got.Error)
		}
		if ids := f.ids(f.load("crm.people"), 0); !slices.Equal(ids, []string{"1", "2"}) {
			t.Errorf("rows were deleted: %v", ids)
		}
	})
	t.Run("made after its DELETE", func(t *testing.T) {
		f := newFixture(t)
		partitionedPeople(t, f, "crm.people", ann, subject)
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		rows := &hookRows{testRows: f.rows, f: f}
		rows.after = func(name string) { unwritableTag(t, f, name, "legacy") }
		f.svc.rows = rows
		alerts := f.alerts()
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		got := runFor(f, r.ID, 3)
		f.blocked(got, StepRewrite, "tag legacy", *alerts)
		rows.after = nil
		if got = f.runTimes(r.ID, maxFailures+2); got.Status != StatusBlocked {
			t.Errorf("status %s, error %q", got.Status, got.Error)
		}
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewRemoveSnapshotRefUpdate("legacy")}); err != nil {
			t.Fatal(err)
		}
		f.completed(f.run(r.ID))
		f.clean(storedEmail)
	})
}

// --------------------------------------------------- 2: format version 3

// Another engine upgrades the second table to format version 3, and
// deletes from it with a deletion vector, while the request deletes from
// the first: the second is checked again, as loaded just before its
// DELETE, which never runs on it. The request has begun deleting, so it
// does not fail: it is blocked, keeps its identifiers, and alerts.
func TestFormatVersion3DuringTheDeleteStep(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	f.people("crm.beta", [][]any{carl, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f}
	upgraded := false
	rows.before = func(name string) {
		if name == "crm.alpha" && !upgraded {
			upgraded = true
			upgradeWithDV(t, f, "crm.beta")
		}
	}
	f.svc.rows = rows
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, 3)
	f.blocked(got, StepDelete, "crm.beta is an Iceberg table of format version 3", *alerts)
	if got = f.runTimes(r.ID, maxFailures+2); got.Status != StatusBlocked {
		t.Errorf("status %s, error %q", got.Status, got.Error)
	}
	if !upgraded || rows.ranOn("crm.beta") {
		t.Errorf("DELETEs: %v", rows.deletes)
	}
}

// The first DELETE of a table fails; before the request runs again, the
// table is upgraded to version 3 and gets a deletion vector. The run that
// follows checks it again before its DELETE, which never runs on it.
func TestFormatVersion3BetweenDeleteRetries(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	f.people("crm.beta", [][]any{carl, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f, fail: map[string]bool{"crm.beta": true}}
	f.svc.rows = rows
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.svc.Run(f.ctx, r.ID)
	if first, _ := f.store.Get(f.ctx, r.ID); first.Status != StatusRetrying || !first.DeleteStarted {
		t.Fatalf("after the first run: %s, %q", first.Status, first.Error)
	}
	upgradeWithDV(t, f, "crm.beta")
	got := runFor(f, r.ID, 3)
	f.blocked(got, StepDelete, "crm.beta is an Iceberg table of format version 3", *alerts)
	if rows.ranOn("crm.beta") {
		t.Errorf("DELETEs: %v", rows.deletes)
	}
}

// A table upgraded to version 3 once the request has deleted from it, and
// appended to, which iceberg-go cannot even read: every step checks its
// tables' format version, and the request is blocked, never failed, at the
// step that found it.
func TestFormatVersion3AfterTheDelete(t *testing.T) {
	upgrade := func(t *testing.T, f *fixture, name string) {
		if _, _, err := f.cat.CommitTable(f.ctx, ident(name), nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
			t.Fatal(err)
		}
		appendRows(t, f.load(name), carl)
	}
	for _, c := range []struct {
		name, step string
		// hook sets up the upgrade.
		hook func(t *testing.T, f *fixture, rows *hookRows)
	}{
		{"after its DELETE", StepRewrite, func(t *testing.T, f *fixture, rows *hookRows) {
			rows.after = func(name string) { upgrade(t, f, name) }
		}},
		{"after its snapshots are expired", StepPurge, func(t *testing.T, f *fixture, rows *hookRows) {
			var mu sync.Mutex
			expired, done := false, false
			f.cat.before = func(_ table.Identifier, updates []table.Update) {
				mu.Lock()
				defer mu.Unlock()
				expired = expired || slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == "remove-snapshots" })
			}
			f.cat.after = func(id table.Identifier, _ table.Metadata) {
				mu.Lock()
				now := expired && !done
				done = done || now
				mu.Unlock()
				if now {
					upgrade(t, f, tableName(id))
				}
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			f.people("crm.people", [][]any{ann, subject})
			f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
			rows := &hookRows{testRows: f.rows, f: f}
			c.hook(t, f, rows)
			f.svc.rows = rows
			alerts := f.alerts()
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			got := runFor(f, r.ID, 3)
			f.blocked(got, c.step, "format version 3", *alerts)
			rows.after, f.cat.before, f.cat.after = nil, nil, nil
			if got = f.runTimes(r.ID, maxFailures+2); got.Status != StatusBlocked || got.Step != c.step || got.Sealed == "" {
				t.Errorf("status %s at %s, error %q", got.Status, got.Step, got.Error)
			}
		})
	}
}

// ----------------------------------------------------------------- 3: hold

// A request that failed at the find step never counted its rows, and never
// deleted any: requests made within a day of it do not wait for it.
func TestFailedRequestsDoNotHoldOthers(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.people("crm.other", [][]any{ann, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	x, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	// x's table becomes one it cannot rewrite: it fails at find, for good.
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
		t.Fatal(err)
	}
	_ = f.svc.Run(f.ctx, x.ID)
	if gx, _ := f.store.Get(f.ctx, x.ID); gx.Status != StatusFailed || gx.Counted {
		t.Fatalf("x: %s at %s, counted %v", gx.Status, gx.Step, gx.Counted)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "other", Column: "email", Value: "contact"})
	f.rows["crm.other"] = iceberg.EqualTo(iceberg.Reference("id"), int64(1))
	y, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "contact", Identifiers: []string{"ann@other.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.svc.Run(f.ctx, y.ID)
	if got, _ := f.store.Get(f.ctx, y.ID); got.Status != StatusCompleted {
		t.Errorf("y: %s, %q", got.Status, got.Error)
	}
}

// ------------------------------------------- 4: position deletes with rows

// rowCopyDeletes writes a partition-scoped position delete file with a row
// column, deleting (path, pos, row) entries, and commits it.
func rowCopyDeletes(t *testing.T, f *fixture, name, file string, entries [][3]any) {
	t.Helper()
	tbl := f.load(name)
	var vals []string
	for _, e := range entries {
		row := e[2].([]any)
		vals = append(vals, fmt.Sprintf("('%s', %d::BIGINT, {'id': %d::BIGINT, 'email': '%s', 'device_id': '%s', 'note': '%s'})",
			e[0], e[1], row[0], row[1], row[2], row[3]))
	}
	p := tbl.Location() + "/data/" + file
	local := p
	if strings.HasPrefix(p, "s3://") {
		local = f.cat.local(p)
	}
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) v(file_path, pos, row)) TO '%s' "+
		"(FIELD_IDS {file_path: 2147483546, pos: 2147483545, row: {__duckdb_field_id: 2147483544, id: 1, email: 2, device_id: 3, note: 4}})",
		strings.Join(vals, ", "), duckdbPath(local)))
	bld, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, p, iceberg.ParquetFile, nil, nil, nil, int64(len(entries)), 900)
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(bld.Build()).Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}

// filesByIDs maps the ids a data file of tbl holds, as "1,2", to its path.
func filesByIDs(t *testing.T, f *fixture, tbl *table.Table) map[string]string {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, task := range tasks {
		p := task.File.FilePath()
		if strings.HasPrefix(p, "s3://") {
			p = f.cat.local(p)
		}
		ids := strings.Join(f.eng.values(t, fmt.Sprintf("SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM '%s'", duckdbPath(p))), "")
		out[ids] = task.File.FilePath()
	}
	return out
}

var (
	eve  = []any{int64(5), "eve@other.com", "dev-eve", "keep"}
	fred = []any{int64(6), "fred@other.com", "dev-fred", "keep"}
)

// The row-copy file's targets were all rewritten already by another engine
// (copy-on-write, which leaves position deletes in place), and an older
// file it deletes no row of is still live, which planning attaches it to:
// it deletes rows of no live file, and goes at once.
func TestRowCopyOfFilesRewrittenElsewhere(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{eve, fred}, [][]any{ann, subject})
	a := filesByIDs(t, f, f.load("crm.people"))["1,2"]
	rowCopyDeletes(t, f, "crm.people", "deletes-with-rows.parquet", [][3]any{{a, 1, subject}})
	tbl := f.load("crm.people")
	rewritten := tbl.Location() + "/data/a-rewritten.parquet"
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT * FROM '%s' WHERE id <> 2) TO '%s' (FIELD_IDS {id: 1, email: 2, device_id: 3, note: 4})", duckdbPath(a), rewritten))
	tx := tbl.NewTransaction()
	if err := tx.ReplaceDataFiles(f.ctx, []string{a}, []string{rewritten}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "5", "6"}) {
		t.Errorf("rows: %v", got)
	}
	f.clean(storedEmail)
}

// The same on a table whose location is a file:// URI, which Canonical
// spells without its scheme, as it does s3://: the delete file that holds
// the row copy goes with the files it deletes rows of, in one pass, and
// an older file it deletes no row of keeps its rows.
func TestRowCopyWithURIPaths(t *testing.T) {
	f := newFixture(t)
	loc := filepath.Join(f.dir, "warehouse", "crm", "people")
	meta, err := table.NewMetadata(peopleSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, "file://"+loc,
		iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident("crm.people"), meta, f.cat.write(t, loc, meta), localFS, f.cat)
	f.cat.mu.Lock()
	f.cat.tables["crm.people"] = tbl
	f.cat.mu.Unlock()
	tbl = appendRows(t, tbl, eve)
	tbl = appendRows(t, tbl, ann, subject)
	tbl = appendRows(t, tbl, carl, dave)
	byIDs := filesByIDs(t, f, tbl)
	a, b := byIDs["1,2"], byIDs["3,4"]
	if !strings.HasPrefix(a, "file://") {
		t.Fatalf("a is %s", a)
	}
	rowCopyDeletes(t, f, "crm.people", "deletes-with-rows.parquet", [][3]any{{a, 1, subject}, {b, 0, carl}})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "4", "5"}) {
		t.Errorf("rows: %v", got)
	}
	f.clean(storedEmail)
}

// s3FS serves s3://bucket/ from a local directory, as S3 serves a table.
type s3FS struct{ dir string }

func (s s3FS) local(p string) string {
	return filepath.Join(s.dir, strings.TrimPrefix(p, "s3://bucket/"))
}

func (s s3FS) Open(p string) (iceio.File, error) { return iceio.LocalFS{}.Open(s.local(p)) }

func (s s3FS) Remove(p string) error { return iceio.LocalFS{}.Remove(s.local(p)) }

func (s s3FS) Create(p string) (iceio.FileWriter, error) { return iceio.LocalFS{}.Create(s.local(p)) }

func (s s3FS) WriteFile(p string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(s.local(p)), 0o755); err != nil {
		return err
	}
	return iceio.LocalFS{}.WriteFile(s.local(p), b)
}

// On a table on S3, whose files are spelled s3://, a rewrite removes the
// position delete file that holds the row copy of the person's row with
// the file it deletes rows of, and keeps the one that deletes rows of a
// file it keeps: the files it replaces are compared with the delete files'
// targets as both spell them.
func TestRewriteOnS3(t *testing.T) {
	f := newFixture(t)
	fsys := s3FS{dir: filepath.Join(f.dir, "warehouse")}
	f.cat.io = map[string]iceio.IO{"crm.people": fsys}
	f.cat.locals = fsys.local
	loc := "s3://bucket/crm/people"
	meta, err := table.NewMetadata(peopleSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc,
		iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident("crm.people"), meta, f.cat.write(t, loc, meta), f.cat.fs("crm.people"), f.cat)
	f.cat.mu.Lock()
	f.cat.tables["crm.people"] = tbl
	f.cat.mu.Unlock()
	tbl = appendRows(t, tbl, ann, subject)
	tbl = appendRows(t, tbl, carl, dave)
	byIDs := filesByIDs(t, f, tbl)
	a, b := byIDs["1,2"], byIDs["3,4"]
	if !strings.HasPrefix(a, "s3://bucket/") {
		t.Fatalf("a is %s", a)
	}
	// P holds the person's row, of a; Q deletes dave, of b.
	rowCopyDeletes(t, f, "crm.people", "p-rows.parquet", [][3]any{{a, 1, subject}})
	rowCopyDeletes(t, f, "crm.people", "q-rows.parquet", [][3]any{{b, 1, dave}})
	view := f.load("crm.people")
	if got := f.ids(view, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Fatalf("rows before: %v", got)
	}
	r := &Request{ID: "r", salt: "0123456789abcdef0123456789abcdef", written: map[string]bool{}}
	r.Subjects = []Subject{testSubject(r.salt, "pii", "email", requestedEmail)}
	if r.fold, err = f.svc.folding(f.ctx); err != nil {
		t.Fatal(err)
	}
	tt := &Table{Name: "crm.people", Ident: ident("crm.people"), Columns: []Column{{Name: "email", Tag: "pii", Value: "email"}}}
	tasks, err := view.Scan().PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	selected := slices.DeleteFunc(tasks, func(task table.FileScanTask) bool { return task.File.FilePath() != a })
	head, err := readCurrent(f.ctx, view)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.rewriteFiles(f.ctx, r, tt, view, table.MainBranch, fields(view, tt), selected, nil, head.live(true)); err != nil {
		t.Fatal(err)
	}
	after := f.load("crm.people")
	if got := f.ids(after, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("rows after: %v", got)
	}
	now, err := readCurrent(f.ctx, after)
	if err != nil {
		t.Fatal(err)
	}
	var deletes []string
	for p, df := range now.live(true) {
		if df.ContentType() == iceberg.EntryContentPosDeletes {
			deletes = append(deletes, base(p))
		}
	}
	if !slices.Equal(deletes, []string{"q-rows.parquet"}) {
		t.Errorf("position deletes left: %v; want q-rows.parquet alone", deletes)
	}
}

// What rewriteRef and rewriteFiles decide of position delete files, with
// the paths a table on S3 has: a delete file is rewritten with the files
// it deletes rows of however it spells them, goes at once if none is live,
// and goes with a commit that replaces every one, as the commit's files
// are spelled; one that names a file otherwise (s3a://) than its manifest
// does stays.
func TestRowCopiesOnS3(t *testing.T) {
	ctx := context.Background()
	s := New(Options{})
	file := func(content iceberg.ManifestEntryContent, p, target string) iceberg.DataFile {
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content, p, iceberg.ParquetFile, nil, nil, nil, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if target != "" {
			b.LowerBoundValues(map[int][]byte{2147483546: []byte(target)}).UpperBoundValues(map[int][]byte{2147483546: []byte(target)})
		}
		return b.Build()
	}
	const dir = "s3://bucket/wh/crm/people/data/"
	a, b := file(iceberg.EntryContentData, dir+"a.parquet", ""), file(iceberg.EntryContentData, dir+"b.parquet", "")
	ofA := file(iceberg.EntryContentPosDeletes, dir+"of-a.parquet", dir+"a.parquet")
	ofGone := file(iceberg.EntryContentPosDeletes, dir+"of-gone.parquet", dir+"rewritten-elsewhere.parquet")
	ofB := file(iceberg.EntryContentPosDeletes, dir+"of-b.parquet", "s3a://bucket/wh/crm/people/data/b.parquet")
	live := map[string]iceberg.DataFile{}
	for _, df := range []iceberg.DataFile{a, b, ofA, ofGone, ofB} {
		live[df.FilePath()] = df
	}
	tasks := []table.FileScanTask{{File: a, DeleteFiles: []iceberg.DataFile{ofA, ofGone}}, {File: b, DeleteFiles: []iceberg.DataFile{ofB}}}
	targeted, gone, err := s.rowCopies(ctx, nil, []string{ofA.FilePath(), ofGone.FilePath(), ofB.FilePath()}, live, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if !targeted.Has(a.FilePath()) || !targeted.Has(b.FilePath()) || len(gone) != 1 || gone[0] != ofGone {
		t.Errorf("targeted %v, gone %v", targeted, gone)
	}
	removable, err := s.removable(ctx, nil, tasks[:1], live)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range removable {
		names = append(names, base(d.FilePath()))
	}
	if slices.Sort(names); !slices.Equal(names, []string{"of-a.parquet", "of-gone.parquet"}) {
		t.Errorf("removable with a: %v", names)
	}
	if removable, err := s.removable(ctx, nil, tasks[1:], live); err != nil || len(removable) != 0 {
		t.Errorf("removable with b: %v, %v", removable, err)
	}
}

// ------------------------------------------------------ other refusals

// A table with a file that is not Parquet, which Igloo can neither search
// for identifiers nor rewrite, is refused, as a request is made and again
// before its first DELETE.
func TestFilesOtherThanParquetAreRefused(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, tbl.Location()+"/data/deletes.avro",
		iceberg.AvroFile, nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	tx := tbl.NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(b.Build()).Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "Parquet files only") {
		t.Errorf("submit: %v", err)
	}
	got := runFor(f, r.ID, 1)
	if got.Status != StatusFailed || got.DeleteStarted || !strings.Contains(got.Error, "deletes.avro") {
		t.Errorf("status %s, delete started %v, error %q", got.Status, got.DeleteStarted, got.Error)
	}
}

// A request's tables are reported while it is not over, for SQL and the
// Iceberg REST endpoint to keep their format version.
func TestErasing(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.svc.rows = failingRows{}
	_ = f.svc.Run(f.ctx, r.ID)
	for _, c := range []struct {
		schema, table string
		want          bool
	}{{"crm", "people", true}, {"CRM", "People", true}, {"crm", "other", false}} {
		if got, err := f.svc.Erasing(f.ctx, c.schema, c.table); err != nil || got != c.want {
			t.Errorf("%s.%s: %v, %v", c.schema, c.table, got, err)
		}
	}
	f.svc.rows = f.rows
	f.completed(f.run(r.ID))
	if got, err := f.svc.Erasing(f.ctx, "crm", "people"); err != nil || got {
		t.Errorf("once the request completed: %v, %v", got, err)
	}
}
