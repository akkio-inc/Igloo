package erasure

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/policy"
)

// The third review's findings, each reproduced by a test it wrote, and
// fixed.

// column returns, for each row of tbl at snapshot, the value of column col
// as text, by id, and whether the snapshot's schema has the column.
func column(t *testing.T, f *fixture, tbl *table.Table, snapshot int64, col string) (map[string]string, bool) {
	t.Helper()
	rows, err := tbl.Scan(table.WithSnapshotID(snapshot)).ToArrowTable(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Release()
	idx := rows.Schema().FieldIndices(col)
	out := map[string]string{}
	tr := array.NewTableReader(rows, 1000)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		for i := range int(rec.NumRows()) {
			v := "<no column>"
			if len(idx) == 1 {
				if c := rec.Column(idx[0]); c.IsNull(i) {
					v = "NULL"
				} else {
					v = c.ValueStr(i)
				}
			}
			out[fmt.Sprint(ids.Value(i))] = v
		}
	}
	return out, len(idx) == 1
}

// A tag made before a column was dropped still reads that column, as a tag
// reads its snapshot with the snapshot's own schema. Its rewrite reads and
// writes it with that schema, and the snapshot that replaces it keeps it:
// with the table's current one, every row of the tag lost the column.
func TestTagRewriteKeepsItsColumns(t *testing.T) {
	f := newFixture(t)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "device_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 5, Name: "revenue", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := f.cat.create(t, f.dir, "crm.people", schema)
	tbl = appendRows(t, tbl,
		[]any{int64(1), "ann@other.com", "dev-ann", "keep", "ann-revenue-100"},
		[]any{int64(2), storedEmail, "dev-x", "erase", "subject-revenue"})
	s1 := tbl.CurrentSnapshot()
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
		[]table.Update{table.NewSetSnapshotRefUpdate("q3-audit", s1.SnapshotID, table.TagRef, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	// Later, someone drops revenue from the table.
	tx := f.load("crm.people").NewTransaction()
	if err := tx.UpdateSchema(true, false).DeleteColumn([]string{"revenue"}).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	appendRows(t, f.load("crm.people"), carl)
	if before, ok := column(t, f, f.load("crm.people"), s1.SnapshotID, "revenue"); !ok || before["1"] != "ann-revenue-100" {
		t.Fatalf("the tag before the request: %v", before)
	}

	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	tbl = f.load("crm.people")
	tag := refs(tbl)["q3-audit"].SnapshotID
	after, ok := column(t, f, tbl, tag, "revenue")
	if !ok || after["1"] != "ann-revenue-100" || len(after) != 1 {
		t.Errorf("the tag after the request: revenue %v (present %v); want ann's row, with its revenue", after, ok)
	}
	if got := tbl.SnapshotByID(tag).SchemaID; got == nil || *got != *s1.SchemaID {
		t.Errorf("the tag's snapshot has schema %v, want %d, its own", got, *s1.SchemaID)
	}
	// The table's current schema, and main, are as they were.
	if _, ok := tbl.Schema().FindFieldByName("revenue"); ok {
		t.Errorf("the current schema has revenue again")
	}
	if got := f.ids(tbl, 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("main: %v", got)
	}
	f.clean(storedEmail)
}

// Tables of format version 3 can hold deletion vectors, which iceberg-go
// cannot read: a request would delete their rows and then fail to rewrite
// the files that hold them, for good, unable to be canceled. It is refused
// before it deletes anything, when it is made and when it runs.
func TestFormatVersion3IsRefused(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
		t.Fatal(err)
	}
	for _, try := range []func() error{
		func() error { _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); return err },
		func() error { _, err := f.svc.Preview(f.ctx, []Wanted{byEmail}); return err },
	} {
		if err := try(); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "format version 3") {
			t.Errorf("a request for a table of format version 3: %v", err)
		}
	}
	// One made before the upgrade stops before its DELETE, and can be
	// canceled.
	_ = f.svc.Run(f.ctx, r.ID)
	got, _ := f.store.Get(f.ctx, r.ID)
	if got.DeleteStarted || got.Status != StatusFailed || !strings.Contains(got.Error, "format version 3") {
		t.Errorf("status %s, delete started %v, error %q", got.Status, got.DeleteStarted, got.Error)
	}
	if got := f.ids(f.load("crm.people"), 0); len(got) != 2 {
		t.Errorf("rows were deleted: %v", got)
	}
}

// A Parquet position delete file scoped to one data file names it as its
// referenced data file, as Java's Iceberg (1.7 and later) writes Spark's.
// iceberg-go took it for a deletion vector, which it cannot read; it is
// read as a position delete file, and the request completes.
func TestFileScopedPositionDeletes(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject, carl})
	tasks, err := tbl.Scan().PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := tasks[0].File.FilePath()
	p := tbl.Location() + "/data/file-scoped-deletes.parquet"
	// Spark's DELETE of carl (position 2), scoped to the file.
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT '%s' AS file_path, 2::BIGINT AS pos) TO '%s' (FIELD_IDS {file_path: 2147483546, pos: 2147483545})", data, p))
	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, p, iceberg.ParquetFile, nil, nil, nil, 1, 600)
	if err != nil {
		t.Fatal(err)
	}
	tx := f.load("crm.people").NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(b.ReferencedDataFile(data).Build()).Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "2"}) {
		t.Fatalf("rows before: %v", got)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1"}) {
		t.Errorf("rows after: %v", got)
	}
	f.clean(storedEmail)
}

// The hold counts the rows of every request made within a day of this
// one, before or after it, in whatever order they run; while one of them
// has yet to count its rows, a request waits for it, and is held if it
// still has not after countWait.
func TestHoldCountsRequestsInEitherOrder(t *testing.T) {
	setup := func(t *testing.T) (*fixture, *Request, *Request) {
		f := newFixture(t)
		var rows [][]any
		for i := range 300 {
			rows = append(rows, []any{int64(i), fmt.Sprintf("p%d@other.com", i), fmt.Sprintf("dev-%d", i/100), "keep"})
		}
		f.people("crm.people", rows)
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "device_id", Value: "device_id"})
		a, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "device_id", Identifiers: []string{"dev-0"}}})
		if err != nil {
			t.Fatal(err)
		}
		b, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "device_id", Identifiers: []string{"dev-1"}}})
		if err != nil {
			t.Fatal(err)
		}
		return f, a, b
	}
	run := func(f *fixture, id, dev string) *Request {
		f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("device_id"), dev)
		_ = f.svc.Run(f.ctx, id)
		r, _ := f.store.Get(f.ctx, id)
		return r
	}

	t.Run("the later first", func(t *testing.T) {
		f, a, b := setup(t)
		if rb := run(f, b.ID, "dev-1"); rb.Status != StatusWaiting || !strings.Contains(rb.Error, a.ID) {
			t.Fatalf("b, while a has not counted: %s, %q", rb.Status, rb.Error)
		}
		if ra := run(f, a.ID, "dev-0"); ra.Status != StatusHeld {
			t.Errorf("a, with b's rows: %s", ra.Status)
		}
		if rb := run(f, b.ID, "dev-1"); rb.Status != StatusHeld {
			t.Errorf("b, with a's rows: %s", rb.Status)
		}
		if got := len(f.ids(f.load("crm.people"), 0)); got != 300 {
			t.Errorf("rows left: %d of 300", got)
		}
	})

	t.Run("the other never counts", func(t *testing.T) {
		f, _, b := setup(t)
		now := time.Now()
		f.svc.now = func() time.Time { return now }
		if rb := run(f, b.ID, "dev-1"); rb.Status != StatusWaiting {
			t.Fatalf("b: %s", rb.Status)
		}
		later := now.Add(countWait + time.Minute)
		f.svc.now = func() time.Time { return later }
		if rb := run(f, b.ID, "dev-1"); rb.Status != StatusHeld {
			t.Errorf("b, after waiting %v for a: %s", countWait, rb.Status)
		}
	})
}

// The spec lets a position delete file carry the row it deletes in its row
// column (Java's PositionDeleteWriter writes it when given a row schema):
// such a file that holds the person's row goes, with the files it deletes
// rows of rewritten so it can, and verification looks in such files.
func TestPositionDeleteRowColumn(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject}, [][]any{carl, dave})
	tasks, err := tbl.Scan().PlanFiles(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var a, b string
	for _, task := range tasks {
		ids := strings.Join(f.eng.values(t, fmt.Sprintf("SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM '%s'", task.File.FilePath())), "")
		if ids == "1,2" {
			a = task.File.FilePath()
		} else {
			b = task.File.FilePath()
		}
	}
	p := tbl.Location() + "/data/deletes-with-rows.parquet"
	// Someone's DELETE, scoped to the partition, of the subject (a,
	// position 1) and of carl (b, position 0), with the rows.
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT * FROM (VALUES ('%s', 1::BIGINT, {'id': 2::BIGINT, 'email': '%s', 'device_id': 'dev-x', 'note': 'erase'}), "+
		"('%s', 0::BIGINT, {'id': 3::BIGINT, 'email': 'carl@other.com', 'device_id': 'dev-carl', 'note': 'keep'})) v(file_path, pos, row)) TO '%s' "+
		"(FIELD_IDS {file_path: 2147483546, pos: 2147483545, row: {__duckdb_field_id: 2147483544, id: 1, email: 2, device_id: 3, note: 4}})",
		a, storedEmail, b, p))
	bld, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes, p, iceberg.ParquetFile, nil, nil, nil, 2, 900)
	if err != nil {
		t.Fatal(err)
	}
	tx := f.load("crm.people").NewTransaction()
	if err := tx.NewRowDelta(nil).AddDeletes(bld.Build()).Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "4"}) {
		t.Fatalf("rows before: %v", got)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "4"}) {
		t.Errorf("rows after: %v; carl stays deleted", got)
	}
	f.clean(storedEmail)
}

// scopedFS reaches every file but those under deny, as one bucket's
// credentials do not reach another's.
type scopedFS struct {
	iceio.LocalFS
	deny string
}

func (s scopedFS) check(p string) error {
	if strings.HasPrefix(strings.TrimPrefix(p, "file://"), s.deny) {
		return fmt.Errorf("access denied: %s", p)
	}
	return nil
}

func (s scopedFS) Open(p string) (iceio.File, error) {
	if err := s.check(p); err != nil {
		return nil, err
	}
	return s.LocalFS.Open(p)
}

func (s scopedFS) Remove(p string) error {
	if err := s.check(p); err != nil {
		return err
	}
	return s.LocalFS.Remove(p)
}

func (s scopedFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	if err := s.check(root); err != nil {
		return err
	}
	return s.LocalFS.WalkDir(root, fn)
}

// Every file is listed and deleted with the IO of the table it belongs
// to: the first table's could not reach another's location.
func TestFilesAreReachedThroughTheirTables(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	beta := f.people("crm.beta", [][]any{carl, subject})
	// alpha, the first of the request's tables, cannot reach beta's files.
	f.cat.io = map[string]iceio.IO{"crm.alpha": scopedFS{deny: beta.Location()}}
	f.cat.reopen(t, "crm.alpha")
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	r := f.erase(byEmail)
	f.completed(r)
	for _, name := range []string{"crm.alpha", "crm.beta"} {
		i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == name })
		if i < 0 || r.Tables[i].FilesDeleted == 0 {
			t.Errorf("%s: %+v", name, r.Tables)
		}
	}
	f.clean(storedEmail)
}

// The scrubber removes the request's salt and hashes, whole or cut short,
// and leaves other long runs of digits, as snapshot IDs and storage
// request IDs, which say what went wrong.
func TestScrubKeepsOtherIDs(t *testing.T) {
	salt := "4f3c2a1b0e9d8c7b6a5f4e3d2c1b0a99887766554433221100ffeeddccbbaa00"
	r := &Request{salt: salt, Subjects: []Subject{testSubject(salt, "pii", "email", "ann@example.com")}}
	hash := r.Subjects[0].plain[0]
	msg := scrub(r, fmt.Sprintf("commit of snapshot 6532141867238520153 failed (S3 request 4442587FB7D0A2F9): sha256('%s' || x) IN ('%s', '%s...')",
		salt, hash, hash[:20]))
	for _, keep := range []string{"6532141867238520153", "4442587FB7D0A2F9"} {
		if !strings.Contains(msg, keep) {
			t.Errorf("%s was scrubbed: %s", keep, msg)
		}
	}
	if strings.Contains(msg, salt[:16]) || strings.Contains(msg, hash[:16]) {
		t.Errorf("not scrubbed: %s", msg)
	}
}

// A UUID compares as a UUID column holds it, with dashes, and as a binary
// column holds it, as hex, whichever form it is given in.
func TestUUIDInBinaryColumns(t *testing.T) {
	f := newFixture(t)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "device", Type: iceberg.PrimitiveTypes.Binary})
	tbl := f.cat.create(t, f.dir, "ads.devices", schema)
	device := []byte{0x6f, 0x96, 0x19, 0xff, 0x8b, 0x86, 0xd0, 0x11, 0xb4, 0x2d, 0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff}
	other := []byte{0x01, 0x96, 0x19, 0xff, 0x8b, 0x86, 0xd0, 0x11, 0xb4, 0x2d, 0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff}
	tx := tbl.NewTransaction()
	if err := tx.AppendTable(f.ctx, arrowRows(t, tbl, []any{int64(1), other}, []any{int64(2), device}), 1000, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "ads", Table: "devices", Column: "device", Value: "device_id"})
	f.completed(f.erase(Wanted{Tag: "pii", Value: "device_id", Identifiers: []string{"6F9619FF-8B86-D011-B42D-00C04FC964FF"}}))
	if got := f.ids(f.load("ads.devices"), 0); !slices.Equal(got, []string{"1"}) {
		t.Errorf("rows after: %v", got)
	}
}

// A snapshot staged by write-audit-publish waits from when the request was
// made, not from when it was staged; publishing by cherry-pick, which
// copies it onto the branch with its wap.id as published-wap-id, ends the
// wait; and a table that keeps snapshots longer is waited for longer, up
// to stagedWaitMax.
func TestStagedSnapshotPublishing(t *testing.T) {
	stage := func(t *testing.T, f *fixture) (int64, string) {
		tbl := f.people("crm.people", [][]any{ann})
		commit := func(updates ...table.Update) {
			t.Helper()
			if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, updates); err != nil {
				t.Fatal(err)
			}
		}
		commit(table.NewSetSnapshotRefUpdate("stage", tbl.CurrentSnapshot().SnapshotID, table.BranchRef, 0, 0, 0))
		tbl = f.load("crm.people")
		tx := tbl.NewTransactionOnBranch("stage")
		if err := tx.AppendTable(f.ctx, arrowRows(t, tbl, []any{int64(4), "eve@other.com", "dev-eve", "unpublished"},
			[]any{int64(5), storedEmail, "dev-y", "unpublished"}), 1000, iceberg.Properties{"wap.id": "audit-42"}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		staged := refs(f.load("crm.people"))["stage"].SnapshotID
		commit(table.NewRemoveSnapshotRefUpdate("stage"))
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		return staged, "audit-42"
	}

	t.Run("cherry-picked", func(t *testing.T) {
		f := newFixture(t)
		staged, wap := stage(t, f)
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		_ = f.svc.Run(f.ctx, r.ID)
		if got, _ := f.store.Get(f.ctx, r.ID); got.Status != StatusWaiting {
			t.Fatalf("status %s", got.Status)
		}
		// Spark's cherry-pick, onto a branch that moved on: a new snapshot
		// with the staged rows and published-wap-id.
		appendRows(t, f.load("crm.people"), carl)
		tbl := f.load("crm.people")
		tx := tbl.NewTransaction()
		if err := tx.AppendTable(f.ctx, arrowRows(t, tbl, []any{int64(4), "eve@other.com", "dev-eve", "unpublished"},
			[]any{int64(5), storedEmail, "dev-y", "unpublished"}), 1000, iceberg.Properties{"published-wap-id": wap}); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		later := time.Now().Add(stagedRecheck + time.Minute)
		f.svc.now = func() time.Time { return later }
		f.completed(f.run(r.ID))
		if f.load("crm.people").SnapshotByID(staged) != nil {
			t.Error("the published staged snapshot was kept")
		}
		if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3", "4"}) {
			t.Errorf("rows: %v", got)
		}
		f.clean(storedEmail)
	})

	t.Run("from the request", func(t *testing.T) {
		f := newFixture(t)
		staged, _ := stage(t, f)
		// The request is made long after the snapshot was staged.
		made := time.Now().Add(8 * 24 * time.Hour)
		f.svc.now = func() time.Time { return made }
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		_ = f.svc.Run(f.ctx, r.ID)
		if got, _ := f.store.Get(f.ctx, r.ID); got.Status != StatusWaiting || f.load("crm.people").SnapshotByID(staged) == nil {
			t.Fatalf("status %s", got.Status)
		}
		later := made.Add(stagedWait + time.Hour)
		f.svc.now = func() time.Time { return later }
		f.completed(f.run(r.ID))
		if f.load("crm.people").SnapshotByID(staged) != nil {
			t.Error("a staged snapshot was kept past the wait")
		}
	})

	t.Run("a longer retention", func(t *testing.T) {
		f := newFixture(t)
		_, _ = stage(t, f)
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{
			"history.expire.max-snapshot-age-ms": fmt.Sprint((14 * 24 * time.Hour).Milliseconds())})}); err != nil {
			t.Fatal(err)
		}
		r := &Request{Created: time.Now()}
		if got := stagedUntil(r, f.load("crm.people")); got.Sub(r.Created) != 14*24*time.Hour {
			t.Errorf("waits %v", got.Sub(r.Created))
		}
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{
			"history.expire.max-snapshot-age-ms": "9223372036854775807"})}); err != nil {
			t.Fatal(err)
		}
		if got := stagedUntil(r, f.load("crm.people")); got.Sub(r.Created) != stagedWaitMax {
			t.Errorf("waits %v, want at most %v", got.Sub(r.Created), stagedWaitMax)
		}
	})
}

// A table the catalog cannot load for a moment stops the run, which tries
// again without counting it a failure. One it refuses for good is left out
// of the search for tables that share files, but, as which of the
// warehouse's files are whose cannot be told while it does (see unowned),
// the request does not complete: the run fails, saying so, and the request
// goes on once the table loads.
func TestUnloadableTables(t *testing.T) {
	for _, c := range []struct {
		err      error
		want     string
		failures int
	}{
		{fmt.Errorf("%w: 503", rest.ErrServiceUnavailable), StatusRetrying, 0},
		{fmt.Errorf("%w: 500 lost its database connection", rest.ErrServerError), StatusRetrying, 0},
		{fmt.Errorf("%w: 403", rest.ErrForbidden), StatusRetrying, 1},
	} {
		t.Run(c.err.Error(), func(t *testing.T) {
			f := newFixture(t)
			f.people("crm.people", [][]any{ann, subject})
			broken := &brokenCatalog{testCatalog: f.cat, broken: "junk.broken", err: c.err}
			f.svc.opts.Catalog = broken
			f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			_ = f.svc.Run(f.ctx, r.ID)
			got, _ := f.store.Get(f.ctx, r.ID)
			if got.Status != c.want || got.Failures != c.failures || c.failures > 0 && !strings.Contains(got.Error, "junk.broken") {
				t.Errorf("status %s, failures %d, error %q", got.Status, got.Failures, got.Error)
			}
			broken.err = nil
			f.completed(f.run(r.ID))
		})
	}
}

// brokenCatalog's table broken cannot be loaded, with err, while err is
// set.
type brokenCatalog struct {
	*testCatalog
	broken string
	err    error
}

func (b *brokenCatalog) Tables(ctx context.Context) ([]table.Identifier, error) {
	ids, err := b.testCatalog.Tables(ctx)
	return append(ids, ident(b.broken)), err
}

func (b *brokenCatalog) LoadTable(ctx context.Context, id table.Identifier) (*table.Table, error) {
	if tableName(id) == b.broken {
		if b.err != nil {
			return nil, b.err
		}
		return nil, icecat.ErrNoSuchTable
	}
	return b.testCatalog.LoadTable(ctx, id)
}
