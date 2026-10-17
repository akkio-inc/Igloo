package erasure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/policy"
)

// The second review's findings, each reproduced by a test it wrote, and
// fixed.

var dave = []any{int64(4), "dave@other.com", "dev-dave", "keep"}

// A live data file the metadata spells otherwise than the listing does
// (file://, as s3a:// against s3://) is the same file, and is not deleted
// as an orphan once it is old enough.
func TestPurgeKeepsLiveFilesSpelledOtherwise(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	p := filepath.Join(tbl.Location(), "data", "added.parquet")
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT * FROM (VALUES (3::BIGINT, 'carl@other.com', 'dev-carl', 'keep'), (4::BIGINT, 'dave@other.com', 'dev-dave', 'keep'))"+
		" v(id, email, device_id, note)) TO '%s' (FIELD_IDS {id: 1, email: 2, device_id: 3, note: 4})", p))
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(f.ctx, []string{"file://" + p}, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-96 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("a live data file of the table was deleted as an orphan: %v", err)
	}
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3", "4"}) {
		t.Errorf("rows: %v", got)
	}
}

func TestCanonicalPaths(t *testing.T) {
	for _, c := range []struct{ a, b string }{
		{"s3a://bucket/wh/t/data/a.parquet", "s3://bucket/wh/t/data/a.parquet"},
		{"file:///tmp/wh/t/a.parquet", "/tmp/wh/t/a.parquet"},
		{"file://localhost/tmp/wh/t/a.parquet", "/tmp/wh/t/a.parquet"},
		{"s3://bucket/wh//t/data/a.parquet", "s3://bucket/wh/t/data/a.parquet"},
		{"s3://bucket/wh/t/data/o%27brien.parquet", "s3://bucket/wh/t/data/o'brien.parquet"},
		{"gs://bucket/wh/t/", "gs://bucket/wh/t"},
	} {
		if canonical(c.a) != canonical(c.b) {
			t.Errorf("%s and %s: %s, %s", c.a, c.b, canonical(c.a), canonical(c.b))
		}
	}
	if canonical("s3://bucket/a/x.parquet") == canonical("s3://bucket/b/x.parquet") {
		t.Errorf("two files are one")
	}
	if !within("s3a://bucket/wh/t/data/a.parquet", "s3://bucket/wh/") || within("s3://bucket/wh2/a", "s3://bucket/wh") {
		t.Errorf("within")
	}
}

// A position delete file scoped to a partition (as iceberg-go, and Spark
// before Iceberg 1.6, write them) that deletes rows of a file the request
// rewrites and of one it does not stays: the other file's deleted rows
// stay deleted.
func TestPositionDeletesOfFilesKept(t *testing.T) {
	f := newFixture(t)
	// One file of ann and the subject, one of carl and dave.
	f.people("crm.people", [][]any{ann, subject}, [][]any{carl, dave})
	if _, err := f.load("crm.people").Delete(f.ctx, iceberg.IsIn(iceberg.Reference("id"), int64(1), int64(3)), nil); err != nil {
		t.Fatal(err)
	}
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"2", "4"}) {
		t.Fatalf("before the request: %v", got)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"4"}) {
		t.Errorf("rows after the request %v, want [4]: carl, deleted before, came back", got)
	}
	f.clean(storedEmail)
}

// With commit.retry.num-retries set, iceberg-go would make a rewrite that
// lost a race again on the new head by adding that head's manifests, which
// still list the rewritten file live: its rows came back twice. The
// request's commits are never retried by iceberg-go; it makes them again
// itself, on a head that only appends moved, with the files it wrote.
func TestCommitRetriesAreNotReplayed(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject}, [][]any{carl})
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
		[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{table.CommitNumRetriesKey: "3"})}); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	raced, busy := false, false
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		if raced || busy || tableName(id) != "crm.people" || !addsSnapshot(updates) {
			return
		}
		raced, busy = true, true
		// Another writer's append lands just before the request's first
		// commit.
		appendRows(t, f.load("crm.people"), []any{int64(9), "zed@other.com", "dev-z", "concurrent"})
		busy = false
	}
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3", "9"}) {
		t.Errorf("rows after the request %v, want [1 3 9]", got)
	}
	if !raced {
		t.Fatal("no commit raced")
	}
	f.clean(storedEmail)
}

func addsSnapshot(updates []table.Update) bool {
	return slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == table.UpdateAddSnapshot })
}

// Placeholders, and the digests of placeholders, are refused, in every
// spelling the review found.
func TestPlaceholdersAreRefused(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.cat.create(t, f.dir, "aud.segments", audienceSchema)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "device_id", Value: "device_id"},
		policy.ColumnTag{Schema: "aud", Table: "segments", Column: "email_sha256", Value: "email:sha256"})
	refused := func(value, id string) {
		t.Helper()
		_, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: value, Identifiers: []string{id}}})
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "stands for no one") {
			t.Errorf("%s %q: %v", value, id, err)
		}
	}
	for _, id := range []string{"00000000-0000-0000-0000-000000000000", "{00000000-0000-0000-0000-000000000000}", "-1",
		" null ", "\tnull", "\uff10\uff10\uff10\uff10\uff10", "ffffffff-ffff-ffff-ffff-ffffffffffff", "(null)", "<null>", "nan",
		"noemail@noemail.com", "00000000_0000", "0x0", "0e0"} {
		refused("device_id", id)
	}
	for _, id := range []string{sha256Hex(""), sha256Hex("null"), sha256Hex("unknown"), hexDigest("md5", "")} {
		refused("email:sha256", id)
	}
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{{Tag: "pii", Value: "device_id", Identifiers: []string{"dev-x"}}}); err != nil {
		t.Errorf("a device ID: %v", err)
	}
}

// The hold counts what other requests made within a day matched in the
// same table, so a table cannot be emptied by requests that each stay
// under it; a request someone confirmed counts no more. A request that
// matches every row of a table is held, however small, and its requester
// cannot confirm it.
func TestHoldCountsRecentRequests(t *testing.T) {
	f := newFixture(t)
	var rows [][]any
	for i := range 300 {
		rows = append(rows, []any{int64(i), fmt.Sprintf("p%d@other.com", i), fmt.Sprintf("dev-%d", i/100), "keep"})
	}
	f.people("crm.people", rows)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "device_id", Value: "device_id"})
	request := func(dev string) *Request {
		t.Helper()
		f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("device_id"), dev)
		return f.erase(Wanted{Tag: "pii", Value: "device_id", Identifiers: []string{dev}})
	}
	r0 := request("dev-0")
	f.completed(r0)
	r1 := request("dev-1")
	if r1.Status != StatusHeld {
		t.Fatalf("a second request of 100 rows of 200 within a day: %s", r1.Status)
	}
	if _, err := f.svc.Confirm(f.ctx, r1.ID, "privacy"); !errors.Is(err, ErrInvalid) {
		t.Errorf("its requester confirmed it: %v", err)
	}
	if _, err := f.svc.Confirm(f.ctx, r1.ID, "pat"); err != nil {
		t.Fatal(err)
	}
	f.completed(f.run(r1.ID))
	// The first request, unconfirmed, still counts.
	if r2 := request("dev-2"); r2.Status != StatusHeld {
		t.Errorf("a third: %s", r2.Status)
	}
	if got := len(f.ids(f.load("crm.people"), 0)); got != 100 {
		t.Errorf("rows left: %d", got)
	}

	f.people("crm.devices", [][]any{{int64(1), "a@other.com", "dev-all", "keep"}, {int64(2), "b@other.com", "dev-all", "keep"}})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "devices", Column: "device_id", Value: "device_id"})
	f.rows["crm.devices"] = iceberg.EqualTo(iceberg.Reference("device_id"), "dev-all")
	f.rows["crm.people"] = iceberg.AlwaysFalse{}
	r3 := f.erase(Wanted{Tag: "pii", Value: "device_id", Identifiers: []string{"dev-all"}})
	if r3.Status != StatusHeld {
		t.Errorf("a request that empties a table of two rows: %s", r3.Status)
	}
}

// A replica whose DELETE committed though no answer came cannot cancel the
// request: its rows are gone, and still in the table's files.
type unknownCommitRows struct{ testRows }

func (u unknownCommitRows) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	if _, err := u.testRows.Delete(ctx, tbl, cond); err != nil {
		return 0, err
	}
	return 0, errors.New("commit state unknown: 504 Gateway Timeout")
}

func TestNoCancelOnceADeleteWasSent(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("email"), storedEmail)
	f.svc.rows = unknownCommitRows{f.rows}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	_ = f.svc.Run(f.ctx, r.ID)
	if got, _ := f.store.Get(f.ctx, r.ID); got.Status != StatusRetrying || got.Step != StepDelete || !got.DeleteStarted {
		t.Fatalf("after the run: %s at %s, delete started %v, error %q", got.Status, got.Step, got.DeleteStarted, got.Error)
	}
	if _, err := f.svc.Cancel(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "begun deleting") {
		t.Fatalf("cancel after a DELETE whose answer was lost: %v", err)
	}
	f.svc.rows = f.rows
	f.completed(f.run(r.ID))
	f.clean(storedEmail)
}

// A tagged column that none of a table's schemas has, as when the table
// was replaced by one that names the column otherwise, is an error, both
// when the request is made and while it runs: completing would leave the
// identifiers where they are.
func TestTaggedColumnGone(t *testing.T) {
	contact := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "contact", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "device_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "note", Type: iceberg.PrimitiveTypes.String})

	f := newFixture(t)
	f.cat.create(t, f.dir, "crm.people", contact)
	appendRows(t, f.load("crm.people"), ann, subject, carl)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "no column of that name") {
		t.Errorf("submit: %v", err)
	}
	if _, err := f.svc.Preview(f.ctx, []Wanted{byEmail}); !errors.Is(err, ErrInvalid) {
		t.Errorf("preview: %v", err)
	}

	// Replaced after the request was made.
	f = newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.cat.create(t, f.dir+"/replaced", "crm.people", contact)
	appendRows(t, f.load("crm.people"), ann, subject)
	r = f.run(r.ID)
	if r.Status == StatusCompleted || !strings.Contains(r.Error, "no column of that name") {
		t.Errorf("status %s, error %q", r.Status, r.Error)
	}
}

// A snapshot staged by write-audit-publish (Spark's wap.id), which no
// branch or tag has, holds others' unpublished rows: the request waits
// for it to be published, and then rewrites it, or discarded, and expires
// it only once it is older than maintenance would keep it.
func TestStagedSnapshotWaits(t *testing.T) {
	for _, publish := range []bool{true, false} {
		t.Run(fmt.Sprintf("published=%v", publish), func(t *testing.T) {
			f := newFixture(t)
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

			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			f.svc.now = func() time.Time { return now }
			_ = f.svc.Run(f.ctx, r.ID)
			r, _ = f.store.Get(f.ctx, r.ID)
			if r.Status != StatusWaiting || r.Step != StepExpire || !strings.Contains(r.Error, "audit-42") {
				t.Fatalf("status %s at %s, error %q", r.Status, r.Step, r.Error)
			}
			if f.load("crm.people").SnapshotByID(staged) == nil {
				t.Fatal("the staged snapshot, with eve's unpublished row, was expired")
			}
			if publish {
				// Published as Spark's cherry-pick does when the branch has
				// not moved: the branch takes the staged snapshot.
				commit(table.NewSetSnapshotRefUpdate(table.MainBranch, staged, table.BranchRef, 0, 0, 0))
				later := now.Add(stagedRecheck + time.Minute)
				f.svc.now = func() time.Time { return later }
				r = f.run(r.ID)
				f.completed(r)
				if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "4"}) {
					t.Errorf("published rows: %v", got)
				}
			} else {
				later := now.Add(stagedWait + time.Hour)
				f.svc.now = func() time.Time { return later }
				r = f.run(r.ID)
				f.completed(r)
				if f.load("crm.people").SnapshotByID(staged) != nil {
					t.Error("a staged snapshot older than the wait was kept")
				}
			}
			f.clean(storedEmail)
		})
	}
}

// What Run returns, which the scheduler records in its runs, logs and
// sends in the failed_runs alert, is scrubbed of the salt and hashes,
// which DuckDB quotes from the DELETE, truncated or not.
func TestRunErrorIsScrubbed(t *testing.T) {
	f := newFixture(t)
	f.eng.exec(t, "ATTACH ':memory:' AS lake")
	f.eng.exec(t, "CREATE SCHEMA lake.crm")
	f.eng.exec(t, "CREATE TABLE lake.crm.people (id BIGINT, contact VARCHAR)")
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.svc.rows = catalogRows{engine: f.eng, catalog: "lake"}
	runErr := f.svc.Run(f.ctx, r.ID)
	got, _ := f.store.Get(f.ctx, r.ID)
	if runErr == nil || got.Status != StatusRetrying {
		t.Fatalf("run: %v, status %s", runErr, got.Status)
	}
	for _, text := range []string{runErr.Error(), got.Error} {
		if m := longHex.FindString(text); m != "" || strings.Contains(strings.ToLower(text), "subject@") {
			t.Errorf("not scrubbed (%q): %s", m, text)
		}
	}
}

// A table other writers keep appending to: the request's commits are made
// again on the new head without rewriting the files again, and win once
// the appends leave them a moment; while they never do, the request tries
// again later, and never fails for it.
func TestBusyTable(t *testing.T) {
	stream := func(f *fixture, commits int) *int {
		appended, busy := 0, false
		f.cat.before = func(id table.Identifier, updates []table.Update) {
			if busy || tableName(id) != "crm.people" || !addsSnapshot(updates) || commits >= 0 && appended >= commits {
				return
			}
			busy = true
			appended++
			appendRows(f.t, f.load("crm.people"), []any{int64(1000 + appended), fmt.Sprintf("p%d@other.com", appended), "dev", "stream"})
			busy = false
		}
		return &appended
	}

	t.Run("a while", func(t *testing.T) {
		f := newFixture(t)
		f.people("crm.people", [][]any{ann, subject})
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		appended := stream(f, rebaseAttempts/2)
		f.completed(f.erase(byEmail))
		if *appended != rebaseAttempts/2 {
			t.Errorf("appends: %d", *appended)
		}
		if got := len(f.ids(f.load("crm.people"), 0)); got != 1+rebaseAttempts/2 {
			t.Errorf("rows: %d", got)
		}
		// The rewrite wrote its file once: no file of it is left behind.
		if orphans := unreferenced(t, f, "crm.people"); len(orphans) > 0 {
			t.Errorf("files the request wrote and did not commit: %v", orphans)
		}
		f.clean(storedEmail)
	})

	t.Run("without end", func(t *testing.T) {
		f := newFixture(t)
		f.people("crm.people", [][]any{ann, subject})
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		stream(f, -1)
		var alerts []string
		f.svc.opts.Failed = func(id, step, err string) { alerts = append(alerts, err) }
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		// Runs that lose do not count as failures, so no number of them
		// fails the request.
		now := time.Now()
		for i := range 3 {
			at := now.Add(time.Duration(i) * 2 * time.Hour)
			f.svc.now = func() time.Time { return at }
			_ = f.svc.Run(f.ctx, r.ID)
		}
		got, _ := f.store.Get(f.ctx, r.ID)
		if got.Status != StatusRetrying || got.Failures != 0 || got.Sealed == "" || len(alerts) > 0 || !strings.Contains(got.Error, "other writers") {
			t.Errorf("status %s, failures %d, error %q, alerts %q", got.Status, got.Failures, got.Error, alerts)
		}
		f.cat.before = nil
		f.svc.now = time.Now
		f.completed(f.run(r.ID))
	})
}

// unreferenced are the Parquet files under a table's location that none of
// its snapshots references.
func unreferenced(t *testing.T, f *fixture, name string) []string {
	t.Helper()
	tbl := f.load(name)
	tf, err := readFiles(f.ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	referenced := tf.referenced()
	var out []string
	err = filepath.WalkDir(tbl.Location(), func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") && !referenced.has(p) {
			out = append(out, filepath.Base(p))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// tempBranchCommit reports whether a commit adds a snapshot to a branch of
// a request's.
func tempBranchCommit(updates []table.Update) bool {
	temp := false
	for _, u := range updates {
		b, _ := json.Marshal(u)
		temp = temp || strings.Contains(string(b), `"ref-name":"`+tagBranch)
	}
	return temp && addsSnapshot(updates)
}

// Someone who moves a tag while the request rewrites it keeps their move:
// the request rewrites the tag where it points now, and drops its branch
// of where it pointed.
func TestTagMovedWhileRewritten(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	s1 := tbl.CurrentSnapshot().SnapshotID
	commit := func(updates ...table.Update) {
		t.Helper()
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, updates); err != nil {
			t.Fatal(err)
		}
	}
	commit(table.NewSetSnapshotRefUpdate("audit", s1, table.TagRef, 0, 0, 0))
	s2 := appendRows(t, f.load("crm.people"), carl).CurrentSnapshot().SnapshotID
	moved := false
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		if !moved && tempBranchCommit(updates) {
			moved = true
			// CREATE OR REPLACE TAG audit, at the newer snapshot.
			commit(table.NewSetSnapshotRefUpdate("audit", s2, table.TagRef, 0, 0, 0))
		}
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if !moved {
		t.Fatal("the tag was not moved")
	}
	tbl = f.load("crm.people")
	if got := f.ids(tbl, refs(tbl)["audit"].SnapshotID); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("the tag reads %v, want [1 3]: where the user moved it, rewritten", got)
	}
	noTempBranches(t, tbl)
	f.clean(storedEmail)
}

func noTempBranches(t *testing.T, tbl *table.Table) {
	t.Helper()
	for name := range refs(tbl) {
		if strings.HasPrefix(name, tagBranch) {
			t.Errorf("the request's branch %s is left behind", name)
		}
	}
}

// A tag dropped while the request rewrites it leaves no branch of the
// request's behind, which maintenance would keep for good.
func TestTagDroppedWhileRewritten(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	commit := func(updates ...table.Update) {
		t.Helper()
		if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, updates); err != nil {
			t.Fatal(err)
		}
	}
	commit(table.NewSetSnapshotRefUpdate("audit", tbl.CurrentSnapshot().SnapshotID, table.TagRef, 0, 0, 0))
	appendRows(t, f.load("crm.people"), carl)
	dropped := false
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		if !dropped && tempBranchCommit(updates) {
			dropped = true
			commit(table.NewRemoveSnapshotRefUpdate("audit")) // DROP TAG audit
		}
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if !dropped {
		t.Fatal("the tag was not dropped")
	}
	noTempBranches(t, f.load("crm.people"))
	f.clean(storedEmail)
}

// A branch left by a run that stopped stays while the request is blocked,
// which goes on from it, and goes when the request completes.
func TestBlockedRequestKeepsItsBranches(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann, subject})
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
		[]table.Update{table.NewSetSnapshotRefUpdate("audit", tbl.CurrentSnapshot().SnapshotID, table.TagRef, 0, 0, 0)}); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	// The run stops once the branch is made, as a replica that died would.
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		if tempBranchCommit(updates) {
			panic(errStop)
		}
	}
	func() {
		defer func() {
			if p := recover(); p != errStop {
				panic(p)
			}
		}()
		_ = f.svc.Run(f.ctx, r.ID)
	}()
	f.cat.before = nil
	if !slices.ContainsFunc(slices.Collect(func(yield func(string) bool) {
		for name := range refs(f.load("crm.people")) {
			if !yield(name) {
				return
			}
		}
	}), func(n string) bool { return strings.HasPrefix(n, tagBranch) }) {
		t.Fatal("no branch was left")
	}
	got, _ := f.store.Get(f.ctx, r.ID)
	f.svc.failed(f.ctx, got, terminal(errors.New("cannot go on")))
	if err := f.store.Save(f.ctx, got); err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusBlocked || got.Sealed == "" {
		t.Fatalf("status %s", got.Status)
	}
	if !slices.ContainsFunc(slices.Collect(maps.Keys(refs(f.load("crm.people")))), func(n string) bool { return strings.HasPrefix(n, tagBranch) }) {
		t.Error("the blocked request's branch was dropped")
	}
	f.completed(f.run(r.ID))
	noTempBranches(t, f.load("crm.people"))
	f.clean(storedEmail)
}

var errStop = errors.New("stop")

// A branch or tag made at a snapshot the request expires, after it looked
// at the table's refs, is not lost with the snapshot: every ref is
// asserted as the catalog has them just before the commit, and one made
// meanwhile is rewritten like the others.
type hookStore struct {
	*MemoryStore
	addFiles func(files []File)
}

func (h hookStore) AddFiles(ctx context.Context, id string, files []File) error {
	if h.addFiles != nil {
		h.addFiles(files)
	}
	return h.MemoryStore.AddFiles(ctx, id, files)
}

func TestRefMadeWhileExpiring(t *testing.T) {
	f := newFixture(t)
	s1 := f.people("crm.people", [][]any{ann, subject}).CurrentSnapshot().SnapshotID
	appendRows(t, f.load("crm.people"), carl)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	made := false
	f.svc.opts.Store = hookStore{MemoryStore: f.store, addFiles: func(files []File) {
		if !made && slices.ContainsFunc(files, func(file File) bool { return file.Kind == fileExpired }) {
			made = true
			if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil,
				[]table.Update{table.NewSetSnapshotRefUpdate("late", s1, table.TagRef, 0, 0, 0)}); err != nil {
				t.Error(err)
			}
		}
	}}
	f.completed(f.erase(byEmail))
	if !made {
		t.Fatal("no ref was made")
	}
	tbl := f.load("crm.people")
	late, ok := refs(tbl)["late"]
	if !ok {
		t.Fatal("the tag made while the request expired snapshots was lost")
	}
	if got := f.ids(tbl, late.SnapshotID); !slices.Equal(got, []string{"1"}) {
		t.Errorf("the tag reads %v", got)
	}
	f.clean(storedEmail)
}

// A file added without field IDs, which is read by column names, is erased
// from too. (iceberg-go refuses to add one whose column is named in
// another case than the table's.)
func TestAddedFileWithoutFieldIDs(t *testing.T) {
	f := newFixture(t)
	tbl := f.people("crm.people", [][]any{ann})
	p := filepath.Join(tbl.Location(), "data", "added.parquet")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT * FROM (VALUES (2::BIGINT, '%s', 'dev-x', 'erase'), (3::BIGINT, 'carl@other.com', 'dev-carl', 'keep'))"+
		" v(id, email, device_id, note)) TO '%s' (FORMAT parquet)", storedEmail, p))
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(f.ctx, []string{p}, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.completed(f.erase(byEmail))
	if got := f.ids(f.load("crm.people"), 0); !slices.Equal(got, []string{"1", "3"}) {
		t.Errorf("rows: %v", got)
	}
	f.clean(storedEmail)
}
