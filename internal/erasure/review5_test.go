package erasure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/policy"
)

// The fifth review's findings, each reproduced by a test it wrote, and
// fixed: a table the request cannot go on with holds only itself; a
// blocked request goes on once the cause is fixed, and an admin can close
// it; manifests keep their partition values; a DELETE sent again after a
// conflict is checked again first.

// swapTable takes table name out of the catalog, as DROP TABLE does, and,
// with v not 0, makes a table of format version v under its name, as
// CREATE TABLE name ... WITH ('format-version' = v) would.
func swapTable(t *testing.T, f *fixture, name string, v int) {
	t.Helper()
	f.cat.mu.Lock()
	delete(f.cat.tables, name)
	f.cat.mu.Unlock()
	if v == 0 {
		return
	}
	loc := filepath.Join(f.dir, "warehouse", strings.ReplaceAll(name, ".", "/")+"-new")
	meta, err := table.NewMetadata(peopleSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc, iceberg.Properties{"format-version": "3"})
	if err != nil {
		t.Fatal(err)
	}
	tbl := table.New(ident(name), meta, f.cat.write(t, loc, meta), localFS, f.cat)
	f.cat.mu.Lock()
	f.cat.tables[name] = tbl
	f.cat.mu.Unlock()
}

// tableOf is r's table name.
func tableOf(t *testing.T, r *Request, name string) Table {
	t.Helper()
	i := slices.IndexFunc(r.Tables, func(t Table) bool { return t.Name == name })
	if i < 0 {
		t.Fatalf("no table %s in %+v", name, r.Tables)
	}
	return r.Tables[i]
}

// settle runs request id until it completes, fails, is held or blocked,
// at most runs times: an hour apart, or, while it waits, once it is done
// waiting.
func settle(f *fixture, id string, runs int) *Request {
	f.t.Helper()
	at := time.Now()
	var r *Request
	for range runs {
		at = at.Add(time.Hour)
		if r != nil && r.Status == StatusWaiting && r.WaitUntil.After(at) {
			at = r.WaitUntil.Add(time.Minute)
		}
		now := at
		f.svc.now = func() time.Time { return now }
		_ = f.svc.Run(f.ctx, id)
		var err error
		if r, err = f.store.Get(f.ctx, id); err != nil {
			f.t.Fatal(err)
		}
		switch r.Status {
		case StatusCompleted, StatusFailed, StatusHeld, StatusBlocked:
			return r
		}
	}
	return r
}

// One table of a request is dropped by another engine once the request
// has deleted from it, and may be made again, where it was or elsewhere,
// as Lakekeeper puts each table (warehouse/<uuid>). Its files, which no
// table references any more, can hold the person's rows: the request goes
// on, deletes those under the dropped table's location once they are old
// enough not to be another commit's (see purge), erases from a table made
// again under the name, and completes, with no file holding the person's
// rows.
func TestDroppedTableFilesGo(t *testing.T) {
	for _, again := range []string{"", "where it was", "elsewhere"} {
		t.Run("made again "+again, func(t *testing.T) {
			f := newFixture(t)
			f.people("crm.alpha", [][]any{ann, subject})
			beta := f.people("crm.beta", [][]any{carl, subject})
			betaFile := filesByIDs(t, f, beta)["2,3"]
			f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
				policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
			f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
			f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
			rows := &hookRows{testRows: f.rows, f: f}
			rows.after = func(name string) {
				if name == "crm.beta" {
					swapTable(t, f, name, 0)
				}
			}
			f.svc.rows = rows
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			// Within the day, before the dropped table's file is old enough.
			got := runFor(f, r.ID, 6)
			rows.after = nil
			b := tableOf(t, got, "crm.beta")
			if got.Status != StatusWaiting || got.Step != StepPurge || !b.Gone || b.Blocked != "" {
				t.Errorf("%s at %s, %q; crm.beta %+v", got.Status, got.Step, got.Error, b)
			}
			if _, err := os.Stat(betaFile); err != nil {
				t.Errorf("the dropped crm.beta's file went before it was old enough: %v", err)
			}
			switch again {
			case "where it was":
				f.people("crm.beta", [][]any{carl})
			case "elsewhere":
				loc := filepath.Join(f.dir, "warehouse", "made-again")
				meta, err := table.NewMetadata(peopleSchema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, loc,
					iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead, "format-version": "2"})
				if err != nil {
					t.Fatal(err)
				}
				tbl := table.New(ident("crm.beta"), meta, f.cat.write(t, loc, meta), localFS, f.cat)
				f.cat.mu.Lock()
				f.cat.tables["crm.beta"] = tbl
				f.cat.mu.Unlock()
				appendRows(t, tbl, carl, subject)
			}
			f.completed(settle(f, r.ID, 10))
			if _, err := os.Stat(betaFile); !os.IsNotExist(err) {
				t.Errorf("the dropped crm.beta's file is still there: %v", err)
			}
			if again != "" && !slices.Equal(f.ids(f.load("crm.beta"), 0), []string{"3"}) {
				t.Errorf("crm.beta made again: %v", f.ids(f.load("crm.beta"), 0))
			}
			f.clean(storedEmail)
		})
	}
}

// One table of a request is replaced by a table of format version 3,
// which requests cannot rewrite yet, by another engine once the request
// has deleted from it: it is set aside, and the other table goes through
// every step, as do the replaced table's files, which no table references.
// The request is then blocked, keeps the identifiers, and takes the table
// up again every hour; an admin can close it, which records what it left
// and forgets the identifiers.
func TestReplacedByVersion3IsSetAside(t *testing.T) {
	f := newFixture(t)
	alpha := f.people("crm.alpha", [][]any{ann, subject})
	before, alphaDir := alpha.CurrentSnapshot().SnapshotID, alpha.Location()
	beta := f.people("crm.beta", [][]any{carl, subject})
	oldBeta, betaFile := beta.Location(), filesByIDs(t, f, beta)["2,3"]
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f}
	rows.after = func(name string) {
		if name == "crm.beta" {
			swapTable(t, f, name, 3)
		}
	}
	f.svc.rows = rows
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 12)
	rows.after = nil
	f.blocked(got, StepRewrite, "set aside crm.beta", *alerts)
	b := tableOf(t, got, "crm.beta")
	if b.BlockedAt != StepRewrite || !strings.Contains(b.Blocked, "format version 3") {
		t.Errorf("crm.beta: %+v", b)
	}
	a := tableOf(t, got, "crm.alpha")
	for _, step := range []string{StepRewrite, StepExpire, StepPurge, StepSharing, StepVerify} {
		if a.Steps[step].IsZero() || a.Blocked != "" {
			t.Errorf("crm.alpha did not go through its %s step: %+v", step, a)
		}
	}
	if tbl := f.load("crm.alpha"); tbl.SnapshotByID(before) != nil || !slices.Equal(f.ids(tbl, 0), []string{"1"}) {
		t.Errorf("crm.alpha still has its snapshot from before the DELETE, or rows %v", f.ids(tbl, 0))
	}
	if found := search(t, f.eng, alphaDir, []string{storedEmail}); len(found) > 0 {
		t.Errorf("left in crm.alpha's files: %v", found)
	}
	if found := search(t, f.eng, oldBeta, []string{storedEmail}); len(found) > 0 {
		t.Errorf("left in the replaced crm.beta's files: %v", found)
	}
	if _, err := os.Stat(betaFile); !os.IsNotExist(err) {
		t.Errorf("the replaced crm.beta's file is still there: %v", err)
	}
	if got = f.runTimes(r.ID, 3); got.Status != StatusBlocked || got.Sealed == "" {
		t.Errorf("an hour later: %s, %q", got.Status, got.Error)
	}
	if _, err := f.svc.Close(f.ctx, r.ID, "pat"); err != nil {
		t.Fatal(err)
	}
	closed, _ := f.store.Get(f.ctx, r.ID)
	if closed.Status != StatusClosed || closed.ClosedBy != "pat" || closed.Sealed != "" || closed.Salt != "" ||
		!strings.Contains(closed.Error, "closed by pat") {
		t.Errorf("closed: %+v", closed)
	}
	if beta, alpha := tableOf(t, closed, "crm.beta"), tableOf(t, closed, "crm.alpha"); beta.Left == "" || alpha.Left != "" {
		t.Errorf("left: crm.beta %q, crm.alpha %q", beta.Left, alpha.Left)
	}
	if _, err := f.svc.Close(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) {
		t.Errorf("closed twice: %v", err)
	}
	if err := f.svc.Run(f.ctx, r.ID); err != nil {
		t.Errorf("a run of a closed request: %v", err)
	}
	if erasing, err := f.svc.Erasing(f.ctx, "crm", "beta"); err != nil || erasing {
		t.Errorf("a closed request erases from crm.beta: %v, %v", erasing, err)
	}
}

// A table set aside that exists but cannot be read, here one another
// engine upgraded to version 3 and whose manifest list is damaged, may
// reference any file under its own location: those stay while it is set
// aside, and the other table's go, so that no table stops the erasure of
// another's rows.
func TestUnreadableTableSetAsideKeepsItsFiles(t *testing.T) {
	f := newFixture(t)
	alphaFile := filesByIDs(t, f, f.people("crm.alpha", [][]any{ann, subject}))["1,2"]
	betaFile := filesByIDs(t, f, f.people("crm.beta", [][]any{carl, subject}))["2,3"]
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f}
	rows.after = func(name string) {
		if name == "crm.beta" {
			if _, _, err := f.cat.CommitTable(f.ctx, ident(name), nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
				t.Fatal(err)
			}
			for _, snap := range f.load(name).Metadata().Snapshots() {
				if err := os.WriteFile(snap.ManifestList, []byte("damaged"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	f.svc.rows = rows
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 12)
	f.blocked(got, got.Step, "crm.beta", *alerts)
	if beta := tableOf(t, got, "crm.beta"); !strings.Contains(beta.Blocked, "format version 3") {
		t.Errorf("crm.beta: %+v", beta)
	}
	if _, err := os.Stat(alphaFile); !os.IsNotExist(err) {
		t.Errorf("crm.alpha's file stayed while a table that cannot reference it could not be read: %v", err)
	}
	if a := tableOf(t, got, "crm.alpha"); a.Steps[StepVerify].IsZero() {
		t.Errorf("crm.alpha: %+v", a)
	}
	if _, err := os.Stat(betaFile); err != nil {
		t.Errorf("crm.beta's file went while it could not be read: %v", err)
	}
	// Closed so, it records that it did not finish with crm.beta only.
	closed, err := f.svc.Close(f.ctx, r.ID, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if beta, alpha := tableOf(t, closed, "crm.beta"), tableOf(t, closed, "crm.alpha"); beta.Left == "" || alpha.Left != "" {
		t.Errorf("left: crm.beta %q, crm.alpha %q", beta.Left, alpha.Left)
	}
}

// A table dropped once the request has rewritten its files is not set
// aside: the files the request recorded for it, which held the person's
// rows and which no table references any more, go, and the request
// completes. Igloo's warehouse uses Lakekeeper's hard deletion, in which a
// dropped table does not come back.
func TestFilesOfDroppedTableGo(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	betaFile := filesByIDs(t, f, f.people("crm.beta", [][]any{carl, subject}))["2,3"]
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	dropped := false
	f.cat.after = func(id table.Identifier, meta table.Metadata) {
		if snap := meta.CurrentSnapshot(); !dropped && tableName(id) == "crm.beta" && snap != nil && snap.Summary != nil &&
			snap.Summary.Operation == table.OpReplace && snap.Summary.Properties["igloo.operation"] == "erase" {
			dropped = true
			swapTable(t, f, "crm.beta", 0)
		}
	}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 12)
	f.completed(got)
	if beta := tableOf(t, got, "crm.beta"); !dropped || beta.FilesRewritten == 0 || !beta.Gone || beta.Blocked != "" {
		t.Fatalf("crm.beta: dropped %v, %+v", dropped, beta)
	}
	if _, err := os.Stat(betaFile); !os.IsNotExist(err) {
		t.Errorf("crm.beta's file is still there: %v", err)
	}
	f.clean(storedEmail)
}

// A request is closed with a table set aside (upgraded to version 3 once
// the request rewrote it) whose old file, which held the person's row,
// the request recorded and could not delete while the table might
// reference it. If the table is still there, the closed request lists the
// file as left; if it was dropped since, the file goes. Either way the
// request forgets nothing that may hold the person's rows without
// deleting it or saying so.
func TestCloseSettlesFilesOfTableSetAside(t *testing.T) {
	for _, dropped := range []bool{false, true} {
		t.Run(fmt.Sprintf("dropped %v", dropped), func(t *testing.T) {
			f := newFixture(t)
			f.people("crm.alpha", [][]any{ann, subject})
			betaFile := filesByIDs(t, f, f.people("crm.beta", [][]any{carl, subject}))["2,3"]
			f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
				policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
			upgraded := false
			f.cat.after = func(id table.Identifier, meta table.Metadata) {
				if snap := meta.CurrentSnapshot(); !upgraded && tableName(id) == "crm.beta" && snap != nil && snap.Summary != nil &&
					snap.Summary.Properties["igloo.operation"] == "erase" {
					upgraded = true
					if _, _, err := f.cat.CommitTable(f.ctx, id, nil, []table.Update{table.NewUpgradeFormatVersionUpdate(3)}); err != nil {
						t.Error(err)
					}
				}
			}
			alerts := f.alerts()
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			got := settle(f, r.ID, 12)
			f.blocked(got, got.Step, "crm.beta", *alerts)
			if _, err := os.Stat(betaFile); err != nil {
				t.Fatalf("crm.beta's file went while it was set aside: %v", err)
			}
			if dropped {
				swapTable(t, f, "crm.beta", 0)
			}
			closed, err := f.svc.Close(f.ctx, r.ID, "ada")
			if err != nil {
				t.Fatal(err)
			}
			beta := tableOf(t, closed, "crm.beta")
			_, statErr := os.Stat(betaFile)
			switch {
			case dropped && !os.IsNotExist(statErr):
				t.Errorf("the dropped crm.beta's file is still there: %v", statErr)
			case dropped && strings.Contains(beta.Left, base(betaFile)):
				t.Errorf("crm.beta: %q", beta.Left)
			case !dropped && (statErr != nil || !strings.Contains(beta.Left, base(betaFile))):
				t.Errorf("crm.beta's file (%v) is not listed as left: %q", statErr, beta.Left)
			}
			if strings.Contains(strings.ToLower(beta.Left), "subject") {
				t.Errorf("an identifier in what the request left: %q", beta.Left)
			}
		})
	}
}

// Before it forgets the identifiers, closing a request redacts them from
// the history, what was recorded since its history step included, deletes
// the sharing copies that hold the person's rows and has every replica
// empty its read cache of the files it deleted; if one of those cannot be
// done yet, as when a replica has yet to empty its cache, the request is
// not closed, and the error says why.
func TestCloseFinishesFirst(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	f.people("crm.beta", [][]any{carl, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.svc.rows = betaFails{f.rows}
	queries := audit.NewMemoryStore(100, 100)
	f.svc.opts.History = []History{queries}
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, maxFailures+5)
	f.blocked(got, StepDelete, "set aside crm.beta", *alerts)
	if got.DeletedThrough.IsZero() {
		t.Fatal("the request deleted no file")
	}
	// Recorded since the request's history step.
	if err := queries.WriteQueries(f.ctx, []audit.Query{{ID: "q1", User: "ana", Start: time.Now(), Outcome: audit.OutcomeOK, StatementType: "SELECT",
		Text: "SELECT * FROM crm.beta WHERE lower(email) = 'subject@example.com'"}}); err != nil {
		t.Fatal(err)
	}
	// A replica that has yet to empty its read cache of what the request
	// deleted: the request is not closed.
	if err := f.store.Seen(f.ctx, "replica-2-cache", "replica-2", f.svc.now()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Close(f.ctx, r.ID, "ada"); !errors.Is(err, ErrNotYet) || !strings.Contains(err.Error(), "read caches") {
		t.Fatalf("closed while a replica's cache may hold what it deleted: %v", err)
	}
	if still, _ := f.store.Get(f.ctx, r.ID); still.Status != StatusBlocked || still.Sealed == "" {
		t.Fatalf("after a refused close: %s, sealed %v", still.Status, still.Sealed != "")
	}
	if err := f.store.Emptied(f.ctx, "replica-2-cache", f.svc.now().Add(time.Hour), ""); err != nil {
		t.Fatal(err)
	}
	closed, err := f.svc.Close(f.ctx, r.ID, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if closed.Status != StatusClosed {
		t.Errorf("closed: %s", closed.Status)
	}
	// Redacted by the first try already, which got that far.
	recorded, err := queries.Queries(f.ctx, audit.Viewer{All: true}, audit.Page{})
	if err != nil || len(recorded) != 1 {
		t.Fatalf("%d records, %v", len(recorded), err)
	}
	for _, q := range recorded {
		if strings.Contains(strings.ToLower(q.Text), "subject@example.com") {
			t.Errorf("the query history still has %q", q.Text)
		}
	}
	if b, a := tableOf(t, closed, "crm.beta"), tableOf(t, closed, "crm.alpha"); !strings.Contains(b.Left, "Binder Error") || a.Left != "" {
		t.Errorf("left: crm.beta %q, crm.alpha %q", b.Left, a.Left)
	}
}

// Only a blocked request can be closed: one that runs on is not stuck, and
// one that has not begun deleting is canceled instead.
func TestCloseOnlyBlocked(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Close(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) {
		t.Errorf("a queued request was closed: %v", err)
	}
	f.completed(f.run(r.ID))
	if _, err := f.svc.Close(f.ctx, r.ID, "pat"); !errors.Is(err, ErrInvalid) {
		t.Errorf("a completed request was closed: %v", err)
	}
}

// One table's DELETE keeps failing: once runs have failed as often as a
// request may fail, the table is set aside, and the other goes on.
func TestFailingTableIsSetAside(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	f.people("crm.beta", [][]any{carl, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.svc.rows = betaFails{f.rows}
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, maxFailures+5)
	f.blocked(got, StepDelete, "set aside crm.beta", *alerts)
	if beta := tableOf(t, got, "crm.beta"); beta.BlockedAt != StepDelete || !strings.Contains(beta.Blocked, "Binder Error") {
		t.Errorf("crm.beta: %+v", beta)
	}
	if a := tableOf(t, got, "crm.alpha"); a.Steps[StepVerify].IsZero() || !slices.Equal(f.ids(f.load("crm.alpha"), 0), []string{"1"}) {
		t.Errorf("crm.alpha: %+v", a)
	}
	// Taken up again an hour later, it gets one try, and is set aside
	// again at once; the request is blocked again on the next run.
	attempts := got.Attempts
	if got = runFor(f, r.ID, 3); got.Status != StatusBlocked || got.Attempts != attempts+2 {
		t.Errorf("taken up again: %s after %d runs, %q", got.Status, got.Attempts-attempts, got.Error)
	}
}

// betaFails fails every DELETE from crm.beta.
type betaFails struct{ testRows }

func (b betaFails) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	if tableName(tbl.Identifier()) == "crm.beta" {
		return 0, errors.New("Binder Error: cannot delete")
	}
	return b.testRows.Delete(ctx, tbl, cond)
}

// Verification found the person's rows again and again, as a feed wrote
// them back after each pass, and blocked the request. Once the feed stops,
// the request goes on from the step that removes them, and completes.
func TestBlockedAtVerifyRecovers(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("email"), storedEmail)
	feeding, expiring := true, false
	f.cat.before = func(_ table.Identifier, updates []table.Update) {
		expiring = slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == "remove-snapshots" })
	}
	f.cat.after = func(id table.Identifier, _ table.Metadata) {
		if feeding && expiring {
			expiring = false
			appendRows(t, f.load(tableName(id)), []any{int64(9), storedEmail, "dev-x", "again"})
		}
	}
	alerts := f.alerts()
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, 40)
	if got.Status != StatusBlocked || !strings.Contains(got.Error, "verification still finds") || got.Step != StepDelete || len(*alerts) == 0 {
		t.Fatalf("while fed: %s at %s, %q", got.Status, got.Step, got.Error)
	}
	feeding = false
	f.completed(f.run(r.ID))
	if rows := f.ids(f.load("crm.people"), 0); !slices.Equal(rows, []string{"1"}) {
		t.Errorf("rows: %v", rows)
	}
	f.clean(storedEmail)
}

// manifestPartitions checks that every file of spec 0 that snapshot id of
// table name lists, live or not, keeps its partition value.
func manifestPartitions(t *testing.T, f *fixture, name string, id int64) {
	t.Helper()
	tbl := f.load(name)
	fsys, err := tbl.FS(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := tbl.SnapshotByID(id).Manifests(fsys)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		for e, err := range m.Entries(fsys, false) {
			if err != nil {
				t.Fatal(err)
			}
			if df := e.DataFile(); df.SpecID() == 0 && df.Partition()[1000] == nil {
				t.Errorf("%s lists %s, of spec 0, without its partition value", base(m.FilePath()), base(df.FilePath()))
			}
		}
	}
}

// A table partitioned by note (spec 0), then no longer (spec 1), and note
// dropped since: the rewrite writes files with spec 1, but the manifests
// that list the files of spec 0 it keeps, which it writes again, keep
// their partition values: iceberg-go writes them with a schema that has
// note. The same for a tag whose snapshot's schema lacks note.
func TestManifestsKeepPartitionValues(t *testing.T) {
	t.Run("main", func(t *testing.T) {
		f := newFixture(t)
		partitionedPeople(t, f, "crm.people", ann, subject, carl)
		tx := f.load("crm.people").NewTransaction()
		if err := tx.UpdateSpec(false).RemoveField("note").Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		tx = f.load("crm.people").NewTransaction()
		if err := tx.UpdateSchema(true, false).DeleteColumn([]string{"note"}).Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
		f.completed(f.erase(byEmail))
		tbl := f.load("crm.people")
		if rows := f.ids(tbl, 0); !slices.Equal(rows, []string{"1", "3"}) {
			t.Errorf("rows: %v", rows)
		}
		manifestPartitions(t, f, "crm.people", tbl.CurrentSnapshot().SnapshotID)
		f.clean(storedEmail)
	})
	t.Run("tag", func(t *testing.T) {
		f := newFixture(t)
		partitionedPeople(t, f, "crm.people", ann, subject)
		tx := f.load("crm.people").NewTransaction()
		if err := tx.UpdateSpec(false).RemoveField("note").Commit(); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		unwritableTag(t, f, "crm.people", "legacy")
		f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
		f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
		f.completed(f.erase(byEmail))
		tbl := f.load("crm.people")
		tag := refs(tbl)["legacy"].SnapshotID
		if rows := f.ids(tbl, tag); !slices.Equal(rows, []string{"1"}) {
			t.Errorf("tag rows: %v", rows)
		}
		manifestPartitions(t, f, "crm.people", tag)
		f.clean(storedEmail)
	})
}

// Files of a spec whose column none of the table's schemas has any more:
// their manifests could not be written again with their partition values,
// and the request is refused.
func TestSpecWithoutColumnsIsRefused(t *testing.T) {
	f := newFixture(t)
	partitionedPeople(t, f, "crm.people", ann, subject)
	tx := f.load("crm.people").NewTransaction()
	if err := tx.UpdateSpec(false).RemoveField("note").Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	tx = f.load("crm.people").NewTransaction()
	if err := tx.UpdateSchema(true, false).DeleteColumn([]string{"note"}).Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.people"), nil, []table.Update{table.NewRemoveSchemasUpdate([]int{0})}); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	if _, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "partition spec 0") {
		t.Errorf("submit: %v", err)
	}
}

// conflictRows stands in for DuckDB's DELETE of crm.beta losing to another
// engine's commit, which upgraded the table to version 3 and deleted from
// it with a deletion vector: the DELETE is not sent again on a table the
// request did not check again.
type conflictRows struct {
	*hookRows
	upgrade bool
	lost    bool
}

func (c *conflictRows) Delete(ctx context.Context, tbl *table.Table, cond string) (int64, error) {
	if tableName(tbl.Identifier()) != "crm.beta" || c.lost {
		return c.hookRows.Delete(ctx, tbl, cond)
	}
	c.lost = true
	c.mu.Lock()
	c.deletes = append(c.deletes, "crm.beta lost@v2")
	c.mu.Unlock()
	if c.upgrade {
		upgradeWithDV(c.f.t, c.f, "crm.beta")
	}
	return 0, &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "TransactionContext Error: Failed to commit: CatalogCommitConflicts"}
}

// A DELETE that lost to another writer's commit is sent again on the
// table loaded and checked again: after a commit that upgraded it, never.
func TestConflictingDeleteIsCheckedAgain(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		f := newFixture(t)
		f.people("crm.alpha", [][]any{ann, subject})
		f.people("crm.beta", [][]any{carl, subject})
		f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
			policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
		f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
		f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
		rows := &conflictRows{hookRows: &hookRows{testRows: f.rows, f: f}, upgrade: upgrade}
		f.svc.rows = rows
		alerts := f.alerts()
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		got := runFor(f, r.ID, 3)
		if rows.ranOn("crm.beta") {
			t.Errorf("upgrade %v: a DELETE ran on crm.beta at version 3: %v", upgrade, rows.deletes)
		}
		if !upgrade {
			f.completed(got)
			if n := slices.Index(rows.deletes, "crm.beta@v2"); n < 0 {
				t.Errorf("the DELETE was not sent again: %v", rows.deletes)
			}
			continue
		}
		f.blocked(got, StepDelete, "format version 3", *alerts)
		if beta := tableOf(t, got, "crm.beta"); beta.BlockedAt != StepDelete {
			t.Errorf("crm.beta: %+v", beta)
		}
	}
}
