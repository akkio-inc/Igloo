package erasure

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/policy"
)

// The sixth review's findings that are the erasure service's own: no
// writer's table can stop or fail a request, a run's failure counts
// against the table it failed with, and a request closed records what the
// last verification found.

// dangle adds to table name a snapshot no branch or tag has, whose
// manifest list does not exist, as a writer could commit through a
// catalog that does not check (Igloo's Iceberg REST endpoint does).
func dangle(t *testing.T, f *fixture, name string, id int64) {
	t.Helper()
	tbl := f.load(name)
	parent := tbl.CurrentSnapshot().SnapshotID
	schemaID := 0
	snap := &table.Snapshot{SnapshotID: id, ParentSnapshotID: &parent, SequenceNumber: tbl.Metadata().LastSequenceNumber() + 1,
		TimestampMs: time.Now().UnixMilli(), ManifestList: tbl.Location() + "/metadata/snap-missing.avro",
		Summary: &table.Summary{Operation: table.OpAppend}, SchemaID: &schemaID}
	if _, _, err := f.cat.CommitTable(f.ctx, ident(name), nil, []table.Update{table.NewAddSnapshotUpdate(snap)}); err != nil {
		t.Fatal(err)
	}
}

// A snapshot whose manifest list does not exist references nothing anyone
// can read: it neither stops a request from being made nor one that has
// begun deleting, whether it is in a table the request erases from or in
// another.
func TestDanglingSnapshotsStopNothing(t *testing.T) {
	f := newFixture(t)
	alphaFile := filesByIDs(t, f, f.people("crm.alpha", [][]any{ann, subject}))["1,2"]
	f.people("crm.beta", [][]any{carl, subject})
	f.cat.create(t, f.dir, "sandbox.junk", iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64}))
	appendRows(t, f.load("sandbox.junk"), []any{int64(1)})
	dangle(t, f, "sandbox.junk", 777000111)
	dangle(t, f, "crm.beta", 777000222)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f}
	rows.after = func(name string) {
		if name == "crm.alpha" {
			dangle(t, f, name, 777000333)
		}
	}
	f.svc.rows = rows
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	f.completed(settle(f, r.ID, 12))
	if _, err := os.Stat(alphaFile); !os.IsNotExist(err) {
		t.Errorf("crm.alpha's old file is still there: %v", err)
	}
	f.clean(storedEmail)
}

// A table that cannot be read, here one whose manifest list is damaged,
// may reference any file under its own location, and those it says it
// shares: those are kept, and the request deletes the rest and completes,
// so that no one's table stops another's erasure.
func TestUnreadableOtherTableKeepsOnlyItsFiles(t *testing.T) {
	f := newFixture(t)
	peopleFile := filesByIDs(t, f, f.people("crm.people", [][]any{ann, subject}))["1,2"]
	junk := f.people("sandbox.junk", [][]any{carl})
	junkFile := filesByIDs(t, f, junk)["3"]
	if err := os.WriteFile(junk.CurrentSnapshot().ManifestList, []byte("damaged"), 0o644); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 12)
	f.completed(got)
	if len(got.Tables) != 1 {
		t.Errorf("tables: %+v", got.Tables)
	}
	if _, err := os.Stat(peopleFile); !os.IsNotExist(err) {
		t.Errorf("crm.people's old file is still there: %v", err)
	}
	if _, err := os.Stat(junkFile); err != nil {
		t.Errorf("the unreadable table's file went: %v", err)
	}
}

// A clone that cannot be read any more once the request has deleted from
// it may reference any file under the locations it says it reads, its
// source's: it is set aside, and the request keeps its source's files
// rather than break it.
func TestUnreadableCloneKeepsItsSourcesFiles(t *testing.T) {
	f := newFixture(t)
	people := f.people("crm.people", [][]any{ann, subject})
	peopleFile := filesByIDs(t, f, people)["1,2"]
	f.people("crm.copy", [][]any{carl, subject})
	if _, _, err := f.cat.CommitTable(f.ctx, ident("crm.copy"), nil, []table.Update{table.NewSetPropertiesUpdate(
		iceberg.Properties{clone.SharesProperty: `["` + people.Location() + `"]`})}); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.copy"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	rows := &hookRows{testRows: f.rows, f: f}
	rows.after = func(name string) {
		if name == "crm.copy" {
			for _, snap := range f.load(name).Metadata().Snapshots() {
				if err := os.WriteFile(snap.ManifestList, []byte("damaged"), 0o644); err != nil {
					t.Error(err)
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
	got := runFor(f, r.ID, 3*maxFailures)
	f.blocked(got, got.Step, "crm.copy", *alerts)
	if c := tableOf(t, got, "crm.copy"); c.Blocked == "" || c.Reason == reasonTagged {
		t.Errorf("crm.copy: %+v", c)
	}
	if _, err := os.Stat(peopleFile); err != nil {
		t.Errorf("crm.people's file, which the unreadable clone may read, went: %v", err)
	}
	if !slices.Equal(f.ids(f.load("crm.people"), 0), []string{"1"}) {
		t.Errorf("crm.people: %v", f.ids(f.load("crm.people"), 0))
	}
}

// A table of the request's that cannot be read any more once the request
// has deleted from it, here one whose manifest lists are damaged once the
// request rewrote it, or once it expired its snapshots, fails the runs it
// is in; after as many as a request may fail, it is set aside on its own,
// and the other table goes through every step.
func TestUnreadableTableIsSetAside(t *testing.T) {
	for _, after := range []string{"rewrite", "expire"} {
		t.Run(after, func(t *testing.T) {
			f := newFixture(t)
			alphaFile := filesByIDs(t, f, f.people("crm.alpha", [][]any{ann, subject}))["1,2"]
			f.people("crm.beta", [][]any{carl, subject})
			f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
				policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
			f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
			f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
			damaged, expiring := false, false
			f.cat.before = func(id table.Identifier, updates []table.Update) {
				expiring = tableName(id) == "crm.beta" && slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == "remove-snapshots" })
			}
			f.cat.after = func(id table.Identifier, meta table.Metadata) {
				snap := meta.CurrentSnapshot()
				rewritten := tableName(id) == "crm.beta" && snap != nil && snap.Summary != nil && snap.Summary.Properties["igloo.operation"] == "erase"
				if damaged || after == "rewrite" && !rewritten || after == "expire" && !expiring {
					return
				}
				damaged = true
				for _, s := range meta.Snapshots() {
					if err := os.WriteFile(s.ManifestList, []byte("damaged"), 0o644); err != nil {
						t.Error(err)
					}
				}
			}
			alerts := f.alerts()
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			got := runFor(f, r.ID, 3*maxFailures)
			f.blocked(got, got.Step, "crm.beta", *alerts)
			if b := tableOf(t, got, "crm.beta"); !damaged || b.Blocked == "" {
				t.Errorf("crm.beta: damaged %v, %+v", damaged, b)
			}
			if a := tableOf(t, got, "crm.alpha"); a.Steps[StepVerify].IsZero() || a.Blocked != "" {
				t.Errorf("crm.alpha: %+v", a)
			}
			if _, err := os.Stat(alphaFile); !os.IsNotExist(err) {
				t.Errorf("crm.alpha's old file is still there: %v", err)
			}
		})
	}
}

// After the request takes up the tables it set aside, a single passing
// error of a table that was never set aside counts against that table
// alone, which goes on: only a table that keeps failing is set aside.
func TestTakeUpCountsFailuresPerTable(t *testing.T) {
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
	f.blocked(runFor(f, r.ID, maxFailures+5), StepDelete, "set aside crm.beta", *alerts)
	// Taken up again, crm.alpha fails once, as on a connection reset.
	rows := &hookRows{testRows: f.rows, f: f, fail: map[string]bool{"crm.alpha": true}}
	f.svc.rows = rows
	got := f.runTimes(r.ID, 1)
	a := tableOf(t, got, "crm.alpha")
	if a.Blocked != "" || a.Failures != 1 {
		t.Errorf("one passing error set crm.alpha aside: %+v", a)
	}
	// It goes on, and the request is blocked on crm.beta alone again.
	f.svc.rows = betaFails{f.rows}
	got = runFor(f, r.ID, maxFailures+5)
	if a := tableOf(t, got, "crm.alpha"); got.Status != StatusBlocked || a.Blocked != "" || a.Steps[StepVerify].IsZero() {
		t.Errorf("%s; crm.alpha %+v", got.Status, a)
	}
}

// A request takes up a table it set aside, and its verification finds the
// person's rows in the other table again and again, as a feed writes them
// back, until the request is blocked. Closed then, it records that it did
// not finish with that table: what its verification found counts, not
// that the table was verified on an earlier pass.
func TestCloseAfterVerificationFoundMore(t *testing.T) {
	f := newFixture(t)
	f.people("crm.alpha", [][]any{ann, subject})
	f.people("crm.beta", [][]any{carl, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "alpha", Column: "email", Value: "email"},
		policy.ColumnTag{Schema: "crm", Table: "beta", Column: "email", Value: "email"})
	f.rows["crm.alpha"] = iceberg.EqualTo(iceberg.Reference("email"), storedEmail)
	f.rows["crm.beta"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.svc.rows = betaFails{f.rows}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, maxFailures+5)
	if a := tableOf(t, got, "crm.alpha"); got.Status != StatusBlocked || a.Steps[StepVerify].IsZero() {
		t.Fatalf("first pass: %s; crm.alpha %+v", got.Status, a)
	}
	feeding, expiring := true, false
	f.cat.before = func(id table.Identifier, updates []table.Update) {
		expiring = tableName(id) == "crm.alpha" && slices.ContainsFunc(updates, func(u table.Update) bool { return u.Action() == "remove-snapshots" })
	}
	f.cat.after = func(id table.Identifier, _ table.Metadata) {
		if feeding && expiring {
			expiring = false
			appendRows(t, f.load("crm.alpha"), []any{int64(9), storedEmail, "dev-x", "again"})
		}
	}
	appendRows(t, f.load("crm.alpha"), []any{int64(9), storedEmail, "dev-x", "again"})
	got = f.runTimes(r.ID, 6)
	feeding = false
	if got.Status != StatusBlocked || !strings.Contains(got.Error, "verification still finds") {
		t.Fatalf("%s at %s: %q", got.Status, got.Step, got.Error)
	}
	if a := tableOf(t, got, "crm.alpha"); !a.Steps[StepVerify].IsZero() {
		t.Errorf("crm.alpha counts as verified: %v", a.Steps[StepVerify])
	}
	closed, err := f.svc.Close(f.ctx, r.ID, "ada")
	if err != nil {
		t.Fatal(err)
	}
	if a := tableOf(t, closed, "crm.alpha"); !strings.Contains(a.Left, "verification still finds") {
		t.Errorf("crm.alpha: %q", a.Left)
	}
}

// Taking up the tables set aside, and verification finding more, have
// every table go through the steps from there again: none keeps the time
// it went through them before.
func TestRedoForgetsLaterSteps(t *testing.T) {
	at := time.Now()
	all := map[string]time.Time{}
	for _, s := range steps {
		all[s] = at
	}
	r := &Request{Step: StepVerify, Tables: []Table{
		{Name: "a", Steps: cloneSteps(all)},
		{Name: "b", Steps: cloneSteps(all), Blocked: "why", BlockedAt: StepRewrite},
	}}
	r.takeUp()
	if r.Step != StepRewrite {
		t.Errorf("step %s", r.Step)
	}
	for _, tb := range r.Tables {
		for i, s := range steps {
			_, ok := tb.Steps[s]
			if want := i < slices.Index(steps, StepRewrite); ok != want {
				t.Errorf("%s: step %s kept %v", tb.Name, s, ok)
			}
		}
		if tb.Blocked != "" {
			t.Errorf("%s still set aside", tb.Name)
		}
	}
	if r.Tables[1].Failures != maxFailures-1 || r.Tables[0].Failures != 0 {
		t.Errorf("failures: %d, %d", r.Tables[0].Failures, r.Tables[1].Failures)
	}
}

func cloneSteps(m map[string]time.Time) map[string]time.Time {
	out := map[string]time.Time{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
