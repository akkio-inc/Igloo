package erasure

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Every request sweeps the warehouse (see unowned): the files under no
// table's or view's location, as those of tables dropped or replaced,
// before the request or while it ran, however it was, can hold the
// person's rows where no table, and so no request, reaches them otherwise.

// plant copies file from to rel under the warehouse, written age ago.
func plant(t *testing.T, f *fixture, rel, from string, age time.Duration) string {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(f.warehouse(), rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	backdate(t, p, age)
	return p
}

// backdate dates the files under p age ago.
func backdate(t *testing.T, p string, age time.Duration) {
	t.Helper()
	at := time.Now().Add(-age)
	err := filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return os.Chtimes(path, at, at)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool { _, err := os.Stat(p); return err == nil }

// The files of a table dropped long ago go, whatever they hold, without
// being read; a newer one that holds the person's rows makes the request
// wait until it is old enough not to be a table's being made, and then
// goes. A file another table references stays, as do the metadata of a
// view, whose location owns it, and Igloo's own files.
func TestSweepDeletesWhatNoTableOwns(t *testing.T) {
	f := newFixture(t)
	people := f.people("crm.people", [][]any{ann, subject})
	personFile := filesByIDs(t, f, people)["1,2"]
	carlFile := filesByIDs(t, f, f.people("sandbox.carl", [][]any{carl}))["3"]
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.rows["crm.people"] = iceberg.EqualTo(iceberg.Reference("id"), int64(2))

	// A table dropped (by an engine going around Igloo, or before this
	// version) whose data file another table's add_files took in.
	gone := f.people("sandbox.gone", [][]any{carl, subject})
	goneFile := filesByIDs(t, f, gone)["2,3"]
	tf, err := readFiles(f.ctx, gone)
	if err != nil {
		t.Fatal(err)
	}
	keep := f.people("sandbox.keep")
	tx := keep.NewTransaction()
	if err := tx.AddDataFiles(f.ctx, slices.Collect(maps.Values(tf.live(true))), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	swapTable(t, f, "sandbox.gone", 0)
	backdate(t, gone.Location(), 10*24*time.Hour)

	day := 24 * time.Hour
	oldCopy := plant(t, f, "01aa-dropped/data/old.parquet", personFile, 10*day)
	oldOther := plant(t, f, "01aa-dropped/data/other.parquet", carlFile, 10*day)
	youngCopy := plant(t, f, "01bb-replaced/data/young.parquet", personFile, time.Hour)
	ours := plant(t, f, ".igloo/other/keep.parquet", personFile, 10*day)
	view := plant(t, f, "01cc-view/metadata/00000.metadata.json", carlFile, 10*day)
	f.svc.opts.Locations = func(ctx context.Context) ([]string, error) {
		locs, err := f.svc.tableLocations(ctx)
		return append(locs, filepath.Join(f.warehouse(), "01cc-view")), err
	}

	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, 6)
	if got.Status != StatusWaiting || got.Step != StepPurge {
		t.Errorf("within the day: %s at %s, %q", got.Status, got.Step, got.Error)
	}
	if exists(oldCopy) || exists(oldOther) || !exists(youngCopy) {
		t.Errorf("within the day: old copy %v, old other %v, young copy %v", exists(oldCopy), exists(oldOther), exists(youngCopy))
	}
	f.completed(settle(f, r.ID, 10))
	for p, want := range map[string]bool{youngCopy: false, ours: true, view: true, goneFile: true} {
		if exists(p) != want {
			t.Errorf("%s exists %v, want %v", p, exists(p), want)
		}
	}
	if found := search(t, f.eng, gone.Location(), []string{"metadata"}); len(found) > 0 {
		t.Logf("left of the dropped table: %v", found)
	}
	if !slices.Equal(f.ids(f.load("sandbox.keep"), 0), []string{"2", "3"}) {
		t.Errorf("sandbox.keep: %v", f.ids(f.load("sandbox.keep"), 0))
	}
	// Only in sandbox.keep's file, which no tag names, and in Igloo's own.
	for _, where := range search(t, f.eng, f.warehouse(), []string{storedEmail}) {
		if !strings.HasPrefix(where, goneFile) && !strings.HasPrefix(where, ours) {
			t.Errorf("the person is left in %s", where)
		}
	}
}

// plantingHistory plants, the first time it redacts, a file that holds
// the person's row under no table's location: written after the
// request's purge step.
type plantingHistory struct {
	plant   func()
	planted bool
}

func (h *plantingHistory) Redact(context.Context, audit.Redactor) (int64, error) {
	if !h.planted {
		h.planted = true
		h.plant()
	}
	return 0, nil
}

// A file under no table's location that holds the person's rows and
// appears after the request swept the warehouse is found when it
// verifies: the request goes back, waits until the file is old enough,
// and completes once it is gone.
func TestSweepLooksAgainBeforeCompleting(t *testing.T) {
	f := newFixture(t)
	people := f.people("crm.people", [][]any{ann, subject})
	// A copy, kept outside the warehouse, as the request deletes the file.
	personFile := filepath.Join(t.TempDir(), "person.parquet")
	b, err := os.ReadFile(filesByIDs(t, f, people)["1,2"])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(personFile, b, 0o644); err != nil {
		t.Fatal(err)
	}
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	var late string
	f.svc.opts.History = []History{&plantingHistory{plant: func() {
		late = plant(t, f, "01ee-late/data/late.parquet", personFile, 0)
	}}}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 12)
	f.completed(got)
	if late == "" || exists(late) {
		t.Errorf("the late file (%q) is still there", late)
	}
	f.clean(storedEmail)
}

// respelled lists the warehouse with paths spelled otherwise than the
// tables' metadata spells them.
type respelled struct{ iceio.LocalFS }

func (respelled) WalkDir(root string, fn fs.WalkDirFunc) error {
	return iceio.LocalFS{}.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		return fn("s3://elsewhere"+p, d, err)
	})
}

// A listing of the warehouse that spells paths otherwise than the tables'
// metadata would make every table's files look unowned: the request
// deletes nothing, and does not complete.
func TestSweepRefusesAListingSpelledOtherwise(t *testing.T) {
	f := newFixture(t)
	people := f.people("crm.people", [][]any{ann, subject})
	others := f.people("sandbox.other", [][]any{carl})
	otherFile := filesByIDs(t, f, others)["3"]
	backdate(t, others.Location(), 10*24*time.Hour)
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	f.svc.opts.Storage = func(context.Context, string) (iceio.IO, error) { return respelled{}, nil }
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := runFor(f, r.ID, 3)
	if got.Status == StatusCompleted || !strings.Contains(got.Error, "listing of the warehouse") {
		t.Errorf("%s: %q", got.Status, got.Error)
	}
	if !exists(otherFile) || len(f.ids(f.load("sandbox.other"), 0)) != 1 {
		t.Errorf("another table's file went")
	}
	_ = people
}

// Which of the warehouse's files are a table's cannot be told while a
// table or view of the catalog cannot be loaded: the request deletes
// nothing under no table's location, and does not complete, until it can.
func TestSweepNeedsEveryLocation(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	failing := true
	f.svc.opts.Locations = func(ctx context.Context) ([]string, error) {
		if failing {
			return nil, errors.New("load view public.v: no route to host")
		}
		return f.svc.tableLocations(ctx)
	}
	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	if got := runFor(f, r.ID, 3); got.Status == StatusCompleted || !strings.Contains(got.Error, "public.v") {
		t.Errorf("%s: %q", got.Status, got.Error)
	}
	failing = false
	f.completed(settle(f, r.ID, 12))
}
