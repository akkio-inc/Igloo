package erasure

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/policy"
)

const stagingName = "crm.people" + policy.StagingMark + "0123456789abcdef"

// stagingFixture has a tagged table of people, and whose rows go by id.
func stagingFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	byID := iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	f.rows["crm.people"], f.rows[stagingName] = byID, byID
	return f
}

// rename makes table from have the name to, as the rename of a fill does.
func rename(f *fixture, from, to string) {
	f.t.Helper()
	tbl := f.load(from)
	f.cat.mu.Lock()
	defer f.cat.mu.Unlock()
	delete(f.cat.tables, from)
	f.cat.tables[to] = table.New(ident(to), tbl.Metadata(), tbl.MetadataLocation(), localFS, f.cat)
}

// A stopped CREATE OR REPLACE TABLE leaves its staging table with the person's
// rows, committed and in a file a worker wrote: the request erases from it as
// from the table it was to replace, and completes with neither left.
func TestRequestErasesFromStoppedCreate(t *testing.T) {
	f := stagingFixture(t)
	staging := f.people(stagingName, [][]any{carl, subject})
	left := filepath.Join(staging.Location(), "data", "igloo-write-fedcba9876543210", "q1", "s1-0", "a0", "f", "part_0.parquet")
	if err := os.MkdirAll(filepath.Dir(left), 0o755); err != nil {
		t.Fatal(err)
	}
	f.eng.exec(t, fmt.Sprintf("COPY (SELECT 2 AS id, '%s' AS email) TO '%s' (FORMAT parquet, FIELD_IDS {'id': 1, 'email': 2})", storedEmail, left))

	r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
	if err != nil {
		t.Fatal(err)
	}
	got := settle(f, r.ID, 10)
	f.completed(got)
	st := tableOf(t, got, stagingName)
	if st.StagingOf != "crm.people" || st.RowsDeleted != 1 || !strings.Contains(st.Reason, "crm.people") {
		t.Errorf("the staging table in the request: %+v", st)
	}
	if ids := f.ids(f.load(stagingName), 0); !slices.Equal(ids, []string{"3"}) {
		t.Errorf("the staging table's rows: %v", ids)
	}
	if _, err := os.Stat(left); !os.IsNotExist(err) {
		t.Errorf("the worker's file is still there: %v", err)
	}
	f.clean(storedEmail)
}

// A request that matches every row of a staging table is not held for it: the
// table is a copy of one the hold looks at, and has few of its rows yet.
func TestStagingTableDoesNotHoldRequest(t *testing.T) {
	f := stagingFixture(t)
	f.people(stagingName, [][]any{subject})
	f.completed(f.erase(byEmail))
	f.clean(storedEmail)
}

// A staging table without the tagged columns cannot say whose its rows are, as
// for any table without the tag, and does not stop the request.
func TestStagingTableWithoutTaggedColumns(t *testing.T) {
	f := stagingFixture(t)
	f.cat.create(t, f.dir, stagingName, iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}))
	got := f.erase(byEmail)
	f.completed(got)
	if slices.ContainsFunc(got.Tables, func(t Table) bool { return t.Name == stagingName }) {
		t.Errorf("the staging table without the column is in the request: %+v", got.Tables)
	}
}

// A staging table made once the request has deleted from the table it replaces
// is found by the verification, which sends the request back.
func TestStagingTableMadeWhileRequestRuns(t *testing.T) {
	f := stagingFixture(t)
	rows := &hookRows{testRows: f.rows, f: f}
	rows.after = func(name string) {
		if name == "crm.people" {
			rows.after = nil
			f.people(stagingName, [][]any{carl, subject})
		}
	}
	f.svc.rows = rows
	got := f.erase(byEmail)
	f.completed(got)
	if st := tableOf(t, got, stagingName); st.RowsDeleted != 1 {
		t.Errorf("the staging table: %+v", st)
	}
	if got.Rounds == 0 {
		t.Errorf("the verification did not send the request back: %d rounds", got.Rounds)
	}
	f.clean(storedEmail)
}

// A fill can end, or what a stopped one left be cleared, as a request runs: a
// staging table renamed into place is what the tagged table is now, and one
// dropped goes with the files of any table dropped. Neither stops the request.
func TestStagingTableGoesWhileRequestRuns(t *testing.T) {
	for _, c := range []struct {
		name string
		goes func(f *fixture)
	}{
		{"renamed into place", func(f *fixture) { rename(f, stagingName, "crm.people") }},
		{"dropped", func(f *fixture) { swapTable(f.t, f, stagingName, 0) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := stagingFixture(t)
			f.people(stagingName, [][]any{carl, subject})
			rows := &hookRows{testRows: f.rows, f: f}
			rows.before = func(name string) {
				if name == "crm.people" {
					rows.before = nil
					c.goes(f)
				}
			}
			f.svc.rows = rows
			r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
			if err != nil {
				t.Fatal(err)
			}
			got := settle(f, r.ID, 10)
			f.completed(got)
			if st := tableOf(t, got, stagingName); !st.Gone || st.Blocked != "" {
				t.Errorf("the staging table: %+v", st)
			}
			f.clean(storedEmail)
		})
	}

	// Renamed to anything else, it still holds its rows: set aside, as any
	// table renamed is.
	t.Run("renamed elsewhere", func(t *testing.T) {
		f := stagingFixture(t)
		f.people(stagingName, [][]any{carl, subject})
		rows := &hookRows{testRows: f.rows, f: f}
		rows.before = func(name string) {
			if name == "crm.people" {
				rows.before = nil
				rename(f, stagingName, "crm.kept")
			}
		}
		f.svc.rows = rows
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		got := settle(f, r.ID, 10)
		if st := tableOf(t, got, stagingName); got.Status != StatusBlocked || !strings.Contains(st.Blocked, "crm.kept") {
			t.Errorf("%s; the staging table: %+v", got.Status, st)
		}
	})
}

// The rows of a fill that began before the request's deletes can land in the
// table after its verification: it waits for the fill, and erases from what it
// made. A fill that began after them read none, and one of a stopped process
// goes on no more.
func TestRequestWaitsForFillUnderWay(t *testing.T) {
	fill := func(at time.Time, host, process string) catalog.Pending {
		return catalog.Pending{Key: "igloo.pending.x", Kind: catalog.PendingFill, Name: stagingName[len("crm."):], Target: "people",
			Host: host, Process: process, At: at}
	}
	running := catalog.NewPending(catalog.PendingFill, stagingName[len("crm."):], "people")

	t.Run("under way", func(t *testing.T) {
		f := stagingFixture(t)
		f.people(stagingName, [][]any{carl})
		notes := []catalog.Pending{running}
		f.svc.opts.Making = func(_ context.Context, schema string) ([]catalog.Pending, error) {
			if schema != "crm" {
				t.Errorf("notes of schema %s", schema)
			}
			return notes, nil
		}
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		_ = f.svc.Run(f.ctx, r.ID)
		got, err := f.store.Get(f.ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != StatusWaiting || got.Step != StepVerify || !strings.Contains(got.Error, stagingName) {
			t.Fatalf("%s at %s: %q", got.Status, got.Step, got.Error)
		}

		// The fill wrote the person's rows, and renamed its table into place.
		appendRows(t, f.load(stagingName), subject)
		rename(f, stagingName, "crm.people")
		notes = nil
		got = settle(f, r.ID, 10)
		f.completed(got)
		if ids := f.ids(f.load("crm.people"), 0); !slices.Equal(ids, []string{"3"}) {
			t.Errorf("the table made: %v", ids)
		}
		f.clean(storedEmail)
	})
	t.Run("began after the deletes", func(t *testing.T) {
		f := stagingFixture(t)
		f.svc.opts.Making = func(context.Context, string) ([]catalog.Pending, error) {
			return []catalog.Pending{fill(time.Now().Add(time.Hour), running.Host, running.Process)}, nil
		}
		f.completed(f.erase(byEmail))
	})
	t.Run("the schema gone", func(t *testing.T) {
		f := stagingFixture(t)
		rows := &hookRows{testRows: f.rows, f: f}
		rows.after = func(string) { swapTable(t, f, "crm.people", 0) }
		f.svc.rows = rows
		f.svc.opts.Making = func(context.Context, string) ([]catalog.Pending, error) {
			if f.cat.tables["crm.people"] == nil {
				return nil, icecat.ErrNoSuchNamespace
			}
			return nil, nil
		}
		r, err := f.svc.Submit(f.ctx, "privacy", []Wanted{byEmail})
		if err != nil {
			t.Fatal(err)
		}
		f.completed(settle(f, r.ID, 10))
	})
	t.Run("stopped", func(t *testing.T) {
		f := stagingFixture(t)
		f.people(stagingName, [][]any{carl})
		f.svc.opts.Making = func(context.Context, string) ([]catalog.Pending, error) {
			return []catalog.Pending{fill(time.Now().Add(-2*catalog.PendingStale), "another-host", "gone")}, nil
		}
		f.completed(f.erase(byEmail))
	})
}
