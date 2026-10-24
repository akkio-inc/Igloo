package pgwire

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/policy"
)

// The changes statements make to the catalog are checked as the catalog
// sees them (see catalogguard.go): a table an open erasure request erases
// from is neither dropped nor renamed, nor its schema dropped, nor another
// table renamed to its name; no table with tags or under erasure is made
// or upgraded to format version 3; Igloo's clone properties are never set
// or removed; and a table in the warehouse dropped that no clone reads has
// its files deleted with it.
func TestCatalogGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	policies := policy.NewMemoryStore()
	if err := policies.Update(ctx, func(cat *policy.Catalog) error {
		now := time.Now()
		if err := cat.PutTag(policy.Tag{Name: "pii"}, false, now); err != nil {
			return err
		}
		return cat.SetColumnTag(policy.ColumnTag{Schema: "crm", Table: "tagged", Column: "email", Tag: "pii", Value: "email"}, now)
	}); err != nil {
		t.Fatal(err)
	}
	store := erasure.NewMemoryStore()
	if err := store.Create(ctx, &erasure.Request{ID: "01open", Status: erasure.StatusBlocked, Step: erasure.StepPurge, DeleteStarted: true,
		Tables: []erasure.Table{{Name: "crm.people", Ident: []string{"crm", "people"}, Reason: "tagged"}}}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.DiscardHandler)
	gov, err := newGovernance(ctx, policies, time.Minute, log, nil, nil, "s3://bucket/wh", "")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{gov: gov, erasure: erasure.New(erasure.Options{Store: store}), log: log}
	refused := func(c catalogproxy.Change, want string) {
		t.Helper()
		v, err := s.Check(ctx, &c)
		if v.Release != nil {
			v.Release()
		}
		var r *catalogproxy.Refusal
		if !errors.As(err, &r) || !strings.Contains(err.Error(), want) {
			t.Errorf("%+v: %v, want a refusal that says %q", c, err, want)
		}
	}
	allowed := func(c catalogproxy.Change) catalogproxy.Verdict {
		t.Helper()
		v, err := s.Check(ctx, &c)
		if err != nil {
			t.Errorf("%+v: %v", c, err)
		}
		return v
	}
	crm := []string{"crm"}
	refused(catalogproxy.Change{Op: catalogproxy.OpDrop, Namespace: crm, Name: "people"}, "an erasure request erases from crm.people")
	refused(catalogproxy.Change{Op: catalogproxy.OpRename, Namespace: crm, Name: "people", To: &catalogproxy.TableName{Namespace: crm, Name: "folks"}},
		"cannot be renamed")
	refused(catalogproxy.Change{Op: catalogproxy.OpRename, Namespace: crm, Name: "other", To: &catalogproxy.TableName{Namespace: crm, Name: "people"}},
		"no other table can take its name")
	refused(catalogproxy.Change{Op: catalogproxy.OpDropNamespace, Namespace: crm}, "schema crm")
	refused(catalogproxy.Change{Op: catalogproxy.OpCommit, Namespace: crm, Name: "people", FormatVersion: 3}, "format version 2")
	refused(catalogproxy.Change{Op: catalogproxy.OpCreate, Namespace: crm, Name: "tagged", FormatVersion: 3}, "has policies or tags")
	refused(catalogproxy.Change{Op: catalogproxy.OpCommit, Namespace: crm, Name: "scratch", Properties: map[string]string{"IGLOO.CLONE.source": "crm.people"}},
		"Igloo's own property")
	refused(catalogproxy.Change{Op: catalogproxy.OpCommit, Namespace: crm, Name: "scratch", Removed: []string{"igloo.clone.shares-files-of"}},
		"Igloo's own property")
	refused(catalogproxy.Change{Op: catalogproxy.OpCreate, Namespace: crm, Name: "scratch", Properties: map[string]string{"igloo.clone.source": "x"}},
		"Igloo's own property")
	allowed(catalogproxy.Change{Op: catalogproxy.OpCommit, Namespace: crm, Name: "people", FormatVersion: 2, Properties: map[string]string{"a": "b"}})
	allowed(catalogproxy.Change{Op: catalogproxy.OpCreate, Namespace: crm, Name: "other", FormatVersion: 3})
	allowed(catalogproxy.Change{Op: catalogproxy.OpRename, Namespace: crm, Name: "other", To: &catalogproxy.TableName{Namespace: crm, Name: "else"}})

	// Drops of other tables: purged when their files may go, as decided
	// for every way Igloo deletes a table's files (clone.Guard.MayPurge).
	root := t.TempDir()
	cat := clonetest.New(filepath.Join(root, "tables"))
	src, err := cat.Create(table.Identifier{"crm", "events"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	src = clonetest.Append(t, src, 1)
	other, err := cat.Create(table.Identifier{"crm", "other"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	shares := clone.NewMemoryStore()
	if _, err := clone.NewManager(cat, shares, log).Clone(ctx, clone.Request{Source: table.Identifier{"crm", "events"},
		Target: table.Identifier{"crm", "copy"}}); err != nil {
		t.Fatal(err)
	}
	guard := func(shares clone.Store) {
		g := clone.NewGuard(cat, shares, log)
		g.SetWarehouse(root + "/")
		s.clonePurge = g.MayPurge
	}
	guard(shares)
	drop := func(name, location string, purge bool) catalogproxy.Change {
		return catalogproxy.Change{Op: catalogproxy.OpDrop, Namespace: crm, Name: name, Location: location, Purge: purge}
	}
	// kept checks that a drop keeps the table's files.
	kept := func(c catalogproxy.Change, why string) {
		t.Helper()
		if v := allowed(c); v.Purge || v.Release != nil {
			t.Errorf("%s: %+v", why, v)
			if v.Release != nil {
				v.Release()
			}
		}
	}
	if v := allowed(drop("other", other.Location(), false)); !v.Purge || v.Release == nil {
		t.Errorf("no clone reads it: %+v", v)
	} else {
		v.Release()
	}
	// A clone reads crm.events: recorded, or, with a memory store not yet
	// rebuilt since a restart, only declared in the clone's properties.
	for _, st := range []clone.Store{shares, clone.NewMemoryStore()} {
		guard(st)
		kept(drop("events", src.Location(), false), "a clone reads it")
		refused(drop("events", src.Location(), true), "clones read its files (crm.copy)")
	}
	guard(failing{shares})
	kept(drop("other", other.Location(), false), "cannot tell whether clones read it")
	refused(drop("other", other.Location(), true), "could not tell")
	guard(shares)
	for _, loc := range []string{root + "/.igloo", root + "/.igloo/sharing/t", "s3://bucket/elsewhere/abc", root, ""} {
		kept(drop("other", loc, false), "location "+loc)
	}
	refused(drop("other", root+"/.igloo/t", true), "Igloo's own files")
	refused(drop("other", "", true), "could not tell")

	// Once the request is over, the table is anyone's.
	r, err := store.Get(ctx, "01open")
	if err != nil {
		t.Fatal(err)
	}
	r.Status = erasure.StatusCompleted
	if err := store.Save(ctx, r); err != nil {
		t.Fatal(err)
	}
	allowed(catalogproxy.Change{Op: catalogproxy.OpDrop, Namespace: crm, Name: "people"})
}

// failing is a clone Store that cannot say which clones read a location.
type failing struct{ clone.Store }

func (failing) Sharers(context.Context, string) ([]clone.Share, error) {
	return nil, errors.New("metadata database unavailable")
}

// DuckDB's error for a change the catalog proxy refused is reported as
// permission denied, with the Guard's reason.
func TestCatalogRefusalIsPermissionDenied(t *testing.T) {
	t.Parallel()
	err := &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: `IO Error: Request to 'http://127.0.0.1:1/x/v1/p/namespaces/crm/tables/people' returned 403: ` +
		`{"error":{"code":403,"message":"` + catalogproxy.RefusalMark + `permission denied: an erasure request erases from crm.people; ` +
		`it cannot be dropped until the request is over","type":"ForbiddenException"}}`}
	got := pgError(err)
	if pgerror.GetCode(got) != codes.InsufficientPrivilege ||
		got.Error() != "permission denied: an erasure request erases from crm.people; it cannot be dropped until the request is over" {
		t.Errorf("%v (%s)", got, pgerror.GetCode(got))
	}
	if other := pgError(errors.New("something else")); pgerror.GetCode(other) == codes.InsufficientPrivilege {
		t.Errorf("%v", other)
	}
}
