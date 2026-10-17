package pgwire

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/catalogproxy"
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
	var readers []string
	var readersErr error
	asked, released := 0, 0
	s := &Server{gov: gov, erasure: erasure.New(erasure.Options{Store: store}), log: log,
		cloneReaders: func(context.Context, string) ([]string, func(), error) {
			asked++
			if readersErr != nil {
				return nil, nil, readersErr
			}
			return readers, func() { released++ }, nil
		}}
	refused := func(c catalogproxy.Change, want string) {
		t.Helper()
		_, err := s.Check(ctx, &c)
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

	// Drops of other tables: purged unless clones may read their files.
	inWarehouse := catalogproxy.Change{Op: catalogproxy.OpDrop, Namespace: crm, Name: "other", Location: "s3://bucket/wh/abc"}
	if v := allowed(inWarehouse); !v.Purge || v.Release == nil {
		t.Errorf("no clone reads it: %+v", v)
	} else {
		v.Release()
	}
	readers = []string{"crm.copy"}
	if v := allowed(inWarehouse); v.Purge || released != 2 {
		t.Errorf("a clone reads it: %+v, released %d", v, released)
	}
	purge := inWarehouse
	purge.Purge = true
	refused(purge, "clones read its files (crm.copy)")
	readers, readersErr = nil, errors.New("down")
	if v := allowed(inWarehouse); v.Purge {
		t.Errorf("cannot tell whether clones read it: %+v", v)
	}
	refused(purge, "could not tell")
	readersErr = nil
	before := asked
	for _, loc := range []string{"s3://bucket/elsewhere/abc", "s3://bucket/whx/abc", ""} {
		c := inWarehouse
		c.Location = loc
		if v := allowed(c); v.Purge {
			t.Errorf("outside the warehouse (%q): %+v", loc, v)
		}
	}
	if asked != before {
		t.Errorf("asked about clones of a table outside the warehouse")
	}

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

// DuckDB's error for a change the catalog proxy refused is reported as
// permission denied, with the Guard's reason.
func TestCatalogRefusalIsPermissionDenied(t *testing.T) {
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
