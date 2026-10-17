package icebergrest

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/clone"
)

// What erasure requests need of other engines' changes (see package
// erasure): a dropped table leaves nothing behind that no one can reach,
// and no snapshot is committed that no one can read.

// A table in the warehouse dropped without purgeRequested is purged all
// the same when no clone reads its files, as in SQL: no one could reach
// what is left of a dropped table to erase from it. One whose files clones
// read keeps them, as does one outside the warehouse, and one whose clones
// cannot be told.
func TestDropPurgesWhatNoCloneReads(t *testing.T) {
	h := newHarness(t)
	withClones(t, h, clone.NewMemoryStore())
	walt := h.login("walt", "walt-password")
	h.cat.mu.Lock()
	h.cat.tables["public\x1fext"] = "s3://elsewhere/ext"
	h.cat.mu.Unlock()
	for _, c := range []struct {
		table string
		purge bool
	}{{"users", true}, {"events", false}, {"ext", false}} {
		if w := h.request("DELETE", "/v1/namespaces/public/tables/"+c.table, walt, nil); w.Code != 204 {
			t.Fatalf("drop %s: %d %s", c.table, w.Code, w.Body)
		}
		if purged := h.reached("DELETE " + prefix + "/namespaces/public/tables/" + c.table + "?purgeRequested=true"); purged != c.purge {
			t.Errorf("%s purged: %v", c.table, purged)
		}
	}
	h = newHarness(t)
	withClones(t, h, failingStore{clone.NewMemoryStore()})
	walt = h.login("walt", "walt-password")
	if w := h.request("DELETE", "/v1/namespaces/public/tables/users", walt, nil); w.Code != 204 ||
		h.reached("DELETE "+prefix+"/namespaces/public/tables/users?purgeRequested=true") {
		t.Errorf("drop without knowing the clones: %d %s", w.Code, w.Body)
	}
}

// A snapshot committed here must have its manifest list in the warehouse's
// storage, however its path spells the warehouse: a catalog takes one that
// is not, which no engine, and no erasure request, could read.
func TestSnapshotsNeedTheirManifestLists(t *testing.T) {
	h := newHarness(t)
	for _, p := range []auth.Privilege{auth.PrivilegeInsert, auth.PrivilegeUpdate, auth.PrivilegeDelete} {
		h.grant("walt", p, "public", "events")
	}
	walt := h.login("walt", "walt-password")
	commit := func(list string) *httptest.ResponseRecorder {
		t.Helper()
		snap := map[string]any{"snapshot-id": 7, "sequence-number": 2, "timestamp-ms": 1, "summary": map[string]string{"operation": "append"}, "schema-id": 0}
		if list != "" {
			snap["manifest-list"] = list
		}
		return h.request("POST", "/v1/namespaces/public/tables/events", walt,
			map[string]any{"requirements": []any{}, "updates": []any{map[string]any{"action": "add-snapshot", "snapshot": snap}}})
	}
	list := warehouse + "/u-events/metadata/snap-7.avro"
	for _, c := range []struct{ list, want string }{
		{list, "does not exist"},
		{strings.Replace(list, "s3://", "s3a://", 1), "does not exist"},
		{"s3://elsewhere/snap-7.avro", "outside the warehouse"},
	} {
		w := commit(c.list)
		if kind, msg := errorType(t, w); w.Code != 400 || kind != errBadRequest || !strings.Contains(msg, c.want) {
			t.Errorf("%s: %d %s %s", c.list, w.Code, kind, msg)
		}
	}
	if h.reached("POST " + prefix + "/namespaces/public/tables/events?") {
		t.Fatal("a refused snapshot reached the catalog")
	}
	h.files.Store(list, true)
	for _, l := range []string{list, strings.Replace(list, "s3://", "s3a://", 1), ""} {
		if w := commit(l); w.Code != 200 {
			t.Errorf("%q: %d %s", l, w.Code, w.Body)
		}
	}
	h.srv.opts.Exists = func(context.Context, string) (bool, error) { return false, errors.New("storage unavailable") }
	if w := commit(list); w.Code != 503 {
		t.Errorf("storage unavailable: %d %s", w.Code, w.Body)
	}
}
