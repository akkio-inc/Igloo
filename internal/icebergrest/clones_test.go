package icebergrest

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/clone/clonetest"
)

// withClones gives the harness's endpoint a guard of clones, with dev.events
// a clone of public.events: the guard's own catalog has it, and its store
// records that it reads files under public.events' location.
func withClones(t *testing.T, h *harness, store clone.Store) {
	t.Helper()
	cat := clonetest.New(t.TempDir())
	dev, err := cat.Create(table.Identifier{"dev", "events"}, clonetest.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(context.Background(), []clone.Share{{Clone: dev.Metadata().TableUUID(),
		Location: h.cat.tables["public\x1fevents"], Schema: "dev", Table: "events"}}); err != nil {
		t.Fatal(err)
	}
	h.srv.opts.Clones = guardOf(cat, store)
}

// guardOf is a guard of the clones of cat, whose shares store records,
// for the harness's warehouse.
func guardOf(cat clone.Catalog, store clone.Store) *clone.Guard {
	g := clone.NewGuard(cat, store, slog.New(slog.DiscardHandler))
	g.SetWarehouse(warehouse + "/")
	return g
}

func (h *harness) reached(request string) bool {
	h.cat.mu.Lock()
	defer h.cat.mu.Unlock()
	return slices.Contains(h.cat.requests, request)
}

func TestPurgeKeepsFilesClonesRead(t *testing.T) {
	h := newHarness(t)
	store := clone.NewMemoryStore()
	withClones(t, h, store)
	walt := h.login("walt", "walt-password")

	// Purging would delete the files dev.events reads: refused, naming it.
	for _, v := range []string{"true", "TRUE", "1"} {
		w := h.request("DELETE", "/v1/namespaces/public/tables/events?purgeRequested="+v, walt, nil)
		if kind, msg := errorType(t, w); w.Code != 409 || kind != errConflict || !strings.Contains(msg, "dev.events") {
			t.Fatalf("purge %s of a table clones read: %d %s %s", v, w.Code, kind, msg)
		}
	}
	if h.reached("DELETE " + prefix + "/namespaces/public/tables/events?purgeRequested=true") {
		t.Fatal("the purge reached the catalog")
	}
	// Other tables are purged; this one may be dropped, keeping its files.
	if w := h.request("DELETE", "/v1/namespaces/public/tables/users?purgeRequested=true", walt, nil); w.Code != 204 {
		t.Fatalf("purge of a table no clone reads: %d %s", w.Code, w.Body)
	}
	if !h.reached("DELETE " + prefix + "/namespaces/public/tables/users?purgeRequested=true") {
		t.Fatal("the purge did not reach the catalog")
	}
	if w := h.request("DELETE", "/v1/namespaces/public/tables/events?purgeRequested=false", walt, nil); w.Code != 204 {
		t.Fatalf("drop without purging: %d %s", w.Code, w.Body)
	}
	if q := h.queries(); !strings.Contains(q[len(q)-2].Text, "purge") || q[len(q)-3].Outcome != "error" {
		t.Errorf("history: %+v", q[len(q)-3:])
	}
}

func TestPurgeAfterClonesAreGone(t *testing.T) {
	h := newHarness(t)
	store := clone.NewMemoryStore()
	walt := h.login("walt", "walt-password")
	gone := func(created time.Time) {
		t.Helper()
		if err := store.Add(context.Background(), []clone.Share{{Clone: uuid.New(), Location: h.cat.tables["public\x1fevents"],
			Schema: "dev", Table: "dropped", Created: created}}); err != nil {
			t.Fatal(err)
		}
	}
	h.srv.opts.Clones = guardOf(clonetest.New(t.TempDir()), store)

	// A clone recorded moments ago that the catalog does not have may be
	// being made: the purge waits.
	gone(time.Now())
	if w := h.request("DELETE", "/v1/namespaces/public/tables/events?purgeRequested=true", walt, nil); w.Code != 409 {
		t.Fatalf("purge while a clone may be made: %d %s", w.Code, w.Body)
	}
	// One recorded long ago that the catalog does not have is gone.
	for _, s := range must(store.All(context.Background())) {
		_ = store.Remove(context.Background(), s.Clone, s.Location)
	}
	gone(time.Now().Add(-2 * time.Hour))
	if w := h.request("DELETE", "/v1/namespaces/public/tables/events?purgeRequested=true", walt, nil); w.Code != 204 {
		t.Fatalf("purge once the clones are gone: %d %s", w.Code, w.Body)
	}
}

// failingStore cannot say which clones read a location.
type failingStore struct{ clone.Store }

func (failingStore) Sharers(context.Context, string) ([]clone.Share, error) {
	return nil, errors.New("metadata database unavailable")
}

func TestCloneChecksFailClosed(t *testing.T) {
	h := newHarness(t)
	withClones(t, h, failingStore{clone.NewMemoryStore()})
	walt := h.login("walt", "walt-password")
	if w := h.request("DELETE", "/v1/namespaces/public/tables/users?purgeRequested=true", walt, nil); w.Code != 503 {
		t.Fatalf("purge without knowing the clones: %d %s", w.Code, w.Body)
	}
	// Write credentials then delete nothing.
	w := h.request("GET", "/v1/namespaces/public/tables/users", walt, nil, accessDelegation, vendedCredentials)
	if w.Code != 200 || !slices.Contains(h.vendor.calls, "walt add "+warehouse+"/u-users") {
		t.Fatalf("write credentials without knowing the clones: %d %v", w.Code, h.vendor.calls)
	}
}

func TestCredentialsAndClones(t *testing.T) {
	h := newHarness(t)
	withClones(t, h, clone.NewMemoryStore())
	walt := h.login("walt", "walt-password")
	vended := []string{accessDelegation, vendedCredentials}

	// public.events' files are read by a clone: writers' credentials for
	// it delete nothing. Other tables' do.
	if w := h.request("GET", "/v1/namespaces/public/tables/events", walt, nil, vended...); w.Code != 200 {
		t.Fatalf("load: %d %s", w.Code, w.Body)
	}
	if w := h.request("GET", "/v1/namespaces/public/tables/users", walt, nil, vended...); w.Code != 200 {
		t.Fatalf("load: %d %s", w.Code, w.Body)
	}
	if want := []string{"walt add " + warehouse + "/u-events", "walt write " + warehouse + "/u-users"}; !slices.Equal(h.vendor.calls, want) {
		t.Fatalf("credentials: %v, want %v", h.vendor.calls, want)
	}
	if q := h.queries(); !strings.Contains(q[len(q)-2].Text, "read-write credentials that delete nothing") {
		t.Errorf("history: %s", q[len(q)-2].Text)
	}

	// A clone reads files under its source's location, which its
	// credentials would not reach: it gets none, and says why.
	h.cat.mu.Lock()
	h.cat.props = map[string]map[string]string{"public\x1fusers": {clone.SharesProperty: `["` + warehouse + `/u-src"]`}}
	h.cat.mu.Unlock()
	for _, path := range []string{"/v1/namespaces/public/tables/users", "/v1/namespaces/public/tables/users/credentials"} {
		for _, user := range []string{"walt", "ada"} {
			w := h.request("GET", path, h.login(user, user+"-password"), nil, vended...)
			if kind, msg := errorType(t, w); w.Code != 403 || kind != errForbidden || !strings.Contains(msg, "is a clone") {
				t.Errorf("%s as %s: %d %s", path, user, w.Code, w.Body)
			}
		}
	}
	if len(h.vendor.calls) != 2 {
		t.Fatalf("credentials for a clone: %v", h.vendor.calls)
	}
	// Engines with access of their own still load it.
	if w := h.request("GET", "/v1/namespaces/public/tables/users", walt, nil); w.Code != 200 {
		t.Fatalf("load of a clone without credentials: %d %s", w.Code, w.Body)
	}
}

func TestCloneProperties(t *testing.T) {
	h := newHarness(t)
	admin := h.login("ada", "ada-password")
	shares := map[string]string{clone.SharesProperty: `[]`}
	for name, body := range map[string]map[string]any{
		"set":    {"updates": []any{map[string]any{"action": "set-properties", "updates": shares}}},
		"remove": {"updates": []any{map[string]any{"action": "remove-properties", "removals": []string{clone.SourceProperty}}}},
	} {
		if w := h.request("POST", "/v1/namespaces/public/tables/events", admin, body); w.Code != 400 || !strings.Contains(w.Body.String(), "is Igloo's") {
			t.Errorf("%s a clone's property: %d %s", name, w.Code, w.Body)
		}
	}
	if w := h.request("POST", "/v1/namespaces/public/tables", admin, map[string]any{"name": "fake", "properties": shares}); w.Code != 400 {
		t.Errorf("create a table with a clone's property: %d %s", w.Code, w.Body)
	}
	if h.reached("POST "+prefix+"/namespaces/public/tables/events") || h.reached("POST "+prefix+"/namespaces/public/tables") {
		t.Error("the change reached the catalog")
	}
	// Others' are the engines' to change.
	body := map[string]any{"updates": []any{map[string]any{"action": "remove-properties", "removals": []string{"comment"}}}}
	if w := h.request("POST", "/v1/namespaces/public/tables/events", admin, body); w.Code != 200 {
		t.Errorf("remove another property: %d %s", w.Code, w.Body)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
