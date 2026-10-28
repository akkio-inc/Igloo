package catalogproxy

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// TestSharedHold checks that while the catalog lock is held shared, the
// proxy refuses listings of a namespace's tables and views, and drops and
// renames, with 423 and counts them, and lets everything else through:
// loads of tables, listings of namespaces, and all of it once the lock is
// no longer held shared.
func TestSharedHold(t *testing.T) {
	e := newListingEnv(t, -1)
	var shared atomic.Bool
	e.proxy.SetSharedHold(shared.Load)
	e.createTable(t, e.proxy.URL(), "orders")
	e.createTable(t, e.proxy.URL(), "lines")
	call := func(method, path, body string, want int) {
		t.Helper()
		status, got := e.call(t, e.proxy.URL(), method, path, body)
		if status != want || want == http.StatusLocked && !strings.Contains(got, heldShared) {
			t.Errorf("%s %s: %d %s, want %d", method, path, status, got, want)
		}
	}
	rename := `{"source": {"namespace": ["public"], "name": "lines"}, "destination": {"namespace": ["public"], "name": "items"}}`

	shared.Store(true)
	call(http.MethodGet, "namespaces/public/tables", "", http.StatusLocked)
	call(http.MethodGet, "namespaces/public/tables?pageToken=x", "", http.StatusLocked)
	call(http.MethodGet, "namespaces/public/views", "", http.StatusLocked)
	call(http.MethodDelete, "namespaces/public/tables/orders?purgeRequested=true", "", http.StatusLocked)
	call(http.MethodPost, "tables/rename", rename, http.StatusLocked)
	call(http.MethodDelete, "namespaces/public", "", http.StatusLocked)
	if n := e.proxy.Refusals(); n != 6 {
		t.Errorf("%d refusals, want 6", n)
	}
	call(http.MethodGet, "namespaces/public/tables/orders", "", http.StatusOK)
	call(http.MethodHead, "namespaces/public/tables/orders", "", http.StatusNoContent)
	call(http.MethodGet, "namespaces/public/tables/missing", "", http.StatusNotFound)
	call(http.MethodGet, "namespaces", "", http.StatusOK)
	call(http.MethodGet, "namespaces/public", "", http.StatusOK)

	shared.Store(false)
	call(http.MethodGet, "namespaces/public/tables", "", http.StatusOK)
	call(http.MethodPost, "tables/rename", rename, http.StatusNoContent)
	call(http.MethodDelete, "namespaces/public/tables/orders", "", http.StatusNoContent)
	if n := e.proxy.Refusals(); n != 6 {
		t.Errorf("%d refusals after, want 6", n)
	}
}
