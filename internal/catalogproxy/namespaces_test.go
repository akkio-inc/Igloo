package catalogproxy

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestHideNamespace(t *testing.T) {
	e := newListingEnv(t, time.Second)
	ctx := context.Background()
	for _, c := range []struct {
		ns     string
		exists bool
	}{{"public", true}, {"nope", false}} {
		if exists, ok := e.proxy.NamespaceExists(ctx, []string{c.ns}); !ok || exists != c.exists {
			t.Fatalf("NamespaceExists(%s): %v %v", c.ns, exists, ok)
		}
	}
	check := func(method string, want int) {
		t.Helper()
		if status, _ := e.call(t, e.proxy.URL(), method, "namespaces/public", ""); status != want {
			t.Fatalf("%s public: %d, want %d", method, status, want)
		}
	}
	// One check is answered 404, HEAD or GET; the next goes to the catalog.
	e.proxy.HideNamespace([]string{"public"})
	check(http.MethodHead, http.StatusNotFound)
	check(http.MethodHead, http.StatusNoContent)
	e.proxy.HideNamespace([]string{"public"})
	check(http.MethodGet, http.StatusNotFound)
	check(http.MethodGet, http.StatusOK)
	// Other paths under it are not hidden, and stopping without a check
	// hides nothing more.
	stop := e.proxy.HideNamespace([]string{"public"})
	if status, _ := e.call(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables", ""); status != http.StatusOK {
		t.Fatalf("tables of public: %d", status)
	}
	stop()
	check(http.MethodHead, http.StatusNoContent)
}
