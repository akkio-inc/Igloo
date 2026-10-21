package catalogproxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// listingEnv is a proxy in front of icebergtest's catalog, with a counter
// of the listings that reach the catalog between them.
type listingEnv struct {
	fake   *icebergtest.Catalog
	proxy  *Proxy
	lists  atomic.Int32
	direct string
}

func newListingEnv(t *testing.T, ttl time.Duration) *listingEnv {
	t.Helper()
	e := &listingEnv{fake: icebergtest.NewCatalog(t.TempDir())}
	t.Cleanup(e.fake.Close)
	target, err := url.Parse(e.fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: target.Scheme, Host: target.Host})
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, target.Path+"/")
		if r.Method == http.MethodGet && listPath.MatchString(rest) {
			e.lists.Add(1)
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(counting.Close)
	if e.proxy, err = New(Options{Catalog: counting.URL + target.Path, ListTTL: ttl}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.proxy.Close() })
	e.direct = e.fake.URL
	e.must(t, e.proxy.URL(), http.MethodPost, "namespaces", `{"namespace": ["public"]}`)
	e.createTable(t, e.proxy.URL(), "t")
	return e
}

// call sends a request to a path under the warehouse of base (the proxy's
// URL, or the catalog's to go around the proxy).
func (e *listingEnv) call(t *testing.T, base, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, base+"/v1/"+icebergtest.Prefix+"/"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func (e *listingEnv) must(t *testing.T, base, method, path, body string) string {
	t.Helper()
	status, b := e.call(t, base, method, path, body)
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("%s %s: %d %s", method, path, status, b)
	}
	return b
}

func (e *listingEnv) createTable(t *testing.T, base, name string) {
	t.Helper()
	e.must(t, base, http.MethodPost, "namespaces/public/tables", `{"name": "`+name+`", "schema":
		{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "n", "required": false, "type": "int"}]}}`)
}

// tables lists public's tables through the proxy.
func (e *listingEnv) tables(t *testing.T) []string {
	t.Helper()
	var list struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}
	if err := json.Unmarshal([]byte(e.must(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables", "")), &list); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, id := range list.Identifiers {
		names = append(names, id.Name)
	}
	slices.Sort(names)
	return names
}

// TestListingsKept checks that the proxy answers a listing again from what
// it kept, and asks the catalog again after any change it learns of: one
// through it, a table that DuckDB loads by name and the listing it kept
// does not have (made around the proxy) or has and the catalog no longer
// does, Invalidate, and time.
func TestListingsKept(t *testing.T) {
	e := newListingEnv(t, time.Minute)
	check := func(step string, want []string, lists int32) {
		t.Helper()
		if got := e.tables(t); !slices.Equal(got, want) {
			t.Errorf("%s: tables %v, want %v", step, got, want)
		}
		if got := e.lists.Load(); got != lists {
			t.Errorf("%s: %d listings reached the catalog, want %d", step, got, lists)
		}
	}
	check("first", []string{"t"}, 1)
	check("again", []string{"t"}, 1)

	// A change through the proxy.
	e.createTable(t, e.proxy.URL(), "u")
	check("after a change through the proxy", []string{"t", "u"}, 2)
	check("again", []string{"t", "u"}, 2)

	// A table made around the proxy, by another engine or replica: the
	// kept listing stands until DuckDB loads it by name, which it does
	// before it gives up on a name its listing lacks.
	e.createTable(t, e.direct, "v")
	check("a table made around the proxy, not yet loaded", []string{"t", "u"}, 2)
	e.must(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/v", "")
	check("after loading it", []string{"t", "u", "v"}, 3)
	// Loading a listed table changes nothing.
	e.must(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/t", "")
	if status, _ := e.call(t, e.proxy.URL(), http.MethodHead, "namespaces/public/tables/u", ""); status != http.StatusNoContent && status != http.StatusOK {
		t.Errorf("HEAD of a table: %d", status)
	}
	check("after loading listed tables", []string{"t", "u", "v"}, 3)

	// A table dropped around the proxy: a load that finds none.
	e.must(t, e.direct, http.MethodDelete, "namespaces/public/tables/u", "")
	if status, _ := e.call(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/u", ""); status != http.StatusNotFound {
		t.Errorf("loading a dropped table: %d", status)
	}
	check("after a table dropped around the proxy", []string{"t", "v"}, 4)
	// Loading a name that is in no listing and in no catalog changes
	// nothing.
	e.call(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/nope", "")
	check("after loading a missing name", []string{"t", "v"}, 4)

	e.createTable(t, e.direct, "w")
	e.proxy.Invalidate()
	check("after Invalidate", []string{"t", "v", "w"}, 5)

	// Namespaces: a listing of them, and one made around the proxy.
	nsList := func() string { return e.must(t, e.proxy.URL(), http.MethodGet, "namespaces", "") }
	nsList()
	nsList()
	if got := e.lists.Load(); got != 6 {
		t.Errorf("namespaces listed twice: %d listings reached the catalog, want 6", got)
	}
	e.must(t, e.direct, http.MethodPost, "namespaces", `{"namespace": ["sales"]}`)
	e.must(t, e.proxy.URL(), http.MethodGet, "namespaces/sales", "")
	if got := nsList(); !strings.Contains(got, "sales") {
		t.Errorf("namespaces after loading one made around the proxy: %s", got)
	}
}

// TestListingsExpire checks that a kept listing is asked for again once
// ListTTL has passed, and that a negative ListTTL keeps none.
func TestListingsExpire(t *testing.T) {
	e := newListingEnv(t, 50*time.Millisecond)
	e.tables(t)
	e.tables(t)
	if got := e.lists.Load(); got != 1 {
		t.Fatalf("%d listings reached the catalog, want 1", got)
	}
	e.createTable(t, e.direct, "u")
	time.Sleep(100 * time.Millisecond)
	if got := e.tables(t); !slices.Equal(got, []string{"t", "u"}) {
		t.Errorf("tables after the listing expired: %v", got)
	}

	off := newListingEnv(t, -1)
	off.tables(t)
	off.tables(t)
	if got := off.lists.Load(); got != 2 {
		t.Errorf("with ListTTL negative: %d listings reached the catalog, want 2", got)
	}
}

// TestListingNotKeptAcrossAChange checks that an answer to a listing begun
// before the listings were forgotten (by a change under way) is not kept.
func TestListingNotKeptAcrossAChange(t *testing.T) {
	l := newListings(time.Minute)
	_, gen := l.lookup("v1/p/namespaces/public/tables")
	l.forget()
	l.keep("v1/p/namespaces/public/tables", gen, &result{status: http.StatusOK, body: []byte(`{"identifiers": []}`)})
	if k, _ := l.lookup("v1/p/namespaces/public/tables"); k != nil {
		t.Error("a listing begun before a change was kept")
	}
	_, gen = l.lookup("v1/p/namespaces/public/tables")
	l.keep("v1/p/namespaces/public/tables", gen, &result{status: http.StatusServiceUnavailable})
	if k, _ := l.lookup("v1/p/namespaces/public/tables"); k != nil {
		t.Error("an answer other than 200 was kept")
	}
}

// TestListingsGeneration checks that Generation changes when the proxy
// forgets its listings and when a listing it asks for again, once expired,
// lists other names, and not when one comes back the same.
func TestListingsGeneration(t *testing.T) {
	e := newListingEnv(t, 50*time.Millisecond)
	e.tables(t)
	g := e.proxy.Generation()
	time.Sleep(100 * time.Millisecond)
	e.tables(t)
	if got := e.proxy.Generation(); got != g {
		t.Errorf("a listing that came back the same changed the generation: %d, was %d", got, g)
	}
	e.createTable(t, e.direct, "u")
	time.Sleep(100 * time.Millisecond)
	e.tables(t)
	if got := e.proxy.Generation(); got == g {
		t.Error("a listing that came back with another table did not change the generation")
	}
	g = e.proxy.Generation()
	e.proxy.Invalidate()
	if got := e.proxy.Generation(); got == g {
		t.Error("Invalidate did not change the generation")
	}
	var none *Proxy
	none.Invalidate()
	if none.Generation() != 0 {
		t.Error("a nil proxy's generation")
	}
}
