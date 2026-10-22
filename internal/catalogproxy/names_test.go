package catalogproxy

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestRespelled checks that the proxy notes the tables loaded through it
// under another name than the catalog spells them with, by the catalog's
// name, once each until Respelled returns them, and that it learns the
// catalog's spellings by listing a namespace only when a table loaded is not
// among those it knows: not for a table spelled as it knows it, one made,
// renamed or dropped through it, or a name the catalog does not have.
func TestRespelled(t *testing.T) {
	e := newListingEnv(t, time.Minute)
	e.fake.IgnoreNameCase()
	e.createTable(t, e.proxy.URL(), "ORDERS")
	lists := e.lists.Load()
	load := func(name string, want int) {
		t.Helper()
		if status, _ := e.call(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/"+name, ""); status != want {
			t.Fatalf("loading %s: %d, want %d", name, status, want)
		}
	}
	expect := func(step string, want []string, listed int32) {
		t.Helper()
		var got []string
		for _, tb := range e.proxy.Respelled() {
			got = append(got, strings.Join(tb.Namespace, ".")+"."+tb.Name)
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: respelled %q, want %q", step, got, want)
		}
		if got := e.lists.Load() - lists; got != listed {
			t.Errorf("%s: the proxy listed the catalog %d times, want %d", step, got, listed)
		}
	}

	load("ORDERS", http.StatusOK)
	expect("a table loaded by its own name", nil, 1)
	load("orders", http.StatusOK)
	load("Orders", http.StatusOK)
	expect("a table loaded by other names", []string{"public.ORDERS"}, 1)
	expect("again", nil, 1)
	load("nope", http.StatusNotFound)
	expect("a name the catalog does not have", nil, 1)

	// Made around the proxy: the proxy lists the namespace again.
	e.createTable(t, e.direct, "Items")
	load("items", http.StatusOK)
	expect("a table made around the proxy", []string{"public.Items"}, 2)

	// Made, renamed and dropped through the proxy.
	e.createTable(t, e.proxy.URL(), "Lines")
	load("LINES", http.StatusOK)
	expect("a table made through the proxy", []string{"public.Lines"}, 2)
	e.must(t, e.proxy.URL(), http.MethodPost, "tables/rename",
		`{"source": {"namespace": ["public"], "name": "lines"}, "destination": {"namespace": ["public"], "name": "Rows"}}`)
	load("rows", http.StatusOK)
	expect("a table renamed through the proxy", []string{"public.Rows"}, 2)
	e.must(t, e.proxy.URL(), http.MethodDelete, "namespaces/public/tables/rows", "")
	e.createTable(t, e.direct, "rows")
	load("rows", http.StatusOK)
	expect("a table dropped through the proxy and made again around it", nil, 3)
}

// TestRespelledListsAgain checks that a table loaded under another name
// than an expired listing of its namespace says is noted by it, without
// waiting for the catalog, and that the namespace is listed again in the
// background, so that later loads go by the catalog as it is now: here,
// another engine replaced the table with one spelled as the load asked.
func TestRespelledListsAgain(t *testing.T) {
	e := newListingEnv(t, 50*time.Millisecond)
	e.fake.IgnoreNameCase()
	e.createTable(t, e.proxy.URL(), "ORDERS")
	load := func(name string) {
		t.Helper()
		e.must(t, e.proxy.URL(), http.MethodGet, "namespaces/public/tables/"+name, "")
	}
	respelled := func() []string {
		var got []string
		for _, tb := range e.proxy.Respelled() {
			got = append(got, tb.Name)
		}
		return got
	}
	load("ORDERS")
	lists := e.lists.Load()
	time.Sleep(100 * time.Millisecond)
	e.must(t, e.direct, http.MethodDelete, "namespaces/public/tables/ORDERS", "")
	e.createTable(t, e.direct, "orders")

	load("orders")
	if got := respelled(); !slices.Equal(got, []string{"ORDERS"}) {
		t.Errorf("loaded by a name an expired listing spells otherwise: respelled %q, want [ORDERS]", got)
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		if tb, _, _ := e.proxy.names.lookup([2]string{"public", "orders"}); tb.Name == "orders" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the namespace was not listed again")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := e.lists.Load() - lists; got != 1 {
		t.Errorf("the namespace was listed %d times again, want 1", got)
	}
	load("orders")
	if got := respelled(); got != nil {
		t.Errorf("loaded by the catalog's name, as listed again: respelled %q", got)
	}
	load("ORDERS")
	if got := respelled(); !slices.Equal(got, []string{"orders"}) {
		t.Errorf("loaded by another name than the one listed again: respelled %q, want [orders]", got)
	}
}

// TestSpellingsFollowPages checks that the proxy learns the spellings of
// every page of a namespace's listing.
func TestSpellingsFollowPages(t *testing.T) {
	pages := map[string]string{
		"":   `{"identifiers": [{"namespace": ["Sales"], "name": "A"}], "next-page-token": "p2"}`,
		"p2": `{"identifiers": [{"namespace": ["Sales"], "name": "B"}], "next-page-token": "p3"}`,
		"p3": `{"identifiers": [{"namespace": ["Sales"], "name": "Carts"}]}`,
	}
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/namespaces/sales/tables"):
			_, _ = w.Write([]byte(pages[r.URL.Query().Get("pageToken")]))
		case strings.HasSuffix(r.URL.Path, "/namespaces/sales/tables/carts"):
			_, _ = w.Write([]byte(`{"metadata-location": "x", "metadata": {}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(catalog.Close)
	p, err := New(Options{Catalog: catalog.URL + "/catalog"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	resp, err := http.Get(p.URL() + "/v1/w/namespaces/sales/tables/carts")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("loading the table: %d", resp.StatusCode)
	}
	if got := p.Respelled(); len(got) != 1 || got[0].Name != "Carts" || !slices.Equal(got[0].Namespace, []string{"Sales"}) {
		t.Errorf("respelled %+v, want Sales.Carts", got)
	}
}
