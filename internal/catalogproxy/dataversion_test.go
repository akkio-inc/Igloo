package catalogproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLocationScanner(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{`{"metadata-location": "s3://w/t/metadata/1.json", "metadata": {"format-version": 2}}`, "s3://w/t/metadata/1.json"},
		// Only the top level's key counts, after values that nest or hold
		// quotes and colons.
		{`{"metadata": {"properties": {"metadata-location": "nested"}}, "config": {"a": "x\":y"}, "metadata-location": "top"}`, "top"},
		{`{"metadata": {"refs": [{"metadata-location": "x"}]}, "metadata-location":"s3://b"}`, "s3://b"},
		// A location it cannot read as it stands, or none.
		{`{"metadata-location": "a\/b"}`, ""},
		{`{"metadata-location": null, "metadata": {}}`, ""},
		{`{"metadata": {}}`, ""},
		{`[{"metadata-location": "x"}]`, ""},
		{`{"metadata-location": "` + strings.Repeat("x", maxLocation+1) + `"}`, ""},
	} {
		// Every split of the answer into two writes reads the same.
		for i := 0; i <= len(c.in); i += 1 + len(c.in)/40 {
			s := &locationScanner{}
			s.Write([]byte(c.in[:i]))
			s.Write([]byte(c.in[i:]))
			if s.location != c.want {
				t.Errorf("%.60s split at %d: %q, want %q", c.in, i, s.location, c.want)
				break
			}
		}
	}
}

// TestDataVersion checks that the data version goes up with each change
// and with each load that finds a table elsewhere than the last load did,
// or that it cannot read, and not otherwise.
func TestDataVersion(t *testing.T) {
	var location atomic.Pointer[string]
	var gzipped atomic.Bool
	loc := func(s string) { location.Store(&s) }
	loc("s3://w/carts/metadata/1.json")
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/namespaces/sales/tables/carts") {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		if gzipped.Load() {
			w.Header().Set("Content-Encoding", "gzip")
		}
		_, _ = w.Write([]byte(`{"metadata-location": "` + *location.Load() + `", "metadata": {"format-version": 2}}`))
	}))
	t.Cleanup(catalog.Close)
	p, err := New(Options{Catalog: catalog.URL + "/catalog"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	send := func(method string) {
		t.Helper()
		req, _ := http.NewRequest(method, p.URL()+"/v1/w/namespaces/sales/tables/carts", strings.NewReader(`{"updates": []}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", method, resp.StatusCode)
		}
	}
	step := func(what string, do func(), up bool) {
		t.Helper()
		before := p.DataVersion()
		do()
		if after := p.DataVersion(); (after > before) != up {
			t.Errorf("%s: data version %d, then %d", what, before, after)
		}
	}
	step("the first load", func() { send(http.MethodGet) }, false)
	step("a load at the same location", func() { send(http.MethodGet) }, false)
	step("a load at another location", func() { loc("s3://w/carts/metadata/2.json"); send(http.MethodGet) }, true)
	step("a load at that location again", func() { send(http.MethodGet) }, false)
	step("a commit", func() { send(http.MethodPost) }, true)
	step("a load it cannot read", func() { gzipped.Store(true); send(http.MethodGet) }, true)
	gzipped.Store(false)
	send(http.MethodGet)
	step("Invalidate", p.Invalidate, true)
}

// TestTablesVersion checks that a table's version changes with changes to
// it, however they reach the catalog, and with loads that find it
// elsewhere, and not with changes to other tables or scans' metrics;
// changes the proxy cannot tell the tables of change every table's.
func TestTablesVersion(t *testing.T) {
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata-location": "s3://w/` + r.URL.Path + `/metadata/1.json", "metadata": {"format-version": 2}}`))
	}))
	t.Cleanup(catalog.Close)
	p, err := New(Options{Catalog: catalog.URL + "/catalog"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	send := func(method, path, body string) {
		t.Helper()
		req, _ := http.NewRequest(method, p.URL()+"/v1/w/"+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	record := p.RecordLoads()
	send(http.MethodGet, "namespaces/sales/tables/carts", "")
	carts := record()
	if len(carts) != 1 {
		t.Fatalf("loads: %v", carts)
	}
	step := func(what string, do func(), up bool) {
		t.Helper()
		before := p.TablesVersion(carts)
		do()
		if after := p.TablesVersion(carts); (after != before) != up {
			t.Errorf("%s: carts's version %d, then %d", what, before, after)
		}
	}
	step("a commit to another table", func() { send(http.MethodPost, "namespaces/sales/tables/orders", `{"updates": []}`) }, false)
	step("a scan's metrics", func() { send(http.MethodPost, "namespaces/sales/tables/carts/metrics", `{}`) }, false)
	step("a table made", func() { send(http.MethodPost, "namespaces/sales/tables", `{"name": "returns"}`) }, false)
	step("a commit", func() { send(http.MethodPost, "namespaces/sales/tables/carts", `{"updates": []}`) }, true)
	step("a commit of a transaction", func() {
		send(http.MethodPost, "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["Sales"], "name": "Carts"}}]}`)
	}, true)
	step("a transaction of another table", func() {
		send(http.MethodPost, "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["sales"], "name": "orders"}}]}`)
	}, false)
	step("a rename", func() {
		send(http.MethodPost, "tables/rename", `{"source": {"namespace": ["sales"], "name": "carts"}, "destination": {"namespace": ["sales"], "name": "old_carts"}}`)
	}, true)
	step("a drop", func() { send(http.MethodDelete, "namespaces/sales/tables/carts", "") }, true)
	step("a namespace made", func() { send(http.MethodPost, "namespaces", `{"namespace": ["staging"]}`) }, false)
	step("a namespace's properties", func() { send(http.MethodPost, "namespaces/sales/properties", `{"updates": {}}`) }, false)
	step("a namespace dropped", func() { send(http.MethodDelete, "namespaces/staging", "") }, false)
	step("a view", func() { send(http.MethodPost, "namespaces/sales/views", `{"name": "v"}`) }, true)
	step("Invalidate", p.Invalidate, true)
}
