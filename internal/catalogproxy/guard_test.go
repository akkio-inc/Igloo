package catalogproxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// testGuard records the changes it is asked about, refuses those refuse
// says to, and has drops purge if purge is set.
type testGuard struct {
	mu       sync.Mutex
	changes  []Change
	refuse   func(c *Change) error
	purge    bool
	released int
}

func (g *testGuard) Check(_ context.Context, c *Change) (Verdict, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.changes = append(g.changes, *c)
	if g.refuse != nil {
		if err := g.refuse(c); err != nil {
			return Verdict{}, err
		}
	}
	return Verdict{Purge: g.purge, Release: func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.released++
	}}, nil
}

func (g *testGuard) last() Change {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.changes) == 0 {
		return Change{}
	}
	return g.changes[len(g.changes)-1]
}

// Every change DuckDB asks of the catalog reaches the Guard as the catalog
// would see it, whichever endpoint it is sent to: a refusal never reaches
// the catalog and is answered 403, with the mark Igloo reports as
// permission denied; a drop the Guard has purge reaches the catalog with
// purgeRequested, and what the Guard holds is released once the catalog
// has answered.
func TestGuardSeesEveryChange(t *testing.T) {
	fake := icebergtest.NewCatalog(t.TempDir())
	t.Cleanup(fake.Close)
	target, err := url.Parse(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	target.Path = ""
	forward := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	var reached []string
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reached = append(reached, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(front.Close)
	p, err := New(Options{Catalog: front.URL + "/catalog", AttemptTimeout: 10 * time.Second, Patience: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	g := &testGuard{}
	p.SetGuard(g)
	do := func(method, path, body string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, p.URL()+"/v1/"+icebergtest.Prefix+"/"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close() //nolint:errcheck
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	reachedCatalog := func(want string) bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.ContainsFunc(reached, func(r string) bool { return strings.HasPrefix(r, want) })
	}

	if status, body := do(http.MethodPost, "namespaces", `{"namespace": ["public"]}`); status != http.StatusOK {
		t.Fatalf("create namespace: %d %s", status, body)
	}
	status, body := do(http.MethodPost, "namespaces/public/tables", `{"name": "t", "properties": {"format-version": "3"}, "schema":
		{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "n", "required": false, "type": "int"}]}}`)
	if c := g.last(); status != http.StatusOK || c.Op != OpCreate || c.Name != "t" || !slices.Equal(c.Namespace, []string{"public"}) || c.FormatVersion != 3 {
		t.Fatalf("create table: %d %s; %+v", status, body, c)
	}
	location := fake.Metadata("public", "t").Location()

	// Refused: answered 403, and the catalog never sees it.
	g.refuse = func(c *Change) error {
		for k := range c.Properties {
			if strings.HasPrefix(k, "igloo.clone.") {
				return Refuse("permission denied: not that one")
			}
		}
		return nil
	}
	for _, c := range []struct{ path, body string }{
		{"namespaces/public/tables/t", `{"requirements": [], "updates": [{"action": "set-properties", "updates": {"igloo.clone.source": "public.people"}}]}`},
		{"transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["public"], "name": "t"}, "requirements": [],
			"updates": [{"action": "set-properties", "updates": {"igloo.clone.source": "public.people"}}]}]}`},
	} {
		status, body := do(http.MethodPost, c.path, c.body)
		if status != http.StatusForbidden || !strings.Contains(body, RefusalMark+"permission denied: not that one") {
			t.Errorf("%s: %d %s", c.path, status, body)
		}
	}
	if reachedCatalog("POST /catalog/v1/"+icebergtest.Prefix+"/namespaces/public/tables/t") ||
		reachedCatalog("POST /catalog/v1/"+icebergtest.Prefix+"/transactions/commit") {
		t.Errorf("a refused change reached the catalog: %v", reached)
	}
	if _, ok := fake.Metadata("public", "t").Properties()["igloo.clone.source"]; ok {
		t.Error("the refused property was set")
	}
	g.refuse = nil

	// What each change does, as the Guard sees it.
	do(http.MethodPost, "namespaces/public/tables/t", `{"requirements": [], "updates": [{"action": "upgrade-format-version", "format-version": 3},
		{"action": "remove-properties", "removals": ["igloo.clone.shares-files-of"]}]}`)
	if c := g.last(); c.Op != OpCommit || c.FormatVersion != 3 || !slices.Equal(c.Removed, []string{"igloo.clone.shares-files-of"}) {
		t.Errorf("commit: %+v", c)
	}
	do(http.MethodPost, "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["public"], "name": "u"},
		"requirements": [{"type": "assert-create"}], "updates": [{"action": "set-properties", "updates": {"format-version": "3"}}]}]}`)
	if c := g.last(); c.Op != OpCreate || c.Name != "u" || c.FormatVersion != 3 {
		t.Errorf("a transaction that creates a table: %+v", c)
	}
	do(http.MethodPost, "tables/rename", `{"source": {"namespace": ["public"], "name": "t"}, "destination": {"namespace": ["other"], "name": "v"}}`)
	if c := g.last(); c.Op != OpRename || c.Name != "t" || c.To == nil || c.To.Name != "v" || !slices.Equal(c.To.Namespace, []string{"other"}) {
		t.Errorf("rename: %+v", c)
	}
	do(http.MethodPost, "namespaces/public/register", `{"name": "w", "metadata-location": "s3://b/m.json"}`)
	if c := g.last(); c.Op != OpRegister || c.Name != "w" {
		t.Errorf("register: %+v", c)
	}
	if status, body := do(http.MethodPost, "tables/rename", `{"source": "t"}`); status != http.StatusForbidden || !strings.Contains(body, RefusalMark) {
		t.Errorf("a change the proxy cannot read: %d %s", status, body)
	}

	// A drop the Guard has purge reaches the catalog asking for it.
	g.purge = true
	g.mu.Lock()
	g.released = 0
	g.mu.Unlock()
	if status, body := do(http.MethodDelete, "namespaces/public/tables/t", ""); status != http.StatusNoContent {
		t.Fatalf("drop: %d %s", status, body)
	}
	if c := g.last(); c.Op != OpDrop || c.Name != "t" || c.Location != location || c.Purge {
		t.Errorf("drop: %+v (location %s)", c, location)
	}
	if !reachedCatalog("DELETE /catalog/v1/" + icebergtest.Prefix + "/namespaces/public/tables/t?purgeRequested=true") {
		t.Errorf("the drop did not ask to purge: %v", reached)
	}
	g.mu.Lock()
	released := g.released
	g.mu.Unlock()
	if released != 1 {
		t.Errorf("released %d times", released)
	}
	do(http.MethodDelete, "namespaces/public", "")
	if c := g.last(); c.Op != OpDropNamespace || !slices.Equal(c.Namespace, []string{"public"}) {
		t.Errorf("drop namespace: %+v", c)
	}

	// A Guard that cannot tell refuses as the catalog not answering would.
	g.refuse = func(*Change) error { return errors.New("the metadata database is down") }
	if status, body := do(http.MethodPost, "namespaces/public/tables", `{"name": "x"}`); status != http.StatusServiceUnavailable ||
		!strings.Contains(body, "the metadata database is down") {
		t.Errorf("a Guard that cannot tell: %d %s", status, body)
	}
}
