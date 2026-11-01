package catalogproxy

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// TestStagedCreatesAreNoted checks that the proxy notes each staged create
// in its namespace's properties and forgets the note once a commit makes
// the table, so that a note left behind is one of a create that never was:
// one whose commit the catalog refused, as when Igloo is stopped.
func TestStagedCreatesAreNoted(t *testing.T) {
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	p, err := New(Options{Catalog: fake.URL, AttemptTimeout: 10 * time.Second, Patience: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	do := func(method, path, body string) int {
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
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	notes := func() []catalog.Pending { return catalog.PendingNotes(fake.NamespaceProperties("sales")) }
	schema := `"schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "n", "required": false, "type": "long"}]}`
	// As DuckDB commits a staged create: the table's metadata in full.
	commit := func(name, require string) int {
		updates := `{"action": "assign-uuid", "uuid": "8f1f8e0e-5d7b-4a35-9f7a-0d6c7c1b2a3e"}, {"action": "upgrade-format-version", "format-version": 2}, ` +
			`{"action": "add-schema", "last-column-id": 1, ` + schema + `}, {"action": "set-current-schema", "schema-id": -1}, ` +
			`{"action": "add-spec", "spec": {"spec-id": 0, "fields": []}}, {"action": "set-default-spec", "spec-id": -1}, ` +
			`{"action": "add-sort-order", "sort-order": {"order-id": 0, "fields": []}}, {"action": "set-default-sort-order", "sort-order-id": -1}, ` +
			`{"action": "set-location", "location": "` + dir + `/` + name + `"}`
		return do(http.MethodPost, "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["sales"], "name": "`+name+`"}, `+
			`"requirements": [`+require+`], "updates": [`+updates+`]}]}`)
	}

	if code := do(http.MethodPost, "namespaces", `{"namespace": ["sales"]}`); code != http.StatusOK {
		t.Fatalf("namespace: %d", code)
	}
	if code := do(http.MethodPost, "namespaces/sales/tables", `{"name": "orders", "stage-create": true, `+schema+`}`); code != http.StatusOK {
		t.Fatalf("staged create: %d", code)
	}
	if n := notes(); len(n) != 1 || n[0].Kind != catalog.PendingStaged || n[0].Name != "orders" || !n[0].Mine() {
		t.Fatalf("after a staged create: %+v", n)
	}
	if code := commit("orders", `{"type": "assert-create"}`); code >= 300 {
		t.Fatalf("commit: %d", code)
	}
	if n := notes(); len(n) != 0 {
		t.Fatalf("after the commit that made the table: %+v", n)
	}

	// A create whose commit the catalog refuses keeps its note.
	do(http.MethodPost, "namespaces/sales/tables", `{"name": "returns", "stage-create": true, `+schema+`}`)
	if code := commit("returns", `{"type": "assert-create"}, {"type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000000"}`); code < 300 {
		t.Fatalf("a commit that should fail: %d", code)
	}
	if n := notes(); len(n) != 1 || n[0].Name != "returns" {
		t.Fatalf("after a refused commit: %+v", n)
	}
	// A plain create is not noted.
	do(http.MethodPost, "namespaces/sales/tables", `{"name": "carts", `+schema+`}`)
	if n := notes(); len(n) != 1 {
		t.Fatalf("after a plain create: %+v", n)
	}
}
