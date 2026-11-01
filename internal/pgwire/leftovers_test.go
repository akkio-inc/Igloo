package pgwire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/policy"
)

// TestStoppedCreatesAreCleared stops a CREATE TABLE ... AS in steps part
// way, as killing Igloo does, and runs it again on an Igloo started again:
// the half-filled table it left goes and the create succeeds. A staged
// create that a stopped Igloo never committed, which Lakekeeper keeps
// holding the name, unseen, goes too, as its note proves it was Igloo's;
// one with no note fails the create before it fills anything, and an
// admin's DROP TABLE releases it. The rename into place failed, after the
// whole fill, with "Tabular with the same name already exists".
func TestStoppedCreatesAreCleared(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	faults, err := icebergtest.NewFaults(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(faults.Close)
	// Every client goes through faults, as the catalog's configuration
	// tells iceberg-go's to use the URL it names.
	fake.URL = faults.URL
	// Its catalog proxy gives up on the catalog after a second, not 90.
	a := startRefreshServerOn(t, fake, dir, faults.URL, []engine.Option{engine.CatalogProxy(func(o *catalogproxy.Options) { o.Patience = time.Second })})
	t.Cleanup(faults.Up)
	mustExec(t, a.admin, "CREATE TABLE src AS SELECT range AS n FROM range(1000)")
	staging := func() []string {
		return slices.DeleteFunc(fake.TableNames("public"), func(n string) bool { return !strings.Contains(n, policy.StagingMark) })
	}
	notes := func() []catalog.Pending { return catalog.PendingNotes(fake.NamespaceProperties("public")) }

	// The new table is made and filled, and then the catalog is gone for
	// this Igloo for good, as for one killed: it renames and cleans up
	// nothing.
	faults.Arm(icebergtest.Pass, icebergtest.DownAfterApply)
	err = runQuery(a.admin, "CREATE TABLE copy AS SELECT n FROM src")
	if err == nil {
		t.Fatal("the create went through with the catalog gone")
	}
	if left, n := staging(), notes(); len(left) != 1 || len(n) != 1 || n[0].Kind != catalog.PendingFill || n[0].Target != "copy" {
		t.Fatalf("a stopped create left tables %v and notes %+v", left, n)
	}

	faults.Up()
	catalog.Restarted()
	b := startRefreshServerOn(t, fake, dir, fake.URL, nil)
	mustExec(t, b.admin, "CREATE TABLE copy AS SELECT n FROM src")
	if n := count(t, b.admin, "SELECT count(*) FROM copy"); n != 1000 {
		t.Errorf("copy: %d rows, want 1000", n)
	}
	if left, n := staging(), notes(); len(left) != 0 || len(n) != 0 {
		t.Errorf("after the create ran again: tables %v and notes %+v", left, n)
	}

	// A staged create a stopped Igloo noted and never committed.
	stageCreate(t, fake, "held")
	host, _ := os.Hostname()
	stopped := catalog.Pending{Key: "igloo.pending.0123456789abcdef", Kind: catalog.PendingStaged, Name: "held", Host: host,
		Process: "stopped", At: time.Now().UTC()}
	setNamespaceProperty(t, fake, stopped.Key, stopped.Value())
	mustExec(t, b.admin, "CREATE TABLE held AS SELECT n FROM src WHERE n < 10")
	if n := count(t, b.admin, "SELECT count(*) FROM held"); n != 10 || fake.Staged("public", "held") || len(notes()) != 0 {
		t.Errorf("after a create over a stopped Igloo's hold: %d rows, held %v, notes %+v", n, fake.Staged("public", "held"), notes())
	}

	// One nobody noted: another engine's, or an older Igloo's.
	stageCreate(t, fake, "taken")
	start := time.Now()
	err = runQuery(b.admin, "CREATE TABLE taken AS SELECT n FROM src")
	if err == nil || !strings.Contains(err.Error(), "never was") || len(staging()) != 0 {
		t.Fatalf("a create over a hold nobody noted: %v, tables %v", err, staging())
	}
	t.Logf("refused in %s, before filling anything", time.Since(start).Round(time.Millisecond))
	mustExec(t, b.admin, "DROP TABLE taken")
	if fake.Staged("public", "taken") {
		t.Fatal("DROP TABLE left the hold")
	}
	mustExec(t, b.admin, "CREATE TABLE taken AS SELECT n FROM src WHERE n < 5")
	if n := count(t, b.admin, "SELECT count(*) FROM taken"); n != 5 {
		t.Errorf("taken: %d rows, want 5", n)
	}
	if err := runQuery(b.admin, "DROP TABLE never_made"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("DROP TABLE of a name nothing holds: %v", err)
	}
}

// stageCreate sends the catalog fake a staged create of public.name, as
// DuckDB does before it makes a table, and no commit.
func stageCreate(t *testing.T, fake *icebergtest.Catalog, name string) {
	t.Helper()
	body := `{"name": "` + name + `", "stage-create": true, "schema": {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "n", "required": false, "type": "long"}]}}`
	post(t, fake.URL+"/v1/"+icebergtest.Prefix+"/namespaces/public/tables", body)
	if !fake.Staged("public", name) {
		t.Fatalf("no staged create of %s", name)
	}
}

// setNamespaceProperty sets a property of the namespace public.
func setNamespaceProperty(t *testing.T, fake *icebergtest.Catalog, key, value string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"updates": map[string]string{key: value}, "removals": []string{}})
	post(t, fake.URL+"/v1/"+icebergtest.Prefix+"/namespaces/public/properties", string(b))
}

func post(t *testing.T, url, body string) {
	t.Helper()
	res, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: %s", url, res.Status)
	}
}
