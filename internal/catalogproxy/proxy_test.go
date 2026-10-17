package catalogproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"

	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// env is a proxy in front of icebergtest's catalog, with faults injected
// between them.
type env struct {
	fake   *icebergtest.Catalog
	faults *icebergtest.Faults
	proxy  *Proxy
	dir    string
	// location is table t's.
	location string
}

func newEnv(t *testing.T, configure ...func(*Options)) *env {
	t.Helper()
	e := &env{dir: t.TempDir()}
	e.fake = icebergtest.NewCatalog(e.dir)
	t.Cleanup(e.fake.Close)
	var err error
	if e.faults, err = icebergtest.NewFaults(e.fake.URL); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.faults.Close)
	opts := Options{
		Catalog:        e.faults.URL,
		Files:          func(ctx context.Context) (iceio.IO, error) { return iceio.LoadFS(ctx, nil, e.dir) },
		AttemptTimeout: 10 * time.Second,
		Patience:       30 * time.Second,
	}
	for _, f := range configure {
		f(&opts)
	}
	if e.proxy, err = New(opts); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.proxy.Close() })

	if status, body := e.do(t, http.MethodPost, "namespaces", `{"namespace": ["public"]}`); status != http.StatusOK {
		t.Fatalf("create namespace: %d %s", status, body)
	}
	status, body := e.do(t, http.MethodPost, "namespaces/public/tables", `{"name": "t", "schema":
		{"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "n", "required": false, "type": "int"}]}}`)
	if status != http.StatusOK {
		t.Fatalf("create table: %d %s", status, body)
	}
	var load struct {
		Metadata struct {
			Location string `json:"location"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(body), &load); err != nil {
		t.Fatal(err)
	}
	e.location = load.Metadata.Location
	return e
}

// do sends a request through the proxy to a path under the warehouse.
func (e *env) do(t *testing.T, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, e.proxy.URL()+"/v1/"+icebergtest.Prefix+"/"+path, strings.NewReader(body))
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

// endpoints a commit can be posted to.
var endpoints = []string{"transaction", "table"}

// appendCommit is a commit adding snapshot id to table t, whose current
// snapshot must be parent (0 for none), as DuckDB makes one.
func (e *env) appendCommit(endpoint string, id, parent int64) (path, body string) {
	parentRequirement, parentField, seq := "null", "", 1
	if parent != 0 {
		parentRequirement, parentField, seq = fmt.Sprint(parent), fmt.Sprintf(`"parent-snapshot-id": %d, `, parent), 2
	}
	change := fmt.Sprintf(`"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": %s}],
		"updates": [
			{"action": "add-snapshot", "snapshot": {"snapshot-id": %d, %s"sequence-number": %d, "timestamp-ms": %d,
				"manifest-list": "%s/metadata/snap-%d.avro", "summary": {"operation": "append"}, "schema-id": 0}},
			{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": %d}]`,
		parentRequirement, id, parentField, seq, time.Now().UnixMilli(), e.location, id, id)
	if endpoint == "table" {
		return "namespaces/public/tables/t", "{" + change + "}"
	}
	return "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["public"], "name": "t"}, ` + change + `}]}`
}

// snapshots lists table t's snapshots.
func (e *env) snapshots() []int64 {
	var ids []int64
	if m := e.fake.Metadata("public", "t"); m != nil {
		for _, s := range m.Snapshots() {
			ids = append(ids, s.SnapshotID)
		}
	}
	return ids
}

func wantSuccess(t *testing.T, endpoint string, status int, body string) {
	t.Helper()
	switch {
	case endpoint == "transaction" && status != http.StatusNoContent:
		t.Errorf("status %d (%s), want 204", status, body)
	case endpoint == "table" && (status != http.StatusOK || !strings.Contains(body, `"metadata-location"`)):
		t.Errorf("status %d (%s), want 200 with the table's metadata", status, body)
	}
}

// TestCommitAppliedButNotConfirmed covers a commit the catalog applied but
// did not confirm: it answered 503, the connection dropped, or no answer
// came in time. The commit lands once and DuckDB hears it did, whether
// the catalog honours idempotency keys or the proxy finds the commit's
// snapshot in the table.
func TestCommitAppliedButNotConfirmed(t *testing.T) {
	faults := map[string]icebergtest.Fault{
		"503":     icebergtest.FailAfterApply,
		"dropped": icebergtest.DropAfterApply,
		"timeout": icebergtest.HangAfterApply,
	}
	for name, fault := range faults {
		for _, keys := range []bool{true, false} {
			for _, endpoint := range endpoints {
				t.Run(fmt.Sprintf("%s/keys=%t/%s", name, keys, endpoint), func(t *testing.T) {
					e := newEnv(t, func(o *Options) {
						if fault == icebergtest.HangAfterApply {
							o.AttemptTimeout = time.Second
						}
					})
					if !keys {
						e.fake.IgnoreIdempotencyKeys()
					}
					e.faults.Arm(fault)
					path, body := e.appendCommit(endpoint, 1001, 0)
					status, resp := e.do(t, http.MethodPost, path, body)
					wantSuccess(t, endpoint, status, resp)
					if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
						t.Errorf("snapshots %v, want [1001]", got)
					}
					if got := e.fake.Commits(); got != 1 {
						t.Errorf("%d commits applied, want 1", got)
					}
					if keys := e.faults.Keys(); len(keys) < 2 || !oneKey(keys) {
						t.Errorf("idempotency keys %q, want the same on every attempt", keys)
					}
				})
			}
		}
	}
}

// TestCommitNotAppliedIsSentOnceMore covers a commit the catalog answered
// 503 without applying it.
func TestCommitNotAppliedIsSentOnceMore(t *testing.T) {
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			e := newEnv(t)
			e.faults.Arm(icebergtest.FailBeforeApply)
			path, body := e.appendCommit(endpoint, 1001, 0)
			status, resp := e.do(t, http.MethodPost, path, body)
			wantSuccess(t, endpoint, status, resp)
			if got := len(e.faults.Commits()); got != 2 {
				t.Errorf("sent %d times, want 2", got)
			}
			if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
				t.Errorf("snapshots %v, want [1001]", got)
			}
		})
	}
}

// TestConflictIsPassedOn checks that a commit refused because another
// writer committed first reaches DuckDB as the catalog said it, after one
// attempt, so that Igloo runs the statement again.
func TestConflictIsPassedOn(t *testing.T) {
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			e := newEnv(t)
			path, body := e.appendCommit(endpoint, 1001, 0)
			if status, resp := e.do(t, http.MethodPost, path, body); status >= 300 {
				t.Fatalf("first commit: %d %s", status, resp)
			}
			path, body = e.appendCommit(endpoint, 1002, 0)
			status, resp := e.do(t, http.MethodPost, path, body)
			if status != http.StatusConflict || !strings.Contains(resp, "CatalogCommitConflicts") {
				t.Errorf("status %d (%s), want 409 CatalogCommitConflicts", status, resp)
			}
			if got := len(e.faults.Commits()); got != 2 {
				t.Errorf("%d commits sent, want 2", got)
			}
			if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
				t.Errorf("snapshots %v, want [1001]", got)
			}
		})
	}
}

// TestConflictAfterUnknownOutcome covers a commit whose first attempt the
// proxy heard nothing certain about and whose second was refused, because
// another writer committed in between: the commit's snapshot is not in the
// table, so the refusal stands.
func TestConflictAfterUnknownOutcome(t *testing.T) {
	for _, keys := range []bool{true, false} {
		t.Run(fmt.Sprintf("keys=%t", keys), func(t *testing.T) {
			e := newEnv(t)
			if !keys {
				e.fake.IgnoreIdempotencyKeys()
			}
			e.faults.Arm(icebergtest.FailBeforeApply)
			e.faults.Before(func() {
				path, body := e.appendCommit("transaction", 1001, 0)
				if status, resp := e.do(t, http.MethodPost, path, body); status >= 300 {
					t.Errorf("the other writer's commit: %d %s", status, resp)
				}
			})
			path, body := e.appendCommit("transaction", 1002, 0)
			status, resp := e.do(t, http.MethodPost, path, body)
			if status != http.StatusConflict {
				t.Errorf("status %d (%s), want 409", status, resp)
			}
			if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
				t.Errorf("snapshots %v, want [1001]", got)
			}
		})
	}
}

// TestRepeatsAreJoined checks that DuckDB sending a change again, having
// given up waiting for the first, does not reach the catalog: it gets the
// first's outcome.
func TestRepeatsAreJoined(t *testing.T) {
	e := newEnv(t, func(o *Options) { o.AttemptTimeout = time.Second })
	e.faults.Arm(icebergtest.HangAfterApply)
	path, body := e.appendCommit("transaction", 1001, 0)

	var wg sync.WaitGroup
	statuses := make([]int, 2)
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statuses[i], _ = e.do(t, http.MethodPost, path, body)
		}()
		if i == 0 {
			waitFor(t, func() bool { return len(e.faults.Commits()) == 1 })
		}
	}
	wg.Wait()
	if !slices.Equal(statuses, []int{204, 204}) {
		t.Errorf("statuses %v, want both 204", statuses)
	}
	// Every attempt was the proxy's, at the one change: a repeat that
	// reached the catalog would have had a key of its own.
	sent := e.faults.Keys()
	if len(sent) < 2 || !oneKey(sent) {
		t.Errorf("idempotency keys %q, want the same on every attempt", sent)
	}

	// A repeat that comes after the outcome gets it too, without reaching
	// the catalog.
	if status, resp := e.do(t, http.MethodPost, path, body); status != http.StatusNoContent {
		t.Errorf("late repeat: %d %s", status, resp)
	}
	if got := len(e.faults.Keys()); got != len(sent) {
		t.Errorf("a late repeat was sent to the catalog")
	}
	if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
		t.Errorf("snapshots %v, want [1001]", got)
	}
	if got := e.fake.Commits(); got != 1 {
		t.Errorf("%d commits applied, want 1", got)
	}
}

// oneKey reports whether keys are all the same, and not empty.
func oneKey(keys []string) bool {
	for _, k := range keys {
		if k == "" || k != keys[0] {
			return false
		}
	}
	return true
}

// TestOtherChangesCanBeMadeAgain checks that a change that can be made
// on purpose more than once, unlike a commit adding a snapshot, is sent
// to the catalog each time.
func TestOtherChangesCanBeMadeAgain(t *testing.T) {
	e := newEnv(t)
	for i := range 2 {
		if status, resp := e.do(t, http.MethodPost, "namespaces", `{"namespace": ["again"]}`); status != http.StatusOK {
			t.Fatalf("create %d: %d %s", i, status, resp)
		}
		if status, resp := e.do(t, http.MethodDelete, "namespaces/again", ""); status != http.StatusNoContent {
			t.Fatalf("drop %d: %d %s", i, status, resp)
		}
	}
}

// TestUnknownOutcome checks a change whose outcome the proxy cannot learn
// in time: DuckDB gets a 502 CommitStateUnknownException, which it does
// not send again, and never a 5xx it would.
func TestUnknownOutcome(t *testing.T) {
	e := newEnv(t, func(o *Options) {
		o.AttemptTimeout = 200 * time.Millisecond
		o.Patience = time.Second
	})
	e.faults.Arm(icebergtest.DownAfterApply)
	path, body := e.appendCommit("transaction", 1001, 0)
	status, resp := e.do(t, http.MethodPost, path, body)
	if status != http.StatusBadGateway || !strings.Contains(resp, "CommitStateUnknownException") {
		t.Errorf("status %d (%s), want 502 CommitStateUnknownException", status, resp)
	}
	e.faults.Up()
	if got := e.snapshots(); !slices.Equal(got, []int64{1001}) {
		t.Errorf("snapshots %v, want [1001]", got)
	}

	// A commit that was never applied, while the catalog keeps failing.
	e.faults.Arm(slices.Repeat([]icebergtest.Fault{icebergtest.FailBeforeApply}, 1000)...)
	path, body = e.appendCommit("transaction", 1002, 1001)
	if status, resp := e.do(t, http.MethodPost, path, body); status != http.StatusBadGateway {
		t.Errorf("status %d (%s), want 502", status, resp)
	}
}

// TestReads checks that reads pass through, and that DuckDB is told to
// try one again when the catalog cannot be reached.
func TestReads(t *testing.T) {
	e := newEnv(t)
	if status, resp := e.do(t, http.MethodGet, "namespaces/public/tables/t", ""); status != http.StatusOK || !strings.Contains(resp, e.location) {
		t.Errorf("load table: %d %s", status, resp)
	}
	if status, _ := e.do(t, http.MethodGet, "namespaces/public/tables/missing", ""); status != http.StatusNotFound {
		t.Errorf("load missing table: %d, want 404", status)
	}
	e.faults.Close()
	if status, _ := e.do(t, http.MethodGet, "namespaces/public/tables/t", ""); status != http.StatusServiceUnavailable {
		t.Errorf("load table from a catalog that is down: %d, want 503", status)
	}
}

// TestOnlyItsPath checks that the proxy serves nothing outside its random
// path.
func TestOnlyItsPath(t *testing.T) {
	e := newEnv(t)
	base := strings.TrimSuffix(e.proxy.URL(), "/"+e.proxy.secret)
	for _, path := range []string{"/v1/config", "/" + e.proxy.secret + "x/v1/config", "/"} {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: %d, want 404", path, resp.StatusCode)
		}
	}
}

// TestAddedFiles checks which files of a refused commit are deleted: those
// its snapshot added under the table's location, and none that earlier
// snapshots wrote or that the snapshot only marks as deleted or existing.
func TestAddedFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	loc := filepath.Join(dir, "t")
	fs, err := iceio.LoadFS(ctx, nil, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(loc, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "n", Type: iceberg.PrimitiveTypes.Int32})
	const oldSnap, newSnap = int64(1), int64(2)

	entry := func(status iceberg.ManifestEntryStatus, snap int64, path string) iceberg.ManifestEntry {
		b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData, path,
			iceberg.ParquetFile, nil, nil, nil, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		seq := int64(1)
		return iceberg.NewManifestEntry(status, &snap, &seq, &seq, b.Build())
	}
	manifest := func(path string, snap int64, entries ...iceberg.ManifestEntry) iceberg.ManifestFile {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() //nolint:errcheck
		m, err := iceberg.WriteManifest(path, f, 2, *iceberg.UnpartitionedSpec, schema, snap, entries)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	// manifestList writes a snapshot's manifest list and returns its
	// manifests as read back, with their sequence numbers.
	manifestList := func(path string, snap, seq int64, parent *int64, manifests ...iceberg.ManifestFile) []iceberg.ManifestFile {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := iceberg.WriteManifestList(2, f, snap, parent, &seq, 0, manifests); err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
		r, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close() //nolint:errcheck
		read, err := iceberg.ReadManifestList(r)
		if err != nil {
			t.Fatal(err)
		}
		return read
	}

	// The table's current snapshot, whose manifest the new manifest list
	// names too.
	earlier := manifestList(loc+"/metadata/snap-1.avro", oldSnap, 1, nil,
		manifest(loc+"/metadata/old-m0.avro", oldSnap, entry(iceberg.EntryStatusADDED, oldSnap, loc+"/data/old.parquet")))
	// The refused snapshot's manifest: a file it adds, one it deletes, one
	// it keeps and one outside the table's location.
	added := manifest(loc+"/metadata/new-m0.avro", newSnap,
		entry(iceberg.EntryStatusADDED, newSnap, loc+"/data/new.parquet"),
		entry(iceberg.EntryStatusDELETED, newSnap, loc+"/data/deleted.parquet"),
		entry(iceberg.EntryStatusEXISTING, oldSnap, loc+"/data/kept.parquet"),
		entry(iceberg.EntryStatusADDED, newSnap, dir+"/elsewhere/other.parquet"))
	list := loc + "/metadata/snap-2.avro"
	parent := oldSnap
	manifestList(list, newSnap, 2, &parent, append(earlier, added)...)

	got, err := addedFiles(fs, loc, snapshot{id: newSnap, manifestList: list})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{loc + "/data/new.parquet", loc + "/metadata/new-m0.avro", list}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("got  %q\nwant %q", got, want)
	}

	if _, err := addedFiles(fs, dir+"/other-table", snapshot{id: newSnap, manifestList: list}); err == nil {
		t.Error("a manifest list outside the table's location was read")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
