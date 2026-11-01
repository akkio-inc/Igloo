package catalogproxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// heldCommit is a commit adding snapshot id to table t, of records rows, as
// DuckDB makes one when the summary says how many.
func (e *env) heldCommit(endpoint string, id, records int64) (path, body string) {
	change := fmt.Sprintf(`"requirements": [],
		"updates": [
			{"action": "add-snapshot", "snapshot": {"snapshot-id": %d, "sequence-number": 1, "timestamp-ms": %d,
				"manifest-list": "%s/metadata/snap-%d.avro", "summary": {"operation": "append", "added-records": "%d"}, "schema-id": 0}},
			{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": %d}]`,
		id, time.Now().UnixMilli(), e.location, id, records, id)
	if endpoint == "table" {
		return "namespaces/public/tables/t", "{" + change + "}"
	}
	return "transactions/commit", `{"table-changes": [{"identifier": {"namespace": ["public"], "name": "t"}, ` + change + `}]}`
}

// dataPath is the write.data.path a load of table t tells DuckDB.
func (e *env) dataPath(t *testing.T) string {
	t.Helper()
	status, body := e.do(t, http.MethodGet, "namespaces/public/tables/t", "")
	if status != http.StatusOK {
		t.Fatalf("load: %d %s", status, body)
	}
	var load struct {
		Location string `json:"metadata-location"`
		Metadata struct {
			Location   string            `json:"location"`
			Properties map[string]string `json:"properties"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(body), &load); err != nil {
		t.Fatal(err)
	}
	if load.Location == "" || load.Metadata.Location != e.location {
		t.Errorf("a load held lost the table's metadata: %s", body)
	}
	return load.Metadata.Properties["write.data.path"]
}

// TestCommitHold covers a commit kept from the catalog: the table's loads tell
// DuckDB where to write; the commit that follows Arm, of the rows it said, is
// answered as made and not made, again if it is sent again; others are refused
// from Arm on, and pass before it and after the hold.
func TestCommitHold(t *testing.T) {
	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			e := newEnv(t)
			const dir = "/somewhere/igloo-refresh-0123"
			h := e.proxy.HoldCommit([]string{"public"}, "t", dir)
			if h == nil {
				t.Fatal("no hold")
			}
			if e.proxy.HoldCommit([]string{"PUBLIC"}, "T", dir) != nil {
				t.Error("a second hold of the table")
			}
			if got := e.dataPath(t); got != dir {
				t.Errorf("write.data.path %q, want %q", got, dir)
			}

			// Before Arm, another session's commit passes.
			path, body := e.heldCommit(endpoint, 1, 5)
			status, resp := e.do(t, http.MethodPost, path, body)
			wantSuccess(t, endpoint, status, resp)
			if got := e.snapshots(); !slices.Equal(got, []int64{1}) {
				t.Fatalf("snapshots %v after a commit before Arm", got)
			}

			h.Arm(10)
			if _, ok := h.Taken(); ok {
				t.Error("a commit kept before one came")
			}
			// Of other rows, it is refused, as a conflict.
			path, body = e.heldCommit(endpoint, 2, 5)
			status, resp = e.do(t, http.MethodPost, path, body)
			if status != http.StatusConflict || !strings.Contains(resp, "CommitFailedException") {
				t.Errorf("a commit of other rows: %d %s", status, resp)
			}
			// Of the rows said, it is kept: DuckDB hears it was made, and it
			// was not.
			path, body = e.heldCommit(endpoint, 3, 10)
			for range 2 {
				status, resp = e.do(t, http.MethodPost, path, body)
				wantSuccess(t, endpoint, status, resp)
			}
			taken, ok := h.Taken()
			if !ok || taken.SnapshotID != 3 || !strings.HasSuffix(taken.ManifestList, "/metadata/snap-3.avro") {
				t.Errorf("the commit kept: %+v %v", taken, ok)
			}
			if got := e.snapshots(); !slices.Equal(got, []int64{1}) {
				t.Errorf("snapshots %v after the commit kept", got)
			}
			// Another of as many rows is not the commit kept.
			path, body = e.heldCommit(endpoint, 4, 10)
			if status, resp = e.do(t, http.MethodPost, path, body); status != http.StatusConflict {
				t.Errorf("a commit after the one kept: %d %s", status, resp)
			}

			h.Release()
			if got := e.dataPath(t); got != "" {
				t.Errorf("write.data.path %q after the hold", got)
			}
			if status, resp = e.do(t, http.MethodPost, path, body); endpoint == "transaction" && status != http.StatusNoContent ||
				endpoint == "table" && status != http.StatusOK {
				t.Errorf("a commit after the hold: %d %s", status, resp)
			}
			if got := e.snapshots(); !slices.Equal(got, []int64{1, 4}) {
				t.Errorf("snapshots %v after the hold", got)
			}
			if e.proxy.HoldCommit([]string{"public"}, "t", dir) == nil {
				t.Error("no hold after the last was released")
			}
		})
	}
}

// TestCommitHoldOfAnotherTable: a hold is of its table.
func TestCommitHoldOfAnotherTable(t *testing.T) {
	e := newEnv(t)
	h := e.proxy.HoldCommit([]string{"public"}, "other", "/somewhere")
	defer h.Release()
	h.Arm(10)
	if got := e.dataPath(t); got != "" {
		t.Errorf("write.data.path %q", got)
	}
	path, body := e.heldCommit("transaction", 1, 5)
	status, resp := e.do(t, http.MethodPost, path, body)
	wantSuccess(t, "transaction", status, resp)
	if got := e.snapshots(); !slices.Equal(got, []int64{1}) {
		t.Errorf("snapshots %v", got)
	}
}
