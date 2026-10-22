package catalogproxy

import (
	"encoding/json"
	"testing"
)

func TestWithDefaults(t *testing.T) {
	const (
		create = "v1/wh/namespaces/public/tables"
		commit = "v1/wh/namespaces/public/tables/t"
		tx     = "v1/wh/transactions/commit"
		staged = `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"u"},` +
			`{"action":"set-properties","updates":{}},{"action":"set-current-schema","schema-id":0}]}`
		append_ = `{"identifier":{"namespace":["public"],"name":"t"},"requirements":[{"type":"assert-table-uuid","uuid":"u"}],` +
			`"updates":[{"action":"add-snapshot","snapshot":{}}]}`
	)
	for _, c := range []struct {
		name, rest, body string
		// want is each table change's properties after, nil for a body
		// that must come back unchanged.
		want []map[string]string
	}{
		{"a staged create", create, `{"stage-create":true,"name":"t","properties":{"format-version":"2"}}`,
			[]map[string]string{{"format-version": "2", targetFileSizeProperty: targetFileSize}}},
		{"a create without properties", create, `{"name":"t"}`,
			[]map[string]string{{targetFileSizeProperty: targetFileSize}}},
		{"a create that sets the size", create, `{"name":"t","properties":{"write.target-file-size-bytes":"1048576"}}`, nil},
		{"the commit of a staged create", commit, staged,
			[]map[string]string{{targetFileSizeProperty: targetFileSize}}},
		{"a create's commit without set-properties", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"u"}]}`,
			[]map[string]string{{targetFileSizeProperty: targetFileSize}}},
		{"a create's commit that sets the size", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"set-properties","updates":{"write.target-file-size-bytes":"8MB"}}]}`, nil},
		{"a commit to a table that exists", commit, append_, nil},
		{"a transaction with a create", tx, `{"table-changes":[` + append_ + `,` + staged + `]}`,
			[]map[string]string{nil, {targetFileSizeProperty: targetFileSize}}},
		{"a transaction without one", tx, `{"table-changes":[` + append_ + `]}`, nil},
		{"a rename", "v1/wh/tables/rename", `{"source":{"namespace":["public"],"name":"a"},"destination":{"namespace":["public"],"name":"b"}}`, nil},
		{"a body that is not JSON", create, `{"name":`, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := withDefaults(c.rest, []byte(c.body))
			if c.want == nil {
				if string(out) != c.body {
					t.Fatalf("changed:\n%s\nto\n%s", c.body, out)
				}
				return
			}
			if got := propertiesOf(t, c.rest, out); !equalProps(got, c.want) {
				t.Fatalf("properties %v, want %v, in %s", got, c.want, out)
			}
			// Sent again, as DuckDB does when it gives up waiting, the
			// change must be the same change.
			if again := withDefaults(c.rest, []byte(c.body)); string(again) != string(out) {
				t.Fatalf("not the same twice:\n%s\n%s", out, again)
			}
			if string(withDefaults(c.rest, out)) != string(out) {
				t.Fatalf("defaults added twice to %s", out)
			}
		})
	}
}

// propertiesOf reads the properties a create request sets, or those each
// table change of a commit sets with set-properties (nil for none).
func propertiesOf(t *testing.T, rest string, body []byte) []map[string]string {
	t.Helper()
	if createTablePath.MatchString(rest) {
		var req struct {
			Properties map[string]string `json:"properties"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatal(err)
		}
		return []map[string]string{req.Properties}
	}
	type change struct {
		Updates []struct {
			Action  string            `json:"action"`
			Updates map[string]string `json:"updates"`
		} `json:"updates"`
	}
	var changes []change
	var tx struct {
		TableChanges []change `json:"table-changes"`
	}
	if err := json.Unmarshal(body, &tx); err == nil && tx.TableChanges != nil {
		changes = tx.TableChanges
	} else {
		var ch change
		if err := json.Unmarshal(body, &ch); err != nil {
			t.Fatal(err)
		}
		changes = []change{ch}
	}
	var out []map[string]string
	for _, ch := range changes {
		var props map[string]string
		for _, u := range ch.Updates {
			if u.Action == "set-properties" {
				props = u.Updates
			}
		}
		out = append(out, props)
	}
	return out
}

func equalProps(a, b []map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for k, v := range a[i] {
			if b[i][k] != v {
				return false
			}
		}
	}
	return true
}
