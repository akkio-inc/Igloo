package catalogproxy

import (
	"encoding/json"
	"strconv"
	"strings"
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
			[]map[string]string{{"format-version": "2", targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}}},
		{"a create without properties", create, `{"name":"t"}`,
			[]map[string]string{{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}}},
		{"a create that sets the size", create, `{"name":"t","properties":{"write.target-file-size-bytes":"1048576"}}`,
			[]map[string]string{{targetFileSizeProperty: "1048576", rowGroupsProperty: rowGroupsAuto}}},
		{"a create that sets both", create, `{"name":"t","properties":{"write.target-file-size-bytes":"1048576","igloo.row-groups":"auto:1000000"}}`, nil},
		{"the commit of a staged create", commit, staged,
			[]map[string]string{{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}}},
		{"a create's commit without set-properties", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"u"}]}`,
			[]map[string]string{{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}}},
		{"a create's commit that sets the size", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"set-properties","updates":{"write.target-file-size-bytes":"8MB"}}]}`,
			[]map[string]string{{targetFileSizeProperty: "8MB", rowGroupsProperty: rowGroupsAuto}}},
		{"a commit to a table that exists", commit, append_, nil},
		{"a transaction with a create", tx, `{"table-changes":[` + append_ + `,` + staged + `]}`,
			[]map[string]string{nil, {targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}}},
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

// TestFirstRowGroups checks the row groups a new table starts with, as the
// requests that create one spell its schema and partition spec.
func TestFirstRowGroups(t *testing.T) {
	const (
		create = "v1/wh/namespaces/public/tables"
		commit = "v1/wh/namespaces/public/tables/t"
	)
	field := func(id int, typ string) string {
		return `{"id":` + strconv.Itoa(id) + `,"name":"c` + strconv.Itoa(id) + `","required":false,"type":"` + typ + `"}`
	}
	schema := func(types ...string) string {
		var fields []string
		for i, typ := range types {
			fields = append(fields, field(i+1, typ))
		}
		return `{"type":"struct","schema-id":0,"fields":[` + strings.Join(fields, ",") + `]}`
	}
	strs := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "string"
		}
		return out
	}
	narrow := schema("long", "string", "date")
	for _, c := range []struct {
		name, rest, body string
		props            map[string]string
	}{
		{"narrow rows, flat", create, `{"name":"t","schema":` + narrow + `}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupSizeProperty: "524288", rowGroupsProperty: "auto:524288"}},
		{"an empty spec", create, `{"name":"t","schema":` + narrow + `,"partition-spec":{"spec-id":0,"fields":[]}}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupSizeProperty: "524288", rowGroupsProperty: "auto:524288"}},
		// 20 strings take 640 bytes: 128 MiB of them.
		{"wide rows, fewer", create, `{"name":"t","schema":` + schema(strs(20)...) + `}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupSizeProperty: "209715", rowGroupsProperty: "auto:209715"}},
		// 40 strings would get fewer rows than DuckDB's own.
		{"very wide rows, DuckDB's", create, `{"name":"t","schema":` + schema(strs(40)...) + `}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}},
		{"partitioned, DuckDB's", create,
			`{"name":"t","schema":` + narrow + `,"partition-spec":{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"name":"k","transform":"identity"}]}}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}},
		{"the statement's size stays, and is not Igloo's", create,
			`{"name":"t","schema":` + narrow + `,"properties":{"write.parquet.row-group-size":"1000"}}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupSizeProperty: "1000", rowGroupsProperty: rowGroupsAuto}},
		{"a schema that cannot be read", create, `{"name":"t","schema":{"fields":7}}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}},
		{"the commit of a staged create", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"u"},` +
				`{"action":"add-schema","schema":` + narrow + `},{"action":"add-spec","spec":{"spec-id":0,"fields":[]}},` +
				`{"action":"set-properties","updates":{}}]}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupSizeProperty: "524288", rowGroupsProperty: "auto:524288"}},
		{"the commit of a partitioned create", commit,
			`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"add-schema","schema":` + narrow + `},` +
				`{"action":"add-spec","spec":{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"name":"k","transform":"identity"}]}},` +
				`{"action":"set-properties","updates":{}}]}`,
			map[string]string{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := withDefaults(c.rest, []byte(c.body))
			if got := propertiesOf(t, c.rest, out); !equalProps(got, []map[string]string{c.props}) {
				t.Fatalf("properties %v, want %v, in %s", got, c.props, out)
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
