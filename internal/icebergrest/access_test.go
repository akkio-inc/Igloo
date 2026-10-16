package icebergrest

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestClassify(t *testing.T) {
	const (
		ins = auth.PrivilegeInsert
		upd = auth.PrivilegeUpdate
		del = auth.PrivilegeDelete
	)
	cases := []struct {
		name   string
		commit string
		table  []auth.Privilege
		create bool
		writer bool
		reject []string
	}{
		{name: "append", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":5,"summary":{"operation":"append"}}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":5}]}`},
		{name: "delete", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":5,"summary":{"operation":"delete"}}},
				{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":5}]}`},
		{name: "overwrite", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":5,"summary":{"operation":"overwrite"}}}]}`},
		{name: "compaction", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":5,"summary":{"operation":"replace"}}}]}`},
		{name: "no operation", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":5}}]}`},
		{name: "rollback", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":2}]}`},
		{name: "statistics", table: []auth.Privilege{ins, upd, del},
			commit: `{"updates":[{"action":"set-statistics","statistics":{}}]}`},
		{name: "schema change", create: true,
			commit: `{"updates":[{"action":"add-schema","schema":{}},{"action":"set-current-schema","schema-id":-1}]}`},
		{name: "properties", writer: true,
			commit: `{"updates":[{"action":"set-properties","updates":{"a":"b"}}]}`},
		{name: "snapshot expiry", writer: true,
			commit: `{"updates":[{"action":"remove-snapshots","snapshot-ids":[1,2]}]}`},
		// A new table is its creator's to fill and describe, as with CREATE
		// TABLE AS, and restates its UUID and location.
		{name: "staged create", create: true,
			commit: `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"u"},
				{"action":"set-location","location":"s3://b/wh/u"},
				{"action":"add-snapshot","snapshot":{"snapshot-id":5,"summary":{"operation":"overwrite"}}},
				{"action":"set-properties","updates":{"a":"b"}}]}`},
		// Its other updates are classified as in any commit.
		{name: "staged create with unknown and writer updates", create: true, writer: true,
			reject: []string{"add-encryption-key"},
			commit: `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"remove-snapshots","snapshot-ids":[1]},
				{"action":"add-encryption-key"}]}`},
		// Moving a table, or changing its UUID, could point it at another
		// table's files.
		{name: "move", reject: []string{"set-location"},
			commit: `{"updates":[{"action":"set-location","location":"s3://b/wh/other"}]}`},
		{name: "new UUID", reject: []string{"assign-uuid"},
			commit: `{"updates":[{"action":"assign-uuid","uuid":"other"}]}`},
		{name: "unknown", reject: []string{"add-encryption-key"},
			commit: `{"updates":[{"action":"add-encryption-key"}]}`},
	}
	for _, c := range cases {
		var req commitRequest
		if err := json.Unmarshal([]byte(c.commit), &req); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		n := classify(req)
		slices.Sort(n.table)
		slices.Sort(c.table)
		if !slices.Equal(n.table, c.table) || n.create != c.create || n.writer != c.writer || !slices.Equal(n.admin, c.reject) {
			t.Errorf("%s: got %+v", c.name, n)
		}
	}
}

func TestWithoutUpdates(t *testing.T) {
	body := []byte(`{"requirements":[{"type":"assert-create"}],"updates":[{"action":"assign-uuid","uuid":"x"},` +
		`{"action":"add-snapshot","snapshot":{"snapshot-id":9007199254740993}},{"action":"set-location","location":"s3://b/<v>"}]}`)
	got, err := withoutUpdates(body, stagedUpdates...)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"requirements":[{"type":"assert-create"}],"updates":[{"action":"add-snapshot","snapshot":{"snapshot-id":9007199254740993}}]}` + "\n"
	if string(got) != want {
		t.Errorf("got %s", got)
	}
	if got, err := withoutUpdates([]byte(`{"requirements":[]}`), stagedUpdates...); err != nil || string(got) != `{"requirements":[]}` {
		t.Errorf("without updates: %s %v", got, err)
	}
}

func TestNames(t *testing.T) {
	for _, c := range []struct {
		name         string
		table, level bool
	}{
		{"events", true, true},
		{"a b", true, true},
		{"a.b", true, false},
		{".", false, false},
		{"..", false, false},
		{"", false, false},
		{"a\x1fb", false, false},
		{"a\nb", false, false},
	} {
		if validName(c.name) != c.table || validLevel(c.name) != c.level {
			t.Errorf("%q: table %v, level %v", c.name, validName(c.name), validLevel(c.name))
		}
	}
	if got := pathName("a+b%20c"); got != "a b%20c" {
		t.Errorf("pathName: %q", got)
	}
}

func TestCallerPrivileges(t *testing.T) {
	events := auth.Object{Schema: "public", Table: "events"}
	none := &caller{user: auth.User{Name: "nora", Role: auth.RoleNone}, privs: &auth.Privileges{Grants: []auth.Grant{
		{Privilege: auth.PrivilegeSelect, Object: auth.Object{Schema: "PUBLIC", Table: "Events"}},
		{Privilege: auth.PrivilegeInsert, Object: auth.Object{Schema: "public"}},
		{Privilege: auth.PrivilegeCreate, Object: auth.Object{Schema: "team"}},
	}}}
	reader := &caller{user: auth.User{Name: "rita", Role: auth.RoleReader}, privs: &auth.Privileges{}}
	writer := &caller{user: auth.User{Name: "walt", Role: auth.RoleWriter}}

	for _, c := range []struct {
		name string
		got  bool
		want bool
	}{
		{"granted SELECT, any case", none.may(auth.PrivilegeSelect, events), true},
		{"INSERT through the schema", none.may(auth.PrivilegeInsert, auth.Object{Schema: "public", Table: "other"}), true},
		{"no UPDATE", none.may(auth.PrivilegeUpdate, events), false},
		{"no write credentials without all three", none.writesData(events), false},
		{"sees a table in a schema it may create in", none.sees(auth.Object{Schema: "team", Table: "x"}), true},
		{"sees a table it may insert into", none.sees(auth.Object{Schema: "public", Table: "other"}), true},
		{"not a schema without grants", none.seesSchema("secret"), false},
		{"reader reads", reader.may(auth.PrivilegeSelect, events), true},
		{"reader does not write", reader.may(auth.PrivilegeInsert, events), false},
		{"reader sees every schema", reader.seesSchema("secret"), true},
		{"writer writes", writer.writesData(events), true},
		{"writer creates", writer.may(auth.PrivilegeCreate, auth.Object{Schema: "any"}), true},
	} {
		if c.got != c.want {
			t.Errorf("%s: got %v", c.name, c.got)
		}
	}
}
