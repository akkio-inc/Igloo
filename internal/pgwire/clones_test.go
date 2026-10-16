package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/policy"
)

func TestParseCloneCommand(t *testing.T) {
	for _, c := range []struct {
		query string
		want  cloneCommand
	}{
		{"CREATE TABLE dev.t CLONE prod.t", cloneCommand{target: []string{"dev", "t"}, source: []string{"prod", "t"}}},
		{"create or replace table Dev.T clone lake.prod.t;", cloneCommand{replace: true, target: []string{"dev", "t"}, source: []string{"lake", "prod", "t"}}},
		{`CREATE TABLE IF NOT EXISTS "My T" CLONE t AT (SNAPSHOT => 123)`,
			cloneCommand{ifNotExists: true, target: []string{"My T"}, source: []string{"t"}, at: &atClause{unit: "SNAPSHOT", value: "123"}}},
		{"CREATE TABLE a CLONE b AT (VERSION => -5)", cloneCommand{target: []string{"a"}, source: []string{"b"}, at: &atClause{unit: "SNAPSHOT", value: "-5"}}},
		{"CREATE TABLE a CLONE b AT(TIMESTAMP => now() - INTERVAL '1 hour')",
			cloneCommand{target: []string{"a"}, source: []string{"b"}, at: &atClause{unit: "TIMESTAMP", value: "now() - INTERVAL '1 hour'"}}},
		{"CREATE TABLE a CLONE b AT (TAG => 'it''s')", cloneCommand{target: []string{"a"}, source: []string{"b"}, at: &atClause{unit: "TAG", value: "it's"}}},
		{"CREATE SCHEMA dev CLONE prod", cloneCommand{schema: true, target: []string{"dev"}, source: []string{"prod"}}},
		{"CREATE OR REPLACE SCHEMA dev CLONE prod AT (TIMESTAMP => TIMESTAMP '2026-10-01')",
			cloneCommand{schema: true, replace: true, target: []string{"dev"}, source: []string{"prod"}, at: &atClause{unit: "TIMESTAMP", value: "TIMESTAMP '2026-10-01'"}}},
	} {
		got, ok, err := parseCloneCommand(c.query)
		if !ok || err != nil {
			t.Errorf("%s: %v %v", c.query, ok, err)
			continue
		}
		if got.schema != c.want.schema || got.replace != c.want.replace || got.ifNotExists != c.want.ifNotExists ||
			!slices.Equal(got.target, c.want.target) || !slices.Equal(got.source, c.want.source) ||
			(got.at == nil) != (c.want.at == nil) || got.at != nil && *got.at != *c.want.at {
			t.Errorf("%s: got %+v %+v", c.query, got, got.at)
		}
	}

	for _, q := range []string{"CREATE TABLE t AS SELECT 1", "CREATE TABLE t (clone INTEGER)", "CREATE SCHEMA s", "SELECT 1"} {
		if _, ok, _ := parseCloneCommand(q); ok {
			t.Errorf("%s: taken for a clone", q)
		}
	}
	for _, q := range []string{
		"CREATE OR REPLACE TABLE IF NOT EXISTS a CLONE b",
		"CREATE TABLE a CLONE",
		"CREATE TABLE a CLONE b AT (SNAPSHOT => 'x')",
		"CREATE TABLE a CLONE b AT (BRANCH => dev)",
		"CREATE TABLE a CLONE b AT (STATEMENT => 'x')",
		"CREATE TABLE a CLONE b AT SNAPSHOT 1",
		"CREATE TABLE a CLONE b BEFORE (TIMESTAMP => now())",
		"CREATE TABLE a CLONE b; DROP TABLE b",
		"CREATE SCHEMA a CLONE b AT (SNAPSHOT => 1)",
	} {
		if _, ok, err := parseCloneCommand(q); !ok || err == nil {
			t.Errorf("%s: %v %v, want an error", q, ok, err)
		}
	}
}

func TestParseRefCommand(t *testing.T) {
	for _, c := range []struct {
		query string
		want  refCommand
	}{
		{"ALTER TABLE t CREATE BRANCH dev", refCommand{verb: "CREATE", table: []string{"t"}, kind: table.BranchRef, name: "dev"}},
		{"ALTER TABLE s.t CREATE OR REPLACE TAG v1 AT (SNAPSHOT => 7)",
			refCommand{verb: "CREATE", table: []string{"s", "t"}, kind: table.TagRef, name: "v1", replace: true, at: &atClause{unit: "SNAPSHOT", value: "7"}}},
		{`ALTER TABLE t CREATE TAG IF NOT EXISTS "Release 1"`, refCommand{verb: "CREATE", table: []string{"t"}, kind: table.TagRef, name: "Release 1", ifNotExists: true}},
		{"ALTER TABLE t DROP BRANCH IF EXISTS dev", refCommand{verb: "DROP", table: []string{"t"}, kind: table.BranchRef, name: "dev", ifExists: true}},
		{"ALTER TABLE t DROP TAG v1;", refCommand{verb: "DROP", table: []string{"t"}, kind: table.TagRef, name: "v1"}},
		{"ALTER TABLE t FAST FORWARD main TO dev", refCommand{verb: "FAST FORWARD", table: []string{"t"}, kind: table.BranchRef, name: "main", to: "dev"}},
		{"SHOW BRANCHES ON TABLE s.t", refCommand{verb: "SHOW", table: []string{"s", "t"}}},
	} {
		got, ok, err := parseRefCommand(c.query)
		if !ok || err != nil {
			t.Errorf("%s: %v %v", c.query, ok, err)
			continue
		}
		if got.verb != c.want.verb || !slices.Equal(got.table, c.want.table) || got.kind != c.want.kind || got.name != c.want.name ||
			got.replace != c.want.replace || got.ifNotExists != c.want.ifNotExists || got.ifExists != c.want.ifExists ||
			got.to != c.want.to || (got.at == nil) != (c.want.at == nil) || got.at != nil && *got.at != *c.want.at {
			t.Errorf("%s: got %+v", c.query, got)
		}
	}
	for _, q := range []string{"ALTER TABLE t ADD COLUMN x INTEGER", "ALTER TABLE t RENAME TO u", "ALTER TABLE t DROP COLUMN branch", "SHOW TAGS"} {
		if _, ok, _ := parseRefCommand(q); ok {
			t.Errorf("%s: taken for a branch statement", q)
		}
	}
	for _, q := range []string{"ALTER TABLE t CREATE BRANCH", "ALTER TABLE t FAST FORWARD main dev", "ALTER TABLE t DROP TAG a b", "SHOW BRANCHES ON"} {
		if _, ok, err := parseRefCommand(q); !ok || err == nil {
			t.Errorf("%s: %v %v, want an error", q, ok, err)
		}
	}
}

func TestFindRefReads(t *testing.T) {
	q := `SELECT * FROM s.t AT (BRANCH => 'dev') JOIN "U" AS u AT (TAG => 'v1') ON true, w x AT (BRANCH=>'b') ` +
		`WHERE 'AT (BRANCH => ''no'')' <> '' -- AT (TAG => 'comment')`
	reads, err := findRefReads(q)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		table []string
		at    clone.At
		text  string
	}{
		{[]string{"s", "t"}, clone.At{Unit: clone.Branch, Ref: "dev"}, "AT (BRANCH => 'dev')"},
		{[]string{"U"}, clone.At{Unit: clone.Tag, Ref: "v1"}, "AT (TAG => 'v1')"},
		{[]string{"w"}, clone.At{Unit: clone.Branch, Ref: "b"}, "AT (BRANCH=>'b')"},
	}
	if len(reads) != len(want) {
		t.Fatalf("found %+v", reads)
	}
	for i, w := range want {
		r := reads[i]
		if !slices.Equal(r.table, w.table) || r.at != w.at || q[r.start:r.end] != w.text {
			t.Errorf("read %d: %+v %q", i, r, q[r.start:r.end])
		}
	}
	for _, bad := range []string{"SELECT 1 AT (BRANCH => 'x')", "SELECT * FROM f() AT (TAG => 'x')"} {
		if _, err := findRefReads(bad); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
	if reads, err := findRefReads("SELECT * FROM t AT (VERSION => 1)"); err != nil || len(reads) != 0 {
		t.Errorf("time travel by version: %v %v", reads, err)
	}
}

func TestCloneWithoutCatalog(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, c := range []struct{ query, code string }{
		{"CREATE TABLE a CLONE b", "0A000"},
		{"CREATE SCHEMA a CLONE b", "0A000"},
		{"ALTER TABLE a CREATE BRANCH dev", "0A000"},
		{"CREATE TABLE a CLONE b AT (SNAPSHOT)", "42601"},
		{"ALTER TABLE a FAST FORWARD main", "42601"},
		{"CREATE TABLE plain (clone INTEGER)", ""},
		{"SELECT clone FROM plain AS branch", ""},
	} {
		_, err := conn.Exec(context.Background(), c.query)
		if pgCode(err) != c.code {
			t.Errorf("%s: %v, want %q", c.query, err, c.code)
		}
	}
}

func TestAttachClonePolicies(t *testing.T) {
	now := time.Now()
	base := func() *policy.Catalog {
		cat := &policy.Catalog{}
		must := func(err error) {
			if err != nil {
				t.Fatal(err)
			}
		}
		must(cat.PutPolicy(policy.Policy{Kind: policy.Masking, Name: "mask_email", Args: []policy.Arg{{Name: "v", Type: "VARCHAR"}}, Returns: "VARCHAR", Body: "'***'"}, false, now))
		must(cat.PutPolicy(policy.Policy{Kind: policy.Masking, Name: "other_mask", Args: []policy.Arg{{Name: "v", Type: "VARCHAR"}}, Returns: "VARCHAR", Body: "v"}, false, now))
		must(cat.PutPolicy(policy.Policy{Kind: policy.RowAccess, Name: "client_rows", Args: []policy.Arg{{Name: "c", Type: "VARCHAR"}}, Returns: "BOOLEAN", Body: "true"}, false, now))
		must(cat.PutTag(policy.Tag{Name: "pii"}, false, now))
		must(cat.SetMask(policy.ColumnMask{Schema: "crm", Table: "contacts", Column: "email", Policy: "mask_email"}, false, now))
		must(cat.AddRowFilter(policy.RowFilter{Schema: "crm", Table: "contacts", Policy: "client_rows", Columns: []string{"client_id"}}, now))
		must(cat.SetColumnTag(policy.ColumnTag{Schema: "crm", Table: "contacts", Column: "device_id", Tag: "pii", Value: "device"}, now))
		return cat
	}
	pair := clonePair{source: auth.Object{Schema: "crm", Table: "contacts"}, target: auth.Object{Schema: "team", Table: "contacts"}}

	cat := base()
	changed, err := attachClonePolicies(cat, []clonePair{pair}, "nora", now)
	if err != nil || !changed {
		t.Fatalf("attach: %v %v", changed, err)
	}
	rules := policy.NewSnapshot(1, *cat).Rules("TEAM", "Contacts")
	if rules == nil || rules.FilterPolicy == nil || rules.FilterPolicy.Name != "client_rows" ||
		rules.Column("email").Mask == nil || rules.Column("email").Mask.Name != "mask_email" ||
		len(rules.Column("device_id").Tags) != 1 || rules.Column("device_id").Tags[0].Value != "device" {
		t.Fatalf("the clone's rules: %+v", rules)
	}
	// Again, as for a clone made every day: nothing changes.
	if changed, err := attachClonePolicies(cat, []clonePair{pair}, "nora", now); err != nil || changed {
		t.Fatalf("attaching again: %v %v", changed, err)
	}

	// A table of that name with a different policy of its own: refused if
	// it exists, replaced if only its policies are left.
	for _, exists := range []bool{true, false} {
		cat := base()
		if err := cat.SetMask(policy.ColumnMask{Schema: "team", Table: "contacts", Column: "email", Policy: "other_mask"}, false, now); err != nil {
			t.Fatal(err)
		}
		p := pair
		p.exists = exists
		_, err := attachClonePolicies(cat, []clonePair{p}, "nora", now)
		if exists {
			if pgerror.GetCode(err) != codes.ObjectNotInPrerequisiteState || !strings.Contains(err.Error(), "other_mask") {
				t.Errorf("conflicting mask on an existing table: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if m := policy.NewSnapshot(1, *cat).Rules("team", "contacts").Column("email").Mask; m == nil || m.Name != "mask_email" {
			t.Errorf("the clone's mask: %+v", m)
		}
	}

	// Nothing to carry: nothing changes.
	cat = base()
	if changed, err := attachClonePolicies(cat, []clonePair{{source: auth.Object{Schema: "crm", Table: "plain"}, target: auth.Object{Schema: "team", Table: "plain"}}}, "nora", now); err != nil || changed {
		t.Fatalf("a table without policies: %v %v", changed, err)
	}
}
