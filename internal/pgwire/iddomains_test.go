package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
)

func TestParseDomainCommand(t *testing.T) {
	for q, want := range map[string]domainCmd{
		"CREATE ID DOMAIN People (VARCHAR)": {kind: dcCreate, name: "people", typ: "varchar"},
		"create id domain if not exists people (uuid) tag pii = 'person_id', src = 'crm' comment = 'who';": {kind: dcCreate, name: "people", typ: "uuid",
			ifNotExists: true, tags: map[string]string{"pii": "person_id", "src": "crm"}, comment: "who"},
		"DROP ID DOMAIN IF EXISTS people":                              {kind: dcDrop, name: "people", ifExists: true},
		"SHOW ID DOMAINS":                                              {kind: dcShow},
		"ALTER TABLE s.t ALTER COLUMN person_id SET ID DOMAIN people":   {kind: dcSet, table: []string{"s", "t"}, column: "person_id", name: "people"},
		"alter table if exists t modify \"Id\" unset id domain":        {kind: dcUnset, table: []string{"t"}, column: "Id", ifExists: true},
		"ALTER TABLE lake.s.t ALTER person_id SET ID DOMAIN people   ;": {kind: dcSet, table: []string{"lake", "s", "t"}, column: "person_id", name: "people"},
	} {
		got, ok, err := parseDomainCommand(q)
		if !ok || err != nil {
			t.Errorf("%s: %v %v", q, ok, err)
			continue
		}
		if got.kind != want.kind || got.name != want.name || got.typ != want.typ || got.ifExists != want.ifExists || got.ifNotExists != want.ifNotExists ||
			strings.Join(got.table, ".") != strings.Join(want.table, ".") || got.column != want.column || got.comment != want.comment ||
			len(got.tags) != len(want.tags) {
			t.Errorf("%s: got %+v", q, got)
		}
		for k, v := range want.tags {
			if got.tags[k] != v {
				t.Errorf("%s: tag %s = %q", q, k, got.tags[k])
			}
		}
	}
	for _, q := range []string{"ALTER TABLE t ALTER COLUMN c SET TAG pii = 'x'", "ALTER TABLE t ALTER COLUMN c SET DATA TYPE BIGINT",
		"CREATE TABLE domain (id INT)", "SHOW TABLES", "ALTER TABLE t ADD COLUMN domain INT"} {
		if _, ok, _ := parseDomainCommand(q); ok {
			t.Errorf("%s: taken for an ID domain statement", q)
		}
	}
	for _, q := range []string{`CREATE ID DOMAIN "People" (VARCHAR)`, "CREATE ID DOMAIN people", "CREATE ID DOMAIN people (VARCHAR) TAG pii",
		"CREATE ID DOMAIN people (VARCHAR) extra", "DROP ID DOMAIN", "ALTER TABLE t ALTER c SET ID DOMAIN", "SHOW ID DOMAINS LIKE 'x'"} {
		if _, ok, err := parseDomainCommand(q); !ok || err == nil {
			t.Errorf("%s: %v %v, want an error", q, ok, err)
		}
	}
}

// TestIDDomains creates an ID domain, makes a column a member and leaves
// and drops it again, through sessions, checking the dictionary, the column
// that holds ids, the table property and who may do what.
func TestIDDomains(t *testing.T) {
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) { o.IDDomainsOn = true })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people AS SELECT 'p' || i AS person_id, i AS n FROM range(10) r(i)",
		"CREATE TAG pii",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
	} {
		mustExec(t, admin, q)
	}
	rita := connect(t, r.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)

	if _, err := rita.Exec(ctx, "CREATE ID DOMAIN people (VARCHAR)"); pgCode(err) != "42501" {
		t.Errorf("a reader's CREATE ID DOMAIN: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE ID DOMAIN people (VARCHAR) TAG nope = 'x'"); pgCode(err) != "42704" {
		t.Errorf("an unknown tag: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE ID DOMAIN people (DOUBLE)"); pgCode(err) != "22023" {
		t.Errorf("a DOUBLE domain: %v", err)
	}
	mustExec(t, admin, "CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id' COMMENT = 'everyone'")
	if _, err := admin.Exec(ctx, "CREATE ID DOMAIN people (VARCHAR)"); pgCode(err) != "42710" {
		t.Errorf("CREATE again: %v", err)
	}
	mustExec(t, admin, "CREATE ID DOMAIN IF NOT EXISTS people (VARCHAR)")

	// The dictionary, with the domain's tag on its values.
	meta := vc.fake.Metadata(iddomain.DictionarySchema, "people")
	var names []string
	for _, f := range meta.CurrentSchema().Fields() {
		names = append(names, f.Name+" "+f.Type.String())
	}
	if got := strings.Join(names, ", "); got != "value string, id long, assigned_at timestamptz" {
		t.Errorf("dictionary columns: %s", got)
	}
	snap := r.srv.gov.snapshot(ctx)
	if !slices.ContainsFunc(snap.Catalog.ColumnTags, func(ct policy.ColumnTag) bool {
		return ct.Schema == iddomain.DictionarySchema && ct.Table == "people" && ct.Column == "value" && ct.Tag == "pii" && ct.Value == "person_id"
	}) {
		t.Errorf("the dictionary's values do not carry the tag: %+v", snap.Catalog.ColumnTags)
	}
	show := func(conn *pgx.Conn) []string {
		t.Helper()
		return texts(t, conn, "SHOW ID DOMAINS")
	}
	if got := show(rita); len(got) != 1 || !strings.HasPrefix(got[0], "people|VARCHAR|pii = 'person_id'|1||0|everyone|igloo|") {
		t.Errorf("SHOW ID DOMAINS: %q", got)
	}

	// Joining: the column must have the domain's type and tags.
	if _, err := admin.Exec(ctx, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people"); pgCode(err) != "55000" {
		t.Errorf("a column without the tag: %v", err)
	}
	mustExec(t, admin, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'")
	if _, err := admin.Exec(ctx, "ALTER TABLE lake.public.people ALTER COLUMN n SET ID DOMAIN people"); pgCode(err) != "42804" {
		t.Errorf("a BIGINT column: %v", err)
	}
	if _, err := rita.Exec(ctx, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people"); pgCode(err) != "42501" {
		t.Errorf("a reader's SET ID DOMAIN: %v", err)
	}
	if _, err := admin.Exec(ctx, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN nobody"); pgCode(err) != "42704" {
		t.Errorf("an unknown domain: %v", err)
	}
	mustExec(t, admin, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people")
	meta = vc.fake.Metadata("public", "people")
	f, ok := meta.CurrentSchema().FindFieldByName("_igloo_id_person_id")
	if !ok || f.Type.String() != "long" || f.Required {
		t.Errorf("the column for the ids: %+v %v", f, ok)
	}
	if got := catalog.IDDomainMembers(meta); got["person_id"] != "people@1" {
		t.Errorf("table property: %v", got)
	}
	commits := vc.fake.Commits()
	mustExec(t, admin, "ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people")
	if vc.fake.Commits() != commits {
		t.Error("joining again committed")
	}
	if got := show(admin); len(got) != 1 || !strings.Contains(got[0], "|public.people.person_id|") {
		t.Errorf("SHOW ID DOMAINS with a member: %q", got)
	}
	// Writes naming their columns leave the ids to Igloo.
	mustExec(t, admin, "INSERT INTO lake.public.people (person_id, n) VALUES ('p10', 10)")
	if got := texts(t, admin, "SELECT count(*), count(_igloo_id_person_id) FROM lake.public.people"); len(got) != 1 || got[0] != "11|0" {
		t.Errorf("rows and ids: %q", got)
	}
	if _, err := admin.Exec(ctx, "DROP ID DOMAIN people"); pgCode(err) != "2BP01" {
		t.Errorf("dropping a domain in use: %v", err)
	}

	// Leaving and dropping.
	mustExec(t, admin, "ALTER TABLE lake.public.people ALTER COLUMN person_id UNSET ID DOMAIN")
	meta = vc.fake.Metadata("public", "people")
	if _, ok := meta.CurrentSchema().FindFieldByName("_igloo_id_person_id"); ok || len(catalog.IDDomainMembers(meta)) > 0 {
		t.Errorf("after UNSET: %v %v", ok, catalog.IDDomainMembers(meta))
	}
	if _, err := admin.Exec(ctx, "ALTER TABLE lake.public.people ALTER COLUMN person_id UNSET ID DOMAIN"); pgCode(err) != "42704" {
		t.Errorf("UNSET again: %v", err)
	}
	// Ids it handed out stay its own: the domain made again counts anew in
	// a generation of its own.
	if _, err := r.srv.idDomains.Allocate(ctx, "people", 1, 5); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, "DROP ID DOMAIN people")
	if vc.fake.Metadata(iddomain.DictionarySchema, "people") != nil {
		t.Error("the dictionary is still there")
	}
	if got := show(admin); len(got) != 0 {
		t.Errorf("SHOW ID DOMAINS after DROP: %q", got)
	}
	mustExec(t, admin, "DROP ID DOMAIN IF EXISTS people")

	// In Snowflake's dialect too.
	mustExec(t, admin, "SET sql_dialect = 'snowflake'")
	mustExec(t, admin, "CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'")
	mustExec(t, admin, "ALTER TABLE LAKE.PUBLIC.PEOPLE ALTER COLUMN PERSON_ID SET ID DOMAIN people")
	if got := texts(t, admin, "SHOW ID DOMAINS"); len(got) != 1 || !strings.HasPrefix(got[0], "people|VARCHAR|pii = 'person_id'|2|") {
		t.Errorf("Snowflake dialect, made again: %q", got)
	}
	mustExec(t, admin, "SET sql_dialect = 'duckdb'")

	// A column of the name the ids would go in, which Igloo did not add,
	// is not taken for them.
	mustExec(t, admin, "CREATE TABLE lake.public.taken AS SELECT 'p1' AS person_id, 7 AS _igloo_id_person_id")
	mustExec(t, admin, "ALTER TABLE lake.public.taken ALTER COLUMN person_id SET TAG pii = 'person_id'")
	if _, err := admin.Exec(ctx, "ALTER TABLE lake.public.taken ALTER COLUMN person_id SET ID DOMAIN people"); pgCode(err) != "42701" {
		t.Errorf("a taken name: %v", err)
	}

	// Without IGLOO_ID_DOMAINS, no column joins a domain.
	off := vc.replica(t).admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, off, "CREATE TABLE lake.public.others AS SELECT 'p1' AS person_id")
	if _, err := off.Exec(ctx, "ALTER TABLE lake.public.others ALTER COLUMN person_id SET ID DOMAIN people"); pgCode(err) != "0A000" {
		t.Errorf("with IGLOO_ID_DOMAINS off: %v", err)
	}
}
