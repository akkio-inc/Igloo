package pgwire

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/policy"
)

func TestParseDomainCommand(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]domainCmd{
		"CREATE ID DOMAIN People (VARCHAR)": {kind: dcCreate, name: "people", typ: "varchar"},
		"create id domain if not exists people (uuid) tag pii = 'person_id', src = 'crm' comment = 'who';": {kind: dcCreate, name: "people", typ: "uuid",
			ifNotExists: true, tags: map[string]string{"pii": "person_id", "src": "crm"}, comment: "who"},
		"DROP ID DOMAIN IF EXISTS people": {kind: dcDrop, name: "people", ifExists: true},
		"SHOW ID DOMAINS":                 {kind: dcShow},
		"ALTER TABLE s.t ALTER COLUMN person_id SET ID DOMAIN people":   {kind: dcSet, table: []string{"s", "t"}, column: "person_id", name: "people"},
		"alter table if exists t modify \"Id\" unset id domain":         {kind: dcUnset, table: []string{"t"}, column: "Id", ifExists: true},
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
	t.Parallel()
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
	// No user's statement reads, writes or unguards a dictionary.
	conn, err := r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO lake.igloo_domains.people VALUES ('p1', 0, now())", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	for _, c := range []*pgx.Conn{admin, rita} {
		if got := texts(t, c, "SELECT count(*) FROM lake.igloo_domains.people"); len(got) != 1 || got[0] != "0" {
			t.Errorf("rows a user reads from the dictionary: %q", got)
		}
	}
	for _, q := range []string{
		"INSERT INTO lake.igloo_domains.people VALUES ('p2', 1, now())",
		"DELETE FROM lake.igloo_domains.people",
		"DROP TABLE lake.igloo_domains.people",
		"ALTER TABLE lake.igloo_domains.people ADD COLUMN x INTEGER",
		"CREATE TABLE lake.igloo_domains.mine AS SELECT 1 AS a",
		"DROP SCHEMA lake.igloo_domains CASCADE",
		"ALTER TABLE lake.igloo_domains.people DROP ROW ACCESS POLICY " + iddomain.DictionaryPolicy,
		"DROP ROW ACCESS POLICY " + iddomain.DictionaryPolicy,
		"CREATE OR REPLACE ROW ACCESS POLICY " + iddomain.DictionaryPolicy + " AS (id BIGINT) RETURNS BOOLEAN -> true",
		"ALTER TABLE lake.igloo_domains.people ALTER COLUMN value UNSET TAG pii",
	} {
		if _, err := admin.Exec(ctx, q); pgCode(err) != "42501" {
			t.Errorf("%s: %v, want permission denied", q, err)
		}
	}
	files := vc.fake.Metadata(iddomain.DictionarySchema, "people").Location() + "/data/**/*.parquet"
	if got, err := rita.Exec(ctx, "SELECT count(*) FROM read_parquet('"+files+"')"); err == nil {
		t.Errorf("a reader read the dictionary's files: %v", got)
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
	// Writes naming their columns leave the ids to Igloo, which fills them.
	mustExec(t, admin, "INSERT INTO lake.public.people (person_id, n) VALUES ('p10', 10)")
	internal := func(q string) []string {
		t.Helper()
		conn, err := r.eng.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		rows, err := queryRows(ctx, conn, q)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, row := range rows {
			parts := make([]string, len(row))
			for i, v := range row {
				parts[i] = fmt.Sprint(v)
			}
			out = append(out, strings.Join(parts, "|"))
		}
		return out
	}
	if got := internal("SELECT count(*), count(_igloo_id_person_id) FROM lake.public.people"); len(got) != 1 || got[0] != "11|11" {
		t.Errorf("rows and ids: %q", got)
	}
	// Statements read the table as if it did not have the column, and
	// none may name it.
	mustExec(t, admin, "CREATE VIEW lake.public.everyone AS SELECT * FROM lake.public.people")
	for _, c := range []*pgx.Conn{admin, rita} {
		for _, q := range []string{"SELECT * FROM lake.public.people LIMIT 1", "SELECT p.* FROM lake.public.people AS p LIMIT 1",
			"SELECT * FROM lake.public.everyone LIMIT 1", "SELECT * FROM (SELECT * FROM lake.public.people) LIMIT 1"} {
			rows, err := c.Query(ctx, q)
			if err != nil {
				t.Errorf("%s: %v", q, err)
				continue
			}
			var names []string
			for _, f := range rows.FieldDescriptions() {
				names = append(names, f.Name)
			}
			rows.Close()
			if strings.Join(names, ",") != "person_id,n" {
				t.Errorf("%s: columns %v", q, names)
			}
		}
		for _, q := range []string{"SELECT _igloo_id_person_id FROM lake.public.people", "SELECT max(\"_IGLOO_ID_PERSON_ID\") FROM lake.public.people",
			"CREATE TABLE lake.public.ids AS SELECT 1 AS _igloo_id_x", "ALTER TABLE lake.public.people RENAME COLUMN n TO _igloo_id_n"} {
			if _, err := c.Exec(ctx, q); pgCode(err) != "42501" {
				t.Errorf("%s: %v, want permission denied", q, err)
			}
		}
	}
	for q, want := range map[string]string{
		"SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'people'": "person_id,n",
		"SELECT string_agg(column_name, ',' ORDER BY column_name) FROM igloo.columns WHERE table_schema = 'public' AND table_name = 'people'":                   "n,person_id",
		"SELECT string_agg(DISTINCT attname, ',' ORDER BY attname) FROM pg_attribute WHERE attname IN ('person_id', 'n') OR attname LIKE '%igloo%'":             "n,person_id",
	} {
		if got := texts(t, rita, q); len(got) != 1 || got[0] != want {
			t.Errorf("%s: %q, want %q", q, got, want)
		}
	}
	// Its files hold the ids, so Delta Sharing refuses it, and Igloo's
	// Iceberg REST endpoint keeps it to admins.
	if got, err := r.srv.ProtectedBy(ctx, "public", "people"); err != nil || strings.Join(got, ",") != "ID domain people" {
		t.Errorf("ProtectedBy: %q, %v", got, err)
	}
	if p, err := r.srv.ProtectedFor(ctx, auth.User{Name: "rita", Role: auth.RoleReader}, "public", "people"); err != nil || !p {
		t.Errorf("ProtectedFor: %v, %v", p, err)
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
	{
		conn, err := r.eng.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE lake.public.taken AS SELECT 'p1' AS person_id, 7 AS _igloo_id_person_id", nil); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
	}
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

// TestEraseDictionaryValue erases a person whose identifier a domain has
// in its dictionary: erasure deletes and rewrites the dictionary's row
// through its own path while every user's statement stays refused, and its
// verification finds nothing left.
func TestEraseDictionaryValue(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) { o.IDDomainsOn = true })
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.people AS SELECT 'p' || i AS person_id, i AS n FROM range(3) r(i)",
		"CREATE TAG pii",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN person_id SET ID DOMAIN people",
	} {
		mustExec(t, admin, q)
	}
	dictionary := func() []string {
		t.Helper()
		conn, err := r.eng.Connect(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		rows, err := queryRows(ctx, conn, "SELECT value || '=' || id FROM lake.igloo_domains.people ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, row := range rows {
			out = append(out, row[0].(string))
		}
		return out
	}
	conn, err := r.eng.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO lake.igloo_domains.people VALUES ('p0', 0, now()), ('p1', 1, now()), ('p2', 2, now())", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	cat, err := catalog.Open(ctx, vc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := erasure.NewMemoryStore()
	svc := erasure.New(erasure.Options{Store: store, Catalog: cat, Policies: r.srv.gov.store.Load, Engine: r.eng,
		Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake", Warehouse: vc.cfg.Storage.Location + "/",
		IDDomains: r.srv.idDomains})
	req, err := svc.Submit(ctx, "privacy", []erasure.Wanted{{Tag: "pii", Value: "person_id", Identifiers: []string{"p1"}}})
	if err != nil {
		t.Fatal(err)
	}
	var got *erasure.Request
	for range 3 {
		if err := svc.Run(ctx, req.ID); err != nil {
			t.Fatal(err)
		}
		if got, err = store.Get(ctx, req.ID); err != nil {
			t.Fatal(err)
		}
		if got.Status == erasure.StatusCompleted {
			break
		}
	}
	if got.Status != erasure.StatusCompleted {
		t.Fatalf("status %s at %s: %s", got.Status, got.Step, got.Error)
	}
	erased := map[string]int64{}
	for _, tt := range got.Tables {
		erased[tt.Name] = tt.RowsDeleted
	}
	if erased["igloo_domains.people"] != 1 || erased["public.people"] != 1 {
		t.Errorf("rows deleted: %v", erased)
	}
	if v := got.Verification; v == nil || v.Rows+v.Files+v.Bounds+v.Statistics != 0 {
		t.Errorf("verification: %+v", got.Verification)
	}
	if d := dictionary(); strings.Join(d, ",") != "p0=0,p2=2" {
		t.Errorf("dictionary after erasing p1: %q", d)
	}
	// It deleted from the dictionary last under the domain's lease, which
	// it counted (see TestIDAssignmentErasedMeanwhile).
	if n, err := r.srv.idDomains.Erased(ctx, "people"); err != nil || n != 1 {
		t.Errorf("erasures of the domain counted: %d, %v", n, err)
	}
	// The guard is still on.
	if got := texts(t, admin, "SELECT count(*) FROM lake.igloo_domains.people"); len(got) != 1 || got[0] != "0" {
		t.Errorf("rows an admin reads from the dictionary: %q", got)
	}
	if _, err := admin.Exec(ctx, "DELETE FROM lake.igloo_domains.people"); pgCode(err) != "42501" {
		t.Errorf("an admin's DELETE: %v", err)
	}
}
