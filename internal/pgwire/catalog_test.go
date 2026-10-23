package pgwire

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// fixedMetadata is a MetadataSource whose metadata a test sets, standing
// in for what the Iceberg catalog records about the stand-in catalog's
// tables.
type fixedMetadata struct {
	mu      sync.Mutex
	md      *catalog.Metadata
	changed []string
}

func (f *fixedMetadata) Metadata() *catalog.Metadata {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.md
}

func (f *fixedMetadata) RefreshMetadata(context.Context) error { return nil }

func (f *fixedMetadata) TableChanged(schema, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.changed = append(f.changed, schema+"."+name)
}

// catalogFixture describes the stand-in catalog's tables (see
// startCatalogServer), with comments a user without access to them must
// not learn.
func catalogFixture() *catalog.Metadata {
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	return &catalog.Metadata{
		Schemas: []catalog.SchemaMeta{{Name: "public", Comment: "Shared tables"}, {Name: "sales", Comment: "Sales team only"}},
		Tables: []catalog.TableMeta{
			{Schema: "public", Name: "orders", Comment: "One row per order", LastModified: at, Snapshots: 2, Rows: 2, Bytes: 1000, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER"}, {Name: "amount", Type: "INTEGER", Comment: "In cents"}}},
			{Schema: "public", Name: "secrets", Comment: "Launch codes", LastModified: at, Snapshots: 1, Rows: 1, Bytes: 500, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER"}, {Name: "code", Type: "VARCHAR", Comment: "The code itself"}}},
			{Schema: "sales", Name: "leads", Comment: "Leads from the CRM", LastModified: at, Snapshots: 1, Rows: 3, Bytes: 700, Files: 1,
				Columns: []catalog.ColumnMeta{{Name: "id", Type: "INTEGER", Comment: "Lead number"}}},
		},
	}
}

// hidden are what a user granted only public.orders must never see.
var hidden = []string{"secrets", "Launch codes", "code itself", "sales", "Sales team", "leads", "Leads from", "Lead number"}

// noLeaks fails the test if any row mentions something hidden.
func noLeaks(t *testing.T, what string, rows []string, extra ...string) {
	t.Helper()
	for _, r := range rows {
		for _, h := range append(slices.Clone(hidden), extra...) {
			if strings.Contains(strings.ToLower(r), strings.ToLower(h)) {
				t.Errorf("%s shows %q: %s", what, h, r)
			}
		}
	}
}

func startCatalogFixture(t *testing.T) (*catalogServer, *fixedMetadata) {
	t.Helper()
	md := &fixedMetadata{md: catalogFixture()}
	cs := startCatalogServer(t, func(o *Options) { o.Metadata = md })
	for _, sql := range []string{
		"CREATE TAG pii",
		"ALTER TABLE lake.public.secrets ALTER COLUMN code SET TAG pii = 'code'",
		"ALTER TABLE lake.public.orders ALTER COLUMN amount SET TAG pii = 'amount'",
		"CREATE MASKING POLICY hide_code AS (v VARCHAR) RETURNS VARCHAR -> '***'",
		"ALTER TABLE lake.public.secrets ALTER COLUMN code SET MASKING POLICY hide_code",
	} {
		mustExec(t, cs.admin, sql)
	}
	return cs, md
}

func TestCatalogShowsOnlyWhatTheUserMaySee(t *testing.T) {
	cs, _ := startCatalogFixture(t)
	nora := cs.user(t, "nora", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO nora")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)

	tables := "SELECT table_schema, table_name, table_type, table_owner, comment, row_count, bytes, snapshot_count FROM igloo.tables ORDER BY 1, 2"
	if got, want := texts(t, nora, tables), []string{"public|orders|BASE TABLE|igloo|One row per order|2|1000|2"}; !slices.Equal(got, want) {
		t.Errorf("nora's tables: %q, want %q", got, want)
	}
	for name, conn := range map[string]*pgx.Conn{"an admin": cs.admin, "a reader": rita} {
		if got := texts(t, conn, tables); len(got) != 3 {
			t.Errorf("%s sees %d tables: %q", name, len(got), got)
		}
	}
	if got, want := texts(t, nora, "SELECT schema_name, schema_owner, comment FROM igloo.schemata"), []string{"public|igloo|Shared tables"}; !slices.Equal(got, want) {
		t.Errorf("nora's schemas: %q, want %q", got, want)
	}
	if got, want := texts(t, nora, "SELECT table_name, column_name, ordinal_position, data_type, comment FROM igloo.columns ORDER BY 3"),
		[]string{"orders|id|1|INTEGER|NULL", "orders|amount|2|INTEGER|In cents"}; !slices.Equal(got, want) {
		t.Errorf("nora's columns: %q, want %q", got, want)
	}
	if got, want := texts(t, nora, "SELECT table_name, column_name, comment FROM igloo_comments() ORDER BY 1 NULLS FIRST, 2 NULLS FIRST"),
		[]string{"NULL|NULL|Shared tables", "orders|NULL|One row per order", "orders|amount|In cents"}; !slices.Equal(got, want) {
		t.Errorf("nora's comments: %q, want %q", got, want)
	}
	if got, want := texts(t, nora, "SELECT object_name, column_name, tag_name, tag_value FROM igloo.tag_references"),
		[]string{"orders|amount|pii|amount"}; !slices.Equal(got, want) {
		t.Errorf("nora's tags: %q, want %q", got, want)
	}
	if got := texts(t, cs.admin, "SELECT ref_entity_name, ref_column_name FROM igloo.policy_references"); !slices.Equal(got, []string{"secrets|code"}) {
		t.Errorf("an admin's policy references: %q", got)
	}
	if got := texts(t, nora, "SELECT ref_entity_name FROM igloo.policy_references"); len(got) != 0 {
		t.Errorf("nora sees the policies of a table she may not see: %q", got)
	}
	if got, want := texts(t, nora, "SHOW OBJECTS"), []string{
		"public|NULL|SCHEMA|igloo|user|Shared tables|NULL|NULL|NULL|NULL",
		"public|orders|TABLE|igloo|user|One row per order|2|1000|2026-10-01 12:00:00 +0000 UTC|NULL",
	}; !slices.Equal(got, want) {
		t.Errorf("nora's SHOW OBJECTS: %q, want %q", got, want)
	}
	if got := texts(t, cs.admin, "SHOW OBJECTS IN SCHEMA sales"); len(got) != 2 {
		t.Errorf("an admin's SHOW OBJECTS IN SCHEMA sales: %q", got)
	}

	// information_schema lists every table's name, as before, but the
	// owners, comments and sizes only of those the user may see.
	info := `SELECT table_schema, table_name, table_owner, comment, TABLE_COMMENT, row_count, bytes, last_altered IS NOT NULL
		FROM information_schema.tables WHERE table_catalog = 'lake' ORDER BY 1, 2`
	if got, want := texts(t, nora, info), []string{
		"public|orders|igloo|One row per order|One row per order|2|1000|true",
		"public|secrets|NULL|NULL|NULL|NULL|NULL|false",
		"sales|leads|NULL|NULL|NULL|NULL|NULL|false",
	}; !slices.Equal(got, want) {
		t.Errorf("nora's information_schema.tables: %q, want %q", got, want)
	}
	if got := texts(t, cs.admin, info); len(got) != 3 || !strings.HasPrefix(got[1], "public|secrets|igloo|Launch codes") {
		t.Errorf("an admin's information_schema.tables: %q", got)
	}
	if got, want := texts(t, nora, "SELECT schema_name, schema_owner, comment FROM information_schema.schemata WHERE catalog_name = 'lake' ORDER BY 1"),
		[]string{"main|duckdb|NULL", "public|igloo|Shared tables", "sales|duckdb|NULL"}; !slices.Equal(got, want) {
		t.Errorf("nora's information_schema.schemata: %q, want %q", got, want)
	}
	// Flight SQL's metadata commands read it as DuckDB's own.
	sameAsInformationSchema(t, nora)
	sameAsInformationSchema(t, cs.admin)

	// Nothing nora may read mentions what she may not see, however she
	// asks.
	for _, q := range []string{
		"SELECT * FROM igloo.tables", "SELECT * FROM igloo.schemata", "SELECT * FROM igloo.columns",
		"SELECT * FROM igloo_comments()", "SELECT * FROM pg_description", "SELECT * FROM igloo.tag_references",
		"SELECT * FROM igloo.policy_references", "SELECT * FROM igloo.lineage", "SELECT * FROM igloo.table_usage",
		"SHOW OBJECTS", "SHOW OBJECTS IN SCHEMA sales",
		"SELECT * FROM igloo_tables()", "SELECT * FROM igloo_lineage()", "SELECT * FROM igloo_table_usage()",
	} {
		noLeaks(t, q, texts(t, nora, q))
	}
	if got := texts(t, cs.admin, "SELECT description FROM pg_description WHERE description = 'Launch codes'"); len(got) != 1 {
		t.Errorf("an admin's pg_description: %q", got)
	}

	// What she sees follows her grants as they change.
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.sales TO nora")
	cs.srv.sessions.expire("nora")
	if got := texts(t, nora, "SELECT table_name FROM igloo.tables ORDER BY 1"); !slices.Equal(got, []string{"leads", "orders"}) {
		t.Errorf("after a grant on sales, nora's tables: %q", got)
	}
	mustExec(t, cs.admin, "REVOKE SELECT ON SCHEMA lake.sales FROM nora")
	mustExec(t, cs.admin, "REVOKE SELECT ON TABLE lake.public.orders FROM nora")
	cs.srv.sessions.expire("nora")
	if got := texts(t, nora, "SELECT * FROM igloo.tables"); len(got) != 0 {
		t.Errorf("without grants, nora sees tables: %q", got)
	}
}

// flightMetadataQueries are the queries of Flight SQL's metadata commands
// (internal/flightsql/metadata.go), with the filters they take, which read
// information_schema: Igloo's versions of its views must answer them as
// DuckDB's own do, with the same catalogs, tables and table types.
var flightMetadataQueries = []struct {
	sql  string
	args []any
}{
	{sql: "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY catalog_name"},
	{sql: "SELECT catalog_name, schema_name FROM information_schema.schemata ORDER BY 1, 2"},
	{sql: `SELECT catalog_name, schema_name FROM information_schema.schemata WHERE catalog_name = $1 AND schema_name LIKE $2 ESCAPE '\'
		ORDER BY 1, 2`, args: []any{"lake", "%"}},
	{sql: `SELECT table_catalog, table_schema, table_name, CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END
		FROM information_schema.tables ORDER BY 1, 2, 3, 4`},
	{sql: `SELECT table_catalog, table_schema, table_name, CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END
		FROM information_schema.tables WHERE table_name LIKE $1 ESCAPE '\'
		AND CASE table_type WHEN 'BASE TABLE' THEN 'TABLE' ELSE table_type END IN ($2, $3) ORDER BY 1, 2, 3, 4`,
		args: []any{"%", "TABLE", "VIEW"}},
	{sql: `SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
		FROM information_schema.columns ORDER BY 1, 2, 3, ordinal_position`},
}

// sameAsInformationSchema checks that Flight SQL's metadata queries read
// the same from information_schema, as Igloo serves it, as from DuckDB's,
// and that Igloo's columns come after DuckDB's.
func sameAsInformationSchema(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	for _, q := range flightMetadataQueries {
		got := texts(t, conn, q.sql, q.args...)
		want := texts(t, conn, strings.ReplaceAll(q.sql, "information_schema.", "system.information_schema."), q.args...)
		if len(want) == 0 || !slices.Equal(got, want) {
			t.Errorf("%s:\n got %q\nwant %q", q.sql, got, want)
		}
	}
	for _, view := range []string{"schemata", "tables"} {
		got, want := columnNames(t, conn, "information_schema."+view), columnNames(t, conn, "system.information_schema."+view)
		if len(got) <= len(want) || !slices.Equal(got[:len(want)], want) {
			t.Errorf("information_schema.%s has the columns %q, want %q and Igloo's", view, got, want)
		}
	}
}

// columnNames lists the columns of a table or view.
func columnNames(t *testing.T, conn *pgx.Conn, table string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT * FROM "+table+" LIMIT 0")
	if err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	defer rows.Close()
	var out []string
	for _, f := range rows.FieldDescriptions() {
		out = append(out, strings.ToLower(f.Name))
	}
	return out
}

// TestExternalTablesInTheCatalog checks that the catalog lists external
// tables (Delta Sharing) as such, with their provider, to whoever may see
// them; that lineage records reads of them; and that their views' files,
// under the proxy's secret prefix, stay out of the history.
func TestExternalTablesInTheCatalog(t *testing.T) {
	dir := t.TempDir()
	proxy := dir + "/proxy/"
	if err := os.MkdirAll(proxy, 0o755); err != nil {
		t.Fatal(err)
	}
	md := &fixedMetadata{md: catalogFixture()}
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) {
			o.Metadata = md
			o.external, o.externalFiles = fakeExternal{"public.viewing": true}, proxy
		})
	file := proxy + "f1.parquet"
	for _, sql := range []string{
		"COPY (SELECT 1 AS id) TO '" + file + "' (FORMAT parquet)",
		"CREATE SCHEMA memory.public",
		"CREATE VIEW memory.public.viewing AS SELECT * FROM read_parquet(['" + file + "'])",
	} {
		mustExec(t, cs.admin, sql)
	}
	nora := cs.user(t, "nora", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)

	tables := "SELECT table_catalog, table_schema, table_name, table_type, provider, row_count FROM igloo.tables WHERE table_name = 'viewing'"
	expectRows(t, cs.admin, tables, "lake|public|viewing|EXTERNAL TABLE|acme|NULL")
	if got := texts(t, nora, tables); len(got) != 0 {
		t.Errorf("nora sees an external table without a grant: %q", got)
	}
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.viewing TO nora")
	mustExec(t, cs.admin, "ALTER TABLE lake.public.viewing OWNER TO nora")
	cs.srv.sessions.expire("nora")
	expectRows(t, nora, tables, "lake|public|viewing|EXTERNAL TABLE|acme|NULL")
	expectRows(t, nora, "SELECT column_name, data_type FROM igloo.columns WHERE table_name = 'viewing'", "id|INTEGER")
	expectRows(t, nora, "SELECT table_catalog, table_type, table_owner FROM information_schema.tables WHERE table_name = 'viewing'",
		"memory|VIEW|nora")
	expectRows(t, nora, "SHOW OBJECTS", "public|NULL|SCHEMA|igloo|user|Shared tables|NULL|NULL|NULL|NULL",
		"public|viewing|EXTERNAL TABLE|nora|user|NULL|NULL|NULL|NULL|NULL")
	sameAsInformationSchema(t, nora)

	// Reading it is reading the table, not its files.
	mustExec(t, cs.admin, "CREATE TABLE lake.public.copied AS SELECT * FROM memory.public.viewing")
	mustExec(t, cs.admin, "CREATE TABLE lake.public.raw AS SELECT * FROM read_parquet('"+file+"')")
	expectRows(t, cs.admin, `SELECT upstream_type, upstream_schema, upstream_name, downstream_name FROM igloo.lineage
		WHERE downstream_name IN ('copied', 'raw')`, "TABLE|public|viewing|copied")
	expectRows(t, cs.admin, `SELECT list_contains(tables_written, 'lake.public.raw'), locations_read IS NULL FROM igloo.query_history
		WHERE statement_type = 'CREATE TABLE' AND query_text LIKE '%read_parquet%'`, "true|true")
	for _, l := range texts(t, cs.admin, "SELECT locations_read FROM igloo.query_history WHERE locations_read IS NOT NULL") {
		if strings.Contains(l, proxy) {
			t.Errorf("the history shows the proxy's prefix: %s", l)
		}
	}
}

func TestLineageAndUsage(t *testing.T) {
	cs, md := startCatalogFixture(t)
	mustExec(t, cs.admin, "CREATE SCHEMA lake.sandbox")
	wally := cs.user(t, "wally", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	nora := cs.user(t, "nora", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	sandy := cs.user(t, "sandy", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"GRANT SELECT ON TABLE lake.public.orders TO nora",
		"GRANT SELECT ON TABLE lake.public.orders TO sandy",
		"GRANT CREATE, SELECT ON SCHEMA lake.sandbox TO sandy",
		"INSERT INTO lake.public.orders SELECT id, 0 FROM lake.public.secrets",
		"CREATE TABLE lake.public.report AS SELECT * FROM lake.public.orders",
	} {
		mustExec(t, cs.admin, sql)
	}
	mustExec(t, wally, "UPDATE lake.public.report SET amount = l.id FROM lake.sales.leads l WHERE l.id = report.id")
	mustExec(t, sandy, "CREATE TABLE lake.sandbox.mine AS SELECT * FROM lake.public.orders")

	edges := `SELECT upstream_type, upstream_schema, upstream_name, downstream_schema, downstream_name, statements, last_user,
		last_statement_type, last_query_id IS NOT NULL, last_seen IS NOT NULL
		FROM igloo.lineage ORDER BY downstream_name NULLS FIRST, upstream_name NULLS FIRST`
	if got, want := texts(t, cs.admin, edges), []string{
		"TABLE|public|orders|sandbox|mine|1|sandy|CREATE TABLE|true|true",
		"TABLE|public|secrets|public|orders|1|igloo|INSERT|true|true",
		"TABLE|sales|leads|public|report|1|wally|UPDATE|true|true",
		"TABLE|public|orders|public|report|1|igloo|CREATE TABLE|true|true",
	}; !slices.Equal(got, want) {
		t.Errorf("an admin's lineage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}

	// Nora may see orders only: each edge to or from it shows, with the
	// other end's name, and when and by what it was made, left out; the
	// edge between two tables she may not see does not show at all.
	if got, want := texts(t, nora, edges), []string{
		"TABLE|public|orders|NULL|NULL|NULL|NULL|NULL|false|false",
		"TABLE|public|orders|NULL|NULL|NULL|NULL|NULL|false|false",
		"TABLE|NULL|NULL|public|orders|NULL|NULL|NULL|false|false",
	}; !slices.Equal(got, want) {
		t.Errorf("nora's lineage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	noLeaks(t, "nora's lineage", texts(t, nora, "SELECT * FROM igloo.lineage"), "report", "sandbox", "mine", "wally", "sandy")

	// Sandy sees her own statement, but not the admin's.
	if got, want := texts(t, sandy, edges), []string{
		"TABLE|public|orders|NULL|NULL|NULL|NULL|NULL|false|false",
		"TABLE|public|orders|sandbox|mine|1|sandy|CREATE TABLE|true|true",
		"TABLE|NULL|NULL|public|orders|NULL|NULL|NULL|false|false",
	}; !slices.Equal(got, want) {
		t.Errorf("sandy's lineage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if got := texts(t, wally, "SELECT last_user, last_query_id IS NULL FROM igloo.lineage WHERE upstream_name = 'secrets'"); !slices.Equal(got, []string{"NULL|true"}) {
		t.Errorf("a writer sees the edge but not an admin's statement: %q", got)
	}

	usage := "SELECT table_schema, table_name, reads_30d, writes_30d, users_30d, last_read IS NOT NULL FROM igloo.table_usage ORDER BY 1, 2"
	if got, want := texts(t, nora, usage), []string{"public|orders|2|3|2|true"}; !slices.Equal(got, want) {
		t.Errorf("nora's usage: %q, want %q", got, want)
	}
	if got := texts(t, cs.admin, usage); len(got) != 5 {
		t.Errorf("an admin's usage: %q", got)
	}

	// The history says what each statement read and wrote, whoever ran
	// it.
	if got := texts(t, wally, "SELECT tables_read, tables_written FROM igloo.query_history WHERE statement_type = 'UPDATE'"); len(got) != 1 ||
		!strings.Contains(got[0], "lake.sales.leads") || !strings.Contains(got[0], "lake.public.report") {
		t.Errorf("wally's UPDATE in the history: %q", got)
	}
	// And the tables written have their metadata read again.
	md.mu.Lock()
	changed := slices.Clone(md.changed)
	md.mu.Unlock()
	for _, want := range []string{"public.orders", "public.report", "sandbox.mine"} {
		if !slices.Contains(changed, want) {
			t.Errorf("the metadata of %s was not read again: %q", want, changed)
		}
	}
}

func TestOwners(t *testing.T) {
	cs, _ := startCatalogFixture(t)
	wally := cs.user(t, "wally", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "CREATE ROLE analysts")
	mustExec(t, cs.admin, "GRANT analysts TO rita")
	owners := func() []string {
		t.Helper()
		var out []string
		ws, _ := cs.grants.ObjectOwners(context.Background())
		for _, w := range ws {
			out = append(out, w.Object.String()+"="+w.Owner+"/"+w.OwnerType)
		}
		slices.Sort(out)
		return out
	}

	// Whoever creates a schema or table owns it.
	mustExec(t, wally, "CREATE SCHEMA lake.marts")
	mustExec(t, wally, "CREATE TABLE lake.marts.spend AS SELECT 1 AS n")
	if got := owners(); !slices.Contains(got, "marts=wally/user") || !slices.Contains(got, "marts.spend=wally/user") ||
		!slices.Contains(got, "public.orders=igloo/user") {
		t.Errorf("owners after creates: %q", got)
	}
	// So does an admin's, in a query of several statements.
	mustExec(t, cs.admin, "CREATE TABLE lake.marts.a (i INTEGER); CREATE SCHEMA lake.extra; CREATE TABLE lake.marts.b AS SELECT 1 AS x")
	if got := owners(); !slices.Contains(got, "marts.a=igloo/user") || !slices.Contains(got, "extra=igloo/user") ||
		!slices.Contains(got, "marts.b=igloo/user") {
		t.Errorf("owners after several creates: %q", got)
	}
	// Replacing a table keeps its owner; the reader cannot change it.
	mustExec(t, cs.admin, "CREATE OR REPLACE TABLE lake.marts.spend AS SELECT 2 AS n")
	expectDenied(t, rita, "ALTER TABLE lake.marts.spend OWNER TO rita", "changing the owner of table marts.spend")
	expectDenied(t, rita, "ALTER SCHEMA lake.marts OWNER TO rita", "changing the owner of schema marts")
	// The owner hands it to a role, whose members then own it.
	mustExec(t, wally, "ALTER TABLE lake.marts.spend OWNER TO analysts")
	cs.srv.ownersChanged()
	mustExec(t, rita, "ALTER TABLE lake.marts.spend OWNER TO wally")
	mustExec(t, wally, "ALTER SCHEMA marts OWNER TO analysts")
	if got := texts(t, rita, "SELECT table_owner, owner_type FROM igloo.tables WHERE table_name = 'orders'"); !slices.Equal(got, []string{"igloo|user"}) {
		t.Errorf("rita reads owners: %q", got)
	}
	if _, err := wally.Exec(context.Background(), "ALTER TABLE lake.marts.spend OWNER TO nobody"); pgCode(err) != "42704" {
		t.Errorf("an unknown owner: %v", err)
	}
	if _, err := wally.Exec(context.Background(), "ALTER TABLE lake.marts.nothing OWNER TO wally"); pgCode(err) != "42P01" {
		t.Errorf("a missing table: %v", err)
	}
	expectAllowed(t, wally, "ALTER TABLE IF EXISTS lake.marts.nothing OWNER TO wally")

	// Owners follow renames and go with drops.
	mustExec(t, wally, "ALTER TABLE lake.marts.spend RENAME TO outlay")
	if got := owners(); !slices.Contains(got, "marts.outlay=wally/user") || slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "marts.spend=") }) {
		t.Errorf("owners after a rename: %q", got)
	}
	mustExec(t, wally, "DROP TABLE lake.marts.outlay")
	mustExec(t, cs.admin, "DROP ROLE analysts")
	mustExec(t, cs.admin, "DROP SCHEMA lake.marts CASCADE")
	if got := owners(); slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "marts") }) {
		t.Errorf("owners after the drops: %q", got)
	}
}

func TestLineageOfLoadsAndJobs(t *testing.T) {
	ls := startLoadServer(t)
	walt := ls.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	nora := ls.user(t, "nora", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, ls.admin, "GRANT SELECT ON TABLE lake.public.sales TO nora")
	ls.write(t, "drops/day1.csv", "id,name,amount\n1,one,1.50\n", false)
	for _, sql := range []string{
		"COPY INTO lake.public.sales FROM '" + ls.dir + "/drops/'",
		"CREATE MATERIALIZED VIEW lake.public.sales_total AS SELECT sum(amount) AS total FROM lake.public.sales",
		"REFRESH MATERIALIZED VIEW lake.public.sales_total",
		"CREATE TABLE lake.public.copies AS SELECT * FROM lake.public.sales WHERE false",
		"CREATE VIEW lake.public.recent AS SELECT * FROM lake.public.sales",
		"CREATE TASK copy SCHEDULE '1 hour' AS INSERT INTO lake.public.copies SELECT * FROM lake.public.recent",
		"EXECUTE TASK copy",
	} {
		mustExec(t, walt, sql)
	}
	edges := `SELECT upstream_type, upstream_name, downstream_name, statements, last_statement_type, last_job
		FROM igloo.lineage ORDER BY downstream_name NULLS FIRST, upstream_name NULLS FIRST`
	if got, want := texts(t, walt, edges), []string{
		// Through the view, from the table it reads.
		"TABLE|sales|copies|2|INSERT|task copy",
		"LOCATION|" + ls.dir + "/drops/|sales|1|COPY|NULL",
		"TABLE|sales|sales_total|2|REFRESH|materialized view public.sales_total",
	}; !slices.Equal(got, want) {
		t.Errorf("lineage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	// A user with the role none may not read files, so where files were
	// loaded from is not theirs to see.
	if got := texts(t, nora, edges); !slices.Equal(got, []string{
		"TABLE|sales|NULL|NULL|NULL|NULL", "TABLE|sales|NULL|NULL|NULL|NULL", "LOCATION|NULL|sales|NULL|NULL|NULL",
	}) {
		t.Errorf("nora's lineage: %q", got)
	}
	for _, row := range texts(t, nora, "SELECT * FROM igloo.lineage") {
		for _, h := range []string{ls.dir, "drops", "copies", "sales_total", "walt", "task"} {
			if strings.Contains(row, h) {
				t.Errorf("nora's lineage shows %q: %s", h, row)
			}
		}
	}
}

// sink is a LineageSink that keeps what it is told.
type sink struct {
	mu sync.Mutex
	qs []audit.Query
}

func (k *sink) Statement(q audit.Query) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.qs = append(k.qs, q)
}

func TestLineageSink(t *testing.T) {
	k := &sink{}
	cs := startCatalogServer(t, func(o *Options) { o.Lineage = k })
	mustExec(t, cs.admin, "CREATE TABLE lake.public.report AS SELECT * FROM lake.public.orders")
	mustExec(t, cs.admin, "SELECT * FROM lake.public.report")
	if _, err := cs.admin.Exec(context.Background(), "INSERT INTO lake.public.report SELECT 1"); err == nil {
		t.Fatal("a failing INSERT succeeded")
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	// Only the statements that wrote a table and succeeded.
	var got []string
	for _, q := range k.qs {
		got = append(got, q.StatementType+" "+strings.Join(q.TablesRead, ",")+" > "+strings.Join(q.TablesWritten, ","))
	}
	want := "CREATE TABLE lake.public.orders > lake.public.report"
	if !slices.Contains(got, want) || slices.ContainsFunc(got, func(s string) bool {
		return strings.HasPrefix(s, "SELECT") || strings.HasSuffix(s, "> lake.public.report") && s != want
	}) {
		t.Errorf("the sink got %q, want %q among them", got, want)
	}
}

func TestFileReads(t *testing.T) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn := c.(*duckdb.Conn)
	defer conn.Close() //nolint:errcheck
	ast, err := parseStatement(context.Background(), conn, `SELECT * FROM read_parquet('s3://landing/a/*.parquet') a
		JOIN 's3://landing/b.csv' b ON true JOIN lake.public.t ON true, read_csv(?) WHERE x IN (SELECT 1 FROM read_json('gs://g/c.json'))`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fileReads(ast), []string{"s3://landing/a/*.parquet", "s3://landing/b.csv", "gs://g/c.json"}; !slices.Equal(sorted(got), sorted(want)) {
		t.Errorf("files: %q, want %q", got, want)
	}
}

func sorted(s []string) []string {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}

func TestLineageOfMergesDeletesAndPipes(t *testing.T) {
	ls := startLoadServer(t)
	walt := ls.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	ls.write(t, "drops/day1.csv", "id,name,amount\n1,one,1.50\n", false)
	for _, sql := range []string{
		"CREATE TABLE lake.public.totals (id INTEGER, amount INTEGER)",
		`MERGE INTO lake.public.totals t USING lake.public.orders o ON t.id = o.id
			WHEN MATCHED THEN UPDATE SET amount = o.amount WHEN NOT MATCHED THEN INSERT VALUES (o.id, o.amount)`,
		"DELETE FROM lake.public.totals USING lake.sales.leads l WHERE totals.id = l.id + 100",
		"CREATE PIPE drops AS COPY INTO lake.public.sales FROM '" + ls.dir + "/drops/'",
	} {
		mustExec(t, walt, sql)
	}
	j, err := ls.tasks.Store().Job(context.Background(), tasks.KindPipe, "drops")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ls.tasks.RunNow(context.Background(), ls.srv, j); err != nil {
		t.Fatal(err)
	}
	if got, want := texts(t, walt, `SELECT upstream_name, downstream_name, last_statement_type, last_job FROM igloo.lineage
		ORDER BY downstream_name, upstream_name`), []string{
		ls.dir + "/drops/|sales|COPY|pipe drops",
		"leads|totals|DELETE|NULL",
		"orders|totals|MERGE|NULL",
	}; !slices.Equal(got, want) {
		t.Errorf("lineage:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
