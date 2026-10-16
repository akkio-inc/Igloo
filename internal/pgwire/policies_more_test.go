package pgwire

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/policy"
)

func TestPoliciesApplyThroughViews(t *testing.T) {
	cs := startPolicyServer(t)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	for _, sql := range []string{
		"CREATE VIEW memory.main.contact_emails AS SELECT id, email FROM lake.crm.contacts",
		"CREATE VIEW memory.main.renamed (who, mail) AS SELECT id, email FROM lake.crm.contacts",
		"CREATE VIEW memory.main.nested AS SELECT mail FROM memory.main.renamed WHERE who < 10",
		"CREATE VIEW memory.main.joined AS SELECT c.id, p.campaign FROM lake.crm.contacts c JOIN lake.public.campaigns p USING (client)",
		"CREATE VIEW memory.main.harmless AS SELECT campaign FROM lake.public.campaigns",
	} {
		mustExec(t, cs.admin, sql)
	}
	expectRows(t, rita, "SELECT id, email FROM contact_emails ORDER BY id", "1|***", "2|***")
	expectRows(t, rita, "SELECT mail FROM renamed ORDER BY who", "***", "***")
	expectRows(t, rita, "SELECT x.m FROM renamed AS x(w, m) WHERE x.w = 1", "***")
	expectRows(t, rita, "SELECT count(*) FROM nested WHERE mail = 'ann@acme.com'", "0")
	expectRows(t, rita, "SELECT campaign, count(*) FROM joined GROUP BY ALL", "spring|2")
	expectRows(t, rita, "SELECT count(*) FROM harmless", "2")
	expectRows(t, rita, "SELECT count(*) FROM contact_emails a JOIN nested b ON a.email = b.mail", "4")

	// A prepared statement reads a replaced view through the policies too.
	const q = "SELECT count(*) FROM harmless WHERE campaign <> $1"
	if n := count(t, rita, q, "x"); n != 2 {
		t.Fatalf("harmless: %d", n)
	}
	mustExec(t, cs.admin, "CREATE OR REPLACE VIEW memory.main.harmless AS SELECT email AS campaign FROM lake.crm.contacts")
	var n int64
	if err := rita.QueryRow(context.Background(), q, "***").Scan(&n); err != nil || n != 0 {
		t.Errorf("a prepared statement over a replaced view: %d %v", n, err)
	}
}

func TestPreparedStatementsFollowPolicyChanges(t *testing.T) {
	ctx := context.Background()
	cs := startPolicyServer(t)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	const q = "SELECT email FROM lake.crm.contacts WHERE id = $1"
	email := func() string {
		t.Helper()
		var e string
		if err := rita.QueryRow(ctx, q, 1).Scan(&e); err != nil {
			t.Fatal(err)
		}
		return e
	}
	if e := email(); e != "***" {
		t.Fatalf("masked: %q", e)
	}
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.contacts ALTER COLUMN email UNSET MASKING POLICY")
	if e := email(); e != "ann@acme.com" {
		t.Errorf("after UNSET: %q", e)
	}
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email")
	if e := email(); e != "***" {
		t.Errorf("after SET again: %q", e)
	}
	mustExec(t, cs.admin, "GRANT pii_reader TO rita")
	if e := email(); e != "ann@acme.com" {
		t.Errorf("with pii_reader: %q", e)
	}
	mustExec(t, cs.admin, "REVOKE pii_reader FROM rita")
	if e := email(); e != "***" {
		t.Errorf("after REVOKE: %q", e)
	}
	// A new body applies to statements prepared before.
	mustExec(t, cs.admin, `CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR -> 'hidden'`)
	if e := email(); e != "hidden" {
		t.Errorf("after the body changed: %q", e)
	}
	// A row policy attached later hides rows from statements prepared
	// before.
	const all = "SELECT count(*) FROM lake.public.campaigns WHERE client <> $1"
	if n := count(t, rita, all, "x"); n != 2 {
		t.Fatalf("campaigns: %d", n)
	}
	mustExec(t, cs.admin, "ALTER TABLE lake.public.campaigns ADD ROW ACCESS POLICY client_rows ON (client)")
	if n := count(t, rita, all, "x"); n != 1 {
		t.Errorf("after a row access policy was added: %d", n)
	}
	// So does one changed on another replica (here, straight in the
	// store), within the refresh interval.
	cs2 := startPolicyServer(t, func(o *Options) { o.RoleRefresh = 200 * time.Millisecond })
	rita2 := cs2.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs2.admin, "GRANT acme_team TO rita")
	if n := count(t, rita2, all, "x"); n != 2 {
		t.Fatalf("campaigns: %d", n)
	}
	if err := cs2.srv.gov.store.Update(ctx, func(c *policy.Catalog) error {
		f := policy.RowFilter{Schema: "public", Table: "campaigns", Policy: "client_rows", Columns: []string{"client"}}
		return c.AddRowFilter(f, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := count(t, rita2, all, "x"); n != 1 {
		t.Errorf("after another replica added a row access policy: %d", n)
	}
}

func TestPoliciesFailClosed(t *testing.T) {
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	mustExec(t, cs.admin, "GRANT acme_team TO rita")

	// Several statements in one simple query run one at a time, each under
	// the policies as if sent alone.
	mustExec(t, cs.admin, "SELECT 1; CREATE TABLE lake.public.raw AS SELECT * FROM "+c+"; SELECT 1")
	expectRows(t, cs.admin, "SELECT DISTINCT email FROM lake.public.raw", "***")
	mustExec(t, cs.admin, "DROP TABLE lake.public.raw")
	// A PIVOT that finds its columns by reading the data first.
	const types = "SELECT count(*) FROM duckdb_types() WHERE type_name LIKE '__pivot_enum_%'"
	before := count(t, rita, types)
	expectRefused(t, rita, "SELECT * FROM (PIVOT "+c+" ON email USING count(*))", "42501", secrets...)
	if n := count(t, rita, types); n != before {
		t.Errorf("a refused PIVOT made a type of the data")
	}
	// Renaming would leave the policies behind.
	expectRefused(t, walt, "ALTER TABLE "+c+" RENAME TO c2", "42501")
	expectRefused(t, walt, "ALTER TABLE "+c+" RENAME COLUMN email TO e", "42501")
	expectRefused(t, walt, "ALTER TABLE "+c+" RENAME client TO cl", "42501")
	expectRefused(t, walt, "ALTER TABLE "+c+" ALTER COLUMN email TYPE BLOB", "42501")
	expectAllowed(t, walt, "ALTER TABLE "+c+" ADD COLUMN note VARCHAR")
	// Macros cannot hide a read.
	mustExec(t, walt, "CREATE TEMP MACRO everything() AS TABLE SELECT * FROM "+c)
	expectRefused(t, walt, "SELECT * FROM everything()", "42501", secrets...)
	mustExec(t, walt, "CREATE TEMP MACRO first_email() AS (SELECT min(email) FROM "+c+")")
	expectRefused(t, walt, "SELECT first_email()", "42501", secrets...)
	// Nor replace a function a policy calls.
	mustExec(t, cs.admin, `CREATE MASKING POLICY hash_device AS (val VARCHAR) RETURNS VARCHAR -> 'h' || md5(val)`)
	mustExec(t, cs.admin, "ALTER TABLE "+c+" ALTER COLUMN device_id SET MASKING POLICY hash_device")
	if got := texts(t, walt, "SELECT device_id FROM "+c+" WHERE id = 1"); len(got) != 1 || !strings.HasPrefix(got[0], "h") || got[0] == "hdev-1" {
		t.Errorf("hashed device: %q", got)
	}
	mustExec(t, walt, "CREATE TEMP MACRO md5(x) AS x")
	expectRefused(t, walt, "SELECT device_id FROM "+c, "42501", "dev-1")
	mustExec(t, walt, `CREATE TEMP MACRO "||"(a, b) AS b`)
	expectRefused(t, walt, "SELECT count(*) FROM "+c, "42501")
	// A temporary table cannot stand in for the one a policy reads.
	rita2 := cs.user(t, "rita2", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, rita2, "CREATE TEMP TABLE client_access AS SELECT 'writer' AS role_name, 'globex' AS client")
	mustExec(t, rita2, "SET search_path = 'temp.main,lake.gov,lake.public'")
	expectRows(t, rita2, "SELECT count(*) FROM "+c, "0")

	// Functions that read tables by name, or table statistics.
	for _, sql := range []string{
		"SELECT * FROM duckdb_table_sample('" + c + "')",
		"SELECT * FROM pragma_storage_info('" + c + "')",
		"CALL pragma_storage_info('" + c + "')",
		"PRAGMA storage_info('" + c + "')",
		"PRAGMA tpch(1)",
		"SELECT stats(email) FROM " + c,
		"SELECT * FROM histogram_values('" + c + "', email)",
	} {
		expectRefused(t, rita, sql, "", secrets...)
	}
	// A query that names the table in a string, plain, escaped or
	// dollar-quoted, or in a name with Unicode escapes.
	mustExec(t, rita, "SET search_path = 'lake.crm'")
	for _, sql := range []string{
		"SELECT email FROM 'contacts' WHERE id = 1",
		`SELECT email FROM lake.crm.U&"\0063ontacts" WHERE id = 1`,
		`SELECT email FROM U&"cont\0061cts" WHERE id = 1`,
		`SELECT email FROM U&'\0063ontacts' WHERE id = 1`,
		`SELECT email FROM E'cont\x61cts' WHERE id = 1`,
		`SELECT email FROM e'\143ontacts' WHERE id = 1`,
		"SELECT email FROM $$contacts$$ WHERE id = 1",
		"SELECT email FROM $q$contacts$q$ WHERE id = 1",
		// The first term of a recursive CTE named like the table reads
		// the table.
		"WITH RECURSIVE contacts AS (SELECT * FROM contacts UNION ALL SELECT * FROM contacts WHERE false) SELECT email FROM contacts WHERE id = 1",
	} {
		// Refused, or masked.
		_, err := rita.Exec(context.Background(), sql)
		switch {
		case err != nil && slices.ContainsFunc(secrets, func(s string) bool { return strings.Contains(err.Error(), s) }):
			t.Errorf("%s: %v", sql, err)
		case err == nil:
			if got := texts(t, rita, sql); !slices.Equal(got, []string{"***"}) {
				t.Errorf("%s: %q", sql, got)
			}
		}
	}
}

// TestPoliciesKeepFilesOut reads the warehouse's files directly, which
// policies cannot see into.
func TestPoliciesKeepFilesOut(t *testing.T) {
	dir := t.TempDir()
	landing, warehouse := dir+"/landing/", dir+"/warehouse/"
	for _, d := range []string{landing, warehouse} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) { o.AllowedLocations, o.Warehouse = []string{landing}, warehouse })
	data, in := warehouse+"data.parquet", landing+"in.parquet"
	mustExec(t, cs.admin, "COPY (SELECT 'ann@acme.com' AS email) TO '"+data+"' (FORMAT parquet)")
	mustExec(t, cs.admin, "COPY (SELECT 1 AS x) TO '"+in+"' (FORMAT parquet)")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	// Without policies, readers read any allowed file, as before.
	expectRows(t, rita, "SELECT email FROM read_parquet('"+data+"')", "ann@acme.com")
	mustExec(t, walt, "CREATE TEMP VIEW peek AS SELECT * FROM read_parquet('"+data+"')")
	for _, sql := range policyFixture {
		mustExec(t, cs.admin, sql)
	}
	for _, sql := range []string{
		"SELECT * FROM read_parquet('" + data + "')",
		"SELECT * FROM '" + data + "'",
		`SELECT * FROM "` + data + `"`,
		"SELECT * FROM read_parquet(['" + in + "', '" + data + "'])",
		"SELECT * FROM read_parquet('" + landing + "../warehouse/data.parquet')",
		"SELECT * FROM read_parquet('" + dir + "/*/*.parquet')",
		"SELECT * FROM glob('" + dir + "/*')",
		"SELECT * FROM parquet_metadata('" + data + "')",
		"SELECT * FROM read_blob('" + data + "')",
		"SELECT * FROM read_parquet('" + warehouse + "' || 'data.parquet')",
		"SELECT * FROM lake.public.campaigns, read_parquet('" + data + "')",
	} {
		expectRefused(t, rita, sql, "42501", "ann@")
	}
	expectRows(t, rita, "SELECT x FROM read_parquet('"+in+"')", "1")
	expectRows(t, rita, "SELECT x FROM '"+in+"'", "1")
	expectRefused(t, walt, "COPY lake.public.campaigns FROM '"+data+"'", "42501", "ann@", "email")
	expectRefused(t, walt, "CREATE TEMP VIEW peek2 AS SELECT * FROM read_parquet('"+data+"')", "42501")
	// A view made before reads through the same checks.
	expectRefused(t, walt, "SELECT * FROM peek", "42501", "ann@")
	// Admins may: they manage the policies.
	expectRows(t, cs.admin, "SELECT email FROM read_parquet('"+data+"')", "ann@acme.com")
}

func TestPolicyStatements(t *testing.T) {
	cs := startPolicyServer(t)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"CREATE MASKING POLICY m AS (v VARCHAR) RETURNS VARCHAR -> v",
		"DROP MASKING POLICY mask_email",
		"ALTER TABLE lake.crm.contacts ALTER COLUMN email UNSET MASKING POLICY",
		"ALTER TABLE lake.crm.contacts DROP ROW ACCESS POLICY client_rows",
		"CREATE TAG pii",
		"ALTER TABLE lake.crm.contacts ALTER COLUMN email SET TAG pii = 'email'",
	} {
		expectDenied(t, rita, sql, "admin")
	}
	for _, tc := range []struct{ sql, code string }{
		{"CREATE MASKING POLICY mask_email AS (v VARCHAR) RETURNS VARCHAR -> v", "42710"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS INTEGER -> 1", "22023"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR, w VARCHAR) RETURNS VARCHAR -> v", "42601"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS VARCHAR -> getvariable('x')", "22023"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS VARCHAR -> (SELECT max(x) FROM read_csv('x.csv'))", "22023"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS VARCHAR -> v || nosuch", "22023"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS VARCHAR -> v || $1", "22023"},
		{"CREATE MASKING POLICY m2 AS (v VARCHAR) RETURNS VARCHAR -> 1", "22023"},
		{"CREATE MASKING POLICY m2 AS (v NOSUCHTYPE) RETURNS NOSUCHTYPE -> v", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS VARCHAR -> v", "42601"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> v", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> tag_value('x') = v", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (client VARCHAR) RETURNS BOOLEAN -> EXISTS (SELECT 1 FROM lake.gov.client_access a WHERE a.client = client)", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> v IN (SELECT client FROM memory.main.v)", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> v) AS ok, (SELECT 1", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> v) FROM lake.gov.client_access WHERE (true", "22023"},
		{"CREATE ROW ACCESS POLICY r2 AS (v VARCHAR) RETURNS BOOLEAN -> v = current_setting('search_path')", "22023"},
		{"ALTER TABLE lake.crm.contacts ALTER COLUMN spend SET MASKING POLICY mask_email", "42804"},
		{"ALTER TABLE lake.crm.contacts ALTER COLUMN nosuch SET MASKING POLICY mask_email", "42703"},
		{"ALTER TABLE lake.crm.contacts ALTER COLUMN device_id SET MASKING POLICY nosuch", "42704"},
		{"ALTER TABLE lake.crm.contacts ALTER COLUMN device_id SET MASKING POLICY client_rows", "42704"},
		{"ALTER TABLE lake.crm.contacts ADD ROW ACCESS POLICY client_rows ON (client)", "42710"},
		{"ALTER TABLE lake.public.campaigns ADD ROW ACCESS POLICY client_rows ON (client, campaign)", "22023"},
		{"ALTER TABLE lake.public.nosuch ADD ROW ACCESS POLICY client_rows ON (client)", "42P01"},
		{"DROP MASKING POLICY mask_email", "2BP01"},
		{"DROP ROW ACCESS POLICY mask_email", "42704"},
		{"ALTER TABLE lake.crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email; SELECT 1", "0A000"},
	} {
		if _, err := cs.admin.Exec(context.Background(), tc.sql); pgCode(err) != tc.code {
			t.Errorf("%s: want %s, got %v", tc.sql, tc.code, err)
		}
	}
	mustExec(t, cs.admin, "CREATE VIEW memory.main.v AS SELECT 1 AS x")
	mustExec(t, cs.admin, "DROP MASKING POLICY IF EXISTS nosuch")
	mustExec(t, cs.admin, "CREATE MASKING POLICY IF NOT EXISTS mask_email AS (v INTEGER) RETURNS INTEGER -> 0")

	// Everyone lists policies; only admins see their expressions.
	type row struct {
		name, kind, signature string
		body                  *string
	}
	list := func(conn *pgx.Conn) []row {
		rows, err := conn.Query(context.Background(), "SHOW POLICIES")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []row
		for rows.Next() {
			var r row
			var comment, by string
			var created, updated time.Time
			if err := rows.Scan(&r.name, &r.kind, &r.signature, &r.body, &comment, &by, &created, &updated); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		return out
	}
	got := list(rita)
	if len(got) != 2 || got[0].name != "client_rows" || got[0].kind != "ROW ACCESS POLICY" || got[0].body != nil ||
		got[1].signature != "(val VARCHAR) RETURNS VARCHAR" {
		t.Errorf("SHOW POLICIES as a reader: %+v", got)
	}
	if got := list(cs.admin); got[0].body == nil || !strings.Contains(*got[0].body, `"lake"."gov"."client_access"`) {
		t.Errorf("an admin sees the body, with its tables qualified: %+v", got)
	}
	if got := texts(t, rita, "SHOW ROW ACCESS POLICIES"); len(got) != 1 || !strings.HasPrefix(got[0], "client_rows|ROW ACCESS POLICY|(c VARCHAR) RETURNS BOOLEAN|NULL|") {
		t.Errorf("SHOW ROW ACCESS POLICIES: %q", got)
	}
}

func TestTags(t *testing.T) {
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	for _, sql := range []string{
		"CREATE TAG pii ALLOWED_VALUES 'email', 'device_id', 'spend' COMMENT = 'personal data'",
		`CREATE MASKING POLICY pii_text AS (val VARCHAR) RETURNS VARCHAR ->
			CASE WHEN is_role_in_session('pii_reader') THEN val
			     WHEN tag_value('pii') = 'device_id' THEN 'device:' || md5(val)[1:6]
			     ELSE '[' || tag_value('pii') || ']' END`,
		"ALTER TAG pii SET MASKING POLICY pii_text",
		"ALTER TABLE " + c + " ALTER COLUMN device_id SET TAG pii = 'device_id'",
		"ALTER TABLE " + c + " MODIFY COLUMN client SET TAG pii = 'email'",
		// A tagged column of a type no policy of the tag takes is hidden.
		"ALTER TABLE " + c + " ALTER COLUMN spend SET TAG pii = 'spend'",
	} {
		mustExec(t, cs.admin, sql)
	}
	got := texts(t, rita, "SELECT client, device_id, spend, email FROM "+c+" WHERE id = 1")
	if len(got) != 1 || !strings.HasPrefix(got[0], "[email]|device:") || !strings.HasSuffix(got[0], "|NULL|***") {
		t.Errorf("tagged columns: %q", got)
	}
	// A policy on the column itself comes first.
	expectRows(t, rita, "SELECT email FROM "+c+" WHERE id = 1", "***")
	if _, err := cs.admin.Exec(context.Background(), "ALTER TABLE "+c+" ALTER COLUMN email SET TAG pii = 'phone'"); pgCode(err) != "22023" {
		t.Errorf("a value the tag does not allow: %v", err)
	}
	if _, err := cs.admin.Exec(context.Background(), "DROP TAG pii"); pgCode(err) != "2BP01" {
		t.Errorf("dropping a tag in use: %v", err)
	}
	expectRows(t, rita, "SELECT tag_name, tag_value, object_schema, object_name, column_name FROM igloo.tag_references ORDER BY column_name",
		"pii|email|crm|contacts|client", "pii|device_id|crm|contacts|device_id", "pii|spend|crm|contacts|spend")
	expectRows(t, rita, "SELECT policy_name, policy_kind, ref_column_name, ref_arg_column_names, tag_name FROM igloo.policy_references ORDER BY all",
		"client_rows|ROW ACCESS POLICY|NULL|[client]|NULL",
		"mask_email|MASKING POLICY|email|NULL|NULL",
		"pii_text|MASKING POLICY|client|NULL|pii",
		"pii_text|MASKING POLICY|device_id|NULL|pii",
		"pii_text|MASKING POLICY|spend|NULL|pii")
	if got := texts(t, rita, "SHOW TAGS"); len(got) != 1 || !strings.HasPrefix(got[0], "pii|email, device_id, spend|pii_text|personal data|igloo|") {
		t.Errorf("SHOW TAGS: %q", got)
	}
	mustExec(t, cs.admin, "ALTER TAG pii UNSET MASKING POLICY pii_text")
	expectRows(t, rita, "SELECT client FROM "+c+" WHERE id = 1", "acme")
	for _, col := range []string{"device_id", "client", "spend"} {
		mustExec(t, cs.admin, "ALTER TABLE "+c+" ALTER COLUMN "+col+" UNSET TAG pii")
	}
	mustExec(t, cs.admin, "DROP TAG pii")
}

func TestPolicyHistory(t *testing.T) {
	cs := startPolicyServer(t)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	const q = "SELECT email FROM lake.crm.contacts WHERE id = 1"
	expectRows(t, rita, q, "***")
	expectRows(t, cs.admin, "SELECT query_text, policies FROM igloo.query_history WHERE user_name = 'rita' AND query_text = '"+q+"'",
		q+"|[client_rows mask_email]")
	got := texts(t, rita, "SELECT count(*) FROM igloo.query_history WHERE query_text LIKE '%__igloo_%'")
	if !slices.Equal(got, []string{"0"}) {
		t.Errorf("the history has rewritten statements: %v", got)
	}
}

func TestPolicyCursorsAndConsole(t *testing.T) {
	ctx := context.Background()
	cs := startPolicyServer(t)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	mustExec(t, rita, "BEGIN")
	mustExec(t, rita, "DECLARE c CURSOR FOR SELECT email FROM lake.crm.contacts ORDER BY id")
	expectRows(t, rita, "FETCH ALL FROM c", "***", "***")
	mustExec(t, rita, "COMMIT")

	user, err := cs.store.Get(ctx, "rita")
	if err != nil {
		t.Fatal(err)
	}
	es, err := cs.srv.OpenEmbedded(ctx, user, "console")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	var out collectWriter
	if err := es.Run(ctx, "SELECT email FROM lake.crm.contacts", &out); err != nil {
		t.Fatal(err)
	}
	if len(out.rows) != 2 || out.rows[0] != "***" {
		t.Errorf("the console shows %q", out.rows)
	}
}

type collectWriter struct{ rows []string }

func (w *collectWriter) Columns([]Column) error { return nil }
func (w *collectWriter) Complete(string) error  { return nil }
func (w *collectWriter) Row(values []any) error {
	s, _ := values[0].(string)
	w.rows = append(w.rows, s)
	return nil
}
