package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/policy"
)

func TestParseErasureCommand(t *testing.T) {
	for _, c := range []struct {
		sql  string
		ok   bool
		err  bool
		want string
	}{
		{"ERASE SUBJECT WHERE pii.email = 'a@b.com'", true, false, `"pii"."email" IN ('***')`},
		{"erase subject where pii.email IN ('a@b.com', 'c@d.com') OR pii.\"Device\" = 'x';", true, false,
			`"pii"."email" IN ('***', '***') OR "pii"."Device" IN ('***')`},
		{"ERASE SUBJECT WHERE pii.email = a@b.com", true, true, ""},
		{"ERASE SUBJECT WHERE email = 'a@b.com'", true, true, ""},
		{"ERASE SUBJECT WHERE pii.email = 'a@b.com' AND pii.x = 'y'", true, true, ""},
		{"ERASE everything", true, true, ""},
		{"SHOW ERASURE REQUESTS", true, false, ""},
		{"ALTER TABLE crm.people SET (data_retention_days = 30)", true, false, ""},
		{"ALTER TABLE IF EXISTS crm.people RESET (data_retention_days)", true, false, ""},
		{"ALTER TABLE crm.people SET (data_retention_days = -1)", true, true, ""},
		{"ALTER TABLE crm.people SET (data_retention_days = 0)", true, true, ""},
		{"ERASE SUBJECT WHERE pii.email = 'a@b.com' DRY RUN", true, false, `"pii"."email" IN ('***') DRY RUN`},
		{"CANCEL ERASURE REQUEST 'r1'", true, false, ""},
		{"CONFIRM ERASURE REQUEST 'r1'", true, false, ""},
		{"CANCEL ERASURE REQUEST r1", true, true, ""},
		{"ALTER TABLE crm.people SET SCHEMA other", false, false, ""},
		{"ALTER TABLE crm.people ALTER COLUMN email SET TAG pii = 'email'", false, false, ""},
		{"SELECT 1", false, false, ""},
	} {
		cmd, ok, err := parseErasureCommand(c.sql)
		if ok != c.ok || (err != nil) != c.err {
			t.Errorf("%s: ok %v, err %v", c.sql, ok, err)
			continue
		}
		if c.want != "" {
			if got := erasureText(c.sql); got != "ERASE SUBJECT WHERE "+c.want {
				t.Errorf("%s: shown as %s", c.sql, got)
			}
			if len(cmd.wanted) == 0 {
				t.Errorf("%s: no subjects", c.sql)
			}
		}
		if err != nil && strings.Contains(err.Error(), "a@b.com") {
			t.Errorf("%s: the error shows the identifier: %v", c.sql, err)
		}
	}
	// What the history keeps of any statement with ERASE SUBJECT, in any
	// form: the identifiers can be anywhere in it.
	for _, sql := range []string{
		"ERASE SUBJECT WHERE pii.email = a@b.com",
		"ERASE SUBJECT WHERE pii.email = 'a@b.com'",
		`EXPLAIN ERASE SUBJECT WHERE pii.email = "a@b.com"`,
		"EXPLAIN ERASE SUBJECT WHERE pii.email = a@b.com",
		"SELECT 1; ERASE SUBJECT WHERE pii.email = 'a@b.com'",
		"select 1; erase /* x */ subject where pii.email = a@b.com",
	} {
		if got := redactQuery(sql); strings.Contains(got, "a@b.com") {
			t.Errorf("history of %s: %s", sql, got)
		}
	}
	// Comments before the keyword do not change which command it is.
	for sql, tag := range map[string]string{
		"CONFIRM ERASURE REQUEST 'r1'":                         tagConfirmErase,
		"/* approved by legal */ CONFIRM ERASURE REQUEST 'r1'": tagConfirmErase,
		"-- approved\nCONFIRM ERASURE REQUEST 'r1'":            tagConfirmErase,
		"/* confirm */ CANCEL ERASURE REQUEST 'r1'":            tagCancelErase,
		"  cancel erasure request 'r1'":                        tagCancelErase,
	} {
		if c, ok, err := parseErasureCommand(sql); !ok || err != nil || c.tag != tag || c.id != "r1" {
			t.Errorf("%q: %s %q, %v, %v", sql, c.tag, c.id, ok, err)
		}
	}
	if c, ok, _ := parseErasureCommand("(CONFIRM ERASURE REQUEST 'r1')"); ok && c.tag != tagConfirmErase {
		t.Errorf("a parenthesized CONFIRM runs as %s", c.tag)
	}

	toks, _ := tokenize("GRANT ERASE ON ACCOUNT TO privacy_officer")
	c, ok, err := parseACLCommand(toks)
	if !ok || err != nil || c.targetKind != targetAccount || c.grantee != "privacy_officer" || c.privileges[0] != auth.PrivilegeErase {
		t.Errorf("GRANT ERASE ON ACCOUNT: %+v, %v, %v", c, ok, err)
	}
	toks, _ = tokenize("GRANT ERASE ON TABLE t TO x")
	if _, _, err := parseACLCommand(toks); err == nil {
		t.Errorf("ERASE is granted on the account only")
	}
}

// Admins and holders of ERASE ON ACCOUNT ask for erasures, set data
// retentions and see the requests; nobody else, and the identifiers stay
// out of the history.
func TestEraseSubject(t *testing.T) {
	ctx := context.Background()
	policies := policy.NewMemoryStore()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connector.Close() })
	eraser := erasure.New(erasure.Options{Store: erasure.NewMemoryStore(), Policies: policies.Load, Catalog: icebergPeople(t),
		Engine: duckdbConnector{connector}, Key: []byte("an erasure key of thirty-two bytes"), CatalogName: "lake",
		Warehouse: t.TempDir() + "/", Rows: countRows(3)})
	cs := startCatalogServer(t, func(o *Options) { o.Policies, o.Erasure = policies, eraser })
	for _, sql := range []string{
		"CREATE TABLE lake.public.people (id INTEGER, email VARCHAR)",
		"CREATE TAG pii ALLOWED_VALUES 'email', 'email:sha256', 'device_id'",
		"ALTER TABLE lake.public.people ALTER COLUMN email SET TAG pii = 'email'",
	} {
		mustExec(t, cs.admin, sql)
	}
	nora := cs.user(t, "nora", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	erase := "ERASE SUBJECT WHERE pii.email IN ('ann@example.com', 'Ann@Example.com') OR pii.email = 'bo@example.com'"
	expectDenied(t, nora, erase, "ERASE ON ACCOUNT")
	expectDenied(t, nora, "SHOW ERASURE REQUESTS")
	expectDenied(t, nora, "ALTER TABLE people SET (data_retention_days = 30)")
	expectRows(t, nora, "SELECT count(*) FROM igloo.erasure_requests", "0")

	for _, sql := range []string{"CREATE ROLE privacy_officer", "GRANT ERASE ON ACCOUNT TO privacy_officer", "GRANT privacy_officer TO nora"} {
		mustExec(t, cs.admin, sql)
	}
	if got := texts(t, cs.admin, "SHOW GRANTS TO privacy_officer"); len(got) != 1 || !strings.HasPrefix(got[0], "privacy_officer|role|ERASE|account||igloo|") {
		t.Errorf("SHOW GRANTS: %q", got)
	}
	rows := texts(t, nora, erase)
	if len(rows) != 1 || !strings.HasSuffix(rows[0], "|queued") {
		t.Fatalf("ERASE SUBJECT: %q", rows)
	}
	id := strings.TrimSuffix(rows[0], "|queued")
	expectRows(t, nora, "SELECT request_id, requester, status, subjects FROM igloo.erasure_requests",
		id+`|nora|queued|[{"tag":"pii","value":"email","identifiers":2}]`)
	if got := texts(t, nora, "SHOW ERASURE REQUESTS"); len(got) != 1 || !strings.HasPrefix(got[0], id+"|nora|queued|find|") {
		t.Errorf("SHOW ERASURE REQUESTS: %q", got)
	}
	expectRefused(t, nora, "ERASE SUBJECT WHERE pii.device_id = 'dev-1'", "22023", "dev-1")
	expectRefused(t, nora, "ERASE SUBJECT WHERE pii.email = ann@example.com", "42601", "ann@")
	expectRefused(t, nora, "SELECT 1; ERASE SUBJECT WHERE pii.email = 'ann@example.com'", "")
	expectRefused(t, nora, "ERASE SUBJECT WHERE pii.email = 'n/a'", "22023")

	// A dry run says what a request would erase, and makes none.
	if got := texts(t, nora, "ERASE SUBJECT WHERE pii.email = 'ann@example.com' DRY RUN"); len(got) != 1 || got[0] != "public.people|tagged|email (pii.email)|3|0" {
		t.Errorf("DRY RUN: %q", got)
	}
	expectRows(t, nora, "SELECT count(*) FROM igloo.erasure_requests", "1")
	// A request can be canceled before it deletes, once.
	if got := texts(t, nora, "CANCEL ERASURE REQUEST '"+id+"'"); len(got) != 1 || got[0] != id+"|canceled" {
		t.Errorf("CANCEL: %q", got)
	}
	expectRefused(t, nora, "CANCEL ERASURE REQUEST '"+id+"'", "22023")
	expectRefused(t, nora, "CONFIRM ERASURE REQUEST 'nope'", "42704")
	expectDenied(t, cs.user(t, "otto", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol), "CANCEL ERASURE REQUEST '"+id+"'")

	// A tagged column can be neither dropped nor renamed, however the
	// statement names the table, nor left out of a table that replaces it.
	expectDenied(t, cs.admin, "ALTER TABLE lake.public.people DROP COLUMN email", "dropping")
	expectDenied(t, cs.admin, "ALTER TABLE lake.public.people DROP COLUMN IF EXISTS email", "dropping")
	expectDenied(t, cs.admin, "ALTER TABLE lake.public.people RENAME COLUMN email TO mail", "renaming")
	expectDenied(t, cs.admin, "ALTER TABLE ONLY lake.public.people RENAME COLUMN email TO mail", "renaming")
	expectDenied(t, cs.admin, "ALTER TABLE IF EXISTS ONLY lake.public.people DROP COLUMN email", "dropping")
	expectDenied(t, cs.admin, "/* x */ ALTER TABLE ONLY lake.public.people RENAME email TO mail", "renaming")
	expectDenied(t, cs.admin, "CREATE OR REPLACE TABLE lake.public.people AS SELECT id, email AS mail FROM lake.public.people", "keep the column")
	expectDenied(t, cs.admin, "CREATE OR REPLACE TABLE lake.public.people (id INTEGER, mail VARCHAR, CHECK (id > 0))", "keep the column")
	expectRows(t, cs.admin, "SELECT column_name FROM information_schema.columns WHERE table_catalog = 'lake' AND table_name = 'people' ORDER BY ordinal_position",
		"id", "email")
	mustExec(t, cs.admin, "CREATE OR REPLACE TABLE lake.public.people AS SELECT id, email, 'x' AS note FROM lake.public.people")
	mustExec(t, nora, "CREATE OR REPLACE TABLE lake.public.fresh AS SELECT 1 AS x")
	mustExec(t, nora, "CREATE OR REPLACE TABLE lake.public.fresh (y INTEGER)")

	// Data retention, kept with tags and listed for everyone. Privacy
	// officers set it on tables with tags, admins on any; a day at least.
	expectRefused(t, nora, "ALTER TABLE people SET (data_retention_days = 0)", "42601")
	expectDenied(t, nora, "ALTER TABLE orders SET (data_retention_days = 30)", "no tagged columns")
	mustExec(t, cs.admin, "ALTER TABLE lake.public.orders SET (data_retention_days = 30)")
	mustExec(t, cs.admin, "ALTER TABLE lake.public.orders RESET (data_retention_days)")
	mustExec(t, nora, "ALTER TABLE people SET (data_retention_days = 30)")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectRows(t, rita, "SELECT table_schema, table_name, data_retention_days, set_by FROM igloo.data_retention", "public|people|30|nora")
	// A privacy officer without a role of their own lists them too.
	pat := cs.user(t, "pat", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT privacy_officer TO pat")
	expectRows(t, pat, "SELECT table_name, data_retention_days FROM igloo.data_retention", "people|30")
	expectDenied(t, rita, "ALTER TABLE people RESET (data_retention_days)")
	expectDenied(t, cs.admin, "ALTER TABLE lake.public.people RENAME TO folks", "data retention")
	mustExec(t, nora, "ALTER TABLE people RESET (data_retention_days)")
	expectRows(t, rita, "SELECT count(*) FROM igloo.data_retention", "0")

	// The history has the statements, without the identifiers.
	deadline := time.Now().Add(5 * time.Second)
	for {
		recorded, err := cs.history.Queries(ctx, audit.Viewer{All: true}, audit.Page{})
		if err != nil {
			t.Fatal(err)
		}
		erases := 0
		for _, q := range recorded {
			if strings.Contains(q.Text, "example.com") || strings.Contains(q.Text, "dev-1") {
				t.Fatalf("history holds an identifier: %s", q.Text)
			}
			if strings.HasPrefix(q.Text, "ERASE") {
				erases++
			}
		}
		if erases >= 6 || time.Now().After(deadline) {
			if erases < 6 {
				t.Errorf("history has %d ERASE statements, want at least 6", erases)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// icebergPeople is a catalog with one Iceberg table, public.people, as the
// stand-in DuckDB catalog has it, for requests to check their columns in.
func icebergPeople(t *testing.T) erasure.Catalog {
	t.Helper()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "email", Type: iceberg.PrimitiveTypes.String})
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fsF := func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }
	return oneTable{table.New(table.Identifier{"public", "people"}, meta, "", fsF, nil)}
}

type oneTable struct{ tbl *table.Table }

func (c oneTable) Tables(context.Context) ([]table.Identifier, error) {
	return []table.Identifier{c.tbl.Identifier()}, nil
}

func (c oneTable) LoadTable(_ context.Context, id table.Identifier) (*table.Table, error) {
	if strings.Join(id, ".") != strings.Join(c.tbl.Identifier(), ".") {
		return nil, icecat.ErrNoSuchTable
	}
	return c.tbl, nil
}

func (oneTable) CommitTable(context.Context, table.Identifier, []table.Requirement, []table.Update) (table.Metadata, string, error) {
	return nil, "", table.ErrCommitFailed
}

type duckdbConnector struct{ c *duckdb.Connector }

func (d duckdbConnector) Connect(ctx context.Context) (*duckdb.Conn, error) {
	c, err := d.c.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return c.(*duckdb.Conn), nil
}

// countRows counts n matching rows in every table, and deletes none.
type countRows int64

func (n countRows) Count(context.Context, *table.Table, string) (int64, error) { return int64(n), nil }
func (countRows) Delete(context.Context, *table.Table, string) (int64, error)  { return 0, nil }
