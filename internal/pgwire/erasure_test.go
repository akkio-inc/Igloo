package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

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
	// What the history keeps of any ERASE.
	for _, sql := range []string{"ERASE SUBJECT WHERE pii.email = a@b.com", "ERASE SUBJECT WHERE pii.email = 'a@b.com'"} {
		if got := redactQuery(sql); strings.Contains(got, "a@b.com") {
			t.Errorf("history of %s: %s", sql, got)
		}
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
	eraser := erasure.New(erasure.Options{Store: erasure.NewMemoryStore(), Policies: policies.Load})
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

	// Data retention, kept with tags and listed for everyone.
	mustExec(t, nora, "ALTER TABLE people SET (data_retention_days = 30)")
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectRows(t, rita, "SELECT table_schema, table_name, data_retention_days, set_by FROM igloo.data_retention", "public|people|30|nora")
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
		if erases >= 4 || time.Now().After(deadline) {
			if erases < 4 {
				t.Errorf("history has %d ERASE statements, want at least 4", erases)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}
