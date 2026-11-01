package pgwire

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// An agency's warehouse: contacts of two clients, with emails and device
// IDs, and a table saying which team may see which client's rows.
var policyFixture = []string{
	"CREATE SCHEMA lake.crm",
	"CREATE SCHEMA lake.gov",
	"CREATE TABLE lake.crm.contacts (id INTEGER, client VARCHAR, email VARCHAR, device_id VARCHAR, spend INTEGER)",
	`INSERT INTO lake.crm.contacts VALUES (1, 'acme', 'ann@acme.com', 'dev-1', 10), (2, 'acme', 'al@acme.com', 'dev-2', 20),
		(3, 'globex', 'gus@globex.com', 'dev-3', 30), (4, 'globex', 'gia@globex.com', 'dev-4', 40)`,
	"CREATE TABLE lake.gov.client_access (role_name VARCHAR, client VARCHAR)",
	"INSERT INTO lake.gov.client_access VALUES ('acme_team', 'acme'), ('globex_team', 'globex')",
	"CREATE TABLE lake.public.campaigns (client VARCHAR, campaign VARCHAR, email VARCHAR)",
	"INSERT INTO lake.public.campaigns VALUES ('acme', 'spring', 'ann@acme.com'), ('globex', 'fall', 'gus@globex.com')",
	"CREATE ROLE acme_team",
	"CREATE ROLE globex_team",
	"CREATE ROLE pii_reader",
	`CREATE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
		CASE WHEN is_role_in_session('pii_reader') THEN val ELSE '***' END`,
	`CREATE ROW ACCESS POLICY client_rows AS (c VARCHAR) RETURNS BOOLEAN ->
		is_role_in_session('admin')
		OR EXISTS (SELECT 1 FROM lake.gov.client_access a WHERE a.client = c AND is_role_in_session(a.role_name))`,
	"ALTER TABLE lake.crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email",
	"ALTER TABLE lake.crm.contacts ADD ROW ACCESS POLICY client_rows ON (client)",
}

func startPolicyServer(t *testing.T, configure ...func(*Options)) *catalogServer {
	t.Helper()
	cs := startCatalogServer(t, configure...)
	for _, sql := range policyFixture {
		mustExec(t, cs.admin, sql)
	}
	return cs
}

// texts runs a query and returns its rows, each as its columns joined by
// |, with NULL for nulls.
func texts(t *testing.T, conn *pgx.Conn, sql string, args ...any) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, args...)
	if err != nil {
		t.Errorf("%s: %v", sql, err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			return nil
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprint(v)
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		t.Errorf("%s: %v", sql, err)
	}
	return out
}

func expectRows(t *testing.T, conn *pgx.Conn, sql string, want ...string) {
	t.Helper()
	if got := texts(t, conn, sql); !slices.Equal(got, want) {
		t.Errorf("%s:\n got %q\nwant %q", sql, got, want)
	}
}

// expectRefused checks that a statement fails without showing any of
// secrets in its error.
func expectRefused(t *testing.T, conn *pgx.Conn, sql string, code string, secrets ...string) {
	t.Helper()
	_, err := conn.Exec(context.Background(), sql)
	if err == nil {
		t.Errorf("%s: succeeded, want an error", sql)
		return
	}
	if code != "" && pgCode(err) != code {
		t.Errorf("%s: want %s, got %v", sql, code, err)
	}
	for _, s := range secrets {
		if strings.Contains(err.Error(), s) {
			t.Errorf("%s: the error shows %q: %v", sql, s, err)
		}
	}
}

// rewritten returns a statement as policies rewrite it for user.
func (cs *catalogServer) rewritten(t *testing.T, name, query string) string {
	t.Helper()
	ctx := context.Background()
	user, err := cs.store.Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	es, err := cs.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	if _, err := sess.conn.ExecContext(ctx, "SET search_path = 'lake.public,memory.main'", nil); err != nil {
		t.Fatal(err)
	}
	st, err := prepare(ctx, sess.conn, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer st.close() //nolint:errcheck
	text, _, _, err := cs.srv.policyRewrite(ctx, sess, user, st.stmtType, query, nil, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return text
}

// edited returns a query as the rewrite makes it for user, without
// preparing or checking it.
func (cs *catalogServer) edited(t *testing.T, name, query string) string {
	t.Helper()
	ctx := context.Background()
	user, err := cs.store.Get(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	es, err := cs.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	sess := es.sess
	snap := cs.srv.gov.snapshot(ctx)
	sens, _, err := cs.srv.sensitivity(ctx, sess, snap)
	if err != nil {
		t.Fatal(err)
	}
	privs, err := cs.srv.privileges(ctx, sess, user)
	if err != nil {
		t.Fatal(err)
	}
	rw, err := cs.srv.newRewriter(ctx, sess, user, privs, snap, sens)
	if err != nil {
		t.Fatal(err)
	}
	edits, err := rw.queryEdits(query, nil, 0)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	text, err := applyEdits(query, edits)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

// secrets are the raw values policies hide from rita: every email, and
// the device IDs of globex, whose rows she may not see.
var secrets = []string{"ann@", "al@", "gus@", "gia@", "dev-3", "dev-4", "globex.com"}

func TestMaskingAppliesEverywhere(t *testing.T) {
	t.Parallel()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			cs := startPolicyServer(t)
			rita := cs.user(t, "rita", auth.RoleReader, mode)
			mustExec(t, cs.admin, "GRANT acme_team TO rita")
			const c = "lake.crm.contacts"
			for sql, want := range map[string][]string{
				"SELECT id, email FROM " + c + " ORDER BY id":                                                                                           {"1|***", "2|***"},
				"SELECT count(*) FROM " + c + " WHERE email = 'ann@acme.com'":                                                                           {"0"},
				"SELECT count(*) FROM " + c + " WHERE email LIKE '%acme%' OR email ILIKE 'A%'":                                                          {"0"},
				"SELECT count(*) FROM " + c + " WHERE email = '***'":                                                                                    {"2"},
				"SELECT count(DISTINCT email) FROM " + c:                                                                                                {"1"},
				"SELECT email, count(*) FROM " + c + " GROUP BY email":                                                                                  {"***|2"},
				"SELECT min(email) || max(email) FROM " + c:                                                                                             {"******"},
				"SELECT string_agg(email, ',' ORDER BY id) FROM " + c:                                                                                   {"***,***"},
				"SELECT first_value(email) OVER (ORDER BY id) FROM " + c + " ORDER BY id LIMIT 1":                                                       {"***"},
				"SELECT count(*) FROM " + c + " x JOIN lake.public.campaigns y ON x.email = y.email":                                                    {"0"},
				"SELECT count(*) FROM lake.public.campaigns y WHERE y.email IN (SELECT email FROM " + c + ")":                                           {"0"},
				"SELECT (SELECT email FROM " + c + " WHERE id = 1)":                                                                                     {"***"},
				"WITH x AS (SELECT * FROM " + c + ") SELECT email FROM x ORDER BY id":                                                                   {"***", "***"},
				"SELECT email FROM " + c + " UNION ALL SELECT email FROM " + c + " WHERE id = 1":                                                        {"***", "***", "***"},
				"SELECT t.email FROM " + c + " t WHERE t.id = 2":                                                                                        {"***"},
				"SELECT contacts.email FROM " + c + " WHERE contacts.id = 2":                                                                            {"***"},
				"SELECT * FROM " + c + " WHERE id = 1":                                                                                                  {"1|acme|***|dev-1|10"},
				"SELECT q FROM " + c + " AS x(p, q, r) WHERE p = 1":                                                                                     {"acme"},
				"SELECT x.e FROM " + c + " c, LATERAL (SELECT c.email AS e) x WHERE c.id = 1":                                                           {"***"},
				"FROM " + c + " SELECT email WHERE id = 2":                                                                                              {"***"},
				`SELECT email FROM "lake"."crm"."contacts" WHERE id = 1`:                                                                                {"***"},
				"SELECT EMAIL FROM LAKE . /* c */ CRM.CONTACTS WHERE ID = 1":                                                                            {"***"},
				"SELECT * FROM (PIVOT (SELECT client, email FROM " + c + ") ON client IN ('acme', 'globex') USING max(email))":                          {"***|NULL"},
				"SELECT * FROM (PIVOT " + c + " ON client IN ('acme') USING max(email)) ORDER BY id":                                                    {"1|dev-1|10|***", "2|dev-2|20|***"},
				"SELECT md5(email) = md5('ann@acme.com') FROM " + c + " WHERE id = 1":                                                                   {"false"},
				"SELECT count(*) FROM " + c + " WHERE starts_with(email, 'ann')":                                                                        {"0"},
				"SELECT max(length(email)) FROM " + c:                                                                                                   {"3"},
				"SELECT column_name, min, max FROM (SUMMARIZE " + c + ") WHERE column_name = 'email'":                                                   {"email|***|***"},
				"SELECT max(column_name) FROM (DESCRIBE " + c + ")":                                                                                     {"spend"},
				"WITH RECURSIVE r(n, e) AS (SELECT 1, (SELECT min(email) FROM " + c + ") UNION ALL SELECT n + 1, e FROM r WHERE n < 2) SELECT e FROM r": {"***", "***"},
			} {
				expectRows(t, rita, sql, want...)
			}
			// The whole row, as a struct.
			for _, row := range texts(t, rita, "SELECT c FROM "+c+" c") {
				if !strings.Contains(row, "***") || slices.ContainsFunc(secrets, func(s string) bool { return strings.Contains(row, s) }) {
					t.Errorf("a whole row shows %q", row)
				}
			}
			// Errors that echo values echo the masked ones.
			expectRefused(t, rita, "SELECT CAST(email AS INTEGER) FROM "+c, "", secrets...)
			expectRefused(t, rita, "SELECT error(email) FROM "+c, "", secrets...)
			expectRefused(t, rita, "SELECT stats(email) FROM "+c, "42501")

			// Who has pii_reader sees emails; admins too only through the
			// policy.
			pia := cs.user(t, "pia", auth.RoleReader, mode)
			mustExec(t, cs.admin, "GRANT acme_team TO pia")
			mustExec(t, cs.admin, "GRANT pii_reader TO pia")
			expectRows(t, pia, "SELECT email FROM "+c+" ORDER BY id", "ann@acme.com", "al@acme.com")
			expectRows(t, cs.admin, "SELECT email FROM "+c+" ORDER BY id", "***", "***", "***", "***")
		})
	}
}

func TestRewrittenStatements(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	for q, want := range map[string]string{
		"SELECT email FROM lake.crm.contacts":                                         `FROM "lake"."crm"."contacts" AS "__igloo_t_`,
		"SELECT * FROM lake.crm.contacts c JOIN lake.public.campaigns USING (client)": `) AS c JOIN lake.public.campaigns USING (client)`,
		"UPDATE lake.crm.contacts SET spend = 0 WHERE email = 'x'":                    `UPDATE "lake"."crm"."contacts" AS "__igloo_g_`,
	} {
		if text := cs.rewritten(t, "rita", q); !strings.Contains(text, want) || !strings.Contains(text, `"lake"."gov"."client_access" AS "__igloo_p_`) {
			t.Errorf("%s\n=> %s", q, text)
		}
	}
	// Time travel reads the old snapshot through the policies. (This
	// stand-in catalog has no snapshots; the e2e test reads Iceberg's.)
	if text := cs.edited(t, "rita", "SELECT email FROM lake.crm.contacts x AT (VERSION => 1)"); !strings.Contains(text, `"."contacts" AS "__igloo_t_`) ||
		!strings.Contains(text, `" AT (VERSION => 1)`) || !strings.HasSuffix(text, ") AS x") {
		t.Errorf("time travel: %s", text)
	}
	// Nothing protected: as written.
	if q := "SELECT * FROM lake.public.campaigns"; cs.rewritten(t, "rita", q) != q {
		t.Errorf("a statement on an unprotected table was rewritten")
	}
	// An admin with pii_reader is exempt from both policies.
	mustExec(t, cs.admin, "GRANT pii_reader TO igloo")
	if q := "SELECT * FROM lake.crm.contacts"; cs.rewritten(t, "igloo", q) != q {
		t.Errorf("a statement of a user the policies exempt was rewritten: %s", cs.rewritten(t, "igloo", q))
	}
}
