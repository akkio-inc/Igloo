package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// shows runs sql and returns the first of secrets that appears in its rows
// or its error, or "". A statement may fail; it must not show a secret.
func shows(conn *pgx.Conn, sql string, secrets ...string) string {
	var b strings.Builder
	rows, err := conn.Query(context.Background(), sql)
	if err == nil {
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				break
			}
			fmt.Fprint(&b, vals...)
		}
		rows.Close()
		err = rows.Err()
	}
	if err != nil {
		b.WriteString(err.Error())
	}
	for _, s := range secrets {
		if strings.Contains(b.String(), s) {
			return s
		}
	}
	return ""
}

func expectHidden(t *testing.T, conn *pgx.Conn, sql string, secrets ...string) {
	t.Helper()
	if s := shows(conn, sql, secrets...); s != "" {
		t.Errorf("%s: shows %q", sql, s)
	}
}

// TestPoliciesHoldInEveryForm reads a protected table in the forms of
// DuckDB's SQL that build rows, lists and structs out of it, or name it
// in unusual ways.
func TestPoliciesHoldInEveryForm(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")
	mustExec(t, cs.admin, "CREATE VIEW lake.public.contact_view AS SELECT id, email, device_id, client FROM "+c)
	for _, sql := range []string{
		"TABLE " + c,
		"FROM " + c,
		"SELECT * EXCLUDE (id) FROM " + c + " ORDER BY id",
		"SELECT * REPLACE (upper(email) AS email) FROM " + c + " ORDER BY id",
		"SELECT COLUMNS('e.*|d.*') FROM " + c + " ORDER BY id",
		"UNPIVOT " + c + " ON email, device_id INTO NAME k VALUE v",
		"SELECT * FROM (PIVOT " + c + " ON client IN ('acme', 'globex') USING max(device_id))",
		"SELECT * FROM (PIVOT " + c + " ON email IN ('ann@acme.com', '***') USING count(*))",
		"SELECT * FROM " + c + " USING SAMPLE 100%",
		"SELECT {'e': email, 'd': device_id} FROM " + c,
		"SELECT [email, device_id] FROM " + c,
		"SELECT to_json(t) FROM " + c + " t",
		"SELECT row(email, device_id) FROM " + c,
		"SELECT list(email), list(device_id), string_agg(email, ',') FROM " + c,
		"SELECT max(email), max(device_id) FROM " + c + " WHERE client = 'globex'",
		"SELECT count(*) FROM " + c + " GROUP BY device_id HAVING max(client) = 'globex'",
		"SELECT device_id FROM " + c + " QUALIFY row_number() OVER (ORDER BY device_id DESC) = 1",
		`SELECT email FROM "lake".crm."contacts" WHERE id = 1`,
		"WITH contacts AS (SELECT 'z' AS email) SELECT email FROM " + c + " WHERE id = 1",
		"SELECT * FROM lake.public.contact_view ORDER BY id",
	} {
		expectHidden(t, rita, sql, secrets...)
	}

	// A type made from a query's rows.
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	for i, sql := range []string{
		"CREATE TYPE leaky0 AS ENUM (SELECT DISTINCT email FROM " + c + ")",
		"CREATE TYPE leaky1 AS ENUM (SELECT device_id FROM " + c + ")",
	} {
		expectHidden(t, walt, sql, secrets...)
		expectHidden(t, walt, fmt.Sprintf("SELECT unnest(enum_range(NULL::leaky%d))", i), secrets...)
	}

	// A temporary table named like the protected table, and one named
	// like the policy's mapping table, first on the search path.
	mustExec(t, walt, "CREATE TEMP TABLE contacts (id INTEGER, email VARCHAR, device_id VARCHAR, client VARCHAR)")
	mustExec(t, walt, "INSERT INTO contacts VALUES (99, 'mine@x', 'mydev', 'acme')")
	mustExec(t, walt, "CREATE TEMP TABLE client_access (role_name VARCHAR, client VARCHAR)")
	mustExec(t, walt, "INSERT INTO client_access VALUES ('writer', 'globex'), ('acme_team', 'globex')")
	mustExec(t, walt, "SET search_path = 'temp.main,lake.gov,lake.crm'")
	// A name that could mean the protected table must be qualified.
	expectRefused(t, walt, "SELECT email FROM contacts", "42501", secrets...)
	expectRows(t, walt, "SELECT email FROM temp.main.contacts", "mine@x")
	expectRows(t, walt, "SELECT email, client FROM "+c+" ORDER BY id", "***|acme", "***|acme")
}

// TestRowPolicyWithoutMasks writes to a table that has only a row access
// policy: rows it hides stay out of reach of RETURNING and subqueries.
func TestRowPolicyWithoutMasks(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	mustExec(t, cs.admin, "ALTER TABLE "+c+" ALTER COLUMN email UNSET MASKING POLICY")
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	hidden := []string{"gus@", "gia@", "dev-3", "dev-4", "globex.com"}
	for _, sql := range []string{
		"UPDATE " + c + " SET spend = spend WHERE id = 3 RETURNING email, device_id",
		"UPDATE " + c + " SET spend = spend WHERE id = 1 RETURNING (SELECT device_id FROM " + c + " WHERE id = 3)",
		"DELETE FROM " + c + " WHERE id = 3 RETURNING device_id",
		"UPDATE " + c + " SET email = (SELECT max(email) FROM " + c + ") WHERE id = 1 RETURNING email",
	} {
		expectHidden(t, walt, sql, hidden...)
	}
	expectRows(t, cs.admin, "SELECT count(*) FROM "+c, "4")
	expectRows(t, walt, "SELECT email FROM "+c+" ORDER BY id", "ann@acme.com", "al@acme.com")
}
