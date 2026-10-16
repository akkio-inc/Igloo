package pgwire

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestRowAccessPolicy(t *testing.T) {
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	mustExec(t, cs.admin, "GRANT acme_team TO rita")

	for sql, want := range map[string][]string{
		"SELECT count(*), sum(spend)::INTEGER FROM " + c:                                                                     {"2|30"},
		"SELECT count(*) FROM " + c + " WHERE client = 'globex'":                                                             {"0"},
		"SELECT DISTINCT client FROM " + c:                                                                                   {"acme"},
		"SELECT count(*) FROM " + c + " a JOIN " + c + " b USING (client)":                                                   {"4"},
		"SELECT count(*) FROM lake.public.campaigns p LEFT JOIN " + c + " c ON p.client = c.client WHERE c.id IS NULL":       {"1"},
		"SELECT max(spend) OVER () FROM " + c + " LIMIT 1":                                                                   {"20"},
		"SELECT (SELECT count(*) FROM " + c + " x WHERE x.client = p.client) FROM lake.public.campaigns p ORDER BY p.client": {"2", "0"},
		"SELECT device_id FROM " + c + " WHERE id = 3":                                                                       nil,
		"SELECT count(*) FROM " + c + " WHERE id IN (3, 4)":                                                                  {"0"},
	} {
		expectRows(t, rita, sql, want...)
	}
	// Conditions on hidden rows never see their values, even when DuckDB
	// evaluates them before the policy: a failing one would show them in
	// its error.
	for _, sql := range []string{
		"SELECT count(*) FROM " + c + " WHERE CASE WHEN client = 'globex' THEN error('LEAK ' || device_id) END IS NULL",
		"SELECT count(*) FROM " + c + " WHERE CAST(device_id AS INTEGER) > 0 OR true",
		"SELECT count(*) FROM " + c + " x JOIN lake.public.campaigns p ON CAST(x.device_id AS INTEGER) = 1 OR x.client = p.client",
		"SELECT count(*) FROM " + c + " WHERE 1 // (spend - 30) > 0 OR true",
		"SELECT count(*) FROM " + c + " WHERE EXISTS (SELECT error(x.device_id) FROM " + c + " x WHERE x.id = 3)",
	} {
		_, err := rita.Exec(context.Background(), sql)
		if err != nil && slices.ContainsFunc(secrets, func(s string) bool { return strings.Contains(err.Error(), s) }) {
			t.Errorf("%s: the error shows a hidden value: %v", sql, err)
		}
	}

	// A user with the none role needs SELECT on the table, not on the
	// table the policy reads.
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, nina, "SELECT * FROM "+c, "SELECT on table crm.contacts")
	mustExec(t, cs.admin, "GRANT SELECT ON TABLE "+c+" TO nina")
	expectRows(t, nina, "SELECT count(*) FROM "+c, "0")
	mustExec(t, cs.admin, "GRANT globex_team TO nina")
	expectRows(t, nina, "SELECT id, email FROM "+c+" ORDER BY id", "3|***", "4|***")
	expectDenied(t, nina, "SELECT * FROM lake.gov.client_access", "gov.client_access")
	expectDenied(t, nina, "SELECT * FROM "+c+" WHERE client IN (SELECT client FROM lake.gov.client_access)", "gov.client_access")

	// Admins are exempt here only because the policy says so.
	expectRows(t, cs.admin, "SELECT count(*) FROM "+c, "4")
	// Someone in no team sees nothing.
	bob := cs.user(t, "bob", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectRows(t, bob, "SELECT count(*) FROM "+c, "0")

	// The roles as a list, and the user's name.
	mustExec(t, cs.admin, `CREATE ROW ACCESS POLICY team_rows AS (c VARCHAR) RETURNS BOOLEAN ->
		list_contains(current_available_roles(), c || '_team') OR current_user = 'bob'`)
	mustExec(t, cs.admin, "ALTER TABLE lake.public.campaigns ADD ROW ACCESS POLICY team_rows ON (client)")
	expectRows(t, rita, "SELECT campaign FROM lake.public.campaigns", "spring")
	expectRows(t, bob, "SELECT count(*) FROM lake.public.campaigns", "2")
	expectRows(t, cs.admin, "SELECT count(*) FROM lake.public.campaigns", "0")
}

func TestPolicyDML(t *testing.T) {
	ctx := context.Background()
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	affected := func(sql string) int64 {
		t.Helper()
		tag, err := walt.Exec(ctx, sql)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			return -1
		}
		return tag.RowsAffected()
	}

	if n := affected("UPDATE " + c + " SET spend = spend + 1"); n != 2 {
		t.Errorf("UPDATE changed %d rows", n)
	}
	if n := affected("DELETE FROM " + c + " WHERE email = 'ann@acme.com'"); n != 0 {
		t.Errorf("a DELETE by a masked value deleted %d rows", n)
	}
	if n := affected("UPDATE " + c + " SET spend = 0 WHERE id = 3"); n != 0 {
		t.Errorf("an UPDATE of a hidden row changed %d rows", n)
	}
	if n := affected("UPDATE " + c + " SET device_id = email WHERE id = 2"); n != 1 {
		t.Errorf("UPDATE from a masked column: %d", n)
	}
	if n := affected("UPDATE " + c + " AS x SET spend = 5 FROM lake.public.campaigns p WHERE p.client = x.client AND x.id = 1"); n != 1 {
		t.Errorf("UPDATE ... FROM: %d", n)
	}
	if n := affected("DELETE FROM " + c + " AS x USING lake.public.campaigns p WHERE p.email = x.email"); n != 0 {
		t.Errorf("DELETE ... USING by a masked value: %d", n)
	}
	// Row 3 is hidden, so only row 2 matches.
	mustExec(t, walt, "MERGE INTO "+c+" t USING (SELECT 3 AS id UNION ALL SELECT 2) s ON t.id = s.id "+
		"WHEN MATCHED AND t.email = '***' THEN UPDATE SET spend = t.spend * 2")
	if n := affected("INSERT INTO " + c + " VALUES (3, 'acme', 'new@acme.com', 'dev-9', 1)"); n != 1 {
		t.Errorf("INSERT: %d", n)
	}
	expectRows(t, cs.admin, "SELECT id, client, device_id, spend FROM "+c+" ORDER BY id, client",
		"1|acme|dev-1|5", "2|acme|***|42", "3|acme|dev-9|1", "3|globex|dev-3|30", "4|globex|dev-4|40")
	if n := affected("DELETE FROM " + c); n != 3 {
		t.Errorf("DELETE deleted %d rows", n)
	}
	expectRows(t, cs.admin, "SELECT count(*) FROM "+c, "2")

	for _, sql := range []string{
		"TRUNCATE " + c,
		"MERGE INTO " + c + " t USING (SELECT 1 AS id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE",
		"UPDATE " + c + " SET spend = 1 RETURNING email",
		"WITH x AS (SELECT 1) DELETE FROM " + c,
		"MERGE INTO " + c + " t USING (SELECT 9 AS id) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (9, 'acme', 'x', 'y', 1)",
	} {
		expectRefused(t, walt, sql, "42501")
	}
	expectRefused(t, walt, "INSERT OR REPLACE INTO "+c+" VALUES (5, 'acme', 'x', 'y', 1)", "")
	expectRows(t, cs.admin, "SELECT count(*) FROM "+c, "2")

	// What a statement copies out of a protected table is what its user
	// sees.
	mustExec(t, walt, "INSERT INTO "+c+" VALUES (7, 'acme', 'seven@acme.com', 'dev-7', 7)")
	mustExec(t, walt, "CREATE TABLE lake.public.copied AS SELECT * FROM "+c)
	expectRows(t, cs.admin, "SELECT id, email FROM lake.public.copied", "7|***")
	mustExec(t, walt, "INSERT INTO lake.public.campaigns SELECT client, 'copy', email FROM "+c)
	expectRows(t, cs.admin, "SELECT email FROM lake.public.campaigns WHERE campaign = 'copy'", "***")

	// A user with the none role, granted SELECT and the writes, through
	// the same checks of what the statement reads.
	nina := cs.user(t, "nina", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT SELECT, UPDATE, DELETE ON TABLE "+c+" TO nina")
	mustExec(t, cs.admin, "GRANT globex_team TO nina")
	if tag, err := nina.Exec(ctx, "UPDATE "+c+" SET spend = spend + 100 WHERE email = '***'"); err != nil || tag.RowsAffected() != 2 {
		t.Errorf("none role UPDATE: %v %v", tag, err)
	}
	if tag, err := nina.Exec(ctx, "DELETE FROM "+c+" WHERE id = 7"); err != nil || tag.RowsAffected() != 0 {
		t.Errorf("none role DELETE of a hidden row: %v %v", tag, err)
	}
	expectDenied(t, nina, "UPDATE "+c+" SET spend = (SELECT count(*) FROM lake.public.campaigns)", "public.campaigns")
	expectRows(t, cs.admin, "SELECT id, spend FROM "+c+" ORDER BY id", "3|130", "4|140", "7|7")
}
