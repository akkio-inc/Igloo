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

// TestMacrosCannotMisleadTheChecks replaces, with temporary macros of the
// session, the functions Igloo's own analysis calls on the session's
// connection: they must not change what Igloo finds a statement reads.
func TestMacrosCannotMisleadTheChecks(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	for _, sql := range []string{
		"CREATE TEMP MACRO lower(x) AS 'campaigns'",
		`CREATE TEMP MACRO json_serialize_sql(q, skip_null := true, skip_empty := true) AS
			'{"error":false,"statements":[{"node":{"type":"SELECT_NODE","select_list":[]}}]}'`,
		"CREATE TEMP MACRO current_setting(x) AS 'lake.public'",
		"CREATE TEMP MACRO current_database() AS 'memory'",
		"CREATE TEMP MACRO duckdb_tables() AS TABLE SELECT 'lake' AS database_name, 'public' AS schema_name, 'contacts' AS table_name",
		"CREATE TEMP MACRO duckdb_views() AS TABLE SELECT 1 AS x WHERE false",
		"CREATE TEMP MACRO duckdb_functions() AS TABLE SELECT 1 AS x WHERE false",
	} {
		mustExec(t, walt, sql)
	}
	for _, sql := range []string{
		"SELECT email FROM " + c,
		"SELECT email FROM crm.contacts",
		"SELECT device_id FROM " + c + " WHERE id = 3",
	} {
		rows, err := walt.Query(context.Background(), sql)
		if err != nil {
			if slices.ContainsFunc(secrets, func(s string) bool { return strings.Contains(err.Error(), s) }) {
				t.Errorf("%s: %v", sql, err)
			}
			continue
		}
		for rows.Next() {
			var v *string
			if err := rows.Scan(&v); err == nil && v != nil && (strings.Contains(*v, "@") || *v == "dev-3") {
				t.Errorf("%s: shows %q", sql, *v)
			}
		}
		rows.Close()
	}
}

// TestViewsWithEscapedNames reads a protected table through views that
// name it with escapes, which only DuckDB decodes.
func TestViewsWithEscapedNames(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	for i, def := range []string{
		`SELECT * FROM lake.crm.U&"\0063ontacts"`,
		`SELECT * FROM lake.crm.U&"cont\0061cts" WHERE id > 0`,
	} {
		name := fmt.Sprintf("escaped%d", i)
		if _, err := walt.Exec(context.Background(), "CREATE TEMP VIEW "+name+" AS "+def); err != nil {
			continue
		}
		for _, sql := range []string{"SELECT email FROM " + name + " ORDER BY id", "SELECT device_id FROM " + name + " WHERE id = 3"} {
			got, err := walt.Query(context.Background(), sql)
			if err != nil {
				continue
			}
			for got.Next() {
				var v *string
				if err := got.Scan(&v); err == nil && v != nil && (strings.Contains(*v, "@") || *v == "dev-3") {
					t.Errorf("%s: shows %q", sql, *v)
				}
			}
			got.Close()
		}
	}
}

// TestSettingsCannotChangePolicies changes, in sessions, the settings that
// change what policy expressions mean: statements that policies apply to
// are refused until they are reset, prepared ones too.
func TestSettingsCannotChangePolicies(t *testing.T) {
	t.Parallel()
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, `CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
		CASE WHEN 1 / 2 = 0 THEN val ELSE '***' END`)
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	carl := cs.user(t, "carl", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	for _, name := range []string{"rita", "carl"} {
		mustExec(t, cs.admin, "GRANT acme_team TO "+name)
	}
	const q = "SELECT email FROM lake.crm.contacts ORDER BY id"
	for _, set := range [][2]string{
		{"integer_division", "true"}, {"default_order", "'DESC'"},
		{"default_null_order", "'nulls_first'"}, {"calendar", "'japanese'"},
	} {
		for _, conn := range []*pgx.Conn{rita, carl} {
			expectRows(t, conn, q, "***", "***")
			mustExec(t, conn, "SET "+set[0]+" = "+set[1])
			_, err := conn.Exec(context.Background(), q)
			if pgCode(err) != "42501" || !strings.Contains(fmt.Sprint(err), set[0]) {
				t.Errorf("%s after SET %s: %v", q, set[0], err)
			}
			mustExec(t, conn, "SELECT count(*) FROM lake.public.campaigns")
			mustExec(t, conn, "RESET "+set[0])
			expectRows(t, conn, q, "***", "***")
		}
	}
	// The same value, written otherwise, is no change.
	mustExec(t, rita, "SET default_order = 'asc'")
	expectRows(t, rita, q, "***", "***")
}
