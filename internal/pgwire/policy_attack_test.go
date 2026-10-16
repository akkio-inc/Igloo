package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Adversarial probes against the masking and row-access rewrite. Each tries,
// as a non-admin, to read a raw masked value or a hidden row, or to bypass a
// policy. A probe that merely errors (unsupported, refused) is not a leak;
// only a secret in the output is. secrets (policies_test.go) lists every raw
// email and everything about globex's hidden rows.
//
// These began as a security review's throwaway probes; they are kept as
// regression tests. leaked() checks results and error text alike.

func leaked(conn *pgx.Conn, sql string) string {
	var b strings.Builder
	rows, err := conn.Query(context.Background(), sql)
	if err == nil {
		for rows.Next() {
			vals, verr := rows.Values()
			if verr != nil {
				err = verr
				break
			}
			for _, v := range vals {
				fmt.Fprintf(&b, "%v\u0001", v)
			}
		}
		rows.Close()
		if err == nil {
			err = rows.Err()
		}
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

func noLeak(t *testing.T, conn *pgx.Conn, label, sql string) {
	t.Helper()
	if s := leaked(conn, sql); s != "" {
		t.Errorf("LEAK [%s]: %q via\n  %s", label, s, sql)
	}
}

func TestPolicyAttacks(t *testing.T) {
	const c = "lake.crm.contacts"

	newRita := func(t *testing.T) *pgx.Conn {
		cs := startPolicyServer(t)
		rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO rita")
		return rita
	}

	// Unicode-escaped identifiers and strings for the table name. sqllex
	// does not decode U&"..." / U&'...', so a name trigger would miss them;
	// Igloo treats any U& token as a trigger (quotedText.opaque), so the
	// statement is analyzed and either rewritten or refused, never run raw.
	// DuckDB 1.5 does not implement U& either, so these also simply error.
	t.Run("unicode_escape_names", func(t *testing.T) {
		rita := newRita(t)
		mustExec(t, rita, "SET search_path = 'lake.crm'")
		for _, sql := range []string{
			`SELECT email FROM lake.crm.U&"\0063ontacts" WHERE id = 1`,
			`SELECT email FROM lake.crm.U&"cont\0061cts" WHERE id = 1`,
			`SELECT email FROM U&'\0063ontacts' WHERE id = 1`,
			`SELECT email FROM U&'cont\0061cts' WHERE id = 1`,
		} {
			noLeak(t, rita, "u&name", sql)
		}
	})

	// String table names with a '/' or '\' separator, which tokensOf does
	// not split into a "contacts" token.
	t.Run("slash_string_name", func(t *testing.T) {
		rita := newRita(t)
		for _, sql := range []string{
			`SELECT email FROM 'crm/contacts' WHERE id = 1`,
			`SELECT email FROM 'crm\contacts' WHERE id = 1`,
			`SELECT email FROM 'lake/crm/contacts' WHERE id = 1`,
		} {
			noLeak(t, rita, "slash-name", sql)
		}
	})

	// CREATE TYPE ... AS ENUM (subquery): DuckDB may run the subquery while
	// preparing, as PIVOT does. No enum may materialize the masked values.
	t.Run("enum_from_subquery", func(t *testing.T) {
		cs := startPolicyServer(t)
		walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO walt")
		for i, sql := range []string{
			"CREATE TYPE leaky0 AS ENUM (SELECT DISTINCT email FROM " + c + ")",
			"CREATE TYPE leaky1 AS ENUM (SELECT device_id FROM " + c + ")",
		} {
			noLeak(t, walt, "enum", sql)
			noLeak(t, walt, "enum-range", fmt.Sprintf("SELECT unnest(enum_range(NULL::leaky%d))", i))
		}
	})

	// Shadowing the functions a row-access body calls with temp macros.
	t.Run("shadow_rowpolicy_operators", func(t *testing.T) {
		cs := startPolicyServer(t)
		walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO walt")
		for _, mac := range []string{
			`CREATE TEMP MACRO "="(a, b) AS true`,
			`CREATE TEMP MACRO list_contains(a, b) AS true`,
			`CREATE TEMP MACRO is_role_in_session(r) AS true`,
			`CREATE TEMP MACRO "exists"(q) AS true`,
		} {
			_, _ = walt.Exec(context.Background(), mac)
		}
		noLeak(t, walt, "shadow-rows", "SELECT client, device_id FROM "+c+" ORDER BY id")
		noLeak(t, walt, "shadow-count", "SELECT count(*) FROM "+c+" WHERE client = 'globex'")
	})

	// EXPORT DATABASE dumps every table's rows to files; its text names no
	// protected table.
	t.Run("export_database", func(t *testing.T) {
		cs := startPolicyServer(t)
		walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
		dir := t.TempDir()
		noLeak(t, walt, "export", "EXPORT DATABASE '"+dir+"'")
		noLeak(t, walt, "export-read", "SELECT * FROM glob('"+dir+"/**')")
	})

	// A view in the catalog schema (lake) that reads the protected table.
	t.Run("catalog_schema_view", func(t *testing.T) {
		cs := startPolicyServer(t)
		rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO rita")
		mustExec(t, cs.admin, "CREATE VIEW lake.public.leak_v AS SELECT id, email, device_id, client FROM "+c)
		noLeak(t, rita, "catalog-view", "SELECT * FROM lake.public.leak_v ORDER BY id")
	})

	// Grouping/filtering that could push a predicate onto the raw filter
	// column before the row-access guard, revealing a hidden row by error.
	t.Run("predicate_pushdown", func(t *testing.T) {
		rita := newRita(t)
		for _, sql := range []string{
			"SELECT count(*) FROM " + c + " WHERE device_id = 'dev-3'",
			"SELECT count(*) FROM " + c + " WHERE length(device_id) > 0 AND client = 'globex'",
			"SELECT count(*) FROM " + c + " GROUP BY device_id HAVING max(client) = 'globex'",
			"SELECT count(*) FROM " + c + " QUALIFY row_number() OVER (ORDER BY device_id) = 1",
		} {
			noLeak(t, rita, "pushdown", sql)
		}
	})

	// The guarded settings a reader may change, which change what policy
	// expressions mean: Igloo refuses statements that policies apply to
	// while they are not at their defaults (see checkSettings).
	t.Run("guarded_settings_blocked", func(t *testing.T) {
		cs := startPolicyServer(t)
		rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO rita")
		mustExec(t, cs.admin, `CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
			CASE WHEN 1 / 2 = 0 THEN val ELSE '***' END`)
		for _, s := range []string{"integer_division", "default_order", "default_null_order", "calendar"} {
			v := "true"
			if s != "integer_division" {
				v = "'DESC'"
				if s == "default_null_order" {
					v = "'nulls_first'"
				} else if s == "calendar" {
					v = "'japanese'"
				}
			}
			mustExec(t, rita, "SET "+s+" = "+v)
			if _, err := rita.Exec(context.Background(), "SELECT email FROM "+c); pgCode(err) != "42501" {
				t.Errorf("SET %s: want 42501, got %v", s, err)
			}
			noLeak(t, rita, "setting", "SELECT email FROM "+c)
			mustExec(t, rita, "RESET "+s)
		}
	})

	// timezone_is_session_by_design documents that the session time zone is
	// NOT pinned (as in Snowflake): a policy that reads a time-of-day in the
	// session zone can be flipped by SET timezone. The documented guidance
	// is to compare TIMESTAMPTZ values instead. This logs; it does not fail.
	t.Run("timezone_is_session_by_design", func(t *testing.T) {
		cs := startPolicyServer(t)
		rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
		mustExec(t, cs.admin, "GRANT acme_team TO rita")
		if _, err := cs.admin.Exec(context.Background(), `CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
			CASE WHEN extract('hour' FROM TIMESTAMPTZ '2020-01-01 00:30:00+00') = 0 THEN '***' ELSE val END`); err != nil {
			t.Skipf("admin could not create the policy: %v", err)
		}
		for _, tz := range []string{"UTC", "America/Los_Angeles", "Asia/Kolkata"} {
			if _, err := rita.Exec(context.Background(), "SET timezone = '"+tz+"'"); err != nil {
				continue
			}
			out := texts(t, rita, "SELECT email FROM "+c+" WHERE id = 1")
			t.Logf("tz=%s -> %q (session zone is by design not pinned)", tz, out)
		}
	})
}
