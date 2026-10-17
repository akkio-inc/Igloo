package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

// TestAlterCannotReadProtectedRows covers a bypass: expression-carrying DDL
// evaluates its expression over every raw row of the table, hidden ones
// included, so it could read masked values and rows a row access policy
// hides through the error channel, a failing cast, or division by zero. Such
// DDL on a policied table is refused, for writers and admins alike. Each
// statement runs on its own connection, since some are refused by DuckDB
// itself at prepare and that leaves the session's transaction aborted.
func TestAlterCannotReadProtectedRows(t *testing.T) {
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	mustExec(t, cs.admin, "CREATE USER walt WITH PASSWORD 'walt-password' ROLE writer")
	mustExec(t, cs.admin, "GRANT acme_team TO walt")
	conn := func() *pgx.Conn { return connect(t, cs.url("walt", "walt-password"), pgx.QueryExecModeSimpleProtocol) }

	// deniedNoLeak is the security property: the statement does not succeed
	// and no secret (a raw email, or anything of a hidden globex row) appears
	// in its error. It does not pin the SQLSTATE, since DuckDB refuses some
	// forms itself before Igloo's check.
	deniedNoLeak := func(sql string) {
		t.Helper()
		cn := conn()
		defer cn.Close(context.Background())
		_, err := cn.Exec(context.Background(), sql)
		if err == nil {
			t.Errorf("%s: succeeded, want refused", sql)
			return
		}
		for _, sec := range secrets {
			if strings.Contains(err.Error(), sec) {
				t.Errorf("%s: error leaks %q: %v", sql, sec, err)
			}
		}
	}
	// refused is deniedNoLeak and also checks Igloo's own 42501, to prove the
	// fix (not just DuckDB) stops these.
	refused := func(sql string) {
		t.Helper()
		cn := conn()
		defer cn.Close(context.Background())
		expectRefused(t, cn, sql, "42501", secrets...)
	}

	// These reach Igloo's check (DuckDB prepares them; the expression would
	// run over the rows at execution): refused with 42501 before they run.
	for _, sql := range []string{
		"ALTER TABLE " + c + " ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE " + c + " ALTER COLUMN spend TYPE BIGINT USING error(email)",
		"ALTER TABLE " + c + " ALTER COLUMN device_id SET DATA TYPE VARCHAR USING " +
			"CASE WHEN client = 'globex' THEN error('hidden ' || device_id) ELSE device_id END",
		"ALTER TABLE " + c + " ALTER COLUMN spend SET DATA TYPE INTEGER USING CAST(email AS INTEGER)",
		"ALTER TABLE " + c + " ALTER COLUMN spend SET DATA TYPE INTEGER USING " +
			"CASE WHEN client = 'globex' THEN 1 // 0 ELSE spend END",
		// A type change with no USING still casts the column over every row.
		"ALTER TABLE " + c + " ALTER COLUMN spend SET DATA TYPE BIGINT",
		"ALTER TABLE " + c + " ALTER COLUMN email SET DATA TYPE VARCHAR(2)",
		// Index build reads every row; an expression index evaluates over them.
		"CREATE INDEX leak_idx ON " + c + " (email)",
		"CREATE INDEX leak_idx2 ON " + c + " ((email || client))",
		// An unprotected table whose ALTER reads a protected one in a subquery.
		"ALTER TABLE lake.public.campaigns ALTER COLUMN campaign SET DATA TYPE VARCHAR USING " +
			"(SELECT error(email) FROM " + c + " LIMIT 1)",
	} {
		refused(sql)
		// Admins are not exempt from the masking policy here, so the same DDL
		// is refused for them too, like a rename.
		cn := cs.admin
		expectRefused(t, cn, sql, "42501", secrets...)
	}

	// These DuckDB refuses itself at prepare (it does not evaluate a DEFAULT
	// over existing rows, nor add a CHECK, generated column or side-effecting
	// index after creation). They are denied with no leak; Igloo's check
	// stands behind them should a later DuckDB allow the form.
	for _, sql := range []string{
		"ALTER TABLE " + c + " ADD COLUMN leak VARCHAR DEFAULT error(email)",
		"ALTER TABLE " + c + " ADD COLUMN okc INTEGER CHECK (error(email) IS NOT NULL)",
		"ALTER TABLE " + c + " ADD COLUMN g VARCHAR GENERATED ALWAYS AS (email)",
		"ALTER TABLE " + c + " ADD COLUMN g2 VARCHAR AS (device_id)",
		"CREATE INDEX leak_idx3 ON " + c + " ((CASE WHEN client = 'globex' THEN error(device_id) ELSE '' END))",
	} {
		deniedNoLeak(sql)
	}

	// Expression-free DDL on the same table still works: it reads no values.
	w := conn()
	defer w.Close(context.Background())
	for _, sql := range []string{
		"ALTER TABLE " + c + " ADD COLUMN note VARCHAR",
		"ALTER TABLE " + c + " ADD COLUMN n INTEGER DEFAULT 0",
		"ALTER TABLE " + c + " ALTER COLUMN note SET DEFAULT 'x'",
		"ALTER TABLE " + c + " DROP COLUMN note",
	} {
		if _, err := w.Exec(context.Background(), sql); err != nil {
			t.Errorf("%s: expression-free DDL refused: %v", sql, err)
		}
	}

	// The ONLY modifier, a trailing '*', IF EXISTS, parentheses, comments and
	// case variants must not slip the target past the check. Each would, on
	// 993d860, read "only" as the table name and leak.
	for _, sql := range []string{
		"ALTER TABLE ONLY " + c + " ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE " + c + " * ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE IF EXISTS ONLY " + c + " ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE ONLY (" + c + ") ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE /* x */ ONLY /* y */ " + c + " ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		"ALTER TABLE ONLY LAKE.CRM.CONTACTS ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)",
		`ALTER TABLE ONLY "lake"."crm"."contacts" ALTER COLUMN spend SET DATA TYPE BIGINT USING error(email)`,
	} {
		refused(sql)
		expectRefused(t, cs.admin, sql, "42501", secrets...)
	}
	// DuckDB rejects CREATE INDEX ... ON ONLY at parse; it is denied with no
	// leak. Igloo's check stands behind it should the grammar be accepted.
	deniedNoLeak("CREATE INDEX leak_only ON ONLY " + c + " (id)")
	// ONLY and '*' must not slip a rename of a protected column or table past
	// the check either.
	for _, sql := range []string{
		"ALTER TABLE ONLY " + c + " RENAME COLUMN email TO e2",
		"ALTER TABLE " + c + " * RENAME COLUMN email TO e2",
		"ALTER TABLE ONLY " + c + " RENAME TO contacts_renamed",
		"ALTER TABLE IF EXISTS ONLY " + c + " RENAME TO contacts_renamed",
	} {
		expectRefused(t, cs.admin, sql, "42501", secrets...)
	}

	// A table with no policy is unaffected: expression DDL on it is allowed.
	mustExec(t, cs.admin, "CREATE TABLE lake.public.plain (id INTEGER, s VARCHAR)")
	mustExec(t, cs.admin, "INSERT INTO lake.public.plain VALUES (1, '2')")
	if _, err := w.Exec(context.Background(), "ALTER TABLE lake.public.plain ALTER COLUMN s SET DATA TYPE INTEGER USING CAST(s AS INTEGER)"); err != nil {
		t.Errorf("type change on an unprotected table refused: %v", err)
	}
	// ONLY on an unprotected table still works, and renaming a non-protected
	// column of a protected table (via ONLY) is still allowed.
	if _, err := w.Exec(context.Background(), "ALTER TABLE ONLY lake.public.plain ADD COLUMN extra INTEGER"); err != nil {
		t.Errorf("ALTER TABLE ONLY on an unprotected table refused: %v", err)
	}
	if _, err := w.Exec(context.Background(), "ALTER TABLE ONLY "+c+" RENAME COLUMN spend TO spend2"); err != nil {
		t.Errorf("renaming a non-protected column via ONLY refused: %v", err)
	}
}

// TestAlterUnsetThenAlter shows the escape hatch the refusals point to:
// unset the policies, run the DDL, set them again.
func TestAlterUnsetThenAlter(t *testing.T) {
	cs := startPolicyServer(t)
	const c = "lake.crm.contacts"
	mustExec(t, cs.admin, "ALTER TABLE "+c+" ALTER COLUMN email UNSET MASKING POLICY")
	mustExec(t, cs.admin, "ALTER TABLE "+c+" DROP ROW ACCESS POLICY client_rows")
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	if _, err := walt.Exec(context.Background(), "ALTER TABLE "+c+" ADD COLUMN flag INTEGER DEFAULT 0"); err != nil {
		t.Errorf("after unsetting policies, DDL refused: %v", err)
	}
	if _, err := walt.Exec(context.Background(), "ALTER TABLE "+c+" ALTER COLUMN spend SET DATA TYPE BIGINT"); err != nil {
		t.Errorf("after unsetting policies, type change refused: %v", err)
	}
}
