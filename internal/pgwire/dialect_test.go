package pgwire

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// showDialect returns the session's sql_dialect.
func showDialectOf(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var d string
	if err := conn.QueryRow(context.Background(), "SHOW sql_dialect").Scan(&d); err != nil {
		t.Fatal(err)
	}
	return d
}

// resultColumns returns the names of a query's result columns.
func resultColumns(t *testing.T, conn *pgx.Conn, sql string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var names []string
	for _, f := range rows.FieldDescriptions() {
		names = append(names, f.Name)
	}
	for rows.Next() {
	}
	return names
}

func TestSnowflakeDialect(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ts.admin(t, mode)
			if d := showDialectOf(t, conn); d != "duckdb" {
				t.Fatalf("default dialect %q", d)
			}
			if _, err := conn.Exec(ctx, "SELECT IFF(true, 1, 2)"); err == nil {
				t.Fatal("IFF ran in the DuckDB dialect")
			}
			mustExec(t, conn, "SET sql_dialect = 'snowflake'")
			if d := showDialectOf(t, conn); d != "snowflake" {
				t.Fatalf("after SET: %q", d)
			}
			var n int64
			var day, s string
			if err := conn.QueryRow(ctx, `SELECT IFF(true, 1, 2), TO_CHAR(DATEADD(day, 1, '2024-01-31'::date), 'YYYY-MM-DD'), 'it\'s'`).Scan(&n, &day, &s); err != nil {
				t.Fatal(err)
			}
			if n != 1 || day != "2024-02-01" || s != "it's" {
				t.Errorf("got %d %q %q", n, day, s)
			}
			// Snowflake's names: unquoted in upper case, quoted as written,
			// expressions after their text.
			if got := resultColumns(t, conn, `SELECT 1 AS foo, 2 AS "bar", count(*), x FROM (SELECT 1 AS x) GROUP BY x`); strings.Join(got, ",") != `FOO,bar,COUNT(*),X` {
				t.Errorf("column names %q", got)
			}
			// Session variables.
			mustExec(t, conn, "SET limit_rows = 3")
			if err := conn.QueryRow(ctx, "SELECT $limit_rows + 1").Scan(&n); err != nil || n != 4 {
				t.Errorf("$limit_rows + 1: %d %v", n, err)
			}
			mustExec(t, conn, "UNSET limit_rows")
			// Settings Igloo keeps stay settings.
			mustExec(t, conn, "SET statement_timeout = '1min'")
			mustExec(t, conn, "RESET statement_timeout")
			// A Snowflake construct Igloo refuses, with a hint.
			_, err := conn.Exec(ctx, "SELECT * FROM t SAMPLE (n)")
			var pgErr *pgconn.PgError
			if !asPgError(err, &pgErr) || pgErr.Code != "0A000" || pgErr.Hint == "" {
				t.Errorf("refusal: %v", err)
			}
			if _, err := conn.Exec(ctx, "SET sql_dialect = 'oracle'"); pgCode(err) != "22023" {
				t.Errorf("unknown dialect: %v", err)
			}
			mustExec(t, conn, "ALTER SESSION SET sql_dialect = 'duckdb'")
			if d := showDialectOf(t, conn); d != "duckdb" {
				t.Fatalf("after ALTER SESSION: %q", d)
			}
			mustExec(t, conn, "SET sql_dialect = snowflake")
			mustExec(t, conn, "RESET sql_dialect")
			if d := showDialectOf(t, conn); d != "duckdb" {
				t.Fatalf("after RESET: %q", d)
			}
		})
	}
}

func asPgError(err error, target **pgconn.PgError) bool {
	pe, ok := err.(*pgconn.PgError)
	if ok {
		*target = pe
	}
	return ok
}

func TestSnowflakeDialectNotices(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	var mu sync.Mutex
	var notices []string
	cfg, err := pgx.ParseConfig(ts.url("igloo", adminPassword))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, n.Message)
		mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	mustExec(t, conn, "CREATE TEMPORARY TABLE events (id INT, kind STRING) CLUSTER BY (id)")
	tag, err := conn.Exec(ctx, "USE WAREHOUSE analytics_wh")
	if err != nil || tag.String() != "USE" {
		t.Errorf("USE WAREHOUSE: %q %v", tag, err)
	}
	mustExec(t, conn, "SELECT HASH(1)")
	mu.Lock()
	defer mu.Unlock()
	want := []string{"CLUSTER BY is ignored", "USE WAREHOUSE changes nothing", "HASH computes DuckDB's hash"}
	for _, w := range want {
		found := false
		for _, n := range notices {
			found = found || strings.Contains(n, w)
		}
		if !found {
			t.Errorf("no notice %q in %q", w, notices)
		}
	}
}

func TestSnowflakeDialectFromConnection(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	for _, params := range []map[string]string{
		{"options": "-c sql_dialect=snowflake"},
		{"options": "--sql_dialect=snowflake"},
		{"sql_dialect": "snowflake"},
	} {
		cfg, err := pgx.ParseConfig(ts.url("igloo", adminPassword))
		if err != nil {
			t.Fatal(err)
		}
		cfg.RuntimeParams = params
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("%v: %v", params, err)
		}
		if d := showDialectOf(t, conn); d != "snowflake" {
			t.Errorf("%v: dialect %q", params, d)
		}
		// RESET returns to the connection's dialect, as in PostgreSQL.
		mustExec(t, conn, "SET sql_dialect = 'duckdb'")
		mustExec(t, conn, "RESET sql_dialect")
		if d := showDialectOf(t, conn); d != "snowflake" {
			t.Errorf("%v: after RESET %q", params, d)
		}
		_ = conn.Close(ctx)
	}
	cfg, _ := pgx.ParseConfig(ts.url("igloo", adminPassword))
	cfg.RuntimeParams = map[string]string{"options": "-c sql_dialect=cobol"}
	if conn, err := pgx.ConnectConfig(ctx, cfg); err == nil || pgCode(err) != "22023" {
		if conn != nil {
			_ = conn.Close(ctx)
		}
		t.Errorf("a connection asking for an unknown dialect: %v", err)
	}
}

func TestSnowflakeDialectRoleSetting(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, "CREATE ROLE migrated")
	ana := cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT migrated TO ana")
	mustExec(t, cs.admin, "ALTER ROLE migrated SET sql_dialect = 'snowflake'")
	if _, err := cs.admin.Exec(ctx, "ALTER ROLE migrated SET sql_dialect = 'cobol'"); pgCode(err) != "22023" {
		t.Errorf("unknown dialect: %v", err)
	}
	// Open sessions follow the change.
	if d := showDialectOf(t, ana); d != "snowflake" {
		t.Errorf("ana's dialect %q", d)
	}
	if got := texts(t, ana, "SELECT NVL(NULL, 'x')"); strings.Join(got, ",") != "x" {
		t.Errorf("NVL: %q", got)
	}
	// The session's own choice comes first; the user's setting before the
	// role's.
	mustExec(t, ana, "SET sql_dialect = 'duckdb'")
	if d := showDialectOf(t, ana); d != "duckdb" {
		t.Errorf("session's own: %q", d)
	}
	mustExec(t, ana, "RESET sql_dialect")
	mustExec(t, cs.admin, "ALTER USER ana SET sql_dialect = 'duckdb'")
	if d := showDialectOf(t, ana); d != "duckdb" {
		t.Errorf("user's setting: %q", d)
	}
}

func TestSnowflakeDialectScripts(t *testing.T) {
	ts := startServer(t)
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	ctx := context.Background()
	// Statements run one at a time, so a dialect set by one applies to the
	// next.
	if _, err := conn.Exec(ctx, "SET sql_dialect = 'snowflake'; SELECT IFF(true, 'a', 'b')"); err != nil {
		t.Fatal(err)
	}
	// In the Snowflake dialect, a script is split as Snowflake reads it.
	if _, err := conn.Exec(ctx, "SET x = 'a;b'; SELECT 'it\\'s; fine', $x // a comment; still one"); err != nil {
		t.Fatal(err)
	}
	var s string
	if err := conn.QueryRow(ctx, "SELECT $x").Scan(&s); err != nil || s != "a;b" {
		t.Errorf("$x: %q %v", s, err)
	}
	if _, err := conn.Exec(ctx, "SET sql_dialect = 'duckdb'; SELECT 7 // 2"); err != nil {
		t.Errorf("back to DuckDB within a script: %v", err)
	}
}

// TestSnowflakeTranslationIsCheckedLikeDuckDB checks that a statement in
// the Snowflake dialect reaches only what its translation, sent in the
// DuckDB dialect, would: Igloo checks the translation, never the
// Snowflake text.
func TestSnowflakeTranslationIsCheckedLikeDuckDB(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			user := "nina_" + name
			conn := cs.user(t, user, auth.RoleNone, mode)
			mustExec(t, cs.admin, "GRANT SELECT ON TABLE lake.public.orders TO "+user)
			statements := []struct {
				sql     string
				allowed bool
			}{
				{"SELECT IFF(amount > 10, 'big', 'small') AS size FROM orders", true},
				{"SELECT * FROM IDENTIFIER('orders')", true},
				{"SELECT * FROM IDENTIFIER('lake.public.secrets')", false},
				{"SELECT * FROM IDENTIFIER('\"SECRETS\"')", false},
				{"SELECT f.value FROM orders o, LATERAL FLATTEN(input => (SELECT to_json(list(code)) FROM secrets)) f", false},
				{"SELECT f.value FROM orders o, LATERAL FLATTEN(input => ARRAY_CONSTRUCT(o.id, o.amount)) f", true},
				{"SELECT LISTAGG(code, ',') FROM secrets", false},
				{"SELECT * FROM orders MINUS SELECT id, 1 FROM secrets", false},
				{"SELECT * FROM orders WHERE id IN (SELECT id FROM secrets SAMPLE (50))", false},
				{"SET v = (SELECT max(code) FROM secrets)", false},
				{"SET v = (SELECT max(amount) FROM orders)", true},
				{"SELECT * FROM orders WHERE 'x' LIKE ANY (SELECT code FROM secrets)", false},
			}
			for _, st := range statements {
				res, err := snowflake.Translate(st.sql, snowflake.Options{
					IsSetting: func(n string) bool { return n == "search_path" },
				})
				if err != nil {
					t.Errorf("%s: %v", st.sql, err)
					continue
				}
				mustExec(t, conn, "SET sql_dialect = 'snowflake'")
				_, sfErr := conn.Exec(ctx, st.sql)
				mustExec(t, conn, "SET sql_dialect = 'duckdb'")
				_, duckErr := conn.Exec(ctx, res.SQL)
				sfDenied, duckDenied := pgCode(sfErr) == "42501", pgCode(duckErr) == "42501"
				if sfDenied != duckDenied {
					t.Errorf("%s:\n  Snowflake dialect: %v\n  its translation, %s: %v", st.sql, sfErr, res.SQL, duckErr)
				}
				if st.allowed && sfErr != nil || !st.allowed && !sfDenied {
					t.Errorf("%s: want allowed=%v, got %v", st.sql, st.allowed, sfErr)
				}
			}
			// IDENTIFIER($v) is resolved to its name before any check.
			mustExec(t, conn, "SET sql_dialect = 'snowflake'")
			mustExec(t, conn, "SET t = 'lake.public.secrets'")
			if _, err := conn.Exec(ctx, "SELECT * FROM IDENTIFIER($t)"); pgCode(err) != "42501" {
				t.Errorf("IDENTIFIER($t) of a table not granted: %v", err)
			}
			mustExec(t, conn, "SET t = 'orders'")
			if _, err := conn.Exec(ctx, "SELECT * FROM IDENTIFIER($t)"); err != nil {
				t.Errorf("IDENTIFIER($t) of a granted table: %v", err)
			}
			mustExec(t, conn, "SET sql_dialect = 'duckdb'")
		})
	}
	// The history holds what ran: the translation.
	q := lastQuery(t, cs, "nina_simple", "SELECT CASE WHEN")
	if !strings.Contains(q.Text, "AS \"SIZE\"") {
		t.Errorf("history: %q", q.Text)
	}
}

// TestSnowflakeTranslationKeepsPolicies checks that masking and row access
// policies apply to translated statements as to any other.
func TestSnowflakeTranslationKeepsPolicies(t *testing.T) {
	cs := startPolicyServer(t)
	mustExec(t, cs.admin, "GRANT SELECT ON SCHEMA lake.crm TO acme_team")
	conn := cs.user(t, "ali", auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO ali")
	mustExec(t, conn, "SET sql_dialect = 'snowflake'")
	for sql, want := range map[string]string{
		"SELECT LISTAGG(email, ',') WITHIN GROUP (ORDER BY email) FROM lake.crm.contacts":                     "***,***",
		"SELECT IFF(email LIKE '%acme%', 'acme', 'other') AS e FROM lake.crm.contacts ORDER BY id":            "other,other",
		"SELECT OBJECT_CONSTRUCT('e', email):e::string FROM lake.crm.contacts ORDER BY id":                    "***,***",
		"SELECT f.value::string FROM lake.crm.contacts c, LATERAL FLATTEN(ARRAY_CONSTRUCT(c.email)) f":        "***,***",
		"SELECT COUNT(*) FROM lake.crm.contacts WHERE client ILIKE ANY ('glo%', 'ac%')":                       "2",
		"SELECT SPLIT_PART(email, '@', 2) FROM lake.crm.contacts QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1": "",
	} {
		got := strings.Join(texts(t, conn, sql), ",")
		if got != want {
			t.Errorf("%s: got %q, want %q", sql, got, want)
		}
	}
}

func TestSnowflakeDialectFlightSchema(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	user, err := ts.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ts.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	if err := es.Run(ctx, "SET sql_dialect = 'snowflake'", discard{}); err != nil {
		t.Fatal(err)
	}
	ps, err := es.Prepare(ctx, `SELECT 1 AS foo, 'x' AS "bar"`)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close() //nolint:errcheck
	schema, err := ps.ArrowSchema()
	if err != nil {
		t.Fatal(err)
	}
	if schema.Field(0).Name != "FOO" || schema.Field(1).Name != "bar" {
		t.Errorf("Arrow schema %s", schema)
	}
	if cols := ps.Columns(); cols[0].Name != "FOO" || cols[1].Name != "bar" {
		t.Errorf("columns %+v", cols)
	}
}

// discard is a ResultWriter that keeps nothing.
// Any user may choose their session's dialect: it changes how their SQL is
// read, never what they may do.
func TestSnowflakeDialectAsNonAdmin(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			for _, role := range []auth.Role{auth.RoleNone, auth.RoleReader} {
				user := "omar_" + string(role) + "_" + name
				conn := cs.user(t, user, role, mode)
				mustExec(t, conn, "SET sql_dialect = 'snowflake'")
				if d := showDialectOf(t, conn); d != "snowflake" {
					t.Fatalf("%s: after SET: %q", role, d)
				}
				var n int64
				if err := conn.QueryRow(ctx, "SELECT IFF(true, 1, 2)").Scan(&n); err != nil || n != 1 {
					t.Errorf("%s: IFF: %d %v", role, n, err)
				}
				mustExec(t, conn, "SET v = 41")
				if err := conn.QueryRow(ctx, "SELECT $v + 1").Scan(&n); err != nil || n != 42 {
					t.Errorf("%s: $v + 1: %d %v", role, n, err)
				}
				mustExec(t, conn, "ALTER SESSION SET sql_dialect = 'duckdb'")
				if d := showDialectOf(t, conn); d != "duckdb" {
					t.Fatalf("%s: after ALTER SESSION: %q", role, d)
				}
				mustExec(t, conn, "SET SESSION sql_dialect TO snowflake")
				mustExec(t, conn, "RESET sql_dialect")
				if d := showDialectOf(t, conn); d != "duckdb" {
					t.Fatalf("%s: after RESET: %q", role, d)
				}
				// A wrong value says so, not that SET needs a role.
				if _, err := conn.Exec(ctx, "SET sql_dialect = 'oracle'"); pgCode(err) != "22023" {
					t.Errorf("%s: SET sql_dialect = 'oracle': want 22023, got %v", role, err)
				}
				// Choosing the dialect grants nothing.
				mustExec(t, conn, "SET sql_dialect = 'snowflake'")
				if role == auth.RoleNone {
					expectDenied(t, conn, "SELECT * FROM IDENTIFIER('lake.public.secrets')")
				}
				expectDenied(t, conn, "ALTER ROLE reader SET sql_dialect = 'snowflake'")
			}
		})
	}
}

// A view created in Snowflake's dialect stores its DuckDB translation,
// which calls the dialect's macros; every session has them.
func TestSnowflakeDialectViewInAnySession(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	sf := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, sf, "SET sql_dialect = 'snowflake'")
	mustExec(t, sf, "CREATE VIEW sf_dates AS SELECT DATEADD(month, 1, '2024-01-31'::date) AS d, DATE_TRUNC('week', '2024-03-14'::date) AS w")
	duck := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	var d, w string
	if err := duck.QueryRow(ctx, "SELECT d::VARCHAR, w::VARCHAR FROM sf_dates").Scan(&d, &w); err != nil {
		t.Fatal(err)
	}
	if d != "2024-02-29" || w != "2024-03-11" {
		t.Errorf("got %s %s", d, w)
	}
}

// Statements Igloo writes itself in an embedded session, such as the
// console's and Flight SQL's catalog lookups, are DuckDB SQL whatever the
// session's dialect.
func TestSnowflakeDialectIglooSQL(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	user, err := ts.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ts.srv.OpenEmbedded(ctx, user, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	if err := es.Run(ctx, "SET sql_dialect = 'snowflake'", discard{}); err != nil {
		t.Fatal(err)
	}
	const query = `SELECT 'a\b' AS x, length('a\b') AS n`
	var own, igloo collected
	if err := es.Run(ctx, query, &own); err != nil {
		t.Fatal(err)
	}
	if err := es.Run(WithIglooSQL(ctx), query, &igloo); err != nil {
		t.Fatal(err)
	}
	if own.columns[0].Name != "X" || own.rows[0][1] != "2" {
		t.Errorf("in Snowflake's dialect: %+v %v", own.columns, own.rows)
	}
	if igloo.columns[0].Name != "x" || igloo.rows[0][0] != `a\b` || igloo.rows[0][1] != "3" {
		t.Errorf("Igloo's SQL: %+v %v", igloo.columns, igloo.rows)
	}
}

type discard struct{}

func (discard) Columns([]Column) error { return nil }
func (discard) Row([]any) error        { return nil }
func (discard) Complete(string) error  { return nil }
