package pgwire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/federation"
)

// These tests attach the PostgreSQL of IGLOO_TEST_DATABASE_URL, and where
// set the MySQL of IGLOO_TEST_MYSQL_URL (mysql://user:password@host:port/db),
// as connections, with DuckDB's postgres and mysql extensions from
// IGLOO_TEST_EXTENSION_DIR (scripts/test.sh fills it), or downloaded once
// into DuckDB's default directory without it.

var (
	extensionsOnce sync.Once
	extensionsErr  error
)

func needExtensions(t *testing.T) {
	t.Helper()
	extensionsOnce.Do(func() {
		ctx := context.Background()
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			extensionsErr = err
			return
		}
		defer connector.Close() //nolint:errcheck
		c, err := connector.Connect(ctx)
		if err != nil {
			extensionsErr = err
			return
		}
		defer c.Close() //nolint:errcheck
		var stmts []string
		if dir := os.Getenv("IGLOO_TEST_EXTENSION_DIR"); dir != "" {
			stmts = append(stmts, "SET extension_directory = "+quoteLiteral(dir))
		}
		for _, stmt := range append(stmts, "INSTALL postgres", "INSTALL mysql") {
			if _, err := c.(*duckdb.Conn).ExecContext(ctx, stmt, nil); err != nil {
				extensionsErr = err
				return
			}
		}
	})
	if extensionsErr != nil {
		t.Skipf("cannot install DuckDB's postgres and mysql extensions: %v", extensionsErr)
	}
}

// remoteDatabase is where a test connection points.
type remoteDatabase struct {
	url            string
	host           string
	port           int
	user, password string
	database       string
}

func remoteFromEnv(t *testing.T, env string) remoteDatabase {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		t.Skip(env + " not set")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	pw, _ := u.User.Password()
	return remoteDatabase{url: raw, host: u.Hostname(), port: port, user: u.User.Username(), password: pw,
		database: strings.TrimPrefix(u.Path, "/")}
}

func (r remoteDatabase) options(t federation.Type) string {
	return "TYPE " + string(t) + " WITH (host '" + r.host + "', port " + strconv.Itoa(r.port) + ", database '" + r.database +
		"', user '" + r.user + "', password '" + r.password + "', connect_timeout '5s'"
}

func uniqueName(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// fedServer is a catalogServer with connections, whose store outlives
// the server so a restart can be simulated by starting another.
type fedServer struct {
	*catalogServer
	fed   *federation.Manager
	store *federation.MemoryStore
	srv   *Server
}

// startFedServer starts a server that attaches store's connections when
// its engine opens, as Igloo does on startup.
func startFedServer(t *testing.T, store *federation.MemoryStore, hooks ...func(*Server)) *fedServer {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fed := federation.NewManager(store, federation.Options{}, log)
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")}}
	eng, err := engine.Open(ctx, cfg, log, engine.BeforeLockdown(fed.Attach))
	if err != nil {
		t.Fatal(err)
	}
	fed.SetConnect(eng.Connect)

	users := &testStore{users: map[string]auth.User{}}
	v, err := auth.NewVerifier(adminPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.EnsureAdmin(ctx, "igloo", v); err != nil {
		t.Fatal(err)
	}
	cs := &catalogServer{history: audit.NewMemoryStore(1000, 1000), grants: auth.NewMemoryGrants(users)}
	hist := audit.New(cs.history, audit.Options{}, log)
	eng.SetHistory(hist, HistoryViewer)
	// s3://bucket/ is where the tests' COPY statements would write, were
	// they not refused for what they read.
	srv, err := New(eng, Options{Users: users, RoleRefresh: time.Hour, Grants: cs.grants, Catalog: "lake", Audit: hist, Federation: fed,
		AllowedLocations: []string{"s3://bucket/"}}, log)
	if err != nil {
		t.Fatal(err)
	}
	for _, hook := range hooks {
		hook(srv)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = eng.Close()
	})
	cs.testServer = &testServer{addr: ln.Addr().String(), store: users}
	cs.admin = cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, sql := range []string{
		"ATTACH ':memory:' AS lake",
		"CREATE SCHEMA lake.public",
		"CREATE TABLE lake.public.audiences (customer_id INTEGER, segment VARCHAR)",
		"INSERT INTO lake.public.audiences VALUES (2, 'loyal'), (4, 'loyal'), (5, 'new')",
	} {
		mustExec(t, cs.admin, sql)
	}
	return &fedServer{catalogServer: cs, fed: fed, store: store, srv: srv}
}

// pgFixture creates a schema with two tables in the test PostgreSQL and
// returns its name.
func pgFixture(t *testing.T, pg remoteDatabase) string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pg.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	schema := uniqueName("crm_")
	for _, sql := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".customers (id int PRIMARY KEY, name text, region text)",
		"INSERT INTO " + schema + ".customers SELECT g, 'c' || g, CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END FROM generate_series(1, 10) g",
		"CREATE TABLE " + schema + ".pii (id int, ssn text)",
		"INSERT INTO " + schema + ".pii VALUES (1, '123-45-6789')",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), pg.url)
		if err == nil {
			_, _ = c.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = c.Close(context.Background())
		}
	})
	return schema
}

func TestConnectionStatements(t *testing.T) {
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	store := federation.NewMemoryStore()
	fs := startFedServer(t, store)
	admin := fs.admin
	walt := fs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	expectDenied(t, walt, "CREATE CONNECTION crm "+pg.options(federation.Postgres)+")", "admin")
	expectDenied(t, walt, "SHOW CONNECTIONS", "admin")
	if _, err := admin.Exec(context.Background(), "CREATE CONNECTION crm TYPE postgres WITH (host '127.0.0.1', port 1, database 'x', user 'u', connect_timeout 1)"); pgCode(err) != "08001" {
		t.Errorf("an unreachable database: %v", err)
	}
	if _, err := admin.Exec(context.Background(), "CREATE CONNECTION crm TYPE oracle WITH (host 'h')"); pgCode(err) != "0A000" {
		t.Errorf("an unsupported type: %v", err)
	}
	if _, err := admin.Exec(context.Background(), "CREATE CONNECTION lake "+pg.options(federation.Postgres)+")"); pgCode(err) != "22023" {
		t.Errorf("a connection named like the catalog: %v", err)
	}
	if _, err := admin.Exec(context.Background(), "CREATE CONNECTION crm "+pg.options(federation.Postgres)+", read_only false)"); pgCode(err) != "22023" {
		t.Errorf("a writable connection: %v", err)
	}
	mustExec(t, admin, "CREATE CONNECTION crm "+pg.options(federation.Postgres)+", query_timeout = '30s')")
	mustExec(t, admin, "CREATE CONNECTION IF NOT EXISTS crm "+pg.options(federation.Postgres)+")")
	if _, err := admin.Exec(context.Background(), "CREATE CONNECTION crm "+pg.options(federation.Postgres)+")"); pgCode(err) != "42710" {
		t.Errorf("a duplicate: %v", err)
	}

	// Created after the engine started: attached at the next start.
	var state, detail, password, timeout string
	if err := admin.QueryRow(context.Background(), "SHOW CONNECTIONS").Scan(new(string), new(string), new(string), new(int32),
		new(string), new(string), &password, new(string), new(string), new(string), &timeout, &state, &detail, new(string),
		new(time.Time), new(time.Time)); err != nil {
		t.Fatal(err)
	}
	if state != "not attached" || !strings.Contains(detail, "restart") || password != "stored" || timeout != "30s" {
		t.Errorf("SHOW CONNECTIONS: %s %s %s %s", state, detail, password, timeout)
	}
	if _, err := admin.Exec(context.Background(), "SELECT * FROM crm.public.x"); pgCode(err) != "55000" ||
		!strings.Contains(err.Error(), "restart") {
		t.Errorf("a connection not attached yet: %v", err)
	}
	// The password is stored, and never shown again.
	rows := queryAll(t, admin, "SHOW CONNECTIONS")
	for _, field := range strings.FieldsFunc(rows, func(r rune) bool { return r == '|' || r == '\n' }) {
		if field == pg.password {
			t.Errorf("SHOW CONNECTIONS shows the password: %s", rows)
		}
	}

	fs2 := startFedServer(t, store)
	if fs2.fed.Catalogs()["crm"] != federation.Postgres {
		t.Fatalf("not attached after a restart: %v", fs2.fed.Catalogs())
	}
	if _, err := fs2.admin.Exec(context.Background(), "ALTER CONNECTION crm SET (password 'wrong')"); pgCode(err) != "08001" {
		t.Errorf("a password that does not work: %v", err)
	}
	mustExec(t, fs2.admin, "ALTER CONNECTION crm SET (password '"+pg.password+"', query_timeout 60)")
	mustExec(t, fs2.admin, "ALTER CONNECTION crm REFRESH")
	if _, err := fs2.admin.Exec(context.Background(), "ALTER CONNECTION ghost SET (port 1)"); pgCode(err) != "42704" {
		t.Errorf("altering a missing connection: %v", err)
	}
	expectDenied(t, fs2.admin, "DETACH crm", "DROP CONNECTION")
	mustExec(t, fs2.admin, "DROP CONNECTION crm")
	mustExec(t, fs2.admin, "DROP CONNECTION IF EXISTS crm")
	if _, err := fs2.admin.Exec(context.Background(), "DROP CONNECTION crm"); pgCode(err) != "42704" {
		t.Errorf("dropping twice: %v", err)
	}
	if _, err := fs2.admin.Exec(context.Background(), "SELECT 1 FROM crm.information_schema.tables"); err == nil {
		t.Error("a dropped connection is still attached")
	}
}

func queryAll(t *testing.T, conn *pgx.Conn, sql string) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		var parts []string
		for _, v := range vals {
			parts = append(parts, strings.TrimSpace(strings.ReplaceAll(fmtValue(v), "\n", " ")))
		}
		out = append(out, strings.Join(parts, "|"))
	}
	return strings.Join(out, "\n")
}

func fmtValue(v any) string {
	switch v := v.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case int32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.FormatInt(v, 10)
	}
	return "?"
}

// TestConnectionGrants checks that a connection's tables are read only
// through grants, by every role but admin, and that nothing reaches them
// otherwise.
func TestConnectionGrants(t *testing.T) {
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	fs := startFedServer(t, store)
	customers, pii := "crm."+schema+".customers", "crm."+schema+".pii"

	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			rita := fs.user(t, uniqueName("rita"), auth.RoleReader, mode)
			nina := fs.user(t, uniqueName("nina"), auth.RoleNone, mode)

			// Admins read everything.
			if n := count(t, fs.admin, "SELECT count(*) FROM "+customers); n != 10 {
				t.Errorf("admin: %d", n)
			}
			// Not even readers read a connection without a grant.
			for _, sql := range []string{
				"SELECT * FROM " + customers,
				"SELECT count(*) FROM " + customers + " WHERE region = 'east'",
				"SELECT * FROM lake.public.audiences a JOIN " + customers + " c ON c.id = a.customer_id",
				"SELECT (SELECT max(name) FROM " + customers + ")",
				"WITH x AS (SELECT * FROM " + customers + ") SELECT * FROM x",
				"EXPLAIN SELECT * FROM " + customers,
				"SUMMARIZE " + customers,
				"SET VARIABLE v = (SELECT max(name) FROM " + customers + ")",
				"SELECT * FROM crm." + schema + ".\"customers\"",
			} {
				expectDenied(t, rita, sql, "connection crm")
				expectDenied(t, nina, sql, "")
			}
			// Metadata needs no grant, as for Iceberg tables.
			for _, sql := range []string{
				"DESCRIBE " + customers,
				"SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'crm'",
				"SELECT count(*) FROM duckdb_columns() WHERE database_name = 'crm'",
			} {
				expectAllowed(t, rita, sql)
				expectAllowed(t, nina, sql)
			}

			// A grant on one table.
			mustExec(t, fs.admin, "GRANT SELECT ON TABLE "+customers+" TO "+userName(t, rita))
			if n := count(t, rita, "SELECT count(*) FROM lake.public.audiences a JOIN "+customers+" c ON c.id = a.customer_id WHERE c.region = 'east'"); n != 2 {
				t.Errorf("join with Iceberg's stand-in: %d", n)
			}
			if n := count(t, rita, "SELECT count(*) FROM "+customers+" WHERE id > $1", 5); n != 5 {
				t.Errorf("with a parameter: %d", n)
			}
			expectDenied(t, rita, "SELECT * FROM "+pii, "pii")
			expectDenied(t, rita, "SELECT * FROM "+customers+" c JOIN "+pii+" p USING (id)", "pii")
			// Names found along the search path need the grant too.
			mustExec(t, rita, "SET search_path = 'crm."+schema+",lake.public'")
			expectAllowed(t, rita, "SELECT count(*) FROM customers")
			expectDenied(t, rita, "SELECT * FROM pii", "pii")
			mustExec(t, rita, "SET search_path = 'lake.public,memory.main'")
			// A view reads with the reader's grants.
			mine, theirs := "memory.main."+uniqueName("mine_"), "memory.main."+uniqueName("theirs_")
			mustExec(t, fs.admin, "CREATE VIEW "+mine+" AS SELECT * FROM "+customers)
			mustExec(t, fs.admin, "CREATE VIEW "+theirs+" AS SELECT c.* FROM lake.public.audiences a, "+pii+" c")
			expectAllowed(t, rita, "SELECT * FROM "+mine)
			expectDenied(t, rita, "SELECT * FROM "+theirs, "pii")

			// None-role users need grants on Iceberg's tables as well.
			mustExec(t, fs.admin, "GRANT SELECT ON SCHEMA crm."+schema+" TO "+userName(t, nina))
			expectAllowed(t, nina, "SELECT * FROM "+pii)
			expectDenied(t, nina, "SELECT * FROM lake.public.audiences a JOIN "+customers+" c ON c.id = a.customer_id", "audiences")
			mustExec(t, fs.admin, "GRANT SELECT ON TABLE lake.public.audiences TO "+userName(t, nina))
			expectAllowed(t, nina, "SELECT * FROM lake.public.audiences a JOIN "+customers+" c ON c.id = a.customer_id")

			// Revoking reaches prepared statements.
			expectQueryAllowed(t, rita, "SELECT * FROM "+customers)
			expectQueryAllowed(t, rita, "SELECT * FROM "+mine)
			mustExec(t, fs.admin, "REVOKE SELECT ON TABLE "+customers+" FROM "+userName(t, rita))
			// Twice: a statement refused when checked again stays refused.
			for range 2 {
				expectQueryDenied(t, rita, "SELECT * FROM "+customers, "customers")
				expectQueryDenied(t, rita, "SELECT * FROM "+mine, "customers")
			}

			// A grant on the whole connection covers its schemas, the
			// remote catalogs included.
			mustExec(t, fs.admin, "GRANT SELECT ON CONNECTION crm TO "+userName(t, rita))
			expectAllowed(t, rita, "SELECT * FROM "+theirs)
			mustExec(t, fs.admin, "REVOKE SELECT ON CONNECTION crm FROM "+userName(t, rita))
			expectDenied(t, rita, "SELECT * FROM "+theirs, "pii")
			// Replacing a view so it reads a connection reaches statements
			// prepared before.
			mustExec(t, fs.admin, "CREATE OR REPLACE VIEW "+mine+" AS SELECT * FROM lake.public.audiences")
			expectQueryAllowed(t, rita, "SELECT * FROM "+mine)
			mustExec(t, fs.admin, "CREATE OR REPLACE VIEW "+mine+" AS SELECT * FROM "+pii)
			expectQueryDenied(t, rita, "SELECT * FROM "+mine, "pii")
		})
	}

	rita := fs.user(t, "rita_misc", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	walt := fs.user(t, "walt_misc", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, fs.admin, "GRANT SELECT ON TABLE "+customers+" TO rita_misc")
	mustExec(t, fs.admin, "GRANT SELECT ON TABLE "+customers+" TO walt_misc")

	// Creating a view of a connection's tables needs a grant on them,
	// though the view reads with its readers' grants.
	expectDenied(t, walt, "CREATE VIEW lake.public.v_pii AS SELECT * FROM "+pii, "pii")
	expectDenied(t, walt, "CREATE OR REPLACE VIEW lake.public.v_pii AS SELECT c.* FROM "+customers+" c JOIN "+pii+" p USING (id)", "pii")
	expectDenied(t, walt, "CREATE TEMP VIEW v_pii AS SELECT * FROM "+pii, "pii")
	mustExec(t, walt, "CREATE VIEW lake.public.v_customers AS SELECT * FROM "+customers)
	mustExec(t, walt, "CREATE TEMP VIEW v_customers AS SELECT * FROM "+customers)
	expectAllowed(t, walt, "SELECT * FROM v_customers")
	expectAllowed(t, rita, "SELECT * FROM lake.public.v_customers")
	mustExec(t, fs.admin, "CREATE VIEW lake.public.v_pii AS SELECT * FROM "+pii)
	expectDenied(t, rita, "SELECT * FROM lake.public.v_pii", "pii")

	// Connections are read-only, and only SELECT is granted on them.
	if _, err := fs.admin.Exec(context.Background(), "GRANT INSERT ON TABLE "+customers+" TO rita_misc"); pgCode(err) != "0LP01" {
		t.Errorf("INSERT on a connection's table: %v", err)
	}
	if _, err := fs.admin.Exec(context.Background(), "INSERT INTO "+customers+" VALUES (99, 'x', 'y')"); err == nil {
		t.Error("an admin wrote to a connection")
	}
	if _, err := walt.Exec(context.Background(), "INSERT INTO "+customers+" VALUES (99, 'x', 'y')"); err == nil {
		t.Error("a writer wrote to a connection")
	}
	// Writers copying a connection's rows into the warehouse need the grant.
	mustExec(t, walt, "CREATE TABLE lake.public.copied AS SELECT * FROM "+customers)
	expectDenied(t, walt, "CREATE TABLE lake.public.copied2 AS SELECT * FROM "+pii, "pii")
	expectDenied(t, walt, "INSERT INTO lake.public.audiences SELECT id, ssn FROM "+pii, "pii")
	expectDenied(t, walt, "COPY (SELECT * FROM "+pii+") TO 's3://bucket/x.csv'", "pii")
	expectDenied(t, walt, "COPY "+pii+" TO 's3://bucket/x.csv'", "pii")
	expectDenied(t, walt, "UPDATE lake.public.audiences SET segment = (SELECT max(ssn) FROM "+pii+")", "pii")
	expectDenied(t, walt, "UPDATE lake.public.audiences a SET segment = p.ssn FROM "+pii+" p WHERE p.id = a.customer_id", "pii")
	expectDenied(t, walt, "DELETE FROM lake.public.audiences a USING "+pii+" p WHERE p.id = a.customer_id", "pii")
	expectDenied(t, walt, "MERGE INTO lake.public.audiences a USING "+pii+" p ON p.id = a.customer_id WHEN MATCHED THEN DELETE", "pii")
	expectDenied(t, walt, "INSERT INTO lake.public.audiences SELECT id, 'x' FROM "+customers+" WHERE id IN (SELECT id FROM "+pii+")", "pii")

	// Nothing hides a read from the check: macros, CTEs named like granted
	// tables, other spellings of the name.
	mustExec(t, fs.admin, "CREATE MACRO memory.main.hidden_rows() AS TABLE SELECT * FROM "+pii)
	mustExec(t, fs.admin, "CREATE MACRO memory.main.hidden_value() AS (SELECT max(ssn) FROM "+pii+")")
	for _, sql := range []string{
		"SELECT * FROM hidden_rows()",
		"SELECT hidden_value()",
		"SELECT * FROM " + customers + ", hidden_rows()",
		"SELECT hidden_value() FROM " + customers,
		"WITH customers AS (SELECT * FROM " + pii + ") SELECT * FROM customers",
		"SELECT * FROM CRM." + strings.ToUpper(schema) + ".PII",
		"SELECT * FROM \"crm\".\"" + schema + "\".\"pii\"",
		"FROM " + pii,
		"SELECT * FROM " + customers + " c, LATERAL (SELECT ssn FROM " + pii + " p WHERE p.id = c.id)",
		"SELECT * FROM " + customers + " UNION ALL SELECT id, ssn, ssn FROM " + pii,
	} {
		expectDenied(t, rita, sql, "")
	}
	if n := count(t, rita, "SELECT count(*) FROM CRM."+strings.ToUpper(schema)+".CUSTOMERS"); n != 10 {
		t.Errorf("a granted table in capitals: %d", n)
	}

	// SQL that runs on the remote server is refused, admins included.
	for _, sql := range []string{
		"SELECT * FROM postgres_query('crm', 'SELECT * FROM " + schema + ".pii')",
		"CALL postgres_execute('crm', 'DROP TABLE " + schema + ".pii')",
		"SELECT * FROM postgres_scan('host=" + pg.host + " user=" + pg.user + "', 'public', 'x')",
		"SELECT * FROM postgres_scan_pushdown('', 'public', 'x')",
		"SELECT * FROM \"postgres_query\"('crm', 'SELECT 1')",
		"SELECT * FROM mysql_query('crm', 'SELECT 1')",
		"CALL postgres_attach('host=localhost')",
	} {
		expectDenied(t, fs.admin, sql, "other databases")
		expectDenied(t, rita, sql, "other databases")
	}
	expectDenied(t, rita, "SELECT * FROM pg_clear_cache()", "admin")
	expectDenied(t, rita, "SET pg_experimental_filter_pushdown = false", "admin")
	expectDenied(t, rita, "SET GLOBAL pg_staleness_query_enabled = true", "admin")

	// A PIVOT that finds its columns in a connection's data runs before it
	// can be checked; one that lists them is checked like any query.
	expectDenied(t, rita, "PIVOT "+customers+" ON region USING count(*)", "IN (")
	expectDenied(t, rita, "SELECT * FROM (PIVOT (SELECT * FROM "+customers+") ON region USING count(*))", "IN (")
	if n := count(t, rita, "SELECT count(*) FROM (PIVOT "+customers+" ON region IN ('east', 'west') USING count(*))"); n != 10 {
		t.Errorf("a PIVOT with its columns listed: %d", n)
	}
	expectDenied(t, rita, "SELECT * FROM (PIVOT "+pii+" ON ssn IN ('x') USING count(*))", "pii")

	// SHOW GRANTS names connections' objects in full.
	got := queryAll(t, rita, "SHOW GRANTS ON TABLE "+customers)
	if !strings.Contains(got, "rita_misc|user|SELECT|table|"+customers+"|igloo") {
		t.Errorf("SHOW GRANTS: %s", got)
	}
	got = queryAll(t, fs.admin, "SHOW GRANTS ON CONNECTION crm")
	if !strings.Contains(got, customers) {
		t.Errorf("SHOW GRANTS ON CONNECTION: %s", got)
	}
	if _, err := fs.admin.Exec(context.Background(), "GRANT SELECT ON CONNECTION ghost TO rita_misc"); pgCode(err) != "42704" {
		t.Errorf("a grant on a missing connection: %v", err)
	}
	if _, err := fs.admin.Exec(context.Background(), "GRANT SELECT ON TABLE crm."+schema+".missing TO rita_misc"); pgCode(err) != "42P01" {
		t.Errorf("a grant on a missing table: %v", err)
	}
	mustExec(t, fs.admin, "GRANT SELECT ON ALL TABLES IN SCHEMA crm."+schema+" TO rita_misc")
	expectAllowed(t, rita, "SELECT * FROM "+pii)

	// The history names what a statement read from connections.
	expectAllowed(t, rita, "SELECT count(*) FROM "+customers+" -- history check")
	var read string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		err := fs.admin.QueryRow(context.Background(), `SELECT array_to_string(tables_read, ',') FROM igloo.query_history
			WHERE user_name = 'rita_misc' AND query_text LIKE '%history check' AND outcome = 'ok'`).Scan(&read)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if read != customers {
		t.Errorf("tables_read: %v", read)
	}

	// Dropping a connection removes its grants.
	mustExec(t, fs.admin, "DROP CONNECTION crm")
	if got := queryAll(t, fs.admin, "SHOW GRANTS"); strings.Contains(got, "crm.") {
		t.Errorf("grants outlive the connection: %s", got)
	}
}

// laterAttach stands in for the federation manager in the checks of
// reads, with its connections hidden until attach: as if they were
// attached after statements were prepared, which Igloo does not do today.
type laterAttach struct {
	m        *federation.Manager
	attached atomic.Bool
	gen      atomic.Uint64
}

func (l *laterAttach) Catalogs() map[string]federation.Type {
	if !l.attached.Load() {
		return nil
	}
	return l.m.Catalogs()
}

func (l *laterAttach) Generation() uint64 { return l.m.Generation() + l.gen.Load() }

func (l *laterAttach) attach() {
	l.attached.Store(true)
	l.gen.Add(1)
}

// TestConnectionAttachedLater checks that a statement prepared while no
// connection was attached is checked again before it runs once one is,
// whatever its user's role: replacing a view it reads so that it reads the
// connection does not get past the checks.
func TestConnectionAttachedLater(t *testing.T) {
	needExtensions(t)
	pg := remoteFromEnv(t, "IGLOO_TEST_DATABASE_URL")
	schema := pgFixture(t, pg)
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.Host, c.Port, c.User, c.Password, c.Database = pg.host, pg.port, pg.user, pg.password, pg.database
	c.Schemas = []string{schema}
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	later := &laterAttach{}
	fs := startFedServer(t, store, func(s *Server) {
		later.m = s.fed
		s.remotes = later
	})
	view := "memory.main." + uniqueName("v_")
	mustExec(t, fs.admin, "CREATE VIEW "+view+" AS SELECT segment FROM lake.public.audiences")
	q := "SELECT count(*) FROM " + view
	rita := fs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
	walt := fs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeCacheStatement)
	for _, conn := range []*pgx.Conn{rita, walt} {
		if n := count(t, conn, q); n != 3 {
			t.Fatalf("%s before: %d", userName(t, conn), n)
		}
	}
	later.attach()
	mustExec(t, fs.admin, "CREATE OR REPLACE VIEW "+view+" AS SELECT ssn AS segment FROM crm."+schema+".pii")
	for _, conn := range []*pgx.Conn{rita, walt} {
		expectQueryDenied(t, conn, q, "pii")
	}
}

// expectQueryDenied runs sql as a query, which a connection that caches
// statements runs as the statement it prepared the last time; Exec without
// arguments would send it again with the simple protocol.
func expectQueryDenied(t *testing.T, conn *pgx.Conn, sql string, mentions ...string) {
	t.Helper()
	err := runQuery(conn, sql)
	if pgCode(err) != "42501" {
		t.Errorf("%s: want 42501, got %v", sql, err)
		return
	}
	for _, m := range mentions {
		if !strings.Contains(err.Error(), m) {
			t.Errorf("%s: error %q does not mention %q", sql, err, m)
		}
	}
}

func expectQueryAllowed(t *testing.T, conn *pgx.Conn, sql string) {
	t.Helper()
	if err := runQuery(conn, sql); err != nil {
		t.Errorf("%s: %v", sql, err)
	}
}

func runQuery(conn *pgx.Conn, sql string) error {
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close()
	return rows.Err()
}

// userName is the user a test connection signed in as.
func userName(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	return conn.Config().User
}

func TestMySQLConnectionGrants(t *testing.T) {
	needExtensions(t)
	my := remoteFromEnv(t, "IGLOO_TEST_MYSQL_URL")
	ctx := context.Background()
	table := uniqueName("campaigns_")

	setup, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close() //nolint:errcheck
	sc, err := setup.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close() //nolint:errcheck
	dsn := "host=" + my.host + " port=" + strconv.Itoa(my.port) + " user=" + my.user + " password=" + my.password + " database=" + my.database
	var stmts []string
	if dir := os.Getenv("IGLOO_TEST_EXTENSION_DIR"); dir != "" {
		stmts = append(stmts, "SET extension_directory = "+quoteLiteral(dir))
	}
	for _, sql := range append(stmts,
		"LOAD mysql",
		"ATTACH '"+dsn+"' AS setup (TYPE mysql)",
		"CALL mysql_execute('setup', 'CREATE TABLE "+table+" (id INT PRIMARY KEY, client VARCHAR(20), budget INT)')",
		"CALL mysql_execute('setup', 'INSERT INTO "+table+" VALUES (2, ''acme'', 100), (4, ''acme'', 200), (5, ''globex'', 300)')",
	) {
		if _, err := sc.(*duckdb.Conn).ExecContext(ctx, sql, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = sc.(*duckdb.Conn).ExecContext(ctx, "CALL mysql_execute('setup', 'DROP TABLE "+table+"')", nil)
	})

	store := federation.NewMemoryStore()
	c := federation.New("ops", federation.MySQL)
	c.Host, c.Port, c.User, c.Password, c.Database = my.host, my.port, my.user, my.password, my.database
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	fs := startFedServer(t, store)
	if fs.fed.Catalogs()["ops"] != federation.MySQL {
		list, _ := fs.fed.List(ctx)
		t.Fatalf("not attached: %+v", list)
	}
	campaigns := "ops." + my.database + "." + table
	rita := fs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, rita, "SELECT * FROM "+campaigns, "connection ops")
	mustExec(t, fs.admin, "GRANT SELECT ON SCHEMA ops."+my.database+" TO rita")
	if n := count(t, rita, "SELECT sum(budget) FROM lake.public.audiences a JOIN "+campaigns+" c ON c.id = a.customer_id WHERE c.client = 'acme'"); n != 300 {
		t.Errorf("join: %d", n)
	}
	expectDenied(t, rita, "SELECT * FROM mysql_query('ops', 'SELECT 1')", "other databases")
	expectDenied(t, rita, "SELECT * FROM mysql_clear_cache()", "admin")
}
