package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
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
	"github.com/akkio-inc/igloo/internal/icebergtest"
)

// These tests attach icebergtest's catalog as a connection of TYPE
// iceberg, behind a stand-in for a catalog that signs clients in with
// OAuth2, its tables' files in a directory of the test. They need DuckDB's
// iceberg extension from IGLOO_TEST_EXTENSION_DIR.

// remoteCatalog is a remote Iceberg REST catalog for a test: icebergtest's,
// behind OAuth2.
type remoteCatalog struct {
	fake *icebergtest.Catalog
	srv  *httptest.Server
	// URL is the catalog's endpoint, as a connection names it.
	URL string
	dir string

	mu sync.Mutex
	// token is the access token the catalog takes, renewed by revoke.
	token string
	// tokens counts the tokens it gave, writes the requests that would
	// have changed the catalog, and requests every request.
	tokens, writes atomic.Int32
	requests       []string
}

const remoteCredential = "igloo-client:a-client-secret-for-tests"

func newRemoteCatalog(t *testing.T) *remoteCatalog {
	t.Helper()
	rc := &remoteCatalog{dir: filepath.Join(t.TempDir(), "remote"), token: "token-1"}
	rc.fake = icebergtest.NewCatalog(rc.dir)
	t.Cleanup(rc.fake.Close)
	target, _ := url.Parse(rc.fake.URL)
	proxy := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
		r.Out.URL.Scheme, r.Out.URL.Host, r.Out.Host = target.Scheme, target.Host, target.Host
	}}
	rc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc.mu.Lock()
		rc.requests = append(rc.requests, r.Method+" "+r.URL.Path)
		token := rc.token
		rc.mu.Unlock()
		if r.URL.Path == "/catalog/v1/oauth/tokens" {
			_ = r.ParseForm()
			if r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("client_id") != "igloo-client" ||
				r.Form.Get("client_secret") != "a-client-secret-for-tests" || r.Form.Get("scope") != "PRINCIPAL_ROLE:ALL" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
				return
			}
			rc.tokens.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"` + token + `","token_type":"bearer","expires_in":3600}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"message":"not signed in","type":"NotAuthorizedException","code":401}}`))
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			rc.writes.Add(1)
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(rc.srv.Close)
	rc.URL = rc.srv.URL + "/catalog"
	return rc
}

// revoke makes the catalog take only a new token.
func (rc *remoteCatalog) revoke(token string) {
	rc.mu.Lock()
	rc.token = token
	rc.mu.Unlock()
}

// exec runs statements on the remote catalog with a DuckDB of its own, as
// another engine writing to it would.
func (rc *remoteCatalog) exec(t *testing.T, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close() //nolint:errcheck
	c, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	conn := c.(*duckdb.Conn)
	setup := []string{
		"SET extension_directory = " + quoteLiteral(os.Getenv("IGLOO_TEST_EXTENSION_DIR")), "LOAD iceberg",
		"ATTACH 'wh' AS r (TYPE iceberg, ENDPOINT " + quoteLiteral(rc.fake.URL) + ", AUTHORIZATION_TYPE 'none')",
	}
	for _, s := range append(setup, stmts...) {
		if _, err := conn.ExecContext(ctx, s, nil); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// icebergServer is Igloo with a connection of TYPE iceberg called ext.
type icebergServer struct {
	*catalogServer
	fed    *federation.Manager
	remote *remoteCatalog
	eng    *engine.Engine
}

// startIcebergServer starts Igloo with store's connections. lake makes its
// catalog icebergtest's too, as Igloo runs on Lakekeeper, so that its scans
// look like the connection's; otherwise it is a DuckDB database.
func startIcebergServer(t *testing.T, store *federation.MemoryStore, rc *remoteCatalog, lake bool) *icebergServer {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: ext, TempDir: t.TempDir()}}
	lakeDir := filepath.Join(t.TempDir(), "lake")
	if lake {
		fake := icebergtest.NewCatalog(lakeDir)
		t.Cleanup(fake.Close)
		cfg.Catalog = config.Catalog{URI: fake.URL, Warehouse: "test", Name: "lake", Schema: "public"}
		cfg.Storage = config.Storage{Location: lakeDir, S3: config.S3{Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused"}}
	}
	fed := federation.NewManager(store, federation.Options{LocalLocations: true, Reserved: []string{lakeDir}}, log)
	eng, err := engine.Open(ctx, cfg, log, engine.BeforeLockdown(fed.Attach), engine.SandboxDirectories(fed.SandboxDirectories))
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
	srv, err := New(eng, Options{Users: users, RoleRefresh: time.Hour, Grants: cs.grants, Catalog: "lake", Audit: hist, Federation: fed,
		AllowedLocations: []string{"s3://bucket/"}}, log)
	if err != nil {
		t.Fatal(err)
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
	cs.testServer = &testServer{addr: ln.Addr().String(), store: users, srv: srv}
	cs.admin = cs.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	setup := []string{"CREATE SCHEMA IF NOT EXISTS lake.public"}
	if !lake {
		setup = append([]string{"ATTACH ':memory:' AS lake"}, setup...)
	}
	setup = append(setup,
		"CREATE TABLE lake.public.audiences (customer_id INTEGER, segment VARCHAR)",
		"INSERT INTO lake.public.audiences VALUES (1, 'loyal'), (2, 'loyal'), (5, 'new')")
	for _, sql := range setup {
		mustExec(t, cs.admin, sql)
	}
	return &icebergServer{catalogServer: cs, fed: fed, remote: rc, eng: eng}
}

// remoteFixture fills the remote catalog: sales.orders, sales.pii, and
// other.hidden in a namespace the connection does not attach.
func remoteFixture(t *testing.T, rc *remoteCatalog) {
	rc.exec(t,
		"CREATE SCHEMA r.sales",
		"CREATE TABLE r.sales.orders AS SELECT range::INTEGER AS id, (range % 3)::INTEGER AS customer_id, 'order ' || range AS note FROM range(30)",
		"CREATE TABLE r.sales.pii AS SELECT 1 AS id, '123-45-6789' AS ssn",
		"CREATE SCHEMA r.other",
		"CREATE TABLE r.other.hidden AS SELECT 42 AS x",
	)
}

// icebergConnection is ext, the remote catalog's connection.
func icebergConnection(t *testing.T, rc *remoteCatalog) federation.Connection {
	t.Helper()
	c := federation.New("ext", federation.Iceberg)
	for k, v := range map[string]string{
		"endpoint": rc.URL, "warehouse": "wh", "credential": remoteCredential, "scope": "PRINCIPAL_ROLE:ALL", "tls": "disable",
		"schema": "sales",
	} {
		if err := c.Set(k, v); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func TestIcebergConnection(t *testing.T) {
	t.Parallel()
	for _, lake := range []bool{false, true} {
		name := "duckdb lake"
		if lake {
			name = "iceberg lake"
		}
		t.Run(name, func(t *testing.T) { testIcebergConnection(t, lake) })
	}
}

func testIcebergConnection(t *testing.T, lake bool) {
	rc := newRemoteCatalog(t)
	remoteFixture(t, rc)
	store := federation.NewMemoryStore()
	if err := store.Create(context.Background(), icebergConnection(t, rc)); err != nil {
		t.Fatal(err)
	}
	is := startIcebergServer(t, store, rc, lake)
	ctx := context.Background()
	orders, pii := "ext.sales.orders", "ext.sales.pii"

	// SHOW CONNECTIONS says what was attached, never the credential.
	got := queryAll(t, is.admin, "SHOW CONNECTIONS")
	if !strings.Contains(got, "ext|iceberg|"+rc.URL+"|") || !strings.Contains(got, "|attached|2 tables attached|") ||
		!strings.Contains(got, "credential stored") || !strings.Contains(got, "scope=PRINCIPAL_ROLE:ALL") {
		t.Errorf("SHOW CONNECTIONS: %s", got)
	}
	if strings.Contains(got, "a-client-secret") || strings.Contains(got, "token-1") {
		t.Errorf("SHOW CONNECTIONS shows a credential: %s", got)
	}

	// Admins read the remote tables, and join them with the catalog's.
	if n := count(t, is.admin, "SELECT count(*) FROM "+orders); n != 30 {
		t.Errorf("admin: %d", n)
	}
	if n := count(t, is.admin, "SELECT count(*) FROM lake.public.audiences a JOIN "+orders+" o ON o.customer_id = a.customer_id"); n != 20 {
		t.Errorf("join: %d", n)
	}
	// Their columns are known before the first read.
	if n := count(t, is.admin, "SELECT count(*) FROM information_schema.columns WHERE table_catalog = 'ext' AND table_name = 'pii'"); n != 2 {
		t.Errorf("pii's columns: %d", n)
	}
	// Only the namespaces asked for are attached.
	expectQueryFails(t, is.admin, "SELECT * FROM ext.other.hidden", "")

	// No one writes to the remote catalog, admins included.
	for _, sql := range []string{
		"INSERT INTO " + orders + " VALUES (100, 1, 'x')",
		"DELETE FROM " + orders,
		"UPDATE " + orders + " SET note = 'x'",
		"CREATE TABLE ext.sales.copy AS SELECT 1 AS x",
		"DROP TABLE " + pii,
		"ALTER TABLE " + orders + " ADD COLUMN y INTEGER",
		"CREATE SCHEMA ext.newschema",
		"DETACH ext",
	} {
		if _, err := is.admin.Exec(ctx, sql); err == nil {
			t.Errorf("%s: an admin changed the remote catalog", sql)
		}
	}
	if n := rc.writes.Load(); n != 0 {
		t.Errorf("%d requests to change the remote catalog reached it", n)
	}
	// Copying a remote table into the warehouse works.
	mustExec(t, is.admin, "CREATE TABLE lake.public.orders_copy AS SELECT * FROM "+orders)
	if n := count(t, is.admin, "SELECT count(*) FROM lake.public.orders_copy"); n != 30 {
		t.Errorf("copy: %d", n)
	}

	rita := is.user(t, uniqueName("rita"), auth.RoleReader, pgx.QueryExecModeCacheStatement)
	nina := is.user(t, uniqueName("nina"), auth.RoleNone, pgx.QueryExecModeSimpleProtocol)
	walt := is.user(t, uniqueName("walt"), auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)

	// Readers read the catalog's tables as before.
	if n := count(t, rita, "SELECT count(*) FROM lake.public.audiences"); n != 3 {
		t.Errorf("reader, catalog: %d", n)
	}
	// Not even readers read the remote tables without a grant, however
	// they name them.
	for _, sql := range []string{
		"SELECT * FROM " + orders,
		"SELECT count(*) FROM " + orders + " WHERE customer_id = 1",
		"SELECT * FROM lake.public.audiences a JOIN " + orders + " o ON o.customer_id = a.customer_id",
		"SELECT (SELECT max(note) FROM " + orders + ")",
		"WITH x AS (SELECT * FROM " + orders + ") SELECT * FROM x",
		"SELECT * FROM lake.public.audiences UNION ALL SELECT customer_id, note FROM " + orders,
		"SUMMARIZE " + orders,
		"EXPLAIN ANALYZE SELECT * FROM " + orders,
		"SET VARIABLE v = (SELECT max(note) FROM " + orders + ")",
		"SELECT * FROM " + orders + " AT (VERSION => " + strconv.FormatInt(rc.fake.Metadata("sales", "orders").CurrentSnapshot().SnapshotID, 10) + ")",
		"SELECT * FROM iceberg_scan(" + orders + ")",
		"SELECT * FROM iceberg_metadata(" + orders + ")",
		"SELECT * FROM iceberg_snapshots(" + orders + ")",
		"SELECT * FROM EXT.sales.orders",
	} {
		expectDenied(t, rita, sql, "")
		expectDenied(t, nina, sql, "")
	}
	// In other case it names no table: DuckDB looks tables of Iceberg REST
	// catalogs up as their catalogs name them.
	expectQueryFails(t, rita, "SELECT * FROM ext.sales.ORDERS", "does not exist")
	// Nor through the files.
	files := remoteDataFiles(t, rc.dir)
	if len(files) == 0 {
		t.Fatal("no data files in the remote catalog")
	}
	expectDenied(t, rita, "SELECT * FROM read_parquet('"+files[0]+"')", "")
	expectDenied(t, rita, "SELECT * FROM iceberg_scan('"+filepath.Dir(filepath.Dir(files[0]))+"')", "")
	// A writer cannot copy them into the warehouse either.
	expectDenied(t, walt, "CREATE TABLE lake.public.stolen AS SELECT * FROM "+pii, "")
	expectDenied(t, walt, "INSERT INTO lake.public.audiences SELECT id, ssn FROM "+pii, "")
	expectDenied(t, walt, "CREATE VIEW lake.public.v_pii AS SELECT * FROM "+pii, "")
	// Metadata needs no grant, as for other connections.
	expectAllowed(t, rita, "DESCRIBE "+orders)

	// A grant on one table.
	mustExec(t, is.admin, "GRANT SELECT ON TABLE "+orders+" TO "+userName(t, rita))
	if n := count(t, rita, "SELECT count(*) FROM lake.public.audiences a JOIN "+orders+" o ON o.customer_id = a.customer_id WHERE o.id < $1", 9); n != 6 {
		t.Errorf("reader, join with a parameter: %d", n)
	}
	if n := count(t, rita, "SELECT count(*) FROM (SELECT * FROM "+orders+" UNION ALL SELECT * FROM "+orders+")"); n != 60 {
		t.Errorf("reader, twice: %d", n)
	}
	expectDenied(t, rita, "SELECT * FROM "+pii, "pii")
	expectDenied(t, rita, "SELECT * FROM "+orders+" o JOIN "+pii+" p USING (id)", "pii")
	// Table functions on the remote tables stay refused: they read them
	// without naming them as tables.
	expectDenied(t, rita, "SELECT * FROM iceberg_scan("+orders+")", "admin")
	// Names along the search path need the grant too.
	mustExec(t, rita, "SET search_path = 'ext.sales,lake.public'")
	if n := count(t, rita, "SELECT count(*) FROM orders"); n != 30 {
		t.Errorf("reader, search path: %d", n)
	}
	expectDenied(t, rita, "SELECT * FROM pii", "pii")
	mustExec(t, rita, "SET search_path = 'lake.public,memory.main'")

	// None-role users read granted tables only, the catalog's included.
	mustExec(t, is.admin, "GRANT SELECT ON SCHEMA ext.sales TO "+userName(t, nina))
	if n := count(t, nina, "SELECT count(*) FROM "+pii); n != 1 {
		t.Errorf("none role: %d", n)
	}
	expectDenied(t, nina, "SELECT * FROM lake.public.audiences a JOIN "+orders+" o ON o.customer_id = a.customer_id", "audiences")
	mustExec(t, is.admin, "GRANT SELECT ON TABLE lake.public.audiences TO "+userName(t, nina))
	if n := count(t, nina, "SELECT count(*) FROM lake.public.audiences a JOIN "+orders+" o ON o.customer_id = a.customer_id"); n != 20 {
		t.Errorf("none role, join: %d", n)
	}

	// Views read with the reader's grants.
	mine, theirs := "memory.main."+uniqueName("mine_"), "memory.main."+uniqueName("theirs_")
	mustExec(t, is.admin, "CREATE VIEW "+mine+" AS SELECT * FROM "+orders)
	mustExec(t, is.admin, "CREATE VIEW "+theirs+" AS SELECT p.* FROM lake.public.audiences a, "+pii+" p")
	expectAllowed(t, rita, "SELECT * FROM "+mine)
	expectDenied(t, rita, "SELECT * FROM "+theirs, "pii")

	// Revoking reaches prepared statements.
	expectQueryAllowed(t, rita, "SELECT * FROM "+orders)
	mustExec(t, is.admin, "REVOKE SELECT ON TABLE "+orders+" FROM "+userName(t, rita))
	expectQueryDenied(t, rita, "SELECT * FROM "+orders, "orders")

	// A grant on the connection covers it all.
	mustExec(t, is.admin, "GRANT SELECT ON CONNECTION ext TO "+userName(t, rita))
	expectAllowed(t, rita, "SELECT * FROM "+theirs)

	// A table created on the remote catalog since startup is not attached:
	// the sandbox was locked with the locations of the tables then.
	rc.exec(t, "CREATE TABLE r.sales.later AS SELECT 1 AS x")
	expectQueryFails(t, is.admin, "SELECT * FROM ext.sales.later", "")
	// New rows of the attached ones are read, the catalog's answers being
	// fetched again.
	rc.exec(t, "INSERT INTO r.sales.orders VALUES (100, 1, 'late')")
	if n := count(t, is.admin, "SELECT count(*) FROM "+orders); n != 31 {
		t.Errorf("after a remote insert: %d", n)
	}

	// A revoked token is replaced by a new one from the token endpoint.
	before := rc.tokens.Load()
	rc.revoke("token-2")
	if n := count(t, is.admin, "SELECT count(*) FROM "+orders); n != 31 {
		t.Errorf("after the token changed: %d", n)
	}
	if rc.tokens.Load() <= before {
		t.Error("no new token was fetched")
	}

	// Sandbox: SQL reads no other local directory, admins included.
	expectQueryFails(t, is.admin, "SELECT * FROM read_text('"+filepath.Dir(rc.dir)+"/../x')", "")

	// Refresh needs a restart for these connections.
	if _, err := is.admin.Exec(ctx, "ALTER CONNECTION ext REFRESH"); pgCode(err) != "22023" {
		t.Errorf("REFRESH: %v", err)
	}

	// The history keeps the credential out.
	mustExec(t, is.admin, "ALTER CONNECTION ext SET (credential '"+remoteCredential+"')")
	deadline := time.Now().Add(10 * time.Second)
	var text string
	for time.Now().Before(deadline) {
		err := is.admin.QueryRow(ctx, `SELECT query_text FROM igloo.query_history WHERE query_text LIKE 'ALTER CONNECTION ext SET%' AND outcome = 'ok'`).Scan(&text)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if text == "" || strings.Contains(text, "a-client-secret") {
		t.Errorf("history: %q", text)
	}
	if _, err := is.admin.Exec(ctx, "ALTER CONNECTION ext SET (token unquoted_token_value)"); pgCode(err) != "42601" {
		t.Errorf("an unquoted token: %v", err)
	}

	// Dropping the connection detaches it and removes its grants.
	mustExec(t, is.admin, "DROP CONNECTION ext")
	expectQueryFails(t, is.admin, "SELECT * FROM "+orders, "")
	if got := queryAll(t, is.admin, "SHOW GRANTS"); strings.Contains(got, "ext.") {
		t.Errorf("grants outlive the connection: %s", got)
	}
}

// expectQueryFails checks that sql fails, with an error mentioning
// mentions.
func expectQueryFails(t *testing.T, conn *pgx.Conn, sql, mentions string) {
	t.Helper()
	err := runQuery(conn, sql)
	if err == nil {
		t.Errorf("%s: succeeded", sql)
		return
	}
	if !strings.Contains(err.Error(), mentions) {
		t.Errorf("%s: %v, want it to mention %q", sql, err, mentions)
	}
}

// remoteDataFiles lists the Parquet files under dir.
func remoteDataFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.HasSuffix(path, ".parquet") {
			out = append(out, path)
		}
		return err
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return out
}

// TestIcebergConnectionLeavesOutOverlaps checks that a table whose files
// are in the warehouse is left out, with why, and the replica still starts.
func TestIcebergConnectionLeavesOutOverlaps(t *testing.T) {
	t.Parallel()
	rc := newRemoteCatalog(t)
	remoteFixture(t, rc)
	store := federation.NewMemoryStore()
	c := icebergConnection(t, rc)
	if err := store.Create(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	// The remote catalog's whole directory is the "warehouse" here.
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	log := slog.New(slog.DiscardHandler)
	fed := federation.NewManager(store, federation.Options{LocalLocations: true, Reserved: []string{rc.dir}}, log)
	eng, err := engine.Open(context.Background(), config.Config{DuckDB: config.DuckDB{ExtensionDir: ext}}, log,
		engine.BeforeLockdown(fed.Attach), engine.SandboxDirectories(fed.SandboxDirectories))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if dirs := fed.SandboxDirectories(); len(dirs) != 0 {
		t.Errorf("the sandbox opens to %v", dirs)
	}
	list, err := fed.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].State != "attached" || !strings.Contains(list[0].Detail, "0 tables attached; 2 left out") ||
		!strings.Contains(list[0].Detail, "overlaps Igloo's warehouse") {
		t.Errorf("status: %+v", list)
	}
	conn, err := eng.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	rows, err := conn.QueryContext(context.Background(), "SELECT count(*) FROM ext.sales.orders", nil)
	if err == nil {
		dest := make([]driver.Value, 1)
		err = rows.Next(dest)
		_ = rows.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("reading a table left out: %v", err)
	}
}

// TestIcebergConnectionPolicies checks masking and row access policies on a
// remote catalog's table, whose policy reads a table of Igloo's catalog:
// both scans look alike in DuckDB's plan.
func TestIcebergConnectionPolicies(t *testing.T) {
	t.Parallel()
	for _, lake := range []bool{false, true} {
		name := "duckdb lake"
		if lake {
			name = "iceberg lake"
		}
		t.Run(name, func(t *testing.T) {
			rc := newRemoteCatalog(t)
			rc.exec(t,
				"CREATE SCHEMA r.sales",
				"CREATE TABLE r.sales.contacts AS SELECT * FROM (VALUES (1, 'acme', 'ann@acme.com'), (2, 'acme', 'al@acme.com'), "+
					"(3, 'globex', 'gus@globex.com')) v(id, client, email)",
			)
			store := federation.NewMemoryStore()
			if err := store.Create(context.Background(), icebergConnection(t, rc)); err != nil {
				t.Fatal(err)
			}
			is := startIcebergServer(t, store, rc, lake)
			admin, contacts := is.admin, "ext.sales.contacts"
			for _, sql := range []string{
				"CREATE SCHEMA lake.gov",
				"CREATE TABLE lake.gov.client_access (role_name VARCHAR, client VARCHAR)",
				"INSERT INTO lake.gov.client_access VALUES ('acme_team', 'acme')",
				"CREATE ROLE acme_team",
				`CREATE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR -> '***'`,
				`CREATE ROW ACCESS POLICY client_rows AS (c VARCHAR) RETURNS BOOLEAN ->
					EXISTS (SELECT 1 FROM lake.gov.client_access a WHERE a.client = c AND is_role_in_session(a.role_name))`,
				"ALTER TABLE " + contacts + " ALTER COLUMN email SET MASKING POLICY mask_email",
				"ALTER TABLE " + contacts + " ADD ROW ACCESS POLICY client_rows ON (client)",
				"GRANT SELECT ON TABLE " + contacts + " TO acme_team",
			} {
				mustExec(t, admin, sql)
			}
			rita := is.user(t, "rita", auth.RoleReader, pgx.QueryExecModeCacheStatement)
			mustExec(t, admin, "GRANT acme_team TO rita")
			want := func(conn *pgx.Conn, sql string, rows ...string) {
				t.Helper()
				if got := texts(t, conn, sql); strings.Join(got, ",") != strings.Join(rows, ",") {
					t.Errorf("%s: got %q, want %q", sql, got, rows)
				}
			}
			want(rita, "SELECT id, email FROM "+contacts+" ORDER BY id", "1|***", "2|***")
			want(rita, "SELECT count(*) FROM "+contacts+" WHERE email = 'ann@acme.com'", "0")
			want(rita, "SELECT count(*) FROM lake.gov.client_access a JOIN "+contacts+" c USING (client)", "2")
			want(admin, "SELECT count(*) FROM "+contacts, "0")
		})
	}
}
