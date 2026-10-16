package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// These tests run against the PostgreSQL of IGLOO_TEST_DATABASE_URL and
// the MySQL of IGLOO_TEST_MYSQL_URL (mysql://user:password@host:port/db),
// which scripts/test.sh starts, with DuckDB's postgres and mysql extensions
// from IGLOO_TEST_EXTENSION_DIR (scripts/test.sh fills it), or downloaded
// once into DuckDB's default directory without it.

var installOnce sync.Once
var installErr error

// installExtensions installs the extensions the connections need, or skips
// the test when it cannot.
func installExtensions(t *testing.T) {
	t.Helper()
	installOnce.Do(func() {
		ctx := context.Background()
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			installErr = err
			return
		}
		defer connector.Close() //nolint:errcheck
		c, err := connector.Connect(ctx)
		if err != nil {
			installErr = err
			return
		}
		defer c.Close() //nolint:errcheck
		for _, stmt := range append(useExtensionDir(), "INSTALL postgres", "INSTALL mysql") {
			if _, err := c.(*duckdb.Conn).ExecContext(ctx, stmt, nil); err != nil {
				installErr = err
				return
			}
		}
	})
	if installErr != nil {
		t.Skipf("cannot install DuckDB's postgres and mysql extensions: %v", installErr)
	}
}

// useExtensionDir returns the statement that points a DuckDB connection at
// IGLOO_TEST_EXTENSION_DIR, if it is set.
func useExtensionDir() []string {
	if dir := os.Getenv("IGLOO_TEST_EXTENSION_DIR"); dir != "" {
		return []string{"SET extension_directory = '" + strings.ReplaceAll(dir, "'", "''") + "'"}
	}
	return nil
}

// remote is a database the tests reach.
type remote struct {
	host           string
	port           int
	user, password string
	database       string
}

func (r remote) connection(name string, t Type) Connection {
	c := New(name, t)
	c.Host, c.Port, c.User, c.Password, c.Database = r.host, r.port, r.user, r.password, r.database
	c.ConnectTimeout = 5 * time.Second
	// The test databases have no TLS.
	c.TLS = TLSDisable
	return c
}

func parseRemote(t *testing.T, env string) remote {
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
	return remote{host: u.Hostname(), port: port, user: u.User.Username(), password: pw, database: strings.TrimPrefix(u.Path, "/")}
}

func randomName(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// startEngine opens an engine that attaches store's connections.
func startEngine(t *testing.T, store Store) (*Manager, *engine.Engine) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := NewManager(store, Options{}, log)
	cfg := config.Config{DuckDB: config.DuckDB{ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")}}
	eng, err := engine.Open(context.Background(), cfg, log, engine.BeforeLockdown(m.Attach))
	if err != nil {
		t.Fatal(err)
	}
	m.SetConnect(eng.Connect)
	t.Cleanup(func() {
		m.closeAll()
		_ = eng.Close()
	})
	return m, eng
}

func query(t *testing.T, eng *engine.Engine, sql string) (string, error) {
	t.Helper()
	conn, err := eng.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	rows, err := conn.QueryContext(context.Background(), sql, nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	var out []string
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return strings.Join(out, "\n"), nil
			}
			return "", err
		}
		var parts []string
		for _, v := range dest {
			parts = append(parts, toString(v))
		}
		out = append(out, strings.Join(parts, "|"))
	}
}

// toString prints a value; queries cast anything unusual to VARCHAR.
func toString(v any) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(v)
}

func expect(t *testing.T, eng *engine.Engine, sql, want string) {
	t.Helper()
	got, err := query(t, eng, sql)
	if err != nil || got != want {
		t.Errorf("%s: got %q, %v; want %q", sql, got, err, want)
	}
}

func expectError(t *testing.T, eng *engine.Engine, sql, mentions string) {
	t.Helper()
	_, err := query(t, eng, sql)
	if err == nil || !strings.Contains(err.Error(), mentions) {
		t.Errorf("%s: got %v, want an error mentioning %q", sql, err, mentions)
	}
}

// forwarder relays TCP connections to target until stopped, standing in
// for a network path to a remote database that can fail.
type forwarder struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
}

func newForwarder(t *testing.T, target string) *forwarder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &forwarder{ln: ln, target: target}
	go f.serve()
	t.Cleanup(f.stop)
	return f
}

func (f *forwarder) port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func (f *forwarder) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", f.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		f.mu.Lock()
		f.conns = append(f.conns, c, up)
		f.mu.Unlock()
		go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
		go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
	}
}

// stop refuses new connections and cuts the open ones.
func (f *forwarder) stop() {
	_ = f.ln.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

// restart listens on the same port again.
func (f *forwarder) restart(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", f.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	f.ln = ln
	go f.serve()
}

func TestPostgresConnection(t *testing.T) {
	installExtensions(t)
	pg := parseRemote(t, "IGLOO_TEST_DATABASE_URL")
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, os.Getenv("IGLOO_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx) //nolint:errcheck
	schema, role := randomName("fed_"), randomName("fedrole_")
	for _, sql := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".customers (id int PRIMARY KEY, name text, region text)",
		"INSERT INTO " + schema + ".customers SELECT g, 'c' || g, CASE WHEN g % 2 = 0 THEN 'east' ELSE 'west' END FROM generate_series(1, 50) g",
		"CREATE ROLE " + role + " LOGIN PASSWORD 'first-password'",
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"GRANT SELECT ON ALL TABLES IN SCHEMA " + schema + " TO " + role,
	} {
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_, _ = admin.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = admin.Exec(ctx, "DROP ROLE "+role)
	})

	store := NewMemoryStore()
	crm := pg.connection("crm", Postgres)
	crm.User, crm.Password, crm.Schemas = role, "first-password", []string{schema}
	crm.QueryTimeout = 2 * time.Second
	fwd := newForwarder(t, net.JoinHostPort(pg.host, strconv.Itoa(pg.port)))
	crm.Host, crm.Port = "127.0.0.1", fwd.port()
	if err := store.Create(ctx, crm); err != nil {
		t.Fatal(err)
	}
	down := pg.connection("down", Postgres)
	down.Host, down.Port, down.ConnectTimeout = "127.0.0.1", 1, time.Second
	if err := store.Create(ctx, down); err != nil {
		t.Fatal(err)
	}

	m, eng := startEngine(t, store)
	if cats := m.Catalogs(); cats["crm"] != Postgres || cats["down"] != "" {
		t.Fatalf("attached: %v", cats)
	}
	list, _ := m.List(ctx)
	for _, s := range list {
		switch s.Name {
		case "crm":
			if s.State != "attached" {
				t.Errorf("crm: %+v", s)
			}
		case "down":
			if s.State != "failed" || !strings.Contains(s.Detail, "restart to retry") {
				t.Errorf("down: %+v", s)
			}
		}
	}

	expect(t, eng, "SELECT count(*) FROM crm."+schema+".customers WHERE region = 'east'", "25")
	expect(t, eng, "SELECT count(*) FROM crm."+schema+".customers c JOIN (SELECT range AS id FROM range(1, 11)) a USING (id)", "10")
	// DuckDB never learns the credentials.
	expect(t, eng, "SELECT count(*) FROM duckdb_databases() WHERE path LIKE '%first-password%' OR path LIKE '%"+role+"%'", "0")
	// Read-only twice over: DuckDB's attachment, and the remote session.
	expectError(t, eng, "INSERT INTO crm."+schema+".customers VALUES (100, 'x', 'y')", "read-only")
	expectError(t, eng, "CALL postgres_execute('crm', 'CREATE TABLE "+schema+".evil (i int)')", "read-only transaction")
	// The remote server ends statements after query_timeout.
	expectError(t, eng, "SELECT * FROM postgres_query('crm', 'SELECT pg_sleep(5)')", "statement timeout")

	// Rotating the password: the old one stops working on the server, and
	// the connection is changed without a restart.
	if _, err := admin.Exec(ctx, "ALTER ROLE "+role+" PASSWORD 'second-password'"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Alter(ctx, "crm", func(c *Connection) error { return c.Set("password", "wrong") }); err == nil {
		t.Error("a password that does not work was accepted")
	}
	if _, err := m.Alter(ctx, "crm", func(c *Connection) error { return c.Set("password", "second-password") }); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1", role); err != nil {
		t.Fatal(err)
	}
	expect(t, eng, "SELECT count(*) FROM crm."+schema+".customers", "50")

	// The database becomes unreachable: listing tables still works from
	// what was loaded, queries of other data are unaffected, and queries
	// of the connection fail instead of hanging.
	fwd.stop()
	expect(t, eng, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'crm'", "1")
	expect(t, eng, "SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'crm'", "1")
	expect(t, eng, "SELECT 42", "42")
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1", role); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := query(t, eng, "SELECT count(*) FROM crm."+schema+".customers"); err == nil {
		t.Error("a query of an unreachable database succeeded")
	} else if strings.Contains(err.Error(), m.token) {
		t.Errorf("the error shows the proxy token: %v", err)
	} else if !strings.Contains(err.Error(), "connection crm") {
		t.Errorf("the error does not name the connection: %v", err)
	}
	if time.Since(start) > 20*time.Second {
		t.Errorf("a query of an unreachable database took %s", time.Since(start))
	}
	if err := m.Refresh(ctx, "crm"); err == nil {
		t.Error("refreshed a connection whose database is down")
	}
	fwd.restart(t)
	expect(t, eng, "SELECT count(*) FROM crm."+schema+".customers", "50")
	if err := m.Refresh(ctx, "crm"); err != nil {
		t.Errorf("refresh: %v", err)
	}

	// Dropping detaches at once.
	gen := m.Generation()
	if err := m.Drop(ctx, "crm"); err != nil {
		t.Fatal(err)
	}
	if m.Generation() == gen {
		t.Error("dropping did not change the generation")
	}
	expectError(t, eng, "SELECT count(*) FROM crm."+schema+".customers", "crm")
	expect(t, eng, "SELECT count(*) FROM duckdb_secrets() WHERE name LIKE 'igloo_connection_%'", "0")
}

func TestMySQLConnection(t *testing.T) {
	installExtensions(t)
	my := parseRemote(t, "IGLOO_TEST_MYSQL_URL")
	ctx := context.Background()
	table := randomName("campaigns_")

	// Set up through DuckDB's own MySQL client, before any sandbox.
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
	for _, sql := range append(useExtensionDir(),
		"LOAD mysql",
		"ATTACH '"+dsn+"' AS setup (TYPE mysql)",
		"CALL mysql_execute('setup', 'CREATE TABLE "+table+" (id INT PRIMARY KEY, name VARCHAR(50), budget DECIMAL(10,2))')",
		"CALL mysql_execute('setup', 'INSERT INTO "+table+" VALUES (1, ''spring'', 1000), (2, ''summer'', 2000), (3, ''fall'', 3000)')",
	) {
		if _, err := sc.(*duckdb.Conn).ExecContext(ctx, sql, nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = sc.(*duckdb.Conn).ExecContext(ctx, "CALL mysql_execute('setup', 'DROP TABLE "+table+"')", nil)
	})

	for _, mode := range []TLSMode{TLSPrefer, TLSDisable} {
		t.Run(string(mode), func(t *testing.T) {
			store := NewMemoryStore()
			ops := my.connection("ops", MySQL)
			ops.TLS, ops.QueryTimeout = mode, 2*time.Second
			fwd := newForwarder(t, net.JoinHostPort(my.host, strconv.Itoa(my.port)))
			ops.Host, ops.Port = "127.0.0.1", fwd.port()
			if err := store.Create(ctx, ops); err != nil {
				t.Fatal(err)
			}
			m, eng := startEngine(t, store)
			if m.Catalogs()["ops"] != MySQL {
				list, _ := m.List(ctx)
				t.Fatalf("not attached: %+v", list)
			}
			db := "ops." + my.database + "." + table
			expect(t, eng, "SELECT count(*), sum(budget)::INT FROM "+db+" WHERE budget > 1500", "2|5000")
			expect(t, eng, "SELECT name FROM "+db+" WHERE id = 1", "spring")
			expectError(t, eng, "INSERT INTO "+db+" VALUES (9, 'x', 1)", "read-only")
			// The remote session is read-only too.
			expectError(t, eng, "SELECT * FROM mysql_query('ops', 'INSERT INTO "+table+" VALUES (9, ''x'', 1)')", "READ ONLY")
			expect(t, eng, "SELECT count(*) FROM "+db, "3")
			// Prepared statements and their parameters go through the
			// proxy too.
			expect(t, eng, "SELECT * FROM mysql_query('ops', 'SELECT count(*) FROM "+table+" WHERE id > ?', params=row(1))", "2")

			// The database becomes unreachable: its tables are still
			// listed, queries of them fail naming the connection, and
			// work again once it is back.
			fwd.stop()
			expect(t, eng, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'ops' AND table_name = '"+table+"'", "1")
			expectError(t, eng, "SELECT count(*) FROM "+db, "connection ops")
			expectError(t, eng, "SELECT count(*) FROM "+db, "connection ops")
			fwd.restart(t)
			expect(t, eng, "SELECT count(*) FROM "+db, "3")
		})
	}
}

func TestPostgresStore(t *testing.T) {
	raw := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	key := bytes.Repeat([]byte{1}, 32)
	s, err := OpenPostgresStore(ctx, raw, key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// The grants table belongs to package auth, which creates it.
	users, err := auth.OpenPostgresStore(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	users.Close()
	name := randomName("conn_")
	c := New(name, Postgres)
	c.Host, c.Database, c.User, c.Password, c.Schemas, c.CreatedBy = "db.example.com", "crm", "reader", "s3cret-pw", []string{"a", "b"}, "root"
	if err := s.Create(ctx, c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Delete(ctx, name) })
	if err := s.Create(ctx, c); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate: %v", err)
	}
	var stored []byte
	if err := s.pool.QueryRow(ctx, "SELECT password FROM igloo_auth.connections WHERE name = $1", name).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if len(stored) == 0 || bytes.Contains(stored, []byte("s3cret-pw")) {
		t.Errorf("the password is not encrypted: %q", stored)
	}
	find := func(s Store) Connection {
		list, err := s.List(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range list {
			if x.Name == name {
				return x
			}
		}
		t.Fatalf("%s not listed", name)
		return Connection{}
	}
	got := find(s)
	if got.Password != "s3cret-pw" || got.ID == "" || strings.Join(got.Schemas, ",") != "a,b" || got.CreatedBy != "root" ||
		got.ConnectTimeout != defaultConnectTimeout || got.Unusable != "" {
		t.Errorf("listed: %+v", got)
	}
	got.Host = "other.example.com"
	_ = got.Set("password_secret", "crm")
	if err := s.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if again := find(s); again.Host != "other.example.com" || again.Password != "" || again.PasswordSecret != "crm" || again.ID != got.ID {
		t.Errorf("after update: %+v", again)
	}
	_ = got.Set("password", "back-again")
	if err := s.Update(ctx, got); err != nil {
		t.Fatal(err)
	}

	// Another deployment's key cannot decrypt it, and says so.
	other, err := OpenPostgresStore(ctx, raw, bytes.Repeat([]byte{2}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if x := find(other); x.Password != "" || x.Unusable == "" {
		t.Errorf("decrypted with the wrong key: %+v", x)
	}

	if err := s.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, name); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleting twice: %v", err)
	}
	if err := s.Update(ctx, got); !errors.Is(err, ErrNotFound) {
		t.Errorf("updating a deleted connection: %v", err)
	}
}
