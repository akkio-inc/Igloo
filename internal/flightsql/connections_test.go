package flightsql

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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// TestConnectionsNeedGrants checks that Flight SQL statements read
// connections' tables under the same grants as SQL clients': a reader, or
// a service account, without a grant on the connection is refused, in
// queries and prepared statements, and reads once granted. It runs
// against the PostgreSQL of IGLOO_TEST_DATABASE_URL, with DuckDB's
// postgres extension from IGLOO_TEST_EXTENSION_DIR (scripts/test.sh sets
// both).
func TestConnectionsNeedGrants(t *testing.T) {
	raw, ext := os.Getenv("IGLOO_TEST_DATABASE_URL"), os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if raw == "" || ext == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL or IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	remote, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close(ctx) //nolint:errcheck
	var b [4]byte
	_, _ = rand.Read(b[:])
	schema := "flight_" + hex.EncodeToString(b[:])
	for _, sql := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".customers (id int, email text)",
		"INSERT INTO " + schema + ".customers VALUES (1, 'ann@acme.com'), (2, 'gus@globex.com')",
	} {
		if _, err := remote.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = remote.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	pw, _ := u.User.Password()
	store := federation.NewMemoryStore()
	c := federation.New("crm", federation.Postgres)
	c.Host, c.Port, c.User, c.Password, c.Database = u.Hostname(), port, u.User.Username(), pw, strings.TrimPrefix(u.Path, "/")
	c.Schemas = []string{schema}
	if err := store.Create(ctx, c); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fed := federation.NewManager(store, federation.Options{}, log)
	eng, err := engine.Open(ctx, config.Config{DuckDB: config.DuckDB{ExtensionDir: ext}}, log, engine.BeforeLockdown(fed.Attach))
	if err != nil {
		t.Fatal(err)
	}
	fed.SetConnect(eng.Connect)
	if fed.Catalogs()["crm"] != federation.Postgres {
		list, _ := fed.List(ctx)
		t.Fatalf("not attached: %+v", list)
	}
	users := auth.NewMemoryDirectory()
	addUser(t, users, "igloo", auth.RoleAdmin)
	addUser(t, users, "rita", auth.RoleReader)
	hlog := audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, log)
	eng.SetHistory(hlog, pgwire.HistoryViewer)
	sql, err := pgwire.New(eng, pgwire.Options{Users: users, Audit: hlog, Federation: fed}, log)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Options{SQL: sql, Version: "test", Log: log})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln) //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = eng.Close()
	})
	ts := &testServer{addr: ln.Addr().String(), users: users, sql: sql}

	customers := "crm." + schema + ".customers"
	admin, adminCtx := ts.signIn(t, "igloo")
	if _, rows := query(t, adminCtx, admin, "SELECT count(*) FROM "+customers); rows[0][0] != "2" {
		t.Errorf("admin counted %v", rows)
	}
	rita, ritaCtx := ts.signIn(t, "rita")
	_, err = rita.Execute(ritaCtx, "SELECT * FROM "+customers)
	wantCode(t, "reader without a grant", err, codes.PermissionDenied, "connection crm")
	_, err = rita.Prepare(ritaCtx, "SELECT email FROM "+customers+" WHERE id = 1")
	wantCode(t, "reader prepares without a grant", err, codes.PermissionDenied, "connection crm")

	// A service account is a user like any other.
	update(t, adminCtx, admin, "CREATE USER etl TYPE SERVICE ROLE reader")
	_, rows := query(t, adminCtx, admin, "CREATE TOKEN nightly FOR USER etl")
	etl, etlCtx := ts.client(t), bearer(rows[0][2])
	_, err = etl.Execute(etlCtx, "SELECT * FROM "+customers)
	wantCode(t, "service account without a grant", err, codes.PermissionDenied, "connection crm")

	update(t, adminCtx, admin, "GRANT SELECT ON TABLE "+customers+" TO rita")
	update(t, adminCtx, admin, "GRANT SELECT ON CONNECTION crm TO etl")
	if _, rows := query(t, ritaCtx, rita, "SELECT count(*) FROM "+customers); rows[0][0] != "2" {
		t.Errorf("reader counted %v", rows)
	}
	prep, err := rita.Prepare(ritaCtx, "SELECT email FROM "+customers+" WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	info, err := prep.Execute(ritaCtx)
	if err != nil {
		t.Fatal(err)
	}
	if _, rows, err := fetch(ritaCtx, rita, info); err != nil || len(rows) != 1 || rows[0][0] != "ann@acme.com" {
		t.Errorf("prepared: %v %v", rows, err)
	}
	if _, rows := query(t, etlCtx, etl, "SELECT count(*) FROM "+customers); rows[0][0] != "2" {
		t.Errorf("service account counted %v", rows)
	}
	// Revoking reaches the prepared statement, which runs when its
	// result is fetched.
	update(t, adminCtx, admin, "REVOKE SELECT ON TABLE "+customers+" FROM rita")
	if info, err = prep.Execute(ritaCtx); err == nil {
		_, _, err = fetch(ritaCtx, rita, info)
	}
	wantCode(t, "prepared after the revoke", err, codes.PermissionDenied, "connection crm")
}
