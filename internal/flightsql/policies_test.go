package flightsql

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// discard is a ResultWriter that keeps nothing, for setting up.
type discard struct{}

func (discard) Columns([]pgwire.Column) error { return nil }
func (discard) Row([]any) error               { return nil }
func (discard) Complete(string) error         { return nil }

// startPolicyServer is startServer with a stand-in Iceberg catalog, lake,
// and custom roles, and an agency's contacts protected by policies: rita's
// team sees acme's rows only, with emails masked.
func startPolicyServer(t *testing.T) *testServer {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(context.Background(), config.Config{}, log)
	if err != nil {
		t.Fatal(err)
	}
	users := auth.NewMemoryDirectory()
	addUser(t, users, "igloo", auth.RoleAdmin)
	addUser(t, users, "rita", auth.RoleReader)
	history := audit.NewMemoryStore(1000, 1000)
	hlog := audit.New(history, audit.Options{}, log)
	eng.SetHistory(hlog, pgwire.HistoryViewer)
	sql, err := pgwire.New(eng, pgwire.Options{Users: users, Audit: hlog, Catalog: "lake",
		Grants: users}, log)
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
	ts := &testServer{addr: ln.Addr().String(), users: users, history: history, sql: sql}
	ts.admin(t,
		"ATTACH ':memory:' AS lake",
		"CREATE SCHEMA lake.crm",
		"CREATE SCHEMA lake.gov",
		"CREATE TABLE lake.crm.contacts (id INTEGER, client VARCHAR, email VARCHAR)",
		"INSERT INTO lake.crm.contacts VALUES (1, 'acme', 'ann@acme.com'), (2, 'globex', 'gus@globex.com')",
		"CREATE TABLE lake.gov.client_access (role_name VARCHAR, client VARCHAR)",
		"INSERT INTO lake.gov.client_access VALUES ('acme_team', 'acme')",
		"CREATE ROLE acme_team",
		"GRANT acme_team TO rita",
		`CREATE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR -> '***'`,
		`CREATE ROW ACCESS POLICY client_rows AS (c VARCHAR) RETURNS BOOLEAN ->
			EXISTS (SELECT 1 FROM lake.gov.client_access a WHERE a.client = c AND is_role_in_session(a.role_name))`,
		"ALTER TABLE lake.crm.contacts ALTER COLUMN email SET MASKING POLICY mask_email",
		"ALTER TABLE lake.crm.contacts ADD ROW ACCESS POLICY client_rows ON (client)",
	)
	return ts
}

// admin runs each of sqls, one at a time, in an admin's embedded session.
func (ts *testServer) admin(t *testing.T, sqls ...string) {
	t.Helper()
	igloo, err := ts.users.Get(context.Background(), "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ts.sql.OpenEmbedded(context.Background(), igloo, "setup")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	for _, sql := range sqls {
		if err := es.Run(context.Background(), sql, discard{}); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

// TestPoliciesApplyOverFlightSQL reads a protected table over Flight SQL,
// Igloo's second query path: statements, prepared statements and their
// results apply the same policies as the Postgres protocol, in DuckDB's
// own Arrow batches.
func TestPoliciesApplyOverFlightSQL(t *testing.T) {
	ts := startPolicyServer(t)
	cl, ctx := ts.signIn(t, "rita")
	secrets := []string{"ann@", "gus@", "globex"}
	shows := func(rows [][]string) string {
		for _, s := range secrets {
			if strings.Contains(flat(rows), s) {
				return s
			}
		}
		return ""
	}

	// A statement: masked emails, and none of globex's rows.
	if _, rows := query(t, ctx, cl, "SELECT id, client, email FROM lake.crm.contacts ORDER BY id"); flat(rows) != "1 acme ***" {
		t.Errorf("statement: %q", flat(rows))
	}
	// Conditions see masked values, so a raw value finds nothing.
	if _, rows := query(t, ctx, cl, "SELECT count(*) FROM lake.crm.contacts WHERE email = 'ann@acme.com'"); flat(rows) != "0" {
		t.Errorf("filter on a raw value: %q", flat(rows))
	}
	for _, sql := range []string{
		"SELECT * FROM lake.crm.contacts",
		"SELECT max(email), count(*) FROM lake.crm.contacts WHERE client = 'globex'",
		"SELECT email FROM lake.crm.contacts UNION ALL SELECT client FROM lake.crm.contacts",
	} {
		if _, rows := query(t, ctx, cl, sql); shows(rows) != "" {
			t.Errorf("%s shows %q: %q", sql, shows(rows), flat(rows))
		}
	}

	// A prepared statement, run again after the policy changes, applies the
	// policy in force when it runs, not when it was prepared.
	ps, err := cl.Prepare(ctx, "SELECT email FROM lake.crm.contacts ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close(ctx) //nolint:errcheck
	run := func() string {
		t.Helper()
		info, err := ps.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, rows, err := fetch(ctx, cl, info)
		if err != nil {
			t.Fatal(err)
		}
		return flat(rows)
	}
	if got := run(); got != "***" {
		t.Errorf("prepared: %q", got)
	}
	ts.admin(t, `CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR -> 'hidden'`)
	if got := run(); got != "hidden" {
		t.Errorf("prepared, after the policy changed: %q", got)
	}
}
