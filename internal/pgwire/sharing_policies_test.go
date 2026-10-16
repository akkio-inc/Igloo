package pgwire

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// fakeExternal stands in for Delta Sharing's external tables: views an
// admin made in the local database, by lower-case schema.name.
type fakeExternal map[string]bool

func (f fakeExternal) IsExternal(schema, name string) (int, bool) {
	return 1, f[strings.ToLower(schema+"."+name)]
}

func (f fakeExternal) TableNames(schema string) []string {
	var out []string
	for k := range f {
		if s, n, _ := strings.Cut(k, "."); strings.EqualFold(s, schema) {
			out = append(out, n)
		}
	}
	return out
}

func (fakeExternal) Prepare(_ context.Context, query string) (string, error) { return query, nil }
func (fakeExternal) Sync(context.Context) (bool, error)                      { return false, nil }

func TestProtectedBy(t *testing.T) {
	cs := startPolicyServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, `CREATE MASKING POLICY pii_hash AS (val VARCHAR) RETURNS VARCHAR -> sha256(val)`)
	mustExec(t, cs.admin, "CREATE TAG pii")
	mustExec(t, cs.admin, "ALTER TAG pii SET MASKING POLICY pii_hash")
	mustExec(t, cs.admin, "ALTER TABLE lake.crm.contacts ALTER COLUMN device_id SET TAG pii = 'device'")
	if got := cs.srv.ProtectedBy(ctx, "CRM", "contacts"); !slices.Equal(got, []string{"client_rows", "mask_email", "pii_hash"}) {
		t.Errorf("ProtectedBy(crm.contacts) = %q", got)
	}
	if got := cs.srv.ProtectedBy(ctx, "public", "campaigns"); got != nil || cs.srv.Protected(ctx, "public", "campaigns") {
		t.Errorf("a table without policies: %q", got)
	}
	// A tag with no masking policy protects nothing.
	mustExec(t, cs.admin, "CREATE TAG cost_center")
	mustExec(t, cs.admin, "ALTER TABLE lake.public.campaigns ALTER COLUMN client SET TAG cost_center = 'x'")
	if got := cs.srv.ProtectedBy(ctx, "public", "campaigns"); got != nil {
		t.Errorf("a table with only a plain tag: %q", got)
	}
	mustExec(t, cs.admin, "ALTER TABLE lake.public.campaigns ALTER COLUMN email SET TAG pii = 'email'")
	if got := cs.srv.ProtectedBy(ctx, "public", "campaigns"); !slices.Equal(got, []string{"pii_hash"}) {
		t.Errorf("a column masked through a tag: %q", got)
	}
}

// TestExternalTablesAndPolicies checks how external tables (Delta
// Sharing) and policies meet: external tables stay readable while other
// tables have policies, take none themselves, and their files, like the
// warehouse's and the copies of shared files in it, are not readable
// directly by users other than admins.
func TestExternalTablesAndPolicies(t *testing.T) {
	dir := t.TempDir()
	warehouse, proxy := dir+"/warehouse/", dir+"/proxy/"
	copies := warehouse + ".igloo/sharing/0193/"
	for _, d := range []string{dir + "/landing", proxy, copies} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Every location is allowed, so that only the checks of the warehouse
	// and of external tables' files stand in the way.
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) {
			o.AllowedLocations, o.Warehouse = []string{dir + "/"}, warehouse
			o.external, o.externalFiles = fakeExternal{"public.viewing": true}, proxy
		})
	viewing, copied := proxy+"f1.parquet", copies+"c1.parquet"
	for _, sql := range []string{
		"COPY (SELECT 'abc' AS network, 5 AS minutes) TO '" + viewing + "' (FORMAT parquet)",
		"COPY (SELECT 'ann@acme.com' AS email) TO '" + copied + "' (FORMAT parquet)",
		"CREATE SCHEMA memory.public",
		"CREATE VIEW memory.public.viewing AS SELECT * FROM read_parquet(['" + viewing + "'])",
		"COPY (SELECT 1 AS x) TO '" + dir + "/landing/in.parquet' (FORMAT parquet)",
	} {
		mustExec(t, cs.admin, sql)
	}
	rita := cs.user(t, "rita", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	expectRows(t, rita, "SELECT network, minutes FROM memory.public.viewing", "abc|5")

	for _, sql := range policyFixture {
		mustExec(t, cs.admin, sql)
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := cs.user(t, "r_"+name, auth.RoleReader, mode)
			// An external table reads as before, alone or with a protected
			// table, whose policies still apply.
			expectRows(t, conn, "SELECT network, minutes FROM memory.public.viewing", "abc|5")
			expectRows(t, conn, "SELECT count(*) FROM lake.crm.contacts c, memory.public.viewing v", "0")
			expectRows(t, conn, "SELECT v.network, c.email FROM lake.public.campaigns c JOIN memory.public.viewing v ON true ORDER BY 2",
				"abc|ann@acme.com", "abc|gus@globex.com")
		})
	}
	// Its files, like the warehouse's and the copies of shared files there,
	// are read through tables only.
	for _, sql := range []string{
		"SELECT * FROM read_parquet('" + viewing + "')",
		"SELECT * FROM '" + viewing + "'",
		"SELECT * FROM read_parquet('" + proxy + "*.parquet')",
		"SELECT * FROM glob('" + dir + "/*')",
		"SELECT * FROM read_parquet('" + copied + "')",
		"SELECT * FROM '" + copies + "*.parquet'",
		"SELECT * FROM read_parquet('" + warehouse + ".igloo/sharing/*/*.parquet')",
		"SELECT * FROM parquet_metadata('" + copied + "')",
	} {
		expectRefused(t, rita, sql, "42501", "ann@")
	}
	expectRefused(t, walt, "COPY lake.public.campaigns FROM '"+copied+"'", "42501", "ann@")
	expectRefused(t, walt, "COPY lake.public.campaigns FROM '"+viewing+"'", "42501")
	expectRefused(t, walt, "CREATE TEMP VIEW peek AS SELECT * FROM read_parquet('"+viewing+"')", "42501")
	// Files elsewhere in the allowed locations stay readable, and admins
	// read anything.
	expectRows(t, rita, "SELECT x FROM read_parquet('"+dir+"/landing/in.parquet')", "1")
	expectRows(t, cs.admin, "SELECT network FROM read_parquet('"+viewing+"')", "abc")
	if err := cs.srv.checkLoadLocation(context.Background(), auth.User{Name: "walt", Role: auth.RoleWriter}, proxy); err == nil {
		t.Error("COPY INTO from external tables' files is allowed")
	}
	if err := cs.srv.checkLoadLocation(context.Background(), auth.User{Name: "walt", Role: auth.RoleWriter}, copies); err == nil {
		t.Error("COPY INTO from the copies of shared files is allowed")
	}

	// Policies and tags do not attach to external tables, and say why.
	for _, sql := range []string{
		"ALTER TABLE public.viewing ALTER COLUMN network SET MASKING POLICY mask_email",
		"ALTER TABLE lake.public.viewing ALTER COLUMN network SET MASKING POLICY mask_email",
		"ALTER TABLE viewing ADD ROW ACCESS POLICY client_rows ON (network)",
		"CREATE TAG region",
		"ALTER TABLE public.viewing ALTER COLUMN network SET TAG region = 'us'",
	} {
		_, err := cs.admin.Exec(context.Background(), sql)
		if sql == "CREATE TAG region" {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if pgCode(err) != "42809" || !strings.Contains(err.Error(), "external table") || !strings.Contains(err.Error(), "CREATE TABLE") {
			t.Errorf("%s: %v", sql, err)
		}
	}
	if got := cs.srv.ProtectedBy(context.Background(), "public", "viewing"); got != nil {
		t.Errorf("the external table has policies: %q", got)
	}
}

// TestRecipientTokensAreNotCredentials signs in with a Delta Sharing
// recipient's bearer token in place of a password or an access token, as
// SQL clients, Flight SQL and the web console send them: it is neither.
func TestRecipientTokensAreNotCredentials(t *testing.T) {
	cs := startCatalogServer(t)
	ctx := context.Background()
	mustExec(t, cs.admin, "CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader")
	// The form of sharing's tokens and activation codes (sharing.newToken).
	recipient := "dst_" + strings.Repeat("0a", 12) + "_" + strings.Repeat("A", 43)
	activation := "dsa_" + strings.Repeat("0a", 12) + "_" + strings.Repeat("A", 43)
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	for _, secret := range []string{recipient, activation} {
		if auth.LooksLikeToken(secret) {
			t.Fatalf("%s looks like an access token", secret)
		}
		if _, err := cs.srv.Authenticate(ctx, "rita", secret, addr); !errors.Is(err, ErrLoginFailed) {
			t.Errorf("console sign-in with %s: %v", secret[:4], err)
		}
		if _, err := cs.srv.AuthenticateClient(ctx, "rita", secret, addr, "flight sql"); !errors.Is(err, ErrLoginFailed) {
			t.Errorf("Flight SQL sign-in with %s: %v", secret[:4], err)
		}
		if _, _, err := cs.srv.AuthenticateToken(ctx, "", secret, addr, "flight sql"); !errors.Is(err, ErrLoginFailed) {
			t.Errorf("Flight SQL bearer %s: %v", secret[:4], err)
		}
		if _, err := tryConnect(cs.url("rita", secret), pgx.QueryExecModeSimpleProtocol); pgCode(err) != "28P01" {
			t.Errorf("SQL login with %s: %v", secret[:4], err)
		}
	}
}
