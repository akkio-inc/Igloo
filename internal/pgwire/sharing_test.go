package pgwire

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestParseShareCommand(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want shareCommand
	}{
		{"CREATE SHARE clients", shareCommand{kind: shCreateShare, tag: "CREATE SHARE", name: "clients"}},
		{"create share if not exists Clients comment 'For ''them'''",
			shareCommand{kind: shCreateShare, tag: "CREATE SHARE", name: "clients", ifNotExists: true, comment: "For 'them'"}},
		{"DROP SHARE IF EXISTS clients", shareCommand{kind: shDropShare, tag: "DROP SHARE", name: "clients", ifExists: true}},
		{"ALTER SHARE clients ADD TABLE lake.public.campaigns AS reports.spend WITH HISTORY",
			shareCommand{kind: shAddTable, tag: "ALTER SHARE", name: "clients", table: []string{"lake", "public", "campaigns"},
				alias: []string{"reports", "spend"}, histo: true}},
		{`ALTER SHARE clients ADD "Campaigns"`,
			shareCommand{kind: shAddTable, tag: "ALTER SHARE", name: "clients", table: []string{"Campaigns"}}},
		{"ALTER SHARE clients REMOVE TABLE reports.spend",
			shareCommand{kind: shRemoveTable, tag: "ALTER SHARE", name: "clients", alias: []string{"reports", "spend"}}},
		{"SHOW SHARES", shareCommand{kind: shShowShares, tag: "SHOW"}},
		{"SHOW ALL IN SHARE clients", shareCommand{kind: shShowShareTables, tag: "SHOW", name: "clients"}},
		{"SHOW GRANTS ON SHARE clients", shareCommand{kind: shShowShareGrants, tag: "SHOW", name: "clients"}},
		{"CREATE RECIPIENT acme TOKEN LIFETIME '90 days' COMMENT 'Acme'",
			shareCommand{kind: shCreateRecipient, tag: "CREATE RECIPIENT", name: "acme", lifetime: 90 * 24 * time.Hour, comment: "Acme"}},
		{"ALTER RECIPIENT acme ROTATE TOKEN KEEP OLD FOR '1h'",
			shareCommand{kind: shRotateToken, tag: "ALTER RECIPIENT", name: "acme", keep: time.Hour}},
		{"ALTER RECIPIENT acme ROTATE TOKEN", shareCommand{kind: shRotateToken, tag: "ALTER RECIPIENT", name: "acme"}},
		{"DROP RECIPIENT acme", shareCommand{kind: shDropRecipient, tag: "DROP RECIPIENT", name: "acme"}},
		{"SHOW RECIPIENTS", shareCommand{kind: shShowRecipients, tag: "SHOW"}},
		{"GRANT SELECT ON SHARE clients TO RECIPIENT acme",
			shareCommand{kind: shGrantShare, tag: "GRANT", name: "clients", recipient: "acme"}},
		{"REVOKE SELECT ON SHARE clients FROM RECIPIENT acme",
			shareCommand{kind: shRevokeShare, tag: "REVOKE", name: "clients", recipient: "acme"}},
		{`CREATE OR REPLACE PROVIDER tvco FROM '{"shareCredentialsVersion": 1}'`,
			shareCommand{kind: shCreateProvider, tag: "CREATE PROVIDER", name: "tvco", replace: true, profile: `{"shareCredentialsVersion": 1}`}},
		{"CREATE PROVIDER IF NOT EXISTS tvco FROM 'x'",
			shareCommand{kind: shCreateProvider, tag: "CREATE PROVIDER", name: "tvco", ifNotExists: true, profile: "x"}},
		{"DROP PROVIDER tvco", shareCommand{kind: shDropProvider, tag: "DROP PROVIDER", name: "tvco"}},
		{"SHOW PROVIDERS", shareCommand{kind: shShowProviders, tag: "SHOW"}},
		{"SHOW SHARES IN PROVIDER tvco", shareCommand{kind: shShowProviderTables, tag: "SHOW", name: "tvco"}},
		{`CREATE EXTERNAL TABLE partners.viewing FROM SHARE tvco.tv."Data".viewership`,
			shareCommand{kind: shCreateExternal, tag: "CREATE EXTERNAL TABLE", name: "tvco", table: []string{"partners", "viewing"},
				remote: []string{"tvco", "tv", "Data", "viewership"}}},
		{"DROP EXTERNAL TABLE IF EXISTS viewing", shareCommand{kind: shDropExternal, tag: "DROP EXTERNAL TABLE", ifExists: true, table: []string{"viewing"}}},
		{"SHOW EXTERNAL TABLES", shareCommand{kind: shShowExternal, tag: "SHOW"}},
	} {
		toks, err := tokenize(c.sql)
		if err != nil {
			t.Fatal(err)
		}
		got, ok, err := parseShareCommand(toks)
		if !ok || err != nil {
			t.Errorf("%s: ok %v, err %v", c.sql, ok, err)
			continue
		}
		if !reflect.DeepEqual(*got, c.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", c.sql, *got, c.want)
		}
	}

	for _, sql := range []string{"SHOW GRANTS", "GRANT SELECT ON TABLE t TO x", "CREATE TABLE t (i INT)", "SHOW TABLES", "DROP TABLE t"} {
		toks, _ := tokenize(sql)
		if _, ok, _ := parseShareCommand(toks); ok {
			t.Errorf("%s parsed as a sharing statement", sql)
		}
	}
	for _, sql := range []string{
		"CREATE SHARE", `CREATE SHARE "Bad"`, "CREATE SHARE s extra", "ALTER SHARE s", "ALTER SHARE s ADD TABLE a.b.c.d",
		"ALTER SHARE s ADD TABLE t AS a.b.c", "CREATE RECIPIENT r TOKEN LIFETIME 'forever'", "CREATE RECIPIENT r TOKEN LIFETIME 90",
		"ALTER RECIPIENT r", "GRANT SELECT ON SHARE s TO r", "CREATE PROVIDER p FROM profile", "CREATE PROVIDER p",
		"CREATE EXTERNAL TABLE t FROM SHARE a.b.c", "CREATE EXTERNAL TABLE t FROM SHARE \"Bad\".b.c.d", "SHOW ALL IN SHARE",
	} {
		toks, _ := tokenize(sql)
		if _, ok, err := parseShareCommand(toks); !ok || err == nil {
			t.Errorf("%s: ok %v, err %v; want a syntax error", sql, ok, err)
		}
	}
}

func TestParseShareDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"90d": 90 * 24 * time.Hour, "36h": 36 * time.Hour, "30m": 30 * time.Minute, "1 day": 24 * time.Hour,
		"30 Days": 30 * 24 * time.Hour, "12 hours": 12 * time.Hour, "0": 0,
	} {
		if got, err := parseShareDuration(in); err != nil || got != want {
			t.Errorf("parseShareDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "forever", "-1d", "1 fortnight", "d"} {
		if _, err := parseShareDuration(in); err == nil {
			t.Errorf("parseShareDuration(%q) succeeded", in)
		}
	}
}

func TestShareCommandsNeedAdminAndSetup(t *testing.T) {
	ts := startServer(t)
	ctx := context.Background()
	v, _ := auth.NewVerifier("reader-password")
	if err := ts.store.Create(ctx, auth.User{Name: "rita", Role: auth.RoleReader, Verifier: v}); err != nil {
		t.Fatal(err)
	}
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			reader := connect(t, ts.url("rita", "reader-password"), mode)
			admin := ts.admin(t, mode)
			for _, sql := range []string{"CREATE SHARE s", "SHOW RECIPIENTS", "GRANT SELECT ON SHARE s TO RECIPIENT r",
				"CREATE PROVIDER p FROM 'x'", "SHOW EXTERNAL TABLES"} {
				_, err := reader.Exec(ctx, sql)
				if pgCode(err) != "42501" || !strings.Contains(err.Error(), "admin role") {
					t.Errorf("reader: %s: %v", sql, err)
				}
				// Without a metadata database and catalog there is no
				// Delta Sharing.
				_, err = admin.Exec(ctx, sql)
				if pgCode(err) != "0A000" || !strings.Contains(err.Error(), "IGLOO_METADATA_DATABASE_URL") {
					t.Errorf("admin: %s: %v", sql, err)
				}
			}
			// Mixed with other SQL, as user management.
			if _, err := admin.Exec(ctx, "SHOW SHARES; SELECT 1", pgx.QueryExecModeSimpleProtocol); err == nil {
				t.Error("a sharing statement mixed with other SQL ran")
			}
		})
	}
}

func TestProviderProfilesAreRedacted(t *testing.T) {
	q := `CREATE PROVIDER tvco FROM '{"shareCredentialsVersion": 1, "endpoint": "https://x.example.com", "bearerToken": "dst_secret"}'`
	got := redactQuery(q)
	if strings.Contains(got, "dst_secret") || !strings.Contains(got, "'***'") {
		t.Errorf("redactQuery = %q", got)
	}
}
