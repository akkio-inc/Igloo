package pgwire

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// backgroundStore hides a MemoryStore's type, so that the history is
// written in the background and statements are analyzed in the
// background, as with PostgreSQL.
type backgroundStore struct{ *audit.MemoryStore }

// TestBackgroundAnalysis checks that statements analyzed in the background
// go to the history once each, with what they read and wrote next to what
// the session and the workload recorded of them.
func TestBackgroundAnalysis(t *testing.T) {
	store := backgroundStore{audit.NewMemoryStore(1000, 1000)}
	history := audit.New(store, audit.Options{Pool: "etl"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if history.Immediate() {
		t.Fatal("the history is written directly")
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		history.Run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})
	cs := startCatalogServer(t, func(o *Options) { o.Audit, o.Pool = history, "etl" })
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeExec)
	mustExec(t, walt, "SET query_tag = 'nightly'")
	statements := []string{
		"CREATE TABLE lake.public.src AS SELECT range AS i FROM range(10)",
		"INSERT INTO lake.public.src SELECT i + 10 FROM lake.public.src",
		"CREATE TABLE lake.public.dst AS SELECT * FROM lake.public.src",
	}
	for _, sql := range statements {
		mustExec(t, walt, sql)
	}
	var qs []audit.Query
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		all, err := store.Queries(context.Background(), audit.Viewer{User: "walt"}, audit.Page{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		qs = slices.DeleteFunc(all, func(q audit.Query) bool { return !slices.Contains(statements, q.Text) })
		if len(qs) >= len(statements) {
			break
		}
	}
	if len(qs) != len(statements) {
		t.Fatalf("%d of the %d statements in the history: %+v", len(qs), len(statements), qs)
	}
	for _, q := range qs {
		if q.QueryTag != "nightly" || q.Pool != "etl" || q.Outcome != audit.OutcomeOK || len(q.TablesWritten) != 1 {
			t.Errorf("%s: %+v", q.Text, q)
		}
		if q.Text == statements[2] && !slices.Equal(q.TablesRead, []string{"lake.public.src"}) {
			t.Errorf("%s read %q", q.Text, q.TablesRead)
		}
	}
	usage, err := history.TableUsage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range usage {
		if u.Table == "lake.public.src" && (u.Reads != 2 || u.Writes != 2) {
			t.Errorf("src's usage counted twice: %+v", u)
		}
	}
}

func TestRedactQuery(t *testing.T) {
	cases := map[string]string{
		"CREATE USER ana WITH PASSWORD 'it''s secret' ROLE writer":                                             "CREATE USER ana WITH PASSWORD '***' ROLE writer",
		"ALTER USER ana PASSWORD 'SCRAM-SHA-256$4096:x$y:z'":                                                   "ALTER USER ana PASSWORD '***'",
		"CREATE USER ana PASSWORD unquoted-secret ROLE writer":                                                 "CREATE USER ana PASSWORD ***",
		"alter user ana password $$dollar$$":                                                                   "alter user ana password '***'",
		`ALTER USER ana PASSWORD "quoted" ROLE reader`:                                                         "ALTER USER ana PASSWORD *** ROLE reader",
		"CREATE SECRET s (TYPE s3, KEY_ID 'AKIA', SECRET 'x')":                                                 "CREATE SECRET s (TYPE s3, KEY_ID '***', SECRET '***')",
		"SET s3_secret_access_key = 'abc'":                                                                     "SET s3_secret_access_key = '***'",
		"SELECT 'password' AS word":                                                                            "SELECT 'password' AS word",
		"SELECT * FROM t WHERE name = 'ana'":                                                                   "SELECT * FROM t WHERE name = 'ana'",
		"CREATE NOTIFICATION INTEGRATION ops TYPE slack URL 'https://hooks.slack.com/services/T/B/x'":          "CREATE NOTIFICATION INTEGRATION ops TYPE slack URL '***'",
		"ALTER NOTIFICATION INTEGRATION ops SET HEADERS = ('Authorization' = 'Bearer x')":                      "ALTER NOTIFICATION INTEGRATION ops SET HEADERS = ('***' = '***')",
		"ALTER CONNECTION sf SET (private_key '-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----')": "ALTER CONNECTION sf SET (private_key '***')",
	}
	for in, want := range cases {
		if got := redactQuery(in); got != want {
			t.Errorf("redactQuery(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	long := "SELECT '" + strings.Repeat("é", maxQueryText) + "'"
	if got := redactQuery(long); len(got) > maxQueryText || !strings.HasPrefix(long, got) {
		t.Errorf("long query kept %d bytes", len(got))
	}
}

func TestTagRowsAndKinds(t *testing.T) {
	for tag, want := range map[string]int64{"SELECT 3": 3, "INSERT 0 5": 5, "UPDATE 0": 0, "CREATE TABLE": -1, "": -1, "FETCH 2": 2} {
		if got := tagRows(tag); got != want {
			t.Errorf("tagRows(%q) = %d, want %d", tag, got, want)
		}
	}
	for query, want := range map[string]string{
		"select 1":                         "SELECT",
		"CREATE OR REPLACE TABLE t AS ...": "CREATE TABLE",
		"/* x */ drop user ana":            "DROP USER",
		"SHOW grants TO ana":               "SHOW GRANTS",
		"GRANT SELECT ON t TO r":           "GRANT",
		"CREATE NOTIFICATION INTEGRATION x TYPE slack URL 'u'":                  "CREATE NOTIFICATION INTEGRATION",
		"CREATE ALERT a SCHEDULE '1 hour' IF (EXISTS (SELECT 1)) THEN NOTIFY x": "CREATE ALERT",
		"": "",
	} {
		if got := statementKind(query); got != want {
			t.Errorf("statementKind(%q) = %q, want %q", query, got, want)
		}
	}
}

func TestRewriteHistoryRefs(t *testing.T) {
	cases := map[string]string{
		"SELECT * FROM igloo.query_history":                       `SELECT * FROM "memory"."igloo".query_history`,
		`SELECT * FROM "igloo"."login_history" h`:                 `SELECT * FROM "memory"."igloo"."login_history" h`,
		"SELECT * FROM IGLOO . QUERY_HISTORY":                     `SELECT * FROM "memory"."igloo" . QUERY_HISTORY`,
		"SELECT * FROM lake.igloo.query_history":                  "SELECT * FROM lake.igloo.query_history",
		"SELECT * FROM igloo.query_history.x":                     "SELECT * FROM igloo.query_history.x",
		"SELECT 'igloo.query_history', igloo.other_history":       "SELECT 'igloo.query_history', igloo.other_history",
		"SELECT * FROM igloo.events":                              "SELECT * FROM igloo.events",
		"SELECT * FROM igloo.query_history, igloo.login_history":  `SELECT * FROM "memory"."igloo".query_history, "memory"."igloo".login_history`,
		"SELECT table_owner FROM information_schema.TABLES t":     `SELECT table_owner FROM "memory"."igloo"."information_schema_tables" t`,
		`SELECT * FROM "information_schema"."schemata"`:           `SELECT * FROM "memory"."igloo"."information_schema_schemata"`,
		"SELECT * FROM system.information_schema.tables":          "SELECT * FROM system.information_schema.tables",
		"SELECT * FROM information_schema.columns":                "SELECT * FROM information_schema.columns",
		"SELECT * FROM igloo.tables JOIN igloo.lineage USING (x)": `SELECT * FROM "memory"."igloo".tables JOIN "memory"."igloo".lineage USING (x)`,
	}
	for in, want := range cases {
		if got := rewriteHistoryRefs(in, "memory"); got != want {
			t.Errorf("rewriteHistoryRefs(%q)\n got %q\nwant %q", in, got, want)
		}
	}
	if v, ok := applicationNameValue("SET application_name = 'my app'"); !ok || v != "my app" {
		t.Errorf("application_name: %q %v", v, ok)
	}
	if v, ok := applicationNameValue("SET SESSION application_name TO psql"); !ok || v != "psql" {
		t.Errorf("application_name TO: %q %v", v, ok)
	}
}

func TestParseACLCommand(t *testing.T) {
	ok := []struct {
		query string
		check func(*aclCommand) bool
	}{
		{"CREATE ROLE analyst", func(c *aclCommand) bool { return c.kind == aclCreateRole && c.role == "analyst" }},
		{`drop role if exists "analyst"`, func(c *aclCommand) bool { return c.kind == aclDropRole && c.ifExists }},
		{"GRANT analyst TO ana", func(c *aclCommand) bool { return c.kind == aclGrantRole && c.role == "analyst" && c.grantee == "ana" }},
		{"REVOKE analyst FROM ana", func(c *aclCommand) bool { return c.kind == aclRevokeRole }},
		{"GRANT SELECT, insert ON TABLE lake.\"Sales\".orders TO analyst", func(c *aclCommand) bool {
			return c.kind == aclGrant && slices.Equal(c.privileges, []auth.Privilege{auth.PrivilegeSelect, auth.PrivilegeInsert}) &&
				c.targetKind == targetTable && slices.Equal(c.target, []string{"lake", "Sales", "orders"})
		}},
		{"GRANT ALL PRIVILEGES ON SCHEMA sales TO analyst", func(c *aclCommand) bool {
			return c.privileges == nil && c.targetKind == targetSchema && c.target[0] == "sales"
		}},
		{"REVOKE ALL ON ALL TABLES IN SCHEMA lake.sales FROM analyst", func(c *aclCommand) bool {
			return c.kind == aclRevoke && c.targetKind == targetAllTables && slices.Equal(c.target, []string{"lake", "sales"})
		}},
		{"GRANT CREATE ON SCHEMA s TO r", func(c *aclCommand) bool { return c.privileges[0] == auth.PrivilegeCreate }},
		{"GRANT SELECT ON orders TO r", func(c *aclCommand) bool { return c.targetKind == targetTable }},
		{"SHOW ROLES", func(c *aclCommand) bool { return c.kind == aclShowRoles }},
		{"SHOW GRANTS", func(c *aclCommand) bool { return c.kind == aclShowGrants && c.grantee == "" && c.target == nil }},
		{"SHOW GRANTS TO ana", func(c *aclCommand) bool { return c.grantee == "ana" }},
		{"SHOW GRANTS ON SCHEMA s", func(c *aclCommand) bool { return c.targetKind == targetSchema }},
		{"SHOW GRANTS ON lake.s.t", func(c *aclCommand) bool { return c.targetKind == targetTable && len(c.target) == 3 }},
	}
	for _, c := range ok {
		cmds, matched, err := parseUserCommands(c.query)
		if !matched || err != nil || len(cmds) != 1 || cmds[0].acl == nil || !c.check(cmds[0].acl) {
			t.Errorf("%q: matched=%v err=%v cmds=%+v", c.query, matched, err, cmds)
		}
	}

	malformed := []string{
		"CREATE ROLE analyst WITH LOGIN",
		"CREATE ROLE \"Analyst\"",
		"GRANT SELECT ON TABLE t",
		"GRANT SELECT ON TABLE t TO a, b",
		"GRANT USAGE ON SCHEMA s TO r",
		"GRANT CREATE ON TABLE t TO r",
		"GRANT SELECT, ON t TO r",
		"GRANT ALL SELECT ON t TO r",
		"GRANT SELECT ON a.b.c.d TO r",
		"GRANT SELECT ON SCHEMA a.b.c TO r",
		"GRANT a TO b TO c",
		"SHOW ROLES x",
		"SHOW GRANTS ON ALL TABLES IN SCHEMA s",
		"GRANT SELECT ON t TO r; SELECT 1",
	}
	for _, q := range malformed {
		if _, matched, err := parseUserCommands(q); matched && err == nil {
			t.Errorf("%q should be rejected", q)
		}
	}
	for _, q := range []string{"SELECT grant FROM t", "CREATE TABLE roles (i INT)", "SHOW TABLES"} {
		if _, matched, _ := parseUserCommands(q); matched {
			t.Errorf("%q is not a role or grant statement", q)
		}
	}
}
