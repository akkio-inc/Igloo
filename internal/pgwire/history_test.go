package pgwire

import (
	"slices"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/auth"
)

func TestRedactQuery(t *testing.T) {
	cases := map[string]string{
		"CREATE USER ana WITH PASSWORD 'it''s secret' ROLE writer":                                    "CREATE USER ana WITH PASSWORD '***' ROLE writer",
		"ALTER USER ana PASSWORD 'SCRAM-SHA-256$4096:x$y:z'":                                          "ALTER USER ana PASSWORD '***'",
		"CREATE USER ana PASSWORD unquoted-secret ROLE writer":                                        "CREATE USER ana PASSWORD ***",
		"alter user ana password $$dollar$$":                                                          "alter user ana password '***'",
		`ALTER USER ana PASSWORD "quoted" ROLE reader`:                                                "ALTER USER ana PASSWORD *** ROLE reader",
		"CREATE SECRET s (TYPE s3, KEY_ID 'AKIA', SECRET 'x')":                                        "CREATE SECRET s (TYPE s3, KEY_ID '***', SECRET '***')",
		"SET s3_secret_access_key = 'abc'":                                                            "SET s3_secret_access_key = '***'",
		"SELECT 'password' AS word":                                                                   "SELECT 'password' AS word",
		"SELECT * FROM t WHERE name = 'ana'":                                                          "SELECT * FROM t WHERE name = 'ana'",
		"CREATE NOTIFICATION INTEGRATION ops TYPE slack URL 'https://hooks.slack.com/services/T/B/x'": "CREATE NOTIFICATION INTEGRATION ops TYPE slack URL '***'",
		"ALTER NOTIFICATION INTEGRATION ops SET HEADERS = ('Authorization' = 'Bearer x')":             "ALTER NOTIFICATION INTEGRATION ops SET HEADERS = ('***' = '***')",
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
