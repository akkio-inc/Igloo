package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/iddomain"
)

// TestDistributedMemberTables: a statement over an ID domain's member
// table, which the policy rewrite reads without its column of ids, runs on
// workers like any other, with the same rows and no column of ids in them,
// Igloo's own text that names that column too; and so it does once the
// table has a row access policy of its own, through the policy.
func TestDistributedMemberTables(t *testing.T) {
	ctx := context.Background()
	ids := iddomain.NewMemoryStore()
	if err := ids.Update(ctx, func(c *iddomain.Catalog) error {
		c.Domains = append(c.Domains, iddomain.Domain{Name: "people", Type: "VARCHAR", Generation: 1, Created: time.Now()})
		c.Members = append(c.Members, iddomain.Member{Domain: "people", Schema: "public", Table: "visits", Column: "person", Added: time.Now()})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ds := startDistServer(t, 2, func(o *Options) { o.IDDomains, o.IDDomainsOn = ids, true })
	// Igloo's own text names the column of ids (see withIDColumns).
	withIDColumns(ds.srv)
	// The member table as Igloo writes one, with its column of ids.
	if _, err := ds.source.conn.ExecContext(ctx, "CREATE TABLE lake.public.visits AS SELECT 'p' || (i % 300) AS person, CAST(i % 300 AS BIGINT) AS "+
		iddomain.HiddenColumn("person")+", CAST(i % 7 AS INTEGER) AS grp FROM range(6000) r(i)", nil); err != nil {
		t.Fatal(err)
	}
	conn := ds.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	// A policy elsewhere, so that statements go through the policy
	// rewrite, which reads member tables without their columns of ids.
	mustExec(t, conn, "CREATE ROW ACCESS POLICY first_only AS (id INTEGER) RETURNS BOOLEAN -> id = 1")
	mustExec(t, conn, "ALTER TABLE lake.public.secrets ADD ROW ACCESS POLICY first_only ON (id)")

	// rowsOn runs query here and on workers, checks the rows agree, and
	// returns how many workers ran it.
	rowsOn := func(what, query string) int {
		t.Helper()
		mustExec(t, conn, "SET distributed_execution = off")
		local := rowsOf(t, conn, query)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := rowsOf(t, conn, query); got != local {
			t.Errorf("%s: on workers\n%s\nhere\n%s", what, got, local)
		}
		return lastWorkers(t, ds.history, query)
	}
	explained := func(query string) string {
		t.Helper()
		mustExec(t, conn, "SET distributed_execution = on")
		return strings.Join(texts(t, conn, "EXPLAIN DISTRIBUTED "+query), "\n")
	}

	counts := "SELECT grp, count(*) AS n, count(DISTINCT person) AS people FROM visits GROUP BY grp ORDER BY grp"
	if n := rowsOn("counts", counts); n != 2 {
		t.Errorf("counts over a member table ran on %d workers, want 2", n)
	}
	// Igloo's own text, which names the column of ids, as a count through
	// ids does.
	own := "SELECT grp, count(DISTINCT CAST(v." + personID + " AS UINTEGER)) AS people FROM visits AS v GROUP BY grp ORDER BY grp"
	if n := rowsOn("a count through ids", own); n != 2 {
		t.Errorf("a count through ids ran on %d workers, want 2", n)
	}
	if plan := explained(own); !strings.Contains(plan, "runs on|workers") {
		t.Errorf("EXPLAIN DISTRIBUTED of a count through ids:\n%s", plan)
	}
	// SELECT * on workers: the member's columns, not its ids.
	star := "SELECT * FROM visits WHERE grp = 3 ORDER BY person, grp LIMIT 20"
	rowsOn("SELECT *", star)
	rows, err := conn.Query(ctx, star)
	if err != nil {
		t.Fatal(err)
	}
	var cols []string
	for _, f := range rows.FieldDescriptions() {
		cols = append(cols, f.Name)
	}
	rows.Close()
	if strings.Join(cols, ",") != "person,grp" {
		t.Errorf("SELECT * on workers has columns %q", cols)
	}

	// A row access policy of the table's own: on workers too, which read
	// the table through it (see distributed's governed.go).
	mustExec(t, conn, "CREATE ROW ACCESS POLICY some_people AS (p VARCHAR) RETURNS BOOLEAN -> p < 'p2'")
	mustExec(t, conn, "ALTER TABLE lake.public.visits ADD ROW ACCESS POLICY some_people ON (person)")
	if n := rowsOn("counts under a policy", counts); n != 2 {
		t.Errorf("counts over a member table with a row access policy ran on %d workers, want 2", n)
	}
	if plan := explained(counts); !strings.Contains(plan, "runs on|workers") || !strings.Contains(plan, "< 'p2'") || strings.Contains(plan, personID) {
		t.Errorf("EXPLAIN DISTRIBUTED of counts under a policy:\n%s", plan)
	}
}
