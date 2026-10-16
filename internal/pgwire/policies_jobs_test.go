package pgwire

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// TestPoliciesInJobs reads protected tables through materialized views,
// tasks and COPY INTO, which run as their owners: the owner's policies
// apply, at every run.
func TestPoliciesInJobs(t *testing.T) {
	dir := t.TempDir()
	warehouse := dir + "/warehouse/"
	if err := os.MkdirAll(warehouse+"data", 0o755); err != nil {
		t.Fatal(err)
	}
	sched := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}},
		func(o *Options) {
			o.Tasks = sched
			o.AllowedLocations, o.Warehouse = []string{dir + "/"}, warehouse
		})
	for _, sql := range policyFixture {
		mustExec(t, cs.admin, sql)
	}
	walt := cs.user(t, "walt", auth.RoleWriter, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, cs.admin, "GRANT acme_team TO walt")

	// A materialized view holds what its owner sees, when it is created
	// and whenever it is refreshed.
	const mv = "SELECT id, email FROM lake.public.contact_mv ORDER BY id"
	mustExec(t, walt, "CREATE MATERIALIZED VIEW lake.public.contact_mv AS SELECT id, email, device_id FROM lake.crm.contacts")
	expectRows(t, cs.admin, mv, "1|***", "2|***")
	mustExec(t, cs.admin, "INSERT INTO lake.crm.contacts VALUES (5, 'acme', 'ann2@acme.com', 'dev-5', 50)")
	mustExec(t, walt, "REFRESH MATERIALIZED VIEW lake.public.contact_mv")
	expectRows(t, cs.admin, mv, "1|***", "2|***", "5|***")
	mustExec(t, cs.admin, "REFRESH MATERIALIZED VIEW lake.public.contact_mv")
	expectRows(t, cs.admin, mv, "1|***", "2|***", "5|***")

	// So does a task's table.
	mustExec(t, walt, "CREATE TABLE lake.public.copied (id INTEGER, email VARCHAR)")
	mustExec(t, walt, "CREATE TASK copy_contacts SCHEDULE '1 hour' AS INSERT INTO lake.public.copied SELECT id, email FROM lake.crm.contacts")
	mustExec(t, cs.admin, "EXECUTE TASK copy_contacts")
	expectRows(t, cs.admin, "SELECT id, email FROM lake.public.copied ORDER BY id", "1|***", "2|***", "5|***")

	// A plain reader, in no team, reading the stored results gets only what
	// the owner could see: masked emails, and none of globex's rows. The
	// secrets never reach her through the materialized view or the table.
	nina := cs.user(t, "nina", auth.RoleReader, pgx.QueryExecModeSimpleProtocol)
	expectRows(t, nina, mv, "1|***", "2|***", "5|***")
	expectRows(t, nina, "SELECT id, email FROM lake.public.copied ORDER BY id", "1|***", "2|***", "5|***")
	for _, sql := range []string{mv, "SELECT * FROM lake.public.copied"} {
		if s := leaked(nina, sql); s != "" {
			t.Errorf("reader sees %q through a stored result:\n  %s", s, sql)
		}
	}

	// COPY INTO and pipes cannot load the warehouse's files, nor a location
	// that holds them.
	mustExec(t, cs.admin, "COPY (SELECT 'ann@acme.com' AS email) TO '"+warehouse+"data/x.parquet' (FORMAT parquet)")
	mustExec(t, walt, "CREATE TABLE lake.public.loaded (email VARCHAR)")
	for _, sql := range []string{
		"COPY INTO lake.public.loaded FROM '" + warehouse + "'",
		"COPY INTO lake.public.loaded FROM '" + warehouse + "data/x.parquet'",
		"COPY INTO lake.public.loaded FROM '" + dir + "/'",
		"COPY INTO lake.public.loaded FROM '" + dir + "/' (PATTERN '.*[.]parquet')",
		"CREATE PIPE warehouse_pipe AS COPY INTO lake.public.loaded FROM '" + dir + "/'",
	} {
		expectRefused(t, walt, sql, "42501", secrets...)
	}
	expectRows(t, cs.admin, "SELECT count(*) FROM lake.public.loaded", "0")
}
