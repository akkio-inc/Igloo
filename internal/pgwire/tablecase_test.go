package pgwire

import (
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestTablesKeepTheirCatalogNames checks that an Iceberg table is listed by
// the name the catalog has for it, whatever case statements and views name
// it in, on the replica that made a view naming it in another case and on
// one that loaded that view at its start, and that every spelling reads it.
// Lakekeeper finds a table by its name in any case, and DuckDB-Iceberg names
// its entry for a table as the table was last looked up: a view that read
// PEOPLE as people had the listings of every replica that loaded it list
// people.
func TestTablesKeepTheirCatalogNames(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	vc.fake.IgnoreNameCase()
	a := vc.replica(t)
	admin := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE SCHEMA lake.crm",
		`CREATE TABLE lake.crm."PEOPLE" AS SELECT 1 AS id UNION ALL SELECT 2`,
		`CREATE TABLE lake.crm."Visits" AS SELECT 1 AS id`,
		"CREATE VIEW lake.crm.people_view AS SELECT id FROM lake.crm.people",
		"CREATE VIEW lake.crm.visits_view AS SELECT id FROM crm.VISITS",
	} {
		mustExec(t, admin, q)
	}

	listed := func(conn *pgx.Conn, when string) {
		t.Helper()
		for _, q := range []string{
			"SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'crm' ORDER BY 1",
			`SELECT table_name FROM information_schema.tables
				WHERE table_catalog = 'lake' AND table_schema = 'crm' AND table_type = 'BASE TABLE' ORDER BY 1`,
		} {
			if got := texts(t, conn, q); !slices.Equal(got, []string{"PEOPLE", "Visits"}) {
				t.Errorf("%s, %s: %q, want [PEOPLE Visits]", when, q, got)
			}
		}
		shown := texts(t, conn, "SELECT name FROM (SHOW TABLES FROM lake.crm) ORDER BY 1")
		if !slices.Contains(shown, "PEOPLE") || !slices.Contains(shown, "Visits") ||
			slices.Contains(shown, "people") || slices.Contains(shown, "VISITS") {
			t.Errorf("%s, SHOW TABLES: %q", when, shown)
		}
	}
	reads := func(conn *pgx.Conn, when string) {
		t.Helper()
		for q, want := range map[string]int64{
			"SELECT count(*) FROM lake.crm.people":      2,
			`SELECT count(*) FROM lake.crm."PEOPLE"`:    2,
			"SELECT count(*) FROM lake.crm.People":      2,
			"SELECT count(*) FROM crm.people_view":      2,
			"SELECT count(*) FROM lake.crm.visits":      1,
			`SELECT count(*) FROM lake.crm."Visits"`:    1,
			"SELECT count(*) FROM crm.visits_view":      1,
			"SELECT count(*) FROM lake.crm.VISITS v":    1,
			"SELECT count(*) FROM lake.crm.people_VIEW": 2,
		} {
			if got := count(t, conn, q); got != want {
				t.Errorf("%s, %s: %d, want %d", when, q, got, want)
			}
		}
		mustExec(t, conn, "SET search_path = 'lake.crm'")
		if got := count(t, conn, "SELECT count(*) FROM people p JOIN visits USING (id)"); got != 1 {
			t.Errorf("%s, unqualified: %d, want 1", when, got)
		}
		mustExec(t, conn, "RESET search_path")
	}

	listed(admin, "after creating the views")
	reads(admin, "the replica that made the views")
	listed(admin, "after reading the tables in other cases")

	// Another replica loads the views as it starts.
	b := vc.replica(t)
	other := b.admin(t, pgx.QueryExecModeSimpleProtocol)
	listed(other, "on a replica that loaded the views")
	reads(other, "a replica that loaded the views")
	listed(other, "on a replica that loaded the views, after reading the tables in other cases")

	// The catalog's own names never changed.
	got := vc.fake.Tables()
	slices.Sort(got)
	if !slices.Equal(got, []string{"crm.PEOPLE", "crm.Visits"}) {
		t.Errorf("the catalog's tables: %q", got)
	}
}
