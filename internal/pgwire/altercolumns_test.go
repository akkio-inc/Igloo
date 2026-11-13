package pgwire

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestCatalogViewsShowAlteredColumnsAtOnce checks that the catalog views list
// a column the moment the ALTER TABLE that added it commits, in the session
// that ran it, in one that listed the table before, and in a new one.
// SQLAlchemy's reflection, and so Alembic's drift check, reads them right
// after its DDL. They used to list the table as its last statement to bind
// it had found it: the columns of ten ADD COLUMNs in a row showed all but the
// tenth.
func TestCatalogViewsShowAlteredColumnsAtOnce(t *testing.T) {
	env := newCatalogEnv(t)
	ctx := context.Background()
	mode := pgx.QueryExecModeSimpleProtocol
	altering, listing := env.admin(t, mode), env.admin(t, mode)
	mustExec(t, altering, `CREATE SCHEMA lake.p`)
	mustExec(t, altering, `CREATE TABLE lake.p."T" ("ID" VARCHAR)`)

	views := map[string]string{
		"information_schema.columns": `SELECT column_name FROM information_schema.columns
			WHERE table_catalog = 'lake' AND table_schema = 'p' AND table_name = 'T' ORDER BY ordinal_position`,
		"duckdb_columns()": `SELECT column_name FROM duckdb_columns()
			WHERE database_name = 'lake' AND schema_name = 'p' AND table_name = 'T' ORDER BY column_index`,
		"pg_attribute": `SELECT a.attname FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'p' AND c.relname = 'T' AND a.attnum > 0 ORDER BY a.attnum`,
	}
	expect := func(step string, want []string, conns map[string]*pgx.Conn) {
		t.Helper()
		for who, conn := range conns {
			for view, q := range views {
				rows, err := conn.Query(ctx, q)
				if err != nil {
					t.Fatalf("%s: %s in %s: %v", step, view, who, err)
				}
				got, err := pgx.CollectRows(rows, pgx.RowTo[string])
				if err != nil {
					t.Fatalf("%s: %s in %s: %v", step, view, who, err)
				}
				if !slices.Equal(got, want) {
					t.Errorf("%s: %s in %s lists %v, want %v", step, view, who, got, want)
				}
			}
		}
	}
	sessions := func() map[string]*pgx.Conn {
		return map[string]*pgx.Conn{"the altering session": altering, "another session": listing,
			"a new session": env.admin(t, mode)}
	}

	want := []string{"ID"}
	expect("after CREATE TABLE", want, sessions())

	mustExec(t, altering, `ALTER TABLE lake.p."T" ADD COLUMN "NEW1" VARCHAR`)
	want = append(want, "NEW1")
	expect("after one ADD COLUMN", want, sessions())

	for i := 2; i <= 11; i++ {
		name := fmt.Sprintf("NEW%d", i)
		mustExec(t, altering, fmt.Sprintf(`ALTER TABLE lake.p."T" ADD COLUMN %q VARCHAR`, name))
		want = append(want, name)
	}
	expect("after ten ADD COLUMNs in a row", want, sessions())

	// In a transaction block, as Alembic runs a migration.
	mustExec(t, altering, `BEGIN`)
	mustExec(t, altering, `ALTER TABLE lake.p."T" ADD COLUMN "IN_BLOCK" VARCHAR`)
	mustExec(t, altering, `ALTER TABLE lake.p."T" DROP COLUMN "NEW1"`)
	mustExec(t, altering, `COMMIT`)
	want = append(slices.Delete(want, 1, 2), "IN_BLOCK")
	expect("after a block that added and dropped a column", want, sessions())

	mustExec(t, altering, `ALTER TABLE lake.p."T" RENAME COLUMN "NEW2" TO "RENAMED"`)
	want[1] = "RENAMED"
	expect("after RENAME COLUMN", want, sessions())

	// A block that read the table before another session changed it lists
	// the change too, as PostgreSQL's catalog does. (It once listed the
	// table as a placeholder: a bind in a transaction that already loaded
	// the table answers from what it loaded.)
	block := env.admin(t, mode)
	mustExec(t, block, `BEGIN`)
	mustExec(t, block, `SELECT * FROM lake.p."T"`)
	mustExec(t, altering, `ALTER TABLE lake.p."T" ADD COLUMN "LATER" VARCHAR`)
	want = append(want, "LATER")
	expect("in a block that read the table before another session changed it", want,
		map[string]*pgx.Conn{"the block": block})
	mustExec(t, block, `COMMIT`)
}
