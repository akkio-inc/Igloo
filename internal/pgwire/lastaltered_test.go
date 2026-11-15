package pgwire

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
)

// TestLastAlteredFollowsChanges checks that information_schema.tables'
// LAST_ALTERED is, as Snowflake's, when the table last changed by DDL or
// DML, and that a replica shows what its own statements did at once: set
// for a table just made, and later after each DROP COLUMN, ADD COLUMN,
// INSERT and DELETE, for the query right after. igloo.tables agrees. An app
// that rebuilds what it derives from a table when LAST_ALTERED moves ran
// these in a row: LAST_ALTERED was NULL after each, as the replica's
// metadata, read before the table was made, was read again only half a
// minute later, and a DDL statement never moved it. With the query history
// on, as without, since the history's record of what a statement wrote
// also has the table read again.
func TestLastAlteredFollowsChanges(t *testing.T) {
	t.Run("without history", func(t *testing.T) { testLastAltered(t) })
	t.Run("with history", func(t *testing.T) {
		testLastAltered(t, func(o *Options) {
			o.Audit = audit.New(audit.NewMemoryStore(1000, 1000), audit.Options{}, slog.New(slog.DiscardHandler))
		})
	})
}

func testLastAltered(t *testing.T, configure ...func(*Options)) {
	vc := newViewCatalog(t)
	vc.fake.IgnoreNameCase() // as Lakekeeper finds tables
	r := vc.replica(t, configure...)
	ctx := context.Background()
	conn := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	lastAltered := func(step string) *time.Time {
		t.Helper()
		mustExec(t, conn, "SET sql_dialect = 'snowflake'")
		defer mustExec(t, conn, "SET sql_dialect = 'duckdb'")
		var at, igloo *time.Time
		err := conn.QueryRow(ctx, `SELECT LAST_ALTERED FROM INFORMATION_SCHEMA.TABLES
			WHERE UPPER(TABLE_SCHEMA) = 'SCRATCH' AND UPPER(TABLE_NAME) = 'T'`).Scan(&at)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		if err := conn.QueryRow(ctx, `SELECT LAST_ALTERED FROM IGLOO.TABLES
			WHERE UPPER(TABLE_SCHEMA) = 'SCRATCH' AND UPPER(TABLE_NAME) = 'T'`).Scan(&igloo); err != nil {
			t.Fatalf("%s: igloo.tables: %v", step, err)
		}
		if at == nil || igloo == nil || !at.Equal(*igloo) {
			t.Fatalf("%s: LAST_ALTERED is %v, igloo.tables' last_altered %v", step, at, igloo)
		}
		return at
	}
	mustExec(t, conn, "CREATE SCHEMA lake.scratch")
	// The replica has read the catalog's metadata, as one that has answered
	// catalog queries has, before the table is made.
	if got := texts(t, conn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'scratch'"); len(got) != 0 {
		t.Fatalf("scratch lists %q before the table is made", got)
	}
	mustExec(t, conn, "CREATE TABLE lake.scratch.T AS SELECT 1 AS A, 2 AS B")
	last := lastAltered("after CREATE TABLE ... AS")
	for _, s := range []string{
		"ALTER TABLE lake.scratch.T DROP COLUMN B",
		"ALTER TABLE lake.scratch.T ADD COLUMN C INTEGER",
		"INSERT INTO lake.scratch.T VALUES (3, 4)",
		"DELETE FROM lake.scratch.T WHERE A = 1",
	} {
		time.Sleep(5 * time.Millisecond) // the catalog keeps milliseconds
		mustExec(t, conn, s)
		at := lastAltered(s)
		if !at.After(*last) {
			t.Errorf("after %s, LAST_ALTERED is %v, not after %v", s, at, last)
		}
		last = at
	}
}
