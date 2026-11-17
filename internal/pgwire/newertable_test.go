package pgwire

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/engine"
)

// TestTableNewerThanTransaction is extensions/iceberg's test of the same
// name (see there) through Igloo, the way a pilot deployment met it. A
// transaction block saved a record, a MERGE and then an INSERT, while other
// sessions created tables and read them. To check the user's grants for the
// INSERT, Igloo listed the catalog's tables on the session's connection,
// inside the block, as it does whenever DDL has run since its last listing;
// the listing reached a table created since the block began, the iceberg
// extension threw an InternalException as the listing ran, and DuckDB
// invalidated its database: every session, new ones included, failed until
// the process restarted. The listing now shows that table at its current
// schema, and the block commits; a block that writes the new table fails
// alone, with SQLSTATE 40001, the server serves on, and the client's retry
// of the block succeeds.
func TestTableNewerThanTransaction(t *testing.T) {
	cases := []struct {
		name string
		// insert is the block's insert, into lake.p.new, the table the other
		// session created, or lake.p.meta, which is older than the block.
		insert string
		// code is the SQLSTATE the insert fails with, "" if it succeeds.
		code string
	}{
		{"insert into an older table", "INSERT INTO lake.p.meta VALUES (3)", ""},
		{"insert into the new table", "INSERT INTO lake.p.new VALUES (2)", "40001"},
	}
	for name, mode := range modes {
		for _, c := range cases {
			t.Run(name+"/"+c.name, func(t *testing.T) {
				vc := newViewCatalog(t)
				ctx := context.Background()
				eng, err := engine.Open(ctx, vc.cfg, slog.New(slog.DiscardHandler))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = eng.Close() })
				srv := startServerOn(t, eng, func(o *Options) {
					o.Grants, o.Catalog = auth.NewMemoryGrants(o.Users), "lake"
				})
				other := srv.admin(t, mode)
				for _, q := range []string{
					"CREATE SCHEMA lake.p",
					"CREATE TABLE lake.p.meta (id INTEGER)",
					"INSERT INTO lake.p.meta VALUES (1)",
					// A team's user: a reader, with the grants of its schema.
					"CREATE USER walt WITH PASSWORD 'walt-password' ROLE reader",
					"GRANT ALL ON SCHEMA lake.p TO walt",
				} {
					mustExec(t, other, q)
				}
				walt := connect(t, srv.url("walt", "walt-password"), mode)
				block := []string{
					"BEGIN",
					"MERGE INTO lake.p.meta USING (SELECT 2 AS id) src ON meta.id = src.id WHEN NOT MATCHED THEN INSERT VALUES (src.id)",
				}
				for _, q := range block {
					mustExec(t, walt, q)
				}
				// The other session creates a table and reads it: DuckDB then
				// holds the table's latest metadata, which a listing shows.
				// The creation is stamped in milliseconds: a later one.
				time.Sleep(5 * time.Millisecond)
				mustExec(t, other, "CREATE TABLE lake.p.new AS SELECT 1 AS id")
				mustExec(t, other, "SELECT count(*) FROM lake.p.new")

				_, err = walt.Exec(ctx, c.insert)
				if got := pgCode(err); got != c.code {
					t.Fatalf("%s: SQLSTATE %q, want %q: %v", c.insert, got, c.code, err)
				}
				if err != nil {
					if msg := err.Error(); !strings.Contains(msg, "Table p.new ") || !strings.Contains(msg, "restart your transaction") {
						t.Errorf("the error does not name p.new or ask for a restart: %v", err)
					}
					mustExec(t, walt, "ROLLBACK")
					for _, q := range append(block, c.insert) {
						mustExec(t, walt, q)
					}
				}
				mustExec(t, walt, "COMMIT")

				// The server serves on: the other session and a new one
				// (count fails the test on an error).
				count(t, other, "SELECT count(*) FROM lake.p.new")
				count(t, srv.admin(t, mode), "SELECT count(*) FROM lake.p.new")
				want := int64(2)
				if strings.Contains(c.insert, "p.meta") {
					want = 3
				}
				if got := count(t, other, "SELECT count(*) FROM lake.p.meta"); got != want {
					t.Errorf("p.meta has %d rows after the block, want %d", got, want)
				}
			})
		}
	}
}
