package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/distributed"
)

// TestWorkersStatementTakesCatalogShared plans and runs a dashboard's statement on
// workers while another holder keeps the catalog lock shared, as other sessions'
// binds do all the time: it finishes, as it holds the lock only shared itself.
// Planning it listed each table's columns, and the session's tables and views,
// holding the lock exclusively, so it waited for every bind before it and held up
// every bind after it: under dashboards and ELT, statements on workers waited 34 s
// at the median for the lock to run for 0.3 s.
func TestWorkersStatementTakesCatalogShared(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := ds.testServer.admin(t, mode)
			ctx := context.Background()
			mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
			mustExec(t, conn, "SET distributed_execution = on")
			query := "SELECT o.amount, count(*) AS n, sum(o.id) AS s FROM orders o GROUP BY o.amount ORDER BY o.amount"
			var want int
			// The first run builds what the server keeps of the catalog's names
			// and of DuckDB's columns, which listing does, holding the lock alone.
			if err := conn.QueryRow(ctx, "SELECT count(*) FROM ("+query+")").Scan(&want); err != nil {
				t.Fatal(err)
			}
			release := ds.srv.engine.CatalogShare()
			done := make(chan error, 1)
			go func() {
				var n int
				done <- conn.QueryRow(ctx, "SELECT count(*) FROM ("+query+")").Scan(&n)
			}()
			select {
			case err := <-done:
				release()
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(20 * time.Second):
				release()
				<-done
				t.Fatal("the statement waited for the catalog lock while it was held shared: something took it exclusively")
			}
			if w := lastWorkers(t, ds.history, "SELECT count(*) FROM ("+query+")"); w != 2 {
				t.Errorf("it ran on %d workers, want 2", w)
			}
		})
	}
}

// TestQueryBesidePoliciesTakesCatalogShared prepares and runs queries of a table no
// policy concerns, an admin's and a reader's, on a replica with a masking policy on
// another, while another holder keeps the catalog lock shared: they finish. Preparing
// one took the lock exclusively to apply the policies, and running a reader's again
// took it to recheck the reader's grants, with nothing changed: both queued every
// query behind the others' binds.
func TestQueryBesidePoliciesTakesCatalogShared(t *testing.T) {
	t.Parallel()
	cs, _ := startCatalogFixture(t)
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ada := cs.user(t, "ada_"+name, auth.RoleAdmin, mode)
			rita := cs.user(t, "rita_"+name, auth.RoleReader, mode)
			ctx := context.Background()
			query := "SELECT count(*) FROM lake.public.items_" + name
			mustExec(t, cs.admin, "CREATE TABLE lake.public.items_"+name+" AS SELECT range AS id FROM range(10)")
			for _, conn := range []*pgx.Conn{ada, rita} {
				var n int
				// The first prepare lists what policies may reach through, holding
				// the lock alone.
				if err := conn.QueryRow(ctx, query).Scan(&n); err != nil || n != 10 {
					t.Fatalf("%d rows, %v", n, err)
				}
			}
			release := cs.srv.engine.CatalogShare()
			done := make(chan error, 2)
			for _, conn := range []*pgx.Conn{ada, rita} {
				go func() {
					var n int
					err := conn.QueryRow(ctx, query+" WHERE id > 2").Scan(&n)
					if err == nil && n != 7 {
						err = fmt.Errorf("%d rows", n)
					}
					if err == nil {
						// Run again, as a dashboard's statements are.
						err = conn.QueryRow(ctx, query+" WHERE id > 2").Scan(&n)
					}
					done <- err
				}()
			}
			timeout := time.After(20 * time.Second)
			for range 2 {
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-timeout:
					release()
					<-done
					t.Fatal("a query waited for the catalog lock while it was held shared: something took it exclusively")
				}
			}
			release()
			// One that reads the protected table still has the policy applied.
			var code string
			if err := rita.QueryRow(ctx, "SELECT code FROM lake.public.secrets LIMIT 1").Scan(&code); err != nil || code != "***" {
				t.Errorf("the protected column read %q, %v", code, err)
			}
		})
	}
}

// TestDescribeTakesCatalogShared describes a table while another holder keeps the
// catalog lock shared: it finishes, a bind as a query's, where it waited to hold the
// lock alone as a listing. One of a table that does not exist still says so.
func TestDescribeTakesCatalogShared(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, "CREATE TABLE lake.main.described (id INTEGER, name VARCHAR)")
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			conn := cs.user(t, "dee_"+name, auth.RoleAdmin, mode)
			ctx := context.Background()
			columns := func() ([]string, error) {
				rows, err := conn.Query(ctx, "DESCRIBE lake.main.described")
				if err != nil {
					return nil, err
				}
				defer rows.Close()
				var out []string
				for rows.Next() {
					vals, err := rows.Values()
					if err != nil {
						return nil, err
					}
					out = append(out, fmt.Sprint(vals[0], " ", vals[1]))
				}
				return out, rows.Err()
			}
			if _, err := columns(); err != nil {
				t.Fatal(err)
			}
			release := cs.srv.engine.CatalogShare()
			done := make(chan error, 1)
			var got []string
			go func() {
				var err error
				got, err = columns()
				done <- err
			}()
			select {
			case err := <-done:
				release()
				if err != nil || len(got) != 2 || got[0] != "id INTEGER" || got[1] != "name VARCHAR" {
					t.Fatalf("%v, %v", got, err)
				}
			case <-time.After(20 * time.Second):
				release()
				<-done
				t.Fatal("DESCRIBE waited for the catalog lock while it was held shared")
			}
			if _, err := conn.Exec(ctx, "DESCRIBE lake.main.not_described"); err == nil || !strings.Contains(err.Error(), "not_described") {
				t.Errorf("DESCRIBE of a missing table: %v", err)
			}
		})
	}
}

// TestBoundColumnsAreListedColumns checks that the columns DuckDB binds for a
// statement that selects all of a table's, which planning for workers reads, are
// the names and types duckdb_columns() lists, for types of every kind.
func TestBoundColumnsAreListedColumns(t *testing.T) {
	t.Parallel()
	cs := startCatalogServer(t)
	mustExec(t, cs.admin, `CREATE TABLE lake.main.kinds (i INTEGER, b BIGINT, d DECIMAL(12, 3), f DOUBLE, s VARCHAR, ok BOOLEAN,
		day DATE, ts TIMESTAMP, tz TIMESTAMPTZ, u UUID, raw BLOB, l INTEGER[], m MAP(VARCHAR, INTEGER),
		st STRUCT(a INTEGER, b VARCHAR), ll STRUCT(x DOUBLE)[])`)
	ctx := context.Background()
	conn, err := cs.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	defer cs.srv.engine.CatalogLock()()
	bound, err := describedColumns(ctx, conn, "SELECT * FROM lake.main.kinds")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := queryRows(ctx, conn, `SELECT column_name, data_type FROM duckdb_columns()
		WHERE database_name = 'lake' AND schema_name = 'main' AND table_name = 'kinds' ORDER BY column_index`)
	if err != nil {
		t.Fatal(err)
	}
	var listed []distributed.Column
	for _, r := range rows {
		name, _ := r[0].(string)
		typ, _ := r[1].(string)
		listed = append(listed, distributed.Column{Name: name, Type: typ})
	}
	if len(bound) != len(listed) || len(bound) != 15 {
		t.Fatalf("bound %v\nlisted %v", bound, listed)
	}
	for i := range bound {
		if bound[i] != listed[i] {
			t.Errorf("column %d: bound %v, listed %v", i, bound[i], listed[i])
		}
	}
}
