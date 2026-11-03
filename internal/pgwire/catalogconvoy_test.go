package pgwire

import (
	"context"
	"testing"
	"time"

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
