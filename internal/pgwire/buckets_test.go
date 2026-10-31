package pgwire

import (
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/audit"
)

// TestBucketedTablesOnWorkers checks Iceberg tables partitioned by
// bucket[N] of a column, as ALTER TABLE ... SET PARTITIONED BY (bucket(N,
// column)) has DuckDB write them, read through the catalog as production
// reads them: a join of two on their bucketed columns, in 8 and 4 buckets,
// NULL keys among them, and groups of one by its bucketed column run on
// workers bucket by bucket, with no exchange, as EXPLAIN DISTRIBUTED says,
// and return what one machine does.
func TestBucketedTablesOnWorkers(t *testing.T) {
	t.Parallel()
	history := audit.NewMemoryStore(1000, 1000)
	it := newDistE2Test(t, 2, func(o *Options) {
		o.Audit = audit.New(history, audit.Options{}, slog.New(slog.DiscardHandler))
	})
	conn := it.admin
	it.exec(
		"CREATE TABLE lake.public.bk_orders (k BIGINT, region VARCHAR, total DOUBLE)",
		"ALTER TABLE lake.public.bk_orders SET PARTITIONED BY (bucket(8, k))",
		"INSERT INTO lake.public.bk_orders SELECT i, 'r' || (i % 5), i * 1.5 FROM range(20000) r(i)",
		"CREATE TABLE lake.public.bk_lines (k INTEGER, n INTEGER, q INTEGER)",
		"ALTER TABLE lake.public.bk_lines SET PARTITIONED BY (bucket(4, k))",
		"INSERT INTO lake.public.bk_lines SELECT i % 20000, i, i % 50 FROM range(100000) r(i)",
		"INSERT INTO lake.public.bk_lines SELECT NULL, i, 1 FROM range(100) r(i)",
	)
	for _, q := range []string{
		"SELECT o.region, count(*) AS n, sum(l.q) AS q FROM lake.public.bk_lines l JOIN lake.public.bk_orders o ON o.k = l.k GROUP BY 1 ORDER BY 1",
		"SELECT k, count(*) AS n, max(q) AS q FROM lake.public.bk_lines GROUP BY 1 HAVING count(*) > 4 ORDER BY 1",
	} {
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, q)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := texts(t, conn, q); !slices.Equal(got, local) || len(local) == 0 {
			t.Errorf("%s: on workers\n%q\non one machine\n%q", q, got, local)
		}
		if w := lastWorkers(t, history, q); w == 0 {
			t.Errorf("%s: ran on one machine", q)
		}
		rows := queryAll(t, conn, "EXPLAIN DISTRIBUTED "+q)
		if !strings.Contains(rows, "buckets of public.bk_lines") || !strings.Contains(rows, "no exchange") || strings.Contains(rows, "(join)") {
			t.Errorf("%s: EXPLAIN DISTRIBUTED\n%s", q, rows)
		}
	}

	// Bucketed by Snowflake's CLUSTER BY with Igloo's BUCKET(n, column).
	snow := it.r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, snow, "SET sql_dialect = 'snowflake'")
	mustExec(t, snow, "CREATE TABLE lake.public.bk_sf (k INT, v STRING) CLUSTER BY (BUCKET(16, k))")
	mustExec(t, snow, "INSERT INTO lake.public.bk_sf SELECT seq4() * 3, 'v' || (seq4() % 9) FROM TABLE(GENERATOR(ROWCOUNT => 6000))")
	q := "SELECT s.v, count(*) AS n, sum(o.total) AS t FROM lake.public.bk_sf s JOIN lake.public.bk_orders o ON o.k = s.k GROUP BY 1 ORDER BY 1"
	mustExec(t, conn, "SET distributed_execution = off")
	local := texts(t, conn, q)
	mustExec(t, conn, "SET distributed_execution = on")
	if got := texts(t, conn, q); !slices.Equal(got, local) || len(local) == 0 {
		t.Errorf("%s: on workers\n%q\non one machine\n%q", q, got, local)
	}
	if rows := queryAll(t, conn, "EXPLAIN DISTRIBUTED "+q); !strings.Contains(rows, "buckets of public.bk_sf") || strings.Contains(rows, "(join)") {
		t.Errorf("CLUSTER BY (BUCKET(16, k)): EXPLAIN DISTRIBUTED\n%s", rows)
	}

	// A table partitioned by bucket after files were written without:
	// those files hold every bucket, so it is exchanged to be joined.
	it.exec(
		"CREATE TABLE lake.public.bk_mixed AS SELECT i AS k, i % 7 AS v FROM range(5000) r(i)",
		"ALTER TABLE lake.public.bk_mixed SET PARTITIONED BY (bucket(8, k))",
		"INSERT INTO lake.public.bk_mixed SELECT i, i % 7 FROM range(5000, 9000) r(i)",
	)
	q = "SELECT m.v, count(*) AS n, sum(o.total) AS t FROM lake.public.bk_mixed m JOIN lake.public.bk_orders o ON o.k = m.k GROUP BY 1 ORDER BY 1"
	mustExec(t, conn, "SET distributed_execution = off")
	local = texts(t, conn, q)
	mustExec(t, conn, "SET distributed_execution = on")
	if got := texts(t, conn, q); !slices.Equal(got, local) {
		t.Errorf("%s: on workers\n%q\non one machine\n%q", q, got, local)
	}
	if rows := queryAll(t, conn, "EXPLAIN DISTRIBUTED "+q); strings.Contains(rows, "buckets of public.bk_mixed") {
		t.Errorf("a table with files of every bucket read by bucket:\n%s", rows)
	}
}
