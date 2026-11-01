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

// TestBucketedBesideOthersOnWorkers checks Iceberg tables DuckDB wrote
// bucketed, read through the catalog, joined to rows that are not: a table
// of each type of key workers can write to its buckets, NULL keys among
// them, a report of a filtered bucketed table and a table not bucketed
// against a bucketed one, and subqueries over bucketed tables that keep the
// key. Workers leave the bucketed tables in place, as EXPLAIN DISTRIBUTED
// says, and return what one machine does.
func TestBucketedBesideOthersOnWorkers(t *testing.T) {
	t.Parallel()
	it := newDistE2Test(t, 2, func(o *Options) { o.Distributed.SetMaxBroadcast(1) })
	conn := it.admin
	same := func(q string) string {
		t.Helper()
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, q)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := texts(t, conn, q); !slices.Equal(got, local) || len(local) == 0 {
			t.Errorf("%s: on workers\n%q\non one machine\n%q", q, got, local)
		}
		return queryAll(t, conn, "EXPLAIN DISTRIBUTED "+q)
	}
	for _, tc := range []struct{ name, typ, value string }{
		{"big", "BIGINT", "i * 3"},
		{"small", "INTEGER", "(i * 3)::INTEGER"},
		{"text", "VARCHAR", "md5(i::VARCHAR)"},
		{"day", "DATE", "DATE '2024-01-01' + (i % 900)::INTEGER"},
		{"ts", "TIMESTAMP", "TIMESTAMP '2024-01-01' + to_seconds(i)"},
		{"tz", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ '2024-01-01 00:00:00+00' + to_seconds(i)"},
		{"num", "DECIMAL(18,3)", "(i / 8.0)::DECIMAL(18,3)"},
	} {
		bucketed, plain := "lake.public.bkb_"+tc.name, "lake.public.bkp_"+tc.name
		it.exec(
			"CREATE TABLE "+bucketed+" (k "+tc.typ+", v INTEGER)",
			"ALTER TABLE "+bucketed+" SET PARTITIONED BY (bucket(8, k))",
			"INSERT INTO "+bucketed+" SELECT "+tc.value+", i % 7 FROM range(12000) r(i)",
			"INSERT INTO "+bucketed+" SELECT NULL, 1 FROM range(50) r(i)",
			"CREATE TABLE "+plain+" (k "+tc.typ+", w INTEGER)",
			"INSERT INTO "+plain+" SELECT "+tc.value+", i % 5 FROM range(0, 12000, 2) r(i)",
			"INSERT INTO "+plain+" SELECT NULL, 1 FROM range(50) r(i)",
		)
		rows := same("SELECT o.v, count(*) AS n, sum(p.w) AS s FROM " + bucketed + " o JOIN " + plain + " p ON p.k = o.k GROUP BY 1 ORDER BY 1")
		if !strings.Contains(rows, "buckets of public.bkb_"+tc.name) || !strings.Contains(rows, "(join) by bucket[8]") {
			t.Errorf("%s: EXPLAIN DISTRIBUTED\n%s", tc.typ, rows)
		}
		rows = same("SELECT o.v, count(*) AS n, count(p.k) AS matched FROM " + bucketed + " o LEFT JOIN " + plain + " p ON p.k = o.k GROUP BY 1 ORDER BY 1")
		if !strings.Contains(rows, "buckets of public.bkb_"+tc.name) {
			t.Errorf("%s, outer: EXPLAIN DISTRIBUTED\n%s", tc.typ, rows)
		}
	}

	// UUIDs, which DuckDB hashes otherwise than the spec, told from the files.
	it.exec(
		"CREATE TABLE lake.public.bkb_id (k UUID, v INTEGER)",
		"ALTER TABLE lake.public.bkb_id SET PARTITIONED BY (bucket(8, k))",
		"INSERT INTO lake.public.bkb_id SELECT md5(i::VARCHAR)::UUID, i % 7 FROM range(12000) r(i)",
		"CREATE TABLE lake.public.bkp_id (k UUID, w INTEGER)",
		"INSERT INTO lake.public.bkp_id SELECT md5(i::VARCHAR)::UUID, i % 5 FROM range(0, 12000, 2) r(i)",
	)
	if rows := same("SELECT o.v, count(*) AS n, sum(p.w) AS s FROM lake.public.bkb_id o JOIN lake.public.bkp_id p ON p.k = o.k GROUP BY 1 ORDER BY 1"); !strings.Contains(rows, "buckets of public.bkb_id") || !strings.Contains(rows, "(join) by bucket[8]") {
		t.Errorf("UUID: EXPLAIN DISTRIBUTED\n%s", rows)
	}

	it.exec(
		"CREATE TABLE lake.public.bkr_segments (person VARCHAR, segment VARCHAR)",
		"ALTER TABLE lake.public.bkr_segments SET PARTITIONED BY (bucket(16, person))",
		"INSERT INTO lake.public.bkr_segments SELECT md5(i::VARCHAR), 'grp_' || ((i + j) % 6) FROM range(8000) a(i), range(3) b(j)",
		"CREATE TABLE lake.public.bkr_members (person VARCHAR, audience VARCHAR, version INTEGER)",
		"ALTER TABLE lake.public.bkr_members SET PARTITIONED BY (bucket(32, person))",
		"INSERT INTO lake.public.bkr_members SELECT md5(i::VARCHAR), 'aud' || (i % 4), j FROM range(8000) a(i), range(2) b(j)",
		"CREATE TABLE lake.public.bkr_versions (audience VARCHAR, version INTEGER)",
		"INSERT INTO lake.public.bkr_versions SELECT 'aud' || i, 1 FROM range(4) t(i)",
		"CREATE TABLE lake.public.bkr_profiles (person VARCHAR, region VARCHAR)",
		"INSERT INTO lake.public.bkr_profiles SELECT md5(i::VARCHAR), 'r' || (i % 3) FROM range(12000) t(i)",
	)
	rows := same(`WITH a AS (SELECT person FROM lake.public.bkr_members WHERE audience = 'aud1' AND version = (SELECT max(version) FROM lake.public.bkr_versions WHERE audience = 'aud1')),
		g AS (SELECT person FROM lake.public.bkr_profiles WHERE region <> 'r0'),
		ac AS (SELECT s.segment, count(DISTINCT s.person) AS n FROM lake.public.bkr_segments s JOIN a ON a.person = s.person GROUP BY 1),
		gc AS (SELECT s.segment, count(DISTINCT s.person) AS n FROM lake.public.bkr_segments s JOIN g ON g.person = s.person GROUP BY 1),
		a_all AS (SELECT count(DISTINCT s.person) AS n FROM lake.public.bkr_segments s JOIN a ON a.person = s.person),
		g_all AS (SELECT count(DISTINCT s.person) AS n FROM lake.public.bkr_segments s JOIN g ON g.person = s.person)
		SELECT ac.segment, ac.n AS audience, gc.n AS general, round(ac.n * 100.0 / a_all.n, 3) AS audience_share, round(gc.n * 100.0 / g_all.n, 3) AS general_share
		FROM ac JOIN gc ON gc.segment = ac.segment CROSS JOIN a_all CROSS JOIN g_all ORDER BY 1`)
	if strings.Contains(rows, "split public.bkr_segments") || !strings.Contains(rows, "buckets of public.bkr_segments") || strings.Contains(rows, "(join)") {
		t.Errorf("the report: EXPLAIN DISTRIBUTED\n%s", rows)
	}
	for _, q := range []string{
		"SELECT s.segment, count(*) AS n FROM lake.public.bkr_segments s JOIN (SELECT person FROM lake.public.bkr_members WHERE audience = 'aud2') m ON m.person = s.person GROUP BY 1 ORDER BY 1",
		"SELECT s.segment, sum(m.c) AS c FROM lake.public.bkr_segments s JOIN (SELECT person, count(*) AS c FROM lake.public.bkr_members GROUP BY 1) m ON m.person = s.person GROUP BY 1 ORDER BY 1",
	} {
		rows := same(q)
		if !strings.Contains(rows, "buckets of public.bkr_segments") || !strings.Contains(rows, "buckets of public.bkr_members") || strings.Contains(rows, "exchange 0 (") && strings.Contains(rows, "(relation)") {
			t.Errorf("%s: EXPLAIN DISTRIBUTED\n%s", q, rows)
		}
	}
	// A key computed from the column, which its buckets do not hold apart.
	rows = same("SELECT s.segment, count(*) AS n FROM lake.public.bkr_segments s JOIN (SELECT upper(person) AS person FROM lake.public.bkr_members) m ON m.person = upper(s.person) GROUP BY 1 ORDER BY 1")
	if strings.Contains(rows, "buckets of public.") {
		t.Errorf("a computed key read by bucket: EXPLAIN DISTRIBUTED\n%s", rows)
	}
}
