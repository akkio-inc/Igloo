package distributed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// addBucketedTable writes the rows of source as table name bucketed by
// column into n buckets, as an Iceberg table partitioned by bucket[n] of
// it keeps them: a file of each bucket's rows, and one of the rows whose
// column is NULL. The bucket is the column's hash, as BIGINT for integers,
// modulo n, so that tables of counts that divide each other bucket alike,
// as Iceberg's transform does.
func (h *harness) addBucketedTable(name, source, column string, n int) {
	h.t.Helper()
	h.addTable(name, source, 1)
	t := h.tables[strings.ToLower(name)]
	key := quoteIdent(column)
	if strings.Contains(columnType(t, column), "INT") {
		key = "CAST(" + key + " AS BIGINT)"
	}
	dir := filepath.Join(h.dir, "warehouse", name+"-buckets")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	var ids []string
	for _, c := range t.Columns {
		ids = append(ids, quoteIdent(c.Name)+": "+fieldIDs(c.ID, c.Nested))
	}
	var fs []File
	t.Bytes = 0
	for b := -1; b < n; b++ {
		where := fmt.Sprintf("hash(%s) %% %d = %d", key, n, b)
		if b < 0 {
			where = quoteIdent(column) + " IS NULL"
		}
		path := filepath.Join(dir, fmt.Sprintf("bucket-%d.parquet", b+1))
		h.exec(fmt.Sprintf("COPY (SELECT * FROM main.%s WHERE %s) TO %s (FORMAT parquet, FIELD_IDS {%s})",
			quoteIdent(name), where, quoteLiteral(path), strings.Join(ids, ", ")))
		info, err := os.Stat(path)
		if err != nil {
			h.t.Fatal(err)
		}
		var rows int64
		if err := h.session.QueryRowContext(context.Background(), "SELECT count(*) FROM read_parquet("+quoteLiteral(path)+")").Scan(&rows); err != nil {
			h.t.Fatal(err)
		}
		fs = append(fs, File{Path: path, Size: info.Size(), Rows: rows, Buckets: []int{b}})
		t.Bytes += info.Size()
	}
	t.Buckets = []Bucketing{{Column: column, N: n}}
	h.exec(fmt.Sprintf("CREATE OR REPLACE VIEW main.%s AS SELECT * FROM read_parquet([%s])", quoteIdent(name), strings.Join(quoted(paths(fs)), ", ")))
	h.files[t.key()] = fs
}

// bucketHarness is a harness whose orders and lineitem are bucketed by
// order key, in 16 and 8 buckets, and in 12 (orders12); customer by its
// key, in 4.
func bucketHarness(t *testing.T) *harness {
	t.Helper()
	h := newHarness(t, 0.01, 2, 3)
	h.addBucketedTable("orders16", "orders", "o_orderkey", 16)
	h.addBucketedTable("orders12", "orders", "o_orderkey", 12)
	h.addBucketedTable("lineitem8", "lineitem", "l_orderkey", 8)
	// Lines of NULL orders, which a LEFT join keeps.
	h.addBucketedTable("lines4", "(SELECT * REPLACE (CASE WHEN l_linenumber = 7 THEN NULL ELSE l_orderkey END AS l_orderkey) FROM lineitem)", "l_orderkey", 4)
	h.co.maxBroadcast = 1
	return h
}

// TestBucketedPlans checks statements over tables bucketed by the column a
// stage needs its rows partitioned by: joins of tables bucketed by their
// join key, in counts that divide each other, groups that include the
// bucket column, its distinct values and windows partitioned by it. Each
// reads the tables by bucket, with no exchange of their rows, and returns
// what one machine does. Counts that do not divide each other are
// exchanged, and EXPLAIN DISTRIBUTED says why either way.
func TestBucketedPlans(t *testing.T) {
	h := bucketHarness(t)
	for _, tc := range []struct {
		query string
		// exchanges is how many stages write the tables' rows to an
		// exchange to join them ("join"), and aggregates how many write
		// partial results to one; note is what EXPLAIN says of buckets.
		exchanges, aggregates int
		note                  string
	}{
		{`SELECT o.o_orderpriority, count(*) AS n, sum(l.l_quantity) AS q FROM lineitem8 l JOIN orders16 o ON l.l_orderkey = o.o_orderkey
			GROUP BY 1 ORDER BY 1`, 0, 1, "no exchange: main.lineitem8 by bucket[8](l_orderkey) and main.orders16 by bucket[16](o_orderkey)"},
		{`SELECT o.o_orderkey, o.o_totalprice, count(*) AS lines FROM orders16 o JOIN lineitem8 l ON l.l_orderkey = o.o_orderkey
			WHERE l.l_quantity > 10 GROUP BY 1, 2 HAVING count(*) > 4 ORDER BY 1`, 0, 0, "no exchange"},
		// NULL keys, in a bucket of their own, which join nothing.
		{`SELECT l.l_returnflag, count(*) AS n, count(o.o_orderkey) AS matched FROM lines4 l JOIN orders16 o ON o.o_orderkey = l.l_orderkey
			GROUP BY 1 ORDER BY 1`, 0, 1, "no exchange"},
		// 12 is no multiple of 8: both are exchanged.
		{`SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN orders12 o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			2, 1, "exchanged although main.lineitem8 by bucket[8](l_orderkey), main.orders12 by bucket[12](o_orderkey): main.orders12's 12 buckets are not a multiple of 8"},
		// Groups that include the bucket column: whole in each bucket.
		{`SELECT l_orderkey, count(*) AS n, sum(l_quantity) AS q FROM lineitem8 GROUP BY 1 HAVING count(*) > 5 ORDER BY 1`, 0, 0, "no exchange: main.lineitem8 by bucket[8](l_orderkey), read by bucket"},
		{`SELECT l_orderkey, l_linenumber % 2 AS odd, max(l_quantity) AS q FROM lines4 GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 10`, 0, 0, "no exchange"},
		// Its distinct values: counted in place, each bucket's own.
		{`SELECT count(DISTINCT l_orderkey) AS orders, count(*) AS n FROM lineitem8`, 0, 0, "no exchange"},
		{`SELECT l_returnflag, count(DISTINCT l_orderkey) AS orders FROM lines4 GROUP BY 1 ORDER BY 1`, 0, 1, "no exchange"},
		// Windows partitioned by it.
		{`SELECT l_orderkey, l_linenumber, l_quantity, row_number() OVER (PARTITION BY l_orderkey ORDER BY l_quantity DESC, l_linenumber) AS rn
			FROM lineitem8 QUALIFY rn = 1 ORDER BY 1`, 0, 0, "no exchange"},
	} {
		t.Run(short(tc.query), func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(tc.query)
			if n := p.exchanges("join"); n != tc.exchanges {
				t.Errorf("%d stages exchange the tables' rows, want %d:\n%s", n, tc.exchanges, p.describe())
			}
			if n := p.exchanges("aggregate"); n != tc.aggregates {
				t.Errorf("%d stages exchange partial results, want %d:\n%s", n, tc.aggregates, p.describe())
			}
			if tc.exchanges == 0 && !strings.Contains(p.describe(), "buckets of") {
				t.Errorf("no stage reads by bucket:\n%s", p.describe())
			}
			rows, err := h.co.Explain(context.Background(), tc.query, harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			var notes []string
			for _, r := range rows {
				if r[0] == "buckets" {
					notes = append(notes, r[1])
				}
			}
			if !strings.Contains(strings.Join(notes, "\n"), tc.note) {
				t.Errorf("EXPLAIN says %q of buckets, want %q", notes, tc.note)
			}
		})
	}

	// Small enough for every worker to read orders whole, it is read by
	// bucket all the same, beside lineitem's buckets: each task reads a
	// share of each.
	h.t = t
	h.co.maxBroadcast = 1 << 40
	defer func() { h.co.maxBroadcast = 1 }()
	q := `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN orders16 o ON l.l_orderkey = o.o_orderkey WHERE l.l_quantity > 20 GROUP BY 1 ORDER BY 1`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	if p := h.planOf(q); p.exchanges("join") != 0 || strings.Count(p.describe(), "buckets of") != 2 {
		t.Errorf("broadcastable:\n%s", p.describe())
	}

	// Text joined under a collation, equal where it buckets apart: read as
	// before.
	h.addBucketedTable("priorities", "(SELECT DISTINCT o_orderpriority, length(o_orderpriority) AS n FROM orders)", "o_orderpriority", 4)
	h.addBucketedTable("orders_p", "(SELECT CASE WHEN o_orderkey % 2 = 0 THEN lower(o_orderpriority) ELSE o_orderpriority END AS o_orderpriority, o_orderkey FROM orders)",
		"o_orderpriority", 8)
	h.exec("SET default_collation = 'nocase'")
	defer h.exec("RESET default_collation")
	q = `SELECT p.n, count(*) AS orders FROM orders_p o JOIN priorities p ON p.o_orderpriority = o.o_orderpriority GROUP BY 1 ORDER BY 1`
	h.check(q)
	if p, err := h.co.Plan(context.Background(), q, harnessEnv{h}, ModeOn); err == nil && strings.Contains(p.describe(), "buckets of") {
		t.Errorf("text under a collation read by bucket:\n%s", p.describe())
	}
}
