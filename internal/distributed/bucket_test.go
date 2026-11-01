package distributed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// addBucketedTable writes the rows of source as table name bucketed by
// column into n buckets, as an Iceberg table partitioned by bucket[n] of
// it keeps them: a file of each bucket's rows, and one of the rows whose
// column is NULL. The bucket is Iceberg's, as the harness loads the iceberg
// extension where it can. Elsewhere it is the column's hash, as BIGINT for
// integers, modulo n, so that tables of counts that divide each other
// bucket alike, as Iceberg's transform does.
func (h *harness) addBucketedTable(name, source, column string, n int) {
	h.t.Helper()
	h.addBucketedUUIDs(name, source, column, n, "")
}

// addBucketedUUIDs is addBucketedTable for a column of UUIDs its files hash as
// uuidHash says.
func (h *harness) addBucketedUUIDs(name, source, column string, n int, uuidHash string) {
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
		if slices.Contains(h.extensions, "iceberg") {
			where = fmt.Sprintf("iceberg_bucket(%d, %s) = %d", n, key, b)
			if uuidHash == uuidSpec {
				where = fmt.Sprintf("iceberg_bucket(%d, unhex(replace(CAST(%s AS VARCHAR), '-', ''))) = %d", n, key, b)
			}
		}
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
	t.Buckets = []Bucketing{{Column: column, N: n, UUIDHash: uuidHash}}
	h.exec(fmt.Sprintf("CREATE OR REPLACE VIEW main.%s AS SELECT * FROM read_parquet([%s])", quoteIdent(name), strings.Join(quoted(paths(fs)), ", ")))
	h.files[t.key()] = fs
}

// bucketHarness is a harness whose orders and lineitem are bucketed by
// order key, in 16 and 8 buckets, and in 12 (orders12); customer by its
// key, in 4.
func bucketHarness(t *testing.T) *harness {
	t.Helper()
	var extensions []string
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") != "" {
		extensions = append(extensions, "iceberg")
	}
	h := newHarnessWith(t, 0.01, 2, 3, "", extensions...)
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

// TestBucketedBesideOthers checks statements that join a table bucketed by
// the join column to something else: its other side is shuffled into the
// table's buckets, and the table stays where it is. A subquery over a
// bucketed table that keeps its key is read by bucket where it stands.
// Rows are those of one machine, NULL keys and outer joins among them, with
// buckets as many as, and more than, the exchanges' partitions. A key the
// statement computes, or counts that do not divide, read as before.
func TestBucketedBesideOthers(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the iceberg extension's bucket function is not here")
	}
	h := bucketHarness(t)
	h.addBucketedTable("priorities", "(SELECT DISTINCT o_orderpriority, length(o_orderpriority) AS n FROM orders)", "o_orderpriority", 4)
	h.addBucketedTable("orders_p", "(SELECT o_orderpriority, o_orderkey FROM orders)", "o_orderpriority", 8)
	for _, tc := range []struct {
		name, query string
		// read are the tables read by bucket, written are how many stages
		// write to their buckets.
		read    []string
		written int
		// exchanges is how many stages write rows to be joined.
		exchanges int
	}{
		{"a table beside one bucketed", `SELECT o.o_orderpriority, count(*) AS n, sum(l.l_quantity) AS q FROM lineitem8 l JOIN orders o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8"}, 1, 1},
		{"a relation beside one bucketed", `SELECT o.m, count(*) AS n FROM lineitem8 l JOIN (SELECT o_orderkey, max(o_orderpriority) AS m FROM orders GROUP BY 1) o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8"}, 1, 0},
		{"a relation read twice", `WITH pool AS MATERIALIZED (SELECT o_orderkey FROM orders16 WHERE o_orderpriority = '1-URGENT'),
			a AS (SELECT l_returnflag, count(*) AS n FROM lineitem8 l JOIN pool p ON p.o_orderkey = l.l_orderkey GROUP BY 1),
			b AS (SELECT l_linestatus, count(DISTINCT l.l_orderkey) AS m FROM lineitem8 l JOIN pool p ON p.o_orderkey = l.l_orderkey GROUP BY 1)
			SELECT * FROM a CROSS JOIN b ORDER BY 1, 3`,
			[]string{"lineitem8"}, 1, 0},
		{"fewer buckets than partitions, and NULL keys", `SELECT l.l_returnflag, count(*) AS n, count(o.o_orderkey) AS matched FROM lines4 l LEFT JOIN orders o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lines4"}, 1, 1},
		{"a full join", `SELECT count(*) AS n, count(l.l_orderkey) AS ls, count(o.o_orderkey) AS os FROM lines4 l FULL JOIN (SELECT * FROM orders WHERE o_orderkey % 3 = 0) o ON o.o_orderkey = l.l_orderkey`,
			[]string{"lines4"}, -1, -1},
		{"text", `SELECT p.n, count(*) AS c FROM orders_p o JOIN (SELECT DISTINCT o_orderpriority, length(o_orderpriority) AS n FROM orders) p ON p.o_orderpriority = o.o_orderpriority GROUP BY 1 ORDER BY 1`,
			[]string{"orders_p"}, 1, 0},
		{"a subquery that filters", `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN (SELECT o_orderkey, o_orderpriority FROM orders16 WHERE o_orderstatus = 'F') o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"one that renames the key", `SELECT o.p, count(*) AS n FROM lineitem8 l JOIN (SELECT o_orderkey AS k, o_orderpriority AS p FROM orders16) o ON l.l_orderkey = o.k GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"one of all columns", `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN (SELECT * FROM orders16 WHERE o_orderstatus = 'F') o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"one over another", `SELECT o.p, count(*) AS n FROM lineitem8 l JOIN (SELECT k, p FROM (SELECT o_orderkey AS k, o_orderpriority AS p FROM orders16) s WHERE k > 10) o ON l.l_orderkey = o.k GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"a common table expression read once", `WITH f AS (SELECT o_orderkey, o_orderpriority FROM orders16 WHERE o_orderstatus = 'F') SELECT f.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN f ON l.l_orderkey = f.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"beside a table shuffled", `SELECT c.o_orderpriority, count(*) AS n FROM (SELECT l_orderkey FROM lineitem8 WHERE l_quantity > 20) l JOIN orders c ON c.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8"}, 1, 1},
		{"the side of an outer join", `SELECT o.o_orderpriority, count(*) AS n, count(l.l_orderkey) AS matched FROM orders16 o LEFT JOIN (SELECT l_orderkey FROM lineitem8 WHERE l_quantity > 40) l ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"one that joins on the key", `SELECT t.o_orderpriority, sum(s.q) AS q FROM (SELECT l.l_orderkey AS k, l.l_quantity AS q FROM lineitem8 l JOIN orders16 o ON o.o_orderkey = l.l_orderkey WHERE o.o_orderstatus = 'F') s JOIN orders16 t ON t.o_orderkey = s.k GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"a semi join", `SELECT o_orderpriority, count(*) AS n FROM orders16 WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem8 WHERE l_quantity > 40) GROUP BY 1 ORDER BY 1`,
			[]string{"orders16"}, 1, 0},
		{"an anti join", `SELECT o_orderpriority, count(*) AS n FROM orders16 o WHERE NOT EXISTS (SELECT 1 FROM lineitem8 l WHERE l.l_orderkey = o.o_orderkey AND l_quantity > 40) GROUP BY 1 ORDER BY 1`,
			[]string{"orders16"}, 1, 0},
		{"the side of an outer join that groups", `SELECT o.o_orderpriority, count(*) AS n, sum(l.c) AS c FROM orders16 o LEFT JOIN (SELECT l_orderkey, count(*) AS c FROM lineitem8 WHERE l_quantity > 40 GROUP BY 1) l ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"a subquery that groups by the key", `SELECT o.o_orderpriority, sum(l.c) AS c, sum(l.q) AS q FROM (SELECT l_orderkey, count(*) AS c, sum(l_quantity) AS q FROM lineitem8 GROUP BY 1) l JOIN orders16 o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},
		{"by the key among others", `SELECT o.o_orderpriority, sum(l.c) AS c FROM (SELECT l_returnflag, l_orderkey, count(*) AS c FROM lineitem8 GROUP BY 1, 2 HAVING count(*) > 1) l JOIN orders o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8"}, 1, 1},
		{"a common table expression that groups", `WITH g AS (SELECT l_orderkey, count(*) AS c FROM lineitem8 GROUP BY 1) SELECT o.o_orderpriority, sum(g.c) AS c FROM orders16 o JOIN g ON g.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 0, 0},

		// A key another column of a join in the subquery computes.
		{"a join on another column", `SELECT t.o_orderpriority, sum(s.q) AS q FROM (SELECT l.l_partkey AS k, l.l_quantity AS q FROM lineitem8 l JOIN orders16 o ON o.o_orderkey = l.l_orderkey) s JOIN orders16 t ON t.o_orderkey = s.k GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8", "orders16"}, 1, 0},
		// A key computed from the table's: the subquery is a relation
		// written to the other table's buckets.
		{"a key the subquery computes", `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN (SELECT o_orderkey + 1 AS o_orderkey, o_orderpriority FROM orders16) o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"lineitem8"}, 1, 0},
		{"groups computed from the key", `SELECT o.o_orderpriority, sum(l.c) AS c FROM (SELECT l_orderkey + 1 AS l_orderkey, count(*) AS c FROM lineitem8 GROUP BY 1) l JOIN orders16 o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"orders16"}, 1, 0},
		// Not by the key: groups no bucket holds whole, as ROLLUP's are.
		{"groups without the key", `SELECT c.c_mktsegment, sum(o.c) AS c FROM (SELECT o_custkey, count(*) AS c FROM orders16 GROUP BY 1) o JOIN customer c ON c.c_custkey = o.o_custkey GROUP BY 1 ORDER BY 1`,
			nil, 0, -1},
		{"rollup", `SELECT o.o_orderpriority, sum(l.c) AS c FROM (SELECT l_orderkey, count(*) AS c FROM lineitem8 GROUP BY ROLLUP (l_orderkey)) l JOIN orders16 o ON o.o_orderkey = l.l_orderkey GROUP BY 1 ORDER BY 1`,
			[]string{"orders16"}, 1, 0},
		{"a limit", `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN (SELECT o_orderkey, o_orderpriority FROM orders16 ORDER BY o_orderkey LIMIT 500) o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			nil, 0, -1},
		// Keys that are not the column, or are more than it.
		{"a join on a computed key", `SELECT o.o_orderpriority, count(*) AS n FROM lineitem8 l JOIN orders o ON l.l_orderkey + 1 = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			nil, 0, -1},
		{"text joined on a function of it", `SELECT p.n, count(*) AS c FROM orders_p o JOIN priorities p ON upper(p.o_orderpriority) = upper(o.o_orderpriority) GROUP BY 1 ORDER BY 1`,
			nil, 0, -1},
		{"two keys", `SELECT count(*) AS n FROM lineitem8 l JOIN orders o ON l.l_orderkey = o.o_orderkey AND l.l_linenumber = o.o_shippriority`,
			nil, 0, -1},
		// Counts that do not divide, with a third input.
		{"counts that do not divide", `SELECT o.m, count(*) AS n FROM lineitem8 l JOIN orders12 t ON t.o_orderkey = l.l_orderkey JOIN (SELECT o_orderkey, max(o_orderpriority) AS m FROM orders GROUP BY 1) o ON l.l_orderkey = o.o_orderkey GROUP BY 1 ORDER BY 1`,
			nil, 0, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				if tc.read != nil {
					t.Fatal("did not run distributed")
				}
				return
			}
			p := h.planOf(tc.query)
			desc := p.describe()
			var read []string
			for _, name := range []string{"lineitem8", "lines4", "orders16", "orders12", "orders_p", "priorities"} {
				if strings.Contains(desc, "buckets of main."+name) {
					read = append(read, name)
				}
			}
			if strings.Join(read, ",") != strings.Join(tc.read, ",") {
				t.Errorf("read by bucket: %v, want %v:\n%s", read, tc.read, desc)
			}
			written := 0
			for _, s := range p.Stages {
				if s.Out != nil && s.Out.Buckets > 0 {
					written++
				}
			}
			if tc.written >= 0 && written != tc.written {
				t.Errorf("%d stages write to buckets, want %d:\n%s", written, tc.written, desc)
			}
			if n := p.exchanges("join"); tc.exchanges >= 0 && n != tc.exchanges {
				t.Errorf("%d stages exchange a table's rows, want %d:\n%s", n, tc.exchanges, desc)
			}
		})
	}
}

// TestBucketedReport checks a report of the shape that bucketing did not
// speed up: an audience, a filter of a bucketed table by a subquery's value,
// and a general population, of a table not bucketed, each counted by
// segment and overall against a bucketed table of segments. The plan over
// the bucketed tables leaves the segments in place, where the plan over
// the same rows unbucketed exchanges them for every count; and it returns
// the rows of one machine. IGLOO_TEST_BUCKET_REPORT_PEOPLE sets its size, and
// has it log how long each plan took.
func TestBucketedReport(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the iceberg extension's bucket function is not here")
	}
	h := newHarnessWith(t, 0.01, 2, 3, "", "iceberg")
	h.co.maxBroadcast = 1
	count := 30000
	if v := os.Getenv("IGLOO_TEST_BUCKET_REPORT_PEOPLE"); v != "" {
		count, _ = strconv.Atoi(v)
	}
	people := "md5(i::VARCHAR)[1:8] || '-' || md5(i::VARCHAR)[9:12]"
	segments := fmt.Sprintf("(SELECT %s AS person, 'grp_' || (i %% 7 + j) AS segment FROM range(%d) a(i), range(3) b(j))", people, count)
	members := fmt.Sprintf("(SELECT %s AS person, 'aud' || (i %% 5) AS audience, j AS version FROM range(%d) a(i), range(2) b(j))", people, count)
	profiles := fmt.Sprintf("(SELECT %s AS person, 'r' || (i %% 4) AS region FROM range(%d) a(i))", people, count+count/3)
	h.addBucketedTable("segments_b", segments, "person", 16)
	h.addBucketedTable("members_b", members, "person", 16)
	h.addTable("profiles", profiles, 2)
	h.addTable("segments_p", segments, 2)
	h.addTable("members_p", members, 2)
	h.addTable("versions", "(SELECT 'aud' || i AS audience, 1 AS version FROM range(5) t(i))", 1)
	report := func(segments, members string) string {
		return fmt.Sprintf(`WITH a AS (SELECT person FROM %[2]s WHERE audience = 'aud1' AND version = (SELECT max(version) FROM versions WHERE audience = 'aud1')),
			g AS (SELECT person FROM profiles WHERE region <> 'r0'),
			ac AS (SELECT s.segment, count(DISTINCT s.person) AS n FROM %[1]s s JOIN a ON a.person = s.person WHERE s.segment LIKE 'grp_%%' GROUP BY 1),
			gc AS (SELECT s.segment, count(DISTINCT s.person) AS n FROM %[1]s s JOIN g ON g.person = s.person WHERE s.segment LIKE 'grp_%%' GROUP BY 1),
			a_all AS (SELECT count(DISTINCT s.person) AS n FROM %[1]s s JOIN a ON a.person = s.person WHERE s.segment LIKE 'grp_%%'),
			g_all AS (SELECT count(DISTINCT s.person) AS n FROM %[1]s s JOIN g ON g.person = s.person WHERE s.segment LIKE 'grp_%%')
			SELECT ac.segment, ac.n AS audience, gc.n AS general, round(ac.n * 100.0 / a_all.n, 3) AS audience_share, round(gc.n * 100.0 / g_all.n, 3) AS general_share
			FROM ac JOIN gc ON gc.segment = ac.segment CROSS JOIN a_all CROSS JOIN g_all ORDER BY 1`, segments, members)
	}
	stages := func(p *Plan) (exchanged, total int) {
		for _, s := range p.Stages {
			if s.Out != nil {
				total++
				if s.Out.purpose == "join" {
					exchanged++
				}
			}
		}
		return exchanged, total
	}
	bucketed := report("segments_b", "members_b")
	if !h.check(bucketed) {
		t.Fatal("did not run distributed")
	}
	plain := report("segments_p", "members_p")
	if !h.check(plain) {
		t.Fatal("did not run distributed")
	}
	if os.Getenv("IGLOO_TEST_BUCKET_REPORT_PEOPLE") != "" {
		for range 3 {
			for _, q := range []string{bucketed, plain} {
				start := time.Now()
				if _, _, _, _, err := h.run(context.Background(), q, ModeOn); err != nil {
					t.Fatal(err)
				}
				t.Logf("%d people, %s: %s", count, q[len("WITH a AS (SELECT person FROM "):len("WITH a AS (SELECT person FROM ")+9], time.Since(start).Round(time.Millisecond))
			}
		}
	}
	want, _, err := h.query(context.Background(), plain)
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, _, err := h.run(context.Background(), bucketed, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("the report over bucketed tables and over the same rows unbucketed differ: %s", msg)
	}
	pb, pp := h.planOf(bucketed), h.planOf(plain)
	be, bt := stages(pb)
	pe, pt := stages(pp)
	if be != 0 || pe == 0 || bt >= pt {
		t.Errorf("exchanges of the tables' rows and all exchanges: %d of %d bucketed, %d of %d not\n%s\n%s", be, bt, pe, pt, pb.describe(), pp.describe())
	}
	if !strings.Contains(pb.describe(), "buckets of main.segments_b") {
		t.Errorf("the segments are not read by bucket:\n%s", pb.describe())
	}
	rows, err := h.co.Explain(context.Background(), bucketed, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	var notes []string
	for _, r := range rows {
		if r[0] == "buckets" {
			notes = append(notes, r[1])
		}
	}
	if !strings.Contains(strings.Join(notes, "\n"), "main.segments_b by bucket[16](person), read by bucket") || !strings.Contains(strings.Join(notes, "\n"), "written to those buckets") {
		t.Errorf("EXPLAIN says %q of buckets", notes)
	}
}

// TestIcebergBucketHash checks the function exchanges write rows to a
// table's buckets with against the hashes in Iceberg's spec, for the types of
// key that are written so: tables other engines wrote keep their rows in the
// buckets of these hashes, and a row written to another would join nothing.
func TestIcebergBucketHash(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the iceberg extension's bucket function is not here")
	}
	h := newHarnessWith(t, 0.01, 1, 1, "", "iceberg")
	for _, tc := range []struct {
		typ, value string
		hash       int32
	}{
		{"INTEGER", "34", 2017239379},
		{"BIGINT", "34", 2017239379},
		{"DECIMAL(9,2)", "14.20", -500754589},
		{"DECIMAL(18,2)", "14.20", -500754589},
		{"DECIMAL(38,2)", "14.20", -500754589},
		{"DATE", "'2017-11-16'", -653330422},
		{"TIMESTAMP", "'2017-11-16 22:31:08'", -2047944441},
		{"TIMESTAMP WITH TIME ZONE", "'2017-11-16 22:31:08+00'", -2047944441},
		{"VARCHAR", "'iceberg'", 1210000089},
	} {
		if !bucketHashes(tc.typ) && tc.typ != "BIGINT" {
			t.Errorf("%s is not written to buckets", tc.typ)
		}
		var got int32
		q := fmt.Sprintf("SELECT iceberg_bucket(2147483647, CAST(%s AS %s))", tc.value, tc.typ)
		if err := h.session.QueryRowContext(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if want := tc.hash & 0x7fffffff; got != want {
			t.Errorf("%s: %d, want %d", q, got, want)
		}
	}
	// A UUID, as the spec and as DuckDB hash it, written both ways.
	const id = "UUID 'f79c3e09-677c-4bbd-a479-3f349cb785e7'"
	for _, tc := range []struct {
		value string
		want  int32
	}{
		{"unhex(replace(CAST(" + id + " AS VARCHAR), '-', ''))", 1488055340},
		{id, 2132039410},
	} {
		var got int32
		q := "SELECT iceberg_bucket(2147483647, " + tc.value + ")"
		if err := h.session.QueryRowContext(context.Background(), q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got != tc.want {
			t.Errorf("%s: %d, want %d", q, got, tc.want)
		}
	}
}

// TestUUIDHashOf checks telling how a table's files hash their UUIDs from the
// buckets of their lowest values: by the spec's value for this one, 12 of 16,
// and DuckDB's, 2; and not at all from a file that fits both, or neither.
func TestUUIDHashOf(t *testing.T) {
	id := []byte{0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85, 0xe7}
	for _, tc := range []struct {
		name    string
		samples []uuidSample
		want    string
	}{
		{"by the spec", []uuidSample{{12, id}}, uuidSpec},
		{"by DuckDB", []uuidSample{{2, id}}, uuidDuckDB},
		{"neither", []uuidSample{{3, id}}, ""},
		{"no files", nil, ""},
		{"not a UUID", []uuidSample{{12, id[:8]}}, ""},
		{"files of both", []uuidSample{{12, id}, {2, id}}, ""},
	} {
		if got := uuidHashOf(16, tc.samples); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestBucketedUUIDs checks tables bucketed by UUIDs, written by either hash:
// what they join is written to their buckets by theirs, and they meet by bucket
// only a table of the same hash. A table whose hash cannot be told is exchanged.
func TestBucketedUUIDs(t *testing.T) {
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR is not set: the iceberg extension's bucket function is not here")
	}
	h := newHarnessWith(t, 0.01, 2, 3, "", "iceberg")
	h.co.maxBroadcast = 1
	source := func(mod int) string {
		return fmt.Sprintf("(SELECT CASE WHEN i %% 50 = 0 THEN NULL ELSE md5((i %% %d)::VARCHAR)::UUID END AS id, i AS v FROM range(20000) t(i))", mod)
	}
	h.addBucketedUUIDs("spec8", source(9000), "id", 8, uuidSpec)
	h.addBucketedUUIDs("spec16", source(7000), "id", 16, uuidSpec)
	h.addBucketedUUIDs("duck8", source(9000), "id", 8, uuidDuckDB)
	h.addBucketedUUIDs("told8", source(9000), "id", 8, uuidDuckDB)
	h.tables["told8"].Buckets[0].UUIDHash = ""
	h.addTable("plain", source(5000), 2)
	for _, tc := range []struct {
		name, query string
		read        []string
		written     int
	}{
		{"beside a table, by the spec", `SELECT count(*) AS n, sum(a.v) AS s FROM spec8 a JOIN plain p ON p.id = a.id`, []string{"spec8"}, 1},
		{"beside a table, by DuckDB", `SELECT count(*) AS n, sum(a.v) AS s FROM duck8 a JOIN plain p ON p.id = a.id`, []string{"duck8"}, 1},
		{"beside a relation", `SELECT count(*) AS n, sum(c) AS s FROM spec8 a LEFT JOIN (SELECT id, count(*) AS c FROM plain GROUP BY 1) p ON p.id = a.id`, []string{"spec8"}, 1},
		{"tables of one hash", `SELECT count(*) AS n, sum(a.v) AS s FROM spec8 a JOIN spec16 b ON b.id = a.id`, []string{"spec16", "spec8"}, 0},
		{"tables of two", `SELECT count(*) AS n, sum(a.v) AS s FROM spec8 a JOIN duck8 b ON b.id = a.id`, nil, 0},
		{"a table it cannot tell", `SELECT count(*) AS n, sum(a.v) AS s FROM told8 a JOIN plain p ON p.id = a.id`, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.t = t
			if !h.check(tc.query) {
				t.Fatal("did not run distributed")
			}
			p := h.planOf(tc.query)
			var read []string
			for _, name := range []string{"duck8", "spec16", "spec8", "told8"} {
				if strings.Contains(p.describe(), "buckets of main."+name) {
					read = append(read, name)
				}
			}
			if strings.Join(read, ",") != strings.Join(tc.read, ",") {
				t.Errorf("read by bucket: %v, want %v:\n%s", read, tc.read, p.describe())
			}
			written := 0
			for _, s := range p.Stages {
				if s.Out != nil && s.Out.Buckets > 0 {
					written++
				}
			}
			if written != tc.written {
				t.Errorf("%d stages write to buckets, want %d:\n%s", written, tc.written, p.describe())
			}
		})
	}
}
