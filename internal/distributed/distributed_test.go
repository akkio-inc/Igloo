package distributed

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTPCH runs the 22 TPC-H queries distributed over three workers and
// compares every result with DuckDB's on one machine. Queries that read
// lineitem more than once, or whose largest table sits where its rows
// cannot be divided, run on one machine; the rest must run distributed.
func TestTPCH(t *testing.T) {
	h := newHarness(t, 0.05, 7, 3)
	distributed := 0
	for q := 1; q <= 22; q++ {
		b, err := os.ReadFile(filepath.Join("..", "..", "bench", "queries", "tpch", fmt.Sprintf("q%02d.sql", q)))
		if err != nil {
			t.Fatal(err)
		}
		t.Run(fmt.Sprintf("q%02d", q), func(t *testing.T) {
			h.t = t
			if h.check(string(b)) {
				distributed++
			}
		})
	}
	t.Logf("%d of 22 queries ran distributed", distributed)
	if distributed < 12 {
		t.Errorf("only %d of 22 TPC-H queries ran distributed", distributed)
	}
}

// TestShapes covers the statement shapes distributed execution handles,
// with results compared to one machine's.
func TestShapes(t *testing.T) {
	h := newHarness(t, 0.02, 5, 3)
	queries := []string{
		// Aggregates and their combinations.
		`SELECT count(*), count(l_comment), sum(l_quantity), min(l_shipdate), max(l_comment), avg(l_discount) FROM lineitem`,
		`SELECT l_returnflag, l_linestatus, sum(l_extendedprice * (1 - l_discount)) / sum(l_quantity) AS r, count(*) FILTER (WHERE l_tax > 0.04) AS taxed FROM lineitem GROUP BY ALL ORDER BY ALL`,
		`SELECT l_returnflag AS f, count(DISTINCT l_partkey) AS parts, count(*) AS n, sum(DISTINCT l_partkey) AS s FROM lineitem GROUP BY 1 ORDER BY f`,
		`SELECT count(DISTINCT l_suppkey) FROM lineitem WHERE l_quantity > 49`,
		`SELECT count(DISTINCT l_suppkey) FROM lineitem WHERE l_quantity > 1000`,
		`SELECT count(*), sum(l_quantity) FROM lineitem WHERE l_quantity > 1000`,
		`SELECT l_shipmode, count(*) FROM lineitem WHERE l_quantity > 1000 GROUP BY l_shipmode`,
		`SELECT l_shipmode, l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM lineitem GROUP BY ROLLUP (l_shipmode, l_returnflag) ORDER BY 1, 2`,
		`SELECT l_shipmode, l_returnflag, GROUPING(l_shipmode, l_returnflag) AS g, count(*) FROM lineitem GROUP BY CUBE (l_shipmode, l_returnflag) ORDER BY 1, 2, 3`,
		`SELECT l_linestatus, sum(l_quantity) AS total FROM lineitem GROUP BY l_linestatus HAVING total > 10 AND count(*) > 5 ORDER BY total DESC`,
		`SELECT extract(year FROM l_shipdate) AS y, sum(l_quantity) FROM lineitem GROUP BY y ORDER BY y`,
		`SELECT lineitem.l_returnflag, sum(l_quantity) FROM lineitem GROUP BY l_returnflag ORDER BY 1`,
		`SELECT l_returnflag, bool_and(l_quantity > 0), bool_or(l_quantity > 49), bit_or(l_linenumber), max(l_shipinstruct) FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT l_returnflag, rank() OVER (ORDER BY sum(l_quantity) DESC) AS r, sum(sum(l_quantity)) OVER () AS total FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT l_returnflag, count(*) AS n FROM lineitem GROUP BY 1 QUALIFY rank() OVER (ORDER BY n DESC) <= 2 ORDER BY 1`,
		`SELECT DISTINCT l_returnflag, l_linestatus FROM lineitem ORDER BY 1, 2`,
		`SELECT CASE WHEN l_quantity < 10 THEN 'small' ELSE 'large' END AS size, count(*) FROM lineitem GROUP BY 1 ORDER BY 1`,
		`SELECT l_returnflag, sum(l_quantity)::BIGINT AS q, count(*) * 2 AS twice FROM lineitem GROUP BY l_returnflag ORDER BY q`,
		`SELECT l_returnflag, any_value(l_returnflag) = l_returnflag AS same FROM lineitem GROUP BY 1 ORDER BY 1`,
		// Joins with the other tables read in full by every worker.
		`SELECT n_name, count(*) FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name ORDER BY 2 DESC, 1`,
		`SELECT p_brand, sum(l_quantity) FROM lineitem LEFT JOIN part ON l_partkey = p_partkey AND p_size > 40 GROUP BY 1 ORDER BY 1 NULLS FIRST`,
		`SELECT count(*) FROM lineitem SEMI JOIN part ON l_partkey = p_partkey AND p_size = 1`,
		`SELECT count(*) FROM lineitem ANTI JOIN part ON l_partkey = p_partkey AND p_size = 1`,
		`SELECT l_shipmode, count(*) FROM lineitem WHERE l_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE '%green%') GROUP BY 1 ORDER BY 1`,
		`SELECT count(*) FROM lineitem l WHERE EXISTS (SELECT 1 FROM supplier s WHERE s.s_suppkey = l.l_suppkey AND s.s_acctbal > 5000)`,
		`SELECT s.n, sum(s.q) FROM (SELECT n_name AS n, l_quantity AS q FROM lineitem, supplier, nation WHERE l_suppkey = s_suppkey AND s_nationkey = n_nationkey) AS s GROUP BY 1 ORDER BY 1`,
		// Rows: filters, top-N, DISTINCT.
		`SELECT l_orderkey, l_linenumber, l_comment FROM lineitem WHERE l_quantity = 50 AND l_discount = 0.1 ORDER BY 1, 2 LIMIT 40`,
		`SELECT l_orderkey, l_linenumber, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 7`,
		`SELECT * FROM lineitem ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 3 OFFSET 2`,
		`SELECT DISTINCT l_orderkey FROM lineitem WHERE l_comment LIKE '%furious%' ORDER BY l_orderkey`,
		`SELECT DISTINCT l_shipmode FROM lineitem`,
		// Time functions every machine must agree on.
		`SELECT count(*) FROM lineitem WHERE l_shipdate < current_date AND l_commitdate < now()`,
		// The coordinator part may hold anything.
		`SELECT f, n FROM (SELECT l_returnflag AS f, count(*) AS n FROM lineitem GROUP BY 1) AS x JOIN (VALUES ('A'), ('R')) v(f2) ON f = f2 ORDER BY 1`,
		`WITH t AS (SELECT l_returnflag AS f, count(*) AS n FROM lineitem GROUP BY 1) SELECT * FROM t WHERE n > 10 ORDER BY 1`,
		`SELECT f, c, count(*) FROM (SELECT l_orderkey, min(l_returnflag) AS f, count(*) AS c FROM lineitem GROUP BY l_orderkey) GROUP BY f, c ORDER BY f, c`,
		`SELECT 'lineitem' AS t, count(*) FROM lineitem UNION ALL SELECT 'orders', count(*) FROM orders ORDER BY 1`,
	}
	for _, q := range queries {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Errorf("did not run distributed")
			}
		})
	}
}

// TestSessionSettings checks that workers compute with the session's
// settings: a time zone that moves rows across days, and integer division.
func TestSessionSettings(t *testing.T) {
	h := newHarness(t, 0.01, 4, 2)
	h.addTable("events", `(SELECT CAST(TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_minutes(i * 37) AS TIMESTAMPTZ) AS ts,
		i % 7 AS k FROM range(5000) t(i))`, 4)
	for _, setting := range []string{"SET TimeZone = 'America/Los_Angeles'", "SET TimeZone = 'Asia/Kolkata'"} {
		h.exec(setting)
		for _, q := range []string{
			`SELECT date_trunc('day', ts) AS day, count(*) FROM events GROUP BY 1 ORDER BY 1`,
			`SELECT CAST(ts AS DATE) AS d, strftime(ts, '%H') AS hour, count(*) FROM events GROUP BY ALL ORDER BY ALL`,
		} {
			t.Run(setting+" "+short(q), func(t *testing.T) {
				h.t = t
				if !h.check(q) {
					t.Error("did not run distributed")
				}
			})
		}
	}
	h.t = t
	h.exec("SET integer_division = true")
	if !h.check(`SELECT k, sum(k / 2) AS half FROM events GROUP BY k ORDER BY k`) {
		t.Error("did not run distributed")
	}
	h.exec("RESET integer_division")
	h.exec("RESET TimeZone")
}

// TestSchemaEvolution checks that workers read files written under older
// schemas as the current schema says, and read files that name the
// current columns by name.
func TestSchemaEvolution(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	dir := filepath.Join(h.dir, "warehouse", "evolved")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, query, ids string) string {
		path := filepath.Join(dir, name+".parquet")
		h.exec(fmt.Sprintf("COPY (%s) TO %s (FORMAT parquet, FIELD_IDS {%s})", query, quoteLiteral(path), ids))
		return path
	}
	// Before name was renamed from old_name, k widened and added added.
	a := write("a", `SELECT CAST(i AS INTEGER) AS k, 'a' || i AS old_name FROM range(1000) t(i)`, "k: 1, old_name: 2")
	b := write("b", `SELECT i AS k, 'b' || i AS name, i / 2 AS added FROM range(1000, 3000) t(i)`, "k: 1, name: 2, added: 3")
	// When a column named added was another one, dropped since.
	c := write("c", `SELECT i AS k, 'c' || i AS name, -1.0 AS added FROM range(3000, 4000) t(i)`, "k: 1, name: 2, added: 4")
	// Written like b, with the current schema.
	d := write("d", `SELECT i AS k, 'd' || i AS name, i / 3 AS added FROM range(4000, 5000) t(i)`, "k: 1, name: 2, added: 3")

	tbl := &Table{Schema: "main", Name: "evolved", Columns: []Column{
		{Name: "k", Type: "BIGINT", ID: 1}, {Name: "name", Type: "VARCHAR", ID: 2}, {Name: "added", Type: "DOUBLE", ID: 3},
	}}
	var fs []File
	for _, p := range []string{a, b, c, d} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		fs = append(fs, File{Path: p, Size: info.Size()})
		tbl.Bytes += info.Size()
	}
	h.tables["evolved"], h.files[tbl.key()] = tbl, fs
	h.exec(fmt.Sprintf(`CREATE OR REPLACE VIEW main.evolved AS
		SELECT CAST(k AS BIGINT) AS k, old_name AS name, CAST(NULL AS DOUBLE) AS added FROM read_parquet(%s)
		UNION ALL SELECT k, name, added FROM read_parquet([%s, %s])
		UNION ALL SELECT k, name, CAST(NULL AS DOUBLE) FROM read_parquet(%s)`, quoteLiteral(a), quoteLiteral(b), quoteLiteral(d), quoteLiteral(c)))

	w := h.workers[0]
	conn, err := w.opts.Engine.Connect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		files []string
		want  bool
	}{
		{[]string{b, d}, true},
		{[]string{b}, true},
		{[]string{a, b}, false}, // renamed and added
		{[]string{c}, false},    // dropped and added again
		{[]string{b, c, d}, false},
		{paths(h.files["main.lineitem"][0:1]), false}, // other columns
	} {
		got, err := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: tbl.Columns, Files: tc.files})
		if err != nil || got != tc.want {
			t.Errorf("%v read by name: %v, %v; want %v", tc.files, got, err, tc.want)
		}
	}
	li := h.tables["lineitem"]
	if got, err := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: li.Columns, Files: paths(h.files[li.key()])}); err != nil || !got {
		t.Errorf("lineitem's files are not read by name: %v", err)
	}
	// DuckDB is told the row count the coordinator knows.
	if stmt, err := w.viewStatement(viewDef{Name: "__igloo_t0", Columns: tbl.Columns, Files: []string{b}, Rows: 2000}); err != nil ||
		!strings.Contains(stmt, "explicit_cardinality = 2000") {
		t.Errorf("a view of 2000 rows: %s, %v", stmt, err)
	}
	// One type across files: k is INTEGER in e and BIGINT in b.
	e := write("e", `SELECT CAST(i AS INTEGER) AS k, 'e' || i AS name, 0.5 AS added FROM range(10) t(i)`, "k: 1, name: 2, added: 3")
	if got, _ := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: tbl.Columns, Files: []string{e, b}}); got {
		t.Error("files with different types read by name")
	}

	for _, q := range []string{
		`SELECT count(*), count(added), sum(k), min(name), max(name), sum(added) FROM evolved`,
		`SELECT k, name, added FROM evolved WHERE k % 250 = 0 ORDER BY k LIMIT 100`,
		`SELECT left(name, 1) AS f, count(*), count(added) FROM evolved GROUP BY 1 ORDER BY 1`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t

	// A file without field IDs is read through the table's name mapping,
	// which workers do not apply: the statement runs on the coordinator,
	// and the workers stay in use.
	plain := filepath.Join(dir, "plain.parquet")
	h.exec(fmt.Sprintf("COPY (SELECT i AS k, 'p' || i AS name, 1.5 AS added FROM range(10) t(i)) TO %s (FORMAT parquet)", quoteLiteral(plain)))
	if _, err := w.readByName(t.Context(), conn, viewDef{Name: "__igloo_t0", Columns: tbl.Columns, Files: []string{b, plain}}); !IsUnsupported(err) {
		t.Errorf("a file without field IDs: %v", err)
	}
	info, err := os.Stat(plain)
	if err != nil {
		t.Fatal(err)
	}
	h.files[tbl.key()] = append(h.files[tbl.key()], File{Path: plain, Size: info.Size()})
	for range 2 {
		if _, _, _, _, err := h.run(t.Context(), `SELECT count(*), sum(k) FROM evolved`, ModeOn); !IsUnsupported(err) {
			t.Errorf("a table with a file without field IDs: %v", err)
		}
	}
	if !h.check(`SELECT l_returnflag, count(*) FROM lineitem GROUP BY 1 ORDER BY 1`) {
		t.Error("workers were left out after refusing a task")
	}
}

// TestSnapshotPins checks that the coordinator's own reads of a table the
// workers read are pinned to the snapshot the workers read, and that a
// view in the coordinator's part, which could read such a table at another
// snapshot, keeps the statement on one machine.
func TestSnapshotPins(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	ctx := t.Context()
	// The job reads nation in full; the scalar subquery, on the
	// coordinator, reads it again.
	const job = `(SELECT n_name, count(*) AS c FROM lineitem JOIN supplier ON l_suppkey = s_suppkey
		JOIN main.nation ON s_nationkey = n_nationkey GROUP BY 1) AS x`
	q := `SELECT x.n_name, x.c, (SELECT count(*) FROM nation) AS nations FROM ` + job + ` ORDER BY 1`
	// The harness's files have no snapshots, so nothing is pinned.
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	nation := h.tables["nation"]
	defer func() { nation.Versioned, nation.Snapshot = false, 0 }()
	nation.Versioned, nation.Snapshot = true, 4242
	plan, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
	if err != nil {
		t.Fatal(err)
	}
	ex.Close()
	if n := strings.Count(ex.SQL, "AT (VERSION => 4242)"); n != 1 || strings.Count(ex.SQL, "AT (") != 1 {
		t.Errorf("want the coordinator's read of nation, alone, pinned (pins %v):\n%q", plan.pins, ex.SQL)
	}
	// A table with snapshots but none yet cannot be pinned.
	nation.Snapshot = 0
	if plan, err = h.co.Plan(ctx, q, harnessEnv{h}, ModeOn); err != nil {
		t.Fatal(err)
	}
	if _, err := h.co.Execute(ctx, plan, h.settings(ctx)); !IsUnsupported(err) {
		t.Errorf("a table without a snapshot yet: %v", err)
	}
	nation.Versioned = false

	h.exec("CREATE VIEW nations AS SELECT * FROM nation")
	for _, q := range []string{
		`SELECT x.n_name, x.c, (SELECT count(*) FROM nations) AS nations FROM ` + job,
		// The coordinator's nation is the common table expression, unless
		// the name means the table there.
		`WITH nation AS (SELECT 1 AS n) SELECT x.n_name, x.c, (SELECT count(*) FROM nation) AS k FROM ` + job,
	} {
		if _, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn); !IsUnsupported(err) {
			t.Errorf("want it to run on one machine, got %v: %s", err, q)
		}
	}
}

// TestSharedFiles checks a table whose files lie under another table's
// directory, as a clone's do: workers read the files its metadata lists,
// wherever they are in the warehouse.
func TestSharedFiles(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	li := h.tables["lineitem"]
	clone := &Table{Schema: "main", Name: "lineitem_clone", Columns: li.Columns, Bytes: li.Bytes}
	h.tables["lineitem_clone"], h.files[clone.key()] = clone, h.files[li.key()]
	h.exec("CREATE OR REPLACE VIEW main.lineitem_clone AS SELECT * FROM lineitem")
	if !h.check(`SELECT l_returnflag, count(*), sum(l_quantity) FROM lineitem_clone GROUP BY 1 ORDER BY 1`) {
		t.Error("did not run distributed")
	}
}

// TestLocal covers statements that must run on one machine.
func TestLocal(t *testing.T) {
	h := newHarness(t, 0.01, 3, 2)
	for _, q := range []string{
		`SELECT median(l_quantity) FROM lineitem`,
		`SELECT l_orderkey, sum(l_quantity) OVER (PARTITION BY l_orderkey) FROM lineitem`,
		`SELECT count(*) FROM lineitem a JOIN lineitem b USING (l_orderkey, l_linenumber)`,
		`SELECT count(*) FROM part LEFT JOIN lineitem ON p_partkey = l_partkey`,
		`SELECT * FROM lineitem LIMIT 5`,
		`SELECT l_quantity FROM lineitem`,
		// Rows without DISTINCT or a top-N would only cross the coordinator's
		// disk on their way to the client.
		`SELECT l_orderkey, l_comment FROM lineitem WHERE l_quantity = 50`,
		`SELECT l_orderkey FROM lineitem WHERE l_comment LIKE '%furious%' ORDER BY l_orderkey`,
		`SELECT count(*) FROM lineitem WHERE l_quantity < (SELECT avg(l_quantity) FROM lineitem)`,
		`SELECT string_agg(l_comment, ',' ORDER BY l_comment) FROM lineitem`,
		`SELECT count(*) FROM lineitem, range(3)`,
		`SELECT count(DISTINCT l_partkey), count(DISTINCT l_suppkey) FROM lineitem`,
		`SELECT getvariable('x'), count(*) FROM lineitem WHERE l_quantity > getvariable('y')`,
		`SELECT count(*) FROM lineitem WHERE l_quantity > $1`,
		`SELECT (SELECT count(*) FROM lineitem) + (SELECT count(*) FROM orders) AS n`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if _, _, _, _, err := h.run(t.Context(), q, ModeOn); !IsUnsupported(err) {
				t.Errorf("want it to run on one machine, got %v", err)
			}
		})
	}
}
