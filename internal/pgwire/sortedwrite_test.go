package pgwire

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/engine"
)

func TestRangeBoundsAndBatches(t *testing.T) {
	t.Parallel()
	keys := []string{"01", "01", "02", "03", "04", "05", "06", "07", "08", "09"}
	if got := rangeBounds(keys, 5); !slices.Equal(got, []string{"02", "04", "06", "08"}) {
		t.Errorf("bounds of 5 ranges: %q", got)
	}
	// The first key bounds nothing, and repeats bound once.
	if got := rangeBounds([]string{"01", "01", "01", "01", "02", "02", "02", "02"}, 4); !slices.Equal(got, []string{"02"}) {
		t.Errorf("bounds of repeated keys: %q", got)
	}
	if got := rangeOf("k", []string{"0A", "0B", "0C"}, 0); got !=
		"CASE WHEN k < unhex('0B') THEN CASE WHEN k < unhex('0A') THEN 0 ELSE 1 END ELSE CASE WHEN k < unhex('0C') THEN 2 ELSE 3 END END" {
		t.Errorf("range of 3 bounds: %s", got)
	}

	files := []rangeFile{
		{rng: 2, path: "r2", rows: 30}, {rng: 0, path: "r0a", rows: 10}, {rng: 0, path: "r0b", rows: 10},
		{rng: 1, path: "r1", rows: 50}, {rng: 3, path: "r3", rows: 200}, {rng: 4, path: "r4", rows: 5},
	}
	// 10 bytes a row, 600 at a time: ranges 0 and 1 (700 bytes would be
	// too many with 2), then 2, range 3 alone though too large, then 4.
	got := sortBatches(files, 10, 600)
	want := [][]string{{"r0a", "r0b"}, {"r1"}, {"r2"}, {"r3"}, {"r4"}}
	if !slices.EqualFunc(got, want, slices.Equal[[]string]) {
		t.Errorf("batches: %q, want %q", got, want)
	}
	if got := sortBatches(files, 1, 1000); len(got) != 1 || len(got[0]) != 6 {
		t.Errorf("one batch: %q", got)
	}
}

func TestParseSortedInsert(t *testing.T) {
	t.Parallel()
	for q, want := range map[string]parsedInsert{
		"INSERT INTO lake.s.t SELECT a FROM u ORDER BY a;":      {target: "lake.s.t", source: "SELECT a FROM u ORDER BY a"},
		"insert into t by position with x as (select 1) from x": {target: "t", source: "with x as (select 1) from x"},
		`INSERT INTO "T" (a, "B") SELECT 1, 2 ORDER BY 1`:       {target: `"T"`, columns: []string{"a", "B"}, source: "SELECT 1, 2 ORDER BY 1"},
		"INSERT INTO t BY NAME SELECT 1 AS a ORDER BY a":        {target: "t", byName: true, source: "SELECT 1 AS a ORDER BY a"},
	} {
		got, ok := parseSortedInsert(q)
		if !ok || got.target != want.target || !slices.Equal(got.columns, want.columns) || got.byName != want.byName || got.source != want.source {
			t.Errorf("%s: %+v %v, want %+v", q, got, ok, want)
		}
	}
	for _, q := range []string{"INSERT INTO t VALUES (1)", "INSERT INTO t SELECT 1 RETURNING *", "INSERT INTO t (SELECT 1)", "UPDATE t SET a = 1"} {
		if _, ok := parseSortedInsert(q); ok {
			t.Errorf("%s: taken for an INSERT of a query", q)
		}
	}
	if got := (parsedInsert{target: "t", columns: []string{"a", "B"}}).head(); got != `INSERT INTO t ("a", "B")` {
		t.Errorf("head: %s", got)
	}
}

// TestSortedWrites writes rows in order into the catalog with a sort spill
// budget far smaller than they take: CREATE TABLE ... AS and INSERT sort
// them a range at a time, into files that each hold rows in order and
// follow one another in it, and writes whose order is not of the result's
// columns sort at once, as before.
func TestSortedWrites(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	metrics := NewMetrics(prometheus.NewRegistry())
	r := vc.replica(t, func(o *Options) {
		o.SortSpillBudget = 64 << 10
		o.Metrics = metrics
	})
	admin := r.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, admin, "CREATE SCHEMA IF NOT EXISTS lake.public")
	const rows = `SELECT i % 997 AS k, 'v' || i AS v, CASE WHEN i % 11 = 0 THEN NULL ELSE i % 101 END AS n FROM range(30000) r(i)`
	sorted := func() float64 { return testutil.ToFloat64(metrics.sortedWrites) }

	// sameRows checks that table holds rows' rows, each once.
	sameRows := func(table string) {
		t.Helper()
		if n := count(t, admin, "SELECT count(*) FROM "+table); n != 30000 {
			t.Errorf("%s: %d rows", table, n)
		}
		if n := count(t, admin, "SELECT count(*) FROM (SELECT k, v, n FROM "+table+" EXCEPT ALL "+rows+")"); n != 0 {
			t.Errorf("%s: %d rows that the query did not give", table, n)
		}
	}
	// inOrder checks that every data file of table holds its rows in the
	// order of key (create_sort_key's arguments), and that each file
	// written after another continues the order, and returns how many
	// files there are.
	inOrder := func(table, key string) int {
		t.Helper()
		files := texts(t, admin, "SELECT file_path || '|' || manifest_sequence_number FROM iceberg_metadata("+table+
			") WHERE content = 'EXISTING' OR content = 'DATA' ORDER BY manifest_sequence_number, file_path")
		last := ""
		for _, f := range files {
			path := strings.TrimPrefix(f[:strings.LastIndex(f, "|")], "file://")
			got := texts(t, admin, "SELECT hex(min(sk)) || '|' || hex(max(sk)) || '|' || coalesce(bool_and(sk >= prev), true) FROM ("+
				"SELECT sk, lag(sk) OVER (ORDER BY file_row_number) AS prev FROM ("+
				"SELECT create_sort_key("+key+") AS sk, file_row_number FROM read_parquet("+quoteLiteral(path)+", file_row_number = true)))")
			parts := strings.Split(got[0], "|")
			if parts[2] != "true" {
				t.Errorf("%s: a file's rows are out of order", table)
			}
			if parts[0] < last {
				t.Errorf("%s: a file starts before the one written before it ends (%s < %s)", table, parts[0], last)
			}
			last = parts[1]
		}
		return len(files)
	}

	mustExec(t, admin, "CREATE TABLE lake.public.ordered AS "+rows+" ORDER BY k DESC, n NULLS FIRST")
	if got := sorted(); got != 1 {
		t.Fatalf("CREATE TABLE AS: %v sorted writes, want 1", got)
	}
	sameRows("lake.public.ordered")
	if files := inOrder("lake.public.ordered", "k, 'DESC NULLS LAST', n, 'ASC NULLS FIRST'"); files < 2 {
		t.Errorf("CREATE TABLE AS wrote %d files, want several", files)
	}

	// An INSERT, by positions and a qualified column, in one commit of
	// the catalog: a snapshot for each batch, which readers see at once.
	mustExec(t, admin, "CREATE TABLE lake.public.ordered2 (k BIGINT, v VARCHAR, n BIGINT)")
	commits := vc.fake.Commits()
	mustExec(t, admin, "INSERT INTO lake.public.ordered2 SELECT s.k, s.v, s.n FROM ("+rows+") AS s ORDER BY s.n DESC, 1")
	if got := sorted(); got != 2 {
		t.Fatalf("INSERT: %v sorted writes, want 2", got)
	}
	if n := vc.fake.Commits() - commits; n != 1 {
		t.Errorf("the INSERT made %d commits, want 1", n)
	}
	sameRows("lake.public.ordered2")
	if files := inOrder("lake.public.ordered2", "n, 'DESC NULLS LAST', k, 'ASC NULLS LAST'"); files < 2 {
		t.Errorf("INSERT wrote %d files, want several", files)
	}

	// Orders that are not of the result's columns, or whose rows depend on
	// them, sort at once, with the same results as ever.
	for i, q := range []string{
		rows + " ORDER BY lower(v)",
		rows + " ORDER BY k LIMIT 30000",
		"SELECT DISTINCT ON (k, v, n) k, v, n FROM (" + rows + ") ORDER BY k, v, n",
	} {
		table := "lake.public.as_written" + string(rune('a'+i))
		mustExec(t, admin, "CREATE TABLE "+table+" AS "+q)
		sameRows(table)
	}
	if got := sorted(); got != 2 {
		t.Errorf("writes not of the result's columns: %v sorted writes, want 2", got)
	}

	// Snowflake's dialect, with its generator.
	mustExec(t, admin, "SET sql_dialect = 'snowflake'")
	mustExec(t, admin, "CREATE TABLE lake.public.sf_ordered AS SELECT SEQ4() AS id, MOD(SEQ4(), 50) AS g FROM TABLE(GENERATOR(ROWCOUNT => 30000)) ORDER BY g, id")
	mustExec(t, admin, "SET sql_dialect = 'duckdb'")
	if got := sorted(); got != 3 {
		t.Errorf("Snowflake's dialect: %v sorted writes, want 3", got)
	}
	if n := count(t, admin, "SELECT count(DISTINCT id) FROM lake.public.sf_ordered"); n != 30000 {
		t.Errorf("Snowflake's dialect: %d ids", n)
	}
	inOrder("lake.public.sf_ordered", "g, 'ASC NULLS LAST', id, 'ASC NULLS LAST'")

	// The rows' files are gone.
	dir := texts(t, admin, "SELECT current_setting('temp_directory')")[0]
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), engine.SortedWriteDirPrefix) {
			t.Errorf("left in the spill directory: %s", e.Name())
		}
	}
}
