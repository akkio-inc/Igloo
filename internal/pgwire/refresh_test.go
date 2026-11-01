package pgwire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// refreshEnv is a server on an Iceberg catalog, with the scheduler's store
// in memory, for materialized views and dynamic tables.
type refreshEnv struct {
	*testServer
	admin *pgx.Conn
	tasks *tasks.Scheduler
	// fake is the catalog, catalog a client of it, and dir where the
	// tables' files are.
	fake    *icebergtest.Catalog
	catalog *catalog.Client
	dir     string
}

func startRefreshServer(t *testing.T, configure ...func(*Options)) *refreshEnv {
	t.Helper()
	return startRefreshServerWith(t, nil, configure...)
}

// startRefreshServerWith is startRefreshServer with the engine opened with
// opts.
func startRefreshServerWith(t *testing.T, opts []engine.Option, configure ...func(*Options)) *refreshEnv {
	t.Helper()
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	return startRefreshServerOn(t, fake, dir, fake.URL, opts, configure...)
}

// startRefreshServerOn is startRefreshServerWith on the catalog fake, whose
// tables' files are in dir, reached at uri (fake's URL, or a Faults in
// front of it), as another replica, or the same one started again, is.
func startRefreshServerOn(t *testing.T, fake *icebergtest.Catalog, dir, uri string, opts []engine.Option, configure ...func(*Options)) *refreshEnv {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	ctx := context.Background()
	cfg := config.Config{
		Catalog: config.Catalog{URI: uri, Warehouse: "test", Name: "lake", Schema: "public"},
		Storage: config.Storage{Location: dir, S3: config.S3{Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused"}},
		DuckDB:  config.DuckDB{ExtensionDir: ext, TempDir: t.TempDir()},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng, err := engine.Open(ctx, cfg, log, opts...)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := catalog.Open(ctx, cfg)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	objects, err := ddl.New(ctx, cat, eng, log)
	if err != nil {
		_ = eng.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	env := &refreshEnv{tasks: tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{}), fake: fake, catalog: cat, dir: dir}
	env.testServer = startServerOn(t, eng, append([]func(*Options){func(o *Options) { o.DDL, o.Tasks = objects, env.tasks }}, configure...)...)
	env.admin = env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	return env
}

// textRows returns query's rows, every column as text, sorted.
func sortedTexts(t *testing.T, conn *pgx.Conn, query string) []string {
	t.Helper()
	rows := texts(t, conn, "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ("+query+") AS q")
	slices.Sort(rows)
	return rows
}

// viewRefresh returns what the last refresh of a materialized view did and
// why, as SHOW MATERIALIZED VIEWS says.
func viewRefresh(t *testing.T, conn *pgx.Conn, name string) (action, detail, mode, reason string) {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SHOW MATERIALIZED VIEWS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		if vals[0] != name {
			continue
		}
		str := func(v any) string {
			if v == nil {
				return ""
			}
			return fmt.Sprint(v)
		}
		return str(vals[13]), str(vals[14]), str(vals[11]), str(vals[12])
	}
	t.Fatalf("no materialized view %s", name)
	return
}

// refreshShapes are queries of every shape refreshes keep incrementally,
// and some they do not, over f (facts) and d (a dimension).
var refreshShapes = []struct {
	name, query string
	// deletes says it takes rows deleted incrementally; dim that it reads
	// d, and how a change to d refreshes it.
	deletes bool
	dim     string
}{
	{"filtered", "SELECT id, g, x * 2 AS x2, s FROM f WHERE x % 3 <> 0 OR x IS NULL", false, ""},
	{"joined", "SELECT f.id, f.x, d.name FROM f JOIN d ON f.g = d.g WHERE f.x > 10", false, "INCREMENTAL"},
	{"left", "SELECT f.id, f.g, d.name FROM f LEFT JOIN d ON f.g = d.g", false, "FULL"},
	{"sums", "SELECT g, count(*) AS n, count(y) AS ny, sum(x) AS sx, avg(x) AS ax, sum(amount) AS sa, sum(y) AS sy FROM f GROUP BY g", true, ""},
	{"extremes", "SELECT g, min(x) AS lo, max(x) AS hi, max(s) AS ms, count(*) AS n FROM f GROUP BY g", false, ""},
	{"whole", "SELECT count(*) AS n, sum(x) AS sx, avg(y) AS ay FROM f", true, ""},
	{"joined_groups", "SELECT d.name, sum(f.x) AS sx, count(*) AS n FROM f JOIN d ON f.g = d.g GROUP BY d.name HAVING count(*) > 1", true, "INCREMENTAL"},
	{"distinct", "SELECT DISTINCT g, y FROM f", true, ""},
	{"distinct_counts", "SELECT g, count(DISTINCT u) AS du, count(*) AS n FROM f GROUP BY ALL", false, ""},
	{"expressions", "SELECT upper(g) || '!' AS k, sum(x) * 2 AS sx2, sum(x) / count(*) AS ratio FROM f GROUP BY g", true, ""},
	{"aliases", "SELECT coalesce(g, '?') AS grp, sum(amount) AS total FROM f GROUP BY grp HAVING total > 0 OR total IS NULL", true, ""},
	{"self_join", "SELECT a.g, count(*) AS pairs, sum(b.x) AS sx FROM f AS a JOIN f AS b ON a.id = b.id + 1 GROUP BY a.g", true, ""},
}

// TestIncrementalRefresh refreshes views of every shape after random
// appends, updates and deletes, and checks that each refresh gives what
// the query gives, exactly, and that refreshes are incremental when the
// change and the shape allow it.
func TestIncrementalRefresh(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	// Small tables change by more than half at a time; reading the change
	// is still what is tested here.
	env.srv.refreshFraction = 1000
	admin := env.admin
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE f (id INTEGER, g VARCHAR, x INTEGER, y BIGINT, amount DECIMAL(10,2), s VARCHAR, u INTEGER)",
		"CREATE TABLE d (g VARCHAR, name VARCHAR)",
		"INSERT INTO d VALUES ('a', 'Alpha'), ('b', 'Beta'), ('c', 'Gamma'), ('d', 'Delta')",
	} {
		mustExec(t, admin, sql)
	}
	rng := rand.New(rand.NewPCG(7, 11))
	nextID := 0
	appendRows := func(n int) {
		var vals []string
		for range n {
			nextID++
			g := []string{"'a'", "'b'", "'c'", "'e'", "NULL"}[rng.IntN(5)]
			x := fmt.Sprint(rng.IntN(100))
			if rng.IntN(8) == 0 {
				x = "NULL"
			}
			y := fmt.Sprint(rng.IntN(5))
			if rng.IntN(4) == 0 {
				y = "NULL"
			}
			amount := fmt.Sprintf("%d.%02d", rng.IntN(1000), rng.IntN(100))
			if rng.IntN(10) == 0 {
				amount = "NULL"
			}
			vals = append(vals, fmt.Sprintf("(%d, %s, %s, %s, %s, 's%d', %d)", nextID, g, x, y, amount, rng.IntN(50), rng.IntN(20)))
		}
		mustExec(t, admin, "INSERT INTO f VALUES "+strings.Join(vals, ", "))
	}
	appendRows(40)
	for _, s := range refreshShapes {
		mustExec(t, admin, "CREATE MATERIALIZED VIEW mv_"+s.name+" AS "+s.query)
	}
	check := func(round int) {
		t.Helper()
		for _, s := range refreshShapes {
			if got, want := sortedTexts(t, admin, "SELECT * FROM mv_"+s.name), sortedTexts(t, admin, s.query); !slices.Equal(got, want) {
				action, detail, _, _ := viewRefresh(t, admin, "public.mv_"+s.name)
				t.Fatalf("round %d, %s (%s: %s):\n got %q\nwant %q", round, s.name, action, detail, got, want)
			}
		}
	}
	refreshAll := func() map[string]string {
		actions := map[string]string{}
		for _, s := range refreshShapes {
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW mv_"+s.name)
			action, detail, _, _ := viewRefresh(t, admin, "public.mv_"+s.name)
			actions[s.name] = action + ": " + detail
		}
		return actions
	}
	// The first refresh builds the state.
	for name, a := range refreshAll() {
		if !strings.HasPrefix(a, "FULL") {
			t.Errorf("%s's first refresh: %s", name, a)
		}
	}
	check(0)
	incremental := map[string]int{}
	for round := 1; round <= 12; round++ {
		kind := "append"
		switch {
		case round%4 == 0:
			kind = "delete"
			mustExec(t, admin, fmt.Sprintf("DELETE FROM f WHERE id %% 7 = %d", rng.IntN(7)))
		case round%4 == 2:
			kind = "update"
			mustExec(t, admin, fmt.Sprintf("UPDATE f SET x = x + 1, g = 'b' WHERE id %% 5 = %d", rng.IntN(5)))
		case round == 7:
			kind = "dimension"
			mustExec(t, admin, "INSERT INTO d VALUES ('e', 'Epsilon')")
		case round == 11:
			kind = "nothing"
		}
		if kind != "nothing" && kind != "dimension" {
			appendRows(1 + rng.IntN(15))
		}
		actions := refreshAll()
		check(round)
		for _, s := range refreshShapes {
			a := actions[s.name]
			if strings.HasPrefix(a, "INCREMENTAL") {
				incremental[s.name]++
			}
			switch {
			case kind == "nothing" && !strings.HasPrefix(a, "NO_DATA"):
				t.Errorf("round %d, %s, nothing changed: %s", round, s.name, a)
			case kind == "append" && !strings.HasPrefix(a, "INCREMENTAL") && s.name != "left":
				t.Errorf("round %d, %s, rows appended: %s", round, s.name, a)
			case (kind == "delete" || kind == "update") && s.deletes && !strings.HasPrefix(a, "INCREMENTAL"):
				t.Errorf("round %d, %s, rows %sd: %s", round, s.name, kind, a)
			case (kind == "delete" || kind == "update") && !s.deletes && !strings.HasPrefix(a, "FULL"):
				t.Errorf("round %d, %s, rows %sd: %s", round, s.name, kind, a)
			case kind == "dimension" && s.dim == "" && !strings.HasPrefix(a, "NO_DATA"):
				t.Errorf("round %d, %s, which does not read it, after the dimension changed: %s", round, s.name, a)
			case kind == "dimension" && s.dim != "" && !strings.HasPrefix(a, s.dim):
				t.Errorf("round %d, %s, dimension changed: %s", round, s.name, a)
			}
		}
	}
	t.Logf("incremental refreshes by shape: %v", incremental)

	// Its state is Igloo's own: no user reads or writes it, admins
	// included.
	state := texts(t, admin, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'igloo_refresh' ORDER BY 1")
	if len(state) == 0 {
		t.Fatal("no state tables")
	}
	if _, err := admin.Exec(ctx, "SELECT * FROM lake.igloo_refresh."+quoteIdent(state[0])); err == nil {
		if n := count(t, admin, "SELECT count(*) FROM lake.igloo_refresh."+quoteIdent(state[0])); n != 0 {
			t.Errorf("an admin read %d rows of a state table", n)
		}
	}
	if _, err := admin.Exec(ctx, "DELETE FROM lake.igloo_refresh."+quoteIdent(state[0])); pgCode(err) != "42501" {
		t.Errorf("an admin wrote a state table: %v", err)
	}

	// A view the shapes do not cover says why.
	mustExec(t, admin, "CREATE MATERIALIZED VIEW mv_window AS SELECT id, row_number() OVER (ORDER BY id) AS r FROM f")
	if _, _, mode, reason := viewRefresh(t, admin, "public.mv_window"); mode != "FULL" || !strings.Contains(reason, "window function") {
		t.Errorf("window: %s, %q", mode, reason)
	}
	// Dropping a view drops its state.
	mustExec(t, admin, "DROP MATERIALIZED VIEW mv_sums")
	mustExec(t, admin, "DROP TABLE mv_extremes")
	if after := texts(t, admin, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'igloo_refresh' ORDER BY 1"); len(after) != len(state)-2 {
		t.Errorf("state tables after drops: %q, before %q", after, state)
	}
}

// TestRefreshFallsBack checks the refreshes that read the tables whole,
// and that each says why.
func TestRefreshFallsBack(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	for _, sql := range []string{
		"CREATE TABLE f (id INTEGER, g VARCHAR, x INTEGER)",
		"INSERT INTO f SELECT i, 'g' || (i % 4), i FROM range(100) AS r(i)",
		"CREATE MATERIALIZED VIEW sums AS SELECT g, sum(x) AS sx FROM f GROUP BY g",
		"CREATE MATERIALIZED VIEW floats AS SELECT g, sum(x * 0.5::DOUBLE) AS sx FROM f GROUP BY g",
		"REFRESH MATERIALIZED VIEW sums",
		"REFRESH MATERIALIZED VIEW floats",
	} {
		mustExec(t, admin, sql)
	}
	if _, _, mode, reason := viewRefresh(t, admin, "public.floats"); mode != "FULL" || !strings.Contains(reason, "DOUBLE") {
		t.Errorf("a sum of doubles: %s, %q", mode, reason)
	}
	// A change of most of the table reads it whole, as reading the change
	// would read about as many bytes.
	mustExec(t, admin, "INSERT INTO f SELECT i, 'g' || (i % 4), i FROM range(100, 2000) AS r(i)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	if action, detail, _, _ := viewRefresh(t, admin, "public.sums"); action != "FULL" || !strings.Contains(detail, "costs about as much as reading its tables whole") {
		t.Errorf("a change of most of the table: %s, %q", action, detail)
	}
	// A small one reads the change.
	mustExec(t, admin, "INSERT INTO f VALUES (500, 'g1', 500)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	if action, detail, _, _ := viewRefresh(t, admin, "public.sums"); action != "INCREMENTAL" || !strings.Contains(detail, "read 1 rows added") {
		t.Errorf("one row: %s, %q", action, detail)
	}
	// A write to the view's table refreshes in full.
	mustExec(t, admin, "INSERT INTO sums VALUES ('zz', 1)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	if action, detail, _, _ := viewRefresh(t, admin, "public.sums"); action != "FULL" || !strings.Contains(detail, "other than by a refresh") {
		t.Errorf("a write to the view: %s, %q", action, detail)
	}
	if got, want := sortedTexts(t, admin, "SELECT * FROM sums"), sortedTexts(t, admin, "SELECT g, sum(x) FROM f GROUP BY g"); !slices.Equal(got, want) {
		t.Errorf("sums: %q, want %q", got, want)
	}
	// So does a change of its query.
	mustExec(t, admin, "CREATE OR REPLACE MATERIALIZED VIEW sums AS SELECT g, sum(x) AS sx, count(*) AS n FROM f GROUP BY g")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	if action, detail, _, _ := viewRefresh(t, admin, "public.sums"); action != "FULL" || !strings.Contains(detail, "since it was created or replaced") {
		t.Errorf("a replaced view: %s, %q", action, detail)
	}
	// The run's detail is in the history.
	if n := count(t, admin, "SELECT count(*) FROM igloo.task_history WHERE name = 'public.sums' AND detail LIKE 'incremental: read 1 rows added%'"); n != 1 {
		t.Errorf("incremental runs in the history: %d", n)
	}
	// A table with a column of the name of DuckDB's column of each row's
	// data file reads whole, rather than by that column, which would read
	// the table's own.
	for _, sql := range []string{
		"CREATE TABLE uploads (filename VARCHAR, bytes INTEGER)",
		"INSERT INTO uploads SELECT 'f' || i, i FROM range(100) AS r(i)",
		"CREATE MATERIALIZED VIEW upload_bytes AS SELECT count(*) AS n, sum(bytes) AS b FROM uploads",
		"REFRESH MATERIALIZED VIEW upload_bytes",
		"INSERT INTO uploads VALUES ('g', 1000)",
		"REFRESH MATERIALIZED VIEW upload_bytes",
	} {
		mustExec(t, admin, sql)
	}
	if action, detail, _, _ := viewRefresh(t, admin, "public.upload_bytes"); action != "FULL" || !strings.Contains(detail, "column named filename") {
		t.Errorf("a column named filename: %s, %q", action, detail)
	}
	if got := sortedTexts(t, admin, "SELECT n, b FROM upload_bytes"); !slices.Equal(got, []string{"101|5950"}) {
		t.Errorf("upload_bytes: %q", got)
	}
}

// TestRefreshCost checks that a refresh weighs the deletions a full one
// would apply: a delete of rows in every one of a table's files reads the
// files once as the last refresh read them and joins the new delete files
// by position, where a full refresh would apply every deletion in its
// scan; and that a small delete from a table that holds many deletions,
// which both would apply, refreshes in full.
func TestRefreshCost(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE spend (id INTEGER, g VARCHAR, x INTEGER)")
	for i := range 4 {
		mustExec(t, admin, fmt.Sprintf("INSERT INTO spend SELECT i, 'g' || (i %% 4), i FROM range(%d, %d) AS r(i)", i*5000, (i+1)*5000))
	}
	mustExec(t, admin, "CREATE MATERIALIZED VIEW spend_sums AS SELECT g, count(*) AS n, sum(x) AS sx FROM spend GROUP BY g")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW spend_sums")
	check := func(step, want, why string) {
		t.Helper()
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW spend_sums")
		if action, detail, _, _ := viewRefresh(t, admin, "public.spend_sums"); action != want || !strings.Contains(detail, why) {
			t.Errorf("%s: %s, %q; want %s", step, action, detail, want)
		}
		if got, want := sortedTexts(t, admin, "SELECT * FROM spend_sums"), sortedTexts(t, admin, "SELECT g, count(*), sum(x) FROM spend GROUP BY g"); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", step, got, want)
		}
	}
	mustExec(t, admin, "DELETE FROM spend WHERE id % 10 = 3")
	check("a tenth deleted", "INCREMENTAL", "")
	mustExec(t, admin, "DELETE FROM spend WHERE id % 10 <> 3 AND id % 10 <> 0")
	check("most of the rest deleted", "INCREMENTAL", "")
	mustExec(t, admin, "DELETE FROM spend WHERE id % 20 = 0")
	check("a few more deleted, among many", "FULL", "costs about as much as reading its tables whole")
}

// TestDynamicTables checks Snowflake's DYNAMIC TABLE statements.
func TestDynamicTables(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	ctx := context.Background()
	for _, sql := range []string{
		"CREATE TABLE events (day DATE, client VARCHAR, spend DECIMAL(12,2))",
		"INSERT INTO events VALUES ('2026-10-01', 'acme', 10.50), ('2026-10-01', 'globex', 3.25), ('2026-10-02', 'acme', 1.00)",
	} {
		mustExec(t, admin, sql)
	}
	// WAREHOUSE is accepted, and ignored.
	mustExec(t, admin, "CREATE DYNAMIC TABLE daily TARGET_LAG = '10 minutes' WAREHOUSE = reporting_wh REFRESH_MODE = AUTO AS "+
		"SELECT day, client, sum(spend) AS spend, count(*) AS events FROM events GROUP BY day, client")
	if got := sortedTexts(t, admin, "SELECT * FROM daily"); len(got) != 3 {
		t.Errorf("created: %q", got)
	}
	show := texts(t, admin, "SHOW DYNAMIC TABLES LIKE 'dai%'")
	if len(show) != 1 || !strings.Contains(show[0], "|daily|lake|public|igloo|10 minutes|INCREMENTAL|NULL|reporting_wh|") {
		t.Errorf("SHOW DYNAMIC TABLES: %q", show)
	}
	if got := texts(t, admin, "SHOW DYNAMIC TABLES LIKE 'nope%'"); len(got) != 0 {
		t.Errorf("SHOW ... LIKE: %q", got)
	}
	j, err := env.tasks.Store().Job(ctx, tasks.KindDynamicTable, "public.daily")
	if err != nil || j.Schedule != "300 seconds" || j.State != tasks.StateStarted || j.NextRun.IsZero() {
		t.Errorf("job: %+v %v", j, err)
	}
	mustExec(t, admin, "INSERT INTO events VALUES ('2026-10-02', 'globex', 7.00)")
	mustExec(t, admin, "ALTER DYNAMIC TABLE daily REFRESH")
	mustExec(t, admin, "INSERT INTO events VALUES ('2026-10-03', 'acme', 2.00)")
	mustExec(t, admin, "ALTER DYNAMIC TABLE daily REFRESH")
	if got, want := sortedTexts(t, admin, "SELECT * FROM daily"),
		sortedTexts(t, admin, "SELECT day, client, sum(spend), count(*) FROM events GROUP BY ALL"); !slices.Equal(got, want) {
		t.Errorf("refreshed: %q, want %q", got, want)
	}
	if n := count(t, admin, "SELECT count(*) FROM igloo.task_history WHERE kind = 'dynamic table' AND detail LIKE 'incremental:%'"); n != 1 {
		t.Errorf("incremental refreshes: %d", n)
	}
	mustExec(t, admin, "ALTER DYNAMIC TABLE daily SUSPEND")
	if got := texts(t, admin, "SHOW DYNAMIC TABLES"); len(got) != 1 || !strings.Contains(got[0], "|SUSPENDED|") {
		t.Errorf("suspended: %q", got)
	}
	mustExec(t, admin, "ALTER DYNAMIC TABLE daily RESUME")
	mustExec(t, admin, "ALTER DYNAMIC TABLE daily SET TARGET_LAG = '1 hour'")
	if j, err := env.tasks.Store().Job(ctx, tasks.KindDynamicTable, "public.daily"); err != nil || j.Schedule != "1800 seconds" {
		t.Errorf("SET TARGET_LAG: %+v %v", j, err)
	}

	// REFRESH_MODE = INCREMENTAL refuses a query it cannot refresh so.
	_, err = admin.Exec(ctx, "CREATE DYNAMIC TABLE ranked TARGET_LAG = '1 hour' WAREHOUSE = wh REFRESH_MODE = INCREMENTAL AS "+
		"SELECT client, rank() OVER (ORDER BY spend) AS r FROM events")
	if pgCode(err) != "0A000" || !strings.Contains(err.Error(), "window function") {
		t.Errorf("INCREMENTAL of a window: %v", err)
	}
	mustExec(t, admin, "CREATE DYNAMIC TABLE ranked TARGET_LAG = DOWNSTREAM WAREHOUSE = wh AS SELECT client, rank() OVER (ORDER BY spend) AS r FROM events")
	if got := texts(t, admin, "SHOW DYNAMIC TABLES LIKE 'ranked'"); len(got) != 1 ||
		!strings.Contains(got[0], "|DOWNSTREAM|FULL|the query has a window function") {
		t.Errorf("a window's mode: %q", got)
	}
	// A dynamic table that reads one whose lag is DOWNSTREAM refreshes it
	// first.
	mustExec(t, admin, "CREATE DYNAMIC TABLE top TARGET_LAG = '1 day' WAREHOUSE = wh AS SELECT client, min(r) AS best FROM ranked GROUP BY client")
	mustExec(t, admin, "INSERT INTO events VALUES ('2026-10-04', 'initech', 0.01)")
	mustExec(t, admin, "ALTER DYNAMIC TABLE top REFRESH")
	if got := sortedTexts(t, admin, "SELECT client FROM top"); !slices.Contains(got, "initech") {
		t.Errorf("top after its upstream refreshed: %q", got)
	}

	// One name, one table.
	for sql, code := range map[string]string{
		"CREATE MATERIALIZED VIEW daily AS SELECT 1 AS one":               "42P07",
		"CREATE DYNAMIC TABLE x AS SELECT 1":                              "42601",
		"CREATE DYNAMIC TABLE x TARGET_LAG = '10 seconds' AS SELECT 1":    "22023",
		"CREATE DYNAMIC TABLE x TARGET_LAG = 'often' AS SELECT 1":         "22023",
		"ALTER DYNAMIC TABLE daily SET SCHEDULE '1 hour'":                 "42601",
		"ALTER DYNAMIC TABLE nope REFRESH":                                "42P01",
		"CREATE DYNAMIC TABLE y TARGET_LAG = '1 hour' INITIALIZE = LATER": "42601",
	} {
		if _, err := admin.Exec(ctx, sql); pgCode(err) != code {
			t.Errorf("%s: %v, want %s", sql, err, code)
		}
	}
	mustExec(t, admin, "CREATE OR REPLACE MATERIALIZED VIEW daily AS SELECT 1 AS one")
	if got := texts(t, admin, "SHOW DYNAMIC TABLES LIKE 'daily'"); len(got) != 0 {
		t.Errorf("a dynamic table a materialized view replaced: %q", got)
	}
	mustExec(t, admin, "CREATE DYNAMIC TABLE later TARGET_LAG = '1 hour' WAREHOUSE = wh INITIALIZE = ON_SCHEDULE AS SELECT * FROM events")
	if n := count(t, admin, "SELECT count(*) FROM later"); n != 0 {
		t.Errorf("INITIALIZE = ON_SCHEDULE: %d rows before its first refresh", n)
	}
	mustExec(t, admin, "DROP DYNAMIC TABLE later")
	if _, err := admin.Exec(ctx, "SELECT * FROM later"); pgCode(err) != "42P01" {
		t.Errorf("after DROP DYNAMIC TABLE: %v", err)
	}
	// Snowflake's dialect: the query is translated, the options kept.
	sf := snowflakeConn(t, env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol))
	mustExec(t, sf, `CREATE OR REPLACE DYNAMIC TABLE client_totals TARGET_LAG = '2 hours' WAREHOUSE = transform_wh REFRESH_MODE = INCREMENTAL
		AS SELECT client, SUM(IFF(spend > 1, spend, 0)) AS big_spend, COUNT(*) AS n FROM events GROUP BY client`)
	mustExec(t, sf, "INSERT INTO events VALUES ('2026-10-05', 'acme', 9.00)")
	mustExec(t, sf, "ALTER DYNAMIC TABLE client_totals REFRESH")
	if got, want := sortedTexts(t, admin, "SELECT * FROM client_totals"),
		sortedTexts(t, admin, "SELECT client, sum(CASE WHEN spend > 1 THEN spend ELSE 0 END), count(*) FROM events GROUP BY client"); !slices.Equal(got, want) {
		t.Errorf("Snowflake's dialect: %q, want %q", got, want)
	}
	if got := texts(t, sf, "SHOW DYNAMIC TABLES LIKE 'CLIENT%'"); len(got) != 1 || !strings.Contains(got[0], "|2 hours|INCREMENTAL|NULL|transform_wh|") {
		t.Errorf("SHOW in Snowflake's dialect: %q", got)
	}
	mustExec(t, sf, "DROP DYNAMIC TABLE client_totals")

	// Only writers schedule them.
	mustExec(t, admin, "CREATE USER nina WITH PASSWORD 'nina-password' ROLE reader")
	nina := connect(t, env.url("nina", "nina-password"), pgx.QueryExecModeSimpleProtocol)
	expectDenied(t, nina, "CREATE DYNAMIC TABLE lake.public.mine TARGET_LAG = '1 hour' AS SELECT 1 AS one", "writer role")
}

// TestRefreshSavings measures what an incremental refresh reads against a
// full one's, after a day's load into a fact table of 30 days.
func TestRefreshSavings(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE facts (day DATE, client INTEGER, campaign INTEGER, person INTEGER, spend DECIMAL(12,2), clicks BIGINT)")
	// IGLOO_TEST_REFRESH_DAY_ROWS measures at another scale.
	perDay := 200_000
	if n, err := strconv.Atoi(os.Getenv("IGLOO_TEST_REFRESH_DAY_ROWS")); err == nil && n > 0 {
		perDay = n
	}
	load := func(day int) {
		mustExec(t, admin, fmt.Sprintf(`INSERT INTO facts SELECT DATE '2026-09-01' + %d, i %% 50, i %% 1000, (i * 7919) %% 200000,
			(i %% 9973) / 100.0, i %% 17 FROM range(%d, %d) AS r(i)`, day, day*perDay, (day+1)*perDay))
	}
	for day := range 30 {
		load(day)
	}
	const query = `SELECT day, client, count(*) AS rows, sum(spend) AS spend, avg(clicks) AS clicks, count(DISTINCT person) AS people
		FROM facts GROUP BY day, client`
	mustExec(t, admin, "CREATE MATERIALIZED VIEW auto_daily AS "+query)
	mustExec(t, admin, "CREATE DYNAMIC TABLE full_daily TARGET_LAG = DOWNSTREAM WAREHOUSE = wh REFRESH_MODE = FULL AS "+query)
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW auto_daily")
	load(30)
	bytesOf := regexp.MustCompile(`\((?:\d+ rows, )?(\d+) bytes of files`)
	measure := func(stmt, name string) (bytes int64, took time.Duration, detail string) {
		t.Helper()
		start := time.Now()
		mustExec(t, admin, stmt)
		took = time.Since(start)
		rows := texts(t, admin, "SELECT detail FROM igloo.task_history WHERE name = '"+name+"' ORDER BY start_time DESC LIMIT 1")
		if len(rows) != 1 {
			t.Fatalf("no run of %s", name)
		}
		detail = rows[0]
		m := bytesOf.FindStringSubmatch(detail)
		if m == nil {
			t.Fatalf("%s: %q", name, detail)
		}
		_, _ = fmt.Sscan(m[1], &bytes)
		return bytes, took, detail
	}
	incBytes, incTook, incDetail := measure("REFRESH MATERIALIZED VIEW auto_daily", "public.auto_daily")
	fullBytes, fullTook, fullDetail := measure("ALTER DYNAMIC TABLE full_daily REFRESH", "public.full_daily")
	t.Logf("incremental: %d bytes of files read, %v: %s", incBytes, incTook, incDetail)
	t.Logf("full: %d bytes of files read, %v: %s", fullBytes, fullTook, fullDetail)
	if !strings.HasPrefix(incDetail, fmt.Sprintf("incremental: read %d rows added", perDay)) || !strings.HasPrefix(fullDetail, "full:") {
		t.Errorf("refreshes: %q, %q", incDetail, fullDetail)
	}
	if incBytes*5 > fullBytes {
		t.Errorf("an incremental refresh read %d bytes, a full one %d", incBytes, fullBytes)
	}
	if got, want := sortedTexts(t, admin, "SELECT * FROM auto_daily"), sortedTexts(t, admin, "SELECT * FROM full_daily"); !slices.Equal(got, want) {
		t.Errorf("incremental and full refreshes differ: %d and %d rows", len(got), len(want))
	}
}
