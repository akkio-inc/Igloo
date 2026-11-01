package pgwire

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
)

// largeSortFile writes rows rows shaped like a report's detail rows to a
// Parquet file: a 36-character ID (NULL in some rows), a timestamp, a
// short key, a decimal and a number unique to the row, to break ties.
func largeSortFile(t testing.TB, path string, rows int64) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	if _, err := db.Exec(fmt.Sprintf(`COPY (
		SELECT CASE WHEN i %% 1009 = 0 THEN NULL ELSE md5((i %% 1000003)::VARCHAR)[1:8] || '-' || md5((i %% 1000003)::VARCHAR)[9:12] || '-' ||
		       md5((i %% 1000003)::VARCHAR)[13:16] || '-' || md5((i %% 1000003)::VARCHAR)[17:20] || '-' || md5((i %% 1000003)::VARCHAR)[21:32] END AS id,
		       TIMESTAMP '2024-11-13' + to_seconds((hash(i) %% 7603200)::BIGINT) AS ts,
		       (hash(i * 7) %% 32893579)::VARCHAR AS k,
		       (hash(i * 13) %% 6146)::DECIMAL(38, 0) AS secs,
		       i AS n
		FROM range(%d) t(i)) TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 122880)`, rows, path)); err != nil {
		t.Fatal(err)
	}
}

// largeSortDirs lists what sorts in runs left in the spill directory.
func largeSortDirs(t testing.TB, dir string) []string {
	t.Helper()
	found, err := filepath.Glob(filepath.Join(dir, engine.SortedWriteDirPrefix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// TestLargeSortInRuns reads rows in order that the plan estimates would not
// fit in memory, sorted in runs and merged: every row, in the order of one
// machine's sort, descending and ascending, NULLs first and last, by a
// result's columns by name and position, in every protocol, leaving nothing
// in the spill directory. A statement that would not fit, but that keeps
// few rows, orders by an expression, has parameters, or that its client
// reads a few rows at a time, sorts at once, as does one that fits.
func TestLargeSortInRuns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	spill := t.TempDir()
	file := filepath.Join(dir, "rows.parquet")
	largeSortFile(t, file, 2_000_000)
	metrics := NewMetrics(prometheus.NewRegistry())
	ts := startServerWithConfig(t, config.Config{
		DuckDB:  config.DuckDB{TempDir: spill, MemoryLimit: "400MiB", Threads: 4, ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")},
		Storage: config.Storage{AllowedLocations: []string{dir + "/"}},
	}, func(o *Options) { o.Metrics = metrics })
	sorts := func() float64 { return testutil.ToFloat64(metrics.largeSorts) }
	from := "read_parquet('" + file + "')"
	plain, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close() //nolint:errcheck

	for i, order := range []string{
		"id NULLS FIRST, n",
		"id DESC NULLS LAST, n DESC",
		"ts, k, n",
		"secs DESC, 1, n",
	} {
		query := "SELECT id, ts, k, secs, n FROM " + from + " ORDER BY " + order
		for name, mode := range modes {
			// Every protocol once: each statement takes seconds.
			if i > 0 && mode != pgx.QueryExecModeSimpleProtocol {
				continue
			}
			t.Run(order+"/"+name, func(t *testing.T) {
				conn := ts.admin(t, mode)
				before := sorts()
				want, err := plain.Query(query)
				if err != nil {
					t.Fatal(err)
				}
				defer want.Close() //nolint:errcheck
				rows, err := conn.Query(context.Background(), query)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				n := 0
				for rows.Next() {
					vals, err := rows.Values()
					if err != nil {
						t.Fatal(err)
					}
					if !want.Next() {
						t.Fatalf("more than the %d rows of one machine's sort", n)
					}
					var id, tsv, k, secs any
					var num int64
					if err := want.Scan(&id, &tsv, &k, &secs, &num); err != nil {
						t.Fatal(err)
					}
					if got := vals[4].(int64); got != num {
						t.Fatalf("row %d: n %d, want %d (%v)", n, got, num, vals)
					}
					n++
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				if want.Next() || n != 2_000_000 {
					t.Fatalf("%d rows, fewer than one machine's sort", n)
				}
				if got := sorts() - before; got != 1 {
					t.Errorf("sorted in runs %v times, want once", got)
				}
				if left := largeSortDirs(t, spill); len(left) > 0 {
					t.Errorf("left in the spill directory: %v", left)
				}
			})
		}
	}

	// A statement with parameters.
	params := ts.admin(t, pgx.QueryExecModeExec)
	before := sorts()
	var rowCount int64
	rows, err := params.Query(context.Background(), "SELECT id, n FROM "+from+" WHERE n >= $1 ORDER BY id, n", int64(0))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		rowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if rowCount != 2_000_000 || sorts() != before {
		t.Errorf("with a parameter: %d rows, sorted in runs %v times", rowCount, sorts()-before)
	}

	// A client that reads a few rows at a time.
	h, err := ts.admin(t, pgx.QueryExecModeSimpleProtocol).PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Conn.Close() //nolint:errcheck
	_ = h.Conn.SetDeadline(time.Now().Add(5 * time.Minute))
	h.Frontend.Send(&pgproto3.Parse{Name: "big", Query: "SELECT id, n FROM " + from + " ORDER BY id, n"})
	h.Frontend.Send(&pgproto3.Bind{PreparedStatement: "big", DestinationPortal: "p"})
	h.Frontend.Send(&pgproto3.Execute{Portal: "p", MaxRows: 10})
	h.Frontend.Send(&pgproto3.Sync{})
	if err := h.Frontend.Flush(); err != nil {
		t.Fatal(err)
	}
	before = sorts()
	limited := 0
	for done := false; !done; {
		msg, err := h.Frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			limited++
		case *pgproto3.ErrorResponse:
			t.Fatal(m.Message)
		case *pgproto3.ReadyForQuery:
			done = true
		}
	}
	if limited != 10 || sorts() != before {
		t.Errorf("a portal of 10 rows: %d rows, sorted in runs %v times", limited, sorts()-before)
	}

	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"SELECT id, n FROM " + from + " ORDER BY id, n LIMIT 5",
		"SELECT id, n FROM " + from + " ORDER BY n + 1 LIMIT 5",
		"SELECT id, count(*) AS c FROM " + from + " GROUP BY 1 ORDER BY 1",
		"SELECT n FROM " + from + " WHERE n < 10 ORDER BY upper(id), n",
		"SELECT * FROM (VALUES (1), (2)) t(x) ORDER BY x",
	} {
		before := sorts()
		rows, err := conn.Query(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for rows.Next() {
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := sorts() - before; got != 0 {
			t.Errorf("%s: sorted in runs", q)
		}
	}
}

// vmHWM is the most memory the process has held, by the kernel's count.
func vmHWM(t testing.TB) int64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skip("no /proc/self/status: ", err)
	}
	m := regexp.MustCompile(`VmHWM:\s+(\d+) kB`).FindSubmatch(data)
	if m == nil {
		t.Skip("no VmHWM")
	}
	n, _ := strconv.ParseInt(string(m[1]), 10, 64)
	return n << 10
}

// TestLargeSortPeak checks, in a process of its own, what a read of more
// rows in order than memory holds takes of the machine: it sorts in runs,
// and its peak stays near DuckDB's limit, where DuckDB's own sort of them,
// at once, goes well past it, to where the memory watch would cancel it on a
// machine with little to spare. IGLOO_TEST_LARGE_SORT_ROWS and _LIMIT set
// its size, ten times this, say, on all of the machine's threads, and
// measure the sort at once too.
func TestLargeSortPeak(t *testing.T) {
	limit, rows, threads := "1GiB", int64(7_400_000), 4
	measured := false
	if v := os.Getenv("IGLOO_TEST_LARGE_SORT_LIMIT"); v != "" {
		limit, threads, measured = v, 0, true
	}
	if v := os.Getenv("IGLOO_TEST_LARGE_SORT_ROWS"); v != "" {
		rows, _ = strconv.ParseInt(v, 10, 64)
	}
	if file := os.Getenv("igloo_large_sort_file"); file != "" {
		largeSortPeakChild(t, file, limit, threads, rows, os.Getenv("igloo_large_sort_budget"))
		return
	}
	if testing.Short() {
		t.Skip("sorts millions of rows")
	}
	vmHWM(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "rows.parquet")
	largeSortFile(t, file, rows)
	limitBytes := int64(1 << 30)
	if n, err := config.ParseSize(limit); err == nil && n > 0 {
		limitBytes = n
	}
	measure := func(budget string) (peak int64, took time.Duration, out string) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLargeSortPeak$", "-test.v", "-test.timeout=30m")
		cmd.Env = append(os.Environ(), "igloo_large_sort_file="+file, "igloo_large_sort_budget="+budget,
			"GOMEMLIMIT=", "GOGC=100")
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var errOut strings.Builder
		cmd.Stderr = &errOut
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		var all strings.Builder
		sc := bufio.NewScanner(stdout)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(&all, line)
			if v, ok := strings.CutPrefix(strings.TrimSpace(line), "largesort: "); ok {
				var secs float64
				if _, err := fmt.Sscanf(v, "peak=%d seconds=%f", &peak, &secs); err != nil {
					t.Fatalf("%q: %v", v, err)
				}
				took = time.Duration(secs * float64(time.Second))
			}
		}
		if err := cmd.Wait(); err != nil {
			t.Fatalf("%v\n%s\n%s", err, all.String(), errOut.String())
		}
		if peak == 0 {
			t.Fatalf("no measure\n%s\n%s", all.String(), errOut.String())
		}
		return peak, took, all.String()
	}
	inRuns, tookRuns, traced := measure("")
	if os.Getenv("IGLOO_TEST_LARGE_SORT_TRACE") != "" {
		t.Log(traced)
	}
	t.Logf("%d rows under a limit of %s: in runs %.2f GiB in %s", rows, limit, float64(inRuns)/(1<<30), tookRuns.Round(time.Millisecond))
	if measured {
		atOnce, tookOnce, traced := measure("-1")
		if os.Getenv("IGLOO_TEST_LARGE_SORT_TRACE") != "" {
			t.Log(traced)
		}
		t.Logf("at once %.2f GiB in %s", float64(atOnce)/(1<<30), tookOnce.Round(time.Millisecond))
	}
	// The limit, and what the process holds besides: Go's, the merge's.
	if most := limitBytes + limitBytes/8; inRuns > most {
		t.Errorf("in runs, the process peaked at %.2f GiB, past %.2f GiB under a limit of %s", float64(inRuns)/(1<<30), float64(most)/(1<<30), limit)
	}
}

// largeSortPeakChild runs the statement in a process of its own, and prints
// what it took.
func largeSortPeakChild(t *testing.T, file, limit string, threads int, rows int64, budget string) {
	spill := t.TempDir()
	var sortBudget int64
	if budget != "" {
		sortBudget, _ = strconv.ParseInt(budget, 10, 64)
	}
	eng, err := engine.Open(context.Background(), config.Config{
		DuckDB:  config.DuckDB{TempDir: spill, MemoryLimit: limit, Threads: threads, ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")},
		Storage: config.Storage{AllowedLocations: []string{filepath.Dir(file) + "/"}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	logs := io.Discard
	trace := os.Getenv("IGLOO_TEST_LARGE_SORT_TRACE") != ""
	if trace {
		logs = os.Stdout
	}
	metrics := NewMetrics(prometheus.NewRegistry())
	ts := startServerLogged(t, eng, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), func(o *Options) {
		o.SortSpillBudget = sortBudget
		o.Metrics = metrics
	})
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	start := time.Now()
	if trace {
		go func() {
			for {
				time.Sleep(time.Second)
				data, _ := os.ReadFile("/proc/self/status")
				rss := regexp.MustCompile(`VmRSS:\s+(\d+) kB`).FindSubmatch(data)
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				if rss != nil {
					kb, _ := strconv.ParseInt(string(rss[1]), 10, 64)
					fmt.Printf("trace t=%.0f rss=%.2f gosys=%.2f goheap=%.2f\n", time.Since(start).Seconds(), float64(kb)/(1<<20), float64(ms.Sys)/(1<<30), float64(ms.HeapAlloc)/(1<<30))
				}
			}
		}()
	}
	rs, err := conn.Query(context.Background(), "SELECT id, ts, k, secs FROM read_parquet('"+file+"') ORDER BY id, ts")
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for rs.Next() {
		n++
	}
	rs.Close()
	if err := rs.Err(); err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("%d rows, want %d", n, rows)
	}
	if got := testutil.ToFloat64(metrics.largeSorts); (got == 1) != (sortBudget >= 0) {
		t.Fatalf("sorted in runs %v times, with a budget of %s", got, budget)
	}
	fmt.Printf("largesort: peak=%d seconds=%f\n", vmHWM(t), time.Since(start).Seconds())
}
