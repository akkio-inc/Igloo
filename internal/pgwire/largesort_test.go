package pgwire

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
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

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
// in the spill directory; by expressions, with parameters, and of types the
// merge does not compare or pass on. A statement that would not fit, but that
// keeps few rows, sorts at once, as does one that fits.
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

	// Orders and columns the merge cannot take as they are, compared with one
	// machine's sort over all of each row: by an expression, by a position of
	// one, and of types it does not compare or pass on. The last column breaks
	// ties, and the sort of the rows holds it.
	const rich = "id, n, [n, NULL, n + 1] AS l, {'a': k, 'b': [id]} AS s, (n * 1000000000000)::HUGEINT AS h, " +
		"md5(n::VARCHAR)::UUID AS u, to_seconds(n % 1000) AS iv"
	smaller := filepath.Join(dir, "smaller.parquet")
	largeSortFile(t, smaller, 1_000_000)
	for _, order := range []string{
		"upper(k) DESC NULLS FIRST, n",
		"8, n",
		"u DESC",
		"h, n",
		"l DESC, n",
	} {
		query := "SELECT " + rich + ", upper(k) AS uk FROM read_parquet('" + smaller + "') ORDER BY " + order
		t.Run(order, func(t *testing.T) {
			conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
			before := sorts()
			got := largeSortText(t, conn, query)
			if sorts()-before != 1 {
				t.Errorf("sorted in runs %v times, want once", sorts()-before)
			}
			mustExec(t, conn, "SET distributed_execution = off")
			text := strings.Replace(query, "SELECT ", "SELECT /* at once */ ", 1)
			ts.srv.sortBudget = -1
			want := largeSortText(t, conn, text)
			ts.srv.sortBudget = 0
			if len(got) != len(want) {
				t.Fatalf("%d rows, want %d", len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("row %d:\n%s\nat once:\n%s", i, got[i], want[i])
				}
			}
		})
	}

	// A statement with parameters, as many as its protocol sends.
	params := ts.admin(t, pgx.QueryExecModeExec)
	before := sorts()
	var rowCount int64
	rows, err := params.Query(context.Background(), "SELECT id, ts, k, secs, n, n + $1::BIGINT AS m FROM "+from+" ORDER BY upper(id), id, n", int64(5))
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
	if rowCount != 2_000_000 || sorts()-before != 1 {
		t.Errorf("with a parameter: %d rows, sorted in runs %v times", rowCount, sorts()-before)
	}

	// A client that reads a few rows at a time, running another statement on
	// the session between its reads, as JDBC's fetch size has it do.
	h, err := ts.admin(t, pgx.QueryExecModeSimpleProtocol).PgConn().Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer h.Conn.Close() //nolint:errcheck
	_ = h.Conn.SetDeadline(time.Now().Add(10 * time.Minute))
	fe := h.Frontend
	receive := func(until string) (rows int, first string) {
		t.Helper()
		for {
			msg, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := msg.(type) {
			case *pgproto3.DataRow:
				if rows++; rows == 1 {
					first = string(m.Values[len(m.Values)-1])
				}
			case *pgproto3.ErrorResponse:
				t.Fatal(m.Message)
			}
			if fmt.Sprintf("%T", msg) == "*pgproto3."+until {
				return rows, first
			}
		}
	}
	send := func(msgs ...pgproto3.FrontendMessage) {
		t.Helper()
		for _, m := range msgs {
			fe.Send(m)
		}
		if err := fe.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	send(&pgproto3.Query{String: "BEGIN"})
	receive("ReadyForQuery")
	before = sorts()
	send(&pgproto3.Parse{Name: "big", Query: "SELECT id, ts, k, secs, n FROM " + from + " ORDER BY n DESC"},
		&pgproto3.Bind{PreparedStatement: "big", DestinationPortal: "p"},
		&pgproto3.Execute{Portal: "p", MaxRows: 10},
		&pgproto3.Flush{})
	got, first := receive("PortalSuspended")
	if got != 10 || first != "1999999" {
		t.Fatalf("the portal's first read: %d rows, from %s", got, first)
	}
	send(&pgproto3.Parse{Query: "SELECT 42"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Flush{})
	if n, v := receive("CommandComplete"); n != 1 || v != "42" {
		t.Fatalf("the statement between its reads: %d rows, %s", n, v)
	}
	send(&pgproto3.Execute{Portal: "p", MaxRows: 5}, &pgproto3.Flush{})
	if got, first = receive("PortalSuspended"); got != 5 || first != "1999989" {
		t.Fatalf("the portal's second read: %d rows, from %s", got, first)
	}
	send(&pgproto3.Execute{Portal: "p"}, &pgproto3.Sync{})
	got, _ = receive("CommandComplete")
	receive("ReadyForQuery")
	// It counts once it has completed, after what the client sees.
	for range 100 {
		if sorts()-before == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got != 2_000_000-15 || sorts()-before != 1 {
		t.Errorf("the portal's last read: %d rows, sorted in runs %v times", got, sorts()-before)
	}
	send(&pgproto3.Query{String: "COMMIT"})
	receive("ReadyForQuery")
	if left := largeSortDirs(t, spill); len(left) > 0 {
		t.Errorf("left in the spill directory: %v", left)
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

// TestLargeSortFalseInterrupt checks a read in runs whose merge fails as it
// streams, DuckDB reporting an interrupt when nothing asked for one. Before a
// row went out it sorts at once, as a merge cannot be read twice, and the
// client gets every row; after, the client gets an internal error.
func TestLargeSortFalseInterrupt(t *testing.T) {
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
	conn := ts.admin(t, pgx.QueryExecModeSimpleProtocol)
	query := "SELECT id, ts, k, secs, n AS sort_stream FROM read_parquet('" + file + "') ORDER BY sort_stream"

	interruptStreams(t, "sort_stream", 0, 1)
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	n := int64(0)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatal(err)
		}
		if vals[4].(int64) != n {
			t.Fatalf("row %d: %v", n, vals)
		}
		n++
	}
	if err := rows.Err(); err != nil || n != 2_000_000 {
		t.Fatalf("after %d rows: %v, want all of them", n, err)
	}
	if got := testutil.ToFloat64(metrics.largeSorts); got != 0 {
		t.Errorf("counted %v reads in runs that ended sorted at once", got)
	}

	interruptStreams(t, "sort_stream", 1000, 1)
	sent, err := drainRows(conn, query)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "XX000" || !strings.Contains(pgErr.Message, "was not canceled") || sent < 1000 {
		t.Fatalf("after %d rows: %v, want the internal error", sent, err)
	}
	if left := largeSortDirs(t, spill); len(left) > 0 {
		t.Errorf("left in the spill directory: %v", left)
	}
}

// sortedArrow is an ArrowWriter that checks a result's last column ascends, and
// returns stop once it has read a batch, if set.
type sortedArrow struct {
	t       *testing.T
	calls   int
	rows    int64
	last    int64
	stop    error
	ordered bool
}

func (a *sortedArrow) Columns([]Column) error { return nil }
func (a *sortedArrow) Row([]any) error        { return nil }
func (a *sortedArrow) Complete(string) error  { return nil }
func (a *sortedArrow) Records(rdr array.RecordReader) (int64, error) {
	a.calls++
	for rdr.Next() {
		rec := rdr.RecordBatch()
		col := rec.Column(int(rec.NumCols()) - 1).(*array.Int64)
		for i := range col.Len() {
			if a.rows > 0 && col.Value(i) < a.last {
				a.t.Errorf("row %d: %d after %d", a.rows, col.Value(i), a.last)
			}
			a.last = col.Value(i)
			a.rows++
		}
		if a.stop != nil {
			return a.rows, a.stop
		}
	}
	return a.rows, rdr.Err()
}

// TestLargeSortOverArrow reads rows in order larger than memory as Arrow record
// batches, as Flight SQL reads them: in runs, every row in order. A statement
// that fails once it has sent batches does not run again, which would send them
// twice.
func TestLargeSortOverArrow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	file := filepath.Join(dir, "rows.parquet")
	largeSortFile(t, file, 2_000_000)
	metrics := NewMetrics(prometheus.NewRegistry())
	ts := startServerWithConfig(t, config.Config{
		DuckDB:  config.DuckDB{TempDir: t.TempDir(), MemoryLimit: "400MiB", Threads: 4, ExtensionDir: os.Getenv("IGLOO_TEST_EXTENSION_DIR")},
		Storage: config.Storage{AllowedLocations: []string{dir + "/"}},
	}, func(o *Options) { o.Metrics = metrics })
	ctx := context.Background()
	user, err := ts.store.Get(ctx, "igloo")
	if err != nil {
		t.Fatal(err)
	}
	es, err := ts.srv.OpenEmbedded(ctx, user, "flight")
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close() //nolint:errcheck
	query := "SELECT id, ts, k, secs, n FROM read_parquet('" + file + "') ORDER BY n"
	rec := &sortedArrow{t: t}
	if err := es.Run(ctx, query, rec); err != nil {
		t.Fatal(err)
	}
	if rec.rows != 2_000_000 || rec.calls != 1 {
		t.Errorf("%d rows in %d calls, want 2000000 in one", rec.rows, rec.calls)
	}
	if got := testutil.ToFloat64(metrics.largeSorts); got != 1 {
		t.Errorf("sorted in runs %v times, want once", got)
	}

	stopped := &sortedArrow{t: t, stop: errors.New("the client went away")}
	if err := es.Run(ctx, query, stopped); err == nil {
		t.Error("no error from a writer that failed")
	}
	if stopped.calls != 1 || stopped.rows == 0 {
		t.Errorf("%d rows in %d calls, want some in one", stopped.rows, stopped.calls)
	}
}

// largeSortText reads query's rows as text.
func largeSortText(t *testing.T, conn *pgx.Conn, query string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, pgx.QueryResultFormats{pgx.TextFormatCode})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var b strings.Builder
		for _, v := range rows.RawValues() {
			b.Write(v)
			b.WriteByte('|')
		}
		out = append(out, b.String())
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
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
