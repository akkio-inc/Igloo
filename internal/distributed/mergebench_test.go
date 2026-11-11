package distributed

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// mergeBenchFiles writes files sorted parts of rows shaped like a report's
// detail rows ordered by person and time, as the tasks of a stage sorting
// every row of a table write them: a 36-character ID, a timestamp, a short
// key and a decimal; and returns them.
func mergeBenchFiles(tb testing.TB, db *sql.DB, dir string, files, perFile int) []string {
	tb.Helper()
	var paths []string
	for f := range files {
		path := filepath.Join(dir, fmt.Sprintf("part-%d.parquet", f))
		if _, err := db.Exec(fmt.Sprintf(`COPY (
			SELECT md5(((i * %[1]d + %[2]d) %% 100003)::VARCHAR)[1:8] || '-' || md5(((i * %[1]d + %[2]d) %% 100003)::VARCHAR)[9:12] || '-' ||
			       md5(((i * %[1]d + %[2]d) %% 100003)::VARCHAR)[13:16] || '-' || md5(((i * %[1]d + %[2]d) %% 100003)::VARCHAR)[17:20] || '-' ||
			       md5(((i * %[1]d + %[2]d) %% 100003)::VARCHAR)[21:32] AS id,
			       TIMESTAMP '2024-11-13' + to_seconds((hash(i * %[1]d + %[2]d) %% 7603200)::BIGINT) AS ts,
			       (hash(i * 7 + %[2]d) %% 32893579)::VARCHAR AS k,
			       (hash(i * 13 + %[2]d) %% 6146)::DECIMAL(38, 0) AS secs
			FROM range(%[3]d) t(i) ORDER BY id, ts) TO '%[4]s' (FORMAT parquet, ROW_GROUP_SIZE 122880)`, files, f, perFile, path)); err != nil {
			tb.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

// mergeBenchKeys orders mergeBenchFiles' rows: by ID, then time.
var mergeBenchKeys = []resolvedKey{{col: 0}, {col: 1}}

// drainRows reads every row of a statement through the driver, as a
// session sends a client the rows of a result DuckDB streams, under ctx.
func drainRows(ctx context.Context, conn *sql.Conn, query string) (int, error) {
	n := 0
	err := conn.Raw(func(dc any) error {
		c := dc.(*duckdb.Conn)
		ps, err := c.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		defer ps.Close() //nolint:errcheck
		rows, err := ps.(*duckdb.Stmt).QueryStreamContext(ctx, nil)
		if err != nil {
			return err
		}
		defer rows.Close() //nolint:errcheck
		dest := make([]driver.Value, len(rows.Columns()))
		for {
			err := rows.Next(dest)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			n++
		}
	})
	return n, err
}

// mergedQuery reads mergeBenchFiles' merge as the coordinator's part of a
// statement does: its columns cast to their types (see partials).
const mergedQuery = `SELECT * FROM (SELECT CAST(id AS VARCHAR) AS id, CAST(ts AS TIMESTAMP) AS ts, CAST(k AS VARCHAR) AS k,
	CAST(secs AS DECIMAL(38, 0)) AS secs FROM ` + MergedFunction + `()) AS __igloo_p`

// orderedRead is one way of reading mergeBenchFiles' rows in order, and the
// rows it read.
type orderedRead func(ctx context.Context) (int, error)

// mergeRuns are the ways to read files' rows in order: merged alone, merged
// and read through DuckDB as a statement reads it, and sorted by DuckDB
// itself, as one machine sorts them.
func mergeRuns(tb testing.TB, files []string, conn *sql.Conn) map[string]orderedRead {
	return map[string]orderedRead{
		"merge": func(ctx context.Context) (int, error) {
			r, err := newMergeReader(ctx, files, mergeBenchKeys)
			if err != nil {
				return 0, err
			}
			defer r.Release()
			n := 0
			for r.Next() {
				n += int(r.RecordBatch().NumRows())
			}
			return n, r.Err()
		},
		"function": func(ctx context.Context) (int, error) {
			// The merge copied into DuckDB, and only counted there.
			r, err := newMergeReader(ctx, files, mergeBenchKeys)
			if err != nil {
				return 0, err
			}
			defer r.Release()
			var n int
			err = conn.QueryRowContext(WithMerged(ctx, []*MergedView{{Reader: r}}),
				"SELECT count(id) + count(ts) + count(k) + count(secs) - 3 * count(*) FROM "+MergedFunction+"()").Scan(&n)
			return n, err
		},
		"merged": func(ctx context.Context) (int, error) {
			r, err := newMergeReader(ctx, files, mergeBenchKeys)
			if err != nil {
				return 0, err
			}
			defer r.Release()
			return drainRows(WithMerged(ctx, []*MergedView{{Reader: r}}), conn, mergedQuery)
		},
		"table": func(ctx context.Context) (int, error) {
			// Rows already sorted, read from a table in memory: what
			// reading them costs whatever orders them.
			return drainRows(ctx, conn, "SELECT * FROM merge_bench_sorted")
		},
		"sorted": func(ctx context.Context) (int, error) {
			list := ""
			for i, f := range files {
				if i > 0 {
					list += ", "
				}
				list += quoteLiteral(f)
			}
			return drainRows(ctx, conn, "SELECT * FROM read_parquet(["+list+"]) ORDER BY id, ts")
		},
	}
}

// mergeBench sets up files of rows to merge, and a session that reads them.
func mergeBench(tb testing.TB, files, perFile int) ([]string, *sql.Conn) {
	tb.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() }) //nolint:errcheck
	paths := mergeBenchFiles(tb, db, tb.TempDir(), files, perFile)
	conn, err := db.Conn(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() }) //nolint:errcheck
	if err := RegisterMerged(conn); err != nil {
		tb.Fatal(err)
	}
	list := ""
	for i, f := range paths {
		if i > 0 {
			list += ", "
		}
		list += quoteLiteral(f)
	}
	if _, err := conn.ExecContext(context.Background(), "CREATE TABLE merge_bench_sorted AS SELECT * FROM read_parquet(["+list+"]) ORDER BY id, ts"); err != nil {
		tb.Fatal(err)
	}
	return paths, conn
}

// BenchmarkMergedRows reads the rows of 32 sorted files of 125,000 rows
// each in order: merged alone; merged into DuckDB and counted there;
// merged and read through DuckDB as a statement's coordinator part reads
// them; read from a table that holds them sorted, what reading them costs
// whatever orders them; and sorted by DuckDB itself, as one machine does.
func BenchmarkMergedRows(b *testing.B) {
	const files, perFile = 32, 125_000
	paths, conn := mergeBench(b, files, perFile)
	for _, name := range []string{"merge", "function", "merged", "table", "sorted"} {
		run := mergeRuns(b, paths, conn)[name]
		b.Run(name, func(b *testing.B) {
			for b.Loop() {
				n, err := run(context.Background())
				if err != nil || n != files*perFile {
					b.Fatalf("%d rows, %v", n, err)
				}
			}
			b.ReportMetric(float64(files*perFile)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}

// TestMergedRowsKeepUp checks that reading sorted files' rows merged, through
// DuckDB as a statement's coordinator part reads them, takes about as long
// as DuckDB sorting the same rows itself, as one machine does, and at most a
// quarter longer, for a runner busy with other work: the coordinator merging
// workers' sorted rows here took three times as long, and longer than one
// machine at scale. Each is timed at its best of three, the two
// interleaved. It times the way the coordinator reads them on this machine's
// cores (see mergesKeepUp): merged where the merge keeps up, and otherwise
// sorted by DuckDB, which on CI's 4 vCPUs took four fifths of the merge's
// time, and on 2 cores two thirds.
func TestMergedRowsKeepUp(t *testing.T) {
	if testing.Short() {
		t.Skip("reads four million rows six times")
	}
	const files, perFile = 32, 125_000
	paths, conn := mergeBench(t, files, perFile)
	runs := mergeRuns(t, paths, conn)
	var threads int
	if err := conn.QueryRowContext(context.Background(), "SELECT current_setting('threads')").Scan(&threads); err != nil {
		t.Fatal(err)
	}
	way := "sorted"
	if mergesKeepUp(0, threads) {
		way = "merged"
	}
	runs["read"] = runs[way]
	best := map[string]time.Duration{}
	for range 3 {
		for _, name := range []string{"read", "sorted"} {
			start := time.Now()
			n, err := runs[name](context.Background())
			took := time.Since(start)
			if err != nil || n != files*perFile {
				t.Fatalf("%s: %d rows, %v", name, n, err)
			}
			if b, ok := best[name]; !ok || took < b {
				best[name] = took
			}
		}
	}
	t.Logf("%d rows on %d cores and %d threads: %s %s, sorted by DuckDB %s", files*perFile, runtime.GOMAXPROCS(0), threads, way, best["read"], best["sorted"])
	if best["read"] > best["sorted"]*5/4 {
		t.Errorf("rows read %s took %s, over a quarter longer than DuckDB's sort of them, %s", way, best["read"], best["sorted"])
	}
}

// TestMergesKeepUp checks the cores and DuckDB threads the coordinator merges
// sorted rows on: both MergeCores or more.
func TestMergesKeepUp(t *testing.T) {
	for _, c := range []struct {
		cores, threads int
		want           bool
	}{
		{1, 0, false}, {2, 0, false}, {4, 0, false}, {4, 8, false}, {8, 0, true}, {8, 4, false}, {16, 32, true}, {16, 8, true},
	} {
		if got := mergesKeepUp(c.cores, c.threads); got != c.want {
			t.Errorf("%d cores, %d threads: %v", c.cores, c.threads, got)
		}
	}
}
