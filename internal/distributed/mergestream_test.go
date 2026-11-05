package distributed

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestMergeReader merges sorted files a batch at a time, in order, every
// row once, with columns copied value by value and, for a type it does not
// copy so (a list), by slices of the files' batches; files alone and in
// groups merged at once; and stops at once, its goroutines with it, when
// released before the end.
func TestMergeReader(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	dir := t.TempDir()
	const files, perFile = 7, 30_000
	var paths []string
	for f := range files {
		path := filepath.Join(dir, fmt.Sprintf("%d.parquet", f))
		// Keys interleave across the files; ties on k break by ts.
		if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT (i * %[1]d + %[2]d) %% 5000 AS k, TIMESTAMP '2024-01-01' + to_seconds(i * %[1]d + %[2]d) AS ts,
			CASE WHEN i %% 11 = 0 THEN NULL ELSE 'v' || (i * %[1]d + %[2]d) END AS s, CAST(i AS DECIMAL(38, 0)) AS d, [i, %[2]d] AS l
			FROM range(%[3]d) t(i) ORDER BY k, ts) TO '%[4]s' (FORMAT parquet, ROW_GROUP_SIZE 4096)`, files, f, perFile, path)); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	keys := []resolvedKey{{col: 0}, {col: 1}}
	defer func(g int) { mergeGroup = g }(mergeGroup)
	for _, tc := range []struct {
		name    string
		columns string
		sliced  bool
		group   int
	}{
		{"values", "k, ts, s, d", false, 8},
		{"slices", "k, ts, s, d, l", true, 8},
		{"values in groups", "k, ts, s, d", false, 2},
		{"slices in groups", "k, ts, s, d, l", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mergeGroup = tc.group
			var in []string
			for i, p := range paths {
				out := filepath.Join(dir, fmt.Sprintf("%s-%d.parquet", strings.ReplaceAll(tc.name, " ", "-"), i))
				if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT %s FROM '%s') TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 4096)`, tc.columns, p, out)); err != nil {
					t.Fatal(err)
				}
				in = append(in, out)
			}
			// Released after a batch, it stops at once.
			early, err := newMergeReader(context.Background(), in, keys)
			if err != nil {
				t.Fatal(err)
			}
			if !early.Next() {
				t.Fatalf("no batch: %v", early.Err())
			}
			released := make(chan struct{})
			go func() {
				early.Release()
				close(released)
			}()
			select {
			case <-released:
			case <-time.After(10 * time.Second):
				t.Fatal("released after a batch, the merge did not stop")
			}
			r, err := newMergeReader(context.Background(), in, keys)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Release()
			if sliced := r.b == nil; sliced != tc.sliced {
				t.Fatalf("copied by slices %v, want %v", sliced, tc.sliced)
			}
			rows, batches := 0, 0
			var lastK int64 = -1
			var lastTS int64
			for r.Next() {
				rec := r.RecordBatch()
				if batches++; rec.NumRows() > mergeBatch {
					t.Fatalf("a batch of %d rows", rec.NumRows())
				}
				k, kok := rec.Column(0).(*array.Int64)
				ts, tok := rec.Column(1).(*array.Timestamp)
				if !kok || !tok {
					t.Fatalf("columns of %s and %s", rec.Column(0).DataType(), rec.Column(1).DataType())
				}
				for i := range int(rec.NumRows()) {
					kv, tv := k.Value(i), int64(ts.Value(i))
					if kv < lastK || kv == lastK && tv < lastTS {
						t.Fatalf("row %d out of order: (%d, %d) after (%d, %d)", rows+i, kv, tv, lastK, lastTS)
					}
					lastK, lastTS = kv, tv
				}
				rows += int(rec.NumRows())
			}
			if err := r.Err(); err != nil {
				t.Fatal(err)
			}
			if rows != files*perFile || batches < rows/mergeBatch {
				t.Fatalf("%d rows in %d batches, want %d rows", rows, batches, files*perFile)
			}
		})
	}
}

// TestStreamedMerge runs statements ordered by ORDER BY without LIMIT with
// the merge of the stage's sorted rows read as the statement reads them,
// through MergedFunction under the statement's context, with several of
// DuckDB's threads: the rows come in the order of one machine's, every one;
// every row of a table sorted among them, which workers sort in shares. A
// statement that calls the function in no execution's context reads
// nothing.
func TestStreamedMerge(t *testing.T) {
	h := behaviorsHarness(t)
	h.co.opts.Cores = mergeCores // enough to merge on (see mergesKeepUp)
	ctx := context.Background()
	if err := RegisterMerged(h.session); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.query(ctx, "SELECT * FROM "+MergedFunction+"()"); err == nil || !strings.Contains(err.Error(), errNoMerge.Error()) {
		t.Fatalf("a user's call: %v", err)
	}
	h.exec("SET threads = 4")
	defer h.exec("RESET threads")
	for _, q := range []string{
		`SELECT bvalue, count(*) AS n FROM behaviors4 GROUP BY 1 ORDER BY n DESC, bvalue`,
		`SELECT person, day, bvalue FROM behaviors4 QUALIFY row_number() OVER (PARTITION BY person ORDER BY day DESC, bvalue) = 1 ORDER BY person DESC`,
		// Every row of a table, sorted, as workers now read it.
		`SELECT person, day, bvalue, amount FROM behaviors4 ORDER BY person, day DESC, bvalue, amount`,
		`SELECT * FROM behaviors4 WHERE amount > 10 ORDER BY ALL`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			want, _, err := h.query(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			plan.StreamMerged()
			ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
			if err != nil {
				t.Fatal(err)
			}
			defer ex.Close()
			if len(ex.Merged()) != 1 || strings.Contains(ex.SQL, "ORDER BY") {
				t.Fatalf("%d merges read as views: %s\n%s", len(ex.Merged()), ex.SQL, plan.describe())
			}
			// Outside the statement's context, the function reads nothing.
			if _, _, err := h.query(ctx, ex.SQL); err == nil || !strings.Contains(err.Error(), errNoMerge.Error()) {
				t.Fatalf("read outside the statement's context: %v", err)
			}
			got, _, err := h.query(WithMerged(ctx, ex.Merged()), ex.SQL)
			if err != nil {
				t.Fatalf("%v\n%s", err, ex.SQL)
			}
			if msg := compareRows(want, got, true); msg != "" {
				t.Errorf("%d rows: %s\n%s", len(want), msg, ex.SQL)
			}
		})
	}
}

// TestMergedFunctionTypes reads, through MergedFunction, a merge of files
// of every type it returns, copied into DuckDB's chunks a column at a time,
// NULLs among them, text short enough to keep in DuckDB's vectors and
// longer: every value is the one DuckDB reads from the files.
func TestMergedFunctionTypes(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() //nolint:errcheck
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if err := RegisterMerged(conn); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	columns := [][2]string{
		{"ti", "CAST(i % 100 AS TINYINT)"}, {"si", "CAST(i AS SMALLINT)"}, {"ii", "CAST(i * 7 AS INTEGER)"}, {"bi", "i * 1000003"},
		{"uti", "CAST(i % 200 AS UTINYINT)"}, {"usi", "CAST(i AS USMALLINT)"}, {"ui", "CAST(i AS UINTEGER)"}, {"ubi", "CAST(i AS UBIGINT)"},
		{"f", "CAST(i / 3 AS FLOAT)"}, {"d", "i / 7"}, {"b", "i % 2 = 0"},
		{"short", "'v' || i"}, {"long", "repeat('long text ', 2) || i"}, {"bl", "CAST('blob' || i AS BLOB)"},
		{"dt", "DATE '2020-01-01' + CAST(i AS INTEGER)"}, {"ts", "TIMESTAMP '2020-01-01 00:00:00.123456' + to_seconds(i)"},
		{"tms", "CAST(TIMESTAMP '2020-01-01 00:00:00.123' + to_seconds(i) AS TIMESTAMP_MS)"},
		{"tsec", "CAST(TIMESTAMP '2020-01-01' + to_seconds(i) AS TIMESTAMP_S)"},
		{"tns", "CAST(TIMESTAMP '2020-01-01 00:00:00.123456' + to_seconds(i) AS TIMESTAMP_NS)"},
		{"tz", "CAST(TIMESTAMP '2020-01-01 00:00:00.5' + to_seconds(i) AS TIMESTAMPTZ)"},
		{"tm", "TIME '01:02:03.456789' + to_seconds(i % 3600)"},
		{"d4", "CAST((i % 9000) / 10 AS DECIMAL(4, 1))"}, {"d9", "CAST(i * 3.25 AS DECIMAL(9, 2))"},
		{"d18", "CAST(i * -7.125 AS DECIMAL(18, 3))"}, {"d38", "CAST(i * 123456789.12345 - 1e15 AS DECIMAL(38, 5))"},
	}
	// Every column but the key NULL in a row of seven.
	selects := []string{"i AS k"}
	for _, c := range columns {
		selects = append(selects, "CASE WHEN i % 7 = 0 THEN NULL ELSE "+c[1]+" END AS "+c[0])
	}
	var files []string
	for f := range 3 {
		path := filepath.Join(dir, fmt.Sprintf("%d.parquet", f))
		q := fmt.Sprintf("COPY (SELECT %s FROM range(%d, 30000, 3) t(i) ORDER BY i) TO '%s' (FORMAT parquet)", strings.Join(selects, ", "), f, path)
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}
	read := func(ctx context.Context, from string) [][]string {
		t.Helper()
		rows, err := conn.QueryContext(ctx, "SELECT CAST(COLUMNS(*) AS VARCHAR) FROM "+from)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close() //nolint:errcheck
		cols, _ := rows.Columns()
		var out [][]string
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			row := make([]string, len(cols))
			for i, v := range vals {
				row[i] = "NULL"
				if v.Valid {
					row[i] = v.String
				}
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	list := ""
	for i, f := range files {
		if i > 0 {
			list += ", "
		}
		list += quoteLiteral(f)
	}
	want := read(context.Background(), "(SELECT * FROM read_parquet(["+list+"]) ORDER BY k)")
	r, err := newMergeReader(context.Background(), files, []resolvedKey{{col: 0}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	if _, _, err := mergedColumns(r.Schema()); err != nil {
		t.Fatal(err)
	}
	got := read(WithMerged(context.Background(), []*MergedView{{Reader: r}}), MergedFunction+"()")
	if len(got) != len(want) || len(want) != 30000 {
		t.Fatalf("%d rows, want %d of 30000", len(got), len(want))
	}
	for i := range want {
		if strings.Join(got[i], "|") != strings.Join(want[i], "|") {
			t.Fatalf("row %d:\n%v\nwant\n%v", i, got[i], want[i])
		}
	}
}
