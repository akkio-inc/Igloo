package distributed

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// TestMergeReader merges sorted files a batch at a time, in order, every
// row once, with columns copied value by value and, for a type it does not
// copy so (a list), by slices of the files' batches; and makes its first
// batch before it has read more than each file's first.
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
	for _, tc := range []struct {
		name    string
		columns string
		sliced  bool
	}{
		{"values", "k, ts, s, d", false},
		{"slices", "k, ts, s, d, l", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var in []string
			for i, p := range paths {
				out := filepath.Join(dir, fmt.Sprintf("%s-%d.parquet", tc.name, i))
				if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT %s FROM '%s') TO '%s' (FORMAT parquet, ROW_GROUP_SIZE 4096)`, tc.columns, p, out)); err != nil {
					t.Fatal(err)
				}
				in = append(in, out)
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
