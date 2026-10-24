package pgwire

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/tasks"
)

// TestIDFillKeepsFileOrder maps a table laid out by a column, in several
// files, as one laid out by segment is, and checks that its files come out
// with the same rows each, in the same ranges of that column: the fill
// writes each file again in its rows' order rather than updating the rows.
func TestIDFillKeepsFileOrder(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	r := vc.replica(t, func(o *Options) {
		o.IDDomainsOn = true
		o.Tasks = tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
		o.idFillRewriteRows = 1
	})
	it := &idTest{t: t, vc: vc, r: r, admin: r.admin(t, pgx.QueryExecModeSimpleProtocol)}
	it.exec(
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.members (segment VARCHAR, person_id VARCHAR, n INTEGER)",
	)
	// One file a write, each of a range of segments, every person in many.
	for f := range 6 {
		it.exec(fmt.Sprintf("INSERT INTO lake.public.members SELECT 'seg' || lpad(CAST(%d * 10 + i // 400 AS VARCHAR), 3, '0'), "+
			"'p' || ((i * 7919 + %d) %% 1500), CAST(i AS INTEGER) FROM range(4000) r(i) ORDER BY 1, 3", f, f))
	}
	ctx := context.Background()
	srv := r.srv
	// files are the table's data files: rows, and the segment's range.
	files := func() []string {
		t.Helper()
		tbl, err := srv.ddl.LoadTable(ctx, "public", "members")
		if err != nil {
			t.Fatal(err)
		}
		field, ok := tbl.Schema().FindFieldByNameCaseInsensitive("segment")
		if !ok {
			t.Fatal("no segment column")
		}
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, task := range tasks {
			df := task.File
			out = append(out, fmt.Sprintf("%d rows %s..%s", df.Count(), df.LowerBoundValues()[field.ID], df.UpperBoundValues()[field.ID]))
		}
		slices.Sort(out)
		return out
	}
	// sorted checks that each data file holds its rows in (segment, n)
	// order, as they were written.
	sorted := func() {
		t.Helper()
		tbl, err := srv.ddl.LoadTable(ctx, "public", "members")
		if err != nil {
			t.Fatal(err)
		}
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range tasks {
			path := task.File.FilePath()
			got := it.one("SELECT count(*) FROM (SELECT segment, n, lag(segment) OVER w AS s0, lag(n) OVER w AS n0 FROM read_parquet(" +
				quoteLiteral(path) + ", file_row_number = true) WINDOW w AS (ORDER BY file_row_number)) WHERE segment < s0 OR segment = s0 AND n < n0")
			if got != "0" {
				t.Errorf("%s: %s rows out of order", path, got)
			}
		}
	}
	before := files()
	if len(before) != 6 {
		t.Fatalf("expected 6 files, got %q", before)
	}
	sorted()

	it.exec(
		"CREATE TAG pii",
		"ALTER TABLE lake.public.members ALTER COLUMN person_id SET TAG pii = 'person_id'",
		"CREATE ID DOMAIN people (VARCHAR) TAG pii = 'person_id'",
		"ALTER TABLE lake.public.members ALTER COLUMN person_id SET ID DOMAIN people",
	)
	srv.MapIDDomains(ctx)
	it.consistent("people", true, "members")
	if after := files(); !slices.Equal(after, before) {
		t.Errorf("the files changed:\nbefore %q\nafter  %q", before, after)
	}
	// And each file's rows come in the order they were written.
	sorted()
	// Rows a later write leaves without ids are filled the same way.
	it.internal("INSERT INTO lake.public.members (segment, person_id, n) SELECT 'seg900', 'q' || i, CAST(i AS INTEGER) FROM range(100) r(i)")
	srv.MapIDDomains(ctx)
	it.consistent("people", true, "members")
	if n := len(files()); n != 7 {
		t.Errorf("after an append and its fill, %d files, want 7", n)
	}
}
