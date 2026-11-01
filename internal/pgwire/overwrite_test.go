package pgwire

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	icecat "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/maintenance"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// liveFiles returns the content and path of each file of the current
// snapshot of table, a name of the lake catalog.
func liveFiles(t *testing.T, conn *pgx.Conn, table string) (data, deletes []string) {
	t.Helper()
	// DuckDB's iceberg_metadata calls a data file's content EXISTING, and a
	// delete file's POSITION_DELETES or EQUALITY_DELETES.
	for _, row := range texts(t, conn, "SELECT content, file_path FROM iceberg_metadata('lake.public."+table+"') WHERE status <> 'DELETED'") {
		content, path, _ := strings.Cut(row, "|")
		if strings.Contains(content, "DELETES") {
			deletes = append(deletes, path)
		} else {
			data = append(data, path)
		}
	}
	return data, deletes
}

// refreshDirs returns the refresh directories paths are in.
func refreshDirs(t *testing.T, paths []string) []string {
	t.Helper()
	var dirs []string
	for _, p := range paths {
		dir := filepath.Dir(p)
		if !strings.Contains(filepath.Base(dir), overwriteDir) {
			t.Errorf("%s is not in a refresh's directory", p)
			continue
		}
		if !slices.Contains(dirs, dir) {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

// filesIn counts the files in dir, none if it is gone.
func filesIn(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// filesNotes returns the notes of refreshes' files in schema public.
func filesNotes(t *testing.T, srv *Server) []catalog.Pending {
	t.Helper()
	notes, err := srv.ddl.PendingNotes(context.Background(), "public")
	if err != nil {
		t.Fatal(err)
	}
	var out []catalog.Pending
	for _, n := range notes {
		if n.Kind == catalog.PendingFiles {
			out = append(out, n)
		}
	}
	return out
}

// TestFullRefreshOverwrites checks that a full refresh puts the view's new
// rows in its table in one commit, without a delete file: of a projection
// and of an aggregate, whose state the next incremental refresh goes on
// from; that the files it replaced stay for older snapshots until snapshot
// expiry deletes them; and that it leaves no note behind.
func TestFullRefreshOverwrites(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	for _, q := range []string{
		"CREATE TABLE src AS SELECT i AS id, i % 7 AS g, i * 2 AS x FROM range(10000) AS r(i)",
		"CREATE MATERIALIZED VIEW proj AS SELECT id, x FROM src WHERE g < 5",
		"CREATE MATERIALIZED VIEW sums AS SELECT g, count(*) AS n, sum(x) AS s FROM src GROUP BY g",
		"REFRESH MATERIALIZED VIEW proj",
		"REFRESH MATERIALIZED VIEW sums",
	} {
		mustExec(t, admin, q)
	}
	// check checks the views, and, after a full refresh, returns the
	// directories of proj's files.
	check := func(step string, full bool) []string {
		t.Helper()
		for _, v := range []struct{ view, query string }{
			{"proj", "SELECT id, x FROM src WHERE g < 5"},
			{"sums", "SELECT g, count(*), sum(x) FROM src GROUP BY g"},
		} {
			if got, want := sortedTexts(t, admin, "SELECT * FROM "+v.view), sortedTexts(t, admin, v.query); !slices.Equal(got, want) {
				t.Errorf("%s: %s holds %d rows, want %d", step, v.view, len(got), len(want))
			}
		}
		data, deletes := liveFiles(t, admin, "proj")
		if len(deletes) > 0 || len(data) == 0 {
			t.Errorf("%s: proj has %d data files and %d delete files", step, len(data), len(deletes))
		}
		if n := len(filesNotes(t, env.srv)); n != 0 {
			t.Errorf("%s: %d notes of refreshes' files left", step, n)
		}
		if !full {
			return nil
		}
		return refreshDirs(t, data)
	}
	first := check("first refresh", true)
	if data, _ := liveFiles(t, admin, "sums"); len(refreshDirs(t, data)) != 1 {
		t.Errorf("sums' files: %q", data)
	}

	// A delete from the table: the projection refreshes in full, by
	// overwrite again; so does the aggregate, made to.
	mustExec(t, admin, "DELETE FROM src WHERE id % 3 = 0")
	env.srv.refreshFraction = 1e-9
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW proj")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	for _, v := range []string{"proj", "sums"} {
		if action, detail, _, _ := viewRefresh(t, admin, "public."+v); action != "FULL" {
			t.Errorf("%s after the delete: %s, %s", v, action, detail)
		}
	}
	second := check("after the delete", true)
	if slices.Equal(first, second) {
		t.Errorf("the second refresh wrote into the first's directory %q", first)
	}
	for _, dir := range first {
		if filesIn(t, dir) == 0 {
			t.Errorf("the files of %s went while an older snapshot holds them", dir)
		}
	}
	// The aggregate's state holds the full refresh's groups: the next
	// refresh is incremental. So is the projection's, an append.
	env.srv.refreshFraction = 0
	mustExec(t, admin, "INSERT INTO src VALUES (20000, 1, 5)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW sums")
	if action, detail, _, _ := viewRefresh(t, admin, "public.sums"); action != "INCREMENTAL" {
		t.Errorf("sums after an insert: %s, %s", action, detail)
	}
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW proj")
	check("after an insert", false)

	// Snapshot expiry deletes the replaced files; the current ones stay.
	time.Sleep(20 * time.Millisecond)
	m := maintenance.New(env.catalog, env.dir, maintenance.Options{Retention: 10 * time.Millisecond}, slog.New(slog.DiscardHandler))
	m.RunOnce(context.Background())
	for _, dir := range first {
		if n := filesIn(t, dir); n != 0 {
			t.Errorf("%d files of the replaced %s are left after snapshot expiry", n, dir)
		}
	}
	data, _ := liveFiles(t, admin, "proj")
	for _, p := range data {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("a current file of proj: %v", err)
		}
	}
	check("after snapshot expiry", false)
}

// TestFullRefreshReadersSeeOldOrNew checks that statements reading a view
// while full refreshes replace its rows read the rows of one refresh or the
// next, never none or a mix: on the replica that refreshes, and on another
// through its result cache, which a refresh invalidates once.
func TestFullRefreshReadersSeeOldOrNew(t *testing.T) {
	t.Parallel()
	vc := newViewCatalog(t)
	sched := tasks.NewScheduler(tasks.NewMemoryStore(), tasks.Options{})
	r1 := vc.replica(t, func(o *Options) { o.Tasks = sched })
	r1.srv.refreshFraction = 1e-9
	metrics := NewMetrics(prometheus.NewRegistry())
	admin := r1.admin(t, pgx.QueryExecModeSimpleProtocol)
	const batch = 20000
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		fmt.Sprintf("CREATE TABLE lake.public.events AS SELECT i AS id, i %% 10 AS x FROM range(%d) AS r(i)", batch),
		"CREATE MATERIALIZED VIEW lake.public.recent AS SELECT id, x FROM lake.public.events",
		"REFRESH MATERIALIZED VIEW lake.public.recent",
	} {
		mustExec(t, admin, q)
	}
	r2 := vc.replica(t, func(o *Options) {
		o.ResultCacheSize = 64 << 20
		o.Metrics = metrics
	})
	// What the view holds after k more batches: batches of ids k*batch on.
	const steps = 8
	valid := map[string]int{}
	for k := 0; k <= steps; k++ {
		n := int64(batch * (k + 1))
		var sum int64
		for i := int64(0); i < n; i++ {
			sum += i % 10
		}
		valid[fmt.Sprintf("%d|%d", n, sum)] = k
	}
	const q = "SELECT count(*), sum(x)::BIGINT FROM lake.public.recent"
	readers := []*pgx.Conn{r2.admin(t, pgx.QueryExecModeSimpleProtocol), r2.admin(t, pgx.QueryExecModeSimpleProtocol),
		r1.admin(t, pgx.QueryExecModeSimpleProtocol)}
	var (
		stop  atomic.Bool
		wg    sync.WaitGroup
		mu    sync.Mutex
		bad   []string
		reads atomic.Int64
	)
	for _, c := range readers {
		wg.Go(func() {
			for !stop.Load() {
				var n, sum int64
				if err := c.QueryRow(context.Background(), q).Scan(&n, &sum); err != nil {
					mu.Lock()
					bad = append(bad, err.Error())
					mu.Unlock()
					return
				}
				reads.Add(1)
				if _, ok := valid[fmt.Sprintf("%d|%d", n, sum)]; !ok {
					mu.Lock()
					bad = append(bad, fmt.Sprintf("%d rows summing to %d", n, sum))
					mu.Unlock()
				}
			}
		})
	}
	for k := 1; k <= steps; k++ {
		mustExec(t, admin, fmt.Sprintf("INSERT INTO lake.public.events SELECT i, i %% 10 FROM range(%d, %d) AS r(i)", k*batch, (k+1)*batch))
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW lake.public.recent")
		if action, detail, _, _ := viewRefresh(t, admin, "public.recent"); action != "FULL" {
			t.Fatalf("refresh %d: %s, %s", k, action, detail)
		}
	}
	stop.Store(true)
	wg.Wait()
	if len(bad) > 0 {
		t.Errorf("%d of %d reads saw neither one refresh's rows nor the next's, as %q", len(bad), reads.Load(), bad[:min(len(bad), 5)])
	}

	// The cache: a refresh's commit makes the next read miss, once.
	dash := r2.admin(t, pgx.QueryExecModeSimpleProtocol)
	outcome := func(o string) float64 { return testutil.ToFloat64(metrics.resultCache.WithLabelValues(o)) }
	texts(t, dash, q)
	texts(t, dash, q)
	hits, misses := outcome("hit"), outcome("missed")
	mustExec(t, admin, "INSERT INTO lake.public.events VALUES (999999, 7)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW lake.public.recent")
	want := texts(t, admin, "SELECT count(*), sum(x)::BIGINT FROM lake.public.events")
	for i := range 3 {
		if got := texts(t, dash, q); !slices.Equal(got, want) {
			t.Errorf("read %d after the refresh: %q, want %q", i, got, want)
		}
	}
	if m, h := outcome("missed")-misses, outcome("hit")-hits; m != 1 || h != 2 {
		t.Errorf("after the refresh: %v misses and %v hits, want 1 and 2", m, h)
	}
}

// TestFullRefreshInterrupted checks that a full refresh that fails, or is
// canceled, between writing its files and committing them, leaves the view
// as it was and deletes its files; and that files a stopped refresh left
// go at the view's next refresh, unless a snapshot holds them.
func TestFullRefreshInterrupted(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	for _, q := range []string{
		"CREATE TABLE src AS SELECT i AS id, i * 2 AS x FROM range(5000) AS r(i)",
		"CREATE MATERIALIZED VIEW proj AS SELECT id, x FROM src WHERE id % 2 = 0",
		"REFRESH MATERIALIZED VIEW proj",
	} {
		mustExec(t, admin, q)
	}
	env.srv.refreshFraction = 1e-9
	before := sortedTexts(t, admin, "SELECT * FROM proj")
	unchanged := func(step string) {
		t.Helper()
		if got := sortedTexts(t, admin, "SELECT * FROM proj"); !slices.Equal(got, before) {
			t.Errorf("%s: proj holds %d rows, want the %d it held", step, len(got), len(before))
		}
		if n := len(filesNotes(t, env.srv)); n != 0 {
			t.Errorf("%s: %d notes of refreshes' files left", step, n)
		}
	}

	// A failure.
	mustExec(t, admin, "INSERT INTO src VALUES (100000, 1)")
	var dir string
	env.srv.beforeOverwrite = func(_ context.Context, d string) error {
		dir = d
		if entries, err := os.ReadDir(d); err != nil || len(entries) == 0 {
			t.Errorf("COPY wrote nothing into %s: %v", d, err)
		}
		return errors.New("an injected failure")
	}
	if _, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW proj"); err == nil || !strings.Contains(err.Error(), "an injected failure") {
		t.Errorf("the refresh: %v", err)
	}
	if dir == "" || filesIn(t, dir) != 0 {
		t.Errorf("the failed refresh left files in %q", dir)
	}
	unchanged("after a failure")

	// A cancel, while the refresh waits to commit.
	waiting := make(chan string, 1)
	env.srv.beforeOverwrite = func(ctx context.Context, d string) error {
		waiting <- d
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return errors.New("never canceled")
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW proj")
		done <- err
	}()
	dir = <-waiting
	if err := admin.PgConn().CancelRequest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || strings.Contains(err.Error(), "never canceled") {
		t.Errorf("the canceled refresh: %v", err)
	}
	if filesIn(t, dir) != 0 {
		t.Errorf("the canceled refresh left files in %s", dir)
	}
	unchanged("after a cancel")
	env.srv.beforeOverwrite = nil

	// Files a stopped process's refresh left, in no snapshot, and the note
	// of files a refresh committed before it could forget it.
	ctx := context.Background()
	tbl, err := env.srv.ddl.LoadTable(ctx, "public", "proj")
	if err != nil {
		t.Fatal(err)
	}
	left := filepath.Join(strings.TrimPrefix(tbl.Location(), "file://"), "data", overwriteDir+"0123456789abcdef")
	conn, err := env.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(ctx, "COPY (SELECT 1 AS id, 2 AS x) TO "+quoteLiteral(left)+" (FORMAT parquet, FILENAME_PATTERN 'part_{i}', FILE_SIZE_BYTES 1000000)", nil); err != nil {
		t.Fatal(err)
	}
	data, _ := liveFiles(t, admin, "proj")
	held := refreshDirs(t, data)
	for _, d := range append([]string{left}, held...) {
		if err := env.srv.ddl.NotePending(ctx, "public", catalog.NewPending(catalog.PendingFiles, d, "proj")); err != nil {
			t.Fatal(err)
		}
	}
	catalog.Restarted()
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW proj")
	if n := filesIn(t, left); n != 0 {
		t.Errorf("%d files a stopped refresh left are still there", n)
	}
	for _, d := range held {
		if filesIn(t, d) == 0 {
			t.Errorf("the files of %s, which a snapshot holds, went", d)
		}
	}
	if n := len(filesNotes(t, env.srv)); n != 0 {
		t.Errorf("%d notes of refreshes' files left", n)
	}
	if got, want := sortedTexts(t, admin, "SELECT * FROM proj"), sortedTexts(t, admin, "SELECT id, x FROM src WHERE id % 2 = 0"); !slices.Equal(got, want) {
		t.Errorf("proj after the leftovers went: %d rows, want %d", len(got), len(want))
	}
}

// TestFullRefreshCost checks that a full refresh of a view of many rows
// takes about what CREATE TABLE ... AS of its query does, rather than the
// DELETE of every row it held, which took 8.5 s for 20 million rows where
// the copy took 0.15 s. A refresh also plans its query, records what it
// read and talks to the catalog, about 0.1 s here whatever the rows, so
// the view has enough rows for the writing to dominate, as at scale.
func TestFullRefreshCost(t *testing.T) {
	if testing.Short() {
		t.Skip("times refreshes of millions of rows")
	}
	env := startRefreshServer(t)
	admin := env.admin
	const rows = 50_000_000
	for _, q := range []string{
		fmt.Sprintf("CREATE TABLE big AS SELECT i AS id, i %% 97 AS g, (i %% 1000)::DECIMAL(12,2) AS amt, 'k' || (i %% 5000) AS k FROM range(%d) AS r(i)", rows),
		"CREATE MATERIALIZED VIEW big_copy AS SELECT id, g, amt, k FROM big",
		"REFRESH MATERIALIZED VIEW big_copy",
	} {
		mustExec(t, admin, q)
	}
	env.srv.refreshFraction = 1e-9
	timed := func(q string) time.Duration {
		start := time.Now()
		mustExec(t, admin, q)
		return time.Since(start)
	}
	best := func(ds []time.Duration) time.Duration { return slices.Min(ds) }
	var copies, refreshes []time.Duration
	for i := range 3 {
		copies = append(copies, timed("CREATE OR REPLACE TABLE big_ctas AS SELECT id, g, amt, k FROM big"))
		mustExec(t, admin, fmt.Sprintf("INSERT INTO big VALUES (%d, 1, 1.00, 'k1')", rows+i))
		refreshes = append(refreshes, timed("REFRESH MATERIALIZED VIEW big_copy"))
		if action, detail, _, _ := viewRefresh(t, admin, "public.big_copy"); action != "FULL" {
			t.Fatalf("refresh %d: %s, %s", i, action, detail)
		}
	}
	ctas, refresh := best(copies), best(refreshes)
	t.Logf("%d rows: CREATE TABLE ... AS %s, full refresh %s (%.2fx)", rows, ctas.Round(time.Millisecond), refresh.Round(time.Millisecond),
		float64(refresh)/float64(ctas))
	if float64(refresh) > 1.5*float64(ctas) {
		t.Errorf("a full refresh of %d rows took %s, more than 1.5 times the %s of CREATE TABLE ... AS", rows, refresh, ctas)
	}
	if _, deletes := liveFiles(t, admin, "big_copy"); len(deletes) > 0 {
		t.Errorf("big_copy has %d delete files", len(deletes))
	}
}

// TestFullRefreshAfterAConflict checks that a full refresh whose commit
// another commit to the view's table beat, as compaction's or a writer's,
// replaces the files of the new snapshot: the view holds its query's rows,
// and none of the other commit's.
func TestFullRefreshAfterAConflict(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	for _, q := range []string{
		"CREATE TABLE src AS SELECT i AS id, i * 2 AS x FROM range(1000) AS r(i)",
		"CREATE MATERIALIZED VIEW proj AS SELECT id, x FROM src",
		"REFRESH MATERIALIZED VIEW proj",
	} {
		mustExec(t, admin, q)
	}
	env.srv.refreshFraction = 1e-9
	other := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	var beat atomic.Int32
	env.srv.beforeOverwrite = func(ctx context.Context, _ string) error {
		if beat.Add(1) == 1 {
			_, err := other.Exec(ctx, "INSERT INTO proj VALUES (-1, -1)")
			return err
		}
		return nil
	}
	mustExec(t, admin, "INSERT INTO src VALUES (5000, 1)")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW proj")
	if beat.Load() != 1 {
		t.Fatalf("the hook ran %d times", beat.Load())
	}
	if got, want := sortedTexts(t, admin, "SELECT * FROM proj"), sortedTexts(t, admin, "SELECT id, x FROM src"); !slices.Equal(got, want) {
		t.Errorf("proj holds %d rows, want %d", len(got), len(want))
	}
	if _, deletes := liveFiles(t, admin, "proj"); len(deletes) > 0 {
		t.Errorf("proj has %d delete files", len(deletes))
	}
	// The refresh's snapshot follows the insert's.
	tbl, err := env.srv.ddl.LoadTable(context.Background(), "public", "proj")
	if err != nil {
		t.Fatal(err)
	}
	cur := tbl.CurrentSnapshot()
	if cur.ParentSnapshotID == nil {
		t.Fatal("the refresh's snapshot has no parent")
	}
	if parent := tbl.SnapshotByID(*cur.ParentSnapshotID); parent == nil || parent.Summary == nil || parent.Summary.Operation != "append" ||
		cur.Summary.Properties["igloo.operation"] != "refresh" {
		t.Errorf("the refresh's snapshot %+v follows %+v, not the insert's", cur.Summary, parent)
	}
}

// metadataLeft returns the Avro files under table's metadata directory that no
// snapshot of it has: a manifest list or manifest of a commit that was not made.
func metadataLeft(t *testing.T, env *refreshEnv, table string) []string {
	t.Helper()
	ctx := context.Background()
	tbl, err := env.srv.ddl.LoadTable(ctx, "public", table)
	if err != nil {
		t.Fatal(err)
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string]bool{}
	for _, snap := range tbl.Metadata().Snapshots() {
		held[strings.TrimPrefix(snap.ManifestList, "file://")] = true
		manifests, err := snap.Manifests(fs)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range manifests {
			held[strings.TrimPrefix(m.FilePath(), "file://")] = true
		}
	}
	files, err := filepath.Glob(filepath.Join(strings.TrimPrefix(tbl.Location(), "file://"), "metadata", "*.avro"))
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(files, func(f string) bool { return held[f] })
}

// notInRefreshDir returns the files of paths in no refresh's directory.
func notInRefreshDir(paths []string) []string {
	return slices.DeleteFunc(slices.Clone(paths), func(p string) bool { return strings.Contains(p, "/data/"+overwriteDir) })
}

// filesUnder counts the files under dir, in any directory.
func filesUnder(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d iofs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestFullRefreshOfWhatCopyCannotWrite checks that a full refresh replaces the
// files of a view's table in one commit when only DuckDB's INSERT can write
// them: partitioned, or with nested columns. No delete files, the rows the
// query's, in the table's partitions, and nothing of DuckDB's commit left; and
// the replaced files go with snapshot expiry, as those a COPY wrote do.
func TestFullRefreshOfWhatCopyCannotWrite(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name, query string
		// after runs after the view's first refresh.
		after       []string
		partitioned bool
	}{
		{name: "partitioned", query: "SELECT id, g, x FROM src", partitioned: true,
			after: []string{"ALTER TABLE lake.public.v SET PARTITIONED BY (bucket(4, id), g)"}},
		{name: "partitioned by a day", query: "SELECT id, DATE '2026-01-01' + CAST(id % 5 AS INTEGER) AS d, x FROM src", partitioned: true,
			after: []string{"ALTER TABLE lake.public.v SET PARTITIONED BY (day(d))"}},
		{name: "nested", query: "SELECT id, {'a': id, 'b': [g, g]} AS s, [{'k': id}] AS l, MAP {g: id} AS m, x FROM src"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			env := startRefreshServer(t)
			env.srv.refreshFraction = 1e-9
			admin := env.admin
			for _, q := range []string{
				"CREATE TABLE src AS SELECT i AS id, 'g' || (i % 3) AS g, i * 2 AS x FROM range(3000) AS r(i)",
				"CREATE MATERIALIZED VIEW v AS " + c.query,
				"REFRESH MATERIALIZED VIEW v",
			} {
				mustExec(t, admin, q)
			}
			for _, q := range c.after {
				mustExec(t, admin, q)
			}
			check := func(step string) {
				t.Helper()
				if got, want := sortedTexts(t, admin, "SELECT * FROM v"), sortedTexts(t, admin, c.query); !slices.Equal(got, want) {
					t.Errorf("%s: v holds %d rows, want %d", step, len(got), len(want))
				}
				if action, detail, _, _ := viewRefresh(t, admin, "public.v"); action != "FULL" {
					t.Errorf("%s: %s, %s", step, action, detail)
				}
				data, deletes := liveFiles(t, admin, "v")
				if len(deletes) > 0 || len(data) == 0 {
					t.Errorf("%s: %d data files and %d delete files", step, len(data), len(deletes))
				}
				if out := notInRefreshDir(data); len(out) > 0 {
					t.Errorf("%s: files outside refreshes' directories: %q", step, out)
				}
				if c.partitioned && len(data) < 2 {
					t.Errorf("%s: the rows of a partitioned table are in %d files", step, len(data))
				}
				if n := len(filesNotes(t, env.srv)); n != 0 {
					t.Errorf("%s: %d notes of refreshes' files left", step, n)
				}
				if left := metadataLeft(t, env, "v"); len(left) > 0 {
					t.Errorf("%s: manifests of a commit not made: %q", step, left)
				}
			}
			mustExec(t, admin, "DELETE FROM src WHERE id % 3 = 0")
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after a delete")
			first, _ := liveFiles(t, admin, "v")

			mustExec(t, admin, "DELETE FROM src WHERE id % 5 = 0")
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after another")
			second, _ := liveFiles(t, admin, "v")
			if slices.ContainsFunc(second, func(p string) bool { return slices.Contains(first, p) }) {
				t.Error("the second refresh kept a file of the first")
			}

			// Nothing to write: the table is empty, and in one commit too.
			mustExec(t, admin, "DELETE FROM src WHERE true")
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			if n := count(t, admin, "SELECT count(*) FROM v"); n != 0 {
				t.Errorf("v holds %d rows of a query of none", n)
			}
			data, deletes := liveFiles(t, admin, "v")
			if len(data) != 0 || len(deletes) != 0 {
				t.Errorf("v of no rows has %d data files and %d delete files", len(data), len(deletes))
			}
			mustExec(t, admin, "INSERT INTO src SELECT i, 'g' || (i % 3), i * 2 FROM range(100) AS r(i)")
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after it was emptied")

			// Snapshot expiry deletes the replaced files; the current ones stay.
			time.Sleep(20 * time.Millisecond)
			m := maintenance.New(env.catalog, env.dir, maintenance.Options{Retention: 10 * time.Millisecond}, slog.New(slog.DiscardHandler))
			m.RunOnce(context.Background())
			for _, p := range first {
				if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("a replaced file is left after snapshot expiry: %s, %v", p, err)
				}
			}
			check("after snapshot expiry")
		})
	}
}

// TestFullRefreshByInsertInterrupted checks that a full refresh by INSERT that
// fails, or is canceled, leaves the view as it was, and nothing it wrote: its
// files and DuckDB's manifests; and that one whose query fails leaves its
// connection out of the transaction it began.
func TestFullRefreshByInsertInterrupted(t *testing.T) {
	t.Parallel()
	env := startRefreshServer(t)
	admin := env.admin
	for _, q := range []string{
		"CREATE TABLE src AS SELECT i AS id, 'g' || (i % 3) AS g, i * 2 AS x FROM range(5000) AS r(i)",
		"CREATE MATERIALIZED VIEW v AS SELECT id, g, x FROM src WHERE id % 2 = 0",
		"REFRESH MATERIALIZED VIEW v",
		"ALTER TABLE lake.public.v SET PARTITIONED BY (bucket(4, id), g)",
	} {
		mustExec(t, admin, q)
	}
	env.srv.refreshFraction = 1e-9
	mustExec(t, admin, "DELETE FROM src WHERE id % 7 = 0")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
	before := sortedTexts(t, admin, "SELECT * FROM v")
	unchanged := func(step string) {
		t.Helper()
		if got := sortedTexts(t, admin, "SELECT * FROM v"); !slices.Equal(got, before) {
			t.Errorf("%s: v holds %d rows, want the %d it held", step, len(got), len(before))
		}
		if n := len(filesNotes(t, env.srv)); n != 0 {
			t.Errorf("%s: %d notes of refreshes' files left", step, n)
		}
		if left := metadataLeft(t, env, "v"); len(left) > 0 {
			t.Errorf("%s: manifests of a commit not made: %q", step, left)
		}
	}

	// A failure, DuckDB's files written and its commit kept.
	mustExec(t, admin, "INSERT INTO src VALUES (100000, 'g9', 1)")
	var dir string
	env.srv.beforeOverwrite = func(_ context.Context, d string) error {
		dir = d
		if filesUnder(t, d) == 0 {
			t.Errorf("DuckDB wrote nothing under %s", d)
		}
		return errors.New("an injected failure")
	}
	if _, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW v"); err == nil || !strings.Contains(err.Error(), "an injected failure") {
		t.Errorf("the refresh: %v", err)
	}
	if dir == "" || filesUnder(t, dir) != 0 {
		t.Errorf("the failed refresh left %d files in %q", filesUnder(t, dir), dir)
	}
	unchanged("after a failure")

	// A cancel, while the refresh waits to commit.
	waiting := make(chan string, 1)
	env.srv.beforeOverwrite = func(ctx context.Context, d string) error {
		waiting <- d
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return errors.New("never canceled")
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW v")
		done <- err
	}()
	dir = <-waiting
	if err := admin.PgConn().CancelRequest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || strings.Contains(err.Error(), "never canceled") {
		t.Errorf("the canceled refresh: %v", err)
	}
	if filesUnder(t, dir) != 0 {
		t.Errorf("the canceled refresh left files in %s", dir)
	}
	unchanged("after a cancel")
	env.srv.beforeOverwrite = nil

	// A failure in DuckDB's INSERT itself: its transaction ends with it.
	for _, q := range []string{
		"CREATE TABLE codes AS SELECT i AS id, CAST(i AS VARCHAR) AS code FROM range(100) AS r(i)",
		"CREATE MATERIALIZED VIEW w AS SELECT id, CAST(code AS INTEGER) AS n FROM codes",
		"REFRESH MATERIALIZED VIEW w",
		"ALTER TABLE lake.public.w SET PARTITIONED BY (bucket(4, id))",
		"INSERT INTO codes VALUES (1000, 'not a number')",
	} {
		mustExec(t, admin, q)
	}
	if _, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW w"); err == nil {
		t.Error("a refresh whose query fails went through")
	}
	if n := count(t, admin, "SELECT count(*) FROM w"); n != 100 {
		t.Errorf("w holds %d rows after its refresh failed", n)
	}
	if n := len(filesNotes(t, env.srv)); n != 0 {
		t.Errorf("%d notes of refreshes' files left after its query failed", n)
	}
	mustExec(t, admin, "DELETE FROM codes WHERE id = 1000")
	mustExec(t, admin, "INSERT INTO codes VALUES (1000, '1000')")
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW w")
	if got, want := sortedTexts(t, admin, "SELECT * FROM w"), sortedTexts(t, admin, "SELECT id, CAST(code AS INTEGER) FROM codes"); !slices.Equal(got, want) {
		t.Errorf("w after its query could run: %d rows, want %d", len(got), len(want))
	}

	// It goes through again.
	mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
	if got, want := sortedTexts(t, admin, "SELECT * FROM v"), sortedTexts(t, admin, "SELECT id, g, x FROM src WHERE id % 2 = 0"); !slices.Equal(got, want) {
		t.Errorf("v after it all: %d rows, want %d", len(got), len(want))
	}
}

// TestRefreshOfWholeAggregateOverwrites checks the rewrite of an aggregating
// view's whole table, which an incremental refresh does when the view's rows
// do not show their groups, or it has one row of no group: by overwrite, no
// delete files; and that a change is never counted twice in the state, whichever
// of the view and the state a failure leaves committed: the view's commit
// failing leaves all as it was, and the next refresh goes on from there; one
// after it makes the next refresh a full one.
func TestRefreshOfWholeAggregateOverwrites(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ name, query string }{
		{"one row", "SELECT count(*) AS n, sum(x) AS s FROM src"},
		{"rows not showing their groups", "SELECT sum(x) AS s FROM src GROUP BY g"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			env := startRefreshServer(t)
			env.srv.refreshFraction = 1000
			admin := env.admin
			for _, q := range []string{
				"CREATE TABLE src AS SELECT i AS id, i % 7 AS g, i * 2 AS x FROM range(1000) AS r(i)",
				"CREATE MATERIALIZED VIEW v AS " + c.query,
				"REFRESH MATERIALIZED VIEW v",
			} {
				mustExec(t, admin, q)
			}
			next := 1000
			change := func() {
				t.Helper()
				mustExec(t, admin, fmt.Sprintf("INSERT INTO src VALUES (%d, %d, 5)", next, next%7))
				next++
			}
			check := func(step, action string) {
				t.Helper()
				if got, want := sortedTexts(t, admin, "SELECT * FROM v"), sortedTexts(t, admin, c.query); !slices.Equal(got, want) {
					t.Errorf("%s: v holds %q, want %q", step, got, want)
				}
				if got, detail, _, _ := viewRefresh(t, admin, "public.v"); got != action {
					t.Errorf("%s: %s, %s, want %s", step, got, detail, action)
				}
				if _, deletes := liveFiles(t, admin, "v"); len(deletes) > 0 {
					t.Errorf("%s: %d delete files", step, len(deletes))
				}
			}
			check("first", "FULL")

			change()
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("incremental", "INCREMENTAL")

			// The view's commit fails: all as it was, and the change is
			// read by the next refresh, once.
			change()
			env.srv.beforeOverwrite = func(context.Context, string) error { return errors.New("an injected failure") }
			if _, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW v"); err == nil {
				t.Error("the refresh went through")
			}
			env.srv.beforeOverwrite = nil
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after a failure before the view's commit", "INCREMENTAL")

			// A stop between the view's commit and the state's: the view
			// holds the change, the state does not. The next refresh is full,
			// and the one after goes on from its state.
			change()
			env.srv.afterOverwrite = func(context.Context) error { return errors.New("an injected failure") }
			if _, err := admin.Exec(context.Background(), "REFRESH MATERIALIZED VIEW v"); err == nil {
				t.Error("the refresh went through")
			}
			env.srv.afterOverwrite = nil
			if got, want := sortedTexts(t, admin, "SELECT * FROM v"), sortedTexts(t, admin, c.query); !slices.Equal(got, want) {
				t.Errorf("after a failure between the commits: v holds %q, want %q", got, want)
			}
			change()
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after a failure between the commits", "FULL")
			change()
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after the full refresh", "INCREMENTAL")

			// A stop between the state's commit and the record of it: the
			// same, as the job says what the last refresh it knows of left.
			store := env.tasks.Store()
			job, err := store.Job(context.Background(), tasks.KindMaterializedView, "public.v")
			if err != nil {
				t.Fatal(err)
			}
			change()
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("before the record is lost", "INCREMENTAL")
			if err := store.SetRefresh(context.Background(), tasks.KindMaterializedView, "public.v", job.Refresh); err != nil {
				t.Fatal(err)
			}
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
			check("after the record was lost", "FULL")
		})
	}
}

// TestFullRefreshOfOtherEnginesTypes checks the same of a view's table another
// engine replaced with columns of a fixed-length or nanosecond type, which
// DuckDB describes as other types than COPY could write them in.
func TestFullRefreshOfOtherEnginesTypes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		field iceberg.NestedField
		value string
		props iceberg.Properties
	}{
		{"fixed", iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.FixedTypeOf(4)}, "CAST('abcd' AS BLOB)", nil},
		{"nanoseconds", iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.PrimitiveTypes.TimestampNs}, "TIMESTAMP '2026-01-01 00:00:00.123'",
			iceberg.Properties{"format-version": "3"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			env := startRefreshServer(t)
			env.srv.refreshFraction = 1e-9
			admin := env.admin
			mustExec(t, admin, "CREATE TABLE src AS SELECT i AS id FROM range(100) AS r(i)")
			mustExec(t, admin, "CREATE MATERIALIZED VIEW v AS SELECT id, "+c.value+" AS f FROM src")
			mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")

			id := table.Identifier{"public", "v"}
			if err := env.catalog.DropTable(ctx, id); err != nil {
				t.Fatal(err)
			}
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64}, c.field)
			var opts []icecat.CreateTableOpt
			if c.props != nil {
				opts = append(opts, icecat.WithProperties(c.props))
			}
			if _, err := env.catalog.CreateTable(ctx, id, schema, opts...); err != nil {
				t.Fatal(err)
			}
			env.srv.engine.CatalogChanged()
			var first []string
			for _, q := range []string{"", "DELETE FROM src WHERE id < 10"} {
				if q != "" {
					mustExec(t, admin, q)
				}
				mustExec(t, admin, "REFRESH MATERIALIZED VIEW v")
				if action, detail, _, _ := viewRefresh(t, admin, "public.v"); action != "FULL" {
					t.Errorf("%s, %s", action, detail)
				}
				want := count(t, admin, "SELECT count(*) FROM src")
				if n := count(t, admin, "SELECT count(*) FROM v WHERE f IS NOT NULL"); n != want {
					t.Errorf("v holds %d rows, want %d", n, want)
				}
				data, deletes := liveFiles(t, admin, "v")
				if len(deletes) > 0 || len(data) == 0 || len(notInRefreshDir(data)) > 0 {
					t.Errorf("v's data files %q, delete files %q", data, deletes)
				}
				if slices.ContainsFunc(data, func(p string) bool { return slices.Contains(first, p) }) {
					t.Errorf("the refresh kept a file of the last: %q", data)
				}
				first = data
			}
		})
	}
}
