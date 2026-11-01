package pgwire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
