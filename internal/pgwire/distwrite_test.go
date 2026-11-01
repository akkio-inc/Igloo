package pgwire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/icebergtest"
	"github.com/akkio-inc/igloo/internal/policy"
)

// writeFault stands in front of a worker, as a network or a worker gone
// wrong would.
type writeFault struct {
	target string
	client *http.Transport
	mu     sync.Mutex
	// answer, if set, sees each answer to a task that writes a table's files,
	// the probe's none: head is the answer as JSON, which it may change, task
	// the request. It returns what to do, which faultDrop does by cutting the
	// connection.
	answer func(task, head map[string]any) faultAction
	// request, if set, sees each such task before it reaches the worker.
	request func(task map[string]any) faultAction
	// probe, if set, answers the probes as a worker refused them.
	probe bool
	// memory, if set, answers the tasks that write as a worker out of memory.
	memory bool
	// short counts those answers.
	short atomic.Int64
	wrote atomic.Int64
	// busy counts the requests the worker has not answered yet.
	busy atomic.Int64
}

type faultAction int

const (
	faultNone faultAction = iota
	// faultDrop cuts the connection: the worker did what it did, or is doing
	// it, and the coordinator does not hear.
	faultDrop
)

func (f *writeFault) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	var task map[string]any
	_ = json.Unmarshal(body, &task)
	writes := task["write"] != nil
	isProbe := writes && task["task"] == float64(-1)
	f.mu.Lock()
	request, answer, probe, memory := f.request, f.answer, f.probe, f.memory
	f.mu.Unlock()
	if writes && !isProbe && memory {
		f.short.Add(1)
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = rw.Write([]byte(`{"error":{"message":"Out of Memory Error: of a test","out_of_memory":true}}` + "\n"))
		return
	}
	if isProbe && probe {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = rw.Write([]byte(`{"error":{"message":"HTTP 403 on ` + distributed.WriteDirMark + `0","write_denied":true}}` + "\n"))
		return
	}
	if writes && !isProbe && request != nil && request(task) == faultDrop {
		// The worker goes on with it, and no one hears.
		f.busy.Add(1)
		go func() {
			defer f.busy.Add(-1)
			_, _, _ = f.forward(context.Background(), r, body)
		}()
		panic(http.ErrAbortHandler)
	}
	// A coordinator that stopped waiting does not stop this: what a task goes on
	// to write is what a statement looks for after it ended.
	f.busy.Add(1)
	res, data, err := f.forward(context.WithoutCancel(r.Context()), r, body)
	f.busy.Add(-1)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if writes && !isProbe && res.StatusCode == http.StatusOK {
		f.wrote.Add(1)
		if answer != nil {
			var head map[string]any
			if err := json.Unmarshal(data, &head); err != nil {
				panic(err)
			}
			if answer(task, head) == faultDrop {
				panic(http.ErrAbortHandler)
			}
			if data, err = json.Marshal(head); err != nil {
				panic(err)
			}
			data = append(data, '\n')
			res.Header.Set("Content-Length", strconv.Itoa(len(data)))
		}
	}
	for k, v := range res.Header {
		rw.Header()[k] = v
	}
	rw.WriteHeader(res.StatusCode)
	_, _ = rw.Write(data)
}

func (f *writeFault) forward(ctx context.Context, r *http.Request, body []byte) (*http.Response, []byte, error) {
	out, err := http.NewRequestWithContext(ctx, r.Method, "https://"+f.target+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	out.Header = r.Header.Clone()
	res, err := f.client.RoundTrip(out)
	if err != nil {
		return nil, nil, err
	}
	defer res.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(res.Body)
	return res, data, err
}

func (f *writeFault) set(fn func(*writeFault)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

// sizedSource claims every table is of size bytes, so that auto mode weighs
// it as a large one.
type sizedSource struct {
	*distributed.Iceberg
	size int64
}

func (s sizedSource) Table(ctx context.Context, schema, name string) (*distributed.Table, error) {
	t, err := s.Iceberg.Table(ctx, schema, name)
	if t != nil && s.size > 0 {
		t.Bytes = s.size
	}
	return t, err
}

// writeLog logs to stderr with IGLOO_TEST_LOG set.
func writeLog() *slog.Logger {
	if os.Getenv("IGLOO_TEST_LOG") == "" {
		return slog.New(slog.DiscardHandler)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// writeEnv is a replica on an Iceberg catalog with workers, which read and
// write the files of its tables under dir as production's do.
type writeEnv struct {
	*refreshEnv
	faults []*writeFault
	gate   *taskGate
	co     *distributed.Coordinator
	faked  *icebergtest.Faults
}

// startWriteServer starts a writeEnv of two workers of 8 cores whose tables
// weigh 300 MB, with a source table src of 40,000 rows and several files.
func startWriteServer(t *testing.T, configure ...func(*Options)) *writeEnv {
	t.Helper()
	return startWriteServerWith(t, nil, configure...)
}

// startWriteServerWith is startWriteServer with the coordinator's options as
// coordinate leaves them.
func startWriteServerWith(t *testing.T, coordinate func(*distributed.Options), configure ...func(*Options)) *writeEnv {
	t.Helper()
	return startWriteServerOn(t, writeWorkers{cores: 8, memory: 4 << 30, claim: 300 << 20}, coordinate, configure...)
}

// writeWorkers says what workers a writeEnv has: of cores, taking their memory
// for memory, and with a DuckDB of limit, if set; and what size every table
// claims, if set.
type writeWorkers struct {
	cores         int
	memory, claim int64
	limit         string
}

// startWriteServerOn is startWriteServerWith on such workers.
func startWriteServerOn(t *testing.T, workers writeWorkers, coordinate func(*distributed.Options), configure ...func(*Options)) *writeEnv {
	t.Helper()
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set")
	}
	dir := t.TempDir()
	fake := icebergtest.NewCatalog(dir)
	t.Cleanup(fake.Close)
	faked, err := icebergtest.NewFaults(fake.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(faked.Close)
	uri := faked.URL
	env := &writeEnv{gate: newTaskGate(), faked: faked}
	serverTLS, clientTLS, err := distributed.WorkerTLS(testWorkerSecret)
	if err != nil {
		t.Fatal(err)
	}
	var addrs []string
	for i := range 2 {
		addr, _, _ := startTestWorkerLimited(t, dir, i, env.gate, "", workers.cores, workers.memory, workers.limit)
		f := &writeFault{target: addr, client: &http.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}}
		srv := httptest.NewUnstartedServer(f)
		srv.TLS = serverTLS.Clone()
		srv.StartTLS()
		t.Cleanup(srv.Close)
		t.Cleanup(f.client.CloseIdleConnections)
		env.faults = append(env.faults, f)
		addrs = append(addrs, strings.TrimPrefix(srv.URL, "https://"))
	}
	cat, err := catalog.Open(context.Background(), config.Config{
		Catalog: config.Catalog{URI: uri, Warehouse: "test", Name: "lake", Schema: "public"},
		Storage: config.Storage{Location: dir, S3: config.S3{Region: "us-east-1", URLStyle: "path", AccessKeyID: "unused", SecretAccessKey: "unused"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	opts := distributed.Options{
		Source: sizedSource{distributed.NewIceberg(cat), workers.claim}, Secret: testWorkerSecret, Version: "test", Cores: 1,
		TempDir: filepath.Join(dir, "coordinator"), Warehouse: dir + "/", Threshold: time.Second, MinWorkerWrite: 1,
		Storage: distributed.NewStorage(icebergio.LocalFS{}), Pool: distributed.PoolOptions{Addresses: addrs},
	}
	if coordinate != nil {
		coordinate(&opts)
	}
	env.co, err = distributed.NewCoordinator(opts, writeLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { env.co.Close() })
	env.refreshEnv = startRefreshServerOn(t, fake, dir, uri, nil, append([]func(*Options){func(o *Options) { o.Distributed = env.co }}, configure...)...)
	env.srv.log = writeLog()
	// Rather than a while, until the workers have done what they were asked.
	env.srv.lateWritesWait = func() {
		for env.faults[0].busy.Load()+env.faults[1].busy.Load() > 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	mustExec(t, env.admin, "SET distributed_execution = off")
	mustExec(t, env.admin, "CREATE TABLE src AS SELECT i AS id, i % 7 AS g, i * 2 AS x, 'name-' || (i % 5) AS name, TIMESTAMP '2026-01-01' + INTERVAL 1 DAY * (i % 40) AS ts FROM range(10000) AS r(i)")
	for n := 1; n < 4; n++ {
		mustExec(t, env.admin, "INSERT INTO src SELECT i AS id, i % 7 AS g, i * 2 AS x, 'name-' || (i % 5) AS name, TIMESTAMP '2026-01-01' + INTERVAL 1 DAY * (i % 40) AS ts FROM range("+
			strconv.Itoa(n*10000)+", "+strconv.Itoa((n+1)*10000)+") AS r(i)")
	}
	mustExec(t, env.admin, "RESET distributed_execution")
	return env
}

// location returns where table's files are, once statements have looked for
// what their tasks still wrote.
func (e *writeEnv) location(t *testing.T, table string) string {
	t.Helper()
	e.srv.lateWrites.Wait()
	tbl, err := e.srv.ddl.LoadTable(context.Background(), "public", table)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(strings.TrimPrefix(tbl.Location(), "file://"), "/")
}

// written returns the files under dir in statements' directories.
func written(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !d.IsDir() && (strings.Contains(p, "/"+distributed.WriteDirMark) || strings.Contains(p, "/"+distributed.SealedDirMark)) {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(out)
	return out
}

// checkWrote checks that workers wrote table's files, that they are all that
// is left of the statement's, and that no note of them is left.
func (e *writeEnv) checkWrote(t *testing.T, table string) {
	t.Helper()
	e.srv.lateWrites.Wait()
	data, deletes := liveFiles(t, e.admin, table)
	if len(data) == 0 || len(deletes) > 0 {
		t.Errorf("%s has %d data files and %d delete files", table, len(data), len(deletes))
	}
	for _, f := range data {
		if !strings.Contains(f, "/"+distributed.SealedDirMark) {
			t.Errorf("%s's file %s is not a copy of a statement's", table, f)
		}
	}
	left := written(t, e.location(t, table))
	slices.Sort(data)
	data = slices.Compact(data)
	if len(left) != len(data) {
		t.Errorf("under %s: %d files of statements' directories, %d committed: %q", table, len(left), len(data), left)
	}
	e.checkNoNotes(t)
}

func (e *writeEnv) checkNoNotes(t *testing.T) {
	t.Helper()
	e.srv.lateWrites.Wait()
	if n := filesNotes(t, e.srv); len(n) != 0 {
		t.Errorf("notes of files left: %+v", n)
	}
}

// force runs statements on workers from here on.
func (e *writeEnv) force(t *testing.T) { mustExec(t, e.admin, "SET distributed_execution = on") }

// local returns query's rows as this replica alone computes them, its
// statements running on workers again afterwards.
func (e *writeEnv) local(t *testing.T, query string) string {
	t.Helper()
	mustExec(t, e.admin, "SET distributed_execution = off")
	defer e.force(t)
	return strings.Join(texts(t, e.admin, query), "\n")
}

// TestWorkersWrite checks that the tasks of a fill write the table's files,
// and that it holds the rows this replica would compute, on each way a table
// is filled from a query: CREATE TABLE ... AS, CREATE OR REPLACE, dbt's
// CREATE TABLE ... AS (query), INSERT INTO ... SELECT with its columns in the
// table's order or another's, and a full refresh. It leaves no file but those
// it committed, no note, and no path holding a value of the table.
func TestWorkersWrite(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	same := func(t *testing.T, table, query string) {
		t.Helper()
		if got, want := env.local(t, "SELECT * FROM "+table+" ORDER BY ALL"), env.local(t, "SELECT * FROM ("+query+") ORDER BY ALL"); got != want {
			t.Errorf("%s: rows differ from those of %s", table, query)
		}
	}
	for _, c := range []struct{ name, fill, table, query string }{
		{"create", "CREATE TABLE w_copy AS SELECT id, g, x, name, ts FROM src", "w_copy", "SELECT id, g, x, name, ts FROM src"},
		{"replace", "CREATE OR REPLACE TABLE w_copy AS SELECT id, x FROM src WHERE g < 3", "w_copy", "SELECT id, x FROM src WHERE g < 3"},
		{"dbt", `create table "lake"."public"."w_dbt__dbt_tmp" as (select id, name from src where g <> 2)`, "w_dbt__dbt_tmp",
			"select id, name from src where g <> 2"},
		{"expressions", "CREATE TABLE w_expr AS SELECT id + 1 AS a, upper(name) AS b, x / 2.0 AS c, NULL::VARCHAR AS d FROM src", "w_expr",
			"SELECT id + 1 AS a, upper(name) AS b, x / 2.0 AS c, NULL::VARCHAR AS d FROM src"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustExec(t, admin, c.fill)
			same(t, c.table, c.query)
			env.checkWrote(t, c.table)
		})
	}

	// Wherever the rest of these write, the same rows and nothing left over.
	for _, c := range []struct{ name, query string }{
		{"aggregate", "SELECT g, count(*) AS n, sum(x) AS s FROM src GROUP BY g"},
		{"distinct", "SELECT DISTINCT g, name FROM src"},
		{"join", "SELECT a.id, b.x FROM src AS a JOIN src AS b ON a.id = b.id WHERE a.g = 1"},
		{"union", "SELECT id FROM src WHERE g = 1 UNION ALL SELECT id FROM src WHERE g = 2"},
		{"window", "SELECT id, row_number() OVER (PARTITION BY g ORDER BY id) AS r FROM src"},
		{"order", "SELECT id, x FROM src ORDER BY x DESC"},
		{"limit", "SELECT id, x FROM src ORDER BY x DESC LIMIT 100"},
		{"common table", "WITH s AS (SELECT id, x FROM src WHERE g < 4) SELECT id FROM s WHERE x > 5"},
	} {
		t.Run(c.name, func(t *testing.T) {
			table := "w_q_" + strings.ReplaceAll(c.name, " ", "_")
			mustExec(t, admin, "CREATE TABLE "+table+" AS "+c.query)
			same(t, table, c.query)
			data, _ := liveFiles(t, admin, table)
			left := written(t, env.location(t, table))
			for _, f := range left {
				if !slices.Contains(data, f) {
					t.Errorf("a file left: %s", f)
				}
			}
			env.checkNoNotes(t)
		})
	}

	t.Run("insert", func(t *testing.T) {
		mustExec(t, admin, "CREATE TABLE w_ins (id BIGINT, x BIGINT)")
		tag, err := admin.Exec(context.Background(), "INSERT INTO w_ins SELECT id, x FROM src WHERE g = 1")
		if err != nil {
			t.Fatal(err)
		}
		if want := "INSERT 0 " + env.local(t, "SELECT count(*) FROM src WHERE g = 1"); tag.String() != want {
			t.Errorf("tag %q, want %q", tag, want)
		}
		mustExec(t, admin, "INSERT INTO w_ins (x, id) SELECT x, id FROM src WHERE g = 2")
		same(t, "w_ins", "SELECT id, x FROM src WHERE g IN (1, 2)")
		env.checkWrote(t, "w_ins")
	})

	t.Run("no rows", func(t *testing.T) {
		mustExec(t, admin, "CREATE TABLE w_none AS SELECT id, x FROM src WHERE g > 100")
		if got := env.local(t, "SELECT count(*) FROM w_none"); got != "0" {
			t.Errorf("%s rows", got)
		}
		if left := written(t, env.location(t, "w_none")); len(left) != 0 {
			t.Errorf("files left: %q", left)
		}
		env.checkNoNotes(t)
	})

	t.Run("materialized view", func(t *testing.T) {
		// Its column renamed in FROM, it is refreshed in full.
		mv := "SELECT i AS id, x FROM lake.public.src AS o(i) WHERE g < 5"
		mustExec(t, admin, "CREATE MATERIALIZED VIEW w_mv AS "+mv)
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW w_mv")
		if _, detail, _, _ := viewRefresh(t, admin, "public.w_mv"); !strings.Contains(detail, "its query ran on") {
			t.Errorf("the refresh: %s", detail)
		}
		same(t, "w_mv", mv)
		env.checkWrote(t, "w_mv")
		mustExec(t, admin, "INSERT INTO src VALUES (-1, 1, -2, 'one more', TIMESTAMP '2026-03-01')")
		defer mustExec(t, admin, "DELETE FROM src WHERE id = -1")
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW w_mv")
		same(t, "w_mv", mv)
		env.checkNoNotes(t)
	})

	t.Run("partitioned materialized view", func(t *testing.T) {
		mv := "SELECT i AS id, g, x FROM lake.public.src AS o(i) WHERE g < 5"
		mustExec(t, admin, "CREATE MATERIALIZED VIEW w_mv_parts AS "+mv)
		mustExec(t, admin, "ALTER TABLE lake.public.w_mv_parts SET PARTITIONED BY (g)")
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW w_mv_parts")
		same(t, "w_mv_parts", mv)
		env.checkWrote(t, "w_mv_parts")
		if data, _ := liveFiles(t, admin, "w_mv_parts"); len(data) < 2 {
			t.Errorf("the rows of a partitioned view are in %d files", len(data))
		}
		if got, want := env.local(t, "SELECT count(*) FROM w_mv_parts WHERE g = 2"), env.local(t, "SELECT count(*) FROM src WHERE g = 2"); got != want {
			t.Errorf("a partition holds %s rows, want %s", got, want)
		}
		env.checkNoNotes(t)
	})
}

// TestWorkersWritePartitions checks that workers write partitioned tables:
// one file per partition per task, each in its partition, which readers prune
// by; and that NULLs and values no path could hold are fine. A table
// partitioned by bucket, whose values a file's bounds do not give, is written
// here.
func TestWorkersWritePartitions(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	ctx := context.Background()
	rows := "SELECT id, g, CASE WHEN id % 11 = 0 THEN NULL WHEN g = 3 THEN 'a/b c=%..' ELSE name END AS name, ts, CAST(ts AS DATE) AS day, " +
		"CAST(ts AS TIMESTAMPTZ) AS tz, (g * 1.5)::DECIMAL(18, 2) AS price, x, CAST(g AS INTEGER) AS small, g > 3 AS flag FROM src"
	const epoch = "DATE '1970-01-01', "
	for _, c := range []struct {
		name, partition, value string
		workers                bool
	}{
		{"identity int", "g", "g", true},
		{"identity int32", "small", "small", true},
		{"identity bool", "flag", "CASE WHEN flag THEN 'true' ELSE 'false' END", true},
		{"identity timestamp", "ts", "datediff('microsecond', TIMESTAMP '1970-01-01', ts)", true},
		{"identity string", "name", "name", true},
		{"identity date", "day", "datediff('day', " + epoch + "day)", true},
		{"day", "day(ts)", "datediff('day', " + epoch + "ts)", true},
		{"day of a zone", "day(tz)", "datediff('day', " + epoch + "timezone('UTC', tz))", true},
		{"month", "month(ts)", "datediff('month', " + epoch + "ts)", true},
		{"year", "year(day)", "datediff('year', " + epoch + "day)", true},
		{"hour", "hour(ts)", "datediff('hour', " + epoch + "ts)", true},
		{"truncate int", "truncate(1000, id)", "id - (id % 1000)", true},
		{"truncate string", "truncate(3, name)", "left(name, 3)", true},
		{"two", "g, day(ts)", "", true},
		{"identity decimal", "price", "", false},
		{"bucket", "bucket(4, g)", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			table := "w_p_" + strings.ReplaceAll(c.name, " ", "_")
			mustExec(t, admin, "CREATE TABLE "+table+" AS "+rows+" LIMIT 0")
			mustExec(t, admin, "ALTER TABLE "+table+" SET PARTITIONED BY ("+c.partition+")")
			mustExec(t, admin, "INSERT INTO "+table+" "+rows)
			if got, want := env.local(t, "SELECT * FROM "+table+" ORDER BY ALL"), env.local(t, rows+" ORDER BY ALL"); got != want {
				t.Errorf("rows differ:\n%.300s\nwant\n%.300s", got, want)
			}
			data, _ := liveFiles(t, admin, table)
			inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
			if c.workers {
				if !slices.ContainsFunc(data, inDir) {
					t.Fatalf("workers did not write it: %d files", len(data))
				}
				env.checkWrote(t, table)
				for _, f := range data {
					// No value of the table in a path.
					if _, rest, _ := strings.Cut(f, "/"+distributed.SealedDirMark); strings.ContainsAny(rest, "% ") || strings.Contains(rest, "name-") {
						t.Errorf("path %s", f)
					}
				}
			} else if slices.ContainsFunc(data, inDir) {
				t.Errorf("workers wrote it: %q", data)
			}
			// Readers prune by the files' partitions, and find what they should.
			for _, filter := range []string{"g = 2", "name = 'name-1'", "name IS NULL", "id BETWEEN 1500 AND 2500", "ts >= TIMESTAMP '2026-01-15' AND ts < TIMESTAMP '2026-02-02'"} {
				if got, want := env.local(t, "SELECT count(*), sum(x) FROM "+table+" WHERE "+filter), env.local(t, "SELECT count(*), sum(x) FROM ("+rows+") WHERE "+filter); got != want {
					t.Errorf("rows of %s: %s, want %s", filter, got, want)
				}
			}
			if c.value != "" {
				// Each file's partition is that of each of its rows.
				tbl, err := env.srv.ddl.LoadTable(ctx, "public", table)
				if err != nil {
					t.Fatal(err)
				}
				files, _, err := currentFiles(ctx, tbl)
				if err != nil {
					t.Fatal(err)
				}
				for _, df := range files {
					var want string
					for _, v := range df.Partition() {
						if v != nil {
							want = fmt.Sprint(v)
						}
					}
					got := strings.Join(texts(t, admin, "SELECT DISTINCT coalesce(CAST("+c.value+" AS VARCHAR), '') FROM read_parquet("+quoteLiteral(df.FilePath())+")"), ",")
					if got != want {
						t.Errorf("%s: its rows' partition is %q, its own %q", df.FilePath(), got, want)
					}
				}
			}
		})
	}
}

// TestWorkersWriteRetry checks that files of an attempt the coordinator did
// not hear from count for nothing: a worker that wrote its files and whose
// answer was lost, and one that goes on writing after that, as a worker
// that seems dead may; the task runs again elsewhere, and the rows are there
// once. A worker killed outright is in scripts/e2e-distributed.sh.
func TestWorkersWriteRetry(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	want := env.local(t, "SELECT count(*), sum(id), count(DISTINCT id) FROM src")
	check := func(t *testing.T, table string, lost []string) {
		t.Helper()
		if got := env.local(t, "SELECT count(*), sum(id), count(DISTINCT id) FROM "+table); got != want {
			t.Errorf("%s holds %s, want %s", table, got, want)
		}
		data, _ := liveFiles(t, admin, table)
		for _, l := range lost {
			if slices.Contains(data, l) {
				t.Errorf("a lost attempt's file %s is in the table", l)
			}
		}
	}

	t.Run("answer lost", func(t *testing.T) {
		var lost []string
		var once sync.Once
		env.faults[0].set(func(f *writeFault) {
			f.answer = func(_, head map[string]any) faultAction {
				act := faultNone
				once.Do(func() {
					for _, file := range head["files"].([]any) {
						lost = append(lost, file.(map[string]any)["path"].(string))
					}
					act = faultDrop
				})
				return act
			}
		})
		defer env.faults[0].set(func(f *writeFault) { f.answer = nil })
		mustExec(t, admin, "CREATE TABLE w_lost AS SELECT id, x FROM src")
		if len(lost) == 0 {
			t.Fatal("no answer was lost")
		}
		check(t, "w_lost", lost)
		env.checkWrote(t, "w_lost")
		for _, l := range lost {
			if _, err := os.Stat(l); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a lost attempt's file %s is still there: %v", l, err)
			}
		}
	})

	t.Run("worker goes on", func(t *testing.T) {
		var once sync.Once
		env.faults[1].set(func(f *writeFault) {
			f.request = func(map[string]any) faultAction {
				act := faultNone
				once.Do(func() { act = faultDrop })
				return act
			}
		})
		defer env.faults[1].set(func(f *writeFault) { f.request = nil })
		mustExec(t, admin, "CREATE TABLE w_goes_on AS SELECT id, x FROM src")
		check(t, "w_goes_on", nil)
		env.checkNoNotes(t)
		data, _ := liveFiles(t, admin, "w_goes_on")
		for _, f := range written(t, env.location(t, "w_goes_on")) {
			if !slices.Contains(data, f) {
				t.Errorf("what the worker went on to write is still there: %s", f)
			}
		}
	})
}

// TestWorkersWriteRefused checks that the coordinator commits only files a
// statement's tasks wrote where they were told to: a worker that names a
// file of another table, one out of its directory or another attempt's, or
// says otherwise than a file's footer of its rows or size, gets nothing
// committed; the statement runs here, the table holds its right rows, and
// the other file is left alone.
func TestWorkersWriteRefused(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE w_secret AS SELECT 'a secret' AS code FROM range(10)")
	foreign, _ := liveFiles(t, admin, "w_secret")
	if len(foreign) == 0 {
		t.Fatal("no files of the other table")
	}
	refused := func(name string, change func(file, head map[string]any)) {
		t.Run(name, func(t *testing.T) {
			var once sync.Once
			env.faults[0].set(func(f *writeFault) {
				f.answer = func(_, head map[string]any) faultAction {
					once.Do(func() {
						files, _ := head["files"].([]any)
						if len(files) > 0 {
							change(files[0].(map[string]any), head)
						}
					})
					return faultNone
				}
			})
			defer env.faults[0].set(func(f *writeFault) { f.answer = nil })
			table := "w_r_" + strings.ReplaceAll(name, " ", "_")
			mustExec(t, admin, "CREATE TABLE "+table+" AS SELECT code AS a, code AS b FROM w_secret LIMIT 0")
			mustExec(t, admin, "INSERT INTO "+table+" SELECT name, name FROM src")
			if got, want := env.local(t, "SELECT count(*), count(a) FROM "+table), env.local(t, "SELECT count(*), count(name) FROM src"); got != want {
				t.Errorf("%s, want %s", got, want)
			}
			data, _ := liveFiles(t, admin, table)
			if slices.ContainsFunc(data, func(f string) bool { return slices.Contains(foreign, f) }) {
				t.Errorf("the other table's file is in this one: %q", data)
			}
			if slices.ContainsFunc(data, func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }) {
				t.Errorf("a suspect worker's statement committed files of workers: %q", data)
			}
			if got := env.local(t, "SELECT count(*) FROM w_secret"); got != "10" {
				t.Errorf("the other table holds %s rows", got)
			}
			env.checkNoNotes(t)
			if left := written(t, env.location(t, table)); len(left) != 0 {
				t.Errorf("files left: %q", left)
			}
		})
	}
	refused("foreign file", func(file, _ map[string]any) { file["path"] = foreign[0] })
	refused("climbs out", func(file, _ map[string]any) {
		file["path"] = filepath.Dir(file["path"].(string)) + "/../../../../../" + filepath.Base(foreign[0])
	})
	refused("another attempt", func(file, _ map[string]any) {
		file["path"] = strings.Replace(file["path"].(string), "/a0/", "/a7/", 1)
	})
	refused("not parquet", func(file, _ map[string]any) { file["path"] = file["path"].(string) + ".txt" })
	refused("named twice", func(file, head map[string]any) {
		twice := file
		head["files"] = append(head["files"].([]any), twice)
		head["rows"] = head["rows"].(float64) * 2
	})
	refused("rows", func(file, head map[string]any) {
		file["rows"] = file["rows"].(float64) + 1
		head["rows"] = head["rows"].(float64) + 1
	})
	refused("rows in sum", func(_, head map[string]any) { head["rows"] = head["rows"].(float64) + 1 })
	refused("size", func(file, _ map[string]any) { file["size"] = file["size"].(float64) + 1 })
	refused("no such file", func(file, _ map[string]any) {
		file["path"] = filepath.Dir(file["path"].(string)) + "/part_99.parquet"
	})
}

// TestWorkersWriteConflict checks that a commit another commit got ahead of
// goes on the new snapshot, with both's rows; and that one whose answer was
// lost is not made twice, nor its rows written again.
func TestWorkersWriteConflict(t *testing.T) {
	t.Parallel()
	var hook atomic.Pointer[func(context.Context, string) error]
	env := startWriteServer(t, func(o *Options) {})
	env.srv.beforeWriteCommit = func(ctx context.Context, dir string) error {
		if h := hook.Load(); h != nil {
			return (*h)(ctx, dir)
		}
		return nil
	}
	env.force(t)
	admin := env.admin
	other := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, other, "SET search_path = 'lake.public,memory.main'")
	mustExec(t, admin, "CREATE TABLE w_conflict (id BIGINT, x BIGINT)")

	t.Run("another commit first", func(t *testing.T) {
		var once sync.Once
		first := func(ctx context.Context, _ string) (err error) {
			once.Do(func() { _, err = other.Exec(ctx, "INSERT INTO w_conflict VALUES (-1, -1)") })
			return err
		}
		hook.Store(&first)
		defer hook.Store(nil)
		mustExec(t, admin, "INSERT INTO w_conflict SELECT id, x FROM src")
		if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_conflict"), env.local(t, "SELECT count(*) + 1, sum(x) - 1 FROM src"); got != want {
			t.Errorf("%s, want %s", got, want)
		}
		env.checkNoNotes(t)
	})

	t.Run("answer lost", func(t *testing.T) {
		before := env.local(t, "SELECT count(*) FROM w_conflict")
		env.faked.Arm(icebergtest.DropAfterApply)
		mustExec(t, admin, "INSERT INTO w_conflict SELECT id, x FROM src WHERE g = 1")
		if got, want := env.local(t, "SELECT count(*) - "+before+" FROM w_conflict"), env.local(t, "SELECT count(*) FROM src WHERE g = 1"); got != want {
			t.Errorf("rows added: %s, want %s", got, want)
		}
		env.checkNoNotes(t)
	})

	t.Run("no way to tell", func(t *testing.T) {
		before := env.local(t, "SELECT count(*) FROM w_conflict")
		env.faked.Arm(icebergtest.DownAfterApply)
		err := func() error {
			_, err := admin.Exec(context.Background(), "INSERT INTO w_conflict SELECT id, x FROM src WHERE g = 2")
			return err
		}()
		env.faked.Up()
		added := env.local(t, "SELECT count(*) - "+before+" FROM w_conflict")
		want := env.local(t, "SELECT count(*) FROM src WHERE g = 2")
		if err == nil && added != want {
			t.Errorf("no error, and %s rows added, want %s", added, want)
		}
		if added != want && added != "0" {
			t.Errorf("rows added: %s, of %s", added, want)
		}
		// The next write of the table tells, and clears what it can.
		mustExec(t, admin, "INSERT INTO w_conflict SELECT id, x FROM src WHERE g = 3")
		env.checkNoNotes(t)
	})
}

// TestWorkersWriteCanceled checks that a statement canceled leaves the table
// as it was and no file: after its tasks wrote, as it commits, and with one
// task running.
func TestWorkersWriteCanceled(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE w_cancel (id BIGINT, x BIGINT)")
	mustExec(t, admin, "INSERT INTO w_cancel VALUES (-1, -1)")
	unchanged := func(t *testing.T) {
		t.Helper()
		if got := env.local(t, "SELECT count(*), CAST(sum(id) AS BIGINT) FROM w_cancel"); got != "1|-1" {
			t.Errorf("the table holds %s", got)
		}
		env.checkNoNotes(t)
		if left := written(t, env.location(t, "w_cancel")); len(left) > 0 {
			t.Errorf("files left: %q", left)
		}
	}
	t.Run("as it commits", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		env.srv.beforeWriteCommit = func(stmt context.Context, _ string) error {
			cancel()
			<-stmt.Done()
			return stmt.Err()
		}
		defer func() { env.srv.beforeWriteCommit = nil }()
		if _, err := admin.Exec(ctx, "INSERT INTO w_cancel SELECT id, x FROM src"); err == nil {
			t.Error("no error")
		}
		admin = reconnect(t, env)
		unchanged(t)
	})
	t.Run("a task running", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		defer env.faults[1].set(func(f *writeFault) { f.request = nil })
		env.faults[1].set(func(f *writeFault) {
			f.request = func(map[string]any) faultAction {
				cancel()
				<-release
				return faultNone
			}
		})
		_, err := admin.Exec(ctx, "INSERT INTO w_cancel SELECT id, x FROM src")
		close(release)
		if err == nil {
			t.Error("no error")
		}
		admin = reconnect(t, env)
		unchanged(t)
	})
}

// reconnect replaces the test's connection, which a canceled statement ends.
func reconnect(t *testing.T, env *writeEnv) *pgx.Conn {
	t.Helper()
	conn := env.testServer.admin(t, pgx.QueryExecModeSimpleProtocol)
	mustExec(t, conn, "SET distributed_execution = on")
	env.admin = conn
	return conn
}

// TestWorkersWriteLeftovers checks what a stopped process's statement left:
// the table's next write deletes it, its note too, unless a snapshot holds its
// files; a CREATE TABLE ... AS's goes with the staging table it was for.
func TestWorkersWriteLeftovers(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	ctx := context.Background()
	conn, err := env.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	leave := func(table, target, name string) string {
		t.Helper()
		dir := env.location(t, table) + "/data/" + distributed.WriteDirMark + name
		task := dir + "/q1/s0-0/a0"
		if err := os.MkdirAll(filepath.Dir(task), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "COPY (SELECT 1 AS id, 2 AS x) TO "+quoteLiteral(task)+" (FORMAT parquet, FILENAME_PATTERN 'part_{i}', FILE_SIZE_BYTES 1000000)", nil); err != nil {
			t.Fatal(err)
		}
		if err := env.srv.ddl.NotePending(ctx, "public", catalog.NewPending(catalog.PendingFiles, dir, target)); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	mustExec(t, admin, "CREATE TABLE w_left AS SELECT id, x FROM src")
	data, _ := liveFiles(t, admin, "w_left")
	dir := leave("w_left", "w_left", "0123456789abcdef")
	// The note of files a statement committed and stopped before it forgot it.
	held := filepath.Join(filepath.Dir(filepath.Dir(data[0])), distributed.WriteDirMark+strings.TrimPrefix(filepath.Base(filepath.Dir(data[0])), distributed.SealedDirMark))
	if err := env.srv.ddl.NotePending(ctx, "public", catalog.NewPending(catalog.PendingFiles, held, "w_left")); err != nil {
		t.Fatal(err)
	}
	// A running statement's stays.
	catalog.Restarted()
	running := leave("w_left", "w_left", "fedcba9876543210")
	for _, n := range filesNotes(t, env.srv) {
		if n.Name == running {
			env.srv.writesRunning.Store(n.Key, true)
		}
	}
	mustExec(t, admin, "INSERT INTO w_left SELECT id, x FROM src WHERE g = 1")
	env.srv.lateWrites.Wait()
	if left := written(t, dir); len(left) != 0 {
		t.Errorf("files a stopped statement left: %q", left)
	}
	for _, f := range data {
		if _, err := os.Stat(f); err != nil {
			t.Errorf("a committed file went: %v", err)
		}
	}
	if len(written(t, running)) == 0 {
		t.Errorf("a running statement's files went")
	}
	notes := filesNotes(t, env.srv)
	if len(notes) != 1 || notes[0].Name != running {
		t.Errorf("notes %+v, want only the running statement's", notes)
	}
	if got, want := env.local(t, "SELECT count(*) FROM w_left"), env.local(t, "SELECT count(*) + count(*) FILTER (g = 1) FROM src"); got != want {
		t.Errorf("%s rows, want %s", got, want)
	}

	// A stopped CREATE TABLE ... AS: its staging table, and its workers' files.
	staging := "w_staged" + policy.StagingMark + "0123456789abcdef"
	mustExec(t, admin, `CREATE TABLE "`+staging+`" (id BIGINT, x BIGINT)`)
	stagedAt := env.location(t, staging)
	stagedDir := leave(staging, staging, "00112233445566")
	if err := env.srv.ddl.NotePending(ctx, "public", catalog.NewPending(catalog.PendingFill, staging, "w_staged")); err != nil {
		t.Fatal(err)
	}
	catalog.Restarted()
	mustExec(t, admin, "CREATE TABLE w_staged AS SELECT id, x FROM src WHERE g = 2")
	if left := written(t, stagedDir); len(left) != 0 {
		t.Errorf("files a stopped CREATE TABLE ... AS left under %s: %q", stagedAt, left)
	}
	if got := env.local(t, "SELECT count(*) FROM duckdb_tables() WHERE table_name = '"+staging+"'"); got != "0" {
		t.Errorf("the staging table is still there")
	}
}

// TestWorkersWriteDenied checks that workers denied the write leave the table
// to this replica, which writes it from their results; that the coordinator
// asks none to write for a while; and that IGLOO_WORKER_WRITES=off does the
// same.
func TestWorkersWriteDenied(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	for _, f := range env.faults {
		f.set(func(f *writeFault) { f.probe = true })
	}
	mustExec(t, admin, "CREATE TABLE w_denied AS SELECT id, x FROM src")
	if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_denied"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("%s, want %s", got, want)
	}
	data, _ := liveFiles(t, admin, "w_denied")
	if slices.ContainsFunc(data, func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }) {
		t.Errorf("workers wrote it: %q", data)
	}
	if left := written(t, env.location(t, "w_denied")); len(left) != 0 {
		t.Errorf("files left: %q", left)
	}
	env.checkNoNotes(t)
	wrote := env.faults[0].wrote.Load() + env.faults[1].wrote.Load()
	for _, f := range env.faults {
		f.set(func(f *writeFault) { f.probe = false })
	}
	mustExec(t, admin, "CREATE TABLE w_after AS SELECT id, x FROM src")
	if now := env.faults[0].wrote.Load() + env.faults[1].wrote.Load(); now != wrote {
		t.Errorf("%d more tasks wrote files, within a while of the denial", now-wrote)
	}
}

// TestWorkersSessionFunctions checks that what a fill's last step computes from
// the session is computed here, where workers would take another branch or read
// their own settings: its rows are those this replica computes, whatever else
// of the statement may run on workers. Statement time is the same on all of them.
func TestWorkersSessionFunctions(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	ctx := context.Background()
	mustExec(t, env.admin, "SET VARIABLE sess_var = 'from-the-session'")
	shapes := map[string]string{
		"grouped":  "SELECT id, count(DISTINCT x) AS n, %s AS v FROM src GROUP BY id",
		"plain":    "SELECT id, 1 AS n, %s AS v FROM (SELECT id FROM src) s",
		"filtered": "SELECT id, 1 AS n, 'kept' AS v FROM (SELECT id FROM src) s WHERE (%s) IS NOT NULL",
	}
	exprs := []string{
		"current_schema()",
		"current_database()",
		"getvariable('sess_var')",
		"CASE WHEN getvariable('sess_var') = 'from-the-session' THEN 'masked' ELSE 'UNMASKED' END",
		"current_setting('temp_directory')",
		"current_setting('threads')",
		"version()",
		"has_table_privilege('src', 'select')",
		"pg_table_is_visible(0)",
		"current_connection_id() > 0",
	}
	n := 0
	for shape, q := range shapes {
		for _, e := range exprs {
			n++
			name := fmt.Sprintf("sess_%d", n)
			query := fmt.Sprintf(q, e)
			if _, err := env.admin.Exec(ctx, "CREATE TABLE "+name+" AS "+query); err != nil {
				t.Errorf("%s, %s: %v", shape, e, err)
				continue
			}
			data, _ := liveFiles(t, env.admin, name)
			if i := slices.IndexFunc(data, func(p string) bool { return strings.Contains(p, "/igloo-write-") }); i >= 0 {
				t.Errorf("%s, %s: workers wrote %s", shape, e, data[i])
			}
			check := "SELECT count(*), min(CAST(v AS VARCHAR)), max(CAST(v AS VARCHAR)) FROM "
			got := env.local(t, check+name)
			if want := env.local(t, check+"("+query+") q"); got != want {
				t.Errorf("%s, %s: committed %s, this replica computes %s", shape, e, got, want)
			}
		}
	}

	// Statement time is fixed before the plan, so workers may compute it, and
	// every file of a statement holds the same.
	mustExec(t, env.admin, "CREATE TABLE sess_time AS SELECT id, count(DISTINCT x) AS n, CAST(now() AS VARCHAR) AS v FROM src GROUP BY id")
	env.checkWrote(t, "sess_time")
	if got := env.local(t, "SELECT count(DISTINCT v) FROM sess_time"); got != "1" {
		t.Errorf("a statement's rows hold %s times", got)
	}
}

// TestWorkersWriteNamedColumns checks that workers write an INSERT of some of a
// table's columns, in any order or by name, as DuckDB's would: NULL for the others.
func TestWorkersWriteNamedColumns(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	for _, c := range []struct{ name, insert, want string }{
		{"some", "INSERT INTO w_named (id, x) SELECT id, x FROM src", "SELECT id, x, NULL AS name, NULL AS g FROM src"},
		{"reordered", "INSERT INTO w_named (x, g, name, id) SELECT x, g, name, id FROM src", "SELECT id, x, name, g FROM src"},
		{"some reordered", "INSERT INTO w_named (name, id) SELECT name, id FROM src", "SELECT id, NULL AS x, name, NULL AS g FROM src"},
		{"by name", "INSERT INTO w_named BY NAME SELECT g, id, name, x FROM src", "SELECT id, x, name, g FROM src"},
		{"some by name", "INSERT INTO w_named BY NAME SELECT x, id FROM src", "SELECT id, x, NULL AS name, NULL AS g FROM src"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mustExec(t, admin, "DROP TABLE IF EXISTS w_named")
			mustExec(t, admin, "CREATE TABLE w_named (id BIGINT, x BIGINT, name VARCHAR, g BIGINT)")
			mustExec(t, admin, c.insert)
			if got, want := env.local(t, "SELECT * FROM w_named ORDER BY ALL"), env.local(t, "SELECT * FROM ("+c.want+") ORDER BY ALL"); got != want {
				t.Errorf("rows differ from those of %s", c.want)
			}
			env.checkWrote(t, "w_named")
		})
	}
}

// TestWorkersDoNotWrite checks that a fill stays with this replica's writes
// inside a transaction block, and over a source with a policy.
func TestWorkersDoNotWrite(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
	mustExec(t, admin, "CREATE TABLE w_tx (id BIGINT, x BIGINT)")
	mustExec(t, admin, "BEGIN")
	mustExec(t, admin, "INSERT INTO w_tx SELECT id, x FROM src")
	mustExec(t, admin, "COMMIT")
	if data, _ := liveFiles(t, admin, "w_tx"); slices.ContainsFunc(data, inDir) {
		t.Errorf("workers wrote inside a transaction block: %q", data)
	}
	if got, want := env.local(t, "SELECT count(*) FROM w_tx"), env.local(t, "SELECT count(*) FROM src"); got != want {
		t.Errorf("%s rows, want %s", got, want)
	}
}

// TestWorkersWriteAuto checks that a copy of a large table runs on workers in
// auto mode, now that they write it.
func TestWorkersWriteAuto(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	mustExec(t, env.admin, "CREATE TABLE w_auto AS SELECT id, x FROM src")
	env.force(t)
	if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_auto"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("%s, want %s", got, want)
	}
	env.checkWrote(t, "w_auto")
}

// TestWorkersWriteManyPartitionsAuto checks that auto mode has this replica write
// a table whose files would be small on workers, with the same rows, and workers
// one of few partitions, and that a statement forced onto workers still goes there.
func TestWorkersWriteManyPartitionsAuto(t *testing.T) {
	t.Parallel()
	env := startWriteServerWith(t, func(o *distributed.Options) { o.MinWorkerWrite = 1 << 20 })
	rows := "SELECT id, g, x FROM src"
	partitioned := func(table, by string) {
		mustExec(t, env.admin, "SET distributed_execution = off")
		mustExec(t, env.admin, "CREATE TABLE "+table+" AS "+rows+" LIMIT 0")
		mustExec(t, env.admin, "ALTER TABLE "+table+" SET PARTITIONED BY ("+by+")")
		mustExec(t, env.admin, "RESET distributed_execution")
	}
	partitioned("w_few", "g")
	mustExec(t, env.admin, "INSERT INTO w_few "+rows)
	env.checkWrote(t, "w_few")

	partitioned("w_many", "truncate(20, id)")
	mustExec(t, env.admin, "INSERT INTO w_many "+rows)
	data, _ := liveFiles(t, env.admin, "w_many")
	if len(data) == 0 {
		t.Fatal("no files")
	}
	for _, f := range data {
		if strings.Contains(f, "/"+distributed.SealedDirMark) {
			t.Fatalf("auto mode had workers write %d files of a table: %s", len(data), f)
		}
	}
	if got, want := env.local(t, "SELECT * FROM w_many ORDER BY ALL"), env.local(t, rows+" ORDER BY ALL"); got != want {
		t.Errorf("rows differ:\n%.300s\nwant\n%.300s", got, want)
	}

	partitioned("w_forced", "truncate(20, id)")
	env.force(t)
	mustExec(t, env.admin, "INSERT INTO w_forced "+rows)
	env.checkWrote(t, "w_forced")
	if data, _ := liveFiles(t, env.admin, "w_forced"); len(data) > 2*2000+2 {
		t.Errorf("%d files of 2,000 partitions on two workers", len(data))
	}
}

// erasedLedger says an erasure request has just deleted files.
type erasedLedger struct{}

func (erasedLedger) LatestDeletion(context.Context) (time.Time, error) { return time.Now(), nil }
func (erasedLedger) WorkerSeen(context.Context, string, string, string, time.Time) error {
	return nil
}
func (erasedLedger) Emptied(context.Context, string, time.Time, string) error { return nil }

// lateLedger says of the latest deletion what it was last told.
type lateLedger struct {
	erasedLedger
	at atomic.Int64
}

func (l *lateLedger) LatestDeletion(context.Context) (time.Time, error) {
	if n := l.at.Load(); n > 0 {
		return time.Unix(0, n), nil
	}
	return time.Time{}, nil
}

// TestWorkersWriteErasedAtCommit checks that a deletion in the ledger stops a
// commit about to land, though the coordinator would have heard of it only up to
// 15 s later: the statement's query runs here on the tables as they are.
func TestWorkersWriteErasedAtCommit(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
	ledger := &lateLedger{}
	ctx, stop := context.WithCancel(context.Background())
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		env.co.WatchErasures(ctx, ledger)
	}()
	defer func() {
		stop()
		<-watched
	}()
	// The first statement commits: nothing has been deleted.
	mustExec(t, admin, "CREATE TABLE w_gate AS SELECT id, x FROM src")
	data, _ := liveFiles(t, admin, "w_gate")
	if !slices.ContainsFunc(data, inDir) {
		t.Fatalf("workers did not write it: %q", data)
	}
	mustExec(t, admin, "CREATE TABLE w_gated (id BIGINT, x BIGINT)")
	env.srv.beforeWriteCommit = func(context.Context, string) error {
		ledger.at.Store(time.Now().UnixNano())
		return nil
	}
	defer func() { env.srv.beforeWriteCommit = nil }()
	mustExec(t, admin, "INSERT INTO w_gated SELECT id, x FROM src")
	if data, _ := liveFiles(t, admin, "w_gated"); slices.ContainsFunc(data, inDir) {
		t.Errorf("files tasks wrote were committed: %q", data)
	}
	if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_gated"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("%s, want %s", got, want)
	}
	env.checkNoNotes(t)
}

// TestWorkersWriteErased checks that what tasks wrote is not committed when an
// erasure request deleted files after they began: it runs here, and holds the
// rows as they are now.
func TestWorkersWriteErased(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	mustExec(t, admin, "CREATE TABLE w_erased (id BIGINT, x BIGINT)")
	var once sync.Once
	for _, f := range env.faults {
		f.set(func(f *writeFault) {
			f.answer = func(_, _ map[string]any) faultAction {
				once.Do(func() {
					if err := env.co.PurgeWorkers(context.Background(), erasedLedger{}); err != nil {
						t.Error(err)
					}
				})
				return faultNone
			}
		})
	}
	mustExec(t, admin, "INSERT INTO w_erased SELECT id, x FROM src")
	if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_erased"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("%s, want %s", got, want)
	}
	data, _ := liveFiles(t, admin, "w_erased")
	if slices.ContainsFunc(data, func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }) {
		t.Errorf("files tasks wrote were committed: %q", data)
	}
	if left := written(t, env.location(t, "w_erased")); len(left) != 0 {
		t.Errorf("files left: %q", left)
	}
	env.checkNoNotes(t)
}

// TestWorkersWriteOtherTargets checks that a statement whose target is not
// what workers can write stays with this replica: a temporary table of the
// name of a table of the catalog, which it must leave alone, and a table with
// a NOT NULL column, which a task's COPY would not check.
func TestWorkersWriteOtherTargets(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	ctx := context.Background()
	mustExec(t, admin, "CREATE TABLE w_shadow (id BIGINT, x BIGINT)")
	mustExec(t, admin, "CREATE TEMP TABLE w_shadow (id BIGINT, x BIGINT)")
	mustExec(t, admin, "INSERT INTO w_shadow SELECT id, x FROM src WHERE g = 1")
	if got := env.local(t, "SELECT count(*) FROM lake.public.w_shadow"); got != "0" {
		t.Errorf("the catalog's table of the name holds %s rows", got)
	}
	if got, want := env.local(t, "SELECT count(*) FROM temp.main.w_shadow"), env.local(t, "SELECT count(*) FROM src WHERE g = 1"); got != want {
		t.Errorf("the temporary table holds %s rows, want %s", got, want)
	}
	if left := written(t, env.location(t, "w_shadow")); len(left) != 0 {
		t.Errorf("files of workers: %q", left)
	}
	mustExec(t, admin, "DROP TABLE temp.main.w_shadow")

	mustExec(t, admin, "CREATE TABLE w_required (id BIGINT NOT NULL, x BIGINT)")
	if _, err := admin.Exec(ctx, "INSERT INTO w_required SELECT CAST(NULL AS BIGINT), x FROM src"); err == nil {
		t.Error("NULLs went into a NOT NULL column")
	}
	if got := env.local(t, "SELECT count(*) FROM w_required"); got != "0" {
		t.Errorf("a failed insert left %s rows", got)
	}
	mustExec(t, admin, "INSERT INTO w_required SELECT id, x FROM src WHERE g = 1")
	if got, want := env.local(t, "SELECT count(*) FROM w_required"), env.local(t, "SELECT count(*) FROM src WHERE g = 1"); got != want {
		t.Errorf("%s rows, want %s", got, want)
	}
	env.checkWrote(t, "w_required")
	if data, _ := liveFiles(t, admin, "w_required"); len(data) == 0 {
		t.Error("no file holds its rows")
	}
	env.checkNoNotes(t)
}

// TestWorkersWriteOff checks IGLOO_WORKER_WRITES=off, and that a worker that
// cannot use the credentials it is given leaves the table to this replica,
// with the credentials in no log.
func TestWorkersWriteOff(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, env *writeEnv) {
		t.Helper()
		env.force(t)
		mustExec(t, env.admin, "CREATE TABLE w_off AS SELECT id, x FROM src")
		if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_off"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
			t.Errorf("%s, want %s", got, want)
		}
		if left := written(t, env.location(t, "w_off")); len(left) != 0 {
			t.Errorf("files of workers: %q", left)
		}
		env.checkNoNotes(t)
	}
	t.Run("off", func(t *testing.T) {
		t.Parallel()
		check(t, startWriteServerWith(t, func(o *distributed.Options) { o.NoWorkerWrites = true }))
	})
	t.Run("credentials", func(t *testing.T) {
		t.Parallel()
		// Test workers have no httpfs, so no secret of type s3.
		var asked atomic.Int64
		check(t, startWriteServerWith(t, func(o *distributed.Options) {
			o.WriteCredentials = func(_ context.Context, dir string) (*distributed.WriteCredentials, error) {
				asked.Add(1)
				// An attempt's directory, the probe's here.
				if !regexp.MustCompile(`/` + distributed.WriteDirMark + `[0-9a-f]+/probe/a[0-9]+$`).MatchString(dir) {
					t.Errorf("credentials for %s", dir)
				}
				return &distributed.WriteCredentials{KeyID: "a-key", Secret: "a-secret", SessionToken: "a-token", Expires: time.Now().Add(time.Hour)}, nil
			}
		}))
		if asked.Load() == 0 {
			t.Error("no credentials were made")
		}
	})
}

// TestWorkersWriteLate checks that a statement looks again for what its tasks
// wrote after it ended, committed or not, keeps its note until then, and that
// a server that stops first leaves the note for the table's next write.
func TestWorkersWriteLate(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	var dir atomic.Pointer[string]
	var fail atomic.Bool
	env.srv.beforeWriteCommit = func(_ context.Context, d string) error {
		dir.Store(&d)
		if fail.Load() {
			return errors.New("no commit")
		}
		return nil
	}
	mustExec(t, admin, "CREATE TABLE w_late (id BIGINT, x BIGINT)")
	late := func(t *testing.T) string {
		t.Helper()
		p := filepath.Join(*dir.Load(), "q", "s9-9", "a0", "f", "part_0.parquet")
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("late"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, c := range []struct {
		name   string
		commit bool
	}{{"committed", true}, {"not committed", false}} {
		t.Run(c.name, func(t *testing.T) {
			fail.Store(!c.commit)
			defer fail.Store(false)
			release := make(chan struct{})
			env.srv.lateWritesWait = func() { <-release }
			mustExec(t, admin, "INSERT INTO w_late SELECT id, x FROM src WHERE g = 1")
			if notes := filesNotes(t, env.srv); len(notes) != 1 {
				t.Errorf("notes while tasks may still write: %+v", notes)
			}
			p := late(t)
			close(release)
			env.srv.lateWrites.Wait()
			if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("what a task wrote late is still there: %v", err)
			}
			data, _ := liveFiles(t, admin, "w_late")
			if c.commit && !slices.ContainsFunc(data, func(f string) bool { return strings.HasPrefix(f, sealedDir(*dir.Load())+"/") }) {
				t.Errorf("the committed files went: %q", data)
			}
			for _, f := range data {
				if _, err := os.Stat(f); err != nil {
					t.Errorf("a file of the table went: %v", err)
				}
			}
			env.checkNoNotes(t)
		})
	}
}

// TestLateWritesStop checks that a server that stops does not wait a while for
// a statement to look again: the note stays, for the table's next write.
func TestLateWritesStop(t *testing.T) {
	t.Parallel()
	s := &Server{lateWritesStop: make(chan struct{})}
	close(s.lateWritesStop)
	if s.waitLateWrites() {
		t.Error("it went on with the server stopped")
	}
}

// TestWorkersWriteSealed checks that a commit holds copies of what tasks wrote, out
// of their directories, and that overwriting what a task wrote, as its credentials
// allow for a while yet, changes nothing of the table, nor does anything that
// leaves the copies out.
func TestWorkersWriteSealed(t *testing.T) {
	t.Parallel()
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	var wrote []string
	for _, f := range env.faults {
		f.set(func(f *writeFault) {
			f.answer = func(_, head map[string]any) faultAction {
				files, _ := head["files"].([]any)
				for _, x := range files {
					p, _ := x.(map[string]any)["path"].(string)
					wrote = append(wrote, p)
				}
				return faultNone
			}
		})
	}
	mustExec(t, admin, "CREATE TABLE w_sealed (id BIGINT, x BIGINT)")
	mustExec(t, admin, "INSERT INTO w_sealed SELECT id, x FROM src")
	if len(wrote) == 0 {
		t.Fatal("no task wrote")
	}
	env.checkWrote(t, "w_sealed")
	for _, p := range wrote {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("what a task wrote is still there: %s, %v", p, err)
		}
		// Written again by a worker with its attempt's credentials.
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("tampered"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_sealed"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
		t.Errorf("%s, want %s", got, want)
	}
}

// TestForgedNoteDeletesNothing: a note of what a stopped write left, as whoever
// reaches the catalog itself can write one, deletes files only in a directory
// of the table it names, as Igloo makes them. Any other is left, and the note
// goes.
func TestForgedNoteDeletesNothing(t *testing.T) {
	env := startWriteServer(t)
	env.force(t)
	admin := env.admin
	ctx := context.Background()
	mustExec(t, admin, "CREATE TABLE w_victim (id BIGINT, x BIGINT)")
	mustExec(t, admin, "INSERT INTO w_victim SELECT id, x FROM src")
	mustExec(t, admin, "CREATE TABLE w_mine (id BIGINT, x BIGINT)")
	loc := env.location(t, "w_victim")
	before := written(t, loc)
	if len(before) == 0 {
		t.Fatal("workers wrote none of the victim's files: the test shows nothing")
	}
	base := strings.TrimSuffix(strings.TrimPrefix(loc, "file://"), "/") + "/data/"
	// The victim's own statement's directory, as its note named it.
	own := strings.Replace(regexp.MustCompile(".*/"+distributed.SealedDirMark+"[0-9a-f]+").FindString(before[0]), distributed.SealedDirMark, distributed.WriteDirMark, 1)
	for _, dir := range []string{base + distributed.WriteDirMark + "*", own} {
		note := catalog.Pending{Key: "igloo.pending.forged", Kind: catalog.PendingFiles, Name: dir, Target: "w_mine", Host: "gone", Process: "gone",
			At: time.Now().Add(-2 * catalog.PendingStale)}
		if err := env.srv.ddl.NotePending(ctx, "public", note); err != nil {
			t.Fatal(err)
		}
		mustExec(t, admin, "INSERT INTO w_mine SELECT id, x FROM src")
		env.srv.lateWrites.Wait()
		if n := len(filesNotes(t, env.srv)); n != 0 {
			t.Errorf("%s: %d notes left", dir, n)
		}
		if after := written(t, loc); !slices.Equal(after, before) {
			t.Errorf("a note of %s deleted the victim's files: %d left of %d", dir, len(after), len(before))
		}
	}
	if got := count(t, admin, "SELECT count(*) FROM w_victim"); got != count(t, admin, "SELECT count(*) FROM src") {
		t.Errorf("the victim holds %d rows", got)
	}

	// What a stopped write of its own left goes.
	left := strings.TrimSuffix(strings.TrimPrefix(env.location(t, "w_mine"), "file://"), "/") + "/data/" + distributed.WriteDirMark + "0123456789abcdef"
	conn, err := env.srv.engine.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	if _, err := conn.ExecContext(ctx, "COPY (SELECT 1 AS id, 2 AS x) TO "+quoteLiteral(left)+" (FORMAT parquet, FILENAME_PATTERN 'part_{i}', FILE_SIZE_BYTES 1000000)", nil); err != nil {
		t.Fatal(err)
	}
	note := catalog.Pending{Key: "igloo.pending.stopped", Kind: catalog.PendingFiles, Name: left, Target: "w_mine", Host: "gone", Process: "gone",
		At: time.Now().Add(-2 * catalog.PendingStale)}
	if err := env.srv.ddl.NotePending(ctx, "public", note); err != nil {
		t.Fatal(err)
	}
	mustExec(t, admin, "INSERT INTO w_mine SELECT id, x FROM src")
	env.srv.lateWrites.Wait()
	if n := filesIn(t, left); n != 0 {
		t.Errorf("%d files a stopped write left are still there", n)
	}
}

// wideRows are rows of about 10.8 KB, 432 MB of them: row groups as DuckDB
// gathers them hold them all.
const wideRows = "SELECT id, g, x, repeat(name, 1800) AS c FROM src"

// checkWide checks that table holds what wideRows does.
func (e *writeEnv) checkWide(t *testing.T, table string) {
	t.Helper()
	sum := "SELECT count(*), sum(length(c)), sum(x), bit_xor(hash(id, c)) FROM "
	if got, want := e.local(t, sum+table), e.local(t, sum+"("+wideRows+")"); got != want {
		t.Errorf("%s: %s, want %s", table, got, want)
	}
}

// TestWorkersWriteWideRows checks that workers of a DuckDB of 256 MB write rows
// a row group of 122,880 of which would take 1.3 GB: the task bounds its row
// groups by the bytes it has, partitioned or not, in auto mode and when forced.
func TestWorkersWriteWideRows(t *testing.T) {
	t.Parallel()
	env := startWriteServerOn(t, writeWorkers{cores: 2, memory: 256 << 20, limit: "256MB"}, nil)
	admin := env.admin
	t.Run("forced", func(t *testing.T) {
		env.force(t)
		mustExec(t, admin, "CREATE TABLE w_wide AS "+wideRows)
		env.checkWide(t, "w_wide")
		env.checkWrote(t, "w_wide")
	})
	t.Run("partitioned", func(t *testing.T) {
		env.force(t)
		mustExec(t, admin, "CREATE TABLE w_wide_p AS "+wideRows+" LIMIT 0")
		mustExec(t, admin, "ALTER TABLE w_wide_p SET PARTITIONED BY (g)")
		mustExec(t, admin, "INSERT INTO w_wide_p "+wideRows)
		env.checkWide(t, "w_wide_p")
		env.checkWrote(t, "w_wide_p")
	})
	t.Run("auto", func(t *testing.T) {
		mustExec(t, admin, "RESET distributed_execution")
		mustExec(t, admin, "CREATE TABLE w_wide_auto AS "+wideRows)
		env.checkWide(t, "w_wide_auto")
		env.checkNoNotes(t)
	})
}

// TestWorkersOutOfMemoryOnWrite checks that a fill that runs out of memory on
// workers, after its retries with smaller tasks, runs on this replica, on every
// way a table is filled: here with workers that do not know their memory is
// 64 MB, so that no task bounds what it holds, and whose out of memory is real.
// (At 128 MB, the retries with smaller tasks succeed.)
func TestWorkersOutOfMemoryOnWrite(t *testing.T) {
	t.Parallel()
	env := startWriteServerOn(t, writeWorkers{cores: 2, memory: 0, limit: "64MB"}, nil)
	env.force(t)
	admin := env.admin
	inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
	leftAlone := func(t *testing.T, table string) {
		t.Helper()
		env.checkWide(t, table)
		data, _ := liveFiles(t, admin, table)
		if slices.ContainsFunc(data, inDir) {
			t.Errorf("workers wrote it: %q", data)
		}
		if left := written(t, env.location(t, table)); len(left) != 0 {
			t.Errorf("files left: %q", left)
		}
		env.checkNoNotes(t)
	}
	t.Run("create", func(t *testing.T) {
		mustExec(t, admin, "CREATE TABLE w_oom AS "+wideRows)
		leftAlone(t, "w_oom")
	})
	t.Run("replace", func(t *testing.T) {
		mustExec(t, admin, "CREATE OR REPLACE TABLE w_oom AS "+wideRows)
		leftAlone(t, "w_oom")
	})
	t.Run("insert", func(t *testing.T) {
		mustExec(t, admin, "CREATE TABLE w_oom_i AS "+wideRows+" LIMIT 0")
		mustExec(t, admin, "INSERT INTO w_oom_i "+wideRows)
		leftAlone(t, "w_oom_i")
	})
	t.Run("materialized view", func(t *testing.T) {
		mustExec(t, admin, "CREATE MATERIALIZED VIEW w_oom_mv AS SELECT i AS id, g, x, repeat(name, 1800) AS c FROM lake.public.src AS o(i)")
		mustExec(t, admin, "REFRESH MATERIALIZED VIEW w_oom_mv")
		env.checkWide(t, "w_oom_mv")
		env.checkNoNotes(t)
	})
}

// TestWorkersShortOfMemoryWriting checks what follows tasks that run out of
// memory only as they write. A statement this replica has the room for runs
// here. One it does not, by its estimates, runs on workers again, with this
// replica writing the table from their results.
func TestWorkersShortOfMemoryWriting(t *testing.T) {
	t.Parallel()
	short := func(env *writeEnv) {
		for _, f := range env.faults {
			f.set(func(f *writeFault) { f.memory = true })
		}
	}
	shortAnswers := func(env *writeEnv) int64 { return env.faults[0].short.Load() + env.faults[1].short.Load() }
	inDir := func(f string) bool { return strings.Contains(f, "/"+distributed.SealedDirMark) }
	t.Run("room here", func(t *testing.T) {
		t.Parallel()
		env := startWriteServer(t)
		env.force(t)
		short(env)
		mustExec(t, env.admin, "CREATE TABLE w_short AS SELECT id, x FROM src")
		if shortAnswers(env) == 0 {
			t.Fatal("no task wrote")
		}
		if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_short"), env.local(t, "SELECT count(*), sum(x) FROM src"); got != want {
			t.Errorf("%s, want %s", got, want)
		}
		if data, _ := liveFiles(t, env.admin, "w_short"); slices.ContainsFunc(data, inDir) {
			t.Errorf("files of tasks: %q", data)
		}
		env.checkNoNotes(t)
	})
	t.Run("no room here", func(t *testing.T) {
		t.Parallel()
		// By the estimates: this replica's 1 MB would not hold what the
		// exchange's stage reads.
		env := startWriteServerWith(t, func(o *distributed.Options) { o.MemoryBytes = 1 << 20 })
		env.force(t)
		short(env)
		query := "SELECT a.id, b.x FROM src AS a JOIN src AS b ON a.id = b.id"
		mustExec(t, env.admin, "CREATE TABLE w_short AS "+query)
		if shortAnswers(env) == 0 {
			t.Fatal("no task wrote")
		}
		if got, want := env.local(t, "SELECT count(*), sum(x) FROM w_short"), env.local(t, "SELECT count(*), sum(b.x) FROM src AS a JOIN src AS b ON a.id = b.id"); got != want {
			t.Errorf("%s, want %s", got, want)
		}
		if data, _ := liveFiles(t, env.admin, "w_short"); slices.ContainsFunc(data, inDir) {
			t.Errorf("files of tasks: %q", data)
		}
		env.checkNoNotes(t)
	})
}
