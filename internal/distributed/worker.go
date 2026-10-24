package distributed

import (
	"bufio"
	"cmp"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/deletefiles"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/snowflake"
)

// A worker runs fragments for coordinators. It is an Igloo process with a
// DuckDB that is locked down as the server's is (no local files, no
// locations outside the warehouse) and attached to no catalog: tasks name
// the data files to read, so a worker reads exactly the files of the
// tables the coordinator authorized, at the snapshot the coordinator
// planned. It accepts tasks only over mutual TLS from holders of the worker
// secret, and keeps nothing between tasks.

// Connector opens DuckDB connections; *engine.Engine implements it.
type Connector interface {
	Connect(ctx context.Context) (*duckdb.Conn, error)
}

// WorkerOptions configures a worker.
type WorkerOptions struct {
	Engine Connector
	// Secret is the shared worker secret (IGLOO_WORKER_SECRET).
	Secret  string
	Version string
	// ID names the worker; empty means the host name.
	ID string
	// TempDir holds results until they are sent. It must be one of the
	// directories DuckDB may write.
	TempDir string
	// Locations are the prefixes data files may have: the warehouse and
	// the allowed locations.
	Locations []string
	// Cores and MemoryBytes are reported to coordinators, which estimate
	// how long workers take from their cores and size the tables every
	// worker reads in full by the smallest worker's memory. Zero cores
	// means those Go may use (runtime.GOMAXPROCS).
	Cores       int
	MemoryBytes int64
	// Threads are DuckDB's threads, reported to coordinators, which
	// estimate how fast workers read object storage cold by their threads
	// a core; zero for unknown.
	Threads int
	// ExchangeBytes caps what exchanges on its disk, written or fetched,
	// may take there (IGLOO_EXCHANGE_MAX_SIZE), as on a volume whose size
	// its free space does not show; zero leaves them to the free space.
	ExchangeBytes int64
	// Tasks is how many tasks run at once; others wait. Zero means as many
	// as Cores and MemoryBytes allow (taskSlots).
	Tasks   int
	Metrics *WorkerMetrics
	// Prefetch, if set, is called as a task starts to run with what it
	// reads of catalog tables' data files, which the coordinator describes
	// with every task: one Scan per view, named by the view, of the files
	// the worker may read. It must return at once, reading the files in the
	// background until ctx, the task's, is done. DuckDB reads the files'
	// footers first (see readByName and checkRows), then the columns.
	// Exchanges' files are left out: DuckDB reads them around the read
	// cache (see engine's cache.go), so blocks read ahead into it would go
	// unread, as 65 GB of a report's did at ten times the data, or they
	// are on workers' disks already.
	Prefetch func(ctx context.Context, scans []Scan)
}

// Worker serves tasks.
type Worker struct {
	opts      WorkerOptions
	transport *transport
	log       *slog.Logger
	activity  *activity.Tracker
	srv       *http.Server
	// beforeTask, if set, runs before each task; tests use it to make a
	// worker fail or hang. r's context is the task's.
	beforeTask func(rw http.ResponseWriter, r *http.Request) bool

	// slots bound the tasks that run at once (see taskSlots); running are
	// those running, which ShedMemory may cancel.
	slots     chan struct{}
	runningMu sync.Mutex
	running   map[*runningTask]bool

	// footerCache holds data files' footers by path; see footers.
	footersMu   sync.Mutex
	footerCache map[string]*footer

	// local are the exchange files it wrote to its disk, peers what fetches
	// those of other workers, into fetchRoot (see localx.go); stopSweep
	// ends their sweeps.
	local     *localExchanges
	peers     *http.Client
	fetchRoot string
	stopSweep chan struct{}
	stopOnce  sync.Once
	// boot tells this run of the worker from others at its address: one
	// that restarted holds no exchange files.
	boot string
	// httpfsOnce finds whether DuckDB has the httpfs extension loaded,
	// whose settings storageRetries are; without it, the worker reads no
	// object storage.
	httpfsOnce sync.Once
	httpfs     bool
}

// NewWorker returns a worker; call Serve to accept tasks.
func NewWorker(opts WorkerOptions, log *slog.Logger) (*Worker, error) {
	t, err := newTransport(opts.Secret)
	if err != nil {
		return nil, err
	}
	if opts.TempDir == "" {
		return nil, errors.New("a worker needs a temporary directory for results")
	}
	if len(opts.Locations) == 0 {
		return nil, errors.New("a worker needs the warehouse location")
	}
	if opts.ID == "" {
		opts.ID, _ = os.Hostname()
	}
	if opts.Cores <= 0 {
		opts.Cores = runtime.GOMAXPROCS(0)
	}
	dir := filepath.Join(opts.TempDir, "igloo-worker")
	// Results of tasks that were running when the worker stopped.
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create result directory: %w", err)
	}
	opts.TempDir = dir
	local, err := newLocalExchanges(filepath.Join(dir, "exchanges"))
	if err != nil {
		return nil, err
	}
	local.limit = opts.ExchangeBytes
	fetchRoot := filepath.Join(dir, "fetched")
	if err := os.MkdirAll(fetchRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create directory for fetched exchanges: %w", err)
	}
	w := &Worker{opts: opts, transport: t, log: log.With("component", "worker"), activity: activity.New(),
		slots: make(chan struct{}, cmp.Or(opts.Tasks, taskSlots(opts.Cores, opts.MemoryBytes))),
		local: local, fetchRoot: filepath.Clean(fetchRoot), stopSweep: make(chan struct{}),
		peers: &http.Client{
			Transport: &http.Transport{TLSClientConfig: t.client.Clone(), MaxIdleConnsPerHost: fetchesPerTask, IdleConnTimeout: time.Minute,
				// A worker answers at once, if only that it is busy.
				ResponseHeaderTimeout: stallTimeout},
			// A peer answers with the file or not at all.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		boot: newID()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+tasksPath, w.serveTask)
	mux.HandleFunc("GET "+statusPath, w.serveStatus)
	mux.HandleFunc("GET "+exchangesPath+"{qid}/{file...}", w.serveExchangeFile)
	mux.HandleFunc("POST "+exchangesPath+"{qid}/keep", w.serveExchangeKeep)
	mux.HandleFunc("DELETE "+exchangesPath+"{qid}", w.serveExchangeRemove)
	w.srv = &http.Server{
		Handler:           mux,
		TLSConfig:         t.server,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}
	return w, nil
}

// Serve accepts coordinators' connections on ln until Shutdown.
func (w *Worker) Serve(ln net.Listener) error {
	go w.sweepLoop(w.stopSweep)
	err := w.srv.ServeTLS(ln, "", "")
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting tasks and waits for running ones until ctx is
// done.
func (w *Worker) Shutdown(ctx context.Context) error {
	w.stopOnce.Do(func() { close(w.stopSweep) })
	return w.srv.Shutdown(ctx)
}

func (w *Worker) serveStatus(rw http.ResponseWriter, _ *http.Request) {
	running, idle := w.activity.Snapshot()
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(workerStatus{
		ID: w.opts.ID, Version: w.opts.Version, Cores: w.opts.Cores, MemoryBytes: w.opts.MemoryBytes, Threads: w.opts.Threads, Slots: cap(w.slots),
		Boot:    w.boot,
		Running: int(running), IdleSeconds: idle.Seconds(),
	})
}

// maxTaskRequest bounds a task's JSON: the file lists of large tables.
const maxTaskRequest = 256 << 20

// taskSlots is how many tasks a worker runs at once. DuckDB runs each on
// all of its threads, so more at once add little speed and much memory,
// which they share: the stages of a report that run side by side each have
// a task per partition on every worker. Three workers of 500 MB running a
// cohort report each spilled 1.9 GB with every task at once, and 0.9 GB
// one at a time; with four times the partitions, all at once ran out of
// memory and one at a time spilled nothing. A task gets at least 4 cores
// and 8 GiB of the memory limit, and waits for them. Cores, not threads:
// a worker reading object storage runs several threads a core.
func taskSlots(cores int, memory int64) int {
	n := max(1, cores/4)
	if memory > 0 {
		n = min(n, max(1, int(memory/(8<<30))))
	}
	return n
}

// runningTask is a task while it runs, for ShedMemory.
type runningTask struct {
	query   string
	task    int
	started time.Time
	cancel  context.CancelCauseFunc
}

// errTaskMemory is what ShedMemory cancels a task with.
var errTaskMemory = errors.New("the worker was running out of memory, and this task had run the longest there")

// ShedMemory cancels the task that has run longest on this worker, for
// memlimit.Watch, when the process is about to run out of memory: DuckDB
// keeps to its memory limit only for what its buffer manager tracks, and
// the kernel would otherwise kill the worker, and every task on it with it.
// The task fails with an out-of-memory error, which fails its statement
// (OutOfMemory). It reports whether a task was running.
func (w *Worker) ShedMemory(available int64) bool {
	w.runningMu.Lock()
	var longest *runningTask
	for t := range w.running {
		if longest == nil || t.started.Before(longest.started) {
			longest = t
		}
	}
	if longest == nil {
		w.runningMu.Unlock()
		w.log.Warn("memory is running out, and no task is running to cancel", "available", memlimit.Format(available))
		return false
	}
	// The next shed, if memory keeps running out, picks another.
	delete(w.running, longest)
	t := *longest
	w.runningMu.Unlock()
	t.cancel(errTaskMemory)
	w.log.Warn("memory is running out; canceled the task that had run longest", "available", memlimit.Format(available),
		"query", t.query, "task", t.task, "running_for", time.Since(t.started).Round(time.Millisecond))
	return true
}

// begin records a task as running, until the function it returns is
// called.
func (w *Worker) begin(t *runningTask) func() {
	w.runningMu.Lock()
	if w.running == nil {
		w.running = map[*runningTask]bool{}
	}
	w.running[t] = true
	w.runningMu.Unlock()
	return func() {
		w.runningMu.Lock()
		delete(w.running, t)
		w.runningMu.Unlock()
	}
}

func (w *Worker) serveTask(rw http.ResponseWriter, r *http.Request) {
	defer w.activity.Begin()()
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	r = r.WithContext(ctx)
	if w.beforeTask != nil && !w.beforeTask(rw, r) {
		return
	}
	start := time.Now()
	var req taskRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxTaskRequest)).Decode(&req); err != nil {
		w.fail(rw, http.StatusBadRequest, &taskErr{Message: "bad task: " + err.Error()})
		return
	}
	// Reading the request to its end lets the server notice when the
	// coordinator goes away, which cancels r's context and so the task.
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	if req.Version != w.opts.Version {
		w.fail(rw, http.StatusConflict, &taskErr{Message: fmt.Sprintf("worker runs Igloo %s, coordinator %s", w.opts.Version, req.Version)})
		return
	}
	log := w.log.With("query", req.Query, "task", req.Task)
	// Wait for a slot (see taskSlots); the coordinator's task waits with it.
	select {
	case w.slots <- struct{}{}:
	case <-ctx.Done():
		log.Debug("task canceled while it waited to run")
		w.opts.Metrics.task("canceled", start)
		return
	}
	defer func() { <-w.slots }()
	done := w.begin(&runningTask{query: req.Query, task: req.Task, started: time.Now(), cancel: cancel})
	defer done()
	path, res, err := w.run(ctx, &req)
	done()
	if path != "" {
		defer os.Remove(path) //nolint:errcheck
	}
	if err != nil && errors.Is(context.Cause(ctx), errTaskMemory) {
		// Another worker would run out of memory the same way: the
		// coordinator fails the statement rather than run it again.
		log.Warn("task canceled for memory", "err", err)
		w.opts.Metrics.task("out_of_memory", start)
		w.fail(rw, http.StatusUnprocessableEntity, &taskErr{Message: errTaskMemory.Error(), OutOfMemory: true})
		return
	}
	if err != nil {
		te := &taskErr{Message: err.Error()}
		var lost *lostError
		if errors.As(err, &lost) && r.Context().Err() == nil {
			log.Warn("task could not fetch exchanged rows from a worker", "worker", lost.worker, "err", err)
			w.opts.Metrics.task("error", start)
			te.Lost = lost.worker
			w.fail(rw, http.StatusUnprocessableEntity, te)
			return
		}
		var de *duckdb.Error
		if errors.As(err, &de) {
			te.Type, te.Message, te.Data = int(de.Type), de.Msg, dataError(err)
			// Bitmaps over their budget are no shortage of the worker's
			// memory: the session runs the statement again without them.
			te.Bitmap = strings.Contains(de.Msg, bitmapBudgetError)
			te.Data = te.Data && !te.Bitmap
			// DuckDB out of its memory limit fails the task as the memory
			// watch would have: the coordinator runs the statement again
			// over smaller tasks, or fails it, rather than run it whole.
			te.OutOfMemory = de.Type == duckdb.ErrorTypeOutOfMemory && !te.Bitmap
		}
		if r.Context().Err() != nil {
			log.Debug("task canceled", "err", err)
			w.opts.Metrics.task("canceled", start)
			return
		}
		if IsUnsupported(err) {
			log.Debug("task refused", "err", err)
			te.Unsupported = true
			w.opts.Metrics.task("refused", start)
			w.fail(rw, http.StatusUnprocessableEntity, te)
			return
		}
		if te.Bitmap {
			log.Warn("task's bitmaps did not fit in memory", "err", err)
			w.opts.Metrics.task("bitmap_memory", start)
			w.fail(rw, http.StatusUnprocessableEntity, te)
			return
		}
		if te.OutOfMemory {
			log.Warn("task ran out of memory", "err", err)
			w.opts.Metrics.task("out_of_memory", start)
			w.fail(rw, http.StatusUnprocessableEntity, te)
			return
		}
		if !te.Data && throttled(err) {
			// The coordinator runs it again in a while.
			log.Warn("object storage turned a task's requests away", "err", err)
			te.Throttled = true
			w.opts.Metrics.task("throttled", start)
			w.fail(rw, http.StatusUnprocessableEntity, te)
			return
		}
		log.Warn("task failed", "err", err)
		w.opts.Metrics.task("error", start)
		w.fail(rw, http.StatusUnprocessableEntity, te)
		return
	}
	if req.Shuffle != "" {
		// The rows are in object storage; the response lists the files.
		head, _ := json.Marshal(res)
		rw.Header().Set("Content-Type", "application/json")
		rw.Header().Set("Content-Length", strconv.Itoa(len(head)+1))
		_, _ = rw.Write(append(head, '\n'))
		w.opts.Metrics.task("ok", start)
		log.Debug("shuffle task done", "rows", res.Rows, "files", len(res.Files), "seconds", time.Since(start).Seconds())
		return
	}

	f, err := os.Open(path)
	if err != nil {
		w.fail(rw, http.StatusInternalServerError, &taskErr{Message: err.Error()})
		return
	}
	defer f.Close() //nolint:errcheck
	info, err := f.Stat()
	if err != nil {
		w.fail(rw, http.StatusInternalServerError, &taskErr{Message: err.Error()})
		return
	}
	head, _ := json.Marshal(res)
	head = append(head, '\n')
	// A length lets the coordinator tell a result cut short by a dropped
	// connection from a whole one.
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.FormatInt(int64(len(head))+info.Size(), 10))
	bw := bufio.NewWriterSize(rw, 1<<20)
	_, _ = bw.Write(head)
	n, err := io.Copy(bw, f)
	if err == nil {
		err = bw.Flush()
	}
	if err != nil {
		log.Debug("could not send result", "err", err)
		w.opts.Metrics.task("canceled", start)
		return
	}
	w.opts.Metrics.task("ok", start)
	w.opts.Metrics.sent(n)
	log.Debug("task done", "rows", res.Rows, "bytes", n, "seconds", time.Since(start).Seconds())
}

func (w *Worker) fail(rw http.ResponseWriter, status int, te *taskErr) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(taskResult{Error: te})
}

// extensionLoaded reports whether DuckDB has the extension name loaded.
func extensionLoaded(ctx context.Context, conn *duckdb.Conn, name string) bool {
	rows, err := conn.QueryContext(ctx, "SELECT count(*) FROM duckdb_extensions() WHERE extension_name = "+quoteLiteral(name)+" AND loaded", nil)
	if err != nil {
		return false
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if rows.Next(dest) != nil {
		return false
	}
	n, _ := dest[0].(int64)
	return n > 0
}

// taskFault, if set, fails a task with its error before it runs, for
// tests.
var taskFault func(req *taskRequest) error

// storageRetries are DuckDB's retries, in a task's session, of a request
// that object storage turns away for now (HTTP 429 or 5xx, S3's 503
// SlowDown among them): 8 more tries, the second after 250 ms and each
// after twice as long as the one before, half a minute in all. DuckDB's
// own (3 more, from 0 to 400 ms) give up within half a second, as a HEAD
// of an exchange's file did on 16 workers at a hundred times the data,
// failing the statement. Past them, the coordinator runs the task again in
// a while (see dispatch).
var storageRetries = []string{"SET http_retries = 8", "SET http_retry_wait_ms = 250", "SET http_retry_backoff = 2"}

// run runs a task on a connection of its own and writes the result to a
// Parquet file, whose path it returns; a shuffle task writes it to object
// storage instead, one directory per partition, and lists the files.
func (w *Worker) run(ctx context.Context, req *taskRequest) (string, *taskResult, error) {
	if taskFault != nil {
		if err := taskFault(req); err != nil {
			return "", nil, err
		}
	}
	if w.opts.Prefetch != nil {
		if scans := w.prefetchScans(req); len(scans) > 0 {
			w.opts.Prefetch(ctx, scans)
		}
	}
	conn, err := w.opts.Engine.Connect(ctx)
	if err != nil {
		return "", nil, err
	}
	defer conn.Close() //nolint:errcheck
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}

	w.httpfsOnce.Do(func() { w.httpfs = extensionLoaded(ctx, conn, "httpfs") })
	if w.httpfs {
		for _, stmt := range storageRetries {
			if err := exec(stmt); err != nil {
				return "", nil, fmt.Errorf("set up retries of object storage's requests: %w", err)
			}
		}
	}
	for _, s := range req.Settings {
		if !knownSetting(s.Name) {
			return "", nil, fmt.Errorf("setting %s is not one tasks may change", s.Name)
		}
		if err := exec("SET " + quoteIdent(s.Name) + " = " + quoteLiteral(s.Value)); err != nil {
			return "", nil, fmt.Errorf("set %s: %w", s.Name, err)
		}
	}
	if err := w.setVariables(ctx, conn, req.Variables); err != nil {
		return "", nil, err
	}
	sfMacros, err := checkTaskSQL(ctx, conn, req)
	if err != nil {
		return "", nil, err
	}
	if sfMacros {
		// Igloo's own macros, which statements translated from Snowflake's
		// dialect call (FLATTEN among them), as every session has them.
		for _, stmt := range snowflake.Macros() {
			if err := exec(stmt); err != nil {
				return "", nil, fmt.Errorf("define Snowflake's functions: %w", err)
			}
		}
	}
	start := time.Now()
	release, err := w.fetchExchanges(ctx, req)
	if err != nil {
		return "", nil, err
	}
	defer release()
	var byID []string
	for i, v := range req.Views {
		if v.Exact {
			if err := checkRows(ctx, conn, v.Files, v.Rows); err != nil {
				return "", nil, fmt.Errorf("view %s: %w", v.Name, err)
			}
		}
		if !v.ByName && len(v.Files) > 0 {
			if v.ByName, err = w.readByName(ctx, conn, v); err != nil {
				return "", nil, err
			}
			if !v.ByName {
				byID = append(byID, v.Name)
			}
		}
		deleted := ""
		if len(v.Deletes) > 0 {
			deleted = fmt.Sprintf("__igloo_deleted_%d", i)
			if err := w.deletedRows(ctx, conn, v, deleted); err != nil {
				return "", nil, err
			}
		}
		stmt, err := w.viewStatement(v, deleted)
		if err != nil {
			return "", nil, err
		}
		if err := exec(stmt); err != nil {
			return "", nil, fmt.Errorf("define %s: %w", v.Name, err)
		}
	}
	w.log.Debug("defined a task's views", "query", req.Query, "task", req.Task, "views", len(req.Views),
		"by_field_id", byID, "seconds", time.Since(start).Seconds())

	query := req.SQL
	if len(req.Columns) > 0 {
		names := make([]string, len(req.Columns))
		for i, c := range req.Columns {
			names[i] = quoteIdent(c)
		}
		query = "SELECT * FROM (" + query + ") AS __igloo_f(" + strings.Join(names, ", ") + ")"
	}
	cols, err := describe(ctx, conn, query)
	if err != nil {
		return "", nil, err
	}
	if req.Shuffle != "" && req.Local != "" {
		res, err := w.shuffleLocal(ctx, conn, req, query, cols)
		return "", res, err
	}
	if req.Shuffle != "" {
		res, err := w.shuffle(ctx, conn, req.Shuffle, query, cols, req.Partitions)
		return "", res, err
	}
	// Columns Parquet would change cross as text; see transferType.
	sel := make([]string, len(cols))
	for i, c := range cols {
		t, err := transferType(c.Type)
		if err != nil {
			return "", nil, err
		}
		sel[i] = quoteIdent(c.Name)
		if t != c.Type {
			sel[i] = "CAST(" + quoteIdent(c.Name) + " AS " + t + ") AS " + quoteIdent(c.Name)
		}
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	path := filepath.Join(w.opts.TempDir, hex.EncodeToString(b[:])+".parquet")
	res, err := conn.ExecContext(ctx, "COPY (SELECT "+strings.Join(sel, ", ")+" FROM ("+query+")) TO "+
		quoteLiteral(path)+" (FORMAT parquet)", nil)
	if err != nil {
		_ = os.Remove(path)
		return "", nil, err
	}
	rows, _ := res.RowsAffected()
	return path, &taskResult{Columns: cols, Rows: rows}, nil
}

// shuffleRowGroup is how many rows a shuffle task writes to a row group.
// DuckDB's partitioned COPY holds a row group of every partition's rows, as
// they are, until it fills: 128 partitions of rows of 264 bytes held 3 GB,
// as much as DuckDB's limit, in its row groups of 122,880 rows, and 0.4 GB
// in groups of 8,192, which took no longer to write. (Workers of 24.6 GiB
// that ran out of memory over 128 partitions at a hundred times the data
// were short of it for the uploads' buffers: see uploadBuffer.) The row
// groups of all partitions get a quarter of the task's
// share of the worker's memory (the limit over its slots), at the width of
// the rows' types, between 2,048 rows and DuckDB's 122,880.
func shuffleRowGroup(memory int64, slots, parts int, cols []Column) int64 {
	const most = 122_880
	if memory <= 0 {
		return most
	}
	if parts <= 0 {
		parts = maxPartitions
	}
	n := memory / int64(max(slots, 1)) / 4 / int64(parts) / max(rowWidth(cols), 1)
	return min(max(n, minShuffleRowGroup), most)
}

// minShuffleRowGroup is the fewest rows a shuffle task writes to a row
// group; the coordinator gives exchanges no more partitions than leave
// their writers room for groups this size (see writerPartitions).
const minShuffleRowGroup = 2_048

// shuffleRowGroupBytes is how many bytes of rows, as written, a shuffle
// task's row group of a partition holds at most: those of all partitions
// within a quarter of the task's share of the worker's memory, as
// shuffleRowGroup counts rows by their types' widths, but by the bytes the
// rows take. A list's type counts eight short strings (typeWidth); at a
// hundred times the data, a report's rows of four lists of strings took
// workers out of memory while their tasks wrote 202 partitions, DuckDB's
// own memory under its limit. Rows of four lists of 22 strings of 40
// bytes, groups of rows sized by types held 2.1 GB a task over 202
// partitions, by bytes 1.2 GB. It is 0, no bound, when the memory is not
// known.
func shuffleRowGroupBytes(memory int64, slots, parts int) int64 {
	if memory <= 0 {
		return 0
	}
	if parts <= 0 {
		parts = maxPartitions
	}
	return max(memory/int64(max(slots, 1))/4/int64(parts), 1<<20)
}

// shuffleFlush is how many rows each of a shuffle task's threads gathers
// before it hands them to the partitions' writers. DuckDB's 524,288 held
// every row of a task of rows of 400 strings, 8.8 GB of a million rows, and
// took a writer of 3 GB to its limit for minutes. Those of all threads get
// a quarter of the task's share of the worker's memory, at the width of the
// rows' types, between 16,384 rows (fewer took longer) and DuckDB's own.
func shuffleFlush(memory int64, slots, threads int, cols []Column) int64 {
	const most, least = 524_288, 16_384
	if memory <= 0 {
		return most
	}
	n := memory / int64(max(slots, 1)) / 4 / int64(max(threads, 1)) / max(rowWidth(cols), 1)
	return min(max(n, least), most)
}

// DuckDB writes a file to object storage as a multipart upload, holding a
// buffer of a part for every file it has open, and sizes parts for the
// largest file it allows over its 10,000 parts: 76.5 MiB at its default of
// 800 GB. A shuffle task has a file a partition open: at 243 partitions a
// task held 18 GiB of buffers for files of a few megabytes, and workers of
// 24.6 GiB running two tasks ran out of memory, as at 128 they had not.
// Shuffle tasks allow files of 50 GB, the least DuckDB takes, in parts of
// uploadBuffer; no partition's file of a task comes near that.
const (
	maxUploadFile = "50GB"
	uploadBuffer  = 5_505_024
)

// uploadsInParts reports whether DuckDB writes files under dir as
// multipart uploads, with a buffer of a part each.
func uploadsInParts(dir string) bool {
	for _, scheme := range []string{"s3://", "s3a://", "s3n://", "gs://", "gcs://", "r2://"} {
		if strings.HasPrefix(dir, scheme) {
			return true
		}
	}
	return false
}

// threadsOf returns DuckDB's threads on conn, or 1 if it cannot tell.
func threadsOf(ctx context.Context, conn *duckdb.Conn) int {
	rows, err := conn.QueryContext(ctx, "SELECT current_setting('threads')::BIGINT", nil)
	if err != nil {
		return 1
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if rows.Next(dest) != nil {
		return 1
	}
	n, _ := dest[0].(int64)
	return max(int(n), 1)
}

// rowWidth is the width of a row of cols, by their types (typeWidth).
func rowWidth(cols []Column) int64 {
	var width int64
	for _, c := range cols {
		width += typeWidth(c.Type)
	}
	return width
}

// shuffle writes the rows of query, partitioned by its __b column, under
// dir in the warehouse's exchanges, and lists the files written.
func (w *Worker) shuffle(ctx context.Context, conn *duckdb.Conn, dir, query string, cols []Column, parts int) (*taskResult, error) {
	if !w.allowed(dir+"/") || !strings.Contains(dir, "/"+config.ShuffleDir+"/") {
		return nil, fmt.Errorf("shuffle directory %s is outside the warehouse's %s", dir, config.ShuffleDir)
	}
	return w.writePartitions(ctx, conn, dir, query, cols, parts)
}

// writePartitions writes the rows of query, partitioned by its __b column,
// under dir, and lists the files written.
func (w *Worker) writePartitions(ctx context.Context, conn *duckdb.Conn, dir, query string, cols []Column, parts int) (*taskResult, error) {
	if !slices.ContainsFunc(cols, func(c Column) bool { return c.Name == "__b" }) {
		return nil, errors.New("a shuffle task's rows have no __b column")
	}
	if !strings.Contains(dir, "://") {
		// A warehouse on local disk, as in tests: DuckDB creates only the
		// last directory. Object stores have no directories to create.
		if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
			return nil, err
		}
	}
	// Columns Parquet would change cross as text, as results do; the views
	// that read them cast them back.
	sel := make([]string, len(cols))
	for i, c := range cols {
		t, err := transferType(c.Type)
		if err != nil {
			return nil, err
		}
		sel[i] = quoteIdent(c.Name)
		if t != c.Type {
			sel[i] = "CAST(" + quoteIdent(c.Name) + " AS " + t + ") AS " + quoteIdent(c.Name)
		}
	}
	// DuckDB keeps 100 of a partitioned COPY's files open, and past that
	// closes them and starts new ones: a task writing 256 partitions wrote
	// 17,275 files of 24 KB, each a request to object storage, for 23
	// minutes. With every partition's file open, a task writes one file a
	// partition.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET partitioned_write_max_open_files = %d", 4*maxPartitions), nil); err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET partitioned_write_flush_threshold = %d",
		shuffleFlush(w.opts.MemoryBytes, cap(w.slots), threadsOf(ctx, conn), cols)), nil); err != nil {
		return nil, err
	}
	if uploadsInParts(dir) {
		if _, err := conn.ExecContext(ctx, "SET s3_uploader_max_filesize = "+quoteLiteral(maxUploadFile), nil); err != nil {
			return nil, err
		}
	}
	opts := fmt.Sprintf("FORMAT parquet, PARTITION_BY (__b), ROW_GROUP_SIZE %d", shuffleRowGroup(w.opts.MemoryBytes, cap(w.slots), parts, cols))
	if n := shuffleRowGroupBytes(w.opts.MemoryBytes, cap(w.slots), parts); n > 0 {
		// DuckDB bounds a row group's bytes only where it need not keep
		// the rows' order, which an exchange's partitions do not.
		if _, err := conn.ExecContext(ctx, "SET preserve_insertion_order = false", nil); err != nil {
			return nil, err
		}
		opts += fmt.Sprintf(", ROW_GROUP_SIZE_BYTES %d", n)
	}
	// RETURN_STATS says what the COPY wrote, file by file: listing the
	// files and reading their footers back took requests one after
	// another, a listing and two per file, after the files were written.
	res, err := conn.QueryContext(ctx, "COPY (SELECT "+strings.Join(sel, ", ")+" FROM ("+query+")) TO "+quoteLiteral(dir)+
		" ("+opts+", RETURN_STATS)", nil)
	if err != nil {
		return nil, err
	}
	defer res.Close() //nolint:errcheck
	out := &taskResult{Files: []shuffleFile{}}
	for _, c := range cols {
		if c.Name != "__b" {
			out.Columns = append(out.Columns, c)
		}
	}
	// filename, count, file_size_bytes, footer_size_bytes,
	// column_statistics, partition_keys.
	dest := make([]driver.Value, len(res.Columns()))
	if len(dest) < 3 {
		return nil, fmt.Errorf("COPY returned %d columns of statistics", len(dest))
	}
	for {
		if err := res.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		f, _ := dest[0].(string)
		rows, rok := asInt64(dest[1])
		size, sok := asInt64(dest[2])
		rest, ok := strings.CutPrefix(f, dir+"/__b=")
		part, _, _ := strings.Cut(rest, "/")
		n, err := strconv.Atoi(part)
		if !ok || err != nil || !rok || !sok {
			return nil, fmt.Errorf("unexpected shuffle file %s", f)
		}
		out.Files = append(out.Files, shuffleFile{Path: f, Partition: n, Rows: rows, Size: size})
		out.Rows += rows
	}
	return out, nil
}

// asInt64 returns v, an integer DuckDB returned, as an int64.
func asInt64(v driver.Value) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case uint64:
		return int64(n), n <= math.MaxInt64
	case int32:
		return int64(n), true
	case uint32:
		return int64(n), true
	}
	return 0, false
}

// fileRows returns how many rows each of Parquet files holds, from their
// footers.
func fileRows(ctx context.Context, conn *duckdb.Conn, files []string) (map[string]int64, error) {
	out := map[string]int64{}
	if len(files) == 0 {
		return out, nil
	}
	quoted := make([]string, len(files))
	for i, f := range files {
		quoted[i] = quoteLiteral(f)
	}
	rows, err := conn.QueryContext(ctx, "SELECT file_name, num_rows FROM parquet_file_metadata(["+strings.Join(quoted, ", ")+"])", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		n, _ := dest[1].(int64)
		out[name] += n
	}
}

// checkRows checks that files hold rows rows in all.
func checkRows(ctx context.Context, conn *duckdb.Conn, files []string, rows int64) error {
	counts, err := fileRows(ctx, conn, files)
	if err != nil {
		return err
	}
	var n int64
	for _, f := range files {
		c, ok := counts[f]
		if !ok {
			return fmt.Errorf("file %s has no footer", f)
		}
		n += c
	}
	if n != rows {
		return fmt.Errorf("its files hold %d rows, not the %d written", n, rows)
	}
	return nil
}

// checkTaskSQL refuses a task whose SQL could read anything but the views
// the task defines over the files it lists: a table of any database, a
// table function other than unnest, json_each and json_tree of values
// (read_parquet of a path, say) or a statement over a table by name
// (SUMMARIZE). Coordinators never send such a task, since their planner
// distributes only statements over catalog tables; this keeps a worker to
// the files a task lists all the same. Only a holder of the worker secret
// can send a task at all. sfMacros says the SQL calls Igloo's macros for
// Snowflake's functions, which the worker then defines.
func checkTaskSQL(ctx context.Context, conn *duckdb.Conn, req *taskRequest) (sfMacros bool, err error) {
	rows, err := conn.QueryContext(ctx, "SELECT json_serialize_sql("+quoteLiteral(req.SQL)+")::VARCHAR", nil)
	if err != nil {
		return false, err
	}
	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	_ = rows.Close()
	if err != nil {
		return false, err
	}
	text, _ := dest[0].(string)
	doc, err := decodeJSON(text)
	if err != nil {
		return false, err
	}
	tree, _ := doc.(node)
	stmts := list(tree, "statements")
	if tree == nil || tree["error"] == true || len(stmts) != 1 {
		return false, unsupported("a task's SQL must be one statement that parses")
	}
	names := map[string]bool{}
	for _, v := range req.Views {
		names[strings.ToLower(v.Name)] = true
	}
	walk(stmts[0], func(n node) bool {
		for _, name := range cteNames(n) {
			names[name] = true
		}
		return true
	})
	walk(stmts[0], func(n node) bool {
		if err != nil {
			return false
		}
		switch t := str(n["type"]); t {
		case "BASE_TABLE":
			if str(n["catalog_name"]) != "" || str(n["schema_name"]) != "" || !names[strings.ToLower(str(n["table_name"]))] {
				err = unsupported("a task's SQL reads %s, which is not one of its views", str(n["table_name"]))
			}
		case "TABLE_FUNCTION":
			// unnest, json_each and json_tree read their arguments, which the
			// walk checks too, and series of integer literals nothing.
			if rowFunction(n) != nil {
				err = unsupported("a task's SQL may read only its views, not a table function")
			}
		case "SHOW_REF":
			err = unsupported("a task's SQL may read only its views, not a show ref")
		}
		if str(n["class"]) == "FUNCTION" && strings.HasPrefix(strings.ToLower(str(n["function_name"])), "sf_") {
			sfMacros = true
		}
		return err == nil
	})
	return sfMacros, err
}

// footer is what a data file's footer says of its columns.
type footer struct {
	// cols are the file's schema elements by name, nested ones too.
	cols map[string]fileColumn
	// ids is whether any column has a field ID. Files without field IDs
	// are read through the table's name mapping, which workers do not
	// apply.
	ids bool
	// top are the file's columns, with the elements nested in them.
	top []*schemaNode
}

// fileColumn is a column of a data file as its footer declares it.
type fileColumn struct {
	id  int64 // 0 without a field ID
	typ string
	// leaf is false for groups (structs, lists, maps).
	leaf bool
	// n counts the elements of this name.
	n int
}

// schemaNode is an element of a data file's schema: a column, or a field
// nested in one.
type schemaNode struct {
	name string
	id   int64 // 0 without a field ID
	// annotation is LIST or MAP for a group that holds one, as its
	// converted or logical type says.
	annotation string
	repeated   bool
	children   []*schemaNode
}

// maxFooters bounds the data files whose footers a worker remembers.
const maxFooters = 10000

// prefetchScans returns what a task reads of catalog tables' data files,
// for WorkerOptions.Prefetch: its views that the coordinator described for
// prefetching, with the files the worker may read.
func (w *Worker) prefetchScans(req *taskRequest) []Scan {
	var scans []Scan
	for _, v := range req.Views {
		p := v.Prefetch
		if p == nil || len(p.Sizes) != len(v.Files) {
			continue
		}
		sc := Scan{Table: v.Name, FieldIDs: slices.Clone(p.Used), Filtered: p.Filtered}
		for i, f := range v.Files {
			if w.allowed(f) {
				sc.Files = append(sc.Files, File{Path: f, Size: p.Sizes[i]})
			}
		}
		for _, c := range v.Columns {
			if slices.Contains(p.Used, c.ID) {
				sc.Columns = append(sc.Columns, c.Name)
			}
		}
		if len(sc.Files) > 0 {
			scans = append(scans, sc)
		}
	}
	return scans
}

// readByName reports whether a view's files can be read by column name,
// and refuses files without field IDs, or that nest a column otherwise
// than the table does (see checkNested), for which the statement runs on
// the coordinator.
//
// Files that hold each of the view's columns under its current name and
// field ID, with one type across the files, read the same by name as by
// field ID, and DuckDB 1.5 estimates reads by name at least as well: once,
// without row counts, TPC-H Q5's fragment took 9 seconds by field ID and
// 0.4 by name. Anything else (a column renamed, added, dropped and added
// again, or widened since some file was written, or a nested column) reads
// by field ID.
func (w *Worker) readByName(ctx context.Context, conn *duckdb.Conn, v viewDef) (bool, error) {
	if slices.ContainsFunc(v.Files, func(f string) bool { return !w.allowed(f) && !v.lent[f] }) ||
		slices.ContainsFunc(v.Columns, func(c Column) bool { return c.ID <= 0 }) {
		// viewStatement refuses them.
		return false, nil
	}
	footers, err := w.footers(ctx, conn, v.Files)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if slices.ContainsFunc(v.Columns, func(c Column) bool { return c.Nested != nil }) {
			// Nested fields are read by name; see checkNested.
			return false, unsupported("could not check how %s's data files nest its columns: %v", v.Name, err)
		}
		// Reading the files fails the same way, or succeeds by field ID.
		w.log.Debug("could not read data file footers; reading by field ID", "view", v.Name, "err", err)
		return false, nil
	}
	for _, f := range v.Files {
		if ft, ok := footers[f]; ok && !ft.ids && len(ft.cols) > 0 {
			return false, unsupported("data file %s has no field IDs", f)
		}
	}
	if slices.ContainsFunc(v.Columns, func(c Column) bool { return nestedType(c.Type) }) {
		return false, checkNested(v, footers)
	}
	types := make([]string, len(v.Columns))
	for _, f := range v.Files {
		ft, ok := footers[f]
		if !ok {
			return false, nil
		}
		for i, c := range v.Columns {
			fc, ok := ft.cols[c.Name]
			if !ok || fc.n != 1 || !fc.leaf || fc.id != int64(c.ID) || (types[i] != "" && fc.typ != types[i]) {
				return false, nil
			}
			types[i] = fc.typ
		}
	}
	return true, nil
}

// footers returns the footers of files, read from the files or
// remembered: a data file never changes.
func (w *Worker) footers(ctx context.Context, conn *duckdb.Conn, files []string) (map[string]*footer, error) {
	out := make(map[string]*footer, len(files))
	var missing []string
	w.footersMu.Lock()
	// Each file once: parquet_schema lists a file's elements as often as
	// it is named.
	seen := map[string]bool{}
	for _, f := range files {
		if ft, ok := w.footerCache[f]; ok {
			out[f] = ft
		} else if !seen[f] {
			seen[f] = true
			missing = append(missing, quoteLiteral(f))
		}
	}
	w.footersMu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	// Element 0 is the schema's root; the others follow it depth first.
	rows, err := conn.QueryContext(ctx, "SELECT file_name, name, coalesce(field_id, 0), coalesce(duckdb_type, ''), "+
		"coalesce(num_children, 0), coalesce(converted_type, ''), coalesce(logical_type, ''), repetition_type = 'REPEATED' "+
		"FROM parquet_schema(["+strings.Join(missing, ", ")+"]) WHERE column_id > 0 ORDER BY file_name, column_id", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	read := map[string]*footer{}
	elems := map[string][]schemaElem{}
	dest := make([]driver.Value, 8)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		file, _ := dest[0].(string)
		name, _ := dest[1].(string)
		ft := read[file]
		if ft == nil {
			ft = &footer{cols: map[string]fileColumn{}}
			read[file] = ft
		}
		c := ft.cols[name]
		c.id, _ = dest[2].(int64)
		c.typ, _ = dest[3].(string)
		children, _ := dest[4].(int64)
		c.leaf = children == 0
		c.n++
		ft.cols[name] = c
		ft.ids = ft.ids || c.id != 0
		converted, _ := dest[5].(string)
		logical, _ := dest[6].(string)
		repeated, _ := dest[7].(bool)
		e := schemaElem{schemaNode: schemaNode{name: name, id: c.id, repeated: repeated}, children: int(children)}
		logical = strings.ToUpper(logical)
		switch {
		case converted == "LIST" || strings.Contains(logical, "LIST"):
			e.annotation = "LIST"
		case converted == "MAP" || converted == "MAP_KEY_VALUE" || strings.Contains(logical, "MAP"):
			e.annotation = "MAP"
		}
		elems[file] = append(elems[file], e)
	}
	for file, ft := range read {
		if ft.top, err = schemaTree(elems[file]); err != nil {
			return nil, fmt.Errorf("data file %s: %w", file, err)
		}
	}
	w.footersMu.Lock()
	if w.footerCache == nil || len(w.footerCache)+len(read) > maxFooters {
		w.footerCache = map[string]*footer{}
	}
	for f, ft := range read {
		w.footerCache[f] = ft
		out[f] = ft
	}
	w.footersMu.Unlock()
	return out, nil
}

// schemaElem is a schema element as parquet_schema lists it: a node and
// how many elements, following it, are its children.
type schemaElem struct {
	schemaNode
	children int
}

// schemaTree rebuilds a file's columns from its schema elements, depth
// first.
func schemaTree(elems []schemaElem) ([]*schemaNode, error) {
	var build func(i int) (*schemaNode, int, error)
	build = func(i int) (*schemaNode, int, error) {
		if i >= len(elems) {
			return nil, i, errors.New("its schema ends early")
		}
		n := elems[i].schemaNode
		n.children = nil
		next := i + 1
		for range elems[i].children {
			c, after, err := build(next)
			if err != nil {
				return nil, next, err
			}
			n.children = append(n.children, c)
			next = after
		}
		return &n, next, nil
	}
	var top []*schemaNode
	for i := 0; i < len(elems); {
		n, next, err := build(i)
		if err != nil {
			return nil, err
		}
		top, i = append(top, n), next
	}
	return top, nil
}

// checkNested makes sure that every data file of a view nests the view's
// nested columns as the table does. DuckDB finds a column in a file by its
// field ID, but the fields nested in it by name, case aside: a struct's
// field renamed since a file was written, or dropped and added again under
// its old name, would read as NULL or as the old field's values. Lists'
// and maps' elements are read where they are, whatever their names. A
// file that nests a column otherwise, or without field IDs, keeps the
// statement on the coordinator, which reads it as Iceberg says.
func checkNested(v viewDef, footers map[string]*footer) error {
	for _, f := range v.Files {
		ft, ok := footers[f]
		if !ok {
			return unsupported("could not check how data file %s nests its columns", f)
		}
		for _, c := range v.Columns {
			if c.Nested == nil {
				if nestedType(c.Type) {
					return unsupported("column %s of %s is nested, and the task does not say how", c.Name, v.Name)
				}
				continue
			}
			var found *schemaNode
			for _, n := range ft.top {
				if n.id == int64(c.ID) {
					if found != nil {
						return unsupported("data file %s has two columns with field ID %d", f, c.ID)
					}
					found = n
				}
			}
			if found == nil {
				// Written before the column was added: it reads as NULL.
				continue
			}
			if err := matchNested(c.Nested, found); err != nil {
				return unsupported("data file %s nests column %s otherwise than the table: %v", f, c.Name, err)
			}
		}
	}
	return nil
}

// matchNested checks that file element n nests its fields as nest says,
// or is a value when nest is nil.
func matchNested(nest *Nesting, n *schemaNode) error {
	if nest == nil {
		if len(n.children) > 0 {
			return fmt.Errorf("%s is a group, not a value", n.name)
		}
		return nil
	}
	switch nest.Kind {
	case "list":
		e, err := listElement(n)
		if err != nil {
			return err
		}
		if len(nest.Fields) != 1 {
			return errors.New("a list has one element")
		}
		return matchField(nest.Fields[0], e)
	case "map":
		if n.annotation != "MAP" || len(n.children) != 1 || !n.children[0].repeated || len(n.children[0].children) != 2 {
			return fmt.Errorf("%s is not a map", n.name)
		}
		if len(nest.Fields) != 2 {
			return errors.New("a map has a key and a value")
		}
		kv := n.children[0].children
		if err := matchField(nest.Fields[0], kv[0]); err != nil {
			return err
		}
		return matchField(nest.Fields[1], kv[1])
	case "struct":
		if n.annotation != "" || n.repeated || len(n.children) == 0 {
			return fmt.Errorf("%s is not a struct", n.name)
		}
		names := map[string]bool{}
		for _, ch := range n.children {
			if ch.id == 0 {
				return fmt.Errorf("field %s has no field ID", ch.name)
			}
			k := strings.ToLower(ch.name)
			if names[k] {
				return fmt.Errorf("%s has two fields called %s", n.name, ch.name)
			}
			names[k] = true
		}
		for _, f := range nest.Fields {
			var found *schemaNode
			for _, ch := range n.children {
				if ch.id == int64(f.ID) {
					found = ch
				}
			}
			switch {
			case found != nil && !strings.EqualFold(found.name, f.Name):
				return fmt.Errorf("field %d is called %s, not %s", f.ID, found.name, f.Name)
			case found == nil && names[strings.ToLower(f.Name)]:
				return fmt.Errorf("field %s has another field ID", f.Name)
			case found != nil:
				if err := matchNested(f.Nested, found); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return fmt.Errorf("unknown kind %s", nest.Kind)
}

// matchField checks a list's element or a map's key or value: it must have
// the table's field ID.
func matchField(f Field, n *schemaNode) error {
	if n.id != int64(f.ID) {
		return fmt.Errorf("%s has field ID %d, not %d", n.name, n.id, f.ID)
	}
	return matchNested(f.Nested, n)
}

// listElement returns the element of a list: the one child of the list's
// repeated group, as Iceberg's writers nest it (three levels), or the
// repeated field itself (two levels, as older writers do).
func listElement(n *schemaNode) (*schemaNode, error) {
	if n.annotation != "LIST" || len(n.children) != 1 || !n.children[0].repeated {
		return nil, fmt.Errorf("%s is not a list", n.name)
	}
	r := n.children[0]
	switch {
	case len(r.children) == 0, r.id != 0:
		return r, nil
	case len(r.children) == 1:
		return r.children[0], nil
	}
	return nil, fmt.Errorf("%s is not a list", n.name)
}

// nestedType reports whether a DuckDB type is a struct, list, map or union.
func nestedType(t string) bool {
	u := strings.ToUpper(strings.TrimSpace(t))
	return strings.HasSuffix(u, "]") || strings.HasPrefix(u, "STRUCT") || strings.HasPrefix(u, "MAP") || strings.HasPrefix(u, "UNION")
}

// viewStatement defines a view over data files: read by Iceberg field ID,
// so files written before a column was renamed, added or widened read as
// the table's current schema does; or, with ByName, read by column name,
// for files that readByName found to name the table's columns, and for
// shuffled files. Paths are never read as Hive partitions, and DuckDB is
// told how many rows the files hold when the coordinator knows. A table
// without files is empty. Files with deletes leave out the rows listed in
// deleted, a table of (f, r): a file as the view reads it, and a row's
// position in it (see deletedRows).
func (w *Worker) viewStatement(v viewDef, deleted string) (string, error) {
	if !strings.HasPrefix(v.Name, "__igloo_") {
		return "", fmt.Errorf("view name %q", v.Name)
	}
	if len(v.Columns) == 0 {
		return "", fmt.Errorf("view %s has no columns", v.Name)
	}
	if len(v.Files) == 0 {
		cols := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			cols[i] = "CAST(NULL AS " + c.Type + ") AS " + quoteIdent(c.Name)
		}
		return "CREATE TEMP VIEW " + quoteIdent(v.Name) + " AS SELECT " + strings.Join(cols, ", ") + " WHERE false", nil
	}
	for _, f := range v.Files {
		if !w.allowed(f) && !v.lent[f] {
			return "", fmt.Errorf("file %s is outside the warehouse", f)
		}
	}
	var schema []string
	if !v.ByName {
		for _, c := range v.Columns {
			if c.ID <= 0 {
				return "", fmt.Errorf("column %s of %s has no field ID", c.Name, v.Name)
			}
			schema = append(schema, fmt.Sprintf("%d: {name: %s, type: %s, default_value: NULL}", c.ID, quoteLiteral(c.Name), quoteLiteral(c.Type)))
		}
	}
	// scan selects the view's columns from files, of rows rows, with more
	// read_parquet options.
	scan := func(files []string, rows int64, more string) string {
		quoted := make([]string, len(files))
		for i, f := range files {
			quoted[i] = quoteLiteral(f)
		}
		options := "hive_partitioning = false"
		if rows > 0 {
			options += ", explicit_cardinality = " + strconv.FormatInt(rows, 10)
		}
		if !v.ByName {
			options = "schema = MAP {" + strings.Join(schema, ", ") + "}, " + options
		}
		cols := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			cols[i] = "__d." + quoteIdent(c.Name)
			if v.ByName {
				cols[i] = "CAST(" + cols[i] + " AS " + c.Type + ") AS " + quoteIdent(c.Name)
			}
		}
		return "SELECT " + strings.Join(cols, ", ") + " FROM read_parquet([" + strings.Join(quoted, ", ") + "], " +
			options + more + ") AS __d"
	}
	if deleted == "" {
		return "CREATE TEMP VIEW " + quoteIdent(v.Name) + " AS " + scan(v.Files, v.Rows, ""), nil
	}

	// Files without deletes are read as they are; the others with each
	// row's file and position, which the rows that position deletes and
	// deletion vectors delete are left out by, and in groups by the
	// equality delete files that apply to them, without the rows those
	// delete.
	for _, c := range v.Columns {
		if strings.EqualFold(c.Name, "file_row_number") || strings.EqualFold(c.Name, "__igloo_file") {
			return "", unsupported("%s has a column called %s, and delete files", v.Name, c.Name)
		}
	}
	type group struct {
		files    []string
		rows     int64
		equality []EqualityDelete
	}
	deletes := map[string]fileDeletes{}
	for _, d := range v.Deletes {
		deletes[d.File] = d
	}
	groups := map[string]*group{}
	var order []string
	var plain []string
	plainRows := v.Rows
	for _, f := range v.Files {
		d, ok := deletes[f]
		if !ok {
			plain = append(plain, f)
			continue
		}
		var keys []string
		for _, e := range d.Equality {
			keys = append(keys, fmt.Sprint(e.Path, e.IDs))
		}
		slices.Sort(keys)
		key := strings.Join(keys, "\x00")
		g := groups[key]
		if g == nil {
			g = &group{equality: d.Equality}
			groups[key] = g
			order = append(order, key)
		}
		g.files = append(g.files, f)
		g.rows += d.Rows
		plainRows -= d.Rows
	}
	known := func(rows int64) int64 {
		if v.Rows == 0 {
			return 0
		}
		return max(rows, 0)
	}
	var parts []string
	if len(plain) > 0 {
		parts = append(parts, scan(plain, known(plainRows), ""))
	}
	for _, key := range order {
		g := groups[key]
		sel := scan(g.files, known(g.rows), ", filename = '__igloo_file', file_row_number = true") +
			" ANTI JOIN " + quoteIdent(deleted) + " AS __x ON __x.f = __d.__igloo_file AND __x.r = __d.file_row_number"
		eq, err := w.equalityJoins(v, g.equality)
		if err != nil {
			return "", err
		}
		parts = append(parts, sel+eq)
	}
	return "CREATE TEMP VIEW " + quoteIdent(v.Name) + " AS " + strings.Join(parts, " UNION ALL "), nil
}

// equalityJoins returns the anti joins that leave out of a view's rows,
// read as __d, those that equality delete files delete: one join for each
// set of columns some of the files compare, over those files, read by
// field ID. Iceberg's equality deletes match NULL to NULL.
func (w *Worker) equalityJoins(v viewDef, deletes []EqualityDelete) (string, error) {
	type set struct {
		ids   []int
		files []string
	}
	var sets []*set
	for _, d := range deletes {
		if !w.allowed(d.Path) {
			return "", fmt.Errorf("file %s is outside the warehouse", d.Path)
		}
		ids := slices.Sorted(slices.Values(d.IDs))
		i := slices.IndexFunc(sets, func(s *set) bool { return slices.Equal(s.ids, ids) })
		if i < 0 {
			sets = append(sets, &set{ids: ids})
			i = len(sets) - 1
		}
		if !slices.Contains(sets[i].files, d.Path) {
			sets[i].files = append(sets[i].files, d.Path)
		}
	}
	var b strings.Builder
	for k, s := range sets {
		var schema, conds []string
		alias := fmt.Sprintf("__e%d", k)
		for _, id := range s.ids {
			i := slices.IndexFunc(v.Columns, func(c Column) bool { return c.ID == id })
			if i < 0 || v.Columns[i].Nested != nil {
				return "", unsupported("equality deletes of %s compare field %d, which is not one of its columns", v.Name, id)
			}
			c := v.Columns[i]
			name := fmt.Sprintf("__c%d", id)
			schema = append(schema, fmt.Sprintf("%d: {name: %s, type: %s, default_value: NULL}", id, quoteLiteral(name), quoteLiteral(c.Type)))
			conds = append(conds, "__d."+quoteIdent(c.Name)+" IS NOT DISTINCT FROM "+alias+"."+quoteIdent(name))
		}
		files := make([]string, len(s.files))
		for i, f := range s.files {
			files[i] = quoteLiteral(f)
		}
		fmt.Fprintf(&b, " ANTI JOIN read_parquet([%s], schema = MAP {%s}, hive_partitioning = false) AS %s ON %s",
			strings.Join(files, ", "), strings.Join(schema, ", "), alias, strings.Join(conds, " AND "))
	}
	return b.String(), nil
}

// deletedRows creates the temporary table name of the rows that the
// position delete files and deletion vectors of v's files delete: (f, r),
// a data file as the view reads it, and the position of a deleted row in
// it. A position delete file lists the rows it deletes by the path of
// their data file, which it may spell otherwise than the table's metadata,
// as after a migration from another engine; the paths are compared as
// deletefiles.Canonical spells them. Each delete file deletes rows only of
// the data files it applies to, as the scan's planning matched them.
func (w *Worker) deletedRows(ctx context.Context, conn *duckdb.Conn, v viewDef, name string) error {
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}
	if err := exec("CREATE TEMP TABLE " + quoteIdent(name) + " (f VARCHAR, r BIGINT)"); err != nil {
		return err
	}
	inView := map[string]bool{}
	for _, f := range v.Files {
		inView[f] = true
	}
	// The data files of the view each position delete file applies to.
	applies := map[string][]string{}
	for _, d := range v.Deletes {
		if !inView[d.File] {
			return fmt.Errorf("deletes of %s, which is not a file of %s", d.File, v.Name)
		}
		for _, p := range d.Positions {
			if !w.allowed(p) {
				return fmt.Errorf("file %s is outside the warehouse", p)
			}
			if !slices.Contains(applies[p], d.File) {
				applies[p] = append(applies[p], d.File)
			}
		}
	}
	if len(applies) > 0 {
		files := slices.Sorted(maps.Keys(applies))
		quoted := make([]string, len(files))
		for i, f := range files {
			quoted[i] = quoteLiteral(f)
		}
		read := "read_parquet([" + strings.Join(quoted, ", ") + "], filename = '__igloo_delete_file', hive_partitioning = false, union_by_name = true)"
		// The data files each delete file names, as it spells them.
		rows, err := conn.QueryContext(ctx, "SELECT DISTINCT __igloo_delete_file, file_path FROM "+read, nil)
		if err != nil {
			return fmt.Errorf("read position delete files: %w", err)
		}
		var mapping []string
		dest := make([]driver.Value, 2)
		for {
			if err := rows.Next(dest); err != nil {
				_ = rows.Close()
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("read position delete files: %w", err)
			}
			dfile, _ := dest[0].(string)
			path, _ := dest[1].(string)
			for _, data := range applies[dfile] {
				if deletefiles.Canonical(path) == deletefiles.Canonical(data) {
					mapping = append(mapping, "("+quoteLiteral(dfile)+", "+quoteLiteral(path)+", "+quoteLiteral(data)+")")
				}
			}
		}
		if len(mapping) > 0 {
			if err := exec("INSERT INTO " + quoteIdent(name) + " SELECT m.data, CAST(p.pos AS BIGINT) FROM " + read +
				" AS p JOIN (VALUES " + strings.Join(mapping, ", ") + ") AS m(dfile, path, data) ON p.__igloo_delete_file = m.dfile AND p.file_path = m.path"); err != nil {
				return err
			}
		}
	}
	return w.vectorRows(ctx, conn, v, name)
}

// vectorRows adds the rows that the deletion vectors of v's files delete
// to the table name. A vector is a blob of a Puffin file, framed and
// serialized as package deletefiles reads it.
func (w *Worker) vectorRows(ctx context.Context, conn *duckdb.Conn, v viewDef, name string) error {
	blobs := map[string][]byte{}
	var runs []string
	flush := func() error {
		if len(runs) == 0 {
			return nil
		}
		_, err := conn.ExecContext(ctx, "INSERT INTO "+quoteIdent(name)+" SELECT f, unnest(range(s, e)) FROM (VALUES "+
			strings.Join(runs, ", ")+") AS t(f, s, e)", nil)
		runs = runs[:0]
		return err
	}
	for _, d := range v.Deletes {
		for _, vec := range d.Vectors {
			if !w.allowed(vec.Path) {
				return fmt.Errorf("file %s is outside the warehouse", vec.Path)
			}
			content, ok := blobs[vec.Path]
			if !ok {
				var err error
				if content, err = readBlob(ctx, conn, vec.Path); err != nil {
					return fmt.Errorf("read deletion vectors: %w", err)
				}
				blobs[vec.Path] = content
			}
			if vec.Offset < 0 || vec.Size < 8 || vec.Offset+vec.Size > int64(len(content)) {
				return unsupported("a deletion vector of %s lies outside its file", d.File)
			}
			data, err := deletefiles.StoredVector(content[vec.Offset:vec.Offset+vec.Size], vec.Size-8)
			if err != nil {
				return unsupported("a deletion vector of %s: %v", d.File, err)
			}
			n, err := deletefiles.VectorRuns(data, func(start, stop uint64) error {
				if stop > math.MaxInt64 {
					return deletefiles.ErrCorruptVector
				}
				runs = append(runs, fmt.Sprintf("(%s, %d, %d)", quoteLiteral(d.File), start, stop))
				if len(runs) == 4096 {
					return flush()
				}
				return nil
			})
			if err != nil {
				return unsupported("a deletion vector of %s: %v", d.File, err)
			}
			if vec.Rows > 0 && n != uint64(vec.Rows) {
				return unsupported("a deletion vector of %s deletes %d rows where the table's metadata says %d", d.File, n, vec.Rows)
			}
		}
	}
	return flush()
}

// readBlob returns the contents of a file.
func readBlob(ctx context.Context, conn *duckdb.Conn, path string) ([]byte, error) {
	rows, err := conn.QueryContext(ctx, "SELECT content FROM read_blob("+quoteLiteral(path)+")", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return nil, err
	}
	b, ok := dest[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("%s has no contents", path)
	}
	return b, nil
}

// allowed reports whether a data file lies under the warehouse or an
// allowed location. The sandbox enforces the same; this refuses a bad task
// before it runs.
func (w *Worker) allowed(path string) bool {
	if strings.Contains(path, "..") {
		return false
	}
	for _, loc := range w.opts.Locations {
		if strings.HasPrefix(path, strings.TrimSuffix(loc, "/")+"/") {
			return true
		}
	}
	return false
}

// describe binds query and returns its result columns.
func describe(ctx context.Context, conn *duckdb.Conn, query string) ([]Column, error) {
	rows, err := conn.QueryContext(ctx, "DESCRIBE "+query, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var cols []Column
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return cols, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		cols = append(cols, Column{Name: name, Type: typ})
	}
}
