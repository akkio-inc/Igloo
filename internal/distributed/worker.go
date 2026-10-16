package distributed

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/config"
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
	// Threads and MemoryBytes are reported to coordinators, which size the
	// tables every worker reads in full by the smallest worker.
	Threads     int
	MemoryBytes int64
	Metrics     *WorkerMetrics
}

// Worker serves tasks.
type Worker struct {
	opts      WorkerOptions
	transport *transport
	log       *slog.Logger
	activity  *activity.Tracker
	srv       *http.Server
	// beforeTask, if set, runs before each task; tests use it to make a
	// worker fail or hang.
	beforeTask func(rw http.ResponseWriter, r *http.Request) bool

	// footerCache holds data files' footers by path; see footers.
	footersMu   sync.Mutex
	footerCache map[string]*footer
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
	dir := filepath.Join(opts.TempDir, "igloo-worker")
	// Results of tasks that were running when the worker stopped.
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create result directory: %w", err)
	}
	opts.TempDir = dir
	w := &Worker{opts: opts, transport: t, log: log.With("component", "worker"), activity: activity.New()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+tasksPath, w.serveTask)
	mux.HandleFunc("GET "+statusPath, w.serveStatus)
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
	err := w.srv.ServeTLS(ln, "", "")
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown stops accepting tasks and waits for running ones until ctx is
// done.
func (w *Worker) Shutdown(ctx context.Context) error { return w.srv.Shutdown(ctx) }

// Activity reports running tasks and how long the worker has been idle.
func (w *Worker) Activity() (int64, time.Duration) { return w.activity.Snapshot() }

func (w *Worker) serveStatus(rw http.ResponseWriter, _ *http.Request) {
	running, idle := w.activity.Snapshot()
	rw.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(rw).Encode(workerStatus{
		ID: w.opts.ID, Version: w.opts.Version, Threads: w.opts.Threads, MemoryBytes: w.opts.MemoryBytes,
		Running: int(running), IdleSeconds: idle.Seconds(),
	})
}

// maxTaskRequest bounds a task's JSON: the file lists of large tables.
const maxTaskRequest = 256 << 20

func (w *Worker) serveTask(rw http.ResponseWriter, r *http.Request) {
	defer w.activity.Begin()()
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
	path, res, err := w.run(r.Context(), &req)
	if path != "" {
		defer os.Remove(path) //nolint:errcheck
	}
	if err != nil {
		te := &taskErr{Message: err.Error()}
		var de *duckdb.Error
		if errors.As(err, &de) {
			te.Type, te.Message, te.Data = int(de.Type), de.Msg, dataError(err)
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

// run runs a task on a connection of its own and writes the result to a
// Parquet file, whose path it returns; a shuffle task writes it to object
// storage instead, one directory per partition, and lists the files.
func (w *Worker) run(ctx context.Context, req *taskRequest) (string, *taskResult, error) {
	conn, err := w.opts.Engine.Connect(ctx)
	if err != nil {
		return "", nil, err
	}
	defer conn.Close() //nolint:errcheck
	exec := func(stmt string) error {
		_, err := conn.ExecContext(ctx, stmt, nil)
		return err
	}

	for _, s := range req.Settings {
		if !knownSetting(s.Name) {
			return "", nil, fmt.Errorf("setting %s is not one tasks may change", s.Name)
		}
		if err := exec("SET " + quoteIdent(s.Name) + " = " + quoteLiteral(s.Value)); err != nil {
			return "", nil, fmt.Errorf("set %s: %w", s.Name, err)
		}
	}
	start := time.Now()
	var byID []string
	for _, v := range req.Views {
		if !v.ByName && len(v.Files) > 0 {
			if v.ByName, err = w.readByName(ctx, conn, v); err != nil {
				return "", nil, err
			}
			if !v.ByName {
				byID = append(byID, v.Name)
			}
		}
		stmt, err := w.viewStatement(v)
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
	if req.Shuffle != "" {
		res, err := w.shuffle(ctx, conn, req.Shuffle, query, cols)
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

// shuffle writes the rows of query, partitioned by its __b column, under
// dir, and lists the files written.
func (w *Worker) shuffle(ctx context.Context, conn *duckdb.Conn, dir, query string, cols []Column) (*taskResult, error) {
	if !w.allowed(dir+"/") || !strings.Contains(dir, "/"+config.ShuffleDir+"/") {
		return nil, fmt.Errorf("shuffle directory %s is outside the warehouse's %s", dir, config.ShuffleDir)
	}
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
	res, err := conn.ExecContext(ctx, "COPY ("+query+") TO "+quoteLiteral(dir)+" (FORMAT parquet, PARTITION_BY (__b))", nil)
	if err != nil {
		return nil, err
	}
	rows, _ := res.RowsAffected()
	out := &taskResult{Rows: rows, Files: []shuffleFile{}}
	for _, c := range cols {
		if c.Name != "__b" {
			out.Columns = append(out.Columns, c)
		}
	}
	files, err := globFiles(ctx, conn, dir+"/*/*.parquet")
	if err != nil {
		return nil, fmt.Errorf("list shuffle files: %w", err)
	}
	for _, f := range files {
		rest, ok := strings.CutPrefix(f, dir+"/__b=")
		part, _, _ := strings.Cut(rest, "/")
		n, err := strconv.Atoi(part)
		if !ok || err != nil {
			return nil, fmt.Errorf("unexpected shuffle file %s", f)
		}
		out.Files = append(out.Files, shuffleFile{Path: f, Partition: n})
	}
	return out, nil
}

func globFiles(ctx context.Context, conn *duckdb.Conn, pattern string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT file FROM glob("+quoteLiteral(pattern)+") ORDER BY file", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []string
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		s, _ := dest[0].(string)
		out = append(out, s)
	}
}

// footer is what a data file's footer says of its columns.
type footer struct {
	// cols are the file's schema elements by name, nested ones too.
	cols map[string]fileColumn
	// ids is whether any column has a field ID. Files without field IDs
	// are read through the table's name mapping, which workers do not
	// apply.
	ids bool
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

// maxFooters bounds the data files whose footers a worker remembers.
const maxFooters = 10000

// readByName reports whether a view's files can be read by column name,
// and refuses files without field IDs, for which the statement runs on
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
	if slices.ContainsFunc(v.Files, func(f string) bool { return !w.allowed(f) }) ||
		slices.ContainsFunc(v.Columns, func(c Column) bool { return c.ID <= 0 }) {
		// viewStatement refuses them.
		return false, nil
	}
	footers, err := w.footers(ctx, conn, v.Files)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
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
		return false, nil
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
	for _, f := range files {
		if ft, ok := w.footerCache[f]; ok {
			out[f] = ft
		} else {
			missing = append(missing, quoteLiteral(f))
		}
	}
	w.footersMu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	// Element 0 is the schema's root.
	rows, err := conn.QueryContext(ctx, "SELECT file_name, name, coalesce(field_id, 0), coalesce(duckdb_type, ''), "+
		"coalesce(num_children, 0) FROM parquet_schema(["+strings.Join(missing, ", ")+"]) WHERE column_id > 0", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	read := map[string]*footer{}
	dest := make([]driver.Value, 5)
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
// without files is empty.
func (w *Worker) viewStatement(v viewDef) (string, error) {
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
	files := make([]string, len(v.Files))
	for i, f := range v.Files {
		if !w.allowed(f) {
			return "", fmt.Errorf("file %s is outside the warehouse", f)
		}
		files[i] = quoteLiteral(f)
	}
	options := "hive_partitioning = false"
	if v.Rows > 0 {
		options += ", explicit_cardinality = " + strconv.FormatInt(v.Rows, 10)
	}
	if v.ByName {
		cols := make([]string, len(v.Columns))
		for i, c := range v.Columns {
			cols[i] = "CAST(" + quoteIdent(c.Name) + " AS " + c.Type + ") AS " + quoteIdent(c.Name)
		}
		return "CREATE TEMP VIEW " + quoteIdent(v.Name) + " AS SELECT " + strings.Join(cols, ", ") +
			" FROM read_parquet([" + strings.Join(files, ", ") + "], " + options + ")", nil
	}
	schema := make([]string, len(v.Columns))
	for i, c := range v.Columns {
		if c.ID <= 0 {
			return "", fmt.Errorf("column %s of %s has no field ID", c.Name, v.Name)
		}
		schema[i] = fmt.Sprintf("%d: {name: %s, type: %s, default_value: NULL}", c.ID, quoteLiteral(c.Name), quoteLiteral(c.Type))
	}
	return "CREATE TEMP VIEW " + quoteIdent(v.Name) + " AS SELECT * FROM read_parquet([" + strings.Join(files, ", ") +
		"], schema = MAP {" + strings.Join(schema, ", ") + "}, " + options + ")", nil
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
