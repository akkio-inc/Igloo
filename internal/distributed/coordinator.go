package distributed

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"
)

// Mode says when a session's statements run distributed: auto (when Igloo
// judges it worth it), on (whenever a statement can, for testing and
// benchmarks) or off.
type Mode string

const (
	ModeAuto Mode = "auto"
	ModeOn   Mode = "on"
	ModeOff  Mode = "off"
)

// ParseMode parses a mode.
func ParseMode(s string) (Mode, bool) {
	switch m := Mode(strings.ToLower(strings.TrimSpace(s))); m {
	case ModeAuto, ModeOn, ModeOff:
		return m, true
	}
	return "", false
}

// When a statement is worth distributing. Igloo estimates how long the
// statement would take on the coordinator from the size of the table it
// would divide: DuckDB scans, filters and aggregates compressed Parquet
// from object storage at roughly 64 MiB/s per thread or better. Running
// workers are used for statements of a few seconds; workers are started
// only for statements of a minute or more, since a start takes a while and
// started workers run until they have been idle for a while.
const (
	scanRatePerThread = 64 << 20
	warmSeconds       = 4
	coldSeconds       = 60
	// bytesPerWorker is how much of the divided table each worker should
	// get at least; smaller statements use fewer workers.
	bytesPerWorker = 1 << 30
	// defaultBroadcast caps what every worker reads in full when workers'
	// memory is not known yet.
	defaultBroadcast = 1 << 30
	// maxAttempts is how often a task is tried, on different workers.
	maxAttempts = 3
)

// Options configures a coordinator.
type Options struct {
	Source  Source
	Pool    PoolOptions
	Secret  string
	Version string
	// TempDir holds partial results while the final query reads them. It
	// must be a directory DuckDB may read: the server's spill directory.
	TempDir string
	// Warehouse is the warehouse root, with a trailing slash. Partitioned
	// joins exchange rows under it.
	Warehouse string
	// Threads are the coordinator's DuckDB threads, for estimating how
	// long a statement would take on it.
	Threads int
	Metrics *Metrics
}

// Coordinator splits statements into parts that workers run.
type Coordinator struct {
	opts Options
	pool *pool
	// maxBroadcast, if set, overrides what every worker may read in full,
	// for tests.
	maxBroadcast int64
	parser       *parser
	aggs         map[string]bool
	dir          string
	log          *slog.Logger
	stop         context.CancelFunc
	done         chan struct{}
}

// NewCoordinator returns a coordinator for the workers opts.Pool names.
func NewCoordinator(opts Options, log *slog.Logger) (*Coordinator, error) {
	t, err := newTransport(opts.Secret)
	if err != nil {
		return nil, err
	}
	if opts.TempDir == "" {
		return nil, errors.New("distributed execution needs IGLOO_DUCKDB_TEMP_DIR for partial results")
	}
	if len(opts.Pool.Addresses) == 0 {
		return nil, errors.New("no worker addresses")
	}
	if opts.Threads <= 0 {
		opts.Threads = 1
	}
	p, err := newParser()
	if err != nil {
		return nil, err
	}
	aggs := map[string]bool{}
	rows, err := p.db.Query(`SELECT DISTINCT lower(function_name) FROM duckdb_functions() WHERE function_type = 'aggregate'`)
	if err != nil {
		return nil, errors.Join(err, p.close())
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close() //nolint:errcheck
			return nil, errors.Join(err, p.close())
		}
		aggs[name] = true
	}
	if err := rows.Close(); err != nil {
		return nil, errors.Join(err, p.close())
	}
	dir := filepath.Join(opts.TempDir, "igloo-distributed")
	// Partial results of statements that were running when Igloo stopped.
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, errors.Join(fmt.Errorf("create directory for partial results: %w", err), p.close())
	}
	log = log.With("component", "distributed")
	ctx, stop := context.WithCancel(context.Background())
	co := &Coordinator{
		opts: opts, parser: p, aggs: aggs, dir: dir, log: log, stop: stop, done: make(chan struct{}),
		pool: newPool(opts.Pool, t, opts.Version, opts.Metrics, log),
	}
	go func() {
		defer close(co.done)
		co.pool.run(ctx)
	}()
	return co, nil
}

// Close stops the coordinator's background work.
func (co *Coordinator) Close() error {
	co.stop()
	<-co.done
	return co.parser.close()
}

// Table returns a catalog table at its current snapshot.
func (co *Coordinator) Table(ctx context.Context, schema, name string) (*Table, error) {
	return co.opts.Source.Table(ctx, schema, name)
}

// Metrics returns the coordinator's metrics, which may be nil.
func (co *Coordinator) Metrics() *Metrics { return co.opts.Metrics }

// Workers returns the number of workers that answered the last check.
func (co *Coordinator) Workers(ctx context.Context) int { return len(co.pool.ready(ctx)) }

// Plan decides whether a statement runs distributed and, if so, how. query
// is the statement as the session prepared it. An error for which
// IsUnsupported is true means it runs on the coordinator as usual.
//
// Every SELECT comes here, so deciding is cheap: workers are known from the
// last background check, and tables' sizes may be a few seconds old.
// Execute reads the tables' current snapshots.
func (co *Coordinator) Plan(ctx context.Context, query string, env Env, mode Mode) (*Plan, error) {
	if mode == ModeOff {
		return nil, unsupported("distributed execution is off for this session")
	}
	opts := planOptions{force: mode == ModeOn, now: time.Now(), maxBroadcast: defaultBroadcast}
	ws := co.pool.known(ctx)
	rate := int64(co.opts.Threads) * scanRatePerThread
	switch {
	case len(ws) > 0:
		opts.minBytes = rate * warmSeconds
		mem := int64(0)
		for _, w := range ws {
			if m := w.status.MemoryBytes; m > 0 && (mem == 0 || m < mem) {
				mem = m
			}
		}
		if mem > 0 {
			// Data files are compressed several times over, and hash tables
			// take more room than the values they hold.
			opts.maxBroadcast = mem / 8
		}
	case co.pool.canStart():
		opts.minBytes = rate*coldSeconds + int64(co.pool.startEstimate().Seconds())*rate
	default:
		return nil, unsupported("no workers are running")
	}
	if co.maxBroadcast > 0 {
		opts.maxBroadcast = co.maxBroadcast
	}
	stmt, err := co.parser.parse(query)
	if err != nil {
		if IsUnsupported(err) {
			return nil, err
		}
		return nil, unsupported("%v", err)
	}
	plan, err := newPlanner(ctx, env, co.parser, co.aggs, opts).plan(stmt)
	if err != nil {
		return nil, err
	}
	plan.force = opts.force
	return plan, nil
}

// Execution is a statement whose distributed parts have run.
type Execution struct {
	// SQL is the rest of the statement, which reads the parts' combined
	// results; the session runs it, with the statement's parameters.
	SQL string
	// Workers is how many workers ran tasks.
	Workers int
	dir     string
}

// Close removes the partial results. Call it once SQL has run.
func (e *Execution) Close() {
	if e != nil && e.dir != "" {
		_ = os.RemoveAll(e.dir)
	}
}

// ErrNoWorkers means no worker could run the statement's parts.
var ErrNoWorkers = errors.New("no workers available")

// Setting is a session setting a statement runs with.
type Setting = setting

// Execute runs a plan's jobs on workers, with the session's values of
// SettingNames. An error that is not a *duckdb.Error from the data, nor the
// context's, means the statement should run on the coordinator instead.
func (co *Coordinator) Execute(ctx context.Context, plan *Plan, settings []Setting) (*Execution, error) {
	start := time.Now()
	ex := &Execution{dir: filepath.Join(co.dir, newID())}
	if err := os.MkdirAll(ex.dir, 0o700); err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			ex.Close()
		}
	}()

	// As many workers as the largest job has gigabytes to divide, or
	// every one when forced.
	var split int64
	for _, j := range plan.Jobs {
		split = max(split, j.Split.Bytes)
	}
	want := int(max(1, (split+bytesPerWorker-1)/bytesPerWorker))
	if plan.force {
		want = max(want, co.opts.Pool.MaxWorkers, len(co.pool.ready(ctx)))
	}
	ws, err := co.pool.ensure(ctx, want, true)
	if err != nil || len(ws) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Join(ErrNoWorkers, err)
	}
	slices.SortFunc(ws, func(a, b *worker) int { return strings.Compare(a.status.ID, b.status.ID) })
	co.pool.touch()

	// Each table's current snapshot and its files, once per statement, so
	// the statement sees every commit made before it.
	files := map[string][]File{}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for _, j := range plan.Jobs {
		for _, v := range j.Views {
			mu.Lock()
			_, seen := files[v.Table.key()]
			files[v.Table.key()] = nil
			mu.Unlock()
			if seen {
				continue
			}
			g.Go(func() error {
				if err := co.opts.Source.Refresh(gctx, v.Table); err != nil {
					return err
				}
				fs, err := co.opts.Source.Files(gctx, v.Table)
				mu.Lock()
				files[v.Table.key()] = fs
				mu.Unlock()
				return err
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	snapshots := time.Since(start)
	qid := filepath.Base(ex.dir)
	results := make([]*jobResult, len(plan.Jobs))
	used := map[string]bool{}
	var usedMu sync.Mutex
	g, gctx = errgroup.WithContext(ctx)
	for i, j := range plan.Jobs {
		g.Go(func() error {
			var r *jobResult
			var err error
			if j.Partition != nil {
				r, err = co.runPartitioned(gctx, qid, ex.dir, j, ws, files, settings)
			} else {
				r, err = co.runJob(gctx, qid, ex.dir, j, ws, files, settings, plan.force)
			}
			if err != nil {
				return err
			}
			results[i] = r
			usedMu.Lock()
			for a := range r.workers {
				used[a] = true
			}
			usedMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		switch {
		case ctx.Err() != nil:
			co.opts.Metrics.query("canceled", start)
			return nil, ctx.Err()
		case dataError(err):
			co.opts.Metrics.query("error", start)
		}
		return nil, err
	}

	final := clone(plan.final).(node)
	for i, j := range plan.Jobs {
		ref, err := co.partials(results[i])
		if err != nil {
			return nil, err
		}
		replaced := false
		rewrite(final, func(n node) (node, bool) {
			if str(n["type"]) == "BASE_TABLE" && str(n["table_name"]) == j.placeholder {
				replaced = true
				return clone(ref).(node), true
			}
			return nil, false
		})
		if !replaced {
			return nil, fmt.Errorf("job %d has no place in the statement", j.ID)
		}
	}
	if ex.SQL, err = co.parser.print(final); err != nil {
		return nil, err
	}
	ex.Workers = len(used)
	co.pool.touch()
	co.log.Debug("ran a statement's parts", "query", qid, "jobs", len(plan.Jobs), "workers", ex.Workers,
		"snapshots_seconds", snapshots.Seconds(), "seconds", time.Since(start).Seconds())
	co.opts.Metrics.query("ok", start)
	ok = true
	return ex, nil
}

// jobResult is where a job's partial results are.
type jobResult struct {
	files   []string
	columns []Column
	workers map[string]bool
}

// runJob divides the job's split table among the workers and runs a task
// for each share.
func (co *Coordinator) runJob(ctx context.Context, qid, dir string, j *Job, ws []*worker, files map[string][]File, settings []setting, force bool) (*jobResult, error) {
	split := files[j.Split.key()]
	n := len(ws)
	if want := int((j.Split.Bytes + bytesPerWorker - 1) / bytesPerWorker); want < n && !force {
		n = max(want, min(2, n))
	}
	n = max(1, min(n, len(split)))
	shares := assign(split, ws[:n])

	var views []viewDef
	for _, v := range j.Views {
		if v.Name == j.SplitView {
			continue
		}
		def := viewDef{Name: v.Name, Columns: v.Table.Columns}
		for _, f := range files[v.Table.key()] {
			def.Files = append(def.Files, f.Path)
		}
		views = append(views, def)
	}

	res := &jobResult{files: make([]string, len(shares)), workers: map[string]bool{}}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for i, share := range shares {
		task := &taskRequest{
			Query: qid, Task: j.ID*10000 + i, Version: co.opts.Version, Settings: settings, SQL: j.Fragment, Columns: j.Columns,
			Views: append([]viewDef{{Name: j.SplitView, Columns: j.Split.Columns, Files: paths(share)}}, views...),
		}
		var bytes int64
		for _, f := range share {
			bytes += f.Size
		}
		path := filepath.Join(dir, fmt.Sprintf("%d-%d.parquet", j.ID, i))
		g.Go(func() error {
			w, head, err := co.dispatch(gctx, task, ws, i, path)
			if err != nil {
				return err
			}
			co.opts.Metrics.scan(bytes)
			mu.Lock()
			defer mu.Unlock()
			res.files[i] = path
			res.workers[w.addr] = true
			if res.columns == nil {
				res.columns = head.Columns
			} else if !slices.Equal(res.columns, head.Columns) {
				return fmt.Errorf("workers returned different columns for job %d", j.ID)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return res, nil
}

func paths(fs []File) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Path
	}
	return out
}

// assign divides files among workers by size. Each file goes to the worker
// that ranks highest for it (rendezvous hashing on the file's path and the
// worker's ID) unless that worker already has more than its share, so
// repeated statements send the same files to the same workers, whose read
// caches then hold them. A table with no files still gets one share, so
// the fragment runs once, over nothing.
func assign(files []File, ws []*worker) [][]File {
	shares := make([][]File, len(ws))
	if len(files) == 0 {
		return shares[:1]
	}
	var total int64
	for _, f := range files {
		total += max(f.Size, 1)
	}
	limit := total/int64(len(ws)) + total/int64(4*len(ws)) + 1
	sorted := slices.Clone(files)
	slices.SortStableFunc(sorted, func(a, b File) int {
		if a.Size != b.Size {
			if a.Size > b.Size {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Path, b.Path)
	})
	load := make([]int64, len(ws))
	for _, f := range sorted {
		best, bestScore := -1, uint64(0)
		least := 0
		for i, w := range ws {
			if load[i] < load[least] {
				least = i
			}
			if load[i]+max(f.Size, 1) > limit {
				continue
			}
			h := fnv.New64a()
			_, _ = h.Write([]byte(f.Path))
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte(w.status.ID))
			if s := h.Sum64(); best < 0 || s > bestScore {
				best, bestScore = i, s
			}
		}
		if best < 0 {
			best = least
		}
		shares[best] = append(shares[best], f)
		load[best] += max(f.Size, 1)
	}
	// Workers left without files do nothing.
	out := shares[:0]
	for _, s := range shares {
		if len(s) > 0 {
			out = append(out, s)
		}
	}
	return out
}

// dispatch runs a task, first on worker first (an index into ws), then on
// other workers if that fails for any reason other than the data. It
// returns the worker that ran it. The result goes to path; a shuffle task
// writes each attempt to a directory of its own, so the files of a failed
// attempt are never read.
func (co *Coordinator) dispatch(ctx context.Context, task *taskRequest, ws []*worker, first int, path string) (*worker, *taskResult, error) {
	var last error
	for attempt := range min(maxAttempts, len(ws)) {
		t := *task
		if t.Shuffle != "" {
			t.Shuffle += "/a" + strconv.Itoa(attempt)
		}
		body, err := json.Marshal(&t)
		if err != nil {
			return nil, nil, err
		}
		w := ws[(first+attempt)%len(ws)]
		head, err := co.fetch(ctx, w, body, path)
		if err == nil {
			co.opts.Metrics.task("ok")
			return w, head, nil
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if IsUnsupported(err) {
			// Every worker would refuse it.
			return nil, nil, err
		}
		if dataError(err) {
			co.opts.Metrics.task("failed")
			return nil, nil, err
		}
		co.log.Warn("task failed on a worker; trying another", "query", task.Query, "task", task.Task, "worker", w.addr, "err", err)
		co.opts.Metrics.task("retried")
		co.pool.markDown(w.addr)
		last = err
	}
	co.opts.Metrics.task("failed")
	return nil, nil, fmt.Errorf("task %d failed on every worker tried: %w", task.Task, last)
}

// fetch sends a task to a worker and writes its result to path.
func (co *Coordinator) fetch(ctx context.Context, w *worker, body []byte, path string) (*taskResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+w.addr+tasksPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := co.pool.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	br := bufio.NewReaderSize(resp.Body, 1<<16)
	if resp.StatusCode != http.StatusOK {
		var res taskResult
		if err := json.NewDecoder(io.LimitReader(br, 1<<20)).Decode(&res); err != nil || res.Error == nil {
			return nil, fmt.Errorf("worker answered %s", resp.Status)
		}
		if res.Error.Unsupported {
			return nil, &errUnsupported{reason: strings.TrimPrefix(res.Error.Message, "cannot distribute: ")}
		}
		if res.Error.Type != 0 {
			return nil, &duckdb.Error{Type: duckdb.ErrorType(res.Error.Type), Msg: res.Error.Message}
		}
		return nil, res.Error
	}
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read result header: %w", err)
	}
	var head taskResult
	if err := json.Unmarshal(line, &head); err != nil {
		return nil, fmt.Errorf("read result header: %w", err)
	}
	if path == "" {
		// A shuffle task's rows are in object storage.
		return &head, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	n, err := io.Copy(f, br)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("receive result: %w", err)
	}
	co.opts.Metrics.receive(n)
	return &head, nil
}

// partials returns the table reference that reads a job's partial
// results, each column cast back to the type the workers computed.
func (co *Coordinator) partials(r *jobResult) (node, error) {
	cols := make([]string, len(r.columns))
	for i, c := range r.columns {
		cols[i] = "CAST(" + quoteIdent(c.Name) + " AS " + c.Type + ") AS " + quoteIdent(c.Name)
	}
	files := make([]string, len(r.files))
	for i, f := range r.files {
		files[i] = quoteLiteral(f)
	}
	n, err := co.parser.parse("SELECT * FROM (SELECT " + strings.Join(cols, ", ") + " FROM read_parquet([" +
		strings.Join(files, ", ") + "])) AS __igloo_p")
	if err != nil {
		return nil, err
	}
	return child(n, "from_table"), nil
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
