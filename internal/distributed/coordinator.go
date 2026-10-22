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
	"sync/atomic"
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
	// ModeOnRunning is on for users other than admins: whenever a statement
	// can, but only on workers already running and on no more of them than
	// the automatic decision would use, so that no user can start workers
	// or occupy them all at will.
	ModeOnRunning Mode = "on, running workers only"
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
	// MaxSpoolBytes bounds what a statement's partial results may take on
	// the coordinator's disk; past it, the statement runs on the
	// coordinator instead. Zero means 1 GiB.
	MaxSpoolBytes int64
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
	opts := planOptions{force: mode == ModeOn || mode == ModeOnRunning, now: time.Now(), maxBroadcast: defaultBroadcast}
	ws := co.pool.known(ctx)
	rate := int64(co.opts.Threads) * scanRatePerThread
	switch {
	case mode == ModeOnRunning && len(ws) == 0:
		return nil, unsupported("no workers are running, and only admins may start them at will")
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
	plan.force, plan.mayStart = opts.force, mode != ModeOnRunning
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

	// As many workers as the largest split table has gigabytes to divide,
	// or every one when an admin forces it. Forced by anyone else, only
	// workers already running, and no more than the gigabytes call for.
	var split int64
	for _, s := range plan.Stages {
		if in := s.split(); in != nil {
			split = max(split, in.Table.Bytes)
		}
	}
	want := int(max(1, (split+bytesPerWorker-1)/bytesPerWorker))
	var ws []*worker
	var err error
	if plan.mayStart {
		if plan.force {
			want = max(want, co.opts.Pool.MaxWorkers, len(co.pool.ready(ctx)))
		}
		ws, err = co.pool.ensure(ctx, want, true)
	} else {
		ws = co.pool.ready(ctx)
	}
	if err != nil || len(ws) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Join(ErrNoWorkers, err)
	}
	slices.SortFunc(ws, func(a, b *worker) int { return strings.Compare(a.status.ID, b.status.ID) })
	if !plan.mayStart && len(ws) > want {
		ws = ws[:want]
	}
	co.pool.touch()
	limit := co.opts.MaxSpoolBytes
	if limit <= 0 {
		limit = defaultMaxSpool
	}

	// Each table's current snapshot and its files, once per statement, so
	// the statement sees every commit made before it.
	files := map[string][]File{}
	snapshots := map[string]*Table{}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	var anyTable *Table
	for _, s := range plan.Stages {
		for _, in := range s.Inputs {
			t := in.Table
			if t == nil {
				continue
			}
			anyTable = t
			mu.Lock()
			_, seen := files[t.key()]
			files[t.key()] = nil
			if !seen {
				snapshots[t.key()] = t
			}
			mu.Unlock()
			if seen {
				continue
			}
			g.Go(func() error {
				if err := co.opts.Source.Refresh(gctx, t); err != nil {
					return err
				}
				fs, err := co.opts.Source.Files(gctx, t)
				mu.Lock()
				files[t.key()] = fs
				mu.Unlock()
				return err
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	for _, key := range plan.pins {
		if t := snapshots[key]; t.Versioned && t.Snapshot == 0 {
			// No snapshot to read it at: it is empty, but a commit could
			// fill it before the coordinator's part reads it.
			return nil, unsupported("table %s has no snapshot yet", t)
		}
	}

	loaded := time.Since(start)
	qid := filepath.Base(ex.dir)
	run := &execution{
		co: co, qid: qid, dir: ex.dir, ws: ws, files: files, settings: settings, force: plan.force,
		sp: &spool{limit: limit}, parts: min(maxPartitions, max(2, 2*len(ws))), root: shuffleRoot(co.opts.Warehouse, qid),
		exchanges: map[int]*exchangeRun{}, results: map[int]*jobResult{}, used: map[string]bool{},
	}
	if slices.ContainsFunc(plan.Stages, func(s *Stage) bool { return s.Out != nil }) {
		defer func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			if err := co.opts.Source.RemoveAll(ctx, anyTable, run.root); err != nil {
				co.log.Warn("could not remove exchanged rows", "dir", run.root, "err", err)
			}
		}()
	}
	if err := run.run(ctx, plan.Stages); err != nil {
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
	for _, s := range plan.Stages {
		if s.Out != nil {
			continue
		}
		ref, err := co.partials(run.results[s.ID])
		if err != nil {
			return nil, err
		}
		replaced := false
		rewrite(final, func(n node) (node, bool) {
			if str(n["type"]) == "BASE_TABLE" && str(n["table_name"]) == s.placeholder {
				replaced = true
				return clone(ref).(node), true
			}
			return nil, false
		})
		if !replaced {
			return nil, fmt.Errorf("stage %d has no place in the statement", s.ID)
		}
	}
	if err := co.pin(final, plan.pins, snapshots); err != nil {
		return nil, err
	}
	if ex.SQL, err = co.parser.print(final); err != nil {
		return nil, err
	}
	ex.Workers = len(run.used)
	co.pool.touch()
	co.log.Debug("ran a statement's parts", "query", qid, "stages", len(plan.Stages), "workers", ex.Workers,
		"snapshots_seconds", loaded.Seconds(), "seconds", time.Since(start).Seconds())
	co.opts.Metrics.query("ok", start)
	ok = true
	return ex, nil
}

// pin points the coordinator's own reads, in final, of tables the jobs
// read at the snapshots the workers read (AT (VERSION => ...)): a commit
// made while the workers ran, by Igloo or another engine, adding rows or
// delete files or changing the schema, is not seen by either part.
func (co *Coordinator) pin(final node, pins map[string]string, tables map[string]*Table) error {
	if len(pins) == 0 {
		return nil
	}
	var err error
	rewrite(final, func(n node) (node, bool) {
		if err != nil || str(n["type"]) != "BASE_TABLE" {
			return nil, false
		}
		key, ok := pins[strings.ToLower(strings.Join(tableParts(n), "\x00"))]
		if !ok {
			return nil, false
		}
		t := tables[key]
		if t == nil || !t.Versioned {
			// A table without snapshots, as in tests: nothing to pin.
			return nil, false
		}
		tmpl, perr := co.parser.parse(fmt.Sprintf("SELECT 1 FROM t AT (VERSION => %d)", t.Snapshot))
		if perr != nil {
			err = perr
			return nil, false
		}
		n["at_clause"] = child(child(tmpl, "from_table"), "at_clause")
		return n, true
	})
	return err
}

// rowCount adds up the files' record counts.
func rowCount(fs []File) int64 {
	var n int64
	for _, f := range fs {
		n += f.Rows
	}
	return n
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
// other workers if that one cannot be reached or stops answering. It
// returns the worker that ran it. A worker that answers with an error ran
// the task and is healthy, and another would answer the same: the error
// ends the job, without setting anyone's worker aside, and the statement
// runs on the coordinator unless the error is in its data. The result goes
// to path, within sp; a shuffle task writes each attempt to a directory of
// its own, so the files of a failed attempt are never read.
func (co *Coordinator) dispatch(ctx context.Context, task *taskRequest, ws []*worker, first int, path string, sp *spool) (*worker, *taskResult, error) {
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
		head, err := co.fetch(ctx, w, body, path, sp)
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
		var te *transportError
		if !errors.As(err, &te) {
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

// transportError is a worker that could not be reached, broke off its
// answer or failed as a server (HTTP 5xx), rather than one that ran a task
// and answered with its error.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// fetch sends a task to a worker and writes its result to path, within sp.
func (co *Coordinator) fetch(ctx context.Context, w *worker, body []byte, path string, sp *spool) (*taskResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+w.addr+tasksPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := co.pool.client.Do(req)
	if err != nil {
		return nil, &transportError{err}
	}
	defer resp.Body.Close() //nolint:errcheck
	br := bufio.NewReaderSize(resp.Body, 1<<16)
	if resp.StatusCode >= 500 {
		return nil, &transportError{fmt.Errorf("worker answered %s", resp.Status)}
	}
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
		return nil, &transportError{fmt.Errorf("read result header: %w", err)}
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
	src := &spoolReader{r: br, sp: sp}
	n, err := io.Copy(f, src)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	switch {
	case err == nil:
	case IsUnsupported(err):
		return nil, err
	case src.err != nil:
		return nil, &transportError{fmt.Errorf("receive result: %w", err)}
	default:
		return nil, fmt.Errorf("receive result: %w", err)
	}
	co.opts.Metrics.receive(n)
	return &head, nil
}

// spool bounds the disk a statement's partial results take on the
// coordinator, where they wait until every worker has answered.
type spool struct {
	limit int64
	used  atomic.Int64
}

// defaultMaxSpool is the spool of a statement when Options say nothing.
const defaultMaxSpool = 1 << 30

// spoolReader reads a worker's result into the spool, and fails, with an
// error that sends the statement back to the coordinator, once the spool
// is full. err is the error reading from the worker, if any.
type spoolReader struct {
	r   io.Reader
	sp  *spool
	err error
}

func (s *spoolReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		s.err = err
	}
	if s.sp != nil && s.sp.used.Add(int64(n)) > s.sp.limit {
		return n, unsupported("its partial results would take more than %d MiB of the coordinator's disk", s.sp.limit>>20)
	}
	return n, err
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
