package distributed

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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

// When a part of a statement is worth considering for workers, in auto
// mode: when the coordinator would take a while to read the table it would
// divide, cold, at its threads a core (see coldRateAt). Running workers are
// considered for parts of a few seconds; workers are started only for parts
// of a minute or more, besides the start, since a start takes a while and
// started workers run until they have been idle for a while. The estimates
// then decide (see choose). At 64 MiB/s a core, as this once took the
// coordinator to read, a report's table of 77 GB at ten times the data was
// held under the minute; read cold, it took 259 s on one machine and 40 s on
// workers.
const (
	warmSeconds = 4
	coldSeconds = 60
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
	// Cores are the coordinator's cores, and Threads its DuckDB threads,
	// for estimating how long a statement would take on it; zero cores
	// means those Go may use (runtime.GOMAXPROCS), zero threads one a
	// core.
	Cores   int
	Threads int
	Metrics *Metrics
	// MaxSpoolBytes bounds what a statement's partial results may take on
	// the coordinator's disk; past it, the statement runs on the
	// coordinator instead. Zero means 1 GiB.
	MaxSpoolBytes int64
	// MemoryBytes is the coordinator's DuckDB memory limit, for estimating
	// whether a statement fits it (see choose); zero for unknown.
	MemoryBytes int64
	// Threshold is how long a statement must take on one machine before
	// auto mode runs it on workers, unless it would run short of memory.
	// Zero means DefaultThreshold.
	Threshold time.Duration
	// Storage is the warehouse's object storage, where the coordinator
	// removes exchanges (see sweep).
	Storage Storage
	// LocalExchanges makes workers write exchanges to their own disks,
	// where their room allows, rather than to object storage
	// (IGLOO_EXCHANGES=local; see localx.go).
	LocalExchanges bool
	// Prefetch tells workers, with each task, the sizes of the data files
	// it reads and the columns its query may read of them, for their
	// prefetchers (see WorkerOptions.Prefetch).
	Prefetch bool
}

// Coordinator splits statements into parts that workers run.
type Coordinator struct {
	opts Options
	pool *pool
	// maxBroadcast, if set, overrides what every worker may read in full,
	// for tests.
	maxBroadcast int64
	// aggregation, if set, overrides how aggregations complete their
	// groups, for tests (see planOptions); bitmaps, if set, whether
	// count(DISTINCT) combines roaring bitmaps.
	aggregation string
	bitmaps     *bool
	parser      *parser
	aggs        map[string]bool
	dir         string
	log         *slog.Logger
	stop        context.CancelFunc
	// background are the pool's checks and the sweeps, until stop.
	background sync.WaitGroup
	// history is what statements took before, for auto mode (see choose).
	history *history
	// outcomes are how relations of uncertain size came out (see probe.go).
	outcomes *outcomes

	// running are the statements whose exchanges are in use, by ID, which
	// sweeps leave (see hold); heartbeat is how often they write under
	// them.
	runningMu sync.Mutex
	running   map[string]bool
	heartbeat time.Duration
	// removals remove statements' exchanges in the background until
	// removeCtx is done.
	removals       sync.WaitGroup
	removeCtx      context.Context
	cancelRemovals context.CancelFunc
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
	if opts.Storage == nil {
		return nil, errors.New("distributed execution needs the warehouse's storage for exchanges")
	}
	if opts.Cores <= 0 {
		opts.Cores = runtime.GOMAXPROCS(0)
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
	removeCtx, cancelRemovals := context.WithCancel(context.Background())
	co := &Coordinator{
		opts: opts, parser: p, aggs: aggs, dir: dir, log: log, stop: stop,
		pool: newPool(opts.Pool, t, opts.Version, opts.Metrics, log), history: newHistory(), outcomes: &outcomes{byKey: map[string]outcome{}},
		running: map[string]bool{}, heartbeat: heartbeatEvery, removeCtx: removeCtx, cancelRemovals: cancelRemovals,
	}
	co.background.Go(func() { co.pool.run(ctx) })
	co.background.Go(func() { co.sweepLoop(ctx) })
	return co, nil
}

// Close stops the coordinator's background work. It waits closeGrace for
// the removal of exchanges, which sweeps finish otherwise.
func (co *Coordinator) Close() error {
	co.stop()
	co.background.Wait()
	removed := make(chan struct{})
	go func() {
		co.removals.Wait()
		close(removed)
	}()
	select {
	case <-removed:
	case <-time.After(closeGrace):
		co.cancelRemovals()
		<-removed
	}
	co.cancelRemovals()
	return co.parser.close()
}

// Table returns a catalog table at its current snapshot, its nested
// columns' types printed as DuckDB prints them.
func (co *Coordinator) Table(ctx context.Context, schema, name string) (*Table, error) {
	t, err := co.opts.Source.Table(ctx, schema, name)
	if err != nil || t == nil {
		return t, err
	}
	for i, c := range t.Columns {
		if c.Nested == nil {
			continue
		}
		typ, err := co.parser.typeName(c.Type)
		if err != nil {
			if t.Unsupported == "" {
				t.Unsupported = fmt.Sprintf("column %s has type %s", c.Name, c.Type)
			}
			continue
		}
		t.Columns[i].Type = typ
	}
	return t, nil
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
	plan, d, err := co.plan(ctx, query, env, mode, false, nil)
	if err != nil {
		return nil, err
	}
	if !d.onWorkers {
		return nil, &errUnsupported{reason: "one machine is faster: " + d.reason, key: plan.Key}
	}
	return plan, nil
}

// decision is where a statement runs and why, with the estimates that
// decided it.
type decision struct {
	onWorkers bool
	reason    string
	e         estimate
	ws        []*worker
}

// plan plans query as Plan does, and says where it runs: on workers unless
// auto mode chooses one machine. A dry run, as EXPLAIN DISTRIBUTED's,
// leaves the history and the log alone. sizes are relations' sizes
// measured before (see Execute).
func (co *Coordinator) plan(ctx context.Context, query string, env Env, mode Mode, dry bool, sizes map[string]knownSize) (*Plan, *decision, error) {
	if mode == ModeOff {
		return nil, nil, unsupported("distributed execution is off for this session")
	}
	opts := planOptions{force: mode == ModeOn || mode == ModeOnRunning, now: time.Now(), maxBroadcast: defaultBroadcast}
	ws := co.pool.known(ctx)
	cores := max(co.opts.Cores, 1)
	rate := int64(float64(cores) * coldRateAt(float64(max(co.opts.Threads, cores))/float64(cores)))
	switch {
	case mode == ModeOnRunning && len(ws) == 0:
		return nil, nil, unsupported("no workers are running, and only admins may start them at will")
	case len(ws) > 0:
		opts.minBytes = rate * warmSeconds
		if mem := smallestMemory(ws); mem > 0 {
			// Data files are compressed several times over, and hash tables
			// take more room than the values they hold.
			opts.maxBroadcast = mem / 8
		}
	case co.pool.canStart():
		opts.minBytes = rate*coldSeconds + int64(co.pool.startEstimate().Seconds())*rate
		// The workers it would start are those that ran before: a table
		// of 1.8 GB that workers of 24 GiB read in full would otherwise be
		// held to defaultBroadcast while they are stopped.
		if mem := co.pool.lastMemory(); mem > 0 {
			opts.maxBroadcast = mem / 8
		}
	default:
		return nil, nil, unsupported("no workers are running")
	}
	if co.maxBroadcast > 0 {
		opts.maxBroadcast = co.maxBroadcast
	}
	opts.aggregation = co.aggregation
	opts.bitmaps = co.bitmapsOn(ctx, env)
	opts.sizes, opts.taskMemory, opts.workers = sizes, taskMemory(ws), len(ws)
	if sizes == nil {
		// Relations measured large lately are planned as such at once.
		opts.large = co.outcomes.large(time.Now())
	}
	stmt, err := co.parser.parse(query)
	if err != nil {
		if IsUnsupported(err) {
			return nil, nil, err
		}
		return nil, nil, unsupported("%v", err)
	}
	plan, err := newPlanner(ctx, env, co.parser, co.aggs, opts).plan(stmt)
	if err != nil {
		return nil, nil, err
	}
	plan.force, plan.mayStart = opts.force, mode != ModeOnRunning
	plan.Key = planKey(stmt, plan.Stages)
	plan.replan = func(sizes map[string]knownSize) (*Plan, error) {
		// Planned as the statement was, whatever auto mode would choose
		// now: it runs on workers already.
		p, _, err := co.plan(ctx, query, env, mode, true, sizes)
		if err == nil {
			p.Key = plan.Key
		}
		return p, err
	}
	m := machine{cores: co.opts.Cores, threads: co.opts.Threads, memory: co.opts.MemoryBytes}
	e := estimateTimes(plan.Stages, m, co.fleet(ws, plan.mayStart))
	plan.workers = e.workers
	r, found := co.history.get(plan.Key)
	plan.shortOfMemory = e.memory || found && r.outOfMemory
	d := &decision{onWorkers: true, reason: "distributed_execution is " + string(mode), e: e, ws: ws}
	if mode != ModeAuto {
		return plan, d, nil
	}
	now := time.Now()
	onWorkers, explore, reason := choose(e, r, found, cmp.Or(co.opts.Threshold, DefaultThreshold), now)
	d.onWorkers, d.reason = onWorkers, reason
	if dry {
		return plan, d, nil
	}
	where := "one machine"
	if onWorkers {
		where = "workers"
	}
	co.log.Info("chose where a statement runs", "on", where, "reason", reason, "key", plan.Key,
		"one_machine_seconds", math.Round(e.oneMachine), "workers_seconds", math.Round(e.onWorkers), "workers", e.workers,
		"levels", e.levels, "stages", e.stages, "bytes", e.bytes, "exchanged", e.exchanged, "rows", e.rows,
		"short_of_memory", e.memory, "spills", e.spills)
	if explore {
		co.history.update(plan.Key, func(r *runs) { r.explored = now })
	}
	return plan, d, nil
}

// fleet describes the workers for an estimate: ws, those that answer, and
// as many more as Compute may start if mayStart.
func (co *Coordinator) fleet(ws []*worker, mayStart bool) fleet {
	f := fleet{cores: co.opts.Cores, running: len(ws), most: len(ws)}
	for i, w := range ws {
		if i == 0 || w.status.Cores > f.cores {
			f.cores, f.threads = w.status.Cores, w.status.Threads
		}
	}
	if mayStart && co.pool.canStart() {
		f.most = max(f.most, co.opts.Pool.MaxWorkers)
		f.start = co.pool.startEstimate()
	}
	return f
}

// Execution is a statement whose distributed parts have run.
type Execution struct {
	// SQL is the rest of the statement, which reads the parts' combined
	// results; the session runs it, with the statement's parameters.
	SQL string
	// Workers is how many workers ran tasks.
	Workers int
	// Stages is how many stages ran, and Exchanges how many exchanges they
	// wrote, rows that later stages read.
	Stages, Exchanges int
	// Probes say what the relations of uncertain size measured, and how
	// the statement read them (see runProbing).
	Probes []string

	dir string
	// exchanges describes what the statement's exchanges held, for tests'
	// failures and debug logs.
	exchanges string
}

// Close removes the partial results. Call it once SQL has run.
func (e *Execution) Close() {
	if e != nil && e.dir != "" {
		_ = os.RemoveAll(e.dir)
	}
}

// ErrNoWorkers means no worker could run the statement's parts.
var ErrNoWorkers = errors.New("no workers available")

// memoryRetries is how many times a statement whose task ran out of memory
// on a worker runs again in smaller tasks before it fails.
const memoryRetries = 2

// Keep keeps the workers from stopping as idle until release is called:
// the server holds them while a statement that ran tasks runs, on the
// coordinator too.
func (co *Coordinator) Keep() (release func()) { return co.pool.keep() }

// Setting is a session setting a statement runs with.
type Setting = setting

// Execute runs a plan's jobs on workers, with the session's values of
// SettingNames. An error that is not a *duckdb.Error from the data, nor the
// context's, means the statement should run on the coordinator instead.
func (co *Coordinator) Execute(ctx context.Context, plan *Plan, settings []Setting) (*Execution, error) {
	start := time.Now()
	defer co.pool.keep()()
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

	// As many workers as the estimates call for (see estimateTimes), or
	// every one when an admin forces it. Forced by anyone else, only
	// workers already running, and no more than the estimates call for.
	want := max(1, plan.workers)
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
	snapshots, files, err := co.load(ctx, plan.Stages)
	if err != nil {
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
	var cleanups []func()
	defer func() {
		for _, f := range cleanups {
			f()
		}
	}()
	parts := exchangePartitions(plan.Stages, ws, len(ws), uploadBytes(co.opts.Warehouse))
	var run *execution
	planned := plan
	// smaller counts the runs again in smaller tasks, and lostOnce says the
	// statement ran again without a worker lost with exchanges on its disk.
	smaller, lostOnce := 0, false
	for attempt := 0; ; attempt++ {
		// Each attempt's tasks and exchanges are its own, so that what a
		// canceled task of the last one still writes is not read; with
		// exchanges on workers' disks, a failed attempt's are removed before
		// the next runs, so that they do not take its room there.
		mark := len(cleanups)
		dropAttempt := func() {
			if !co.opts.LocalExchanges {
				// Object storage has room: they go at the end, as ever.
				return
			}
			for _, f := range cleanups[mark:] {
				f()
			}
			cleanups = cleanups[:mark]
		}
		aqid := qid
		if attempt > 0 {
			aqid += "-" + strconv.Itoa(attempt)
		}
		newRun := func(id string) *execution {
			r := &execution{
				co: co, qid: id, dir: ex.dir, ws: ws, files: files, settings: settings, force: planned.force,
				sp: &spool{limit: limit}, parts: parts, smaller: smaller, root: shuffleRoot(co.opts.Warehouse, id),
				exchanges: map[int]*exchangeRun{}, results: map[int]*jobResult{}, used: map[string]bool{},
			}
			// The exchanges are read only by the stages: once they have run,
			// the statement's rows need not wait for their removal.
			release := co.hold(id, r.root)
			releaseLocal := func() {}
			if co.opts.LocalExchanges {
				releaseLocal = co.holdLocal(id, r.ws)
			}
			cleanups = append(cleanups, func() {
				release()
				releaseLocal()
				co.removeLater(id, r.root)
				if co.opts.LocalExchanges {
					co.removeLocalLater(id, r.ws)
				}
			})
			return r
		}
		var err error
		if len(planned.uncertain) > 0 {
			ex.Probes = nil
		}
		run, plan, err = co.runProbing(ctx, planned, newRun(aqid), newRun, func(stages []*Stage) error {
			return co.loadMore(ctx, stages, snapshots, files)
		}, &ex.Probes)
		if err == nil {
			break
		}
		if OutOfMemory(err) && ctx.Err() == nil && attempt < memoryRetries {
			// Smaller tasks: twice the partitions, each task's share of a
			// divided table halved, and half the memory to group
			// partitions in (see groups).
			next := min(2*parts, maxPartitions)
			if most := writerPartitions(planned.Stages, ws, uploadBytes(co.opts.Warehouse)); most > 0 {
				next = min(next, max(most, len(ws), parts))
			}
			co.log.Warn("a task ran out of memory on a worker; running the statement's parts again in smaller tasks",
				"query", qid, "attempt", attempt+1, "partitions", next, "err", err)
			parts = next
			smaller++
			dropAttempt()
			if len(plan.uncertain) == 0 {
				// Its relations measured, the plan made with their sizes
				// runs again, rather than measuring them again.
				planned = plan
			}
			continue
		}
		var lost *lostError
		if errors.As(err, &lost) && ctx.Err() == nil && !lostOnce && co.confirmLost(ctx, ws, lost.worker) {
			// What the lost worker wrote to its disk is gone, and readers
			// cannot have it: the statement's parts run again, once, on the
			// other workers.
			rest := slices.DeleteFunc(slices.Clone(ws), func(w *worker) bool { return w.addr == lost.worker })
			if len(rest) > 0 && len(rest) < len(ws) {
				co.log.Warn("a worker holding exchanged rows on its disk was lost; running the statement's parts again without it",
					"query", qid, "attempt", attempt+1, "worker", lost.worker, "err", err)
				co.pool.markDown(lost.worker)
				ws, lostOnce = rest, true
				dropAttempt()
				if len(plan.uncertain) == 0 {
					planned = plan
				}
				continue
			}
		}
		switch {
		case ctx.Err() != nil:
			co.opts.Metrics.query("canceled", start)
			return nil, ctx.Err()
		case dataError(err):
			co.opts.Metrics.query("error", start)
		case OutOfMemory(err):
			co.opts.Metrics.query("out_of_memory", start)
		case BitmapMemory(err):
			co.opts.Metrics.query("bitmap_memory", start)
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
	for _, x := range run.overfull() {
		co.log.Warn("an exchange has more files than its tasks times its partitions", "query", qid, "exchange", x)
	}
	ex.Workers = len(run.used)
	ex.Stages, ex.Exchanges = len(plan.Stages), len(run.exchanges)
	ex.exchanges = run.describe()
	co.pool.touch()
	co.log.Debug("ran a statement's parts", "query", qid, "stages", len(plan.Stages), "workers", ex.Workers, "exchanges", ex.exchanges,
		"local", strings.Join(plan.local, "; "),
		"snapshots_seconds", loaded.Seconds(), "seconds", time.Since(start).Seconds())
	co.opts.Metrics.query("ok", start)
	ok = true
	return ex, nil
}

// load brings every table the stages read to its current snapshot and lists
// its data files, once per table: the tables and their files, by key. The
// tables are all collected before any listing starts, so that nothing but
// its listing writes a table's files, however many inputs read it.
func (co *Coordinator) load(ctx context.Context, stages []*Stage) (map[string]*Table, map[string][]File, error) {
	tables := map[string]*Table{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if t := in.Table; t != nil && tables[t.key()] == nil {
				tables[t.key()] = t
			}
		}
	}
	files := make(map[string][]File, len(tables))
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for key, t := range tables {
		g.Go(func() error {
			if err := co.opts.Source.Refresh(gctx, t); err != nil {
				return err
			}
			fs, err := co.opts.Source.Files(gctx, t)
			if err != nil {
				return err
			}
			mu.Lock()
			files[key] = fs
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	return tables, files, nil
}

// loadMore loads, as load does, the tables stages read that tables and
// files do not hold yet, into them.
func (co *Coordinator) loadMore(ctx context.Context, stages []*Stage, tables map[string]*Table, files map[string][]File) error {
	var missing []*Stage
	for _, s := range stages {
		if slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Table != nil && tables[in.Table.key()] == nil }) {
			missing = append(missing, s)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	more, moreFiles, err := co.load(ctx, missing)
	if err != nil {
		return err
	}
	for k, t := range more {
		if tables[k] == nil {
			tables[k], files[k] = t, moreFiles[k]
		}
	}
	return nil
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
		if t.Local != "" {
			t.Local += "/a" + strconv.Itoa(attempt)
		}
		w := ws[(first+attempt)%len(ws)]
		t.Worker = w.addr
		body, err := json.Marshal(&t)
		if err != nil {
			return nil, nil, err
		}
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
		if res.Error.Lost != "" {
			return nil, &lostError{worker: res.Error.Lost, err: fmt.Errorf("a task on %s: %s", w.addr, res.Error.Message)}
		}
		switch {
		case res.Error.Bitmap:
			// The message keeps the extension's words, which the session
			// recognizes to run the statement again without bitmaps.
			return nil, fmt.Errorf("%w: a task on %s: %s", ErrBitmapMemory, w.addr, res.Error.Message)
		case duckdb.ErrorType(res.Error.Type) == duckdb.ErrorTypeOutOfMemory:
			return nil, fmt.Errorf("%w: a task on %s: %s", ErrWorkerMemory, w.addr, res.Error.Message)
		case res.Error.OutOfMemory:
			return nil, fmt.Errorf("%w: %s, and canceled a task of the statement, which had run the longest there", ErrWorkerMemory, w.addr)
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
		cols[i] = quoteIdent(c.Name)
		if _, err := co.parser.expr("CAST(NULL AS " + c.Type + ")"); err == nil {
			cols[i] = "CAST(" + quoteIdent(c.Name) + " AS " + c.Type + ") AS " + quoteIdent(c.Name)
		}
		// Otherwise a type DuckDB prints but cannot read, such as a row's
		// STRUCT(BIGINT, BIGINT), which Parquet keeps as it is.
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
