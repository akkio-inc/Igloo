package distributed

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/config"
)

// A plan runs as stages. A stage is a set of tasks that each run the
// stage's fragment over their part of its inputs, and send the rows either
// to the coordinator, as partial results its part of the statement reads,
// or to an exchange: Parquet files in object storage, under the
// warehouse's _igloo_shuffle/, one directory per partition of a hash of
// some of the rows' columns. Later stages read an exchange one partition
// per task, so that rows with equal keys meet in one task, or all of it in
// every task.
//
// A stage's tasks are:
//
//   - one per share of the data files of its split input, a catalog table
//     divided among the workers;
//   - otherwise one per partition, when it reads exchanges by partition;
//   - otherwise one.
//
// Every exchange of a statement has the same number of partitions, and
// hashes its keys the same way (see Exchange), so stages may read several
// exchanges partitioned on equal keys together. Workers stay stateless: a
// task that fails runs again elsewhere over the same files, and each
// attempt of a task that writes an exchange writes a directory of its own,
// whose files are read only if it succeeded.

// Stage is a step of a plan: tasks that run one fragment each.
type Stage struct {
	ID int
	// SQL is the fragment every task runs.
	SQL string
	// Columns rename the fragment's result columns positionally, when the
	// fragment's own names may clash, as in SELECT * of a join.
	Columns []string
	// Inputs are the relations the fragment reads, by view name.
	Inputs []Input
	// Out is the exchange the stage writes its rows to. Without one, the
	// rows go to the coordinator, where the statement's final query reads
	// them in place of placeholder.
	Out *Exchange

	fragment    node
	placeholder string
}

// Input is a relation a stage's fragment reads under a view name: a
// catalog table, or an exchange.
type Input struct {
	View string
	// Table is a catalog table. Split divides its data files among the
	// stage's tasks; otherwise every task reads all of them.
	Table *Table
	Split bool
	// Exchange is the rows earlier stages wrote. Partitioned gives each task
	// one partition; otherwise every task reads all of them.
	Exchange    *Exchange
	Partitioned bool
}

// Exchange is rows that stages write to object storage, partitioned by a
// hash of their key columns, for later stages to read.
type Exchange struct {
	ID int
	// Keys are the columns of the producers' rows that decide a row's
	// partition, by name; with none, every row goes to partition 0.
	Keys []string
	// Casts are the types the keys are hashed as, or "" for their own:
	// integers of every width are hashed as BIGINT, so that equal values of
	// different types land in the same partition.
	Casts []string
	// purpose says what the exchange is for, in logs and tests.
	purpose string
}

// String describes a stage for logs.
func (s *Stage) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stage %d", s.ID)
	for _, in := range s.Inputs {
		switch {
		case in.Table != nil && in.Split:
			fmt.Fprintf(&b, " split %s", in.Table)
		case in.Exchange != nil && in.Partitioned:
			fmt.Fprintf(&b, " partitions of exchange %d", in.Exchange.ID)
		case in.Exchange != nil:
			fmt.Fprintf(&b, " all of exchange %d", in.Exchange.ID)
		}
	}
	if s.Out != nil {
		fmt.Fprintf(&b, " to exchange %d (%s)", s.Out.ID, s.Out.purpose)
	} else {
		b.WriteString(" to the coordinator")
	}
	return b.String()
}

// split returns the stage's split input, if it has one.
func (s *Stage) split() *Input {
	for i := range s.Inputs {
		if s.Inputs[i].Table != nil && s.Inputs[i].Split {
			return &s.Inputs[i]
		}
	}
	return nil
}

// partitioned reports whether the stage reads an exchange by partition.
func (s *Stage) partitioned() bool {
	return slices.ContainsFunc(s.Inputs, func(in Input) bool { return in.Exchange != nil && in.Partitioned })
}

// hashKey returns the expression a key column is hashed by.
func hashKey(name, cast string) string {
	e := quoteIdent(name)
	if cast != "" {
		e = "CAST(" + e + " AS " + cast + ")"
	}
	return e
}

// exchangeSQL wraps a stage's fragment for writing to its exchange: the
// fragment's rows, renamed, with __b, the partition of each.
func exchangeSQL(s *Stage, parts int) string {
	inner := s.SQL
	if len(s.Columns) > 0 {
		names := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			names[i] = quoteIdent(c)
		}
		inner = "SELECT * FROM (" + inner + ") AS __igloo_f(" + strings.Join(names, ", ") + ")"
	}
	b := "CAST(0 AS INTEGER)"
	if len(s.Out.Keys) > 0 {
		keys := make([]string, len(s.Out.Keys))
		for i, k := range s.Out.Keys {
			keys[i] = hashKey(k, s.Out.Casts[i])
		}
		b = fmt.Sprintf("CAST(hash(%s) %% %d AS INTEGER)", strings.Join(keys, ", "), parts)
	}
	return "SELECT *, " + b + " AS __b FROM (" + inner + ") AS __igloo_x"
}

// --- running stages ---------------------------------------------------------

// maxPartitions caps the partitions of a statement's exchanges, and so the
// tasks of a stage that reads them by partition; partitionBytes is how much
// of the largest divided table a partition is for (see partitions).
const (
	maxPartitions  = 256
	partitionBytes = 256 << 20
)

// partitions is how many partitions a statement's exchanges have: twice
// the workers, so that a stage that reads them by partition keeps every
// worker busy, and more as the divided tables grow, one for each per bytes
// of the largest, so that each task's hash tables (the distinct people of
// a share of the cohorts, say) hold a share of the rows that fits a
// worker's memory: a report's hash tables held several times its tables'
// compressed bytes. But no more than eight times the workers: an exchange
// is as many files as the tasks that write it times its partitions, and
// what it holds is often far smaller than the table, as a filtered
// audience is.
func partitions(workers int, largest, per int64) int {
	n := max(2, 2*workers)
	if per > 0 {
		n = max(n, int(min((largest+per-1)/per, maxPartitions)))
	}
	return min(n, max(2, 8*workers), maxPartitions)
}

// execution runs one statement's stages.
type execution struct {
	co       *Coordinator
	qid, dir string
	ws       []*worker
	files    map[string][]File
	settings []setting
	force    bool
	sp       *spool
	// parts is how many partitions every exchange has.
	parts int
	// smaller is how many times the statement ran again because a task ran
	// out of memory: each halves the tasks (see Execute).
	smaller int
	// root is the statement's directory in object storage for exchanges.
	root      string
	exchanges map[int]*exchangeRun
	results   map[int]*jobResult
	// held are stages, by ID, that wait for their channel to close once
	// what they read is complete: those that read relations being
	// measured, until the statement is planned again or not (see
	// runProbing).
	held map[int]<-chan struct{}

	mu sync.Mutex
	// used are the workers that ran its tasks, and usedBy those of each
	// stage, by ID.
	used   map[string]bool
	usedBy map[int]map[string]bool
}

// exchangeRun is an exchange while its producers run.
type exchangeRun struct {
	// producers counts the stages that still write to it; done is closed
	// when none does.
	producers int
	done      chan struct{}

	mu      sync.Mutex
	columns []Column
	files   [][]string
	// rows counts the rows written, in all and by partition, and bytes
	// the bytes of their files, in all and by partition; tasks counts the
	// tasks that wrote them.
	rows      int64
	partRows  []int64
	bytes     int64
	partBytes []int64
	tasks     int
}

// jobResult is where a coordinator-bound stage's partial results are.
type jobResult struct {
	files   []string
	columns []Column
	workers map[string]bool
}

// run runs every stage, each once the exchanges it reads are complete.
func (e *execution) run(ctx context.Context, stages []*Stage) error {
	return e.start(ctx, stages)()
}

// start starts running stages as run does, and returns what waits for them
// to finish. The exchanges they write are in e.exchanges when it returns.
func (e *execution) start(ctx context.Context, stages []*Stage) (wait func() error) {
	for _, s := range stages {
		if s.Out == nil {
			continue
		}
		x := e.exchanges[s.Out.ID]
		if x == nil {
			x = &exchangeRun{done: make(chan struct{}), files: make([][]string, e.parts), partRows: make([]int64, e.parts),
				partBytes: make([]int64, e.parts)}
			e.exchanges[s.Out.ID] = x
		}
		x.producers++
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, s := range stages {
		g.Go(func() error {
			for _, in := range s.Inputs {
				if in.Exchange == nil {
					continue
				}
				x := e.exchanges[in.Exchange.ID]
				if x == nil {
					return fmt.Errorf("stage %d reads exchange %d, which no stage writes", s.ID, in.Exchange.ID)
				}
				select {
				case <-x.done:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			if held := e.held[s.ID]; held != nil {
				select {
				case <-held:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			start := time.Now()
			if err := e.stage(gctx, s); err != nil {
				return err
			}
			e.co.log.Debug("ran a stage", "query", e.qid, "stage", s.String(), "seconds", time.Since(start).Seconds())
			if s.Out != nil {
				x := e.exchanges[s.Out.ID]
				x.mu.Lock()
				x.producers--
				if x.producers == 0 {
					close(x.done)
				}
				x.mu.Unlock()
			}
			return nil
		})
	}
	return g.Wait
}

// prefetchUsed returns, with prefetching on (Options.Prefetch), the field
// IDs of the columns the stage's query may read of each catalog table it
// reads, by view name; nil with it off, or for a query the walk cannot
// read, whose tasks are then not prefetched.
func (e *execution) prefetchUsed(ctx context.Context, s *Stage) map[string][]int {
	if !e.co.opts.Prefetch {
		return nil
	}
	env := stageViews{}
	for _, in := range s.Inputs {
		if in.Table != nil {
			env[strings.ToLower(in.View)] = in
		}
	}
	w, err := walkScans(ctx, e.co.parser, s.SQL, env)
	if err != nil {
		e.co.log.Debug("could not tell what a stage reads, for prefetching", "query", e.qid, "stage", s.String(), "err", err)
		return nil
	}
	used := map[string][]int{}
	for _, key := range w.order {
		u := w.uses[key]
		in := env[strings.ToLower(u.name)]
		var sc Scan
		w.read(&sc, in.Table.Columns, u)
		used[in.View] = append([]int{}, sc.FieldIDs...)
	}
	return used
}

// stageViews resolves the names of a stage's views of catalog tables, by
// view name in lower case, for walkScans.
type stageViews map[string]Input

func (v stageViews) Resolve(_ context.Context, parts []string) (string, string, bool, error) {
	if len(parts) != 1 {
		return "", "", false, nil
	}
	in, ok := v[strings.ToLower(parts[0])]
	return "", in.View, ok, nil
}

func (stageViews) ViewDefinition(context.Context, []string) (*ViewDefinition, bool, error) {
	return nil, false, nil
}

// prefetchOf returns what a worker's prefetcher needs of a view of files,
// with used from prefetchUsed: nil if the stage's query was not read so.
func prefetchOf(used map[string][]int, view string, files []File) *viewPrefetch {
	ids, ok := used[view]
	if !ok {
		return nil
	}
	p := &viewPrefetch{Sizes: make([]int64, len(files)), Used: ids}
	for i, f := range files {
		p.Sizes[i] = f.Size
	}
	return p
}

// stage runs one stage's tasks.
func (e *execution) stage(ctx context.Context, s *Stage) error {
	// The tasks, and each one's share of the split input.
	tasks := 1
	var shares [][]File
	var owners []int
	for _, in := range s.Inputs {
		if in.Table == nil {
			continue
		}
		if _, ok := e.files[in.Table.key()]; !ok {
			// Read as empty, its rows would be missing from the results.
			return fmt.Errorf("stage %d reads table %s, whose files were not listed", s.ID, in.Table)
		}
	}
	if in := s.split(); in != nil {
		split := e.files[in.Table.key()]
		n := len(e.ws)
		if want := int((in.Table.Bytes + bytesPerWorker - 1) / bytesPerWorker); want < n && !e.force {
			n = max(want, min(2, n))
		}
		n = max(1, min(n, len(split)))
		shares, owners = divide(assign(split, e.ws[:n]), 1<<e.smaller)
		tasks = len(shares)
	}
	var groups [][]int
	if s.partitioned() {
		groups = e.groups(s)
		tasks = len(groups)
	}
	e.co.log.Debug("running a stage", "query", e.qid, "stage", s.String(), "tasks", tasks, "partitions", e.parts)

	// What every task reads in full, and its bytes.
	used := e.prefetchUsed(ctx, s)
	whole := map[string]viewDef{}
	var wholeBytes int64
	for _, in := range s.Inputs {
		switch {
		case in.Table != nil && !in.Split:
			def := viewDef{Name: in.View, Columns: in.Table.Columns, Rows: rowCount(e.files[in.Table.key()])}
			def.Files, def.Deletes = paths(e.files[in.Table.key()]), deletesOf(e.files[in.Table.key()])
			def.Prefetch = prefetchOf(used, in.View, e.files[in.Table.key()])
			whole[in.View] = def
			for _, f := range e.files[in.Table.key()] {
				wholeBytes += f.Size
			}
		case in.Exchange != nil && !in.Partitioned:
			x := e.exchanges[in.Exchange.ID]
			def := viewDef{Name: in.View, Columns: x.columns, ByName: true, Rows: x.rows, Exact: true}
			for _, fs := range x.files {
				def.Files = append(def.Files, fs...)
			}
			whole[in.View] = def
			wholeBytes += x.bytes
		}
	}

	var res *jobResult
	if s.Out == nil {
		res = &jobResult{files: make([]string, tasks), workers: map[string]bool{}}
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	for i := range tasks {
		task := &taskRequest{Query: e.qid, Task: s.ID*10000 + i, Version: e.co.opts.Version, Settings: e.settings}
		// bytes are what it reads of a table's share, reads all it reads.
		var bytes int64
		reads := wholeBytes
		for _, in := range s.Inputs {
			switch {
			case in.Table != nil && in.Split:
				share := shares[i]
				for _, f := range share {
					bytes += f.Size
				}
				task.Views = append(task.Views, viewDef{Name: in.View, Columns: in.Table.Columns, Files: paths(share), Rows: rowCount(share),
					Deletes: deletesOf(share), Prefetch: prefetchOf(used, in.View, share)})
			case in.Exchange != nil && in.Partitioned:
				x := e.exchanges[in.Exchange.ID]
				def := viewDef{Name: in.View, Columns: x.columns, ByName: true, Exact: true}
				for _, p := range groups[i] {
					def.Files = append(def.Files, x.files[p]...)
					def.Rows += x.partRows[p]
					reads += x.partBytes[p]
				}
				task.Views = append(task.Views, def)
			default:
				task.Views = append(task.Views, whole[in.View])
			}
		}
		out := ""
		if s.Out == nil {
			task.SQL, task.Columns = s.SQL, s.Columns
			out = filepath.Join(e.dir, fmt.Sprintf("%d-%d.parquet", s.ID, i))
		} else {
			task.SQL = exchangeSQL(s, e.parts)
			task.Shuffle = e.root + "/s" + strconv.Itoa(s.ID) + "/" + strconv.Itoa(i)
			task.Partitions = e.parts
			if e.co.opts.LocalExchanges {
				// What it writes is taken to be at most what it reads.
				task.Local = e.qid + "/s" + strconv.Itoa(s.ID) + "/" + strconv.Itoa(i)
				task.Expect = reads + bytes
			}
		}
		if e.co.opts.LocalExchanges {
			for _, w := range e.ws {
				task.Peers = append(task.Peers, w.addr)
			}
		}
		g.Go(func() error {
			var sp *spool
			if s.Out == nil {
				sp = e.sp
			}
			first := i % len(e.ws)
			if owners != nil {
				first = owners[i]
			}
			w, head, err := e.co.dispatch(gctx, task, e.ws, first, out, sp)
			if err != nil {
				return err
			}
			e.co.opts.Metrics.scan(bytes)
			e.mu.Lock()
			e.used[w.addr] = true
			if e.usedBy == nil {
				e.usedBy = map[int]map[string]bool{}
			}
			if e.usedBy[s.ID] == nil {
				e.usedBy[s.ID] = map[string]bool{}
			}
			e.usedBy[s.ID][w.addr] = true
			e.mu.Unlock()
			if s.Out != nil {
				return e.received(s, task, head, w)
			}
			mu.Lock()
			defer mu.Unlock()
			res.files[i] = out
			res.workers[w.addr] = true
			if res.columns == nil {
				res.columns = head.Columns
			} else if !slices.Equal(res.columns, head.Columns) {
				return fmt.Errorf("workers returned different columns for stage %d", s.ID)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if res != nil {
		e.mu.Lock()
		e.results[s.ID] = res
		e.mu.Unlock()
	}
	return nil
}

// describe says what each exchange held: rows and files by partition.
func (e *execution) describe() string {
	ids := slices.Sorted(maps.Keys(e.exchanges))
	var b strings.Builder
	for _, id := range ids {
		x := e.exchanges[id]
		fmt.Fprintf(&b, "exchange %d: %d rows;", id, x.rows)
		for p := range x.files {
			fmt.Fprintf(&b, " %d:%d/%d", p, x.partRows[p], len(x.files[p]))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// overfull describes the exchanges with more files than their tasks times
// their partitions, which a task writes at most (see Worker.shuffle).
func (e *execution) overfull() []string {
	var out []string
	for _, id := range slices.Sorted(maps.Keys(e.exchanges)) {
		x := e.exchanges[id]
		files := 0
		for _, fs := range x.files {
			files += len(fs)
		}
		if files > x.tasks*e.parts {
			out = append(out, fmt.Sprintf("exchange %d: %d files from %d tasks of %d partitions", id, files, x.tasks, e.parts))
		}
	}
	return out
}

// received records the files a task wrote to its stage's exchange.
func (e *execution) received(s *Stage, task *taskRequest, head *taskResult, w *worker) error {
	x := e.exchanges[s.Out.ID]
	x.mu.Lock()
	defer x.mu.Unlock()
	x.rows += head.Rows
	x.tasks++
	if x.columns == nil {
		x.columns = head.Columns
	} else if !slices.Equal(x.columns, head.Columns) {
		return fmt.Errorf("stages wrote different columns to exchange %d", s.Out.ID)
	}
	var listed int64
	for _, f := range head.Files {
		path := f.Path
		switch {
		case f.Partition < 0 || f.Partition >= e.parts:
			return fmt.Errorf("a worker wrote an unexpected file %s", f.Path)
		case strings.HasPrefix(f.Path, task.Shuffle+"/"):
		case task.Local != "" && strings.HasPrefix(f.Path, task.Local+"/"):
			// On the worker's disk: readers fetch it from there.
			path = localRef(w.addr, f.Path, f.Size)
		default:
			return fmt.Errorf("a worker wrote an unexpected file %s", f.Path)
		}
		x.files[f.Partition] = append(x.files[f.Partition], path)
		x.partRows[f.Partition] += f.Rows
		x.partBytes[f.Partition] += f.Size
		x.bytes += f.Size
		listed += f.Rows
	}
	if listed != head.Rows {
		return fmt.Errorf("a worker listed files of %d rows for the %d it wrote to exchange %d", listed, head.Rows, s.Out.ID)
	}
	return nil
}

// shuffleRoot is a statement's directory for exchanges in object storage.
func shuffleRoot(warehouse, qid string) string {
	return warehouse + config.ShuffleDir + "/" + qid
}
