package distributed

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	// completes says the stage completes an aggregation's groups from
	// partial results it reads by partition, whose partitions hold them
	// whole: one task may complete them all (see execution.groups).
	completes bool
	// merge, for a stage whose rows are the statement's result, ordered by
	// ORDER BY without LIMIT, are the columns its tasks sort their rows by,
	// which the coordinator merges their files by (see merge.go).
	merge []mergeKey
	// enough, for a stage whose rows go to the coordinator to take any so
	// many of them (LIMIT without ORDER BY), is that many: once the tasks
	// that ended sent as many, the others are stopped, and the coordinator
	// reads what those sent.
	enough int64
	// writeSQL, for a stage whose rows are the result of a statement that
	// fills a table, row by row (see markWrites), is the query that gives a
	// task's rows of the result: its tasks write them, where the statement
	// has a target (see writes.go).
	writeSQL string
	// rows, for a stage that writes an aggregation's partial results, is how
	// it may write them a row at a time, and rowSQL the fragment that does
	// (see rowSpec).
	rows   *rowSpec
	rowSQL string
}

// Input is a relation a stage's fragment reads under a view name: a
// catalog table, or an exchange.
type Input struct {
	View string
	// Table is a catalog table. Split divides its data files among the
	// stage's tasks; Bucketed gives each task the files of some buckets of
	// its bucketing Bucket (see bucket.go); otherwise every task reads all
	// of them.
	Table    *Table
	Split    bool
	Bucketed bool
	Bucket   int
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
	// different types land in the same partition. A cast may instead be an
	// expression of the key, upper(%s), whose value is hashed (see
	// keyExpression).
	Casts []string
	// Buckets, if not 0, says its rows go to the partitions the files of a
	// table bucketed by its one key into so many buckets are read by, beside
	// which a stage reads them (see bucket.go): by the key's Iceberg bucket
	// rather than its hash.
	Buckets int
	// UUIDHash, for a key of UUIDs written to buckets, is how the table's files
	// hash them (see bucket.go).
	UUIDHash string
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
		case in.Table != nil && in.Bucketed:
			fmt.Fprintf(&b, " buckets of %s", in.Table)
		case in.Exchange != nil && in.Partitioned:
			fmt.Fprintf(&b, " partitions of exchange %d", in.Exchange.ID)
		case in.Exchange != nil:
			fmt.Fprintf(&b, " all of exchange %d", in.Exchange.ID)
		}
	}
	if s.Out != nil {
		fmt.Fprintf(&b, " to exchange %d (%s)", s.Out.ID, s.Out.purpose)
		if s.Out.Buckets > 0 {
			fmt.Fprintf(&b, " by bucket[%d]", s.Out.Buckets)
		}
		switch {
		case s.rowSQL != "" && s.rows.first:
			b.WriteString(", partial results a row at a time")
		case s.rowSQL != "":
			b.WriteString(", partial results a row at a time once a task's groups are single rows")
		}
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

// hashKey returns the expression a key column is hashed by: the column, as
// a type, or in an expression of it (see keyExpression).
func hashKey(name, cast string) string {
	e := quoteIdent(name)
	switch {
	case keyExpression(cast):
		e = fmt.Sprintf(cast, e)
	case cast != "":
		e = "CAST(" + e + " AS " + cast + ")"
	}
	return e
}

// keyExpression says whether an exchange's key cast is an expression of
// the key column, %s, such as upper(%s), rather than a type: rows are
// partitioned by its value, as a join on upper(a.x) = upper(b.y) needs.
func keyExpression(cast string) bool { return strings.Contains(cast, "%s") }

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
		if s.Out.Buckets > 0 {
			key := keys[0]
			if s.Out.UUIDHash == uuidSpec {
				// Its 16 bytes: the function hashes binary values as the spec does.
				key = "unhex(replace(CAST(" + key + " AS VARCHAR), '-', ''))"
			}
			// NULL keys are where a bucketed table's are (see bucketGroups).
			b = fmt.Sprintf("CAST(COALESCE(iceberg_bucket(%d, %s), 0) %% %d AS INTEGER)", s.Out.Buckets, key, parts)
		}
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
	// variables are the values the coordinator computed, which tasks set
	// before they run (see values.go).
	variables []variable
	// target, if set, says the tasks of a stage that can write the
	// statement's rows do, and written lists what they wrote (see writes.go).
	target  *WriteTarget
	written []WrittenFile
	// local says workers write exchanges to their own disks (see localFor).
	local bool
	// erased is the latest deletion of erasure requests the coordinator
	// knew of when the statement began, which its tasks carry.
	erased int64
	force  bool
	sp     *spool
	// parts is how many partitions every exchange has.
	parts int
	// smaller is how many times the statement ran again because a task ran
	// out of memory: each halves the tasks (see Execute). slots are the
	// workers' slots each task takes, and the shares of a worker's memory
	// it has (see retryShape); 0 means 1.
	smaller int
	slots   int
	// places are its places on its workers, which its stages' tasks take
	// (see places); nil sends each task at once.
	places *places
	// roots are the statement's directories in object storage for
	// exchanges (see spreadRoots).
	roots     []string
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
	// started are its stages, and finished those that ended, by ID; lost the
	// workers it went on without, as they took their exchanges with them; see
	// repair.go, which repairing keeps to one at a time.
	started   []*Stage
	finished  map[int]bool
	lost      map[string]bool
	repairing sync.Mutex
	// holders are the workers watched for the exchange files they hold of the run,
	// while watching, the run's first context, lasts (see watchHolder).
	holders   map[string]bool
	watching  context.Context
	stopWatch context.CancelFunc
}

// exchangeRun is an exchange while its producers run.
type exchangeRun struct {
	// producers counts the stages that still write to it; done is closed
	// when none does.
	producers int
	done      chan struct{}

	mu      sync.Mutex
	columns []Column
	// files are the files of each partition.
	files [][]string
	// rows counts the rows written, in all and by partition, and bytes
	// the bytes of their files, in all and by partition; tasks counts the
	// tasks that wrote them.
	rows      int64
	partRows  []int64
	bytes     int64
	partBytes []int64
	tasks     int
	// ran are the tasks whose files are on workers' disks (see repair.go), and
	// again counts the times one of them ran again, which changed its files.
	ran   []*ranTask
	again int64
}

// jobResult is where a coordinator-bound stage's partial results are.
type jobResult struct {
	files   []string
	columns []Column
	workers map[string]bool
	// merged, if set, is the files merged as they are read (see
	// mergeOrdered).
	merged *MergedView
}

// run runs every stage, each once the exchanges it reads are complete.
func (e *execution) run(ctx context.Context, stages []*Stage) error {
	return e.start(ctx, stages)()
}

// start starts running stages as run does, and returns what waits for them
// to finish. The exchanges they write are in e.exchanges when it returns.
func (e *execution) start(ctx context.Context, stages []*Stage) (wait func() error) {
	e.mu.Lock()
	e.started = append(e.started, stages...)
	if e.watching == nil {
		e.watching, e.stopWatch = context.WithCancel(ctx)
	}
	e.mu.Unlock()
	for _, s := range stages {
		if s.Out == nil {
			continue
		}
		x := e.exchanges[s.Out.ID]
		if x == nil {
			x = &exchangeRun{done: make(chan struct{}), files: make([][]string, e.parts),
				partRows: make([]int64, e.parts), partBytes: make([]int64, e.parts)}
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
			e.mu.Lock()
			if e.finished == nil {
				e.finished = map[int]bool{}
			}
			e.finished[s.ID] = true
			e.mu.Unlock()
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

// piece is task i of a stage, or the j-th of k smaller tasks it is divided into when a
// worker running it, or holding what it wrote, is lost (see repair.go): the j-th of
// every k of its buckets, partitions or files of the split table.
type piece struct{ i, j, k int }

// wholeTask is task i, undivided.
func wholeTask(i int) piece { return piece{i: i, k: 1} }

// split reports whether p is a smaller task of one divided.
func (p piece) split() bool { return p.k > 1 }

// pieceIndex spaces the directories of pieces from their tasks' (see index).
const pieceIndex = 100000

// tasksPerStage numbers a stage's tasks: task i of stage s is s*tasksPerStage + i.
const tasksPerStage = 10000

// index names p's directories and files: its task's number, or one of its own above
// all tasks', so that what an attempt of the undivided task leaves is never read.
func (p piece) index() int {
	if p.split() {
		return p.i + pieceIndex*(p.j+1)
	}
	return p.i
}

// pieceOf returns p's share of list: all of it for a task undivided.
func pieceOf[T any](list []T, p piece) []T {
	if !p.split() {
		return list
	}
	var out []T
	for n := p.j; n < len(list); n += p.k {
		out = append(out, list[n])
	}
	return out
}

// ranPiece is a piece that ran, and where.
type ranPiece struct {
	p      piece
	first  int
	rounds int
	task   *taskRequest
	head   *taskResult
	w      *worker
	bytes  int64
	out    string
}

// onePartition is a task that ran out of memory over one partition of the
// exchanges it read, which smaller tasks would not divide (see Execute);
// rows are the most rows one of those exchanges held in all.
type onePartition struct {
	err  error
	rows int64
}

func (e *onePartition) Error() string { return e.err.Error() }
func (e *onePartition) Unwrap() error { return e.err }

// exchangeRows returns the most rows an exchange that stage s reads by
// partition holds, as written.
func (e *execution) exchangeRows(s *Stage) int64 {
	var most int64
	for _, in := range s.Inputs {
		if in.Exchange == nil || !in.Partitioned {
			continue
		}
		if x := e.exchanges[in.Exchange.ID]; x != nil {
			x.mu.Lock()
			most = max(most, x.rows)
			x.mu.Unlock()
		}
	}
	return most
}

// prefetchUsed returns, with prefetching on (Options.Prefetch), what the
// stage's query reads of each catalog table it reads, by view name: the
// field IDs of the columns it may read, and whether a WHERE clause names
// one; nil with it off, or for a query the walk cannot read, whose tasks
// are then not prefetched.
func (e *execution) prefetchUsed(ctx context.Context, s *Stage) map[string]Scan {
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
	used := map[string]Scan{}
	for _, key := range w.order {
		u := w.uses[key]
		in := env[strings.ToLower(u.name)]
		var sc Scan
		w.read(&sc, in.Table.Columns, u)
		used[in.View] = Scan{FieldIDs: append([]int{}, sc.FieldIDs...), Filtered: sc.Filtered, Filters: append([]int{}, sc.Filters...)}
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
func prefetchOf(used map[string]Scan, view string, files []File) *viewPrefetch {
	sc, ok := used[view]
	if !ok {
		return nil
	}
	p := &viewPrefetch{Sizes: make([]int64, len(files)), Used: append([]int{}, sc.FieldIDs...), Filtered: sc.Filtered,
		Filters: append([]int{}, sc.Filters...)}
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
		ws := e.targets()
		n := len(ws)
		if want := int((in.Table.Bytes + bytesPerWorker - 1) / bytesPerWorker); want < n && !e.force {
			n = max(want, min(2, n))
		}
		n = max(1, min(n, len(split)))
		shares, owners = divide(assign(split, ws[:n]), 1<<e.smaller)
		tasks = len(shares)
	}
	var groups [][]int
	// Tables read by bucket: each task reads the files of some buckets, and
	// the partitions of the exchanges written to those buckets.
	var buckets [][]int
	var bucketFiles map[string][][]File
	if n := s.bucketCount(); n > 0 {
		var err error
		if buckets, bucketFiles, err = e.bucketGroups(s, n); err != nil {
			return err
		}
		tasks = len(buckets)
		if s.partitioned() {
			groups = buckets
		}
	} else if s.partitioned() {
		groups = e.groups(s)
		tasks = len(groups)
	}
	e.co.log.Debug("running a stage", "query", e.qid, "stage", s.String(), "tasks", tasks, "partitions", e.parts)

	// What every task reads in full, and its bytes: as it is when a task is
	// made, as a worker's files may have been written again by then, by this run
	// or by another that wrote the exchange (see runProbing).
	used := e.prefetchUsed(ctx, s)
	var wholeMu sync.Mutex
	var whole map[string]viewDef
	var wholeBytes int64
	wholeAt := int64(-1)
	wholeViews := func() (map[string]viewDef, int64) {
		wholeMu.Lock()
		defer wholeMu.Unlock()
		at := int64(0)
		for _, in := range s.Inputs {
			if in.Exchange != nil && !in.Partitioned {
				x := e.exchanges[in.Exchange.ID]
				x.mu.Lock()
				at += x.again
				x.mu.Unlock()
			}
		}
		if at == wholeAt {
			return whole, wholeBytes
		}
		wholeAt = at
		whole, wholeBytes = map[string]viewDef{}, 0
		for _, in := range s.Inputs {
			switch {
			case in.Table != nil && in.Bucketed:
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
				x.mu.Lock()
				def := viewDef{Name: in.View, Columns: x.columns, ByName: true, Rows: x.rows, Exact: true}
				for _, fs := range x.files {
					def.Files = append(def.Files, fs...)
				}
				wholeBytes += x.bytes
				x.mu.Unlock()
				whole[in.View] = def
			}
		}
		return whole, wholeBytes
	}

	// A stage that writes the statement's rows sends none.
	writes := e.target != nil && s.writeSQL != ""
	var res *jobResult
	if s.Out == nil && !writes {
		res = &jobResult{files: make([]string, tasks), workers: map[string]bool{}}
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	// A stage that needs only so many rows, any of them, stops its other
	// tasks once those that ended sent that many (see Stage.enough).
	tctx, stop := context.WithCancel(gctx)
	defer stop()
	var sent atomic.Int64
	var stopped atomic.Bool
	// units are what task i reads that it may be divided by, into smaller tasks
	// (see piece): its buckets, its partitions, or its files of the split table.
	units := func(i int) int {
		switch {
		case buckets != nil:
			return len(buckets[i])
		case groups != nil:
			return len(groups[i])
		case shares != nil:
			return len(shares[i])
		}
		return 0
	}
	// request makes piece p of the stage, of what its inputs hold now, for the
	// round-th time.
	request := func(p piece, round int) (*taskRequest, int64) {
		i := p.i
		whole, wholeBytes := wholeViews()
		task := &taskRequest{Query: e.qid, Task: s.ID*tasksPerStage + i, Version: e.co.opts.Version, Settings: e.settings, Erased: e.erased}
		if e.slots > 1 {
			task.Slots = e.slots
		}
		// bytes are what it reads of a table's share, reads all it reads.
		var bytes int64
		reads := wholeBytes
		for _, in := range s.Inputs {
			switch {
			case in.Table != nil && in.Split:
				share := shares[i]
				if buckets == nil && groups == nil {
					share = pieceOf(share, p)
				}
				for _, f := range share {
					bytes += f.Size
				}
				task.Views = append(task.Views, viewDef{Name: in.View, Columns: in.Table.Columns, Files: paths(share), Rows: rowCount(share),
					Deletes: deletesOf(share), Prefetch: prefetchOf(used, in.View, share)})
			case in.Table != nil && in.Bucketed:
				var share []File
				for _, b := range pieceOf(buckets[i], p) {
					share = append(share, bucketFiles[in.View][b]...)
				}
				for _, f := range share {
					bytes += f.Size
				}
				task.Views = append(task.Views, viewDef{Name: in.View, Columns: in.Table.Columns, Files: paths(share), Rows: rowCount(share),
					Deletes: deletesOf(share), Prefetch: prefetchOf(used, in.View, share)})
			case in.Exchange != nil && in.Partitioned:
				x := e.exchanges[in.Exchange.ID]
				x.mu.Lock()
				def := viewDef{Name: in.View, Columns: x.columns, ByName: true, Exact: true}
				// Bucketed, its groups are its buckets, and a piece takes the same
				// of both.
				for _, part := range pieceOf(groups[i], p) {
					def.Files = append(def.Files, x.files[part]...)
					def.Rows += x.partRows[part]
					reads += x.partBytes[part]
				}
				x.mu.Unlock()
				task.Views = append(task.Views, def)
			default:
				task.Views = append(task.Views, whole[in.View])
			}
		}
		task.round = round
		idx := strconv.Itoa(p.index())
		if writes {
			task.SQL = writeSQL(s, e.target)
			task.Write = writeRequestFor(e.target, e.target.Dir+"/"+e.qid+"/s"+strconv.Itoa(s.ID)+"-"+idx)
			// A task out of memory may have been sorting them.
			task.Write.AsTheyCome = task.Write.AsTheyCome || e.smaller > 0
		} else if s.Out == nil {
			task.SQL, task.Columns = s.SQL, s.Columns
		} else {
			task.SQL = exchangeSQL(s, e.parts)
			if s.rowSQL != "" {
				byRow := *s
				byRow.SQL = s.rowSQL
				task.RowSQL, task.RowsFirst = exchangeSQL(&byRow, e.parts), s.rows.first
			}
			task.Shuffle = e.rootOf(s.ID, i) + "/s" + strconv.Itoa(s.ID) + "/" + idx
			task.Partitions = e.parts
			if e.local {
				// What it writes is taken to be at most what it reads.
				task.Local = e.qid + "/s" + strconv.Itoa(s.ID) + "/" + idx
				task.Expect = reads + bytes
			}
		}
		task.Variables = variablesOf(e.variables, task.SQL)
		if e.local {
			for _, w := range e.workers() {
				task.Peers = append(task.Peers, w.addr)
			}
		}
		return task, bytes
	}
	// outOf is where piece p's rows go, for a stage whose rows go to the coordinator.
	outOf := func(p piece) string {
		if s.Out != nil || writes {
			return ""
		}
		return filepath.Join(e.dir, fmt.Sprintf("%d-%d.parquet", s.ID, p.index()))
	}
	var sp *spool
	if s.Out == nil && !writes {
		sp = e.sp
	}
	// run runs piece p, first on worker first; a whole task whose worker is lost
	// while it runs it runs as smaller tasks on the others instead.
	var run func(p piece, first, round int) ([]ranPiece, error)
	run = func(p piece, first, round int) ([]ranPiece, error) {
		task, bytes := request(p, round)
		task.splittable = !p.split() && units(p.i) > 1
		w, head, err := e.dispatchPlaced(tctx, task, first, outOf(p), sp)
		// A worker lost with files the task reads: once what they held is
		// written again, the task runs on what is there now (see repair).
		rounds := round
		for err != nil && rounds < round+repairRounds && e.repair(tctx, err) {
			rounds++
			task, bytes = request(p, rounds)
			task.splittable = !p.split() && units(p.i) > 1
			w, head, err = e.dispatchPlaced(tctx, task, first, outOf(p), sp)
		}
		var lost *taskLostError
		if errors.As(err, &lost) && tctx.Err() == nil {
			k := min(units(p.i), e.usable())
			if k < 2 {
				// Not worth dividing after all: as one task, on the others.
				task.splittable = false
				w, head, err = e.dispatchPlaced(tctx, task, first+1, outOf(p), sp)
			} else {
				e.co.log.Warn("a worker was lost while it ran a task; running the task as smaller ones on the others", "query", e.qid,
					"stage", s.ID, "task", p.i, "worker", lost.worker, "tasks", k)
				pieces := make([][]ranPiece, k)
				pg, _ := errgroup.WithContext(tctx)
				for j := range k {
					pg.Go(func() error {
						var err error
						pieces[j], err = run(piece{i: p.i, j: j, k: k}, first+1+j, rounds+1)
						return err
					})
				}
				if err := pg.Wait(); err != nil {
					return nil, err
				}
				return slices.Concat(pieces...), nil
			}
		}
		if err != nil {
			return nil, err
		}
		return []ranPiece{{p: p, first: first, rounds: rounds, task: task, head: head, w: w, bytes: bytes, out: outOf(p)}}, nil
	}
	for i := range tasks {
		g.Go(func() error {
			first := i
			if owners != nil {
				first = owners[i]
			}
			ran, err := run(wholeTask(i), first, 0)
			if err != nil && stopped.Load() && gctx.Err() == nil {
				// Not needed: the tasks that ended sent enough rows.
				return nil
			}
			if err != nil {
				if OutOfMemory(err) && groups != nil && len(groups[i]) == 1 {
					return &onePartition{err: err, rows: e.exchangeRows(s)}
				}
				return err
			}
			for _, r := range ran {
				e.co.opts.Metrics.scan(r.bytes)
				e.mu.Lock()
				e.used[r.w.addr] = true
				if e.usedBy == nil {
					e.usedBy = map[int]map[string]bool{}
				}
				if e.usedBy[s.ID] == nil {
					e.usedBy[s.ID] = map[string]bool{}
				}
				e.usedBy[s.ID][r.w.addr] = true
				e.mu.Unlock()
				switch {
				case s.Out != nil:
					if err := e.receivedRan(tctx, &ranTask{run: e, stage: s, piece: r.p, units: units(i), first: r.first, request: request,
						task: r.task, head: r.head, worker: r.w.addr, rounds: r.rounds}); err != nil {
						return err
					}
				case writes:
					if err := e.writtenBy(r.task, r.head, r.w); err != nil {
						return err
					}
				default:
					if err := func() error {
						mu.Lock()
						defer mu.Unlock()
						if r.p.split() {
							res.files = append(res.files, r.out)
						} else {
							res.files[i] = r.out
						}
						res.workers[r.w.addr] = true
						if res.columns == nil {
							res.columns = r.head.Columns
						} else if !slices.Equal(res.columns, r.head.Columns) {
							return fmt.Errorf("workers returned different columns for stage %d", s.ID)
						}
						if s.enough > 0 && sent.Add(r.head.Rows) >= s.enough && !stopped.Swap(true) {
							e.co.log.Debug("a stage's tasks sent the rows it needs; stopping the others", "query", e.qid, "stage", s.ID,
								"rows", sent.Load(), "enough", s.enough)
							stop()
						}
						return nil
					}(); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	if res != nil && stopped.Load() {
		// Only the files of the tasks that ended.
		res.files = slices.DeleteFunc(res.files, func(f string) bool { return f == "" })
	}
	if res != nil {
		e.mu.Lock()
		e.results[s.ID] = res
		e.mu.Unlock()
	}
	return nil
}

// dispatchPlaced runs task as dispatch does, on the statement's workers, once
// one has a place for it, worker first first (see places).
func (e *execution) dispatchPlaced(ctx context.Context, task *taskRequest, first int, path string, sp *spool) (*worker, *taskResult, error) {
	ws := e.targets()
	if e.places == nil || len(ws) == 0 {
		return e.co.dispatch(ctx, task, ws, first, path, sp)
	}
	at, release, err := e.places.take(ctx, ws, first%len(ws))
	if err != nil {
		return nil, nil, err
	}
	defer release()
	return e.co.dispatch(ctx, task, ws, at, path, sp)
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

// receivedLocked records the files a task wrote to x, its stage's exchange,
// whose lock the caller holds (see receivedRan).
func (e *execution) receivedLocked(x *exchangeRun, task *taskRequest, head *taskResult, w *worker) error {
	x.rows += head.Rows
	x.tasks++
	if x.columns == nil {
		x.columns = head.Columns
	} else if !slices.Equal(x.columns, head.Columns) {
		return fmt.Errorf("stages wrote different columns to the exchange of task %d", task.Task)
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
		return fmt.Errorf("a worker listed files of %d rows for the %d task %d wrote", listed, head.Rows, task.Task)
	}
	return nil
}

// shuffleRoot is a statement's directory for exchanges in object storage.
func shuffleRoot(warehouse, qid string) string {
	return warehouse + config.ShuffleDir + "/" + qid
}

// spread is how many directories a statement's exchanges are spread over
// in object storage (see spreadRoots).
const spread = 16

// spreadRoots are a statement's directories for exchanges in object
// storage: one for each hexadecimal digit, which starts the directory's
// name, before the statement's ID. S3 serves a prefix so many requests a
// second and turns the rest away (503 SlowDown) until it has split the
// prefix, which it does by the characters after it: a statement's 187,000
// files under one ID on 16 workers drew SlowDown at a hundred times the
// data. Tasks' files spread over prefixes that every statement shares,
// which S3 splits once and keeps split. Sweeps and removals take the
// directories of an ID together (see exchangeQID).
func spreadRoots(warehouse, qid string) []string {
	roots := make([]string, spread)
	for i := range roots {
		roots[i] = shuffleRoot(warehouse, strconv.FormatInt(int64(i), 16)+"."+qid)
	}
	return roots
}

// exchangeQID returns the statement ID of a directory under the exchanges'
// root: name itself, or what follows the digit of a spread directory.
func exchangeQID(name string) string {
	if len(name) > 2 && name[1] == '.' && strings.ContainsRune("0123456789abcdef", rune(name[0])) {
		return name[2:]
	}
	return name
}

// rootOf returns the directory of a task's exchange files: the one of the
// statement's spread over that a hash of the stage and the task picks.
func (e *execution) rootOf(stage, task int) string {
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%d/%d", stage, task)
	return e.roots[h.Sum32()%uint32(len(e.roots))]
}
