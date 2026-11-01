package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"strings"
	"time"
)

// In auto mode a statement runs on workers only when that is faster than
// one machine: workers that start a statement only to finish it later
// than the coordinator would have are a loss whatever they cost. Igloo
// estimates both times from what the statement's parts read, and corrects
// the estimates with what the same statement took before (see history).
//
// The rates are rough, from the scale runs, and per core, since DuckDB
// runs several threads a core on object storage: a machine reads object
// storage cold at about 16 MB/s a core at four threads a core, and less at
// fewer (coldRateAt), as DuckDB's threads are capped at 2 GiB of its
// memory each (a worker of 8 cores and 24.6 GiB runs 12); tables a stage
// filters selectively count a hundredth of their bytes and rows; and a
// statement on workers pays for its start and for each level of its stages
// (startCost, levelCost in fleet.go). Against the scale environment's reports at 1x, 10x and
// 100x on a coordinator of 16 cores and 16 workers of 8, the estimates pick
// the side that was faster by half again, or either where the two were
// closer (see TestScaleReports), but for statements whose rows cost far
// more than their count says: unnesting FLATTEN's arrays (a report that
// took 251 s on one machine at 10x, estimated at 7 s), and joins through a
// view's computed keys counted distinct (691 s on one machine and 170 s on
// workers, estimated at 160 s and 153 s). Those run on one machine first,
// and their history sends them to workers after (see choose). How many
// workers, or the large worker, is weighed in fleet.go.
const (
	coldRate = 16e6
	// startCost is what a statement on workers takes beside its stages: its
	// own start, planning its tasks and reading their results.
	startCost = 8.0
	// DefaultThreshold is how long a statement must take on one machine,
	// by its estimate or its last run there, before auto mode runs it on
	// workers, unless it would run short of memory: below it, the
	// estimates are too rough to bet on.
	DefaultThreshold = time.Minute
	// A statement runs short of memory on the coordinator when its working
	// set, workingSetFactor times what its stages that write exchanges
	// read, is over memoryShare of the coordinator's memory limit: the
	// joins and distinct counts over people that such stages feed hold
	// several times their tables' compressed bytes.
	workingSetFactor = 3
	memoryShare      = 0.7
	// heldFactor is how many times the compressed bytes its exchanges'
	// stages read, as selective filters leave them, auto mode takes a
	// statement's working set on one machine to be. At ten times the data,
	// reports whose stages read 12 GB so took 621 to 691 s on one machine
	// of 49 GiB, counting people distinct, and 172 s for one of 14 GB; their
	// workers took 84 to 188 s.
	heldFactor = 6
	// Workers must be estimated this much faster than one machine.
	speedup = 1.5
	// shareCap is the most other statements on the replica auto mode counts
	// as sharing its cores with a statement (see choose): beside three or
	// more, a statement takes about four times as long there, and slows
	// each of them.
	shareCap = 3
	// farBeyondRatio is how many times its room a working set must be for
	// the coordinator not to run a statement whose workers failed (see
	// Plan.FitsHere): 4 times, by the spill curve twice as long, where a
	// report it ran at 6.8 times by its estimate was killed after 9 minutes
	// and 167 GB of spill.
	farBeyondRatio = 4
	// footprintTrust is how many times what a run of a statement's shape
	// allocated here its plan's working set may be and still yield to it.
	footprintTrust = 3
	// rowRate is how many rows a core goes through a second for each pass
	// over them (see rowWork). It is the fastest of the scale environment's
	// reports on one machine of 16 cores: two that went through 1.1 and
	// 1.35e10 such rows at 1x took 3 to 6 s, as filters the estimate does
	// not see cut their rows; one that joins a 400-million-row table to
	// itself 16 times went through 6.8e10 at 10x in 1,073 s. So the rows
	// set a floor under one machine's time, which only plans with far more
	// rows than bytes rise above.
	rowRate = 1.5e8
	// filterSelectivity is the share of a table's rows a filter on one of
	// its columns by a constant is taken to keep (see rowWork).
	filterSelectivity = 0.01
	// workerThreadsPerCore are the threads a core coldRate is measured at
	// (see engine's thread defaults).
	workerThreadsPerCore = 4
	// exploreEvery is how seldom auto mode runs a statement where it has
	// not run, against the estimates, to learn how long it takes there
	// (see choose); exploreUnder is how short its runs where it has must
	// be for that to be worth a try.
	exploreEvery = 24 * time.Hour
	exploreUnder = 10 * time.Minute
	// exploreWithin is how many times what a statement took on one side
	// the other side's estimate may be for that side to be worth a try:
	// at ten times the data, a report that took 57 s on workers was tried
	// on one machine, estimated at 176 s and short of memory, and took 251
	// s and spilled 8.4 GB, where the runs exploring fixed had been
	// estimated close (160 s against 153), or had taken several times
	// their own estimate (a one machine's 7 s that took longer than the
	// workers' 51).
	exploreWithin = 2
	// spillWithin is how many times one machine's estimate workers may be
	// estimated at and still run a statement that one machine should spill:
	// its estimate is then a floor, which ran four to thirty-five times
	// short at ten times the data (a report estimated at 56 s took 220 s
	// on one machine and 66 s on workers). Two estimated to spill that ran
	// five times as fast on one machine had workers estimated at 1.7 and 2
	// times its estimate.
	spillWithin = 1.5
	// slowOnWorkers is how many times one machine's estimate a statement
	// that ran only on workers must have taken there for auto mode to try
	// one machine once, though one machine should run short of memory: an
	// estimate short of memory ran four to five times short where the
	// statement spilled (two reports estimated at 157 s took 809 s and 610
	// s on one machine), so ten times it is past what spilling explains.
	// A dashboard's statements at ten times the data, estimated at 14 s on
	// one machine and short of memory, took 1,312 s split over two workers
	// that shared the replica's machine, and stayed on workers after it
	// from the query history, through a burst on workers of their own;
	// the replica alone ran them in 25 s at the median, 96 at once.
	slowOnWorkers = 10.0
)

// estimate is how long a planned statement would take on one machine and
// on workers.
type estimate struct {
	// bytes are what its stages read of tables, as selective filters leave
	// them, and localBytes what one machine reads of them, by DuckDB's plan
	// where it says (see planEstimate), or bytes; exchanged what the stages
	// that write exchanges read of them, in full; levels its stages'
	// longest chain, and stages how many there are; rows the rows its
	// stages go through (see rowWork).
	bytes, localBytes, exchanged int64
	levels, stages               int
	rows                         float64
	// oneMachine is the coordinator's time, onWorkers that of workers,
	// as many as workers says, or of the large worker if large, in seconds,
	// by the estimates (see pick for the choice, which what the statement
	// took before corrects).
	oneMachine, onWorkers float64
	workers               int
	large                 bool
	// memory says the working set is over memoryShare of the coordinator's
	// memory, by workingSetFactor, and spills that it is by heldFactor, as
	// local says, whose oom says it should run out of memory there;
	// farBeyond that the working set by DuckDB's plan, or else by the
	// stages, is past farBeyondRatio times the room (see Plan.FitsHere).
	memory, spills, farBeyond bool
	local                     spill
	// others are the statements running on the replica beside it, which
	// share its cores (see choose).
	others int
	// stream is how long the client takes its rows, either way (see
	// clientFieldRate); sorted is what its largest sort or window holds,
	// and risky says that is near the replica's room (see riskyShare).
	stream, sorted float64
	risky          bool
	// options are the ways on workers weighed, dollars says their costs are
	// in dollars, and fleet why the one in workers and large, picked, was
	// chosen.
	options []option
	dollars bool
	fleet   string
	picked  option
	// planned says the working set is DuckDB's plan's, and parts what its
	// operators hold.
	planned bool
	parts   []heldPart
}

// coldRateAt is how fast a core reads object storage cold at
// threadsPerCore threads a core. A machine of 8 cores read through its read
// cache at 56 MB/s on 8 threads, 91 on 16 and 131 on 32: 7, 11.4 and
// 16.4 MB/s a core, about as the 0.6th power of the threads.
func coldRateAt(threadsPerCore float64) float64 {
	return coldRate * math.Min(1, math.Pow(math.Max(threadsPerCore, 0.25)/workerThreadsPerCore, 0.6))
}

// machine is what the coordinator knows of itself for an estimate: its
// cores and DuckDB threads and its memory limit, zero for unknown.
type machine struct {
	cores, threads int
	memory         int64
	// ranHere says a statement of the same shape as the one estimated ran
	// to its end on this machine, allocating allocated bytes over its run
	// (see Footprints).
	ranHere   bool
	allocated int64
}

// atLimit is the share of a machine's memory limit past which what a
// statement of a shape held over a run it completed there (see
// Footprints) says only that it held its limit: it may have spilled any
// amount. At ten times the data, from a cleared plan history, a report
// whose plan held about 160 GB, three times the replica's 53 GB, was run
// there on what a run of its shape had held, at the limit, estimated at
// 47 s and fitting; it held 51 GB, spilled 35 GB, and the guard moved it
// after 88 s. A report that held 38.5 GB of the 53, and did not spill,
// still fits so.
const atLimit = 0.9

// fitsHere says a statement of the shape of the one estimated ran to its
// end on m holding less than atLimit of m's memory limit: proof that it
// fits there.
func (m machine) fitsHere() bool {
	return m.ranHere && (m.memory <= 0 || float64(m.allocated) < atLimit*float64(m.memory))
}

// Loads is an Env whose session knows how many other statements run on the
// replica now, which share its cores with the statement being planned
// (see choose).
type Loads interface {
	Others() int
}

// Footprints is an Env whose session knows what statements of the shape
// of the one being planned did on this replica: Footprint returns what the
// latest such statement allocated over its run, which it completed here,
// and ok is false when none has. One machine is then not short of memory
// for the statement, whatever its estimate says, and spills only if what
// it allocated, all of its run's allocations, is over its share: a
// dashboard's statements at ten times the data, estimated short of memory,
// went to workers and took 250 s there, where the replica alone ran them
// in 20 s at the median, 96 at once.
type Footprints interface {
	Footprint() (allocated int64, ok bool)
}

// fleet is what the coordinator knows of its workers for an estimate.
type fleet struct {
	// cores are a worker's cores, and threads its DuckDB threads, zero for
	// unknown; running how many workers run; most how many may run; start
	// how long starting more takes, and tail how long workers started stay
	// up idle after. alone says idle workers stop alone while others work
	// (see pool.shrink), so a statement on fewer of the running workers
	// bills fewer.
	cores, threads, running, most int
	start, tail                   time.Duration
	alone                         bool
	// memory is the smallest worker's memory limit, and price what a
	// worker costs an hour, zero for unknown; replicaPrice is what the
	// replica costs an hour, zero for unknown, and replicaCores its cores.
	memory       int64
	price        float64
	replicaPrice float64
	replicaCores int
	// large is the compute pool's large worker, if it has one the
	// statement may run on.
	large *largeWorker
}

// estimateTimes estimates a plan's times on the coordinator, m, and on
// workers, f, from what its stages read and the rows they go through.
func estimateTimes(stages []*Stage, m machine, f fleet) estimate {
	return planFeatures(stages).estimate(m, f)
}

// features are what a plan's estimates are made of.
type features struct {
	// bytes are what its stages read of tables, as selective filters leave
	// them (see filtersSelectively); exchanged what the stages that write
	// exchanges read of tables in full, and held as selective filters leave
	// it.
	bytes, exchanged int64
	held             float64
	// rows are the rows its stages go through (see rowWork); levels its
	// stages' longest chain, and stages how many there are.
	rows           float64
	levels, stages int
	// planSet is the working set DuckDB's plan of the statement holds,
	// where planned says the session could plan it, and planParts what each
	// of its operators holds (see workingSetOf); planBytes is what one
	// machine reads of tables by the plan, zero where it does not say, and
	// planWork the core-seconds its hash tables take (see planEstimate).
	planSet   float64
	planned   bool
	planParts []heldPart
	planBytes int64
	planWork  float64
	// fill says the statement fills a table from the query's rows, which
	// this replica writes wherever the query runs, and planKeeps the share
	// of the rows its scans read that the query's result has, by the plan
	// (see fillCost).
	fill      bool
	planKeeps float64
	// workersWrite says tasks write the rows, on as many cores as they run
	// on, and none cross to this replica (see writes.go).
	workersWrite bool
	// planFields are the fields of its result's rows, which a read sends
	// the client (see clientFieldRate), and planSorted what its largest
	// sort or window holds, by the plan (see riskyShare); planJoins the
	// share of its working set joins hold (see lenience).
	planFields, planSorted, planJoins float64
}

// lenience is how many times one machine's room a working set by the plan
// x may be estimated at and still count as fitting: slightSpill for what
// joins hold, whose estimates ran high (a report's joins, estimated at 46
// GB, 1.24 times the room, held 33 GB), and none for the rest. A report
// whose aggregates and windows, counting people distinct over the values
// of a FLATTEN, were estimated at 41 GB, 1.1 times the room, so fitting,
// held 51 GB and spilled 35 GB, as its first run on one machine, from a
// cleared plan history, did before the guard moved it. A working set not
// by the plan keeps slightSpill.
func (x features) lenience() float64 {
	if !x.planned {
		return slightSpill
	}
	return 1 + (slightSpill-1)*min(max(x.planJoins, 0), 1)
}

const (
	// clientFieldRate is the fields a second a client takes of a result:
	// 361 million rows of four columns, ordered, took 516 s to reach it,
	// and 36 million 49 s, about 2.8 million fields a second either way.
	// One machine's estimate counted none of it, 30 s for the 516.
	clientFieldRate = 2.8e6
	// riskyShare is the share of this replica's room for a statement past
	// which a sort or window, holding that much, is risky there: DuckDB's
	// sort takes about 40% more memory than its limit, and a sort of 361
	// million rows of four columns, held at about 20 GB in a room of 37,
	// finished within 2 GB of the memory watch's cancel. riskyWithin is how
	// many times one machine's time workers may take and still run such a
	// statement, where workers are configured.
	riskyShare  = 0.5
	riskyWithin = 1.25
)

// Fills is an Env whose statement fills a table from the query planned,
// as CREATE TABLE ... AS does: this replica writes the table from the
// query's rows wherever the query runs, from the workers' results where it
// runs on workers (see fillCost). Its runs are kept apart from reads of the
// same query.
type Fills interface {
	Fill() bool
	// FillWrites says workers could write the table's files, if they may
	// and its tasks hold its rows (see writes.go).
	FillWrites() bool
}

const (
	// fillWriteRate is the bytes of table files a second a core of this
	// replica writes, compressed as it reads them: one of 16 cores wrote a
	// billion rows of four columns, about 10 GB of files, in 163 s, about
	// 60 MB/s in all.
	fillWriteRate = 3.75e6
	// fillFetchRate is the bytes of files a second this replica fetches of
	// the workers' results, to write the table from them.
	fillFetchRate = 400e6
)

// fillCost is what writing a fill's rows adds to its time, on a replica of
// cores: it writes them wherever the query runs (here), and fetches them
// from the workers first where it runs there (onWorkers), files about the
// share of the bytes it reads that its rows are of the rows it reads (see
// planEstimate.keeps). A copy, whose rows are as many as it reads, takes
// about as long either way, writing them, and runs on workers only where
// they are clearly faster still; an aggregate's few rows add nothing.
func (x features) fillCost(cores int) (here, onWorkers float64) {
	if !x.fill || !x.planned || x.planKeeps <= 0 {
		return 0, 0
	}
	out := x.fillOut()
	write := out / (fillWriteRate * float64(max(cores, 1)))
	if x.workersWrite {
		// Each way on workers counts its own (see onFleet, onLarge).
		return write, 0
	}
	return write, write + out/fillFetchRate
}

// fillOut is the bytes of files a fill's rows take, compressed as what it
// reads is: where DuckDB's plan does not say what share of the rows it
// reads they are, all of them.
func (x features) fillOut() float64 {
	read := float64(x.bytes)
	if x.planBytes > 0 {
		read = float64(x.planBytes)
	}
	if !x.planned {
		return read
	}
	return x.planKeeps * read
}

// onWorkersWrite is what tasks on cores take to write a fill's rows.
func (x features) onWorkersWrite(cores int) float64 {
	if !x.workersWrite {
		return 0
	}
	return x.fillOut() / (fillWriteRate * float64(max(cores, 1)))
}

const (
	// writeVectorRows are the rows DuckDB holds together, whatever else
	// bounds them, and writeVectorHeld how many vectors a thread of a task
	// that writes holds: rows of 10.8 KB held 110 MiB, of 102 KB 837 MiB and of
	// 1 MiB over 3.8 GiB, on two threads.
	writeVectorRows, writeVectorHeld = 2048, 2
	// rowFileShare is what a row takes in a table's files, of what it takes in
	// memory, as DuckDB's compression of text leaves it.
	rowFileShare = 0.25
)

// rowBytes is the bytes a row a fill writes takes in memory, by what its scans
// read a row; 0 where that is not known. What an expression makes of a row,
// such as repeat(), no estimate says: a task bounds what it holds, and a fill
// whose workers run out of memory runs elsewhere (see Plan.FitsHere).
func (x features) rowBytes() float64 {
	if x.rows <= 0 {
		return 0
	}
	read := float64(x.bytes)
	if x.planBytes > 0 {
		read = float64(x.planBytes)
	}
	return read / x.rows / rowFileShare
}

// writeFits reports whether a worker's task of f holds a fill's rows as it
// writes them: at least what its threads' vectors take, within half of the
// task's share of the worker's memory.
func (x features) writeFits(f fleet) bool {
	width := x.rowBytes()
	if width <= 0 || f.memory <= 0 {
		return true
	}
	threads := float64(max(f.threads, f.cores, 1))
	share := float64(f.memory) / float64(taskSlots(f.cores, f.memory))
	return writeVectorRows*writeVectorHeld*threads*width <= share/2
}

// PartitionedFills is an Env whose statement fills a partitioned table: the
// files workers would write of it are as many as its tasks and partitions.
type PartitionedFills interface {
	// FillPartitions counts the table's partitions among the first rows of query,
	// the fill's source: in the first half of them, and in all of them, of sampled
	// rows. all is zero for a table with no partitions.
	FillPartitions(ctx context.Context, query string, rows int64) (half, all, sampled int64, err error)
}

const (
	// leastPartitionFile is the smallest mean file of a partitioned table workers
	// write in auto mode. On S3, a file took a thread of a worker about 120 ms
	// beside its bytes, what about 4 MiB of them take, and more of the coordinator's
	// time and of every later read: below this, a file costs more than its bytes.
	// Options.MinWorkerWrite, set below it, lowers it too.
	leastPartitionFile = 8 << 20
	// partitionSample and partitionSampleTime bound the look at a fill's first rows:
	// nothing that long-running fills would notice, and most reads stop early.
	partitionSample     = 1 << 20
	partitionSampleTime = 3 * time.Second
)

// partitionsOf estimates the partitions of a fill of rows rows from a sample of
// them: what its second half added goes on at that rate, for rows in the order of
// a partition's column, and ends where it added none.
func partitionsOf(half, all, sampled int64, rows float64) float64 {
	rest := rows - float64(sampled)
	if sampled <= 0 || rest <= 0 {
		return float64(all)
	}
	return float64(all) + float64(all-half)/(float64(sampled)/2)*rest
}

// writeTasks is about how many tasks read a fill's bytes, as a stage divides a
// table among workers of f (see execution.stage).
func writeTasks(bytes int64, f fleet) float64 {
	most := max(f.most, f.running, 1)
	want := int((bytes + bytesPerWorker - 1) / bytesPerWorker)
	return float64(min(max(want, min(2, most)), most))
}

// fillPartitions estimates how many partitions a fill's rows have, by a look at them:
// zero for a table with none, or if it cannot tell.
func (co *Coordinator) fillPartitions(ctx context.Context, env Env, query string, x features) float64 {
	pf, ok := env.(PartitionedFills)
	if !ok {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, partitionSampleTime)
	defer cancel()
	half, all, sampled, err := pf.FillPartitions(ctx, query, partitionSample)
	if err != nil {
		co.log.Debug("could not count a fill's partitions", "err", err)
		return 0
	}
	return partitionsOf(half, all, sampled, x.rows)
}

// partitionFilesFit reports whether the files workers would write of a fill of parts
// partitions are worth writing there: its tasks each write a file of each partition,
// so that a table of many gets many small files, and of more than a worker's task
// can keep open they first sort the rows, which is more than a read of them takes.
func (co *Coordinator) partitionFilesFit(parts float64, x features, f fleet) bool {
	if parts == 0 {
		return true
	}
	tasks := writeTasks(x.bytes, f)
	if x.fillOut()/(tasks*parts) >= min(leastPartitionFile, co.minWorkerWrite()) &&
		(f.memory <= 0 || parts <= float64(writeFilesFit(f.memory, true))) {
		return true
	}
	co.log.Info("this replica writes a partitioned table, whose files would be small on workers, or many for them", "partitions", int64(parts),
		"tasks", int64(tasks), "bytes", int64(x.fillOut()), "worker_memory", f.memory)
	return false
}

// defaultMinWorkerWrite is the fewest bytes of files workers write rather
// than the coordinator: fewer take the coordinator a few seconds, and each
// task's file would be a small one.
const defaultMinWorkerWrite = 256 << 20

// minWorkerWrite is Options.MinWorkerWrite, as it says.
func (co *Coordinator) minWorkerWrite() float64 {
	switch {
	case co.opts.MinWorkerWrite < 0:
		return 0
	case co.opts.MinWorkerWrite == 0:
		return defaultMinWorkerWrite
	}
	return float64(co.opts.MinWorkerWrite)
}

// set is the working set of a plan of features x on one machine: what
// DuckDB's plan of the statement holds, where the session could plan it
// (see workingSetOf), or else heldFactor times what its exchanges' stages
// read, as selective filters leave it.
func (x features) set() float64 {
	if x.planned {
		return x.planSet
	}
	return heldFactor * x.held
}

// planFeatures measures a plan for its estimates.
func planFeatures(stages []*Stage) features {
	var x features
	// What the stages read of each table, and what those that write
	// exchanges read: the columns their fragments refer to. A table every
	// stage that reads it filters selectively counts filterSelectivity of
	// those bytes.
	tables := map[string]*Table{}
	read, exchanged := map[string]columnSet{}, map[string]columnSet{}
	selective := map[string]bool{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table == nil {
				continue
			}
			k := in.Table.key()
			if _, seen := tables[k]; !seen {
				selective[k] = true
			}
			tables[k] = in.Table
			selective[k] = selective[k] && filtersSelectively(s.fragment, in.Table)
			cols := columnsRead(s.fragment, in.Table)
			read[k] = union(read, k, cols)
			if s.Out != nil {
				exchanged[k] = union(exchanged, k, cols)
			}
		}
	}
	for k, cols := range read {
		b := cols.bytes(tables[k])
		if selective[k] {
			b = int64(float64(b) * filterSelectivity)
		}
		x.bytes += b
	}
	for k, cols := range exchanged {
		b := cols.bytes(tables[k])
		x.exchanged += b
		if selective[k] {
			x.held += float64(b) * filterSelectivity
		} else {
			x.held += float64(b)
		}
	}
	x.levels, x.stages = levels(stages), len(stages)
	x.rows = rowWork(stages, filtersSelectively)
	return x
}

// estimate estimates the times of a plan of features x on the coordinator,
// m, and on workers, f.
func (x features) estimate(m machine, f fleet) estimate {
	e := estimate{bytes: x.bytes, exchanged: x.exchanged, levels: x.levels, stages: x.stages, rows: x.rows, planned: x.planned, parts: x.planParts}
	// Short of memory on the coordinator, which a statement whose workers
	// fail then does not fall back to, by the bytes its exchanges' stages
	// read.
	share := memoryShare * float64(m.memory)
	fits := m.fitsHere()
	e.memory = m.memory > 0 && float64(workingSetFactor*x.exchanged) > share && !m.ranHere
	// The ways on workers, and the one auto mode would take without what
	// the statement took before (see plan, which corrects them).
	e.options = x.options(f)
	_, _, _, e.dollars = f.prices()
	e.pick(runs{}, f)

	// One machine reads cold from object storage at its threads a core: its
	// read cache holds what it read lately, which a statement's first run
	// had seldom read. At ten times the data, reports' first runs took 24
	// to 259 s and their second 3 to 51 s, and an estimate that took the
	// cache for warm put a report of 258 s at 15 s.
	cores := float64(max(m.cores, 1))
	threadsPerCore := float64(max(m.threads, m.cores)) / cores
	// What it reads by DuckDB's plan, the columns its scans project, where
	// the stages' fragments may read a view's every column: a report whose
	// stages read 77 GB of a table of 470 columns read 9.6 GB there.
	e.localBytes = e.bytes
	if x.planned && x.planBytes > 0 {
		e.localBytes = x.planBytes
	}
	e.oneMachine = float64(e.localBytes) / (cores * coldRateAt(threadsPerCore))
	// What it reads, or what it computes over the rows, whichever takes
	// longer: a report that reads 2 GB of a table but joins its 400 million
	// rows to themselves 16 times is no 1-second statement. By the plan,
	// its hash tables' rows, at hashMissRate where they outgrow the caches.
	e.oneMachine = max(e.oneMachine, e.rows/(cores*rowRate), x.planWork/cores)
	// Short of memory, it takes longer, spilling, as much as its working
	// set is over its share of memory (see spillOn): heldFactor times what
	// its exchanges' stages read as the selective filters leave it, or what
	// a statement of its shape allocated over its run here, which fits if
	// that is within its share.
	set := x.set()
	e.farBeyond = m.memory > 0 && set > farBeyondRatio*memoryShare*float64(m.memory)
	within := x.lenience()
	switch {
	case fits && !(x.planned && set > footprintTrust*float64(m.allocated) && set > slightSpill*memoryShare*float64(m.memory)):
		// What a run of its shape allocated here, unless DuckDB's plan holds
		// far more, as when its tables have grown since: a report's shape
		// that ran here at a tenth of the data allocated 53 GB, and its plan
		// at a hundred times held 250 GB. It held that and completed: it
		// fits.
		set, within = float64(m.allocated), math.Inf(1)
	case m.ranHere:
		// It ran here at its memory limit, holding at least that.
		set = max(set, float64(m.allocated))
	}
	e.local = spillLenient(set, m.memory, within)
	e.spills = e.local.factor > 1
	e.oneMachine *= e.local.factor
	// A fill's rows, written here wherever the query runs; a read's, sent
	// to the client, from here or from the workers' merge alike.
	here, there := x.fillCost(m.cores)
	if !x.fill && x.planned {
		e.stream = x.planFields / clientFieldRate
		here, there = e.stream, e.stream
	}
	e.oneMachine += here
	// A sort or window that holds near the replica's room is risky there
	// (see riskyShare).
	e.sorted = x.planSorted
	e.risky = x.planned && m.memory > 0 && x.planSorted >= riskyShare*memoryShare*float64(m.memory)
	if there > 0 {
		for i := range e.options {
			e.options[i].estimate += there
			e.options[i].seconds = e.options[i].estimate
			e.options[i].cost = e.options[i].costOn(f)
		}
		e.pick(runs{}, f)
	}
	return e
}

// pick chooses the way on workers e's options say, corrected by what the
// statement took before, r (see correct and pick): workers is how many, or
// 1 and large for the large worker, and onWorkers its time by the
// estimates; with no way on workers, workers is zero and onWorkers
// infinite.
func (e *estimate) pick(r runs, f fleet) {
	e.workers, e.large, e.onWorkers, e.fleet = 0, false, math.Inf(1), ""
	if len(e.options) == 0 {
		return
	}
	correct(e.options, r, f)
	best, why, ok := pick(e.options, e.dollars)
	e.fleet = why
	if ok {
		e.workers, e.large, e.onWorkers, e.picked = best.workers, best.large, best.estimate, best
	}
}

// rowWork estimates the rows a plan's stages go through: each stage's
// tables' rows and the rows of the exchanges it reads, taken as the rows
// that fed them, once for the scan and once more for each join and for
// grouping in its fragment. A table its fragment filters, as a report
// picks one audience out of every audience's members, counts
// filterSelectivity of its rows.
func rowWork(stages []*Stage, filters func(node, *Table) bool) float64 {
	fed := map[*Exchange]float64{}
	var work float64
	for _, s := range stages {
		var in float64
		for _, i := range s.Inputs {
			switch {
			case i.Table != nil:
				rows := float64(max(i.Table.Rows, 0))
				if filters(s.fragment, i.Table) {
					rows *= filterSelectivity
				}
				in += rows
			case i.Exchange != nil:
				in += fed[i.Exchange]
			}
		}
		work += in * float64(1+operators(s.fragment))
		if s.Out != nil {
			fed[s.Out] += in
		}
	}
	return work
}

// filtersByConstant reports whether fragment compares a column of t to a
// constant, with = or IN, in a WHERE clause.
func filtersByConstant(fragment node, t *Table) bool {
	cols := map[string]bool{}
	for _, c := range t.Columns {
		cols[strings.ToLower(c.Name)] = true
	}
	found := false
	walk(fragment, func(m node) bool {
		where := child(m, "where_clause")
		if found || where == nil {
			return !found
		}
		for _, c := range conjuncts(where) {
			var col, val node
			switch str(c["type"]) {
			case "COMPARE_EQUAL":
				col, val = child(c, "left"), child(c, "right")
				if str(col["class"]) != "COLUMN_REF" {
					col, val = val, col
				}
			case "COMPARE_IN":
				kids := list(c, "children")
				if len(kids) < 2 {
					continue
				}
				col, _ = kids[0].(node)
				val, _ = kids[1].(node)
			default:
				continue
			}
			if str(col["class"]) != "COLUMN_REF" || str(val["class"]) != "CONSTANT" {
				continue
			}
			names := list(col, "column_names")
			if len(names) > 0 && cols[strings.ToLower(str(names[len(names)-1]))] {
				found = true
			}
		}
		return !found
	})
	return found
}

// filtersSelectively reports whether fragment keeps few of t's rows, as
// auto mode's estimates take it: it compares a column of t to a constant
// or to a subquery's value, with = or IN (see selectiveFilter), as reports
// pick one audience, or its latest version, out of a lookup of all of
// them. DuckDB reads only the row groups whose values may match, and an
// audience's members are written together.
func filtersSelectively(fragment node, t *Table) bool {
	cols := map[string]bool{}
	for _, c := range t.Columns {
		cols[strings.ToLower(c.Name)] = true
	}
	found := false
	walk(fragment, func(m node) bool {
		if found {
			return false
		}
		for _, c := range conjuncts(child(m, "where_clause")) {
			if !selectiveFilter(c) {
				continue
			}
			var col node
			switch str(c["type"]) {
			case "COMPARE_EQUAL":
				if col = child(c, "left"); str(col["class"]) != "COLUMN_REF" {
					col = child(c, "right")
				}
			case "COMPARE_IN":
				col, _ = list(c, "children")[0].(node)
			default:
				col = child(c, "child")
			}
			names := list(col, "column_names")
			if str(col["class"]) == "COLUMN_REF" && len(names) > 0 && cols[strings.ToLower(str(names[len(names)-1]))] {
				found = true
			}
		}
		return !found
	})
	return found
}

// operators counts the joins in a fragment, and its grouping.
func operators(fragment node) int {
	n := 0
	walk(fragment, func(m node) bool {
		if str(m["type"]) == "JOIN" {
			n++
		}
		if len(list(m, "group_expressions")) > 0 {
			n++
		}
		return true
	})
	return n
}

// columnSet is the columns of a table a plan reads, by lower-case name;
// nil means all of them.
type columnSet map[string]bool

// columnsRead returns the columns of t that a stage's fragment refers to:
// every column if its select list has a star, which it writes out, and
// otherwise those it names anywhere, under any qualifier. A star in a
// subquery, as a view's s.* is, passes columns through for the names
// around it to pick. A stage without a fragment, as in tests, reads all.
func columnsRead(fragment node, t *Table) columnSet {
	if fragment == nil {
		return nil
	}
	for _, item := range list(fragment, "select_list") {
		if n, ok := item.(node); ok && str(n["class"]) == "STAR" {
			return nil
		}
	}
	named := map[string]bool{}
	walk(fragment, func(n node) bool {
		if str(n["class"]) == "COLUMN_REF" {
			for _, part := range list(n, "column_names") {
				named[strings.ToLower(str(part))] = true
			}
		}
		return true
	})
	cols := columnSet{}
	for _, c := range t.Columns {
		if name := strings.ToLower(c.Name); named[name] {
			cols[name] = true
		}
	}
	if len(cols) == 0 && len(t.Columns) > 0 {
		// count(*): DuckDB reads a column all the same.
		cols[strings.ToLower(t.Columns[0].Name)] = true
	}
	return cols
}

// union returns sets[k], if any, with cols; nil, as either, is all
// columns.
func union(sets map[string]columnSet, k string, cols columnSet) columnSet {
	prev, ok := sets[k]
	switch {
	case !ok:
		return cols
	case prev == nil || cols == nil:
		return nil
	}
	out := maps.Clone(prev)
	maps.Copy(out, cols)
	return out
}

// bytes estimates what reading the columns s of t takes: t's size times
// the columns' share of its columns, weighed by their types (typeBytes).
func (s columnSet) bytes(t *Table) int64 {
	if s == nil {
		return t.Bytes
	}
	var all, some float64
	for _, c := range t.Columns {
		w := typeBytes(c.Type)
		all += w
		if s[strings.ToLower(c.Name)] {
			some += w
		}
	}
	if all == 0 {
		return t.Bytes
	}
	return int64(float64(t.Bytes) * some / all)
}

// typeBytes guesses how many compressed bytes a value of type typ takes,
// to weigh a table's columns, whose sizes Igloo does not know: a report
// that reads three of the 420 columns of a table of 77 GB reads a few of
// its gigabytes, not all of them.
func typeBytes(typ string) float64 {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch {
	case strings.HasSuffix(t, "]"), strings.HasPrefix(t, "MAP"), strings.HasPrefix(t, "STRUCT"), strings.HasPrefix(t, "UNION"):
		return 32
	case t == "BOOLEAN", t == "TINYINT", t == "UTINYINT":
		return 1
	case t == "SMALLINT", t == "USMALLINT":
		return 2
	case t == "INTEGER", t == "UINTEGER", t == "FLOAT", t == "DATE":
		return 4
	case t == "HUGEINT", t == "UHUGEINT", t == "UUID", t == "INTERVAL", t == "BLOB":
		return 16
	case strings.HasPrefix(t, "DECIMAL"):
		if p, _, ok := strings.Cut(strings.TrimPrefix(t, "DECIMAL("), ","); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 18 {
				return 16
			}
		}
		return 8
	case strings.HasPrefix(t, "VARCHAR"):
		return 16
	}
	return 8
}

// levels is the length of the longest chain of stages each of which reads
// what the one before it wrote: the rounds of tasks a statement waits for.
func levels(stages []*Stage) int {
	depth := map[*Exchange]int{}
	most := 0
	// Stages come after those whose exchanges they read.
	for _, s := range stages {
		d := 1
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				d = max(d, depth[in.Exchange]+1)
			}
		}
		if s.Out != nil {
			depth[s.Out] = max(depth[s.Out], d)
		}
		most = max(most, d)
	}
	return most
}

// choose decides whether a statement whose plan key is key and whose
// estimates are e runs on workers, from e and what the statement took
// before (r, if found), and says why. It may run a statement where it has
// not run, against the estimates (explore), at most once in exploreEvery,
// to learn how long it takes there, but never one that ran on one machine
// within threshold on workers, nor on a side estimated at more than
// exploreWithin times what it took on the other, nor on one machine where
// its working set should spill (see spillOn), or, after a run on workers
// far slower than one machine's estimate, where it is far beyond one
// machine's memory; nor does pick try the large worker where its working
// set would spill there (see option.unproven).
//
// Statements running beside it on the replica (e.others) share its cores:
// a statement takes 1 + e.others times as long there, up to shareCap + 1
// times, which is what auto mode weighs one machine's time at, so that a
// heavy statement runs on workers rather than take the cores of the
// replica's other statements, as a dashboard's, and one ran only on
// workers is not tried on the replica meanwhile. At ten times the data,
// beside 16 users' dashboards, a report that had run in 25 s on one
// machine, and 66 s on 16 workers alone, ran 577 s there, the dashboards'
// median rising from 4 ms to 23 s.
func choose(e estimate, r runs, found bool, threshold time.Duration, now time.Time) (workers, explore bool, reason string) {
	workers, explore, reason = chooseShared(e, r, found, threshold, now, float64(1+min(max(e.others, 0), shareCap)))
	if e.others > 0 {
		reason += fmt.Sprintf("; %d other statements run on this replica", e.others)
	}
	return workers, explore, reason
}

// chooseShared is choose with one machine's time taken share times as
// long, as other statements share its cores.
func chooseShared(e estimate, r runs, found bool, threshold time.Duration, now time.Time, share float64) (workers, explore bool, reason string) {
	limit := threshold.Seconds()
	// Alone, workers must be clearly faster; beside others, no slower than
	// the replica shared, which they leave to the others.
	faster := speedup
	if share > 1 {
		faster = 1
	}
	worth := func(oneMachine float64) bool {
		return e.workers > 0 && (oneMachine*share > limit || e.spills) && e.onWorkers < oneMachine*share/faster
	}
	// Measured on one side only, a statement that took under exploreUnder
	// there is tried once a day on the other, whatever the estimates say,
	// so that the history comes to compare runs with runs: an estimate
	// that is wrong would otherwise keep it on the slower side for good,
	// as a run on workers that beat an estimate of one machine far too
	// high would.
	mayExplore := now.Sub(r.explored) >= exploreEvery
	// Worth trying where its estimate is close to what the statement took
	// on the other side, and, on one machine, where it should fit.
	tryWorkers := func(took float64) bool { return e.onWorkers < exploreWithin*took }
	tryOne := func(took float64) bool {
		return e.oneMachine < exploreWithin*took && !e.memory && !e.spills && !r.heavy && !e.risky
	}
	// A sort or window near the replica's room: workers, where they are
	// configured, unless they take riskyWithin times one machine's time.
	risky := func(oneMachine float64) bool {
		return e.risky && e.workers > 0 && e.onWorkers <= riskyWithin*oneMachine*share
	}
	riskyReason := func(what string) string {
		return fmt.Sprintf("%s, and its sort holds %s, near the %s one machine has for it (DuckDB's sort takes more than its limit)",
			what, gigabytes(int64(e.sorted)), gigabytes(int64(e.local.room)))
	}
	// Why one machine, by the rule that decided: its time oneMachine.
	here := func(oneMachine float64) string {
		t := oneMachine * share
		switch {
		case e.workers == 0 || math.IsInf(e.onWorkers, 1):
			return fmt.Sprintf("one machine should take %.0f s, and no way on workers would finish", t)
		case t <= limit && !e.spills:
			return fmt.Sprintf("one machine should take %.0f s, within the %.0f s a statement must take for workers to be worth it", t, limit)
		case faster > 1:
			return fmt.Sprintf("workers should take %.0f s, not %.1f times as fast as one machine's %.0f s, as a statement running alone needs", e.onWorkers, faster, t)
		}
		return fmt.Sprintf("workers should take %.0f s, no faster than one machine's %.0f s, shared", e.onWorkers, t)
	}
	switch {
	case found && r.outOfMemory && e.workers > 0:
		return true, false, "it ran out of memory on one machine"
	case found && r.guarded && e.workers > 0:
		return true, false, fmt.Sprintf("it ran on one machine for %.0f s, far past its estimate", r.local)
	case found && r.dist > 0 && r.distWorkers > 0 && e.workers >= 2*r.distWorkers && mayExplore:
		// Measured on far fewer workers than it would run on now, as
		// after an earlier session's runs on a smaller fleet: a report
		// that took 230 s on 4 workers took 84 s on 16, but stayed on one
		// machine at 178 s.
		return true, true, fmt.Sprintf("it ran in %.0f s on %d workers; trying %d once, estimated at %.0f s", r.dist, r.distWorkers, e.workers, e.onWorkers)
	case found && r.local > 0 && r.dist > 0:
		switch {
		case r.dist < r.local*share:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers and %.0f s on one machine", r.dist, r.local)
		case e.risky && r.dist <= riskyWithin*r.local*share:
			return true, false, riskyReason(fmt.Sprintf("it ran in %.0f s on workers and %.0f s on one machine", r.dist, r.local))
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine and %.0f s on workers", r.local, r.dist)
	case found && r.local > 0:
		switch {
		case r.local*share < limit:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, under %.0f s", r.local, limit/share)
		case e.workers > 0 && mayExplore && r.local < exploreUnder.Seconds() && tryWorkers(r.local):
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers once, estimated at %.0f s", r.local, e.onWorkers)
		case share == 1 && r.local < e.oneMachine/2:
			return false, false, fmt.Sprintf("it ran in %.0f s on one machine, well under the %.0f s estimated", r.local, e.oneMachine)
		case worth(r.local):
			return true, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
		case risky(r.local):
			return true, false, riskyReason(fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers))
		case e.workers > 0 && mayExplore && tryWorkers(r.local):
			return true, true, fmt.Sprintf("it ran in %.0f s on one machine; trying workers, estimated at %.0f s", r.local, e.onWorkers)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on one machine, and workers should take %.0f s", r.local, e.onWorkers)
	case found && r.dist > 0:
		switch {
		case share == 1 && mayExplore && r.dist < exploreUnder.Seconds() && tryOne(r.dist):
			return false, true, fmt.Sprintf("it ran in %.0f s on workers; trying one machine once, estimated at %.0f s", r.dist, e.oneMachine)
		case share == 1 && mayExplore && !r.heavy && !e.farBeyond && r.dist > slowOnWorkers*e.oneMachine:
			// Far slower on workers than one machine should take it, even
			// short of memory: without this it stayed there for good. Not
			// where its working set is far beyond one machine's memory,
			// which it would spill for far longer than its estimate says.
			return false, true, fmt.Sprintf("it ran in %.0f s on workers, over %.0f times one machine's estimate of %.0f s; trying one machine once",
				r.dist, slowOnWorkers, e.oneMachine)
		case e.memory || e.spills:
			// One machine's estimate runs four or five times short where it
			// spills: two reports that took 146 s and 141 s on workers,
			// against one machine's 157 s, took 809 s and 610 s there.
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should run short of memory", r.dist)
		case r.heavy:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, whose exchanges held %.1f GB, more than one machine's memory holds", r.dist, float64(r.exchangeBytes)/1e9)
		case r.dist <= e.onWorkers/2 || r.dist < e.oneMachine*share/faster:
			return true, false, fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine)
		case e.risky && r.dist <= riskyWithin*e.oneMachine*share:
			return true, false, riskyReason(fmt.Sprintf("it ran in %.0f s on workers, and one machine should take %.0f s", r.dist, e.oneMachine))
		}
		if faster > 1 {
			return false, false, fmt.Sprintf("it ran in %.0f s on workers, not %.1f times as fast as one machine's estimate of %.0f s, as a statement running alone needs",
				r.dist, faster, e.oneMachine)
		}
		return false, false, fmt.Sprintf("it ran in %.0f s on workers, no faster than one machine's estimate of %.0f s, shared", r.dist, e.oneMachine*share)
	case e.spills && e.workers > 0 && e.onWorkers <= spillWithin*e.oneMachine:
		return true, false, fmt.Sprintf("one machine should take %.0f s or more, spilling, and workers %.0f s", e.oneMachine, e.onWorkers)
	case worth(e.oneMachine):
		return true, false, fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers)
	case risky(e.oneMachine):
		return true, false, riskyReason(fmt.Sprintf("one machine should take %.0f s and workers %.0f s", e.oneMachine, e.onWorkers))
	}
	return false, false, here(e.oneMachine)
}

// planKey identifies a statement in the history of its runs: its syntax
// tree without positions, without its literals other than integers and
// without the snapshots it reads tables at, and the sizes of the tables
// its stages read, each to a factor of two. A BI tool's statement run
// again for another day is the same statement; one over tables that have
// grown fourfold is not.
func planKey(stmt node, stages []*Stage) string {
	shape := rewrite(clone(stmt), func(n node) (node, bool) {
		if at := child(n, "at_clause"); at != nil {
			// The snapshot a pin names moves with every commit, as the
			// statement Igloo writes to answer from bitmap indexes does.
			n["at_clause"] = node{"unit": at["unit"]}
		}
		if str(n["class"]) != "CONSTANT" {
			return nil, false
		}
		if _, ok := intConstant(n); ok {
			// Positions in GROUP BY and ORDER BY, LIMITs: part of the shape.
			return nil, false
		}
		return node{"class": "CONSTANT"}, true
	})
	h := sha256.New()
	h.Write([]byte(canonical(shape)))
	sizes := map[string]int{}
	for _, s := range stages {
		for _, in := range s.Inputs {
			if in.Table != nil {
				sizes[in.Table.key()] = bits.Len64(uint64(max(in.Table.Bytes, 0)))
			}
		}
	}
	for _, k := range slices.Sorted(maps.Keys(sizes)) {
		fmt.Fprintf(h, "\x00%s\x00%d", k, sizes[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
