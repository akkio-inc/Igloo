package distributed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Running again only what a lost worker takes with it. Exchange files on a worker's
// disk go with the worker, and every part of a statement ran again without it: at ten
// times the data, a report of 116 to 141 s took 228 s with one of 16 workers powered
// off. But what it lost is what its tasks wrote and readers have yet to read. So the
// tasks it ran of exchanges still to be read run again on the others, those whose own
// inputs it held after those that wrote them; the tasks that failed run again, on what
// the exchanges hold now; and nothing else does. A statement whose tasks write
// something else each time cannot mix two runs of a task, and runs whole again as
// before, as it does if a task writes other rows than it did, or anything else goes
// wrong here.
//
// A task is a worker's whole share of its stage, and run again as one, on one of the
// others, it took as long as the stage had: running only what was lost added 39 s to
// that report, all of its parts again 35 s. So a task lost, as it ran or with what it
// wrote, runs as smaller ones spread over the workers left, one each, each the j-th of
// every k of its buckets, partitions or files of the split table (see piece), which the
// plan divides its tasks by in any case. Those write to directories of their own, so
// that what the lost worker wrote, should it turn up late, is never read; and the
// exchange lists only what they wrote, in place of what the task had.

// repairRounds bounds how many times a task runs again for workers lost.
const repairRounds = 3

// repairFault, if set, fails what writeAgain does; a test's.
var repairFault func() error

// ranTask is a task that wrote to an exchange, on a worker's disk if it could.
type ranTask struct {
	// run is the run it was part of, which another's may read (see runProbing).
	run   *execution
	stage *Stage
	// piece is which task, or which part of one, and units how many parts its task
	// may be divided into.
	piece   piece
	units   int
	first   int
	request func(p piece, round int) (*taskRequest, int64)
	task    *taskRequest
	head    *taskResult
	worker  string
	// rounds counts how many times it ran again.
	rounds int
}

// taskLostError is a task whose worker was lost while it ran it, which its stage runs
// as smaller tasks on the others (see dispatch and taskRequest.splittable).
type taskLostError struct {
	worker string
	err    error
}

func (e *taskLostError) Error() string {
	return fmt.Sprintf("worker %s was lost while it ran the task: %v", e.worker, e.err)
}
func (e *taskLostError) Unwrap() error { return e.err }

// usable counts the workers the run goes on with that are not set aside.
func (e *execution) usable() int {
	n := 0
	for _, w := range e.workers() {
		if !e.co.pool.isDown(w.addr) {
			n++
		}
	}
	return n
}

// onDisk reports whether t wrote a file to its worker's disk.
func (t *ranTask) onDisk() bool {
	return t.task.Local != "" && slices.ContainsFunc(t.head.Files, func(f shuffleFile) bool {
		return strings.HasPrefix(f.Path, t.task.Local+"/")
	})
}

// ranOn notes t, which its exchange's files now hold.
func (e *execution) ranOn(t *ranTask) {
	if !e.local {
		return
	}
	x := e.exchanges[t.stage.Out.ID]
	x.mu.Lock()
	defer x.mu.Unlock()
	x.ran = append(x.ran, t)
}

// workers returns the workers the run goes on with.
func (e *execution) workers() []*worker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ws
}

// wentWithout reports whether the run went on without the worker at addr. A run may
// be nil.
func (e *execution) wentWithout(addr string) bool {
	if e == nil {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lost[addr]
}

// repeatable reports whether a task of stage s writes the same rows each time, as far
// as its text tells: it calls no volatile function, and takes no rows that any would do.
func repeatable(s *Stage) bool {
	if s.enough > 0 {
		return false
	}
	for _, w := range sqllex.CodeWords(s.SQL) {
		switch w = strings.ToLower(w); {
		case volatileFunctions[w], w == "limit", w == "sample", w == "tablesample", w == "reservoir":
			return false
		}
	}
	return true
}

// repair is what a task that failed with err does before it runs again: if a worker
// was lost with exchange files, it has what those held that is still to be read written
// again on the others. It reports whether the task should run again, and otherwise the
// error stands, and the statement's parts run again if that is what it calls for (see
// Execute).
func (e *execution) repair(ctx context.Context, err error) bool {
	var lost *lostError
	if !e.local || !errors.As(err, &lost) || ctx.Err() != nil {
		return false
	}
	e.repairing.Lock()
	defer e.repairing.Unlock()
	addr := lost.worker
	if !e.wentWithout(addr) {
		ws := e.workers()
		if !e.co.confirmLost(ctx, ws, addr) {
			// It answers as the run it was, which has its files: the reader
			// gave up on it too soon, as may happen on a machine run ragged.
			e.co.log.Warn("a reader gave up on a worker that still answers; running its task again", "query", e.qid, "worker", addr, "err", err)
			return true
		}
		rest := slices.DeleteFunc(slices.Clone(ws), func(w *worker) bool { return w.addr == addr })
		if len(rest) == 0 || len(rest) == len(ws) {
			return false
		}
		e.co.pool.markDown(addr)
		e.mu.Lock()
		e.ws = rest
		if e.lost == nil {
			e.lost = map[string]bool{}
		}
		e.lost[addr] = true
		e.mu.Unlock()
	}
	start := time.Now()
	n, rerr := e.writeAgain(ctx, addr)
	if rerr != nil {
		e.co.log.Warn("could not run again only what a lost worker held; the statement's parts run again", "query", e.qid, "worker", addr, "err", rerr)
		return false
	}
	if n > 0 {
		e.co.log.Warn("a worker holding exchanged rows on its disk was lost; ran again only its tasks that readers still need", "query", e.qid,
			"worker", addr, "tasks", n, "seconds", time.Since(start).Seconds())
	}
	return true
}

// writeAgain runs again what readers yet to run need of what the lost worker at addr
// wrote, returning how many tasks that took: for each exchange that the stages yet to
// end read, the tasks that wrote it there, once those that wrote there what they read
// in turn have.
func (e *execution) writeAgain(ctx context.Context, addr string) (int, error) {
	if repairFault != nil {
		if err := repairFault(); err != nil {
			return 0, err
		}
	}
	seen := map[*exchangeRun]bool{}
	var order [][]*ranTask
	var visit func(x *exchangeRun)
	visit = func(x *exchangeRun) {
		if x == nil || seen[x] {
			return
		}
		seen[x] = true
		x.mu.Lock()
		var tasks []*ranTask
		for _, t := range x.ran {
			if t.worker == addr && t.onDisk() {
				tasks = append(tasks, t)
			}
		}
		x.mu.Unlock()
		for _, t := range tasks {
			for _, in := range t.stage.Inputs {
				if in.Exchange != nil {
					visit(t.run.exchanges[in.Exchange.ID])
				}
			}
		}
		if len(tasks) > 0 {
			order = append(order, tasks)
		}
	}
	e.mu.Lock()
	var open []*Stage
	for _, s := range e.started {
		if !e.finished[s.ID] {
			open = append(open, s)
		}
	}
	e.mu.Unlock()
	for _, s := range open {
		for _, in := range s.Inputs {
			if in.Exchange != nil {
				visit(e.exchanges[in.Exchange.ID])
			}
		}
	}
	var count atomic.Int64
	for _, tasks := range order {
		for _, t := range tasks {
			if !repeatable(t.stage) {
				return 0, fmt.Errorf("stage %d's tasks may write something else each time", t.stage.ID)
			}
		}
		g, gctx := errgroup.WithContext(ctx)
		for _, t := range tasks {
			g.Go(func() error {
				n, err := e.taskAgain(gctx, t)
				if err != nil {
					return err
				}
				count.Add(int64(n))
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return 0, err
		}
	}
	return int(count.Load()), nil
}

// taskAgain runs t again on the workers left, as smaller tasks if it is a whole one
// that can be divided, and has its exchange hold what they wrote in place of what it
// wrote before. It returns how many tasks it ran.
func (e *execution) taskAgain(ctx context.Context, t *ranTask) (int, error) {
	round := t.rounds + 1
	pieces := []piece{t.piece}
	if k := min(t.units, e.usable()); !t.piece.split() && k > 1 {
		pieces = make([]piece, k)
		for j := range pieces {
			pieces[j] = piece{i: t.piece.i, j: j, k: k}
		}
	}
	again := make([]*ranTask, len(pieces))
	g, gctx := errgroup.WithContext(ctx)
	for n, p := range pieces {
		g.Go(func() error {
			task, _ := t.request(p, round)
			first := t.first + 1 + n
			w, head, err := e.co.dispatch(gctx, task, e.workers(), first, "", nil)
			if err != nil {
				return err
			}
			again[n] = &ranTask{run: t.run, stage: t.stage, piece: p, units: t.units, first: first, request: t.request,
				task: task, head: head, worker: w.addr, rounds: round}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	if len(again) == 1 && again[0].head.Rows != t.head.Rows {
		// Divided, a task that groups its rows may well write more of them.
		return 0, fmt.Errorf("task %d wrote %d rows, and %d when it ran again", t.task.Task, t.head.Rows, again[0].head.Rows)
	}
	x := t.run.exchanges[t.stage.Out.ID]
	// At once: a task made in between would read short of what t wrote, and check
	// the rows of its files against those.
	x.mu.Lock()
	t.run.forget(x, t)
	var err error
	for _, a := range again {
		if err = t.run.receivedLocked(x, a.task, a.head, &worker{addr: a.worker}); err != nil {
			break
		}
	}
	if err == nil {
		if i := slices.Index(x.ran, t); i >= 0 {
			x.ran = slices.Replace(x.ran, i, i+1, again...)
		}
	}
	x.mu.Unlock()
	if err != nil {
		return 0, err
	}
	t.run.writtenAgain.Add(1)
	e.writtenAgain.Add(1)
	for range again {
		e.co.opts.Metrics.task("ran_again_for_lost_worker")
	}
	return len(again), nil
}

// forget takes what t wrote out of x, whose lock the caller holds.
func (e *execution) forget(x *exchangeRun, t *ranTask) {
	x.rows -= t.head.Rows
	x.tasks--
	for _, f := range t.head.Files {
		path := f.Path
		if t.task.Local != "" && strings.HasPrefix(f.Path, t.task.Local+"/") {
			path = localRef(t.worker, f.Path, f.Size)
		}
		x.files[f.Partition] = slices.DeleteFunc(x.files[f.Partition], func(p string) bool { return p == path })
		x.partRows[f.Partition] -= f.Rows
		x.partBytes[f.Partition] -= f.Size
		x.bytes -= f.Size
	}
}
