package distributed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
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
//
// And it runs again as soon as the worker is found lost, rather than when a reader
// finds its files gone: at ten times the data, with one of 16 workers powered off,
// what it held ran again 53 to 113 s after it was found, once the stages before the
// readers had run, and the readers waited on it. So while a statement runs, the
// coordinator watches every worker that holds its exchange files, as it watches those
// that run its tasks.

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
	for _, w := range e.targets() {
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

// receivedRan adds what t wrote to its exchange, and notes t there as a task
// whose files the exchange holds, at once, so that a repair finds the files of
// any task the exchange lists. If t's worker was lost before it answered, what
// it wrote on its disk went with it, and is written again on the others before
// any reader is given it. Noted apart from its files, a task that answered as
// its worker was found lost was missed by the repair, and its files stayed
// listed on a worker the run had gone on without: a reader failed with
// "not a worker of the statement".
func (e *execution) receivedRan(ctx context.Context, t *ranTask) error {
	x := e.exchanges[t.stage.Out.ID]
	x.mu.Lock()
	err := e.receivedLocked(x, t.task, t.head, &worker{addr: t.worker})
	if err == nil && e.local {
		x.ran = append(x.ran, t)
	}
	x.mu.Unlock()
	if err != nil || !e.local || !t.onDisk() {
		return err
	}
	if !e.wentWithout(t.worker) {
		e.watchHolder(t.worker)
		return nil
	}
	lost := &lostError{worker: t.worker, err: fmt.Errorf("worker %s answered task %d after the statement went on without it", t.worker, t.task.Task)}
	if !e.recover(ctx, t.worker, lost, false) {
		return lost
	}
	return nil
}

// watchHolder watches the worker at addr, which holds exchange files of the run on its
// disk, while the run's exchanges are kept: should it be lost, what readers yet to run
// need of what it held runs again at once.
func (e *execution) watchHolder(addr string) {
	e.mu.Lock()
	ctx := e.watching
	if ctx == nil || e.holders[addr] || e.lost[addr] {
		e.mu.Unlock()
		return
	}
	if e.holders == nil {
		e.holders = map[string]bool{}
	}
	e.holders[addr] = true
	e.mu.Unlock()
	lost, why, release := e.co.pool.beats.watch(addr)
	go func() {
		defer release()
		select {
		case <-ctx.Done():
		case <-lost:
			e.recover(ctx, addr, why(), false)
		}
	}()
}

// stopWatching ends the run's watches of the workers holding its exchange files.
func (e *execution) stopWatching() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopWatch != nil {
		e.stopWatch()
	}
}

// workers returns the workers of the run, whose disks its tasks may read: those it
// goes on with, and one it went without until what it held is written again.
func (e *execution) workers() []*worker {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ws
}

// targets returns the workers the run gives tasks to: its workers but those it
// went without.
func (e *execution) targets() []*worker {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.lost) == 0 {
		return e.ws
	}
	return slices.DeleteFunc(slices.Clone(e.ws), func(w *worker) bool { return e.lost[w.addr] })
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
	return e.recover(ctx, lost.worker, err, true)
}

// recover goes on without the worker at addr, found lost with err by a reader or by
// its heartbeat, and has what readers yet to run need of what it held written again on
// the others, as repair says. Found by a reader, it is first asked whether it is lost;
// its heartbeat has.
func (e *execution) recover(ctx context.Context, addr string, err error, reader bool) bool {
	by := "its heartbeat"
	if reader {
		by = "a reader"
	}
	e.repairing.Lock()
	defer e.repairing.Unlock()
	if !e.wentWithout(addr) {
		ws := e.workers()
		if reader && !e.co.confirmLost(ctx, ws, addr) {
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
		if e.lost == nil {
			e.lost = map[string]bool{}
		}
		e.lost[addr] = true
		e.mu.Unlock()
		if e.places != nil {
			e.places.leave(addr)
		}
	}
	start := time.Now()
	n, waited, rerr := e.writeAgain(ctx, addr)
	// Tasks may read its disk no more: what readers need of it is on the others.
	// Until now they could, as a task made meanwhile still lists its files, and
	// finds them gone as lost, rather than not the statement's.
	e.mu.Lock()
	e.ws = slices.DeleteFunc(slices.Clone(e.ws), func(w *worker) bool { return w.addr == addr })
	e.mu.Unlock()
	if rerr != nil {
		if ctx.Err() == nil {
			e.co.log.Warn("could not run again only what a lost worker held; the statement's parts run again", "query", e.qid, "worker", addr,
				"found_by", by, "err", rerr)
		}
		return false
	}
	if n > 0 {
		e.co.log.Warn("a worker holding exchanged rows on its disk was lost; ran again only its tasks that readers still need", "query", e.qid,
			"worker", addr, "found_by", by, "tasks", n, "seconds", time.Since(start).Seconds(), "longest_slot_wait_seconds", waited)
	}
	return true
}

// writeAgain runs again what readers yet to run need of what the lost worker at addr
// wrote, returning how many tasks that took, and the longest any of them waited for a
// worker's slots, in seconds: for each exchange that the stages yet to end read, the
// tasks that wrote it there, once those that wrote there what they read in turn have.
func (e *execution) writeAgain(ctx context.Context, addr string) (int, float64, error) {
	if repairFault != nil {
		if err := repairFault(); err != nil {
			return 0, 0, err
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
	var (
		count  atomic.Int64
		mu     sync.Mutex
		waited float64
	)
	for _, tasks := range order {
		for _, t := range tasks {
			if !repeatable(t.stage) {
				return 0, 0, fmt.Errorf("stage %d's tasks may write something else each time", t.stage.ID)
			}
		}
		g, gctx := errgroup.WithContext(ctx)
		for _, t := range tasks {
			g.Go(func() error {
				n, w, err := e.taskAgain(gctx, t)
				if err != nil {
					return err
				}
				count.Add(int64(n))
				mu.Lock()
				waited = max(waited, w)
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return 0, 0, err
		}
	}
	return int(count.Load()), waited, nil
}

// taskAgain runs t again on the workers left, as smaller tasks if it is a whole one
// that can be divided, and has its exchange hold what they wrote in place of what it
// wrote before. It returns how many tasks it ran, and the longest any waited for a
// worker's slots, in seconds; they go before the other tasks waiting there
// (taskRequest.Repair).
func (e *execution) taskAgain(ctx context.Context, t *ranTask) (int, float64, error) {
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
			task.Repair = true
			first := t.first + 1 + n
			w, head, err := e.co.dispatch(gctx, task, e.targets(), first, "", nil)
			if err != nil {
				return err
			}
			again[n] = &ranTask{run: t.run, stage: t.stage, piece: p, units: t.units, first: first, request: t.request,
				task: task, head: head, worker: w.addr, rounds: round}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, 0, err
	}
	if len(again) == 1 && again[0].head.Rows != t.head.Rows {
		// Divided, a task that groups its rows may well write more of them.
		return 0, 0, fmt.Errorf("task %d wrote %d rows, and %d when it ran again", t.task.Task, t.head.Rows, again[0].head.Rows)
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
		x.again++
	}
	x.mu.Unlock()
	if err != nil {
		return 0, 0, err
	}
	waited := 0.0
	for _, a := range again {
		if a.onDisk() {
			t.run.watchHolder(a.worker)
		}
		waited = max(waited, a.head.Waited)
	}
	for range again {
		e.co.opts.Metrics.task("ran_again_for_lost_worker")
	}
	return len(again), waited, nil
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
