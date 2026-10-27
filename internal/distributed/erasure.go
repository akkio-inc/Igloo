package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Erasure requests and workers. An erasure request (package erasure)
// deletes a person's rows and the files that held them, and then has every
// replica empty its read cache, which may hold blocks of those files. A
// worker keeps, past the tasks that read them, its read cache on disk and
// in memory (blocks of data files, which the prefetcher also writes), the
// models it scored with, data files' footers and the exchange files of
// statements on its disk, its own and those it fetched; its spill and its
// tasks' results live only as long as a task, and go when it restarts. A
// worker cannot reach the metadata database where requests are kept, so
// coordinators keep it up with them:
//
//   - Every replica's coordinator learns the latest deletion of any
//     request, every erasureEvery, and has every worker purged through less
//     purge (POST /v1/purge, PurgeWorkers). It records in the requests'
//     ledger that each worker that answered runs, under the name of what it
//     keeps (WorkerErasure.Cache), and which are purged through the latest
//     deletion. A request waits for running workers, as for replicas, and
//     counts those that keep their cache past a stop and were not purged
//     (see erasure's caches).
//   - Every task carries the latest deletion its coordinator knew of when
//     the statement began, its epoch. A worker purged through less purges
//     before it runs the task, so a worker that was stopped, or cut off,
//     while a request ran purges before it serves again. A worker purged
//     through more refuses the task: its statement began before the
//     deletion, and may read the files it deleted, or exchange files
//     written from them, back into the cache; it runs on the coordinator.
//   - A purge first refuses older tasks, then cancels those running and
//     waits for them to end, holds the prefetcher, and only then empties
//     the read cache and the rest: nothing written from before the deletion
//     comes back after.

// erasureEvery is how often a coordinator keeps the workers up with
// erasure requests, as replicas look for deletions (erasure.Purger).
const erasureEvery = 15 * time.Second

// purgeTasksWait bounds how long a purge waits for the tasks it canceled.
const purgeTasksWait = time.Minute

// WorkerErasure is what a worker purges for erasure requests.
type WorkerErasure struct {
	// Cache names what the worker keeps past a task in the requests'
	// ledger (erasure.WorkerCache): its read cache's identity, and whether
	// the cache outlives the worker's process.
	Cache string
	// Through is the latest deletion the worker was purged of when it
	// stopped, as kept with its read cache on disk; zero for none.
	Through time.Time
	// Purge empties the read cache, on disk and in memory, and drops the
	// models and methods' results the process holds
	// (engine.Engine.EmptyCache).
	Purge func(ctx context.Context) error
	// Save keeps the latest deletion the worker was purged of with its
	// read cache on disk; nil without one.
	Save func(through time.Time) error
	// Hold, if set, runs fn while nothing writes into the read cache ahead
	// of DuckDB (prefetch.Prefetcher.Hold).
	Hold func(fn func() error) error
}

// errErased is what a purge cancels the tasks of statements that began
// before the deletion with.
var errErased = errors.New("an erasure request had the worker purge what it deleted while the task ran")

// admit records a task as running, unless the statement began before the
// latest deletion the worker was purged of (a taskErr then); a worker
// purged of less purges first.
func (w *Worker) admit(ctx context.Context, t *runningTask) (done func(), err error) {
	w.erasureMu.Lock()
	defer w.erasureMu.Unlock()
	if t.erased > w.purged.Load() {
		if err := w.purgeLocked(ctx, t.erased); err != nil {
			return nil, fmt.Errorf("the worker could not purge what erasure requests deleted before it runs the task: %w", err)
		}
	}
	if admitted := w.admitted.Load(); t.erased < admitted {
		return nil, &taskErr{Message: "an erasure request had the worker purge what it deleted since the statement began", Erased: admitted}
	}
	return w.begin(t), nil
}

// purgeLocked purges the worker of what erasure requests deleted through
// through, in Unix nanoseconds. w.erasureMu is held, so no task begins
// meanwhile.
func (w *Worker) purgeLocked(ctx context.Context, through int64) error {
	target := max(through, w.admitted.Load())
	// From now on, tasks of statements that began before are refused; those
	// running are canceled, and the purge waits for them, so that nothing
	// they read goes back into the cache after it.
	w.admitted.Store(target)
	w.runningMu.Lock()
	var old []*runningTask
	for t := range w.running {
		if t.erased < target {
			old = append(old, t)
		}
	}
	w.runningMu.Unlock()
	for _, t := range old {
		t.cancel(errErased)
	}
	wait := time.NewTimer(purgeTasksWait)
	defer wait.Stop()
	for _, t := range old {
		select {
		case <-t.done:
		case <-wait.C:
			return fmt.Errorf("a task canceled for the purge, of statement %s, did not end within %s", t.query, purgeTasksWait)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e := w.opts.Erasure
	empty := func() error {
		if e.Purge != nil {
			if err := e.Purge(ctx); err != nil {
				return fmt.Errorf("empty the read cache: %w", err)
			}
		}
		w.footersMu.Lock()
		w.footerCache = nil
		w.footersMu.Unlock()
		if err := w.local.clear(); err != nil {
			return fmt.Errorf("remove exchange files: %w", err)
		}
		return clearDir(w.fetchRoot)
	}
	var err error
	if e.Hold != nil {
		err = e.Hold(empty)
	} else {
		err = empty()
	}
	if err != nil {
		return err
	}
	if e.Save != nil {
		if err := e.Save(time.Unix(0, target)); err != nil {
			return fmt.Errorf("keep what the read cache was purged of: %w", err)
		}
	}
	w.purged.Store(target)
	w.log.Info("purged what erasure requests deleted", "through", time.Unix(0, target).UTC(), "canceled_tasks", len(old))
	return nil
}

// clearDir removes what dir holds, keeping dir.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		errs = append(errs, os.RemoveAll(filepath.Join(dir, e.Name())))
	}
	return errors.Join(errs...)
}

// clear removes every exchange file the worker wrote.
func (l *localExchanges) clear() error {
	l.mu.Lock()
	for qid := range l.queries {
		l.drop(qid)
	}
	l.mu.Unlock()
	return clearDir(l.root)
}

// servePurge purges the worker through the request's Through, if it is
// purged through less, and answers with its status.
func (w *Worker) servePurge(rw http.ResponseWriter, r *http.Request) {
	var req purgeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		http.Error(rw, "bad purge: "+err.Error(), http.StatusBadRequest)
		return
	}
	w.erasureMu.Lock()
	var err error
	if req.Through > w.purged.Load() {
		err = w.purgeLocked(r.Context(), req.Through)
	}
	w.erasureMu.Unlock()
	if err != nil {
		w.log.Warn("could not purge what erasure requests deleted", "err", err)
		http.Error(rw, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	w.serveStatus(rw, r)
}

// noteErased records that erasure requests deleted files through n, Unix
// nanoseconds, which the coordinator's next statements' tasks carry.
func (co *Coordinator) noteErased(n int64) {
	for {
		cur := co.erased.Load()
		if n <= cur || co.erased.CompareAndSwap(cur, n) {
			return
		}
	}
}

// ErasureLedger is where erasure requests (package erasure) keep what they
// deleted and which caches were purged of it: erasure.WorkerLedger.
type ErasureLedger interface {
	// LatestDeletion is when any request last deleted files.
	LatestDeletion(ctx context.Context) (time.Time, error)
	// WorkerSeen records that a worker that keeps cache (WorkerErasure.
	// Cache) answered, at host.
	WorkerSeen(ctx context.Context, cache, host string, at time.Time) error
	// Emptied records that cache was purged of what requests deleted
	// through through.
	Emptied(ctx context.Context, cache string, through time.Time, token string) error
}

// WatchErasures keeps the workers up with erasure requests (PurgeWorkers),
// every erasureEvery, until ctx is done.
func (co *Coordinator) WatchErasures(ctx context.Context, ledger ErasureLedger) {
	t := time.NewTicker(erasureEvery)
	defer t.Stop()
	for {
		if err := co.PurgeWorkers(ctx, ledger); err != nil && ctx.Err() == nil {
			co.log.Warn("could not keep the workers up with erasure requests", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PurgeWorkers learns the latest deletion of erasure requests from ledger,
// which the coordinator's statements' tasks carry from then on; has every
// worker that answers and was purged through less purge, whatever version
// of Igloo it runs; and records in ledger every worker that answered, and
// those purged through the latest deletion. A worker that does not say
// what it keeps (another version of Igloo) is left out.
func (co *Coordinator) PurgeWorkers(ctx context.Context, ledger ErasureLedger) error {
	latest, err := ledger.LatestDeletion(ctx)
	if err != nil {
		return err
	}
	var through int64
	if !latest.IsZero() {
		through = latest.UnixNano()
		co.noteErased(through)
	}
	ws := co.pool.answering(ctx)
	errs := make([]error, len(ws))
	var wg sync.WaitGroup
	for i, w := range ws {
		wg.Go(func() {
			st := w.status
			if st.Cache == "" {
				return
			}
			if through > st.Erased {
				purged, err := co.pool.purge(ctx, w.addr, through)
				if err != nil {
					errs[i] = fmt.Errorf("purge worker %s at %s: %w", st.ID, w.addr, err)
				} else {
					st = purged
				}
			}
			if err := ledger.WorkerSeen(ctx, st.Cache, st.ID+" at "+w.addr, time.Now()); err != nil {
				errs[i] = errors.Join(errs[i], err)
				return
			}
			if through > 0 && st.Erased >= through {
				errs[i] = errors.Join(errs[i], ledger.Emptied(ctx, st.Cache, latest, ""))
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}
