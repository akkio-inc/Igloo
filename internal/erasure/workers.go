package erasure

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Workers of distributed execution (package distributed) keep what they
// read past a task, as replicas do: their read cache, on disk and in
// memory, and more (see distributed's erasure.go). They cannot reach the
// metadata database, so coordinators keep them up with requests, and
// record in the ledger, under WorkerCache's names, every worker that
// answers and those purged through the latest deletion (WorkerLedger).
// A request waits for running workers as for replicas. A worker whose
// cache outlives it (kept: on a disk that stays when it stops) and that
// does not answer has not purged what the request deleted, and keeps the
// request from completing: the request is blocked, and alerts, until the
// worker comes back and purges (which it does before it serves again),
// is deregistered (Service.DeregisterWorker) or the request is closed. So
// does a worker that does not say what it keeps (another version of
// Igloo), until it is upgraded or deregistered. A worker whose cache goes
// with it (in memory, or on a Kubernetes pod's volume) holds nothing once
// it is gone, and one that runs but that no coordinator reaches empties
// its caches by itself once it has heard from none for its isolated
// purge time (distributed.WorkerErasure.Isolated): the request waits that
// long, and isolatedMargin, from when it last deleted files before it
// counts such a worker as purged, never at once.

// workerPrefix starts the ledger's names of workers' caches.
const workerPrefix = "worker/"

// liveWorkerGone is how long a worker whose cache goes with it stays in
// the ledger after it was last seen.
const liveWorkerGone = 24 * time.Hour

// isolatedMargin is what a request allows a worker cut off from the
// coordinators, past its isolated purge time, to empty its caches: its
// checks, the wait for its tasks, the emptying.
const isolatedMargin = 2 * time.Minute

// WorkerCache is the ledger's name of what worker keeps: cache, its read
// cache's identity (engine.Engine.CacheIdentity), kept when the cache
// outlives the worker's process; otherwise isolated is how long the
// worker goes without hearing from a coordinator before it empties its
// caches by itself.
func WorkerCache(worker, cache string, kept bool, isolated time.Duration) string {
	kind := "ephemeral:" + strconv.FormatInt(int64(isolated.Seconds()), 10)
	if kept {
		kind = "kept"
	}
	return workerPrefix + kind + "/" + worker + "/" + cache
}

// unknownWorker is the ledger's name of a worker that does not say what it
// keeps.
func unknownWorker(worker string) string { return workerPrefix + "unknown/" + worker + "/-" }

// workerCache is a worker's cache as the ledger names it: kept, gone with
// the worker once it purged alone (isolated), or unknown.
type workerCache struct {
	worker   string
	kept     bool
	unknown  bool
	isolated time.Duration
}

// parseWorkerCache reads a name WorkerCache or unknownWorker made.
func parseWorkerCache(id string) (workerCache, bool) {
	parts := strings.SplitN(id, "/", 4)
	if len(parts) != 4 || parts[0]+"/" != workerPrefix || parts[2] == "" || parts[3] == "" {
		return workerCache{}, false
	}
	w := workerCache{worker: parts[2]}
	switch secs, ephemeral := strings.CutPrefix(parts[1], "ephemeral:"); {
	case parts[1] == "kept":
		w.kept = true
	case parts[1] == "unknown":
		w.unknown = true
	case ephemeral:
		n, err := strconv.ParseInt(secs, 10, 64)
		if err != nil || n <= 0 {
			return workerCache{}, false
		}
		w.isolated = time.Duration(n) * time.Second
	default:
		return workerCache{}, false
	}
	return w, true
}

// purgedAlone is when a worker whose cache goes with it and that no
// coordinator reaches counts as purged of what r deleted: its isolated
// purge time, and isolatedMargin, after r last deleted files.
func (w workerCache) purgedAlone(r *Request) time.Time {
	return r.DeletedThrough.Add(w.isolated + isolatedMargin)
}

// WorkerLedger is the ledger as coordinators keep workers in it
// (distributed.ErasureLedger).
type WorkerLedger struct {
	Store
	// Purged, if set, is called when a worker is recorded purged of more
	// than it was (see Emptied), as Service.WakeBlocked.
	Purged func()
}

// NewWorkerLedger returns the ledger of store, for coordinators.
func NewWorkerLedger(store Store) *WorkerLedger { return &WorkerLedger{Store: store} }

// WorkerSeen records that the worker that keeps cache answered, at host.
// The ledger forgets the caches the same worker kept before that have not
// been seen since it last ran: a worker that comes back with another cache
// came back without that one, as with a disk that does not outlive its
// machine. It also forgets the caches, gone with their workers, of
// workers not seen for a day.
func (l *WorkerLedger) WorkerSeen(ctx context.Context, worker, cache, host string, at time.Time) error {
	if cache == "" {
		cache = unknownWorker(worker)
	}
	w, ok := parseWorkerCache(cache)
	if !ok {
		return fmt.Errorf("%q is not the cache of a worker (%s)", cache, worker)
	}
	if err := l.Seen(ctx, cache, host, at); err != nil {
		return err
	}
	caches, err := l.Caches(ctx)
	if err != nil {
		return err
	}
	for _, c := range caches {
		o, ok := parseWorkerCache(c.ID)
		if !ok || c.ID == cache {
			continue
		}
		if o.worker == w.worker && at.Sub(c.Seen) > liveWindow || o.isolated > 0 && at.Sub(c.Seen) > liveWorkerGone {
			if err := l.ForgetCache(ctx, c.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Emptied records that cache was purged of what requests deleted through
// through, and calls Purged if that is more than it was.
func (l *WorkerLedger) Emptied(ctx context.Context, cache string, through time.Time, token string) error {
	more := false
	if l.Purged != nil {
		caches, err := l.Caches(ctx)
		if err != nil {
			return err
		}
		for _, c := range caches {
			more = more || c.ID == cache && c.Emptied.Before(through)
		}
	}
	if err := l.Store.Emptied(ctx, cache, through, token); err != nil {
		return err
	}
	if more {
		l.Purged()
	}
	return nil
}

// WakeBlocked has the requests blocked at their cache step, waiting for
// workers, run now rather than at their next hourly try: a worker was
// purged.
func (s *Service) WakeBlocked(ctx context.Context) {
	requests, err := s.opts.Store.List(ctx)
	if err != nil {
		s.log.Warn("could not list erasure requests", "err", err)
		return
	}
	for _, r := range requests {
		if r.Status == StatusBlocked && r.Step == StepCache {
			s.wake(ctx, r.ID, s.now())
		}
	}
}

// workersLeft names the workers whose caches outlive them, that are not
// running and that were not purged of what r deleted, and the workers that
// do not say what they keep: the request cannot complete until each comes
// back and purges (or is upgraded), is deregistered, or the request is
// closed.
func (s *Service) workersLeft(ctx context.Context, r *Request) ([]string, error) {
	if r.DeletedThrough.IsZero() {
		return nil, nil
	}
	caches, err := s.opts.Store.Caches(ctx)
	if err != nil {
		return nil, err
	}
	through := r.DeletedThrough.Truncate(time.Microsecond)
	now := s.now()
	var out []string
	for _, c := range caches {
		w, ok := parseWorkerCache(c.ID)
		switch {
		case !ok:
			continue
		case w.unknown:
			out = append(out, fmt.Sprintf("%s, which does not say what it keeps: upgrade it (%s)", c.Host, c.ID))
		case w.kept && c.Emptied.Before(through) && now.Sub(c.Seen) >= liveWindow:
			out = append(out, fmt.Sprintf("%s, last seen %s (%s)", c.Host, c.Seen.UTC().Format(time.RFC3339), c.ID))
		}
	}
	return out, nil
}

// DeregisterWorker forgets a worker's cache that will not come back, as
// the requests' ledger names it (WorkerCache): requests no longer wait for
// it to purge. Its callers allow admins only.
func (s *Service) DeregisterWorker(ctx context.Context, cache string) error {
	if _, ok := parseWorkerCache(cache); !ok {
		return fmt.Errorf("%w: %q is not a worker's cache, as blocked requests name them (worker/kept/...)", ErrInvalid, cache)
	}
	caches, err := s.opts.Store.Caches(ctx)
	if err != nil {
		return err
	}
	for _, c := range caches {
		if c.ID == cache {
			s.log.Warn("worker deregistered: erasure requests no longer wait for it to purge what they deleted", "cache", cache,
				"worker", c.Host)
			return s.opts.Store.ForgetCache(ctx, cache)
		}
	}
	return fmt.Errorf("%w: no worker's cache %s", ErrNotFound, cache)
}
