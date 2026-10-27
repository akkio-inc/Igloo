package erasure

import (
	"context"
	"fmt"
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
// is deregistered (Service.DeregisterWorker) or the request is closed. A
// worker whose cache goes with it (in memory, or on a Kubernetes pod's
// volume) holds nothing once it is gone.

// workerPrefix starts the ledger's names of workers' caches.
const workerPrefix = "worker/"

// liveWorkerGone is how long a worker whose cache goes with it stays in
// the ledger after it was last seen.
const liveWorkerGone = 24 * time.Hour

// WorkerCache is the ledger's name of what worker keeps: cache, its read
// cache's identity (engine.Engine.CacheIdentity), kept when the cache
// outlives the worker's process.
func WorkerCache(worker, cache string, kept bool) string {
	kind := "ephemeral"
	if kept {
		kind = "kept"
	}
	return workerPrefix + kind + "/" + worker + "/" + cache
}

// workerCache is a worker's cache as the ledger names it.
type workerCache struct {
	worker string
	kept   bool
}

// parseWorkerCache reads a name WorkerCache made.
func parseWorkerCache(id string) (workerCache, bool) {
	parts := strings.SplitN(id, "/", 4)
	if len(parts) != 4 || parts[0]+"/" != workerPrefix || parts[2] == "" || parts[3] == "" {
		return workerCache{}, false
	}
	switch parts[1] {
	case "kept":
		return workerCache{worker: parts[2], kept: true}, true
	case "ephemeral":
		return workerCache{worker: parts[2]}, true
	}
	return workerCache{}, false
}

// WorkerLedger is the ledger as coordinators keep workers in it
// (distributed.ErasureLedger).
type WorkerLedger struct {
	Store
}

// NewWorkerLedger returns the ledger of store, for coordinators.
func NewWorkerLedger(store Store) *WorkerLedger { return &WorkerLedger{store} }

// WorkerSeen records that the worker that keeps cache answered, at host.
// The ledger forgets the caches the same worker kept before that have not
// been seen since it last ran: a worker that comes back with another cache
// came back without that one, as with a disk that does not outlive its
// machine. It also forgets the caches, gone with their workers, of
// workers not seen for a day.
func (l *WorkerLedger) WorkerSeen(ctx context.Context, cache, host string, at time.Time) error {
	w, ok := parseWorkerCache(cache)
	if !ok {
		return fmt.Errorf("%q is not a worker's cache", cache)
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
		if o.worker == w.worker && at.Sub(c.Seen) > liveWindow || !o.kept && at.Sub(c.Seen) > liveWorkerGone {
			if err := l.ForgetCache(ctx, c.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// workersLeft names the workers whose caches outlive them, that are not
// running and that were not purged of what r deleted: the request cannot
// complete until each comes back and purges, is deregistered, or the
// request is closed.
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
		if !ok || !w.kept || !c.Emptied.Before(through) || now.Sub(c.Seen) < liveWindow {
			continue
		}
		out = append(out, fmt.Sprintf("%s, last seen %s (%s)", c.Host, c.Seen.UTC().Format(time.RFC3339), c.ID))
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
