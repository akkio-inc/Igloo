package erasure

import (
	"context"
	"log/slog"
	"time"
)

// ReadCache is a replica's read cache of object storage files, on disk and
// in memory; *engine.Engine is one.
type ReadCache interface {
	// EmptyCache drops everything the cache holds. Requests empty caches
	// whole rather than drop the files they deleted: a cache also holds
	// blocks of files deleted before, by maintenance or another engine,
	// which no request knows of.
	EmptyCache(ctx context.Context) error
}

// MemoryCache starts the IDs of replicas without a cache on disk: what they
// keep in memory goes when they stop, so they need not empty it then.
const MemoryCache = "memory:"

// purgeInterval is how often replicas look for deletions.
const purgeInterval = 15 * time.Second

// Purger empties this replica's read cache whenever a request deleted
// files since it last did, and records that it did, so requests know.
// Every replica runs one, and a replica that was stopped empties its cache
// when it starts.
type Purger struct {
	store Store
	cache ReadCache
	id    string
	host  string
	log   *slog.Logger
}

// NewPurger purges cache, identified by id, on host.
func NewPurger(store Store, cache ReadCache, id, host string, log *slog.Logger) *Purger {
	return &Purger{store: store, cache: cache, id: id, host: host, log: log.With("component", "erasure")}
}

// Run purges every purgeInterval until ctx is canceled, starting at once.
func (p *Purger) Run(ctx context.Context) {
	for {
		if err := p.Once(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("could not empty the read cache of erased data", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(purgeInterval):
		}
	}
}

// Once empties the cache if a request deleted files since it last was,
// and records that the replica is running.
func (p *Purger) Once(ctx context.Context) error {
	if err := p.store.Seen(ctx, p.id, p.host, time.Now()); err != nil {
		return err
	}
	latest, err := p.store.LatestDeletion(ctx)
	if err != nil || latest.IsZero() {
		return err
	}
	caches, err := p.store.Caches(ctx)
	if err != nil {
		return err
	}
	for _, c := range caches {
		if c.ID == p.id && !c.Emptied.Before(latest) {
			return nil
		}
	}
	if err := p.cache.EmptyCache(ctx); err != nil {
		return err
	}
	p.log.Info("emptied the read cache of data erasure requests deleted")
	return p.store.Emptied(ctx, p.id, latest)
}
