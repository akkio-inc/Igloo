package erasure

import (
	"context"
	"log/slog"
	"time"
)

// ReadCache is a replica's read cache of object storage files, on disk and
// in memory; *engine.Engine is one.
type ReadCache interface {
	// ForgetCachedFiles drops whatever the cache holds of paths.
	ForgetCachedFiles(ctx context.Context, paths []string) error
}

// MemoryCache starts the IDs of replicas without a cache on disk: what they
// keep in memory goes when they stop, so they need not drop files then.
const MemoryCache = "memory:"

// purgeInterval is how often replicas look for files to drop.
const purgeInterval = 15 * time.Second

// Purger drops the files requests deleted from this replica's read cache,
// and records that it did, so requests know. Every replica runs one, and a
// replica that was stopped drops what it missed when it starts.
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
			p.log.Warn("could not drop erased files from the read cache", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(purgeInterval):
		}
	}
}

// Once drops what requests deleted since the last time, and records that
// the replica is running.
func (p *Purger) Once(ctx context.Context) error {
	if err := p.store.Seen(ctx, p.id, p.host, time.Now()); err != nil {
		return err
	}
	pending, err := p.store.Unpurged(ctx, p.id)
	if err != nil {
		return err
	}
	for _, u := range pending {
		if err := p.cache.ForgetCachedFiles(ctx, u.Paths); err != nil {
			return err
		}
		if err := p.store.Purged(ctx, u.Request, p.id, u.Through); err != nil {
			return err
		}
		p.log.Info("dropped erased files from the read cache", "request", u.Request, "files", len(u.Paths))
	}
	return nil
}
