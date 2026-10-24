package erasure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
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

// claimTTL is how long a replica's claim on its cache's ID lasts unless it
// renews it, which it does each purgeInterval: a replica that stops
// without letting its claim go keeps it this long.
const claimTTL = time.Minute

// Purger empties this replica's read cache whenever a request deleted
// files since it last did, and records that it did, so requests know.
// Every replica runs one, and a replica that was stopped empties its cache
// when it starts, if a request deleted files since.
//
// A cache on disk keeps its ID with the disk, in a file beside its blocks
// (see OnDisk and engine.Engine.CacheIdentity), so a replica made again on
// the same disk, as a container recreated with its volume, goes on with
// the cache it had. Two disks must never share an ID, though: the ledger's
// record that a cache was emptied must be about the disk that holds it,
// or a copy of a disk, made with a VM image, say, would keep what the
// original was emptied of. So:
//   - One running replica at a time uses an ID: it claims it in the store
//     (Store.Claim), renews the claim each purgeInterval and lets it go
//     when it stops. A replica refused the claim for longer than a claim
//     lasts shares the ID with another running replica, a copy of its
//     disk, and takes a new ID, emptying its cache.
//   - Each time it empties its cache, a replica gives its disk a new purge
//     token, which it writes beside the ID and records with the ledger's
//     entry (Store.Emptied). A disk whose token is not the ledger's is not
//     the one the ledger was last told about, a copy of it emptied since or
//     one left behind, and takes a new ID, emptying its cache.
//
// So a cache that may hold what a request deleted since it was last
// emptied is emptied before it counts as emptied, as before, and only
// such caches are.
type Purger struct {
	store Store
	cache ReadCache
	id    string
	host  string
	log   *slog.Logger

	// token and save are set for a cache on disk (OnDisk): its purge
	// token, and what keeps an ID and token with the disk.
	token string
	save  func(id, token string) error
	// holder names this process in claims; refused is when the claim was
	// first refused, while it is.
	holder  string
	refused time.Time
	now     func() time.Time
	mu      sync.Mutex
}

// NewPurger purges cache, identified by id, on host.
func NewPurger(store Store, cache ReadCache, id, host string, log *slog.Logger) *Purger {
	return &Purger{store: store, cache: cache, id: id, host: host, log: log.With("component", "erasure"), now: time.Now}
}

// OnDisk says the cache is on disk, with purge token token, and that save
// keeps a new ID and token with it (see the Purger's comment).
func (p *Purger) OnDisk(token string, save func(id, token string) error) *Purger {
	p.token, p.save = token, save
	p.holder = p.host + "/" + randomHex()
	return p
}

// ID is the cache's ID as the purger has it now.
func (p *Purger) ID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.id
}

// Run purges every purgeInterval until ctx is canceled, starting at once,
// and then lets the purger's claim go.
func (p *Purger) Run(ctx context.Context) {
	for {
		if err := p.Once(ctx); err != nil && ctx.Err() == nil {
			p.log.Warn("could not empty the read cache of erased data", "err", err)
		}
		select {
		case <-ctx.Done():
			p.release()
			return
		case <-time.After(purgeInterval):
		}
	}
}

// release lets this process's claim on its cache's ID go, so that the next
// one on the disk goes on with it at once.
func (p *Purger) release() {
	if p.save == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.store.Release(ctx, p.id, p.holder); err != nil {
		p.log.Warn("could not let the read cache's claim go; the next replica on its disk waits for it to run out", "err", err)
	}
}

// Once empties the cache if a request deleted files since it last was,
// and records that the replica is running.
func (p *Purger) Once(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.save != nil {
		ok, token, err := p.store.Claim(ctx, p.id, p.holder, claimTTL)
		if err != nil {
			return err
		}
		switch {
		case !ok:
			// The previous replica on this disk, whose claim runs out, or
			// another running replica on a copy of it.
			now := p.now()
			if p.refused.IsZero() {
				p.refused = now
			}
			if now.Sub(p.refused) < claimTTL+purgeInterval {
				return nil
			}
			p.log.Warn("another running replica claims this read cache's ID, as a copy of its disk would; it takes a new one and is emptied")
			return p.rekey(ctx)
		case token != "" && token != p.token:
			p.log.Warn("the erasure ledger was told of another disk with this read cache's ID, as a copy of it; it takes a new one and is emptied")
			return p.rekey(ctx)
		}
		p.refused = time.Time{}
	}
	if err := p.store.Seen(ctx, p.id, p.host, p.now()); err != nil {
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
	return p.empty(ctx, latest)
}

// empty empties the cache and records it emptied through latest, with a
// new purge token for a cache on disk, written to the disk first.
func (p *Purger) empty(ctx context.Context, latest time.Time) error {
	if err := p.cache.EmptyCache(ctx); err != nil {
		return err
	}
	p.log.Info("emptied the read cache of data erasure requests deleted")
	token := ""
	if p.save != nil {
		token = "t:" + randomHex()
		if err := p.save(p.id, token); err != nil {
			return err
		}
		p.token = token
	}
	return p.store.Emptied(ctx, p.id, latest, token)
}

// rekey gives the cache on disk a new ID and empties it.
func (p *Purger) rekey(ctx context.Context) error {
	if err := p.cache.EmptyCache(ctx); err != nil {
		return err
	}
	id, token := "disk:"+randomHex(), "t:"+randomHex()
	if err := p.save(id, token); err != nil {
		return err
	}
	p.id, p.token, p.refused = id, token, time.Time{}
	if _, _, err := p.store.Claim(ctx, p.id, p.holder, claimTTL); err != nil {
		return err
	}
	if err := p.store.Seen(ctx, p.id, p.host, p.now()); err != nil {
		return err
	}
	latest, err := p.store.LatestDeletion(ctx)
	if err != nil || latest.IsZero() {
		return err
	}
	return p.store.Emptied(ctx, p.id, latest, token)
}

func randomHex() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
