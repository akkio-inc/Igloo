package erasure

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// disk stands in for a cache's disk: its blocks (a cache that counts its
// empties) and the ID file beside them.
type disk struct {
	cache     *readCache
	mu        sync.Mutex
	id, token string
}

func (d *disk) save(id, token string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.id, d.token = id, token
	return nil
}

// copyOf is another disk with d's blocks and ID file, as a VM image copies
// one.
func (d *disk) copyOf() *disk {
	d.mu.Lock()
	defer d.mu.Unlock()
	return &disk{cache: &readCache{}, id: d.id, token: d.token}
}

// replica starts a replica's purger on d, with the clock now.
func (d *disk) replica(store Store, host string, now func() time.Time) *Purger {
	d.mu.Lock()
	id, token := d.id, d.token
	d.mu.Unlock()
	p := NewPurger(store, d.cache, id, host, slog.New(slog.DiscardHandler)).OnDisk(token, d.save)
	p.now = now
	return p
}

func (d *disk) emptied() int {
	d.cache.mu.Lock()
	defer d.cache.mu.Unlock()
	return d.cache.emptied
}

// deleted records a request that deleted files at at.
func deleted(t *testing.T, store Store, id string, at time.Time) *Request {
	t.Helper()
	r := &Request{ID: id, Status: StatusRunning, DeletedThrough: at}
	if err := store.Create(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

// TestCacheKeptAcrossRecreate goes on with a cache on disk when a replica
// is made again on the same disk, as a container recreated with its
// volume, whatever its host name: it empties the cache only for what was
// deleted since it was last emptied.
func TestCacheKeptAcrossRecreate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clock := time.Now()
	now := func() time.Time { return clock }
	d := &disk{cache: &readCache{}, id: "disk:a", token: "t:0"}

	first := d.replica(store, "container-1", now)
	r1 := deleted(t, store, "r1", clock.Add(-time.Minute))
	if err := first.Once(ctx); err != nil || d.emptied() != 1 {
		t.Fatalf("after a deletion: emptied %d, %v", d.emptied(), err)
	}
	first.release()

	// Made again, at once, under another host name.
	second := d.replica(store, "container-2", now)
	if err := second.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if d.emptied() != 1 || second.ID() != "disk:a" {
		t.Errorf("made again with nothing deleted since: emptied %d times, ID %s", d.emptied(), second.ID())
	}
	svc := New(Options{Store: store})
	if emptied, _, waiting, err := svc.caches(ctx, r1); err != nil || emptied != 1 || len(waiting) != 0 {
		t.Errorf("caches for the first request: %d emptied, waiting %v, %v", emptied, waiting, err)
	}
	// A deletion while it runs empties it once more.
	r2 := deleted(t, store, "r2", clock)
	if err := second.Once(ctx); err != nil || d.emptied() != 2 {
		t.Errorf("after a second deletion: emptied %d, %v", d.emptied(), err)
	}
	if emptied, _, waiting, err := svc.caches(ctx, r2); err != nil || emptied != 1 || len(waiting) != 0 {
		t.Errorf("caches for the second request: %d emptied, waiting %v, %v", emptied, waiting, err)
	}

	// Killed without letting its claim go: the next one waits for the
	// claim to run out, keeping the cache, and is not counted meanwhile.
	third := d.replica(store, "container-3", now)
	if err := third.Once(ctx); err != nil || d.emptied() != 2 {
		t.Fatalf("while the claim lasts: emptied %d, %v", d.emptied(), err)
	}
	clock = clock.Add(claimTTL + time.Second)
	store.now = now
	if err := third.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if d.emptied() != 2 || third.ID() != "disk:a" {
		t.Errorf("once the claim ran out: emptied %d times, ID %s", d.emptied(), third.ID())
	}
}

// TestCacheCarriedAway empties a disk carried to another machine, or left
// stopped, of what requests deleted since it was last emptied, and keeps
// its ID: the ledger knows the disk by it.
func TestCacheCarriedAway(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clock := time.Now()
	now := func() time.Time { return clock }
	d := &disk{cache: &readCache{}, id: "disk:a", token: "t:0"}
	p := d.replica(store, "machine-1", now)
	if err := p.Once(ctx); err != nil {
		t.Fatal(err)
	}
	p.release()
	// Deleted while the disk was away: a request counts it pending.
	r := deleted(t, store, "r1", clock)
	clock = clock.Add(time.Hour)
	svc := New(Options{Store: store, Purger: nil})
	svc.now = now
	if emptied, pending, waiting, err := svc.caches(ctx, r); err != nil || emptied != 0 || pending != 1 || len(waiting) != 0 {
		t.Errorf("while away: %d emptied, %d pending, waiting %v, %v", emptied, pending, waiting, err)
	}
	moved := d.replica(store, "machine-2", now)
	if err := moved.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if d.emptied() != 1 || moved.ID() != "disk:a" {
		t.Errorf("on the new machine: emptied %d times, ID %s", d.emptied(), moved.ID())
	}
	if emptied, pending, _, err := svc.caches(ctx, r); err != nil || emptied != 1 || pending != 0 {
		t.Errorf("on the new machine: %d emptied, %d pending, %v", emptied, pending, err)
	}
}

// TestCacheCopies tells copies of a disk apart: a copy whose original was
// emptied since takes a new ID and is emptied, and so is one running while
// the original does, so that no ledger entry says a disk was emptied that
// was not.
func TestCacheCopies(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	clock := time.Now()
	now := func() time.Time { return clock }
	store.now = now
	original := &disk{cache: &readCache{}, id: "disk:a", token: "t:0"}
	copied := original.copyOf()

	a := original.replica(store, "vm-a", now)
	if err := a.Once(ctx); err != nil {
		t.Fatal(err)
	}
	r := deleted(t, store, "r1", clock)
	if err := a.Once(ctx); err != nil || original.emptied() != 1 {
		t.Fatalf("original: emptied %d, %v", original.emptied(), err)
	}
	a.release()

	// The copy, started later, holds what the original was emptied of.
	b := copied.replica(store, "vm-b", now)
	if err := b.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if copied.emptied() != 1 || b.ID() == "disk:a" || copied.id != b.ID() {
		t.Errorf("a copy behind the ledger: emptied %d times, ID %s (file %s)", copied.emptied(), b.ID(), copied.id)
	}
	svc := New(Options{Store: store})
	svc.now = now
	if emptied, _, waiting, err := svc.caches(ctx, r); err != nil || emptied != 2 || len(waiting) != 0 {
		t.Errorf("both emptied: %d, waiting %v, %v", emptied, waiting, err)
	}

	// Two copies running at once, before either was emptied: the one
	// refused the claim for longer than a claim lasts takes a new ID. (The
	// replicas above have stopped by then.)
	clock = clock.Add(3 * time.Minute)
	one := &disk{cache: &readCache{}, id: "disk:c", token: "t:c"}
	two := one.copyOf()
	p1, p2 := one.replica(store, "vm-1", now), two.replica(store, "vm-2", now)
	if err := p1.Once(ctx); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if err := p1.Once(ctx); err != nil {
			t.Fatal(err)
		}
		if err := p2.Once(ctx); err != nil {
			t.Fatal(err)
		}
		clock = clock.Add(purgeInterval)
	}
	if p1.ID() != "disk:c" || p2.ID() == "disk:c" || two.emptied() != 1 {
		t.Errorf("running copies: IDs %s and %s, the second emptied %d times", p1.ID(), p2.ID(), two.emptied())
	}
	// A deletion now: each empties its own disk under its own ID.
	r2 := deleted(t, store, "r2", clock)
	for _, p := range []*Purger{p1, p2} {
		if err := p.Once(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if emptied, _, waiting, err := svc.caches(ctx, r2); err != nil || len(waiting) != 0 || emptied < 2 {
		t.Errorf("after a deletion: %d emptied, waiting %v, %v", emptied, waiting, err)
	}
}
