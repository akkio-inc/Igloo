package erasure

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/policy"
)

// TestDerivedPurged: a request purges what the AI functions cached of the
// tables it erases from (the cache of package ai), and what tables
// written from them gave, and leaves the rest.
func TestDerivedPurged(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	cache := ai.NewMemoryStore()
	f.svc.opts.Derived = []Derived{cache}
	epoch, _, _ := cache.State(f.ctx)
	now := time.Now()
	put := func(key string, tables ...string) {
		t.Helper()
		ok, err := cache.Put(f.ctx, epoch, []ai.CacheEntry{{Key: key, Function: "ai_summarize", Model: "main/m", Value: []byte(`"x"`),
			Tables: tables, Epoch: epoch, Created: now, Expires: now.Add(time.Hour)}})
		if err != nil || !ok {
			t.Fatalf("put %s: %v %v", key, ok, err)
		}
	}
	put("people", "crm.people")
	put("people and campaigns", "crm.people", "demo.campaigns")
	put("campaigns", "demo.campaigns")
	put("a file", ai.AnyTable)

	r := f.erase(byEmail)
	f.completed(r)
	if r.DerivedPurged != 3 || r.Verification.Derived != 0 {
		t.Errorf("purged %d, verification %+v", r.DerivedPurged, r.Verification)
	}
	hits, _ := cache.Lookup(f.ctx, []string{"people", "people and campaigns", "campaigns", "a file"}, now)
	if len(hits) != 1 || hits["campaigns"].Key == "" {
		t.Errorf("left %v", hits)
	}
	// A statement begun before the request keeps nothing more.
	if ok, _ := cache.Put(f.ctx, epoch, []ai.CacheEntry{{Key: "late", Tables: []string{"crm.people"}, Epoch: epoch, Expires: now.Add(time.Hour)}}); ok {
		t.Errorf("a statement begun before the request kept an answer after it")
	}
}

// racingCache is a cache in which, once, an entry derived from the
// request's table turns up after its purge, as one kept by a statement
// that began before it would without the cache's own guard.
type racingCache struct {
	*ai.MemoryStore
	mu    sync.Mutex
	raced bool
}

func (c *racingCache) Remaining(ctx context.Context, tables []string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.raced {
		c.raced = true
		return 1, nil
	}
	return c.MemoryStore.Remaining(ctx, tables)
}

// TestDerivedLeftSendsBack: a request that finds derived entries left
// after its purge goes back to its history step, and completes only once
// none are.
func TestDerivedLeftSendsBack(t *testing.T) {
	f := newFixture(t)
	f.people("crm.people", [][]any{ann, subject, carl})
	f.tag(policy.ColumnTag{Schema: "crm", Table: "people", Column: "email", Value: "email"})
	cache := &racingCache{MemoryStore: ai.NewMemoryStore()}
	f.svc.opts.Derived = []Derived{cache}
	r := f.erase(byEmail)
	f.completed(r)
	if r.Rounds < 1 || !cache.raced {
		t.Errorf("rounds %d, raced %v", r.Rounds, cache.raced)
	}
}
