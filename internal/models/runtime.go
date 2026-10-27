package models

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// cacheModels is how many decoded models a process keeps.
const cacheModels = 16

// Runtime is what a process needs to score models and serve methods'
// results inside DuckDB: the blobs, a cache of decoded models, and the
// results statements read through igloo_ml_result. The server and every
// worker have one.
type Runtime struct {
	blobs atomic.Pointer[Blobs]

	mu    sync.Mutex
	cache map[string]*cached
	tick  uint64
	// loading holds the loads in flight, so that DuckDB's threads binding
	// the same model wait for one load.
	loading map[string]*load

	results sync.Map // token -> *Result
}

type cached struct {
	art  *Artifact
	used uint64
	// pinned models stay until Unpin: a statement's model that is in no
	// store.
	pinned bool
}

type load struct {
	done chan struct{}
	art  *Artifact
	err  error
}

// NewRuntime returns a runtime with no blobs: SetBlobs gives it some.
func NewRuntime() *Runtime {
	return &Runtime{cache: map[string]*cached{}, loading: map[string]*load{}}
}

// SetBlobs sets where blobs are read from.
func (r *Runtime) SetBlobs(b Blobs) { r.blobs.Store(&b) }

// Blobs returns where blobs are, or nil.
func (r *Runtime) Blobs() Blobs {
	if b := r.blobs.Load(); b != nil {
		return *b
	}
	return nil
}

// Load returns the model whose blob has SHA-256 sha, from the cache or
// the blobs.
func (r *Runtime) Load(ctx context.Context, sha string) (*Artifact, error) {
	r.mu.Lock()
	if c, ok := r.cache[sha]; ok {
		r.tick++
		c.used = r.tick
		r.mu.Unlock()
		return c.art, nil
	}
	if l, ok := r.loading[sha]; ok {
		r.mu.Unlock()
		select {
		case <-l.done:
			return l.art, l.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	l := &load{done: make(chan struct{})}
	r.loading[sha] = l
	r.mu.Unlock()

	l.art, l.err = r.fetch(ctx, sha)
	r.mu.Lock()
	delete(r.loading, sha)
	if l.err == nil {
		r.put(sha, l.art, false)
	}
	r.mu.Unlock()
	close(l.done)
	return l.art, l.err
}

func (r *Runtime) fetch(ctx context.Context, sha string) (*Artifact, error) {
	b := r.Blobs()
	if b == nil {
		return nil, ErrNoBlob
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	defer cancel()
	data, err := b.Get(ctx, sha)
	if err != nil {
		return nil, err
	}
	return DecodeArtifact(data, sha)
}

// put adds a model to the cache, evicting the least recently used
// unpinned one when full. r.mu is held.
func (r *Runtime) put(sha string, art *Artifact, pinned bool) {
	r.tick++
	if c, ok := r.cache[sha]; ok {
		c.used, c.pinned = r.tick, c.pinned || pinned
		return
	}
	unpinned := 0
	oldest := ""
	for k, c := range r.cache {
		if !c.pinned {
			unpinned++
			if oldest == "" || c.used < r.cache[oldest].used {
				oldest = k
			}
		}
	}
	if unpinned >= cacheModels && oldest != "" {
		delete(r.cache, oldest)
	}
	r.cache[sha] = &cached{art: art, used: r.tick, pinned: pinned}
}

// Remember caches a model just trained, so its first use need not read
// its blob back; pinned keeps it until Unpin.
func (r *Runtime) Remember(sha string, art *Artifact, pinned bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.put(sha, art, pinned)
}

// Unpin lets the cache drop a pinned model, and drops it now.
func (r *Runtime) Unpin(sha string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cache, sha)
}

// Forget drops a model from the cache: its blob was deleted.
func (r *Runtime) Forget(sha string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.cache[sha]; ok && !c.pinned {
		delete(r.cache, sha)
	}
}

// AddResult keeps a method's result for igloo_ml_result to read, and
// returns its token.
func (r *Runtime) AddResult(res *Result) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	token := hex.EncodeToString(b[:])
	r.results.Store(token, res)
	return token
}

// DropResult forgets a result.
func (r *Runtime) DropResult(token string) { r.results.Delete(token) }

// result returns the result of token.
func (r *Runtime) result(token string) (*Result, error) {
	v, ok := r.results.Load(token)
	if !ok {
		return nil, errors.New("this statement's model results are gone: prepare it again")
	}
	return v.(*Result), nil
}
