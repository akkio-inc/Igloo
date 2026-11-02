package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"gocloud.dev/blob"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/prefetch"
)

// setupPrefetch starts the prefetcher (package prefetch) when IGLOO_PREFETCH
// is on and the read cache, which it reads ahead into, is set up. One that
// cannot start only costs speed: Igloo logs why and runs without it.
func (e *Engine) setupPrefetch(cfg config.Config, dir string, log *slog.Logger) {
	if !cfg.Prefetch.Enabled {
		return
	}
	c := e.cache
	objects := &objectOpener{storage: cfg.Storage}
	p, err := prefetch.New(prefetch.Options{
		Dir: dir, Concurrency: cfg.Prefetch.Concurrency, Log: log,
		Open: objects.open,
		Room: c.room,
	})
	if err != nil {
		log.Warn("prefetch off", "err", err)
		return
	}
	e.prefetcher, e.objects = p, objects
	// Unset, the concurrency is the prefetcher's default, which the log
	// says rather than 0.
	concurrency := cfg.Prefetch.Concurrency
	if concurrency <= 0 {
		concurrency = prefetch.DefaultConcurrency
	}
	log.Info("prefetch on", "dir", dir, "concurrency", concurrency)
}

// Prefetcher is the prefetcher, or nil when prefetching is off.
func (e *Engine) Prefetcher() *prefetch.Prefetcher { return e.prefetcher }

// bucket is what the prefetcher needs of a gocloud bucket, which the
// catalog's file systems for object storage embed.
type bucket interface {
	Attributes(ctx context.Context, key string) (*blob.Attributes, error)
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (*blob.Reader, error)
}

// objectOpener opens data files in object storage with Igloo's own
// credentials, keeping one file system a bucket.
type objectOpener struct {
	storage config.Storage
	mu      sync.Mutex
	buckets map[string]bucket
}

func (o *objectOpener) open(ctx context.Context, path string) (prefetch.Object, error) {
	scheme, rest, ok := strings.Cut(path, "://")
	host, key, ok2 := strings.Cut(rest, "/")
	if !ok || !ok2 || host == "" || key == "" {
		return nil, fmt.Errorf("not an object storage path: %q", path)
	}
	root := scheme + "://" + host
	o.mu.Lock()
	b, ok := o.buckets[root]
	o.mu.Unlock()
	if !ok {
		fsys, err := catalog.FS(ctx, o.storage, path)
		if err != nil {
			return nil, err
		}
		if b, ok = fsys.(bucket); !ok {
			return nil, errors.New("the warehouse's file system cannot read ranges")
		}
		o.mu.Lock()
		if o.buckets == nil {
			o.buckets = map[string]bucket{}
		}
		o.buckets[root] = b
		o.mu.Unlock()
	}
	attrs, err := b.Attributes(ctx, key)
	if err != nil {
		return nil, err
	}
	return &object{ctx: ctx, b: b, key: key, etag: attrs.ETag}, nil
}

// object reads ranges of a data file.
type object struct {
	ctx  context.Context
	b    bucket
	key  string
	etag string
}

func (o *object) ReadAt(p []byte, off int64) (int, error) {
	r, err := o.b.NewRangeReader(o.ctx, o.key, off, int64(len(p)), nil)
	if err != nil {
		return 0, err
	}
	defer r.Close() //nolint:errcheck
	return io.ReadFull(r, p)
}

func (o *object) Close() error { return nil }
func (o *object) ETag() string { return o.etag }
