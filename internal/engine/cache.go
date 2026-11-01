package engine

// Igloo keeps the table files it reads from object storage on local disk, in
// DuckDB's cache_httpfs community extension. The extension wraps httpfs's
// S3 filesystem (which also serves gs://): it splits every read into
// 512 KiB blocks and serves a block from a file in the cache directory when
// it has one, or fetches it and then stores it there. The files survive
// restarts, so the first queries after a start, for example after scaling
// to zero, read local disk instead of object storage. DuckDB's in-memory
// cache stays in front of it, so repeated reads within a run are as fast as
// before.
//
// Only the warehouse is cached. Iceberg writes its data, delete, manifest
// and metadata files once, under unique names, and never changes them, so a
// cached block cannot go stale. Files that do change, such as one a user
// overwrites with COPY TO and reads by path, are still read correctly: a
// block cached from a file opened with its ETag (from a HEAD request or a
// listing) records it and is used only while it matches. (Iceberg's own
// reads take file sizes from manifests and pass no ETag; their files never
// change.) The extension's caches of directory listings, file handles and
// file sizes, which would see changes late, are off.
//
// The extension bounds the cache by the free space it leaves on the disk:
// below that it stores no more blocks and, on each miss, deletes the least
// recently used one. Igloo sets that free space so the cache takes at most
// half of the disk space nothing else uses, and at most maxAutoCacheSize
// (or IGLOO_CACHE_SIZE), and remeasures every cacheMeasureInterval as other
// files on the disk come and go. When less than 5% of the disk would be free
// even without the cache, Igloo empties and pauses it until there is room.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/config"
)

const (
	// maxAutoCacheSize caps a cache sized from the free disk space. Every
	// 512 KiB block is a file, and the extension's LRU index holds them
	// all in memory once the cache is full.
	maxAutoCacheSize = 100 << 30
	// cacheMeasureInterval is how often Igloo measures the cache and its
	// disk and adjusts the free space the extension leaves.
	cacheMeasureInterval = 5 * time.Minute
	// cacheSubdir is the directory, inside IGLOO_CACHE_DIR, that holds the
	// extension's files. The extension expects nothing else in it (a
	// volume's lost+found, for instance), and clearing the cache deletes
	// and recreates it, which a mount point would not allow.
	cacheSubdir = "blocks"
)

// readCache is the state of an enabled cache.
type readCache struct {
	// dir is the extension's cache directory.
	dir string
	// maxSize is IGLOO_CACHE_SIZE, or 0 to size the cache automatically.
	maxSize uint64
	// ephemeral is IGLOO_CACHE_EPHEMERAL: the cache's volume goes with the
	// pod (see CacheIdentity).
	ephemeral bool
	// dedicated is IGLOO_CACHE_DEDICATED: the cache has its volume to
	// itself (see cacheLimits).
	dedicated bool
	// reserve is the free space the extension currently leaves on the disk,
	// and on whether it caches at all. Once the cache is set up, only
	// measureCache touches them.
	reserve uint64
	on      bool
	// size and limit are the cache's size and what it may grow to, as of
	// the last measurement, for metrics.
	size, limit atomic.Uint64
	// floor and caching are reserve and on, for the prefetcher, which reads
	// them while measureCache changes them (see room).
	floor   atomic.Uint64
	caching atomic.Bool

	stop, done chan struct{}
}

// setupCache loads and configures the cache when IGLOO_CACHE_DIR is set and
// the warehouse is on S3 or GCS. A cache that cannot be set up only costs
// speed, so Igloo then logs why and runs without it.
func (e *Engine) setupCache(exec func(string) error, cfg config.Config, log *slog.Logger) error {
	if cfg.Cache.Dir == "" {
		return nil
	}
	if cfg.Storage.Type == config.StorageAzure {
		log.Info("the read cache does not cover Azure storage yet; reading from it directly")
		return nil
	}
	dir := filepath.Join(cfg.Cache.Dir, cacheSubdir)
	if err := checkWritable(dir); err != nil {
		log.Warn("read cache off: its directory is not writable", "dir", dir, "err", err)
		return nil
	}
	total, avail, err := diskSpace(dir)
	if err != nil {
		log.Warn("read cache off: cannot measure its disk", "dir", dir, "err", err)
		return nil
	}
	c := &readCache{dir: dir, maxSize: uint64(cfg.Cache.MaxSize), ephemeral: cfg.Cache.Ephemeral, dedicated: cfg.Cache.Dedicated}
	// Measuring what is already cached means listing every file, so it
	// happens in the background once Igloo is up (see measure). Until then
	// the cache counts as empty.
	limit, reserve := cacheLimits(total, avail, 0, c.maxSize, c.dedicated)
	c.reserve, c.on = reserve, limit > 0
	c.limit.Store(limit)
	c.floor.Store(reserve)
	c.caching.Store(c.on)

	if err := exec("LOAD cache_httpfs"); err != nil {
		log.Warn("read cache off: cannot load the cache_httpfs extension", "err", err)
		return nil
	}
	for _, stmt := range cacheStatements(dir, cfg.Storage.Root(), reserve, c.on) {
		if err := exec(stmt); err != nil {
			// The extension is loaded with its own defaults, which
			// cache directory listings: turn it into a plain httpfs.
			for _, off := range cacheOffStatements() {
				if offErr := exec(off); offErr != nil {
					return fmt.Errorf("configure the read cache (%s): %w", stmt, errors.Join(err, offErr))
				}
			}
			log.Warn("read cache off: cannot configure it", "statement", stmt, "err", err)
			return nil
		}
	}
	e.cache = c
	if c.on {
		log.Info("read cache on", "dir", dir, "max_bytes", limit)
	} else {
		log.Warn("read cache paused: its disk is almost full", "dir", dir, "disk_free", avail)
	}
	if !e.isBackground {
		e.setupPrefetch(cfg, dir, log)
	}
	return nil
}

// cacheStatements configure the loaded extension, caching if on. The
// settings that keep cached state from going stale come first, so
// cacheOffStatements can rely on them.
func cacheStatements(dir, warehouseRoot string, reserve uint64, on bool) []string {
	cacheType := "noop"
	if on {
		cacheType = "on_disk"
	}
	return append(cacheOffStatements()[1:],
		"SET GLOBAL cache_httpfs_enable_cache_validation = true",
		"SET GLOBAL cache_httpfs_cache_directory = "+literal(dir),
		"SET GLOBAL cache_httpfs_evict_policy = 'lru_sp'",
		fmt.Sprintf("SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = %d", reserve),
		// The extension's own memory cache reads its files with direct
		// I/O; DuckDB's external file cache, which the extension turns
		// off, does that job better and is what Igloo used before.
		"SET GLOBAL cache_httpfs_disk_cache_reader_enable_memory_cache = false",
		"SET GLOBAL enable_external_file_cache = true",
		// Cache only the warehouse: other allowed locations hold files
		// that are read once, such as data to load, and so do the rows
		// distributed-execution workers exchange.
		"SELECT cache_httpfs_add_exclusion_regex("+literal("^(?!"+regexp.QuoteMeta(warehouseRoot)+")")+")",
		"SELECT cache_httpfs_add_exclusion_regex("+literal("^"+regexp.QuoteMeta(warehouseRoot+config.ShuffleDir+"/"))+")",
		// Remove blocks a crash left half written.
		"SELECT cache_httpfs_cleanup_dead_temp()",
		"SET GLOBAL cache_httpfs_type = "+literal(cacheType),
	)
}

// cacheOffStatements make the loaded extension behave like plain httpfs.
func cacheOffStatements() []string {
	return []string{
		"SET GLOBAL cache_httpfs_type = 'noop'",
		"SET GLOBAL cache_httpfs_enable_glob_cache = false",
		"SET GLOBAL cache_httpfs_enable_metadata_cache = false",
		"SET GLOBAL cache_httpfs_enable_file_handle_cache = false",
	}
}

// cacheLimits returns how large the cache may grow and the free space the
// extension must leave on its disk for that, given the disk's size and free
// space and the bytes already cached. On a disk it shares (not dedicated),
// the cache gets at most half of the space nothing else uses, whatever
// maxSize says, so that a small disk is never filled, as a node's disk
// under an emptyDir must not be. Either way it gets at most maxSize, or
// maxAutoCacheSize without it, and the disk keeps at least 5% free, as the
// extension does by default; when it cannot, the limit is 0.
func cacheLimits(total, avail, used, maxSize uint64, dedicated bool) (limit, reserve uint64) {
	usable := avail + used
	if maxSize == 0 {
		maxSize = maxAutoCacheSize
	}
	limit = maxSize
	if !dedicated {
		limit = min(usable/2, maxSize)
	}
	reserve = max(total/20, 1)
	if usable > limit {
		reserve = max(reserve, usable-limit)
	}
	if usable <= reserve {
		return 0, reserve
	}
	return usable - reserve, reserve
}

// start measures the cache in the background until Close.
func (e *Engine) startCache(log *slog.Logger) {
	c := e.cache
	c.stop, c.done = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(c.done)
		t := time.NewTicker(cacheMeasureInterval)
		defer t.Stop()
		for {
			if err := e.measureCache(log); err != nil {
				log.Warn("cannot resize the read cache", "err", err)
			}
			select {
			case <-c.stop:
				return
			case <-t.C:
			}
		}
	}()
}

// stopCache waits for the background measurements to end.
func (e *Engine) stopCache() {
	if c := e.cache; c != nil && c.stop != nil {
		close(c.stop)
		<-c.done
	}
}

// CacheLimit returns what the read cache may grow to, as of its last
// measurement, or 0 without one.
func (e *Engine) CacheLimit() int64 {
	if e.cache == nil {
		return 0
	}
	return int64(e.cache.limit.Load())
}

// measureCache measures the cache and its disk and moves the extension's
// free-space reserve to match.
//
// When the disk is so full that the cache would have no room even empty,
// the extension would only evict a block on every miss, which costs time
// and frees space slowly. Igloo then pauses the cache and empties it, and
// resumes once there is room again.
func (e *Engine) measureCache(log *slog.Logger) error {
	c := e.cache
	used := dirBytes(c.dir)
	total, avail, err := diskSpace(c.dir)
	if err != nil {
		return err
	}
	limit, reserve := cacheLimits(total, avail, used, c.maxSize, c.dedicated)
	c.size.Store(used)
	c.limit.Store(limit)
	log.Debug("measured the read cache", "bytes", used, "max_bytes", limit, "disk_free", avail)
	on := limit > 0
	stmts := c.adjustments(reserve, on, used)
	if len(stmts) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := e.connector.Connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	for _, stmt := range stmts {
		if _, err := conn.(*duckdb.Conn).ExecContext(ctx, stmt, nil); err != nil {
			return err
		}
	}
	c.reserve = reserve
	switch {
	case on && !c.on:
		log.Info("read cache resumed", "max_bytes", limit)
	case !on && (c.on || used > 0):
		c.size.Store(0)
		log.Warn("read cache paused and emptied: its disk is almost full", "dir", c.dir, "disk_free", avail)
	}
	c.on = on
	c.floor.Store(reserve)
	c.caching.Store(on)
	return nil
}

// room is how many more bytes the cache may take before cache_httpfs
// evicts a block on each miss, for the prefetcher: what the last
// measurement left it, and at most what the disk has free now beyond the
// free space the extension keeps. The measurement is minutes old, and in
// between the disk fills, with the cache's blocks and with what else is on
// it, as the spill files and exchanges of a worker sharing it: a worker
// started with an empty disk read ahead as if the cache were still empty
// for a minute after it was full, while cache_httpfs evicted the blocks
// read ahead as they landed and failed a read of one (see package
// prefetch's errUnversioned).
func (c *readCache) room() int64 {
	limit, size := c.limit.Load(), c.size.Load()
	if !c.caching.Load() || size >= limit {
		return 0
	}
	room := int64(limit - size)
	if _, avail, err := diskSpace(c.dir); err == nil {
		room = min(room, int64(avail)-int64(c.floor.Load()))
	}
	return max(room, 0)
}

// adjustments are the statements that move the extension from its current
// reserve and state to the given ones; used is the bytes cached now.
func (c *readCache) adjustments(reserve uint64, on bool, used uint64) []string {
	var stmts []string
	if reserve != c.reserve {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL cache_httpfs_min_disk_bytes_for_cache = %d", reserve))
	}
	switch {
	case on && !c.on:
		stmts = append(stmts, "SET GLOBAL cache_httpfs_type = 'on_disk'")
	case !on && (c.on || used > 0):
		stmts = append(stmts, "SET GLOBAL cache_httpfs_type = 'noop'", "SELECT cache_httpfs_clear_cache()")
	}
	return stmts
}

// Metrics are the read cache's Prometheus metrics. They read zero while the
// cache is off.
func (e *Engine) Metrics() []prometheus.Collector {
	var extra []prometheus.Collector
	if e.prefetcher != nil {
		extra = e.prefetcher.Collectors()
	}
	gauge := func(name, help string, v func(*readCache) *atomic.Uint64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, func() float64 {
			if e.cache == nil {
				return 0
			}
			return float64(v(e.cache).Load())
		})
	}
	return append([]prometheus.Collector{
		gauge("igloo_cache_size_bytes",
			"Bytes of object storage data cached on local disk, measured every 5 minutes.",
			func(c *readCache) *atomic.Uint64 { return &c.size }),
		gauge("igloo_cache_limit_bytes",
			"Size the local disk cache may grow to, given the disk's free space, measured every 5 minutes.",
			func(c *readCache) *atomic.Uint64 { return &c.limit }),
	}, extra...)
}

// checkWritable creates dir if needed and makes sure files can be written
// in it. The probe file has the suffix of the extension's own temporary
// files, which it skips and cleans up should the probe outlive Igloo.
func checkWritable(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "igloo-probe-*.httpfs_local_cache")
	if err != nil {
		return err
	}
	return errors.Join(f.Close(), os.Remove(f.Name()))
}

// dirBytes is the total size of the regular files in dir.
func dirBytes(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var n uint64
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		// Files the extension evicts meanwhile are skipped.
		if info, err := entry.Info(); err == nil {
			n += uint64(info.Size())
		}
	}
	return n
}
