package engine

// What DuckDB holds of table files in memory, kept across restarts.
//
// DuckDB keeps the ranges of table files that queries read in its external
// file cache, in its own memory, so repeated reads of a column are memory
// reads. A machine that stops loses them, and the first queries after a
// start read every range again from the read cache on disk, as fast as the
// disk is provisioned for: TPC-H at scale 100 reads 25 GB, which a gp3 disk
// of 500 MB/s gives in 50 s, most of a first run whose warm one takes 15 s.
// Object storage gives the same bytes several times faster to many requests
// at once (2.2 GB/s to one c7g.16xlarge), and the disk is free meanwhile for
// whatever the first queries read that the last run did not.
//
// So the server keeps a manifest of the cache's ranges (the rb extension's
// igloo_file_cache_ranges) beside the read cache, in the order queries first
// read them, written every fileCacheSaveEvery while they change and when it
// stops. After a start it reads them back from object storage in the
// background, in that order, a few large requests per file, and hands them
// to DuckDB (igloo_file_cache_put) under the version DuckDB had for the file:
// none for Iceberg's data files, which never change and which DuckDB does
// not check, and for other files the ETag or modification time DuckDB
// checks when a query opens one. A query that reads a range before it is
// back reads it as it would have, and the restore skips it. The restore
// takes at most restoreShare of DuckDB's memory, as the ranges are buffers
// DuckDB drops first when queries need the room, and stops early when DuckDB
// has none to spare. Nothing is saved until it ends, so a stop in the middle
// keeps the manifest it started from.
//
// Emptying the cache (EmptyCache, as erasure requests do) stops a restore
// under way and deletes the manifest, so no range DuckDB dropped comes back.

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/lockorder"
	"github.com/akkio-inc/igloo/internal/prefetch"
)

const (
	// fileCacheManifest is the manifest's name, in IGLOO_CACHE_DIR beside
	// the extension's directory of blocks.
	fileCacheManifest = "memory.json"
	// fileCacheSaveEvery is how often the manifest is written while the
	// cache changes, and fileCacheLookEvery how often it is listed then,
	// to know which ranges queries read first.
	fileCacheSaveEvery = 30 * time.Second
	fileCacheLookEvery = 2 * time.Second
	// restoreShare bounds a restore to this share of DuckDB's memory.
	restoreShare = 0.75
	// restoreRequest is the most a request of a restore reads, of ranges of
	// one file, and restoreGap the most bytes between two ranges it reads
	// across rather than make two requests: TPC-H's 24 GB at scale 100 are
	// 54,000 ranges, a median of 200 KB, which this makes 6,900 requests.
	restoreRequest = 8 << 20
	restoreGap     = 1 << 20
	// restoreConcurrency is how many requests a restore keeps in flight. A
	// c7g.16xlarge restored those 24 GB in 16.7 s with 64 requests of up
	// to 16 MiB in flight, and in 7.3 s, at its network's 3.6 GB/s, with
	// 256 of up to 8 MiB.
	restoreConcurrency = 256
)

// manifest is what DuckDB held of table files: the files, with their
// version as DuckDB knew it, and their ranges, in the order queries first
// read them.
type manifest struct {
	Files []manifestFile `json:"files"`
	// Ranges are the file's index in Files, offset and length.
	Ranges [][3]int64 `json:"ranges"`
}

type manifestFile struct {
	Path string `json:"path"`
	// Version is the file's version tag (an object's ETag; bytes of its
	// metadata for a local file), Modified its modification time in
	// microseconds, as DuckDB had them; both are empty for files DuckDB
	// keeps without checking them.
	Version  []byte `json:"version,omitempty"`
	Modified int64  `json:"modified"`
	Size     int64  `json:"size"`
}

// cachedRange is a range of DuckDB's cache, as listed.
type cachedRange struct {
	file           manifestFile
	offset, length int64
}

type rangeKey struct {
	path   string
	offset int64
}

// fileCache is the state of saving and restoring the manifest.
type fileCache struct {
	path string
	open func(ctx context.Context, path string) (prefetch.Object, error)

	mu lockorder.Mutex
	// cancel stops a restore under way, and running waits for it.
	cancel  context.CancelFunc
	running sync.WaitGroup
	// first numbers the ranges in the order they were first seen, from
	// the manifest's and then the listings'; next is the next number.
	first map[rangeKey]int64
	next  int64
	// memory is DuckDB's memory in its external file cache at the last
	// listing, which says whether to list again.
	memory int64
	// saved is the manifest's last contents, so an unchanged cache is not
	// written again.
	saved []byte
	// ready says the restore has ended, and saves may replace the manifest.
	ready atomic.Bool
	// gen counts forget's calls: a save whose listing began before one
	// writes nothing.
	gen atomic.Uint64

	stop, done chan struct{}
}

// startFileCache restores the manifest in the background, then follows
// DuckDB's cache and saves the manifest until Close. It needs the read
// cache's directory for the manifest, the prefetcher's access to object
// storage, and the rb extension's functions.
func (e *Engine) startFileCache(dir string, open func(ctx context.Context, path string) (prefetch.Object, error), log *slog.Logger) {
	f := &fileCache{path: filepath.Join(dir, fileCacheManifest), open: open, first: map[rangeKey]int64{},
		stop: make(chan struct{}), done: make(chan struct{})}
	e.fileCache = f
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.running.Add(1)
	go func() {
		defer close(f.done)
		func() {
			defer f.running.Done()
			e.restoreFileCache(ctx, log)
			if ctx.Err() == nil {
				f.ready.Store(true)
			}
		}()
		look := time.NewTicker(fileCacheLookEvery)
		defer look.Stop()
		lastSave := time.Now()
		for {
			select {
			case <-f.stop:
				return
			case <-look.C:
			}
			if err := e.lookAtFileCache(context.Background()); err != nil {
				log.Warn("could not list what DuckDB holds in memory", "err", err)
				continue
			}
			if time.Since(lastSave) >= fileCacheSaveEvery {
				lastSave = time.Now()
				if err := e.saveFileCache(context.Background()); err != nil {
					log.Warn("could not save the list of what DuckDB holds in memory", "err", err)
				}
			}
		}
	}()
}

// stopFileCache stops following DuckDB's cache and a restore under way, and
// saves the manifest once more.
func (e *Engine) stopFileCache(log *slog.Logger) {
	f := e.fileCache
	if f == nil {
		return
	}
	f.stopRestore()
	close(f.stop)
	<-f.done
	if err := e.saveFileCache(context.Background()); err != nil {
		log.Warn("could not save the list of what DuckDB holds in memory", "err", err)
	}
}

// stopRestore cancels a restore under way and waits for it to end.
func (f *fileCache) stopRestore() {
	f.mu.Lock()
	cancel := f.cancel
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	f.running.Wait()
}

// forgetFileCache deletes the manifest, so what it lists is not restored,
// and has a save that listed DuckDB's cache before it write nothing.
func (e *Engine) forgetFileCache() error {
	if e.fileCache == nil {
		return nil
	}
	if err := e.fileCache.forget(); err != nil {
		return fmt.Errorf("delete the list of what DuckDB holds in memory: %w", err)
	}
	return nil
}

func (f *fileCache) forget() error {
	f.stopRestore()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen.Add(1)
	f.first, f.next, f.memory, f.saved = map[rangeKey]int64{}, 0, 0, nil
	f.ready.Store(true)
	if err := os.Remove(f.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// lookAtFileCache numbers the ranges DuckDB's cache holds that it has not
// seen, when the cache's memory changed since it last looked.
func (e *Engine) lookAtFileCache(ctx context.Context) error {
	f := e.fileCache
	memory, err := e.fileCacheMemory(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	same := memory == f.memory
	f.mu.Unlock()
	if same {
		return nil
	}
	gen := f.gen.Load()
	ranges, err := e.fileCacheRanges(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen.Load() != gen {
		return nil
	}
	f.memory = memory
	f.number(ranges)
	return nil
}

// number gives the ranges not yet numbered the next numbers, in the order
// of the listing. Called with f.mu held.
func (f *fileCache) number(ranges []cachedRange) {
	for _, r := range ranges {
		k := rangeKey{r.file.Path, r.offset}
		if _, ok := f.first[k]; !ok {
			f.first[k] = f.next
			f.next++
		}
	}
}

// saveFileCache writes the manifest of what DuckDB holds now, when that
// changed since the last write.
func (e *Engine) saveFileCache(ctx context.Context) error {
	f := e.fileCache
	if !f.ready.Load() {
		return nil
	}
	gen := f.gen.Load()
	ranges, err := e.fileCacheRanges(ctx)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen.Load() != gen {
		return nil
	}
	f.number(ranges)
	data, err := json.Marshal(f.manifestOf(ranges))
	if err != nil {
		return err
	}
	if slices.Equal(data, f.saved) {
		return nil
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	f.saved = data
	return nil
}

// manifestOf is the manifest of ranges, in the order they were first seen.
// Called with f.mu held, every range numbered.
func (f *fileCache) manifestOf(ranges []cachedRange) manifest {
	ranges = slices.Clone(ranges)
	sort.SliceStable(ranges, func(a, b int) bool {
		return f.first[rangeKey{ranges[a].file.Path, ranges[a].offset}] < f.first[rangeKey{ranges[b].file.Path, ranges[b].offset}]
	})
	var m manifest
	index := map[string]int{}
	for _, r := range ranges {
		i, ok := index[r.file.Path]
		if !ok {
			i = len(m.Files)
			index[r.file.Path] = i
			m.Files = append(m.Files, r.file)
		}
		m.Ranges = append(m.Ranges, [3]int64{int64(i), r.offset, r.length})
	}
	return m
}

// fileCacheMemory is DuckDB's memory in its external file cache.
func (e *Engine) fileCacheMemory(ctx context.Context) (int64, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Close() //nolint:errcheck
	v, err := queryString(ctx, c.(*duckdb.Conn),
		"SELECT coalesce(sum(memory_usage_bytes), 0)::VARCHAR FROM duckdb_memory() WHERE tag = 'EXTERNAL_FILE_CACHE'")
	if err != nil {
		return 0, err
	}
	var n int64
	_, err = fmt.Sscan(v, &n)
	return n, err
}

// fileCacheRanges lists DuckDB's external file cache.
func (e *Engine) fileCacheRanges(ctx context.Context) ([]cachedRange, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close() //nolint:errcheck
	rows, err := c.(*duckdb.Conn).QueryContext(ctx,
		"SELECT path, location, nr_bytes, version_tag, last_modified, file_size FROM igloo_file_cache_ranges()", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []cachedRange
	dest := make([]driver.Value, 6)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		path, _ := dest[0].(string)
		offset, _ := dest[1].(uint64)
		length, _ := dest[2].(uint64)
		version, _ := dest[3].([]byte)
		modified, _ := dest[4].(int64)
		size, _ := dest[5].(uint64)
		out = append(out, cachedRange{file: manifestFile{Path: path, Version: version, Modified: modified, Size: int64(size)},
			offset: int64(offset), length: int64(length)})
	}
}

// readManifest reads the manifest, or returns an empty one when there is
// none.
func (f *fileCache) readManifest() (manifest, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return manifest{}, nil
	}
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, fmt.Errorf("%s: %w", f.path, err)
	}
	for _, r := range m.Ranges {
		if r[0] < 0 || r[0] >= int64(len(m.Files)) {
			return manifest{}, fmt.Errorf("%s: a range of file %d of %d", f.path, r[0], len(m.Files))
		}
	}
	return m, nil
}

// restoreRead is one request of a restore: ranges of one file that lie
// close together, read at once.
type restoreRead struct {
	file       *manifestFile
	start, end int64
	// ranges are offset and length.
	ranges [][2]int64
	// first is where the earliest of the ranges is in the manifest.
	first int
}

// restoreReads groups each file's ranges into requests (see restoreRequest
// and restoreGap) and orders them by the earliest range each holds, as many
// as budget bytes of ranges allow, taken in the manifest's order.
func restoreReads(m *manifest, budget int64) []restoreRead {
	type item struct {
		offset, length int64
		order          int
	}
	byFile := make([][]item, len(m.Files))
	var total int64
	for i, r := range m.Ranges {
		f := &m.Files[r[0]]
		if r[2] <= 0 || r[1] < 0 || r[1]+r[2] > f.Size {
			continue
		}
		if total+r[2] > budget {
			break
		}
		total += r[2]
		byFile[r[0]] = append(byFile[r[0]], item{r[1], r[2], i})
	}
	var reads []restoreRead
	for fi, items := range byFile {
		sort.Slice(items, func(a, b int) bool { return items[a].offset < items[b].offset })
		var cur *restoreRead
		for _, it := range items {
			start, end := it.offset, it.offset+it.length
			if cur != nil && start-cur.end <= restoreGap && max(end, cur.end)-cur.start <= restoreRequest {
				cur.end = max(cur.end, end)
				cur.ranges = append(cur.ranges, [2]int64{start, it.length})
				cur.first = min(cur.first, it.order)
				continue
			}
			reads = append(reads, restoreRead{file: &m.Files[fi], start: start, end: end,
				ranges: [][2]int64{{start, it.length}}, first: it.order})
			cur = &reads[len(reads)-1]
		}
	}
	sort.SliceStable(reads, func(a, b int) bool { return reads[a].first < reads[b].first })
	return reads
}

// restoreFileCache reads the manifest's ranges back into DuckDB's memory
// (see above).
func (e *Engine) restoreFileCache(ctx context.Context, log *slog.Logger) {
	f := e.fileCache
	m, err := f.readManifest()
	if err != nil {
		log.Warn("could not read the list of what DuckDB held in memory; starting without it", "err", err)
		return
	}
	if len(m.Ranges) == 0 {
		return
	}
	// Ranges keep their places in the order across the restart.
	f.mu.Lock()
	for _, r := range m.Ranges {
		k := rangeKey{m.Files[r[0]].Path, r[1]}
		if _, ok := f.first[k]; !ok {
			f.first[k] = f.next
			f.next++
		}
	}
	f.mu.Unlock()
	reads := restoreReads(&m, int64(float64(e.MemoryLimit(ctx))*restoreShare))
	if len(reads) == 0 {
		return
	}
	began := time.Now()
	var restored, skipped, failed atomic.Int64
	// full says DuckDB had no memory to spare for a range: the rest would
	// only evict what queries use.
	var full atomic.Bool
	var opens sync.Map // path -> *restoreObject
	work := make(chan restoreRead)
	var wg sync.WaitGroup
	for range restoreConcurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := e.connector.Connect(ctx)
			if err != nil {
				for range work {
				}
				return
			}
			defer c.Close() //nolint:errcheck
			conn := c.(*duckdb.Conn)
			for r := range work {
				if ctx.Err() != nil || full.Load() {
					continue
				}
				obj, err := openOnce(ctx, &opens, r.file.Path, f.open).get()
				if err != nil || (len(r.file.Version) > 0 && !sameETag(obj.ETag(), string(r.file.Version))) {
					skipped.Add(int64(len(r.ranges)))
					continue
				}
				buf := make([]byte, r.end-r.start)
				if _, err := obj.ReadAt(buf, r.start); err != nil && !(errors.Is(err, io.EOF) && r.end == r.file.Size) {
					failed.Add(int64(len(r.ranges)))
					continue
				}
				for _, rg := range r.ranges {
					status, err := putRange(ctx, conn, r.file, rg[0], buf[rg[0]-r.start:rg[0]-r.start+rg[1]])
					switch {
					case err != nil:
						failed.Add(1)
					case status == putDone:
						restored.Add(rg[1])
					case status == putNoMemory:
						full.Store(true)
					default:
						skipped.Add(1)
					}
				}
			}
		}()
	}
	for _, r := range reads {
		if ctx.Err() != nil || full.Load() {
			break
		}
		select {
		case work <- r:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	log.Info("restored what DuckDB held in memory from object storage", "bytes", restored.Load(), "requests", len(reads),
		"took", time.Since(began).Round(time.Millisecond), "ranges_skipped", skipped.Load(), "ranges_failed", failed.Load(),
		"memory_full", full.Load())
}

// restoreObject opens a file once for every request of a restore.
type restoreObject struct {
	once sync.Once
	obj  prefetch.Object
	err  error
	ctx  context.Context
	path string
	open func(ctx context.Context, path string) (prefetch.Object, error)
}

func openOnce(ctx context.Context, opens *sync.Map, path string, open func(context.Context, string) (prefetch.Object, error)) *restoreObject {
	v, _ := opens.LoadOrStore(path, &restoreObject{ctx: ctx, path: path, open: open})
	return v.(*restoreObject)
}

func (o *restoreObject) get() (prefetch.Object, error) {
	o.once.Do(func() { o.obj, o.err = o.open(o.ctx, o.path) })
	return o.obj, o.err
}

// sameETag compares ETags with or without their quotes.
func sameETag(a, b string) bool {
	return a != "" && strings.Trim(a, `"`) == strings.Trim(b, `"`)
}

// What igloo_file_cache_put returns: the range is DuckDB's now, it was
// skipped (DuckDB holds it, or holds the file under another version), or
// DuckDB had no memory to spare.
const (
	putSkipped  = 0
	putDone     = 1
	putNoMemory = 2
)

// putRange hands DuckDB the bytes of a range of f.
func putRange(ctx context.Context, conn *duckdb.Conn, f *manifestFile, offset int64, data []byte) (int32, error) {
	// No version is an empty one, not NULL.
	version := f.Version
	if version == nil {
		version = []byte{}
	}
	rows, err := conn.QueryContext(ctx, "SELECT igloo_file_cache_put($1, $2, $3, $4, $5, $6)", []driver.NamedValue{
		{Ordinal: 1, Value: f.Path}, {Ordinal: 2, Value: uint64(offset)}, {Ordinal: 3, Value: data},
		{Ordinal: 4, Value: version}, {Ordinal: 5, Value: f.Modified}, {Ordinal: 6, Value: uint64(f.Size)},
	})
	if err != nil {
		return 0, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, err
	}
	status, _ := dest[0].(int32)
	return status, nil
}
