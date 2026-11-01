// Package prefetch reads ahead, into the read cache, the blocks of the data
// files DuckDB's scans of Iceberg tables are about to read, with a
// concurrency of its own.
//
// Reading object storage cold waits on requests: a DuckDB thread reads a
// row group's column chunks a request round after another, so how many
// requests are in flight follows DuckDB's threads, which Igloo caps by
// memory (see engine.threadsFor). The prefetcher keeps many more requests
// in flight, up to Options.Concurrency for the whole process, and writes
// each block it fetches into the directory of the read cache, the
// cache_httpfs extension's, as the extension names and marks its own
// blocks: a file sha256(path)-<base name>-<offset>-<length> holding a
// block-aligned range, with the path in the extended attribute
// user.cache_httpfs_remote_path.000. DuckDB then reads the block from local
// disk as a cache hit. A block that is not there yet, or one the prefetcher
// fails to fetch, DuckDB fetches itself, so prefetching only ever costs
// requests, never correctness: the blocks are ranges of Iceberg data files,
// which never change.
//
// For each file of a scan it fetches the footer first (which DuckDB reads
// too), and then the blocks of the column chunks of the scan's columns, in
// the order of the scan's files. It keeps at most a share of the cache's
// room for blocks DuckDB has not read yet, so that prefetched blocks do not
// evict each other, or other statements' blocks, before DuckDB reads them,
// and none once the cache has no room left, where cache_httpfs evicts a
// block on each of DuckDB's misses. It writes only blocks of files whose
// ETag it knows, which it marks them with as cache_httpfs does.
// It writes a block with an access time an hour before its modification
// time and tells one DuckDB read by its access time catching up: a read
// moves it past the modification time, and cache_httpfs, which keeps its
// blocks in least-recently-used order, sets both to the time of the read.
// On a file system mounted noatime, which keeps
// access times still, it tells one by the kernel's file events instead
// (inotify, on Linux), as cache_httpfs opens a block's file to read it.
// Where neither works, it cannot tell, and each statement reads ahead at
// most boundedAhead, and never more than its share of the cache's free
// room, so that reading ahead evicts nothing. Blocks it fetched that DuckDB
// never read count as overfetch.
package prefetch

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"
)

const (
	// DefaultConcurrency is how many requests the prefetcher keeps in
	// flight, for every statement of the process together.
	DefaultConcurrency = 256
	// DefaultBlockSize is cache_httpfs's block size, which blocks must have
	// for the extension to find them.
	DefaultBlockSize = 512 << 10
	// pathAttr is the extended attribute cache_httpfs keeps a block's remote
	// path in; it discards a block without it.
	pathAttr = "user.cache_httpfs_remote_path.000"
	// tmpSuffix marks a block being written, renamed into place when whole.
	tmpSuffix = ".igloo-prefetch"
	// roomShare is the share of the cache's room the blocks DuckDB has not
	// read yet may take, for every statement together.
	roomShare = 4
	// checkReads is how often a statement looks at which of its blocks
	// DuckDB has read.
	checkReads = 100 * time.Millisecond
	// footerGuess is how much of a file's end the prefetcher reads for its
	// footer at first.
	footerGuess = 64 << 10
	// versionAttr is the extended attribute cache_httpfs keeps a block's
	// object version (its ETag) in.
	versionAttr = "user.cache_version"
)

// minAhead is what a statement may keep ahead of its scans however little
// room the cache reports, as long as it reports some, when the prefetcher
// can tell which blocks DuckDB read.
var minAhead int64 = 64 << 20

// boundedAhead is the most a statement reads ahead, in all, when the
// prefetcher cannot tell which blocks DuckDB read.
var boundedAhead int64 = 256 << 20

// tracking is how the prefetcher tells which blocks DuckDB read.
type tracking int

const (
	// byAccessTime reads blocks' access times.
	byAccessTime tracking = iota
	// byOpens watches the kernel's events of opens of blocks' files, as
	// on a file system mounted noatime.
	byOpens
	// bounded cannot tell: a statement reads ahead boundedAhead at most.
	bounded
)

// Object reads ranges of a file in object storage. ReadAt may be called
// from several goroutines at once.
type Object interface {
	io.ReaderAt
	io.Closer
	// ETag is the object's entity tag as object storage reports it,
	// quotes included, which cache_httpfs checks a block against when
	// DuckDB knows the file's: a block without it is then refetched.
	ETag() string
}

// Options configure a Prefetcher.
type Options struct {
	// Dir is the read cache's directory, which cache_httpfs reads blocks
	// from.
	Dir string
	// BlockSize is cache_httpfs's block size; 0 means DefaultBlockSize.
	BlockSize int64
	// Concurrency is how many requests the process keeps in flight; 0
	// means DefaultConcurrency.
	Concurrency int
	// Open opens a data file, by its path as Iceberg's manifests have it,
	// for range reads.
	Open func(ctx context.Context, path string) (Object, error)
	// Room returns how many more bytes the read cache may hold before it
	// evicts blocks, as of now: cache_httpfs evicts as soon as its disk is
	// short of the free space it keeps. Nil means unbounded.
	Room func() int64
	Log  *slog.Logger
}

// File is a data file a scan reads.
type File struct {
	// Path is the file's path as Iceberg's manifests have it, which DuckDB
	// passes to the read cache.
	Path string
	Size int64
}

// Scan is a scan of an Iceberg table: the files it reads, in the order it
// reads them, and its columns, by Iceberg field ID (top-level or leaf);
// none means it reads no column, only the files' footers, as COUNT(*)
// does. Whole says it reads every column, as of the files of a distributed
// statement's exchange, which have no field IDs. Filtered says the
// statement filters the table, by a WHERE clause or a join DuckDB filters
// the scan by, on the columns Filters names (field IDs, top-level or leaf;
// none when not known), so the scan may skip files and row groups: the
// prefetcher reads ahead only the footer of a file whose row groups such
// filters could skip (see skippable), and the rest of a file's blocks
// otherwise.
type Scan struct {
	Files    []File
	FieldIDs []int32
	Whole    bool
	Filtered bool
	Filters  []int32
}

// Prefetcher reads ahead for statements' scans.
type Prefetcher struct {
	opts  Options
	track tracking
	// watch tells blocks read when track is byOpens.
	watch *openWatch

	mu     sync.Mutex
	cond   *sync.Cond
	active []*statement
	next   int
	closed bool
	wg     sync.WaitGroup
	// writing is held, shared, by every fetch, and whole by Hold.
	writing sync.RWMutex

	fetched, unread, present, failed prometheus.Counter
	blocks                           prometheus.Counter
}

// New starts a prefetcher. It writes a block into dir as a probe: when the
// directory's file system does not keep extended attributes, without which
// cache_httpfs would discard every block, it returns an error and no
// prefetcher.
func New(opts Options) (*Prefetcher, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = DefaultBlockSize
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	atime, err := probe(opts.Dir)
	if err != nil {
		return nil, err
	}
	// Blocks a crash left half written, each in a file of its own (see
	// writeBlock).
	if left, err := filepath.Glob(filepath.Join(opts.Dir, "*"+tmpSuffix)); err == nil {
		for _, f := range left {
			_ = os.Remove(f)
		}
	}
	p := &Prefetcher{opts: opts,
		fetched: prometheus.NewCounter(prometheus.CounterOpts{Name: "igloo_prefetch_bytes_total",
			Help: "Bytes of data files the prefetcher read ahead into the read cache."}),
		blocks: prometheus.NewCounter(prometheus.CounterOpts{Name: "igloo_prefetch_blocks_total",
			Help: "Blocks of data files the prefetcher read ahead into the read cache."}),
		unread: prometheus.NewCounter(prometheus.CounterOpts{Name: "igloo_prefetch_unread_bytes_total",
			Help: "Bytes the prefetcher read ahead that the statement's scans did not read (overfetch)."}),
		present: prometheus.NewCounter(prometheus.CounterOpts{Name: "igloo_prefetch_present_blocks_total",
			Help: "Blocks the prefetcher found in the read cache already, which it did not fetch."}),
		failed: prometheus.NewCounter(prometheus.CounterOpts{Name: "igloo_prefetch_errors_total",
			Help: "Reads ahead that failed, which the scans then made themselves."}),
	}
	p.cond = sync.NewCond(&p.mu)
	for range opts.Concurrency {
		p.wg.Add(1)
		go p.work()
	}
	if !atime {
		if p.watch, err = newOpenWatch(opts.Dir); err == nil {
			p.track = byOpens
			opts.Log.Info("the read cache's file system keeps no access times (noatime): prefetch tells which blocks DuckDB read by the file events of their opens")
		} else {
			p.track = bounded
			opts.Log.Warn("the read cache's file system keeps no access times (noatime), and file events are not available: "+
				"prefetch cannot tell which blocks DuckDB read, so each statement reads ahead at most a bounded amount, "+
				"within the cache's free room, and overfetch is not counted", "bound", boundedAhead, "err", err)
		}
	}
	return p, nil
}

// Collectors returns the prefetcher's metrics.
func (p *Prefetcher) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.fetched, p.blocks, p.unread, p.present, p.failed}
}

// Close stops the prefetcher's workers.
func (p *Prefetcher) Close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
	if p.watch != nil {
		p.watch.close()
	}
}

// readSince reports whether DuckDB read the block file name since the
// prefetcher wrote it; ok is false when it is gone, or the prefetcher cannot
// tell (see tracking).
func (p *Prefetcher) readSince(name string) (read, ok bool) {
	switch p.track {
	case byAccessTime:
		return readSince(filepath.Join(p.opts.Dir, name))
	case byOpens:
		return p.watch.readSince(name)
	}
	return false, false
}

// forget stops tracking the block file name.
func (p *Prefetcher) forget(name string) {
	if p.track == byOpens {
		p.watch.untrack(name)
	}
}

// probe writes a block with the attribute cache_httpfs needs into dir and
// reports whether the file system moves access times on reads.
func probe(dir string) (bool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	f, err := os.CreateTemp(dir, "igloo-prefetch-probe-*"+tmpSuffix)
	if err != nil {
		return false, err
	}
	name := f.Name()
	defer os.Remove(name) //nolint:errcheck
	_, err = f.Write([]byte("probe"))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return false, err
	}
	if err := unix.Setxattr(name, pathAttr, []byte("probe"), 0); err != nil {
		return false, fmt.Errorf("the read cache's file system keeps no extended attributes, which cache_httpfs needs on a block: %w", err)
	}
	// An access time older than the file's modification time moves on a
	// read under relatime and strictatime, but not under noatime.
	now := time.Now()
	if err := os.Chtimes(name, now.Add(-time.Hour), now.Add(-time.Minute)); err != nil {
		return false, nil
	}
	if b, err := os.ReadFile(name); err != nil || len(b) == 0 {
		return false, nil
	}
	read, ok := readSince(name)
	return ok && read, nil
}

// readSince reports whether the file was read since the prefetcher wrote
// it, an hour before its modification time (see writeBlock): its access
// time is no longer far behind. A read under relatime or strictatime moves
// the access time past the modification time; cache_httpfs, reading a
// block, sets both to the time of the read, which leaves the access time
// no later than the modification time. ok is false when the file is gone.
func readSince(name string) (read, ok bool) {
	fi, err := os.Stat(name)
	if err != nil {
		return false, false
	}
	a, known := accessTime(fi)
	return known && fi.ModTime().Sub(a) < readLag, true
}

// readLag is how far a block's access time may stay behind its
// modification time once read; writeBlock puts it an hour behind.
const readLag = 30 * time.Minute

// BlockName is the name cache_httpfs gives the file of the block at offset
// of length bytes of the file at path.
func BlockName(path string, offset, length int64) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:]) + "-" + filepath.Base(path) + "-" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(length, 10)
}

// blockPath is a block's file (see BlockName) as DuckDB's errors quote it.
var blockPath = regexp.MustCompile(`/[0-9a-f]{64}-[^/"]*-[0-9]+-[0-9]+"`)

// BlockRace reports whether err is DuckDB's I/O error over a block's file
// in the read cache: the file changed while cache_httpfs read it. Two of
// its steps race others over one block: it deletes a block it finds stale
// by checking that the file exists and then removing it, which fails when
// another read or its eviction removed it in between ("Could not remove
// file"), and it reads a block in full, which fails on one being rewritten
// ("Could not read enough bytes"). The block is a copy of a range of a
// data file in object storage, so the same read a moment later succeeds:
// the caller runs it again rather than fail the statement.
func BlockRace(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "IO Error") && blockPath.MatchString(msg)
}

// block is a block of a file to fetch.
type block struct {
	file   *openFile
	offset int64
	length int64
	name   string
	// done, if set, is told when the block has been fetched or found.
	done chan error
}

// openFile is a file of a statement's scans, opened once.
type openFile struct {
	File
	once sync.Once
	r    Object
	err  error
}

// statement is a statement's reading ahead.
type statement struct {
	p      *Prefetcher
	ctx    context.Context
	cancel context.CancelFunc
	queue  []block
	// ahead are the bytes of the blocks fetched that DuckDB has not read
	// yet, which unread lists, oldest first.
	ahead  int64
	unread []fetchedBlock
	files  []*openFile
	done   chan struct{}
	mu     sync.Mutex
	space  *sync.Cond
}

type fetchedBlock struct {
	name   string
	length int64
}

// Start reads ahead for scans, the scans of a statement about to run,
// until ctx ends or stop is called.
func (p *Prefetcher) Start(ctx context.Context, scans []Scan) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	s := &statement{p: p, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.space = sync.NewCond(&s.mu)
	p.mu.Lock()
	p.active = append(p.active, s)
	p.mu.Unlock()
	go func() {
		defer close(s.done)
		s.run(scans)
	}()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		s.space.Broadcast()
		s.mu.Unlock()
	}()
	return func() {
		cancel()
		<-s.done
		p.mu.Lock()
		p.active = deleteStatement(p.active, s)
		p.mu.Unlock()
		s.finish()
	}
}

func deleteStatement(list []*statement, s *statement) []*statement {
	for i, x := range list {
		if x == s {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// budget is how many bytes a statement may keep ahead of its scans: a
// share of the cache's room, split between the statements reading ahead.
func (s *statement) budget() int64 {
	p := s.p
	p.mu.Lock()
	n := int64(len(p.active))
	p.mu.Unlock()
	if n == 0 {
		n = 1
	}
	if p.track == bounded {
		// What it reads ahead is never credited back, so this bounds
		// all it reads ahead, which stays within the free room so that
		// it evicts nothing.
		if p.opts.Room == nil {
			return boundedAhead
		}
		return min(boundedAhead, p.opts.Room()/roomShare/n)
	}
	if p.opts.Room == nil {
		return 1 << 62
	}
	room := p.opts.Room()
	if room <= 0 {
		// cache_httpfs evicts a block on each of DuckDB's misses: blocks
		// read ahead would only take the place of others, and the churn of
		// blocks written and evicted at once fails DuckDB's reads of them
		// (see errUnversioned).
		return 0
	}
	return max(minAhead, room/roomShare/n)
}

// roomAtAll waits until the statement may read anything ahead, reporting
// false when it ends first: with no room, even a file's footer, which it
// reads through the cache, would take another block's place.
func (s *statement) roomAtAll() bool {
	for s.ctx.Err() == nil {
		if s.budget() > 0 {
			return true
		}
		select {
		case <-s.ctx.Done():
		case <-time.After(checkReads):
		}
	}
	return false
}

func (s *statement) run(scans []Scan) {
	for _, sc := range scans {
		ids := map[int32]bool{}
		for _, id := range sc.FieldIDs {
			ids[id] = true
		}
		var filter *scanFilter
		if sc.Filtered {
			filter = newScanFilter(sc.Filters)
		}
		// A filtered scan's file waits for the next file's footer, which
		// tells whether the files themselves divide the filters' values.
		var held *heldFile
		for _, f := range sc.Files {
			if !s.roomAtAll() {
				return
			}
			of := &openFile{File: f}
			s.files = append(s.files, of)
			meta, ranges, err := s.columnRanges(of, ids, sc.Whole)
			if err != nil {
				if s.ctx.Err() == nil {
					s.p.failed.Inc()
					s.p.opts.Log.Debug("prefetch: cannot read a data file's footer", "path", f.Path, "err", err)
				}
				continue
			}
			if filter == nil {
				if !s.enqueueAll(s.p.blocksOf(of, ranges)) {
					return
				}
				continue
			}
			next := &heldFile{file: of, ranges: ranges, cols: filter.add(meta)}
			if held != nil && !s.release(held, filter) {
				held = nil
				break
			}
			held = next
		}
		if held != nil {
			s.release(held, filter)
		}
		if filter != nil {
			s.p.opts.Log.Debug("prefetch: read ahead a filtered scan", "files", len(sc.Files),
				"whole", filter.whole, "footers_only", filter.footers, "files_divide_values", filter.divided)
		}
	}
}

// heldFile is a file of a filtered scan whose blocks wait for the next
// file's footer: what it reads, and what its footer says of the columns
// the filters name.
type heldFile struct {
	file   *openFile
	ranges [][2]int64
	cols   map[int32]*colRange
}

// release reads ahead the blocks of a filtered scan's file h, unless
// DuckDB may skip row groups of it, whose footer is then all it reads
// ahead. It reports false when the scan should read nothing further ahead:
// the statement ended, or the files divide the filters' values, so DuckDB
// may skip whole files, footers included.
func (s *statement) release(h *heldFile, f *scanFilter) bool {
	skip, files := f.skippable(h.cols)
	if files {
		f.divided = true
		return false
	}
	if skip {
		f.footers++
		return true
	}
	f.whole++
	return s.enqueueAll(s.p.blocksOf(h.file, h.ranges))
}

// enqueueAll queues blocks once there is room for them, reporting false
// when the statement ends first.
func (s *statement) enqueueAll(bs []block) bool {
	for _, b := range bs {
		if !s.waitForRoom(b.length) {
			return false
		}
		s.enqueue(b)
	}
	return true
}

// waitForRoom waits until the statement may keep n more bytes ahead,
// reporting false when it ends first.
func (s *statement) waitForRoom(n int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.ctx.Err() == nil {
		// One block at a time at least, unless reading ahead is bounded or
		// the cache has no room.
		if budget := s.budget(); s.ahead+n <= budget || s.ahead == 0 && s.p.track != bounded && budget > 0 {
			s.ahead += n
			return true
		}
		s.mu.Unlock()
		s.checkReads()
		s.mu.Lock()
		if s.ahead+n <= s.budget() {
			continue
		}
		timer := time.AfterFunc(checkReads, func() {
			s.mu.Lock()
			s.space.Broadcast()
			s.mu.Unlock()
		})
		s.space.Wait()
		timer.Stop()
	}
	return false
}

// checkReads credits the statement with the blocks DuckDB has read: those
// whose access time moved, and those before one it read, which it skipped,
// as a filter on a row group's statistics does, and counts as overfetch.
func (s *statement) checkReads() {
	if s.p.track == bounded {
		return
	}
	s.mu.Lock()
	pending := append([]fetchedBlock(nil), s.unread...)
	s.mu.Unlock()
	last := -1
	read := make([]bool, len(pending))
	for i, b := range pending {
		r, ok := s.p.readSince(b.name)
		read[i] = r || !ok
		if read[i] {
			last = i
		}
	}
	if last < 0 {
		return
	}
	var credit, skipped int64
	for i := 0; i <= last; i++ {
		credit += pending[i].length
		if !read[i] {
			skipped += pending[i].length
		}
		s.p.forget(pending[i].name)
	}
	s.p.unread.Add(float64(skipped))
	s.mu.Lock()
	s.unread = s.unread[last+1:]
	s.ahead -= credit
	s.space.Broadcast()
	s.mu.Unlock()
}

// finish counts the statement's blocks DuckDB did not read as overfetch,
// and closes its files.
func (s *statement) finish() {
	s.checkReads()
	if s.p.track != bounded {
		s.mu.Lock()
		var n int64
		for _, b := range s.unread {
			if r, ok := s.p.readSince(b.name); ok && !r {
				n += b.length
			}
			s.p.forget(b.name)
		}
		s.unread = nil
		s.mu.Unlock()
		s.p.unread.Add(float64(n))
	}
	for _, f := range s.files {
		if f.r != nil {
			_ = f.r.Close()
		}
	}
}

// enqueue queues a block for the workers.
func (s *statement) enqueue(b block) {
	p := s.p
	p.mu.Lock()
	s.queue = append(s.queue, b)
	p.cond.Signal()
	p.mu.Unlock()
}

// work fetches blocks, taking statements' queues in turn, so concurrent
// statements share the requests in flight.
func (p *Prefetcher) work() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		var (
			s *statement
			b block
		)
		for {
			if p.closed {
				p.mu.Unlock()
				return
			}
			for i := range p.active {
				k := (p.next + i) % len(p.active)
				if c := p.active[k]; len(c.queue) > 0 {
					s, b = c, c.queue[0]
					c.queue = c.queue[1:]
					p.next = (k + 1) % len(p.active)
					break
				}
			}
			if s != nil {
				break
			}
			p.cond.Wait()
		}
		p.mu.Unlock()
		p.writing.RLock()
		err := p.fetch(s.ctx, b)
		p.writing.RUnlock()
		if b.done != nil {
			b.done <- err
			continue
		}
		s.mu.Lock()
		if err == nil && s.ctx.Err() != nil {
			// The statement ended, and may have finished its accounts.
			p.forget(b.name)
		} else if err == nil {
			s.unread = append(s.unread, fetchedBlock{b.name, b.length})
		} else {
			s.ahead -= b.length
			s.space.Broadcast()
		}
		s.mu.Unlock()
	}
}

// Hold runs fn once no fetch is in flight, and keeps fetches from writing
// into the cache until it returns: an erasure request empties the cache in
// fn, and a block fetched before it, which may hold what the request
// deleted, must not land after (see package distributed's erasure.go).
func (p *Prefetcher) Hold(fn func() error) error {
	p.writing.Lock()
	defer p.writing.Unlock()
	return fn()
}

// errPresent says a block was in the cache already.
var errPresent = errors.New("in the cache already")

// errUnversioned says a block was not read ahead because object storage
// gave its file no ETag. cache_httpfs, which Igloo has check a block's
// version against the file's ETag as DuckDB opened it, would find such a
// block stale and delete it, by a check that the block exists followed by
// its removal, which fails the read when another read of the same block
// deleted it in between ("Could not remove file").
var errUnversioned = errors.New("its file has no ETag")

// fetch reads a block and writes it into the cache as cache_httpfs would.
func (p *Prefetcher) fetch(ctx context.Context, b block) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	dst := filepath.Join(p.opts.Dir, b.name)
	if _, err := os.Stat(dst); err == nil {
		p.present.Inc()
		return errPresent
	}
	b.file.once.Do(func() { b.file.r, b.file.err = p.opts.Open(ctx, b.file.Path) })
	if b.file.err != nil {
		// An open the statement's end cut short failed for that alone: a
		// worker's task ends, and with it its reading ahead, while opens
		// are in flight.
		if ctx.Err() == nil {
			p.failed.Inc()
		}
		return b.file.err
	}
	if b.file.r.ETag() == "" {
		return errUnversioned
	}
	buf := make([]byte, b.length)
	if _, err := b.file.r.ReadAt(buf, b.offset); err != nil && !(errors.Is(err, io.EOF) && b.offset+b.length == b.file.Size) {
		if ctx.Err() == nil {
			p.failed.Inc()
		}
		return err
	}
	// A block read ahead is watched from before it is in place, so no
	// open of it goes unseen; a footer's, which the prefetcher reads itself,
	// is not (see readThroughCache).
	tracked := p.track == byOpens && b.done == nil
	if tracked {
		p.watch.track(b.name)
	}
	if err := writeBlock(dst, b.file.Path, b.file.r.ETag(), buf); err != nil {
		if tracked {
			p.watch.untrack(b.name)
		}
		if ctx.Err() == nil {
			p.failed.Inc()
		}
		return err
	}
	p.fetched.Add(float64(len(buf)))
	p.blocks.Inc()
	return nil
}

// writeBlock writes a block's file whole and marked, in a file of its own,
// then renames it into place, so cache_httpfs never reads one half written.
// The file is this write's alone: statements, as a worker's tasks over one
// table's files, read the same blocks ahead at once, and two writes of one
// file, the second truncating what the first had renamed into place, left
// DuckDB reading a block short ("Could not read enough bytes"). Its access
// time is an hour before its modification time, which DuckDB's first read
// moves on (see readSince).
func writeBlock(dst, path, etag string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".*"+tmpSuffix)
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(tmp, 0o644)
	}
	if err == nil {
		err = unix.Setxattr(tmp, pathAttr, []byte(path), 0)
	}
	if err == nil && etag != "" {
		err = unix.Setxattr(tmp, versionAttr, []byte(etag), 0)
	}
	if err == nil {
		now := time.Now()
		_ = os.Chtimes(tmp, now.Add(-time.Hour), now)
	}
	if err == nil {
		err = os.Rename(tmp, dst)
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}

// blocksOf returns the blocks that hold ranges of file f, in order, each
// once.
func (p *Prefetcher) blocksOf(f *openFile, ranges [][2]int64) []block {
	bs := p.opts.BlockSize
	seen := map[int64]bool{}
	var keys []int64
	for _, r := range ranges {
		if r[1] <= r[0] {
			continue
		}
		for k := r[0] / bs; k <= (r[1]-1)/bs; k++ {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]block, 0, len(keys))
	for _, k := range keys {
		off := k * bs
		n := min(bs, f.Size-off)
		if n <= 0 {
			continue
		}
		out = append(out, block{file: f, offset: off, length: n, name: BlockName(f.Path, off, n)})
	}
	return out
}

// columnRanges fetches f's footer, through the cache, and returns it and
// the byte ranges of the column chunks of the columns ids names (all
// columns when whole), with the footer's own.
func (s *statement) columnRanges(f *openFile, ids map[int32]bool, whole bool) (*metadata.FileMetaData, [][2]int64, error) {
	if f.Size < 12 {
		return nil, nil, fmt.Errorf("a data file of %d bytes", f.Size)
	}
	tail := min(f.Size, footerGuess)
	data, err := s.readThroughCache(f, f.Size-tail, tail)
	if err != nil {
		return nil, nil, err
	}
	if string(data[len(data)-4:]) != "PAR1" {
		return nil, nil, errors.New("not a Parquet file")
	}
	metaLen := int64(binary.LittleEndian.Uint32(data[len(data)-8:]))
	footer := metaLen + 8
	if footer > f.Size {
		return nil, nil, errors.New("a footer longer than its file")
	}
	if footer > tail {
		if data, err = s.readThroughCache(f, f.Size-footer, footer); err != nil {
			return nil, nil, err
		}
	}
	meta, err := metadata.NewFileMetaData(data[len(data)-int(footer):len(data)-8], nil)
	if err != nil {
		return nil, nil, err
	}
	want := map[int]bool{}
	for i := 0; i < meta.Schema.NumColumns(); i++ {
		if whole || projected(meta.Schema.Column(i).SchemaNode(), ids) {
			want[i] = true
		}
	}
	ranges := [][2]int64{{f.Size - footer, f.Size}}
	for g := 0; g < meta.NumRowGroups(); g++ {
		rg := meta.RowGroup(g)
		for c := 0; c < rg.NumColumns(); c++ {
			if !want[c] {
				continue
			}
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				return nil, nil, err
			}
			start := cc.DataPageOffset()
			if cc.HasDictionaryPage() && cc.DictionaryPageOffset() > 0 && cc.DictionaryPageOffset() < start {
				start = cc.DictionaryPageOffset()
			}
			ranges = append(ranges, [2]int64{start, start + cc.TotalCompressedSize()})
		}
	}
	return meta, ranges, nil
}

// projected reports whether a leaf column, or a column it is part of, is
// one of ids.
func projected(n schema.Node, ids map[int32]bool) bool {
	for ; n != nil && n.Parent() != nil; n = n.Parent() {
		if ids[n.FieldID()] {
			return true
		}
	}
	return false
}

// readThroughCache reads a range of f through the cache: it fetches the
// blocks that hold it, at the front of the statement's queue, and reads
// them from the cache's files, or from f where one is gone.
func (s *statement) readThroughCache(f *openFile, offset, length int64) ([]byte, error) {
	bs := s.p.blocksOf(f, [][2]int64{{offset, offset + length}})
	waits := make([]chan error, len(bs))
	p := s.p
	p.mu.Lock()
	front := make([]block, 0, len(bs)+len(s.queue))
	for i := range bs {
		waits[i] = make(chan error, 1)
		bs[i].done = waits[i]
		front = append(front, bs[i])
	}
	s.queue = append(front, s.queue...)
	p.cond.Broadcast()
	p.mu.Unlock()
	out := make([]byte, 0, length)
	for i, b := range bs {
		select {
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		case err := <-waits[i]:
			if err != nil && !errors.Is(err, errPresent) && !errors.Is(err, errUnversioned) {
				return nil, err
			}
		}
		data, err := os.ReadFile(filepath.Join(p.opts.Dir, b.name))
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) || err == nil && int64(len(data)) != b.length {
			// Evicted meanwhile, or not whole: read it again, straight. A
			// block read short, as two writes of one file once left one,
			// took the worker down here.
			b.file.once.Do(func() { b.file.r, b.file.err = p.opts.Open(s.ctx, b.file.Path) })
			if b.file.err != nil {
				return nil, b.file.err
			}
			data = make([]byte, b.length)
			if _, err := b.file.r.ReadAt(data, b.offset); err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		lo, hi := max(offset, b.offset)-b.offset, min(offset+length, b.offset+b.length)-b.offset
		out = append(out, data[lo:hi]...)
	}
	return out, nil
}
