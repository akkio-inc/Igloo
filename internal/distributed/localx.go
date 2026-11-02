package distributed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"golang.org/x/sync/errgroup"
)

// Exchanges on workers' disks. With IGLOO_EXCHANGES=local, a shuffle task
// writes its partitions under its worker's temporary directory instead of
// to object storage, and the tasks that read them fetch the files from that
// worker, whole, to their own disk before they read them. Object storage
// costs a request a file to write and several to read, each tens of
// milliseconds, and a buffer of an upload's part for every file a writer
// has open: in a lab of four writers and four readers of 3 GB over 128
// partitions, the stage took 54 s through a fake object store that
// answered in 25 ms, and 17.5 s through the writers' disks, its writers'
// memory falling from 1.6 to 0.9 GB.
//
// A worker's disk does not keep what a lost worker wrote, as object storage
// does: a reader that cannot fetch a file fails its task as lost (Lost),
// and the statement's parts run again, once, without that worker (see
// Execute).
//
// A worker serves its exchange files only to peers that hold the worker
// secret (the mutual TLS of transport.go), and only the files it wrote
// itself, by the names it gave them: {qid}/s{stage}/{task}/a{attempt}/
// __b={partition}/data_{n}.parquet, with the size it wrote. It serves few
// at once, refuses past a queue, and removes a statement's files when the
// coordinator is done with them (DELETE) or when nothing used them for
// staleExchanges, as after the coordinator stopped (see sweep).

const (
	exchangesPath = "/v1/exchanges/"
	// localScheme marks an exchange file on a worker's disk, as the
	// coordinator lists it to readers: igloo-worker://{worker}/{qid}/
	// {file}?size={bytes}.
	localScheme = "igloo-worker://"
	// localShare is the share of its disk's free space, one in localShare,
	// that a task may expect to write there: the disk holds spill and the
	// read cache as well.
	localShare = 4
	// maxServing bounds the exchange files a worker sends at once, and
	// maxWaiting the requests that wait for one of those; it refuses
	// others, which their readers ask again (see fetchFile).
	maxServing = 32
	maxWaiting = 512
	// fetchesPerTask bounds the files a reading task fetches at once.
	fetchesPerTask = 8
	// maxLocalFile is the largest exchange file a worker serves or a
	// reader takes.
	maxLocalFile = 64 << 30
	// localSweepEvery is how often a worker sweeps its exchanges.
	localSweepEvery = 10 * time.Minute
)

var (
	// servingWait is how long a request for an exchange file waits for one
	// of maxServing before the worker answers that it is busy (503), and
	// stallTimeout how long a worker sending a file, or a reader receiving
	// one, waits for the other to make progress; variables for tests.
	servingWait  = 5 * time.Second
	stallTimeout = time.Minute

	queryIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
	localDirPattern  = regexp.MustCompile(`^s[0-9]{1,6}/[0-9]{1,9}/a[0-9]{1,3}$`)
	localFilePattern = regexp.MustCompile(`^s[0-9]{1,6}/[0-9]{1,9}/a[0-9]{1,3}/__b=[0-9]{1,6}/data_[0-9]{1,6}\.parquet$`)
)

// localExchanges are the exchange files a worker wrote to its disk.
type localExchanges struct {
	// root is their directory, as DuckDB writes and reads it, and real the
	// same with symbolic links resolved.
	root, real string
	// free measures the free space of a directory's filesystem.
	free func(string) (int64, error)
	now  func() time.Time

	// dirRoot opens files under root, never outside it.
	dirRoot *os.Root

	mu      sync.Mutex
	queries map[string]*localQuery
	// reserved are the bytes tasks running expect to write or fetch, which
	// the free space does not count yet; held are those of the files
	// written and fetched that are still there; limit, if not zero, caps
	// held and reserved together.
	reserved, held, limit int64

	serving chan struct{}
	waiting atomic.Int32
}

// localQuery is one statement's exchange files on a worker: their names,
// under root/{qid}, and sizes, and when it last wrote, served or kept them.
type localQuery struct {
	files map[string]int64
	used  time.Time
}

func newLocalExchanges(dir string) (*localExchanges, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create exchange directory: %w", err)
	}
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	return &localExchanges{root: filepath.Clean(dir), real: real, dirRoot: root, free: freeSpace, now: time.Now,
		queries: map[string]*localQuery{}, serving: make(chan struct{}, maxServing)}, nil
}

// reserveWait reserves n bytes of the disk for a task to fetch, as reserve
// does, waiting while other tasks' reservations keep them from fitting; it
// fails only if they would not fit with none, or ctx ends.
func (l *localExchanges) reserveWait(ctx context.Context, n int64) (release func(), err error) {
	wait := 20 * time.Millisecond
	for {
		release, ok := l.reserve(n)
		if ok {
			return release, nil
		}
		free, err := l.free(l.root)
		l.mu.Lock()
		none := l.reserved == 0
		over := l.limit > 0 && n > l.limit
		l.mu.Unlock()
		if err != nil || none || over || n > free/localShare {
			return func() {}, fmt.Errorf("the worker's disk is short of room for the %d bytes of exchanged rows the task reads", n)
		}
		select {
		case <-ctx.Done():
			return func() {}, ctx.Err()
		case <-time.After(wait):
		}
		wait = min(2*wait, time.Second)
	}
}

// reserve reserves n bytes of the disk for a task to write or fetch, if
// they fit localShare of its free space with what other tasks reserved, and
// returns what releases them once the free space counts the files.
func (l *localExchanges) reserve(n int64) (release func(), ok bool) {
	free, err := l.free(l.root)
	n = max(n, 0)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil || n+l.reserved > free/localShare || l.limit > 0 && n+l.reserved+l.held > l.limit {
		return func() {}, false
	}
	l.reserved += n
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.reserved -= n
			l.mu.Unlock()
		})
	}, true
}

// dir returns the directory, under root, of local, a shuffle task's
// {qid}/s{stage}/{task}/a{attempt}.
func (l *localExchanges) dir(local string) (qid, dir string, err error) {
	qid, rest, ok := strings.Cut(local, "/")
	if !ok || !queryIDPattern.MatchString(qid) || !localDirPattern.MatchString(rest) {
		return "", "", fmt.Errorf("bad exchange directory %q", local)
	}
	return qid, filepath.Join(l.root, qid, filepath.FromSlash(rest)), nil
}

// register records files a task wrote under root/{qid}, by name and size.
func (l *localExchanges) register(qid string, files map[string]int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	q := l.queries[qid]
	if q == nil {
		q = &localQuery{files: map[string]int64{}}
		l.queries[qid] = q
	}
	for name, size := range files {
		l.held += size - q.files[name]
		q.files[name] = size
	}
	q.used = l.now()
}

// drop forgets statement qid's files, as removed. l.mu must be held.
func (l *localExchanges) drop(qid string) {
	if q := l.queries[qid]; q != nil {
		for _, size := range q.files {
			l.held -= size
		}
		delete(l.queries, qid)
	}
}

// statements counts the statements whose exchange files the worker holds.
func (l *localExchanges) statements() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.queries)
}

// hold counts n bytes more, or fewer, of files fetched to the disk.
func (l *localExchanges) hold(n int64) {
	l.mu.Lock()
	l.held += n
	l.mu.Unlock()
}

// open opens the exchange file name of statement qid, if this worker wrote
// it, and returns it with its size: a name it did not give, or a file not
// as it wrote it (a symbolic link anywhere on its path, another size), is
// an error.
func (l *localExchanges) open(qid, name string) (*os.File, int64, error) {
	if !queryIDPattern.MatchString(qid) || !localFilePattern.MatchString(name) {
		return nil, 0, errNoExchange
	}
	l.mu.Lock()
	q := l.queries[qid]
	var size int64
	ok := false
	if q != nil {
		size, ok = q.files[name]
		q.used = l.now()
	}
	l.mu.Unlock()
	if !ok || size > maxLocalFile {
		return nil, 0, errNoExchange
	}
	path := filepath.Join(l.root, qid, filepath.FromSlash(name))
	if resolved, err := filepath.EvalSymlinks(path); err != nil || resolved != filepath.Join(l.real, qid, filepath.FromSlash(name)) {
		return nil, 0, errNoExchange
	}
	f, err := l.dirRoot.OpenFile(filepath.Join(qid, filepath.FromSlash(name)), os.O_RDONLY|noFollow, 0)
	if err != nil {
		return nil, 0, errNoExchange
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		_ = f.Close()
		return nil, 0, errNoExchange
	}
	return f, size, nil
}

// errNoExchange is an exchange file a worker does not serve.
var errNoExchange = errors.New("no such exchange file")

// keep records that statement qid still runs, and reports whether the
// worker holds files of it.
func (l *localExchanges) keep(qid string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	q := l.queries[qid]
	if q != nil {
		q.used = l.now()
	}
	return q != nil
}

// remove removes statement qid's exchange files.
func (l *localExchanges) remove(qid string) error {
	if !queryIDPattern.MatchString(qid) {
		return errNoExchange
	}
	l.mu.Lock()
	l.drop(qid)
	l.mu.Unlock()
	return os.RemoveAll(filepath.Join(l.root, qid))
}

// sweep removes the exchange files of statements nothing wrote, served or
// kept for staleExchanges before now, as when their coordinator stopped
// without removing them, and the directories of tasks that failed before
// they listed theirs, as old.
func (l *localExchanges) sweep(now time.Time) []string {
	l.mu.Lock()
	var stale []string
	for qid, q := range l.queries {
		if now.Sub(q.used) >= staleExchanges {
			stale = append(stale, qid)
		}
	}
	held := map[string]bool{}
	for qid := range l.queries {
		held[qid] = true
	}
	l.mu.Unlock()
	if entries, err := os.ReadDir(l.root); err == nil {
		for _, e := range entries {
			info, err := e.Info()
			if err == nil && !held[e.Name()] && now.Sub(info.ModTime()) >= staleExchanges {
				stale = append(stale, e.Name())
			}
		}
	}
	var removed []string
	for _, qid := range stale {
		l.mu.Lock()
		l.drop(qid)
		l.mu.Unlock()
		if err := os.RemoveAll(filepath.Join(l.root, filepath.Base(qid))); err == nil {
			removed = append(removed, qid)
		}
	}
	return removed
}

// serveExchangeFile sends an exchange file this worker wrote.
func (w *Worker) serveExchangeFile(rw http.ResponseWriter, r *http.Request) {
	if w.local.waiting.Add(1) > maxWaiting {
		w.local.waiting.Add(-1)
		http.Error(rw, "too many requests for exchange files", http.StatusServiceUnavailable)
		return
	}
	timer := time.NewTimer(servingWait)
	defer timer.Stop()
	select {
	case w.local.serving <- struct{}{}:
		w.local.waiting.Add(-1)
	case <-timer.C:
		w.local.waiting.Add(-1)
		http.Error(rw, "too busy sending exchange files", http.StatusServiceUnavailable)
		return
	case <-r.Context().Done():
		w.local.waiting.Add(-1)
		return
	}
	defer func() { <-w.local.serving }()
	f, size, err := w.local.open(r.PathValue("qid"), r.PathValue("file"))
	if err != nil {
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}
	defer f.Close() //nolint:errcheck
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	// A reader that stops reading gives up its slot.
	rc := http.NewResponseController(rw)
	var n int64
	for n < size {
		_ = rc.SetWriteDeadline(time.Now().Add(stallTimeout))
		m, err := io.CopyN(rw, f, min(size-n, 1<<20))
		n += m
		if err != nil {
			w.log.Debug("could not send an exchange file", "query", r.PathValue("qid"), "err", err)
			return
		}
	}
	w.opts.Metrics.sent(n)
}

func (w *Worker) serveExchangeKeep(rw http.ResponseWriter, r *http.Request) {
	w.hear()
	if !w.local.keep(r.PathValue("qid")) {
		http.Error(rw, errNoExchange.Error(), http.StatusNotFound)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

func (w *Worker) serveExchangeRemove(rw http.ResponseWriter, r *http.Request) {
	w.hear()
	if err := w.local.remove(r.PathValue("qid")); err != nil {
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

// sweepLoop sweeps the worker's exchanges every localSweepEvery until
// stop is closed.
func (w *Worker) sweepLoop(stop <-chan struct{}) {
	t := time.NewTicker(localSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		for _, qid := range w.local.sweep(time.Now()) {
			w.log.Info("removed the exchanges of a statement that no longer runs", "query", qid)
		}
	}
}

// shuffleLocal writes a shuffle task's partitions under its directory of
// the worker's disk, or to object storage when the disk is short of room
// for what the task expects to write, and lists the files: those on the
// disk by their names under its exchanges, {qid}/....
func (w *Worker) shuffleLocal(ctx context.Context, conn *duckdb.Conn, req *taskRequest, query string, cols []Column) (*taskResult, error) {
	qid, dir, err := w.local.dir(req.Local)
	if err != nil {
		return nil, err
	}
	// Room for its files, and for the copies of them its readers fetch.
	release, ok := w.local.reserve(2 * req.Expect)
	defer release()
	if !ok {
		w.log.Info("writing an exchange to object storage: the worker's disk is short of room for it",
			"query", req.Query, "task", req.Task, "expect", req.Expect)
		return w.shuffle(ctx, conn, req.Shuffle, query, cols, req.Partitions, req.Slots)
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return nil, err
	}
	res, err := w.writePartitions(ctx, conn, dir, query, cols, req.Partitions, req.Slots)
	if err == nil && ctx.Err() != nil {
		// Canceled as it finished: the coordinator may have removed the
		// statement's files already, and would not list these.
		err = ctx.Err()
	}
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	base := filepath.Join(w.local.root, qid) + string(filepath.Separator)
	files := map[string]int64{}
	for i, f := range res.Files {
		name, ok := strings.CutPrefix(f.Path, base)
		name = filepath.ToSlash(name)
		info, err := os.Lstat(f.Path)
		if !ok || !localFilePattern.MatchString(name) || err != nil || !info.Mode().IsRegular() || info.Size() > maxLocalFile {
			_ = os.RemoveAll(dir)
			return nil, fmt.Errorf("unexpected exchange file %s", f.Path)
		}
		files[name] = info.Size()
		res.Files[i].Path, res.Files[i].Size = qid+"/"+name, info.Size()
	}
	w.local.register(qid, files)
	return res, nil
}

// localFile is an exchange file on a worker's disk, as a reader is given it.
type localFile struct {
	worker, qid, name string
	size              int64
}

// localRef returns the reference readers are given to file, which worker
// wrote to its disk as {qid}/{name}, of size bytes.
func localRef(worker, file string, size int64) string {
	return localScheme + worker + "/" + file + "?size=" + strconv.FormatInt(size, 10)
}

// parseLocalRef parses a reference localRef made.
func parseLocalRef(ref string) (localFile, error) {
	rest, ok := strings.CutPrefix(ref, localScheme)
	path, size, sok := strings.Cut(rest, "?size=")
	worker, file, wok := strings.Cut(path, "/")
	qid, name, qok := strings.Cut(file, "/")
	n, err := strconv.ParseInt(size, 10, 64)
	if !ok || !sok || !wok || !qok || err != nil || n < 0 || n > maxLocalFile || worker == "" ||
		strings.ContainsAny(worker, "/?#@") || !queryIDPattern.MatchString(qid) || !localFilePattern.MatchString(name) {
		return localFile{}, fmt.Errorf("bad exchange file %q", ref)
	}
	return localFile{worker: worker, qid: qid, name: name, size: n}, nil
}

// fetchExchanges replaces the exchange files on workers' disks that req's
// views read with files on this worker's disk: its own where it wrote them,
// and the others fetched, whole, into a directory of the task's, which the
// returned function removes. A file it cannot fetch fails the task as lost
// (lostError).
func (w *Worker) fetchExchanges(ctx context.Context, req *taskRequest) (func(), error) {
	type fetch struct {
		file      localFile
		view, pos int
	}
	var fetches []fetch
	for i := range req.Views {
		for j, ref := range req.Views[i].Files {
			if !strings.HasPrefix(ref, localScheme) {
				continue
			}
			f, err := parseLocalRef(ref)
			if err != nil {
				return func() {}, err
			}
			if !slices.Contains(req.Peers, f.worker) {
				return func() {}, fmt.Errorf("exchange file %s is on %s, not a worker of the statement", ref, f.worker)
			}
			if req.Views[i].lent == nil {
				req.Views[i].lent = map[string]bool{}
			}
			if f.worker == req.Worker && req.Worker != "" {
				// Its own: read in place, as it wrote it.
				file, size, err := w.local.open(f.qid, f.name)
				if err != nil || size != f.size {
					return func() {}, &lostError{worker: f.worker, err: fmt.Errorf("its own exchange file %s/%s is gone", f.qid, f.name)}
				}
				_ = file.Close()
				path := filepath.Join(w.local.root, f.qid, filepath.FromSlash(f.name))
				req.Views[i].Files[j], req.Views[i].lent[path] = path, true
				continue
			}
			fetches = append(fetches, fetch{file: f, view: i, pos: j})
		}
	}
	if len(fetches) == 0 {
		return func() {}, nil
	}
	var total int64
	for _, f := range fetches {
		total += f.file.size
	}
	release, err := w.local.reserveWait(ctx, total)
	if err != nil {
		return func() {}, err
	}
	defer release()
	dir, err := os.MkdirTemp(w.fetchRoot, "task-")
	if err != nil {
		return func() {}, err
	}
	done := func() { _ = os.RemoveAll(dir) }
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(fetchesPerTask)
	for n, f := range fetches {
		dst := filepath.Join(dir, strconv.Itoa(n)+".parquet")
		req.Views[f.view].lent[dst] = true
		g.Go(func() error {
			if err := w.fetchFile(gctx, f.file, dst); err != nil {
				return err
			}
			req.Views[f.view].Files[f.pos] = dst
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		done()
		return func() {}, err
	}
	// Fetched, the files count in the free space, and as held until the
	// task removes them, rather than as reserved.
	w.local.hold(total)
	release()
	return func() {
		done()
		w.local.hold(-total)
	}, nil
}

// fetchFile fetches exchange file f from the worker that wrote it to dst.
// The worker is lost if it cannot be reached, does not have the file, breaks
// off sending it, or stops answering at all (see heartbeats); a worker too
// busy to send it now (HTTP 503) is asked again, for up to fetchPatience.
func (w *Worker) fetchFile(ctx context.Context, f localFile, dst string) error {
	lost, why, release := w.beats.watch(f.worker)
	defer release()
	fctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		select {
		case <-lost:
			stop()
		case <-fctx.Done():
		}
	}()
	err := w.fetchAsked(fctx, f, dst)
	select {
	case <-lost:
		if ctx.Err() == nil {
			return &lostError{worker: f.worker, err: fmt.Errorf("it stopped answering: %w", why())}
		}
	default:
	}
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// fetchAsked is fetchFile but for its worker's heartbeat, which ends ctx.
func (w *Worker) fetchAsked(ctx context.Context, f localFile, dst string) error {
	url := "https://" + f.worker + exchangesPath + f.qid + "/" + f.name
	wait, waited := 100*time.Millisecond, time.Duration(0)
	for {
		rctx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			return err
		}
		resp, err := w.peers.Do(req)
		switch {
		case err != nil && ctx.Err() != nil:
			cancel()
			return ctx.Err()
		case err != nil:
			// Refused, cut off, timed out or unreachable: lost, as far as
			// this reader can tell; the coordinator asks the worker before
			// it runs the statement without it (see confirmLost).
			cancel()
			return &lostError{worker: f.worker, err: err}
		}
		if resp.StatusCode == http.StatusServiceUnavailable && waited < fetchPatience {
			_ = resp.Body.Close()
			cancel()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			waited += wait
			wait = min(2*wait, 5*time.Second)
			continue
		}
		// A worker that stops sending loses the request, and the file.
		stall := time.AfterFunc(stallTimeout, cancel)
		err = w.saveFile(resp, f, dst, func() { stall.Reset(stallTimeout) })
		stall.Stop()
		_ = resp.Body.Close()
		cancel()
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
}

// fetchPatience is how long a reader keeps asking a busy worker for a file.
const fetchPatience = 5 * time.Minute

// saveFile writes the body of resp, exchange file f, to dst. A body cut
// short loses its worker; a write to this worker's disk that fails does not.
func (w *Worker) saveFile(resp *http.Response, f localFile, dst string, progress func()) error {
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return &lostError{worker: f.worker, err: fmt.Errorf("%s/%s: %s", f.qid, f.name, resp.Status)}
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("fetch exchange file %s/%s from %s: %s", f.qid, f.name, f.worker, resp.Status)
	case resp.ContentLength != f.size:
		return &lostError{worker: f.worker, err: fmt.Errorf("%s/%s is %d bytes, not the %d written", f.qid, f.name, resp.ContentLength, f.size)}
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	body := &bodyReader{r: io.LimitReader(resp.Body, f.size+1), progress: progress}
	n, err := io.Copy(out, body)
	err = errors.Join(err, out.Close())
	switch {
	case body.err != nil || err == nil && n != f.size:
		return &lostError{worker: f.worker, err: fmt.Errorf("%s/%s: %d of %d bytes: %v", f.qid, f.name, n, f.size, body.err)}
	case err != nil:
		return fmt.Errorf("save exchange file %s/%s: %w", f.qid, f.name, err)
	}
	return nil
}

// bodyReader records what reading a response's body failed with, apart
// from what writing it failed with.
type bodyReader struct {
	r        io.Reader
	err      error
	progress func()
}

func (b *bodyReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		b.progress()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		b.err = err
	}
	return n, err
}

// holdLocal keeps statement qid's exchanges on the disks of workers ws
// until the returned function is called: every heartbeatEvery, it tells
// them the statement still runs, so that their sweeps leave the files
// however long its last stages take.
func (co *Coordinator) holdLocal(qid string, ws []*worker) (release func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(co.heartbeat)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				var wg sync.WaitGroup
				for _, w := range ws {
					wg.Go(func() { _ = co.exchangeRequest(ctx, w, http.MethodPost, qid+"/keep") })
				}
				wg.Wait()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// removeLocalLater removes statement qid's exchanges from the disks of
// workers ws, in the background. A worker it cannot reach removes them in
// its sweep.
func (co *Coordinator) removeLocalLater(qid string, ws []*worker) {
	for _, w := range ws {
		co.removals.Go(func() {
			if err := co.exchangeRequest(co.removeCtx, w, http.MethodDelete, qid); err != nil {
				co.log.Debug("could not remove exchanged rows from a worker's disk", "query", qid, "worker", w.addr, "err", err)
			}
		})
	}
}

// exchangeRequest sends a request about statement qid's exchanges on its
// disk to w.
func (co *Coordinator) exchangeRequest(ctx context.Context, w *worker, method, path string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "https://"+w.addr+exchangesPath+path, nil)
	if err != nil {
		return err
	}
	resp, err := co.pool.client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("worker answered %s", resp.Status)
	}
	return nil
}

// confirmLost reports whether worker addr, of ws, a reader could not fetch
// exchange files from, is lost with them: its heartbeat here says so, it does
// not answer, or it answers as another run of it, which holds none. One that
// answers as the same run was busy or briefly unreachable, and the statement
// fails as it is rather than run again without a worker that has its files.
func (co *Coordinator) confirmLost(ctx context.Context, ws []*worker, addr string) bool {
	i := slices.IndexFunc(ws, func(w *worker) bool { return w.addr == addr })
	if i < 0 {
		return false
	}
	if co.pool.beats.lostWithin(addr, time.Minute) {
		return true
	}
	// The reader waited out its misses already: a couple of checks are enough.
	for try := 0; try < 2; try++ {
		cctx, cancel := context.WithTimeout(ctx, taskCheckTimeout)
		st, err := co.pool.status(cctx, addr)
		cancel()
		if err == nil {
			return st.Boot != ws[i].status.Boot
		}
		if ctx.Err() != nil {
			return true
		}
	}
	return true
}
