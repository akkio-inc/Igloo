package distributed

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/akkio-inc/igloo/internal/config"
)

// exchangeWorker starts a worker that serves exchange files, and returns it
// with its address and a client holding the worker secret.
func exchangeWorker(t *testing.T) (*Worker, string, *http.Client) {
	t.Helper()
	w, err := NewWorker(WorkerOptions{Secret: testSecret, Version: "test", TempDir: t.TempDir(),
		Locations: []string{t.TempDir()}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = w.Serve(ln) }()
	t.Cleanup(func() { _ = w.Shutdown(context.Background()) })
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tr.client},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return w, ln.Addr().String(), client
}

// writeExchange writes an exchange file of statement qid on w's disk and,
// if register, records it as w writing it.
func writeExchange(t *testing.T, w *Worker, qid, name, body string, register bool) string {
	t.Helper()
	path := filepath.Join(w.local.root, qid, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if register {
		w.local.register(qid, map[string]int64{name: int64(len(body))})
	}
	return path
}

// TestExchangeFiles checks what a worker serves of its exchange files: a
// file it wrote, by the name it gave it, to a peer that holds the worker
// secret; nothing else.
func TestExchangeFiles(t *testing.T) {
	w, addr, client := exchangeWorker(t)
	const name = "s1/0/a0/__b=3/data_0.parquet"
	writeExchange(t, w, "q1", name, "rows of q1", true)
	get := func(path string) (int, string) {
		t.Helper()
		resp, err := client.Get("https://" + addr + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer resp.Body.Close() //nolint:errcheck
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get(exchangesPath + "q1/" + name); code != http.StatusOK || body != "rows of q1" {
		t.Fatalf("its own file: %d %q", code, body)
	}

	// Without the secret, no connection.
	other, err := newTransport("another-secret-that-is-long-enough-12345")
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []*tls.Config{other.client, {InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}} { //nolint:gosec
		c := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
		if resp, err := c.Get("https://" + addr + exchangesPath + "q1/" + name); err == nil {
			resp.Body.Close() //nolint:errcheck
			t.Errorf("served a peer without the secret: %s", resp.Status)
		}
	}

	// A file of another statement, and one the worker did not write, on
	// its disk all the same.
	writeExchange(t, w, "q2", name, "rows of q2", false)
	writeExchange(t, w, "q1", "s1/0/a0/__b=4/data_0.parquet", "not listed", false)
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("hidden"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		exchangesPath + "q2/" + name,
		exchangesPath + "q1/s1/0/a0/__b=4/data_0.parquet",
		exchangesPath + "q1/s1/0/a0/__b=3/../../../../../q2/" + name,
		exchangesPath + "q1/s1/0/a0/__b=3/%2e%2e/%2e%2e/%2e%2e/%2e%2e/%2e%2e/q2/" + name,
		exchangesPath + "q1/" + urlPath(secret),
		exchangesPath + "q1//" + strings.TrimPrefix(secret, "/"),
		exchangesPath + "..%2f..%2f/" + name,
		exchangesPath + "q1/s1/0/a0/__b=3/data_0.parquet%00.txt",
		exchangesPath + "q1/s1/0/a0/__b=3/data_0.csv",
		exchangesPath + strings.Repeat("q", 200) + "/" + name,
	} {
		if code, body := get(path); code == http.StatusOK || strings.Contains(body, "rows of q2") || strings.Contains(body, "hidden") {
			t.Errorf("%s: %d %q", path, code, body)
		}
	}

	// A file replaced by a symbolic link, or under a directory replaced by
	// one, or of another size than written.
	link := writeExchange(t, w, "q3", name, "hidden", true)
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, link); err != nil {
		t.Fatal(err)
	}
	writeExchange(t, w, "q4", name, "hidden", true)
	dir := filepath.Join(w.local.root, "q4", "s1", "0", "a0")
	if err := os.Rename(dir, dir+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(filepath.Dir(secret), "__b=3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(secret, filepath.Join(filepath.Dir(secret), "__b=3", "data_0.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(secret), dir); err != nil {
		t.Fatal(err)
	}
	grown := writeExchange(t, w, "q5", name, "rows", true)
	if err := os.WriteFile(grown, []byte("rows and more"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, qid := range []string{"q3", "q4", "q5"} {
		if code, body := get(exchangesPath + qid + "/" + name); code != http.StatusNotFound {
			t.Errorf("%s: %d %q", qid, code, body)
		}
	}
	// Too large to serve, though listed.
	w.local.register("q6", map[string]int64{name: maxLocalFile + 1})
	if code, _ := get(exchangesPath + "q6/" + name); code != http.StatusNotFound {
		t.Errorf("a file past maxLocalFile: %d", code)
	}

	// Past the requests it lets wait, it refuses.
	for range maxServing {
		w.local.serving <- struct{}{}
	}
	w.local.waiting.Store(maxWaiting)
	if code, _ := get(exchangesPath + "q1/" + name); code != http.StatusServiceUnavailable {
		t.Errorf("past maxWaiting: %d", code)
	}
	w.local.waiting.Store(0)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+exchangesPath+"q1/"+name, nil)
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close() //nolint:errcheck
		t.Errorf("past maxServing: %s, not waiting", resp.Status)
	}
	cancel()
	for range maxServing {
		<-w.local.serving
	}

	// Kept while it runs; removed when done, and then not served.
	do := func(method, path string) int {
		t.Helper()
		req, _ := http.NewRequest(method, "https://"+addr+exchangesPath+path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close() //nolint:errcheck
		return resp.StatusCode
	}
	if code := do(http.MethodPost, "q1/keep"); code != http.StatusNoContent {
		t.Errorf("keep: %d", code)
	}
	if code := do(http.MethodPost, "q9/keep"); code != http.StatusNotFound {
		t.Errorf("keep of nothing: %d", code)
	}
	if code := do(http.MethodDelete, "q1"); code != http.StatusNoContent {
		t.Errorf("remove: %d", code)
	}
	if code, _ := get(exchangesPath + "q1/" + name); code != http.StatusNotFound {
		t.Errorf("removed, then served: %d", code)
	}
	if _, err := os.Stat(filepath.Join(w.local.root, "q1")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("removed, then left: %v", err)
	}
	if code := do(http.MethodDelete, ".."); code == http.StatusNoContent {
		t.Error("removed ..")
	}
}

// urlPath writes a path as it would appear in a URL path.
func urlPath(path string) string { return strings.TrimPrefix(path, "/") }

// TestExchangeSweep checks that a worker removes the exchange files of
// statements nothing used for staleExchanges, as after their coordinator
// stopped without removing them, and the directories of tasks that failed
// before they listed their files, but not those still in use; and that a
// coordinator's heartbeat keeps them.
func TestExchangeSweep(t *testing.T) {
	w, _, _ := exchangeWorker(t)
	const name = "s1/0/a0/__b=0/data_0.parquet"
	now := time.Now()
	w.local.now = func() time.Time { return now.Add(-staleExchanges - time.Hour) }
	writeExchange(t, w, "stale", name, "x", true)
	w.local.now = time.Now
	writeExchange(t, w, "fresh", name, "x", true)
	stray := writeExchange(t, w, "stray", name, "x", false)
	old := now.Add(-staleExchanges - time.Hour)
	for d := stray; d != w.local.root; d = filepath.Dir(d) {
		if err := os.Chtimes(d, old, old); err != nil {
			t.Fatal(err)
		}
	}
	writeExchange(t, w, "newstray", name, "x", false)
	removed := w.local.sweep(now)
	for _, qid := range []string{"stale", "stray"} {
		if _, err := os.Stat(filepath.Join(w.local.root, qid)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s left: %v (removed %v)", qid, err, removed)
		}
	}
	for _, qid := range []string{"fresh", "newstray"} {
		if _, err := os.Stat(filepath.Join(w.local.root, qid)); err != nil {
			t.Errorf("%s removed: %v", qid, err)
		}
	}

	// A heartbeat keeps a statement's files on its workers' disks.
	h := newHarness(t, 0.001, 1, 1)
	h.co.heartbeat = 10 * time.Millisecond
	hw := h.workers[0]
	hw.local.now = func() time.Time { return now.Add(-staleExchanges) }
	writeExchange(t, hw, "kept", name, "x", true)
	hw.local.now = time.Now
	release := h.co.holdLocal("kept", h.co.pool.known(context.Background()))
	deadline := time.Now().Add(5 * time.Second)
	for {
		hw.local.mu.Lock()
		used := hw.local.queries["kept"].used
		hw.local.mu.Unlock()
		if now.Sub(used) < staleExchanges/2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no heartbeat kept the statement's files")
		}
		time.Sleep(10 * time.Millisecond)
	}
	release()
	if removed := hw.local.sweep(time.Now()); len(removed) != 0 {
		t.Errorf("swept files a heartbeat kept: %v", removed)
	}
}

// exchangeQuery is a statement that exchanges rows: a join of two tables
// partitioned to meet.
const exchangeQuery = `SELECT o.o_orderpriority, count(*), sum(l.l_quantity) FROM lineitem AS l JOIN orders AS o ON o.o_orderkey = l.l_orderkey
	GROUP BY 1 ORDER BY 1`

// localHarness is a harness whose workers exchange rows through their
// disks.
func localHarness(t *testing.T, workers int) *harness {
	t.Helper()
	h := newHarness(t, 0.01, 3, workers)
	h.co.maxBroadcast = 1
	h.co.opts.LocalExchanges = true
	return h
}

// shuffleFiles lists the exchange files in the warehouse's object storage.
func shuffleFiles(t *testing.T, h *harness) []string {
	t.Helper()
	var files []string
	_ = filepath.WalkDir(filepath.Join(h.dir, "warehouse", config.ShuffleDir), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}
		return nil
	})
	return files
}

// localFiles lists what workers hold of exchanges on their disks, fetched
// or written.
func localFiles(t *testing.T, h *harness) []string {
	t.Helper()
	var files []string
	for _, w := range h.workers {
		for _, dir := range []string{w.local.root, w.fetchRoot} {
			_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					files = append(files, path)
				}
				return nil
			})
		}
	}
	return files
}

// TestLocalExchanges checks a statement whose workers exchange rows through
// their disks: it returns what one machine does, writes nothing to object
// storage, and leaves nothing on the workers' disks once it is done.
func TestLocalExchanges(t *testing.T) {
	h := localHarness(t, 3)
	var written atomic.Int64
	for _, w := range h.workers {
		free := w.local.free
		w.local.free = func(dir string) (int64, error) {
			written.Add(1)
			return free(dir)
		}
	}
	if !h.check(exchangeQuery) {
		t.Fatal("did not run distributed")
	}
	if written.Load() == 0 {
		t.Error("no task wrote to its disk")
	}
	h.co.removals.Wait()
	if files := shuffleFiles(t, h); len(files) > 0 {
		t.Errorf("wrote to object storage: %v", files)
	}
	if files := localFiles(t, h); len(files) > 0 {
		t.Errorf("left on workers' disks: %v", files)
	}
}

// TestLocalExchangeRoom checks that a worker whose disk is short of room
// for what a task expects to write writes it to object storage instead,
// and the statement returns what one machine does.
func TestLocalExchangeRoom(t *testing.T) {
	h := localHarness(t, 3)
	for _, w := range h.workers {
		w.local.free = func(string) (int64, error) { return 1 << 10, nil }
	}
	var fellBack atomic.Int32
	hook := func(r slog.Record) {
		if strings.HasPrefix(r.Message, "writing an exchange to object storage") {
			fellBack.Add(1)
		}
	}
	h.hook.Store(&hook)
	if !h.check(exchangeQuery) {
		t.Fatal("did not run distributed")
	}
	if fellBack.Load() == 0 {
		t.Error("no task wrote to object storage")
	}
	for _, w := range h.workers {
		w.local.mu.Lock()
		n := len(w.local.queries)
		w.local.mu.Unlock()
		if n != 0 {
			t.Errorf("%d statements' exchanges on a disk short of room", n)
		}
	}
}

// TestLostExchange checks a statement one of whose workers is lost after
// its writing stages ran, before the stages that read what they wrote:
// what it wrote to its disk is gone, and the statement's parts run again
// on the other workers, and return what one machine does. Two lost fail
// the statement, which returns nothing.
func TestLostExchange(t *testing.T) {
	for _, lose := range []int{1, 2} {
		h := localHarness(t, 3)
		var ran atomic.Int32
		var killed atomic.Int32
		hook := func(r slog.Record) {
			// After the second of the stages that write the exchanges the
			// join reads, of each run.
			if r.Message != "ran a stage" || !strings.Contains(attr(r, "stage"), " to exchange ") {
				return
			}
			if n := int(ran.Add(1)); n == 2 || lose == 2 && n == 4 {
				i := 3 - n/2
				_ = h.workers[i].Shutdown(context.Background())
				_ = h.lns[i].Close()
				killed.Add(1)
			}
		}
		h.hook.Store(&hook)
		want, _, err := h.query(context.Background(), exchangeQuery)
		if err != nil {
			t.Fatal(err)
		}
		got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
		switch lose {
		case 1:
			if err != nil {
				t.Fatalf("one worker lost: %v", err)
			}
			if msg := compareRows(want, got, true); msg != "" {
				t.Errorf("one worker lost: %s", msg)
			}
		case 2:
			if !errors.Is(err, ErrExchangeLost) || got != nil {
				t.Errorf("two workers lost: %d rows, %v", len(got), err)
			}
		}
		if int(killed.Load()) != lose {
			t.Errorf("%d workers lost, want %d", killed.Load(), lose)
		}
	}
}

// TestDrainServesExchanges checks a worker told to stop after its writing
// stages ran, before the stages that read what they wrote, as on a Spot
// interruption notice: it takes no more tasks, which the other workers
// run, says it is retiring, and serves its exchange files to the readers,
// so that the statement runs on without running any part again and returns
// what one machine does; once the statement removed its files, the drain
// ends. A worker with nothing to drain stops at once, and one whose files
// no statement removes stops when its deadline passes.
func TestDrainServesExchanges(t *testing.T) {
	h := localHarness(t, 3)
	var ran, again atomic.Int32
	drained := make(chan error, 1)
	hook := func(r slog.Record) {
		if strings.HasPrefix(r.Message, "a worker holding exchanged rows on its disk was lost") {
			again.Add(1)
		}
		if r.Message != "ran a stage" || !strings.Contains(attr(r, "stage"), " to exchange ") {
			return
		}
		if ran.Add(1) == 2 {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				drained <- h.workers[2].Drain(ctx)
			}()
			for !h.workers[2].Retiring() {
				time.Sleep(time.Millisecond)
			}
		}
	}
	h.hook.Store(&hook)
	want, _, err := h.query(context.Background(), exchangeQuery)
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
	if err != nil {
		t.Fatalf("a worker drained: %v", err)
	}
	if msg := compareRows(want, got, true); msg != "" {
		t.Errorf("a worker drained: %s", msg)
	}
	if again.Load() != 0 {
		t.Errorf("ran the statement's parts again %d times without the drained worker", again.Load())
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Errorf("drain: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the drain did not end once the statement had ended; it holds files of %d statements", h.workers[2].local.statements())
	}
	if st, err := h.co.pool.status(context.Background(), h.lns[2].Addr().String()); err != nil || !st.Retiring {
		t.Errorf("a drained worker's status: retiring %v, %v", st.Retiring, err)
	}

	// Nothing to drain: at once. Files no statement removes: until the
	// deadline.
	start := time.Now()
	if err := h.workers[1].Drain(context.Background()); err != nil || time.Since(start) > time.Second {
		t.Errorf("a worker with nothing to drain: %v after %s", err, time.Since(start))
	}
	h.workers[0].local.register("orphaned", map[string]int64{"s1/0/a0/__b=0/data_0.parquet": 1})
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	if err := h.workers[0].Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a worker holding files no statement removes: %v", err)
	}
}

// TestLocalRefs checks the references to exchange files on workers' disks
// that readers are given, and that a reader takes nothing else for one.
func TestLocalRefs(t *testing.T) {
	ref := localRef("10.0.0.1:7433", "q1/s1/0/a0/__b=3/data_0.parquet", 42)
	f, err := parseLocalRef(ref)
	if err != nil || f.worker != "10.0.0.1:7433" || f.qid != "q1" || f.name != "s1/0/a0/__b=3/data_0.parquet" || f.size != 42 {
		t.Fatalf("%s: %+v %v", ref, f, err)
	}
	for _, bad := range []string{
		localScheme + "10.0.0.1:7433/q1/../../etc/passwd?size=1",
		localScheme + "10.0.0.1:7433/q1//etc/passwd?size=1",
		localScheme + "10.0.0.1:7433/q1/s1/0/a0/__b=3/data_0.parquet",
		localScheme + "10.0.0.1:7433/q1/s1/0/a0/__b=3/data_0.parquet?size=-1",
		localScheme + "user@10.0.0.1:7433/q1/s1/0/a0/__b=3/data_0.parquet?size=1",
		localScheme + "/q1/s1/0/a0/__b=3/data_0.parquet?size=1",
		localScheme + "10.0.0.1:7433/q1/s1/0/a0/__b=3/data_0.parquet?size=99999999999999",
	} {
		if f, err := parseLocalRef(bad); err == nil {
			t.Errorf("%s: %+v", bad, f)
		}
	}
}

// TestExchangeRoom checks what tasks may reserve of a worker's disk: with
// what other tasks reserved, no more than a quarter of its free space.
func TestExchangeRoom(t *testing.T) {
	w, _, _ := exchangeWorker(t)
	w.local.free = func(string) (int64, error) { return 400, nil }
	r1, ok1 := w.local.reserve(60)
	r2, ok2 := w.local.reserve(40)
	_, ok3 := w.local.reserve(1)
	if !ok1 || !ok2 || ok3 {
		t.Errorf("reserved 60 %v, 40 %v, then 1 %v of a quarter of 400", ok1, ok2, ok3)
	}
	r1()
	r1()
	if _, ok := w.local.reserve(60); !ok {
		t.Error("released, and still reserved")
	}
	r2()
	// Under IGLOO_EXCHANGE_MAX_SIZE, with what files on the disk hold.
	w.local.limit = 100
	w.local.register("q7", map[string]int64{"s1/0/a0/__b=0/data_0.parquet": 70})
	if _, ok := w.local.reserve(40); ok {
		t.Error("reserved past the limit")
	}
	if err := w.local.remove("q7"); err != nil {
		t.Fatal(err)
	}
	if r, ok := w.local.reserve(40); !ok {
		t.Error("removed, and still held")
	} else {
		r()
	}
	w.local.limit = 0
	w.local.free = func(string) (int64, error) { return 0, errors.New("no disk") }
	if _, ok := w.local.reserve(0); ok {
		t.Error("reserved without knowing the free space")
	}
	// Exchange files on its disk are read only as fetchExchanges lends them.
	path := writeExchange(t, w, "q1", "s1/0/a0/__b=0/data_0.parquet", "x", true)
	if w.allowed(path) || w.allowed(filepath.Join(w.fetchRoot, "task-1", "0.parquet")) {
		t.Error("a task may name files of the worker's disk outside the warehouse")
	}
}

// TestExchangeFetch checks how a reader fetches a file from a peer: a busy
// peer is asked again, and one that cannot be reached, does not have the
// file or breaks off loses its files; a peer that answers otherwise fails
// the task without losing them, and a peer is never followed elsewhere.
func TestExchangeFetch(t *testing.T) {
	w, addr, _ := exchangeWorker(t)
	const name = "s1/0/a0/__b=0/data_0.parquet"
	writeExchange(t, w, "q1", name, "rows", true)
	r, _, _ := exchangeWorker(t)
	fetch := func(worker string, size int64) error {
		t.Helper()
		dir := t.TempDir()
		return r.fetchFile(context.Background(), localFile{worker: worker, qid: "q1", name: name, size: size}, filepath.Join(dir, "f"))
	}
	if err := fetch(addr, 4); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var lost *lostError
	if err := fetch(addr, 5); !errors.As(err, &lost) {
		t.Errorf("another size: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	ln.Close()
	if err := fetch(closed, 4); !errors.As(err, &lost) || lost.worker != closed {
		t.Errorf("unreachable: %v", err)
	}
	w.local.remove("q1")
	if err := fetch(addr, 4); !errors.As(err, &lost) {
		t.Errorf("not found: %v", err)
	}

	// A peer of its own making: busy twice, then the file; an error; a
	// redirect elsewhere.
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	var busy, followed atomic.Int32
	elsewhere := httptestServer(t, tr, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) { followed.Add(1) }))
	peer := httptestServer(t, tr, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "redirect"):
			http.Redirect(rw, r, "https://"+elsewhere+r.URL.Path, http.StatusFound)
		case strings.Contains(r.URL.Path, "/q2/"):
			http.Error(rw, "broken", http.StatusInternalServerError)
		case busy.Add(1) <= 2:
			http.Error(rw, "busy", http.StatusServiceUnavailable)
		default:
			rw.Header().Set("Content-Length", "4")
			_, _ = rw.Write([]byte("rows"))
		}
	}))
	if err := fetch(peer, 4); err != nil || busy.Load() != 3 {
		t.Errorf("busy twice: %v after %d asks", err, busy.Load())
	}
	dir := t.TempDir()
	err = r.fetchFile(context.Background(), localFile{worker: peer, qid: "q2", name: name, size: 4}, filepath.Join(dir, "f"))
	if err == nil || errors.As(err, &lost) {
		t.Errorf("an error that loses nothing: %v", err)
	}
	err = r.fetchFile(context.Background(), localFile{worker: peer, qid: "redirect", name: name, size: 4}, filepath.Join(dir, "g"))
	if err == nil || followed.Load() != 0 {
		t.Errorf("redirected: %v, followed %d times", err, followed.Load())
	}

	// A file on a worker that is not the statement's.
	req := &taskRequest{Worker: "10.0.0.2:7433", Peers: []string{"10.0.0.2:7433"},
		Views: []viewDef{{Files: []string{localRef(peer, "q1/"+name, 4)}}}}
	if _, err := r.fetchExchanges(context.Background(), req); err == nil || errors.As(err, &lost) {
		t.Errorf("a worker not the statement's: %v", err)
	}
}

// httptestServer serves h over the workers' mutual TLS, and returns its
// address.
func httptestServer(t *testing.T, tr *transport, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h, TLSConfig: tr.server, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// TestUnconfirmedLoss checks a statement whose reader could not fetch a
// file from a worker that still answers as the same run of it, as one
// briefly unreachable would: it fails as lost, without running again
// without that worker, and returns nothing.
func TestUnconfirmedLoss(t *testing.T) {
	h := localHarness(t, 3)
	var ran, again atomic.Int32
	hook := func(r slog.Record) {
		if strings.HasPrefix(r.Message, "a worker holding exchanged rows on its disk was lost") {
			again.Add(1)
		}
		if r.Message != "ran a stage" || !strings.Contains(attr(r, "stage"), " to exchange ") {
			return
		}
		if ran.Add(1) == 2 {
			w := h.workers[2]
			w.local.mu.Lock()
			for qid := range w.local.queries {
				delete(w.local.queries, qid)
			}
			w.local.mu.Unlock()
		}
	}
	h.hook.Store(&hook)
	got, _, _, _, err := h.run(context.Background(), exchangeQuery, ModeOn)
	if !errors.Is(err, ErrExchangeLost) || got != nil || again.Load() != 0 {
		t.Errorf("%d rows, %v, ran again %d times", len(got), err, again.Load())
	}
}

// TestExchangeWaits checks the waits that bound fetching exchange files: a
// reader waits for room while other tasks hold it, but not for room that
// is not there; a worker answers that it is busy when no slot frees up; and
// a reader whose worker stops sending loses it.
func TestExchangeWaits(t *testing.T) {
	w, addr, client := exchangeWorker(t)
	w.local.free = func(string) (int64, error) { return 400, nil }
	held, _ := w.local.reserve(80)
	got := make(chan error, 1)
	go func() {
		release, err := w.local.reserveWait(context.Background(), 60)
		release()
		got <- err
	}()
	select {
	case err := <-got:
		t.Fatalf("did not wait for room another task held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	held()
	if err := <-got; err != nil {
		t.Errorf("room freed: %v", err)
	}
	if _, err := w.local.reserveWait(context.Background(), 101); err == nil {
		t.Error("waited for room that is not there")
	}

	defer func(d time.Duration) { servingWait = d }(servingWait)
	servingWait = 50 * time.Millisecond
	const name = "s1/0/a0/__b=0/data_0.parquet"
	writeExchange(t, w, "q1", name, "rows", true)
	for range maxServing {
		w.local.serving <- struct{}{}
	}
	resp, err := client.Get("https://" + addr + exchangesPath + "q1/" + name)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("no slot free: %v %v", resp, err)
	}
	if err == nil {
		resp.Body.Close() //nolint:errcheck
	}
	for range maxServing {
		<-w.local.serving
	}

	defer func(d time.Duration) { stallTimeout = d }(stallTimeout)
	stallTimeout = 100 * time.Millisecond
	tr, err := newTransport(testSecret)
	if err != nil {
		t.Fatal(err)
	}
	stuck := make(chan struct{})
	defer close(stuck)
	peer := httptestServer(t, tr, http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Length", "8")
		_, _ = rw.Write([]byte("ro"))
		rw.(http.Flusher).Flush()
		<-stuck
	}))
	err = w.fetchFile(context.Background(), localFile{worker: peer, qid: "q1", name: name, size: 8}, filepath.Join(t.TempDir(), "f"))
	var lost *lostError
	if !errors.As(err, &lost) {
		t.Errorf("a worker that stopped sending: %v", err)
	}
}

// With IGLOO_EXCHANGES=auto, workers write a statement's exchanges to their
// own disks when every one reports room there and runs outside
// Kubernetes, and to object storage otherwise; the statement returns what
// one machine does either way.
func TestAutoExchanges(t *testing.T) {
	co := &Coordinator{opts: Options{AutoExchanges: true}}
	room := func(n int64, k8s bool) *worker { return &worker{status: workerStatus{LocalRoom: n, Kubernetes: k8s}} }
	for _, c := range []struct {
		ws   []*worker
		want bool
	}{
		{[]*worker{room(64<<30, false), room(2<<30, false)}, true},
		{[]*worker{room(64<<30, false), room(minLocalRoom-1, false)}, false},
		{[]*worker{room(64<<30, false), room(64<<30, true)}, false},
		{[]*worker{room(64<<30, false), {}}, false},
		{nil, false},
	} {
		if got := co.localFor(c.ws); got != c.want {
			t.Errorf("%v: %v, want %v", c.ws, got, c.want)
		}
	}
	co.opts = Options{}
	if co.localFor([]*worker{room(64<<30, false)}) {
		t.Error("object storage by default")
	}
	co.opts.LocalExchanges = true
	if !co.localFor([]*worker{room(0, true)}) {
		t.Error("IGLOO_EXCHANGES=local writes to workers' disks whatever they report")
	}

	h := newHarness(t, 0.01, 3, 2)
	h.co.maxBroadcast = 1
	h.co.opts.AutoExchanges = true
	h.co.opts.Metrics = NewMetrics(prometheus.NewRegistry())
	q := `SELECT o_orderpriority, count(*), sum(l_quantity) FROM orders JOIN lineitem ON l_orderkey = o_orderkey GROUP BY 1 ORDER BY 1`
	if !h.check(q) {
		t.Fatal("did not run distributed")
	}
	local := testutil.ToFloat64(h.co.opts.Metrics.exchanges.WithLabelValues("local"))
	object := testutil.ToFloat64(h.co.opts.Metrics.exchanges.WithLabelValues("object"))
	if local != 1 || object != 0 {
		t.Errorf("exchanged %v times on workers' disks and %v through object storage, want 1 and 0", local, object)
	}
}
