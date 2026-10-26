// Package catalogproxy stands between DuckDB and the Iceberg REST catalog
// and makes each change DuckDB asks of the catalog happen once.
//
// DuckDB's HTTP client sends a request again when the answer is a 5xx, 408
// or 429 or does not come, POSTs included. A commit that the catalog
// applied and then answered with 503 (overloaded, or a load balancer in
// front of it giving up) was sent a second time; its requirements no
// longer held, since the table had moved on by that very commit, and the
// catalog refused it with 409. DuckDB took the commit for failed and
// deleted the data files it had written, which the committed snapshot
// referenced, and Igloo, seeing a conflict, ran the statement again: the
// rows landed twice, and the first copy could not be read.
//
// DuckDB's catalog requests go through a Proxy on the loopback interface,
// under a random path. Reads pass straight through. Each change (a POST or
// DELETE) is the proxy's to finish:
//
//   - It carries an Idempotency-Key, the same for every attempt. A catalog
//     that honours them, as Lakekeeper does, answers a repeat of a change it
//     applied with that change's success, and applies it no second time.
//   - The proxy, not DuckDB, sends it again: DuckDB never gets a 5xx or a
//     timeout for a change. When the catalog's answer is one, or none comes,
//     the proxy sends the change again, with the same key, until the
//     catalog says yes or no.
//   - DuckDB's own repeats of a change it gave up waiting for are joined to
//     the first, so they never reach the catalog, and get its outcome.
//   - A refused commit that adds snapshots is checked against the table:
//     if its snapshots are there, an attempt landed (one whose answer was
//     lost, or one sent again on the way without the proxy knowing), and
//     DuckDB hears that the commit succeeded. Any other change refused
//     after an attempt whose outcome was lost is sent once more with its
//     key, which the catalog answers with success if it was applied.
//   - A refused commit whose snapshots are not in the table never landed,
//     and never will, as its requirements no longer hold. If every attempt
//     at it was answered, the proxy deletes the files DuckDB wrote for it.
//     DuckDB itself deletes nothing, because Igloo attaches the catalog
//     with REMOVE_FILES_ON_DELETE false: when the proxy cannot learn a
//     commit's outcome in time it answers 502 CommitStateUnknownException
//     (which DuckDB does not retry, and Igloo reports as SQLSTATE 40003),
//     and the commit's files must stay, since it may have landed. Files of
//     commits that never landed are left for the orphan cleanup then.
//
// Before a change reaches the catalog, a Guard, if one is set, checks it
// (see guard.go): the proxy is where every change Igloo's SQL makes to
// the catalog passes, whatever the statement that made it.
package catalogproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	iceio "github.com/apache/iceberg-go/io"
)

// Default timings. DuckDB waits 30 seconds for an answer (http_timeout)
// and sends a request 3 more times (http_retries), so it gives up on a
// change after about two minutes; the proxy answers well within that.
const (
	defaultAttemptTimeout = 30 * time.Second
	defaultPatience       = 90 * time.Second
	defaultRemember       = 5 * time.Minute
)

const (
	// maxBody bounds a change DuckDB sends.
	maxBody = 64 << 20
	// maxResponse bounds the catalog's answer to one. Table metadata with
	// a long history can run to tens of megabytes.
	maxResponse = 256 << 20
	// The proxy waits between attempts, doubling up to maxBackoff.
	firstBackoff = 100 * time.Millisecond
	maxBackoff   = 5 * time.Second
)

// Options configure a Proxy.
type Options struct {
	// Catalog is the catalog's base URL (IGLOO_CATALOG_URI), such as
	// http://lakekeeper:8181/catalog.
	Catalog string
	// Files opens the object store that holds tables' files, to delete
	// those of commits the catalog refused. Nil deletes none.
	Files func(ctx context.Context) (iceio.IO, error)
	Log   *slog.Logger

	// AttemptTimeout bounds one request to the catalog, Patience how long
	// a change is tried, and Remember how long the outcome of a commit is
	// kept for DuckDB's repeats of it. Zero means the default; tests
	// shorten them.
	AttemptTimeout, Patience, Remember time.Duration
	// ListTTL is how long the proxy keeps the catalog's answers to
	// listings (see listings.go): zero means the default, a negative value
	// that it keeps none.
	ListTTL time.Duration
}

// Proxy forwards DuckDB's catalog requests.
type Proxy struct {
	opts   Options
	base   string
	secret string
	url    string
	srv    *http.Server
	http   *http.Client
	reads  *httputil.ReverseProxy
	log    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	changes map[[sha256.Size]byte]*change

	filesOnce sync.Once
	files     iceio.IO
	filesErr  error

	// guard checks changes (see SetGuard).
	guard atomic.Pointer[Guard]

	// lists are the listings the proxy keeps.
	lists *listings
	// names are how the catalog spells its tables' names (see names.go).
	names *names
	// data goes up whenever a table may have changed (see DataVersion).
	data dataVersion

	// prefix is the warehouse prefix DuckDB uses, and hide the namespaces
	// whose next existence check the proxy answers with 404 (see
	// namespaces.go), keyed by their names joined by \x1f; mu guards it.
	prefix atomic.Pointer[string]
	hide   map[string]bool
}

// New starts a proxy to the catalog on a random loopback port.
func New(opts Options) (*Proxy, error) {
	u, err := url.Parse(opts.Catalog)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("catalog proxy: %q is not an http(s) URL", opts.Catalog)
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = defaultAttemptTimeout
	}
	if opts.Patience <= 0 {
		opts.Patience = defaultPatience
	}
	if opts.Remember <= 0 {
		opts.Remember = defaultRemember
	}
	if opts.ListTTL == 0 {
		opts.ListTTL = defaultListTTL
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// DuckDB loads many tables at once.
	transport.MaxIdleConnsPerHost = 64

	secret := make([]byte, 16)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("catalog proxy: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{
		opts:    opts,
		base:    strings.TrimSuffix(opts.Catalog, "/"),
		secret:  hex.EncodeToString(secret),
		http:    &http.Client{Transport: transport},
		log:     opts.Log.With("component", "catalog-proxy"),
		ctx:     ctx,
		cancel:  cancel,
		changes: map[[sha256.Size]byte]*change{},
		hide:    map[string]bool{},
		lists:   newListings(opts.ListTTL),
		names:   newNames(opts.ListTTL),
	}
	p.url = "http://" + ln.Addr().String() + "/" + p.secret
	p.reads = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			target, _ := url.Parse(p.target(r.In))
			r.Out.URL = target
			r.Out.Host = ""
		},
		// DuckDB sends a read again on a 503, as it did when it reached
		// the catalog itself and could not.
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() == nil {
				p.log.Warn("catalog request failed", "method", r.Method, "path", p.rest(r), "err", err)
			}
			writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", "the catalog is not reachable")
		},
		ErrorLog: slog.NewLogLogger(p.log.Handler(), slog.LevelDebug),
	}
	p.srv = &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second,
		ErrorLog: slog.NewLogLogger(p.log.Handler(), slog.LevelDebug)}
	go func() {
		if err := p.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.log.Error("catalog proxy stopped", "err", err)
		}
	}()
	proxies.Store(p.url, p.base)
	return p, nil
}

// URL is the catalog's URL through the proxy, for DuckDB's ATTACH.
func (p *Proxy) URL() string { return p.url }

// Close stops the proxy. Changes still being tried end with an unknown
// outcome.
func (p *Proxy) Close() error {
	p.cancel()
	err := p.srv.Close()
	p.wg.Wait()
	proxies.Delete(p.url)
	return err
}

// proxies maps the URLs of this process's proxies to their catalogs'.
var proxies sync.Map

// Scrub replaces proxies' URLs in s, which DuckDB quotes in its errors,
// with their catalogs'.
func Scrub(s string) string {
	proxies.Range(func(k, v any) bool {
		s = strings.ReplaceAll(s, k.(string), v.(string))
		return true
	})
	return s
}

// rest is a request's path and query after the proxy's prefix, or "" if
// it is not under it.
func (p *Proxy) rest(r *http.Request) string {
	rest, ok := strings.CutPrefix(r.URL.EscapedPath(), "/"+p.secret+"/")
	if !ok {
		return ""
	}
	if r.URL.RawQuery != "" {
		rest += "?" + r.URL.RawQuery
	}
	return rest
}

func (p *Proxy) target(r *http.Request) string {
	return p.base + "/" + p.rest(r)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest := p.rest(r)
	if rest == "" {
		http.NotFound(w, r)
		return
	}
	p.notePrefix(rest)
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		if p.hidden(r, rest) {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", "the namespace does not exist")
			return
		}
		if r.Method == http.MethodGet && p.opts.ListTTL > 0 && listPath.MatchString(rest) {
			p.serveListing(w, r, rest)
			return
		}
		sw := &statusWriter{ResponseWriter: w}
		table := tableLoad(r, rest)
		if table != "" {
			sw.scan = &locationScanner{}
		}
		if r.Method == http.MethodGet {
			// Before DuckDB has a table it loaded, note whether it asked for
			// it by another name than the catalog's (see names.go).
			sw.before = func(status int) {
				if status == http.StatusOK {
					p.checkSpelling(r, rest)
				}
			}
		}
		p.reads.ServeHTTP(sw, r)
		p.lists.loaded(rest, sw.status)
		if table != "" && sw.status == http.StatusOK {
			location := sw.scan.location
			if enc := w.Header().Get("Content-Encoding"); enc != "" && enc != "identity" {
				location = ""
			}
			p.data.loaded(table, location)
		}
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "BadRequestException", "the request is too large")
		return
	}
	body = withDefaults(rest, body)
	// A change makes the listings kept stale: forgotten before it, so no
	// listing begun before it is kept, and after it, so none answered
	// while it was under way is.
	p.lists.forget()
	p.data.bump()
	c := p.begin(r, rest, body)
	select {
	case <-c.done:
		p.lists.forget()
		p.data.bump()
		c.res.write(w)
	case <-r.Context().Done():
		// DuckDB gave up waiting; it sends the change again, which joins
		// this one.
		go func() {
			<-c.done
			p.lists.forget()
			p.data.bump()
		}()
	}
}

// statusWriter notes the status a read was answered with, and calls before,
// if set, with it before the answer goes out; scan, if set, reads the
// answer as it passes.
type statusWriter struct {
	http.ResponseWriter
	status int
	before func(status int)
	scan   *locationScanner
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 && status >= http.StatusOK {
		w.status = status
		if w.before != nil {
			w.before(status)
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.scan != nil {
		w.scan.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// change is one change DuckDB asked for, however often it sent it.
type change struct {
	id     [sha256.Size]byte
	method string
	rest   string
	header http.Header
	body   []byte
	key    string
	commit *commit
	done   chan struct{}
	res    *result
	// releases are called once the change is through (see Verdict).
	releases []func()
}

// result is an answer for DuckDB.
type result struct {
	status int
	header http.Header
	body   []byte
}

func (r *result) write(w http.ResponseWriter) {
	for k, v := range r.header {
		w.Header()[k] = v
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body)
}

// begin returns the change r asks for: the one already under way if r
// repeats it, or a new one.
func (p *Proxy) begin(r *http.Request, rest string, body []byte) *change {
	h := sha256.New()
	h.Write([]byte(r.Method + " " + rest + "\x00"))
	h.Write(body)
	var id [sha256.Size]byte
	h.Sum(id[:0])

	p.mu.Lock()
	defer p.mu.Unlock()
	if c := p.changes[id]; c != nil {
		return c
	}
	key, err := uuid.NewV7()
	if err != nil {
		key = uuid.New()
	}
	c := &change{
		id: id, method: r.Method, rest: rest, header: forwardHeader(r.Header), body: body,
		key: key.String(), commit: parseCommit(rest, body), done: make(chan struct{}),
	}
	p.changes[id] = c
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		res := p.check(c)
		if res == nil {
			res = p.resolve(c)
			c.release()
		}
		p.names.changed(c.method, c.rest, c.body, res.status)
		p.mu.Lock()
		defer p.mu.Unlock()
		c.res = res
		close(c.done)
		// A repeat of a commit that adds a snapshot or a table is always
		// DuckDB's, as its snapshot or table ID is new, so its outcome is
		// kept for DuckDB's repeats that come late. Other changes can be
		// made again on purpose, and are joined only while under way.
		if c.commit.unique() {
			time.AfterFunc(p.opts.Remember, func() { p.forget(c) })
		} else {
			delete(p.changes, c.id)
		}
	}()
	return c
}

func (p *Proxy) forget(c *change) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.changes[c.id] == c {
		delete(p.changes, c.id)
	}
}

// resolve tries a change until the catalog applies or refuses it, and
// returns the answer for DuckDB.
func (p *Proxy) resolve(c *change) *result {
	ctx, cancel := context.WithTimeout(p.ctx, p.opts.Patience)
	defer cancel()
	log := p.log.With("method", c.method, "path", c.rest, "idempotency_key", c.key)

	// unsure is set once an attempt may have been applied without the
	// proxy hearing so; checked once a refusal after that was met with
	// one more attempt.
	unsure, checked := false, false
	wait := firstBackoff
	for attempt := 1; ; attempt++ {
		res, err := p.send(ctx, c)
		switch {
		case err == nil && !retryable(res):
			if res.status < 400 {
				if unsure {
					log.Info("the catalog applied a change after an attempt whose outcome was unknown", "attempt", attempt)
				}
				return res
			}
			if c.commit.addsSnapshots() {
				// Refused, but an attempt may have landed first: one
				// whose answer was lost, or one sent again on the way
				// without the proxy knowing (by a load balancer, say).
				landed, err := p.landed(ctx, c.commit)
				switch {
				case err == nil && landed != nil:
					log.Warn("the catalog refused a commit it had already applied; answering that it was applied",
						"attempt", attempt, "status", res.status)
					return landed
				case err == nil && !unsure:
					// Never applied, and never will be: the files DuckDB
					// wrote for it will never be read.
					p.discard(ctx, c, log)
					return res
				case err == nil, errors.Is(err, errTableGone):
					// Not applied, but an attempt's answer was lost, or
					// the table was dropped meanwhile (Lakekeeper can
					// restore dropped tables): the files are left to the
					// orphan cleanup.
					return res
				}
				unsure = true
				log.Warn("cannot tell whether a commit was applied", "err", err)
			} else if !unsure {
				return res
			} else if !checked {
				// With its key, a repeat of a change that was applied is
				// answered as one.
				checked = true
				continue
			} else {
				return res
			}
		case err != nil:
			unsure = true
			log.Warn("the catalog did not answer a change; sending it again", "attempt", attempt, "err", err)
		default:
			unsure = true
			log.Warn("the catalog failed a change; sending it again", "attempt", attempt, "status", res.status,
				"body", truncate(string(res.body), 500))
		}

		select {
		case <-ctx.Done():
			return p.giveUp(c, log)
		case <-time.After(wait/2 + mrand.N(wait/2+1)):
		}
		wait = min(2*wait, maxBackoff)
	}
}

// giveUp answers a change whose outcome the proxy could not learn in time.
func (p *Proxy) giveUp(c *change, log *slog.Logger) *result {
	if c.commit.addsSnapshots() && p.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(p.ctx, p.opts.AttemptTimeout)
		defer cancel()
		if landed, err := p.landed(ctx, c.commit); err == nil && landed != nil {
			log.Warn("the catalog applied a commit it did not confirm")
			return landed
		}
	}
	log.Error("the catalog did not confirm whether a change was applied; its outcome is unknown")
	return errorResult(http.StatusBadGateway, "CommitStateUnknownException",
		"the catalog did not confirm whether the change was applied; check before trying again")
}

// send makes one attempt at a change.
func (p *Proxy) send(ctx context.Context, c *change) (*result, error) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.AttemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, c.method, p.base+"/"+c.rest, bytes.NewReader(c.body))
	if err != nil {
		return nil, err
	}
	for k, v := range c.header {
		req.Header[k] = v
	}
	req.Header.Set("Idempotency-Key", c.key)
	// Go's transport sends a request with an Idempotency-Key again by
	// itself when a reused connection fails, which would hide from the
	// proxy an attempt whose outcome is unknown. It cannot without a way
	// to read the body again.
	req.GetBody = nil
	return p.do(req)
}

// get reads from the catalog.
func (p *Proxy) get(ctx context.Context, rest string) (*result, error) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.AttemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/"+rest, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return p.do(req)
}

func (p *Proxy) do(req *http.Request) (*result, error) {
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxResponse {
		return nil, fmt.Errorf("the catalog's answer is larger than %d bytes", maxResponse)
	}
	h := http.Header{}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	}
	return &result{status: resp.StatusCode, header: h, body: body}, nil
}

// retryable reports whether an answer leaves the change's outcome open, or
// asks for it to be sent again.
func retryable(r *result) bool {
	switch {
	case r.status >= 500, r.status == http.StatusRequestTimeout, r.status == http.StatusTooManyRequests:
		return true
	case r.status == http.StatusConflict:
		// Lakekeeper is applying another attempt with the same key.
		return errorType(r.body) == "RequestInProgress"
	}
	return false
}

// errTableGone is landed's error for a commit to a table that is no more.
var errTableGone = errors.New("the table does not exist")

// landed checks whether the snapshots a commit adds are in its tables. It
// returns the answer for DuckDB if they are, nil if none is, and an error
// if it cannot tell.
func (p *Proxy) landed(ctx context.Context, c *commit) (*result, error) {
	found, missing := 0, 0
	var single *result
	for _, t := range c.tables {
		if len(t.snapshots) == 0 {
			continue
		}
		res, err := p.get(ctx, t.path)
		if err != nil {
			return nil, err
		}
		if res.status == http.StatusNotFound {
			if !t.creates {
				return nil, fmt.Errorf("%s: %w", t.path, errTableGone)
			}
			missing += len(t.snapshots)
			continue
		}
		if res.status != http.StatusOK {
			return nil, fmt.Errorf("loading table %s: HTTP %d", t.path, res.status)
		}
		var load struct {
			MetadataLocation string          `json:"metadata-location"`
			Metadata         json.RawMessage `json:"metadata"`
		}
		var meta struct {
			Snapshots []struct {
				ID int64 `json:"snapshot-id"`
			} `json:"snapshots"`
		}
		if err := json.Unmarshal(res.body, &load); err != nil {
			return nil, fmt.Errorf("loading table %s: %w", t.path, err)
		}
		if err := json.Unmarshal(load.Metadata, &meta); err != nil {
			return nil, fmt.Errorf("loading table %s: %w", t.path, err)
		}
		ids := map[int64]bool{}
		for _, s := range meta.Snapshots {
			ids[s.ID] = true
		}
		for _, s := range t.snapshots {
			if ids[s.id] {
				found++
			} else {
				missing++
			}
		}
		if c.single {
			body, err := json.Marshal(map[string]any{"metadata-location": load.MetadataLocation, "metadata": load.Metadata})
			if err != nil {
				return nil, err
			}
			single = &result{status: http.StatusOK, header: http.Header{"Content-Type": {"application/json"}}, body: body}
		}
	}
	switch {
	case found > 0 && missing > 0:
		return nil, errors.New("some of the commit's snapshots are in its tables and some are not")
	case found == 0:
		return nil, nil
	case single != nil:
		return single, nil
	}
	return &result{status: http.StatusNoContent, header: http.Header{}}, nil
}

// discard deletes the files DuckDB wrote for a commit the catalog
// refused: its data files, manifests and manifest lists. It deletes only
// files under the table's location, listed as added by the commit's own
// snapshots.
func (p *Proxy) discard(ctx context.Context, c *change, log *slog.Logger) {
	if !c.commit.addsSnapshots() || p.opts.Files == nil {
		return
	}
	p.filesOnce.Do(func() { p.files, p.filesErr = p.opts.Files(context.WithoutCancel(p.ctx)) })
	if p.filesErr != nil {
		log.Warn("cannot delete a refused commit's files; the orphan cleanup will", "err", p.filesErr)
		return
	}
	for _, t := range c.commit.tables {
		if len(t.snapshots) == 0 {
			continue
		}
		location := t.location
		if location == "" {
			res, err := p.get(ctx, t.path)
			if err != nil || res.status != http.StatusOK {
				log.Warn("cannot delete a refused commit's files; the orphan cleanup will", "table", t.path, "err", err)
				continue
			}
			var load struct {
				Metadata struct {
					Location string `json:"location"`
				} `json:"metadata"`
			}
			if json.Unmarshal(res.body, &load) != nil || load.Metadata.Location == "" {
				continue
			}
			location = load.Metadata.Location
		}
		var removed int
		for _, s := range t.snapshots {
			files, err := addedFiles(p.files, location, s)
			if err != nil {
				log.Warn("cannot list a refused commit's files; the orphan cleanup will delete them", "err", err)
				continue
			}
			for _, f := range files {
				if err := p.files.Remove(f); err != nil && !errors.Is(err, iofs.ErrNotExist) {
					log.Warn("cannot delete a refused commit's file; the orphan cleanup will", "file", f, "err", err)
					continue
				}
				removed++
			}
		}
		log.Debug("deleted a refused commit's files", "table", t.path, "files", removed)
	}
}

// commit is what the proxy needs to know of a commit.
type commit struct {
	// single is set for a commit to one table's path, which is answered
	// with the table's metadata; a transaction's is answered with no body.
	single bool
	tables []tableCommit
}

type tableCommit struct {
	// path is the table's API path under the catalog's URL.
	path string
	// creates is set for a commit that makes the table (assert-create).
	creates bool
	// newUUID is set for a commit that assigns the table a UUID.
	newUUID bool
	// location is the table's location if the commit sets it.
	location  string
	snapshots []snapshot
}

type snapshot struct {
	id           int64
	manifestList string
}

// commitPath matches the paths commits are posted to: a table's, or the
// warehouse's transactions/commit.
var commitPath = regexp.MustCompile(`^(v1/[^/?]+/)(?:namespaces/[^/?]+/tables/[^/?]+|transactions/commit)(?:\?|$)`)

// parseCommit reads a commit, or returns nil for another change.
func parseCommit(rest string, body []byte) *commit {
	m := commitPath.FindStringSubmatch(rest)
	if m == nil {
		return nil
	}
	type change struct {
		Identifier *struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifier"`
		Requirements []struct {
			Type string `json:"type"`
		} `json:"requirements"`
		Updates []struct {
			Action   string `json:"action"`
			Location string `json:"location"`
			Snapshot *struct {
				ID           int64  `json:"snapshot-id"`
				ManifestList string `json:"manifest-list"`
			} `json:"snapshot"`
		} `json:"updates"`
	}
	var changes []change
	c := &commit{}
	if path, _, _ := strings.Cut(rest, "?"); strings.HasSuffix(path, "/transactions/commit") {
		var tx struct {
			TableChanges []change `json:"table-changes"`
		}
		if json.Unmarshal(body, &tx) != nil {
			return nil
		}
		changes = tx.TableChanges
	} else {
		var ch change
		if json.Unmarshal(body, &ch) != nil {
			return nil
		}
		changes = []change{ch}
		c.single = true
	}

	for _, ch := range changes {
		var t tableCommit
		if c.single {
			t.path, _, _ = strings.Cut(rest, "?")
		} else {
			if ch.Identifier == nil {
				return nil
			}
			t.path = m[1] + "namespaces/" + url.PathEscape(strings.Join(ch.Identifier.Namespace, "\x1f")) +
				"/tables/" + url.PathEscape(ch.Identifier.Name)
		}
		for _, r := range ch.Requirements {
			t.creates = t.creates || r.Type == "assert-create"
		}
		for _, u := range ch.Updates {
			switch u.Action {
			case "add-snapshot":
				if u.Snapshot != nil {
					t.snapshots = append(t.snapshots, snapshot{id: u.Snapshot.ID, manifestList: u.Snapshot.ManifestList})
				}
			case "assign-uuid":
				t.newUUID = true
			case "set-location":
				t.location = u.Location
			}
		}
		c.tables = append(c.tables, t)
	}
	return c
}

// addsSnapshots reports whether c is a commit that adds snapshots.
func (c *commit) addsSnapshots() bool {
	if c == nil {
		return false
	}
	for _, t := range c.tables {
		if len(t.snapshots) > 0 {
			return true
		}
	}
	return false
}

// unique reports whether c is a commit no other can repeat: it adds a
// snapshot, whose ID is random, or assigns a new table's UUID.
func (c *commit) unique() bool {
	if c == nil {
		return false
	}
	for _, t := range c.tables {
		if len(t.snapshots) > 0 || t.newUUID {
			return true
		}
	}
	return false
}

// hopHeaders are not forwarded.
var hopHeaders = []string{
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length", "Accept-Encoding", "Idempotency-Key",
}

func forwardHeader(in http.Header) http.Header {
	out := in.Clone()
	for _, h := range strings.Split(in.Get("Connection"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			out.Del(h)
		}
	}
	for _, h := range hopHeaders {
		out.Del(h)
	}
	return out
}

func errorType(body []byte) string {
	var m struct {
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &m)
	return m.Error.Type
}

func errorResult(status int, typ, msg string) *result {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": status}})
	return &result{status: status, header: http.Header{"Content-Type": {"application/json"}}, body: body}
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	errorResult(status, typ, msg).write(w)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
