package sharing

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DuckDB reads external tables' data files through Proxy, an HTTP server on
// the loopback interface. The query sandbox lets SQL reach only the
// warehouse and the allowed locations, and cannot be loosened once locked,
// so it gets the proxy's address, with a random path prefix, before it is
// locked. The proxy serves only files a provider listed for an external
// table, under keys derived from a secret, and forwards each request to that
// file's URL: SQL gains no way to reach any other URL. Providers' URLs
// expire, so when one has, the proxy lists the table's files again and
// retries.

// Proxy serves external tables' data files to DuckDB.
type Proxy struct {
	ln     net.Listener
	srv    *http.Server
	secret string
	key    []byte
	prefix string
	http   *http.Client
	dialer *guardedDialer
	log    *slog.Logger

	mu    sync.RWMutex
	files map[string]*proxyFile
	// refresh lists an external table's files again; the consumer sets it.
	refresh func(ctx context.Context, table string) error
}

// proxyFile is a file the proxy may serve.
type proxyFile struct {
	// table is the external table it belongs to.
	table   string
	url     string
	size    int64
	expires time.Time
	// added is reported as the file's modification time.
	added time.Time
	// used is when DuckDB last read the file, in Unix nanoseconds.
	used int64
}

// NewProxy listens on a random loopback port. Its Prefix must be among
// DuckDB's allowed directories before the sandbox is locked.
func NewProxy(log *slog.Logger) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("sharing proxy: %w", err)
	}
	secret := make([]byte, 16)
	key := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	dialer := newGuardedDialer()
	p := &Proxy{
		ln:     ln,
		secret: hex.EncodeToString(secret),
		key:    key,
		http:   newHTTPClient(dialer),
		dialer: dialer,
		log:    log.With("component", "sharing-proxy"),
		files:  map[string]*proxyFile{},
	}
	p.prefix = "http://" + ln.Addr().String() + "/" + p.secret + "/"
	p.srv = &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second}
	return p, nil
}

// Prefix is the URL prefix of every file the proxy serves.
func (p *Proxy) Prefix() string { return p.prefix }

// Serve serves DuckDB's requests until Close.
func (p *Proxy) Serve() error {
	err := p.srv.Serve(p.ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close stops the proxy.
func (p *Proxy) Close() error { return p.srv.Close() }

// fileKey names a file in proxy URLs: stable for the same file of the same
// table, unguessable without the proxy's key.
func (p *Proxy) fileKey(table, fileID string) string {
	m := hmac.New(sha256.New, p.key)
	m.Write([]byte(table))
	m.Write([]byte{0})
	m.Write([]byte(fileID))
	return hex.EncodeToString(m.Sum(nil)[:16])
}

// register makes a table's files servable, with their current URLs, and
// returns the URL DuckDB reads each from. Keys of files the table no longer
// lists stay until their URLs expire, for queries that started before.
// partitions are the table's partition columns, which go into the URL as
// Hive-style directories, as read_parquet expects.
func (p *Proxy) register(table string, files []remoteFile, partitions []string) []string {
	urls := make([]string, len(files))
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, f := range p.files {
		if now.After(f.expires) {
			delete(p.files, k)
		}
	}
	tableKey := p.fileKey(table, "")[:16]
	for i, f := range files {
		key := p.fileKey(table, f.ID)
		old, ok := p.files[key]
		added := now
		if ok {
			added = old.added
		}
		p.files[key] = &proxyFile{table: table, url: f.URL, size: f.Size, expires: f.Expires, added: added}
		var dirs strings.Builder
		for _, col := range partitions {
			// The protocol writes a null partition value as "".
			v := f.PartitionValues[col]
			value := "__HIVE_DEFAULT_PARTITION__"
			if v != nil && *v != "" {
				value = hiveEscape(*v)
			}
			dirs.WriteString(hiveEscape(col) + "=" + value + "/")
		}
		urls[i] = p.prefix + tableKey + "/" + dirs.String() + key + ".parquet"
	}
	return urls
}

// hiveEscape escapes a partition column or value for a Hive-style
// directory name, which DuckDB decodes.
func hiveEscape(s string) string {
	return strings.ReplaceAll(url.PathEscape(s), "=", "%3D")
}

// forget stops serving a table's files.
func (p *Proxy) forget(table string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, f := range p.files {
		if f.table == table {
			delete(p.files, k)
		}
	}
}

// lastUsed returns when DuckDB last read one of the table's files.
func (p *Proxy) lastUsed(table string) time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var last int64
	for _, f := range p.files {
		if f.table == table && f.used > last {
			last = f.used
		}
	}
	return time.Unix(0, last)
}

func (p *Proxy) lookup(key string) (proxyFile, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.files[key]
	if !ok {
		return proxyFile{}, false
	}
	f.used = time.Now().UnixNano()
	return *f, true
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/"+p.secret+"/")
	if !ok || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
		http.NotFound(w, r)
		return
	}
	key := strings.TrimSuffix(rest[strings.LastIndexByte(rest, '/')+1:], ".parquet")
	f, ok := p.lookup(key)
	if !ok {
		http.Error(w, "the shared table changed while the query ran; run it again", http.StatusNotFound)
		return
	}
	h := w.Header()
	h.Set("ETag", `"`+key+`"`)
	h.Set("Last-Modified", f.added.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Type", "application/octet-stream")
	if r.Method == http.MethodHead {
		h.Set("Content-Length", strconv.FormatInt(f.size, 10))
		return
	}

	resp, err := p.fetch(r.Context(), f.url, r.Header.Get("Range"))
	if err == nil && (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized ||
		resp.StatusCode == http.StatusBadRequest) && p.refresh != nil {
		// Most likely the URL expired: list the files again and retry.
		resp.Body.Close() //nolint:errcheck
		if err = p.refresh(r.Context(), f.table); err == nil {
			if f, ok = p.lookup(key); !ok {
				http.Error(w, "the shared table changed while the query ran; run it again", http.StatusNotFound)
				return
			}
			resp, err = p.fetch(r.Context(), f.url, r.Header.Get("Range"))
		}
	}
	if err != nil {
		p.log.Warn("could not read a shared data file", "table", f.table, "err", err)
		http.Error(w, "could not read the shared data file", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		p.log.Warn("the provider refused a data file", "table", f.table, "status", resp.Status)
		http.Error(w, "the provider refused the shared data file: "+resp.Status, http.StatusBadGateway)
		return
	}
	for _, name := range []string{"Content-Length", "Content-Range"} {
		if v := resp.Header.Get(name); v != "" {
			h.Set(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) fetch(ctx context.Context, fileURL, rangeHeader string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, err
	}
	if req.URL.Scheme != "https" && req.URL.Scheme != "http" {
		return nil, fmt.Errorf("unsupported file URL scheme %q", req.URL.Scheme)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			// Pre-signed URLs carry credentials; keep them out of logs.
			err = ue.Err
		}
		return nil, err
	}
	return resp, nil
}
