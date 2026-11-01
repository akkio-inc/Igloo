package icebergtest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"regexp"
	"sync"
)

// Fault is what Faults does to a commit.
type Fault int

const (
	// Pass forwards the commit and its answer.
	Pass Fault = iota
	// FailAfterApply forwards the commit and, once the catalog has applied
	// it, answers 503 Service Unavailable instead, as an overloaded catalog
	// or a load balancer that gave up waiting for it does.
	FailAfterApply
	// FailBeforeApply answers 503 without forwarding the commit.
	FailBeforeApply
	// DropAfterApply forwards the commit and, once the catalog has applied
	// it, closes the connection without an answer, as a client sees a
	// request time out.
	DropAfterApply
	// HangAfterApply forwards the commit and, once the catalog has applied
	// it, answers nothing until the client gives up.
	HangAfterApply
	// DownAfterApply forwards the commit and, once the catalog has applied
	// it, answers 503 to it and to every request after it, until Up.
	DownAfterApply
)

// Faults forwards requests to an Iceberg REST catalog and does the faults
// it is armed with to the commits that pass, one fault per commit, and to
// the table drops and renames, which are armed apart.
type Faults struct {
	// URL is the base URL to use in place of the catalog's.
	URL string

	srv   *httptest.Server
	proxy *httputil.ReverseProxy

	mu    sync.Mutex
	armed []Fault
	// drops and renames are the faults for the next table drops and
	// renames.
	drops, renames []Fault
	down           bool
	// before, if set, runs before the next commit is forwarded.
	before  func()
	commits []string
	keys    []string
}

// commitPath matches the paths commits are posted to: a table's, or the
// warehouse's transactions/commit.
var commitPath = regexp.MustCompile(`/v1/[^/]+/(namespaces/[^/]+/tables/[^/]+|transactions/commit)$`)

// tablePath and renamePath match the paths of a table, which a drop
// deletes, and of renames.
var (
	tablePath  = regexp.MustCompile(`/v1/[^/]+/namespaces/[^/]+/tables/[^/]+$`)
	renamePath = regexp.MustCompile(`/v1/[^/]+/tables/rename$`)
)

// NewFaults forwards to the catalog at upstream, a base URL such as
// http://lakekeeper:8181/catalog. Close stops it.
func NewFaults(upstream string) (*Faults, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	f := &Faults{proxy: httputil.NewSingleHostReverseProxy(u)}
	f.srv = httptest.NewServer(f)
	f.URL = f.srv.URL
	return f, nil
}

// Close stops the proxy.
func (f *Faults) Close() { f.srv.Close() }

// Arm sets the faults for the next commits, in order.
func (f *Faults) Arm(faults ...Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = append(f.armed, faults...)
}

// ArmDrops and ArmRenames set the faults for the next table drops and
// renames, in order, as Arm does for commits.
func (f *Faults) ArmDrops(faults ...Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drops = append(f.drops, faults...)
}

func (f *Faults) ArmRenames(faults ...Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renames = append(f.renames, faults...)
}

// Up ends DownAfterApply's outage.
func (f *Faults) Up() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = false
}

// Before runs fn, once, when the next commit arrives and before it is
// forwarded. Commits fn makes are not counted as the next one.
func (f *Faults) Before(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.before = fn
}

// Commits returns the bodies of the commits that arrived, in order.
func (f *Faults) Commits() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commits...)
}

// Keys returns the Idempotency-Key headers of the commits that arrived.
func (f *Faults) Keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.keys...)
}

func (f *Faults) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	down := f.down
	f.mu.Unlock()
	if down {
		unavailable(w)
		return
	}
	var armed *[]Fault
	switch {
	case r.Method == http.MethodPost && renamePath.MatchString(r.URL.Path):
		armed = &f.renames
	case r.Method == http.MethodPost && commitPath.MatchString(r.URL.Path):
		armed = &f.armed
	case r.Method == http.MethodDelete && tablePath.MatchString(r.URL.Path):
		armed = &f.drops
	default:
		f.proxy.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	f.mu.Lock()
	var before func()
	if armed == &f.armed {
		before, f.before = f.before, nil
		f.commits = append(f.commits, string(body))
		f.keys = append(f.keys, r.Header.Get("Idempotency-Key"))
	}
	fault := Pass
	if len(*armed) > 0 {
		fault, *armed = (*armed)[0], (*armed)[1:]
	}
	f.mu.Unlock()
	if before != nil {
		before()
	}

	switch fault {
	case Pass:
		f.proxy.ServeHTTP(w, r)
		return
	case FailBeforeApply:
		unavailable(w)
		return
	}

	rec := httptest.NewRecorder()
	f.proxy.ServeHTTP(rec, r)
	if rec.Code >= 300 {
		// Not applied: pass the refusal on.
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}
	switch fault {
	case FailAfterApply:
		unavailable(w)
	case DownAfterApply:
		f.mu.Lock()
		f.down = true
		f.mu.Unlock()
		unavailable(w)
	case DropAfterApply:
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
	case HangAfterApply:
		<-r.Context().Done()
	}
}

func unavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, `{"error": {"message": "the server is overloaded", "type": "ServiceUnavailable", "code": 503}}`)
}
