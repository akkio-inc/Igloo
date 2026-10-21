package catalogproxy

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Listings. DuckDB lists the catalog's namespaces, and their tables, again
// in each transaction that looks a table up or lists the catalog
// (duckdb_tables(), information_schema, SHOW), and each statement outside
// a transaction block is a transaction: a catalog of hundreds of tables
// answers such a listing in tens of milliseconds, and Igloo's checks list
// the catalog for every statement of a user whose role does not read every
// table. So the proxy keeps the catalog's answers to listings for a few
// seconds (Options.ListTTL), and forgets them:
//
//   - when a change goes through it (a POST or DELETE: a table or view
//     created, dropped or renamed, a namespace, a commit), and when Igloo
//     changes the catalog another way and says so (Invalidate);
//   - when DuckDB loads a table, view or namespace by name that the
//     listings kept do not have, or that they have and the catalog no longer
//     does: another engine or replica made or dropped it.
//
// DuckDB loads a name it does not find in a listing from the catalog itself
// before it gives up on it, so it never binds a name to what a kept listing
// says, and a statement's checks, which list the catalog after DuckDB has
// bound the statement, see the table it bound. Only a listing's answer 200
// is kept, and not when a change overtook the request for it.

// defaultListTTL is how long the proxy keeps a listing.
const defaultListTTL = 10 * time.Second

// listPath matches the requests that list the catalog: its namespaces (or
// a namespace's children), and a namespace's tables or views.
var listPath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces(?:/([^/?]+)/(tables|views))?(?:\?|$)`)

// loadPath matches the requests that load (or check, with HEAD) one table,
// view or namespace.
var loadPath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/([^/?]+)(?:/(tables|views)/([^/?]+))?(?:\?|$)`)

// listings are the listings the proxy keeps.
type listings struct {
	ttl time.Duration

	mu sync.Mutex
	// gen counts the times the listings were forgotten; a listing is kept
	// only under the gen its request began in.
	gen  uint64
	kept map[string]*listing
	// renewed counts the listings asked for again, once they expired, that
	// listed other names than before (see Generation).
	renewed uint64
}

// listing is a kept answer to one request: rest, its path and query.
type listing struct {
	at     time.Time
	header http.Header
	body   []byte
	// kind is "namespaces", "tables" or "views", and namespace the one
	// whose tables or views it lists, or the parent of the namespaces.
	kind, namespace string
	// names are what it lists, lower-cased: namespaces' full names (their
	// levels joined by \x1f, as in a path), or tables' or views' names.
	names map[string]bool
}

func newListings(ttl time.Duration) *listings {
	return &listings{ttl: ttl, kept: map[string]*listing{}}
}

// forget drops every kept listing.
func (l *listings) forget() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gen++
	clear(l.kept)
}

// lookup returns the kept answer to rest, if it is fresh, or else the gen
// a request for it begins in.
func (l *listings) lookup(rest string) (*listing, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if k := l.kept[rest]; k != nil && time.Since(k.at) < l.ttl {
		return k, l.gen
	}
	return nil, l.gen
}

// keep keeps res as the answer to rest, unless the listings were forgotten
// since gen.
func (l *listings) keep(rest string, gen uint64, res *result) {
	m := listPath.FindStringSubmatch(rest)
	if m == nil || res.status != http.StatusOK {
		return
	}
	k := &listing{at: time.Now(), header: res.header, body: res.body, names: map[string]bool{}}
	if m[2] == "" {
		k.kind = "namespaces"
		if u, err := url.Parse(rest); err == nil {
			k.namespace = strings.ToLower(u.Query().Get("parent"))
		}
		var list struct {
			Namespaces [][]string `json:"namespaces"`
		}
		if json.Unmarshal(res.body, &list) != nil {
			return
		}
		for _, ns := range list.Namespaces {
			k.names[strings.ToLower(strings.Join(ns, "\x1f"))] = true
		}
	} else {
		ns, err := url.PathUnescape(m[1])
		if err != nil {
			return
		}
		k.kind, k.namespace = m[2], strings.ToLower(ns)
		var list struct {
			Identifiers []struct {
				Name string `json:"name"`
			} `json:"identifiers"`
		}
		if json.Unmarshal(res.body, &list) != nil {
			return
		}
		for _, id := range list.Identifiers {
			k.names[strings.ToLower(id.Name)] = true
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.gen != gen {
		return
	}
	if old := l.kept[rest]; old != nil && !maps.Equal(old.names, k.names) {
		l.renewed++
	}
	l.kept[rest] = k
}

// loaded learns from the catalog's answer, status, to a request that
// loaded or checked one table, view or namespace by name (rest): if the
// kept listings say otherwise, they are stale, and forgotten.
func (l *listings) loaded(rest string, status int) {
	m := loadPath.FindStringSubmatch(rest)
	if m == nil {
		return
	}
	var exists bool
	switch status {
	case http.StatusOK, http.StatusNoContent:
		exists = true
	case http.StatusNotFound:
	default:
		return
	}
	ns, err := url.PathUnescape(m[1])
	if err != nil {
		return
	}
	ns = strings.ToLower(ns)
	kind, name := m[2], ""
	if kind == "" {
		// A namespace: listed by its parent's listing.
		kind, name = "namespaces", ns
		if i := strings.LastIndexByte(ns, '\x1f'); i >= 0 {
			ns = ns[:i]
		} else {
			ns = ""
		}
	} else if name, err = url.PathUnescape(m[3]); err != nil {
		return
	}
	name = strings.ToLower(name)

	l.mu.Lock()
	listed, seen := false, false
	for _, k := range l.kept {
		if k.kind == kind && k.namespace == ns {
			seen = true
			listed = listed || k.names[name]
		}
	}
	stale := seen && listed != exists
	l.mu.Unlock()
	if stale {
		l.forget()
	}
}

// Generation changes whenever what DuckDB's listings of the catalog list
// may have changed: when the proxy forgets the listings it keeps, and when
// one it asks for again, once it expired, lists other names. What Igloo
// works out from DuckDB's listings holds while it stays the same.
func (p *Proxy) Generation() uint64 {
	if p == nil {
		return 0
	}
	p.lists.mu.Lock()
	defer p.lists.mu.Unlock()
	return p.lists.gen + p.lists.renewed
}

// Invalidate makes the proxy forget the listings it keeps. Igloo calls it
// when it changes the catalog other than through DuckDB, as when it
// creates a view, or another engine does through Igloo's Iceberg REST
// endpoint.
func (p *Proxy) Invalidate() {
	if p != nil {
		p.lists.forget()
	}
}

// serveListing answers a request that lists the catalog, from a kept
// listing if there is one.
func (p *Proxy) serveListing(w http.ResponseWriter, r *http.Request, rest string) {
	k, gen := p.lists.lookup(rest)
	if k != nil {
		(&result{status: http.StatusOK, header: k.header, body: k.body}).write(w)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, p.target(r), nil)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", "the catalog is not reachable")
		return
	}
	req.Header = forwardHeader(r.Header)
	// The transport asks for, and undoes, compression itself; the answer
	// is kept, and written, as plain JSON.
	req.Header.Del("Accept-Encoding")
	res, err := p.do(req)
	if err != nil {
		if r.Context().Err() == nil {
			p.log.Warn("catalog request failed", "method", r.Method, "path", rest, "err", err)
		}
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", "the catalog is not reachable")
		return
	}
	p.lists.keep(rest, gen, res)
	res.write(w)
}
