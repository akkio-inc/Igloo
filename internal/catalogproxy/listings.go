package catalogproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
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
//   - when a change that may make, drop or rename a name goes through it:
//     the listings of the namespaces it changes, and those of namespaces
//     when it makes or drops one (see staleListings). A commit to a table
//     that is there, as each INSERT, MERGE and maintenance pass makes,
//     forgets none. When Igloo changes the catalog another way it says so
//     (Invalidate), and they are all forgotten;
//   - when DuckDB loads a table, view or namespace by name that the
//     listings of its namespace do not have, or have and the catalog no
//     longer does: another engine or replica made or dropped it. Those
//     listings are forgotten.
//
// A listing asked for once half its time is up is asked of the catalog again
// in the background, so one asked for often never waits for the catalog.
// DuckDB walks the catalog with the catalog lock held alone (see holds.go),
// and lists each namespace's tables as it walks: forgetting every listing at
// each change, and asking for each one again as it expired, had a walk of 77
// namespaces hold the lock for 7.7 s while every dashboard waited.
//
// DuckDB loads a name it does not find in a listing from the catalog itself
// before it gives up on it, so it never binds a name to what a kept listing
// says, and a statement's checks, which list the catalog after DuckDB has
// bound the statement, see the table it bound. Only a listing's answer 200
// is kept, and not when a change overtook the request for it.

// listScope names kept listings: those of a kind ("namespaces", "tables" or
// "views") of a namespace, lower-cased as listing.namespace is; for
// "namespaces", of every parent.
type listScope struct{ kind, namespace string }

var (
	// registerTablePath matches the requests that register a table,
	// viewsPath those that create, replace or drop a view, and
	// renameViewPath those that rename one.
	registerTablePath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/([^/?]+)/register(?:\?|$)`)
	viewsPath         = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/([^/?]+)/views(?:/[^/?]+)?(?:\?|$)`)
	renameViewPath    = regexp.MustCompile(`^v1/(?:[^/?]+/)?views/rename(?:\?|$)`)
)

// staleListings returns the kept listings that a change, method on rest with
// body, may make stale, or all when it may make any so, as a change the proxy
// does not know may. A create, drop or rename of a table or view makes those
// of its namespaces stale, and a namespace made or dropped those of
// namespaces; a commit to a table that is there, or a namespace's properties
// set, none. A commit that asserts its table does not exist yet creates it.
func staleListings(method, rest string, body []byte) (scopes []listScope, all bool) {
	path, _, _ := strings.Cut(rest, "?")
	post, del := method == http.MethodPost, method == http.MethodDelete
	of := func(kind, ns string) listScope { return listScope{kind, strings.ToLower(unescape(ns))} }
	type ident struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	}
	in := func(kind string, id *ident) listScope {
		return listScope{kind, strings.ToLower(strings.Join(id.Namespace, "\x1f"))}
	}
	switch {
	case post && (metricsPath.MatchString(path) || namespacePropertiesPath.MatchString(path)):
		return nil, false
	case post && namespacesPath.MatchString(path):
		return []listScope{{kind: "namespaces"}}, false
	case del && namespaceCheckPath.MatchString(rest):
		ns := namespaceCheckPath.FindStringSubmatch(rest)[1]
		return []listScope{{kind: "namespaces"}, of("tables", ns), of("views", ns)}, false
	case (post || del) && loadTablePath.MatchString(rest):
		if post && !bytes.Contains(body, []byte(`"assert-create"`)) {
			return nil, false
		}
		return []listScope{of("tables", loadTablePath.FindStringSubmatch(rest)[2])}, false
	case post && createTablePath.MatchString(rest):
		return []listScope{of("tables", createTablePath.FindStringSubmatch(rest)[1])}, false
	case post && registerTablePath.MatchString(rest):
		return []listScope{of("tables", registerTablePath.FindStringSubmatch(rest)[1])}, false
	case (post || del) && viewsPath.MatchString(rest):
		return []listScope{of("views", viewsPath.FindStringSubmatch(rest)[1])}, false
	case post && (renameTablePath.MatchString(rest) || renameViewPath.MatchString(rest)):
		var c struct {
			Source      *ident `json:"source"`
			Destination *ident `json:"destination"`
		}
		if json.Unmarshal(body, &c) != nil || c.Source == nil || c.Destination == nil {
			return nil, true
		}
		kind := "tables"
		if renameViewPath.MatchString(rest) {
			kind = "views"
		}
		return []listScope{in(kind, c.Source), in(kind, c.Destination)}, false
	case post && strings.HasSuffix(path, "/transactions/commit"):
		var tx struct {
			TableChanges []struct {
				Identifier   *ident `json:"identifier"`
				Requirements []struct {
					Type string `json:"type"`
				} `json:"requirements"`
			} `json:"table-changes"`
		}
		if json.Unmarshal(body, &tx) != nil {
			return nil, true
		}
		for _, c := range tx.TableChanges {
			if c.Identifier == nil {
				return nil, true
			}
			for _, r := range c.Requirements {
				if r.Type == "assert-create" {
					scopes = append(scopes, in("tables", c.Identifier))
					break
				}
			}
		}
		return scopes, false
	}
	return nil, true
}

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
	// gen counts the times listings were forgotten; a listing is kept only
	// under the gen its request began in.
	gen  uint64
	kept map[string]*listing
	// renewing are the listings being asked for again in the background.
	renewing map[string]bool
	// renewed counts the listings asked for again, once they expired or in
	// the background, that listed other names than before (see Generation).
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
	return &listings{ttl: ttl, kept: map[string]*listing{}, renewing: map[string]bool{}}
}

// forget drops every kept listing.
func (l *listings) forget() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gen++
	clear(l.kept)
}

// forgetSome drops the kept listings of scopes.
func (l *listings) forgetSome(scopes []listScope) {
	if len(scopes) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gen++
	for rest, k := range l.kept {
		if slices.ContainsFunc(scopes, func(s listScope) bool {
			return k.kind == s.kind && (s.kind == "namespaces" || k.namespace == s.namespace)
		}) {
			delete(l.kept, rest)
		}
	}
}

// renew reports whether the caller is to ask the catalog for rest again, in
// the background: its kept listing k is half way to expiring, and nobody
// asks for it yet. renewEnded is to be called when that ends.
func (l *listings) renew(rest string, k *listing) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(k.at) < l.ttl/2 || l.renewing[rest] {
		return false
	}
	l.renewing[rest] = true
	return true
}

func (l *listings) renewEnded(rest string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.renewing, rest)
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
		l.forgetSome([]listScope{{kind, ns}})
	}
}

// Generation changes whenever what DuckDB's listings of the catalog list
// may have changed: when the proxy forgets listings it keeps, and when one
// it asks for again lists other names. What Igloo
// works out from DuckDB's listings holds while it stays the same.
func (p *Proxy) Generation() uint64 {
	if p == nil {
		return 0
	}
	p.lists.mu.Lock()
	defer p.lists.mu.Unlock()
	return p.lists.gen + p.lists.renewed
}

// Invalidate makes the proxy forget the listings it keeps, and raises the
// data version (see DataVersion). Igloo calls it when it changes the
// catalog other than through DuckDB, as when it creates a view, or another
// engine does through Igloo's Iceberg REST endpoint.
func (p *Proxy) Invalidate() {
	if p != nil {
		p.lists.forget()
		p.data.bump()
	}
}

// serveListing answers a request that lists the catalog, from a kept
// listing if there is one.
func (p *Proxy) serveListing(w http.ResponseWriter, r *http.Request, rest string) {
	k, gen := p.lists.lookup(rest)
	if k != nil {
		if p.lists.renew(rest, k) {
			p.renewListing(r, rest, gen)
		}
		(&result{status: http.StatusOK, header: k.header, body: k.body}).write(w)
		return
	}
	res, err := p.askListing(r.Context(), p.target(r), r.Header)
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

// askListing asks the catalog for a listing, at target, with the headers of
// DuckDB's request.
func (p *Proxy) askListing(ctx context.Context, target string, header http.Header) (*result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header = forwardHeader(header)
	// The transport asks for, and undoes, compression itself; the answer
	// is kept, and written, as plain JSON.
	req.Header.Del("Accept-Encoding")
	return p.do(req)
}

// renewListing asks the catalog again, in the background, for the listing
// rest that r asked for, and keeps its answer unless the listings were
// forgotten since gen.
func (p *Proxy) renewListing(r *http.Request, rest string, gen uint64) {
	target, header := p.target(r), r.Header.Clone()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer p.lists.renewEnded(rest)
		ctx, cancel := context.WithTimeout(p.ctx, p.opts.AttemptTimeout)
		defer cancel()
		if res, err := p.askListing(ctx, target, header); err == nil {
			p.lists.keep(rest, gen, res)
		}
	}()
}
