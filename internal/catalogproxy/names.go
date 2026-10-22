package catalogproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Names. Lakekeeper finds a table by its name in any case: a table made as
// ORDERS loads as orders too. DuckDB-Iceberg keeps one in-memory entry for
// each of the catalog's tables, which listings (duckdb_tables(),
// information_schema, SHOW) list by the entry's name, and each time a
// statement looks a table up it replaces the table's entry with one named as
// the statement spelled it; DuckDB binds names whatever their case, and a
// listing keeps the entries it finds. So once a statement or a view read
// ORDERS as orders, every replica that ran it listed orders, until a
// statement spelled it ORDERS again. The catalog's name never changed.
//
// DuckDB loads a table from the catalog each time it looks it up, and the
// proxy sees under which name before DuckDB has the answer: it learns how
// the catalog spells the names of a namespace's tables by listing them, the
// first time DuckDB loads a table it has not seen listed, and from the
// tables made, renamed and dropped through it; and it notes the tables
// DuckDB loaded under another spelling. The engine looks those up again by
// the catalog's names before anything lists the catalog (see Respelled).

// loadTablePath matches the requests that load or check one table: the
// API's path up to "namespaces/", the namespace and the table's name.
var loadTablePath = regexp.MustCompile(`^(v1/(?:[^/?]+/)?)namespaces/([^/?]+)/tables/([^/?]+)(?:\?|$)`)

// createTablePath matches the requests that create a table in a namespace,
// and renameTablePath those that rename one.
var (
	createTablePath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/([^/?]+)/tables(?:\?|$)`)
	renameTablePath = regexp.MustCompile(`^v1/(?:[^/?]+/)?tables/rename(?:\?|$)`)
)

// Table is one of the catalog's tables: its namespace's levels and its name.
type Table struct {
	Namespace []string
	Name      string
}

// names is what the proxy knows of how the catalog spells its tables'
// names, and the tables DuckDB loaded under other names.
type names struct {
	mu sync.Mutex
	// spellings are each namespace's, by namespace: its levels joined by
	// \x1f, as in a path, and lower-cased.
	spellings map[string]*spellings
	// respelled are the tables DuckDB loaded under another name than the
	// catalog's since Respelled last returned them, by namespace and
	// lower-cased name.
	respelled map[[2]string]Table
	// relisting are the namespaces being listed again in the background.
	relisting map[string]bool
	// listing serializes listing namespaces that DuckDB waits for.
	listing sync.Mutex
	// ttl is how long a listing of a namespace is trusted to say how the
	// catalog spells its tables' names: the proxy's ListTTL, if it keeps
	// listings. Past that, a table DuckDB loads under another spelling is
	// noted by the listing's, and the namespace is listed again in the
	// background, in case another engine replaced the table with one
	// spelled as DuckDB asked: the next load goes by that listing.
	ttl time.Duration
}

// spellings are the names of one namespace's tables as the catalog spells
// them, by lower-cased name, as of a listing at at.
type spellings struct {
	namespace []string
	names     map[string]string
	at        time.Time
}

func newNames(ttl time.Duration) *names {
	if ttl <= 0 {
		ttl = defaultListTTL
	}
	return &names{spellings: map[string]*spellings{}, respelled: map[[2]string]Table{}, relisting: map[string]bool{}, ttl: ttl}
}

// lookup returns the catalog's spelling of a table, by its lower-cased
// namespace and name, whether its namespace's listing has it, and when the
// namespace was listed (zero if never).
func (n *names) lookup(key [2]string) (Table, bool, time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	sp := n.spellings[key[0]]
	if sp == nil {
		return Table{}, false, time.Time{}
	}
	name, ok := sp.names[key[1]]
	return Table{Namespace: sp.namespace, Name: name}, ok, sp.at
}

// checkSpelling runs before DuckDB has the catalog's answer 200 to r, whose
// path under the proxy is rest: if r loaded a table under another name than
// the catalog spells it with, the table is noted for Respelled.
func (p *Proxy) checkSpelling(r *http.Request, rest string) {
	m := loadTablePath.FindStringSubmatch(rest)
	if m == nil || r.Method != http.MethodGet {
		return
	}
	ns, err := url.PathUnescape(m[2])
	if err != nil {
		return
	}
	name, err := url.PathUnescape(m[3])
	if err != nil {
		return
	}
	key := [2]string{strings.ToLower(ns), strings.ToLower(name)}
	t, known, at := p.names.lookup(key)
	switch {
	case known && t.Name == name:
		return
	case known:
		if time.Since(at) >= p.names.ttl {
			p.relistSpellings(r.Header, m[1], m[2], key[0])
		}
	default:
		// A table not seen listed: list its namespace, unless another
		// request did meanwhile.
		begun := time.Now()
		p.names.listing.Lock()
		if t, known, at = p.names.lookup(key); !known && !at.After(begun) {
			err = p.listSpellings(r.Context(), r.Header, m[1], m[2], key[0])
			t, known, _ = p.names.lookup(key)
		}
		p.names.listing.Unlock()
		if err != nil && r.Context().Err() == nil {
			p.log.Warn("cannot list a namespace to learn how the catalog spells its tables' names",
				"namespace", ns, "err", err)
		}
	}
	if known && t.Name != name {
		p.names.mu.Lock()
		p.names.respelled[key] = t
		p.names.mu.Unlock()
	}
}

// relistSpellings lists a namespace again in the background, as
// listSpellings does, unless it is already being listed so.
func (p *Proxy) relistSpellings(header http.Header, api, nsPath, key string) {
	p.names.mu.Lock()
	if p.names.relisting[key] {
		p.names.mu.Unlock()
		return
	}
	p.names.relisting[key] = true
	p.names.mu.Unlock()
	header = header.Clone()
	p.wg.Go(func() {
		ctx, cancel := context.WithTimeout(p.ctx, p.opts.AttemptTimeout)
		defer cancel()
		if err := p.listSpellings(ctx, header, api, nsPath, key); err != nil && p.ctx.Err() == nil {
			p.log.Warn("cannot list a namespace to learn how the catalog spells its tables' names",
				"namespace", key, "err", err)
		}
		p.names.mu.Lock()
		delete(p.names.relisting, key)
		p.names.mu.Unlock()
	})
}

// maxSpellingPages bounds the pages of one listing of a namespace's tables.
const maxSpellingPages = 10000

// listSpellings lists the tables of a namespace, nsPath as in a path under
// api (the API's path up to "namespaces/"), with header, the headers of a
// request DuckDB made, and keeps their names as the namespace's spellings
// under key.
func (p *Proxy) listSpellings(ctx context.Context, header http.Header, api, nsPath, key string) error {
	sp := &spellings{names: map[string]string{}}
	for token, page := "", 0; ; page++ {
		rest := api + "namespaces/" + nsPath + "/tables"
		if token != "" {
			rest += "?pageToken=" + url.QueryEscape(token)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/"+rest, nil)
		if err != nil {
			return err
		}
		req.Header = forwardHeader(header)
		req.Header.Del("Accept-Encoding")
		res, err := p.do(req)
		if err != nil {
			return err
		}
		if res.status != http.StatusOK {
			return fmt.Errorf("HTTP %d", res.status)
		}
		var list struct {
			Identifiers []struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"identifiers"`
			Next string `json:"next-page-token"`
		}
		if err := json.Unmarshal(res.body, &list); err != nil {
			return err
		}
		for _, id := range list.Identifiers {
			sp.names[strings.ToLower(id.Name)] = id.Name
			if sp.namespace == nil {
				sp.namespace = id.Namespace
			}
		}
		if list.Next == "" || list.Next == token || page == maxSpellingPages {
			break
		}
		token = list.Next
	}
	if sp.namespace == nil {
		ns, err := url.PathUnescape(nsPath)
		if err != nil {
			return err
		}
		sp.namespace = strings.Split(ns, "\x1f")
	}
	sp.at = time.Now()
	p.names.mu.Lock()
	p.names.spellings[key] = sp
	p.names.mu.Unlock()
	return nil
}

// changed learns the names of the tables that a change through the proxy
// made, renamed or dropped (method, rest and body), which the catalog
// answered with status, in the namespaces it has spellings of.
func (n *names) changed(method, rest string, body []byte, status int) {
	if status < 200 || status >= 300 {
		return
	}
	set := func(ns []string, name, spelled string) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if sp := n.spellings[strings.ToLower(strings.Join(ns, "\x1f"))]; sp != nil {
			if spelled == "" {
				delete(sp.names, strings.ToLower(name))
			} else {
				sp.names[strings.ToLower(name)] = spelled
			}
		}
	}
	switch {
	case method == http.MethodDelete:
		if m := loadTablePath.FindStringSubmatch(rest); m != nil {
			ns, err1 := url.PathUnescape(m[2])
			name, err2 := url.PathUnescape(m[3])
			if err1 == nil && err2 == nil {
				set(strings.Split(ns, "\x1f"), name, "")
			}
		}
	case method != http.MethodPost:
	case createTablePath.MatchString(rest):
		ns, err := url.PathUnescape(createTablePath.FindStringSubmatch(rest)[1])
		var req struct {
			Name string `json:"name"`
		}
		if err == nil && json.Unmarshal(body, &req) == nil && req.Name != "" {
			set(strings.Split(ns, "\x1f"), req.Name, req.Name)
		}
	case renameTablePath.MatchString(rest):
		type ident struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		}
		var req struct {
			Source      ident `json:"source"`
			Destination ident `json:"destination"`
		}
		if json.Unmarshal(body, &req) == nil && req.Destination.Name != "" {
			set(req.Source.Namespace, req.Source.Name, "")
			set(req.Destination.Namespace, req.Destination.Name, req.Destination.Name)
		}
	}
}

// Respelled returns the tables, by the catalog's names, that DuckDB loaded
// under other names since it last returned them, and forgets them. Looking
// a table up by the catalog's name has DuckDB-Iceberg's entry for it carry
// that name again.
func (p *Proxy) Respelled() []Table {
	if p == nil {
		return nil
	}
	p.names.mu.Lock()
	defer p.names.mu.Unlock()
	if len(p.names.respelled) == 0 {
		return nil
	}
	out := make([]Table, 0, len(p.names.respelled))
	for _, t := range p.names.respelled {
		out = append(out, t)
	}
	clear(p.names.respelled)
	return out
}
