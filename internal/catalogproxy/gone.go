package catalogproxy

import (
	"net/http"
	"net/url"
	"strings"
)

// DuckDB's Iceberg catalog keeps an entry for each table it listed, and
// lists it until it drops or renames the table itself: a table the catalog
// renamed when DuckDB heard the rename fail stays listed under its old
// name, on every session, for as long as DuckDB runs. Looking the name up
// finds nothing, and DROP TABLE IF EXISTS then does nothing. So Igloo has
// DuckDB drop it while the proxy (Forget) answers DuckDB's load of the old
// name with the table's under its new one, and the drop with success,
// without asking the catalog: DuckDB erases its entry, and the catalog
// hears nothing of it.

// Forget makes the proxy answer DuckDB's loads of the table name in
// namespace ns with those of the table as in the same namespace, the name
// the catalog renamed it to, and DuckDB's next drop of name with 204,
// without asking the catalog. It returns a func that stops it if no drop
// came.
func (p *Proxy) Forget(ns []string, name, as string) func() {
	if p == nil {
		return func() {}
	}
	key := goneKey(strings.Join(ns, "\x1f"), name)
	p.mu.Lock()
	p.gone[key] = as
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		delete(p.gone, key)
		p.mu.Unlock()
	}
}

func goneKey(ns, name string) [2]string { return [2]string{strings.ToLower(ns), strings.ToLower(name)} }

// forgotten answers r, whose path under the proxy is rest, if it loads or
// drops a table Forget forgets, and reports whether it did.
func (p *Proxy) forgotten(w http.ResponseWriter, r *http.Request, rest string) bool {
	p.mu.Lock()
	none := len(p.gone) == 0
	p.mu.Unlock()
	if none {
		return false
	}
	path, query, hasQuery := strings.Cut(rest, "?")
	m := loadTablePath.FindStringSubmatch(path)
	if m == nil {
		return false
	}
	key := goneKey(unescape(m[2]), unescape(m[3]))
	p.mu.Lock()
	as, ok := p.gone[key]
	if ok && r.Method == http.MethodDelete {
		delete(p.gone, key)
	}
	p.mu.Unlock()
	switch {
	case !ok:
		return false
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
		return true
	case r.Method != http.MethodGet && r.Method != http.MethodHead:
		return false
	}
	to := m[1] + "namespaces/" + m[2] + "/tables/" + url.PathEscape(as)
	if hasQuery {
		to += "?" + query
	}
	u, err := url.Parse("/" + p.secret + "/" + to)
	if err != nil {
		return false
	}
	r = r.Clone(r.Context())
	r.URL = u
	p.reads.ServeHTTP(w, r)
	return true
}
