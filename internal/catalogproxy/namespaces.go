package catalogproxy

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// DuckDB's Iceberg catalog remembers a namespace as missing once a lookup
// of a table in it found neither, for as long as it runs: a namespace made
// afterwards, by this replica's CREATE SCHEMA or another replica's, stays
// missing to every session. DuckDB forgets it only on DROP SCHEMA IF
// EXISTS of a namespace the catalog says does not exist. So the engine
// (Engine.ForgetMissingSchema) runs that, in a transaction it rolls back,
// while the proxy answers its one existence check with 404 (HideNamespace):
// DuckDB drops what it remembers and the catalog never hears of it.

// namespaceCheckPath is a namespace's own path, which DuckDB reads (HEAD or
// GET) to check that it exists; prefixPath captures the warehouse prefix
// of any path under the namespaces.
var (
	namespaceCheckPath = regexp.MustCompile(`^v1/[^/?]+/namespaces/([^/?]+)(?:\?|$)`)
	prefixPath         = regexp.MustCompile(`^v1/([^/?]+)/namespaces(?:[/?]|$)`)
)

// notePrefix notes the warehouse prefix of a request at rest, for
// NamespaceExists.
func (p *Proxy) notePrefix(rest string) {
	if m := prefixPath.FindStringSubmatch(rest); m != nil {
		if old := p.prefix.Load(); old == nil || *old != m[1] {
			p.prefix.Store(&m[1])
		}
	}
}

// hidden reports whether a request at rest is DuckDB's existence check of
// a namespace HideNamespace hides, and if so stops hiding it.
func (p *Proxy) hidden(r *http.Request, rest string) bool {
	m := namespaceCheckPath.FindStringSubmatch(rest)
	if m == nil || r.Method != http.MethodHead && r.Method != http.MethodGet {
		return false
	}
	key := strings.Join(namespace(m[1]), "\x1f")
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.hide[key] {
		return false
	}
	delete(p.hide, key)
	return true
}

// HideNamespace makes the proxy answer DuckDB's next check that namespace
// ns exists with 404, without asking the catalog, and returns a func that
// stops it if no check came.
func (p *Proxy) HideNamespace(ns []string) func() {
	key := strings.Join(ns, "\x1f")
	p.mu.Lock()
	p.hide[key] = true
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		delete(p.hide, key)
		p.mu.Unlock()
	}
}

// NamespaceExists asks the catalog whether namespace ns exists. ok is false
// if the catalog did not say, or if the proxy has not yet seen DuckDB check
// a namespace, which tells it the warehouse's prefix.
func (p *Proxy) NamespaceExists(ctx context.Context, ns []string) (exists, ok bool) {
	prefix := p.prefix.Load()
	if prefix == nil {
		return false, false
	}
	parts := make([]string, len(ns))
	for i, n := range ns {
		parts[i] = url.PathEscape(n)
	}
	target := p.base + "/v1/" + *prefix + "/namespaces/" + strings.Join(parts, "%1F")
	var res *http.Response
	// A catalog that does not answer HEAD answers GET.
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		req, err := http.NewRequestWithContext(ctx, method, target, nil)
		if err != nil {
			return false, false
		}
		if res, err = p.http.Do(req); err != nil {
			return false, false
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusMethodNotAllowed {
			break
		}
	}
	switch {
	case res.StatusCode == http.StatusNotFound:
		return false, true
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return true, true
	}
	return false, false
}
