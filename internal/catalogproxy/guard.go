package catalogproxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Every change Igloo's SQL makes to the catalog is one DuckDB asks of it
// through the proxy, whatever the statement's text: DROP TABLE, EXPLAIN
// ANALYZE DROP TABLE, a macro, CREATE OR REPLACE TABLE, a call of
// set_iceberg_table_properties with its keys computed or behind a
// comment. A Guard sees each such change as the catalog would (which
// table, which properties), and refuses those Igloo's rules forbid, such
// as dropping a table an erasure request erases from, before the catalog
// gets them. It also decides whether a table dropped has its files
// deleted with it (see Verdict).

// Op is the kind of a change.
type Op string

const (
	// OpDrop drops a table; OpDropNamespace a namespace.
	OpDrop          Op = "drop"
	OpDropNamespace Op = "drop-namespace"
	// OpRename renames a table, or moves it to another namespace.
	OpRename Op = "rename"
	// OpCreate makes a table, by a create request or a commit that
	// asserts it does not exist yet.
	OpCreate Op = "create"
	// OpRegister registers a table's metadata file.
	OpRegister Op = "register"
	// OpCommit changes a table that exists.
	OpCommit Op = "commit"
)

// Change is a change DuckDB asks of the catalog, for a Guard.
type Change struct {
	Op Op
	// Namespace and Name are the table's (only Namespace for a namespace's
	// change), and To where a rename moves it.
	Namespace []string
	Name      string
	To        *TableName
	// Location is the location of a table dropped, if the catalog could
	// say.
	Location string
	// Properties are those a create gives the table or a commit sets, and
	// Removed those a commit removes.
	Properties map[string]string
	Removed    []string
	// FormatVersion is the Iceberg format version a create asks for or a
	// commit upgrades the table to, or 0.
	FormatVersion int
	// Purge says a drop asks the catalog to delete the table's files.
	Purge bool
}

// TableName names a table.
type TableName struct {
	Namespace []string
	Name      string
}

// Verdict is a Guard's answer to a change it allows. Purge has the
// catalog delete a dropped table's files with it, and Release, if set, is
// called once the catalog has answered.
type Verdict struct {
	Purge   bool
	Release func()
}

// Guard checks the changes DuckDB asks of the catalog.
type Guard interface {
	// Check returns an error made by Refuse to refuse change c, which
	// DuckDB reports to the statement's user; any other error refuses it
	// too, as the catalog not answering.
	Check(ctx context.Context, c *Change) (Verdict, error)
}

// Refusal is a Guard's refusal of a change.
type Refusal struct{ msg string }

func (r *Refusal) Error() string { return r.msg }

// Refuse makes a Guard's refusal, whose message DuckDB passes on.
func Refuse(msg string) error { return &Refusal{msg: msg} }

// RefusalMark starts the message of every change the proxy refuses for a
// Guard, as DuckDB's error quotes it: Igloo reports such an error as
// permission denied (SQLSTATE 42501).
const RefusalMark = "Igloo refused the catalog change: "

// SetGuard has g check every change from now on.
func (p *Proxy) SetGuard(g Guard) {
	p.guard.Store(&g)
}

var (
	tablePath     = regexp.MustCompile(`^v1/[^/?]+/namespaces/([^/?]+)/tables/([^/?]+)$`)
	tablesPath    = regexp.MustCompile(`^v1/[^/?]+/namespaces/([^/?]+)/tables$`)
	registerPath  = regexp.MustCompile(`^v1/[^/?]+/namespaces/([^/?]+)/register$`)
	namespacePath = regexp.MustCompile(`^v1/[^/?]+/namespaces/([^/?]+)$`)
	renamePath    = regexp.MustCompile(`^v1/[^/?]+/tables/rename$`)
)

// check has the Guard, if there is one, check c. It returns the answer
// for DuckDB if the change is refused, or nil, having set what the Guard
// decided for the change.
func (p *Proxy) check(c *change) *result {
	gp := p.guard.Load()
	if gp == nil {
		return nil
	}
	g := *gp
	changes, ok := p.parseChanges(c)
	if !ok {
		return errorResult(http.StatusForbidden, "ForbiddenException",
			RefusalMark+"permission denied: Igloo cannot tell what this change to the catalog does, so it does not make it")
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.opts.Patience)
	defer cancel()
	for _, ch := range changes {
		if ch.Op == OpDrop {
			ch.Location = p.location(ctx, c.rest)
		}
		v, err := g.Check(ctx, ch)
		var r *Refusal
		switch {
		case errors.As(err, &r):
			c.release()
			return errorResult(http.StatusForbidden, "ForbiddenException", RefusalMark+r.msg)
		case err != nil:
			c.release()
			p.log.Warn("could not check a change to the catalog", "method", c.method, "path", c.rest, "err", err)
			return errorResult(http.StatusServiceUnavailable, "ServiceUnavailableException",
				RefusalMark+"Igloo could not check the change to the catalog: "+err.Error())
		}
		if v.Release != nil {
			c.releases = append(c.releases, v.Release)
		}
		if ch.Op == OpDrop && v.Purge && !ch.Purge {
			c.rest = withPurge(c.rest)
		}
	}
	return nil
}

// release calls what the Guard asked to be called once the change is
// through.
func (c *change) release() {
	for _, f := range c.releases {
		f()
	}
	c.releases = nil
}

// withPurge asks the catalog to purge the table a drop drops.
func withPurge(rest string) string {
	path, query, _ := strings.Cut(rest, "?")
	q, err := url.ParseQuery(query)
	if err != nil {
		q = url.Values{}
	}
	q.Set("purgeRequested", "true")
	return path + "?" + q.Encode()
}

// location is the location of the table at path, or "".
func (p *Proxy) location(ctx context.Context, rest string) string {
	path, _, _ := strings.Cut(rest, "?")
	res, err := p.get(ctx, path)
	if err != nil || res.status != http.StatusOK {
		return ""
	}
	var load struct {
		Metadata struct {
			Location string `json:"location"`
		} `json:"metadata"`
	}
	if json.Unmarshal(res.body, &load) != nil {
		return ""
	}
	return load.Metadata.Location
}

// namespace reads a namespace as a path has it: its levels joined by the
// unit separator, escaped.
func namespace(s string) []string {
	if u, err := url.PathUnescape(s); err == nil {
		s = u
	}
	return strings.Split(s, "\x1f")
}

func unescape(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

// parseChanges reads what change c does; ok is false for a change the
// proxy cannot read, which it refuses. Changes to views and namespaces'
// properties pass.
func (p *Proxy) parseChanges(c *change) (out []*Change, ok bool) {
	path, query, _ := strings.Cut(c.rest, "?")
	switch {
	case c.method == http.MethodDelete:
		if m := tablePath.FindStringSubmatch(path); m != nil {
			q, _ := url.ParseQuery(query)
			purge, _ := strconv.ParseBool(q.Get("purgeRequested"))
			return []*Change{{Op: OpDrop, Namespace: namespace(m[1]), Name: unescape(m[2]), Purge: purge}}, true
		}
		if m := namespacePath.FindStringSubmatch(path); m != nil {
			return []*Change{{Op: OpDropNamespace, Namespace: namespace(m[1])}}, true
		}
		return nil, true
	case c.method != http.MethodPost:
		return nil, true
	case renamePath.MatchString(path):
		var req struct {
			Source      *jsonIdent `json:"source"`
			Destination *jsonIdent `json:"destination"`
		}
		if json.Unmarshal(c.body, &req) != nil || req.Source == nil || req.Destination == nil {
			return nil, false
		}
		return []*Change{{Op: OpRename, Namespace: req.Source.Namespace, Name: req.Source.Name,
			To: &TableName{Namespace: req.Destination.Namespace, Name: req.Destination.Name}}}, true
	case tablesPath.MatchString(path):
		m := tablesPath.FindStringSubmatch(path)
		var req struct {
			Name       string            `json:"name"`
			Properties map[string]string `json:"properties"`
		}
		if json.Unmarshal(c.body, &req) != nil || req.Name == "" {
			return nil, false
		}
		ch := &Change{Op: OpCreate, Namespace: namespace(m[1]), Name: req.Name, Properties: req.Properties}
		ch.FormatVersion = formatVersion(req.Properties)
		return []*Change{ch}, true
	case registerPath.MatchString(path):
		m := registerPath.FindStringSubmatch(path)
		var req struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(c.body, &req) != nil || req.Name == "" {
			return nil, false
		}
		return []*Change{{Op: OpRegister, Namespace: namespace(m[1]), Name: req.Name}}, true
	case commitPath.MatchString(c.rest):
		return parseCommitChanges(path, c.body)
	}
	return nil, true
}

type jsonIdent struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

// formatVersion reads the format-version property, or 0.
func formatVersion(props map[string]string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(props["format-version"]))
	return v
}

// parseCommitChanges reads a commit to one table at path, or a
// transaction's, as changes.
func parseCommitChanges(path string, body []byte) ([]*Change, bool) {
	type tableChange struct {
		Identifier   *jsonIdent `json:"identifier"`
		Requirements []struct {
			Type string `json:"type"`
		} `json:"requirements"`
		Updates []struct {
			Action        string            `json:"action"`
			Updates       map[string]string `json:"updates"`
			Removals      []string          `json:"removals"`
			FormatVersion int               `json:"format-version"`
		} `json:"updates"`
	}
	var changes []tableChange
	if strings.HasSuffix(path, "/transactions/commit") {
		var tx struct {
			TableChanges []tableChange `json:"table-changes"`
		}
		if json.Unmarshal(body, &tx) != nil {
			return nil, false
		}
		changes = tx.TableChanges
	} else {
		var ch tableChange
		if json.Unmarshal(body, &ch) != nil {
			return nil, false
		}
		m := tablePath.FindStringSubmatch(path)
		if m == nil {
			return nil, false
		}
		ch.Identifier = &jsonIdent{Namespace: namespace(m[1]), Name: unescape(m[2])}
		changes = []tableChange{ch}
	}
	var out []*Change
	for _, tc := range changes {
		if tc.Identifier == nil {
			return nil, false
		}
		ch := &Change{Op: OpCommit, Namespace: tc.Identifier.Namespace, Name: tc.Identifier.Name, Properties: map[string]string{}}
		for _, r := range tc.Requirements {
			if r.Type == "assert-create" {
				ch.Op = OpCreate
			}
		}
		for _, u := range tc.Updates {
			switch u.Action {
			case "set-properties":
				for k, v := range u.Updates {
					ch.Properties[k] = v
				}
			case "remove-properties":
				ch.Removed = append(ch.Removed, u.Removals...)
			case "upgrade-format-version":
				ch.FormatVersion = max(ch.FormatVersion, u.FormatVersion)
			}
		}
		ch.FormatVersion = max(ch.FormatVersion, formatVersion(ch.Properties))
		out = append(out, ch)
	}
	return out, true
}
