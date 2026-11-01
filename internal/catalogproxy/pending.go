package catalogproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// Pending creations. Before it passes on a staged create, the proxy notes
// it in the namespace's properties, and once a commit that makes the table
// goes through, it forgets the note (see catalog.Pending): a staged create
// that DuckDB never committed, because Igloo was stopped or the statement
// failed, keeps holding its name in Lakekeeper, unseen, and the note says
// it is Igloo's own, which ddl.Manager.ClearDeadPending may clear. Notes
// are best effort: a create goes on without one when its namespace's
// properties cannot be written.
type pendingCreates struct {
	mu sync.Mutex
	// byTable are the notes of staged creates not yet committed, by
	// tableKey.
	byTable map[string]catalog.Pending
}

// tableChange is a table's change in a commit, as far as the notes need.
type tableChange struct {
	Identifier *struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	} `json:"identifier"`
	Requirements []struct {
		Type string `json:"type"`
	} `json:"requirements"`
}

// notePending writes the note of a staged create, a request to rest with
// body, and returns the table's key and the note; key is "" for any other
// request.
func (p *Proxy) notePending(r *http.Request, rest string, body []byte) (string, catalog.Pending) {
	m := createTablePath.FindStringSubmatch(rest)
	if r.Method != http.MethodPost || m == nil {
		return "", catalog.Pending{}
	}
	var c struct {
		Name        string `json:"name"`
		StageCreate bool   `json:"stage-create"`
	}
	if json.Unmarshal(body, &c) != nil || !c.StageCreate || c.Name == "" {
		return "", catalog.Pending{}
	}
	note := catalog.NewPending(catalog.PendingStaged, c.Name, "")
	path, _, _ := strings.Cut(rest, "?")
	if !p.setProperties(r, strings.TrimSuffix(path, "tables")+"properties", map[string]string{note.Key: note.Value()}, nil) {
		return "", catalog.Pending{}
	}
	key := tableKey(namespace(m[1]), c.Name)
	p.pending.mu.Lock()
	if p.pending.byTable == nil {
		p.pending.byTable = map[string]catalog.Pending{}
	}
	p.pending.byTable[key] = note
	p.pending.mu.Unlock()
	return key, note
}

// settlePending forgets notes once a change went through with status: the
// note of a staged create the catalog refused, and those of the tables a
// commit made (its requirement assert-create).
func (p *Proxy) settlePending(r *http.Request, rest string, body []byte, status int, key string, note catalog.Pending) {
	ok := status >= 200 && status < 300
	path, _, _ := strings.Cut(rest, "?")
	if key != "" {
		if !ok {
			p.forgetPending(r, strings.TrimSuffix(path, "tables")+"properties", key, note)
		}
		return
	}
	if !ok || r.Method != http.MethodPost {
		return
	}
	var prefix string
	var changes []tableChange
	if m := loadTablePath.FindStringSubmatch(rest); m != nil {
		var c tableChange
		if json.Unmarshal(body, &c) != nil {
			return
		}
		c.Identifier = &struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		}{namespace(m[2]), unescape(m[3])}
		prefix, changes = m[1], []tableChange{c}
	} else if strings.HasSuffix(path, "/transactions/commit") {
		var tx struct {
			TableChanges []tableChange `json:"table-changes"`
		}
		if json.Unmarshal(body, &tx) != nil {
			return
		}
		prefix, changes = strings.TrimSuffix(path, "transactions/commit"), tx.TableChanges
	}
	for _, c := range changes {
		if c.Identifier == nil {
			continue
		}
		creates := false
		for _, req := range c.Requirements {
			creates = creates || req.Type == "assert-create"
		}
		k := tableKey(c.Identifier.Namespace, c.Identifier.Name)
		p.pending.mu.Lock()
		n, found := p.pending.byTable[k]
		p.pending.mu.Unlock()
		if creates && found {
			ns := url.PathEscape(strings.Join(c.Identifier.Namespace, "\x1f"))
			p.forgetPending(r, prefix+"namespaces/"+ns+"/properties", k, n)
		}
	}
}

// forgetPending removes the note of the table key from the properties at
// path.
func (p *Proxy) forgetPending(r *http.Request, path, key string, note catalog.Pending) {
	p.pending.mu.Lock()
	if n, ok := p.pending.byTable[key]; ok && n.Key == note.Key {
		delete(p.pending.byTable, key)
	}
	p.pending.mu.Unlock()
	p.setProperties(r, path, nil, []string{note.Key})
}

// setProperties sets and removes properties of the namespace whose
// properties' path under the proxy is path, with r's headers; it reports
// whether the catalog did.
func (p *Proxy) setProperties(r *http.Request, path string, updates map[string]string, removals []string) bool {
	if updates == nil {
		updates = map[string]string{}
	}
	if removals == nil {
		removals = []string{}
	}
	body, err := json.Marshal(map[string]any{"updates": updates, "removals": removals})
	if err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(r.Context()), http.MethodPost, p.base+"/"+path, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header = forwardHeader(r.Header)
	req.Header.Del("Accept-Encoding")
	req.Header.Del("Idempotency-Key")
	req.Header.Set("Content-Type", "application/json")
	res, err := p.do(req)
	if err != nil || res.status >= 300 {
		p.log.Debug("could not note a pending create in the namespace's properties", "path", path, "err", err)
		return false
	}
	return true
}
