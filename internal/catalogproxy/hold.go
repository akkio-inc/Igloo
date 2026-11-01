package catalogproxy

import (
	"encoding/json"
	"net/http"
	"sync"
)

// Commits kept from the catalog. A full refresh of a materialized view puts
// the new rows in the view's table in place of all its files, in one commit
// (see pgwire's overwrite.go). COPY writes those files for a table of plain
// columns, but only DuckDB's own INSERT writes them as any table says, in its
// partitions and nested types, and DuckDB commits what it wrote as an append,
// beside the old rows. So for the length of such a write the proxy holds the
// table: it tells DuckDB, as the table loads, to write under a directory of
// the refresh's own (the property write.data.path, which DuckDB follows and
// Lakekeeper refuses to store), and it keeps the commit from the catalog and
// tells DuckDB it was made. The refresh then makes its own commit of the
// files, which readers see at once in place of the old ones.
//
// DuckDB's requests do not say which session sent them. A hold keeps only the
// commit that follows Arm, of the rows the INSERT said it wrote, and from Arm
// on refuses the table's others, as a conflict, which their writers try again
// once the hold is over; before Arm they pass, and the refresh replaces them.
// Another session's commit in that moment of just as many rows would be kept
// in place of the refresh's, which then fails, and that session's rows, in a
// view's table, are what the next refresh replaces either way.

// TakenCommit is a commit a hold kept from the catalog.
type TakenCommit struct {
	SnapshotID   int64
	ManifestList string
}

// CommitHold holds a table's next commit (see above).
type CommitHold struct {
	p   *Proxy
	key string
	dir string

	mu      sync.Mutex
	armed   bool
	records int64
	taken   *TakenCommit
}

// HoldCommit holds the table ns.name, whose files DuckDB is to write under
// dir, until Release. It returns nil if another hold has it.
func (p *Proxy) HoldCommit(ns []string, name, dir string) *CommitHold {
	h := &CommitHold{p: p, key: tableKey(ns, name), dir: dir}
	p.holdMu.Lock()
	defer p.holdMu.Unlock()
	if p.holds[h.key] != nil {
		return nil
	}
	if p.holds == nil {
		p.holds = map[string]*CommitHold{}
	}
	p.holds[h.key] = h
	p.holdCount.Add(1)
	return h
}

// Arm has the hold keep the next commit of the table that adds records rows.
func (h *CommitHold) Arm(records int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed, h.records = true, records
}

// Taken returns the commit the hold kept, if it did.
func (h *CommitHold) Taken() (TakenCommit, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.taken == nil {
		return TakenCommit{}, false
	}
	return *h.taken, true
}

// Release ends the hold.
func (h *CommitHold) Release() {
	h.p.holdMu.Lock()
	defer h.p.holdMu.Unlock()
	if h.p.holds[h.key] == h {
		delete(h.p.holds, h.key)
		h.p.holdCount.Add(-1)
	}
}

// holdOf returns the hold of the table of key, if any.
func (p *Proxy) holdOf(key string) *CommitHold {
	if p.holdCount.Load() == 0 {
		return nil
	}
	p.holdMu.Lock()
	defer p.holdMu.Unlock()
	return p.holds[key]
}

// takeCommit answers a commit of a held table that the hold keeps or refuses;
// it reports whether it did.
func (p *Proxy) takeCommit(w http.ResponseWriter, r *http.Request, rest string, body []byte) bool {
	if p.holdCount.Load() == 0 {
		return false
	}
	c := parseCommit(rest, body)
	if c == nil {
		return false
	}
	var h *CommitHold
	for _, t := range c.tables {
		if m := tablePath.FindStringSubmatch(t.path); m != nil && h == nil {
			h = p.holdOf(tableKey(namespace(m[1]), unescape(m[2])))
		}
	}
	if h == nil {
		return false
	}
	// What the hold could keep is one table's commit of one snapshot.
	var s *snapshot
	if len(c.tables) == 1 && len(c.tables[0].snapshots) == 1 && !c.tables[0].creates {
		s = &c.tables[0].snapshots[0]
	}
	kept, refused := h.take(s)
	switch {
	case refused:
		writeError(w, http.StatusConflict, "CommitFailedException", "a refresh is replacing the table's files")
		return true
	case !kept:
		return false
	case !c.single:
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	// A commit to the table's own path is answered with its metadata, as
	// it is.
	res, err := p.get(r.Context(), c.tables[0].path)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailableException", err.Error())
		return true
	}
	res.write(w)
	return true
}

// take says what the hold does with a commit of its table, of s if that is
// what it could keep (see above). A commit it has is kept again: DuckDB sends
// a commit again when it does not hear the answer.
func (h *CommitHold) take(s *snapshot) (kept, refused bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case !h.armed:
		return false, false
	case s == nil:
		return false, true
	case h.taken != nil:
		return h.taken.SnapshotID == s.id, h.taken.SnapshotID != s.id
	case s.addedRecords >= 0 && s.addedRecords != h.records:
		return false, true
	}
	h.taken = &TakenCommit{SnapshotID: s.id, ManifestList: s.manifestList}
	return true, false
}

// serveHeldLoad answers a load of a held table: the catalog's answer, with
// the hold's directory as where the table's data goes. It reports whether it
// answered; if it could not, the load goes as any other, and DuckDB writes
// the files where it would.
func (p *Proxy) serveHeldLoad(w http.ResponseWriter, r *http.Request, h *CommitHold) bool {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, p.target(r), nil)
	if err != nil {
		return false
	}
	req.Header = forwardHeader(r.Header)
	req.Header.Del("Accept-Encoding")
	req.Header.Del("If-None-Match")
	res, err := p.do(req)
	if err != nil || res.status != http.StatusOK {
		return false
	}
	out, err := withDataPath(res.body, h.dir)
	if err != nil {
		return false
	}
	res.body = out
	res.write(w)
	return true
}

// withDataPath returns a table load's answer with write.data.path dir among
// its metadata's properties, the rest as it was.
func withDataPath(body []byte, dir string) ([]byte, error) {
	var load map[string]json.RawMessage
	if err := json.Unmarshal(body, &load); err != nil {
		return nil, err
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(load["metadata"], &meta); err != nil {
		return nil, err
	}
	props := map[string]string{}
	if raw, ok := meta["properties"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &props); err != nil {
			return nil, err
		}
	}
	props["write.data.path"] = dir
	raw, err := json.Marshal(props)
	if err != nil {
		return nil, err
	}
	meta["properties"] = raw
	if load["metadata"], err = json.Marshal(meta); err != nil {
		return nil, err
	}
	return json.Marshal(load)
}
