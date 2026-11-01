package catalogproxy

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// Data version. Igloo keeps the results of queries over the catalog's
// tables and sends them again (see pgwire's resultcache.go) only while no
// table they may have read has changed since. DataVersion tells: it goes
// up whenever a table may have changed. The proxy sees each change DuckDB
// asks of the catalog, and the version goes up before the change is sent
// and again once it is answered, so that no result of a statement that ran
// across a change is kept, and none kept while one was under way is used
// after it. It also sees each load of a table, and DuckDB loads every table
// a statement reads when it prepares the statement, so a table that
// another engine or replica changed shows when one of this replica's
// statements next loads it: at another metadata location than its last
// load found. The version goes up then too, and when a load's location
// cannot be read, as when the catalog compresses its answer.
//
// Each table has a version of its own too, which goes up whenever that
// table may have changed (see TablesVersion): a change the proxy can tell
// the tables of (a commit, a drop, a rename, a table made) raises theirs,
// and a load that finds a table at another location raises that table's.
// A change it cannot tell the tables of, as one to a namespace or a view,
// or Invalidate, raises every table's, through the epoch.
type dataVersion struct {
	version atomic.Uint64

	mu sync.Mutex
	// tables are the tables' versions, by tableKey; epoch is added to
	// every one of them.
	tables map[string]uint64
	epoch  uint64
	// locations are the metadata locations the last loads found, by the
	// table's path in the catalog's API.
	locations map[string]string
	// recorders take the loads made while they are set (see RecordLoads).
	recorders []*loadRecorder
}

// Load is a load of a table DuckDB made, which Reload makes again.
type Load struct {
	// path is the table, as locations has it; rest the request's path
	// under the proxy, and header its headers.
	path, rest string
	header     http.Header
}

// Table is the table loaded, its namespace's levels and its name joined
// with dots, in lower case, as erasure requests name tables.
func (l Load) Table() string {
	ns, name, _ := strings.Cut(l.path, "/")
	if u, err := url.PathUnescape(ns); err == nil {
		ns = u
	}
	if u, err := url.PathUnescape(name); err == nil {
		name = u
	}
	return strings.ReplaceAll(ns, "\x1f", ".") + "." + name
}

// loadRecorder takes the loads made while it is set, one a table.
type loadRecorder struct {
	mu    sync.Mutex
	loads map[string]Load
}

// RecordLoads records the loads of tables DuckDB makes from now on, until
// the func it returns, which returns them, is called. A statement prepared
// holding the catalog lock exclusively, which no other bind runs alongside,
// loads every table it reads, its views' included, so the loads recorded
// then are the tables the statement reads.
func (p *Proxy) RecordLoads() func() []Load {
	if p == nil {
		return func() []Load { return nil }
	}
	r := &loadRecorder{loads: map[string]Load{}}
	d := &p.data
	d.mu.Lock()
	d.recorders = append(d.recorders, r)
	d.mu.Unlock()
	return func() []Load {
		d.mu.Lock()
		d.recorders = slices.DeleteFunc(d.recorders, func(x *loadRecorder) bool { return x == r })
		d.mu.Unlock()
		r.mu.Lock()
		defer r.mu.Unlock()
		return slices.Collect(maps.Values(r.loads))
	}
}

// record gives a load to the recorders set.
func (d *dataVersion) record(l Load) {
	d.mu.Lock()
	recorders := slices.Clone(d.recorders)
	d.mu.Unlock()
	for _, r := range recorders {
		r.mu.Lock()
		if _, ok := r.loads[l.path]; !ok {
			r.loads[l.path] = Load{path: l.path, rest: l.rest, header: l.header.Clone()}
		}
		r.mu.Unlock()
	}
}

// Reload loads the tables of loads again, all at once, as DuckDB loaded
// them, so that DataVersion goes up if any of them changed since, here or
// through another replica or engine (see loaded), without DuckDB binding
// anything. It returns the first error.
func (p *Proxy) Reload(ctx context.Context, loads []Load) error {
	if p == nil || len(loads) == 0 {
		return nil
	}
	errs := make(chan error, len(loads))
	for _, l := range loads {
		go func() {
			errs <- p.reload(ctx, l)
		}()
	}
	var first error
	for range loads {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *Proxy) reload(ctx context.Context, l Load) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+"/"+l.rest, nil)
	if err != nil {
		return err
	}
	req.Header = forwardHeader(l.header)
	req.Header.Del("Accept-Encoding")
	res, err := p.do(req)
	if err != nil {
		return err
	}
	if res.status != http.StatusOK {
		// Dropped, say: whatever read it no longer reads the same.
		p.data.bumpTables([]string{loadKey(l.path)}, true)
		return fmt.Errorf("loading %s again: HTTP %d", l.path, res.status)
	}
	var loaded struct {
		Location string `json:"metadata-location"`
	}
	if err := json.Unmarshal(res.body, &loaded); err != nil {
		p.data.loaded(l.path, "")
		return err
	}
	p.data.loaded(l.path, loaded.Location)
	return nil
}

// DataVersion goes up whenever a table of the catalog may have changed.
func (p *Proxy) DataVersion() uint64 {
	if p == nil {
		return 0
	}
	return p.data.version.Load()
}

// TablesVersion is a version of the tables loads loaded: it changes
// whenever any of them may have changed, here or, once one of this
// replica's statements loads it again, through another replica or engine,
// and stays the same while none does, whatever happens to other tables.
func (p *Proxy) TablesVersion(loads []Load) uint64 {
	if p == nil {
		return 0
	}
	keys := make([]string, len(loads))
	for i, l := range loads {
		keys[i] = loadKey(l.path)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	d := &p.data
	h := sha256.New()
	d.mu.Lock()
	_ = binary.Write(h, binary.LittleEndian, d.epoch)
	for _, k := range keys {
		_ = binary.Write(h, binary.LittleEndian, uint64(len(k)))
		h.Write([]byte(k))
		_ = binary.Write(h, binary.LittleEndian, d.tables[k])
	}
	d.mu.Unlock()
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return binary.LittleEndian.Uint64(sum[:8])
}

// bump raises every table's version.
func (d *dataVersion) bump() {
	d.mu.Lock()
	d.epoch++
	d.mu.Unlock()
	d.version.Add(1)
}

// bumpTables raises the versions of the tables keys, tableKey's, or every
// table's when ok is false.
func (d *dataVersion) bumpTables(keys []string, ok bool) {
	if !ok {
		d.bump()
		return
	}
	d.mu.Lock()
	if d.tables == nil {
		d.tables = map[string]uint64{}
	}
	for _, k := range keys {
		d.tables[k]++
	}
	d.mu.Unlock()
	d.version.Add(1)
}

// metricsPath matches the reports of scans' metrics that clients send a
// table, which change nothing.
var metricsPath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/[^/?]+/tables/[^/?]+/metrics$`)

// namespacesPath is where a namespace is made (POST), and
// namespacePropertiesPath where its properties are set; neither has a
// query.
var (
	namespacesPath          = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces$`)
	namespacePropertiesPath = regexp.MustCompile(`^v1/(?:[^/?]+/)?namespaces/[^/?]+/properties$`)
)

// tableKey identifies a table for its version: its namespace's levels and
// its name, unescaped, in lower case.
func tableKey(ns []string, name string) string {
	return strings.ToLower(strings.Join(ns, "\x1f") + "/" + name)
}

// loadKeyOf is loadKey of a load's table, "" for what is not one.
func loadKeyOf(table string) string {
	if table == "" {
		return ""
	}
	return loadKey(table)
}

// loadKey is the tableKey of a load's table, whose path is as tableLoad
// has it.
func loadKey(path string) string {
	ns, name, _ := strings.Cut(path, "/")
	return tableKey(namespace(ns), unescape(name))
}

// changedTables returns the tables a change, a request of method to the
// catalog's path rest with body, may change, by tableKey: ok is false
// when it cannot tell. none says the request changes no table's rows or
// columns, as a scan's metrics report.
func changedTables(method, rest string, body []byte) (keys []string, ok, none bool) {
	path, _, _ := strings.Cut(rest, "?")
	if method == http.MethodPost && strings.HasSuffix(path, "/metrics") && metricsPath.MatchString(path) {
		return nil, true, true
	}
	if m := loadTablePath.FindStringSubmatch(rest); m != nil && (method == http.MethodPost || method == http.MethodDelete) {
		return []string{tableKey(namespace(m[2]), unescape(m[3]))}, true, false
	}
	// Making a namespace, dropping one (only an empty one can be) or
	// setting its properties changes no table, though it changes the
	// listings: CREATE SCHEMA IF NOT EXISTS, which dbt runs each time,
	// raised every table's version.
	if method == http.MethodPost && namespacesPath.MatchString(path) ||
		method == http.MethodDelete && namespaceCheckPath.MatchString(rest) ||
		method == http.MethodPost && namespacePropertiesPath.MatchString(path) {
		return nil, true, false
	}
	type ident struct {
		Namespace []string `json:"namespace"`
		Name      string   `json:"name"`
	}
	switch {
	case method == http.MethodPost && strings.HasSuffix(path, "/transactions/commit"):
		var tx struct {
			TableChanges []struct {
				Identifier *ident `json:"identifier"`
			} `json:"table-changes"`
		}
		if json.Unmarshal(body, &tx) != nil {
			return nil, false, false
		}
		for _, c := range tx.TableChanges {
			if c.Identifier == nil {
				return nil, false, false
			}
			keys = append(keys, tableKey(c.Identifier.Namespace, c.Identifier.Name))
		}
		return keys, true, false
	case method == http.MethodPost && createTablePath.MatchString(rest):
		var c struct {
			Name string `json:"name"`
		}
		m := createTablePath.FindStringSubmatch(rest)
		if json.Unmarshal(body, &c) != nil || c.Name == "" {
			return nil, false, false
		}
		return []string{tableKey(namespace(m[1]), c.Name)}, true, false
	case method == http.MethodPost && renameTablePath.MatchString(rest):
		var c struct {
			Source      *ident `json:"source"`
			Destination *ident `json:"destination"`
		}
		if json.Unmarshal(body, &c) != nil || c.Source == nil || c.Destination == nil {
			return nil, false, false
		}
		return []string{tableKey(c.Source.Namespace, c.Source.Name), tableKey(c.Destination.Namespace, c.Destination.Name)}, true, false
	}
	return nil, false, false
}

// loaded notes that a load of the table at path found its metadata at
// location, "" when it could not be read.
func (d *dataVersion) loaded(path, location string) {
	if location == "" {
		d.bumpTables([]string{loadKey(path)}, true)
		return
	}
	d.mu.Lock()
	if d.locations == nil {
		d.locations = map[string]string{}
	}
	last, seen := d.locations[path]
	d.locations[path] = location
	d.mu.Unlock()
	if seen && last != location {
		d.bumpTables([]string{loadKey(path)}, true)
	}
}

// tableLoad returns the table a request loads, as its path without the
// API's prefix, or "".
func tableLoad(r *http.Request, rest string) string {
	if r.Method != http.MethodGet {
		return ""
	}
	m := loadTablePath.FindStringSubmatch(rest)
	if m == nil {
		return ""
	}
	return strings.ToLower(m[2] + "/" + m[3])
}

// locationScanner reads the metadata-location of a table load's answer as
// it passes, without keeping the answer: it follows the JSON's strings and
// nesting, and takes the string that follows that key at the top level.
type locationScanner struct {
	depth      int
	inString   bool
	escaped    bool
	afterColon bool
	// text is the string being read, while it is at the top level and
	// short enough; long says it was not.
	text []byte
	long bool
	key  string

	location string
	done     bool
}

// maxLocation bounds the strings the scanner keeps.
const maxLocation = 4096

func (s *locationScanner) Write(b []byte) {
	for _, c := range b {
		if s.done {
			return
		}
		if s.inString {
			switch {
			case s.escaped:
				s.escaped = false
			case c == '\\':
				s.escaped = true
			case c == '"':
				s.inString = false
				s.endString()
				continue
			}
			if s.depth == 1 {
				if len(s.text) < maxLocation {
					s.text = append(s.text, c)
				} else {
					s.long = true
				}
			}
			continue
		}
		switch c {
		case '"':
			s.inString, s.text, s.long = true, s.text[:0], false
		case '{', '[':
			s.depth++
			s.afterColon = false
		case '}', ']':
			s.depth--
		case ':':
			s.afterColon = s.depth == 1
		case ',':
			s.afterColon = false
		}
	}
}

// endString takes a top-level string: a key, or the value after one.
func (s *locationScanner) endString() {
	if s.depth != 1 {
		return
	}
	if !s.afterColon {
		s.key = string(s.text)
		return
	}
	s.afterColon = false
	if s.key == "metadata-location" && !s.long && !strings.ContainsRune(string(s.text), '\\') {
		s.location, s.done = string(s.text), true
	}
}
