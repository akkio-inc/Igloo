package catalogproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
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
type dataVersion struct {
	version atomic.Uint64

	mu sync.Mutex
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
		p.data.bump()
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

func (d *dataVersion) bump() { d.version.Add(1) }

// loaded notes that a load of the table at path found its metadata at
// location, "" when it could not be read.
func (d *dataVersion) loaded(path, location string) {
	if location == "" {
		d.bump()
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
		d.bump()
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
