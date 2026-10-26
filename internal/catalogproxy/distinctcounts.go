package catalogproxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"hash/crc64"
	"io"
	"maps"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/akkio-inc/igloo/internal/catalog"
)

// Distinct counts. ANALYZE keeps the distinct counts of a table's columns
// in the table property catalog.DistinctCountsProperty, which the engine
// hands to DuckDB's planner. DuckDB loads every table a statement reads as
// it binds the statement, so the proxy takes each table's counts from those
// loads, as they pass, and the engine never asks the catalog for them.

// maxCountsBody bounds the table loads whose counts the proxy reads.
const maxCountsBody = 8 << 20

// distinctCounts are the tables' counts as their last loads had them, and
// those loads' fingerprints: a table's metadata, which holds every snapshot
// it had, can take tens of milliseconds to decode, and DuckDB loads every
// table each time it binds a statement, so a load that is the last one
// again is not decoded again.
type distinctCounts struct {
	gen    atomic.Uint64
	mu     sync.Mutex
	tables map[string]map[string]int64
	seen   map[string]uint64
}

var crcTable = crc64.MakeTable(crc64.ECMA)

// DistinctCounts returns the distinct counts of the tables whose last load
// had any, by "namespace.table" (lower-cased, a nested namespace's levels
// joined by dots) and then by column name, and a generation that changes
// whenever they do.
func (p *Proxy) DistinctCounts() (uint64, map[string]map[string]int64) {
	if p == nil {
		return 0, nil
	}
	d := &p.counts
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen.Load(), maps.Clone(d.tables)
}

// countsKey is the key DistinctCounts gives the table a load's path names,
// or "".
func countsKey(rest string) string {
	m := loadTablePath.FindStringSubmatch(rest)
	if m == nil {
		return ""
	}
	ns, err1 := url.PathUnescape(m[2])
	name, err2 := url.PathUnescape(m[3])
	if err1 != nil || err2 != nil {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(ns, "\x1f", ".") + "." + name)
}

// loaded takes the counts of the table key from the body of its load,
// compressed with encoding ("" if not).
func (d *distinctCounts) loaded(key string, body []byte, encoding string) {
	if key == "" {
		return
	}
	fp := crc64.Checksum(body, crcTable) ^ uint64(len(body))<<1 ^ uint64(len(encoding))
	d.mu.Lock()
	same := d.seen[key] == fp
	d.mu.Unlock()
	if same {
		return
	}
	var counts map[string]int64
	if encoding == "gzip" {
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return
		}
		if body, err = io.ReadAll(io.LimitReader(r, maxCountsBody)); err != nil {
			return
		}
	} else if encoding != "" && encoding != "identity" {
		return
	}
	if bytes.Contains(body, []byte(catalog.DistinctCountsProperty)) {
		var load struct {
			Metadata struct {
				Properties map[string]string `json:"properties"`
			} `json:"metadata"`
		}
		if json.Unmarshal(body, &load) != nil {
			return
		}
		if json.Unmarshal([]byte(load.Metadata.Properties[catalog.DistinctCountsProperty]), &counts) != nil {
			counts = nil
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = map[string]uint64{}
	}
	d.seen[key] = fp
	if maps.Equal(d.tables[key], counts) {
		return
	}
	if d.tables == nil {
		d.tables = map[string]map[string]int64{}
	}
	if len(counts) == 0 {
		delete(d.tables, key)
	} else {
		d.tables[key] = counts
	}
	d.gen.Add(1)
}
