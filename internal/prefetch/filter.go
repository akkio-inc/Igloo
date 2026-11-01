package prefetch

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/apache/arrow-go/v18/parquet/metadata"
)

// What a filtered scan reads ahead. DuckDB skips a row group of a scan
// whose statistics (the least and greatest value of each column chunk)
// rule out the scan's filters, those of a WHERE clause and those it takes
// from a join's other side, and one whose Bloom filter rules out the
// values an equality looks for; Iceberg's manifests let it skip whole files
// the same way. Before a statement runs, the prefetcher does not know the
// filters' values, but a file's footer tells whether they could skip
// anything: when every row group of a file holds values from across the
// range of each filtered column, as a column in no particular order does
// (an audience's people, a random key), no filter on a range skips any
// row group, and DuckDB reads the file's projected columns whole, which
// the prefetcher then reads ahead in full. Otherwise (row groups of narrow
// ranges, as of a column the file is sorted by; files of narrow ranges
// within the scan's, as of a table partitioned by it; a Bloom filter; or
// statistics it cannot compare) it reads ahead only the file's footer,
// which DuckDB reads first anyway: reading ahead what a filter skips only
// costs requests and the cache's room.

const (
	// wideGroup is the share of a file's range of a column that a row
	// group's range must cover to count as wide.
	wideGroup = 0.5
	// wideGroups is the share of a file's row groups that must be wide for
	// a filter on the column to skip none of them.
	wideGroups = 0.9
	// wideFile is the share of the scan's range of a column, over the
	// files read so far and the next, that a file's range must cover for
	// a filter on the column not to skip whole files.
	wideFile = 0.6
)

// colRange is what a file's footer says of one leaf column a filter names:
// order-preserving keys (see statKey) of each row group's least and
// greatest value and of the file's, whether every row group has both, and
// whether any has a Bloom filter.
type colRange struct {
	known  bool
	bloom  bool
	lo, hi []byte
	groups [][2][]byte
}

// scanFilter decides, file by file, what of a filtered scan to read ahead.
type scanFilter struct {
	// ids are the field IDs of the columns the filters name.
	ids map[int32]bool
	// span is each filtered column's range over the scan's files so far,
	// by its field ID.
	span map[int32][2][]byte
	// whole, footers and divided count, for the log, the files read ahead
	// in full and by footer only, and whether reading stopped because the
	// files divide the filters' values.
	whole, footers int
	divided        bool
}

func newScanFilter(ids []int32) *scanFilter {
	f := &scanFilter{ids: map[int32]bool{}, span: map[int32][2][]byte{}}
	for _, id := range ids {
		f.ids[id] = true
	}
	return f
}

// add reads, from a file's footer, the ranges of the leaf columns the
// filters name, by field ID, and widens the scan's span by them.
func (f *scanFilter) add(meta *metadata.FileMetaData) map[int32]*colRange {
	out := map[int32]*colRange{}
	for i := 0; i < meta.Schema.NumColumns(); i++ {
		node := meta.Schema.Column(i).SchemaNode()
		if !projected(node, f.ids) {
			continue
		}
		c := &colRange{known: true}
		for g := 0; g < meta.NumRowGroups(); g++ {
			cc, err := meta.RowGroup(g).ColumnChunk(i)
			if err != nil {
				c.known = false
				break
			}
			if cc.BloomFilterOffset() > 0 {
				c.bloom = true
			}
			lo, hi, ok := statRange(cc)
			if !ok {
				c.known = false
				continue
			}
			c.groups = append(c.groups, [2][]byte{lo, hi})
			if c.lo == nil || bytes.Compare(lo, c.lo) < 0 {
				c.lo = lo
			}
			if c.hi == nil || bytes.Compare(hi, c.hi) > 0 {
				c.hi = hi
			}
		}
		if c.lo == nil {
			c.known = false
		}
		id := node.FieldID()
		out[id] = c
		if c.known {
			sp, ok := f.span[id]
			if !ok || bytes.Compare(c.lo, sp[0]) < 0 {
				sp[0] = c.lo
			}
			if !ok || bytes.Compare(c.hi, sp[1]) > 0 {
				sp[1] = c.hi
			}
			f.span[id] = sp
		}
	}
	return out
}

// skippable reports whether DuckDB may skip row groups of a file whose
// filtered columns are cols by the filters on them, and files whether it
// may skip whole files of the scan: the file's range of a column is narrow
// within the scan's.
func (f *scanFilter) skippable(cols map[int32]*colRange) (skip, files bool) {
	if len(cols) == 0 {
		// No column the filters name, or none known: they may skip
		// anything.
		return true, false
	}
	for id, c := range cols {
		if !c.known || c.bloom {
			skip = true
			continue
		}
		if len(c.groups) > 1 {
			wide := 0
			for _, g := range c.groups {
				if spread(c.lo, c.hi, g[0], g[1]) >= wideGroup {
					wide++
				}
			}
			if float64(wide) < wideGroups*float64(len(c.groups)) {
				skip = true
			}
		}
		if sp, ok := f.span[id]; ok && spread(sp[0], sp[1], c.lo, c.hi) < wideFile {
			skip, files = true, true
		}
	}
	return skip, files
}

// spread is the share of the range lo..hi that a..b, within it, covers,
// from the first 8 bytes in which lo and hi differ; 1 when lo and hi are
// equal.
func spread(lo, hi, a, b []byte) float64 {
	n := 0
	for n < len(lo) && n < len(hi) && lo[n] == hi[n] {
		n++
	}
	vlo, vhi, va, vb := keyValue(lo, n), keyValue(hi, n), keyValue(a, n), keyValue(b, n)
	if vhi <= vlo {
		return 1
	}
	if vb <= va {
		return 0
	}
	// Differences first: as floats, numbers near 2^63 lose their low digits.
	return float64(vb-va) / float64(vhi-vlo)
}

// keyValue is the 8 bytes of key k from byte n on, as a number.
func keyValue(k []byte, n int) uint64 {
	var buf [8]byte
	if n < len(k) {
		copy(buf[:], k[n:])
	}
	return binary.BigEndian.Uint64(buf[:])
}

// statRange returns order-preserving keys of a column chunk's least and
// greatest value, if its statistics have them and their type compares:
// integers (dates, times, timestamps and decimals among them), floats,
// booleans and byte arrays (strings).
func statRange(cc *metadata.ColumnChunkMetaData) (lo, hi []byte, ok bool) {
	if set, err := cc.StatsSet(); err != nil || !set {
		return nil, nil, false
	}
	st, err := cc.Statistics()
	if err != nil || st == nil || !st.HasMinMax() {
		return nil, nil, false
	}
	switch s := st.(type) {
	case *metadata.Int32Statistics:
		return intKey(int64(s.Min())), intKey(int64(s.Max())), true
	case *metadata.Int64Statistics:
		return intKey(s.Min()), intKey(s.Max()), true
	case *metadata.Float32Statistics:
		return floatKey(float64(s.Min())), floatKey(float64(s.Max())), !math.IsNaN(float64(s.Min())) && !math.IsNaN(float64(s.Max()))
	case *metadata.Float64Statistics:
		return floatKey(s.Min()), floatKey(s.Max()), !math.IsNaN(s.Min()) && !math.IsNaN(s.Max())
	case *metadata.BooleanStatistics:
		return boolKey(s.Min()), boolKey(s.Max()), true
	case *metadata.ByteArrayStatistics:
		return bytes.Clone(s.Min()), bytes.Clone(s.Max()), true
	}
	return nil, nil, false
}

func intKey(v int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(v)^(1<<63))
}

func floatKey(v float64) []byte {
	b := math.Float64bits(v)
	if b>>63 == 1 {
		b = ^b
	} else {
		b |= 1 << 63
	}
	return binary.BigEndian.AppendUint64(nil, b)
}

func boolKey(v bool) []byte {
	if v {
		return intKey(1)
	}
	return intKey(0)
}
