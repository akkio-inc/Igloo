package ml

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"slices"
	"sort"
	"strings"
)

// Kind is how a feature is read: numbers are continuous, everything else
// a category, as Snowflake's ML functions read them.
type Kind string

const (
	Numeric     Kind = "numeric"
	Categorical Kind = "categorical"
)

// Column is one feature's values. A numeric column holds them in Num, NaN
// for a missing value; a categorical one holds codes into Levels in Codes,
// -1 for a missing value.
type Column struct {
	Name   string
	Kind   Kind
	Num    []float64
	Codes  []int32
	Levels []string
}

// Len is the column's number of rows.
func (c *Column) Len() int {
	if c.Kind == Numeric {
		return len(c.Num)
	}
	return len(c.Codes)
}

// Frame is a table of features, all of the same length.
type Frame struct {
	Cols []Column
	N    int
}

// NumericColumn makes a numeric column.
func NumericColumn(name string, v []float64) Column {
	return Column{Name: name, Kind: Numeric, Num: v}
}

// CategoricalColumn makes a categorical column from strings; nil entries
// in missing (if given) mark missing values.
func CategoricalColumn(name string, v []string, missing []bool) Column {
	c := Column{Name: name, Kind: Categorical, Codes: make([]int32, len(v))}
	index := map[string]int32{}
	for i, s := range v {
		if missing != nil && missing[i] {
			c.Codes[i] = -1
			continue
		}
		code, ok := index[s]
		if !ok {
			code = int32(len(c.Levels))
			index[s] = code
			c.Levels = append(c.Levels, s)
		}
		c.Codes[i] = code
	}
	return c
}

// Builder appends rows to a column whose kind it is given.
type Builder struct {
	col   Column
	index map[string]int32
}

// NewBuilder starts a column.
func NewBuilder(name string, kind Kind) *Builder {
	return &Builder{col: Column{Name: name, Kind: kind}, index: map[string]int32{}}
}

// AddNum appends a number; NaN is missing.
func (b *Builder) AddNum(v float64) { b.col.Num = append(b.col.Num, v) }

// AddString appends a category.
func (b *Builder) AddString(s string) {
	code, ok := b.index[s]
	if !ok {
		code = int32(len(b.col.Levels))
		b.index[s] = code
		b.col.Levels = append(b.col.Levels, s)
	}
	b.col.Codes = append(b.col.Codes, code)
}

// AddMissing appends a missing value.
func (b *Builder) AddMissing() {
	if b.col.Kind == Numeric {
		b.col.Num = append(b.col.Num, math.NaN())
	} else {
		b.col.Codes = append(b.col.Codes, -1)
	}
}

// Column returns what was built.
func (b *Builder) Column() Column { return b.col }

// Check reports a frame whose columns differ in length or are unnamed.
func (f *Frame) Check() error {
	seen := map[string]bool{}
	for i := range f.Cols {
		c := &f.Cols[i]
		if c.Len() != f.N {
			return fmt.Errorf("column %s has %d rows, not %d", c.Name, c.Len(), f.N)
		}
		key := strings.ToLower(c.Name)
		if seen[key] {
			return fmt.Errorf("column %s appears twice", c.Name)
		}
		seen[key] = true
	}
	return nil
}

// Col returns the column called name (case-insensitively), or nil.
func (f *Frame) Col(name string) *Column {
	for i := range f.Cols {
		if strings.EqualFold(f.Cols[i].Name, name) {
			return &f.Cols[i]
		}
	}
	return nil
}

// Take returns the frame's rows at rows, in that order.
func (f *Frame) Take(rows []int) *Frame {
	out := &Frame{N: len(rows), Cols: make([]Column, len(f.Cols))}
	for j := range f.Cols {
		c := &f.Cols[j]
		o := Column{Name: c.Name, Kind: c.Kind, Levels: c.Levels}
		if c.Kind == Numeric {
			o.Num = make([]float64, len(rows))
			for i, r := range rows {
				o.Num[i] = c.Num[r]
			}
		} else {
			o.Codes = make([]int32, len(rows))
			for i, r := range rows {
				o.Codes[i] = c.Codes[r]
			}
		}
		out.Cols[j] = o
	}
	return out
}

// Without returns the frame without the columns named (case-insensitively).
func (f *Frame) Without(names ...string) *Frame {
	out := &Frame{N: f.N}
	for _, c := range f.Cols {
		if !slices.ContainsFunc(names, func(n string) bool { return strings.EqualFold(n, c.Name) }) {
			out.Cols = append(out.Cols, c)
		}
	}
	return out
}

// RowHashes returns a 64-bit hash of each row's values, with extra mixed in
// (a target, say), that does not depend on the row's position.
func (f *Frame) RowHashes(extra []float64) []uint64 {
	out := make([]uint64, f.N)
	var buf [8]byte
	for i := range f.N {
		h := fnv.New64a()
		for j := range f.Cols {
			c := &f.Cols[j]
			if c.Kind == Numeric {
				v := c.Num[i]
				if math.IsNaN(v) {
					v = math.NaN() // one bit pattern for every missing value
				}
				binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
				h.Write(buf[:]) //nolint:errcheck
			} else if code := c.Codes[i]; code < 0 {
				h.Write([]byte{0xff, 0}) //nolint:errcheck
			} else {
				h.Write([]byte(c.Levels[code])) //nolint:errcheck
				h.Write([]byte{0})              //nolint:errcheck
			}
		}
		if extra != nil {
			binary.LittleEndian.PutUint64(buf[:], math.Float64bits(extra[i]))
			h.Write(buf[:]) //nolint:errcheck
		}
		out[i] = h.Sum64()
	}
	return out
}

// CanonicalOrder returns the frame's row numbers sorted so that the same
// rows, in whatever order a query returned them, come out in the same
// order: by the hash of their values, then by the values themselves.
// Training on rows in this order makes a model depend only on the rows,
// not on the order DuckDB's threads read them in.
func (f *Frame) CanonicalOrder(extra []float64) []int {
	hashes := f.RowHashes(extra)
	rows := make([]int, f.N)
	for i := range rows {
		rows[i] = i
	}
	sort.SliceStable(rows, func(a, b int) bool {
		ra, rb := rows[a], rows[b]
		if hashes[ra] != hashes[rb] {
			return hashes[ra] < hashes[rb]
		}
		return f.compareRows(ra, rb, extra) < 0
	})
	return rows
}

// compareRows orders two rows by their values, for rows whose hashes
// collide.
func (f *Frame) compareRows(a, b int, extra []float64) int {
	for j := range f.Cols {
		c := &f.Cols[j]
		if c.Kind == Numeric {
			if d := compareFloat(c.Num[a], c.Num[b]); d != 0 {
				return d
			}
			continue
		}
		la, lb := "", ""
		if c.Codes[a] >= 0 {
			la = "\x01" + c.Levels[c.Codes[a]]
		}
		if c.Codes[b] >= 0 {
			lb = "\x01" + c.Levels[c.Codes[b]]
		}
		if d := strings.Compare(la, lb); d != 0 {
			return d
		}
	}
	if extra != nil {
		return compareFloat(extra[a], extra[b])
	}
	return 0
}

func compareFloat(a, b float64) int {
	switch an, bn := math.IsNaN(a), math.IsNaN(b); {
	case an && bn:
		return 0
	case an:
		return -1
	case bn:
		return 1
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// mix64 is SplitMix64's finalizer, for hashing row numbers and seeds into
// well-spread values.
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// unitHash maps (seed, salt, i) to a number in [0, 1).
func unitHash(seed, salt uint64, i int) float64 {
	return float64(mix64(seed^mix64(salt^mix64(uint64(i))))>>11) / (1 << 53)
}
