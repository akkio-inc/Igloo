package ml

import (
	"math"
	"sort"
	"strconv"
)

// Values reach trees as bins of at most a byte: bin 0 holds missing values;
// a numeric feature's bins 1..len(Bounds)+1 hold ranges of values, and a
// categorical feature's bins 1..len(Levels) its frequent categories, and
// bin len(Levels)+1 every other category ("other"), including categories
// training never saw.
const (
	missingBin = 0
	// MissingBin is the bin of a missing value, for scorers that bin
	// values themselves.
	MissingBin = missingBin
	maxBins    = 256
	// binSample is how many values numeric bins are cut from.
	binSample = 200_000
)

// Binner maps one feature's values to bins.
type Binner struct {
	Name string `json:"name"`
	Kind Kind   `json:"kind"`
	// Bounds are a numeric feature's bins' upper bounds: bin k+1 holds the
	// values in (Bounds[k-1], Bounds[k]], the last bin those above every
	// bound.
	Bounds []float64 `json:"bounds,omitempty"`
	// Levels are a categorical feature's categories with bins of their own.
	Levels []string `json:"levels,omitempty"`
	// Other says some category in training had no bin of its own.
	Other bool `json:"other,omitempty"`

	index map[string]uint8
}

// prepare readies a binner for use from several goroutines at once.
func (b *Binner) prepare() {
	if b.Kind == Categorical && b.index == nil {
		b.index = make(map[string]uint8, len(b.Levels))
		for i, l := range b.Levels {
			b.index[l] = uint8(i + 1)
		}
	}
}

// NumBins is the number of bins, the missing bin included.
func (b *Binner) NumBins() int {
	if b.Kind == Numeric {
		return len(b.Bounds) + 2
	}
	return len(b.Levels) + 2
}

// otherBin is a categorical feature's bin for other categories.
func (b *Binner) otherBin() uint8 { return uint8(len(b.Levels) + 1) }

// NumBin returns the bin of a number.
func (b *Binner) NumBin(v float64) uint8 {
	if math.IsNaN(v) {
		return missingBin
	}
	return uint8(sort.SearchFloat64s(b.Bounds, v) + 1)
}

// CatBin returns the bin of a category. The binner must be prepared.
func (b *Binner) CatBin(s string) uint8 {
	if k, ok := b.index[s]; ok {
		return k
	}
	return b.otherBin()
}

// BinLabel describes bin k for explanations: a range of numbers or a
// category.
func (b *Binner) BinLabel(k int) string {
	switch {
	case k == missingBin:
		return "NULL"
	case b.Kind == Categorical && k <= len(b.Levels):
		return b.Levels[k-1]
	case b.Kind == Categorical:
		return "(other)"
	}
	return strconv.FormatFloat(b.upper(k), 'g', -1, 64)
}

// upper is the upper bound of numeric bin k, +Inf for the last.
func (b *Binner) upper(k int) float64 {
	if k-1 < len(b.Bounds) {
		return b.Bounds[k-1]
	}
	return math.Inf(1)
}

// newBinner cuts a column's bins. Numbers get up to maxBins-2 bins of
// about equal counts, a bin of their own for each value when there are
// that few; categories that occur at least minCount times get a bin of
// their own, the most frequent first, the rest share one. Rare categories
// are kept out of models on purpose: a category that a handful of rows
// have can identify them.
func newBinner(c *Column, minCount int, seed uint64) *Binner {
	b := &Binner{Name: c.Name, Kind: c.Kind}
	if c.Kind == Categorical {
		counts := make([]int, len(c.Levels))
		for _, code := range c.Codes {
			if code >= 0 {
				counts[code]++
			}
		}
		order := make([]int, 0, len(counts))
		for i, n := range counts {
			if n > 0 {
				order = append(order, i)
			}
		}
		sort.Slice(order, func(i, j int) bool {
			a, z := order[i], order[j]
			if counts[a] != counts[z] {
				return counts[a] > counts[z]
			}
			return c.Levels[a] < c.Levels[z]
		})
		for _, i := range order {
			if counts[i] < minCount || len(b.Levels) == maxBins-3 {
				b.Other = true
				continue
			}
			b.Levels = append(b.Levels, c.Levels[i])
		}
		b.prepare()
		return b
	}
	vals := make([]float64, 0, min(len(c.Num), binSample))
	n := len(c.Num)
	rate := float64(binSample) / float64(n)
	for i, v := range c.Num {
		if math.IsNaN(v) {
			continue
		}
		if n > binSample && unitHash(seed, 0xb1, i) >= rate {
			continue
		}
		vals = append(vals, v)
	}
	b.Bounds = cutBounds(vals, maxBins-2)
	return b
}

// cutBounds returns at most bins-1 bounds that split vals into bins of
// about equal counts, each distinct value in one bin.
func cutBounds(vals []float64, bins int) []float64 {
	if len(vals) == 0 {
		return nil
	}
	sort.Float64s(vals)
	var distinct []float64
	var counts []int
	for i, v := range vals {
		if i == 0 || v != vals[i-1] {
			distinct = append(distinct, v)
			counts = append(counts, 0)
		}
		counts[len(counts)-1]++
	}
	mid := func(i int) float64 {
		a, z := distinct[i], distinct[i+1]
		m := a + (z-a)/2
		if m <= a || m >= z || math.IsInf(m, 0) {
			return a
		}
		return m
	}
	var bounds []float64
	if len(distinct) <= bins {
		for i := 0; i+1 < len(distinct); i++ {
			bounds = append(bounds, mid(i))
		}
		return bounds
	}
	per := float64(len(vals)) / float64(bins)
	seen := 0
	next := per
	for i := 0; i+1 < len(distinct); i++ {
		seen += counts[i]
		if float64(seen) >= next {
			bounds = append(bounds, mid(i))
			for next <= float64(seen) {
				next += per
			}
			if len(bounds) == bins-1 {
				break
			}
		}
	}
	return bounds
}

// binned holds a frame's values as bins, a column per feature.
type binned struct {
	n    int
	bins [][]uint8
}

// binFrame bins every row of f with the binners.
func binFrame(f *Frame, binners []*Binner) *binned {
	out := &binned{n: f.N, bins: make([][]uint8, len(binners))}
	parallelFor(len(binners), 0, func(j int) {
		c, b := &f.Cols[j], binners[j]
		col := make([]uint8, f.N)
		if c.Kind == Numeric {
			for i, v := range c.Num {
				col[i] = b.NumBin(v)
			}
		} else {
			lookup := make([]uint8, len(c.Levels))
			for i, l := range c.Levels {
				lookup[i] = b.CatBin(l)
			}
			for i, code := range c.Codes {
				if code < 0 {
					col[i] = missingBin
				} else {
					col[i] = lookup[code]
				}
			}
		}
		out.bins[j] = col
	})
	return out
}
