package ml

import (
	"math"
	"sort"
)

// Encoder turns features into vectors of numbers, for the models that take
// vectors (neural networks, k-means, PCA): each numeric feature one slot,
// standardized or not, its missing values at its mean; each categorical
// feature a slot per frequent category, 1 for a row in that category and
// 0 otherwise (rare, unseen and missing categories are all zeros).
type Encoder struct {
	Inputs []EncodedInput `json:"inputs"`
	// Width is the vectors' length.
	Width int `json:"width"`
}

// EncodedInput is how one feature is encoded, at slots Offset onwards.
type EncodedInput struct {
	Name string `json:"name"`
	Kind Kind   `json:"kind"`
	// A numeric feature's value v becomes (v - Shift) / Scale; Mean stands
	// in for a missing value.
	Mean  float64 `json:"mean,omitempty"`
	Shift float64 `json:"shift,omitempty"`
	Scale float64 `json:"scale,omitempty"`
	// Levels are a categorical feature's slots, in order.
	Levels []string `json:"levels,omitempty"`
	Offset int      `json:"offset"`

	index map[string]int
}

// FitEncoder learns an encoding of f's columns: numbers standardized (to
// mean 0 and standard deviation 1) when standardize, and categories that
// occur at least minCount times, the maxLevels most frequent of them.
func FitEncoder(f *Frame, standardize bool, minCount, maxLevels int) *Encoder {
	e := &Encoder{}
	for ci := range f.Cols {
		c := &f.Cols[ci]
		in := EncodedInput{Name: c.Name, Kind: c.Kind, Offset: e.Width, Scale: 1}
		if c.Kind == Numeric {
			var sum, sq float64
			n := 0
			for _, v := range c.Num {
				if !math.IsNaN(v) {
					sum += v
					n++
				}
			}
			if n > 0 {
				in.Mean = sum / float64(n)
				for _, v := range c.Num {
					if !math.IsNaN(v) {
						sq += (v - in.Mean) * (v - in.Mean)
					}
				}
			}
			if standardize {
				in.Shift = in.Mean
				if sd := math.Sqrt(sq / float64(max(n, 1))); sd > 0 {
					in.Scale = sd
				}
			}
			e.Width++
		} else {
			counts := make([]int, len(c.Levels))
			for _, code := range c.Codes {
				if code >= 0 {
					counts[code]++
				}
			}
			order := make([]int, 0, len(counts))
			for i, n := range counts {
				if n >= max(minCount, 1) {
					order = append(order, i)
				}
			}
			sort.Slice(order, func(i, j int) bool {
				a, b := order[i], order[j]
				if counts[a] != counts[b] {
					return counts[a] > counts[b]
				}
				return c.Levels[a] < c.Levels[b]
			})
			if maxLevels > 0 && len(order) > maxLevels {
				order = order[:maxLevels]
			}
			for _, i := range order {
				in.Levels = append(in.Levels, c.Levels[i])
			}
			e.Width += len(in.Levels)
		}
		e.Inputs = append(e.Inputs, in)
	}
	e.Prepare()
	return e
}

// Prepare readies a decoded encoder for use from several goroutines.
func (e *Encoder) Prepare() {
	for i := range e.Inputs {
		in := &e.Inputs[i]
		if in.Kind == Categorical && in.index == nil {
			in.index = make(map[string]int, len(in.Levels))
			for k, l := range in.Levels {
				in.index[l] = k
			}
		}
	}
}

// Encode returns f's rows as vectors, row-major (row i at
// out[i*Width:(i+1)*Width]), matching f's columns to the inputs by name.
func (e *Encoder) Encode(f *Frame) []float64 {
	out := make([]float64, f.N*e.Width)
	for ii := range e.Inputs {
		in := &e.Inputs[ii]
		c := f.Col(in.Name)
		if in.Kind == Numeric {
			for i := range f.N {
				v := in.Mean
				if c != nil && c.Kind == Numeric && !math.IsNaN(c.Num[i]) {
					v = c.Num[i]
				}
				out[i*e.Width+in.Offset] = (v - in.Shift) / in.Scale
			}
			continue
		}
		if c == nil || c.Kind != Categorical {
			continue
		}
		slot := make([]int, len(c.Levels))
		for k, l := range c.Levels {
			slot[k] = -1
			if s, ok := in.index[l]; ok {
				slot[k] = s
			}
		}
		for i, code := range c.Codes {
			if code >= 0 && slot[code] >= 0 {
				out[i*e.Width+in.Offset+slot[code]] = 1
			}
		}
	}
	return out
}

// EncodeRow encodes one row of the inputs' features, in their order, into
// out (Width long).
func (e *Encoder) EncodeRow(r *RowValues, out []float64) {
	clear(out)
	for j := range e.Inputs {
		in := &e.Inputs[j]
		if in.Kind == Numeric {
			v := r.Num[j]
			if math.IsNaN(v) {
				v = in.Mean
			}
			out[in.Offset] = (v - in.Shift) / in.Scale
			continue
		}
		if !r.Missing[j] {
			if s, ok := in.index[r.Str[j]]; ok {
				out[in.Offset+s] = 1
			}
		}
	}
}

// Slot describes slot k: its feature, and its category for a categorical
// feature's.
func (e *Encoder) Slot(k int) (feature, level string) {
	for _, in := range e.Inputs {
		if in.Kind == Numeric && k == in.Offset {
			return in.Name, ""
		}
		if in.Kind == Categorical && k >= in.Offset && k < in.Offset+len(in.Levels) {
			return in.Name, in.Levels[k-in.Offset]
		}
	}
	return "", ""
}

// Decode turns a vector's numeric slots back to the features' scale: the
// value slot k stands for.
func (e *Encoder) Decode(k int, v float64) float64 {
	for _, in := range e.Inputs {
		if in.Kind == Numeric && k == in.Offset {
			return v*in.Scale + in.Shift
		}
	}
	return v
}
