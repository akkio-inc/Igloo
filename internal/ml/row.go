package ml

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
)

// RowValues is one row of a model's features as a scorer reads them, in
// the model's order: feature j's number in Num[j] (NaN when missing) if it
// is numeric, its category in Str[j] if it is categorical, with Missing[j]
// marking a missing category.
type RowValues struct {
	Num     []float64
	Str     []string
	Missing []bool
}

// Size readies r for n features.
func (r *RowValues) Size(n int) {
	if cap(r.Num) < n {
		r.Num, r.Str, r.Missing = make([]float64, n), make([]string, n), make([]bool, n)
	}
	r.Num, r.Str, r.Missing = r.Num[:n], r.Str[:n], r.Missing[:n]
}

// Frame returns a frame of one row holding r's values for features of
// the kinds given, named as given.
func (r *RowValues) Frame(names []string, kinds []Kind) *Frame {
	f := &Frame{N: 1}
	for j, k := range kinds {
		if k == Numeric {
			f.Cols = append(f.Cols, NumericColumn(names[j], []float64{r.Num[j]}))
		} else {
			f.Cols = append(f.Cols, CategoricalColumn(names[j], []string{r.Str[j]}, []bool{r.Missing[j]}))
		}
	}
	return f
}

// BinRow bins a row of the model's features into out (len(m.Features)).
func (m *GBM) BinRow(r *RowValues, out []uint8) {
	for j, b := range m.Features {
		if b.Kind == Numeric {
			out[j] = b.NumBin(r.Num[j])
		} else if r.Missing[j] {
			out[j] = missingBin
		} else {
			out[j] = b.CatBin(r.Str[j])
		}
	}
}

// Float32s is a vector stored compactly in a model's blob: little-endian
// float32s, in base64. Large factor matrices are a quarter of their size
// as JSON numbers this way.
type Float32s []float32

func (v Float32s) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 4*len(v))
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, errors.New("model holds a non-finite number")
		}
		binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(x))
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(buf))
}

func (v *Float32s) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(buf)%4 != 0 {
		return errors.New("bad float32 vector in model blob")
	}
	out := make(Float32s, len(buf)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	*v = out
	return nil
}

// Int32s is a vector of int32s stored as little-endian bytes in base64.
type Int32s []int32

func (v Int32s) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 4*len(v))
	for i, x := range v {
		binary.LittleEndian.PutUint32(buf[4*i:], uint32(x))
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(buf))
}

func (v *Int32s) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(buf)%4 != 0 {
		return errors.New("bad int32 vector in model blob")
	}
	out := make(Int32s, len(buf)/4)
	for i := range out {
		out[i] = int32(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	*v = out
	return nil
}
