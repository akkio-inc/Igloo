package modelpack

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"unsafe"
)

// Tensors are the tensors of a safetensors file, the format Hugging Face
// keeps weights in: an 8-byte little-endian header length, a JSON header
// of each tensor's dtype, shape and byte range, and the data. The file is
// mapped into memory, read only; F32 tensors are read in place, and F16
// and BF16 ones converted to float32.
type Tensors struct {
	data   []byte
	unmap  func() error
	header map[string]tensorInfo
	// Metadata is the header's __metadata__.
	Metadata map[string]string
}

type tensorInfo struct {
	DType   string   `json:"dtype"`
	Shape   []int64  `json:"shape"`
	Offsets [2]int64 `json:"data_offsets"`
}

// Tensor is a tensor's values, row-major, and its shape.
type Tensor struct {
	Shape []int
	Data  []float32
}

// OpenTensors opens the safetensors file at path.
func OpenTensors(path string) (*Tensors, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	data, unmap, err := mapFile(f, st.Size())
	if err != nil {
		return nil, err
	}
	t, err := parseTensors(data)
	if err != nil {
		_ = unmap()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	t.unmap = unmap
	return t, nil
}

// ParseTensors reads safetensors data held in memory.
func ParseTensors(data []byte) (*Tensors, error) { return parseTensors(data) }

func parseTensors(data []byte) (*Tensors, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("not a safetensors file")
	}
	n := binary.LittleEndian.Uint64(data[:8])
	if n > uint64(len(data)-8) || n > 100<<20 {
		return nil, fmt.Errorf("not a safetensors file: header of %d bytes", n)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data[8:8+n], &raw); err != nil {
		return nil, fmt.Errorf("not a safetensors file: %v", err)
	}
	t := &Tensors{data: data[8+n:], header: map[string]tensorInfo{}}
	for name, v := range raw {
		if name == "__metadata__" {
			_ = json.Unmarshal(v, &t.Metadata)
			continue
		}
		var info tensorInfo
		if err := json.Unmarshal(v, &info); err != nil {
			return nil, fmt.Errorf("tensor %s: %v", name, err)
		}
		size := dtypeSize(info.DType)
		if size == 0 {
			return nil, fmt.Errorf("tensor %s has dtype %s; F32, F16 and BF16 are read", name, info.DType)
		}
		count := int64(1)
		for _, d := range info.Shape {
			if d < 0 {
				return nil, fmt.Errorf("tensor %s has a negative dimension", name)
			}
			count *= d
		}
		if info.Offsets[0] < 0 || info.Offsets[1] > int64(len(t.data)) || info.Offsets[1]-info.Offsets[0] != count*size {
			return nil, fmt.Errorf("tensor %s's bytes do not fit its shape", name)
		}
		t.header[name] = info
	}
	return t, nil
}

func dtypeSize(dtype string) int64 {
	switch dtype {
	case "F32":
		return 4
	case "F16", "BF16":
		return 2
	}
	return 0
}

// Names lists the tensors' names, sorted.
func (t *Tensors) Names() []string {
	out := make([]string, 0, len(t.header))
	for k := range t.header {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// Has reports whether the file holds tensor name.
func (t *Tensors) Has(name string) bool {
	_, ok := t.header[name]
	return ok
}

// Get returns tensor name, which must have shape (each -1 is any length);
// shape nil is any shape.
func (t *Tensors) Get(name string, shape ...int) (Tensor, error) {
	info, ok := t.header[name]
	if !ok {
		return Tensor{}, fmt.Errorf("the weights have no tensor %s", name)
	}
	dims := make([]int, len(info.Shape))
	for i, d := range info.Shape {
		dims[i] = int(d)
	}
	if shape != nil {
		ok := len(shape) == len(dims)
		for i := 0; ok && i < len(shape); i++ {
			ok = shape[i] < 0 || shape[i] == dims[i]
		}
		if !ok {
			return Tensor{}, fmt.Errorf("tensor %s has shape %v, not %v", name, dims, shape)
		}
	}
	raw := t.data[info.Offsets[0]:info.Offsets[1]]
	n := len(raw) / int(dtypeSize(info.DType))
	out := Tensor{Shape: dims}
	switch info.DType {
	case "F32":
		if n > 0 && uintptr(unsafe.Pointer(&raw[0]))%4 == 0 && littleEndian {
			out.Data = unsafe.Slice((*float32)(unsafe.Pointer(&raw[0])), n)
		} else {
			out.Data = make([]float32, n)
			for i := range out.Data {
				out.Data[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
			}
		}
	case "F16":
		out.Data = make([]float32, n)
		for i := range out.Data {
			out.Data[i] = halfToFloat(binary.LittleEndian.Uint16(raw[2*i:]))
		}
	case "BF16":
		out.Data = make([]float32, n)
		for i := range out.Data {
			out.Data[i] = math.Float32frombits(uint32(binary.LittleEndian.Uint16(raw[2*i:])) << 16)
		}
	}
	return out, nil
}

// Close unmaps the file; tensors read in place are invalid after.
func (t *Tensors) Close() error {
	if t.unmap == nil {
		return nil
	}
	err := t.unmap()
	t.unmap = nil
	return err
}

var littleEndian = func() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

// halfToFloat converts an IEEE 754 half-precision value.
func halfToFloat(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := int32(h>>10) & 0x1f
	frac := uint32(h & 0x3ff)
	switch {
	case exp == 0 && frac == 0:
		return math.Float32frombits(sign)
	case exp == 0:
		// Subnormal: normalize.
		for frac&0x400 == 0 {
			frac <<= 1
			exp--
		}
		exp++
		frac &= 0x3ff
	case exp == 0x1f:
		return math.Float32frombits(sign | 0x7f800000 | frac<<13)
	}
	return math.Float32frombits(sign | uint32(exp+112)<<23 | frac<<13)
}

// WriteTensors writes F32 tensors in the safetensors format, for tests and
// tools.
func WriteTensors(path string, tensors map[string]Tensor) error {
	names := make([]string, 0, len(tensors))
	for k := range tensors {
		names = append(names, k)
	}
	slices.Sort(names)
	header := map[string]tensorInfo{}
	var off int64
	for _, k := range names {
		t := tensors[k]
		shape := make([]int64, len(t.Shape))
		for i, d := range t.Shape {
			shape[i] = int64(d)
		}
		header[k] = tensorInfo{DType: "F32", Shape: shape, Offsets: [2]int64{off, off + 4*int64(len(t.Data))}}
		off += 4 * int64(len(t.Data))
	}
	h, err := json.Marshal(header)
	if err != nil {
		return err
	}
	// Pad the header so the data is 8-byte aligned.
	for (8+len(h))%8 != 0 {
		h = append(h, ' ')
	}
	var b strings.Builder
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(len(h)))
	b.Write(n[:])
	b.Write(h)
	var w [4]byte
	for _, k := range names {
		for _, v := range tensors[k].Data {
			binary.LittleEndian.PutUint32(w[:], math.Float32bits(v))
			b.Write(w[:])
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
