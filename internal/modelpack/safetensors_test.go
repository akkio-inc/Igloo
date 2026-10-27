package modelpack

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"
)

func TestTensors(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "model.safetensors")
	want := map[string]Tensor{
		"encoder.weight": {Shape: []int{2, 3}, Data: []float32{1, 2, 3, 4, 5, 6}},
		"bias":           {Shape: []int{3}, Data: []float32{-0.5, 0, 0.25}},
	}
	if err := WriteTensors(path, want); err != nil {
		t.Fatal(err)
	}
	ts, err := OpenTensors(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close() //nolint:errcheck
	if names := ts.Names(); len(names) != 2 || names[0] != "bias" {
		t.Errorf("names %v", names)
	}
	w, err := ts.Get("encoder.weight", 2, -1)
	if err != nil {
		t.Fatal(err)
	}
	if w.Shape[1] != 3 || w.Data[4] != 5 {
		t.Errorf("weight %+v", w)
	}
	if _, err := ts.Get("encoder.weight", 3, 2); err == nil {
		t.Error("a wrong shape was read")
	}
	if _, err := ts.Get("nope"); err == nil {
		t.Error("a missing tensor was read")
	}
}

func TestHalfPrecision(t *testing.T) {
	t.Parallel()
	header := []byte(`{"h":{"dtype":"F16","shape":[4],"data_offsets":[0,8]},"b":{"dtype":"BF16","shape":[2],"data_offsets":[8,12]}}`)
	data := make([]byte, 8+len(header)+12)
	binary.LittleEndian.PutUint64(data, uint64(len(header)))
	copy(data[8:], header)
	body := data[8+len(header):]
	for i, h := range []uint16{0x3c00, 0xc000, 0x0001, 0x7c00} { // 1, -2, smallest subnormal, +Inf
		binary.LittleEndian.PutUint16(body[2*i:], h)
	}
	binary.LittleEndian.PutUint16(body[8:], uint16(math.Float32bits(1.5)>>16))
	binary.LittleEndian.PutUint16(body[10:], uint16(math.Float32bits(-3)>>16))
	ts, err := ParseTensors(data)
	if err != nil {
		t.Fatal(err)
	}
	h, _ := ts.Get("h")
	if h.Data[0] != 1 || h.Data[1] != -2 || h.Data[2] != float32(math.Ldexp(1, -24)) || !math.IsInf(float64(h.Data[3]), 1) {
		t.Errorf("F16 %v", h.Data)
	}
	b, _ := ts.Get("b")
	if b.Data[0] != 1.5 || b.Data[1] != -3 {
		t.Errorf("BF16 %v", b.Data)
	}
	if _, err := ParseTensors([]byte{1, 2, 3}); err == nil {
		t.Error("garbage parsed")
	}
}
