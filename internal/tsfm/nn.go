package tsfm

import (
	"fmt"
	"math"
	"runtime"
	"sync"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// The networks' building blocks, in float32 as the weights are: dense
// layers, residual blocks, RMS norms, attention. Activations are row-major
// matrices, a row a token.

// linear is a dense layer y = xWᵀ + b, its weight W of out rows of in
// values, as PyTorch keeps nn.Linear's.
type linear struct {
	w       []float32
	b       []float32
	in, out int
}

// weights reads a model's tensors by name.
type weights struct {
	t      *modelpack.Tensors
	prefix string
	err    error
}

func (w *weights) sub(prefix string) *weights {
	return &weights{t: w.t, prefix: w.prefix + prefix}
}

func (w *weights) fail(err error) {
	if w.err == nil {
		w.err = err
	}
}

// vec reads a vector of n values.
func (w *weights) vec(name string, n int) []float32 {
	t, err := w.t.Get(w.prefix+name, n)
	if err != nil {
		w.fail(err)
		return make([]float32, n)
	}
	return t.Data
}

// mat reads a matrix of rows×cols values.
func (w *weights) mat(name string, rows, cols int) []float32 {
	t, err := w.t.Get(w.prefix+name, rows, cols)
	if err != nil {
		w.fail(err)
		return make([]float32, rows*cols)
	}
	return t.Data
}

// shape returns tensor name's shape, or nil.
func (w *weights) shape(name string) []int {
	t, err := w.t.Get(w.prefix + name)
	if err != nil {
		return nil
	}
	return t.Shape
}

func (w *weights) has(name string) bool { return w.t.Has(w.prefix + name) }

// linear reads a dense layer, with its bias if bias.
func (w *weights) linear(name string, in, out int, bias bool) linear {
	l := linear{w: w.mat(name+".weight", out, in), in: in, out: out}
	if bias {
		l.b = w.vec(name+".bias", out)
	}
	return l
}

// apply computes y = xWᵀ + b for the rows of x.
func (l linear) apply(x []float32, rows int) []float32 {
	y := make([]float32, rows*l.out)
	matmulNT(y, x, rows, l.in, l.w, l.out, l.b)
	return y
}

// residualBlock is Chronos' and TimesFM's ResidualBlock:
// output(act(hidden(x))) + residual(x).
type residualBlock struct {
	hidden, output, residual linear
	act                      func([]float32)
}

func (w *weights) residualBlock(name string, in, hidden, out int, bias bool, act func([]float32)) residualBlock {
	return residualBlock{
		hidden: w.linear(name+".hidden_layer", in, hidden, bias), output: w.linear(name+".output_layer", hidden, out, bias),
		residual: w.linear(name+".residual_layer", in, out, bias), act: act,
	}
}

func (r residualBlock) apply(x []float32, rows int) []float32 {
	h := r.hidden.apply(x, rows)
	r.act(h)
	y := r.output.apply(h, rows)
	res := r.residual.apply(x, rows)
	for i := range y {
		y[i] += res[i]
	}
	return y
}

func relu(x []float32) {
	for i, v := range x {
		if v < 0 {
			x[i] = 0
		}
	}
}

// silu is x·sigmoid(x), PyTorch's SiLU and TimesFM's swish.
func silu(x []float32) {
	for i, v := range x {
		x[i] = v / (1 + float32(math.Exp(float64(-v))))
	}
}

// rmsNorm scales each row of x, of n values, to unit root mean square and
// multiplies by weight: T5's layer norm and TimesFM's RMSNorm.
func rmsNorm(dst, x []float32, n int, weight []float32, eps float32) {
	for r := 0; r+n <= len(x); r += n {
		row := x[r : r+n]
		var ss float32
		for _, v := range row {
			ss += v * v
		}
		inv := float32(1 / math.Sqrt(float64(ss/float32(n)+eps)))
		out := dst[r : r+n]
		for i, v := range row {
			out[i] = v * inv * weight[i]
		}
	}
}

// softmax normalizes x in place.
func softmax(x []float32) {
	m := float32(math.Inf(-1))
	for _, v := range x {
		if v > m {
			m = v
		}
	}
	var sum float32
	for i, v := range x {
		e := float32(math.Exp(float64(v - m)))
		x[i] = e
		sum += e
	}
	for i := range x {
		x[i] /= sum
	}
}

// minFloat32 is torch.finfo(torch.float32).min, which masks attention.
const minFloat32 = -math.MaxFloat32

// attention computes softmax(q·kᵀ + bias) v for one head: q of nq rows,
// k and v of nk rows, each of d values at stride in their matrices;
// bias(i, j) adds to the score of query i and key j.
func attention(out []float32, outStride int, q []float32, qStride int, k, v []float32, kvStride int, nq, nk, d int,
	bias func(i, j int) float32, scores []float32) {
	for i := 0; i < nq; i++ {
		qi := q[i*qStride : i*qStride+d]
		s := scores[:nk]
		for j := 0; j < nk; j++ {
			s[j] = dot(qi, k[j*kvStride:j*kvStride+d])
			if bias != nil {
				s[j] += bias(i, j)
			}
		}
		softmax(s)
		o := out[i*outStride : i*outStride+d]
		clear(o)
		for j := 0; j < nk; j++ {
			axpy(o, v[j*kvStride:j*kvStride+d], s[j])
		}
	}
}

func dot(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return (s0 + s1) + (s2 + s3)
}

// axpy adds a·x to y.
func axpy(y, x []float32, a float32) {
	x = x[:len(y)]
	for i := range y {
		y[i] += a * x[i]
	}
}

// threads is how many goroutines a matrix product may use; 1 when the
// caller parallelizes over series instead.
var threads = runtime.GOMAXPROCS(0)

// matmulNT computes y = xWᵀ + b: x of m rows of k values, W of n rows of k
// values, y of m rows of n values.
func matmulNT(y, x []float32, m, k int, w []float32, n int, b []float32) {
	if len(x) < m*k || len(w) < n*k || len(y) < m*n {
		panic(fmt.Sprintf("tsfm: matmul of %d×%d by %d×%d into %d", m, k, n, k, len(y)))
	}
	work := m * n * k
	parts := 1
	if t := currentThreads(); t > 1 && work >= 1<<18 {
		parts = min(t, (n+15)/16)
	}
	if parts <= 1 {
		gemmNT(y, x, m, k, w, n, b, 0, n)
		return
	}
	var wg sync.WaitGroup
	per := (n + parts - 1) / parts
	per = (per + 3) &^ 3
	for from := 0; from < n; from += per {
		to := min(from+per, n)
		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			gemmNT(y, x, m, k, w, n, b, from, to)
		}(from, to)
	}
	wg.Wait()
}

// gemmNT computes columns [from, to) of y = xWᵀ + b, four rows and four
// columns at a time.
func gemmNT(y, x []float32, m, k int, w []float32, n int, b []float32, from, to int) {
	j := from
	for ; j+4 <= to; j += 4 {
		w0, w1, w2, w3 := w[j*k:(j+1)*k], w[(j+1)*k:(j+2)*k], w[(j+2)*k:(j+3)*k], w[(j+3)*k:(j+4)*k]
		i := 0
		for ; i+4 <= m; i += 4 {
			var acc [16]float32
			dot4x4(&acc, x[i*k:(i+1)*k], x[(i+1)*k:(i+2)*k], x[(i+2)*k:(i+3)*k], x[(i+3)*k:(i+4)*k], w0, w1, w2, w3)
			for r := 0; r < 4; r++ {
				row := y[(i+r)*n+j : (i+r)*n+j+4]
				row[0], row[1], row[2], row[3] = acc[r*4], acc[r*4+1], acc[r*4+2], acc[r*4+3]
			}
		}
		for ; i < m; i++ {
			xi := x[i*k : (i+1)*k]
			row := y[i*n+j : i*n+j+4]
			row[0], row[1], row[2], row[3] = dot(xi, w0), dot(xi, w1), dot(xi, w2), dot(xi, w3)
		}
	}
	for ; j < to; j++ {
		wj := w[j*k : (j+1)*k]
		for i := 0; i < m; i++ {
			y[i*n+j] = dot(x[i*k:(i+1)*k], wj)
		}
	}
	if b != nil {
		for i := 0; i < m; i++ {
			row := y[i*n+from : i*n+to]
			bb := b[from:to]
			for c := range row {
				row[c] += bb[c]
			}
		}
	}
}
