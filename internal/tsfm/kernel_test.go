package tsfm

import (
	"math"
	"math/rand/v2"
	"testing"
)

// TestMatmul checks the matrix product, whichever kernel this machine
// uses, against float64 sums, for shapes that are and are not multiples
// of the kernels' blocks, on one thread and on several.
func TestMatmul(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, s := range []struct{ m, k, n int }{{1, 1, 1}, {3, 5, 7}, {4, 16, 4}, {5, 33, 9}, {33, 512, 70}, {65, 129, 300}, {2, 1280, 1281}} {
		x := make([]float32, s.m*s.k)
		w := make([]float32, s.n*s.k)
		b := make([]float32, s.n)
		for _, v := range [][]float32{x, w, b} {
			for i := range v {
				v[i] = float32(rng.NormFloat64())
			}
		}
		for _, th := range []int{1, 8} {
			saved := threads
			threads = th
			y := make([]float32, s.m*s.n)
			matmulNT(y, x, s.m, s.k, w, s.n, b)
			threads = saved
			for i := 0; i < s.m; i++ {
				for j := 0; j < s.n; j++ {
					var want float64
					for p := 0; p < s.k; p++ {
						want += float64(x[i*s.k+p]) * float64(w[j*s.k+p])
					}
					want += float64(b[j])
					if got := float64(y[i*s.n+j]); math.Abs(got-want) > 1e-4*math.Sqrt(float64(s.k))+1e-5 {
						t.Fatalf("%v threads %d: y[%d][%d] = %v, want %v", s, th, i, j, got, want)
					}
				}
			}
		}
	}
}

func BenchmarkMatmul(b *testing.B) {
	m, k, n := 33, 512, 2048
	x := make([]float32, m*k)
	w := make([]float32, n*k)
	y := make([]float32, m*n)
	for i := range x {
		x[i] = 1
	}
	for i := range w {
		w[i] = 0.5
	}
	saved := threads
	threads = 1
	defer func() { threads = saved }()
	b.ResetTimer()
	for range b.N {
		matmulNT(y, x, m, k, w, n, nil)
	}
	b.ReportMetric(float64(2*m*k*n)*float64(b.N)/b.Elapsed().Seconds()/1e9, "GFLOP/s")
}

func BenchmarkMatmulGo(b *testing.B) {
	saved := dot4x4
	dot4x4 = dot4x4Go
	defer func() { dot4x4 = saved }()
	BenchmarkMatmul(b)
}
