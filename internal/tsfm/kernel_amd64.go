package tsfm

import "golang.org/x/sys/cpu"

// dot2x4AVX computes the 2×4 dot products of k values (a multiple of 8),
// each in eight lanes. Implemented in kernel_amd64.s; needs AVX2 and FMA.
//
//go:noescape
func dot2x4AVX(acc *[64]float32, x0, x1, w0, w1, w2, w3 *float32, k int)

// dot4x4 is the 4×4 kernel of matrix products: AVX2 for the multiples of 8
// where the CPU has it, Go for the rest.
var dot4x4 = dot4x4Go

func init() {
	if cpu.X86.HasAVX2 && cpu.X86.HasFMA {
		dot4x4 = dot4x4AVX
	}
}

func dot4x4AVX(acc *[16]float32, x0, x1, x2, x3, w0, w1, w2, w3 []float32) {
	k := len(x0)
	x1, x2, x3 = x1[:k], x2[:k], x3[:k]
	w0, w1, w2, w3 = w0[:k], w1[:k], w2[:k], w3[:k]
	k8 := k &^ 7
	if k8 == 0 {
		dot4x4Go(acc, x0, x1, x2, x3, w0, w1, w2, w3)
		return
	}
	var lanes [64]float32
	sum := func(l []float32) float32 {
		return ((l[0] + l[1]) + (l[2] + l[3])) + ((l[4] + l[5]) + (l[6] + l[7]))
	}
	dot2x4AVX(&lanes, &x0[0], &x1[0], &w0[0], &w1[0], &w2[0], &w3[0], k8)
	for i := 0; i < 8; i++ {
		acc[i] = sum(lanes[i*8 : i*8+8])
	}
	dot2x4AVX(&lanes, &x2[0], &x3[0], &w0[0], &w1[0], &w2[0], &w3[0], k8)
	for i := 0; i < 8; i++ {
		acc[8+i] = sum(lanes[i*8 : i*8+8])
	}
	if k8 < k {
		var rest [16]float32
		dot4x4Go(&rest, x0[k8:], x1[k8:], x2[k8:], x3[k8:], w0[k8:], w1[k8:], w2[k8:], w3[k8:])
		for i := range acc {
			acc[i] += rest[i]
		}
	}
}
