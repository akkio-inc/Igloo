package tsfm

// dot4x4NEON computes the 4×4 dot products of k values (a multiple of 4),
// each in four lanes. Implemented in kernel_arm64.s.
//
//go:noescape
func dot4x4NEON(acc *[64]float32, x0, x1, x2, x3, w0, w1, w2, w3 *float32, k int)

// dot4x4 is the 4×4 kernel of matrix products: NEON for the multiples of 4,
// Go for the rest.
var dot4x4 = func(acc *[16]float32, x0, x1, x2, x3, w0, w1, w2, w3 []float32) {
	k := len(x0)
	x1, x2, x3 = x1[:k], x2[:k], x3[:k]
	w0, w1, w2, w3 = w0[:k], w1[:k], w2[:k], w3[:k]
	k4 := k &^ 3
	if k4 == 0 {
		dot4x4Go(acc, x0, x1, x2, x3, w0, w1, w2, w3)
		return
	}
	var lanes [64]float32
	dot4x4NEON(&lanes, &x0[0], &x1[0], &x2[0], &x3[0], &w0[0], &w1[0], &w2[0], &w3[0], k4)
	for i := range acc {
		l := lanes[i*4 : i*4+4]
		acc[i] = (l[0] + l[1]) + (l[2] + l[3])
	}
	if k4 < k {
		var rest [16]float32
		dot4x4Go(&rest, x0[k4:], x1[k4:], x2[k4:], x3[k4:], w0[k4:], w1[k4:], w2[k4:], w3[k4:])
		for i := range acc {
			acc[i] += rest[i]
		}
	}
}
