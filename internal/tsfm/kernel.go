package tsfm

import "sync/atomic"

// active counts the series being forecast at once: their matrix products
// share the machine's threads.
var active atomic.Int64

// currentThreads is how many goroutines a matrix product may use now.
func currentThreads() int {
	a := int(active.Load())
	if a <= 1 {
		return threads
	}
	return max(1, threads/a)
}

// dot4x4Go sets acc[r*4+c] to the dot product of x_r and w_c.
func dot4x4Go(acc *[16]float32, x0, x1, x2, x3, w0, w1, w2, w3 []float32) {
	k := len(x0)
	x1, x2, x3 = x1[:k], x2[:k], x3[:k]
	w0, w1, w2, w3 = w0[:k], w1[:k], w2[:k], w3[:k]
	var a00, a01, a02, a03, a10, a11, a12, a13, a20, a21, a22, a23, a30, a31, a32, a33 float32
	for t := 0; t < k; t++ {
		v0, v1, v2, v3 := w0[t], w1[t], w2[t], w3[t]
		u := x0[t]
		a00 += u * v0
		a01 += u * v1
		a02 += u * v2
		a03 += u * v3
		u = x1[t]
		a10 += u * v0
		a11 += u * v1
		a12 += u * v2
		a13 += u * v3
		u = x2[t]
		a20 += u * v0
		a21 += u * v1
		a22 += u * v2
		a23 += u * v3
		u = x3[t]
		a30 += u * v0
		a31 += u * v1
		a32 += u * v2
		a33 += u * v3
	}
	*acc = [16]float32{a00, a01, a02, a03, a10, a11, a12, a13, a20, a21, a22, a23, a30, a31, a32, a33}
}
