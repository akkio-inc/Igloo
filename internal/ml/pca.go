package ml

import (
	"errors"
	"math"
	"sort"
)

// PCA is a principal component analysis of vectors: the rows' mean, and
// the components, the covariance matrix's eigenvectors by decreasing
// eigenvalue.
type PCA struct {
	D int
	// Mean centers rows; Components holds component c at
	// Components[c*D:(c+1)*D]; Eigenvalues are their variances.
	Mean        []float64
	Components  []float64
	Eigenvalues []float64
	// TotalVariance is the sum of every eigenvalue, kept or not.
	TotalVariance float64
}

// covChunk is how many rows a task of the covariance sums.
const covChunk = 1 << 16

// FitPCA computes the principal components of n vectors of d values
// (row-major), keeping k of them, or, when k is 0, as many as explain at
// least ratio of the variance.
func FitPCA(x []float64, n, d, k int, ratio float64, threads int) (*PCA, error) {
	if n < 2 || d == 0 {
		return nil, errors.New("PCA needs two rows or more")
	}
	mean := make([]float64, d)
	chunks := (n + covChunk - 1) / covChunk
	sums := make([][]float64, chunks)
	parallelFor(chunks, threads, func(c int) {
		s := make([]float64, d)
		for i := c * covChunk; i < min((c+1)*covChunk, n); i++ {
			for j, v := range x[i*d : (i+1)*d] {
				s[j] += v
			}
		}
		sums[c] = s
	})
	for _, s := range sums {
		for j, v := range s {
			mean[j] += v
		}
	}
	for j := range mean {
		mean[j] /= float64(n)
	}
	// The rows' products, over their nonzero entries (one-hot rows are
	// mostly zeros), less the means' after: Σ(x-μ)(x-μ)ᵀ = Σxxᵀ - nμμᵀ.
	covs := make([][]float64, chunks)
	parallelFor(chunks, threads, func(c int) {
		cov := make([]float64, d*d)
		var nz []int
		for i := c * covChunk; i < min((c+1)*covChunk, n); i++ {
			row := x[i*d : (i+1)*d]
			nz = nonzero(row, nz[:0])
			for ia, a := range nz {
				ra := row[a]
				cr := cov[a*d:]
				for _, b := range nz[ia:] {
					cr[b] += ra * row[b]
				}
			}
		}
		covs[c] = cov
	})
	cov := make([]float64, d*d)
	for _, part := range covs {
		for i, v := range part {
			cov[i] += v
		}
	}
	for a := range d {
		for b := a; b < d; b++ {
			v := (cov[a*d+b] - float64(n)*mean[a]*mean[b]) / float64(n-1)
			cov[a*d+b], cov[b*d+a] = v, v
		}
	}
	vals, vecs := symmetricEigen(cov, d)
	p := &PCA{D: d, Mean: mean}
	for _, v := range vals {
		p.TotalVariance += math.Max(v, 0)
	}
	if k <= 0 {
		k = d
		if ratio > 0 && p.TotalVariance > 0 {
			var cum float64
			for i, v := range vals {
				cum += math.Max(v, 0)
				if cum/p.TotalVariance >= ratio-1e-12 {
					k = i + 1
					break
				}
			}
		}
	}
	k = min(k, d)
	for c := range k {
		p.Eigenvalues = append(p.Eigenvalues, math.Max(vals[c], 0))
		p.Components = append(p.Components, vecs[c*d:(c+1)*d]...)
	}
	return p, nil
}

// Project writes v's coordinates on the components into out.
func (p *PCA) Project(v, out []float64) {
	for c := range out {
		comp := p.Components[c*p.D : (c+1)*p.D]
		var s float64
		for j, x := range v {
			s += (x - p.Mean[j]) * comp[j]
		}
		out[c] = s
	}
}

// ReconstructionError is v's mean squared distance from its projection
// back from the components; coords are its coordinates (Project's). The
// components being orthonormal, it is what of v's squared distance from
// the mean its coordinates do not account for.
func (p *PCA) ReconstructionError(v, coords []float64) float64 {
	var s float64
	for j, x := range v {
		s += (x - p.Mean[j]) * (x - p.Mean[j])
	}
	for _, c := range coords {
		s -= c * c
	}
	return math.Max(s, 0) / float64(p.D)
}

// symmetricEigen returns the eigenvalues of a symmetric d by d matrix in
// decreasing order and their unit eigenvectors (row c is value c's), by
// cyclic Jacobi rotations. Each vector's largest entry is made positive,
// so that the result does not depend on rounding's whims.
func symmetricEigen(m []float64, d int) ([]float64, []float64) {
	a := append([]float64(nil), m...)
	v := make([]float64, d*d)
	for i := range d {
		v[i*d+i] = 1
	}
	for sweep := 0; sweep < 100; sweep++ {
		var off, diag float64
		for i := range d {
			diag += a[i*d+i] * a[i*d+i]
			for j := i + 1; j < d; j++ {
				off += a[i*d+j] * a[i*d+j]
			}
		}
		if off <= 1e-24*math.Max(diag, 1e-300) {
			break
		}
		for p := 0; p < d; p++ {
			for q := p + 1; q < d; q++ {
				apq := a[p*d+q]
				if math.Abs(apq) < 1e-300 {
					continue
				}
				app, aqq := a[p*d+p], a[q*d+q]
				theta := (aqq - app) / (2 * apq)
				t := math.Copysign(1, theta) / (math.Abs(theta) + math.Sqrt(theta*theta+1))
				c := 1 / math.Sqrt(t*t+1)
				s := t * c
				for k := range d {
					akp, akq := a[k*d+p], a[k*d+q]
					a[k*d+p] = c*akp - s*akq
					a[k*d+q] = s*akp + c*akq
				}
				for k := range d {
					apk, aqk := a[p*d+k], a[q*d+k]
					a[p*d+k] = c*apk - s*aqk
					a[q*d+k] = s*apk + c*aqk
				}
				for k := range d {
					vkp, vkq := v[k*d+p], v[k*d+q]
					v[k*d+p] = c*vkp - s*vkq
					v[k*d+q] = s*vkp + c*vkq
				}
			}
		}
	}
	order := make([]int, d)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool { return a[order[i]*d+order[i]] > a[order[j]*d+order[j]] })
	vals := make([]float64, d)
	vecs := make([]float64, d*d)
	for r, i := range order {
		vals[r] = a[i*d+i]
		big := 0
		for k := range d {
			if math.Abs(v[k*d+i]) > math.Abs(v[big*d+i]) {
				big = k
			}
		}
		sign := 1.0
		if v[big*d+i] < 0 {
			sign = -1
		}
		for k := range d {
			vecs[r*d+k] = sign * v[k*d+i]
		}
	}
	return vals, vecs
}
