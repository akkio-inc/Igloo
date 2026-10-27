package ml

import (
	"errors"
	"math"
)

// ALS is a matrix factorization of ratings fit by alternating least
// squares. An explicit model predicts a rating as
// Mean + UserBias[u] + ItemBias[i] + U[u]·V[i]; an implicit one (Hu, Koren
// and Volinsky's) predicts a preference, near 1 for items a user would
// interact with, as U[u]·V[i].
type ALS struct {
	Implicit bool `json:"implicit"`
	Factors  int  `json:"factors"`
	Users    int  `json:"users"`
	Items    int  `json:"items"`
	// U and V hold user u's factors at U[u*Factors:] and item i's at
	// V[i*Factors:].
	U Float32s `json:"u"`
	V Float32s `json:"v"`
	// Mean, UserBias and ItemBias are an explicit model's.
	Mean     float64  `json:"mean,omitempty"`
	UserBias Float32s `json:"user_bias,omitempty"`
	ItemBias Float32s `json:"item_bias,omitempty"`
}

// ALSParams configure TrainALS.
type ALSParams struct {
	Implicit bool
	// Factors is the factors' number (default 16); L2 the penalty on them,
	// scaled by each user's and item's ratings for explicit feedback
	// (default 1); Alpha how much a rating raises an implicit preference's
	// confidence (default 40).
	Factors int
	L2      float64
	Alpha   float64
	// Iterations is the alternations (default 15).
	Iterations int
	Seed       uint64
	Threads    int
	Interrupt  func() error
}

// ALSInfo is what training learned besides the factors: the training loss
// after each iteration.
type ALSInfo struct{ Loss []float64 }

// Ratings are interactions by row: user, item and rating.
type Ratings struct {
	User, Item []int32
	Value      []float64
}

// csr groups interactions by one side: rows of others[start[r]:start[r+1]]
// with their values.
type csr struct {
	start  []int
	others []int32
	values []float64
}

func group(n int, by, other []int32, values []float64) csr {
	c := csr{start: make([]int, n+1), others: make([]int32, len(by)), values: make([]float64, len(by))}
	for _, r := range by {
		c.start[r+1]++
	}
	for r := range n {
		c.start[r+1] += c.start[r]
	}
	pos := append([]int(nil), c.start[:n]...)
	for k, r := range by {
		c.others[pos[r]] = other[k]
		c.values[pos[r]] = values[k]
		pos[r]++
	}
	return c
}

// TrainALS factorizes the ratings of users users and items items.
func TrainALS(r Ratings, users, items int, p ALSParams) (*ALS, ALSInfo, error) {
	var info ALSInfo
	if len(r.User) == 0 {
		return nil, info, errors.New("no ratings to train on")
	}
	if p.Factors <= 0 {
		p.Factors = 16
	}
	if p.L2 <= 0 {
		p.L2 = 1
	}
	if p.Alpha <= 0 {
		p.Alpha = 40
	}
	if p.Iterations <= 0 {
		p.Iterations = 15
	}
	k := p.Factors
	m := &ALS{Implicit: p.Implicit, Factors: k, Users: users, Items: items}
	byUser := group(users, r.User, r.Item, r.Value)
	byItem := group(items, r.Item, r.User, r.Value)
	U, V := make([]float64, users*k), make([]float64, items*k)
	scale := 0.1 / math.Sqrt(float64(k))
	for i := range U {
		U[i] = (2*unitHash(p.Seed, 0xa150, i) - 1) * scale
	}
	for i := range V {
		V[i] = (2*unitHash(p.Seed, 0xa151, i) - 1) * scale
	}
	var ub, ib []float64
	if !p.Implicit {
		var sum float64
		for _, v := range r.Value {
			sum += v
		}
		m.Mean = sum / float64(len(r.Value))
		ub, ib = make([]float64, users), make([]float64, items)
	}
	for range p.Iterations {
		if p.Interrupt != nil {
			if err := p.Interrupt(); err != nil {
				return nil, info, err
			}
		}
		if p.Implicit {
			solveImplicit(byUser, V, U, k, p)
			solveImplicit(byItem, U, V, k, p)
			info.Loss = append(info.Loss, implicitLoss(byUser, U, V, k, p))
		} else {
			solveExplicit(byUser, V, ib, U, ub, m.Mean, k, p)
			solveExplicit(byItem, U, ub, V, ib, m.Mean, k, p)
			info.Loss = append(info.Loss, explicitLoss(byUser, U, V, ub, ib, m.Mean, k, p.Threads))
		}
	}
	m.U, m.V = toFloat32s(U), toFloat32s(V)
	if !p.Implicit {
		m.UserBias, m.ItemBias = toFloat32s(ub), toFloat32s(ib)
	}
	return m, info, nil
}

func toFloat32s(v []float64) Float32s {
	out := make(Float32s, len(v))
	for i, x := range v {
		out[i] = float32(x)
	}
	return out
}

// solveExplicit sets each row's factors and bias (rows of g, against the
// other side's factors Y and biases yb) to the least squares fit of its
// ratings less the mean and the others' biases, penalized by L2 times its
// ratings.
func solveExplicit(g csr, Y, yb, X, xb []float64, mean float64, k int, p ALSParams) {
	rows := len(g.start) - 1
	parallelFor((rows+255)/256, p.Threads, func(c int) {
		dim := k + 1
		A, b, z := make([]float64, dim*dim), make([]float64, dim), make([]float64, dim)
		for r := c * 256; r < min((c+1)*256, rows); r++ {
			lo, hi := g.start[r], g.start[r+1]
			if lo == hi {
				continue
			}
			clear(A)
			clear(b)
			for e := lo; e < hi; e++ {
				o := int(g.others[e])
				copy(z, Y[o*k:(o+1)*k])
				z[k] = 1
				t := g.values[e] - mean - yb[o]
				for a := range dim {
					za := z[a]
					b[a] += za * t
					row := A[a*dim:]
					for bb := a; bb < dim; bb++ {
						row[bb] += za * z[bb]
					}
				}
			}
			lambda := p.L2 * float64(hi-lo)
			for a := range dim {
				for bb := 0; bb < a; bb++ {
					A[a*dim+bb] = A[bb*dim+a]
				}
				A[a*dim+a] += lambda
			}
			sol, ok := cholSolve(A, b, dim)
			if !ok {
				continue
			}
			copy(X[r*k:(r+1)*k], sol[:k])
			xb[r] = sol[k]
		}
	})
}

// solveImplicit sets each row's factors to the weighted least squares fit
// of preferences 1 for its interactions and 0 for everything else, with
// confidence 1 + Alpha*rating for its interactions and 1 elsewhere, using
// YᵀY for the zeros.
func solveImplicit(g csr, Y, X []float64, k int, p ALSParams) {
	others := len(Y) / k
	YtY := gram(Y, others, k, p.Threads)
	rows := len(g.start) - 1
	parallelFor((rows+255)/256, p.Threads, func(c int) {
		A, b := make([]float64, k*k), make([]float64, k)
		for r := c * 256; r < min((c+1)*256, rows); r++ {
			lo, hi := g.start[r], g.start[r+1]
			copy(A, YtY)
			clear(b)
			for e := lo; e < hi; e++ {
				o := int(g.others[e])
				y := Y[o*k : (o+1)*k]
				conf := 1 + p.Alpha*math.Max(g.values[e], 0)
				pref := 0.0
				if g.values[e] > 0 {
					pref = 1
				}
				for a := range k {
					ya := y[a]
					b[a] += conf * pref * ya
					row := A[a*k:]
					for bb := range k {
						row[bb] += (conf - 1) * ya * y[bb]
					}
				}
			}
			for a := range k {
				A[a*k+a] += p.L2
			}
			if sol, ok := cholSolve(A, b, k); ok {
				copy(X[r*k:(r+1)*k], sol)
			}
		}
	})
}

// gram returns YᵀY of n rows of k factors, summed in fixed chunks.
func gram(Y []float64, n, k, threads int) []float64 {
	parts := make([][]float64, numChunks(n))
	parallelFor(len(parts), threads, func(c int) {
		lo, hi := chunkRange(c, n)
		g := make([]float64, k*k)
		for i := lo; i < hi; i++ {
			y := Y[i*k : (i+1)*k]
			for a := range k {
				for b := range k {
					g[a*k+b] += y[a] * y[b]
				}
			}
		}
		parts[c] = g
	})
	out := make([]float64, k*k)
	for _, g := range parts {
		for i, v := range g {
			out[i] += v
		}
	}
	return out
}

// explicitLoss is the root mean squared error of the training ratings.
func explicitLoss(g csr, U, V, ub, ib []float64, mean float64, k, threads int) float64 {
	rows := len(g.start) - 1
	parts := make([]float64, (rows+4095)/4096)
	parallelFor(len(parts), threads, func(c int) {
		var s float64
		for r := c * 4096; r < min((c+1)*4096, rows); r++ {
			for e := g.start[r]; e < g.start[r+1]; e++ {
				o := int(g.others[e])
				d := g.values[e] - (mean + ub[r] + ib[o] + dot(U[r*k:(r+1)*k], V[o*k:(o+1)*k]))
				s += d * d
			}
		}
		parts[c] = s
	})
	var s float64
	for _, v := range parts {
		s += v
	}
	return math.Sqrt(s / float64(max(len(g.others), 1)))
}

// implicitLoss is the weighted squared error of every preference, the
// zeros included, per user: Σ x·(YᵀY)·x over users for the zeros, and
// each interaction's correction.
func implicitLoss(g csr, U, V []float64, k int, p ALSParams) float64 {
	YtY := gram(V, len(V)/k, k, p.Threads)
	rows := len(g.start) - 1
	parts := make([]float64, (rows+4095)/4096)
	parallelFor(len(parts), p.Threads, func(c int) {
		var s float64
		for r := c * 4096; r < min((c+1)*4096, rows); r++ {
			x := U[r*k : (r+1)*k]
			for a := range k {
				for b := range k {
					s += x[a] * YtY[a*k+b] * x[b]
				}
			}
			for e := g.start[r]; e < g.start[r+1]; e++ {
				o := int(g.others[e])
				pred := dot(x, V[o*k:(o+1)*k])
				conf := 1 + p.Alpha*math.Max(g.values[e], 0)
				pref := 0.0
				if g.values[e] > 0 {
					pref = 1
				}
				s += conf*(pref-pred)*(pref-pred) - pred*pred
			}
		}
		parts[c] = s
	})
	var s float64
	for _, v := range parts {
		s += v
	}
	return s / float64(max(rows, 1))
}

// Predict returns user u's predicted rating or preference of item i;
// either may be -1 for one training never saw.
func (m *ALS) Predict(u, i int) float64 {
	k := m.Factors
	var s float64
	if u >= 0 && i >= 0 {
		uf, vf := m.U[u*k:(u+1)*k], m.V[i*k:(i+1)*k]
		for f := range k {
			s += float64(uf[f]) * float64(vf[f])
		}
	}
	if m.Implicit {
		return s
	}
	s += m.Mean
	if u >= 0 {
		s += float64(m.UserBias[u])
	}
	if i >= 0 {
		s += float64(m.ItemBias[i])
	}
	return s
}

// cholSolve solves A x = b for a symmetric positive definite A (d by d,
// row-major); ok is false when A is not.
func cholSolve(A, b []float64, d int) ([]float64, bool) {
	L := make([]float64, d*d)
	for i := range d {
		for j := 0; j <= i; j++ {
			s := A[i*d+j]
			for k := range j {
				s -= L[i*d+k] * L[j*d+k]
			}
			if i == j {
				if s <= 0 {
					return nil, false
				}
				L[i*d+i] = math.Sqrt(s)
			} else {
				L[i*d+j] = s / L[j*d+j]
			}
		}
	}
	z := make([]float64, d)
	for i := range d {
		s := b[i]
		for k := range i {
			s -= L[i*d+k] * z[k]
		}
		z[i] = s / L[i*d+i]
	}
	x := make([]float64, d)
	for i := d - 1; i >= 0; i-- {
		s := z[i]
		for k := i + 1; k < d; k++ {
			s -= L[k*d+i] * x[k]
		}
		x[i] = s / L[i*d+i]
	}
	return x, true
}
