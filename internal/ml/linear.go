package ml

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

// Linear is a linear or logistic regression fit by iteratively reweighted
// least squares, with a small ridge penalty: a model whose coefficients
// explain it. Numeric features enter standardized, categorical ones as
// indicators of their frequent levels against the most frequent.
type Linear struct {
	Logistic bool `json:"logistic"`
	// Terms are the design's columns; Coef their coefficients and StdErr
	// their standard errors, on the features' own scale.
	Terms     []Term    `json:"terms"`
	Intercept float64   `json:"intercept"`
	Coef      []float64 `json:"coef"`
	StdErr    []float64 `json:"stderr"`
	// Iterations is how many reweighting steps fitting took.
	Iterations int `json:"iterations"`
}

// Term is a column of a linear model's design: a numeric feature, or one
// level of a categorical feature (1 when the row has it).
type Term struct {
	Feature string `json:"feature"`
	Level   string `json:"level,omitempty"`
	// Mean and Scale standardize a numeric feature; Mean also fills in
	// its missing values. A level's Mean is its frequency.
	Mean  float64 `json:"mean"`
	Scale float64 `json:"scale"`
	Cat   bool    `json:"cat,omitempty"`
}

// Name is how explanations name the term.
func (t Term) Name() string {
	if t.Level != "" {
		return t.Feature + " = " + t.Level
	}
	return t.Feature
}

// FitLinear fits y on x: a logistic regression of labels 0 and 1, or a
// linear regression. lambda is the ridge penalty on standardized
// coefficients (0 means 1e-6 times the rows).
func FitLinear(x *Frame, y []float64, logistic bool, lambda float64) (*Linear, error) {
	if err := x.Check(); err != nil {
		return nil, err
	}
	if x.N == 0 || len(y) != x.N {
		return nil, errors.New("a linear model needs rows, and a label for each")
	}
	if lambda <= 0 {
		lambda = 1e-6 * float64(x.N)
	}
	l := &Linear{Logistic: logistic}
	for ci := range x.Cols {
		c := &x.Cols[ci]
		if c.Kind == Numeric {
			var sum, sq float64
			n := 0
			for _, v := range c.Num {
				if !math.IsNaN(v) {
					sum += v
					n++
				}
			}
			if n == 0 {
				continue
			}
			mu := sum / float64(n)
			for _, v := range c.Num {
				if !math.IsNaN(v) {
					sq += (v - mu) * (v - mu)
				}
			}
			sd := math.Sqrt(sq / float64(n))
			if sd == 0 {
				continue
			}
			l.Terms = append(l.Terms, Term{Feature: c.Name, Mean: mu, Scale: sd})
			continue
		}
		counts := make([]int, len(c.Levels))
		for _, code := range c.Codes {
			if code >= 0 {
				counts[code]++
			}
		}
		order := make([]int, len(counts))
		for i := range order {
			order[i] = i
		}
		sort.Slice(order, func(i, j int) bool {
			a, b := order[i], order[j]
			if counts[a] != counts[b] {
				return counts[a] > counts[b]
			}
			return c.Levels[a] < c.Levels[b]
		})
		// The most frequent level is the reference; rare ones join it.
		for _, i := range order[min(1, len(order)):] {
			if counts[i] < 5 || counts[i] == x.N {
				continue
			}
			l.Terms = append(l.Terms, Term{Feature: c.Name, Level: c.Levels[i], Mean: float64(counts[i]) / float64(x.N), Scale: 1, Cat: true})
		}
	}
	X := l.design(x)
	p := len(l.Terms) + 1
	beta := make([]float64, p)
	if logistic {
		q := clamp(mean(y), 1e-6, 1-1e-6)
		beta[0] = math.Log(q / (1 - q))
	} else {
		beta[0] = mean(y)
	}
	var inv []float64
	iterations := 1
	if logistic {
		iterations = 50
	}
	var sigma2 float64 = 1
	for it := 0; it < iterations; it++ {
		A := make([]float64, p*p)
		b := make([]float64, p)
		// Sums over fixed chunks of rows, added in order: the same rows
		// give the same fit whatever the threads.
		type part struct{ A, b []float64 }
		parts := make([]part, numChunks(x.N))
		parallelFor(len(parts), 0, func(c int) {
			lo, hi := chunkRange(c, x.N)
			pa, pb := make([]float64, p*p), make([]float64, p)
			for i := lo; i < hi; i++ {
				xi := X[i*p : (i+1)*p]
				eta := dot(xi, beta)
				w, z := 1.0, y[i]
				if logistic {
					mu := sigmoid(eta)
					w = math.Max(mu*(1-mu), 1e-10)
					z = eta + (y[i]-mu)/w
				}
				for a := range p {
					wa := w * xi[a]
					if wa == 0 {
						continue
					}
					pb[a] += wa * z
					row := pa[a*p:]
					for c := a; c < p; c++ {
						row[c] += wa * xi[c]
					}
				}
			}
			parts[c] = part{pa, pb}
		})
		for _, pt := range parts {
			for k := range A {
				A[k] += pt.A[k]
			}
			for k := range b {
				b[k] += pt.b[k]
			}
		}
		for a := range p {
			for c := 0; c < a; c++ {
				A[a*p+c] = A[c*p+a]
			}
			if a > 0 {
				A[a*p+a] += lambda
			}
		}
		next, inverse, err := solveSPD(A, b, p)
		if err != nil {
			return nil, fmt.Errorf("fit the linear model: %w", err)
		}
		inv = inverse
		change := 0.0
		for k := range beta {
			change = math.Max(change, math.Abs(next[k]-beta[k]))
		}
		beta = next
		l.Iterations = it + 1
		if change < 1e-9 {
			break
		}
	}
	if !logistic {
		var rss float64
		for i := range x.N {
			d := y[i] - dot(X[i*p:(i+1)*p], beta)
			rss += d * d
		}
		sigma2 = rss / float64(max(x.N-p, 1))
	}
	// Back to the features' own scale.
	l.Intercept = beta[0]
	l.Coef = make([]float64, len(l.Terms))
	l.StdErr = make([]float64, len(l.Terms))
	for k, t := range l.Terms {
		l.Coef[k] = beta[k+1] / t.Scale
		l.StdErr[k] = math.Sqrt(math.Max(inv[(k+1)*p+k+1], 0)*sigma2) / t.Scale
		if !t.Cat {
			l.Intercept -= beta[k+1] * t.Mean / t.Scale
		}
	}
	return l, nil
}

// design returns x's rows as the model's standardized design, an
// intercept column first, row-major.
func (l *Linear) design(x *Frame) []float64 {
	p := len(l.Terms) + 1
	X := make([]float64, x.N*p)
	for i := range x.N {
		X[i*p] = 1
	}
	for k, t := range l.Terms {
		c := x.Col(t.Feature)
		if c == nil {
			continue
		}
		if !t.Cat {
			for i, v := range c.Num {
				if !math.IsNaN(v) {
					X[i*p+k+1] = (v - t.Mean) / t.Scale
				}
			}
			continue
		}
		code := int32(-2)
		for li, lv := range c.Levels {
			if lv == t.Level {
				code = int32(li)
			}
		}
		for i, cv := range c.Codes {
			if cv == code {
				X[i*p+k+1] = 1
			}
		}
	}
	return X
}

// Predict returns the model's predictions for x's rows: probabilities of
// label 1 for a logistic model.
func (l *Linear) Predict(x *Frame) []float64 {
	out := make([]float64, x.N)
	contrib := l.Contributions(x)
	K := len(l.Terms) + 1
	for i := range x.N {
		s := 0.0
		for k := range K {
			s += contrib[i*K+k]
		}
		if l.Logistic {
			s = sigmoid(s)
		}
		out[i] = s
	}
	return out
}

// Contributions returns each term's contribution to each row's raw score,
// its coefficient times the row's distance from the term's mean, then the
// bias (the score at the means): row i's at out[i*(T+1):(i+1)*(T+1)].
// For a linear model these are the exact SHAP values of independent
// features.
func (l *Linear) Contributions(x *Frame) []float64 {
	T := len(l.Terms)
	out := make([]float64, x.N*(T+1))
	bias := l.Intercept
	for k, t := range l.Terms {
		bias += l.Coef[k] * t.Mean
	}
	for i := range x.N {
		out[i*(T+1)+T] = bias
	}
	for k, t := range l.Terms {
		c := x.Col(t.Feature)
		if c == nil {
			continue
		}
		for i := range x.N {
			v := t.Mean
			if !t.Cat {
				if !math.IsNaN(c.Num[i]) {
					v = c.Num[i]
				}
			} else {
				v = 0
				if code := c.Codes[i]; code >= 0 && c.Levels[code] == t.Level {
					v = 1
				}
			}
			out[i*(T+1)+k] = l.Coef[k] * (v - t.Mean)
		}
	}
	return out
}

func dot(a, b []float64) float64 {
	s := 0.0
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// solveSPD solves A x = b for a symmetric positive definite A (p by p, row
// major) by Cholesky, and returns A's inverse too.
func solveSPD(A, b []float64, p int) ([]float64, []float64, error) {
	L := make([]float64, p*p)
	for i := range p {
		for j := 0; j <= i; j++ {
			s := A[i*p+j]
			for k := range j {
				s -= L[i*p+k] * L[j*p+k]
			}
			if i == j {
				if s <= 0 {
					return nil, nil, errors.New("the features are collinear")
				}
				L[i*p+i] = math.Sqrt(s)
			} else {
				L[i*p+j] = s / L[j*p+j]
			}
		}
	}
	solve := func(rhs []float64) []float64 {
		z := make([]float64, p)
		for i := range p {
			s := rhs[i]
			for k := range i {
				s -= L[i*p+k] * z[k]
			}
			z[i] = s / L[i*p+i]
		}
		x := make([]float64, p)
		for i := p - 1; i >= 0; i-- {
			s := z[i]
			for k := i + 1; k < p; k++ {
				s -= L[k*p+i] * x[k]
			}
			x[i] = s / L[i*p+i]
		}
		return x
	}
	x := solve(b)
	inv := make([]float64, p*p)
	e := make([]float64, p)
	for j := range p {
		clear(e)
		e[j] = 1
		col := solve(e)
		for i := range p {
			inv[i*p+j] = col[i]
		}
	}
	return x, inv, nil
}
