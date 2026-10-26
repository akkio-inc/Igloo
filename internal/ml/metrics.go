package ml

import (
	"math"
	"sort"
)

// AUC is the area under the ROC curve of scores for rows whose label is
// positive against the others: the chance that a random positive scores
// above a random negative, ties counting half. NaN without both.
func AUC(scores []float64, positive []bool) float64 {
	idx := make([]int, len(scores))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return scores[idx[a]] < scores[idx[b]] })
	var pos, neg float64
	var rankSum float64
	for i := 0; i < len(idx); {
		j := i
		for j < len(idx) && scores[idx[j]] == scores[idx[i]] {
			j++
		}
		rank := float64(i+j+1) / 2 // average 1-based rank of the tie group
		for k := i; k < j; k++ {
			if positive[idx[k]] {
				rankSum += rank
				pos++
			} else {
				neg++
			}
		}
		i = j
	}
	if pos == 0 || neg == 0 {
		return math.NaN()
	}
	return (rankSum - pos*(pos+1)/2) / (pos * neg)
}

// ClassStats are one class's metrics against the rest.
type ClassStats struct {
	Precision, Recall, F1 float64
	Support               int
}

// ClassReport compares predicted classes with actual ones (numbers below
// k): each class's precision, recall, F1 and support, and the confusion
// matrix, actual by predicted.
func ClassReport(actual, predicted []int, k int) ([]ClassStats, [][]int) {
	confusion := make([][]int, k)
	for i := range confusion {
		confusion[i] = make([]int, k)
	}
	for i, a := range actual {
		confusion[a][predicted[i]]++
	}
	out := make([]ClassStats, k)
	for c := range k {
		tp := confusion[c][c]
		var predictedC, actualC int
		for o := range k {
			predictedC += confusion[o][c]
			actualC += confusion[c][o]
		}
		s := ClassStats{Support: actualC}
		if predictedC > 0 {
			s.Precision = float64(tp) / float64(predictedC)
		}
		if actualC > 0 {
			s.Recall = float64(tp) / float64(actualC)
		}
		if s.Precision+s.Recall > 0 {
			s.F1 = 2 * s.Precision * s.Recall / (s.Precision + s.Recall)
		}
		out[c] = s
	}
	return out, confusion
}

// Averages are the macro (unweighted) and support-weighted means of
// per-class values.
func Averages(values []float64, support []int) (macro, weighted float64) {
	var total float64
	for i, v := range values {
		macro += v
		weighted += v * float64(support[i])
		total += float64(support[i])
	}
	if len(values) > 0 {
		macro /= float64(len(values))
	}
	if total > 0 {
		weighted /= total
	}
	return macro, weighted
}

// LogLoss is the mean negative log probability of the actual classes;
// probs holds row i's probability of class c at probs[c*n+i].
func LogLoss(probs []float64, actual []int) float64 {
	n := len(actual)
	if n == 0 {
		return math.NaN()
	}
	var s float64
	for i, a := range actual {
		s -= math.Log(clamp(probs[a*n+i], 1e-15, 1))
	}
	return s / float64(n)
}

// Threshold is one class's counts and rates when rows whose probability
// of the class is at least Threshold are predicted to be in it.
type Threshold struct {
	Threshold               float64
	Precision, Recall, F1   float64
	TPR, FPR, Accuracy      float64
	TP, FP, TN, FN, Support int
}

// Thresholds returns a class's metrics at thresholds 0, 0.01, ..., 1 for
// probabilities prob of being in the class and actual membership.
func Thresholds(prob []float64, actual []bool) []Threshold {
	out := make([]Threshold, 0, 101)
	for s := 0; s <= 100; s++ {
		th := float64(s) / 100
		var t Threshold
		t.Threshold = th
		for i, p := range prob {
			pred := p >= th
			switch {
			case pred && actual[i]:
				t.TP++
			case pred:
				t.FP++
			case actual[i]:
				t.FN++
			default:
				t.TN++
			}
		}
		t.Support = t.TP + t.FN
		if t.TP+t.FP > 0 {
			t.Precision = float64(t.TP) / float64(t.TP+t.FP)
		}
		if t.Support > 0 {
			t.Recall = float64(t.TP) / float64(t.Support)
		}
		t.TPR = t.Recall
		if t.FP+t.TN > 0 {
			t.FPR = float64(t.FP) / float64(t.FP+t.TN)
		}
		if t.Precision+t.Recall > 0 {
			t.F1 = 2 * t.Precision * t.Recall / (t.Precision + t.Recall)
		}
		if n := len(prob); n > 0 {
			t.Accuracy = float64(t.TP+t.TN) / float64(n)
		}
		out = append(out, t)
	}
	return out
}

// Point forecast errors of predictions f of actual values y.

// MAE is the mean absolute error.
func MAE(y, f []float64) float64 {
	var s float64
	for i := range y {
		s += math.Abs(y[i] - f[i])
	}
	return s / float64(max(len(y), 1))
}

// MSE is the mean squared error.
func MSE(y, f []float64) float64 {
	var s float64
	for i := range y {
		d := y[i] - f[i]
		s += d * d
	}
	return s / float64(max(len(y), 1))
}

// MAPE is the mean absolute percentage error over the nonzero actuals.
func MAPE(y, f []float64) float64 {
	var s float64
	n := 0
	for i := range y {
		if y[i] != 0 {
			s += math.Abs((y[i] - f[i]) / y[i])
			n++
		}
	}
	if n == 0 {
		return math.NaN()
	}
	return s / float64(n)
}

// SMAPE is the symmetric mean absolute percentage error, as a fraction.
func SMAPE(y, f []float64) float64 {
	var s float64
	n := 0
	for i := range y {
		d := math.Abs(y[i]) + math.Abs(f[i])
		n++
		if d > 0 {
			s += 2 * math.Abs(y[i]-f[i]) / d
		}
	}
	if n == 0 {
		return math.NaN()
	}
	return s / float64(n)
}

// MDA is the mean directional accuracy: how often a prediction moves away
// from the previous actual value in the direction the actual value did.
// prev is the actual value before y[0].
func MDA(prev float64, y, f []float64) float64 {
	var hits float64
	for i := range y {
		last := prev
		if i > 0 {
			last = y[i-1]
		}
		if sign(y[i]-last) == sign(f[i]-last) {
			hits++
		}
	}
	return hits / float64(max(len(y), 1))
}

func sign(x float64) int {
	switch {
	case x > 0:
		return 1
	case x < 0:
		return -1
	}
	return 0
}

// Winkler is the mean Winkler score of intervals [lo, hi] at level
// 1-alpha: their width, plus 2/alpha times how far outside them actual
// values fall.
func Winkler(y, lo, hi []float64, alpha float64) float64 {
	var s float64
	for i := range y {
		w := hi[i] - lo[i]
		switch {
		case y[i] < lo[i]:
			w += 2 / alpha * (lo[i] - y[i])
		case y[i] > hi[i]:
			w += 2 / alpha * (y[i] - hi[i])
		}
		s += w
	}
	return s / float64(max(len(y), 1))
}

// Coverage is the share of actual values inside their intervals.
func Coverage(y, lo, hi []float64) float64 {
	var in float64
	for i := range y {
		if y[i] >= lo[i] && y[i] <= hi[i] {
			in++
		}
	}
	return in / float64(max(len(y), 1))
}

// NormalQuantile is the standard normal distribution's quantile function
// (Acklam's rational approximation, refined by a Newton step).
func NormalQuantile(p float64) float64 {
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	a := []float64{-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00}
	b := []float64{-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01}
	c := []float64{-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00}
	d := []float64{7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00}
	const lo, hi = 0.02425, 1 - 0.02425
	var x float64
	switch {
	case p < lo:
		q := math.Sqrt(-2 * math.Log(p))
		x = (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	case p <= hi:
		q := p - 0.5
		r := q * q
		x = (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r + a[5]) * q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r + 1)
	default:
		q := math.Sqrt(-2 * math.Log(1-p))
		x = -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	}
	e := NormalCDF(x) - p
	u := e * math.Sqrt(2*math.Pi) * math.Exp(x*x/2)
	return x - u/(1+x*u/2)
}

// NormalCDF is the standard normal distribution function.
func NormalCDF(x float64) float64 { return 0.5 * math.Erfc(-x/math.Sqrt2) }
