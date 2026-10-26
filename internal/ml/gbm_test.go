package ml

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"
)

// synth makes n rows of features x1, x2 (numbers) and c (a category of
// a..d), drawn with seed.
func synth(n int, seed uint64) (*Frame, *rand.Rand) {
	r := rand.New(rand.NewPCG(seed, 7))
	x1, x2 := make([]float64, n), make([]float64, n)
	c := make([]string, n)
	for i := range n {
		x1[i] = r.NormFloat64()
		x2[i] = r.Float64()*4 - 2
		c[i] = string(rune('a' + r.IntN(4)))
	}
	return &Frame{N: n, Cols: []Column{NumericColumn("x1", x1), NumericColumn("x2", x2), CategoricalColumn("c", c, nil)}}, r
}

func splitAt(f *Frame, y []float64, at int) (*Frame, []float64, *Frame, []float64) {
	var a, b []int
	for i := range f.N {
		if i < at {
			a = append(a, i)
		} else {
			b = append(b, i)
		}
	}
	return f.Take(a), y[:at], f.Take(b), y[at:]
}

func TestBinarySeparable(t *testing.T) {
	f, _ := synth(4000, 1)
	y := make([]float64, f.N)
	for i := range f.N {
		if f.Cols[0].Num[i] > 0.3 || f.Cols[2].Levels[f.Cols[2].Codes[i]] == "b" {
			y[i] = 1
		}
	}
	tr, ty, te, tey := splitAt(f, y, 3000)
	m, _, err := TrainGBM(tr, ty, nil, nil, Params{Objective: Binary, Rounds: 60, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Predict(te)
	if err != nil {
		t.Fatal(err)
	}
	pos := make([]bool, len(tey))
	wrong := 0
	for i, v := range tey {
		pos[i] = v == 1
		if (p[i] >= 0.5) != pos[i] {
			wrong++
		}
	}
	if auc := AUC(p, pos); auc < 0.995 {
		t.Errorf("AUC %.4f on a separable set", auc)
	}
	if wrong > 10 {
		t.Errorf("%d of %d test rows misclassified", wrong, len(tey))
	}
}

func TestBinaryNoisy(t *testing.T) {
	f, r := synth(6000, 2)
	y := make([]float64, f.N)
	truth := make([]float64, f.N)
	for i := range f.N {
		truth[i] = sigmoid(1.5*f.Cols[0].Num[i] - f.Cols[1].Num[i])
		if r.Float64() < truth[i] {
			y[i] = 1
		}
	}
	tr, ty, te, tey := splitAt(f, y, 4500)
	vtr, vty, ftr, fty := splitAt(tr, ty, 600)
	m, info, err := TrainGBM(ftr, fty, vtr, vty, Params{Objective: Binary, Rounds: 300, EarlyStopping: 20, Seed: 3})
	if err != nil {
		t.Fatal(err)
	}
	if info.Rounds >= 300 || info.Rounds < 5 {
		t.Errorf("early stopping kept %d rounds", info.Rounds)
	}
	p, _ := m.Predict(te)
	pos := make([]bool, len(tey))
	for i, v := range tey {
		pos[i] = v == 1
	}
	best := AUC(truth[4500:], pos)
	if auc := AUC(p, pos); auc < best-0.03 {
		t.Errorf("AUC %.4f, the true probabilities' %.4f", auc, best)
	}
	base := 0.0
	for _, v := range ty {
		base += v
	}
	base /= float64(len(ty))
	var ll, llBase float64
	for i, v := range tey {
		ll += logLoss1(v, p[i])
		llBase += logLoss1(v, base)
	}
	if ll >= llBase*0.9 {
		t.Errorf("log loss %.4f, no better than the base rate's %.4f", ll/float64(len(tey)), llBase/float64(len(tey)))
	}
}

func TestMulticlass(t *testing.T) {
	f, _ := synth(3000, 4)
	y := make([]float64, f.N)
	for i := range f.N {
		switch x := f.Cols[0].Num[i]; {
		case x < -0.5:
			y[i] = 0
		case x < 0.5:
			y[i] = 1
		default:
			y[i] = 2
		}
		if f.Cols[2].Codes[i] >= 0 && f.Cols[2].Levels[f.Cols[2].Codes[i]] == "d" {
			y[i] = 3
		}
	}
	tr, ty, te, tey := splitAt(f, y, 2400)
	m, _, err := TrainGBM(tr, ty, nil, nil, Params{Objective: Multiclass, Classes: 4, Rounds: 40, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := m.Predict(te)
	n := te.N
	actual, pred := make([]int, n), make([]int, n)
	for i := range n {
		bestK := 0
		for k := 1; k < 4; k++ {
			if p[k*n+i] > p[bestK*n+i] {
				bestK = k
			}
		}
		actual[i], pred[i] = int(tey[i]), bestK
	}
	stats, confusion := ClassReport(actual, pred, 4)
	for k, s := range stats {
		if s.F1 < 0.95 {
			t.Errorf("class %d: F1 %.3f; confusion %v", k, s.F1, confusion)
		}
	}
	if ll := LogLoss(p, actual); ll > 0.2 {
		t.Errorf("log loss %.3f", ll)
	}
}

func TestRegressionAndMissing(t *testing.T) {
	f, r := synth(5000, 5)
	y := make([]float64, f.N)
	for i := range f.N {
		y[i] = 3*math.Sin(f.Cols[0].Num[i]) + f.Cols[1].Num[i]*f.Cols[1].Num[i] + 0.1*r.NormFloat64()
		// Where x1 is missing, the target is high: the model must learn
		// which way missing values go.
		if r.Float64() < 0.2 {
			f.Cols[0].Num[i] = math.NaN()
			y[i] = 10 + 0.1*r.NormFloat64()
		}
	}
	tr, ty, te, tey := splitAt(f, y, 4000)
	m, _, err := TrainGBM(tr, ty, nil, nil, Params{Objective: Regression, Rounds: 200, Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := m.Predict(te)
	var ss, st float64
	mu := mean(tey)
	for i, v := range tey {
		ss += (v - p[i]) * (v - p[i])
		st += (v - mu) * (v - mu)
	}
	if r2 := 1 - ss/st; r2 < 0.97 {
		t.Errorf("R² %.4f", r2)
	}
}

func TestQuantileCoverage(t *testing.T) {
	f, r := synth(8000, 6)
	y := make([]float64, f.N)
	for i := range f.N {
		y[i] = 2*f.Cols[0].Num[i] + (1+math.Abs(f.Cols[1].Num[i]))*r.NormFloat64()
	}
	tr, ty, te, tey := splitAt(f, y, 6000)
	for _, alpha := range []float64{0.1, 0.9} {
		m, _, err := TrainGBM(tr, ty, nil, nil, Params{Objective: Quantile, Alpha: alpha, Rounds: 100, Seed: 2})
		if err != nil {
			t.Fatal(err)
		}
		p, _ := m.Predict(te)
		below := 0.0
		for i, v := range tey {
			if v <= p[i] {
				below++
			}
		}
		if got := below / float64(len(tey)); math.Abs(got-alpha) > 0.04 {
			t.Errorf("quantile %.2f: %.3f of test rows below", alpha, got)
		}
	}
}

func TestDeterministic(t *testing.T) {
	f, r := synth(3000, 8)
	y := make([]float64, f.N)
	for i := range f.N {
		if f.Cols[0].Num[i]+0.5*r.NormFloat64() > 0 {
			y[i] = 1
		}
	}
	train := func(f *Frame, y []float64, threads int) string {
		order := f.CanonicalOrder(y)
		ys := make([]float64, len(order))
		for i, o := range order {
			ys[i] = y[o]
		}
		m, _, err := TrainGBM(f.Take(order), ys, nil, nil, Params{Objective: Binary, Rounds: 30, Seed: 42,
			BaggingFraction: 0.8, FeatureFraction: 0.8, Threads: threads})
		if err != nil {
			t.Fatal(err)
		}
		_, sha, err := Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		return sha
	}
	a := train(f, y, 1)
	if b := train(f, y, 8); a != b {
		t.Errorf("1 and 8 threads trained different models: %s, %s", a, b)
	}
	// The same rows in another order.
	perm := r.Perm(f.N)
	ys := make([]float64, f.N)
	for i, p := range perm {
		ys[i] = y[p]
	}
	if c := train(f.Take(perm), ys, 4); a != c {
		t.Errorf("shuffled rows trained a different model: %s, %s", a, c)
	}
	// Another seed samples other rows.
	order := f.CanonicalOrder(y)
	m, _, _ := TrainGBM(f.Take(order), y, nil, nil, Params{Objective: Binary, Rounds: 30, Seed: 43, BaggingFraction: 0.8})
	if _, sha, _ := Encode(m); sha == a {
		t.Error("another seed trained the same model")
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	f, _ := synth(1000, 3)
	y := make([]float64, f.N)
	for i := range y {
		y[i] = f.Cols[0].Num[i]
	}
	m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: Regression, Rounds: 10})
	if err != nil {
		t.Fatal(err)
	}
	blob, sha, err := Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	var back GBM
	if err := Decode(blob, sha, &back); err != nil {
		t.Fatal(err)
	}
	back.Prepare()
	a, _ := m.Predict(f)
	b, _ := back.Predict(f)
	if !slices.Equal(a, b) {
		t.Error("a decoded model predicts otherwise")
	}
	blob[len(blob)/2] ^= 1
	if err := Decode(blob, sha, &back); err == nil {
		t.Error("a corrupt blob decoded")
	}
}

func TestShapAdditive(t *testing.T) {
	f, r := synth(2000, 11)
	for _, obj := range []Objective{Regression, Binary, Multiclass} {
		y := make([]float64, f.N)
		for i := range y {
			switch obj {
			case Regression:
				y[i] = f.Cols[0].Num[i]*2 + f.Cols[1].Num[i]
			case Binary:
				if f.Cols[0].Num[i]+r.NormFloat64()*0.3 > 0 {
					y[i] = 1
				}
			default:
				y[i] = float64(f.Cols[2].Codes[i] % 3)
			}
			if i%7 == 0 {
				f.Cols[1].Num[i] = math.NaN()
			}
		}
		m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: obj, Classes: 3, Rounds: 20, Seed: 1})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := m.binFrame(f)
		raw := m.rawBinned(b)
		bias := m.Bias()
		F, K := len(m.Features), m.outputs()
		for i := 0; i < f.N; i += 37 {
			phi := m.Contributions(m.RowBins(b, i), bias)
			for k := range K {
				s := 0.0
				for j := 0; j <= F; j++ {
					s += phi[k*(F+1)+j]
				}
				if math.Abs(s-raw[k*f.N+i]) > 1e-9*math.Max(1, math.Abs(s)) {
					t.Fatalf("%s row %d class %d: contributions add up to %v, the score is %v", obj, i, k, s, raw[k*f.N+i])
				}
			}
		}
	}
}

// exactShap computes a tree's SHAP values for a row by Shapley's formula
// over every subset of features, with TreeSHAP's expectation: features
// outside the subset follow both branches, weighted by cover.
func exactShap(t *Tree, row []uint8, nf int) []float64 {
	var expect func(node int, in uint) float64
	expect = func(node int, in uint) float64 {
		n := &t.Nodes[node]
		if n.IsLeaf() {
			return n.Value
		}
		if in&(1<<uint(n.Feature)) != 0 {
			if n.goesLeft(row[n.Feature]) {
				return expect(int(n.LeftChild), in)
			}
			return expect(int(n.RightChild), in)
		}
		l, r := &t.Nodes[n.LeftChild], &t.Nodes[n.RightChild]
		return (l.Cover*expect(int(n.LeftChild), in) + r.Cover*expect(int(n.RightChild), in)) / n.Cover
	}
	fact := func(k int) float64 {
		f := 1.0
		for i := 2; i <= k; i++ {
			f *= float64(i)
		}
		return f
	}
	phi := make([]float64, nf)
	for i := range nf {
		for s := uint(0); s < 1<<uint(nf); s++ {
			if s&(1<<uint(i)) != 0 {
				continue
			}
			size := 0
			for j := range nf {
				if s&(1<<uint(j)) != 0 {
					size++
				}
			}
			w := fact(size) * fact(nf-size-1) / fact(nf)
			phi[i] += w * (expect(0, s|1<<uint(i)) - expect(0, s))
		}
	}
	return phi
}

func TestShapExact(t *testing.T) {
	n := 3000
	r := rand.New(rand.NewPCG(5, 5))
	cols := make([]Column, 5)
	vals := make([][]float64, 5)
	for j := range cols {
		vals[j] = make([]float64, n)
		for i := range n {
			vals[j][i] = r.NormFloat64()
		}
		cols[j] = NumericColumn(fmt.Sprintf("f%d", j), vals[j])
	}
	y := make([]float64, n)
	for i := range n {
		y[i] = vals[0][i]*vals[1][i] + math.Max(vals[2][i], 0) - vals[3][i] + 0.5*vals[0][i]
		if i%5 == 0 {
			vals[4][i] = math.NaN()
		}
	}
	f := &Frame{N: n, Cols: cols}
	m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: Regression, Rounds: 3, NumLeaves: 12, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := m.binFrame(f)
	for i := 0; i < 40; i++ {
		row := m.RowBins(b, i*71)
		for ti := range m.Trees {
			tr := &m.Trees[ti]
			got := make([]float64, 5)
			tr.shap(row, got)
			want := exactShap(tr, row, 5)
			for j := range got {
				if math.Abs(got[j]-want[j]) > 1e-9 {
					t.Fatalf("row %d tree %d feature %d: TreeSHAP %v, Shapley %v", i*71, ti, j, got, want)
				}
			}
		}
	}
}

func TestImportanceFindsTheSignal(t *testing.T) {
	f, r := synth(3000, 12)
	y := make([]float64, f.N)
	for i := range y {
		y[i] = 5*f.Cols[1].Num[i] + 0.1*r.NormFloat64()
	}
	m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: Regression, Rounds: 50})
	if err != nil {
		t.Fatal(err)
	}
	gain := m.Importance()
	shap, err := m.MeanAbsContributions(f, 500)
	if err != nil {
		t.Fatal(err)
	}
	if gain[1] < 0.9 || shap[1] < 5*(shap[0]+shap[2]) {
		t.Errorf("x2 drives the target; gain shares %v, mean |SHAP| %v", gain, shap)
	}
}

func TestRareCategoriesShareABin(t *testing.T) {
	n := 1000
	vals := make([]string, n)
	for i := range n {
		vals[i] = "common"
		if i < 3 {
			vals[i] = fmt.Sprintf("person_%d", i)
		}
	}
	f := &Frame{N: n, Cols: []Column{CategoricalColumn("who", vals, nil)}}
	y := make([]float64, n)
	m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: Regression, Rounds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if b := m.Features[0]; !slices.Equal(b.Levels, []string{"common"}) || !b.Other {
		t.Errorf("levels %v, other %v: rare categories must not be kept", b.Levels, b.Other)
	}
}

func TestLinear(t *testing.T) {
	n := 20000
	r := rand.New(rand.NewPCG(3, 3))
	x1, x2 := make([]float64, n), make([]float64, n)
	seg := make([]string, n)
	yl, yb := make([]float64, n), make([]float64, n)
	for i := range n {
		x1[i] = r.NormFloat64()*2 + 1
		x2[i] = r.Float64() * 10
		seg[i] = []string{"north", "south", "west"}[r.IntN(3)]
		eta := 0.5 + 1.0*x1[i] - 0.2*x2[i]
		if seg[i] == "west" {
			eta += 0.8
		}
		yl[i] = eta + r.NormFloat64()*0.5
		if r.Float64() < sigmoid(eta) {
			yb[i] = 1
		}
	}
	f := &Frame{N: n, Cols: []Column{NumericColumn("x1", x1), NumericColumn("x2", x2), CategoricalColumn("seg", seg, nil)}}
	want := map[string]float64{"x1": 1.0, "x2": -0.2}
	for _, logistic := range []bool{false, true} {
		y := yl
		if logistic {
			y = yb
		}
		l, err := FitLinear(f, y, logistic, 0)
		if err != nil {
			t.Fatal(err)
		}
		tol := 0.02
		if logistic {
			tol = 0.08
		}
		var west float64
		var southSeen bool
		for k, term := range l.Terms {
			if w, ok := want[term.Name()]; ok && math.Abs(l.Coef[k]-w) > tol*math.Max(1, math.Abs(w))*5 {
				t.Errorf("logistic %v: %s coefficient %.3f, want %.3f (± %.3f)", logistic, term.Name(), l.Coef[k], w, l.StdErr[k])
			}
			if l.StdErr[k] <= 0 {
				t.Errorf("%s has standard error %v", term.Name(), l.StdErr[k])
			}
			switch term.Level {
			case "west":
				west = l.Coef[k]
			case "south":
				southSeen = true
			}
		}
		_ = southSeen
		// west against the reference level (the most frequent of the
		// others, whose effect is 0): about 0.8.
		if math.Abs(west-0.8) > 0.25 {
			t.Errorf("logistic %v: west's effect %.3f, want about 0.8", logistic, west)
		}
		// Contributions plus bias give the prediction.
		p := l.Predict(f)
		c := l.Contributions(f)
		T := len(l.Terms)
		for i := 0; i < n; i += 997 {
			s := 0.0
			for k := 0; k <= T; k++ {
				s += c[i*(T+1)+k]
			}
			if logistic {
				s = sigmoid(s)
			}
			if math.Abs(s-p[i]) > 1e-9 {
				t.Fatalf("contributions give %v, the prediction is %v", s, p[i])
			}
		}
	}
}

func TestMetrics(t *testing.T) {
	if got := AUC([]float64{0.1, 0.4, 0.35, 0.8}, []bool{false, false, true, true}); got != 0.75 {
		t.Errorf("AUC %v, want 0.75", got)
	}
	if got := AUC([]float64{0.5, 0.5}, []bool{false, true}); got != 0.5 {
		t.Errorf("AUC of a tie %v, want 0.5", got)
	}
	if got := MDA(1, []float64{2, 1, 3}, []float64{1.5, 2.5, 4}); math.Abs(got-2.0/3) > 1e-12 {
		t.Errorf("MDA %v", got)
	}
	if got := Coverage([]float64{1, 5}, []float64{0, 0}, []float64{2, 2}); got != 0.5 {
		t.Errorf("coverage %v", got)
	}
	for _, p := range []float64{0.005, 0.025, 0.5, 0.9, 0.995} {
		if back := NormalCDF(NormalQuantile(p)); math.Abs(back-p) > 1e-12 {
			t.Errorf("Φ(Φ⁻¹(%v)) = %v", p, back)
		}
	}
	if q := NormalQuantile(0.975); math.Abs(q-1.959963984540054) > 1e-9 {
		t.Errorf("Φ⁻¹(0.975) = %v", q)
	}
}

// BenchmarkTrain1M trains a binary model on a million rows of 50 features
// (40 numeric, 10 categorical), the scale docs/ml-functions.md quotes. It
// runs only with IGLOO_ML_BENCH=1.
func BenchmarkTrain1M(b *testing.B) {
	if os.Getenv("IGLOO_ML_BENCH") == "" {
		b.Skip("set IGLOO_ML_BENCH=1")
	}
	n := 1_000_000
	r := rand.New(rand.NewPCG(1, 1))
	f := &Frame{N: n}
	for j := range 40 {
		v := make([]float64, n)
		for i := range v {
			v[i] = r.NormFloat64()
		}
		f.Cols = append(f.Cols, NumericColumn(fmt.Sprintf("n%d", j), v))
	}
	for j := range 10 {
		v := make([]string, n)
		for i := range v {
			v[i] = fmt.Sprintf("v%d", r.IntN(20+j*10))
		}
		f.Cols = append(f.Cols, CategoricalColumn(fmt.Sprintf("c%d", j), v, nil))
	}
	y := make([]float64, n)
	for i := range y {
		eta := f.Cols[0].Num[i] - 0.5*f.Cols[1].Num[i] + 0.3*f.Cols[2].Num[i]*f.Cols[3].Num[i]
		if f.Cols[40].Codes[i]%3 == 0 {
			eta += 0.7
		}
		if r.Float64() < sigmoid(eta) {
			y[i] = 1
		}
	}
	for b.Loop() {
		start := time.Now()
		m, _, err := TrainGBM(f, y, nil, nil, Params{Objective: Binary, Rounds: 100, Seed: 1})
		if err != nil {
			b.Fatal(err)
		}
		train := time.Since(start)
		start = time.Now()
		if _, err := m.Predict(f); err != nil {
			b.Fatal(err)
		}
		b.Logf("100 rounds on 1M rows x 50 features: trained in %v, scored in %v", train, time.Since(start))
	}
}
