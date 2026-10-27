package ml

import (
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
)

func TestRandomForest(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 3))
	n := 3000
	x0, x1 := make([]float64, n), make([]float64, n)
	y, cls := make([]float64, n), make([]float64, n)
	for i := range n {
		x0[i], x1[i] = r.Float64()*4-2, r.Float64()*4-2
		y[i] = 3*x0[i] + math.Sin(2*x1[i]) + 0.1*r.NormFloat64()
		if x0[i]*x1[i] > 0 {
			cls[i] = 1
		}
	}
	f := &Frame{N: n, Cols: []Column{NumericColumn("a", x0), NumericColumn("b", x1)}}
	p := Params{Objective: Regression, Forest: true, Rounds: 60, BaggingFraction: 0.632, FeatureFractionByNode: 0.8,
		NumLeaves: 127, MinDataInLeaf: 3, Seed: 7}
	m, _, err := TrainGBM(f, y, nil, nil, p)
	if err != nil {
		t.Fatal(err)
	}
	pred, _ := m.Predict(f)
	if r2 := 1 - MSE(y, pred)/variance(y); r2 < 0.95 {
		t.Errorf("forest regression R² %.3f", r2)
	}
	p.Threads = 1
	m1, _, _ := TrainGBM(f, y, nil, nil, p)
	if !reflect.DeepEqual(m.Trees, m1.Trees) {
		t.Error("a forest depends on the threads")
	}
	p.Objective, p.Threads = Binary, 0
	c, _, err := TrainGBM(f, cls, nil, nil, p)
	if err != nil {
		t.Fatal(err)
	}
	probs, _ := c.Predict(f)
	right := 0
	for i, q := range probs {
		if q < 0 || q > 1 {
			t.Fatalf("probability %v", q)
		}
		if (q >= 0.5) == (cls[i] == 1) {
			right++
		}
	}
	if right < n*95/100 {
		t.Errorf("forest classifier right on %d of %d", right, n)
	}
	// SHAP adds up to the raw score for a forest too.
	row := make([]uint8, 2)
	var rv RowValues
	rv.Size(2)
	rv.Num[0], rv.Num[1] = 1, -1
	c.BinRow(&rv, row)
	phi := c.Contributions(row, c.Bias())
	raw := make([]float64, 1)
	c.RawRow(row, raw)
	if math.Abs(phi[0]+phi[1]+phi[2]-raw[0]) > 1e-9 {
		t.Errorf("contributions %v add up to %v, not %v", phi, phi[0]+phi[1]+phi[2], raw[0])
	}
}

func variance(v []float64) float64 {
	m := mean(v)
	var s float64
	for _, x := range v {
		s += (x - m) * (x - m)
	}
	return s / float64(len(v))
}

func TestEncoder(t *testing.T) {
	f := &Frame{N: 6, Cols: []Column{
		NumericColumn("n", []float64{1, 2, 3, math.NaN(), 5, 6}),
		CategoricalColumn("c", []string{"a", "b", "a", "a", "b", "z"}, nil),
	}}
	e := FitEncoder(f, true, 2, 0)
	if e.Width != 3 || !slices.Equal(e.Inputs[1].Levels, []string{"a", "b"}) {
		t.Fatalf("encoder %+v", e)
	}
	X := e.Encode(f)
	var rv RowValues
	rv.Size(2)
	out := make([]float64, 3)
	for i := range f.N {
		rv.Num[0] = f.Cols[0].Num[i]
		rv.Str[1] = f.Cols[1].Levels[f.Cols[1].Codes[i]]
		e.EncodeRow(&rv, out)
		if !slices.Equal(out, X[i*3:(i+1)*3]) {
			t.Errorf("row %d: %v, not %v", i, out, X[i*3:(i+1)*3])
		}
	}
	if X[3*3] != 0 || X[5*3+1] != 0 || X[5*3+2] != 0 {
		t.Errorf("a missing number is its mean and an unknown category all zeros: %v", X)
	}
	if v := e.Decode(0, X[0]); math.Abs(v-1) > 1e-12 {
		t.Errorf("decoded %v", v)
	}
}

func TestMLP(t *testing.T) {
	r := rand.New(rand.NewPCG(5, 5))
	n, d := 2000, 2
	x := make([]float64, n*d)
	y, cls := make([]float64, n), make([]float64, n)
	for i := range n {
		a, b := r.Float64()*2-1, r.Float64()*2-1
		x[i*d], x[i*d+1] = a, b
		y[i] = a*a + 0.5*b
		if a*b > 0 {
			cls[i] = 1
		}
	}
	p := MLPParams{Hidden: []int{16, 8}, Loss: LossMSE, Outputs: 1, LearningRate: 0.01, BatchSize: 64, Epochs: 60, Seed: 1}
	m, info, err := TrainMLP(x, n, d, y, nil, 0, nil, p)
	if err != nil {
		t.Fatal(err)
	}
	if mse := m.Loss(x, n, y, LossMSE, 0); mse > 0.01 {
		t.Errorf("regression MSE %v (losses %v)", mse, info.Loss)
	}
	p.Threads = 1
	m1, _, _ := TrainMLP(x, n, d, y, nil, 0, nil, p)
	if !reflect.DeepEqual(m.W, m1.W) {
		t.Error("a network depends on the threads")
	}
	p = MLPParams{Hidden: []int{16}, Loss: LossBinary, LearningRate: 0.01, BatchSize: 32, Epochs: 80, Dropout: 0.1, Seed: 2}
	c, _, err := TrainMLP(x[:1600*d], 1600, d, cls[:1600], x[1600*d:], 400, cls[1600:], p)
	if err != nil {
		t.Fatal(err)
	}
	s := c.NewScratch()
	right := 0
	for i := 1600; i < n; i++ {
		if out := c.Forward(x[i*d:(i+1)*d], c.Layers(), s); (out[0] >= 0.5) == (cls[i] == 1) {
			right++
		}
	}
	if right < 370 {
		t.Errorf("classifier right on %d of 400 held-out rows", right)
	}
}

func TestKMeans(t *testing.T) {
	r := rand.New(rand.NewPCG(9, 9))
	centers := [][2]float64{{0, 0}, {6, 0}, {0, 6}}
	n, d := 900, 2
	x := make([]float64, n*d)
	for i := range n {
		c := centers[i%3]
		x[i*d], x[i*d+1] = c[0]+0.5*r.NormFloat64(), c[1]+0.5*r.NormFloat64()
	}
	km, _, err := TrainKMeans(x, n, d, 3, KMeansParams{Seed: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range centers {
		k, dd := km.Nearest(c[:])
		if dd > 0.1 {
			t.Errorf("no centroid near %v (nearest %d at %v)", c, k, km.Centroids)
		}
	}
	st := km.Stats(x, n, 0)
	if st.DaviesBouldin > 0.3 || slices.Min(st.Counts) != 300 {
		t.Errorf("stats %+v", st)
	}
	km2, _, _ := TrainKMeans(x, n, d, 3, KMeansParams{Seed: 4, Threads: 1})
	if !slices.Equal(km.Centroids, km2.Centroids) {
		t.Error("k-means depends on the threads")
	}
	// Two clusters fit these rows worse.
	km2, _, _ = TrainKMeans(x, n, d, 2, KMeansParams{Seed: 4})
	if st2 := km2.Stats(x, n, 0); st2.DaviesBouldin <= st.DaviesBouldin {
		t.Errorf("Davies-Bouldin of 2 clusters %v, of 3 %v", st2.DaviesBouldin, st.DaviesBouldin)
	}
}

func TestPCA(t *testing.T) {
	r := rand.New(rand.NewPCG(11, 11))
	n, d := 1000, 5
	// Two latent factors spread over five columns, and a little noise.
	load := [][]float64{{1, 2, 0, 1, 0}, {0, 1, 3, 0, 1}}
	x := make([]float64, n*d)
	for i := range n {
		z0, z1 := r.NormFloat64(), r.NormFloat64()
		for j := range d {
			x[i*d+j] = 10 + z0*load[0][j] + z1*load[1][j] + 0.01*r.NormFloat64()
		}
	}
	p, err := FitPCA(x, n, d, 0, 0.99, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Eigenvalues) != 2 {
		t.Fatalf("kept %d components (%v of %v)", len(p.Eigenvalues), p.Eigenvalues, p.TotalVariance)
	}
	// The planted loadings lie in the components' span.
	for _, l := range load {
		var norm, inSpan float64
		for _, v := range l {
			norm += v * v
		}
		for c := range 2 {
			proj := 0.0
			for j := range d {
				proj += l[j] * p.Components[c*d+j]
			}
			inSpan += proj * proj
		}
		if inSpan/norm < 0.999 {
			t.Errorf("loading %v: %.4f of it in the components' span", l, inSpan/norm)
		}
	}
	coords := make([]float64, 2)
	p.Project(x[:d], coords)
	if e := p.ReconstructionError(x[:d], coords); e > 0.001 {
		t.Errorf("reconstruction error %v", e)
	}
	// Components are unit vectors, orthogonal.
	var dotc, n0 float64
	for j := range d {
		dotc += p.Components[j] * p.Components[d+j]
		n0 += p.Components[j] * p.Components[j]
	}
	if math.Abs(dotc) > 1e-9 || math.Abs(n0-1) > 1e-9 {
		t.Errorf("components not orthonormal: %v %v", dotc, n0)
	}
}

func TestALS(t *testing.T) {
	r := rand.New(rand.NewPCG(13, 13))
	users, items, k := 300, 120, 2
	uf, vf := make([]float64, users*k), make([]float64, items*k)
	for i := range uf {
		uf[i] = r.NormFloat64()
	}
	for i := range vf {
		vf[i] = r.NormFloat64()
	}
	var train, test Ratings
	for u := range users {
		for i := range items {
			if r.Float64() > 0.3 {
				continue
			}
			v := 3 + uf[u*k]*vf[i*k] + uf[u*k+1]*vf[i*k+1] + 0.1*r.NormFloat64()
			dst := &train
			if r.Float64() < 0.1 {
				dst = &test
			}
			dst.User, dst.Item, dst.Value = append(dst.User, int32(u)), append(dst.Item, int32(i)), append(dst.Value, v)
		}
	}
	m, info, err := TrainALS(train, users, items, ALSParams{Factors: 4, L2: 0.05, Iterations: 15, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	var se float64
	for e := range test.User {
		d := m.Predict(int(test.User[e]), int(test.Item[e])) - test.Value[e]
		se += d * d
	}
	if rmse := math.Sqrt(se / float64(len(test.User))); rmse > 0.3 {
		t.Errorf("held-out RMSE %.3f (training %v)", rmse, info.Loss)
	}
	m1, _, _ := TrainALS(train, users, items, ALSParams{Factors: 4, L2: 0.05, Iterations: 15, Seed: 1, Threads: 1})
	if !slices.Equal(m.U, m1.U) {
		t.Error("ALS depends on the threads")
	}
	// Implicit: users of two groups each interact with their own half of
	// the items.
	var imp Ratings
	for u := range 200 {
		for i := range 100 {
			if (u < 100) == (i < 50) && r.Float64() < 0.3 {
				imp.User, imp.Item, imp.Value = append(imp.User, int32(u)), append(imp.Item, int32(i)), append(imp.Value, 1)
			}
		}
	}
	im, _, err := TrainALS(imp, 200, 100, ALSParams{Implicit: true, Factors: 4, L2: 0.1, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	wrong := 0
	for u := range 200 {
		var own, other float64
		for i := range 100 {
			if (u < 100) == (i < 50) {
				own += im.Predict(u, i)
			} else {
				other += im.Predict(u, i)
			}
		}
		if own <= other {
			wrong++
		}
	}
	if wrong > 0 {
		t.Errorf("%d users prefer the other group's items", wrong)
	}
	// Factors survive the blob's encoding.
	blob, sha, err := Encode(m)
	if err != nil {
		t.Fatal(err)
	}
	var back ALS
	if err := Decode(blob, sha, &back); err != nil || !slices.Equal(back.U, m.U) || back.Mean != m.Mean {
		t.Errorf("decoded %v", err)
	}
}
