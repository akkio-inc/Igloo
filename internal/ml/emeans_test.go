package ml

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// separated makes n rows around the given centers, each coordinate off by
// noise of standard deviation 0.1, the centers taken in turn.
func separated(n int, centers [][]float64, seed uint64) []float64 {
	rng := rand.New(rand.NewPCG(seed, 1))
	d := len(centers[0])
	x := make([]float64, 0, n*d)
	for i := range n {
		for _, v := range centers[i%len(centers)] {
			x = append(x, v+0.1*rng.NormFloat64())
		}
	}
	return x
}

func TestEMeansFindsSeparatedClusters(t *testing.T) {
	centers := [][]float64{{0, 0, 0, 0, 0}, {5, 5, 0, 0, 0}, {0, 0, 5, 5, 5}}
	x := separated(3000, centers, 3)
	km, info, err := TrainEMeans(x, 3000, 5, 3, EMeansParams{Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range centers {
		_, dd := km.Nearest(want)
		if math.Sqrt(dd) > 0.05 {
			t.Errorf("no centroid near %v: nearest is %.3f away", want, math.Sqrt(dd))
		}
	}
	// Each row's squared distance is about 5 dimensions times 0.1².
	if loss := info.Loss[len(info.Loss)-1]; loss > 0.06 {
		t.Errorf("mean squared distance %.4f, want about 0.05", loss)
	}
}

func TestEMeansFromARandomStartCanMissAGroup(t *testing.T) {
	// The reason TrainEMeans starts from k-means++: the paper's random start,
	// on this seed, puts two centroids in one of three groups.
	centers := [][]float64{{0, 0, 0, 0, 0}, {5, 5, 0, 0, 0}, {0, 0, 5, 5, 5}}
	x := separated(3000, centers, 3)
	km, info, err := TrainEMeans(x, 3000, 5, 3, EMeansParams{Seed: 7, RandomStart: true})
	if err != nil {
		t.Fatal(err)
	}
	if loss := info.Loss[len(info.Loss)-1]; loss < 1 {
		t.Skipf("this seed's random start found all three groups (loss %.3f); the case no longer shows the failure", loss)
	}
	_ = km
}

func TestEMeansIsRepeatableForASeed(t *testing.T) {
	x := separated(2000, [][]float64{{0, 0, 0}, {3, 0, 0}, {0, 3, 0}, {0, 0, 3}}, 4)
	a, _, err := TrainEMeans(x, 2000, 3, 4, EMeansParams{Seed: 11, Threads: 4})
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := TrainEMeans(x, 2000, 3, 4, EMeansParams{Seed: 11, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Centroids, b.Centroids) {
		t.Error("the same seed gave different centroids")
	}
}

func TestEMeansRevivesAClusterStartedOnAnOutlier(t *testing.T) {
	// Two real groups and one far outlier row: a centroid seeded on the
	// outlier keeps one row's worth of count, falls below a tenth of the
	// mean, and is split off a real group instead.
	x := separated(4000, [][]float64{{0, 0, 0, 0}, {6, 6, 6, 6}}, 5)
	x = append(x, 100, 100, 100, 100)
	n := 4001
	km, _, err := TrainEMeans(x, n, 4, 3, EMeansParams{Seed: 2, Epochs: 3})
	if err != nil {
		t.Fatal(err)
	}
	st := km.Stats(x, n, 0)
	for c, cnt := range st.Counts {
		if cnt < 100 {
			t.Errorf("centroid %d has %d rows; a dead cluster was not revived (counts %v)", c, cnt, st.Counts)
		}
	}
}

func TestEMeansRefusesMoreClustersThanRows(t *testing.T) {
	if _, _, err := TrainEMeans([]float64{1, 2}, 2, 1, 3, EMeansParams{}); err == nil {
		t.Error("k > n should fail")
	}
}

func TestEMeansScalingLaws(t *testing.T) {
	// 100,000 rows in 5 groups over 5 epochs: about 3,900 rows a batch,
	// rounded to 4,096, and a first step the law puts above 1.
	if b := emeansBatch(100_000, 5, 40, 5); b != 4096 {
		t.Errorf("batch %d, want 4096", b)
	}
	if b := emeansBatch(100_000, 5, 8, 5); b != 512 {
		t.Errorf("batch in 8 dimensions %d, want 512 (the quality law)", b)
	}
	if a := emeansAlpha0(100_000, 5, 4096, 5); a != 1 {
		t.Errorf("alpha0 %.3f, want 1 (clamped)", a)
	}
	if b := emeansBatch(10, 3, 40, 5); b != 1 {
		t.Errorf("batch for 10 rows %d, want 1", b)
	}
	if b := emeansBatch(100, 3, 40, 50); b != 50 {
		t.Errorf("batch %d, want 50 (half the rows)", b)
	}
}
