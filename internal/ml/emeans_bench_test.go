package ml

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"testing"
	"time"
)

// TestEMeansAgainstLloyd compares e-means with this package's k-means++ and
// Lloyd on held-out rows. It prints a table and runs only with
// IGLOO_EMEANS_BENCH=1.
func TestEMeansAgainstLloyd(t *testing.T) {
	if os.Getenv("IGLOO_EMEANS_BENCH") == "" {
		t.Skip("set IGLOO_EMEANS_BENCH=1 to run the e-means benchmark")
	}
	type shape struct {
		name             string
		n, num, cats, kt int
		fit              []int // the k values fitted; 0 asks Davies-Bouldin to choose 2..10
	}
	shapes := []shape{
		{"people 20k, 3 groups", 20_000, 12, 4, 3, []int{3, 0}},
		{"people 200k, 6 groups", 200_000, 30, 10, 6, []int{6, 0}},
		{"people 1M, 5 groups", 1_000_000, 40, 12, 5, []int{5, 8}},
		{"people 200k, 8 groups, 100 dims", 200_000, 60, 8, 8, []int{8}},
		{"people 200k, 16 groups, 40 dims", 200_000, 32, 2, 16, []int{16}},
		{"people 200k, 32 groups, 40 dims", 200_000, 32, 2, 32, []int{32}},
		{"people 200k, 64 groups, 64 dims", 200_000, 64, 0, 64, []int{100}},
	}
	seeds := []uint64{1, 2, 3, 4, 5}
	if os.Getenv("IGLOO_EMEANS_BENCH_LARGE") != "" {
		// The paper's regime, many centroids: two seeds, as each fit takes minutes.
		shapes = []shape{{"vectors 200k x 64, k=1024", 200_000, 64, 0, 1024, []int{1024}}}
		seeds = seeds[:2]
	}
	fmt.Printf("%-34s %6s %-27s %12s %9s %9s\n", "data", "k", "method", "test MSE", "worst", "seconds")
	for _, s := range shapes {
		x, d := mixture(s.n, s.num, s.cats, s.kt, 7)
		train, test := s.n*4/5, s.n-s.n*4/5
		xt, xv := x[:train*d], x[train*d:]
		for _, k := range s.fit {
			methods := []struct {
				name string
				fit  func(k int, seed uint64) *KMeans
			}{
				{"k-means++ Lloyd", func(k int, seed uint64) *KMeans {
					km, _, err := TrainKMeans(xt, train, d, k, KMeansParams{Seed: seed})
					must(t, err)
					return km
				}},
				{"e-means 5 ep, random start", func(k int, seed uint64) *KMeans {
					km, _, err := TrainEMeans(xt, train, d, k, EMeansParams{Seed: seed, RandomStart: true})
					must(t, err)
					return km
				}},
				{"e-means 5 ep", func(k int, seed uint64) *KMeans {
					km, _, err := TrainEMeans(xt, train, d, k, EMeansParams{Seed: seed})
					must(t, err)
					return km
				}},
				{"e-means 2 ep", func(k int, seed uint64) *KMeans {
					km, _, err := TrainEMeans(xt, train, d, k, EMeansParams{Seed: seed, Epochs: 2})
					must(t, err)
					return km
				}},
			}
			for _, m := range methods {
				var mses []float64
				var picks []int
				start := time.Now()
				for _, seed := range seeds {
					var km *KMeans
					if k == 0 {
						km = chooseK(m.fit, xt, train, seed)
						picks = append(picks, km.K)
					} else {
						km = m.fit(k, seed)
					}
					mses = append(mses, km.Stats(xv, test, 0).MeanSquaredDistance)
				}
				secs := time.Since(start).Seconds() / float64(len(seeds))
				sort.Float64s(mses)
				label := fmt.Sprint(k)
				if k == 0 {
					label = fmt.Sprintf("auto%v", picks)
				}
				fmt.Printf("%-34s %6s %-27s %12.5f %9.5f %9.2f\n", s.name, label, m.name, average(mses), mses[len(mses)-1], secs)
			}
		}
	}
}

// chooseK fits k = 2..10 and keeps the lowest Davies-Bouldin index, as
// IGLOO.ML.KMEANS does when no k is given.
func chooseK(fit func(int, uint64) *KMeans, x []float64, n int, seed uint64) *KMeans {
	var best *KMeans
	bestDB := math.Inf(1)
	for k := 2; k <= 10; k++ {
		km := fit(k, seed)
		if db := km.Stats(x, n, 0).DaviesBouldin; db < bestDB {
			best, bestDB = km, db
		}
	}
	return best
}

// mixture makes n people of kt groups: num standardized numeric features,
// each group shifted on a few of them with its own spread, and cats
// one-hot categories of 4 values, each group favouring one value. Groups
// differ in size. Rows come shuffled.
func mixture(n, num, cats, kt int, seed uint64) ([]float64, int) {
	rng := rand.New(rand.NewPCG(seed, 0xbeef))
	d := num + 4*cats
	weights := make([]float64, kt)
	var wsum float64
	for g := range kt {
		weights[g] = 1 + float64(g)
		wsum += weights[g]
	}
	centers := make([][]float64, kt)
	spread := make([]float64, kt)
	favour := make([][]int, kt)
	for g := range kt {
		centers[g] = make([]float64, num)
		for j := range num {
			if rng.Float64() < 0.3 {
				centers[g][j] = rng.NormFloat64() * 1.5
			}
		}
		spread[g] = 0.6 + rng.Float64()*0.8
		favour[g] = make([]int, cats)
		for c := range cats {
			favour[g][c] = rng.IntN(4)
		}
	}
	x := make([]float64, n*d)
	for i := range n {
		u, g := rng.Float64()*wsum, 0
		for u > weights[g] && g < kt-1 {
			u -= weights[g]
			g++
		}
		row := x[i*d : (i+1)*d]
		for j := range num {
			row[j] = centers[g][j] + rng.NormFloat64()*spread[g]
		}
		for c := range cats {
			v := favour[g][c]
			if rng.Float64() < 0.4 {
				v = rng.IntN(4)
			}
			row[num+4*c+v] = 1
		}
	}
	return x, d
}

func average(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Lloyd's k-means, which IGLOO.ML.KMEANS trained by before e-means: the
// benchmark's baseline.

// KMeansParams configure TrainKMeans.
type KMeansParams struct {
	// Iterations bounds Lloyd's iterations (default 20); training stops
	// sooner once the mean squared distance improves by less than
	// MinRelProgress of itself (default 0.01).
	Iterations     int
	MinRelProgress float64
	Seed           uint64
	Threads        int
	Interrupt      func() error
}

// TrainKMeans clusters n vectors of d values (row-major) into k clusters:
// centroids seeded by k-means++, then Lloyd's iterations.
func TrainKMeans(x []float64, n, d, k int, p KMeansParams) (*KMeans, KMeansInfo, error) {
	var info KMeansInfo
	if k < 1 || k > n {
		return nil, info, errors.New("k-means needs at least as many distinct rows as clusters")
	}
	if p.Iterations <= 0 {
		p.Iterations = 20
	}
	if p.MinRelProgress <= 0 {
		p.MinRelProgress = 0.01
	}
	km := &KMeans{K: k, D: d, Centroids: seedCentroids(x, n, d, k, p.Seed)}
	assign := make([]int32, n)
	prev := math.Inf(1)
	for range p.Iterations {
		if p.Interrupt != nil {
			if err := p.Interrupt(); err != nil {
				return nil, info, err
			}
		}
		loss := km.assign(x, n, assign, p.Threads)
		info.Loss = append(info.Loss, loss)
		km.update(x, n, assign, p.Threads)
		if prev-loss < p.MinRelProgress*loss {
			break
		}
		prev = loss
	}
	return km, info, nil
}

// update moves each centroid to the mean of its rows; a centroid left
// with none moves to the row farthest from its own.
func (km *KMeans) update(x []float64, n int, assign []int32, threads int) {
	K, D := km.K, km.D
	type part struct {
		sum   []float64
		count []int
	}
	parts := make([]part, numChunks(n))
	parallelFor(len(parts), threads, func(ch int) {
		lo, hi := chunkRange(ch, n)
		p := part{sum: make([]float64, K*D), count: make([]int, K)}
		for i := lo; i < hi; i++ {
			c := int(assign[i])
			p.count[c]++
			row := x[i*D : (i+1)*D]
			s := p.sum[c*D : (c+1)*D]
			for j, v := range row {
				s[j] += v
			}
		}
		parts[ch] = p
	})
	sum, count := make([]float64, K*D), make([]int, K)
	for _, p := range parts {
		for i, v := range p.sum {
			sum[i] += v
		}
		for c, v := range p.count {
			count[c] += v
		}
	}
	for c := range K {
		if count[c] == 0 {
			far, fd := 0, -1.0
			for i := range n {
				own := int(assign[i])
				if dd := sqDist(x[i*D:(i+1)*D], km.Centroids[own*D:(own+1)*D]); dd > fd {
					far, fd = i, dd
				}
			}
			copy(km.Centroids[c*D:(c+1)*D], x[far*D:(far+1)*D])
			assign[far] = int32(c)
			continue
		}
		for j := range D {
			km.Centroids[c*D+j] = sum[c*D+j] / float64(count[c])
		}
	}
}
