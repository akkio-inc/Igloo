package ml

import (
	"errors"
	"math"
	"math/rand/v2"
	"sort"
)

// e-means (Douze et al., "e-means: outperforming k-means (in one-tenth the
// epochs)", arXiv 2611.09501): k-means trained on mini-batches, each
// centroid the ratio of exponentially decayed sums of the rows assigned to
// it and of their count,
//
//	N_j ← γ N_j + (1-γ) n_j
//	S_j ← γ S_j + (1-γ) s_j
//	c_j = S_j / N_j
//
// where n_j and s_j are the count and sum of the batch's rows nearest c_j,
// and 1-γ = α follows a cosine from α0 down to a tenth of it over the run.
// A cluster whose decayed count falls below a tenth of the mean is revived
// by splitting a cluster drawn in proportion to its count. The last epoch's
// assignments give the final centroids, as the mean of each one's rows.
// The batch size and α0 follow the paper's scaling laws in n, k and the
// epochs, unless set.
//
// The centroids start from k-means++, not from the paper's k rows drawn at
// random: with few clusters a random start can put two centroids in one
// group and one between two others, which revival never repairs because
// neither shared centroid's count falls low enough (three groups in five
// dimensions ended at about 170 times the k-means++ loss).

// EMeansParams configure TrainEMeans.
type EMeansParams struct {
	// Epochs is the passes over the rows (default 5).
	Epochs int
	// Batch is the rows a centroid update reads (0: the scaling law).
	Batch int
	// Alpha0 is the update's largest step (0: the scaling law).
	Alpha0 float64
	// RandomStart starts the centroids at k rows drawn at random, as the
	// paper does, rather than by k-means++.
	RandomStart bool
	Seed        uint64
	Threads     int
	Interrupt   func() error
}

const (
	// emeansFloor is α's last value as a share of α0 (the paper's ε).
	emeansFloor = 0.1
	// emeansReviveEvery is the updates between checks for dead clusters,
	// and emeansReviveBelow the share of the mean decayed count below
	// which a cluster is dead.
	emeansReviveEvery = 5
	emeansReviveBelow = 0.1
	// emeansSplit is how far a split moves the two halves of a cluster
	// apart, as a share of each coordinate.
	emeansSplit = 1.0 / 1024
	// emeansPrior is each cluster's decayed count before any row.
	emeansPrior = 1.0 / 65536
	// emeansChunk is the rows of a batch one thread assigns at a time. A
	// batch is often smaller than chunkRows, which would leave one thread
	// doing all of it; a fixed size keeps the sums independent of threads.
	emeansChunk = 512
)

// emeansBatch is the paper's batch size law for n rows, k clusters and
// the epochs, rounded to a power of 2 and at most half the rows. In 16
// dimensions or fewer it is the quality law, an eighth of that.
func emeansBatch(n, k, d, epochs int) int {
	b := 0.035134 * math.Pow(float64(k), 0.134749) * math.Pow(float64(n), 0.876473) *
		math.Pow(float64(epochs), 0.813344)
	if d <= 16 {
		b /= 8
	}
	b = math.Exp2(math.Round(math.Log2(math.Max(b, 1))))
	return max(1, min(int(b), n/2))
}

// emeansAlpha0 is the paper's law for the largest step, in [1e-4, 1].
func emeansAlpha0(n, k, batch, epochs int) float64 {
	a := 0.8244 * math.Pow(float64(k), -0.4617) * math.Pow(float64(batch), 0.9003) *
		math.Pow(float64(n), -0.4498) * math.Pow(float64(epochs), -0.1057)
	return math.Min(math.Max(a, 1e-4), 1)
}

// TrainEMeans clusters n vectors of d values (row-major) into k clusters
// by e-means. Its Loss holds each epoch's mean squared distance of the rows
// to the centroids they were assigned to as the epoch went, then the rows'
// mean squared distance to the final centroids.
func TrainEMeans(x []float64, n, d, k int, p EMeansParams) (*KMeans, KMeansInfo, error) {
	var info KMeansInfo
	if k < 1 || k > n {
		return nil, info, errors.New("k-means needs at least as many distinct rows as clusters")
	}
	if p.Epochs <= 0 {
		p.Epochs = 5
	}
	batch := p.Batch
	if batch <= 0 {
		batch = emeansBatch(n, k, d, p.Epochs)
	}
	batch = min(batch, n)
	alpha0 := p.Alpha0
	if alpha0 <= 0 {
		alpha0 = emeansAlpha0(n, k, batch, p.Epochs)
	}
	rng := rand.New(rand.NewPCG(p.Seed, 0xe3ea))

	km := &KMeans{K: k, D: d}
	if p.RandomStart {
		km.Centroids = make([]float64, 0, k*d)
		for _, r := range rng.Perm(n)[:k] {
			km.Centroids = append(km.Centroids, x[r*d:(r+1)*d]...)
		}
	} else {
		km.Centroids = seedCentroids(x, n, d, k, p.Seed)
	}
	count := make([]float64, k)
	sum := make([]float64, k*d)
	for c := range k {
		count[c] = emeansPrior
		for j := range d {
			sum[c*d+j] = emeansPrior * km.Centroids[c*d+j]
		}
	}

	perEpoch := (n + batch - 1) / batch
	total := float64(p.Epochs * perEpoch)
	revive := d > 2
	assign := make([]int32, n)
	batchCount := make([]float64, k)
	batchSum := make([]float64, k*d)
	step := 0
	for range p.Epochs {
		var epochLoss float64
		order := rng.Perm(n)
		for start := 0; start < n; start += batch {
			if p.Interrupt != nil {
				if err := p.Interrupt(); err != nil {
					return nil, info, err
				}
			}
			rows := order[start:min(start+batch, n)]
			step++
			alpha := alpha0 * (emeansFloor + (1-emeansFloor)*0.5*(1+math.Cos(math.Pi*float64(step)/total)))
			epochLoss += km.accumulate(x, rows, assign, batchCount, batchSum, p.Threads)
			for c := range k {
				count[c] = (1-alpha)*count[c] + alpha*batchCount[c]
				for j := range d {
					sum[c*d+j] = (1-alpha)*sum[c*d+j] + alpha*batchSum[c*d+j]
				}
				if count[c] > 0 {
					for j := range d {
						km.Centroids[c*d+j] = sum[c*d+j] / count[c]
					}
				}
			}
			if revive && step%emeansReviveEvery == 0 {
				km.reviveDead(count, sum, rng)
			}
		}
		info.Loss = append(info.Loss, epochLoss/float64(n))
	}
	// The last epoch's assignments: each centroid becomes its rows' mean.
	finalCount := make([]float64, k)
	finalSum := make([]float64, k*d)
	for i := range n {
		c := int(assign[i])
		finalCount[c]++
		row := x[i*d : (i+1)*d]
		for j, v := range row {
			finalSum[c*d+j] += v
		}
	}
	for c := range k {
		if finalCount[c] > 0 {
			for j := range d {
				km.Centroids[c*d+j] = finalSum[c*d+j] / finalCount[c]
			}
		}
	}
	all := make([]int32, n)
	info.Loss = append(info.Loss, km.assign(x, n, all, p.Threads))
	return km, info, nil
}

// accumulate assigns each of rows to its nearest centroid, records it in
// assign, writes each centroid's count and sum of its rows into count and
// sum, and returns the rows' total squared distance to their centroids.
func (km *KMeans) accumulate(x []float64, rows []int, assign []int32, count, sum []float64, threads int) float64 {
	K, D := km.K, km.D
	type part struct {
		count, sum []float64
		loss       float64
	}
	parts := make([]part, (len(rows)+emeansChunk-1)/emeansChunk)
	parallelFor(len(parts), threads, func(ch int) {
		lo, hi := ch*emeansChunk, min((ch+1)*emeansChunk, len(rows))
		pt := part{count: make([]float64, K), sum: make([]float64, K*D)}
		for _, r := range rows[lo:hi] {
			row := x[r*D : (r+1)*D]
			c, dd := km.Nearest(row)
			assign[r] = int32(c)
			pt.loss += dd
			pt.count[c]++
			s := pt.sum[c*D : (c+1)*D]
			for j, v := range row {
				s[j] += v
			}
		}
		parts[ch] = pt
	})
	clear(count)
	clear(sum)
	var loss float64
	for _, pt := range parts {
		loss += pt.loss
		for c, v := range pt.count {
			count[c] += v
		}
		for i, v := range pt.sum {
			sum[i] += v
		}
	}
	return loss
}

// reviveDead splits a live cluster in two for each dead one: donors drawn
// without replacement in proportion to their decayed counts, each half
// moved a little apart, coordinate by coordinate in alternate directions,
// with half the donor's count.
func (km *KMeans) reviveDead(count, sum []float64, rng *rand.Rand) {
	K, D := km.K, km.D
	var mass float64
	for _, v := range count {
		mass += v
	}
	threshold := emeansReviveBelow * mass / float64(K)
	var dead []int
	type donor struct {
		c   int
		key float64
	}
	var live []donor
	for c := range K {
		if count[c] < threshold {
			dead = append(dead, c)
		} else {
			// Gumbel keys: the largest are a draw without replacement in
			// proportion to count.
			live = append(live, donor{c, math.Log(count[c]) - math.Log(-math.Log(1-rng.Float64()))})
		}
	}
	if len(dead) == 0 || len(live) == 0 {
		return
	}
	sort.Slice(live, func(a, b int) bool { return live[a].key > live[b].key })
	for i, child := range dead {
		if i >= len(live) {
			break
		}
		src := live[i].c
		half := count[src] / 2
		for j := range D {
			v := km.Centroids[src*D+j]
			shift := emeansSplit
			if j%2 == 1 {
				shift = -shift
			}
			km.Centroids[src*D+j] = v * (1 + shift)
			km.Centroids[child*D+j] = v * (1 - shift)
		}
		count[src], count[child] = half, half
		for j := range D {
			sum[src*D+j] = half * km.Centroids[src*D+j]
			sum[child*D+j] = half * km.Centroids[child*D+j]
		}
	}
}
