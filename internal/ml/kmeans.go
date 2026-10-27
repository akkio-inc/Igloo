package ml

import (
	"errors"
	"math"
	"math/rand/v2"
)

// KMeans is a k-means clustering of vectors: its centroids.
type KMeans struct {
	K, D int
	// Centroids holds centroid c at Centroids[c*D:(c+1)*D].
	Centroids []float64
}

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

// KMeansInfo is what training learned besides the centroids.
type KMeansInfo struct {
	// Loss is the mean squared distance of rows to their centroids after
	// each iteration.
	Loss []float64
}

// initSample bounds the rows k-means++ seeds the centroids from.
const initSample = 100_000

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

// seedCentroids picks k rows by k-means++: the first at random, each next
// with a chance proportional to its squared distance from the nearest
// picked, among up to initSample rows.
func seedCentroids(x []float64, n, d, k int, seed uint64) []float64 {
	rows := make([]int, 0, min(n, initSample))
	for i := range n {
		if n <= initSample || unitHash(seed, 0x5a3, i) < float64(initSample)/float64(n) {
			rows = append(rows, i)
		}
	}
	rng := rand.New(rand.NewPCG(seed, 0xc3a7))
	out := make([]float64, 0, k*d)
	dist := make([]float64, len(rows))
	for i := range dist {
		dist[i] = math.Inf(1)
	}
	pick := rows[rng.IntN(len(rows))]
	for c := range k {
		out = append(out, x[pick*d:(pick+1)*d]...)
		if c == k-1 {
			break
		}
		cen := out[c*d : (c+1)*d]
		var total float64
		for j, r := range rows {
			dist[j] = math.Min(dist[j], sqDist(x[r*d:(r+1)*d], cen))
			total += dist[j]
		}
		if total == 0 {
			// Every row sits on a centroid: take the next row not picked.
			pick = rows[(c+1)%len(rows)]
			continue
		}
		u := rng.Float64() * total
		pick = rows[len(rows)-1]
		for j, r := range rows {
			if u < dist[j] {
				pick = r
				break
			}
			u -= dist[j]
		}
	}
	return out
}

func sqDist(a, b []float64) float64 {
	var s float64
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

// Nearest returns the centroid nearest v and its squared distance.
func (km *KMeans) Nearest(v []float64) (int, float64) {
	best, bd := 0, math.Inf(1)
	for c := range km.K {
		if dd := sqDist(v, km.Centroids[c*km.D:(c+1)*km.D]); dd < bd {
			best, bd = c, dd
		}
	}
	return best, bd
}

// Distances writes v's distance (not squared) to each centroid into out.
func (km *KMeans) Distances(v, out []float64) {
	for c := range km.K {
		out[c] = math.Sqrt(sqDist(v, km.Centroids[c*km.D:(c+1)*km.D]))
	}
}

// assign sets each row's nearest centroid and returns the mean squared
// distance.
func (km *KMeans) assign(x []float64, n int, assign []int32, threads int) float64 {
	parts := make([]float64, numChunks(n))
	parallelFor(len(parts), threads, func(ch int) {
		lo, hi := chunkRange(ch, n)
		var s float64
		for i := lo; i < hi; i++ {
			c, dd := km.Nearest(x[i*km.D : (i+1)*km.D])
			assign[i] = int32(c)
			s += dd
		}
		parts[ch] = s
	})
	var total float64
	for _, v := range parts {
		total += v
	}
	return total / float64(max(n, 1))
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

// KMeansStats are a clustering's quality on rows.
type KMeansStats struct {
	// DaviesBouldin is the Davies-Bouldin index: lower is better.
	DaviesBouldin float64
	// MeanSquaredDistance is the rows' mean squared distance to their
	// centroids.
	MeanSquaredDistance float64
	// Counts are each centroid's rows; Spread each one's mean distance of
	// its rows from it.
	Counts []int
	Spread []float64
}

// Stats measures the clustering on rows x.
func (km *KMeans) Stats(x []float64, n int, threads int) KMeansStats {
	K, D := km.K, km.D
	type part struct {
		dist, sq []float64
		count    []int
	}
	parts := make([]part, numChunks(n))
	parallelFor(len(parts), threads, func(ch int) {
		lo, hi := chunkRange(ch, n)
		p := part{dist: make([]float64, K), sq: make([]float64, K), count: make([]int, K)}
		for i := lo; i < hi; i++ {
			c, dd := km.Nearest(x[i*D : (i+1)*D])
			p.count[c]++
			p.sq[c] += dd
			p.dist[c] += math.Sqrt(dd)
		}
		parts[ch] = p
	})
	st := KMeansStats{Counts: make([]int, K), Spread: make([]float64, K)}
	var sq float64
	for _, p := range parts {
		for c := range K {
			st.Counts[c] += p.count[c]
			st.Spread[c] += p.dist[c]
			sq += p.sq[c]
		}
	}
	for c := range K {
		if st.Counts[c] > 0 {
			st.Spread[c] /= float64(st.Counts[c])
		}
	}
	st.MeanSquaredDistance = sq / float64(max(n, 1))
	if K > 1 {
		var db float64
		for a := range K {
			worst := 0.0
			for b := range K {
				if a == b {
					continue
				}
				sep := math.Sqrt(sqDist(km.Centroids[a*D:(a+1)*D], km.Centroids[b*D:(b+1)*D]))
				if sep > 0 {
					worst = math.Max(worst, (st.Spread[a]+st.Spread[b])/sep)
				}
			}
			db += worst
		}
		st.DaviesBouldin = db / float64(K)
	}
	return st
}
