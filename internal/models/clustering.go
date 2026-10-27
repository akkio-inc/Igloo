package models

import (
	"math"
	"sort"
	"strconv"
	"sync"

	"github.com/akkio-inc/igloo/internal/ml"
)

// KMEANS: k-means clustering of the rows' features, standardized and one
// hot encoded, seeded by k-means++; the number of clusters given, or the
// one of 2 to 10 whose Davies-Bouldin index is lowest. Rows far from their
// centroid, for the distances of its own training rows, are anomalies.

// Clusters is an IGLOO.ML.KMEANS model.
type Clusters struct {
	Features []Feature   `json:"features"`
	Encoder  *ml.Encoder `json:"encoder"`
	Model    *ml.KMeans  `json:"model"`
	// Counts are each centroid's training rows, Spread their mean distance
	// from it, which a row's distance is divided by to normalize it.
	Counts []int     `json:"counts"`
	Spread []float64 `json:"spread"`
	// Quantiles are the training rows' normalized distances' 0, 0.001, ...,
	// 1 quantiles, which DETECT_ANOMALIES's contamination picks a
	// threshold from.
	Quantiles []float64      `json:"quantiles"`
	Eval      *Evaluation    `json:"eval"`
	Training  []Iteration    `json:"training"`
	Rows      int            `json:"rows"`
	Hyper     map[string]any `json:"hyperparameters"`

	pool sync.Pool
}

func (c *Clusters) prepare() { c.Encoder.Prepare() }

func (c *Clusters) summary(out map[string]any) {
	out["rows"], out["clusters"] = c.Rows, c.Model.K
	addEval(out, c.Eval)
}

// autoClusters is the most clusters 'num_clusters' left out tries.
const autoClusters = 10

// kmeansOptions reads KMEANS's options: standardize_features, num_clusters
// (0 to pick), and the iterations'.
func kmeansOptions(cfg *config) (standardize bool, k int, p ml.KMeansParams, err error) {
	if standardize, err = cfg.boolean("standardize_features", true); err != nil {
		return
	}
	if k, err = cfg.integer("num_clusters", 0, 2, 100); err != nil {
		return
	}
	if p.Iterations, err = cfg.integer("max_iterations", 20, 1, 1000); err != nil {
		return
	}
	if p.MinRelProgress, err = cfg.num("min_rel_progress", 0.01, 0, 1); err != nil {
		return
	}
	stop, err := cfg.boolean("early_stop", true)
	if err != nil {
		return
	}
	if !stop {
		p.MinRelProgress = 1e-12
	}
	if err = cfg.accept("kmeans_init_method", "kmeans++"); err != nil {
		return
	}
	if err = cfg.accept("distance_type", "euclidean"); err != nil {
		return
	}
	p.Seed, err = cfg.seed()
	return
}

func trainKMeans(t *Table, a IglooArgs, cfg *config) (*Clusters, error) {
	standardize, k, p, err := kmeansOptions(cfg)
	if err != nil {
		return nil, err
	}
	p.Interrupt = a.Interrupt
	frame, feats, err := featureFrame(t, otherNames(t))
	if err != nil {
		return nil, err
	}
	if len(feats) == 0 {
		return nil, inputErr("INPUT_DATA has no columns to cluster")
	}
	frame = frame.Take(frame.CanonicalOrder(nil))
	enc := ml.FitEncoder(frame, standardize, 5, 100)
	if enc.Width == 0 {
		return nil, inputErr("the features hold nothing to cluster on")
	}
	X := enc.Encode(frame)
	n, d := frame.N, enc.Width
	c := &Clusters{Features: feats, Encoder: enc, Rows: n, Hyper: map[string]any{"standardize_features": standardize}}
	if k == 0 {
		if k, err = bestK(X, n, d, p, c.Hyper); err != nil {
			return nil, err
		}
	}
	if n < k {
		return nil, inputErr("num_clusters is %d but INPUT_DATA has %d rows", k, n)
	}
	km, info, err := ml.TrainKMeans(X, n, d, k, p)
	if err != nil {
		return nil, inputErr("%v", err)
	}
	c.Model = km
	c.Hyper["num_clusters"] = k
	for i, l := range info.Loss {
		c.Training = append(c.Training, Iteration{Iteration: i + 1, Loss: l})
	}
	st := km.Stats(X, n, 0)
	c.Counts, c.Spread = st.Counts, st.Spread
	c.Eval = &Evaluation{Rows: n, Metrics: finiteMetrics([]Metric{{"davies_bouldin_index", st.DaviesBouldin},
		{"mean_squared_distance", st.MeanSquaredDistance}})}
	rows := sampleRows(n, quantileRows)
	norm := make([]float64, len(rows))
	parallelRows(len(rows), func(lo, hi int) {
		for k := lo; k < hi; k++ {
			i := rows[k]
			cen, dd := km.Nearest(X[i*d : (i+1)*d])
			norm[k] = c.normalize(cen, math.Sqrt(dd))
		}
	})
	c.Quantiles = quantiles(norm, 1001)
	return c, nil
}

// bestK picks the number of clusters, from 2 to 10, whose clustering of
// up to 20,000 of the rows has the lowest Davies-Bouldin index.
func bestK(X []float64, n, d int, p ml.KMeansParams, hyper map[string]any) (int, error) {
	rows := sampleRows(n, 20_000)
	sx := make([]float64, 0, len(rows)*d)
	for _, r := range rows {
		sx = append(sx, X[r*d:(r+1)*d]...)
	}
	best, bestDB := 0, math.Inf(1)
	tried := map[string]any{}
	for k := 2; k <= min(autoClusters, len(rows)-1); k++ {
		km, _, err := ml.TrainKMeans(sx, len(rows), d, k, p)
		if err != nil {
			return 0, err
		}
		db := km.Stats(sx, len(rows), 0).DaviesBouldin
		tried[strconv.Itoa(k)] = finiteOr(db, 0)
		if db < bestDB-1e-12 {
			best, bestDB = k, db
		}
	}
	if best == 0 {
		return 0, inputErr("INPUT_DATA has too few rows to cluster")
	}
	hyper["davies_bouldin_by_num_clusters"] = tried
	return best, nil
}

// normalize divides a distance from centroid cen by the mean distance of
// the centroid's training rows.
func (c *Clusters) normalize(cen int, dist float64) float64 {
	if s := c.Spread[cen]; s > 0 {
		return dist / s
	}
	return dist
}

// --- row methods ---------------------------------------------------------

func (c *Clusters) rowFeatures(method string) ([]Feature, bool) {
	return c.Features, method == "PREDICT" || method == "DETECT_ANOMALIES"
}

func (c *Clusters) rowOptions(method string, cfg *config) (*RowOptions, error) {
	if method == "DETECT_ANOMALIES" {
		return contaminationOptions(cfg, c.Quantiles)
	}
	return &RowOptions{}, nil
}

func (c *Clusters) rowTable(method string, _ *RowOptions) RowTable {
	if method == "DETECT_ANOMALIES" {
		return RowTable{Columns: []RowColumn{{"is_anomaly", "$.is_anomaly", "BOOLEAN"},
			{"normalized_distance", "$.normalized_distance", "DOUBLE"}, {"centroid_id", "$.centroid_id", "BIGINT"}}}
	}
	return RowTable{Columns: []RowColumn{{"centroid_id", "$.centroid_id", "BIGINT"},
		{"nearest_centroids_distance", "$.nearest_centroids_distance", "JSON"}}}
}

type clusterScratch struct {
	vec, dist []float64
	order     []int
}

func (c *Clusters) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte {
	sc, _ := c.pool.Get().(*clusterScratch)
	if sc == nil {
		sc = &clusterScratch{vec: make([]float64, c.Encoder.Width), dist: make([]float64, c.Model.K), order: make([]int, c.Model.K)}
	}
	defer c.pool.Put(sc)
	c.Encoder.EncodeRow(r, sc.vec)
	c.Model.Distances(sc.vec, sc.dist)
	for i := range sc.order {
		sc.order[i] = i
	}
	sort.SliceStable(sc.order, func(a, b int) bool { return sc.dist[sc.order[a]] < sc.dist[sc.order[b]] })
	nearest := sc.order[0]
	buf = append(buf, '{')
	if method == "DETECT_ANOMALIES" {
		norm := c.normalize(nearest, sc.dist[nearest])
		buf = appendKey(buf, "is_anomaly", true)
		if norm > opt.Threshold {
			buf = append(buf, "true"...)
		} else {
			buf = append(buf, "false"...)
		}
		buf = appendKey(buf, "normalized_distance", false)
		buf = appendNum(buf, norm)
		buf = appendKey(buf, "centroid_id", false)
		buf = appendNum(buf, float64(nearest+1))
		return append(buf, '}')
	}
	buf = appendKey(buf, "centroid_id", true)
	buf = appendNum(buf, float64(nearest+1))
	buf = appendKey(buf, "nearest_centroids_distance", false)
	buf = append(buf, '[')
	for i, k := range sc.order {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"centroid_id":`...)
		buf = appendNum(buf, float64(k+1))
		buf = append(buf, `,"distance":`...)
		buf = appendNum(buf, sc.dist[k])
		buf = append(buf, '}')
	}
	return append(buf, "]}"...)
}

// --- table methods ---------------------------------------------------------

func (c *Clusters) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	add := func(row ...any) { res.Rows = append(res.Rows, row) }
	switch method {
	case "EVALUATE":
		res.Rows = append(res.Rows, evaluationRow(c.Eval, cols))
	case "CENTROIDS":
		km := c.Model
		for k := range km.K {
			cen := km.Centroids[k*km.D : (k+1)*km.D]
			for _, in := range c.Encoder.Inputs {
				if in.Kind == ml.Numeric {
					add(int64(k+1), in.Name, c.Encoder.Decode(in.Offset, cen[in.Offset]), nil, nil)
					continue
				}
				for li, l := range in.Levels {
					add(int64(k+1), in.Name, nil, l, cen[in.Offset+li])
				}
			}
		}
	case "TRAINING_INFO":
		for _, it := range c.Training {
			add(int64(it.Iteration), it.Loss, nil, nil)
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

// evaluateOn measures the clustering on the rows of t.
func (c *Clusters) evaluateOn(t *Table) (*Evaluation, error) {
	f, err := modelFrame(t, c.Features)
	if err != nil {
		return nil, err
	}
	X := c.Encoder.Encode(f)
	st := c.Model.Stats(X, f.N, 0)
	return &Evaluation{Rows: f.N, Metrics: finiteMetrics([]Metric{{"davies_bouldin_index", st.DaviesBouldin},
		{"mean_squared_distance", st.MeanSquaredDistance}})}, nil
}
