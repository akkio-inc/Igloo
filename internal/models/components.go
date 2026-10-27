package models

import (
	"strconv"
	"sync"

	"github.com/akkio-inc/igloo/internal/ml"
)

// PCA: principal components of the rows' features, standardized and one
// hot encoded; as many as num_principal_components says, or as explain
// pca_explained_variance_ratio of the variance. A row that its components
// reconstruct poorly, for the errors of the training rows, is an anomaly.

// maxComponentsWidth bounds the encoded features a PCA takes: its
// eigenvectors take the cube of their number to compute.
const maxComponentsWidth = 1000

// Components is an IGLOO.ML.PCA model.
type Components struct {
	Features []Feature   `json:"features"`
	Encoder  *ml.Encoder `json:"encoder"`
	Model    *ml.PCA     `json:"model"`
	// Quantiles are the training rows' reconstruction errors' 0, 0.001,
	// ..., 1 quantiles, for DETECT_ANOMALIES.
	Quantiles []float64      `json:"quantiles"`
	Eval      *Evaluation    `json:"eval"`
	Rows      int            `json:"rows"`
	Hyper     map[string]any `json:"hyperparameters"`

	pool sync.Pool
}

func (c *Components) prepare() { c.Encoder.Prepare() }

func (c *Components) summary(out map[string]any) {
	out["rows"], out["components"] = c.Rows, len(c.Model.Eigenvalues)
	addEval(out, c.Eval)
}

// pcaOptions reads PCA's options: scale_features, and one of
// num_principal_components and pca_explained_variance_ratio.
func pcaOptions(cfg *config) (scale bool, k int, ratio float64, err error) {
	if scale, err = cfg.boolean("scale_features", true); err != nil {
		return
	}
	if k, err = cfg.integer("num_principal_components", 0, 1, maxComponentsWidth); err != nil {
		return
	}
	if ratio, err = cfg.num("pca_explained_variance_ratio", 0, 1e-9, 1); err != nil {
		return
	}
	if (k == 0) == (ratio == 0) {
		err = inputErr("PCA takes one of num_principal_components and pca_explained_variance_ratio")
		return
	}
	if err = cfg.accept("pca_solver", "full", "randomized", "auto"); err != nil {
		return
	}
	_, err = cfg.seed()
	return
}

func trainPCA(t *Table, a IglooArgs, cfg *config) (*Components, error) {
	scale, k, ratio, err := pcaOptions(cfg)
	if err != nil {
		return nil, err
	}
	frame, feats, err := featureFrame(t, otherNames(t))
	if err != nil {
		return nil, err
	}
	if len(feats) == 0 {
		return nil, inputErr("INPUT_DATA has no columns")
	}
	frame = frame.Take(frame.CanonicalOrder(nil))
	enc := ml.FitEncoder(frame, scale, 5, 100)
	switch {
	case enc.Width == 0:
		return nil, inputErr("the features hold nothing to analyse")
	case enc.Width > maxComponentsWidth:
		return nil, inputErr("the features encode to %d columns (a column per category); PCA takes at most %d: leave out "+
			"categorical columns with many categories", enc.Width, maxComponentsWidth)
	case k > enc.Width:
		return nil, inputErr("num_principal_components is %d, but the features encode to %d columns", k, enc.Width)
	}
	X := enc.Encode(frame)
	n, d := frame.N, enc.Width
	p, err := ml.FitPCA(X, n, d, k, ratio, 0)
	if err != nil {
		return nil, inputErr("%v", err)
	}
	c := &Components{Features: feats, Encoder: enc, Model: p, Rows: n,
		Hyper: map[string]any{"scale_features": scale, "num_principal_components": len(p.Eigenvalues)}}
	rows := sampleRows(n, quantileRows)
	errs := make([]float64, len(rows))
	parallelRows(len(rows), func(lo, hi int) {
		coords := make([]float64, len(p.Eigenvalues))
		for k := lo; k < hi; k++ {
			i := rows[k]
			p.Project(X[i*d:(i+1)*d], coords)
			errs[k] = p.ReconstructionError(X[i*d:(i+1)*d], coords)
		}
	})
	c.Quantiles = quantiles(errs, 1001)
	c.Eval = &Evaluation{Rows: n, Metrics: []Metric{{"total_explained_variance_ratio", c.explained()}}}
	return c, nil
}

// explained is the share of the training rows' variance the components
// keep.
func (c *Components) explained() float64 {
	var kept float64
	for _, v := range c.Model.Eigenvalues {
		kept += v
	}
	if c.Model.TotalVariance <= 0 {
		return 1
	}
	return kept / c.Model.TotalVariance
}

// --- row methods ---------------------------------------------------------

func (c *Components) rowFeatures(method string) ([]Feature, bool) {
	return c.Features, method == "PREDICT" || method == "DETECT_ANOMALIES"
}

func (c *Components) rowOptions(method string, cfg *config) (*RowOptions, error) {
	if method == "DETECT_ANOMALIES" {
		return contaminationOptions(cfg, c.Quantiles)
	}
	return &RowOptions{}, nil
}

func (c *Components) rowTable(method string, _ *RowOptions) RowTable {
	if method == "DETECT_ANOMALIES" {
		return RowTable{Columns: []RowColumn{{"is_anomaly", "$.is_anomaly", "BOOLEAN"}, {"mean_squared_error", "$.mean_squared_error", "DOUBLE"}}}
	}
	var cols []RowColumn
	for i := range c.Model.Eigenvalues {
		name := "principal_component_" + strconv.Itoa(i+1)
		cols = append(cols, RowColumn{name, "$." + name, "DOUBLE"})
	}
	return RowTable{Columns: cols}
}

type componentScratch struct{ vec, coords []float64 }

func (c *Components) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte {
	sc, _ := c.pool.Get().(*componentScratch)
	if sc == nil {
		sc = &componentScratch{vec: make([]float64, c.Encoder.Width), coords: make([]float64, len(c.Model.Eigenvalues))}
	}
	defer c.pool.Put(sc)
	c.Encoder.EncodeRow(r, sc.vec)
	c.Model.Project(sc.vec, sc.coords)
	buf = append(buf, '{')
	if method == "DETECT_ANOMALIES" {
		e := c.Model.ReconstructionError(sc.vec, sc.coords)
		buf = appendKey(buf, "is_anomaly", true)
		buf = strconv.AppendBool(buf, e > opt.Threshold)
		buf = appendKey(buf, "mean_squared_error", false)
		buf = appendNum(buf, e)
		return append(buf, '}')
	}
	for i, v := range sc.coords {
		buf = appendKey(buf, "principal_component_"+strconv.Itoa(i+1), i == 0)
		buf = appendNum(buf, v)
	}
	return append(buf, '}')
}

// --- table methods ---------------------------------------------------------

func (c *Components) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	add := func(row ...any) { res.Rows = append(res.Rows, row) }
	p := c.Model
	switch method {
	case "EVALUATE":
		res.Rows = append(res.Rows, evaluationRow(c.Eval, cols))
	case "PRINCIPAL_COMPONENTS":
		for k := range p.Eigenvalues {
			comp := p.Components[k*p.D : (k+1)*p.D]
			for _, in := range c.Encoder.Inputs {
				if in.Kind == ml.Numeric {
					add(int64(k+1), in.Name, comp[in.Offset], nil, nil)
					continue
				}
				for li, l := range in.Levels {
					add(int64(k+1), in.Name, nil, l, comp[in.Offset+li])
				}
			}
		}
	case "PRINCIPAL_COMPONENT_INFO":
		var cum float64
		for k, v := range p.Eigenvalues {
			ratio := 0.0
			if p.TotalVariance > 0 {
				ratio = v / p.TotalVariance
			}
			cum += ratio
			add(int64(k+1), v, ratio, cum)
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

// evaluateOn measures the share of t's rows' variance the components
// keep.
func (c *Components) evaluateOn(t *Table) (*Evaluation, error) {
	f, err := modelFrame(t, c.Features)
	if err != nil {
		return nil, err
	}
	if f.N == 0 {
		return nil, inputErr("INPUT_DATA has no rows")
	}
	X := c.Encoder.Encode(f)
	d := c.Encoder.Width
	p := c.Model
	coords := make([]float64, len(p.Eigenvalues))
	var kept, total float64
	for i := range f.N {
		row := X[i*d : (i+1)*d]
		p.Project(row, coords)
		for _, v := range coords {
			kept += v * v
		}
		for j, v := range row {
			total += (v - p.Mean[j]) * (v - p.Mean[j])
		}
	}
	ratio := 1.0
	if total > 0 {
		ratio = kept / total
	}
	return &Evaluation{Rows: f.N, Metrics: []Metric{{"total_explained_variance_ratio", ratio}}}, nil
}
