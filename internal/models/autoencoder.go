package models

import (
	"math"
	"strconv"
	"sync"

	"github.com/akkio-inc/igloo/internal/ml"
)

// AUTOENCODER: a network that squeezes the rows' features, standardized
// and one hot, through a narrow layer (the latent space) and rebuilds them.
// PREDICT gives a row's latent values; rows it rebuilds poorly, for the
// errors of the training rows, are anomalies.

// Autoencoder is an IGLOO.ML.AUTOENCODER model.
type Autoencoder struct {
	Features []Feature   `json:"features"`
	Encoder  *ml.Encoder `json:"encoder"`
	Net      *ml.MLP     `json:"net"`
	// Latent is the number of dense layers up to and with the latent one.
	Latent int `json:"latent"`
	// Quantiles are the training rows' mean squared reconstruction errors'
	// 0, 0.001, ..., 1 quantiles, for DETECT_ANOMALIES.
	Quantiles []float64      `json:"quantiles"`
	Eval      *Evaluation    `json:"eval"`
	Training  []Iteration    `json:"training"`
	Rows      int            `json:"rows"`
	Hyper     map[string]any `json:"hyperparameters"`

	pool sync.Pool
}

func (a *Autoencoder) prepare() { a.Encoder.Prepare() }

func (a *Autoencoder) summary(out map[string]any) {
	out["rows"], out["features"], out["latent"] = a.Rows, len(a.Features), a.Net.Sizes[a.Latent]
	addEval(out, a.Eval)
}

func trainAutoencoder(t *Table, args IglooArgs, cfg *config) (*Autoencoder, error) {
	frame, feats, err := featureFrame(t, otherNames(t))
	if err != nil {
		return nil, err
	}
	if len(feats) == 0 {
		return nil, inputErr("INPUT_DATA has no columns")
	}
	frame = frame.Take(frame.CanonicalOrder(nil))
	seed, err := cfg.seed()
	if err != nil {
		return nil, err
	}
	enc := ml.FitEncoder(frame, true, 5, 100)
	w := enc.Width
	if w < 2 {
		return nil, inputErr("the features encode to %d columns; an autoencoder needs two or more", w)
	}
	h := min(64, max(4, w/2))
	latent := min(16, max(2, w/4))
	o, err := readNetOptions(cfg, []int{h, latent, h})
	if err != nil {
		return nil, err
	}
	// The narrowest layer is the latent space, the middle one of several.
	lat := 0
	narrow := math.MaxInt
	for _, u := range o.hidden {
		narrow = min(narrow, u)
	}
	var at []int
	for i, u := range o.hidden {
		if u == narrow {
			at = append(at, i)
		}
	}
	lat = at[len(at)/2]
	train, eval, err := splitRows(cfg, frame.N, nil, false, seed)
	if err != nil {
		return nil, err
	}
	X := enc.Encode(frame)
	tx, _ := takeRows(X, w, make([]float64, frame.N), train)
	p := o.params(len(train), seed, args.Interrupt)
	p.Loss, p.Outputs = ml.LossMSE, w
	// The latent layer is linear: its values are coordinates, as PCA's.
	p.HiddenActivations[lat] = "identity"
	keep, held := validationSplit(len(train), nil, false, seed)
	if !o.stop {
		keep, held = identity(len(train)), nil
	}
	kx, _ := takeRows(tx, w, make([]float64, len(train)), keep)
	vx, _ := takeRows(tx, w, make([]float64, len(train)), held)
	net, info, err := ml.TrainMLP(kx, len(keep), w, kx, vx, len(held), vx, p)
	if err != nil {
		return nil, inputErr("%v", err)
	}
	a := &Autoencoder{Features: feats, Encoder: enc, Net: net, Latent: lat + 1, Rows: len(train),
		Hyper: map[string]any{"hidden_units": o.hidden, "activation_fn": o.activation, "dropout": o.dropout, "batch_size": p.BatchSize,
			"learn_rate": o.lr, "max_iterations": info.BestEpoch, "l2_reg": o.l2}}
	a.Training = netTraining(info, o.lr)
	errs := make([]float64, len(train))
	parallelRows(len(train), func(lo, hi int) {
		s := net.NewScratch()
		for i := lo; i < hi; i++ {
			_, mse, _ := a.reconstruction(tx[i*w:(i+1)*w], s)
			errs[i] = mse
		}
	})
	a.Quantiles = quantiles(errs, 1001)
	rows := eval
	src := X
	if len(rows) == 0 {
		rows, src = identity(len(train)), tx
	}
	a.Eval = a.evaluate(src, rows)
	return a, nil
}

// reconstruction rebuilds encoded row x and returns its mean absolute,
// squared and squared log errors (the log of values below 0 taken as 0's).
func (a *Autoencoder) reconstruction(x []float64, s *ml.MLPScratch) (mae, mse, msle float64) {
	out := a.Net.Forward(x, a.Net.Layers(), s)
	for j, v := range x {
		d := v - out[j]
		mae += math.Abs(d)
		mse += d * d
		l := math.Log1p(math.Max(v, 0)) - math.Log1p(math.Max(out[j], 0))
		msle += l * l
	}
	n := float64(len(x))
	return mae / n, mse / n, msle / n
}

// evaluate is EVALUATE's reconstruction errors of rows of X.
func (a *Autoencoder) evaluate(X []float64, rows []int) *Evaluation {
	w := a.Encoder.Width
	type part struct{ mae, mse, msle float64 }
	parts := make([]part, len(rows))
	parallelRows(len(rows), func(lo, hi int) {
		s := a.Net.NewScratch()
		for k := lo; k < hi; k++ {
			r := rows[k]
			mae, mse, msle := a.reconstruction(X[r*w:(r+1)*w], s)
			parts[k] = part{mae, mse, msle}
		}
	})
	var mae, mse, msle float64
	for _, p := range parts {
		mae, mse, msle = mae+p.mae, mse+p.mse, msle+p.msle
	}
	n := float64(max(len(rows), 1))
	return &Evaluation{Rows: len(rows), Metrics: []Metric{{"mean_absolute_error", mae / n}, {"mean_squared_error", mse / n},
		{"mean_squared_log_error", msle / n}}}
}

// --- row methods ---------------------------------------------------------

func (a *Autoencoder) rowFeatures(method string) ([]Feature, bool) {
	switch method {
	case "PREDICT", "RECONSTRUCTION_LOSS", "DETECT_ANOMALIES":
		return a.Features, true
	}
	return nil, false
}

func (a *Autoencoder) rowOptions(method string, cfg *config) (*RowOptions, error) {
	if method == "DETECT_ANOMALIES" {
		return contaminationOptions(cfg, a.Quantiles)
	}
	return &RowOptions{}, nil
}

func (a *Autoencoder) rowTable(method string, _ *RowOptions) RowTable {
	switch method {
	case "DETECT_ANOMALIES":
		return RowTable{Columns: []RowColumn{{"is_anomaly", "$.is_anomaly", "BOOLEAN"}, {"mean_squared_error", "$.mean_squared_error", "DOUBLE"}}}
	case "RECONSTRUCTION_LOSS":
		return RowTable{Columns: []RowColumn{{"mean_absolute_error", "$.mean_absolute_error", "DOUBLE"},
			{"mean_squared_error", "$.mean_squared_error", "DOUBLE"}, {"mean_squared_log_error", "$.mean_squared_log_error", "DOUBLE"}}}
	}
	var cols []RowColumn
	for i := range a.Net.Sizes[a.Latent] {
		name := "latent_col_" + strconv.Itoa(i+1)
		cols = append(cols, RowColumn{name, "$." + name, "DOUBLE"})
	}
	return RowTable{Columns: cols}
}

type autoencoderScratch struct {
	vec []float64
	net *ml.MLPScratch
}

func (a *Autoencoder) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte {
	sc, _ := a.pool.Get().(*autoencoderScratch)
	if sc == nil {
		sc = &autoencoderScratch{vec: make([]float64, a.Encoder.Width), net: a.Net.NewScratch()}
	}
	defer a.pool.Put(sc)
	a.Encoder.EncodeRow(r, sc.vec)
	buf = append(buf, '{')
	switch method {
	case "PREDICT":
		for i, v := range a.Net.Forward(sc.vec, a.Latent, sc.net) {
			buf = appendKey(buf, "latent_col_"+strconv.Itoa(i+1), i == 0)
			buf = appendNum(buf, v)
		}
	case "RECONSTRUCTION_LOSS":
		mae, mse, msle := a.reconstruction(sc.vec, sc.net)
		buf = appendKey(buf, "mean_absolute_error", true)
		buf = appendNum(buf, mae)
		buf = appendKey(buf, "mean_squared_error", false)
		buf = appendNum(buf, mse)
		buf = appendKey(buf, "mean_squared_log_error", false)
		buf = appendNum(buf, msle)
	default:
		_, mse, _ := a.reconstruction(sc.vec, sc.net)
		buf = appendKey(buf, "is_anomaly", true)
		buf = strconv.AppendBool(buf, mse > opt.Threshold)
		buf = appendKey(buf, "mean_squared_error", false)
		buf = appendNum(buf, mse)
	}
	return append(buf, '}')
}

// --- table methods ---------------------------------------------------------

func (a *Autoencoder) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	switch method {
	case "EVALUATE":
		res.Rows = append(res.Rows, evaluationRow(a.Eval, cols))
	case "TRAINING_INFO":
		for _, it := range a.Training {
			res.Rows = append(res.Rows, []any{int64(it.Iteration), it.Loss, it.EvalLoss, it.LearningRate})
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

// evaluateOn measures how well the model rebuilds t's rows.
func (a *Autoencoder) evaluateOn(t *Table) (*Evaluation, error) {
	f, err := modelFrame(t, a.Features)
	if err != nil {
		return nil, err
	}
	if f.N == 0 {
		return nil, inputErr("INPUT_DATA has no rows")
	}
	return a.evaluate(a.Encoder.Encode(f), identity(f.N)), nil
}
