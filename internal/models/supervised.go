package models

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/akkio-inc/igloo/internal/ml"
)

// Supervised is a model of Igloo's that predicts a target from features:
// IGLOO.ML.REGRESSION and CLASSIFICATION (boosted trees, a random forest,
// or a linear or logistic regression), DNN_REGRESSOR and DNN_CLASSIFIER (a
// neural network), and the AUTOML classes' best model.
type Supervised struct {
	// Task is regression or classification.
	Task string `json:"task"`
	// Algorithm is boosted_trees, random_forest, linear, logistic or dnn.
	Algorithm  string    `json:"algorithm"`
	Target     string    `json:"target"`
	TargetType string    `json:"target_type"`
	TargetSQL  string    `json:"target_sql,omitempty"`
	Classes    []string  `json:"classes,omitempty"`
	Features   []Feature `json:"features"`
	// Trees is a tree model's; Linear a linear model's, one per class of
	// three or more (one against the rest); Net and Encoder a network's,
	// whose regression targets were standardized by YMean and YScale.
	Trees   *ml.GBM      `json:"trees,omitempty"`
	Linear  []*ml.Linear `json:"linear,omitempty"`
	Net     *ml.MLP      `json:"net,omitempty"`
	Encoder *ml.Encoder  `json:"encoder,omitempty"`
	YMean   float64      `json:"y_mean,omitempty"`
	YScale  float64      `json:"y_scale,omitempty"`
	// Attribution is each feature's mean absolute attribution, for
	// GLOBAL_EXPLAIN.
	Attribution []float64      `json:"attribution"`
	Eval        *Evaluation    `json:"eval,omitempty"`
	Training    []Iteration    `json:"training,omitempty"`
	Trials      []Trial        `json:"trials,omitempty"`
	Rows        int            `json:"rows"`
	Hyper       map[string]any `json:"hyperparameters,omitempty"`

	names     []string
	positions [][]int
	bias      []float64
	pool      sync.Pool
}

func (s *Supervised) classify() bool { return s.Task == "classification" }

func (s *Supervised) summary(out map[string]any) {
	out["rows"], out["features"], out["algorithm"] = s.Rows, len(s.Features), s.Algorithm
	addEval(out, s.Eval)
}

// outputs is the number of values a row's prediction has: its value, or
// each class's probability.
func (s *Supervised) outputs() int {
	if s.classify() {
		return len(s.Classes)
	}
	return 1
}

func (s *Supervised) prepare() {
	if s.Trees != nil {
		s.Trees.Prepare()
		s.bias = s.Trees.Bias()
	}
	if s.Encoder != nil {
		s.Encoder.Prepare()
	}
	s.names = make([]string, len(s.Features))
	for j, f := range s.Features {
		s.names[j] = f.Name
	}
	s.positions = nil
	for _, l := range s.Linear {
		s.positions = append(s.positions, l.Positions(s.names))
	}
}

// supervisedScratch holds a row's buffers.
type supervisedScratch struct {
	bins           []uint8
	raw, vec, pred []float64
	contrib        []float64
	net            *ml.MLPScratch
	attrs          []attribution
}

func (s *Supervised) scratch() *supervisedScratch {
	if v := s.pool.Get(); v != nil {
		return v.(*supervisedScratch)
	}
	sc := &supervisedScratch{raw: make([]float64, max(s.outputs(), 1)), pred: make([]float64, max(s.outputs(), 1))}
	if s.Trees != nil {
		sc.bins = make([]uint8, len(s.Trees.Features))
		sc.contrib = make([]float64, s.outputs()*(len(s.Trees.Features)+1))
	}
	if s.Net != nil {
		sc.vec = make([]float64, s.Encoder.Width)
		sc.net = s.Net.NewScratch()
	}
	if len(s.Linear) > 0 {
		sc.contrib = make([]float64, len(s.Linear[0].Terms)+1)
	}
	return sc
}

// Trial is a line of an AutoML model's TRIAL_INFO: an algorithm and
// hyperparameters it tried, and how they did.
type Trial struct {
	ID        int            `json:"id"`
	Algorithm string         `json:"algorithm"`
	Hyper     map[string]any `json:"hyperparameters"`
	// Status is SUCCEEDED or FAILED; Objective the cross-validated
	// objective (lower is better); Optimal marks the trial kept.
	Status    string  `json:"status"`
	Objective float64 `json:"objective"`
	Optimal   bool    `json:"optimal"`
}

// --- training ---------------------------------------------------------------

// labeledRows reads a training set: the rows whose target is not NULL,
// their features in canonical order, and their labels (class numbers for
// classification).
func labeledRows(t *Table, target string, classify bool) (*labeled, *TableCol, error) {
	tc := t.Col(target)
	if tc == nil {
		return nil, nil, inputErr("TARGET_COLNAME %s is not a column of INPUT_DATA", target)
	}
	if classify {
		d, err := prepareLabeled(t, target)
		return d, tc, err
	}
	if tc.Type != TypeNumber {
		return nil, nil, inputErr("the target %s is of type %s; a regression predicts numbers", tc.Name, tc.TypeName)
	}
	var rows []int
	for i := range t.N {
		if !tc.Null[i] {
			rows = append(rows, i)
		}
	}
	if len(rows) < 2 {
		return nil, nil, inputErr("the target %s needs at least two rows that are not NULL to train on", tc.Name)
	}
	names := otherNames(t, target)
	if len(names) == 0 {
		return nil, nil, inputErr("INPUT_DATA has no columns besides the target")
	}
	sub := t.Take(rows)
	frame, feats, err := featureFrame(sub, names)
	if err != nil {
		return nil, nil, err
	}
	y := sub.Col(target).Num
	order := frame.CanonicalOrder(y)
	ys := make([]float64, len(order))
	for k, o := range order {
		ys[k] = y[o]
	}
	return &labeled{frame: frame.Take(order), features: feats, y: ys}, tc, nil
}

func trainSupervised(class string, t *Table, a IglooArgs, cfg *config) (*Supervised, error) {
	classify := class == ClassIglooClassifier || class == ClassDNNClassifier
	if a.Target == "" {
		return nil, inputErr("%s needs TARGET_COLNAME", class)
	}
	algorithm := "dnn"
	var err error
	switch class {
	case ClassRegression:
		algorithm, err = cfg.str("algorithm", "boosted_trees", "boosted_trees", "random_forest", "linear")
	case ClassIglooClassifier:
		algorithm, err = cfg.str("algorithm", "boosted_trees", "boosted_trees", "random_forest", "logistic")
	}
	if err != nil {
		return nil, err
	}
	seed, err := cfg.seed()
	if err != nil {
		return nil, err
	}
	d, tc, err := labeledRows(t, a.Target, classify)
	if err != nil {
		return nil, err
	}
	train, eval, err := splitRows(cfg, d.frame.N, d.y, classify, seed)
	if err != nil {
		return nil, err
	}
	s := newSupervised(classify, algorithm, tc, d)
	if err := s.fit(d, train, eval, cfg, seed, a.Interrupt); err != nil {
		return nil, err
	}
	return s, nil
}

func newSupervised(classify bool, algorithm string, tc *TableCol, d *labeled) *Supervised {
	s := &Supervised{Task: "regression", Algorithm: algorithm, Target: tc.Name, TargetType: tc.Type.String(), TargetSQL: tc.SQLType,
		Features: d.features, Classes: d.classes}
	if classify {
		s.Task = "classification"
	}
	return s
}

// fit trains s's algorithm on rows train of d, measures it on rows eval,
// and explains it.
func (s *Supervised) fit(d *labeled, train, eval []int, cfg *config, seed uint64, interrupt func() error) error {
	tf, ty := subset(d.frame, d.y, train)
	if err := s.fitAlgorithm(tf, ty, cfg, seed, interrupt); err != nil {
		return err
	}
	var err error
	if len(eval) > 0 {
		ef, ey := subset(d.frame, d.y, eval)
		if s.Eval, err = s.evaluate(ef, ey); err != nil {
			return err
		}
	}
	return s.explain(tf, ty, seed)
}

// fitAlgorithm trains s's algorithm on rows f with labels y, and readies
// it to predict.
func (s *Supervised) fitAlgorithm(f *ml.Frame, y []float64, cfg *config, seed uint64, interrupt func() error) error {
	s.Rows = f.N
	var err error
	switch s.Algorithm {
	case "boosted_trees", "random_forest":
		err = s.fitTrees(f, y, cfg, seed, interrupt)
	case "linear", "logistic":
		err = s.fitLinear(f, y, cfg)
	case "dnn":
		err = s.fitNet(f, y, cfg, seed, interrupt)
	default:
		return inputErr("unknown algorithm %s", s.Algorithm)
	}
	if err != nil {
		return err
	}
	s.prepare()
	return nil
}

// fitTrees trains boosted trees or a random forest, with BigQuery's
// options for them.
func (s *Supervised) fitTrees(f *ml.Frame, y []float64, cfg *config, seed uint64, interrupt func() error) error {
	forest := s.Algorithm == "random_forest"
	p, err := treeParams(cfg, forest, seed)
	if err != nil {
		return err
	}
	p.Interrupt = interrupt
	p.Objective = ml.Regression
	if s.classify() {
		p.Objective = ml.Binary
		if K := len(s.Classes); K > 2 {
			p.Objective, p.Classes = ml.Multiclass, K
		}
	}
	s.Hyper = treeHyper(p)
	if forest {
		m, _, err := ml.TrainGBM(f, y, nil, nil, p)
		if err != nil {
			return err
		}
		s.Trees = m
		return nil
	}
	p.TrackLoss = true
	m, info, err := trainBoostedInfo(f, y, p, s.classify())
	if err != nil {
		return err
	}
	s.Trees = m
	s.Hyper["max_iterations"] = info.Rounds
	for r := range min(len(info.TrainLoss), info.Rounds) {
		it := Iteration{Iteration: r + 1, Loss: finiteOr(info.TrainLoss[r], 0), LearningRate: p.LearningRate}
		if r < len(info.ValidLoss) {
			it.EvalLoss = finiteOr(info.ValidLoss[r], 0)
		}
		s.Training = append(s.Training, it)
	}
	return nil
}

// treeParams reads BigQuery's options of boosted trees and random forests.
func treeParams(cfg *config, forest bool, seed uint64) (ml.Params, error) {
	p := gbmParams(seed)
	defBag, defNode, defDepth := 1.0, 1.0, 12
	rounds := "max_iterations"
	if forest {
		p = ml.Params{Forest: true, Rounds: 100, NumLeaves: 127, MinDataInLeaf: 2, Lambda: 1e-6, Seed: seed}
		defBag, defNode = 0.8, 0.8
		rounds = "num_parallel_tree"
	} else if n, err := cfg.integer("num_parallel_tree", 1, 1, 1_000); err != nil || n != 1 {
		return p, inputErr("num_parallel_tree is a random forest's number of trees: boosted trees grow one at a time")
	}
	var err error
	if p.Rounds, err = cfg.integer(rounds, p.Rounds, 1, 10_000); err != nil {
		return p, err
	}
	if !forest {
		if p.LearningRate, err = cfg.num("learn_rate", p.LearningRate, 1e-6, 1); err != nil {
			return p, err
		}
		stop, err := cfg.boolean("early_stop", true)
		if err != nil {
			return p, err
		}
		if !stop {
			p.EarlyStopping = 0
		}
		if _, err := cfg.num("min_rel_progress", 0, 0, 1); err != nil {
			return p, err
		}
	}
	if p.MaxDepth, err = cfg.integer("max_tree_depth", defDepth, 1, 64); err != nil {
		return p, err
	}
	if p.NumLeaves, err = cfg.integer("num_leaves", p.NumLeaves, 2, 65_536); err != nil {
		return p, err
	}
	if p.BaggingFraction, err = cfg.num("subsample", defBag, 0.01, 1); err != nil {
		return p, err
	}
	if p.FeatureFraction, err = cfg.num("colsample_bytree", 1, 0.01, 1); err != nil {
		return p, err
	}
	if p.FeatureFractionByNode, err = cfg.num("colsample_bynode", defNode, 0.01, 1); err != nil {
		return p, err
	}
	l2, err := cfg.num("l2_reg", -1, 0, 1e9)
	if err != nil {
		return p, err
	}
	if l2 >= 0 {
		p.Lambda = math.Max(l2, 1e-9)
	}
	if p.MinHessian, err = cfg.num("min_tree_child_weight", 0, 0, 1e9); err != nil {
		return p, err
	}
	if p.MinDataInLeaf, err = cfg.integer("min_data_in_leaf", p.MinDataInLeaf, 0, 1_000_000); err != nil {
		return p, err
	}
	if l1, err := cfg.num("l1_reg", 0, 0, 1e9); err != nil || l1 != 0 {
		return p, inputErr("l1_reg is not supported: Igloo's trees take l2_reg")
	}
	for key, allowed := range map[string][]string{"booster_type": {"gbtree"}, "tree_method": {"auto", "exact", "approx", "hist"}} {
		if err := cfg.accept(key, allowed...); err != nil {
			return p, err
		}
	}
	if _, err := cfg.boolean("enable_global_explain", true); err != nil {
		return p, err
	}
	return p, nil
}

// treeHyper records a tree model's hyperparameters.
func treeHyper(p ml.Params) map[string]any {
	h := map[string]any{"num_leaves": p.NumLeaves, "max_tree_depth": p.MaxDepth, "subsample": p.BaggingFraction,
		"colsample_bytree": p.FeatureFraction, "colsample_bynode": p.FeatureFractionByNode, "l2_reg": p.Lambda}
	if p.Forest {
		h["num_parallel_tree"] = p.Rounds
	} else {
		h["learn_rate"] = p.LearningRate
	}
	return h
}

// trainBoostedInfo is trainBoosted, with its training information.
func trainBoostedInfo(f *ml.Frame, y []float64, p ml.Params, classify bool) (*ml.GBM, ml.TrainInfo, error) {
	if f.N < 50 {
		p.EarlyStopping = 0
		p.Rounds = min(p.Rounds, 100)
		return ml.TrainGBM(f, y, nil, nil, p)
	}
	var keep, held []int
	if classify {
		keep, held = stratified(y, 0.1, p.Seed, 0x7a1)
	} else {
		for i := range f.N {
			if rank(p.Seed, 0x7a1, i)%10 == 0 {
				held = append(held, i)
			} else {
				keep = append(keep, i)
			}
		}
	}
	tf, ty := subset(f, y, keep)
	vf, vy := subset(f, y, held)
	return ml.TrainGBM(tf, ty, vf, vy, p)
}

// maxLinearLevels bounds the indicators a categorical feature gets in a
// linear model.
const maxLinearLevels = 100

// fitLinear fits a linear or logistic regression (one against the rest
// for three classes or more).
// linearOptions reads a linear or logistic regression's options: l2_reg,
// and BigQuery's others that IRLS needs none of.
func linearOptions(cfg *config) (float64, error) {
	l2, err := cfg.num("l2_reg", 0, 0, 1e9)
	if err != nil {
		return 0, err
	}
	if l1, err := cfg.num("l1_reg", 0, 0, 1e9); err != nil || l1 != 0 {
		return 0, inputErr("l1_reg is not supported: Igloo's linear models take l2_reg")
	}
	if _, err := cfg.integer("max_iterations", 0, 1, 10_000); err != nil {
		return 0, err
	}
	if _, err := cfg.num("learn_rate", 0, 0, 1e9); err != nil {
		return 0, err
	}
	for key, allowed := range map[string][]string{"optimize_strategy": {"auto_strategy", "normal_equation", "batch_gradient_descent"},
		"category_encoding_method": {"dummy_encoding", "one_hot_encoding"}, "learn_rate_strategy": {"line_search", "constant"}} {
		if err := cfg.accept(key, allowed...); err != nil {
			return 0, err
		}
	}
	for _, key := range []string{"calculate_p_values", "enable_global_explain", "early_stop"} {
		if _, err := cfg.boolean(key, true); err != nil {
			return 0, err
		}
	}
	if _, err := cfg.num("min_rel_progress", 0, 0, 1); err != nil {
		return 0, err
	}
	return l2, nil
}

// algorithmOptions reads a supervised algorithm's options.
func algorithmOptions(algorithm string, cfg *config) error {
	var err error
	switch algorithm {
	case "boosted_trees", "random_forest":
		_, err = treeParams(cfg, algorithm == "random_forest", 0)
	case "linear", "logistic":
		_, err = linearOptions(cfg)
	case "dnn":
		_, err = readNetOptions(cfg, []int{64, 32})
	}
	return err
}

func (s *Supervised) fitLinear(f *ml.Frame, y []float64, cfg *config) error {
	l2, err := linearOptions(cfg)
	if err != nil {
		return err
	}
	s.Hyper = map[string]any{"l2_reg": l2}
	switch {
	case !s.classify():
		l, err := ml.FitLinearLevels(f, y, false, l2, maxLinearLevels)
		if err != nil {
			return inputErr("%v", err)
		}
		s.Linear = []*ml.Linear{l}
	case len(s.Classes) == 2:
		l, err := ml.FitLinearLevels(f, y, true, l2, maxLinearLevels)
		if err != nil {
			return inputErr("%v", err)
		}
		s.Linear = []*ml.Linear{l}
	default:
		for k := range s.Classes {
			yk := make([]float64, len(y))
			for i, v := range y {
				if int(v) == k {
					yk[i] = 1
				}
			}
			l, err := ml.FitLinearLevels(f, yk, true, l2, maxLinearLevels)
			if err != nil {
				return inputErr("%v", err)
			}
			s.Linear = append(s.Linear, l)
		}
	}
	return nil
}

// predictFrame predicts a frame's rows: each row's value, or class k's
// probability of row i at k*n+i.
func (s *Supervised) predictFrame(f *ml.Frame) ([]float64, error) {
	n := f.N
	switch {
	case s.Trees != nil:
		p, err := s.Trees.Predict(f)
		if err != nil {
			return nil, err
		}
		if s.classify() && len(s.Classes) == 2 {
			out := make([]float64, 2*n)
			for i := range n {
				out[i], out[n+i] = 1-p[i], p[i]
			}
			return out, nil
		}
		return p, nil
	case len(s.Linear) > 0:
		if !s.classify() {
			return s.Linear[0].Predict(f), nil
		}
		if len(s.Classes) == 2 {
			p := s.Linear[0].Predict(f)
			out := make([]float64, 2*n)
			for i := range n {
				out[i], out[n+i] = 1-p[i], p[i]
			}
			return out, nil
		}
		K := len(s.Classes)
		out := make([]float64, K*n)
		for k, l := range s.Linear {
			copy(out[k*n:(k+1)*n], l.Predict(f))
		}
		normalizeClasses(out, n, K)
		return out, nil
	case s.Net != nil:
		X := s.Encoder.Encode(f)
		O := s.outputs()
		out := make([]float64, O*n)
		w := s.Encoder.Width
		parallelRows(n, func(lo, hi int) {
			sc := s.Net.NewScratch()
			for i := lo; i < hi; i++ {
				o := s.Net.Forward(X[i*w:(i+1)*w], s.Net.Layers(), sc)
				s.netOutputs(o, out, n, i)
			}
		})
		return out, nil
	}
	return nil, inputErr("the model holds no predictor")
}

// netOutputs turns a network's outputs for row i into predictions.
func (s *Supervised) netOutputs(o, out []float64, n, i int) {
	switch {
	case !s.classify():
		out[i] = o[0]*s.YScale + s.YMean
	case len(s.Classes) == 2:
		out[i], out[n+i] = 1-o[0], o[0]
	default:
		for k, v := range o {
			out[k*n+i] = v
		}
	}
}

// normalizeClasses makes each row's class probabilities add up to 1.
func normalizeClasses(p []float64, n, K int) {
	for i := range n {
		var sum float64
		for k := range K {
			sum += p[k*n+i]
		}
		for k := range K {
			if sum > 0 {
				p[k*n+i] /= sum
			} else {
				p[k*n+i] = 1 / float64(K)
			}
		}
	}
}

// parallelRows calls fn on fixed chunks of [0, n) in parallel.
func parallelRows(n int, fn func(lo, hi int)) {
	const chunk = 16384
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for lo := 0; lo < n; lo += chunk {
		wg.Add(1)
		sem <- struct{}{}
		go func(lo int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(lo, min(lo+chunk, n))
		}(lo)
	}
	wg.Wait()
}

// predictRow predicts one row into out: its value, or each class's
// probability.
func (s *Supervised) predictRow(r *ml.RowValues, sc *supervisedScratch, out []float64) {
	switch {
	case s.Trees != nil:
		s.Trees.BinRow(r, sc.bins)
		s.Trees.RawRow(sc.bins, sc.raw)
		s.Trees.TransformRow(sc.raw)
		if s.classify() && len(s.Classes) == 2 {
			out[0], out[1] = 1-sc.raw[0], sc.raw[0]
			return
		}
		copy(out, sc.raw)
	case len(s.Linear) > 0:
		if !s.classify() {
			out[0] = s.Linear[0].RowContributions(r, s.positions[0], sc.contrib)
			return
		}
		if len(s.Classes) == 2 {
			p := ml.Sigmoid(s.Linear[0].RowContributions(r, s.positions[0], sc.contrib))
			out[0], out[1] = 1-p, p
			return
		}
		for k, l := range s.Linear {
			out[k] = ml.Sigmoid(l.RowContributions(r, s.positions[k], sc.contrib))
		}
		normalizeClasses(out, 1, len(out))
	case s.Net != nil:
		s.Encoder.EncodeRow(r, sc.vec)
		o := s.Net.Forward(sc.vec, s.Net.Layers(), sc.net)
		s.netOutputs(o, out, 1, 0)
	}
}

// --- evaluation ------------------------------------------------------------

// evaluate measures the model on rows f with labels y, as BigQuery's
// ML.EVALUATE does.
func (s *Supervised) evaluate(f *ml.Frame, y []float64) (*Evaluation, error) {
	pred, err := s.predictFrame(f)
	if err != nil {
		return nil, err
	}
	if !s.classify() {
		return &Evaluation{Rows: f.N, Metrics: finiteMetrics(regressionMetrics(y, pred))}, nil
	}
	e := classificationEval(pred, y, len(s.Classes))
	e.Metrics = finiteMetrics(e.Metrics)
	return e, nil
}

// finiteMetrics leaves out metrics that are not defined (NaN), which
// EVALUATE shows as NULL.
func finiteMetrics(ms []Metric) []Metric {
	out := ms[:0]
	for _, m := range ms {
		if !math.IsNaN(m.Value) && !math.IsInf(m.Value, 0) {
			out = append(out, m)
		}
	}
	return out
}

// regressionMetrics are ML.EVALUATE's for a regression.
func regressionMetrics(y, pred []float64) []Metric {
	n := len(y)
	abs := make([]float64, n)
	var msle float64
	logOK := true
	for i := range n {
		abs[i] = math.Abs(y[i] - pred[i])
		if y[i] < 0 || pred[i] <= -1 {
			logOK = false
		} else {
			d := math.Log1p(y[i]) - math.Log1p(math.Max(pred[i], 0))
			msle += d * d
		}
	}
	msle /= float64(max(n, 1))
	if !logOK {
		msle = math.NaN()
	}
	varY := variance(y)
	resid := make([]float64, n)
	for i := range n {
		resid[i] = y[i] - pred[i]
	}
	mse := ml.MSE(y, pred)
	r2, ev := math.NaN(), math.NaN()
	if varY > 0 {
		r2 = 1 - mse/varY
		ev = 1 - variance(resid)/varY
	}
	return []Metric{
		{"mean_absolute_error", ml.MAE(y, pred)},
		{"mean_squared_error", mse},
		{"mean_squared_log_error", msle},
		{"median_absolute_error", median(abs)},
		{"r2_score", r2},
		{"explained_variance", ev},
	}
}

func variance(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var m float64
	for _, x := range v {
		m += x
	}
	m /= float64(len(v))
	var s float64
	for _, x := range v {
		s += (x - m) * (x - m)
	}
	return s / float64(len(v))
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	w := slices.Clone(v)
	sort.Float64s(w)
	if len(w)%2 == 1 {
		return w[len(w)/2]
	}
	return (w[len(w)/2-1] + w[len(w)/2]) / 2
}

// classificationEval is ML.EVALUATE's for a classifier: the positive
// class's precision, recall and F1 at a threshold of 0.5 for two classes,
// their macro averages for more; accuracy, log loss and ROC AUC; and the
// confusion matrix and ROC curve.
func classificationEval(probs []float64, y []float64, K int) *Evaluation {
	n := len(y)
	actual, predicted := make([]int, n), make([]int, n)
	for i := range n {
		best := 0
		for k := 1; k < K; k++ {
			if probs[k*n+i] > probs[best*n+i] {
				best = k
			}
		}
		actual[i], predicted[i] = int(y[i]), best
	}
	stats, confusion := ml.ClassReport(actual, predicted, K)
	correct := 0
	for i := range n {
		if actual[i] == predicted[i] {
			correct++
		}
	}
	e := &Evaluation{Rows: n, Confusion: confusion}
	var precision, recall, f1, auc float64
	if K == 2 {
		precision, recall, f1 = stats[1].Precision, stats[1].Recall, stats[1].F1
		pos := make([]bool, n)
		for i := range n {
			pos[i] = actual[i] == 1
		}
		auc = ml.AUC(probs[n:2*n], pos)
		for _, th := range ml.Thresholds(probs[n:2*n], pos) {
			e.ROC = append(e.ROC, ROCPoint{Threshold: th.Threshold, Recall: th.Recall, FPR: th.FPR, TP: th.TP, FP: th.FP, TN: th.TN, FN: th.FN})
		}
	} else {
		var aucs []float64
		for k, st := range stats {
			precision += st.Precision / float64(K)
			recall += st.Recall / float64(K)
			f1 += st.F1 / float64(K)
			pos := make([]bool, n)
			for i := range n {
				pos[i] = actual[i] == k
			}
			if a := ml.AUC(probs[k*n:(k+1)*n], pos); !math.IsNaN(a) {
				aucs = append(aucs, a)
			}
		}
		auc = math.NaN()
		if len(aucs) > 0 {
			auc = 0
			for _, a := range aucs {
				auc += a / float64(len(aucs))
			}
		}
	}
	e.Metrics = []Metric{
		{"precision", precision},
		{"recall", recall},
		{"accuracy", float64(correct) / float64(max(n, 1))},
		{"f1_score", f1},
		{"log_loss", ml.LogLoss(probs, actual)},
		{"roc_auc", auc},
	}
	return e
}

// --- explanation ------------------------------------------------------------

// explain sets each feature's mean absolute attribution: SHAP values for
// trees, coefficients times distances from the means for a linear model,
// and the loss a shuffled feature costs for a network.
func (s *Supervised) explain(f *ml.Frame, y []float64, seed uint64) error {
	F := len(s.Features)
	s.Attribution = make([]float64, F)
	switch {
	case s.Trees != nil:
		// Means of SHAP values settle on a sample; a forest's trees are many
		// and deep, and take longer.
		limit := 2_000
		if s.Trees.Forest {
			limit = 500
		}
		imp, err := s.Trees.MeanAbsContributions(f, limit)
		if err != nil {
			return err
		}
		copy(s.Attribution, imp)
	case len(s.Linear) > 0:
		rows := sampleRows(f.N, 10_000)
		sub := f.Take(rows)
		for _, l := range s.Linear {
			T := len(l.Terms)
			c := l.Contributions(sub)
			pos := l.Positions(s.names)
			for i := range sub.N {
				perFeature := make([]float64, F)
				for k := range T {
					if j := pos[k]; j >= 0 {
						perFeature[j] += c[i*(T+1)+k]
					}
				}
				for j, v := range perFeature {
					s.Attribution[j] += math.Abs(v) / float64(sub.N)
				}
			}
		}
	case s.Net != nil:
		return s.permutationImportance(f, y, seed)
	}
	return nil
}

// permutationImportance sets each feature's attribution to how much the
// model's loss grows when the feature's values are shuffled among rows.
func (s *Supervised) permutationImportance(f *ml.Frame, y []float64, seed uint64) error {
	rows := sampleRows(f.N, 5_000)
	sub := f.Take(rows)
	sy := make([]float64, len(rows))
	for k, r := range rows {
		sy[k] = y[r]
	}
	base, err := s.loss(sub, sy)
	if err != nil {
		return err
	}
	for j := range s.Features {
		shuffled := sub.Take(identity(sub.N))
		c := &shuffled.Cols[j]
		perm := make([]int, sub.N)
		for i := range perm {
			perm[i] = i
		}
		sort.SliceStable(perm, func(a, b int) bool {
			return rank(seed, uint64(j)+0x9e0, perm[a]) < rank(seed, uint64(j)+0x9e0, perm[b])
		})
		if c.Kind == ml.Numeric {
			orig := slices.Clone(c.Num)
			for i, p := range perm {
				c.Num[i] = orig[p]
			}
		} else {
			orig := slices.Clone(c.Codes)
			for i, p := range perm {
				c.Codes[i] = orig[p]
			}
		}
		l, err := s.loss(shuffled, sy)
		if err != nil {
			return err
		}
		s.Attribution[j] = math.Max(l-base, 0)
	}
	return nil
}

// loss is the model's mean squared error or log loss on rows f.
func (s *Supervised) loss(f *ml.Frame, y []float64) (float64, error) {
	pred, err := s.predictFrame(f)
	if err != nil {
		return 0, err
	}
	if !s.classify() {
		return ml.MSE(y, pred), nil
	}
	actual := make([]int, len(y))
	for i, v := range y {
		actual[i] = int(v)
	}
	return ml.LogLoss(pred, actual), nil
}

func identity(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// sampleRows picks up to limit of n rows, evenly.
func sampleRows(n, limit int) []int {
	step := 1
	if n > limit {
		step = (n + limit - 1) / limit
	}
	var out []int
	for i := 0; i < n; i += step {
		out = append(out, i)
	}
	return out
}

// --- row methods -------------------------------------------------------------

func (s *Supervised) rowFeatures(method string) ([]Feature, bool) {
	switch method {
	case "PREDICT":
		return s.Features, true
	case "EXPLAIN_PREDICT":
		return s.Features, s.Net == nil
	}
	return nil, false
}

func (s *Supervised) rowOptions(method string, cfg *config) (*RowOptions, error) {
	opt := &RowOptions{}
	if method == "EXPLAIN_PREDICT" {
		k, err := cfg.integer("top_k_features", 5, 1, 10_000)
		if err != nil {
			return nil, err
		}
		opt.TopK = k
	}
	return opt, nil
}

func (s *Supervised) predictedColumn() RowColumn {
	name := sqlName("predicted_", s.Target)
	if !s.classify() {
		return RowColumn{name, "$.value", "DOUBLE"}
	}
	typ := s.TargetSQL
	if typ == "" {
		typ = "VARCHAR"
	}
	return RowColumn{name, "$.class", typ}
}

func (s *Supervised) rowTable(method string, _ *RowOptions) RowTable {
	cols := []RowColumn{s.predictedColumn()}
	if method == "PREDICT" {
		if s.classify() {
			cols = append(cols, RowColumn{sqlName("predicted_", s.Target, "_probs"), "$.probability", "JSON"})
		}
		return RowTable{Columns: cols}
	}
	if s.classify() {
		cols = append(cols, RowColumn{"probability", "$.class_probability", "DOUBLE"})
	}
	return RowTable{Columns: append(cols,
		RowColumn{"top_feature_attributions", "$.top_feature_attributions", "JSON"},
		RowColumn{"baseline_prediction_value", "$.baseline_prediction_value", "DOUBLE"},
		RowColumn{"prediction_value", "$.prediction_value", "DOUBLE"},
		RowColumn{"approximation_error", "$.approximation_error", "DOUBLE"})}
}

func (s *Supervised) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) []byte {
	sc := s.scratch()
	defer s.pool.Put(sc)
	out := sc.pred
	s.predictRow(r, sc, out)
	buf = append(buf, '{')
	best := 0
	if !s.classify() {
		buf = appendKey(buf, "value", true)
		buf = appendNum(buf, out[0])
	} else {
		for k := range out {
			if out[k] > out[best] {
				best = k
			}
		}
		buf = appendKey(buf, "class", true)
		buf = append(buf, jsonString(s.Classes[best])...)
		if method == "PREDICT" {
			buf = appendKey(buf, "probability", false)
			buf = append(buf, '{')
			for k, l := range s.Classes {
				buf = appendKey(buf, l, k == 0)
				buf = appendNum(buf, out[k])
			}
			buf = append(buf, '}')
		} else {
			buf = appendKey(buf, "class_probability", false)
			buf = appendNum(buf, out[best])
		}
	}
	if method == "EXPLAIN_PREDICT" {
		base, value, attrs := s.attributions(r, sc, best)
		buf = appendKey(buf, "top_feature_attributions", false)
		buf = appendAttributions(buf, attrs, opt.TopK)
		buf = appendKey(buf, "baseline_prediction_value", false)
		buf = appendNum(buf, base)
		buf = appendKey(buf, "prediction_value", false)
		buf = appendNum(buf, value)
		buf = appendKey(buf, "approximation_error", false)
		buf = append(buf, '0')
	}
	return append(buf, '}')
}

// attributions explains a row's prediction of class k (or its value): the
// baseline, the raw score it adds up to, and each feature's part.
func (s *Supervised) attributions(r *ml.RowValues, sc *supervisedScratch, k int) (float64, float64, []attribution) {
	F := len(s.Features)
	attrs := sc.attrs[:0]
	switch {
	case s.Trees != nil:
		s.Trees.BinRow(r, sc.bins)
		phi := s.Trees.Contributions(sc.bins, s.bias)
		out, sign := 0, 1.0
		if s.Trees.Objective == ml.Multiclass {
			out = k
		} else if s.classify() && k == 0 {
			// Two classes have one score, class 1's; class 0's is its
			// opposite for boosted trees, its complement for a forest.
			sign = -1
		}
		row := phi[out*(F+1) : (out+1)*(F+1)]
		base, value := row[F], row[F]
		for j := range F {
			value += row[j]
		}
		if sign < 0 {
			if s.Trees.Forest {
				base, value = 1-base, 1-value
			} else {
				base, value = -base, -value
			}
		}
		for j := range F {
			attrs = append(attrs, attribution{s.Features[j].Name, sign * row[j]})
		}
		sc.attrs = attrs
		return base, value, attrs
	default:
		li, sign := 0, 1.0
		if len(s.Linear) > 1 {
			li = k
		} else if s.classify() && k == 0 {
			sign = -1
		}
		l := s.Linear[li]
		contrib := make([]float64, len(l.Terms)+1)
		value := l.RowContributions(r, s.positions[li], contrib)
		perFeature := make([]float64, F)
		for t := range l.Terms {
			if j := s.positions[li][t]; j >= 0 {
				perFeature[j] += contrib[t]
			}
		}
		for j := range F {
			attrs = append(attrs, attribution{s.Features[j].Name, sign * perFeature[j]})
		}
		sc.attrs = attrs
		return sign * contrib[len(l.Terms)], sign * value, attrs
	}
}

// --- table methods ------------------------------------------------------------

// method computes one of the model's methods that return a table.
func (s *Supervised) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	add := func(row ...any) { res.Rows = append(res.Rows, row) }
	switch method {
	case "EVALUATE":
		if s.Eval == nil {
			return nil, errNoEval()
		}
		res.Rows = append(res.Rows, evaluationRow(s.Eval, cols))
	case "CONFUSION_MATRIX":
		if s.Eval == nil {
			return nil, errNoEval()
		}
		for a := range s.Eval.Confusion {
			for p, n := range s.Eval.Confusion[a] {
				add(s.Classes[a], s.Classes[p], int64(n))
			}
		}
	case "ROC_CURVE":
		if s.Eval == nil {
			return nil, errNoEval()
		}
		if len(s.Classes) != 2 {
			return nil, inputErr("ROC_CURVE is a binary classifier's; this model has %d classes", len(s.Classes))
		}
		for _, p := range s.Eval.ROC {
			add(p.Threshold, p.Recall, p.FPR, int64(p.TP), int64(p.FP), int64(p.TN), int64(p.FN))
		}
	case "GLOBAL_EXPLAIN":
		order := identity(len(s.Features))
		sort.SliceStable(order, func(a, b int) bool { return s.Attribution[order[a]] > s.Attribution[order[b]] })
		for _, j := range order {
			add(s.Features[j].Name, s.Attribution[j])
		}
	case "FEATURE_IMPORTANCE":
		if s.Trees == nil {
			return nil, inputErr("FEATURE_IMPORTANCE is a tree model's; this one is %s: see GLOBAL_EXPLAIN", s.Algorithm)
		}
		F := len(s.Features)
		weight, gain, cover := make([]float64, F), make([]float64, F), make([]float64, F)
		for _, t := range s.Trees.Trees {
			for _, n := range t.Nodes {
				if !n.IsLeaf() {
					weight[n.Feature]++
					gain[n.Feature] += n.Gain
					cover[n.Feature] += n.Cover
				}
			}
		}
		order := identity(F)
		sort.SliceStable(order, func(a, b int) bool { return gain[order[a]] > gain[order[b]] })
		for _, j := range order {
			c := 0.0
			if weight[j] > 0 {
				c = cover[j] / weight[j]
			}
			add(s.Features[j].Name, int64(weight[j]), gain[j], c)
		}
	case "WEIGHTS":
		if len(s.Linear) == 0 {
			return nil, inputErr("WEIGHTS are a linear model's; this one is %s: see GLOBAL_EXPLAIN", s.Algorithm)
		}
		for li, l := range s.Linear {
			var class any
			if len(s.Linear) > 1 {
				class = s.Classes[li]
			}
			add(class, "__INTERCEPT__", nil, l.Intercept, nil, nil)
			for k, t := range l.Terms {
				var cat any
				if t.Cat {
					cat = t.Level
				}
				var se, p any
				if k < len(l.StdErr) && l.StdErr[k] > 0 {
					se = l.StdErr[k]
					p = 2 * (1 - ml.NormalCDF(math.Abs(l.Coef[k]/l.StdErr[k])))
				}
				add(class, t.Feature, cat, l.Coef[k], se, p)
			}
		}
	case "TRAINING_INFO":
		for _, it := range s.Training {
			add(int64(it.Iteration), it.Loss, it.EvalLoss, it.LearningRate)
		}
		if len(s.Training) == 0 {
			add(int64(1), nil, nil, nil)
		}
	case "TRIAL_INFO":
		if len(s.Trials) == 0 {
			return nil, inputErr("TRIAL_INFO is an AutoML model's")
		}
		for _, t := range s.Trials {
			var objective any
			if t.Status == "SUCCEEDED" {
				objective = t.Objective
			}
			add(int64(t.ID), t.Algorithm, jsonText(t.Hyper), t.Status, objective, t.Optimal)
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

func errNoEval() error {
	return inputErr("the model held no rows out of training to evaluate on ('data_split_method': 'no_split', or fewer " +
		"than 500 rows): call EVALUATE with INPUT_DATA, or create it again with 'data_split_method': 'random'")
}

// evaluationRow lays out an evaluation's metrics as EVALUATE's columns.
func evaluationRow(e *Evaluation, cols []ResultColumn) []any {
	row := make([]any, len(cols))
	for i, c := range cols {
		row[i] = finiteNull(e.metric(lower(c.Name)))
	}
	return row
}

func finiteNull(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return v
}

// evaluateOn measures the model on a table holding its features and
// target.
func (s *Supervised) evaluateOn(t *Table) (*Evaluation, error) {
	tc := t.Col(s.Target)
	if tc == nil {
		return nil, inputErr("INPUT_DATA has no column %s, the target", s.Target)
	}
	var rows []int
	var y []float64
	index := map[string]int{}
	for k, c := range s.Classes {
		index[c] = k
	}
	for i := range t.N {
		if tc.Null[i] {
			continue
		}
		if s.classify() {
			k, ok := index[labelText(tc, i)]
			if !ok {
				continue // a class the model never saw
			}
			y = append(y, float64(k))
		} else {
			if tc.Type != TypeNumber {
				return nil, inputErr("the target %s is not a number", tc.Name)
			}
			y = append(y, tc.Num[i])
		}
		rows = append(rows, i)
	}
	if len(rows) == 0 {
		return nil, inputErr("INPUT_DATA has no rows with a target the model knows")
	}
	f, err := s.frameOf(t.Take(rows))
	if err != nil {
		return nil, err
	}
	return s.evaluate(f, y)
}

// frameOf reads a table's columns as the model's features: missing ones
// are missing in every row.
func (s *Supervised) frameOf(t *Table) (*ml.Frame, error) {
	return modelFrame(t, s.Features)
}

// modelFrame reads a table as features feats, matching names
// case-insensitively; a feature the table lacks is missing in every row.
func modelFrame(t *Table, feats []Feature) (*ml.Frame, error) {
	f := &ml.Frame{N: t.N}
	for _, ft := range feats {
		c := t.Col(ft.Name)
		kind := featureKind(ft)
		switch {
		case c == nil && kind == ml.Numeric:
			v := make([]float64, t.N)
			for i := range v {
				v[i] = math.NaN()
			}
			f.Cols = append(f.Cols, ml.NumericColumn(ft.Name, v))
		case c == nil:
			miss := make([]bool, t.N)
			for i := range miss {
				miss[i] = true
			}
			f.Cols = append(f.Cols, ml.CategoricalColumn(ft.Name, make([]string, t.N), miss))
		default:
			col, err := c.Feature()
			if err != nil {
				return nil, inputErr("%v", err)
			}
			if col.Kind != kind {
				col = recast(c, kind)
			}
			col.Name = ft.Name
			f.Cols = append(f.Cols, col)
		}
	}
	return f, nil
}

// recast reads a column as a feature of another kind than its type's: a
// category's text as a number, or a number as a category.
func recast(c *TableCol, kind ml.Kind) ml.Column {
	if kind == ml.Numeric {
		v := make([]float64, len(c.Null))
		for i := range v {
			v[i] = math.NaN()
			if !c.Null[i] && c.Str != nil {
				if f, err := strconv.ParseFloat(c.Str[i], 64); err == nil {
					v[i] = f
				}
			}
		}
		return ml.NumericColumn(c.Name, v)
	}
	s := make([]string, len(c.Null))
	for i := range s {
		if !c.Null[i] {
			s[i] = labelText(c, i)
		}
	}
	return ml.CategoricalColumn(c.Name, s, c.Null)
}
