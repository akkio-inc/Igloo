package models

import (
	"math"
	"slices"
	"sort"
	"time"

	"github.com/akkio-inc/igloo/internal/ml"
)

// AUTOML_CLASSIFIER and AUTOML_REGRESSOR: a search over the supervised
// algorithms and their hyperparameters, each candidate scored by k-fold
// cross-validation on (up to 100,000 of) the training rows, within a time
// budget. The best is trained again on every training row and kept; every
// trial is shown by TRIAL_INFO.

// searchRows bounds the rows candidates are cross-validated on.
const searchRows = 100_000

// candidate is an algorithm and options to try.
type candidate struct {
	algorithm string
	options   map[string]any
}

// candidates are tried in this order, the quick ones first, so that a
// short budget still compares each kind of model.
func candidates(classify bool) []candidate {
	linear := "linear"
	if classify {
		linear = "logistic"
	}
	return []candidate{
		{linear, map[string]any{}},
		{"boosted_trees", map[string]any{}},
		{"random_forest", map[string]any{}},
		{"dnn", map[string]any{"hidden_units": []any{32.0}, "learn_rate": 0.003}},
		{"boosted_trees", map[string]any{"learn_rate": 0.05, "num_leaves": 63.0, "colsample_bytree": 0.8, "subsample": 0.8}},
		{"boosted_trees", map[string]any{"learn_rate": 0.1, "num_leaves": 15.0, "min_data_in_leaf": 50.0}},
		{linear, map[string]any{"l2_reg": 10.0}},
		{"dnn", map[string]any{"hidden_units": []any{64.0, 32.0}}},
		{"random_forest", map[string]any{"num_parallel_tree": 200.0, "colsample_bynode": 0.5}},
		{"boosted_trees", map[string]any{"learn_rate": 0.03, "num_leaves": 127.0, "colsample_bytree": 0.7}},
		{"dnn", map[string]any{"hidden_units": []any{128.0, 64.0}, "dropout": 0.1}},
		{"boosted_trees", map[string]any{"learn_rate": 0.2, "num_leaves": 7.0}},
	}
}

// objectives are what a search minimizes, by optimization_objective.
var objectives = map[string]func(pred, y []float64, K int) float64{
	"minimize_log_loss": func(pred, y []float64, K int) float64 {
		actual := make([]int, len(y))
		for i, v := range y {
			actual[i] = int(v)
		}
		return ml.LogLoss(pred, actual)
	},
	"maximize_au_roc": func(pred, y []float64, K int) float64 {
		auc := classificationEval(pred, y, K).metric("roc_auc")
		if math.IsNaN(auc) {
			return math.Inf(1)
		}
		return -auc
	},
	"minimize_rmse": func(pred, y []float64, _ int) float64 { return math.Sqrt(ml.MSE(y, pred)) },
	"minimize_mae":  func(pred, y []float64, _ int) float64 { return ml.MAE(y, pred) },
	"minimize_rmsle": func(pred, y []float64, _ int) float64 {
		for _, m := range regressionMetrics(y, pred) {
			if m.Name == "mean_squared_log_error" {
				return math.Sqrt(m.Value)
			}
		}
		return math.NaN()
	},
}

// automlSettings are an AutoML search's options.
type automlSettings struct {
	budget           float64
	maxTrials, folds int
	objective        string
}

// automlOptions reads budget_hours, max_trials, cv_folds and
// optimization_objective.
func automlOptions(cfg *config, classify bool) (automlSettings, error) {
	var o automlSettings
	var err error
	if o.budget, err = cfg.num("budget_hours", 1, 1e-6, 72); err != nil {
		return o, err
	}
	n := len(candidates(classify))
	if o.maxTrials, err = cfg.integer("max_trials", n, 1, n); err != nil {
		return o, err
	}
	if o.folds, err = cfg.integer("cv_folds", 3, 2, 10); err != nil {
		return o, err
	}
	if classify {
		o.objective, err = cfg.str("optimization_objective", "minimize_log_loss", "minimize_log_loss", "maximize_au_roc")
	} else {
		o.objective, err = cfg.str("optimization_objective", "minimize_rmse", "minimize_rmse", "minimize_mae", "minimize_rmsle")
	}
	return o, err
}

func trainAutoML(class string, t *Table, a IglooArgs, cfg *config) (*Supervised, error) {
	classify := class == ClassAutoMLClassifier
	if a.Target == "" {
		return nil, inputErr("%s needs TARGET_COLNAME", class)
	}
	o, err := automlOptions(cfg, classify)
	if err != nil {
		return nil, err
	}
	cands := candidates(classify)
	budget, maxTrials, folds, objective := o.budget, o.maxTrials, o.folds, o.objective
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
	// The search's rows and their folds.
	search := train
	if len(search) > searchRows {
		step := (len(search) + searchRows - 1) / searchRows
		var picked []int
		for i := 0; i < len(search); i += step {
			picked = append(picked, search[i])
		}
		search = picked
	}
	sf, sy := subset(d.frame, d.y, search)
	fold := foldsOf(sy, folds, classify, seed)
	score := objectives[objective]
	deadline := time.Now().Add(time.Duration(budget * float64(time.Hour)))
	var trials []Trial
	best, bestLoss := -1, math.Inf(1)
	for i, c := range cands[:maxTrials] {
		if i > 0 && time.Now().After(deadline) {
			break
		}
		if a.Interrupt != nil {
			if err := a.Interrupt(); err != nil {
				return nil, err
			}
		}
		tr := Trial{ID: i + 1, Algorithm: c.algorithm, Hyper: c.options, Status: "SUCCEEDED"}
		v, err := crossValidate(c, d, tc, sf, sy, fold, folds, classify, score, seed, a.Interrupt)
		if a.Interrupt != nil {
			if ierr := a.Interrupt(); ierr != nil {
				return nil, ierr
			}
		}
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			tr.Status, v = "FAILED", math.Inf(1)
		}
		// Shown as the objective names it: an AUC, not its opposite.
		tr.Objective = finiteOr(v, 0)
		if objective == "maximize_au_roc" {
			tr.Objective = -tr.Objective
		}
		trials = append(trials, tr)
		if tr.Status == "SUCCEEDED" && (best < 0 || v < bestLoss) {
			best, bestLoss = len(trials)-1, v
		}
	}
	if best < 0 {
		return nil, inputErr("no trial of the search succeeded")
	}
	trials[best].Optimal = true
	c := cands[trials[best].ID-1]
	s := newSupervised(classify, c.algorithm, tc, d)
	if err := s.fit(d, train, eval, newConfig(c.options), seed, a.Interrupt); err != nil {
		return nil, err
	}
	s.Trials = trials
	if s.Hyper == nil {
		s.Hyper = map[string]any{}
	}
	s.Hyper["optimization_objective"] = objective
	return s, nil
}

// foldsOf assigns each of n rows a fold, at random by seed: within each
// class for a classifier, so that folds keep the classes' shares.
func foldsOf(y []float64, k int, classify bool, seed uint64) []int {
	n := len(y)
	order := identity(n)
	sort.SliceStable(order, func(a, b int) bool { return rank(seed, 0xf01d, order[a]) < rank(seed, 0xf01d, order[b]) })
	if classify {
		sort.SliceStable(order, func(a, b int) bool { return y[order[a]] < y[order[b]] })
	}
	out := make([]int, n)
	for pos, r := range order {
		out[r] = pos % k
	}
	return out
}

// crossValidate fits candidate c on all folds but one, in turn, and
// returns its mean objective on the folds left out.
func crossValidate(c candidate, d *labeled, tc *TableCol, f *ml.Frame, y []float64, fold []int, k int, classify bool,
	score func(pred, y []float64, K int) float64, seed uint64, interrupt func() error) (float64, error) {
	var total float64
	for held := range k {
		var tr, te []int
		for i, fo := range fold {
			if fo == held {
				te = append(te, i)
			} else {
				tr = append(tr, i)
			}
		}
		if len(te) == 0 || len(tr) == 0 {
			continue
		}
		tf, ty := subset(f, y, tr)
		ef, ey := subset(f, y, te)
		if classify && len(distinct(ty)) < len(d.classes) {
			// A fold without some class cannot train the same classifier.
			return math.NaN(), inputErr("a fold lacks a class")
		}
		s := newSupervised(classify, c.algorithm, tc, d)
		if err := s.fitAlgorithm(tf, ty, newConfig(c.options), seed, interrupt); err != nil {
			return math.NaN(), err
		}
		pred, err := s.predictFrame(ef)
		if err != nil {
			return math.NaN(), err
		}
		total += score(pred, ey, len(d.classes))
	}
	return total / float64(k), nil
}

func distinct(v []float64) []float64 {
	out := slices.Clone(v)
	slices.Sort(out)
	return slices.Compact(out)
}
