package models

import (
	"math"
	"slices"
	"sort"
	"strings"
)

// Igloo's own classes, IGLOO.ML.<class>: BigQuery ML's model types on
// Igloo's engine, with the statements and methods of Snowflake's classes
// (docs/ml-functions.md). Their class is stored with its namespace.
const (
	ClassRegression       = "IGLOO.ML.REGRESSION"
	ClassIglooClassifier  = "IGLOO.ML.CLASSIFICATION"
	ClassKMeans           = "IGLOO.ML.KMEANS"
	ClassPCA              = "IGLOO.ML.PCA"
	ClassFactorization    = "IGLOO.ML.MATRIX_FACTORIZATION"
	ClassDNNClassifier    = "IGLOO.ML.DNN_CLASSIFIER"
	ClassDNNRegressor     = "IGLOO.ML.DNN_REGRESSOR"
	ClassAutoencoder      = "IGLOO.ML.AUTOENCODER"
	ClassAutoMLClassifier = "IGLOO.ML.AUTOML_CLASSIFIER"
	ClassAutoMLRegressor  = "IGLOO.ML.AUTOML_REGRESSOR"
)

// IglooClasses are Igloo's classes, in the order the docs list them.
var IglooClasses = []string{ClassRegression, ClassIglooClassifier, ClassKMeans, ClassPCA, ClassFactorization}

// IsIgloo reports whether class is one of Igloo's own.
func IsIgloo(class string) bool { return strings.HasPrefix(class, "IGLOO.ML.") }

// QualifiedClass names a class as statements write it: SNOWFLAKE.ML.x for
// Snowflake's, IGLOO.ML.x for Igloo's.
func QualifiedClass(class string) string {
	if IsIgloo(class) {
		return class
	}
	return "SNOWFLAKE.ML." + class
}

// MaxTrainRows bounds the rows Igloo's classes train on. Above it, or
// above MaxValues values, they train on a sample of about that many rows
// (package pgwire samples them by a hash of each row, so the same rows
// give the same sample).
const MaxTrainRows = 10_000_000

// TrainRowLimit is the most rows of cols columns a model trains on.
func TrainRowLimit(cols int) int {
	return min(MaxTrainRows, MaxValues/max(cols, 1))
}

// IglooArgs are CREATE IGLOO.ML.<class>'s arguments besides INPUT_DATA:
// TARGET_COLNAME; MATRIX_FACTORIZATION's USER_COLNAME, ITEM_COLNAME and
// RATING_COLNAME; CONFIG_OBJECT, as JSON values.
type IglooArgs struct {
	Target             string
	User, Item, Rating string
	Config             map[string]any
	// Sampled says the input was sampled down to Rows rows of Total.
	Sampled     bool
	Total, Rows int
	Interrupt   func() error
}

// TrainIgloo trains a model of one of Igloo's classes on t, and returns
// it with a summary of its metrics for the model's row.
func TrainIgloo(class string, t *Table, a IglooArgs) (*Artifact, map[string]any, error) {
	cfg := newConfig(a.Config)
	art := &Artifact{Class: class}
	var err error
	switch class {
	case ClassRegression, ClassIglooClassifier:
		art.Supervised, err = trainSupervised(class, t, a, cfg)
	case ClassKMeans:
		art.Clusters, err = trainKMeans(t, a, cfg)
	case ClassPCA:
		art.Components, err = trainPCA(t, a, cfg)
	case ClassFactorization:
		art.Factors, err = trainFactorization(t, a, cfg)
	default:
		return nil, nil, inputErr("%s is not one of Igloo's classes", class)
	}
	if err != nil {
		return nil, nil, err
	}
	if err := cfg.unused(); err != nil {
		return nil, nil, err
	}
	return art, art.summary(), nil
}

// config reads CONFIG_OBJECT's keys (case-insensitively), and refuses
// any it was not asked for.
type config struct {
	m    map[string]any
	used map[string]bool
}

func newConfig(m map[string]any) *config {
	c := &config{m: map[string]any{}, used: map[string]bool{}}
	for k, v := range m {
		c.m[strings.ToLower(k)] = v
	}
	return c
}

func (c *config) get(key string) (any, bool) {
	c.used[key] = true
	v, ok := c.m[key]
	if v == nil {
		return nil, false
	}
	return v, ok
}

// unused refuses the keys nothing read.
func (c *config) unused() error {
	var keys []string
	for k := range c.m {
		if !c.used[k] {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return inputErr("unknown CONFIG_OBJECT key %s", strings.Join(keys, ", "))
}

func (c *config) str(key, def string, allowed ...string) (string, error) {
	v, ok := c.get(key)
	if !ok {
		return def, nil
	}
	s, isStr := v.(string)
	if !isStr {
		return "", inputErr("%s takes a string", key)
	}
	s = strings.ToLower(s)
	if len(allowed) > 0 && !slices.Contains(allowed, s) {
		return "", inputErr("%s is one of '%s', not '%s'", key, strings.Join(allowed, "', '"), s)
	}
	return s, nil
}

func (c *config) num(key string, def, lo, hi float64) (float64, error) {
	v, ok := c.get(key)
	if !ok {
		return def, nil
	}
	f, isNum := v.(float64)
	if !isNum || math.IsNaN(f) || f < lo || f > hi {
		return 0, inputErr("%s takes a number from %g to %g", key, lo, hi)
	}
	return f, nil
}

func (c *config) integer(key string, def, lo, hi int) (int, error) {
	v, ok := c.get(key)
	if !ok {
		return def, nil
	}
	f, isNum := v.(float64)
	if !isNum || f != math.Trunc(f) || f < float64(lo) || f > float64(hi) {
		return 0, inputErr("%s takes a whole number from %d to %d", key, lo, hi)
	}
	return int(f), nil
}

func (c *config) boolean(key string, def bool) (bool, error) {
	v, ok := c.get(key)
	if !ok {
		return def, nil
	}
	b, isBool := v.(bool)
	if !isBool {
		return false, inputErr("%s takes TRUE or FALSE", key)
	}
	return b, nil
}

// ints reads an array of whole numbers, such as hidden_units.
func (c *config) ints(key string, def []int, lo, hi int) ([]int, error) {
	v, ok := c.get(key)
	if !ok {
		return def, nil
	}
	arr, isArr := v.([]any)
	if !isArr || len(arr) == 0 {
		return nil, inputErr("%s takes an array of whole numbers, such as [64, 32]", key)
	}
	out := make([]int, len(arr))
	for i, e := range arr {
		f, isNum := e.(float64)
		if !isNum || f != math.Trunc(f) || f < float64(lo) || f > float64(hi) {
			return nil, inputErr("%s takes whole numbers from %d to %d", key, lo, hi)
		}
		out[i] = int(f)
	}
	return out, nil
}

// seed reads 'seed', which makes models vary.
func (c *config) seed() (uint64, error) {
	s, err := c.integer("seed", 0, 0, math.MaxInt32)
	return uint64(s), err
}

// accept reads keys Igloo takes for BigQuery's sake but needs nothing of,
// checking they hold one of the values given (any, if none).
func (c *config) accept(key string, allowed ...string) error {
	v, ok := c.get(key)
	if !ok || len(allowed) == 0 {
		return nil
	}
	s, isStr := v.(string)
	if !isStr || !slices.Contains(allowed, strings.ToLower(s)) {
		return inputErr("%s is one of '%s' in Igloo", key, strings.Join(allowed, "', '"))
	}
	return nil
}

// summary is what a model's row records of its metrics.
func (a *Artifact) summary() map[string]any {
	out := map[string]any{}
	if m := a.igloo(); m != nil {
		m.summary(out)
	}
	return out
}

// addEval records an evaluation's metrics in a summary.
func addEval(out map[string]any, e *Evaluation) {
	if e == nil {
		return
	}
	out["eval_rows"] = e.Rows
	for _, m := range e.Metrics {
		if !math.IsNaN(m.Value) && !math.IsInf(m.Value, 0) {
			out[m.Name] = m.Value
		}
	}
}

// Metric is a named evaluation metric, as BigQuery's ML.EVALUATE names it.
type Metric struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

// Evaluation is a model's metrics on rows held out of training (or given
// to EVALUATE).
type Evaluation struct {
	Rows    int      `json:"rows"`
	Metrics []Metric `json:"metrics"`
	// Confusion is a classifier's, actual by predicted; ROC a binary
	// classifier's curve.
	Confusion [][]int    `json:"confusion,omitempty"`
	ROC       []ROCPoint `json:"roc,omitempty"`
}

// ROCPoint is a binary classifier's counts at one threshold.
type ROCPoint struct {
	Threshold float64 `json:"threshold"`
	Recall    float64 `json:"recall"`
	FPR       float64 `json:"fpr"`
	TP        int     `json:"tp"`
	FP        int     `json:"fp"`
	TN        int     `json:"tn"`
	FN        int     `json:"fn"`
}

// metric returns the evaluation's metric called name, or NaN.
func (e *Evaluation) metric(name string) float64 {
	if e != nil {
		for _, m := range e.Metrics {
			if m.Name == name {
				return m.Value
			}
		}
	}
	return math.NaN()
}

// Iteration is a line of TRAINING_INFO: a training step's losses.
type Iteration struct {
	Iteration    int     `json:"iteration"`
	Loss         float64 `json:"loss"`
	EvalLoss     float64 `json:"eval_loss"`
	LearningRate float64 `json:"learning_rate"`
}

// finite turns NaN and infinities, which blobs cannot hold, into 0.
func finiteOr(v, def float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return def
	}
	return v
}

// splitRows holds back rows for evaluation as BigQuery's data_split_method
// does: 'auto_split' holds back none of fewer than 500 rows, a fifth of up
// to 50,000, and 10,000 of more; 'random' holds back
// data_split_eval_fraction (default 0.2); 'no_split' none. Snowflake's
// 'evaluate': FALSE is 'no_split'. y, if given, stratifies by class.
func splitRows(cfg *config, n int, y []float64, classify bool, seed uint64) (train, eval []int, err error) {
	method, err := cfg.str("data_split_method", "auto_split", "auto_split", "random", "no_split")
	if err != nil {
		return nil, nil, err
	}
	frac, err := cfg.num("data_split_eval_fraction", 0.2, 0.0001, 0.9)
	if err != nil {
		return nil, nil, err
	}
	evaluate, err := cfg.boolean("evaluate", true)
	if err != nil {
		return nil, nil, err
	}
	if !evaluate {
		method = "no_split"
	}
	switch method {
	case "no_split":
		frac = 0
	case "auto_split":
		switch {
		case n < 500:
			frac = 0
		case n <= 50_000:
			frac = 0.2
		default:
			frac = 10_000 / float64(n)
		}
	}
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}
	if frac == 0 {
		return all, nil, nil
	}
	if classify {
		keep, held := stratified(y, frac, seed, 0xe7a1)
		return keep, held, nil
	}
	for i := range n {
		if float64(rank(seed, 0xe7a1, i)>>11)/(1<<53) < frac {
			eval = append(eval, i)
		} else {
			train = append(train, i)
		}
	}
	if len(train) == 0 || len(eval) == 0 {
		return all, nil, nil
	}
	return train, eval, nil
}
