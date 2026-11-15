package models

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/ml"
	"github.com/akkio-inc/igloo/internal/xgboost"
)

// XGBoost is an IGLOO.ML.XGBOOST model: a booster that XGBoost's own
// library trained (package xgboost), with XGBoost's parameters as
// CONFIG_OBJECT gave them, on the rows as XGBoost's Python package reads
// them: so it is the model the Python package trains on the same rows in
// the same order. See docs/ml-functions.md.
type XGBoost struct {
	// Model is the booster as XGBoost saves it, in Universal Binary JSON.
	// Config is XGBoost's configuration of it (its parameters, objective
	// and metrics, which the model leaves out); Version the release that
	// trained it.
	Model   []byte `json:"model"`
	Config  string `json:"config"`
	Version string `json:"xgboost_version"`
	// Params are the booster's parameters as XGBoost was given them, in
	// order: an eval_metric once for each metric.
	Params    [][2]string `json:"params"`
	Objective string      `json:"objective"`
	Features  []Feature   `json:"features"`
	// Target is TARGET_COLNAME, or Lower and Upper are the columns of an
	// interval label's bounds; Weight is WEIGHT_COLNAME.
	Target     string `json:"target,omitempty"`
	TargetType string `json:"target_type,omitempty"`
	TargetSQL  string `json:"target_sql,omitempty"`
	Lower      string `json:"label_lower_bound,omitempty"`
	Upper      string `json:"label_upper_bound,omitempty"`
	Weight     string `json:"weight,omitempty"`
	// Classes are a classification objective's classes, 0, 1, ..., as
	// the target shows them (True and False for booleans).
	Classes []string `json:"classes,omitempty"`
	// Outputs is how many values a row's prediction has; Groups how many
	// its margin has, each with its own contributions.
	Outputs int `json:"outputs"`
	Groups  int `json:"groups"`
	// Rounds is how many rounds trained. Best is the round predictions
	// end at, from 0: early stopping's best, or the last.
	Rounds         int            `json:"rounds"`
	Best           int            `json:"best_iteration"`
	EarlyStopping  int            `json:"early_stopping_rounds,omitempty"`
	Rows           int            `json:"rows"`
	ValidationRows int            `json:"validation_rows"`
	Training       []XGBoostRound `json:"training"`
	// Eval is XGBoost's metrics on the validation rows, at Best.
	Eval *Evaluation `json:"eval,omitempty"`

	// booster is Model, configured, predicting on one thread: DuckDB's
	// threads each score a row at a time. err is why it did not load.
	booster *xgboost.Booster
	err     error
}

// XGBoostRound is a line of TRAINING_INFO: one metric of one round, as
// XGBoost measured it on the training and validation rows (nil where it
// is not a finite number, or there are no validation rows).
type XGBoostRound struct {
	Iteration  int      `json:"iteration"`
	Metric     string   `json:"metric"`
	Train      *float64 `json:"train"`
	Validation *float64 `json:"validation,omitempty"`
}

// xgbClassObjectives are XGBoost's classification objectives: their
// models predict classes, 0 to num_class - 1.
var xgbClassObjectives = []string{"binary:logistic", "binary:logitraw", "binary:hinge", "multi:softmax", "multi:softprob"}

func (x *XGBoost) classify() bool { return len(x.Classes) > 0 }

func (x *XGBoost) summary(out map[string]any) {
	out["rows"], out["features"], out["objective"] = x.Rows, len(x.Features), x.Objective
	out["rounds"], out["best_iteration"], out["xgboost_version"] = x.Rounds, x.Best, x.Version
	addEval(out, x.Eval)
}

// end is where predictions stop: after early stopping's best round, as
// XGBoost's scikit-learn models predict; otherwise every round (0).
func (x *XGBoost) end() int {
	if x.EarlyStopping > 0 {
		return x.Best + 1
	}
	return 0
}

func (x *XGBoost) prepare() {
	x.booster, x.err = nil, nil
	b, err := xgboost.LoadBooster(x.Model)
	if err == nil {
		if err = b.LoadConfig(x.Config); err == nil {
			err = b.SetParam("nthread", "1")
		}
		if err != nil {
			b.Free()
		}
	}
	if err != nil {
		x.err = fmt.Errorf("load the XGBoost model: %w", err)
		return
	}
	x.booster = b
}

// --- options ---------------------------------------------------------------

// xgbOptions are what CONFIG_OBJECT holds besides XGBoost's parameters:
// how many rounds to train, early stopping, and which rows validate.
type xgbOptions struct {
	rounds, stopping int
	split            splitOptions
	splitCol         string
	params           [][2]string
}

// readXGBoostOptions reads CONFIG_OBJECT: Igloo's training options, and
// every other key as one of XGBoost's parameters, passed to it as given
// (XGBoost refuses what it does not take; see fit).
func readXGBoostOptions(cfg *config) (*xgbOptions, error) {
	o := &xgbOptions{}
	var err error
	if o.rounds, err = cfg.integer("num_boost_round", 10, 1, 1_000_000); err != nil {
		return nil, err
	}
	if o.stopping, err = cfg.integer("early_stopping_rounds", 0, 1, 1_000_000); err != nil {
		return nil, err
	}
	if o.split.method, err = cfg.str("data_split_method", "no_split", "no_split", "auto_split", "random", "custom"); err != nil {
		return nil, err
	}
	if o.split.frac, err = cfg.num("data_split_eval_fraction", 0.2, 0.0001, 0.9); err != nil {
		return nil, err
	}
	if v, ok := cfg.get("data_split_col"); ok {
		s, isStr := v.(string)
		if !isStr || s == "" {
			return nil, inputErr("data_split_col names a boolean column, TRUE for the validation rows")
		}
		o.splitCol = s
	}
	switch {
	case o.split.method == "custom" && o.splitCol == "":
		return nil, inputErr("'data_split_method': 'custom' needs 'data_split_col', a boolean column TRUE for the validation rows")
	case o.split.method != "custom" && o.splitCol != "":
		return nil, inputErr("data_split_col goes with 'data_split_method': 'custom'")
	case o.stopping > 0 && o.split.method == "no_split":
		return nil, inputErr("early_stopping_rounds stops on validation rows: hold some back with data_split_method " +
			"('random', 'auto_split', or 'custom' with data_split_col)")
	}
	keys := make([]string, 0, len(cfg.m))
	for k := range cfg.m {
		if !cfg.used[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		v, ok := cfg.get(k)
		if !ok {
			continue
		}
		vals, err := xgbParamValues(k, v)
		if err != nil {
			return nil, err
		}
		for _, s := range vals {
			o.params = append(o.params, [2]string{k, s})
		}
	}
	return o, nil
}

// xgbParamValues writes a parameter's value as XGBoost's Python package
// passes it to XGBoosterSetParam: an eval_metric array as one value a
// metric, monotone_constraints' array as (1,0,-1), other arrays as lists,
// numbers as Python writes them (whole numbers without a point, which
// XGBoost's whole-number parameters need).
func xgbParamValues(key string, v any) ([]string, error) {
	if arr, ok := v.([]any); ok && key == "eval_metric" {
		var out []string
		for _, e := range arr {
			s, isStr := e.(string)
			if !isStr {
				return nil, inputErr("eval_metric takes a metric's name or an array of them")
			}
			out = append(out, s)
		}
		return out, nil
	}
	s, err := xgbParamText(v)
	if err != nil {
		return nil, inputErr("%s: %v", key, err)
	}
	if arr, ok := v.([]any); ok && key == "monotone_constraints" {
		parts := make([]string, len(arr))
		for i, e := range arr {
			parts[i], _ = xgbParamText(e)
		}
		s = "(" + strings.Join(parts, ",") + ")"
	}
	return []string{s}, nil
}

func xgbParamText(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case bool:
		return strconv.FormatBool(x), nil
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1e15 {
			return strconv.FormatInt(int64(x), 10), nil
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			s, err := xgbParamText(e)
			if err != nil {
				return "", err
			}
			if _, isStr := e.(string); isStr {
				s = strconv.Quote(s)
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	}
	return "", fmt.Errorf("XGBoost's parameters are strings, numbers, booleans or arrays of them")
}

// --- training ----------------------------------------------------------------

// xgbRows are rows as XGBoost reads them: n rows of f features in
// row-major order, a missing value NaN, and the float fields XGBoost
// trains and evaluates with (label, label_lower_bound, label_upper_bound,
// weight).
type xgbRows struct {
	n, f   int
	x      []float32
	fields map[string][]float32
}

func (r *xgbRows) take(rows []int) *xgbRows {
	out := &xgbRows{n: len(rows), f: r.f, x: make([]float32, 0, len(rows)*r.f), fields: map[string][]float32{}}
	for _, i := range rows {
		out.x = append(out.x, r.x[i*r.f:(i+1)*r.f]...)
	}
	for k, v := range r.fields {
		w := make([]float32, len(rows))
		for j, i := range rows {
			w[j] = v[i]
		}
		out.fields[k] = w
	}
	return out
}

// matrix makes XGBoost's matrix of the rows, its features named.
func (r *xgbRows) matrix(names []string) (*xgboost.Matrix, error) {
	m, err := xgboost.NewMatrix(r.x, r.n, r.f)
	if err != nil {
		return nil, err
	}
	fields := make([]string, 0, len(r.fields))
	for k := range r.fields {
		fields = append(fields, k)
	}
	sort.Strings(fields)
	for _, k := range fields {
		if err = m.SetFloatInfo(k, r.fields[k]); err != nil {
			break
		}
	}
	if err == nil {
		err = m.SetFeatureNames(names)
	}
	if err != nil {
		m.Free()
		return nil, xgbErr(err)
	}
	return m, nil
}

// xgbValue is a table's value as an XGBoost feature: a number, a boolean
// as 1 or 0, a time in seconds since 1970; text that reads as either (a
// boolean feature scored from text); otherwise missing.
func xgbValue(c *TableCol, i int) float32 {
	if c.Null[i] {
		return float32(math.NaN())
	}
	switch c.Type {
	case TypeNumber:
		return float32(c.Num[i])
	case TypeTime:
		return float32(float64(c.Micros[i]) / 1e6)
	}
	return xgbText(c.Str[i])
}

func xgbText(s string) float32 {
	switch s {
	case "true":
		return 1
	case "false":
		return 0
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return float32(f)
	}
	return float32(math.NaN())
}

// xgbFeature reads column c as a feature, or says why XGBoost cannot.
func xgbFeature(c *TableCol) (Feature, error) {
	switch c.Type {
	case TypeNumber, TypeBool, TypeTime:
		return Feature{Name: c.Name, Type: c.Type.String()}, nil
	case TypeString:
		return Feature{}, inputErr("column %s is text, and IGLOO.ML.XGBOOST trains on numbers, booleans and timestamps, as XGBoost's "+
			"matrices hold them: encode it in INPUT_DATA's query (such as %s = 'value' AS %s_value, one for each category), "+
			"or leave it out", c.Name, c.Name, c.Name)
	}
	return Feature{}, inputErr("column %s is of type %s; IGLOO.ML.XGBOOST trains on numbers, booleans and timestamps (cast it)", c.Name, c.TypeName)
}

// xgbNumbers reads a label column's values (bounds, weights): numbers, or
// booleans as 1 and 0.
func xgbNumbers(t *Table, arg, name string, booleans bool) (*TableCol, error) {
	c := t.Col(name)
	if c == nil {
		return nil, inputErr("%s %s is not a column of INPUT_DATA", arg, name)
	}
	if c.Type != TypeNumber && (!booleans || c.Type != TypeBool) {
		hint := ""
		if booleans {
			hint = ": XGBoost's labels are numbers, classes 0, 1, ... (cast it)"
		}
		return nil, inputErr("%s %s is of type %s, not a number%s", arg, c.Name, c.TypeName, hint)
	}
	return c, nil
}

// xgbData reads INPUT_DATA as XGBoost trains on it: the features (every
// column but the label's, the weight's and the split's), and the fields.
// Rows whose target is NULL are left out; a NULL bound or weight is
// refused. It returns the rows, the features, the target's column, and
// which rows validate (nil without data_split_col).
func xgbData(t *Table, a IglooArgs, splitCol string) (*xgbRows, []Feature, *TableCol, []bool, error) {
	var except []string
	var target *TableCol
	var bounds [2]*TableCol
	var weight *TableCol
	var err error
	switch {
	case a.Target != "" && (a.Lower != "" || a.Upper != ""):
		return nil, nil, nil, nil, inputErr("give TARGET_COLNAME, or LABEL_LOWER_BOUND_COLNAME and LABEL_UPPER_BOUND_COLNAME for " +
			"an interval label (survival:aft), not both")
	case a.Target != "":
		if target, err = xgbNumbers(t, "TARGET_COLNAME", a.Target, true); err != nil {
			return nil, nil, nil, nil, err
		}
		except = append(except, a.Target)
	case a.Lower != "" && a.Upper != "":
		for k, b := range []struct{ arg, name string }{{"LABEL_LOWER_BOUND_COLNAME", a.Lower}, {"LABEL_UPPER_BOUND_COLNAME", a.Upper}} {
			if bounds[k], err = xgbNumbers(t, b.arg, b.name, false); err != nil {
				return nil, nil, nil, nil, err
			}
			except = append(except, b.name)
		}
	default:
		return nil, nil, nil, nil, inputErr("IGLOO.ML.XGBOOST needs TARGET_COLNAME, or LABEL_LOWER_BOUND_COLNAME and " +
			"LABEL_UPPER_BOUND_COLNAME for an interval label (survival:aft)")
	}
	if a.Weight != "" {
		if weight, err = xgbNumbers(t, "WEIGHT_COLNAME", a.Weight, false); err != nil {
			return nil, nil, nil, nil, err
		}
		except = append(except, a.Weight)
	}
	var split *TableCol
	if splitCol != "" {
		if split = t.Col(splitCol); split == nil || split.Type != TypeBool {
			return nil, nil, nil, nil, inputErr("data_split_col %s is not a boolean column of INPUT_DATA", splitCol)
		}
		except = append(except, splitCol)
	}
	var feats []Feature
	var cols []*TableCol
	for _, name := range otherNames(t, except...) {
		c := t.Col(name)
		f, err := xgbFeature(c)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		feats, cols = append(feats, f), append(cols, c)
	}
	if len(feats) == 0 {
		return nil, nil, nil, nil, inputErr("INPUT_DATA has no features besides the label's columns")
	}
	r := &xgbRows{f: len(feats), fields: map[string][]float32{}}
	var valid []bool
	for i := range t.N {
		if target != nil && target.Null[i] {
			continue
		}
		for _, c := range []*TableCol{bounds[0], bounds[1], weight, split} {
			if c != nil && c.Null[i] {
				return nil, nil, nil, nil, inputErr("%s is NULL in a row; it needs a value in every row%s", c.Name, nullHint(c, a))
			}
		}
		for _, c := range cols {
			r.x = append(r.x, xgbValue(c, i))
		}
		if target != nil {
			r.fields["label"] = append(r.fields["label"], xgbValue(target, i))
		}
		if bounds[0] != nil {
			r.fields["label_lower_bound"] = append(r.fields["label_lower_bound"], float32(bounds[0].Num[i]))
			r.fields["label_upper_bound"] = append(r.fields["label_upper_bound"], float32(bounds[1].Num[i]))
		}
		if weight != nil {
			r.fields["weight"] = append(r.fields["weight"], float32(weight.Num[i]))
		}
		if split != nil {
			valid = append(valid, split.Str[i] == "true")
		}
		r.n++
	}
	if r.n == 0 {
		return nil, nil, nil, nil, inputErr("INPUT_DATA has no rows whose target %s is not NULL", a.Target)
	}
	return r, feats, target, valid, nil
}

func nullHint(c *TableCol, a IglooArgs) string {
	if strings.EqualFold(c.Name, a.Upper) {
		return " (a right-censored row's upper bound is 'Infinity'::DOUBLE)"
	}
	return ""
}

// trainXGBoost trains an IGLOO.ML.XGBOOST model on t.
func trainXGBoost(t *Table, a IglooArgs, cfg *config) (*XGBoost, error) {
	o, err := readXGBoostOptions(cfg)
	if err != nil {
		return nil, err
	}
	seed, err := cfg.seed()
	if err != nil {
		return nil, err
	}
	rows, feats, target, isValid, err := xgbData(t, a, o.splitCol)
	if err != nil {
		return nil, err
	}
	x := &XGBoost{Version: xgboost.Version(), Params: o.params, Features: feats, Target: a.Target, Lower: a.Lower, Upper: a.Upper,
		Weight: a.Weight, EarlyStopping: o.stopping}
	if target != nil {
		x.Target, x.TargetType, x.TargetSQL = target.Name, target.Type.String(), target.SQLType
	}
	var train, valid []int
	switch o.split.method {
	case "custom":
		for i, v := range isValid {
			if v {
				valid = append(valid, i)
			} else {
				train = append(train, i)
			}
		}
	default:
		objective := ""
		for _, p := range o.params {
			if p[0] == "objective" {
				objective = p[1]
			}
		}
		train, valid = o.split.rows(rows.n, f64(rows.fields["label"]), slices.Contains(xgbClassObjectives, objective), seed)
	}
	if len(train) == 0 {
		return nil, inputErr("no rows are left to train on: data_split_col %s is TRUE in every row", o.splitCol)
	}
	if o.stopping > 0 && len(valid) == 0 {
		return nil, inputErr("early_stopping_rounds stops on validation rows, and the split held back none")
	}
	var v *xgbRows
	if len(valid) > 0 {
		v = rows.take(valid)
	}
	if err := x.fit(rows.take(train), v, o.rounds, a.Interrupt); err != nil {
		return nil, err
	}
	x.prepare()
	return x, x.err
}

func f64(v []float32) []float64 {
	out := make([]float64, len(v))
	for i, f := range v {
		out[i] = float64(f)
	}
	return out
}

// xgbErr makes XGBoost's errors the user's: they are about the parameters
// and data the user gave it.
func xgbErr(err error) error {
	if xe, ok := err.(*xgboost.Error); ok {
		return inputErr("XGBoost: %s", xe.Message)
	}
	return err
}

// fit trains x on train for up to rounds rounds, as XGBoost's Python
// package's xgboost.train does with evals [(train, "train"), (valid,
// "validation")]: XGBoost's own metrics of each round, and early stopping
// on the validation rows' last metric.
func (x *XGBoost) fit(train, valid *xgbRows, rounds int, interrupt func() error) error {
	names := make([]string, len(x.Features))
	for j, f := range x.Features {
		names[j] = f.Name
	}
	dtrain, err := train.matrix(names)
	if err != nil {
		return err
	}
	defer dtrain.Free()
	sets, setNames := []*xgboost.Matrix{dtrain}, []string{"train"}
	if valid != nil {
		dvalid, err := valid.matrix(names)
		if err != nil {
			return err
		}
		defer dvalid.Free()
		sets, setNames = append(sets, dvalid), append(setNames, "validation")
	}
	x.Rows = train.n
	if valid != nil {
		x.ValidationRows = valid.n
	}
	b, err := xgboost.NewBooster(sets...)
	if err != nil {
		return err
	}
	defer b.Free()
	if err := b.SetFeatureNames(names); err != nil {
		return xgbErr(err)
	}
	for _, p := range x.Params {
		if err := b.SetParam(p[0], p[1]); err != nil {
			return xgbErr(err)
		}
	}
	// As the Python package does, so that XGBoost names parameters it did
	// not use, which Igloo refuses as it refuses other unknown keys.
	if err := b.SetParam("validate_parameters", "1"); err != nil {
		return err
	}
	stop := xgbStopping{rounds: x.EarlyStopping}
	var last []xgboost.EvalResult
	for r := range rounds {
		if interrupt != nil {
			if err := interrupt(); err != nil {
				return err
			}
		}
		warnings, err := b.Update(r, dtrain)
		if err != nil {
			return xgbErr(err)
		}
		for _, w := range warnings {
			if strings.Contains(w, "are not used") {
				return inputErr("XGBoost does not use these parameters here: %s", strings.TrimSpace(strings.TrimPrefix(w, "Parameters:")))
			}
		}
		line, err := b.Eval(r, sets, setNames)
		if err != nil {
			return xgbErr(err)
		}
		last, err = xgboost.ParseEval(line)
		if err != nil {
			return err
		}
		x.record(r, last)
		x.Rounds = r + 1
		if x.EarlyStopping > 0 && stop.after(r, last) {
			break
		}
	}
	x.Best = x.Rounds - 1
	if x.EarlyStopping > 0 {
		// The attributes XGBoost's Python package sets, which predict with
		// the best round when the model is loaded there.
		x.Best = stop.bestIter
		if err := b.SetAttr("best_iteration", strconv.Itoa(stop.bestIter)); err != nil {
			return err
		}
		if err := b.SetAttr("best_score", pyRepr(stop.best)); err != nil {
			return err
		}
	}
	if valid != nil {
		x.Eval = &Evaluation{Rows: valid.n}
		for _, rd := range x.Training {
			if rd.Iteration == x.Best && rd.Validation != nil {
				x.Eval.Metrics = append(x.Eval.Metrics, Metric{Name: rd.Metric, Value: *rd.Validation})
			}
		}
	}
	if x.Model, err = b.Save(); err != nil {
		return err
	}
	if x.Config, err = b.SaveConfig(); err != nil {
		return err
	}
	return x.describe(b, dtrain)
}

// record keeps a round's metrics for TRAINING_INFO.
func (x *XGBoost) record(r int, res []xgboost.EvalResult) {
	index := map[string]int{}
	for _, e := range res {
		k, ok := index[e.Metric]
		if !ok {
			k = len(x.Training)
			index[e.Metric] = k
			x.Training = append(x.Training, XGBoostRound{Iteration: r, Metric: e.Metric})
		}
		v := finitePtr(e.Value)
		if e.Data == "train" {
			x.Training[k].Train = v
		} else {
			x.Training[k].Validation = v
		}
	}
}

func finitePtr(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

// describe reads what the methods need of the trained booster: its
// objective and classes from XGBoost's configuration, and the shape of
// its predictions.
func (x *XGBoost) describe(b *xgboost.Booster, train *xgboost.Matrix) error {
	var cfg struct {
		Learner struct {
			Objective struct {
				Name string `json:"name"`
			} `json:"objective"`
			Param struct {
				NumClass string `json:"num_class"`
			} `json:"learner_model_param"`
		} `json:"learner"`
	}
	if err := json.Unmarshal([]byte(x.Config), &cfg); err != nil {
		return fmt.Errorf("read XGBoost's configuration: %w", err)
	}
	x.Objective = cfg.Learner.Objective.Name
	if slices.Contains(xgbClassObjectives, x.Objective) {
		k, _ := strconv.Atoi(cfg.Learner.Param.NumClass)
		if strings.HasPrefix(x.Objective, "binary:") {
			k = 2
		}
		for c := range k {
			label := strconv.Itoa(c)
			if x.TargetType == TypeBool.String() {
				label = []string{"False", "True"}[c]
			}
			x.Classes = append(x.Classes, label)
		}
	}
	one, err := xgboost.NewMatrix(make([]float32, len(x.Features)), 1, len(x.Features))
	if err != nil {
		return err
	}
	defer one.Free()
	for _, p := range []struct {
		typ xgboost.PredictType
		dst *int
	}{{xgboost.Value, &x.Outputs}, {xgboost.Margin, &x.Groups}} {
		pred, err := b.Predict(one, p.typ, 0)
		if err != nil {
			return xgbErr(err)
		}
		*p.dst = len(pred.Values)
	}
	return nil
}

// xgbStopping is the Python package's EarlyStopping callback: it watches
// the last data set's last metric, maximizes the metrics whose names say
// they are better higher (as the Python package lists them), and stops
// after rounds rounds without improvement.
type xgbStopping struct {
	rounds   int
	maximize bool
	started  bool
	best     float64
	bestIter int
	since    int
}

func (s *xgbStopping) after(iter int, res []xgboost.EvalResult) bool {
	if len(res) == 0 {
		return false
	}
	last := res[len(res)-1]
	score := last.Value
	switch {
	case !s.started:
		s.started = true
		s.maximize = last.Metric != "mape" && slices.ContainsFunc([]string{"auc", "aucpr", "pre", "pre@", "map", "ndcg", "auc@",
			"aucpr@", "map@", "ndcg@"}, func(p string) bool { return strings.HasPrefix(last.Metric, p) })
		s.best, s.bestIter = score, iter
	case s.maximize && score > s.best, !s.maximize && s.best > score:
		s.best, s.bestIter, s.since = score, iter, 0
	default:
		s.since++
	}
	return s.since >= s.rounds
}

// pyRepr writes f as Python's repr does, as the Python package records
// best_score.
func pyRepr(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	e := strconv.FormatFloat(f, 'e', -1, 64)
	exp, _ := strconv.Atoi(e[strings.IndexByte(e, 'e')+1:])
	if exp < -4 || exp >= 16 {
		return e
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".") {
		s += ".0"
	}
	return s
}

// --- row methods -------------------------------------------------------------

func (x *XGBoost) rowFeatures(method string) ([]Feature, bool) {
	switch method {
	case "PREDICT", "EXPLAIN_PREDICT":
		return x.Features, true
	}
	return nil, false
}

func (x *XGBoost) rowOptions(method string, cfg *config) (*RowOptions, error) {
	opt := &RowOptions{}
	var err error
	switch method {
	case "PREDICT":
		opt.Margin, err = cfg.boolean("output_margin", false)
	case "EXPLAIN_PREDICT":
		opt.TopK, err = cfg.integer("top_k_features", 5, 1, 10_000)
	}
	return opt, err
}

// targetName is what predicted_<target> is named after: the target, or
// label for an interval label's.
func (x *XGBoost) targetName() string {
	if x.Target != "" {
		return x.Target
	}
	return "label"
}

func (x *XGBoost) rowTable(method string, opt *RowOptions) RowTable {
	name := sqlName("predicted_", x.targetName())
	valueType, marginType := "DOUBLE", "DOUBLE"
	if x.Outputs > 1 {
		valueType = "JSON"
	}
	if x.Groups > 1 {
		marginType = "JSON"
	}
	var cols []RowColumn
	if x.classify() {
		typ := x.TargetSQL
		if typ == "" {
			typ = "VARCHAR"
		}
		cols = append(cols, RowColumn{name, "$.class", typ})
	} else {
		cols = append(cols, RowColumn{name, "$.value", valueType})
	}
	if method == "PREDICT" {
		if x.classify() {
			cols = append(cols, RowColumn{sqlName("predicted_", x.targetName(), "_probs"), "$.probability", "JSON"})
		}
		if opt.Margin {
			cols = append(cols, RowColumn{sqlName("predicted_", x.targetName(), "_margin"), "$.margin", marginType})
		}
		return RowTable{Columns: cols}
	}
	if x.classify() {
		cols = append(cols, RowColumn{"probability", "$.class_probability", "DOUBLE"})
	}
	return RowTable{Columns: append(cols,
		RowColumn{"top_feature_attributions", "$.top_feature_attributions", "JSON"},
		RowColumn{"baseline_prediction_value", "$.baseline_prediction_value", "DOUBLE"},
		RowColumn{"prediction_value", "$.prediction_value", "DOUBLE"},
		RowColumn{"approximation_error", "$.approximation_error", "DOUBLE"})}
}

// rowValues reads a scored row's features, as training read them.
func (x *XGBoost) rowValues(r *ml.RowValues) []float32 {
	v := make([]float32, len(x.Features))
	for j, f := range x.Features {
		switch {
		case featureKind(f) == ml.Numeric:
			v[j] = float32(r.Num[j])
		case r.Missing[j]:
			v[j] = float32(math.NaN())
		default:
			v[j] = xgbText(r.Str[j])
		}
	}
	return v
}

func (x *XGBoost) scoreRow(buf []byte, method string, opt *RowOptions, r *ml.RowValues) ([]byte, error) {
	if x.err != nil {
		return nil, x.err
	}
	m, err := xgboost.NewMatrix(x.rowValues(r), 1, len(x.Features))
	if err != nil {
		return nil, err
	}
	defer m.Free()
	value, err := x.booster.Predict(m, xgboost.Value, x.end())
	if err != nil {
		return nil, err
	}
	var margin []float32
	if opt.Margin || method == "EXPLAIN_PREDICT" || x.Objective == "multi:softmax" {
		p, err := x.booster.Predict(m, xgboost.Margin, x.end())
		if err != nil {
			return nil, err
		}
		margin = p.Values
	}
	buf = append(buf, '{')
	buf = appendKey(buf, "value", true)
	buf = appendFloats(buf, value.Values)
	class := 0
	if x.classify() {
		var probs []float64
		class, probs = x.classProbabilities(value.Values, margin)
		buf = appendKey(buf, "class", false)
		buf = append(buf, jsonString(x.Classes[class])...)
		switch {
		case method == "EXPLAIN_PREDICT":
			buf = appendKey(buf, "class_probability", false)
			if probs == nil {
				buf = append(buf, "null"...)
			} else {
				buf = appendNum32(buf, probs[class])
			}
		case probs != nil:
			buf = appendKey(buf, "probability", false)
			buf = append(buf, '{')
			for k, l := range x.Classes {
				buf = appendKey(buf, l, k == 0)
				buf = appendNum32(buf, probs[k])
			}
			buf = append(buf, '}')
		}
	}
	if opt.Margin {
		buf = appendKey(buf, "margin", false)
		buf = appendFloats(buf, margin)
	}
	if method == "EXPLAIN_PREDICT" {
		contrib, err := x.booster.Predict(m, xgboost.Contributions, x.end())
		if err != nil {
			return nil, err
		}
		// A multiclass model's predicted class's; otherwise the first
		// output's (a binary model's one, class 1's log-odds).
		g := 0
		if x.classify() && x.Groups > 1 {
			g = class
		}
		F := len(x.Features)
		row := contrib.Values[g*(F+1) : (g+1)*(F+1)]
		attrs := make([]attribution, F)
		sum := float64(row[F])
		for j, f := range x.Features {
			attrs[j] = attribution{f.Name, float64(row[j])}
			sum += float64(row[j])
		}
		buf = appendKey(buf, "top_feature_attributions", false)
		buf = appendAttributions(buf, attrs, opt.TopK)
		buf = appendKey(buf, "baseline_prediction_value", false)
		buf = appendNum32(buf, float64(row[F]))
		buf = appendKey(buf, "prediction_value", false)
		buf = appendNum32(buf, float64(margin[g]))
		buf = appendKey(buf, "approximation_error", false)
		buf = appendNum(buf, math.Abs(float64(margin[g])-sum))
	}
	return append(buf, '}'), nil
}

// classProbabilities reads a row's class, and its classes' probabilities
// where the objective has them, from its prediction and margin.
func (x *XGBoost) classProbabilities(value, margin []float32) (int, []float64) {
	switch x.Objective {
	case "binary:logistic", "binary:logitraw":
		p := float64(value[0])
		if x.Objective == "binary:logitraw" {
			p = ml.Sigmoid(p)
		}
		if p > 0.5 {
			return 1, []float64{1 - p, p}
		}
		return 0, []float64{1 - p, p}
	case "multi:softprob":
		probs := f64(value)
		best := 0
		for k, p := range probs {
			if p > probs[best] {
				best = k
			}
		}
		return best, probs
	case "multi:softmax":
		probs := f64(margin)
		top := slices.Max(probs)
		var sum float64
		for k, v := range probs {
			probs[k] = math.Exp(v - top)
			sum += probs[k]
		}
		for k := range probs {
			probs[k] /= sum
		}
		return min(max(int(value[0]), 0), len(x.Classes)-1), probs
	}
	// binary:hinge predicts 0 or 1, and no probability.
	return min(max(int(value[0]), 0), len(x.Classes)-1), nil
}

// appendNum32 writes a number at the precision of XGBoost's float32
// results, shortest.
func appendNum32(buf []byte, v float64) []byte {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return append(buf, "null"...)
	}
	return strconv.AppendFloat(buf, v, 'g', -1, 32)
}

// appendFloats writes one value as a number, several as an array.
func appendFloats(buf []byte, v []float32) []byte {
	if len(v) == 1 {
		return appendNum32(buf, float64(v[0]))
	}
	buf = append(buf, '[')
	for i, f := range v {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendNum32(buf, float64(f))
	}
	return append(buf, ']')
}

// --- table methods -------------------------------------------------------------

// bestBooster returns a booster of the trees predictions use, a copy:
// evaluating caches what it predicts in the booster, which rows scored
// alongside share.
func (x *XGBoost) bestBooster() (*xgboost.Booster, error) {
	if x.err != nil {
		return nil, x.err
	}
	end := x.end()
	if end == 0 {
		end = x.Rounds
	}
	return x.booster.Slice(0, end)
}

func (x *XGBoost) method(class, method string) (*Result, error) {
	cols, _ := MethodColumns(class, method)
	res := &Result{Columns: cols}
	add := func(row ...any) { res.Rows = append(res.Rows, row) }
	switch method {
	case "EVALUATE":
		if x.Eval == nil {
			return nil, inputErr("the model held no rows out of training to evaluate on ('data_split_method': 'no_split', the " +
				"default): call EVALUATE with INPUT_DATA, or create it again with data_split_method")
		}
		return xgbEvaluation(cols, x.Eval), nil
	case "TRAINING_INFO":
		for _, rd := range x.Training {
			var train, valid any
			if rd.Train != nil {
				train = *rd.Train
			}
			if rd.Validation != nil {
				valid = *rd.Validation
			}
			add(int64(rd.Iteration), rd.Metric, train, valid, rd.Iteration == x.Best)
		}
	case "FEATURE_IMPORTANCE":
		b, err := x.bestBooster()
		if err != nil {
			return nil, err
		}
		defer b.Free()
		scores := map[string]map[string]float32{}
		for _, typ := range []string{"weight", "gain", "cover"} {
			if scores[typ], err = b.FeatureScore(typ); err != nil {
				return nil, err
			}
		}
		order := identity(len(x.Features))
		sort.SliceStable(order, func(a, b int) bool {
			return scores["gain"][x.Features[order[a]].Name] > scores["gain"][x.Features[order[b]].Name]
		})
		for _, j := range order {
			n := x.Features[j].Name
			add(n, int64(scores["weight"][n]), float64(scores["gain"][n]), float64(scores["cover"][n]))
		}
	default:
		return nil, inputErr("%s models have no method %s; they have %s", QualifiedClass(class), method, joinMethods(class))
	}
	return res, nil
}

// xgbEvaluation lays out EVALUATE: a row for each of XGBoost's metrics.
func xgbEvaluation(cols []ResultColumn, e *Evaluation) *Result {
	res := &Result{Columns: cols}
	for _, m := range e.Metrics {
		res.Rows = append(res.Rows, []any{m.Name, finiteNull(m.Value)})
	}
	return res
}

// evaluateOn measures the model on t's rows with XGBoost's metrics: those
// it was trained with, or its objective's.
func (x *XGBoost) evaluateOn(t *Table) (*Evaluation, error) {
	r := &xgbRows{f: len(x.Features), fields: map[string][]float32{}}
	cols := make([]*TableCol, len(x.Features))
	for j, f := range x.Features {
		cols[j] = t.Col(f.Name)
	}
	var label []*TableCol
	var fields []string
	for _, l := range []struct{ field, arg, name string }{{"label", "TARGET_COLNAME", x.Target},
		{"label_lower_bound", "LABEL_LOWER_BOUND_COLNAME", x.Lower}, {"label_upper_bound", "LABEL_UPPER_BOUND_COLNAME", x.Upper},
		{"weight", "WEIGHT_COLNAME", x.Weight}} {
		if l.name == "" {
			continue
		}
		c, err := xgbNumbers(t, l.arg, l.name, l.field == "label")
		if err != nil {
			return nil, err
		}
		label, fields = append(label, c), append(fields, l.field)
	}
rows:
	for i := range t.N {
		for _, c := range label {
			if c.Null[i] {
				continue rows
			}
		}
		for _, c := range cols {
			if c == nil {
				r.x = append(r.x, float32(math.NaN()))
			} else {
				r.x = append(r.x, xgbValue(c, i))
			}
		}
		for k, c := range label {
			r.fields[fields[k]] = append(r.fields[fields[k]], xgbValue(c, i))
		}
		r.n++
	}
	if r.n == 0 {
		return nil, inputErr("INPUT_DATA has no rows with the model's labels")
	}
	names := make([]string, len(x.Features))
	for j, f := range x.Features {
		names[j] = f.Name
	}
	m, err := r.matrix(names)
	if err != nil {
		return nil, err
	}
	defer m.Free()
	b, err := x.bestBooster()
	if err != nil {
		return nil, err
	}
	defer b.Free()
	line, err := b.Eval(0, []*xgboost.Matrix{m}, []string{"input"})
	if err != nil {
		return nil, xgbErr(err)
	}
	res, err := xgboost.ParseEval(line)
	if err != nil {
		return nil, err
	}
	e := &Evaluation{Rows: r.n}
	for _, m := range res {
		e.Metrics = append(e.Metrics, Metric{Name: m.Metric, Value: m.Value})
	}
	return e, nil
}
