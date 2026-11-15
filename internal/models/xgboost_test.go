package models

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// The reference values are what XGBoost's Python package trained and
// predicted on the same rows with the same parameters
// (testdata/xgboost/make_reference.py, which says how to make them again).
// On the platform that made them, Igloo's models are the Python package's
// byte for byte; on others, compilers round a float here and there
// otherwise, in XGBoost's library as in its Python package, so the tests
// compare within a tolerance.

type xgbReference struct {
	XGBoost  string `json:"xgboost"`
	Platform string `json:"platform"`
	Binary   struct {
		ModelSHA256       string                          `json:"model_sha256"`
		History           map[string]map[string][]float64 `json:"history"`
		ValidationAUC     float64                         `json:"validation_auc"`
		ValidationLogloss float64                         `json:"validation_logloss"`
		Predictions       []float64                       `json:"predictions"`
		Margins           []float64                       `json:"margins"`
		Contributions     []float64                       `json:"contributions"`
	} `json:"binary"`
	EarlyStopping struct {
		ModelSHA256   string                          `json:"model_sha256"`
		BestIteration int                             `json:"best_iteration"`
		BestScore     float64                         `json:"best_score"`
		Rounds        int                             `json:"rounds"`
		History       map[string]map[string][]float64 `json:"history"`
		Predictions   []float64                       `json:"predictions"`
	} `json:"early_stopping"`
	WeightedRegression struct {
		ModelSHA256 string                          `json:"model_sha256"`
		History     map[string]map[string][]float64 `json:"history"`
		Predictions []float64                       `json:"predictions"`
	} `json:"weighted_regression"`
	AFT struct {
		ModelSHA256  string    `json:"model_sha256"`
		FinalNLogLik float64   `json:"final_aft_nloglik"`
		Predictions  []float64 `json:"predictions"`
	} `json:"aft"`
}

func xgbRef(t *testing.T) *xgbReference {
	t.Helper()
	b, err := os.ReadFile("testdata/xgboost/reference.json")
	if err != nil {
		t.Fatal(err)
	}
	var r xgbReference
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return &r
}

// readCSV reads a CSV file as a table: columns of numbers (empty is NULL,
// inf is +Infinity), of True and False as booleans.
func readCSV(t *testing.T, path string) *Table {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	tbl := &Table{N: len(recs) - 1}
	for j, name := range recs[0] {
		c := &TableCol{Name: name, Null: make([]bool, tbl.N)}
		if v := recs[1][j]; v == "True" || v == "False" {
			c.Type, c.TypeName, c.SQLType, c.Str = TypeBool, "BOOLEAN", "BOOLEAN", make([]string, tbl.N)
		} else {
			c.Type, c.TypeName, c.SQLType, c.Num = TypeNumber, "DOUBLE", "DOUBLE", make([]float64, tbl.N)
		}
		for i, rec := range recs[1:] {
			v := rec[j]
			switch {
			case v == "":
				c.Null[i] = true
				if c.Num != nil {
					c.Num[i] = math.NaN()
				}
			case c.Type == TypeBool:
				c.Str[i] = strings.ToLower(v)
			default:
				if c.Num[i], err = strconv.ParseFloat(v, 64); err != nil {
					t.Fatalf("%s: %v", name, err)
				}
			}
		}
		tbl.Cols = append(tbl.Cols, c)
	}
	return tbl
}

// field reads a number (or an array's elements) at key of each object.
func field(t *testing.T, objects []string, key string) []float64 {
	t.Helper()
	var out []float64
	for _, o := range objects {
		var m map[string]any
		if err := json.Unmarshal([]byte(o), &m); err != nil {
			t.Fatalf("%s: %v", o, err)
		}
		switch v := m[key].(type) {
		case float64:
			out = append(out, v)
		case []any:
			for _, e := range v {
				out = append(out, e.(float64))
			}
		default:
			t.Fatalf("%s has no number %s", o, key)
		}
	}
	return out
}

// agree reports how many of got are want's float32s (XGBoost's results
// are float32s) and the largest difference, and fails where one differs
// by more than tol, relative to values above 1.
func agree(t *testing.T, what string, got, want []float64, tol float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d values, want %d", what, len(got), len(want))
	}
	worst, same := 0.0, 0
	for i := range got {
		if float32(got[i]) == float32(want[i]) {
			same++
			continue
		}
		d := math.Abs(got[i]-want[i]) / max(1, math.Abs(want[i]))
		worst = max(worst, d)
		if !(d <= tol) {
			t.Errorf("%s: value %d is %v, the Python package's %v", what, i, got[i], want[i])
		}
	}
	t.Logf("%s: %d of %d the Python package's float32, the rest within %.3g", what, same, len(got), worst)
}

// sameModel checks, on the platform that made the reference, that the
// model is the Python package's byte for byte: as XGBoost saves it, with
// its trees, attributes and feature names.
func sameModel(t *testing.T, ref *xgbReference, x *XGBoost, want string) {
	t.Helper()
	sum := sha256.Sum256(x.Model)
	same := hex.EncodeToString(sum[:]) == want
	here := runtime.GOOS + "/" + runtime.GOARCH
	switch {
	case same:
		t.Logf("the model's %d bytes are the Python package's", len(x.Model))
	case here == ref.Platform:
		t.Errorf("the model's %d bytes are not the Python package's", len(x.Model))
	default:
		t.Logf("the model's %d bytes are not those the Python package saved on %s", len(x.Model), ref.Platform)
	}
}

func xgbTrain(t *testing.T, tbl *Table, a IglooArgs) *Artifact {
	t.Helper()
	art := trainIgloo(t, ClassXGBoost, tbl, a)
	if art.XGBoost == nil {
		t.Fatal("no XGBoost model")
	}
	return art
}

func binaryTable(t *testing.T) *Table {
	return only(readCSV(t, "testdata/xgboost/binary.csv"), "x1", "x2", "x3", "x4", "flag", "y", "is_validation")
}

func binaryConfig() map[string]any {
	return map[string]any{"objective": "binary:logistic", "eval_metric": []any{"logloss", "auc"}, "max_depth": 4.0, "eta": 0.1,
		"lambda": 1.0, "min_child_weight": 1.0, "seed": 7.0, "tree_method": "hist", "num_boost_round": 50.0,
		"data_split_method": "custom", "data_split_col": "is_validation"}
}

// A binary classifier is the Python package's: the same metrics each
// round, the same AUC on the validation rows, the same predictions,
// margins and SHAP values, which add up with the bias to the margin.
func TestXGBoostBinaryMatchesPython(t *testing.T) {
	ref := xgbRef(t)
	tbl := binaryTable(t)
	art := xgbTrain(t, tbl, IglooArgs{Target: "y", Config: binaryConfig()})
	x := art.XGBoost
	t.Logf("XGBoost %s here, %s in Python", x.Version, ref.XGBoost)
	sameModel(t, ref, x, ref.Binary.ModelSHA256)
	if x.Objective != "binary:logistic" || len(x.Classes) != 2 || x.Rows != 1506 || x.ValidationRows != 494 || x.Rounds != 50 {
		t.Fatalf("model %s %v, %d and %d rows, %d rounds", x.Objective, x.Classes, x.Rows, x.ValidationRows, x.Rounds)
	}
	res, err := art.Method("EVALUATE")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, r := range res.Rows {
		got[r[0].(string)] = r[1].(float64)
	}
	t.Logf("validation AUC %.17g here, %.17g in Python; log loss %.17g, %.17g", got["auc"], ref.Binary.ValidationAUC,
		got["logloss"], ref.Binary.ValidationLogloss)
	agree(t, "validation AUC", []float64{got["auc"]}, []float64{ref.Binary.ValidationAUC}, 1e-6)
	agree(t, "validation log loss", []float64{got["logloss"]}, []float64{ref.Binary.ValidationLogloss}, 1e-6)
	info, err := art.Method("TRAINING_INFO")
	if err != nil {
		t.Fatal(err)
	}
	var hist []float64
	var want []float64
	for _, r := range info.Rows {
		it, metric := int(r[0].(int64)), r[1].(string)
		hist = append(hist, r[2].(float64), r[3].(float64))
		want = append(want, ref.Binary.History["train"][metric][it], ref.Binary.History["validation"][metric][it])
	}
	agree(t, "each round's metrics", hist, want, 1e-6)

	preds := scoreRows(t, art, "PREDICT", map[string]any{"output_margin": true}, tbl)
	agree(t, "predictions", field(t, preds, "value"), ref.Binary.Predictions, 1e-5)
	agree(t, "margins", field(t, preds, "margin"), ref.Binary.Margins, 1e-5)
	var p struct {
		Class       string             `json:"class"`
		Probability map[string]float64 `json:"probability"`
	}
	if err := json.Unmarshal([]byte(preds[0]), &p); err != nil || len(p.Probability) != 2 ||
		math.Abs(p.Probability["1"]-ref.Binary.Predictions[0]) > 1e-5 || (p.Class == "1") != (ref.Binary.Predictions[0] > 0.5) {
		t.Errorf("PREDICT %s, %v", preds[0], err)
	}

	// EXPLAIN_PREDICT: every feature's SHAP value and the bias, the
	// Python package's pred_contribs, adding up to the margin.
	explained := scoreRows(t, art, "EXPLAIN_PREDICT", map[string]any{"top_k_features": 5.0}, tbl)
	var contribs []float64
	worst := 0.0
	for i, e := range explained {
		var r struct {
			Attrs []struct {
				Feature     string  `json:"feature"`
				Attribution float64 `json:"attribution"`
			} `json:"top_feature_attributions"`
			Base   float64 `json:"baseline_prediction_value"`
			Value  float64 `json:"prediction_value"`
			Approx float64 `json:"approximation_error"`
		}
		if err := json.Unmarshal([]byte(e), &r); err != nil || len(r.Attrs) != 5 {
			t.Fatalf("EXPLAIN_PREDICT %s: %v", e, err)
		}
		by := map[string]float64{}
		sum := r.Base
		for _, a := range r.Attrs {
			by[a.Feature] = a.Attribution
			sum += a.Attribution
		}
		for _, f := range []string{"x1", "x2", "x3", "x4", "flag"} {
			contribs = append(contribs, by[f])
		}
		contribs = append(contribs, r.Base)
		margin := ref.Binary.Margins[i]
		worst = max(worst, math.Abs(sum-margin), math.Abs(r.Value-margin))
		if r.Approx > 1e-5 {
			t.Errorf("row %d: approximation error %g", i, r.Approx)
		}
	}
	t.Logf("SHAP values and bias add up to the margin within %.3g in every row", worst)
	if worst > 1e-5 {
		t.Errorf("SHAP values and bias add up to the margin within only %g", worst)
	}
	agree(t, "SHAP values and bias of the first 500 rows", contribs[:len(ref.Binary.Contributions)], ref.Binary.Contributions, 1e-5)

	// The stored model loads and predicts the same, to the bit.
	blob, sha, err := art.Encode()
	if err != nil {
		t.Fatal(err)
	}
	again, err := DecodeArtifact(blob, sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"PREDICT", "EXPLAIN_PREDICT"} {
		before, after := scoreRows(t, art, method, nil, tbl), scoreRows(t, again, method, nil, tbl)
		if strings.Join(before, "\n") != strings.Join(after, "\n") {
			t.Errorf("%s: the reloaded model predicts otherwise", method)
		}
	}
	if e, err := again.EvaluateOn(tbl); err != nil || len(e.Rows) != 2 {
		t.Errorf("EVALUATE on given rows: %v, %v", e, err)
	}
	imp, err := again.Method("FEATURE_IMPORTANCE")
	if err != nil || len(imp.Rows) != 5 || imp.Rows[0][0] != "x1" && imp.Rows[0][0] != "x2" || imp.Rows[0][1].(int64) == 0 {
		t.Errorf("FEATURE_IMPORTANCE %v, %v", imp, err)
	}
}

// Early stopping stops where the Python package's does: the validation
// rows' last metric, log loss, has not improved for 10 rounds; the model
// keeps the rounds after the best, and predicts with the best's.
func TestXGBoostEarlyStoppingMatchesPython(t *testing.T) {
	all := xgbRef(t)
	ref := all.EarlyStopping
	tbl := binaryTable(t)
	art := xgbTrain(t, tbl, IglooArgs{Target: "y", Config: map[string]any{"objective": "binary:logistic",
		"eval_metric": []any{"auc", "logloss"}, "max_depth": 6.0, "eta": 0.3, "seed": 7.0, "num_boost_round": 500.0,
		"early_stopping_rounds": 10.0, "data_split_method": "custom", "data_split_col": "is_validation"}})
	x := art.XGBoost
	sameModel(t, all, x, ref.ModelSHA256)
	t.Logf("best iteration %d of %d rounds here, %d of %d in Python", x.Best, x.Rounds, ref.BestIteration, ref.Rounds)
	if x.Best != ref.BestIteration || x.Rounds != ref.Rounds {
		t.Errorf("best iteration %d of %d rounds, the Python package's %d of %d", x.Best, x.Rounds, ref.BestIteration, ref.Rounds)
	}
	res, _ := art.Method("EVALUATE")
	for _, r := range res.Rows {
		if r[0] == "logloss" {
			agree(t, "best score", []float64{r[1].(float64)}, []float64{ref.BestScore}, 1e-6)
		}
	}
	info, _ := art.Method("TRAINING_INFO")
	best := 0
	for _, r := range info.Rows {
		if r[4].(bool) {
			best++
			if int(r[0].(int64)) != ref.BestIteration {
				t.Errorf("TRAINING_INFO marks iteration %v best", r[0])
			}
		}
	}
	if best != 2 || len(info.Rows) != 2*ref.Rounds {
		t.Errorf("TRAINING_INFO has %d rows, %d best", len(info.Rows), best)
	}
	agree(t, "predictions at the best iteration", field(t, scoreRows(t, art, "PREDICT", nil, tbl), "value"), ref.Predictions, 1e-5)
}

// WEIGHT_COLNAME weighs each row as the Python package's DMatrix weight.
func TestXGBoostWeightedRegressionMatchesPython(t *testing.T) {
	all := xgbRef(t)
	ref := all.WeightedRegression
	tbl := only(readCSV(t, "testdata/xgboost/binary.csv"), "x1", "x2", "x3", "x4", "flag", "w", "value")
	art := xgbTrain(t, tbl, IglooArgs{Target: "value", Weight: "w", Config: map[string]any{"objective": "reg:squarederror",
		"max_depth": 3.0, "learning_rate": 0.2, "seed": 3.0, "num_boost_round": 30.0}})
	sameModel(t, all, art.XGBoost, ref.ModelSHA256)
	agree(t, "predictions", field(t, scoreRows(t, art, "PREDICT", nil, tbl), "value"), ref.Predictions, 1e-5)
	info, _ := art.Method("TRAINING_INFO")
	var rmse []float64
	for _, r := range info.Rows {
		rmse = append(rmse, r[2].(float64))
	}
	agree(t, "each round's weighted RMSE", rmse, ref.History["train"]["rmse"], 1e-6)
	if _, err := art.Method("EVALUATE"); err == nil || !strings.Contains(err.Error(), "no rows out of training") {
		t.Errorf("EVALUATE without validation rows: %v", err)
	}
}

// survival:aft trains on interval labels, right-censored rows' upper
// bound +Infinity, as the Python package's does.
func TestXGBoostAFTMatchesPython(t *testing.T) {
	all := xgbRef(t)
	ref := all.AFT
	tbl := readCSV(t, "testdata/xgboost/survival.csv")
	art := xgbTrain(t, tbl, IglooArgs{Lower: "time_lower", Upper: "time_upper", Config: map[string]any{"objective": "survival:aft",
		"eval_metric": "aft-nloglik", "aft_loss_distribution": "normal", "aft_loss_distribution_scale": 1.0, "tree_method": "hist",
		"learning_rate": 0.05, "max_depth": 3.0, "lambda": 0.01, "alpha": 0.02, "num_boost_round": 100.0}})
	sameModel(t, all, art.XGBoost, ref.ModelSHA256)
	agree(t, "predicted times", field(t, scoreRows(t, art, "PREDICT", nil, tbl), "value"), ref.Predictions, 1e-5)
	info, _ := art.Method("TRAINING_INFO")
	last := info.Rows[len(info.Rows)-1]
	t.Logf("final aft-nloglik %.17g here, %.17g in Python", last[2], ref.FinalNLogLik)
	agree(t, "final aft-nloglik", []float64{last[2].(float64)}, []float64{ref.FinalNLogLik}, 1e-6)
	e, err := art.EvaluateOn(tbl)
	if err != nil || len(e.Rows) != 1 || e.Rows[0][0] != "aft-nloglik" {
		t.Fatalf("EVALUATE on given rows: %v, %v", e, err)
	}
	agree(t, "EVALUATE's aft-nloglik", []float64{e.Rows[0][1].(float64)}, []float64{ref.FinalNLogLik}, 1e-6)
}

// Parameters XGBoost refuses, or does not use, come back with its
// message; data it cannot train on is refused saying why.
func TestXGBoostRefusals(t *testing.T) {
	tbl := binaryTable(t)
	with := func(extra map[string]any) map[string]any {
		cfg := map[string]any{"objective": "binary:logistic", "num_boost_round": 2.0}
		for k, v := range extra {
			cfg[k] = v
		}
		return cfg
	}
	text := &TableCol{Name: "region", Type: TypeString, TypeName: "VARCHAR", Str: make([]string, tbl.N), Null: make([]bool, tbl.N)}
	withText := &Table{N: tbl.N, Cols: append(append([]*TableCol{}, tbl.Cols...), text)}
	for _, c := range []struct {
		tbl  *Table
		args IglooArgs
		want string
	}{
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"objective": "binary:nonsense"})}, "XGBoost: Unknown objective function: `binary:nonsense`"},
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"max_dept": 3.0})}, `does not use these parameters here: { "max_dept" }`},
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"max_depth": "deep"})}, "XGBoost: Invalid Parameter format for max_depth"},
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"objective": "multi:softprob"})}, "XGBoost: value 0 for Parameter num_class should be greater equal to 1"},
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"early_stopping_rounds": 5.0})}, "early_stopping_rounds stops on validation rows"},
		{tbl, IglooArgs{Target: "y", Config: with(map[string]any{"data_split_method": "custom"})}, "needs 'data_split_col'"},
		{tbl, IglooArgs{Target: "x1", Lower: "x2", Upper: "x3", Config: with(nil)}, "not both"},
		{tbl, IglooArgs{Config: with(nil)}, "needs TARGET_COLNAME"},
		{withText, IglooArgs{Target: "y", Config: with(nil)}, "column region is text"},
		{withText, IglooArgs{Target: "region", Config: with(nil)}, "XGBoost's labels are numbers"},
		{tbl, IglooArgs{Lower: "x1", Upper: "x2", Config: with(map[string]any{"objective": "survival:aft"})}, "x1 is NULL in a row"},
	} {
		_, _, err := TrainIgloo(ClassXGBoost, c.tbl, c.args)
		if err == nil || !strings.Contains(err.Error(), c.want) || !InputError(err) {
			t.Errorf("%v: %v, want %q", c.args.Config, err, c.want)
		}
	}
	art := xgbTrain(t, tbl, IglooArgs{Target: "y", Config: with(nil)})
	for _, c := range []struct {
		method string
		cfg    map[string]any
		want   string
	}{
		{"PREDICT", map[string]any{"top_k_features": 2.0}, "unknown CONFIG_OBJECT key top_k_features"},
		{"EXPLAIN_PREDICT", map[string]any{"output_margin": true}, "unknown CONFIG_OBJECT key output_margin"},
		{"DETECT_ANOMALIES", nil, "have no row method DETECT_ANOMALIES"},
	} {
		if _, err := art.RowMethod(c.method, c.cfg); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s %v: %v", c.method, c.cfg, err)
		}
	}
}

// Classification objectives give a class, and their classes'
// probabilities where XGBoost has them: softprob's own, softmax's from its
// margins, none for hinge. A multiclass model explains its predicted
// class's margin.
func TestXGBoostClassObjectives(t *testing.T) {
	base := readCSV(t, "testdata/xgboost/binary.csv")
	tier := &TableCol{Name: "tier", Type: TypeNumber, TypeName: "BIGINT", SQLType: "BIGINT", Num: make([]float64, base.N), Null: make([]bool, base.N)}
	for i, v := range base.Col("value").Num {
		tier.Num[i] = float64(min(max(int(math.Floor(v/2))+1, 0), 2))
	}
	tbl := only(base, "x1", "x2", "x3", "x4", "flag")
	tbl.Cols = append(tbl.Cols, tier)
	for _, objective := range []string{"multi:softprob", "multi:softmax", "binary:hinge"} {
		target, cfg := "tier", map[string]any{"objective": objective, "num_class": 3.0, "max_depth": 3.0, "num_boost_round": 20.0}
		if objective == "binary:hinge" {
			target, tbl = "y", only(base, "x1", "x2", "x3", "x4", "flag", "y")
			delete(cfg, "num_class")
		}
		art := xgbTrain(t, tbl, IglooArgs{Target: target, Config: cfg})
		x := art.XGBoost
		if want := map[bool]int{true: 2, false: 3}[objective == "binary:hinge"]; len(x.Classes) != want {
			t.Fatalf("%s: classes %v", objective, x.Classes)
		}
		preds := scoreRows(t, art, "PREDICT", nil, head(tbl, 200))
		explained := scoreRows(t, art, "EXPLAIN_PREDICT", map[string]any{"top_k_features": 5.0}, head(tbl, 200))
		for i, o := range preds {
			var p struct {
				Value       any                `json:"value"`
				Class       string             `json:"class"`
				Probability map[string]float64 `json:"probability"`
			}
			if err := json.Unmarshal([]byte(o), &p); err != nil {
				t.Fatal(err)
			}
			if objective == "binary:hinge" {
				if p.Probability != nil || p.Class != strconv.Itoa(int(p.Value.(float64))) {
					t.Fatalf("%s: %s", objective, o)
				}
				continue
			}
			sum, best := 0.0, ""
			for c, v := range p.Probability {
				sum += v
				if best == "" || v > p.Probability[best] {
					best = c
				}
			}
			if len(p.Probability) != 3 || math.Abs(sum-1) > 1e-5 || p.Class != best {
				t.Fatalf("%s: %s", objective, o)
			}
			var e struct {
				Class string `json:"class"`
				Attrs []struct {
					Attribution float64 `json:"attribution"`
				} `json:"top_feature_attributions"`
				Base   float64 `json:"baseline_prediction_value"`
				Margin float64 `json:"prediction_value"`
			}
			if err := json.Unmarshal([]byte(explained[i]), &e); err != nil || e.Class != p.Class {
				t.Fatalf("%s: %s, %v", objective, explained[i], err)
			}
			total := e.Base
			for _, a := range e.Attrs {
				total += a.Attribution
			}
			if math.Abs(total-e.Margin) > 1e-5 {
				t.Fatalf("%s: %s does not add up", objective, explained[i])
			}
		}
	}
}
