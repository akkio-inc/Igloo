package models

import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/ml"
)

// campaignTable is a synthetic table of campaign rows: region, spend and
// visits, and three targets planted on them: value = 2·spend − 3·visits +
// a region's effect + noise; converted, a logistic draw on the same; tier,
// three classes by value.
func campaignTable(n int, seed uint64) *Table {
	r := rand.New(rand.NewPCG(seed, seed))
	regions := []string{"north", "south", "east", "west"}
	effect := map[string]float64{"north": 0, "south": 5, "east": -2, "west": 1}
	t := &Table{N: n}
	region := &TableCol{Name: "region", Type: TypeString, TypeName: "VARCHAR", SQLType: "VARCHAR", Str: make([]string, n), Null: make([]bool, n)}
	spend := &TableCol{Name: "spend", Type: TypeNumber, TypeName: "DOUBLE", SQLType: "DOUBLE", Num: make([]float64, n), Null: make([]bool, n)}
	visits := &TableCol{Name: "visits", Type: TypeNumber, TypeName: "DOUBLE", SQLType: "DOUBLE", Num: make([]float64, n), Null: make([]bool, n)}
	value := &TableCol{Name: "value", Type: TypeNumber, TypeName: "DOUBLE", SQLType: "DOUBLE", Num: make([]float64, n), Null: make([]bool, n)}
	converted := &TableCol{Name: "converted", Type: TypeBool, TypeName: "BOOLEAN", SQLType: "BOOLEAN", Str: make([]string, n), Null: make([]bool, n)}
	tier := &TableCol{Name: "tier", Type: TypeString, TypeName: "VARCHAR", SQLType: "VARCHAR", Str: make([]string, n), Null: make([]bool, n)}
	for i := range n {
		region.Str[i] = regions[r.IntN(4)]
		spend.Num[i] = r.Float64() * 10
		visits.Num[i] = r.Float64() * 5
		v := 2*spend.Num[i] - 3*visits.Num[i] + effect[region.Str[i]]
		value.Num[i] = v + r.NormFloat64()
		converted.Str[i] = strconv.FormatBool(r.Float64() < 1/(1+math.Exp(-(v-3)/2)))
		switch {
		case v < 0:
			tier.Str[i] = "low"
		case v < 8:
			tier.Str[i] = "mid"
		default:
			tier.Str[i] = "high"
		}
	}
	t.Cols = []*TableCol{region, spend, visits, value, converted, tier}
	return t
}

func trainIgloo(t *testing.T, class string, tbl *Table, a IglooArgs) *Artifact {
	t.Helper()
	art, _, err := TrainIgloo(class, tbl, a)
	if err != nil {
		t.Fatalf("%s: %v", class, err)
	}
	art.Prepare()
	return art
}

// only keeps the table's columns named.
func only(t *Table, names ...string) *Table {
	out := &Table{N: t.N}
	for _, n := range names {
		out.Cols = append(out.Cols, t.Col(n))
	}
	return out
}

// head returns the table's first n rows.
func head(t *Table, n int) *Table {
	rows := make([]int, min(n, t.N))
	for i := range rows {
		rows[i] = i
	}
	return t.Take(rows)
}

// scoreRows scores every row of t with a row method, as igloo_ml_predict
// does.
func scoreRows(t *testing.T, art *Artifact, method string, cfg map[string]any, tbl *Table) []string {
	t.Helper()
	rm, err := art.RowMethod(method, cfg)
	if err != nil {
		t.Fatal(err)
	}
	kinds := make([]ml.Kind, len(rm.Features))
	for j, f := range rm.Features {
		kinds[j] = featureKind(f)
	}
	var out []string
	var row ml.RowValues
	for i := range tbl.N {
		vals := make([]any, len(rm.Features))
		for j, f := range rm.Features {
			c := tbl.Col(f.Name)
			switch {
			case c == nil || c.Null[i]:
			case c.Type == TypeNumber:
				vals[j] = c.Num[i]
			default:
				vals[j] = c.Str[i]
			}
		}
		toRowAny(vals, kinds, &row)
		out = append(out, string(rm.Score(art, nil, &row)))
	}
	return out
}

func toRowAny(vals []any, kinds []ml.Kind, r *ml.RowValues) {
	dv := make([]driver.Value, len(vals))
	for i, v := range vals {
		dv[i] = v
	}
	ToRow(dv, kinds, r)
}

func metric(t *testing.T, art *Artifact, name string) float64 {
	t.Helper()
	res, err := art.Method("EVALUATE")
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range res.Columns {
		if strings.EqualFold(c.Name, name) {
			v, _ := res.Rows[0][i].(float64)
			return v
		}
	}
	t.Fatalf("no metric %s", name)
	return 0
}

func TestRegressionAlgorithms(t *testing.T) {
	tbl := only(campaignTable(3000, 1), "region", "spend", "visits", "value")
	for _, alg := range []string{"boosted_trees", "random_forest", "linear"} {
		art := trainIgloo(t, ClassRegression, tbl, IglooArgs{Target: "value", Config: map[string]any{"algorithm": alg}})
		if r2 := metric(t, art, "r2_score"); r2 < 0.9 {
			t.Errorf("%s: R² %.3f on held-out rows", alg, r2)
		}
		// The same rows give the same blob.
		again := trainIgloo(t, ClassRegression, tbl, IglooArgs{Target: "value", Config: map[string]any{"algorithm": alg}})
		_, sha1, _ := art.Encode()
		_, sha2, _ := again.Encode()
		if sha1 != sha2 {
			t.Errorf("%s: two trainings differ", alg)
		}
		// Scoring a row as igloo_ml_predict does matches the frame's.
		scores := scoreRows(t, art, "PREDICT", nil, head(tbl, 5))
		f, _ := art.Supervised.frameOf(tbl)
		pred, _ := art.Supervised.predictFrame(f)
		for i := range 5 {
			var got struct{ Value float64 }
			if err := json.Unmarshal([]byte(scores[i]), &got); err != nil || math.Abs(got.Value-pred[i]) > 1e-9*math.Max(1, math.Abs(pred[i])) {
				t.Errorf("%s row %d: %s, not %v", alg, i, scores[i], pred[i])
			}
		}
		// EXPLAIN_PREDICT's attributions add up to the prediction.
		for _, js := range scoreRows(t, art, "EXPLAIN_PREDICT", map[string]any{"top_k_features": 10.0}, head(only(tbl, "region", "spend", "visits"), 5)) {
			var e struct {
				Value float64 `json:"value"`
				Base  float64 `json:"baseline_prediction_value"`
				Pred  float64 `json:"prediction_value"`
				Attrs []struct {
					Feature     string  `json:"feature"`
					Attribution float64 `json:"attribution"`
				} `json:"top_feature_attributions"`
			}
			if err := json.Unmarshal([]byte(js), &e); err != nil {
				t.Fatal(err)
			}
			sum := e.Base
			for _, a := range e.Attrs {
				sum += a.Attribution
			}
			if math.Abs(sum-e.Value) > 1e-6 || math.Abs(e.Pred-e.Value) > 1e-6 || len(e.Attrs) != 3 {
				t.Errorf("%s: %s does not add up", alg, js)
			}
		}
		imp, _ := art.Method("GLOBAL_EXPLAIN")
		if imp.Rows[0][0] != "spend" {
			t.Errorf("%s: the most important feature is %v", alg, imp.Rows[0])
		}
	}
	// A linear model recovers the planted coefficients, with their
	// standard errors.
	art := trainIgloo(t, ClassRegression, tbl, IglooArgs{Target: "value", Config: map[string]any{"algorithm": "linear"}})
	w, err := art.Method("WEIGHTS")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"spend": 2, "visits": -3}
	for _, row := range w.Rows {
		name, _ := row[1].(string)
		if c, ok := want[name]; ok {
			if v := row[3].(float64); math.Abs(v-c) > 0.05 {
				t.Errorf("weight of %s %v, not %v", name, v, c)
			}
			if se, ok := row[4].(float64); !ok || se <= 0 || se > 0.05 || row[5].(float64) > 1e-6 {
				t.Errorf("%s: standard error %v, p-value %v", name, row[4], row[5])
			}
		}
	}
	if _, err := art.Method("FEATURE_IMPORTANCE"); err == nil {
		t.Error("a linear model has no tree importance")
	}
	// Unknown options are refused.
	if _, _, err := TrainIgloo(ClassRegression, tbl, IglooArgs{Target: "value", Config: map[string]any{"learn_rte": 0.1}}); err == nil ||
		!strings.Contains(err.Error(), "unknown CONFIG_OBJECT key learn_rte") {
		t.Errorf("a misspelled option: %v", err)
	}
}

func TestClassifierAlgorithms(t *testing.T) {
	base := campaignTable(3000, 2)
	for _, target := range []string{"converted", "tier"} {
		tbl := only(base, "region", "spend", "visits", target)
		for _, alg := range []string{"boosted_trees", "random_forest", "logistic"} {
			art := trainIgloo(t, ClassIglooClassifier, tbl, IglooArgs{Target: target, Config: map[string]any{"algorithm": alg}})
			min := 0.75
			if target == "tier" {
				min = 0.9
			}
			if acc := metric(t, art, "accuracy"); acc < min {
				t.Errorf("%s %s: accuracy %.3f", target, alg, acc)
			}
			if auc := metric(t, art, "roc_auc"); auc < 0.85 {
				t.Errorf("%s %s: AUC %.3f", target, alg, auc)
			}
			cm, err := art.Method("CONFUSION_MATRIX")
			if err != nil || len(cm.Rows) != len(art.Supervised.Classes)*len(art.Supervised.Classes) {
				t.Errorf("%s %s: confusion %v, %v", target, alg, cm, err)
			}
			_, err = art.Method("ROC_CURVE")
			if (err == nil) != (target == "converted") {
				t.Errorf("%s %s: ROC_CURVE: %v", target, alg, err)
			}
			for _, js := range scoreRows(t, art, "PREDICT", nil, head(tbl, 5)) {
				var p struct {
					Class       string             `json:"class"`
					Probability map[string]float64 `json:"probability"`
				}
				if err := json.Unmarshal([]byte(js), &p); err != nil {
					t.Fatal(err)
				}
				var sum float64
				for _, v := range p.Probability {
					sum += v
				}
				if math.Abs(sum-1) > 1e-9 || p.Probability[p.Class] < 1/float64(len(p.Probability)) {
					t.Errorf("%s %s: %s", target, alg, js)
				}
			}
			for _, js := range scoreRows(t, art, "EXPLAIN_PREDICT", nil, head(tbl, 3)) {
				if !strings.Contains(js, `"top_feature_attributions":[{"feature":`) {
					t.Errorf("%s %s: %s", target, alg, js)
				}
			}
		}
	}
	// Booleans are classes True and False, as Snowflake's.
	art := trainIgloo(t, ClassIglooClassifier, only(base, "spend", "visits", "converted"), IglooArgs{Target: "converted"})
	if c := art.Supervised.Classes; len(c) != 2 || c[0] != "False" || c[1] != "True" {
		t.Errorf("classes %v", c)
	}
	if _, _, err := TrainIgloo(ClassIglooClassifier, only(base, "spend", "converted"), IglooArgs{Target: "converted",
		Config: map[string]any{"algorithm": "linear"}}); err == nil || !strings.Contains(err.Error(), "algorithm is one of") {
		t.Errorf("a regression's algorithm for a classifier: %v", err)
	}
}

// Every class refuses options it does not take before it trains, and
// takes those it does.
func TestOptionsCheckedFirst(t *testing.T) {
	for class, cfg := range map[string]map[string]any{
		ClassRegression:       {"algorithm": "random_forest", "num_parallel_tree": 10.0, "data_split_method": "no_split"},
		ClassIglooClassifier:  {"algorithm": "logistic", "l2_reg": 1.0, "calculate_p_values": true},
		ClassDNNRegressor:     {"hidden_units": []any{8.0}, "dropout": 0.1},
		ClassDNNClassifier:    {"activation_fn": "tanh", "optimizer": "adam"},
		ClassAutoMLRegressor:  {"budget_hours": 0.1, "optimization_objective": "minimize_mae"},
		ClassAutoMLClassifier: {"max_trials": 2.0, "cv_folds": 4.0},
		ClassKMeans:           {"num_clusters": 3.0, "kmeans_init_method": "KMEANS++"},
		ClassPCA:              {"num_principal_components": 2.0, "pca_solver": "full"},
		ClassFactorization:    {"feedback_type": "implicit", "wals_alpha": 10.0},
		ClassAutoencoder:      {"hidden_units": []any{8.0, 2.0, 8.0}, "seed": 3.0},
	} {
		if err := checkOptions(class, cfg); err != nil {
			t.Errorf("%s: %v", class, err)
		}
		bad := map[string]any{"learn_rte": 0.1}
		for k, v := range cfg {
			bad[k] = v
		}
		if err := checkOptions(class, bad); err == nil || !strings.Contains(err.Error(), "unknown CONFIG_OBJECT key learn_rte") {
			t.Errorf("%s with a misspelled key: %v", class, err)
		}
	}
	// KMEANS and PCA hold no rows out.
	if err := checkOptions(ClassKMeans, map[string]any{"data_split_method": "random"}); err == nil {
		t.Error("KMEANS took data_split_method")
	}
	// The search's options are refused before it starts.
	if _, _, err := TrainIgloo(ClassAutoMLRegressor, nil, IglooArgs{Target: "v", Config: map[string]any{"budget_hour": 1.0}}); err == nil ||
		!strings.Contains(err.Error(), "budget_hour") {
		t.Errorf("a misspelled budget: %v", err)
	}
}
