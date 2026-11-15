package pgwire

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/config"
)

// xgbTestdata is the directory of IGLOO.ML.XGBOOST's data and of what
// XGBoost's Python package trained on it (see package models' tests).
func xgbTestdata(t *testing.T) (string, map[string]json.RawMessage) {
	t.Helper()
	dir, err := filepath.Abs("../models/testdata/xgboost")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "reference.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ref map[string]json.RawMessage
	if err := json.Unmarshal(b, &ref); err != nil {
		t.Fatal(err)
	}
	return dir, ref
}

func xgbFloats(t *testing.T, rows []string) []float64 {
	t.Helper()
	out := make([]float64, len(rows))
	for i, r := range rows {
		v, err := strconv.ParseFloat(r[strings.LastIndexByte(r, '|')+1:], 64)
		if err != nil {
			t.Fatalf("row %q: %v", r, err)
		}
		out[i] = v
	}
	return out
}

// sameFloat32s reports how many of got are want's float32s, as XGBoost
// computes them, and fails unless the rest are within tol, relative to
// values above 1 (other platforms than the reference's round a little
// otherwise; see package models' tests).
func sameFloat32s(t *testing.T, what string, got, want []float64, tol float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d values, want %d", what, len(got), len(want))
	}
	same, worst := 0, 0.0
	for i := range got {
		if float32(got[i]) == float32(want[i]) {
			same++
			continue
		}
		worst = max(worst, math.Abs(got[i]-want[i])/max(1, math.Abs(want[i])))
	}
	t.Logf("%s: %d of %d the Python package's float32, the rest within %.3g", what, same, len(got), worst)
	if !(worst <= tol) {
		t.Errorf("%s differ from the Python package's by up to %g", what, worst)
	}
}

// IGLOO.ML.XGBOOST in SQL: the model XGBoost's Python package trains on
// the same rows with the same parameters, its methods in a query and as
// tables, and XGBoost's refusals as SQL errors.
func TestIglooXGBoost(t *testing.T) {
	t.Parallel()
	dir, raw := xgbTestdata(t)
	var ref struct {
		Binary struct {
			ValidationAUC float64   `json:"validation_auc"`
			Predictions   []float64 `json:"predictions"`
			Margins       []float64 `json:"margins"`
			Contributions []float64 `json:"contributions"`
		} `json:"binary"`
		AFT struct {
			FinalNLogLik float64   `json:"final_aft_nloglik"`
			Predictions  []float64 `json:"predictions"`
		} `json:"aft"`
	}
	for k, dst := range map[string]any{"binary": &ref.Binary, "aft": &ref.AFT} {
		if err := json.Unmarshal(raw[k], dst); err != nil {
			t.Fatal(err)
		}
	}
	cs := startCatalogServerWithConfig(t, config.Config{Storage: config.Storage{AllowedLocations: []string{dir + "/"}}})
	mustExec(t, cs.admin, "CREATE TABLE lake.public.leads AS SELECT * FROM read_csv('"+dir+"/binary.csv')")
	mustExec(t, cs.admin, "CREATE TABLE lake.public.survival AS SELECT row_number() OVER () AS id, z1, z2, z3, time_lower, "+
		"CAST(time_upper AS DOUBLE) AS time_upper FROM read_csv('"+dir+"/survival.csv')")
	conn := snowflakeConn(t, cs.admin)
	mustExec(t, conn, `CREATE IGLOO.ML.XGBOOST lead_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT x1, x2, x3, x4, flag, y, is_validation FROM leads ORDER BY id'),
		TARGET_COLNAME => 'y',
		CONFIG_OBJECT => {'objective': 'binary:logistic', 'eval_metric': ['logloss', 'auc'], 'max_depth': 4, 'eta': 0.1,
			'lambda': 1, 'min_child_weight': 1, 'seed': 7, 'tree_method': 'hist', 'num_boost_round': 50,
			'data_split_method': 'custom', 'data_split_col': 'is_validation'})`)

	auc := texts(t, conn, "SELECT value FROM TABLE(lead_model!EVALUATE()) WHERE metric = 'auc'")
	sameFloat32s(t, "validation AUC", xgbFloats(t, auc), []float64{ref.Binary.ValidationAUC}, 1e-6)
	sameFloat32s(t, "predictions in a query", xgbFloats(t, texts(t, conn,
		"SELECT id, lead_model!PREDICT(INPUT_DATA => {*}):value::FLOAT FROM leads ORDER BY id")), ref.Binary.Predictions, 1e-5)
	sameFloat32s(t, "margins", xgbFloats(t, texts(t, conn, `SELECT id, lead_model!PREDICT(INPUT_DATA => {*},
		CONFIG_OBJECT => {'output_margin': TRUE}):margin::FLOAT FROM leads ORDER BY id`)), ref.Binary.Margins, 1e-5)
	sameFloat32s(t, "class 1's probabilities as a table", xgbFloats(t, texts(t, conn, `SELECT id, predicted_y_probs:"1"::FLOAT
		FROM TABLE(lead_model!PREDICT(INPUT_DATA => TABLE(leads))) ORDER BY id`)), ref.Binary.Predictions, 1e-5)
	if n := count(t, conn, `SELECT count(*) FROM TABLE(lead_model!PREDICT(INPUT_DATA => TABLE(leads)))
		WHERE predicted_y = CASE WHEN predicted_y_probs:"1"::FLOAT > 0.5 THEN 1 ELSE 0 END`); n != 2000 {
		t.Errorf("%d of 2000 classes follow their probability", n)
	}

	// SHAP values with the bias, as pred_contribs gives them, adding up to
	// the margin.
	rows := texts(t, conn, `SELECT TO_JSON(top_feature_attributions), baseline_prediction_value, prediction_value, approximation_error
		FROM TABLE(lead_model!EXPLAIN_PREDICT(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM leads ORDER BY id LIMIT 500'),
			CONFIG_OBJECT => {'top_k_features': 5})) ORDER BY id`)
	if len(rows) != 500 {
		t.Fatalf("EXPLAIN_PREDICT gave %d rows", len(rows))
	}
	var contribs []float64
	worst := 0.0
	for i, r := range rows {
		parts := strings.Split(r, "|")
		var attrs []struct {
			Feature     string  `json:"feature"`
			Attribution float64 `json:"attribution"`
		}
		if err := json.Unmarshal([]byte(parts[0]), &attrs); err != nil || len(attrs) != 5 {
			t.Fatalf("attributions %q: %v", parts[0], err)
		}
		by := map[string]float64{}
		base, _ := strconv.ParseFloat(parts[1], 64)
		value, _ := strconv.ParseFloat(parts[2], 64)
		sum := base
		for _, a := range attrs {
			by[a.Feature] = a.Attribution
			sum += a.Attribution
		}
		for _, f := range []string{"x1", "x2", "x3", "x4", "flag"} {
			contribs = append(contribs, by[f])
		}
		contribs = append(contribs, base)
		worst = max(worst, math.Abs(sum-ref.Binary.Margins[i]), math.Abs(value-ref.Binary.Margins[i]))
	}
	t.Logf("SHAP values and bias add up to the margin within %.3g", worst)
	if worst > 1e-5 {
		t.Errorf("SHAP values add up to the margin within only %g", worst)
	}
	sameFloat32s(t, "SHAP values and bias", contribs, ref.Binary.Contributions, 1e-5)

	if got := texts(t, conn, `SELECT count(*), count(*) FILTER (WHERE is_best_iteration AND iteration = 49), max(iteration)
		FROM TABLE(lead_model!TRAINING_INFO()) WHERE validation_value IS NOT NULL AND training_value IS NOT NULL`); len(got) != 1 ||
		got[0] != "100|2|49" {
		t.Errorf("TRAINING_INFO: %q", got)
	}
	if got := texts(t, conn, "SELECT count(*), sum(importance_weight) > 0 FROM TABLE(lead_model!FEATURE_IMPORTANCE())"); len(got) != 1 ||
		got[0] != "5|true" {
		t.Errorf("FEATURE_IMPORTANCE: %q", got)
	}
	if got := texts(t, conn, "CALL lead_model!EVALUATE()"); len(got) != 2 {
		t.Errorf("CALL EVALUATE: %q", got)
	}

	// survival:aft, on interval labels: right-censored rows' upper bound
	// is +Infinity.
	mustExec(t, conn, `CREATE IGLOO.ML.XGBOOST survival_model(
		INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT z1, z2, z3, time_lower, time_upper FROM survival ORDER BY id'),
		LABEL_LOWER_BOUND_COLNAME => 'time_lower', LABEL_UPPER_BOUND_COLNAME => 'time_upper',
		CONFIG_OBJECT => {'objective': 'survival:aft', 'eval_metric': 'aft-nloglik', 'aft_loss_distribution': 'normal',
			'aft_loss_distribution_scale': 1.0, 'tree_method': 'hist', 'learning_rate': 0.05, 'max_depth': 3, 'lambda': 0.01,
			'alpha': 0.02, 'num_boost_round': 100})`)
	sameFloat32s(t, "predicted times", xgbFloats(t, texts(t, conn, `SELECT id, predicted_label
		FROM TABLE(survival_model!PREDICT(INPUT_DATA => TABLE(survival))) ORDER BY id`)), ref.AFT.Predictions, 1e-5)
	sameFloat32s(t, "aft-nloglik", xgbFloats(t, texts(t, conn, `SELECT value
		FROM TABLE(survival_model!EVALUATE(INPUT_DATA => TABLE(survival))) WHERE metric = 'aft-nloglik'`)), []float64{ref.AFT.FinalNLogLik}, 1e-6)

	if got := texts(t, conn, "SHOW IGLOO.ML.XGBOOST"); len(got) != 2 {
		t.Errorf("SHOW: %q", got)
	}
	for sql, want := range map[string][2]string{
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'y', CONFIG_OBJECT => {'objective': 'binary:nonsense'})`: {
			"22023", "XGBoost: Unknown objective function: `binary:nonsense`"},
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'y', CONFIG_OBJECT => {'max_depth': -2})`: {
			"22023", "XGBoost: value -2 for Parameter max_depth should be greater equal to 0"},
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'y', CONFIG_OBJECT => {'n_estimators': 100})`: {
			"22023", `XGBoost does not use these parameters here: { "n_estimators" }`},
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'value', CONFIG_OBJECT => {'objective': 'binary:logistic'})`: {
			"22023", "XGBoost: label must be in (0, 1) for logistic regression"},
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'y', CONFIG_OBJECT => {'early_stopping_rounds': 5})`: {
			"22023", "early_stopping_rounds stops on validation rows"},
		`CREATE IGLOO.ML.XGBOOST m(INPUT_DATA => TABLE(leads), TARGET_COLNAME => 'y', USER_COLNAME => 'id')`: {
			"42601", "takes INPUT_DATA, CONFIG_OBJECT, TARGET_COLNAME"},
		`SELECT lead_model!PREDICT(INPUT_DATA => {*}, CONFIG_OBJECT => {'top_k_features': 3}) FROM leads`: {
			"22023", "unknown CONFIG_OBJECT key top_k_features"},
		`CALL lead_model!WEIGHTS()`: {"42883", "has no method WEIGHTS"},
	} {
		expectCode(t, conn, sql, want[0], want[1])
	}
	mustExec(t, conn, "DROP IGLOO.ML.XGBOOST survival_model")
}

// Workers score an XGBoost model's rows, loading it from the warehouse,
// as the server does.
func TestDistributedXGBoostScoring(t *testing.T) {
	t.Parallel()
	ds := startDistServer(t, 2)
	mustExec(t, ds.admin, campaignsTable)
	conn := snowflakeConn(t, ds.admin)
	mustExec(t, conn, `CREATE IGLOO.ML.XGBOOST converts(INPUT_DATA => SYSTEM$QUERY_REFERENCE(
		'SELECT spend, visits, region = ''south'' AS south, converted FROM campaigns'), TARGET_COLNAME => 'converted',
		CONFIG_OBJECT => {'objective': 'binary:logistic', 'max_depth': 3, 'num_boost_round': 20})`)
	for _, query := range []string{
		`SELECT region, count(*), round(sum(predicted_converted_probs:"True"::FLOAT), 3) FROM TABLE(converts!PREDICT(
			INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT *, region = ''south'' AS south FROM campaigns'))) WHERE predicted_converted
			GROUP BY region ORDER BY region`,
		`SELECT region, count(*) FROM campaigns WHERE converts!EXPLAIN_PREDICT(INPUT_DATA => {'spend': spend, 'visits': visits,
			'south': region = 'south'}):class::STRING = 'True' GROUP BY region ORDER BY region`,
	} {
		mustExec(t, conn, "SET distributed_execution = off")
		local := texts(t, conn, query)
		mustExec(t, conn, "SET distributed_execution = on")
		if got := texts(t, conn, query); strings.Join(got, ";") != strings.Join(local, ";") || len(got) != 4 {
			t.Errorf("on workers: %q; here: %q", got, local)
		}
		if q := newestRun(t, ds.history, "igloo_ml_predict("); q.Workers != 2 {
			t.Errorf("%d workers scored, want 2 (%s)", q.Workers, q.Text)
		}
	}
}
