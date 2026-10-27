package models

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestDNNModels(t *testing.T) {
	base := campaignTable(2000, 7)
	small := map[string]any{"hidden_units": []any{16.0, 8.0}, "learn_rate": 0.01}
	reg := trainIgloo(t, ClassDNNRegressor, only(base, "region", "spend", "visits", "value"), IglooArgs{Target: "value", Config: small})
	if r2 := metric(t, reg, "r2_score"); r2 < 0.95 {
		t.Errorf("DNN_REGRESSOR R² %.3f", r2)
	}
	again := trainIgloo(t, ClassDNNRegressor, only(base, "region", "spend", "visits", "value"), IglooArgs{Target: "value", Config: small})
	_, sha1, _ := reg.Encode()
	_, sha2, _ := again.Encode()
	if sha1 != sha2 {
		t.Error("two trainings differ")
	}
	tbl := only(base, "region", "spend", "visits")
	f, _ := reg.Supervised.frameOf(tbl)
	pred, _ := reg.Supervised.predictFrame(f)
	for i, js := range scoreRows(t, reg, "PREDICT", nil, head(tbl, 3)) {
		var p struct{ Value float64 }
		if err := json.Unmarshal([]byte(js), &p); err != nil || math.Abs(p.Value-pred[i]) > 1e-9 {
			t.Errorf("row %d: %s, not %v", i, js, pred[i])
		}
	}
	imp, _ := reg.Method("GLOBAL_EXPLAIN")
	if imp.Rows[0][0] != "spend" {
		t.Errorf("GLOBAL_EXPLAIN %v", imp.Rows)
	}
	info, _ := reg.Method("TRAINING_INFO")
	if len(info.Rows) < 5 || info.Rows[0][2] == nil {
		t.Errorf("TRAINING_INFO %v", info.Rows)
	}
	if _, err := reg.RowMethod("EXPLAIN_PREDICT", nil); err == nil {
		t.Error("a network explained a prediction")
	}

	for target, min := range map[string]float64{"tier": 0.9, "converted": 0.75} {
		cfg := map[string]any{"hidden_units": []any{16.0}, "dropout": 0.1, "batch_size": 64.0}
		art := trainIgloo(t, ClassDNNClassifier, only(base, "region", "spend", "visits", target), IglooArgs{Target: target, Config: cfg})
		if acc := metric(t, art, "accuracy"); acc < min {
			t.Errorf("%s: accuracy %.3f", target, acc)
		}
		for _, js := range scoreRows(t, art, "PREDICT", nil, head(base, 3)) {
			var p struct {
				Class       string             `json:"class"`
				Probability map[string]float64 `json:"probability"`
			}
			if err := json.Unmarshal([]byte(js), &p); err != nil || len(p.Probability) != len(art.Supervised.Classes) {
				t.Errorf("%s: %s", target, js)
			}
		}
	}
	if _, _, err := TrainIgloo(ClassDNNClassifier, only(base, "spend", "converted"), IglooArgs{Target: "converted",
		Config: map[string]any{"optimizer": "sgd"}}); err == nil || !strings.Contains(err.Error(), "optimizer") {
		t.Errorf("an optimizer other than Adam: %v", err)
	}
}
