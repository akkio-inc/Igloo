package models

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAutoencoderModel(t *testing.T) {
	tbl := factorTable(1500, 6, 8)
	cfg := map[string]any{"hidden_units": []any{8.0, 2.0, 8.0}, "learn_rate": 0.01, "data_split_method": "no_split"}
	art := trainIgloo(t, ClassAutoencoder, tbl, IglooArgs{Config: cfg})
	again := trainIgloo(t, ClassAutoencoder, tbl, IglooArgs{Config: cfg})
	_, sha1, _ := art.Encode()
	_, sha2, _ := again.Encode()
	if sha1 != sha2 {
		t.Error("two trainings differ")
	}
	// Two planted factors: a latent space of two rebuilds the rows well.
	if mse := metric(t, art, "mean_squared_error"); mse > 0.05 {
		t.Errorf("reconstruction MSE %.4f", mse)
	}
	for _, js := range scoreRows(t, art, "PREDICT", nil, head(tbl, 2)) {
		var p map[string]float64
		if err := json.Unmarshal([]byte(js), &p); err != nil || len(p) != 2 {
			t.Errorf("PREDICT %s: %v", js, err)
		}
	}
	for _, js := range scoreRows(t, art, "RECONSTRUCTION_LOSS", nil, head(tbl, 2)) {
		if !strings.Contains(js, `"mean_absolute_error":`) || !strings.Contains(js, `"mean_squared_log_error":`) {
			t.Errorf("RECONSTRUCTION_LOSS %s", js)
		}
	}
	flagged := 0
	for i, js := range scoreRows(t, art, "DETECT_ANOMALIES", map[string]any{"contamination": 0.01}, tbl) {
		if strings.Contains(js, `"is_anomaly":true`) {
			flagged++
		} else if i >= 1500 {
			t.Errorf("outlier %d not flagged: %s", i, js)
		}
	}
	if flagged > 30 {
		t.Errorf("%d rows flagged", flagged)
	}
	if res, err := art.EvaluateOn(head(tbl, 1500)); err != nil || res.Rows[0][1].(float64) > 0.05 {
		t.Errorf("EVALUATE on rows: %v, %v", res, err)
	}
	info, _ := art.Method("TRAINING_INFO")
	if len(info.Rows) < 3 {
		t.Errorf("TRAINING_INFO %v", info.Rows)
	}
	// By default the latent space is about a quarter of the encoded
	// columns, at least two.
	def := trainIgloo(t, ClassAutoencoder, tbl, IglooArgs{})
	if got := def.Autoencoder.Net.Sizes[def.Autoencoder.Latent]; got != 2 {
		t.Errorf("default latent width %d", got)
	}
}
