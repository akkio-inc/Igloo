package models

import (
	"testing"
)

func TestAutoML(t *testing.T) {
	base := campaignTable(1500, 9)
	reg := trainIgloo(t, ClassAutoMLRegressor, only(base, "region", "spend", "visits", "value"),
		IglooArgs{Target: "value", Config: map[string]any{"max_trials": 4.0, "cv_folds": 2.0}})
	trials, err := reg.Method("TRIAL_INFO")
	if err != nil || len(trials.Rows) != 4 {
		t.Fatalf("TRIAL_INFO %v, %v", trials, err)
	}
	optimal, best := 0, ""
	bestRMSE := 1e18
	for _, row := range trials.Rows {
		if row[3] != "SUCCEEDED" {
			t.Errorf("trial %v", row)
			continue
		}
		v := row[4].(float64)
		if v < bestRMSE {
			bestRMSE, best = v, row[1].(string)
		}
		if row[5].(bool) {
			optimal++
		}
	}
	// The value is planted linear in the features: a linear model wins.
	if optimal != 1 || best != "linear" || reg.Supervised.Algorithm != "linear" {
		t.Errorf("chose %s (%v); the best trial was %s", reg.Supervised.Algorithm, trials.Rows, best)
	}
	if r2 := metric(t, reg, "r2_score"); r2 < 0.95 {
		t.Errorf("R² %.3f", r2)
	}
	if _, err := reg.Method("WEIGHTS"); err != nil {
		t.Errorf("the linear winner's WEIGHTS: %v", err)
	}
	if _, err := reg.RowMethod("EXPLAIN_PREDICT", nil); err != nil {
		t.Errorf("EXPLAIN_PREDICT: %v", err)
	}

	cls := trainIgloo(t, ClassAutoMLClassifier, only(base, "region", "spend", "visits", "tier"),
		IglooArgs{Target: "tier", Config: map[string]any{"max_trials": 3.0, "optimization_objective": "maximize_au_roc"}})
	trials, _ = cls.Method("TRIAL_INFO")
	for _, row := range trials.Rows {
		if v, ok := row[4].(float64); !ok || v < 0.8 || v > 1 {
			t.Errorf("a trial's AUC: %v", row)
		}
	}
	if acc := metric(t, cls, "accuracy"); acc < 0.85 {
		t.Errorf("accuracy %.3f", acc)
	}
	// A budget of milliseconds stops the search after a trial or two.
	quick := trainIgloo(t, ClassAutoMLRegressor, only(base, "spend", "visits", "value"),
		IglooArgs{Target: "value", Config: map[string]any{"budget_hours": 1e-6}})
	if trials, _ := quick.Method("TRIAL_INFO"); len(trials.Rows) < 1 || len(trials.Rows) > 3 {
		t.Errorf("trials within no budget: %v", trials.Rows)
	}
}
