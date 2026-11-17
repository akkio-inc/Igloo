package xgboost

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"testing"
)

// toy is n rows of three features, a few missing, with y = 1 when the
// first two add up to more than 1.
func toy(n int) (x, y []float32) {
	for i := range n {
		a, b, c := float32(i%17)/17, float32(i*7%13)/13, float32(i%5)
		if i%11 == 0 {
			c = float32(math.NaN())
		}
		x = append(x, a, b, c)
		if a+b > 1 {
			y = append(y, 1)
		} else {
			y = append(y, 0)
		}
	}
	return x, y
}

func trained(t *testing.T, rounds int, params map[string]string) (*Booster, *Matrix) {
	t.Helper()
	x, y := toy(400)
	m, err := NewMatrix(x, 400, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Free)
	if err := m.SetFloatInfo("label", y); err != nil {
		t.Fatal(err)
	}
	if err := m.SetFeatureNames([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	b, err := NewBooster(m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Free)
	if err := b.SetFeatureNames([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	for k, v := range params {
		if err := b.SetParam(k, v); err != nil {
			t.Fatal(err)
		}
	}
	for i := range rounds {
		if _, err := b.Update(i, m); err != nil {
			t.Fatal(err)
		}
	}
	return b, m
}

func TestVersion(t *testing.T) {
	if v := Version(); strings.Count(v, ".") != 2 || v == "0.0.0" {
		t.Errorf("Version() = %q", v)
	}
}

// A saved model loads and predicts what it did, contributions included,
// which with the bias add up to the margin.
func TestSaveLoadPredictsTheSame(t *testing.T) {
	b, m := trained(t, 20, map[string]string{"objective": "binary:logistic", "max_depth": "3"})
	model, err := b.Save()
	if err != nil {
		t.Fatal(err)
	}
	if len(model) == 0 || model[0] != '{' {
		t.Fatalf("not UBJSON: % x", model[:min(len(model), 8)])
	}
	again, err := LoadBooster(model)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Free()
	for _, typ := range []PredictType{Value, Margin, Contributions} {
		p, err := b.Predict(m, typ, 0)
		if err != nil {
			t.Fatal(err)
		}
		q, err := again.Predict(m, typ, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Values) == 0 || !equal(p.Values, q.Values) {
			t.Errorf("type %d: predictions differ after loading", typ)
		}
	}
	margin, _ := b.Predict(m, Margin, 0)
	contrib, _ := b.Predict(m, Contributions, 0)
	if want := []int{400, 1, 4}; len(contrib.Shape) != 3 || contrib.Shape[0] != want[0] || contrib.Shape[1] != want[1] || contrib.Shape[2] != want[2] {
		t.Fatalf("contributions' shape %v, want %v", contrib.Shape, want)
	}
	for i := range 400 {
		var sum float64
		for j := range 4 {
			sum += float64(contrib.Values[i*4+j])
		}
		if d := math.Abs(sum - float64(margin.Values[i])); d > 1e-5 {
			t.Fatalf("row %d: contributions add up to %v, the margin is %v", i, sum, margin.Values[i])
		}
	}
	saved, err := again.Save()
	if err != nil || !bytes.Equal(saved, model) {
		t.Errorf("saving the loaded model gave other bytes (%v)", err)
	}
}

// Many goroutines predict with one booster at once.
func TestPredictConcurrently(t *testing.T) {
	b, m := trained(t, 10, map[string]string{"objective": "binary:logistic"})
	want, err := b.Predict(m, Value, 0)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				x, _ := toy(400)
				row, err := NewMatrix(x[3:6], 1, 3)
				if err != nil {
					t.Error(err)
					return
				}
				p, err := b.Predict(row, Value, 0)
				row.Free()
				if err != nil || p.Values[0] != want.Values[1] {
					t.Errorf("row 1 predicted %v (%v), want %v", p, err, want.Values[1])
					return
				}
			}
		})
	}
	wg.Wait()
}

// XGBoost's errors come back with its message, without its time, source
// line or stack trace.
func TestErrorsCarryXGBoostsMessage(t *testing.T) {
	x, y := toy(50)
	m, err := NewMatrix(x, 50, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Free()
	if err := m.SetFloatInfo("label", y); err != nil {
		t.Fatal(err)
	}
	b, err := NewBooster(m)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Free()
	if err := b.SetParam("objective", "binary:nonsense"); err != nil {
		t.Fatal(err)
	}
	_, err = b.Update(0, m)
	if err == nil || !strings.Contains(err.Error(), "binary:nonsense") || strings.Contains(err.Error(), "Stack trace") ||
		strings.HasPrefix(err.Error(), "[") || strings.Contains(err.Error(), ".cc:") {
		t.Errorf("error %q", err)
	}
	if _, err := LoadBooster([]byte("not a model")); err == nil {
		t.Error("loaded a model from junk")
	}
	if err := m.SetFloatInfo("label", y[:3]); err == nil {
		t.Error("set 3 labels on 50 rows")
	}
}

// XGBoost warns of parameters nothing used, which Update returns.
func TestUpdateReturnsWarnings(t *testing.T) {
	x, y := toy(50)
	m, _ := NewMatrix(x, 50, 3)
	defer m.Free()
	_ = m.SetFloatInfo("label", y)
	b, _ := NewBooster(m)
	defer b.Free()
	for k, v := range map[string]string{"objective": "binary:logistic", "max_dept": "3", "validate_parameters": "1"} {
		if err := b.SetParam(k, v); err != nil {
			t.Fatal(err)
		}
	}
	warnings, err := b.Update(0, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) == 0 || !strings.Contains(strings.Join(warnings, "\n"), `"max_dept"`) {
		t.Errorf("warnings %q", warnings)
	}
	if warnings, _ := b.Update(1, m); len(warnings) != 0 {
		t.Errorf("the second round warned %q", warnings)
	}
}

func TestEvalSliceAndScores(t *testing.T) {
	b, m := trained(t, 12, map[string]string{"objective": "binary:logistic", "eval_metric": "logloss"})
	line, err := b.Eval(11, []*Matrix{m}, []string{"train"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := ParseEval(line)
	if err != nil || len(res) != 1 || res[0].Data != "train" || res[0].Metric != "logloss" || !(res[0].Value > 0) {
		t.Fatalf("ParseEval(%q) = %+v, %v", line, res, err)
	}
	if n, err := b.Rounds(); err != nil || n != 12 {
		t.Errorf("Rounds() = %d, %v", n, err)
	}
	s, err := b.Slice(0, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Free()
	if n, _ := s.Rounds(); n != 5 {
		t.Errorf("the slice has %d rounds", n)
	}
	sliced, _ := s.Predict(m, Value, 0)
	ranged, _ := b.Predict(m, Value, 5)
	if !equal(sliced.Values, ranged.Values) {
		t.Error("the slice predicts otherwise than the first 5 rounds")
	}
	gain, err := b.FeatureScore("gain")
	if err != nil || gain["a"] <= 0 || gain["b"] <= 0 {
		t.Errorf("gain %v, %v", gain, err)
	}
	if err := b.SetAttr("best_iteration", "4"); err != nil {
		t.Fatal(err)
	}
	cfg, err := b.SaveConfig()
	if err != nil || !strings.Contains(cfg, `"logloss"`) {
		t.Fatalf("config %q, %v", cfg, err)
	}
	if err := s.LoadConfig(cfg); err != nil {
		t.Error(err)
	}
}

func TestParseEval(t *testing.T) {
	res, err := ParseEval("[3]\ttrain-aft-nloglik:1.25000000000000000\tvalidation-auc:0.5\tvalidation-error@0.7:-nan")
	if err != nil || len(res) != 3 {
		t.Fatalf("%+v, %v", res, err)
	}
	if res[0].Data != "train" || res[0].Metric != "aft-nloglik" || res[0].Value != 1.25 || res[2].Metric != "error@0.7" ||
		!math.IsNaN(res[2].Value) {
		t.Errorf("%+v", res)
	}
	if _, err := ParseEval("nothing"); err == nil {
		t.Error("parsed nothing")
	}
}

func equal(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BenchmarkPredictRow is what scoring a row costs, as IGLOO.ML.XGBOOST
// scores rows: a booster loaded from its saved model, on one thread, a
// matrix of one row, and its prediction or its SHAP values.
func BenchmarkPredictRow(b *testing.B) {
	x, y := toy(2000)
	m, _ := NewMatrix(x, 2000, 3)
	defer m.Free()
	_ = m.SetFloatInfo("label", y)
	trained, _ := NewBooster(m)
	defer trained.Free()
	for k, v := range map[string]string{"objective": "binary:logistic", "max_depth": "6"} {
		_ = trained.SetParam(k, v)
	}
	for i := range 100 {
		_, _ = trained.Update(i, m)
	}
	model, _ := trained.Save()
	bst, _ := LoadBooster(model)
	defer bst.Free()
	_ = bst.SetParam("nthread", "1")
	for _, typ := range []PredictType{Value, Contributions} {
		b.Run(map[PredictType]string{Value: "value", Contributions: "contributions"}[typ], func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				row, _ := NewMatrix(x[i%2000*3:i%2000*3+3], 1, 3)
				if _, err := bst.Predict(row, typ, 0); err != nil {
					b.Fatal(err)
				}
				row.Free()
			}
		})
	}
}
