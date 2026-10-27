package ai

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

var day = Step{Micros: 24 * 3600 * 1_000_000}

func twoSeries() []ForecastSeries {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return []ForecastSeries{{Values: []float64{1, 2, 3, 4}, Start: start}, {Values: []float64{10, 20, 30}, Start: start}}
}

// steps is n values from v, by 1.
func steps(n int, v float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v + float64(i)
	}
	return out
}

func TestStepPandas(t *testing.T) {
	jan := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		step Step
		want string
	}{{day, "D"}, {Step{Micros: 7 * 24 * 3600 * 1_000_000}, "7D"}, {Step{Micros: 3600 * 1_000_000}, "h"},
		{Step{Micros: 15 * 60 * 1_000_000}, "15min"}, {Step{Months: 1}, "MS"}, {Step{Months: 3}, "QS"}, {Step{Months: 12}, "YS"},
		{Step{Months: 2}, "2MS"}} {
		if got := tc.step.Pandas(jan); got != tc.want {
			t.Errorf("%+v: %s, want %s", tc.step, got, tc.want)
		}
	}
	if got := (Step{Months: 1}).Pandas(time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)); got != "ME" {
		t.Errorf("month ends: %s", got)
	}
}

func TestNixtlaForecast(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		return 200, map[string]any{"input_tokens": 7, "output_tokens": 4, "mean": []float64{5, 6, 40, 50},
			"intervals": map[string]any{"lo-80": []float64{4, 5, 35, 45}, "hi-80": []float64{6, 7, 45, 55}}}
	})
	p := mustProvider(t, Integration{Name: "n", Provider: ProviderNixtla, BaseURL: f.srv.URL, APIKey: "nixtla-test-key"}).(forecaster)
	res, err := p.Forecast(ctx, ForecastRequest{Model: "timegpt-1", Series: twoSeries(), Horizon: 2, Step: day, Levels: []float64{0.1, 0.5, 0.9}})
	if err != nil {
		t.Fatal(err)
	}
	r := f.last()
	series := r.Body["series"].(map[string]any)
	if r.Path != "/v2/forecast" || r.Header.Get("Authorization") != "Bearer nixtla-test-key" || r.Body["freq"] != "D" ||
		r.Body["h"] != float64(2) || r.Body["model"] != "timegpt-1" || fmt.Sprint(series["sizes"]) != "[4 3]" ||
		fmt.Sprint(series["y"]) != "[1 2 3 4 10 20 30]" || fmt.Sprint(r.Body["level"]) != "[80]" {
		t.Errorf("request %s %v", r.Path, r.Body)
	}
	if len(res.Series) != 2 || fmt.Sprint(res.Series[1].Point) != "[40 50]" || fmt.Sprint(res.Series[1].Levels) != "[0.1 0.9]" ||
		fmt.Sprint(res.Series[1].Quantiles) != "[[35 45] [45 55]]" || res.Usage.InputTokens != 7 {
		t.Errorf("forecasts %+v", res)
	}
	if _, err := mustProvider(t, Integration{Name: "n", Provider: ProviderNixtla, APIKey: "k"}).Complete(ctx, CompletionRequest{}); !errors.Is(err, ErrUnsupported) {
		t.Errorf("complete: %v", err)
	}
}

func chronosAnswer(r fakeRequest) (int, any) {
	inputs := r.Body["inputs"].([]any)
	h := int(r.Body["parameters"].(map[string]any)["prediction_length"].(float64))
	var preds []any
	for i := range inputs {
		v := float64(10 * (i + 1))
		preds = append(preds, map[string]any{"mean": steps(h, v), "0.1": steps(h, v-1), "0.5": steps(h, v), "0.9": steps(h, v+1)})
	}
	return 200, map[string]any{"predictions": preds}
}

func TestChronosEndpoints(t *testing.T) {
	t.Parallel()
	f := newFake(t, chronosAnswer)
	sm := mustProvider(t, Integration{Name: "s", Provider: ProviderSageMaker, Region: "us-east-1", BaseURL: f.srv.URL,
		AWSAccessKeyID: "AKIDEXAMPLE", AWSSecretAccessKey: "secret-example-key"}).(forecaster)
	req := ForecastRequest{Model: "chronos-2-endpoint", Series: twoSeries(), Horizon: 3, Step: day, Levels: []float64{0.025, 0.1, 0.5, 0.9, 0.975}}
	res, err := sm.Forecast(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r := f.last()
	auth := r.Header.Get("Authorization")
	params := r.Body["parameters"].(map[string]any)
	if r.Path != "/endpoints/chronos-2-endpoint/invocations" || !strings.Contains(auth, "/us-east-1/sagemaker/aws4_request") ||
		fmt.Sprint(params["quantile_levels"]) != "[0.1 0.5 0.9]" || params["freq"] != "D" ||
		r.Body["inputs"].([]any)[0].(map[string]any)["start"] != "2026-01-01T00:00:00" {
		t.Errorf("request %s %s %v", r.Path, auth, r.Body)
	}
	if fmt.Sprint(res.Series[1].Point) != "[20 21 22]" || fmt.Sprint(res.Series[1].Levels) != "[0.1 0.5 0.9]" {
		t.Errorf("forecast %+v", res.Series[1])
	}

	arn := "arn:aws:sagemaker:us-east-1:000000000000:endpoint/chronos-bolt-demo"
	br := mustProvider(t, Integration{Name: "b", Provider: ProviderBedrock, Region: "us-east-1", BaseURL: f.srv.URL,
		AWSAccessKeyID: "AKIDEXAMPLE", AWSSecretAccessKey: "secret-example-key"}).(forecaster)
	req.Model = arn
	if res, err = br.Forecast(ctx, req); err != nil || len(res.Series) != 2 {
		t.Fatalf("bedrock %+v %v", res, err)
	}
	if r := f.last(); !strings.HasSuffix(r.Path, "/invoke") || !strings.Contains(r.Header.Get("Authorization"), "/bedrock/aws4_request") {
		t.Errorf("bedrock request %s", r.Path)
	}
	if err := (&Integration{Name: "s", Provider: ProviderSageMaker, Region: "us-east-1", APIKey: "k"}).Check(nil); err == nil {
		t.Error("a sagemaker integration without AWS credentials")
	}
}

// serviceAccount is a service account's key, made up, whose tokens the
// fake mints at /token.
func serviceAccount(t *testing.T, f *fake) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	sa, _ := json.Marshal(map[string]string{"type": "service_account", "client_email": "igloo@demo-project.iam.gserviceaccount.com",
		"private_key": string(pemKey), "private_key_id": "kid", "token_uri": f.srv.URL + "/token"})
	return string(sa)
}

var tokenAnswer = map[string]any{"access_token": "google-test-token", "token_type": "Bearer", "expires_in": 3600}

func TestVertexTimesFM(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		if r.Path == "/token" {
			return 200, tokenAnswer
		}
		var preds []any
		for range r.Body["instances"].([]any) {
			pred := map[string]any{"point_forecast": []float64{1, 2}, "mean": []float64{1, 2}}
			for d := 1; d <= 9; d++ {
				pred[fmt.Sprintf("p%d0", d)] = []float64{float64(d) / 10, 1 + float64(d)/10}
			}
			preds = append(preds, pred)
		}
		return 200, map[string]any{"predictions": preds, "deployedModelId": "1"}
	})
	p := mustProvider(t, Integration{Name: "v", Provider: ProviderVertex, ServiceAccount: serviceAccount(t, f), Project: "demo-project",
		Region: "us-central1", BaseURL: f.srv.URL + "/v1/projects/demo-project/locations/us-central1/publishers/google/models"}).(forecaster)
	res, err := p.Forecast(ctx, ForecastRequest{Model: "1234567890", Series: twoSeries(), Horizon: 2, Step: Step{Months: 1}})
	if err != nil {
		t.Fatal(err)
	}
	r := f.last()
	inst := r.Body["instances"].([]any)[0].(map[string]any)
	if r.Path != "/v1/projects/demo-project/locations/us-central1/endpoints/1234567890:predict" ||
		r.Header.Get("Authorization") != "Bearer google-test-token" || inst["horizon"] != float64(2) || inst["freq"] != float64(1) {
		t.Errorf("request %s %v", r.Path, r.Body)
	}
	if len(res.Series) != 2 || len(res.Series[0].Levels) != 9 || res.Series[0].Quantiles[8][1] != 1.9 || res.Series[0].Point[1] != 2 {
		t.Errorf("forecast %+v", res.Series[0])
	}
}

func TestBigQueryForecast(t *testing.T) {
	t.Parallel()
	var polls int
	f := newFake(t, func(r fakeRequest) (int, any) {
		switch {
		case r.Path == "/token":
			return 200, tokenAnswer
		case r.Method == http.MethodPost:
			return 200, map[string]any{"jobComplete": false, "jobReference": map[string]any{"jobId": "job_1", "location": "US"}}
		}
		polls++
		row := func(id, t int, v float64, status string) map[string]any {
			cell := func(s string) map[string]any { return map[string]any{"v": s} }
			return map[string]any{"f": []any{cell(fmt.Sprint(id)), cell(fmt.Sprint(int64(t) * 86_400_000_000)), cell(fmt.Sprint(v)),
				cell(fmt.Sprint(v - 1)), cell(fmt.Sprint(v + 1)), cell(status)}}
		}
		if !strings.Contains(r.Query, "pageToken") {
			return 200, map[string]any{"jobComplete": true, "pageToken": "p2", "rows": []any{row(0, 20000, 5, ""), row(0, 20001, 6, "")}}
		}
		return 200, map[string]any{"jobComplete": true, "rows": []any{map[string]any{"f": []any{map[string]any{"v": "1"}, map[string]any{"v": nil},
			map[string]any{"v": nil}, map[string]any{"v": nil}, map[string]any{"v": nil}, map[string]any{"v": "too few points"}}}}}
	})
	p := mustProvider(t, Integration{Name: "bq", Provider: ProviderBigQuery, ServiceAccount: serviceAccount(t, f), Project: "demo-project",
		Region: "US", BaseURL: f.srv.URL + "/bigquery/v2"}).(forecaster)
	res, err := p.Forecast(ctx, ForecastRequest{Model: "TimesFM 3.0", Series: twoSeries(), Horizon: 2, Step: day, Interval: 0.9})
	if err != nil {
		t.Fatal(err)
	}
	var post fakeRequest
	for _, r := range f.requests {
		if r.Method == http.MethodPost && r.Path != "/token" {
			post = r
		}
	}
	q, _ := post.Body["query"].(string)
	param := post.Body["queryParameters"].([]any)[0].(map[string]any)
	values := param["parameterValue"].(map[string]any)["arrayValues"].([]any)
	if post.Path != "/bigquery/v2/projects/demo-project/queries" || !strings.Contains(q, "model => 'TimesFM 3.0'") ||
		!strings.Contains(q, "horizon => 2") || !strings.Contains(q, "confidence_level => 0.9") || len(values) != 7 ||
		post.Body["location"] != "US" || post.Body["requestId"] == nil || post.Header.Get("Authorization") != "Bearer google-test-token" {
		t.Errorf("query %s %v", post.Path, post.Body)
	}
	if polls != 2 || len(res.Series) != 2 || fmt.Sprint(res.Series[0].Point) != "[5 6]" ||
		fmt.Sprint(res.Series[0].Quantiles) != "[[4 5] [5 6] [6 7]]" || res.Series[0].Levels[0] != 0.05 ||
		!strings.Contains(res.Series[1].Error, "too few points") {
		t.Errorf("polls %d, forecasts %+v", polls, res)
	}
	if _, err := p.Forecast(ctx, ForecastRequest{Model: "x'; DROP TABLE t; --", Series: twoSeries(), Horizon: 1, Step: day}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a model's name with a quote: %v", err)
	}
	if err := (&Integration{Name: "bq", Provider: ProviderBigQuery, Project: "demo-project"}).Check(nil); err == nil {
		t.Error("a bigquery integration without a service account")
	}
}

// Statement.Forecast resolves the model through the integrations, sends a
// batch, and answers the same series from the cache.
func TestStatementForecast(t *testing.T) {
	t.Parallel()
	f := newFake(t, func(r fakeRequest) (int, any) {
		sizes := r.Body["series"].(map[string]any)["sizes"].([]any)
		h := int(r.Body["h"].(float64))
		mean := steps(len(sizes)*h, 1)
		return 200, map[string]any{"input_tokens": 7, "output_tokens": 2, "mean": mean,
			"intervals": map[string]any{"lo-80": steps(len(sizes)*h, 0), "hi-80": steps(len(sizes)*h, 2)}}
	})
	rt := testRuntime(t, f, func(in *Integration) {
		in.Provider, in.BaseURL, in.Models = ProviderNixtla, f.srv.URL, map[string]string{"timegpt": "timegpt-1"}
		in.DefaultModel, in.Prices = "", nil
	})
	ana := StatementOptions{User: "ana", Roles: []string{"analysts"}}
	if tg, ok, err := rt.ForecastModel(ctx, ana, "TimeGPT"); !ok || err != nil || tg.Integration != "main" || tg.Model != "timegpt-1" ||
		tg.Provider != ProviderNixtla || tg.Target != f.srv.URL {
		t.Errorf("timegpt for ana: %+v %v %v", tg, ok, err)
	}
	if tg, ok, _ := rt.ForecastModel(ctx, ana, "main/timegpt"); !ok || tg.Model != "timegpt-1" {
		t.Errorf("main/timegpt: %+v %v", tg, ok)
	}
	if _, ok, _ := rt.ForecastModel(ctx, StatementOptions{User: "nina"}, "timegpt"); ok {
		t.Error("nina has no USAGE on the integration")
	}
	if _, ok, _ := rt.ForecastModel(ctx, ana, "chronos-2"); ok {
		t.Error("an unmapped name")
	}
	if _, ok, _ := rt.ForecastModel(ctx, ana, "other/timegpt"); ok {
		t.Error("an integration that does not exist")
	}
	for round := range 2 {
		s := begin(t, rt)
		fs, errs, err := s.Forecast("timegpt", twoSeries(), 2, day, []float64{0.1, 0.5, 0.9}, 0.8, false)
		if err != nil || errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: %v %v", round, err, errs)
		}
		if fmt.Sprint(fs[1].Point) != "[3 4]" || fmt.Sprint(fs[1].Quantiles) != "[[2 3] [4 5]]" {
			t.Errorf("round %d: %+v", round, fs[1])
		}
		sp := s.End()
		if round == 1 && (sp.Calls != 0 || sp.CacheHits != 2) || round == 0 && sp.Calls != 1 {
			t.Errorf("round %d: %+v", round, sp)
		}
	}
	if n := f.calls.Load(); n != 1 {
		t.Errorf("%d requests for two series, twice", n)
	}
	// integration/name is the same model, answered from the cache.
	s := begin(t, rt)
	if _, errs, err := s.Forecast("main/timegpt", twoSeries(), 2, day, []float64{0.1, 0.5, 0.9}, 0.8, false); err != nil || errs[0] != nil ||
		s.End().CacheHits != 2 {
		t.Errorf("main/timegpt: %v %v", err, errs)
	}
	if _, _, err := begin(t, rt).Forecast("main/prophet", twoSeries(), 2, day, nil, 0.8, false); !errors.Is(err, ErrModel) {
		t.Errorf("main/prophet: %v", err)
	}
	// A model of an integration without time series models.
	rt2 := testRuntime(t, f)
	if _, _, err := begin(t, rt2).Forecast("m", twoSeries(), 2, day, nil, 0.9, false); !errors.Is(err, ErrUnsupported) {
		t.Errorf("an openai model: %v", err)
	}
	if _, ok, _ := rt2.ForecastModel(ctx, ana, "m"); ok {
		t.Error("an openai model forecasts")
	}
}
