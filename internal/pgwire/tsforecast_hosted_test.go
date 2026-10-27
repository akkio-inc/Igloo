package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/tsforecast/tsforecasttest"
)

// fakeTimeGPT is Nixtla's /v2/forecast for tests: each series' forecast is
// its last value, its intervals that value plus and minus a tenth of their
// level. No test reaches Nixtla or uses a real key.
type fakeTimeGPT struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []map[string]any
}

func newFakeTimeGPT(t *testing.T) *fakeTimeGPT {
	f := &fakeTimeGPT{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		f.mu.Lock()
		f.requests = append(f.requests, body)
		f.mu.Unlock()
		if r.URL.Path != "/v2/forecast" || r.Header.Get("Authorization") != "Bearer nixtla-test-not-a-real-key" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		series, _ := body["series"].(map[string]any)
		ys, _ := series["y"].([]any)
		sizes, _ := series["sizes"].([]any)
		h := int(body["h"].(float64))
		levels, _ := body["level"].([]any)
		var mean []float64
		intervals := map[string][]float64{}
		at := 0
		for _, s := range sizes {
			n := int(s.(float64))
			at += n
			last := ys[at-1].(float64)
			for range h {
				mean = append(mean, last)
				for _, l := range levels {
					lv := l.(float64)
					key := strings.TrimSuffix(fmt.Sprintf("%g", lv), ".0")
					intervals["lo-"+key] = append(intervals["lo-"+key], last-lv/10)
					intervals["hi-"+key] = append(intervals["hi-"+key], last+lv/10)
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"input_tokens": len(ys), "output_tokens": len(mean), "mean": mean,
			"intervals": intervals})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeTimeGPT) sent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// A model an AI integration maps forecasts through the integration, on the
// coordinator, with its USAGE grants, cache and history.
func TestHostedForecast(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "models")
	tsforecasttest.InstallTiny(t, dir, "test-bolt")
	cs := startCatalogServerWithConfig(t, config.Config{Models: config.Models{Dir: dir}}, withAI)
	mustExec(t, cs.admin, dailySpend)
	f := newFakeTimeGPT(t)
	mustExec(t, cs.admin, `CREATE AI INTEGRATION timeseries PROVIDER = nixtla BASE_URL = '`+f.srv.URL+`'
		API_KEY = 'nixtla-test-not-a-real-key' MODELS = ('timegpt' = 'timegpt-1', 'test-bolt' = 'timegpt-1', 'chronos-2' = 'timegpt-1')`)
	conn := cs.admin
	mustExec(t, conn, "SET search_path = 'lake.public,memory.main'")
	// Series are sent as float32 values, as the model pack's models read them.
	q := `SELECT campaign, forecast_timestamp::VARCHAR, abs(forecast_value - last) < 1e-3,
			round(prediction_interval_upper_bound - forecast_value, 3), ai_forecast_status
		FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', id_cols => ['campaign'], model => 'TimeGPT',
			horizon => 3, confidence_level => 0.8)
		JOIN (SELECT campaign, arg_max(spend, day) AS last FROM daily_spend GROUP BY 1) USING (campaign) ORDER BY 1, 2`
	got := texts(t, conn, q)
	if len(got) != 6 || got[0] != "c1|2025-06-30 00:00:00|true|8|" || got[5] != "c2|2025-07-02 00:00:00|true|8|" {
		t.Errorf("forecasts %q", got)
	}
	f.mu.Lock()
	req := f.requests[len(f.requests)-1]
	f.mu.Unlock()
	if req["model"] != "timegpt-1" || req["freq"] != "D" || req["h"] != float64(3) ||
		!strings.Contains(fmt.Sprint(req["series"].(map[string]any)["sizes"]), "180") {
		t.Errorf("request %v", req)
	}
	// A request a chunk of series: one or two.
	if h := lastAIQuery(t, cs, "igloo", "TimeGPT"); h.AI == nil || h.AI.Calls != int64(f.sent()) || h.AI.InputTokens != 360 {
		t.Errorf("history %+v after %d requests", h.AI, f.sent())
	}
	n := f.sent()
	texts(t, conn, q)
	if f.sent() != n {
		t.Errorf("the same forecast sent %d requests again; the cache should answer", f.sent()-n)
	}
	if h := lastAIQuery(t, cs, "igloo", "TimeGPT"); h.AI == nil || h.AI.Calls != 0 || h.AI.CacheHits != 2 {
		t.Errorf("history %+v", h.AI)
	}
	// Databricks' detection, with the same model.
	if got := texts(t, conn, `SELECT campaign, count(*), count(anomaly_score) FROM ai_detect_anomalies(observed => TABLE(daily_spend),
		time_col => 'day', value_col => 'spend', group_col => 'campaign', model => 'timegpt') GROUP BY 1 ORDER BY 1`); strings.Join(got, ";") !=
		"c1|180|172;c2|180|172" {
		t.Errorf("detection %q", got)
	}

	// The name is the integration's for those who may use it; for others,
	// a name the model pack does not have.
	ana := snowflakeConn(t, cs.user(t, "ana", auth.RoleReader, pgx.QueryExecModeSimpleProtocol))
	hosted := `SELECT count(*) FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => 'timegpt', horizon => 2)`
	expectCode(t, ana, hosted, "42704", "not a forecasting model")
	mustExec(t, cs.admin, "GRANT USAGE ON INTEGRATION timeseries TO USER ana")
	if n := count(t, ana, hosted); n != 2 {
		t.Errorf("ana forecasts %d steps", n)
	}
	// A name the model pack has is the pack's, though the integration maps
	// it too: no series leaves unless the call names the integration.
	sent := f.sent()
	pack := `SELECT count(*) FROM AI.FORECAST(TABLE daily_spend, data_col => 'spend', timestamp_col => 'day', model => '%s', horizon => 2)`
	if n := count(t, ana, fmt.Sprintf(pack, "test-bolt")); n != 2 || f.sent() != sent {
		t.Errorf("ana forecasts %d steps with test-bolt, and sent %d requests", n, f.sent()-sent)
	}
	// (Four steps: the cache holds the two-step forecast, from timegpt,
	// the same model.)
	if n := count(t, ana, strings.Replace(fmt.Sprintf(pack, "timeseries/test-bolt"), "horizon => 2", "horizon => 4", 1)); n != 4 ||
		f.sent() != sent+1 {
		t.Errorf("ana forecasts %d steps with timeseries/test-bolt, and sent %d requests", n, f.sent()-sent)
	}
	// EXPLAIN says which, and sends nothing.
	explain := func(model string) string {
		t.Helper()
		for _, row := range texts(t, conn, "EXPLAIN "+fmt.Sprintf(pack, model)) {
			if v, ok := strings.CutPrefix(row, "forecast model|"); ok {
				return v
			}
		}
		return ""
	}
	sent = f.sent()
	if got := explain("TEST-BOLT"); got != "test-bolt: the model pack's, run by Igloo; the series stay in Igloo" {
		t.Errorf("EXPLAIN of test-bolt: %q", got)
	}
	if got := explain("timeseries/test-bolt"); !strings.HasPrefix(got,
		"timeseries/test-bolt: hosted: the AI integration timeseries's timegpt-1 (nixtla, http://127.0.0.1:") ||
		!strings.HasSuffix(got, "the series are sent there") {
		t.Errorf("EXPLAIN of timeseries/test-bolt: %q", got)
	}
	if got := explain("timegpt"); !strings.HasPrefix(got, "timegpt: hosted: the AI integration timeseries's timegpt-1") {
		t.Errorf("EXPLAIN of timegpt: %q", got)
	}
	if f.sent() != sent {
		t.Errorf("EXPLAIN sent %d requests", f.sent()-sent)
	}
	// A pack model that is not installed says how to reach the hosted one.
	_, err := ana.Exec(context.Background(), fmt.Sprintf(pack, "chronos-2"))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55000" || !strings.Contains(pgErr.Message, "igloo models install chronos-2") ||
		!strings.Contains(pgErr.Hint, "'timeseries/chronos-2'") {
		t.Errorf("chronos-2, not installed: %v", err)
	}
}
