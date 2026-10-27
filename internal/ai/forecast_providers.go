package ai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"golang.org/x/oauth2"
)

// The providers' time series APIs. Each forecasts a batch of series in
// one request; none is called but from a Statement (Statement.Forecast).

// --- Nixtla's TimeGPT --------------------------------------------------------

// nixtla speaks Nixtla's API (/v2/forecast), at api.nixtla.io or a
// deployment that serves it, such as Microsoft Foundry's TimeGPT.
type nixtla struct {
	in Integration
	c  *client
}

func (p *nixtla) Complete(context.Context, CompletionRequest) (Completion, error) {
	return Completion{}, unsupported("Nixtla", "text models; it serves time series models alone")
}

func (p *nixtla) Embed(context.Context, EmbeddingRequest) (Embeddings, error) {
	return Embeddings{}, unsupported("Nixtla", "embeddings")
}

func (p *nixtla) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("Nixtla", "decision models")
}

func (p *nixtla) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported("Nixtla", "token counting")
}

func (p *nixtla) forecastBatch() int { return 100 }

// Forecast sends every series in one request: their values concatenated,
// with their sizes. Quantiles are asked for as Nixtla's levels, the
// coverage of intervals in percent: the 0.1 and 0.9 quantiles are the 80%
// interval's bounds.
func (p *nixtla) Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error) {
	var y []float64
	var sizes []int
	for _, s := range r.Series {
		y = append(y, s.Values...)
		sizes = append(sizes, len(s.Values))
	}
	var levels []float64
	for _, q := range r.Levels {
		if l := math.Round(math.Abs(100-200*q)*1e6) / 1e6; l > 0 && l < 100 && !slices.Contains(levels, l) {
			levels = append(levels, l)
		}
	}
	slices.Sort(levels)
	body := map[string]any{"series": map[string]any{"y": y, "sizes": sizes}, "freq": r.Step.Pandas(r.Series[0].Start),
		"h": r.Horizon, "model": r.Model}
	if len(levels) > 0 {
		body["level"] = levels
	}
	h := http.Header{"Accept": {"application/json"}}
	for k, v := range p.in.Headers {
		h.Set(k, v)
	}
	if p.in.APIKey != "" {
		h.Set("Authorization", "Bearer "+p.in.APIKey)
	}
	var out struct {
		InputTokens  int64                `json:"input_tokens"`
		OutputTokens int64                `json:"output_tokens"`
		Mean         []float64            `json:"mean"`
		Intervals    map[string][]float64 `json:"intervals"`
	}
	if err := p.c.postJSON(ctx, strings.TrimRight(p.in.Base(), "/")+"/v2/forecast", h, body, &out); err != nil {
		return Forecasts{}, err
	}
	res := Forecasts{Usage: Usage{InputTokens: out.InputTokens, OutputTokens: out.OutputTokens}}
	n := len(r.Series) * r.Horizon
	if len(out.Mean) != n {
		return res, rowErrorf("the provider forecast %d values, not %d", len(out.Mean), n)
	}
	for i := range r.Series {
		at := func(vs []float64) []float64 { return slices.Clone(vs[i*r.Horizon : (i+1)*r.Horizon]) }
		byLevel := map[float64][]float64{}
		for k, vs := range out.Intervals {
			side, l, ok := strings.Cut(k, "-")
			lv, err := strconv.ParseFloat(l, 64)
			if !ok || err != nil || len(vs) != n || lv <= 0 || lv >= 100 {
				continue
			}
			switch side {
			case "lo":
				byLevel[(1-lv/100)/2] = at(vs)
			case "hi":
				byLevel[(1+lv/100)/2] = at(vs)
			}
		}
		res.Series = append(res.Series, fromQuantiles(byLevel, at(out.Mean)))
	}
	return res, nil
}

// --- Chronos on SageMaker ----------------------------------------------------

// sageMaker invokes SageMaker endpoints of Chronos models (Chronos-2,
// Chronos-Bolt from JumpStart), signed with AWS credentials as the service
// sagemaker.
type sageMaker struct {
	in     Integration
	c      *client
	signer *v4.Signer
}

func newSageMaker(in Integration, c *client) *sageMaker {
	return &sageMaker{in: in, c: c, signer: v4.NewSigner()}
}

func (p *sageMaker) Complete(context.Context, CompletionRequest) (Completion, error) {
	return Completion{}, unsupported("SageMaker", "text models through Igloo; it serves time series models alone")
}

func (p *sageMaker) Embed(context.Context, EmbeddingRequest) (Embeddings, error) {
	return Embeddings{}, unsupported("SageMaker", "embeddings through Igloo")
}

func (p *sageMaker) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("SageMaker", "decision models")
}

func (p *sageMaker) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported("SageMaker", "token counting")
}

// forecastBatch is under the 1,000 series a Chronos endpoint takes.
func (p *sageMaker) forecastBatch() int { return 100 }

func (p *sageMaker) region() string {
	if p.in.Region != "" {
		return p.in.Region
	}
	if u, err := url.Parse(p.in.Base()); err == nil {
		parts := strings.Split(u.Hostname(), ".")
		if len(parts) > 3 && parts[0] == "runtime" && parts[1] == "sagemaker" {
			return parts[2]
		}
	}
	return "us-east-1"
}

// Forecast invokes the endpoint the model names with Chronos' request.
func (p *sageMaker) Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error) {
	var raw json.RawMessage
	err := awsPost(ctx, p.c, p.signer, p.in, "sagemaker", p.region(), p.in.Base()+"/endpoints/"+url.PathEscape(r.Model)+"/invocations",
		chronosRequest(r, chronosMaxLevel), &raw)
	if err != nil {
		return Forecasts{}, err
	}
	fs, err := chronosForecasts(raw, len(r.Series))
	return Forecasts{Series: fs}, err
}

// chronosMaxLevel bounds the quantiles asked of Chronos' endpoints, as
// Chronos-Bolt's clip them: [0.1, 0.9]; the rest are extended from them.
const chronosMaxLevel = 0.9

// awsPost sends body to target, signed for service in region with in's AWS
// credentials, and decodes the answer into out.
func awsPost(ctx context.Context, c *client, signer *v4.Signer, in Integration, service, region, target string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	h := http.Header{"Accept": {"application/json"}}
	for k, v := range in.Headers {
		h.Set(k, v)
	}
	creds := aws.Credentials{AccessKeyID: in.AWSAccessKeyID, SecretAccessKey: in.AWSSecretAccessKey, SessionToken: in.AWSSessionToken}
	r := request{method: http.MethodPost, url: target, header: h, body: b, sign: func(ctx context.Context, req *http.Request, body []byte) error {
		sum := sha256.Sum256(body)
		return signer.SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), service, region, time.Now())
	}}
	return c.send(ctx, r, out)
}

// --- Chronos on Bedrock Marketplace -------------------------------------------

func (p *bedrock) forecastBatch() int { return 100 }

// Forecast invokes a Bedrock Marketplace endpoint of Chronos (InvokeModel,
// the model being the endpoint's ARN) with Chronos' request.
func (p *bedrock) Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error) {
	var raw json.RawMessage
	if err := p.post(ctx, modelPath(r.Model, "invoke"), chronosRequest(r, chronosMaxLevel), &raw); err != nil {
		return Forecasts{}, err
	}
	fs, err := chronosForecasts(raw, len(r.Series))
	return Forecasts{Series: fs}, err
}

// --- TimesFM on Vertex AI ----------------------------------------------------

func (p *gemini) forecastBatch() int { return 32 }

// timesFMQuantile reads the deciles of TimesFM's endpoint: p10 ... p90.
var timesFMQuantile = regexp.MustCompile(`^p(\d{1,2})$`)

// Forecast calls a Vertex AI endpoint of TimesFM (from Model Garden), the
// model being the endpoint: its ID, endpoints/ID, or
// projects/P/locations/L/endpoints/ID. The endpoint answers each instance
// with its point forecast and deciles (p10 ... p90).
func (p *gemini) Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error) {
	if p.in.Provider != ProviderVertex {
		return Forecasts{}, unsupported("Gemini", "time series models")
	}
	if p.in.Project == "" || p.in.Region == "" {
		return Forecasts{}, fmt.Errorf("%w: a TimesFM endpoint needs a vertex integration with a SERVICE_ACCOUNT, PROJECT and REGION",
			ErrInvalid)
	}
	resource := r.Model
	switch {
	case strings.HasPrefix(resource, "projects/"):
	case strings.HasPrefix(resource, "endpoints/"):
		resource = "projects/" + p.in.Project + "/locations/" + p.in.Region + "/" + resource
	default:
		resource = "projects/" + p.in.Project + "/locations/" + p.in.Region + "/endpoints/" + resource
	}
	h, err := p.header()
	if err != nil {
		return Forecasts{}, err
	}
	freq := 0
	switch {
	case r.Step.Months >= 3:
		freq = 2
	case r.Step.Months > 0 || r.Step.Micros >= 7*24*3600*1_000_000:
		freq = 1
	}
	var instances []map[string]any
	for _, s := range r.Series {
		instances = append(instances, map[string]any{"input": s.Values, "horizon": r.Horizon, "freq": freq})
	}
	var out struct {
		Predictions []map[string]json.RawMessage `json:"predictions"`
	}
	if err := p.c.postJSON(ctx, p.in.Target()+"/v1/"+resource+":predict", h, map[string]any{"instances": instances}, &out); err != nil {
		return Forecasts{}, err
	}
	if len(out.Predictions) != len(r.Series) {
		return Forecasts{}, rowErrorf("the provider answered with %d forecasts for %d series", len(out.Predictions), len(r.Series))
	}
	var res Forecasts
	for _, pr := range out.Predictions {
		byLevel := map[float64][]float64{}
		var point []float64
		for k, raw := range pr {
			vs, ok := floats(raw)
			if !ok {
				continue
			}
			switch {
			case k == "point_forecast":
				point = vs
			case k == "mean" && point == nil:
				point = vs
			default:
				if m := timesFMQuantile.FindStringSubmatch(k); m != nil {
					n, _ := strconv.Atoi(m[1])
					if n > 0 && n < 100 {
						byLevel[float64(n)/100] = vs
					}
				}
			}
		}
		if len(point) > r.Horizon {
			point = point[:r.Horizon]
		}
		for q, vs := range byLevel {
			if len(vs) > r.Horizon {
				byLevel[q] = vs[:r.Horizon]
			}
		}
		res.Series = append(res.Series, fromQuantiles(byLevel, point))
	}
	return res, nil
}

// floats reads a list of numbers, or a list holding one.
func floats(raw json.RawMessage) ([]float64, bool) {
	var vs []float64
	if json.Unmarshal(raw, &vs) == nil {
		return vs, true
	}
	var nested [][]float64
	if json.Unmarshal(raw, &nested) == nil && len(nested) == 1 {
		return nested[0], true
	}
	return nil, false
}

// --- BigQuery's AI.FORECAST --------------------------------------------------

// bigQuery runs BigQuery's AI.FORECAST over the series, as a query in the
// integration's project (jobs.query), with a service account's key: the
// series go to BigQuery, which bills the query.
type bigQuery struct {
	in     Integration
	c      *client
	tokens oauth2.TokenSource
	mu     sync.Mutex
}

func newBigQuery(in Integration, c *client) (*bigQuery, error) {
	tokens, err := googleTokens(in.ServiceAccount, c)
	if err != nil {
		return nil, err
	}
	return &bigQuery{in: in, c: c, tokens: tokens}, nil
}

func (p *bigQuery) Complete(context.Context, CompletionRequest) (Completion, error) {
	return Completion{}, unsupported("BigQuery", "text models through Igloo; it serves time series models alone")
}

func (p *bigQuery) Embed(context.Context, EmbeddingRequest) (Embeddings, error) {
	return Embeddings{}, unsupported("BigQuery", "embeddings through Igloo")
}

func (p *bigQuery) Decide(context.Context, DecisionRequest) (Decision, error) {
	return Decision{}, unsupported("BigQuery", "decision models")
}

func (p *bigQuery) CountTokens(context.Context, CompletionRequest) (int64, error) {
	return 0, unsupported("BigQuery", "token counting")
}

// forecastBatch: 32 series of 2,048 points keep a request under
// bigQueryPoints.
func (p *bigQuery) forecastBatch() int { return 32 }

// bigQueryModel is a model's name as AI.FORECAST takes it: 'TimesFM 3.0'.
var bigQueryModel = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 ._-]{0,63}$`)

// bigQueryWait is how long each call waits for the query to finish.
const bigQueryWait = 60_000

// bigQueryPoints are the most points a request sends, about 110 bytes each
// as a query parameter, under BigQuery's 10 MB a request.
const bigQueryPoints = 80_000

// Forecast runs AI.FORECAST over the series, passed as a query parameter
// (an array of id, timestamp and value), with the interval at r.Interval:
// the forecast is its median and the interval's bounds its quantiles.
func (p *bigQuery) Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error) {
	if !bigQueryModel.MatchString(r.Model) {
		return Forecasts{}, fmt.Errorf("%w: %s is not a model's name AI.FORECAST takes, such as 'TimesFM 3.0'", ErrInvalid, quote(r.Model))
	}
	conf := r.Interval
	if conf <= 0 || conf >= 1 {
		conf = 0.95
	}
	query := fmt.Sprintf("SELECT id, UNIX_MICROS(forecast_timestamp) AS t, forecast_value, prediction_interval_lower_bound, "+
		"prediction_interval_upper_bound, ai_forecast_status FROM AI.FORECAST((SELECT id, t, y FROM UNNEST(@series)), "+
		"data_col => 'y', timestamp_col => 't', id_cols => ['id'], model => '%s', horizon => %d, confidence_level => %s) "+
		"ORDER BY id, t", r.Model, r.Horizon, strconv.FormatFloat(conf, 'f', -1, 64))
	// Each series' last points, as many as keep the request under 10 MB.
	per := bigQueryPoints / max(len(r.Series), 1)
	var values []any
	for i, s := range r.Series {
		from := max(0, len(s.Values)-per)
		for k := from; k < len(s.Values); k++ {
			v := s.Values[k]
			values = append(values, map[string]any{"structValues": map[string]any{
				"id": map[string]string{"value": strconv.Itoa(i)},
				"t":  map[string]string{"value": r.Step.At(s.Start, k).UTC().Format("2006-01-02 15:04:05.999999+00:00")},
				"y":  map[string]string{"value": strconv.FormatFloat(v, 'g', -1, 64)},
			}})
		}
	}
	field := func(name, typ string) map[string]any {
		return map[string]any{"name": name, "type": map[string]string{"type": typ}}
	}
	param := map[string]any{"name": "series",
		"parameterType": map[string]any{"type": "ARRAY", "arrayType": map[string]any{"type": "STRUCT",
			"structTypes": []any{field("id", "INT64"), field("t", "TIMESTAMP"), field("y", "FLOAT64")}}},
		"parameterValue": map[string]any{"arrayValues": values}}
	body := map[string]any{"query": query, "useLegacySql": false, "parameterMode": "NAMED", "queryParameters": []any{param},
		"timeoutMs": bigQueryWait}
	if p.in.Region != "" {
		body["location"] = p.in.Region
	}
	// A request ID makes a retried request the same query, not a second one.
	b, _ := json.Marshal(body)
	sum := sha256.Sum256(b)
	body["requestId"] = hex.EncodeToString(sum[:16])
	base := strings.TrimRight(p.in.Base(), "/") + "/projects/" + url.PathEscape(p.in.Project) + "/queries"

	auth, err := bearer(&p.mu, p.tokens, &p.in)
	if err != nil {
		return Forecasts{}, err
	}
	h := http.Header{"Authorization": {auth}}
	var page bigQueryPage
	if err := p.c.postJSON(ctx, base, h, body, &page); err != nil {
		return Forecasts{}, err
	}
	var rows [][]string
	for {
		if err := page.err(); err != nil {
			return Forecasts{}, err
		}
		if page.JobComplete {
			rows = append(rows, page.values()...)
			if page.PageToken == "" {
				break
			}
		}
		q := url.Values{"timeoutMs": {strconv.Itoa(bigQueryWait)}}
		if page.JobReference.Location != "" {
			q.Set("location", page.JobReference.Location)
		}
		if page.JobComplete {
			q.Set("pageToken", page.PageToken)
		}
		if auth, err = bearer(&p.mu, p.tokens, &p.in); err != nil {
			return Forecasts{}, err
		}
		next := bigQueryPage{}
		get := request{method: http.MethodGet, url: base + "/" + url.PathEscape(page.JobReference.JobID) + "?" + q.Encode(),
			header: http.Header{"Authorization": {auth}}}
		if err := p.c.send(ctx, get, &next); err != nil {
			return Forecasts{}, err
		}
		next.JobReference = page.JobReference
		page = next
	}
	return bigQueryForecasts(rows, len(r.Series), r.Horizon, conf)
}

// bigQueryPage is an answer of jobs.query or jobs.getQueryResults.
type bigQueryPage struct {
	JobComplete  bool `json:"jobComplete"`
	JobReference struct {
		JobID    string `json:"jobId"`
		Location string `json:"location"`
	} `json:"jobReference"`
	PageToken string `json:"pageToken"`
	Rows      []struct {
		F []struct {
			V any `json:"v"`
		} `json:"f"`
	} `json:"rows"`
	Errors []struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"errors"`
}

func (p *bigQueryPage) err() error {
	if len(p.Errors) > 0 {
		return &RowError{Msg: "BigQuery: " + firstLine(p.Errors[0].Message, 300)}
	}
	return nil
}

// values are the page's rows' fields, as text ("" for NULL).
func (p *bigQueryPage) values() [][]string {
	out := make([][]string, len(p.Rows))
	for i, r := range p.Rows {
		for _, f := range r.F {
			s, _ := f.V.(string)
			out[i] = append(out[i], s)
		}
	}
	return out
}

// bigQueryForecasts reads AI.FORECAST's rows (id, time, value, lower,
// upper, status) into n series' forecasts of horizon steps.
func bigQueryForecasts(rows [][]string, n, horizon int, conf float64) (Forecasts, error) {
	type step struct {
		t                   int64
		value, lower, upper float64
	}
	steps := make([][]step, n)
	status := make([]string, n)
	for _, r := range rows {
		if len(r) < 6 {
			return Forecasts{}, rowErrorf("BigQuery answered with %d columns, not 6", len(r))
		}
		id, err := strconv.Atoi(r[0])
		if err != nil || id < 0 || id >= n {
			return Forecasts{}, rowErrorf("BigQuery answered with a series %q Igloo did not send", r[0])
		}
		if r[5] != "" {
			status[id] = r[5]
			continue
		}
		t, _ := strconv.ParseInt(r[1], 10, 64)
		num := func(s string) float64 {
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return math.NaN()
			}
			return v
		}
		steps[id] = append(steps[id], step{t, num(r[2]), num(r[3]), num(r[4])})
	}
	lo, hi := (1-conf)/2, (1+conf)/2
	var res Forecasts
	for i := range n {
		f := SeriesForecast{}
		switch {
		case status[i] != "":
			f.Error = "BigQuery: " + firstLine(status[i], 300)
		case len(steps[i]) < horizon:
			f.Error = fmt.Sprintf("BigQuery forecast %d steps, not %d", len(steps[i]), horizon)
		default:
			ss := steps[i]
			sort.Slice(ss, func(a, b int) bool { return ss[a].t < ss[b].t })
			ss = ss[:horizon]
			byLevel := map[float64][]float64{lo: make([]float64, horizon), 0.5: make([]float64, horizon), hi: make([]float64, horizon)}
			for k, s := range ss {
				byLevel[lo][k], byLevel[0.5][k], byLevel[hi][k] = s.lower, s.value, s.upper
			}
			f = fromQuantiles(byLevel, nil)
		}
		res.Series = append(res.Series, f)
	}
	return res, nil
}
