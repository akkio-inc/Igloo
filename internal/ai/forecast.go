package ai

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Hosted time series models: forecasts from a provider's model, with an
// integration's keys, for BigQuery's AI.FORECAST and AI.DETECT_ANOMALIES
// and Databricks' ai_forecast and ai_detect_anomalies when they name a
// model an integration maps (package tsforecast defines the functions;
// docs/ml-functions.md). Nixtla's TimeGPT (also as Microsoft Foundry
// serves it), Chronos on SageMaker endpoints and Bedrock Marketplace,
// TimesFM on Vertex AI endpoints, and BigQuery's own AI.FORECAST, whose
// TimesFM 3.0 is available nowhere else for commercial use.

// The hosted forecasting functions. Like the AI functions, they call
// providers with keys only the coordinator holds, so they run there.
const (
	ForecastFunction = "ai_ts_forecast"
	DetectFunction   = "ai_ts_detect"
)

// Step is a series' step: a number of months, or of microseconds.
type Step struct {
	Months int   `json:"months,omitempty"`
	Micros int64 `json:"micros,omitempty"`
}

// At is the time k steps after start.
func (s Step) At(start time.Time, k int) time.Time {
	if s.Months > 0 {
		return start.AddDate(0, s.Months*k, 0)
	}
	return start.Add(time.Duration(s.Micros*int64(k)) * time.Microsecond)
}

// Pandas is the step as pandas writes a frequency, which Nixtla's and
// Chronos' APIs take: "D", "h", "15min", "7D", "MS", "QS", "YS".
func (s Step) Pandas(start time.Time) string {
	if s.Months > 0 {
		unit := "MS"
		n := s.Months
		switch {
		case s.Months%12 == 0:
			unit, n = "YS", s.Months/12
		case s.Months%3 == 0:
			unit, n = "QS", s.Months/3
		}
		if start.Day() != 1 {
			unit = strings.Replace(unit, "S", "E", 1)
		}
		if n == 1 {
			return unit
		}
		return strconv.Itoa(n) + unit
	}
	us := s.Micros
	for _, u := range []struct {
		micros int64
		unit   string
	}{{86_400_000_000, "D"}, {3_600_000_000, "h"}, {60_000_000, "min"}, {1_000_000, "s"}, {1_000, "ms"}, {1, "us"}} {
		if us >= u.micros && us%u.micros == 0 {
			n := us / u.micros
			if n == 1 {
				return u.unit
			}
			return strconv.FormatInt(n, 10) + u.unit
		}
	}
	return "D"
}

// ForecastSeries is a series to forecast: its values at regular steps,
// without gaps, the first at Start.
type ForecastSeries struct {
	Values []float64 `json:"values"`
	Start  time.Time `json:"start"`
}

// ForecastRequest asks a hosted model for forecasts of several series.
type ForecastRequest struct {
	// Model is the provider's model: a model's name, an endpoint or an
	// ARN, as the integration maps it.
	Model   string
	Series  []ForecastSeries
	Horizon int
	Step    Step
	// Levels are the quantiles wanted, sorted, in (0, 1); Interval the
	// coverage of the interval that matters most, for providers that give
	// one interval (BigQuery).
	Levels   []float64
	Interval float64
}

// SeriesForecast is a hosted model's forecast of a series: the point
// forecast, and the quantiles it gave, by level, Horizon steps each.
type SeriesForecast struct {
	Point     []float64   `json:"point"`
	Levels    []float64   `json:"levels"`
	Quantiles [][]float64 `json:"quantiles"`
	// Error, if set, says why the provider did not forecast this series,
	// while it forecast the others.
	Error string `json:"error,omitempty"`
}

// Forecasts are a request's series' forecasts, in order.
type Forecasts struct {
	Series []SeriesForecast
	Usage  Usage
}

// forecaster is a provider with hosted time series models.
type forecaster interface {
	Forecast(ctx context.Context, r ForecastRequest) (Forecasts, error)
	// forecastBatch is how many series a request holds at most.
	forecastBatch() int
}

// Forecasts reports whether the integration's provider has time series
// models.
func (in *Integration) Forecasts() bool {
	switch in.Provider {
	case ProviderNixtla, ProviderSageMaker, ProviderBedrock, ProviderVertex, ProviderBigQuery:
		return true
	}
	return false
}

// ForecastModel reports whether a hosted model forecasts for model, as
// SQL names it: an integration the user may use maps it, and its provider
// has time series models. Such a name is the integration's, not the model
// pack's.
func (rt *Runtime) ForecastModel(ctx context.Context, o StatementOptions, model string) (bool, error) {
	_, version, err := rt.opts.Store.State(ctx)
	if err != nil {
		return false, fmt.Errorf("read the AI cache's state: %w", err)
	}
	bindings, usage, err := rt.load(ctx, version)
	if err != nil {
		return false, err
	}
	for name, b := range bindings {
		if o.Integration != "" && name != o.Integration {
			continue
		}
		if _, ok := b.in.Model(model); ok && b.in.Enabled && b.in.Forecasts() && (o.Admin || mayUse(usage, name, o.User, o.Roles)) {
			return true, nil
		}
	}
	return false, nil
}

// Forecast forecasts series with the hosted model SQL names model, horizon
// steps of step each: one call a series, answered from the cache when the
// same series was forecast the same way before, and sent to the provider in
// batches. errs are the series' own failures; err fails the statement.
func (s *Statement) Forecast(model string, series []ForecastSeries, horizon int, step Step, levels []float64, interval float64,
	refresh bool) (out []SeriesForecast, errs []error, err error) {
	if len(series) == 0 {
		return nil, nil, nil
	}
	b, provider, err := s.binding(ForecastFunction, model, "")
	if err != nil {
		return nil, nil, s.fail(err)
	}
	fc, ok := b.provider.(forecaster)
	if !ok || !b.in.Forecasts() {
		return nil, nil, s.fail(fmt.Errorf("%w: the AI integration %s (%s) has no time series models; map %s on a nixtla, sagemaker, "+
			"bedrock, vertex or bigquery integration", ErrUnsupported, b.in.Name, b.in.Provider, quote(model)))
	}
	levels = slices.Clone(levels)
	for i, q := range levels {
		levels[i] = roundLevel(q)
	}
	slices.Sort(levels)
	levels = slices.Compact(levels)
	calls := make([]*call, len(series))
	for i, sr := range series {
		calls[i] = &call{
			key: cacheKey(ForecastFunction, b.in.Name, provider, strconv.Itoa(horizon), canonical(step), canonical(levels),
				strconv.FormatFloat(interval, 'g', -1, 64), sr.Start.UTC().Format(time.RFC3339Nano), valuesKey(sr.Values)),
			fn: ForecastFunction, b: b, model: provider, refresh: refresh,
			// Tokens, as Nixtla counts them: a value each.
			estimate: Usage{InputTokens: int64(len(sr.Values)), OutputTokens: int64(horizon * max(len(levels), 1))},
			payload:  sr,
		}
	}
	batch := func(ctx context.Context, cs []*call) ([][]byte, Usage, error) {
		req := ForecastRequest{Model: provider, Horizon: horizon, Step: step, Levels: levels, Interval: interval}
		for _, c := range cs {
			req.Series = append(req.Series, c.payload.(ForecastSeries))
		}
		res, err := fc.Forecast(ctx, req)
		if err != nil {
			return nil, res.Usage, err
		}
		if len(res.Series) != len(cs) {
			return nil, res.Usage, rowErrorf("the provider answered with %d forecasts for %d series", len(res.Series), len(cs))
		}
		values := make([][]byte, len(cs))
		for k, f := range res.Series {
			if err := f.check(horizon); f.Error == "" && err != nil {
				return nil, res.Usage, err
			}
			if values[k], err = json.Marshal(f); err != nil {
				return nil, res.Usage, err
			}
		}
		return values, res.Usage, nil
	}
	values, callErrs, err := s.do(calls, batch, fc.forecastBatch())
	if err != nil {
		return nil, nil, err
	}
	out = make([]SeriesForecast, len(series))
	errs = make([]error, len(series))
	for i := range series {
		switch {
		case errors.Is(callErrs[i], errDryRun):
			errs[i] = &RowError{Msg: "not sent: EXPLAIN counts the calls"}
		case callErrs[i] != nil:
			s.rowError(callErrs[i])
			errs[i] = callErrs[i]
		default:
			if err := json.Unmarshal(values[i], &out[i]); err != nil {
				errs[i] = rowErrorf("a cached forecast cannot be read: %v", err)
			} else if out[i].Error != "" {
				errs[i] = &RowError{Msg: out[i].Error}
				s.rowError(errs[i])
			}
		}
	}
	return out, errs, nil
}

// check validates a provider's forecast of horizon steps.
func (f SeriesForecast) check(horizon int) error {
	if len(f.Point) != horizon {
		return rowErrorf("the provider forecast %d steps, not %d", len(f.Point), horizon)
	}
	if len(f.Levels) != len(f.Quantiles) {
		return rowErrorf("the provider's quantiles are malformed")
	}
	for _, q := range f.Quantiles {
		if len(q) != horizon {
			return rowErrorf("the provider gave %d steps of a quantile, not %d", len(q), horizon)
		}
	}
	return nil
}

// valuesKey is a series' values, as bytes, for a cache key.
func valuesKey(vs []float64) string {
	b := make([]byte, 8*len(vs))
	for i, v := range vs {
		binary.LittleEndian.PutUint64(b[8*i:], math.Float64bits(v))
	}
	return string(b)
}

// roundLevel rounds a quantile level to nine places, so that (1 - 0.8) / 2
// is 0.1.
func roundLevel(q float64) float64 { return math.Round(q*1e9) / 1e9 }

// fromQuantiles makes a forecast of the quantiles a provider gave, by
// level, and its point forecast (its median, unless point is given).
func fromQuantiles(byLevel map[float64][]float64, point []float64) SeriesForecast {
	f := SeriesForecast{Point: point}
	levels := make([]float64, 0, len(byLevel))
	for q := range byLevel {
		levels = append(levels, q)
	}
	slices.Sort(levels)
	for _, q := range levels {
		f.Levels = append(f.Levels, roundLevel(q))
		f.Quantiles = append(f.Quantiles, byLevel[q])
	}
	if f.Point == nil {
		f.Point = byLevel[0.5]
	}
	return f
}

// chronosRequest is the body SageMaker's and Bedrock Marketplace's Chronos
// endpoints take.
func chronosRequest(r ForecastRequest, maxLevel float64) map[string]any {
	var inputs []map[string]any
	for _, s := range r.Series {
		inputs = append(inputs, map[string]any{"target": s.Values, "start": s.Start.UTC().Format("2006-01-02T15:04:05")})
	}
	var levels []float64
	for _, q := range r.Levels {
		if q >= 1-maxLevel && q <= maxLevel {
			levels = append(levels, q)
		}
	}
	if !slices.Contains(levels, 0.5) {
		levels = append(levels, 0.5)
		slices.Sort(levels)
	}
	freq := ""
	if len(r.Series) > 0 {
		freq = r.Step.Pandas(r.Series[0].Start)
	}
	return map[string]any{"inputs": inputs, "parameters": map[string]any{"prediction_length": r.Horizon, "quantile_levels": levels,
		"freq": freq}}
}

// chronosForecasts reads Chronos' answer: for each series, "mean" and each
// quantile, by its level.
func chronosForecasts(body []byte, n int) ([]SeriesForecast, error) {
	var out struct {
		Predictions []map[string]json.RawMessage `json:"predictions"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, rowErrorf("the provider's answer is not what was expected: %v", err)
	}
	if len(out.Predictions) != n {
		return nil, rowErrorf("the provider answered with %d forecasts for %d series", len(out.Predictions), n)
	}
	fs := make([]SeriesForecast, n)
	for i, p := range out.Predictions {
		byLevel := map[float64][]float64{}
		var mean []float64
		for k, raw := range p {
			var vs []float64
			if json.Unmarshal(raw, &vs) != nil {
				continue
			}
			if k == "mean" {
				mean = vs
				continue
			}
			if q, err := strconv.ParseFloat(k, 64); err == nil && q > 0 && q < 1 {
				byLevel[q] = vs
			}
		}
		if _, ok := byLevel[0.5]; !ok && mean != nil {
			byLevel[0.5] = mean
		}
		fs[i] = fromQuantiles(byLevel, nil)
	}
	return fs, nil
}
