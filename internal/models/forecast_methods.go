package models

import (
	"context"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/ml"
)

// ForecastQuery is a call of FORECAST, DETECT_ANOMALIES or
// SHOW_EVALUATION_METRICS with new data.
type ForecastQuery struct {
	// Periods is FORECASTING_PERIODS, without new data.
	Periods int
	// SeriesValue, if set, forecasts that series alone.
	SeriesValue *string
	// Input holds the rows to forecast (with exogenous values) or to
	// check (with the target), named by TimeCol, TargetCol and SeriesCol.
	Input                         *Table
	TimeCol, TargetCol, SeriesCol string
	// Interval is the prediction interval, as a fraction.
	Interval float64
	// SkipErrors leaves out series that cannot be forecast.
	SkipErrors bool
	// Foundation forecasts a foundation model's series, and Context is
	// the statement's (see foundation.go).
	Foundation Foundation
	Context    context.Context
}

// step is one forecast: a series' step, its forecast and interval, and the
// actual value when there is one.
type step struct {
	st       *SeriesState
	k        int
	forecast float64
	sigma    float64
	y        float64
	hasY     bool
}

// future lays the query's rows out on the series' grids from their
// training start, and returns each series' steps to forecast and their
// rows' values.
func (f *Forecaster) future(q ForecastQuery, withTarget bool) (map[*SeriesState]*series, []*SeriesState, error) {
	exogNames := make([]string, len(f.Exog))
	for i, e := range f.Exog {
		exogNames[i] = e.Name
	}
	target := ""
	if withTarget {
		target = q.TargetCol
	}
	if !f.single() && q.SeriesCol == "" {
		return nil, nil, inputErr("the model has several series: name the input's SERIES_COLNAME")
	}
	if f.single() && q.SeriesCol != "" {
		return nil, nil, inputErr("the model was trained on one series; SERIES_COLNAME is not for it")
	}
	g, err := newGridInput(q.Input, q.TimeCol, target, q.SeriesCol, "", exogNames)
	if err != nil {
		return nil, nil, err
	}
	keys, groups, _ := g.bySeries()
	out := map[*SeriesState]*series{}
	var order []*SeriesState
	for _, key := range keys {
		st := f.seriesByKey(key)
		if st == nil {
			if q.SkipErrors {
				continue
			}
			return nil, nil, inputErr("the model has no series %s", key)
		}
		if q.SeriesValue != nil && key != *q.SeriesValue {
			continue
		}
		s, err := g.layout(key, groups[key], f.Freq, st.Start, f.AggTarget, f.ExogAgg)
		if err != nil {
			return nil, nil, err
		}
		first := f.Freq.index(st.Start, g.ts.Micros[groups[key][0]])
		if first < st.N {
			return nil, nil, errSeries(key, inputErr("the timestamps to %s must come after the training data's, which end at %s",
				map[bool]string{true: "check", false: "forecast"}[withTarget],
				time.UnixMicro(f.Freq.at(st.Start, st.N-1)).UTC().Format("2006-01-02 15:04:05")))
		}
		out[st] = s
		order = append(order, st)
	}
	return out, order, nil
}

// forecastSteps forecasts a series' steps from N through the last of
// want (sorted), and returns those of want.
func (f *Forecaster) forecastSteps(st *SeriesState, want []int, ex exogAt) []step {
	if len(want) == 0 {
		return nil
	}
	last := want[len(want)-1]
	out := make([]step, 0, len(want))
	if st.Naive || (f.Model == nil && f.Linear == nil) {
		for _, k := range want {
			h := k - st.N + 1
			out = append(out, step{st: st, k: k, forecast: f.clamp(st.Last), sigma: st.NaiveSigma * math.Sqrt(float64(h))})
		}
		return out
	}
	path := f.path(f.current(), st, st.History, st.N, st.N, last, ex)
	for _, k := range want {
		h := k - st.N + 1
		out = append(out, step{st: st, k: k, forecast: f.level(st, k, path[k-st.N]), sigma: f.sigma(h) * st.Scale})
	}
	return out
}

func (f *Forecaster) clamp(v float64) float64 {
	if f.Lower != nil && v < *f.Lower {
		v = *f.Lower
	}
	if f.Upper != nil && v > *f.Upper {
		v = *f.Upper
	}
	return v
}

func noExog(int, int) (float64, string, bool) { return math.NaN(), "", false }

// Forecast carries out the FORECAST method.
func (f *Forecaster) Forecast(q ForecastQuery) (*Result, error) {
	if f.Foundation != nil {
		return f.foundationForecast(q)
	}
	cols, _ := MethodColumns(ClassForecast, "FORECAST")
	res := &Result{Columns: cols}
	z := ml.NormalQuantile(0.5 + q.Interval/2)
	emit := func(steps []step) {
		for _, s := range steps {
			w := z * s.sigma
			res.Rows = append(res.Rows, []any{fmtSeries(s.st.Key, f.single()), time.UnixMicro(f.Freq.at(s.st.Start, s.k)).UTC(),
				s.forecast, f.clamp(s.forecast - w), f.clamp(s.forecast + w)})
		}
	}
	if q.Input == nil {
		if len(f.Exog) > 0 {
			names := make([]string, len(f.Exog))
			for i, e := range f.Exog {
				names[i] = e.Name
			}
			return nil, inputErr("the model was trained with the exogenous columns %s: forecast with INPUT_DATA holding their future "+
				"values, and TIMESTAMP_COLNAME", strings.Join(names, ", "))
		}
		if q.Periods <= 0 {
			return nil, inputErr("FORECASTING_PERIODS must be a positive number")
		}
		if q.SeriesValue != nil && f.seriesByKey(*q.SeriesValue) == nil {
			return nil, inputErr("the model has no series %s", *q.SeriesValue)
		}
		for _, st := range f.Series {
			if q.SeriesValue != nil && st.Key != *q.SeriesValue {
				continue
			}
			want := make([]int, q.Periods)
			for i := range want {
				want[i] = st.N + i
			}
			emit(f.forecastSteps(st, want, noExog))
		}
		return res, nil
	}
	rows, order, err := f.future(q, false)
	if err != nil {
		return nil, err
	}
	for _, st := range order {
		s := rows[st]
		var want []int
		for k := st.N; k < len(s.y); k++ {
			if s.present(k) {
				want = append(want, k)
			}
		}
		emit(f.forecastSteps(st, want, seriesExog(s)))
	}
	return res, nil
}

// present reports whether the input had a row at step k.
func (s *series) present(k int) bool {
	if k >= len(s.y) {
		return false
	}
	if s.known[k] {
		return true
	}
	for j := range s.num {
		if s.num[j] != nil && !math.IsNaN(s.num[j][k]) || s.catOK[j] != nil && s.catOK[j][k] {
			return true
		}
	}
	return s.rowAt != nil && s.rowAt[k]
}

// checked forecasts the steps of the query's rows that have a target.
func (f *Forecaster) checked(q ForecastQuery) ([]step, error) {
	if q.TargetCol == "" {
		return nil, inputErr("TARGET_COLNAME is required")
	}
	rows, order, err := f.future(q, true)
	if err != nil {
		return nil, err
	}
	var out []step
	for _, st := range order {
		s := rows[st]
		var want []int
		for k := st.N; k < len(s.y); k++ {
			if s.known[k] {
				want = append(want, k)
			}
		}
		steps := f.forecastSteps(st, want, seriesExog(s))
		for i := range steps {
			steps[i].y, steps[i].hasY = s.y[steps[i].k], true
		}
		out = append(out, steps...)
	}
	return out, nil
}

// Detect carries out DETECT_ANOMALIES: each new point, its forecast and
// interval, and whether it falls outside.
func (f *Forecaster) Detect(q ForecastQuery) (*Result, error) {
	if f.Foundation != nil {
		return f.foundationDetect(q)
	}
	steps, err := f.checked(q)
	if err != nil {
		return nil, err
	}
	cols, _ := MethodColumns(ClassAnomaly, "DETECT_ANOMALIES")
	res := &Result{Columns: cols}
	z := ml.NormalQuantile(0.5 + q.Interval/2)
	for _, s := range steps {
		w := z * s.sigma
		lo, hi := s.forecast-w, s.forecast+w
		var distance, percentile any
		switch {
		case s.sigma > 0:
			d := (s.y - s.forecast) / s.sigma
			distance, percentile = d, ml.NormalCDF(d)
		case s.y == s.forecast:
			distance, percentile = 0.0, 0.5
		}
		res.Rows = append(res.Rows, []any{fmtSeries(s.st.Key, f.single()), time.UnixMicro(f.Freq.at(s.st.Start, s.k)).UTC(),
			s.y, s.forecast, lo, hi, s.y < lo || s.y > hi, percentile, distance})
	}
	return res, nil
}

// EvaluateOn carries out SHOW_EVALUATION_METRICS with new data: the
// metrics of the forecasts of its points.
func (f *Forecaster) EvaluateOn(q ForecastQuery) (*Result, error) {
	if f.Foundation != nil {
		return f.foundationEvaluate(q)
	}
	steps, err := f.checked(q)
	if err != nil {
		return nil, err
	}
	bySeries := map[*SeriesState]*cvRun{}
	var order []*SeriesState
	for _, s := range steps {
		r := bySeries[s.st]
		if r == nil {
			r = &cvRun{prev: s.st.Last, scale: s.st.Scale}
			bySeries[s.st] = r
			order = append(order, s.st)
		}
		r.ys, r.fs, r.hs = append(r.ys, s.y), append(r.fs, s.forecast), append(r.hs, s.k-s.st.N+1)
	}
	var metrics []SeriesMetrics
	for _, st := range order {
		r := bySeries[st]
		if st.Naive {
			// Naive forecasts' spread is the series' own.
			r.scale = 1
			sav := f.Sigma
			f.Sigma = []float64{st.NaiveSigma}
			metrics = append(metrics, f.seriesMetrics(st.Key, []*cvRun{r}, q.Interval))
			f.Sigma = sav
			continue
		}
		metrics = append(metrics, f.seriesMetrics(st.Key, []*cvRun{r}, q.Interval))
	}
	return f.metricsResult(metrics, q.Interval), nil
}

func (f *Forecaster) metricsResult(metrics []SeriesMetrics, interval float64) *Result {
	cols, _ := MethodColumns(ClassForecast, "SHOW_EVALUATION_METRICS")
	res := &Result{Columns: cols}
	for _, m := range metrics {
		for _, name := range metricNames(interval) {
			var sd any
			if v, ok := m.StdDev[name]; ok {
				sd = v
			}
			res.Rows = append(res.Rows, []any{fmtSeries(m.Key, f.single()), name, m.Values[name], sd, nil})
		}
	}
	return res
}

// Method computes one of the methods that need no new data.
func (f *Forecaster) Method(class, method string) (*Result, error) {
	cols, ok := MethodColumns(class, method)
	if !ok {
		return nil, inputErr("SNOWFLAKE.ML.%s models have no method %s", class, method)
	}
	res := &Result{Columns: cols}
	switch method {
	case "SHOW_EVALUATION_METRICS":
		if f.Metrics == nil {
			return nil, errNoEvaluation(method)
		}
		return f.metricsResult(f.Metrics, f.MetricsInterval), nil
	case "EXPLAIN_FEATURE_IMPORTANCE":
		for _, w := range f.Importance {
			order := make([]int, len(w.Features))
			for i := range order {
				order[i] = i
			}
			slices.SortStableFunc(order, func(a, b int) int {
				switch {
				case w.Scores[a] > w.Scores[b]:
					return -1
				case w.Scores[a] < w.Scores[b]:
					return 1
				}
				return 0
			})
			for rank, i := range order {
				res.Rows = append(res.Rows, []any{fmtSeries(w.Key, f.single()), int64(rank + 1), w.Features[i], w.Scores[i], w.Types[i]})
			}
		}
		return res, nil
	case "SHOW_TRAINING_LOGS":
		logs := map[string][]string{}
		for _, l := range f.Logs {
			logs[l.Key] = append(logs[l.Key], l.Message...)
		}
		for _, st := range f.Series {
			var v any
			if m := logs[st.Key]; len(m) > 0 {
				v = jsonArray(m)
			}
			res.Rows = append(res.Rows, []any{fmtSeries(st.Key, f.single()), v})
		}
		if general := logs[""]; len(general) > 0 && !f.single() {
			res.Rows = append(res.Rows, []any{nil, jsonArray(general)})
		}
		return res, nil
	}
	return nil, inputErr("%s needs data: name INPUT_DATA", method)
}
