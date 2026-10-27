package models

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/ml"
	"github.com/akkio-inc/igloo/internal/tsfm"
)

// FORECAST and ANOMALY_DETECTION with a foundation model: CONFIG_OBJECT's
// method names a forecasting model of the model pack (an Igloo extension of
// Snowflake's 'fast' and 'best'), such as 'chronos-2' or 'timesfm-2.5'.
// Nothing is trained: the model forecasts each series zero-shot, from its
// last steps, which the model object keeps (CONFIG_OBJECT's context_window
// of them, 2,048 by default). Intervals are the model's own quantiles.
// Exogenous columns are refused: these models forecast from the target
// alone.

// Foundation forecasts with foundation models (package tsforecast); the
// server passes it in ForecastConfig and ForecastQuery.
type Foundation interface {
	// Model returns the model a method names; ok is false when it names
	// none.
	Model(method string) (name string, ok bool)
	// MaxContext is the most history model reads.
	MaxContext(model string) (int, error)
	// Forecast forecasts horizon steps after each series, whose missing
	// steps are NaN, reading at most context of its last steps.
	Forecast(ctx context.Context, model string, series [][]float32, horizon, context int) ([]tsfm.Forecast, error)
}

// FoundationState is what a foundation model's FORECAST keeps: the model,
// and each series' last steps (in its SeriesState's History, the last one
// step N-1's), with the steps that had no value.
type FoundationState struct {
	Model   string           `json:"model"`
	Context int              `json:"context"`
	Gaps    map[string][]int `json:"gaps,omitempty"`
}

// defaultFoundationContext is how many of each series' last steps a
// foundation model's FORECAST keeps unless context_window says otherwise.
const defaultFoundationContext = 2048

// InferFrequency is the most common gap between the series' distinct
// timestamps (in microseconds, sorted), as FORECAST infers it.
func InferFrequency(series [][]int64) (Frequency, error) { return inferFrequency(series) }

// Index is the step of timestamp t on a grid from start.
func (f Frequency) Index(start, t int64) int { return f.index(start, t) }

// At is the timestamp of step k on a grid from start.
func (f Frequency) At(start int64, k int) int64 { return f.at(start, k) }

// foundationFor returns the foundation model cfg's method names, if any;
// err says the method names nothing.
func foundationFor(cfg ForecastConfig) (string, bool, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Method)) {
	case "", "best", "fast":
		return "", false, nil
	}
	if cfg.Foundation != nil {
		if name, ok := cfg.Foundation.Model(cfg.Method); ok {
			return name, true, nil
		}
	}
	return "", false, inputErr("method is 'best', 'fast' or a forecasting model of the model pack, such as 'chronos-2' or "+
		"'timesfm-2.5', not %q (docs/models.md)", cfg.Method)
}

func (f *Forecaster) foundationCtx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// trainFoundation makes a FORECAST or ANOMALY_DETECTION model of a
// foundation model: each series' last steps, and its evaluation.
func trainFoundation(t *Table, timeCol, targetCol, seriesCol, labelCol string, cfg ForecastConfig, model string) (*Forecaster, error) {
	fd := cfg.Foundation
	f := &Forecaster{AggTarget: strings.ToUpper(cfg.AggTarget), interrupt: cfg.Interrupt}
	if f.AggTarget == "" {
		f.AggTarget = strings.ToUpper(cfg.AggNumeric)
	}
	if f.AggTarget == "" {
		f.AggTarget = "MEAN"
	}
	if err := checkAggregation("aggregation_target", f.AggTarget, false); err != nil {
		return nil, err
	}
	var exog []string
	for _, c := range t.Cols {
		if !slices.ContainsFunc([]string{timeCol, targetCol, seriesCol, labelCol}, func(n string) bool { return n != "" && strings.EqualFold(n, c.Name) }) {
			exog = append(exog, c.Name)
		}
	}
	if len(exog) > 0 {
		return nil, inputErr("method %s forecasts from the target alone: leave the other columns (%s) out of INPUT_DATA", cfg.Method,
			strings.Join(exog, ", "))
	}
	g, err := newGridInput(t, timeCol, targetCol, seriesCol, labelCol, nil)
	if err != nil {
		return nil, err
	}
	if g.target == nil {
		return nil, inputErr("TARGET_COLNAME is required")
	}
	f.TimeCol, f.TargetCol = g.ts.Name, g.target.Name
	if g.seriesC != nil {
		f.SeriesCol = g.seriesC.Name
	}
	maxContext, err := fd.MaxContext(model)
	if err != nil {
		return nil, err
	}
	keep := min(defaultFoundationContext, maxContext)
	if cfg.ContextWindow > 0 {
		if cfg.ContextWindow > maxContext {
			return nil, inputErr("context_window %d is more than %s reads, %d", cfg.ContextWindow, model, maxContext)
		}
		keep = cfg.ContextWindow
	}
	f.Foundation = &FoundationState{Model: model, Context: keep, Gaps: map[string][]int{}}
	keys, groups, skipped := g.bySeries()
	if len(keys) == 0 {
		return nil, inputErr("the input has no rows with a timestamp")
	}
	if skipped > 0 {
		f.Logs = append(f.Logs, SeriesLog{Message: []string{itoa(skipped) + " rows with a NULL timestamp or series were left out"}})
	}
	if cfg.Frequency != "" {
		if f.Freq, err = ParseFrequency(cfg.Frequency); err != nil {
			return nil, err
		}
	} else {
		var stamps [][]int64
		for _, k := range keys {
			var ts []int64
			for _, i := range groups[k] {
				if len(ts) == 0 || ts[len(ts)-1] != g.ts.Micros[i] {
					ts = append(ts, g.ts.Micros[i])
				}
			}
			stamps = append(stamps, ts)
		}
		if f.Freq, err = inferFrequency(stamps); err != nil {
			return nil, err
		}
	}
	f.Lower, f.Upper = cfg.LowerBound, cfg.UpperBound
	var all []*series
	for _, k := range keys {
		rows := groups[k]
		s, err := g.layout(k, rows, f.Freq, g.ts.Micros[rows[0]], f.AggTarget, nil)
		if err != nil {
			return nil, err
		}
		points := 0
		for _, ok := range s.known {
			if ok {
				points++
			}
		}
		if points < 2 {
			err := errSeries(k, inputErr("at least two unique timestamps are required"))
			if cfg.SkipErrors && !f.single() {
				f.Logs = append(f.Logs, SeriesLog{Key: k, Message: []string{err.Error()}})
				continue
			}
			return nil, err
		}
		key := k
		if f.single() {
			key = ""
		}
		st := &SeriesState{Key: key, Start: s.start, N: len(s.y), Points: points}
		from := max(0, len(s.y)-keep)
		st.History = make([]float64, len(s.y)-from)
		for i := from; i < len(s.y); i++ {
			if s.known[i] {
				st.History[i-from] = s.y[i]
				st.Last = s.y[i]
			} else {
				f.Foundation.Gaps[key] = append(f.Foundation.Gaps[key], i-from)
			}
		}
		s.st = st
		f.Series = append(f.Series, st)
		all = append(all, s)
	}
	if len(all) == 0 {
		return nil, inputErr("no series has two points to forecast from")
	}
	f.Logs = append(f.Logs, SeriesLog{Message: []string{"method " + model + ": forecasts zero-shot from each series' last " +
		itoa(keep) + " steps"}})
	interval := cfg.PredictionInterval
	if interval <= 0 || interval >= 1 {
		interval = 0.95
	}
	f.MetricsInterval = interval
	if cfg.Evaluate {
		if err := f.foundationBacktest(cfg, all, interval); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func itoa(n int) string { return strconv.Itoa(n) }

// history is a series' kept steps, NaN where it had none, as float32.
func (f *Forecaster) history(st *SeriesState) []float32 {
	out := make([]float32, len(st.History))
	for i, v := range st.History {
		out[i] = float32(v)
	}
	for _, g := range f.Foundation.Gaps[st.Key] {
		out[g] = float32(math.NaN())
	}
	return out
}

// foundationBacktest measures the model on each series' last steps, as
// scikit-learn's TimeSeriesSplit splits them: forecasts from the steps
// before each split.
func (f *Forecaster) foundationBacktest(cfg ForecastConfig, all []*series, interval float64) error {
	splits := max(cfg.NSplits, 1)
	runs := map[*series][]*foundationRun{}
	for i := range splits {
		var ctxs [][]float32
		var owners []*series
		var cuts, ends []int
		horizon := 0
		for _, s := range all {
			n := len(s.y)
			test := cfg.TestSize
			if test <= 0 {
				test = n / (splits + 1)
			}
			cut := n - (splits-i)*test
			histEnd := cut - cfg.Gap
			if test <= 0 || histEnd < 2 {
				continue
			}
			from := max(0, histEnd-f.Foundation.Context)
			if cfg.MaxTrainSize > 0 {
				from = max(from, histEnd-cfg.MaxTrainSize)
			}
			c := make([]float32, histEnd-from)
			for k := from; k < histEnd; k++ {
				c[k-from] = float32(math.NaN())
				if s.known[k] {
					c[k-from] = float32(s.y[k])
				}
			}
			ctxs, owners = append(ctxs, c), append(owners, s)
			cuts, ends = append(cuts, cut), append(ends, min(cut+test, n))
			horizon = max(horizon, min(cut+test, n)-histEnd)
		}
		if len(ctxs) == 0 || horizon <= 0 {
			continue
		}
		if cfg.Interrupt != nil {
			if err := cfg.Interrupt(); err != nil {
				return err
			}
		}
		fs, err := cfg.Foundation.Forecast(f.foundationCtx(cfg.Context), f.Foundation.Model, ctxs, horizon, f.Foundation.Context)
		if err != nil {
			return err
		}
		for j, s := range owners {
			run := &foundationRun{prev: math.NaN()}
			for k := cuts[j] - cfg.Gap - 1; k >= 0; k-- {
				if s.known[k] {
					run.prev = s.y[k]
					break
				}
			}
			histEnd := cuts[j] - cfg.Gap
			for k := cuts[j]; k < ends[j]; k++ {
				if !s.known[k] {
					continue
				}
				run.add(fs[j], k-histEnd, s.y[k], interval, f)
			}
			if len(run.ys) > 0 {
				runs[s] = append(runs[s], run)
			}
		}
	}
	for _, s := range all {
		if rs := runs[s]; len(rs) > 0 {
			f.Metrics = append(f.Metrics, foundationMetrics(s.st.Key, rs, interval))
		}
	}
	return nil
}

// foundationRun is a series' forecasts of one split, with their intervals.
type foundationRun struct {
	ys, fs, lo, hi []float64
	prev           float64
}

func (r *foundationRun) add(fc tsfm.Forecast, t int, y, interval float64, f *Forecaster) {
	r.ys = append(r.ys, y)
	r.fs = append(r.fs, f.clamp(fc.Point[t]))
	r.lo = append(r.lo, f.clamp(fc.QuantileAt((1-interval)/2, t)))
	r.hi = append(r.hi, f.clamp(fc.QuantileAt((1+interval)/2, t)))
}

func foundationMetrics(key string, runs []*foundationRun, interval float64) SeriesMetrics {
	names := metricNames(interval)
	m := SeriesMetrics{Key: key, Values: map[string]float64{}, StdDev: map[string]float64{}}
	vals := make([][]float64, len(names))
	for _, r := range runs {
		for i, v := range []float64{ml.MAE(r.ys, r.fs), ml.MAPE(r.ys, r.fs), ml.MDA(r.prev, r.ys, r.fs), ml.MSE(r.ys, r.fs),
			ml.SMAPE(r.ys, r.fs), ml.Winkler(r.ys, r.lo, r.hi, 1-interval), ml.Coverage(r.ys, r.lo, r.hi)} {
			vals[i] = append(vals[i], v)
		}
	}
	for i, name := range names {
		m.Values[name] = mean(vals[i])
		if len(vals[i]) > 1 {
			mu := mean(vals[i])
			var sq float64
			for _, v := range vals[i] {
				sq += (v - mu) * (v - mu)
			}
			m.StdDev[name] = math.Sqrt(sq / float64(len(vals[i])-1))
		}
	}
	return m
}

// foundationJob is a series to forecast, and the steps wanted.
type foundationJob struct {
	st   *SeriesState
	want []int
	s    *series
}

// foundationRun forecasts the jobs' series as far as their last wanted
// steps.
func (f *Forecaster) foundationForecasts(q ForecastQuery, jobs []foundationJob) ([]tsfm.Forecast, error) {
	if q.Foundation == nil {
		return nil, inputErr("forecasting with %s is not available here", f.Foundation.Model)
	}
	horizon := 0
	ctxs := make([][]float32, len(jobs))
	for i, j := range jobs {
		ctxs[i] = f.history(j.st)
		if len(j.want) > 0 {
			horizon = max(horizon, j.want[len(j.want)-1]-j.st.N+1)
		}
	}
	if horizon <= 0 {
		return make([]tsfm.Forecast, len(jobs)), nil
	}
	return q.Foundation.Forecast(f.foundationCtx(q.Context), f.Foundation.Model, ctxs, horizon, f.Foundation.Context)
}

// foundationForecast carries out FORECAST.
func (f *Forecaster) foundationForecast(q ForecastQuery) (*Result, error) {
	cols, _ := MethodColumns(ClassForecast, "FORECAST")
	res := &Result{Columns: cols}
	var jobs []foundationJob
	if q.Input == nil {
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
			jobs = append(jobs, foundationJob{st: st, want: want})
		}
	} else {
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
			jobs = append(jobs, foundationJob{st: st, want: want})
		}
	}
	fs, err := f.foundationForecasts(q, jobs)
	if err != nil {
		return nil, err
	}
	for i, j := range jobs {
		for _, k := range j.want {
			t := k - j.st.N
			res.Rows = append(res.Rows, []any{fmtSeries(j.st.Key, f.single()), time.UnixMicro(f.Freq.at(j.st.Start, k)).UTC(),
				f.clamp(fs[i].Point[t]), f.clamp(fs[i].QuantileAt((1-q.Interval)/2, t)), f.clamp(fs[i].QuantileAt((1+q.Interval)/2, t))})
		}
	}
	return res, nil
}

// foundationChecked forecasts the steps of the query's rows that have a
// target.
func (f *Forecaster) foundationChecked(q ForecastQuery) ([]foundationJob, []tsfm.Forecast, error) {
	if q.TargetCol == "" {
		return nil, nil, inputErr("TARGET_COLNAME is required")
	}
	rows, order, err := f.future(q, true)
	if err != nil {
		return nil, nil, err
	}
	var jobs []foundationJob
	for _, st := range order {
		s := rows[st]
		var want []int
		for k := st.N; k < len(s.y); k++ {
			if s.known[k] {
				want = append(want, k)
			}
		}
		jobs = append(jobs, foundationJob{st: st, want: want, s: s})
	}
	fs, err := f.foundationForecasts(q, jobs)
	return jobs, fs, err
}

// foundationDetect carries out DETECT_ANOMALIES: each new point, its
// forecast and interval, and whether it falls outside.
func (f *Forecaster) foundationDetect(q ForecastQuery) (*Result, error) {
	jobs, fs, err := f.foundationChecked(q)
	if err != nil {
		return nil, err
	}
	cols, _ := MethodColumns(ClassAnomaly, "DETECT_ANOMALIES")
	res := &Result{Columns: cols}
	for i, j := range jobs {
		for _, k := range j.want {
			t := k - j.st.N
			fc := fs[i]
			y := j.s.y[k]
			lo, hi := fc.QuantileAt((1-q.Interval)/2, t), fc.QuantileAt((1+q.Interval)/2, t)
			p := fc.CDF(y, t)
			// The distance is y's z-score, with the spread of the forecast's
			// middle 68%.
			var distance any
			if sd := (fc.QuantileAt(0.8413447, t) - fc.QuantileAt(0.1586553, t)) / 2; sd > 0 {
				distance = (y - fc.Point[t]) / sd
			}
			res.Rows = append(res.Rows, []any{fmtSeries(j.st.Key, f.single()), time.UnixMicro(f.Freq.at(j.st.Start, k)).UTC(),
				y, fc.Point[t], lo, hi, y < lo || y > hi, p, distance})
		}
	}
	return res, nil
}

// foundationEvaluate carries out SHOW_EVALUATION_METRICS with new data.
func (f *Forecaster) foundationEvaluate(q ForecastQuery) (*Result, error) {
	jobs, fs, err := f.foundationChecked(q)
	if err != nil {
		return nil, err
	}
	var metrics []SeriesMetrics
	for i, j := range jobs {
		run := &foundationRun{prev: j.st.Last}
		for _, k := range j.want {
			run.add(fs[i], k-j.st.N, j.s.y[k], q.Interval, f)
		}
		if len(run.ys) > 0 {
			metrics = append(metrics, foundationMetrics(j.st.Key, []*foundationRun{run}, q.Interval))
		}
	}
	return f.metricsResult(metrics, q.Interval), nil
}
