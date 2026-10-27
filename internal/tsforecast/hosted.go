package tsforecast

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/tsfm"
)

// Hosted models: the functions of Register as ai_ts_forecast and
// ai_ts_detect, whose model is one an AI integration maps (package ai):
// TimeGPT, Chronos and TimesFM endpoints, and BigQuery's TimesFM 3.0. They
// lay series out and score points as the model pack's functions do, and
// call the provider through the statement's ai.Statement, with its caps,
// cache and usage, so they run only where the statement's AI functions do:
// on the coordinator.

// hostedContext is how many of a series' last steps a hosted model is sent
// unless the call's context window says otherwise.
const hostedContext = 2048

// hostedLevels are the quantiles asked of hosted models besides the
// interval's bounds: the deciles, which the scores' distributions are read
// from.
var hostedLevels = []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}

// hosted forecasts with the hosted models of a statement's integrations.
type hosted struct{ s *ai.Statement }

func (h hosted) model(name string) (string, bool) { return name, strings.TrimSpace(name) != "" }

// run sends each job's series, its last steps with gaps filled in, to the
// model, grouped by what the requests share.
func (h hosted) run(_ context.Context, jobs []*job) ([]tsfm.Forecast, []error) {
	out := make([]tsfm.Forecast, len(jobs))
	errs := make([]error, len(jobs))
	type key struct {
		model    string
		horizon  int
		interval float64
		step     ai.Step
	}
	groups := map[key][]int{}
	var order []key
	series := make([]ai.ForecastSeries, len(jobs))
	for i, j := range jobs {
		s, err := hostedSeries(j)
		if err != nil {
			errs[i] = err
			continue
		}
		series[i] = s
		k := key{j.model, j.horizon, j.interval, ai.Step{Months: j.grid.Freq.Months, Micros: j.grid.Freq.Micros}}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], i)
	}
	for _, k := range order {
		idx := groups[k]
		batch := make([]ai.ForecastSeries, len(idx))
		for n, i := range idx {
			batch[n] = series[i]
		}
		levels := slices.Clone(hostedLevels)
		if k.interval > 0 && k.interval < 1 {
			levels = append(levels, (1-k.interval)/2, (1+k.interval)/2)
		}
		fs, ferrs, err := h.s.Forecast(k.model, batch, k.horizon, k.step, levels, k.interval, false)
		for n, i := range idx {
			switch {
			case err != nil:
				errs[i] = err
			case ferrs[n] != nil:
				errs[i] = fmt.Errorf("%w: %s", ErrInput, ferrs[n].Error())
			default:
				f := tsfm.Forecast{Levels: fs[n].Levels, Quantiles: fs[n].Quantiles, Point: fs[n].Point}
				if len(f.Levels) == 0 {
					// A point forecast alone: an interval of nothing.
					f.Levels, f.Quantiles = []float64{0.5}, [][]float64{slices.Clone(f.Point)}
				}
				monotone(&f)
				out[i] = f
			}
		}
	}
	return out, errs
}

// hostedSeries is a job's series as a hosted model takes it: its last
// steps, at most the context window's, from its first value, with gaps
// filled in between their neighbours and a missing last step by the value
// before.
func hostedSeries(j *job) (ai.ForecastSeries, error) {
	vals := j.context
	limit := j.opts.Context
	if limit <= 0 {
		limit = hostedContext
	}
	if len(vals) > limit {
		vals = vals[len(vals)-limit:]
	}
	first := slices.IndexFunc(vals, func(v float32) bool { return !isNaN(v) })
	if first < 0 {
		return ai.ForecastSeries{}, fmt.Errorf("%w: the series has no values before the steps to forecast", ErrInput)
	}
	offset := j.from - len(vals) + first
	vals = vals[first:]
	out := make([]float64, len(vals))
	known := 0
	for k := 0; k < len(vals); {
		if !isNaN(vals[k]) {
			out[k] = float64(vals[k])
			known++
			k++
			continue
		}
		// A gap from k to the next value, or to the end.
		end := k
		for end < len(vals) && isNaN(vals[end]) {
			end++
		}
		before := out[k-1]
		for m := k; m < end; m++ {
			if end < len(vals) {
				after := float64(vals[end])
				out[m] = before + (after-before)*float64(m-k+1)/float64(end-k+1)
			} else {
				out[m] = before
			}
		}
		k = end
	}
	if known < 2 {
		return ai.ForecastSeries{}, fmt.Errorf("%w: the series has %d points before the steps to forecast; it needs at least 2", ErrInput, known)
	}
	if slices.ContainsFunc(out, func(v float64) bool { return math.IsInf(v, 0) || math.IsNaN(v) }) {
		return ai.ForecastSeries{}, fmt.Errorf("%w: the series has infinite values", ErrInput)
	}
	return ai.ForecastSeries{Values: out, Start: time.UnixMicro(j.grid.At(offset)).UTC()}, nil
}

// hostedKey carries, in a call's bind context, how it finds its statement.
type hostedKey struct{}

// RegisterHosted defines ai_ts_forecast and ai_ts_detect on conn's
// database: Register's functions, with hosted models. hook finds the
// statement that calls them, as the AI functions' does; where it returns
// nil, as on workers, every call fails.
func RegisterHosted(conn *sql.Conn, hook func() ai.Hook) error {
	forecast, detect, err := types()
	if err != nil {
		return err
	}
	if err := duckdb.RegisterScalarUDF(conn, ai.ForecastFunction, &hostedUDF{hook: hook, sig: forecast}); err != nil {
		return err
	}
	return duckdb.RegisterScalarUDF(conn, ai.DetectFunction, &hostedUDF{hook: hook, sig: detect, detect: true})
}

// hostedUDF is a hosted function: volatile, as the AI functions are, so
// that DuckDB neither folds a call of constants nor merges two calls.
type hostedUDF struct {
	hook   func() ai.Hook
	sig    signature
	detect bool
}

func (u *hostedUDF) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{InputTypeInfos: u.sig.args, ResultTypeInfo: u.sig.result, Volatile: true}
}

func (u *hostedUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		ScalarBinder: func(ctx context.Context, _ []duckdb.ScalarUDFArg) (context.Context, error) {
			var get func() *ai.Statement
			if h := u.hook(); h != nil {
				get = h.Bind(ctx)
			}
			return context.WithValue(context.WithoutCancel(ctx), hostedKey{}, get), nil
		},
		ChunkContextExecutor: func(ctx context.Context, chunk *duckdb.ChunkIteratorState) error {
			get, _ := ctx.Value(hostedKey{}).(func() *ai.Statement)
			var s *ai.Statement
			if get != nil {
				s = get()
			}
			if s == nil {
				return ai.ErrNoStatement
			}
			return evalChunk(s.Context(), chunk, hosted{s}, u.detect)
		},
	}
}
