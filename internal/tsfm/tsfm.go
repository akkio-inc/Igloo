// Package tsfm runs time-series foundation models in Go: Amazon's
// Chronos-Bolt and Chronos-2 and Google's TimesFM 2.5, from their
// published weights in the model pack (package modelpack), on the CPU, as
// their official Python implementations do, without Python, a GPU or a
// runtime library. A model forecasts a series zero-shot: from its history
// alone, no training.
//
// The networks are written out here from the official sources (Apache-2.0),
// layer for layer, and checked against the official implementations'
// outputs: on tiny random models of each architecture in the tests, and on
// the real weights in an opt-in test.
package tsfm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// Forecast is a series' forecast.
type Forecast struct {
	// Levels are the quantile levels of Quantiles, ascending.
	Levels []float64
	// Quantiles holds each level's forecast: Quantiles[l][t] for step t.
	Quantiles [][]float64
	// Point is the point forecast: the median.
	Point []float64
}

// Options tune a forecast.
type Options struct {
	// Context, if positive, is the most recent points of history the
	// model reads (BigQuery's context_window); 0 is the model's default.
	Context int
}

// Model is a forecasting model.
type Model interface {
	// Name is the model's name in the pack.
	Name() string
	// Levels are the quantile levels it forecasts.
	Levels() []float64
	// MaxContext is the most history it reads.
	MaxContext() int
	// MaxHorizon is the most steps it forecasts.
	MaxHorizon() int
	// Forecast forecasts horizon steps after series, whose missing
	// values are NaN.
	Forecast(series []float32, horizon int, opts Options) (Forecast, error)
}

// ErrInput is a series or horizon a model cannot forecast.
var ErrInput = errors.New("invalid forecast input")

// architectures load models by their manifests' architecture.
var architectures = map[string]func(*modelpack.Model) (Model, error){
	"chronos-bolt": loadChronosBolt,
	"chronos-2":    loadChronos2,
	"timesfm-2.5":  loadTimesFM,
}

// Architectures lists the architectures this package runs.
func Architectures() []string {
	out := make([]string, 0, len(architectures))
	for a := range architectures {
		out = append(out, a)
	}
	slices.Sort(out)
	return out
}

// Loader loads models from a pack and keeps them for the process.
type Loader struct {
	pack *modelpack.Pack

	mu     sync.Mutex
	models map[string]*loadedModel
}

type loadedModel struct {
	once  sync.Once
	model Model
	err   error
	from  *modelpack.Model
}

// NewLoader returns a loader of pack's models.
func NewLoader(pack *modelpack.Pack) *Loader {
	return &Loader{pack: pack, models: map[string]*loadedModel{}}
}

// Pack is the loader's pack.
func (l *Loader) Pack() *modelpack.Pack { return l.pack }

// Load returns the forecasting model name: verified by the pack, loaded
// once. It never downloads anything.
func (l *Loader) Load(name string) (Model, error) {
	m, err := l.pack.Load(name)
	if err != nil {
		return nil, err
	}
	if m.Kind != modelpack.KindForecast {
		return nil, fmt.Errorf("%w: model %s is a %s model, not a forecasting one", ErrInput, name, m.Kind)
	}
	l.mu.Lock()
	lm := l.models[name]
	if lm != nil && lm.from != nil && lm.from.Version != m.Version {
		lm = nil
	}
	if lm == nil {
		lm = &loadedModel{from: m}
		l.models[name] = lm
	}
	l.mu.Unlock()
	lm.once.Do(func() {
		load, ok := architectures[m.Architecture]
		if !ok {
			lm.err = fmt.Errorf("model %s has architecture %s; Igloo runs %v", name, m.Architecture, Architectures())
			return
		}
		lm.model, lm.err = load(m)
	})
	if lm.err != nil {
		l.mu.Lock()
		if l.models[name] == lm {
			delete(l.models, name)
		}
		l.mu.Unlock()
	}
	return lm.model, lm.err
}

// ForecastAll forecasts every series, on the machine's threads; it stops
// when ctx ends.
func ForecastAll(ctx context.Context, m Model, series [][]float32, horizon int, opts Options) ([]Forecast, error) {
	out := make([]Forecast, len(series))
	errs := make([]error, len(series))
	workers := min(len(series), threads)
	next := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				active.Add(1)
				out[i], errs[i] = m.Forecast(series[i], horizon, opts)
				active.Add(-1)
			}
		}()
	}
feed:
	for i := range series {
		select {
		case next <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(next)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, errors.Join(errs...)
}

// QuantileAt returns the value of level p of f at step t: interpolated
// between f's levels, and beyond them extrapolated as a normal
// distribution fitted to its outermost levels.
func (f Forecast) QuantileAt(p float64, t int) float64 {
	n := len(f.Levels)
	switch {
	case n == 0:
		return math.NaN()
	case n == 1:
		return f.Quantiles[0][t]
	}
	i := sort.SearchFloat64s(f.Levels, p)
	if i < n && f.Levels[i] == p {
		return f.Quantiles[i][t]
	}
	if i > 0 && i < n {
		lo, hi := f.Levels[i-1], f.Levels[i]
		w := (p - lo) / (hi - lo)
		return f.Quantiles[i-1][t] + w*(f.Quantiles[i][t]-f.Quantiles[i-1][t])
	}
	// Outside: a normal through the outermost pair, centered on the
	// median.
	lo, hi := f.Quantiles[0][t], f.Quantiles[n-1][t]
	zlo, zhi := normalQuantile(f.Levels[0]), normalQuantile(f.Levels[n-1])
	sigma := (hi - lo) / (zhi - zlo)
	mid := f.Point[t]
	if p < f.Levels[0] {
		return math.Min(lo, mid+normalQuantile(p)*sigma)
	}
	return math.Max(hi, mid+normalQuantile(p)*sigma)
}

// CDF returns the level at which f's forecast of step t is y: the
// forecast's distribution function, as QuantileAt inverts it, within
// [0.0001, 0.9999].
func (f Forecast) CDF(y float64, t int) float64 {
	lo, hi := 0.0001, 0.9999
	if y <= f.QuantileAt(lo, t) {
		return lo
	}
	if y >= f.QuantileAt(hi, t) {
		return hi
	}
	for range 50 {
		mid := (lo + hi) / 2
		if f.QuantileAt(mid, t) < y {
			lo = mid
		} else {
			hi = mid
		}
	}
	return (lo + hi) / 2
}

// normalQuantile is the standard normal distribution's quantile function.
func normalQuantile(p float64) float64 { return math.Sqrt2 * math.Erfinv(2*p-1) }

// prepare returns the series' last n points as float32, and whether any
// is a number.
func lastPoints(series []float32, n int) []float32 {
	if n > 0 && len(series) > n {
		return series[len(series)-n:]
	}
	return series
}

func isNaN32(v float32) bool { return v != v }
