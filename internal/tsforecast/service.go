// Package tsforecast forecasts time series with foundation models, for
// every surface Igloo has for them: Snowflake's FORECAST and
// ANOMALY_DETECTION with a model as their method (package models),
// BigQuery's AI.FORECAST and AI.DETECT_ANOMALIES, and Databricks'
// ai_forecast and ai_detect_anomalies (package pgwire writes those as
// queries of this package's functions). Local models run in Go from the
// model pack (packages tsfm and modelpack), on the server and on workers.
package tsforecast

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/modelpack"
	"github.com/akkio-inc/igloo/internal/tsfm"
)

// Service forecasts with a process's models.
type Service struct {
	loader *tsfm.Loader
}

// New returns a service of the models in pack.
func New(pack *modelpack.Pack) *Service { return &Service{loader: tsfm.NewLoader(pack)} }

// ErrInput is a series, horizon or option that cannot be forecast.
var ErrInput = errors.New("invalid forecast")

// Canonical is the name of the model SQL names: lower case, words joined
// by '-', so BigQuery's 'TimesFM 2.5' is the pack's timesfm-2.5.
func Canonical(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.Join(strings.FieldsFunc(n, func(r rune) bool { return r == ' ' || r == '_' }), "-")
	return n
}

// Model returns the forecasting model method names: one of the catalog's,
// installed or not, or one installed from a manifest. ok is false when it
// names none.
func (s *Service) Model(method string) (string, bool) {
	name := Canonical(method)
	if name == timesFM3 {
		// Named, so that using it says why it is not here.
		return name, true
	}
	if m, ok := modelpack.Lookup(name); ok {
		return name, m.Kind == modelpack.KindForecast
	}
	installed, _, err := s.loader.Pack().Installed()
	if err != nil {
		return "", false
	}
	for _, m := range installed {
		if m.Name == name {
			return name, m.Kind == modelpack.KindForecast
		}
	}
	return "", false
}

// Provides returns the model of the pack a name means, if it means one the
// pack can run, installed or not: a model of the catalog, or one installed
// from a manifest; not TimesFM 3.0, which Model names only to refuse it.
func (s *Service) Provides(name string) (string, bool) {
	model, ok := s.Model(name)
	return model, ok && model != timesFM3
}

// timesFM3 is TimesFM 3.0, whose weights are not openly licensed.
const timesFM3 = "timesfm-3.0"

// load returns model name, with how to install it if it is not.
func (s *Service) load(name string) (tsfm.Model, error) {
	if name == timesFM3 {
		return nil, fmt.Errorf("%w: TimesFM 3.0's weights are licensed for non-commercial use only, so the model pack does not "+
			"hold them; forecast with timesfm-2.5, or call TimesFM 3.0 in BigQuery through a bigquery AI integration that maps it "+
			"(docs/ml-functions.md)", modelpack.ErrLicense)
	}
	return s.loader.Load(name)
}

// MaxContext is the most history model reads.
func (s *Service) MaxContext(model string) (int, error) {
	m, err := s.load(model)
	if err != nil {
		return 0, err
	}
	return m.MaxContext(), nil
}

// Forecast forecasts horizon steps after each series, reading at most
// context of their last steps (0: the model's default).
func (s *Service) Forecast(ctx context.Context, model string, series [][]float32, horizon, context int) ([]tsfm.Forecast, error) {
	m, err := s.load(model)
	if err != nil {
		return nil, err
	}
	fs, err := tsfm.ForecastAll(ctx, m, series, horizon, tsfm.Options{Context: context})
	if err != nil {
		return nil, err
	}
	for i := range fs {
		monotone(&fs[i])
	}
	return fs, nil
}

// monotone sorts each step's quantiles, so that intervals never cross
// (the models' quantiles are trained, not constrained, to be in order),
// and takes the point forecast from the sorted median.
func monotone(f *tsfm.Forecast) {
	if len(f.Quantiles) == 0 {
		return
	}
	col := make([]float64, len(f.Quantiles))
	for t := range f.Quantiles[0] {
		sorted := true
		for q := range f.Quantiles {
			col[q] = f.Quantiles[q][t]
			if q > 0 && col[q] < col[q-1] {
				sorted = false
			}
		}
		if sorted {
			continue
		}
		slices.Sort(col)
		for q := range f.Quantiles {
			f.Quantiles[q][t] = col[q]
		}
		if m := slices.Index(f.Levels, 0.5); m >= 0 {
			f.Point[t] = col[m]
		}
	}
}
