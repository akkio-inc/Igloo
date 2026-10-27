package tsfm

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// The real models, opt-in: IGLOO_TEST_MODEL_DIR names a model pack holding
// them (`igloo models install chronos-bolt-tiny chronos-bolt-small
// chronos-2 chronos-2-small timesfm-2.5`), and testdata/real holds the
// official implementations' forecasts with their weights. CI downloads no
// weights, so it skips these.

func realPack(t testing.TB) *Loader {
	dir := os.Getenv("IGLOO_TEST_MODEL_DIR")
	if dir == "" {
		t.Skip("IGLOO_TEST_MODEL_DIR is not set")
	}
	return NewLoader(modelpack.Open(dir))
}

func TestRealModels(t *testing.T) {
	loader := realPack(t)
	files, _ := filepath.Glob(filepath.Join("testdata", "real", "*.json"))
	for _, path := range files {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			m, err := loader.Load(name)
			if err != nil {
				t.Fatal(err)
			}
			fx := readFixture(t, path)
			for _, c := range fx.Cases {
				start := time.Now()
				var f Forecast
				switch tm := m.(type) {
				case *timesfm:
					f = tm.forecast(c.series(), c.Horizon, c.MaxContext, c.MaxHorizon, recommended)
					want := make([]float64, 0, 9*c.Horizon)
					for q := 1; q < 10; q++ {
						for s := 0; s < c.Horizon; s++ {
							want = append(want, c.Quantiles[s*10+q])
						}
					}
					c.Quantiles = want
				default:
					if f, err = m.Forecast(c.series(), c.Horizon, Options{}); err != nil {
						t.Fatal(err)
					}
				}
				label := fmt.Sprintf("%s h=%d", c.Series, c.Horizon)
				checkReal(t, label, f, c.Quantiles, c.Horizon)
				t.Logf("%s: %v", label, time.Since(start))
			}
		})
	}
}

// checkReal compares forecasts relative to the forecast's spread: the
// models' float32 arithmetic, summed in other orders than PyTorch's, moves
// values by a little of their spread.
func checkReal(t *testing.T, name string, f Forecast, want []float64, horizon int) {
	t.Helper()
	nq := len(f.Quantiles)
	var worst float64
	for s := 0; s < horizon; s++ {
		spread := math.Abs(want[(nq-1)*horizon+s]-want[s]) + 1e-6
		for q := 0; q < nq; q++ {
			if d := math.Abs(f.Quantiles[q][s]-want[q*horizon+s]) / spread; d > worst {
				worst = d
			}
		}
	}
	if worst > 0.01 {
		t.Errorf("%s: forecasts differ from the official ones by up to %.3g of their spread", name, worst)
	}
	t.Logf("%s: worst difference %.2g of the spread", name, worst)
}

// BenchmarkRealModels measures throughput: series a second, every thread
// forecasting series of a context length and horizon.
//
//	IGLOO_TEST_MODEL_DIR=... go test -run x -bench RealModels ./internal/tsfm/
func BenchmarkRealModels(b *testing.B) {
	loader := realPack(b)
	for _, name := range []string{"chronos-bolt-tiny", "chronos-bolt-small", "chronos-bolt-base", "chronos-2-small", "chronos-2", "timesfm-2.5"} {
		m, err := loader.Load(name)
		if err != nil {
			b.Logf("%s: %v", name, err)
			continue
		}
		for _, shape := range []struct{ context, horizon int }{{128, 24}, {512, 24}, {512, 96}, {2048, 96}} {
			b.Run(fmt.Sprintf("%s/context=%d/horizon=%d", name, shape.context, shape.horizon), func(b *testing.B) {
				series := make([][]float32, 4*threads)
				for i := range series {
					s := make([]float32, shape.context)
					for t := range s {
						s[t] = float32(10 + 3*math.Sin(2*math.Pi*float64(t)/7+float64(i)) + 0.01*float64(t))
					}
					series[i] = s
				}
				b.ResetTimer()
				start := time.Now()
				n := 0
				for range b.N {
					if _, err := ForecastAll(context.Background(), m, series, shape.horizon, Options{Context: shape.context}); err != nil {
						b.Fatal(err)
					}
					n += len(series)
				}
				b.ReportMetric(float64(n)/time.Since(start).Seconds(), "series/s")
			})
		}
	}
}
