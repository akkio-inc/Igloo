package tsfm

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// The fixtures in testdata/tiny are tiny models of each architecture, with
// random weights, built with the official Python classes
// (chronos-forecasting 2.3.2, timesfm at e51928e, torch 2.8.0), and the
// official pipelines' forecasts of a few series: the Go networks must give
// the same numbers.

type fixtureCase struct {
	Series     string     `json:"series"`
	Values     []*float64 `json:"values"`
	Horizon    int        `json:"horizon"`
	Flags      string     `json:"flags"`
	MaxContext int        `json:"max_context"`
	MaxHorizon int        `json:"max_horizon"`
	Point      []float64  `json:"point"`
	Quantiles  []float64  `json:"quantiles"`
}

type fixture struct {
	Levels []float64     `json:"quantile_levels"`
	Cases  []fixtureCase `json:"cases"`
}

func readFixture(t *testing.T, path string) fixture {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func (c fixtureCase) series() []float32 {
	out := make([]float32, len(c.Values))
	for i, v := range c.Values {
		if v == nil {
			out[i] = float32(math.NaN())
		} else {
			out[i] = float32(*v)
		}
	}
	return out
}

func testModel(t *testing.T, dir, arch string) *modelpack.Model {
	t.Helper()
	return &modelpack.Model{Manifest: modelpack.Manifest{Name: "tiny-" + arch, Architecture: arch, Kind: modelpack.KindForecast}, Dir: dir}
}

// close reports whether got is within tolerance of want.
func closeTo(got, want, tol float64) bool {
	return math.Abs(got-want) <= tol*math.Max(1, math.Abs(want))
}

func checkQuantiles(t *testing.T, name string, f Forecast, want []float64, horizon int, tol float64) {
	t.Helper()
	checkQuantilesFrom(t, name, f, want, horizon, tol, horizon, tol)
}

// checkQuantilesFrom checks steps from `from` on with tolerance late: the
// steps a model forecasts from its own forecasts, whose tails' quantiles
// are interpolated where tiny differences in float32 rounding move them.
func checkQuantilesFrom(t *testing.T, name string, f Forecast, want []float64, horizon int, tol float64, from int, late float64) {
	t.Helper()
	if len(want) != len(f.Quantiles)*horizon {
		t.Fatalf("%s: %d expected values for %d quantiles of %d steps", name, len(want), len(f.Quantiles), horizon)
	}
	bad := 0
	for q := range f.Quantiles {
		if len(f.Quantiles[q]) != horizon {
			t.Fatalf("%s: quantile %d has %d steps", name, q, len(f.Quantiles[q]))
		}
		for s := 0; s < horizon; s++ {
			tol := tol
			if s >= from {
				tol = late
			}
			if w := want[q*horizon+s]; !closeTo(f.Quantiles[q][s], w, tol) {
				if bad < 5 {
					t.Errorf("%s: quantile %v step %d = %v, want %v", name, f.Levels[q], s, f.Quantiles[q][s], w)
				}
				bad++
			}
		}
	}
}

func TestChronosBoltTiny(t *testing.T) {
	dir := filepath.Join("testdata", "tiny", "chronos-bolt")
	m, err := loadChronosBolt(testModel(t, dir, "chronos-bolt"))
	if err != nil {
		t.Fatal(err)
	}
	fx := readFixture(t, filepath.Join(dir, "expected.json"))
	for _, c := range fx.Cases {
		f, err := m.Forecast(c.series(), c.Horizon, Options{})
		if err != nil {
			t.Fatalf("%s h=%d: %v", c.Series, c.Horizon, err)
		}
		checkQuantiles(t, c.Series, f, c.Quantiles, c.Horizon, 1e-4)
	}
}

func TestChronos2Tiny(t *testing.T) {
	dir := filepath.Join("testdata", "tiny", "chronos-2")
	m, err := loadChronos2(testModel(t, dir, "chronos-2"))
	if err != nil {
		t.Fatal(err)
	}
	fx := readFixture(t, filepath.Join(dir, "expected.json"))
	single := m.(*chronos2).cfg.Chronos.MaxOutputPatches * m.(*chronos2).cfg.Chronos.OutputPatchSize
	for _, c := range fx.Cases {
		f, err := m.Forecast(c.series(), c.Horizon, Options{})
		if err != nil {
			t.Fatalf("%s h=%d: %v", c.Series, c.Horizon, err)
		}
		checkQuantilesFrom(t, c.Series, f, c.Quantiles, c.Horizon, 1e-4, single, 2e-3)
	}
}

func TestTimesFMTiny(t *testing.T) {
	dir := filepath.Join("testdata", "tiny", "timesfm-2.5")
	m, err := loadTimesFM(testModel(t, dir, "timesfm-2.5"))
	if err != nil {
		t.Fatal(err)
	}
	tf := m.(*timesfm)
	fx := readFixture(t, filepath.Join(dir, "expected.json"))
	for _, c := range fx.Cases {
		flags := recommended
		if c.Flags == "raw" {
			flags = timesfmFlags{}
		}
		f := tf.forecast(c.series(), c.Horizon, c.MaxContext, c.MaxHorizon, flags)
		// The fixture holds all ten channels: the mean, then the deciles.
		want := make([]float64, 0, 9*c.Horizon)
		for q := 1; q < 10; q++ {
			for s := 0; s < c.Horizon; s++ {
				want = append(want, c.Quantiles[s*10+q])
			}
		}
		checkQuantiles(t, c.Series+"/"+c.Flags, f, want, c.Horizon, 5e-4)
		for s := 0; s < c.Horizon; s++ {
			if !closeTo(f.Point[s], c.Point[s], 5e-4) {
				t.Errorf("%s/%s: point %d = %v, want %v", c.Series, c.Flags, s, f.Point[s], c.Point[s])
			}
		}
	}
}
