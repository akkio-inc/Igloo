package tsfm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// installTiny installs a tiny fixture model into a pack in dir, as an
// admin does from a manifest.
func installTiny(t *testing.T, dir, name, arch, kind string) {
	t.Helper()
	src := filepath.Join("testdata", "tiny", arch)
	m := modelpack.Manifest{Name: name, Version: "test", License: "Apache-2.0", Kind: kind, Architecture: arch}
	for _, f := range []string{"config.json", "model.safetensors"} {
		b, err := os.ReadFile(filepath.Join(src, f))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		m.Files = append(m.Files, modelpack.File{Path: f, URL: "https://example.com/" + f, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(b))})
	}
	if _, err := modelpack.Install(context.Background(), dir, m, modelpack.Source{From: src}); err != nil {
		t.Fatal(err)
	}
}

func TestLoader(t *testing.T) {
	dir := t.TempDir()
	installTiny(t, dir, "tiny-bolt", "chronos-bolt", modelpack.KindForecast)
	installTiny(t, dir, "tiny-embedder", "chronos-bolt", modelpack.KindEmbedding)
	l := NewLoader(modelpack.Open(dir))
	m, err := l.Load("tiny-bolt")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := l.Load("tiny-bolt"); again != m {
		t.Error("the model was loaded twice")
	}
	series := make([][]float32, 20)
	for i := range series {
		series[i] = make([]float32, 30+i)
		for j := range series[i] {
			series[i][j] = float32(10 + math.Sin(float64(j+i)))
		}
	}
	fs, err := ForecastAll(context.Background(), m, series, 12, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range fs {
		one, _ := m.Forecast(series[i], 12, Options{})
		if len(f.Point) != 12 || f.Point[11] != one.Point[11] || f.QuantileAt(0.5, 3) != f.Point[3] {
			t.Errorf("series %d: %v", i, f.Point)
		}
	}
	if _, err := l.Load("tiny-embedder"); !errors.Is(err, ErrInput) {
		t.Errorf("an embedding model: %v", err)
	}
	if _, err := l.Load("chronos-bolt-small"); !errors.Is(err, modelpack.ErrNotInstalled) || !strings.Contains(err.Error(), "igloo models install") {
		t.Errorf("not installed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ForecastAll(ctx, m, series, 12, Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if _, err := m.Forecast(series[0], 0, Options{}); !errors.Is(err, ErrInput) {
		t.Errorf("no horizon: %v", err)
	}
}

func TestQuantileAt(t *testing.T) {
	f := Forecast{Levels: []float64{0.1, 0.5, 0.9}, Quantiles: [][]float64{{8}, {10}, {12}}, Point: []float64{10}}
	if v := f.QuantileAt(0.3, 0); math.Abs(v-9) > 1e-12 {
		t.Errorf("0.3: %v", v)
	}
	// Beyond the levels: a normal through the outermost ones.
	lo, hi := f.QuantileAt(0.025, 0), f.QuantileAt(0.975, 0)
	sigma := 4 / (2 * 1.2815515655446004)
	if math.Abs(hi-(10+1.959963984540054*sigma)) > 1e-9 || math.Abs(lo-(10-1.959963984540054*sigma)) > 1e-9 {
		t.Errorf("0.025, 0.975: %v %v", lo, hi)
	}
}
