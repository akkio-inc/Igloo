// Package tsforecasttest installs a tiny forecasting model for tests,
// which forecast with it without downloading a model.
package tsforecasttest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

// InstallTiny installs the tiny Chronos-Bolt model of package tsfm's
// tests, random weights in the real architecture, as name, in a model
// pack in dir. Its forecasts are the architecture's, not good ones.
func InstallTiny(t testing.TB, dir, name string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no source path")
	}
	src := filepath.Join(filepath.Dir(file), "..", "..", "tsfm", "testdata", "tiny", "chronos-bolt")
	m := modelpack.Manifest{Name: name, Version: "test", License: "Apache-2.0", Kind: modelpack.KindForecast, Architecture: "chronos-bolt"}
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
