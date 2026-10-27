package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/modelpack"
)

func TestModelsCommand(t *testing.T) {
	dir, from := t.TempDir(), t.TempDir()
	weights := []byte("weights")
	sum := sha256.Sum256(weights)
	if err := os.WriteFile(filepath.Join(from, "w.bin"), weights, 0o644); err != nil {
		t.Fatal(err)
	}
	m := modelpack.Manifest{Name: "demo-forecaster", Version: "1", License: "MIT", Kind: modelpack.KindForecast, Architecture: "test",
		Files: []modelpack.File{{Path: "w.bin", URL: "https://example.com/w.bin", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(weights))}}}
	b, _ := json.Marshal(m)
	manifest := filepath.Join(t.TempDir(), "demo.json")
	if err := os.WriteFile(manifest, b, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := modelsCommand(args, &out)
		return out.String(), err
	}
	if out, err := run("install", "--dir", dir, "--from", from, "--manifest", manifest); err != nil || !strings.Contains(out, "demo-forecaster: installed 1, MIT, verified") {
		t.Fatalf("install: %q %v", out, err)
	}
	out, err := run("list", "--dir", dir)
	if err != nil || !strings.Contains(out, "chronos-bolt-small") || !strings.Contains(out, "demo-forecaster") ||
		!strings.Contains(out, "yes (from a manifest)") {
		t.Errorf("list: %q %v", out, err)
	}
	if out, err := run("verify", "--dir", dir); err != nil || !strings.Contains(out, "demo-forecaster: verified") {
		t.Errorf("verify: %q %v", out, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "demo-forecaster", "w.bin"), []byte("WEIGHTS"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := run("verify", "--dir", dir, "demo-forecaster"); err == nil || !strings.Contains(out, "SHA-256") {
		t.Errorf("verify a changed file: %q %v", out, err)
	}
	if _, err := run("install", "--dir", dir, "no-such-model"); err == nil || !strings.Contains(err.Error(), "igloo models list") {
		t.Errorf("unknown model: %v", err)
	}
	if _, err := run("install", "--dir", "relative/dir", "chronos-bolt-tiny"); err == nil {
		t.Error("a relative directory was used")
	}
	if out, err := run("remove", "--dir", dir, "demo-forecaster"); err != nil || !strings.Contains(out, "removed") {
		t.Errorf("remove: %q %v", out, err)
	}
	if _, err := run("frobnicate"); err == nil || !strings.Contains(err.Error(), "usage") {
		t.Errorf("unknown command: %v", err)
	}
}
