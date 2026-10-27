package modelpack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// server serves files over TLS, as a model's upstream does, and counts
// requests.
type server struct {
	srv      *httptest.Server
	files    map[string][]byte
	requests atomic.Int64
}

func newServer(t *testing.T, files map[string][]byte) *server {
	s := &server{files: files}
	s.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		b, ok := s.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// testManifest is a model of two files that s serves.
func testManifest(s *server, name string) Manifest {
	config := []byte(`{"d_model": 4}`)
	weights := []byte(strings.Repeat("w", 1000))
	s.files["/"+name+"/config.json"] = config
	s.files["/"+name+"/model.safetensors"] = weights
	return Manifest{Name: name, Version: "r1", License: "Apache-2.0", Kind: KindForecast, Architecture: "test",
		Source: "https://example.com/" + name, Files: []File{
			{Path: "config.json", URL: s.srv.URL + "/" + name + "/config.json", SHA256: sum(config), Size: int64(len(config))},
			{Path: "model.safetensors", URL: s.srv.URL + "/" + name + "/model.safetensors", SHA256: sum(weights), Size: int64(len(weights))},
		}}
}

func TestInstallAndLoad(t *testing.T) {
	t.Parallel()
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "tiny-test")
	dir := t.TempDir()
	src := Source{Client: s.srv.Client()}
	installed, err := Install(context.Background(), dir, m, src)
	if err != nil || !installed {
		t.Fatalf("install: %v %v", installed, err)
	}
	// Again: nothing to do, nothing downloaded.
	before := s.requests.Load()
	if installed, err := Install(context.Background(), dir, m, src); err != nil || installed || s.requests.Load() != before {
		t.Errorf("reinstall: %v %v, %d requests", installed, err, s.requests.Load()-before)
	}
	p := Open(dir)
	got, bad, err := p.Installed()
	if err != nil || len(got) != 1 || got[0].Name != "tiny-test" || len(bad) != 0 {
		t.Fatalf("installed %v %v %v", got, bad, err)
	}
	model, err := p.Load("tiny-test")
	if err != nil {
		t.Fatal(err)
	}
	if b, err := model.ReadFile("config.json"); err != nil || string(b) != `{"d_model": 4}` {
		t.Errorf("config %q %v", b, err)
	}
	// Loading never downloads.
	before = s.requests.Load()
	s.srv.Close()
	if _, err := Open(dir).Load("tiny-test"); err != nil || s.requests.Load() != before {
		t.Errorf("load after the upstream went away: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("the pack holds %d entries, want the model alone", len(entries))
	}
}

func TestInstallChecksFiles(t *testing.T) {
	t.Parallel()
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "tiny-test")
	dir := t.TempDir()
	src := Source{Client: s.srv.Client()}

	// A file that is not what the manifest pins.
	s.files["/tiny-test/model.safetensors"] = []byte(strings.Repeat("x", 1000))
	if _, err := Install(context.Background(), dir, m, src); !errors.Is(err, ErrVerify) || !strings.Contains(err.Error(), "SHA-256") {
		t.Errorf("a changed file: %v", err)
	}
	// One of another size.
	s.files["/tiny-test/model.safetensors"] = []byte(strings.Repeat("w", 2000))
	if _, err := Install(context.Background(), dir, m, src); !errors.Is(err, ErrVerify) || !strings.Contains(err.Error(), "bytes") {
		t.Errorf("a longer file: %v", err)
	}
	// Nothing was installed, and nothing is left behind.
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("the pack holds %d entries after failed installs", len(entries))
	}
	if _, err := Open(dir).Load("tiny-test"); !errors.Is(err, ErrNotInstalled) || !strings.Contains(err.Error(), "igloo models") {
		t.Errorf("load: %v", err)
	}
}

func TestLicenseRefused(t *testing.T) {
	t.Parallel()
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "tiny-test")
	m.License = "CC-BY-NC-4.0"
	dir := t.TempDir()
	if _, err := Install(context.Background(), dir, m, Source{Client: s.srv.Client()}); !errors.Is(err, ErrLicense) {
		t.Errorf("install: %v", err)
	}
	if s.requests.Load() != 0 {
		t.Error("a refused model was downloaded")
	}
	// A manifest someone wrote into the pack by hand is refused too.
	m.License = "Apache-2.0"
	if _, err := Install(context.Background(), dir, m, Source{Client: s.srv.Client()}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "tiny-test", "manifest.json")
	b, _ := os.ReadFile(path)
	b = []byte(strings.Replace(string(b), `"Apache-2.0"`, `"LicenseRef-non-commercial"`, 1))
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir).Load("tiny-test"); !errors.Is(err, ErrLicense) {
		t.Errorf("load: %v", err)
	}
	for _, l := range []string{"Apache-2.0", "apache-2.0", "MIT"} {
		if !LicenseAllowed(l) {
			t.Errorf("%s refused", l)
		}
	}
	for _, l := range []string{"CC-BY-NC-4.0", "other", "", "Apache-2.0 OR GPL-3.0"} {
		if LicenseAllowed(l) {
			t.Errorf("%s allowed", l)
		}
	}
}

func TestLoadVerifiesFiles(t *testing.T) {
	t.Parallel()
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "tiny-test")
	dir := t.TempDir()
	if _, err := Install(context.Background(), dir, m, Source{Client: s.srv.Client()}); err != nil {
		t.Fatal(err)
	}
	p := Open(dir)
	if _, err := p.Load("tiny-test"); err != nil {
		t.Fatal(err)
	}
	// A file changed on disk, of the same size, is found by a process that
	// loaded the model before, and by a new one.
	path := filepath.Join(dir, "tiny-test", "model.safetensors")
	if err := os.WriteFile(path, []byte(strings.Repeat("v", 1000)), 0o644); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	_ = os.Chtimes(path, later, later)
	for _, pack := range []*Pack{p, Open(dir)} {
		if _, err := pack.Load("tiny-test"); !errors.Is(err, ErrVerify) || !strings.Contains(err.Error(), "installs it again") {
			t.Errorf("a changed file: %v", err)
		}
	}
	// A missing one.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir).Load("tiny-test"); !errors.Is(err, ErrVerify) {
		t.Errorf("a missing file: %v", err)
	}
	// Installing again repairs it.
	if installed, err := Install(context.Background(), dir, m, Source{Client: s.srv.Client()}); err != nil || !installed {
		t.Fatalf("repair: %v %v", installed, err)
	}
	if _, err := p.Load("tiny-test"); err != nil {
		t.Errorf("after the repair: %v", err)
	}
}

func TestCatalog(t *testing.T) {
	t.Parallel()
	names := map[string]bool{}
	for _, m := range Catalog() {
		if err := m.Check(); err != nil {
			t.Errorf("%s: %v", m.Name, err)
		}
		if names[m.Name] {
			t.Errorf("%s twice", m.Name)
		}
		names[m.Name] = true
		for _, f := range m.Files {
			if !strings.Contains(f.URL, "/resolve/"+m.Version+"/") {
				t.Errorf("%s's %s is not pinned to its version: %s", m.Name, f.Path, f.URL)
			}
		}
	}
	for _, n := range []string{"chronos-bolt-tiny", "chronos-bolt-small", "chronos-2", "timesfm-2.5"} {
		if _, ok := Lookup(n); !ok {
			t.Errorf("%s is not in the catalog", n)
		}
	}
	// A manifest may not take a catalog model's name with other files, and
	// an installed one that is not the catalog's does not load.
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "chronos-bolt-tiny")
	dir := t.TempDir()
	if _, err := Install(context.Background(), dir, m, Source{Client: s.srv.Client()}); !errors.Is(err, ErrManifest) {
		t.Errorf("install over a catalog name: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "chronos-bolt-tiny"), 0o755); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "chronos-bolt-tiny", "manifest.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir).Load("chronos-bolt-tiny"); !errors.Is(err, ErrVerify) || !strings.Contains(err.Error(), "pins") {
		t.Errorf("load of another revision: %v", err)
	}
}

func TestManifestCheck(t *testing.T) {
	t.Parallel()
	good := Manifest{Name: "m", Version: "1", License: "MIT", Kind: KindEmbedding, Architecture: "a",
		Files: []File{{Path: "w.bin", URL: "https://example.com/w.bin", SHA256: strings.Repeat("a", 64), Size: 1}}}
	if err := good.Check(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*Manifest){
		"name":     func(m *Manifest) { m.Name = "../m" },
		"kind":     func(m *Manifest) { m.Kind = "llm" },
		"path":     func(m *Manifest) { m.Files[0].Path = "../w.bin" },
		"manifest": func(m *Manifest) { m.Files[0].Path = "manifest.json" },
		"sha":      func(m *Manifest) { m.Files[0].SHA256 = "abc" },
		"size":     func(m *Manifest) { m.Files[0].Size = 0 },
		"http":     func(m *Manifest) { m.Files[0].URL = "http://example.com/w.bin" },
		"files":    func(m *Manifest) { m.Files = nil },
	} {
		m := good
		m.Files = append([]File(nil), good.Files...)
		edit(&m)
		if err := m.Check(); !errors.Is(err, ErrManifest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	path := filepath.Join(t.TempDir(), "m.json")
	if err := os.WriteFile(path, []byte(`{"name": "m", "licence": "MIT"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(path); !errors.Is(err, ErrManifest) {
		t.Errorf("a misspelt key: %v", err)
	}
}

func TestInstallFromMirrorAndDirectory(t *testing.T) {
	t.Parallel()
	s := newServer(t, map[string][]byte{})
	m := testManifest(s, "tiny-test")
	// A mirror holds <model>/<file>; the manifest's URLs are not used.
	mirror := newServer(t, map[string][]byte{
		"/models/tiny-test/config.json":       s.files["/tiny-test/config.json"],
		"/models/tiny-test/model.safetensors": s.files["/tiny-test/model.safetensors"],
	})
	dir := t.TempDir()
	if _, err := Install(context.Background(), dir, m, Source{Client: mirror.srv.Client(), Mirror: mirror.srv.URL + "/models/"}); err != nil {
		t.Fatal(err)
	}
	if s.requests.Load() != 0 || mirror.requests.Load() != 2 {
		t.Errorf("%d requests upstream, %d to the mirror", s.requests.Load(), mirror.requests.Load())
	}
	// A directory holds the files, for hosts without a network.
	from := t.TempDir()
	for _, f := range []string{"config.json", "model.safetensors"} {
		if err := os.WriteFile(filepath.Join(from, f), s.files["/tiny-test/"+f], 0o644); err != nil {
			t.Fatal(err)
		}
	}
	dir2 := t.TempDir()
	if _, err := Install(context.Background(), dir2, m, Source{From: from}); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir2).Load("tiny-test"); err != nil {
		t.Error(err)
	}
	if err := Remove(dir2, "tiny-test"); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir2); len(entries) != 0 {
		t.Errorf("%d entries after remove", len(entries))
	}
}

func TestNoPack(t *testing.T) {
	t.Parallel()
	if _, err := Open("").Load("chronos-bolt-small"); !errors.Is(err, ErrNotInstalled) || !strings.Contains(err.Error(), "IGLOO_MODEL_DIR") {
		t.Errorf("no pack: %v", err)
	}
	if _, err := Open(t.TempDir()).Load("chronos-bolt-small"); !errors.Is(err, ErrNotInstalled) ||
		!strings.Contains(err.Error(), "igloo models install chronos-bolt-small") {
		t.Errorf("not installed: %v", err)
	}
}
