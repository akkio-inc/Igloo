package modelpack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// manifestFile is the name of a model's manifest in its directory.
const manifestFile = "manifest.json"

// Pack is the model pack in a directory: a directory per model, holding
// its manifest.json and its files.
type Pack struct {
	dir string

	mu sync.Mutex
	// loaded are the models this process verified, by name, and the
	// files' sizes and times they had then.
	loaded map[string]*loaded
}

type loaded struct {
	once  sync.Once
	done  atomic.Bool
	model *Model
	err   error
	stats []fileStat
}

type fileStat struct {
	size int64
	mod  time.Time
}

// Open returns the pack in dir; "" is a pack that holds nothing.
func Open(dir string) *Pack { return &Pack{dir: dir, loaded: map[string]*loaded{}} }

// Dir is the pack's directory.
func (p *Pack) Dir() string { return p.dir }

// Model is a model the pack holds, its files verified.
type Model struct {
	Manifest
	// Dir is the model's directory.
	Dir string
}

// Path returns the path of the model's file name.
func (m *Model) Path(name string) string { return filepath.Join(m.Dir, name) }

// ReadFile reads the model's file name.
func (m *Model) ReadFile(name string) ([]byte, error) { return os.ReadFile(m.Path(name)) }

// Installed lists the manifests of the models in the pack, by name; a
// model whose manifest cannot be read is listed with its error.
func (p *Pack) Installed() ([]Manifest, map[string]error, error) {
	if p.dir == "" {
		return nil, nil, nil
	}
	entries, err := os.ReadDir(p.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var out []Manifest
	bad := map[string]error{}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		m, err := readManifest(filepath.Join(p.dir, e.Name()))
		if err != nil {
			bad[e.Name()] = err
			continue
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b Manifest) int { return strings.Compare(a.Name, b.Name) })
	return out, bad, nil
}

func readManifest(dir string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("%w: %s: %v", ErrManifest, filepath.Join(dir, manifestFile), err)
	}
	if m.Name != filepath.Base(dir) {
		return m, fmt.Errorf("%w: %s names the model %q", ErrManifest, filepath.Join(dir, manifestFile), m.Name)
	}
	return m, m.Check()
}

// Load returns the pack's model name, its files verified: their sizes and
// SHA-256 are their manifest's, its license is allowed, and a model of the
// catalog is the catalog's. A process verifies a model's files once, and
// again if they change on disk. It never downloads anything.
func (p *Pack) Load(name string) (*Model, error) {
	if p.dir == "" {
		return nil, fmt.Errorf("model %s is %w: the model pack is not set up (IGLOO_MODEL_DIR); an admin sets it, and installs "+
			"the model with `igloo models install %s` (docs/models.md)", name, ErrNotInstalled, name)
	}
	if !nameRE.MatchString(name) {
		return nil, fmt.Errorf("model %q is %w", name, ErrNotInstalled)
	}
	dir := filepath.Join(p.dir, name)
	p.mu.Lock()
	l := p.loaded[name]
	if l != nil && l.done.Load() {
		if stats, err := statFiles(dir, l); err != nil || !slices.Equal(stats, l.stats) {
			// Its files changed since: verify them again.
			l = nil
		}
	}
	if l == nil {
		l = &loaded{}
		p.loaded[name] = l
	}
	p.mu.Unlock()
	l.once.Do(func() {
		l.model, l.stats, l.err = p.verify(name, dir)
		l.done.Store(true)
	})
	if l.err != nil {
		// Try again next time: an admin may fix it.
		p.mu.Lock()
		if p.loaded[name] == l {
			delete(p.loaded, name)
		}
		p.mu.Unlock()
	}
	return l.model, l.err
}

func statFiles(dir string, l *loaded) ([]fileStat, error) {
	if l.model == nil {
		return nil, errors.New("not loaded")
	}
	out := make([]fileStat, len(l.model.Files))
	for i, f := range l.model.Files {
		st, err := os.Stat(filepath.Join(dir, f.Path))
		if err != nil {
			return nil, err
		}
		out[i] = fileStat{st.Size(), st.ModTime()}
	}
	return out, nil
}

func (p *Pack) verify(name, dir string) (*Model, []fileStat, error) {
	m, err := readManifest(dir)
	if errors.Is(err, fs.ErrNotExist) {
		hint := "an admin installs it with `igloo models install " + name + "`"
		if _, ok := Lookup(name); !ok {
			hint = "the models Igloo pins are listed by `igloo models list`"
		}
		return nil, nil, fmt.Errorf("model %s is %w in the model pack (%s); %s (docs/models.md)", name, ErrNotInstalled, p.dir, hint)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("model %s: %w", name, err)
	}
	if c, ok := Lookup(name); ok && !c.same(m) {
		return nil, nil, fmt.Errorf("model %s: %w: the pack holds revision %s, and this Igloo pins %s; an admin installs it again "+
			"with `igloo models install %s`", name, ErrVerify, short(m.Version), short(c.Version), name)
	}
	stats := make([]fileStat, len(m.Files))
	for i, f := range m.Files {
		st, err := verifyFile(filepath.Join(dir, f.Path), f)
		if err != nil {
			return nil, nil, fmt.Errorf("model %s: %w; an admin installs it again with `igloo models install %s`", name, err, name)
		}
		stats[i] = st
	}
	return &Model{Manifest: m, Dir: dir}, stats, nil
}

// verifyFile checks the file at path against f.
func verifyFile(path string, f File) (fileStat, error) {
	file, err := os.Open(path)
	if err != nil {
		return fileStat{}, fmt.Errorf("%w: %s: %v", ErrVerify, f.Path, err)
	}
	defer file.Close() //nolint:errcheck
	st, err := file.Stat()
	if err != nil {
		return fileStat{}, err
	}
	if st.Size() != f.Size {
		return fileStat{}, fmt.Errorf("%w: %s has %d bytes, not %d", ErrVerify, f.Path, st.Size(), f.Size)
	}
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		return fileStat{}, fmt.Errorf("%w: %s: %v", ErrVerify, f.Path, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fileStat{}, fmt.Errorf("%w: %s's SHA-256 is %s, not %s", ErrVerify, f.Path, got, f.SHA256)
	}
	return fileStat{st.Size(), st.ModTime()}, nil
}

// Verify checks the files of the pack's model name, as Load does, without
// keeping it loaded.
func (p *Pack) Verify(name string) error {
	if p.dir == "" {
		return fmt.Errorf("the model pack is not set up (IGLOO_MODEL_DIR)")
	}
	_, _, err := p.verify(name, filepath.Join(p.dir, name))
	return err
}

func short(v string) string {
	if len(v) > 12 {
		return v[:12]
	}
	return v
}
