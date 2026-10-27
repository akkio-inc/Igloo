package modelpack

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Source says where Install reads a model's files from: their manifest's
// URLs, a mirror, or a local directory.
type Source struct {
	// Client sends the requests; nil is a client of Igloo's defaults.
	Client *http.Client
	// Mirror, if set, is a base URL that holds the files as
	// <Mirror>/<model>/<file>, in place of their URLs: a copy inside the
	// network, for hosts that cannot reach Hugging Face. The files are
	// still checked against their SHA-256.
	Mirror string
	// From, if set, is a local directory that holds the files, as
	// <From>/<file> or <From>/<model>/<file>, for hosts without a network.
	From string
	// Progress, if set, is told of each file as it starts and ends.
	Progress func(model, file string, size int64, done bool)
}

// Install installs m into the pack in dir, as an admin does with `igloo
// models install`: each file is read from src, checked against its size
// and SHA-256 as it arrives, and the model replaces any installed one of
// its name only once every file checks out. A model installed already,
// file for file, is left as it is.
func Install(ctx context.Context, dir string, m Manifest, src Source) (installed bool, err error) {
	if err := m.Check(); err != nil {
		return false, err
	}
	if c, ok := Lookup(m.Name); ok && !c.same(m) {
		return false, fmt.Errorf("%w: %s is the name of a model Igloo pins; give the manifest's model another name", ErrManifest, m.Name)
	}
	if dir == "" {
		return false, errors.New("the model pack's directory is not set: set IGLOO_MODEL_DIR or pass --dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	final := filepath.Join(dir, m.Name)
	if old, err := readManifest(final); err == nil && old.same(m) {
		if err := Open(dir).Verify(m.Name); err == nil {
			return false, nil
		}
	}
	tmp, err := os.MkdirTemp(dir, ".install-"+m.Name+"-")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(tmp) //nolint:errcheck
	for _, f := range m.Files {
		if src.Progress != nil {
			src.Progress(m.Name, f.Path, f.Size, false)
		}
		if err := fetch(ctx, filepath.Join(tmp, f.Path), m.Name, f, src); err != nil {
			return false, fmt.Errorf("model %s: %w", m.Name, err)
		}
		if src.Progress != nil {
			src.Progress(m.Name, f.Path, f.Size, true)
		}
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.WriteFile(filepath.Join(tmp, manifestFile), append(b, '\n'), 0o644); err != nil {
		return false, err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		return false, err
	}
	// Swap the new directory in; the old one, if any, goes after.
	var stale string
	if _, err := os.Stat(final); err == nil {
		stale = filepath.Join(dir, ".old-"+m.Name+"-"+randomSuffix())
		if err := os.Rename(final, stale); err != nil {
			return false, err
		}
	}
	if err := os.Rename(tmp, final); err != nil {
		if stale != "" {
			_ = os.Rename(stale, final)
		}
		return false, err
	}
	if stale != "" {
		_ = os.RemoveAll(stale)
	}
	return true, nil
}

// Remove removes model name from the pack in dir.
func Remove(dir, name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("model %q is %w", name, ErrNotInstalled)
	}
	final := filepath.Join(dir, name)
	if _, err := os.Stat(filepath.Join(final, manifestFile)); err != nil {
		return fmt.Errorf("model %s is %w in %s", name, ErrNotInstalled, dir)
	}
	stale := filepath.Join(dir, ".old-"+name+"-"+randomSuffix())
	if err := os.Rename(final, stale); err != nil {
		return err
	}
	return os.RemoveAll(stale)
}

func randomSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// fetch writes f to path from src, checking it as it arrives.
func fetch(ctx context.Context, path, model string, f File, src Source) error {
	if src.From != "" {
		for _, p := range []string{filepath.Join(src.From, f.Path), filepath.Join(src.From, model, f.Path)} {
			in, err := os.Open(p)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			defer in.Close() //nolint:errcheck
			return save(path, in, f)
		}
		return fmt.Errorf("%s is not in %s", f.Path, src.From)
	}
	u := f.URL
	if src.Mirror != "" {
		base, err := url.Parse(strings.TrimRight(src.Mirror, "/"))
		if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" {
			return fmt.Errorf("the mirror %q must be an http or https URL", src.Mirror)
		}
		u = base.String() + "/" + url.PathEscape(model) + "/" + url.PathEscape(f.Path)
	}
	client := src.Client
	if client == nil {
		client = defaultClient()
	}
	var last error
	for attempt := range 4 {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * time.Second):
			}
		}
		last = download(ctx, client, u, path, f)
		if last == nil || errors.Is(last, ErrVerify) || ctx.Err() != nil {
			return last
		}
	}
	return last
}

func defaultClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, ResponseHeaderTimeout: time.Minute, ForceAttemptHTTP2: true},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Hugging Face redirects to its content delivery network.
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			if via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
				return errors.New("refusing a redirect from https to " + req.URL.Scheme)
			}
			return nil
		},
	}
}

func download(ctx context.Context, client *http.Client, u, path string, f File) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", redact(u), resp.Status)
	}
	return save(path, resp.Body, f)
}

// save writes r to path, checking its size and SHA-256 against f.
func save(path string, r io.Reader, f File) error {
	out, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), io.LimitReader(r, f.Size+1))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n != f.Size {
		return fmt.Errorf("%w: %s has %d bytes, not %d", ErrVerify, f.Path, n, f.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("%w: %s's SHA-256 is %s, not %s", ErrVerify, f.Path, got, f.SHA256)
	}
	return nil
}

// redact drops a URL's query, which may hold a signature.
func redact(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

// ReadManifest reads a manifest from a file, for models outside the
// catalog.
func ReadManifest(path string) (Manifest, error) {
	var m Manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return m, fmt.Errorf("%w: %s: %v", ErrManifest, path, err)
	}
	return m, m.Check()
}
