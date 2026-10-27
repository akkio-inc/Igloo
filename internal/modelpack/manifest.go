// Package modelpack keeps the model pack: open-weight models' files, which
// an admin installs into a directory (IGLOO_MODEL_DIR) with `igloo models
// install`, and which Igloo's processes, servers and workers alike, load
// from there. Every file is pinned by its SHA-256 and size in a manifest;
// installing checks each file as it downloads, and loading checks it again
// before a process first uses it, so a file that changed on disk is never
// used. Only models whose weights are under a license the allow list holds
// (Apache-2.0 or MIT) are installed or loaded. Nothing is downloaded at
// query time: a query that needs a model the pack lacks fails, saying how
// to install it.
//
// The pack serves any kind of model: time-series forecasting models now,
// local embedding and decision models later.
package modelpack

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
)

// Licenses are the SPDX identifiers of the licenses whose models the pack
// holds.
var Licenses = []string{"Apache-2.0", "MIT"}

// Kinds of model.
const (
	KindForecast  = "forecast"
	KindEmbedding = "embedding"
	KindDecision  = "decision"
)

// Errors.
var (
	// ErrNotInstalled is a model the pack does not hold.
	ErrNotInstalled = errors.New("not installed")
	// ErrLicense is a model whose license the allow list does not hold.
	ErrLicense = errors.New("license not allowed")
	// ErrVerify is a file whose size or SHA-256 is not its manifest's.
	ErrVerify = errors.New("verification failed")
	// ErrManifest is a manifest that cannot be used.
	ErrManifest = errors.New("invalid manifest")
)

// File is one of a model's files.
type File struct {
	// Path is the file's name in the model's directory.
	Path string `json:"path"`
	// URL is where it is downloaded from: an HTTPS URL pinned to a
	// revision, such as a Hugging Face repository's at a commit.
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// Manifest describes a model of the pack.
type Manifest struct {
	// Name is how SQL names the model, such as chronos-bolt-small.
	Name string `json:"name"`
	// Version is the upstream revision the files are pinned to.
	Version string `json:"version"`
	// License is the weights' license, an SPDX identifier.
	License string `json:"license"`
	// Kind is what the model does: forecast, embedding or decision.
	Kind string `json:"kind"`
	// Architecture names the code that runs the model, such as
	// chronos-bolt.
	Architecture string `json:"architecture"`
	// Source is the upstream page of the model, for people.
	Source string `json:"source"`
	// Description says what the model is, for people.
	Description string `json:"description,omitempty"`
	Files       []File `json:"files"`
}

var (
	nameRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	sha256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
	pathRE   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// LicenseAllowed reports whether license is one the pack holds.
func LicenseAllowed(license string) bool {
	return slices.ContainsFunc(Licenses, func(l string) bool { return strings.EqualFold(l, strings.TrimSpace(license)) })
}

// Check reports what makes m unusable: a name, file or pin that is not
// well formed, or a license the allow list does not hold.
func (m Manifest) Check() error {
	if !nameRE.MatchString(m.Name) {
		return fmt.Errorf("%w: the name %q must be 1 to 64 lower-case letters, digits, '.', '_' and '-'", ErrManifest, m.Name)
	}
	if strings.TrimSpace(m.Version) == "" {
		return fmt.Errorf("%w: %s has no version", ErrManifest, m.Name)
	}
	if !LicenseAllowed(m.License) {
		return fmt.Errorf("%w: %s is licensed %q; the model pack holds only models licensed %s", ErrLicense, m.Name, m.License,
			strings.Join(Licenses, " or "))
	}
	switch m.Kind {
	case KindForecast, KindEmbedding, KindDecision:
	default:
		return fmt.Errorf("%w: %s has kind %q; kinds are forecast, embedding and decision", ErrManifest, m.Name, m.Kind)
	}
	if m.Architecture == "" {
		return fmt.Errorf("%w: %s has no architecture", ErrManifest, m.Name)
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("%w: %s has no files", ErrManifest, m.Name)
	}
	seen := map[string]bool{}
	for _, f := range m.Files {
		if !pathRE.MatchString(f.Path) || f.Path == "manifest.json" {
			return fmt.Errorf("%w: %s's file %q must be a plain file name", ErrManifest, m.Name, f.Path)
		}
		if seen[f.Path] {
			return fmt.Errorf("%w: %s lists %s twice", ErrManifest, m.Name, f.Path)
		}
		seen[f.Path] = true
		if !sha256RE.MatchString(f.SHA256) {
			return fmt.Errorf("%w: %s's file %s needs its SHA-256, 64 lower-case hex digits", ErrManifest, m.Name, f.Path)
		}
		if f.Size <= 0 {
			return fmt.Errorf("%w: %s's file %s needs its size in bytes", ErrManifest, m.Name, f.Path)
		}
		u, err := url.Parse(f.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("%w: %s's file %s needs an https URL", ErrManifest, m.Name, f.Path)
		}
	}
	return nil
}

// Bytes is the size of m's files.
func (m Manifest) Bytes() int64 {
	var n int64
	for _, f := range m.Files {
		n += f.Size
	}
	return n
}

// same reports whether m and o pin the same files.
func (m Manifest) same(o Manifest) bool {
	if m.Name != o.Name || m.Version != o.Version || len(m.Files) != len(o.Files) {
		return false
	}
	for i := range m.Files {
		a, b := m.Files[i], o.Files[i]
		if a.Path != b.Path || a.SHA256 != b.SHA256 || a.Size != b.Size {
			return false
		}
	}
	return true
}

// huggingFace is a file of a Hugging Face repository at a commit.
func huggingFace(repo, commit, file, sha string, size int64) File {
	return File{Path: path.Base(file), URL: "https://huggingface.co/" + repo + "/resolve/" + commit + "/" + file, SHA256: sha, Size: size}
}
