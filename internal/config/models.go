package config

import (
	"net/url"
	"path/filepath"
	"strings"
)

// Models configures the model pack (docs/models.md): the open-weight
// models an admin installs with `igloo models install`, which servers and
// workers load from the same directory.
type Models struct {
	// Dir is the pack's directory (IGLOO_MODEL_DIR); "" is no pack.
	Dir string
	// Mirror, if set, is where `igloo models install` reads models' files,
	// as <mirror>/<model>/<file>, in place of their upstream URLs
	// (IGLOO_MODEL_MIRROR).
	Mirror string
}

// loadModels reads the IGLOO_MODEL_* settings.
func loadModels(cfg *Config, lookup func(string) (string, bool), addErr func(string, ...any)) {
	get := func(key string) string {
		v, _ := lookup(key)
		return strings.TrimSpace(v)
	}
	m := Models{Dir: get("IGLOO_MODEL_DIR"), Mirror: get("IGLOO_MODEL_MIRROR")}
	if m.Dir != "" && !filepath.IsAbs(m.Dir) {
		addErr("IGLOO_MODEL_DIR must be an absolute path, got %q", m.Dir)
	}
	if m.Mirror != "" {
		if u, err := url.Parse(m.Mirror); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
			addErr("IGLOO_MODEL_MIRROR must be an http or https URL, got %q", m.Mirror)
		}
	}
	cfg.Models = m
}
