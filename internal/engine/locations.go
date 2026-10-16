package engine

import (
	"strings"

	"github.com/akkio-inc/igloo/internal/config"
)

// Locations are the URL prefixes COPY INTO may load files from: the
// warehouse and IGLOO_ALLOWED_LOCATIONS, each ending in a slash. The
// sandbox also lets DuckDB use the spill directory and the read cache,
// which are the server's own and not on the list.
func (e *Engine) Locations() []string { return e.locations }

func loadLocations(cfg config.Config) []string {
	var out []string
	if cfg.Storage.Location != "" {
		out = append(out, cfg.Storage.Root())
	}
	for _, loc := range cfg.Storage.AllowedLocations {
		out = append(out, strings.TrimSuffix(loc, "/")+"/")
	}
	return out
}
