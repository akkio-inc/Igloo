package config

import (
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// AI configures the AI functions (docs/ai-functions.md). They are off until
// an admin creates an AI integration; these settings bound what they may
// do once one exists.
type AI struct {
	// AllowHosts are host names and addresses AI integrations may reach
	// although they are not public, and over plain HTTP: models served on
	// the private network, by vLLM, llama.cpp or Ollama.
	AllowHosts []string
	// Cache keeps AI functions' results across statements, for
	// CacheRetention, so that running a statement again calls no provider
	// for inputs it has seen (IGLOO_AI_CACHE, on by default;
	// IGLOO_AI_CACHE_RETENTION, 30 days by default).
	Cache          bool
	CacheRetention time.Duration
	// MaxCalls and MaxCostUSD cap each statement's calls to providers and
	// their estimated cost, unless a role's ai_max_calls or
	// ai_max_cost_usd says otherwise (IGLOO_AI_MAX_CALLS, 1000 by default;
	// IGLOO_AI_MAX_COST_USD, 1.00 by default); 0 is no cap.
	MaxCalls   int64
	MaxCostUSD float64
	// RequestTimeout bounds each request to a provider, retries apart
	// (IGLOO_AI_REQUEST_TIMEOUT, 2 minutes by default).
	RequestTimeout time.Duration
}

// loadAI reads the IGLOO_AI_* settings.
func loadAI(cfg *Config, lookup func(string) (string, bool), addErr func(string, ...any)) {
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	a := AI{Cache: true, CacheRetention: 30 * 24 * time.Hour, MaxCalls: 1000, MaxCostUSD: 1, RequestTimeout: 2 * time.Minute}
	for _, v := range strings.Split(get("IGLOO_AI_ALLOW_HOSTS", ""), ",") {
		if v = strings.ToLower(strings.TrimSpace(v)); v != "" && !slices.Contains(a.AllowHosts, v) {
			if strings.ContainsAny(v, "/:@ ") && net.ParseIP(v) == nil {
				addErr("IGLOO_AI_ALLOW_HOSTS entry %q must be a host name or an IP address, without a scheme or port", v)
			}
			a.AllowHosts = append(a.AllowHosts, v)
		}
	}
	switch v := strings.ToLower(get("IGLOO_AI_CACHE", "on")); v {
	case "on", "true", "1":
	case "off", "false", "0":
		a.Cache = false
	default:
		addErr("IGLOO_AI_CACHE must be on or off, got %q", v)
	}
	if d, err := parseDays(get("IGLOO_AI_CACHE_RETENTION", "30d")); err != nil || d <= 0 {
		addErr("IGLOO_AI_CACHE_RETENTION must be a positive duration such as 30d or 12h")
	} else {
		a.CacheRetention = d
	}
	if v := get("IGLOO_AI_MAX_CALLS", "1000"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			addErr("IGLOO_AI_MAX_CALLS must be a whole number, 0 for no cap, got %q", v)
		}
		a.MaxCalls = n
	}
	if v := get("IGLOO_AI_MAX_COST_USD", "1"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 {
			addErr("IGLOO_AI_MAX_COST_USD must be an amount in US dollars, 0 for no cap, got %q", v)
		}
		a.MaxCostUSD = f
	}
	if d, err := time.ParseDuration(get("IGLOO_AI_REQUEST_TIMEOUT", "2m")); err != nil || d <= 0 {
		addErr("IGLOO_AI_REQUEST_TIMEOUT must be a positive duration such as 2m")
	} else {
		a.RequestTimeout = d
	}
	cfg.AI = a
}
