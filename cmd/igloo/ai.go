package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/akkio-inc/igloo/internal/ai"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/notify"
)

// openAI makes the runtime of the AI functions: AI integrations, their
// grants and the cache of results in the metadata database, with the
// integrations' keys encrypted under a key every replica shares, or else in
// this replica's memory. Providers are reached through an egress guard of
// their own, which reaches only public addresses over TLS unless
// IGLOO_AI_ALLOW_HOSTS names the host.
func openAI(ctx context.Context, cfg config.Config, users auth.Store, log *slog.Logger) (*ai.Runtime, error) {
	var store ai.Store
	if pg, ok := users.(*auth.PostgresStore); ok {
		key, err := pg.Secret(ctx, ai.KeyName)
		if err != nil {
			return nil, fmt.Errorf("load the AI secrets key: %w", err)
		}
		if store, err = ai.OpenPostgres(ctx, cfg.MetadataDatabaseURL, key); err != nil {
			return nil, fmt.Errorf("AI functions: %w", err)
		}
	} else {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; AI integrations and the AI cache are kept in memory and lost on restart")
		store = ai.NewMemoryStore()
	}
	guard := notify.NewGuard(cfg.AI.AllowHosts, nil)
	return ai.NewRuntime(ai.Options{
		Store:          store,
		Dial:           guard.DialContext,
		CheckURL:       func(u string) error { _, err := guard.CheckURL(u); return err },
		Cache:          cfg.AI.Cache,
		CacheRetention: cfg.AI.CacheRetention,
		MaxCalls:       cfg.AI.MaxCalls,
		MaxCostUSD:     cfg.AI.MaxCostUSD,
		RequestTimeout: cfg.AI.RequestTimeout,
		Log:            log,
	}), nil
}
