package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/federation"
)

// openFederation opens the connections to other databases, kept in the
// metadata database with their passwords encrypted under a key every
// replica shares. Without a metadata database there are none, and the
// manager is nil.
func openFederation(ctx context.Context, cfg config.Config, users auth.Store, log *slog.Logger) (*federation.Manager, func(), error) {
	pg, ok := users.(*auth.PostgresStore)
	if !ok {
		return nil, func() {}, nil
	}
	key, err := pg.Secret(ctx, federation.KeyName)
	if err != nil {
		return nil, nil, fmt.Errorf("load the connection credentials key: %w", err)
	}
	store, err := federation.OpenPostgresStore(ctx, cfg.MetadataDatabaseURL, key)
	if err != nil {
		return nil, nil, fmt.Errorf("connections: %w", err)
	}
	m := federation.NewManager(store, federation.Options{SecretsDir: cfg.ConnectionSecretsDir}, log)
	return m, store.Close, nil
}
