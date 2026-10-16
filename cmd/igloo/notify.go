package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/notify"
)

// openNotifications opens where notification integrations and the alert
// history are kept: the metadata database, with integrations' secrets
// encrypted under a key every replica shares, or else this replica's
// memory.
func openNotifications(ctx context.Context, cfg config.Config, users auth.Store, log *slog.Logger) (notify.Store, error) {
	pg, ok := users.(*auth.PostgresStore)
	if !ok {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; notification integrations and the alert history are kept in memory and lost on restart")
		return notify.NewMemoryStore(), nil
	}
	key, err := pg.Secret(ctx, notify.KeyName)
	if err != nil {
		return nil, fmt.Errorf("load the notification secrets key: %w", err)
	}
	store, err := notify.OpenPostgres(ctx, cfg.MetadataDatabaseURL, key)
	if err != nil {
		return nil, fmt.Errorf("notifications: %w", err)
	}
	return store, nil
}
