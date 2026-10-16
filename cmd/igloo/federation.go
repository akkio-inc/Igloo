package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"

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
	m := federation.NewManager(store, federation.Options{
		SecretsDir: cfg.ConnectionSecretsDir,
		Protected:  metadataServers(cfg.MetadataDatabaseURL),
	}, log)
	return m, store.Close, nil
}

// metadataServers returns the host:port addresses of the metadata
// database, which holds Igloo's users and grants (and often the catalog's
// tables) and so must never become a connection.
func metadataServers(url string) []string {
	c, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil
	}
	out := []string{net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port)))}
	for _, f := range c.Fallbacks {
		out = append(out, net.JoinHostPort(f.Host, strconv.Itoa(int(f.Port))))
	}
	return out
}
