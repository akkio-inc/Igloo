package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/sharing"
)

// openCleanRooms sets up clean rooms, which need what Delta Sharing needs:
// the metadata database, the catalog, and recipients for partners. It
// returns the service and a function that closes it.
func openCleanRooms(ctx context.Context, cfg config.Config, eng *engine.Engine, policies policy.Store, log *slog.Logger) (*cleanroom.Service, func(), error) {
	store, err := cleanroom.OpenStore(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, nil, err
	}
	// Templates are parsed on a connection of their own.
	conn, err := eng.Connect(ctx)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	svc, err := cleanroom.New(cleanroom.Options{
		Store: store, Policies: policies, Parser: cleanroom.NewParser(conn),
		Databases: cleanroom.Databases{Catalog: eng.Catalog(), Local: eng.LocalDatabase()},
		Retention: cfg.Audit.Retention, Log: log,
	})
	if err != nil {
		_ = conn.Close()
		store.Close()
		return nil, nil, err
	}
	return svc, func() {
		svc.Close()
		_ = conn.Close()
	}, nil
}

// sharingHandler serves Delta Sharing and, at /clean-rooms, the clean
// rooms' API, to the same recipients with the same tokens.
func sharingHandler(shares *sharing.Service, rooms *cleanroom.Service, cfg config.Config, history *audit.Log, log *slog.Logger) http.Handler {
	if rooms == nil {
		return shares.Handler()
	}
	api := cleanroom.NewAPI(rooms, shares.Handler(), history, cfg.StatementTimeout, log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == cleanroom.APIPrefix || strings.HasPrefix(r.URL.Path, cleanroom.APIPrefix+"/") {
			api.ServeHTTP(w, r)
			return
		}
		shares.Handler().ServeHTTP(w, r)
	})
}
