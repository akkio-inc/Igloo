package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/sharing"
)

// sharingSecret names the key in igloo_auth.secrets that signs Delta
// Sharing file URLs and encrypts providers' tokens.
const sharingSecret = "delta-sharing"

// sharingPossible reports whether Delta Sharing can run: it keeps shares
// and tokens in the metadata database and shares the catalog's tables.
func sharingPossible(cfg config.Config) bool {
	return cfg.MetadataDatabaseURL != "" && cfg.Catalog.URI != ""
}

// startSharingProxy starts the loopback proxy DuckDB reads external tables
// through, and lets the query sandbox reach it. It must run before the
// engine locks the sandbox.
func startSharingProxy(cfg *config.Config, errc chan<- error, log *slog.Logger) (*sharing.Proxy, error) {
	proxy, err := sharing.NewProxy(log)
	if err != nil {
		return nil, err
	}
	cfg.Storage.AllowedLocations = append(cfg.Storage.AllowedLocations, proxy.Prefix())
	go func() {
		if err := proxy.Serve(); err != nil {
			errc <- fmt.Errorf("sharing proxy: %w", err)
		}
	}()
	return proxy, nil
}

// openSharing sets up Delta Sharing: the statements, external tables and the
// sharing endpoint's handler.
func openSharing(ctx context.Context, cfg config.Config, users auth.Store, cat *catalog.Client, eng *engine.Engine,
	proxy *sharing.Proxy, history *audit.Log, reg prometheus.Registerer, log *slog.Logger) (*sharing.Service, error) {
	pg, ok := users.(*auth.PostgresStore)
	if !ok {
		return nil, errors.New("delta sharing needs IGLOO_METADATA_DATABASE_URL")
	}
	key, err := pg.Secret(ctx, sharingSecret)
	if err != nil {
		return nil, fmt.Errorf("delta sharing key: %w", err)
	}
	store, err := sharing.OpenStore(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, err
	}
	// The file system keeps its context for every read, long after
	// startup.
	files, err := catalog.FS(context.WithoutCancel(ctx), cfg.Storage, cfg.Storage.Location)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("delta sharing: open the warehouse: %w", err)
	}
	svc, err := sharing.New(ctx, sharing.Options{
		Store: store, Key: key, Catalog: cat, Files: files, Warehouse: cfg.Storage.Root(),
		CatalogName: cfg.Catalog.Name, URL: cfg.SharingURL, LocalURL: localSharingURL(cfg),
		Engine: eng, Proxy: proxy, Audit: history, Log: log, Metrics: sharing.NewMetrics(reg),
	})
	if err != nil {
		store.Close()
		return nil, err
	}
	return svc, nil
}

// localSharingURL is the sharing endpoint's URL on this machine, for
// activation links when IGLOO_SHARING_URL is not set.
func localSharingURL(cfg config.Config) string {
	_, port, err := net.SplitHostPort(cfg.SharingAddr)
	if err != nil || port == "" {
		port = "8444"
	}
	scheme := "http"
	if cfg.TLSCertFile != "" {
		scheme = "https"
	}
	return scheme + "://localhost:" + port + sharing.Prefix
}

// sharingAddr is where the sharing endpoint listens, for the log, or "" when
// it does not.
func sharingAddr(shares *sharing.Service, cfg config.Config) string {
	if shares == nil {
		return ""
	}
	return cfg.SharingAddr
}

// serveSharing serves the Delta Sharing protocol on ln, over HTTPS with the
// SQL port's certificate when TLS is configured. Serving errors go to errc.
func serveSharing(ln net.Listener, handler http.Handler, tlsConfig *tls.Config, errc chan<- error, log *slog.Logger) *http.Server {
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		// Bounds reading a request; a data file may take as long as it
		// needs to download.
		ReadTimeout:    30 * time.Second,
		IdleTimeout:    2 * time.Minute,
		MaxHeaderBytes: 64 << 10,
		ErrorLog:       slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}
	serve := func() error { return srv.Serve(ln) }
	if tlsConfig != nil {
		srv.TLSConfig = tlsConfig.Clone()
		serve = func() error { return srv.ServeTLS(ln, "", "") }
	} else {
		log.Warn("the Delta Sharing endpoint is served without TLS; it answers only requests to localhost until IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE are set")
	}
	go func() {
		if err := serve(); !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("delta sharing: %w", err)
		}
	}()
	return srv
}
