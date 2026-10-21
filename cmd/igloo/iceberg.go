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

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/icebergrest"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// icebergSecret names the key in igloo_auth.secrets that seals the access
// tokens of the Iceberg REST endpoint.
const icebergSecret = "iceberg-token"

// icebergClient names the endpoint's sign-ins in the log and the login
// history.
const icebergClient = "iceberg rest"

// icebergDeps is what the Iceberg REST endpoint shares with the rest of
// the server.
type icebergDeps struct {
	sql     *pgwire.Server
	users   auth.Store
	grants  auth.GrantStore
	history *audit.Log
	work    *activity.Tracker
	reg     prometheus.Registerer
	tls     *tls.Config
	// clones keeps the files clones share; nil without a catalog.
	clones *clone.Guard
}

// serveIceberg serves the Iceberg REST catalog for other engines on ln,
// over HTTPS with the SQL port's certificate when TLS is configured.
// Serving errors go to errc.
func serveIceberg(ctx context.Context, ln net.Listener, cfg config.Config, deps icebergDeps, errc chan<- error, log *slog.Logger) (*http.Server, error) {
	key, err := sharedKey(ctx, deps.users, icebergSecret)
	if err != nil {
		return nil, fmt.Errorf("iceberg token key: %w", err)
	}
	vendor, err := icebergCredentials(ctx, cfg.Storage, log)
	if err != nil {
		return nil, err
	}
	// Without a metadata database there are no access tokens.
	tokens, _ := deps.users.(auth.TokenStore)
	var clones icebergrest.Clones
	if deps.clones != nil {
		clones = deps.clones
	}
	// Owners are kept with the grants, as for SQL (see pgwire.Options).
	owners, _ := deps.grants.(auth.OwnerStore)
	handler, err := icebergrest.New(icebergrest.Options{
		Clones:      clones,
		CatalogURI:  cfg.Catalog.URI,
		Warehouse:   cfg.Catalog.Warehouse,
		CatalogName: cfg.Catalog.Name,
		Users:       deps.users,
		Grants:      deps.grants,
		Owners:      owners,
		Authenticate: func(ctx context.Context, name, password string, addr net.Addr) (auth.User, error) {
			user, err := deps.sql.AuthenticateClient(ctx, name, password, addr, icebergClient)
			var blocked *pgwire.LoginBlockedError
			switch {
			case errors.Is(err, pgwire.ErrLoginFailed):
				err = icebergrest.ErrLoginFailed
			case errors.Is(err, pgwire.ErrUserDisabled):
				err = icebergrest.ErrUserDisabled
			case errors.As(err, &blocked):
				err = &icebergrest.LoginBlockedError{Wait: blocked.Wait}
			}
			return user, err
		},
		AuthenticateToken: func(ctx context.Context, name, secret string, addr net.Addr) (auth.User, auth.Token, error) {
			user, token, err := deps.sql.AuthenticateToken(ctx, name, secret, addr, icebergClient)
			if errors.Is(err, pgwire.ErrLoginFailed) {
				err = icebergrest.ErrLoginFailed
			}
			return user, token, err
		},
		Tokens: tokens,
		// Policies reach every replica within the refresh interval; each
		// request asks this replica's current ones.
		Protected: func(ctx context.Context, user auth.User, o auth.Object) (bool, error) {
			return deps.sql.ProtectedFor(ctx, user, o.Schema, o.Table)
		},
		Classified: func(ctx context.Context, o auth.Object) ([]string, bool, error) {
			columns, classified := deps.sql.Classified(ctx, o.Schema, o.Table)
			return columns, classified, nil
		},
		Erasing: func(ctx context.Context, o auth.Object) (bool, error) {
			return deps.sql.Erasing(ctx, o.Schema, o.Table)
		},
		StorageRoot: cfg.Storage.Root(),
		Exists: func(ctx context.Context, path string) (bool, error) {
			return catalog.Exists(ctx, cfg.Storage, path)
		},
		Key:         key,
		Credentials: vendor,
		TLS:         deps.tls != nil,
		Audit:       deps.history,
		Activity:    deps.work,
		Metrics:     icebergrest.NewMetrics(deps.reg),
		Log:         log,
	})
	if err != nil {
		return nil, err
	}
	httpSrv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       time.Minute,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}
	serve := func() error { return httpSrv.Serve(ln) }
	if deps.tls != nil {
		httpSrv.TLSConfig = deps.tls.Clone()
		serve = func() error { return httpSrv.ServeTLS(ln, "", "") }
	} else {
		log.Warn("the Iceberg REST endpoint is served without TLS; it serves clients on private networks only until IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE are set")
	}
	go func() {
		if err := serve(); !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("iceberg: %w", err)
		}
	}()
	return httpSrv, nil
}

// icebergCredentials returns what hands other engines storage credentials
// limited to one table, or nil when the warehouse's storage has no way to
// limit them yet. Engines then get table metadata only, and need access of
// their own to read or write the files.
func icebergCredentials(ctx context.Context, s config.Storage, log *slog.Logger) (icebergrest.Vendor, error) {
	if s.Type != config.StorageS3 {
		log.Warn("other engines get no storage credentials for gs:// and abfss:// warehouses yet; they need their own access to the table files")
		return nil, nil
	}
	v, err := icebergrest.NewS3Vendor(ctx, s, log)
	if err != nil {
		return nil, fmt.Errorf("iceberg storage credentials: %w", err)
	}
	if v == nil {
		log.Warn("IGLOO_S3_STS_ROLE_ARN is not set, so other engines get table metadata but no storage credentials")
		return nil, nil
	}
	return v, nil
}
