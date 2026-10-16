// Command igloo serves SQL over the Postgres wire protocol, and a web console,
// running queries on an embedded DuckDB attached to an Iceberg catalog.
//
// Configuration comes from IGLOO_* environment variables; see
// docs/configuration.md. Subcommands:
//
//	igloo version                   print the version
//	igloo healthcheck               exit 0 if the server is up (for containers)
//	igloo install-extensions <dir>  download DuckDB extensions into dir
//	igloo gateway                   run the scale-to-zero gateway (docs/scale-to-zero.md)
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/maintenance"
	"github.com/akkio-inc/igloo/internal/pgwire"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	// startupTimeout bounds how long Igloo waits for its dependencies.
	startupTimeout = 3 * time.Minute
	// shutdownTimeout bounds how long running queries get to finish.
	shutdownTimeout = 30 * time.Second
)

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "version":
		fmt.Println(version)
		return
	case len(os.Args) > 1 && os.Args[1] == "healthcheck":
		err = healthcheck()
	case len(os.Args) > 1 && os.Args[1] == "gateway":
		err = serveGateway()
	case len(os.Args) > 1 && os.Args[1] == "install-extensions":
		if len(os.Args) != 3 {
			err = errors.New("usage: igloo install-extensions <dir>")
			break
		}
		err = engine.InstallExtensions(context.Background(), os.Args[2])
	default:
		err = serve()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "igloo:", err)
		os.Exit(1)
	}
}

func serve() error {
	cfg, err := config.Load(os.LookupEnv)
	if err != nil {
		return err
	}
	log := newLogger(cfg)
	log.Info("starting igloo", "version", version)

	tlsConfig, err := loadTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return err
	}
	switch {
	case tlsConfig == nil:
		log.Warn("TLS is off; set IGLOO_TLS_CERT_FILE and IGLOO_TLS_KEY_FILE before exposing Igloo beyond a private network")
	case !cfg.TLSRequired:
		log.Warn("TLS is optional (IGLOO_TLS_REQUIRED=false); clients may still connect unencrypted")
	}

	started := time.Now()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	users, err := openUsers(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer users.Close()

	httpClient := &http.Client{Timeout: 10 * time.Second}
	if cfg.Catalog.URI != "" && cfg.Catalog.Setup {
		if err := catalog.Setup(startCtx, cfg, httpClient, log); err != nil {
			return err
		}
	}

	eng, err := engine.Open(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer eng.Close() //nolint:errcheck

	var (
		cat     *catalog.Client
		objects *ddl.Manager
	)
	if cfg.Catalog.URI != "" {
		if cat, err = catalog.Open(startCtx, cfg); err != nil {
			return err
		}
		if objects, err = ddl.New(startCtx, cat, eng, log); err != nil {
			return err
		}
		defer objects.Close() //nolint:errcheck
		go objects.Run(ctx)
	}
	var historyStore audit.Store
	if cfg.Audit.Enabled {
		if historyStore, err = openHistory(startCtx, cfg, log); err != nil {
			return err
		}
		defer historyStore.Close()
	}
	cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(eng.Metrics()...)
	work := activity.New()

	// The history is written in the background until the server has shut
	// down, then flushed.
	var history *audit.Log
	historyDone := make(chan struct{})
	historyCtx, stopHistory := context.WithCancel(context.Background())
	if historyStore != nil {
		history = audit.New(historyStore, audit.Options{Retention: cfg.Audit.Retention, Metrics: audit.NewMetrics(reg)}, log)
		eng.SetHistory(history, pgwire.HistoryViewer)
		go func() {
			defer close(historyDone)
			history.Run(historyCtx)
		}()
	} else {
		close(historyDone)
	}
	defer func() {
		stopHistory()
		<-historyDone
	}()

	srv, err := pgwire.New(eng, pgwire.Options{
		Users:            users,
		TLS:              tlsConfig,
		RequireTLS:       cfg.TLSRequired,
		Metrics:          pgwire.NewMetrics(reg),
		StatementTimeout: cfg.StatementTimeout,
		MaxConnections:   cfg.MaxConnections,
		Version:          version,
		DDL:              objects,
		Activity:         work,
		Audit:            history,
	}, log)
	if err != nil {
		return err
	}

	maintains := cat != nil && cfg.Maintenance.Enabled
	var schedule maintenance.Schedule
	if maintains {
		stopMaintenance, lock, err := startMaintenance(ctx, cfg, cat, reg, work, log)
		if err != nil {
			return err
		}
		defer stopMaintenance()
		if lock != nil {
			schedule = lock
		}
	}

	health := &health{engine: eng, users: users, catalog: cfg.Catalog, client: httpClient, activity: work}
	errc := make(chan error, 3)
	var consoleSrv *http.Server
	if cfg.ConsoleAddr != "" {
		consoleLn, err := listenConsole(cfg.ConsoleAddr)
		if err != nil {
			return err
		}
		status := consoleStatus(cfg, started, work, health, schedule, maintains)
		if consoleSrv, err = serveConsole(ctx, consoleLn, srv, users, tlsConfig, status, errc, log); err != nil {
			_ = consoleLn.Close()
			return err
		}
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	go func() { errc <- srv.Serve(ln) }()

	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		httpSrv = &http.Server{Addr: cfg.HTTPAddr, Handler: health.handler(reg), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("http: %w", err)
			}
		}()
	}
	log.Info("igloo is ready", "sql", ln.Addr().String(), "http", cfg.HTTPAddr, "console", cfg.ConsoleAddr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Report not ready first, and keep serving for a moment, so load
	// balancers stop routing here before the listener closes.
	health.stopping.Store(true)
	if cfg.ShutdownDelay > 0 {
		log.Info("draining before shutdown", "delay", cfg.ShutdownDelay)
		select {
		case <-time.After(cfg.ShutdownDelay):
		case err := <-errc:
			return err
		}
	}
	log.Info("shutting down")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	if consoleSrv != nil {
		_ = consoleSrv.Shutdown(shutdownCtx)
	}
	return srv.Shutdown(shutdownCtx)
}

// openUsers connects the user store and makes sure the bootstrap admin
// exists.
func openUsers(ctx context.Context, cfg config.Config, log *slog.Logger) (auth.Store, error) {
	var store auth.Store
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; the admin is the only user and CREATE USER is unavailable")
		store = auth.NewMemoryStore()
	} else {
		var err error
		for {
			store, err = auth.OpenPostgresStore(ctx, cfg.MetadataDatabaseURL)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil, err
			}
			log.Warn("metadata database not reachable yet, retrying", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}

	verifier, err := auth.NewVerifier(cfg.AdminPassword)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("IGLOO_ADMIN_PASSWORD: %w", err)
	}
	created, err := store.EnsureAdmin(ctx, cfg.AdminUser, verifier)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("create admin user: %w", err)
	}
	if created {
		log.Info("created admin user", "user", cfg.AdminUser)
	}
	return store, nil
}

// openHistory opens where the query and login history is kept: the
// metadata database, shared by every replica, or else this replica's
// memory.
func openHistory(ctx context.Context, cfg config.Config, log *slog.Logger) (audit.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; each replica keeps only its own recent query and login history, in memory")
		return audit.NewMemoryStore(audit.MemoryQueries, audit.MemoryLogins), nil
	}
	store, err := audit.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	log.Info("query and login history is on", "retention", cfg.Audit.Retention)
	return store, nil
}

// startMaintenance compacts and expires tables in the background until ctx is
// canceled. Replicas sharing a metadata database take turns, through lock,
// which also records when passes ran; it is nil without one.
func startMaintenance(ctx context.Context, cfg config.Config, cat *catalog.Client, reg prometheus.Registerer, work *activity.Tracker, log *slog.Logger) (stop func(), lock *maintenance.PostgresLock, err error) {
	opts := maintenance.Options{
		Interval:  cfg.Maintenance.Interval,
		Retention: cfg.Maintenance.Retention,
		Metrics:   maintenance.NewMetrics(reg),
		Activity:  work,
	}
	if cfg.MetadataDatabaseURL != "" {
		if lock, err = maintenance.NewPostgresLock(ctx, cfg.MetadataDatabaseURL); err != nil {
			return nil, nil, fmt.Errorf("maintenance lock: %w", err)
		}
		opts.Lock, opts.Schedule = lock, lock
	}
	m := maintenance.New(cat, cfg.Storage.Root(), opts, log)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Run(ctx)
	}()
	log.Info("table maintenance is on", "interval", cfg.Maintenance.Interval, "snapshot_retention", cfg.Maintenance.Retention)
	return func() {
		cancel()
		<-done
		if lock != nil {
			lock.Close()
		}
	}, lock, nil
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func loadTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS certificate: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}

// healthcheck asks the HTTP endpoint whether the process is up, or dials the
// SQL port when HTTP is disabled.
func healthcheck() error {
	httpAddr, ok := os.LookupEnv("IGLOO_HTTP_ADDR")
	if !ok {
		httpAddr = ":8080"
	}
	if httpAddr != "" {
		_, port, err := net.SplitHostPort(httpAddr)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz")
		if err != nil {
			return err
		}
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("healthz returned %s", resp.Status)
		}
		return nil
	}

	addr := os.Getenv("IGLOO_LISTEN_ADDR")
	if addr == "" {
		addr = ":5432"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), 2*time.Second)
	if err != nil {
		return err
	}
	return conn.Close()
}
