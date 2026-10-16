// Command igloo serves SQL over the Postgres wire protocol and Arrow Flight
// SQL, and a web console, running queries on an embedded DuckDB attached to
// an Iceberg catalog, and serves that catalog's tables to other engines over
// the Iceberg REST protocol.
//
// Configuration comes from IGLOO_* environment variables; see
// docs/configuration.md. Subcommands:
//
//	igloo version                   print the version
//	igloo healthcheck               exit 0 if the server is up (for containers)
//	igloo install-extensions <dir>  download DuckDB extensions into dir
//	igloo gateway                   run the scale-to-zero gateway (docs/scale-to-zero.md)
//	igloo worker                    run a distributed-execution worker (docs/distributed-execution.md)
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
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/flightsql"
	"github.com/akkio-inc/igloo/internal/maintenance"
	"github.com/akkio-inc/igloo/internal/notify"
	"github.com/akkio-inc/igloo/internal/openlineage"
	"github.com/akkio-inc/igloo/internal/pgwire"
	"github.com/akkio-inc/igloo/internal/policy"
	"github.com/akkio-inc/igloo/internal/scim"
	"github.com/akkio-inc/igloo/internal/sharing"
	"github.com/akkio-inc/igloo/internal/sso"
	"github.com/akkio-inc/igloo/internal/tasks"
	"github.com/akkio-inc/igloo/internal/workload"
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
	case len(os.Args) > 1 && os.Args[1] == "worker":
		err = serveWorker()
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
	// SQL sessions and the Iceberg REST endpoint check the same grants.
	grants, ok := users.(auth.GrantStore)
	if !ok {
		grants = auth.NewMemoryGrants(users)
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	if cfg.Catalog.URI != "" && cfg.Catalog.Setup {
		if err := catalog.Setup(startCtx, cfg, httpClient, log); err != nil {
			return err
		}
	}

	// One for each server and the sharing proxy.
	errc := make(chan error, 8)
	var proxy *sharing.Proxy
	if sharingPossible(cfg) {
		if proxy, err = startSharingProxy(&cfg, errc, log); err != nil {
			return err
		}
		defer proxy.Close() //nolint:errcheck
	}

	fed, closeFederation, err := openFederation(startCtx, cfg, users, log)
	if err != nil {
		return err
	}
	defer closeFederation()
	var engineOpts []engine.Option
	if fed != nil {
		// Connections are attached before DuckDB's sandbox is locked.
		engineOpts = append(engineOpts, engine.BeforeLockdown(fed.Attach))
	}
	eng, err := engine.Open(startCtx, cfg, log, engineOpts...)
	if err != nil {
		return err
	}
	defer eng.Close() //nolint:errcheck
	if fed != nil {
		fed.SetConnect(eng.Connect)
		go fed.Run(ctx)
	}

	var (
		cat     *catalog.Client
		objects *ddl.Manager
		clones  *clone.Manager
		guard   *clone.Guard
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
		cloneStore, err := openCloneStore(startCtx, cfg)
		if err != nil {
			return err
		}
		defer cloneStore.Close()
		clones, guard = clone.NewManager(cat, cloneStore, log), clone.NewGuard(cat, cloneStore, log)
	}
	policies, err := openPolicies(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer policies.Close()
	var historyStore audit.Store
	if cfg.Audit.Enabled {
		if historyStore, err = openHistory(startCtx, cfg, log); err != nil {
			return err
		}
		defer historyStore.Close()
	}
	taskStore, err := openTasks(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer taskStore.Close()
	notifyStore, err := openNotifications(startCtx, cfg, users, log)
	if err != nil {
		return err
	}
	defer notifyStore.Close()
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
		history = audit.New(historyStore, audit.Options{Retention: cfg.Audit.Retention, Metrics: audit.NewMetrics(reg),
			Pool: cfg.Pool, PricePerHour: cfg.PricePerHour}, log)
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

	var lineage pgwire.LineageSink
	if cfg.OpenLineage.URL != "" {
		ol, err := openlineage.New(openlineage.Options{URL: cfg.OpenLineage.URL, APIKey: cfg.OpenLineage.APIKey,
			Namespace: cfg.OpenLineage.Namespace, AllowPrivate: cfg.OpenLineage.AllowPrivate, SendSQL: cfg.OpenLineage.SendSQL,
			Registerer: reg, Log: log})
		if err != nil {
			return err
		}
		if history == nil {
			log.Warn("IGLOO_OPENLINEAGE_URL has no effect without the query history (IGLOO_AUDIT=false)")
		}
		// Events are sent until the server has shut down.
		lineageCtx, stopLineage := context.WithCancel(context.Background())
		defer stopLineage()
		go ol.Run(lineageCtx)
		lineage = ol
	}

	var shares *sharing.Service
	if proxy != nil && cat != nil {
		sharingCtx, cancelSharing := context.WithTimeout(ctx, startupTimeout)
		shares, err = openSharing(sharingCtx, cfg, users, cat, eng, proxy, history, reg, log)
		cancelSharing()
		if err != nil {
			return err
		}
		runCtx, stopRun := context.WithCancel(ctx)
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			shares.Run(runCtx)
		}()
		defer func() {
			stopRun()
			<-runDone
			_ = shares.Close()
		}()
	}

	// Statements that read large tables run in parts on workers.
	var dist *distributed.Coordinator
	if len(cfg.Workers.Addresses) > 0 {
		if cat == nil {
			return errors.New("IGLOO_WORKERS needs IGLOO_CATALOG_URI")
		}
		if dist, err = newCoordinator(ctx, cfg, distributed.NewIceberg(cat), eng, reg, log); err != nil {
			return err
		}
		defer dist.Close() //nolint:errcheck
	}

	// srv is set before it serves anyone; single sign-on tells it about
	// role changes, and the long query alert asks it what runs.
	var srv *pgwire.Server
	notifier := notify.New(notify.Options{
		Store:      notifyStore,
		AllowHosts: cfg.Notifications.AllowHosts,
		SMTP:       cfg.Notifications.SMTP,
		Jobs:       taskStore,
		Statements: func() []notify.Statement { return srv.RunningStatements() },
		Retention:  cfg.Audit.Retention,
		Metrics:    notify.NewMetrics(reg),
		Log:        log,
	})
	eng.SetAlertHistory(notifyStore, pgwire.HistoryViewer)
	jobs := tasks.NewScheduler(taskStore, tasks.Options{Retention: cfg.Audit.Retention, Ended: notifier.RunEnded, Log: log})
	eng.SetJobHistory(taskStore, pgwire.HistoryViewer)

	var eraser *erasure.Service
	if cat != nil {
		erasureStore, purger, err := openErasure(ctx, cfg, eng, log)
		if err != nil {
			return err
		}
		defer erasureStore.Close()
		if eraser, err = newEraser(cfg, erasureStore, purger, cat, eng, policies, historyStore, taskStore, jobs, log); err != nil {
			return err
		}
		// Every replica drops what requests delete from its read cache,
		// until shutdown, before the engine closes.
		purgeCtx, stopPurging := context.WithCancel(ctx)
		purged := make(chan struct{})
		go func() {
			defer close(purged)
			purger.Run(purgeCtx)
		}()
		defer func() {
			stopPurging()
			<-purged
		}()
	}

	single, err := openSSO(cfg, users, func(user string) { srv.UserChanged(user) }, log)
	if err != nil {
		return err
	}
	// Memory reservations for role limits come out of DuckDB's memory.
	limits := workload.NewController(eng.MemoryLimit(ctx))
	reg.MustRegister(limits.Metrics()...)

	opts := pgwire.Options{
		Users:               users,
		TLS:                 tlsConfig,
		RequireTLS:          cfg.TLSRequired,
		Metrics:             pgwire.NewMetrics(reg),
		StatementTimeout:    cfg.StatementTimeout,
		MaxConnections:      cfg.MaxConnections,
		Version:             version,
		DDL:                 objects,
		Activity:            work,
		Grants:              grants,
		Audit:               history,
		Tasks:               jobs,
		DisableSSOPasswords: single != nil && !cfg.OIDC.PasswordSignIn,
		TokenMaxLifetime:    cfg.TokenMaxLifetime,
		Policies:            policies,
		AllowedLocations:    cfg.Storage.AllowedLocations,
		Warehouse:           warehouseRoot(cfg),
		Workload:            limits,
		Pool:                cfg.Pool,
		Sharing:             shares,
		Federation:          fed,
		Notify:              notifier,
		Clones:              clones,
		Lineage:             lineage,
		Distributed:         dist,
		Erasure:             eraser,
	}
	if single != nil && single.AccessTokensEnabled() {
		opts.AccessTokens = single
		if tlsConfig == nil {
			log.Warn("IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE has no effect without TLS: access tokens are accepted only inside TLS")
		}
	}
	if srv, err = pgwire.New(eng, opts, log); err != nil {
		return err
	}
	if shares != nil {
		// Tables with masking or row access policies are neither shared
		// nor served; until now the sharing endpoint served none.
		shares.SetProtection(srv.ProtectedBy)
	}
	if eraser != nil {
		eng.SetErasure(eraser.List, srv.ErasureViewer)
	}
	provisioning, err := openSCIM(users, single, func(user string) { srv.UserChanged(user) }, log)
	if err != nil {
		return err
	}

	// Built-in alerts go out until shutdown begins.
	notifyCtx, stopNotify := context.WithCancel(ctx)
	notifyDone := make(chan struct{})
	go func() {
		defer close(notifyDone)
		notifier.Run(notifyCtx)
	}()
	// Notifications still waiting once scheduled work has stopped are
	// given up.
	defer func() {
		stopNotify()
		<-notifyDone
		notifier.Close()
	}()

	// Tasks, pipes, materialized views and alerts start until shutdown
	// begins.
	// Runs under way then get the time running queries get; any left when
	// the server returns are canceled before the engine closes.
	jobsCtx, stopJobs := context.WithCancel(ctx)
	jobsDone := make(chan struct{})
	go func() {
		defer close(jobsDone)
		jobs.Run(jobsCtx, srv)
	}()
	defer func() {
		stopJobs()
		<-jobsDone
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		jobs.Wait(canceled)
	}()

	maintains := cat != nil && cfg.Maintenance.Enabled
	var schedule maintenance.Schedule
	if maintains {
		stopMaintenance, lock, err := startMaintenance(ctx, cfg, cat, guard, policies, reg, work, notifier.MaintenanceFailed, log)
		if err != nil {
			return err
		}
		defer stopMaintenance()
		if lock != nil {
			schedule = lock
		}
	}

	health := &health{engine: eng, users: users, catalog: cfg.Catalog, client: httpClient, activity: work, jobs: jobs}
	var consoleSrv *http.Server
	if cfg.ConsoleAddr != "" {
		consoleLn, err := listenConsole(cfg.ConsoleAddr)
		if err != nil {
			return err
		}
		status := consoleStatus(cfg, started, work, health, schedule, maintains)
		if consoleSrv, err = serveConsole(ctx, consoleLn, srv, users, single, provisioning, tlsConfig, status, errc, log); err != nil {
			_ = consoleLn.Close()
			return err
		}
	}
	var flightSrv *flightsql.Server
	if cfg.FlightAddr != "" {
		flightLn, err := listenFlight(cfg.FlightAddr)
		if err != nil {
			return err
		}
		if flightSrv, err = serveFlight(flightLn, srv, tlsConfig, errc, log); err != nil {
			_ = flightLn.Close()
			return err
		}
	}
	// Like the console, the Iceberg REST endpoint listens before the SQL
	// port, which the scale-to-zero gateway waits for.
	var icebergSrv *http.Server
	icebergAddr := cfg.IcebergAddr
	if cat == nil {
		icebergAddr = ""
	}
	if icebergAddr != "" {
		icebergLn, err := net.Listen("tcp", icebergAddr)
		if err != nil {
			return fmt.Errorf("iceberg: %w", err)
		}
		deps := icebergDeps{sql: srv, users: users, grants: grants, history: history, work: work, reg: reg, tls: tlsConfig, clones: guard}
		if icebergSrv, err = serveIceberg(ctx, icebergLn, cfg, deps, errc, log); err != nil {
			_ = icebergLn.Close()
			return err
		}
	}

	// Like the console, the sharing endpoint listens before the SQL port.
	var sharingSrv *http.Server
	if shares != nil && cfg.SharingAddr != "" {
		sharingLn, err := net.Listen("tcp", cfg.SharingAddr)
		if err != nil {
			return fmt.Errorf("delta sharing: %w", err)
		}
		sharingSrv = serveSharing(sharingLn, shares.Handler(), tlsConfig, errc, log)
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
	log.Info("igloo is ready", "sql", ln.Addr().String(), "http", cfg.HTTPAddr, "console", cfg.ConsoleAddr,
		"flight_sql", cfg.FlightAddr, "iceberg", icebergAddr, "sharing", sharingAddr(shares, cfg), "pool", cfg.Pool)

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
	if flightSrv != nil {
		_ = flightSrv.Shutdown(shutdownCtx)
	}
	if icebergSrv != nil {
		_ = icebergSrv.Shutdown(shutdownCtx)
	}
	if sharingSrv != nil {
		_ = sharingSrv.Shutdown(shutdownCtx)
	}
	err = srv.Shutdown(shutdownCtx)
	jobs.Wait(shutdownCtx)
	return err
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

// openSSO sets up single sign-on, when IGLOO_OIDC_ISSUER is set. It needs
// the metadata database, which keeps the links between users and identity
// provider accounts. changed is told about users whose roles a sign-in
// changed.
func openSSO(cfg config.Config, users auth.Store, changed func(string), log *slog.Logger) (*sso.Service, error) {
	if !cfg.OIDC.Enabled() {
		return nil, nil
	}
	accounts, ok1 := users.(auth.AccountStore)
	links, ok2 := users.(auth.SSOStore)
	if !ok1 || !ok2 {
		return nil, errors.New("IGLOO_OIDC_ISSUER needs IGLOO_METADATA_DATABASE_URL, where single sign-on keeps who is who")
	}
	provisioned, _ := users.(auth.SCIMStore)
	single, err := sso.New(sso.Options{Config: cfg.OIDC, Users: users, Accounts: accounts, SSO: links, SCIM: provisioned,
		Changed: changed, Log: log})
	if err != nil {
		return nil, err
	}
	if len(cfg.OIDC.AllowedDomains) == 0 && len(cfg.OIDC.AllowedGroups) == 0 {
		log.Warn("single sign-on admits every account of the identity provider; set IGLOO_OIDC_ALLOWED_DOMAINS or IGLOO_OIDC_ALLOWED_GROUPS to limit it",
			"issuer", cfg.OIDC.Issuer)
	}
	log.Info("single sign-on is on", "issuer", cfg.OIDC.Issuer, "create_users", cfg.OIDC.CreateUsers,
		"password_sign_in", cfg.OIDC.PasswordSignIn, "access_tokens", cfg.OIDC.AccessTokenAudience != "")
	return single, nil
}

// openSCIM sets up SCIM provisioning, which comes with single sign-on. It
// returns nil without single sign-on.
func openSCIM(users auth.Store, single *sso.Service, changed func(string), log *slog.Logger) (http.Handler, error) {
	store, ok := users.(auth.SCIMStore)
	accounts, _ := users.(auth.AccountStore)
	tokens, _ := users.(auth.TokenStore)
	if single == nil || !ok || accounts == nil || tokens == nil {
		return nil, nil
	}
	h, err := scim.New(scim.Options{Users: users, Accounts: accounts, SCIM: store, Tokens: auth.NewTokenChecker(users, tokens),
		SSO: single, Changed: changed, Log: log})
	if err != nil {
		return nil, err
	}
	return h, nil
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

// openTasks opens where tasks, pipes, materialized views and the load
// history are kept: the metadata database, shared by every replica, or
// else this replica's memory.
func openTasks(ctx context.Context, cfg config.Config, log *slog.Logger) (tasks.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; tasks, pipes, materialized views and the load history are kept in memory and lost on restart")
		return tasks.NewMemoryStore(), nil
	}
	store, err := tasks.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("tasks: %w", err)
	}
	return store, nil
}

// openPolicies opens where masking policies, row access policies and tags
// are kept: the metadata database, shared by every replica, or else this
// replica's memory.
func openPolicies(ctx context.Context, cfg config.Config, log *slog.Logger) (policy.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; policies and tags are kept in memory and lost on restart")
		return policy.NewMemoryStore(), nil
	}
	store, err := policy.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("policies: %w", err)
	}
	return store, nil
}

// openErasure opens where erasure requests are kept, the metadata database
// or else this replica's memory, and the purger that drops what they
// delete from this replica's read cache.
func openErasure(ctx context.Context, cfg config.Config, eng *engine.Engine, log *slog.Logger) (erasure.Store, *erasure.Purger, error) {
	var store erasure.Store
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; erasure requests are kept in memory and lost on restart")
		store = erasure.NewMemoryStore()
	} else {
		var err error
		if store, err = erasure.OpenPostgres(ctx, cfg.MetadataDatabaseURL); err != nil {
			return nil, nil, fmt.Errorf("erasure requests: %w", err)
		}
	}
	cacheID, err := eng.CacheID()
	if err != nil {
		store.Close()
		return nil, nil, fmt.Errorf("read cache ID: %w", err)
	}
	host, _ := os.Hostname()
	return store, erasure.NewPurger(store, eng, cacheID, host, log), nil
}

// newEraser sets up erasure requests. They redact the query history and
// the load and task history, so both stores must be able to: a store that
// cannot would keep identifiers the request is meant to erase.
func newEraser(cfg config.Config, store erasure.Store, purger *erasure.Purger, cat *catalog.Client, eng *engine.Engine,
	policies policy.Store, history audit.Store, taskStore tasks.Store, jobs *tasks.Scheduler, log *slog.Logger) (*erasure.Service, error) {
	opts := erasure.Options{
		Store: store, Catalog: cat, Policies: policies.Load, Engine: eng,
		CatalogName: cfg.Catalog.Name, Schema: cfg.Catalog.Schema, Warehouse: cfg.Storage.Root(),
		Jobs: jobs, Purger: purger, Log: log,
	}
	tasksHistory, ok := taskStore.(erasure.History)
	if !ok {
		return nil, errors.New("the task store cannot redact its history")
	}
	opts.History = append(opts.History, tasksHistory)
	if history != nil {
		queries, ok := history.(erasure.History)
		lineage, ok2 := history.(erasure.Lineage)
		if !ok || !ok2 {
			return nil, errors.New("the query history cannot be redacted")
		}
		opts.History, opts.Lineage = append(opts.History, queries), lineage
	}
	return erasure.New(opts), nil
}

// warehouseRoot is where the warehouse's files are, or "" without one.
func warehouseRoot(cfg config.Config) string {
	if cfg.Storage.Location == "" {
		return ""
	}
	return cfg.Storage.Root()
}

// openCloneStore opens where the record of which tables share files with
// clones is kept: the metadata database, shared by every replica, or else
// this replica's memory, which maintenance rebuilds from the clones' own
// metadata.
func openCloneStore(ctx context.Context, cfg config.Config) (clone.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		return clone.NewMemoryStore(), nil
	}
	store, err := clone.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("clones: %w", err)
	}
	return store, nil
}

// startMaintenance compacts and expires tables in the background until ctx is
// canceled. Replicas sharing a metadata database take turns, through lock,
// which also records when passes ran; it is nil without one. guard keeps
// the files clones share, and policies have the tables' data retentions.
func startMaintenance(ctx context.Context, cfg config.Config, cat *catalog.Client, guard *clone.Guard, policies policy.Store,
	reg prometheus.Registerer, work *activity.Tracker, failed func(string, error), log *slog.Logger) (stop func(), lock *maintenance.PostgresLock, err error) {
	opts := maintenance.Options{
		Interval:  cfg.Maintenance.Interval,
		Retention: cfg.Maintenance.Retention,
		Metrics:   maintenance.NewMetrics(reg),
		Activity:  work,
		Failed:    failed,
		Clones:    guard,
		DataRetention: func(ctx context.Context) (map[string]time.Duration, error) {
			snap, err := policies.Load(ctx)
			if err != nil {
				return nil, err
			}
			limits := map[string]time.Duration{}
			for _, r := range snap.Catalog.Retentions {
				limits[strings.ToLower(r.Schema+"."+r.Table)] = time.Duration(r.Days) * 24 * time.Hour
			}
			return limits, nil
		},
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
