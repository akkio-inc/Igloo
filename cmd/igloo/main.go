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
//	igloo models ...                list, install, verify and remove the model pack's models (docs/models.md)
//	igloo check-extensions [dir]    check that the rb extension in dir (default IGLOO_DUCKDB_EXTENSION_DIR) loads
//	igloo gateway                   run the scale-to-zero gateway (docs/scale-to-zero.md)
//	igloo worker                    run a distributed-execution worker (docs/distributed-execution.md)
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/cleanroom"
	"github.com/akkio-inc/igloo/internal/clone"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/ddl"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/flightsql"
	"github.com/akkio-inc/igloo/internal/iddomain"
	"github.com/akkio-inc/igloo/internal/maintenance"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/models"
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
	// spotPoll is how often a worker on EC2 asks whether AWS is taking the
	// instance back, and spotMargin how long before then it stops waiting
	// for its tasks.
	spotPoll   = 5 * time.Second
	spotMargin = 10 * time.Second
	// peakSampleEvery is how often the workload controller reads what
	// DuckDB holds for running statements; a statement shorter than that
	// may fall between two reads, and is learned by what it allocated.
	peakSampleEvery = 100 * time.Millisecond
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
	case len(os.Args) > 1 && os.Args[1] == "check-extensions":
		dir := os.Getenv("IGLOO_DUCKDB_EXTENSION_DIR")
		if len(os.Args) == 3 {
			dir = os.Args[2]
		}
		var msg string
		if msg, err = engine.CheckExtensions(context.Background(), dir); err == nil {
			fmt.Println(msg)
		}
	case len(os.Args) > 1 && os.Args[1] == "models":
		err = modelsCommand(os.Args[2:], os.Stdout)
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
	engine.ExitOnCrash(log)

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
	switch {
	case cfg.Catalog.URI != "" && cfg.Catalog.Setup:
		if err := catalog.Setup(startCtx, cfg, httpClient, log); err != nil {
			return err
		}
	case cfg.Catalog.URI != "":
		// Setup is what limits the warehouse's format versions.
		log.Warn("IGLOO_CATALOG_SETUP is false, so Igloo does not limit the warehouse's Iceberg format versions to 1 and 2 in the catalog: "+
			"it refuses version 3 itself for tables with tags and tables erasure requests erase from, but an engine that reaches the "+
			"catalog without Igloo can make one; set the warehouse's format-version policy in the catalog yourself (see docs/privacy.md)",
			"catalog", cfg.Catalog.URI)
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
		// Connections are attached before DuckDB's sandbox is locked, which
		// then lets SQL read the files of the tables of the Iceberg REST
		// catalogs among them.
		engineOpts = append(engineOpts, engine.BeforeLockdown(fed.Attach), engine.SandboxDirectories(fed.SandboxDirectories))
	}
	eng, err := engine.Open(startCtx, cfg, log, engineOpts...)
	if err != nil {
		return err
	}
	defer eng.Close() //nolint:errcheck
	memory := limitMemory(eng.MemoryLimit(startCtx), log)
	if fed != nil {
		fed.SetConnect(eng.Connect)
		go fed.Run(ctx)
	}

	var (
		cat        *catalog.Client
		objects    *ddl.Manager
		clones     *clone.Manager
		guard      *clone.Guard
		cloneStore clone.Store
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
		if cloneStore, err = openCloneStore(startCtx, cfg); err != nil {
			return err
		}
		defer cloneStore.Close()
		clones, guard = clone.NewManager(cat, cloneStore, log), clone.NewGuard(cat, cloneStore, log)
		// Every way Igloo deletes a table's files decides with guard.
		guard.SetWarehouse(warehouseRoot(cfg))
		// A table a clone replaces has its files deleted, with Igloo's
		// own credentials, when a drop's would be.
		clones.Purging(warehouseRoot(cfg), func(ctx context.Context, location string) (iceio.IO, error) {
			return catalog.FS(ctx, cfg.Storage, location)
		})
	}
	policies, err := openPolicies(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer policies.Close()
	domains, err := openIDDomains(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer domains.Close()
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
	aiRuntime, err := openAI(startCtx, cfg, users, log)
	if err != nil {
		return err
	}
	defer aiRuntime.Store().Close()
	modelStore, err := openModels(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer modelStore.Close()
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
	// Clean rooms need what Delta Sharing needs; partners are recipients.
	var rooms *cleanroom.Service
	if shares != nil {
		roomsCtx, cancelRooms := context.WithTimeout(ctx, startupTimeout)
		var closeRooms func()
		rooms, closeRooms, err = openCleanRooms(roomsCtx, cfg, eng, policies, log)
		cancelRooms()
		if err != nil {
			return fmt.Errorf("clean rooms: %w", err)
		}
		defer closeRooms()
	}

	// The catalog's tables as Igloo plans reads of them: their metadata and
	// data files, loaded once for distributed execution and for listing a
	// statement's scans ahead of DuckDB (distributed.Scanner) alike.
	var tables *distributed.Iceberg
	if cat != nil {
		tables = distributed.NewIceberg(cat)
	}

	// Reading ahead for statements lists their scans as distributed
	// execution plans them.
	var scanner *distributed.Scanner
	if cfg.Prefetch.Enabled && tables != nil && eng.Prefetcher() != nil {
		if scanner, err = distributed.NewScanner(tables); err != nil {
			return err
		}
		defer scanner.Close() //nolint:errcheck
	}

	// Statements that read large tables run in parts on workers.
	var dist *distributed.Coordinator
	if len(cfg.Workers.Addresses) > 0 {
		if cat == nil {
			return errors.New("IGLOO_WORKERS needs IGLOO_CATALOG_URI")
		}
		if dist, err = newCoordinator(ctx, cfg, tables, eng, reg, log); err != nil {
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
	// Erasure requests erase ids from bitmap indexes through the SQL
	// server, which is made after them (see pgwire.BitmapIDSets).
	bitmapIDs := &pgwire.BitmapIDSets{}
	// They train models again through it too (see pgwire.ModelRetrainer).
	retrainer := &pgwire.ModelRetrainer{}
	if cat != nil {
		erasureStore, purger, err := openErasure(ctx, cfg, eng, log)
		if err != nil {
			return err
		}
		defer erasureStore.Close()
		// Requests hold the locks of tables' locations that making a clone
		// takes while they delete files, and a request that fails for good,
		// or is blocked, raises the built-in alert for failed runs.
		eraser, err = newEraser(cfg, erasure.Options{
			Store: erasureStore, Catalog: cat, Policies: policies.Load, Engine: eng, Jobs: jobs, Purger: purger,
			Locks: cloneStore, Failed: notifier.ErasureFailed, Blocked: notifier.ErasureBlocked, Metrics: erasure.NewMetrics(reg), Log: log,
			Locations: cat.Locations, Memory: rewriteMemory(cfg, memory), IDDomains: domains, IDSets: idSetsFor(cfg, bitmapIDs),
			// What AI functions derived from the tables, cached, and the
			// models trained from them.
			Derived: []erasure.Derived{aiRuntime.Store()}, Models: retrainer,
		}, historyStore, taskStore)
		if err != nil {
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
		// Workers keep up with requests through the coordinator, which
		// records them in the requests' ledger (see distributed's
		// erasure.go).
		if dist != nil {
			// A worker purged has requests blocked for it run at once.
			ledger := erasure.NewWorkerLedger(erasureStore)
			ledger.Purged = func() { eraser.WakeBlocked(context.WithoutCancel(ctx)) }
			watchCtx, stopWatching := context.WithCancel(ctx)
			watched := make(chan struct{})
			go func() {
				defer close(watched)
				dist.WatchErasures(watchCtx, ledger)
			}()
			defer func() {
				stopWatching()
				<-watched
			}()
		}
	}

	single, err := openSSO(cfg, users, func(user string) { srv.UserChanged(user) }, log)
	if err != nil {
		return err
	}
	// Memory reservations for role limits come out of DuckDB's memory.
	limits := workload.NewController(eng.MemoryLimit(ctx))
	replica := cfg.MaxConcurrentStatements
	if replica < 0 {
		// auto: as many as the cores Go may use, which follow a
		// container's CPU limit.
		replica = runtime.GOMAXPROCS(0)
	}
	limits.LimitReplica(replica)
	limits.EstimateFromHistory(cfg.StatementReservations)
	reg.MustRegister(limits.Metrics()...)
	// What statements hold at once, for their reservations (see
	// workload.Controller.SampleMemory).
	if read, closeRead, err := eng.QueryMemory(ctx); err != nil {
		log.Warn("statements' peak memory is not sampled; reservations come from what they allocated", "err", err)
	} else {
		defer closeRead() //nolint:errcheck
		go limits.SampleMemory(ctx, peakSampleEvery, read)
	}
	if historyStore != nil {
		var seed func([]distributed.Run)
		if dist != nil {
			seed = dist.Seed
		}
		go seedHistory(ctx, seed, limits, eng.MemoryLimit(ctx), historyStore, log)
	}

	opts := pgwire.Options{
		Users:               users,
		TLS:                 tlsConfig,
		RequireTLS:          cfg.TLSRequired,
		Metrics:             pgwire.NewMetrics(reg),
		StatementTimeout:    cfg.StatementTimeout,
		MaxConnections:      cfg.MaxConnections,
		LoginTimeout:        cfg.LoginTimeout,
		AccessMaxStaleness:  cfg.AccessMaxStaleness,
		Version:             version,
		DDL:                 objects,
		Activity:            work,
		Grants:              grants,
		Audit:               history,
		Tasks:               jobs,
		DisableSSOPasswords: single != nil && !cfg.OIDC.PasswordSignIn,
		TokenMaxLifetime:    cfg.TokenMaxLifetime,
		Policies:            policies,
		IDDomains:           domains,
		Scanner:             scanner,
		Prefetcher:          eng.Prefetcher(),
		IDDomainsOn:         cfg.IDDomains,
		IDSyncMinRows:       cfg.IDSyncMinRows,
		SortSpillBudget:     cfg.DuckDB.SortSpillBudget,
		Bitmaps:             cfg.IDDomains && cfg.BitmapMinRows >= 0 && eng.BitmapsLoaded(),
		BitmapMinRows:       cfg.BitmapMinRows,
		BitmapIndexes:       cfg.BitmapIndexes && cfg.IDDomains && eng.BitmapsLoaded(),
		ReportRewrite:       cfg.ReportRewrite,
		ExclusivePrepares:   !cfg.SharedPrepares,
		ResultCacheSize:     cfg.ResultCacheSize,
		QueryMemory:         eng.MemoryLimit(ctx),
		RewriteMemory:       rewriteMemory(cfg, memory),
		MemoryTotal:         memory.Total,
		FreeMemory:          func(ctx context.Context) { dropDuckDBCaches(ctx, eng, log) },
		AllowedLocations:    cfg.Storage.AllowedLocations,
		Warehouse:           warehouseRoot(cfg),
		Workload:            limits,
		Pool:                cfg.Pool,
		Sharing:             shares,
		Federation:          fed,
		Notify:              notifier,
		AI:                  aiRuntime,
		Clones:              clones,
		Lineage:             lineage,
		Distributed:         dist,
		Models:              modelStore,
		Erasure:             eraser,
		CleanRooms:          rooms,
	}
	if guard != nil {
		// A table dropped that no clone reads has its files deleted with
		// it (see pgwire's catalogguard.go).
		opts.ClonePurge = guard.MayPurge
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
	bitmapIDs.Bind(srv)
	retrainer.Bind(srv)
	// The AI functions run in this server's sessions; workers have none.
	eng.SetAI(srv.AIHook())
	// The AI cache's expired entries go, every hour, until shutdown.
	aiCtx, stopAI := context.WithCancel(ctx)
	defer stopAI()
	go aiRuntime.Run(aiCtx, time.Hour)
	// Some of DuckDB's work ignores its memory limit; rather than let the
	// kernel kill the process, cancel the statement that has run longest
	// when memory runs low (see memlimit.Watch).
	// Before that, DuckDB's memory limit gives way to the memory DuckDB
	// uses besides (see memlimit.Adapt).
	if memory.Total > 0 {
		go memlimit.Watch(ctx, memory.Total, srv.ShedMemory)
		go memlimit.Adapt(ctx, memory.Total, memory.DuckDB, func(n int64) error { return eng.SetMemoryLimit(ctx, n) }, log)
		log.Info("memory watch is on", "cancels_below", memlimit.Format(memlimit.Low(memory.Total)),
			"spills_more_below", memlimit.Format(memlimit.High(memory.Total)))
	}
	if shares != nil {
		// Tables with masking or row access policies are neither shared
		// nor served; until now the sharing endpoint served none.
		shares.SetProtection(srv.ProtectedBy)
	}
	if eraser != nil {
		eng.SetErasure(eraser.List, srv.ErasureViewer)
	}
	if rooms != nil {
		// Runs execute through the SQL server, as each room's identity.
		// Each start writes the rooms' confinements again, in case a
		// change stopped halfway.
		rooms.SetEngine(srv)
		if err := rooms.Reconcile(ctx); err != nil {
			log.Warn("could not check the clean rooms' confinements", "err", err)
		}
	}
	provisioning, err := openSCIM(users, single, func(user string) { srv.UserChanged(user) }, log)
	if err != nil {
		return err
	}
	// The roles of users whose groups SCIM changed, which a failure left
	// behind, are applied until shutdown begins.
	var scimHandler http.Handler
	if provisioning != nil {
		scimHandler = provisioning
		scimCtx, stopSCIM := context.WithCancel(ctx)
		scimDone := make(chan struct{})
		go func() {
			defer close(scimDone)
			provisioning.Run(scimCtx, time.Minute)
		}()
		defer func() {
			stopSCIM()
			<-scimDone
		}()
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
		threads, err := eng.Threads(ctx)
		if err != nil {
			return err
		}
		stopMaintenance, lock, err := startMaintenance(ctx, cfg, cat, guard, policies, memory, threads, reg, work, notifier.MaintenanceFailed, func(ctx context.Context) {
			// Bitmap indexes hold ids, so they are built once ids are mapped.
			srv.MapIDDomains(ctx)
			srv.MaintainBitmapIndexes(ctx)
		}, log)
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
		if consoleSrv, err = serveConsole(ctx, consoleLn, srv, users, single, scimHandler, tlsConfig, status, errc, log); err != nil {
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
		deps := icebergDeps{sql: srv, users: users, grants: grants, history: history, work: work, reg: reg, tls: tlsConfig, clones: guard,
			changed: eng.CatalogChanged}
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
		sharingSrv = serveSharing(sharingLn, sharingHandler(shares, rooms, cfg, history, log), tlsConfig, errc, log)
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

	// The first statement that lists columns would load every Iceberg
	// table's schema, seconds with hundreds of tables; begin now, in the
	// background (see engine.Engine.WarmSchemas). It stops before DuckDB
	// does.
	warmCtx, stopWarm := context.WithCancel(ctx)
	warmDone := make(chan struct{})
	go func() {
		defer close(warmDone)
		start := time.Now()
		if err := eng.WarmSchemas(warmCtx); err != nil && warmCtx.Err() == nil {
			log.Warn("could not load every table's schema in the background", "err", err)
			return
		}
		log.Info("table schemas loaded", "took", time.Since(start).Round(time.Millisecond))
	}()
	defer func() {
		stopWarm()
		<-warmDone
	}()

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

	if err := ensureAdmin(ctx, store, cfg.AdminUser, cfg.AdminPassword, log); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

// ensureAdmin creates the bootstrap administrator, user, with password,
// unless an account of that name exists. Only a password that passes
// config.CheckAdminPassword creates one. A weaker one is refused when the
// account is missing, but only warned about when it exists, since it is
// not used then: installs made before the check keep starting.
//
// A weak password never reaches EnsureAdmin, which inserts the account
// only if no user of that name exists, in one statement. So replicas
// starting together cannot create the account with it, whichever of them
// gets there first; a store without a metadata database creates the
// account on every start, and so always checks.
func ensureAdmin(ctx context.Context, store auth.Store, user, password string, log *slog.Logger) error {
	weak := config.CheckAdminPassword(user, password)
	if weak == nil {
		verifier, err := auth.NewVerifier(password)
		if err != nil {
			return fmt.Errorf("IGLOO_ADMIN_PASSWORD: %w", err)
		}
		created, err := store.EnsureAdmin(ctx, user, verifier)
		if err != nil {
			return fmt.Errorf("create admin user: %w", err)
		}
		if created {
			log.Info("created admin user", "user", user)
		}
		return nil
	}
	switch _, err := store.Get(ctx, user); {
	case errors.Is(err, auth.ErrNotFound):
		return weak
	case err != nil:
		return fmt.Errorf("look up admin user: %w", err)
	}
	// Which check failed is left out: the account's password may well be
	// this value, and logs are read more widely than secrets.
	log.Warn(fmt.Sprintf("IGLOO_ADMIN_PASSWORD is too weak to create the admin account with: it has fewer than %d characters, "+
		"or is a placeholder or easily guessed. The account exists, so Igloo starts and does not use it. "+
		"Give the account a long random password with ALTER USER and set IGLOO_ADMIN_PASSWORD to the same, "+
		"or Igloo will refuse to start if it has to create the account again", config.MinAdminPasswordLength),
		"user", user)
	return nil
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
func openSCIM(users auth.Store, single *sso.Service, changed func(string), log *slog.Logger) (*scim.Handler, error) {
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

// openModels opens where models' rows and grants are kept (their blobs
// are in the warehouse): the metadata database, shared by every replica,
// or else this replica's memory.
func openModels(ctx context.Context, cfg config.Config, log *slog.Logger) (models.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		log.Warn("IGLOO_METADATA_DATABASE_URL is not set; models (SNOWFLAKE.ML classes) are kept in memory and lost on restart")
		return models.NewMemoryStore(), nil
	}
	store, err := models.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("models: %w", err)
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

// openIDDomains opens where ID domains and their members are kept, with
// their id counters: the metadata database, shared by every replica, or
// else this replica's memory.
func openIDDomains(ctx context.Context, cfg config.Config, log *slog.Logger) (iddomain.Store, error) {
	if cfg.MetadataDatabaseURL == "" {
		if cfg.IDDomains {
			log.Warn("IGLOO_METADATA_DATABASE_URL is not set; ID domains are kept in memory and lost on restart")
		}
		return iddomain.NewMemoryStore(), nil
	}
	store, err := iddomain.OpenPostgres(ctx, cfg.MetadataDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("ID domains: %w", err)
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
	cacheID, token, save, err := eng.CacheIdentity()
	if err != nil {
		store.Close()
		return nil, nil, fmt.Errorf("read cache ID: %w", err)
	}
	host, _ := os.Hostname()
	purger := erasure.NewPurger(store, eng, cacheID, host, log)
	if save != nil {
		purger.OnDisk(token, save)
	}
	return store, purger, nil
}

// newEraser sets up erasure requests. They redact the query history and
// the load and task history, so both stores must be able to: a store that
// cannot would keep identifiers the request is meant to erase.
func newEraser(cfg config.Config, opts erasure.Options, history audit.Store, taskStore tasks.Store) (*erasure.Service, error) {
	opts.CatalogName, opts.Schema, opts.Warehouse = cfg.Catalog.Name, cfg.Catalog.Schema, cfg.Storage.Root()
	// The warehouse's files, those of tables dropped included, are listed
	// and deleted with Igloo's own credentials.
	opts.Storage = func(ctx context.Context, location string) (iceio.IO, error) {
		return catalog.FS(ctx, cfg.Storage, location)
	}
	log := opts.Log
	// The key that seals requests' identifiers is not in the metadata
	// database. A replica without one keeps its requests in memory, where
	// a key of its own does.
	switch {
	case cfg.ErasureKey != "":
		key := sha256.Sum256([]byte(cfg.ErasureKey))
		opts.Key = key[:]
	case cfg.MetadataDatabaseURL == "":
		opts.Key = make([]byte, 32)
		if _, err := rand.Read(opts.Key); err != nil {
			return nil, err
		}
	default:
		log.Warn("IGLOO_ERASURE_KEY is not set; ERASE SUBJECT is refused until it is")
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
// the files clones share, policies have the tables' data retentions, and
// memory says how much memory Go has, of which compaction gets a share.
// rewriteMemory is what rewriting a table's files may hold at once, for
// compaction and erasure alike: IGLOO_MAINTENANCE_MEMORY, or a share of Go's
// memory limit.
func rewriteMemory(cfg config.Config, memory memlimit.Plan) int64 {
	budget := cfg.Maintenance.Memory
	if budget == 0 {
		budget = memory.Maintenance()
	}
	if budget == 0 {
		budget = maintenance.DefaultMemory
	}
	return budget
}

// dropDuckDBCaches makes DuckDB write out or drop the buffers it can, its
// caches among them: it asks for a memory limit of 256 MiB, which DuckDB
// evicts buffers to meet (and refuses, having evicted what it could, while
// running work pins more), then puts the limit back. The memory watch does
// this before it gives up on work no statement owns any more.
func dropDuckDBCaches(ctx context.Context, eng *engine.Engine, log *slog.Logger) {
	limit := eng.MemoryLimit(ctx)
	if limit <= 256<<20 {
		return
	}
	if err := eng.SetMemoryLimit(ctx, 256<<20); err != nil {
		log.Debug("DuckDB could not free enough for a 256 MiB memory limit", "err", err)
		return
	}
	if err := eng.SetMemoryLimit(ctx, limit); err != nil {
		log.Warn("could not restore DuckDB's memory limit after dropping its caches", "limit", memlimit.Format(limit), "err", err)
	}
}

func startMaintenance(ctx context.Context, cfg config.Config, cat *catalog.Client, guard *clone.Guard, policies policy.Store,
	memory memlimit.Plan, threads int, reg prometheus.Registerer, work *activity.Tracker, failed func(string, error), beforePass func(context.Context),
	log *slog.Logger) (stop func(), lock *maintenance.PostgresLock, err error) {
	budget := rewriteMemory(cfg, memory)
	if memory.Go > 0 && budget > memory.Go/2 {
		log.Warn("IGLOO_MAINTENANCE_MEMORY is more than half of Go's memory limit; compaction may leave too little for the rest of Igloo",
			"maintenance_memory", memlimit.Format(budget), "go_memory_limit", memlimit.Format(memory.Go))
	}
	opts := maintenance.Options{
		Interval:  cfg.Maintenance.Interval,
		Retention: cfg.Maintenance.Retention,
		Memory:    budget,
		Threads:   threads,
		// Clustering spills where DuckDB does, on local disk where it
		// is set (IGLOO_DUCKDB_TEMP_DIR).
		SpillDir:   cfg.DuckDB.TempDir,
		Metrics:    maintenance.NewMetrics(reg),
		Activity:   work,
		Failed:     failed,
		Clones:     guard,
		BeforePass: beforePass,
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
	log.Info("table maintenance is on", "interval", cfg.Maintenance.Interval, "snapshot_retention", cfg.Maintenance.Retention,
		"memory", memlimit.Format(budget))
	return func() {
		cancel()
		<-done
		if lock != nil {
			lock.Close()
		}
	}, lock, nil
}

// limitMemory gives Go a memory limit that, with DuckDB's (duckdb bytes),
// fits in the memory this process may use (see package memlimit), unless
// GOMEMLIMIT sets one, and logs how the memory is divided.
func limitMemory(duckdb int64, log *slog.Logger) memlimit.Plan {
	p := memlimit.Apply(duckdb)
	size := func(n int64) string {
		if n <= 0 {
			return "unknown"
		}
		return memlimit.Format(n)
	}
	goLimit := size(p.Go)
	switch {
	case p.FromEnv:
		goLimit += " (GOMEMLIMIT)"
	case p.Go == 0:
		goLimit = "none"
	}
	log.Info("memory", "total", size(p.Total), "from", p.Source, "duckdb", size(p.DuckDB), "go", goLimit)
	if p.Overcommitted() {
		log.Warn("DuckDB's and Go's memory limits add up to more than this process may use, so the kernel may kill Igloo "+
			"when both are busy; lower IGLOO_DUCKDB_MEMORY_LIMIT (or GOMEMLIMIT)",
			"total", size(p.Total), "duckdb", size(p.DuckDB), "go", size(p.Go), "reserve", size(memlimit.Reserve(p.Total)))
	}
	return p
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

// idSetsFor is what erasure requests erase ids from: bitmap indexes, which
// exist only with ID domains, and stay in their tables when
// IGLOO_BITMAP_INDEXES is turned off again.
func idSetsFor(cfg config.Config, bitmaps *pgwire.BitmapIDSets) erasure.IDSets {
	if !cfg.IDDomains {
		return nil
	}
	return bitmaps
}
