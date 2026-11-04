package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/erasure"
	"github.com/akkio-inc/igloo/internal/gateway"
	"github.com/akkio-inc/igloo/internal/icebergrest"
	"github.com/akkio-inc/igloo/internal/memlimit"
	"github.com/akkio-inc/igloo/internal/prefetch"
	"github.com/akkio-inc/igloo/internal/spot"
	"github.com/akkio-inc/igloo/internal/workload"
)

// serveWorker runs a distributed-execution worker
// (docs/distributed-execution.md): it runs parts of statements for Igloo
// servers holding the same IGLOO_WORKER_SECRET, on its own DuckDB.
func serveWorker() error {
	cfg, err := config.LoadWorker(os.LookupEnv)
	if err != nil {
		return err
	}
	log := newLogger(cfg).With("role", "worker")
	log.Info("starting igloo worker", "version", version)
	engine.ExitOnCrash(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	eng, err := engine.OpenWorker(startCtx, cfg, log)
	if err != nil {
		return err
	}
	defer eng.Close() //nolint:errcheck
	threads, err := eng.Threads(startCtx)
	if err != nil {
		return err
	}
	memory := eng.MemoryLimit(startCtx)
	if memory == 0 {
		log.Warn("could not read DuckDB's memory limit")
	}
	plan := limitMemory(memory, log)
	cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(eng.Metrics()...)
	locations := append([]string{cfg.Storage.Root()}, cfg.Storage.AllowedLocations...)
	opts := distributed.WorkerOptions{
		Engine: eng, Secret: cfg.Workers.Secret, Version: version, TempDir: cfg.DuckDB.TempDir,
		Locations: locations, MemoryBytes: memory, MemoryNow: eng.MemoryLimit, Threads: threads, ExchangeBytes: cfg.Workers.ExchangeMax,
		Metrics: distributed.NewWorkerMetrics(reg),
	}
	if p := eng.Prefetcher(); p != nil {
		opts.Prefetch = prefetchTasks(p)
	}
	// What machine it runs on, for coordinators that were not told its
	// price (package prices): on EC2, outside Kubernetes, whose chart tells
	// them and whose pods seldom reach the metadata service.
	if cfg.Workers.SpotNotice && os.Getenv("KUBERNETES_SERVICE_HOST") == "" {
		dctx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
		if in, err := spot.DescribeEC2(dctx, spot.EC2); err == nil {
			opts.InstanceType, opts.Region, opts.Spot, opts.Instance = in.Type, in.Region, in.Spot, in.ID
			log.Info("running on EC2", "instance_type", in.Type, "region", in.Region, "spot", in.Spot)
		}
		dcancel()
	}
	if opts.Erasure, err = workerErasure(eng, &opts, cfg.Workers.IsolatedPurgeAfter); err != nil {
		return err
	}
	w, err := distributed.NewWorker(opts, log)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.Workers.ListenAddr)
	if err != nil {
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- w.Serve(ln) }()
	// Cancel the task that has run longest when memory runs low (see
	// memlimit.Watch), rather than let the kernel kill the worker and every
	// task on it; its statement fails as out of memory.
	if plan.Total > 0 {
		go memlimit.Watch(ctx, plan.Total, w.ShedMemory)
		go memlimit.Adapt(ctx, plan.Total, plan.DuckDB, func(n int64) error { return eng.SetMemoryLimit(ctx, n) },
			func() (int64, bool) { return eng.HeldMemory(ctx) }, log)
		log.Info("memory watch is on", "cancels_below", memlimit.Format(memlimit.Low(plan.Total)),
			"spills_more_below", memlimit.Format(memlimit.High(plan.Total)))
	}

	// Health, readiness and metrics, unauthenticated as on the server; tasks
	// are accepted only on the TLS port.
	var stopping atomic.Bool
	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(rw, "ok") })
		mux.HandleFunc("GET /readyz", func(rw http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			if stopping.Load() {
				http.Error(rw, "shutting down", http.StatusServiceUnavailable)
				return
			}
			if w.Retiring() {
				// Not ready, so that Kubernetes removes this pod first when a
				// coordinator scales the workers down (see distributed's
				// pool.shrink).
				http.Error(rw, "retiring", http.StatusServiceUnavailable)
				return
			}
			if err := eng.Ping(ctx); err != nil {
				http.Error(rw, "engine: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			_, _ = fmt.Fprintln(rw, "ok")
		})
		// Coordinators learn whether a worker is busy from its status, on
		// the TLS port; nothing here says so.
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		httpSrv = &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("http: %w", err)
			}
		}()
	}
	log.Info("igloo worker is ready", "tasks", ln.Addr().String(), "http", cfg.HTTPAddr, "threads", threads, "memory_bytes", memory)

	// On a Spot Instance, AWS says two minutes ahead that it takes the
	// instance back: the worker takes no more tasks from then on, which
	// coordinators run elsewhere, finishes those it has and serves the
	// exchange files on its disk until then (see Worker.Drain), as it does
	// for drainTimeout when a signal stops it.
	notices := make(chan spot.Notice, 1)
	if cfg.Workers.SpotNotice {
		go func() {
			n, err := spot.WatchEC2(ctx, spot.EC2, spotPoll)
			switch {
			case err == nil:
				notices <- n
			case errors.Is(err, spot.ErrNotEC2):
				// Off EC2, or in a container the instance's metadata hop
				// limit of 1 keeps from the service.
				log.Info("not watching for Spot interruptions: no instance metadata", "err", err)
			}
		}()
	}
	grace := drainTimeout
	var reclaimed time.Time
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	case n := <-notices:
		grace, reclaimed = n.Grace(spotMargin, shutdownTimeout), n.Time
		log.Warn("AWS is taking this instance back; finishing the tasks running and taking no more",
			"action", n.Action, "at", n.Time, "grace", grace.Round(time.Second))
	}
	stopping.Store(true)
	log.Info("shutting down: taking no more tasks, finishing those running and serving the exchange files on this worker's disk",
		"for_at_most", grace.Round(time.Second))
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), grace)
	defer cancelShutdown()
	_ = w.Drain(shutdownCtx)
	// What is left of the grace, at least a moment to end the connections.
	deadline, _ := shutdownCtx.Deadline()
	stopCtx, cancelStop := context.WithTimeout(context.Background(), max(time.Until(deadline), time.Second))
	defer cancelStop()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(stopCtx)
	}
	err = w.Shutdown(stopCtx)
	if !reclaimed.IsZero() {
		// Exiting now, the worker would be started again by its supervisor
		// and take tasks it cannot finish: it stays down until AWS has
		// taken the instance, or a minute past the time it said.
		select {
		case <-ctx.Done():
		case <-time.After(time.Until(reclaimed) + time.Minute):
		}
	}
	return err
}

// newCoordinator sets up distributed execution on the server, when
// IGLOO_WORKERS names workers.
func newCoordinator(ctx context.Context, cfg config.Config, source distributed.Source, eng *engine.Engine, reg prometheus.Registerer, log *slog.Logger) (*distributed.Coordinator, error) {
	compute, err := workerCompute(ctx, cfg.Workers)
	if err != nil {
		return nil, err
	}
	// The large worker, started and stopped as the others, by its own
	// Deployment, commands or instances.
	l := cfg.Workers.Large
	var large distributed.PoolOptions
	if l.Address != "" {
		lc, err := workerCompute(ctx, config.Workers{Compute: l.Compute, K8sNamespace: cfg.Workers.K8sNamespace, K8sDeployment: l.K8sDeployment,
			Max: 1, StartCommand: l.StartCommand, StopCommand: l.StopCommand, EC2InstanceIDs: l.EC2InstanceIDs})
		if err != nil {
			return nil, fmt.Errorf("large worker: %w", err)
		}
		large = distributed.PoolOptions{Addresses: []string{l.Address}, Compute: lc, MaxWorkers: 1,
			IdleAfter: cfg.Workers.IdleAfter, IdleAuto: cfg.Workers.IdleAuto, StartTimeout: cfg.Workers.StartTimeout}
	}
	// The file system keeps its context for every request, long after
	// startup.
	files, err := catalog.FS(context.WithoutCancel(ctx), cfg.Storage, cfg.Storage.Location)
	if err != nil {
		return nil, fmt.Errorf("distributed execution: open the warehouse: %w", err)
	}
	threads, err := eng.Threads(ctx)
	if err != nil {
		return nil, err
	}
	credentials, err := workerWriteCredentials(ctx, cfg, log)
	if err != nil {
		return nil, err
	}
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: source, Secret: cfg.Workers.Secret, Version: version, TempDir: cfg.DuckDB.TempDir,
		Warehouse: cfg.Storage.Root(), Threads: threads, Metrics: distributed.NewMetrics(reg),
		MemoryBytes: eng.MemoryLimit(ctx), Threshold: cfg.Workers.Threshold, Storage: distributed.NewStorage(files),
		LocalExchanges: cfg.Workers.Exchanges == "local", AutoExchanges: cfg.Workers.Exchanges == "auto",
		Pool: distributed.PoolOptions{
			Addresses: cfg.Workers.Addresses, Compute: compute, MaxWorkers: cfg.Workers.Max,
			IdleAfter: cfg.Workers.IdleAfter, IdleAuto: cfg.Workers.IdleAuto, StartTimeout: cfg.Workers.StartTimeout,
			Warm: cfg.Workers.Warm,
		},
		WorkerPrice: cfg.Workers.Price, Price: cfg.PricePerHour, Large: large, LargeCores: l.Cores, LargeMemory: l.Memory, LargePrice: l.Price,
		WorkerInstanceType: cfg.Workers.InstanceType, WorkerSpot: cfg.Workers.Spot, Region: cfg.Workers.Region,
		LargeInstanceType: l.InstanceType, LargeSpot: l.Spot,
		NoWorkerWrites: cfg.Workers.NoWrites, WriteCredentials: credentials,
	}, log)
	if err != nil {
		return nil, fmt.Errorf("distributed execution: %w", err)
	}
	log.Info("distributed execution is on", "workers", cfg.Workers.Addresses, "compute", cfg.Workers.Compute, "max_workers", cfg.Workers.Max,
		"large_worker", l.Address)
	return co, nil
}

// workerWriteCredentials returns what makes the credentials a task's attempt
// writes a table's files with, limited to its directory, with no right to
// delete, for an hour: nil where none can be made (GCS and Azure, and S3 without
// IGLOO_S3_STS_ROLE_ARN), and workers write with their own identity, as far
// as the deployment grants it; or none, and this replica writes the tables
// (see distributed.ErrWriteDenied).
func workerWriteCredentials(ctx context.Context, cfg config.Config, log *slog.Logger) (func(context.Context, string) (*distributed.WriteCredentials, error), error) {
	if cfg.Workers.NoWrites {
		log.Info("workers write no tables' files (IGLOO_WORKER_WRITES=off)")
		return nil, nil
	}
	v, err := icebergrest.NewS3Vendor(ctx, cfg.Storage, log)
	if err != nil {
		return nil, fmt.Errorf("distributed execution: credentials for workers' writes: %w", err)
	}
	if v == nil {
		log.Info("workers write tables' files with their own identity, which needs write access to directories named " + distributed.WriteDirMark +
			"* under tables' locations and no more: without it, this replica writes them (docs/distributed-execution.md)")
		return nil, nil
	}
	s3 := cfg.Storage.S3
	return func(ctx context.Context, dir string) (*distributed.WriteCredentials, error) {
		c, err := v.Vend(ctx, dir, icebergrest.AddFiles, "worker-write")
		if err != nil {
			return nil, err
		}
		return &distributed.WriteCredentials{KeyID: c.Config["s3.access-key-id"], Secret: c.Config["s3.secret-access-key"],
			SessionToken: c.Config["s3.session-token"], Region: s3.Region, Endpoint: s3.Endpoint, URLStyle: s3.URLStyle, UseSSL: s3.UseSSL,
			Expires: c.Expires}, nil
	}, nil
}

// footprints learns what statements held at once, by their shapes: the
// workload controller.
type footprints interface {
	Observe(shape string, footprint int64)
	ObserveUnfinished(shape string, held int64)
	ObserveForm(key string, lists bool, cpu time.Duration, footprint int64, ran time.Duration)
	ObserveCost(shape string, cpu time.Duration)
	ObserveTook(shape string, took time.Duration)
	ObserveEmpty(shape string)
	ObserveOnWorkers(key string)
}

// longUnmeasured is how long a run that ended early must have run, without
// a peak in the history, to be taken for one that needed all its memory.
const longUnmeasured = 10 * time.Second

// seedHistory reads the past week's statements from the query history,
// oldest last, and tells seed, if any, how long statements that could run on
// workers took, by their plan keys, so that auto mode keeps choosing the
// faster after a restart (see distributed.Run), and fp what statements that
// ran here held and took, by their shapes, so that heavy ones reserve memory
// from the first after it (see workload.Controller.Observe) and auto mode
// knows they ran here (see distributed.Footprints and LocalRuns), whatever
// ran them here: only statements planned for workers have plan keys. Runs
// that ended early, cancelled or out of memory, count too, at least what they
// were seen holding; one from before the history kept that, which ran a while, at
// least all the memory it ran with: it ran its next time alone, rather than
// beside others like it, all spilling, as runs cancelled at their timeout
// did when nothing learned from them. A run is taken only with the memory
// it ran with: a footprint only from this replica's memory limit, memory,
// or one within a quarter of it.
func seedHistory(ctx context.Context, seed func([]distributed.Run), fp footprints, memory int64, store audit.Store, log *slog.Logger) {
	const pageSize, most = 1000, 20000
	page := audit.Page{Since: time.Now().Add(-7 * 24 * time.Hour), Limit: pageSize}
	var rs []distributed.Run
	type footprint struct {
		shape      string
		held       int64
		unfinished bool
		// cpu is what a run it completed took; -1 for none.
		cpu time.Duration
		// empty says it read nothing (see workload.ReadNothing).
		empty bool
		// took is how long a run it completed took, past what it waited.
		took time.Duration
		// exact is its key with its literals (see workload.Controller.Alike).
		exact string
		// workers says it ran on workers.
		workers bool
	}
	var fs []footprint
	type form struct {
		key       string
		lists     bool
		cpu       time.Duration
		footprint int64
		ran       time.Duration
	}
	var forms []form
	for read := 0; read < most; read += pageSize {
		qs, err := store.Queries(ctx, audit.Viewer{All: true}, page)
		if err != nil {
			log.Warn("could not read past statements' runs", "err", err)
			break
		}
		for _, q := range qs {
			empty := q.Resources != nil && workload.ReadNothing(q.Resources.BytesRead, q.Resources.CPUTime)
			if q.FlattenKey != "" && q.Outcome == audit.OutcomeOK && q.Resources != nil && q.Workers == 0 && !empty {
				forms = append(forms, form{q.FlattenKey, q.FlattenLists, q.Resources.CPUTime,
					workload.FootprintOf(q.Resources.MemoryAllocated, q.Peak, q.Peak > 0), q.Duration - q.Queued})
			}
			switch {
			case q.PlanKey == "":
			case q.Outcome == audit.OutcomeOK:
				rs = append(rs, distributed.Run{Key: q.PlanKey, Workers: q.Workers, Large: distributed.OnLarge(q.Placement), Duration: q.Duration,
					Memory: q.Memory, Peak: q.Peak})
			case q.Workers > 0 && q.Outcome == audit.OutcomeError:
				// It failed on workers, as when one ran out of memory.
				rs = append(rs, distributed.Run{Key: q.PlanKey, Workers: q.Workers, Large: distributed.OnLarge(q.Placement), Duration: q.Duration,
					Memory: q.Memory, Failed: true, At: q.Start})
			case q.Workers == 0 && q.SQLState == "53200":
				// It ran out of memory on one machine.
				rs = append(rs, distributed.Run{Key: q.PlanKey, OutOfMemory: true, Memory: q.Memory})
			}
			if ran, memory, ok := distributed.GuardedRun(q.Placement); ok {
				// What came before the run on workers that its row is about,
				// as these are read newest first.
				rs = append(rs, distributed.Run{Key: q.PlanKey, Guarded: true, Duration: ran, Memory: memory})
			}
			if q.Shape != "" && q.Workers > 0 && q.Outcome == audit.OutcomeOK {
				fs = append(fs, footprint{shape: q.Shape, exact: q.Exact, workers: true})
				continue
			}
			if q.Shape == "" || q.Workers > 0 || !sameMemory(q.Memory, memory) {
				continue
			}
			early := q.Outcome == audit.OutcomeCanceled || q.SQLState == "53200"
			switch {
			case q.Outcome == audit.OutcomeOK && q.Resources != nil:
				fs = append(fs, footprint{q.Shape, workload.FootprintOf(q.Resources.MemoryAllocated, q.Peak, q.Peak > 0), false, q.Resources.CPUTime, empty,
					q.Duration - q.Queued, q.Exact, false})
			case early && q.Peak > 0:
				fs = append(fs, footprint{q.Shape, q.Peak, true, -1, false, 0, q.Exact, false})
			case early && q.Duration-q.Queued >= longUnmeasured:
				fs = append(fs, footprint{q.Shape, q.Memory, true, -1, false, 0, q.Exact, false})
			}
		}
		if len(qs) < pageSize {
			break
		}
		last := qs[len(qs)-1]
		page.AfterTime, page.AfterID = last.Start, last.ID
	}
	// Newest first: oldest first, so the last run weighs most.
	slices.Reverse(rs)
	slices.Reverse(fs)
	slices.Reverse(forms)
	for _, f := range forms {
		fp.ObserveForm(f.key, f.lists, f.cpu, f.footprint, f.ran)
	}
	if seed != nil {
		seed(rs)
	}
	for _, f := range fs {
		if f.workers {
			fp.ObserveOnWorkers(f.shape)
			fp.ObserveOnWorkers(f.exact)
			continue
		}
		if f.empty {
			fp.ObserveEmpty(f.exact)
			continue
		}
		for _, key := range []string{f.shape, f.exact} {
			if f.unfinished {
				fp.ObserveUnfinished(key, f.held)
			} else {
				fp.Observe(key, f.held)
			}
			if f.cpu >= 0 {
				fp.ObserveCost(key, f.cpu)
			}
		}
		fp.ObserveTook(f.shape, f.took)
	}
	log.Debug("read past statements' runs", "runs", len(rs), "footprints", len(fs))
}

// sameMemory reports whether a run's memory, ran, is this replica's,
// memory, within a quarter; a run that does not say is not.
func sameMemory(ran, memory int64) bool {
	return ran > 0 && memory > 0 && max(ran, memory) <= min(ran, memory)/4*5
}

// workerErasure is what erasure requests have the worker purge (see
// distributed's erasure.go). Its read cache is named, in the requests'
// ledger, by the worker's host name and the cache's identity, kept with
// the cache on disk; a cache on disk outlives the worker, unless on
// Kubernetes, where it is on the pod's own volume, which goes with the pod
// (the chart's emptyDir). Cut off from the coordinators for isolated, it
// empties its caches by itself.
func workerErasure(eng *engine.Engine, opts *distributed.WorkerOptions, isolated time.Duration) (distributed.WorkerErasure, error) {
	if opts.ID == "" {
		opts.ID, _ = os.Hostname()
	}
	cache, _, onDisk, err := eng.CacheIdentity()
	if err != nil {
		return distributed.WorkerErasure{}, fmt.Errorf("read the read cache's identity: %w", err)
	}
	through, save, err := eng.ErasureMark()
	if err != nil {
		return distributed.WorkerErasure{}, fmt.Errorf("read what the read cache was purged of: %w", err)
	}
	kept := onDisk != nil && os.Getenv("KUBERNETES_SERVICE_HOST") == ""
	e := distributed.WorkerErasure{Cache: erasure.WorkerCache(opts.ID, cache, kept, isolated), Through: through, Purge: eng.EmptyCache, Save: save,
		Isolated: isolated}
	if p := eng.Prefetcher(); p != nil {
		e.Hold = p.Hold
	}
	return e, nil
}

// workerCompute returns the driver that starts and stops workers, or nil
// when they always run.
func workerCompute(ctx context.Context, w config.Workers) (distributed.Compute, error) {
	switch w.Compute {
	case "kubernetes":
		k, err := gateway.NewKubernetes(w.K8sNamespace, w.K8sDeployment, w.Max)
		if err != nil {
			return nil, err
		}
		return kubernetesWorkers{k}, nil
	case "command":
		c := gateway.Command{StartCommand: w.StartCommand, StopCommand: w.StopCommand}
		return distributed.StartStop{Start: c.Start, Stop: c.Stop}, nil
	case "ec2":
		e, err := gateway.NewEC2(ctx, w.EC2InstanceIDs)
		if err != nil {
			return nil, err
		}
		return ec2Workers{e}, nil
	}
	return nil, nil
}

// kubernetesWorkers scales the workers' Deployment up as statements need
// workers, never below what another Igloo replica asked for, and down past
// workers retired while others work (see distributed.Shrinker), whose pods
// are not ready and go first.
type kubernetesWorkers struct{ k *gateway.Kubernetes }

func (k kubernetesWorkers) Scale(ctx context.Context, n int) error { return k.k.ScaleAtLeast(ctx, n) }

func (k kubernetesWorkers) Shrink(ctx context.Context, retired []distributed.RetiredWorker, running int) (bool, error) {
	return k.k.ShrinkTo(ctx, running-len(retired), running)
}

// ec2Workers starts as many of the workers' instances as statements need,
// stops them all when idle, and stops those of workers retired while
// others work (see distributed.Shrinker), as their instance metadata names
// them.
type ec2Workers struct{ e *gateway.EC2 }

func (e ec2Workers) Scale(ctx context.Context, n int) error {
	if n == 0 {
		return e.e.Stop(ctx)
	}
	return e.e.StartUpTo(ctx, n)
}

func (e ec2Workers) Shrink(ctx context.Context, retired []distributed.RetiredWorker, _ int) (bool, error) {
	var ids []string
	for _, r := range retired {
		if r.Instance == "" {
			return false, nil
		}
		ids = append(ids, r.Instance)
	}
	return true, e.e.StopInstances(ctx, ids)
}

// prefetchTasks reads ahead, as each task starts, the blocks of the files
// its tables' views read, until the task ends (see package prefetch).
func prefetchTasks(p *prefetch.Prefetcher) func(context.Context, []distributed.Scan) {
	return func(ctx context.Context, scans []distributed.Scan) {
		stop := p.Start(ctx, prefetchScans(scans))
		go func() {
			<-ctx.Done()
			stop()
		}()
	}
}

// prefetchScans are the planner's scans as the prefetcher reads them.
func prefetchScans(scans []distributed.Scan) []prefetch.Scan {
	out := make([]prefetch.Scan, 0, len(scans))
	for _, sc := range scans {
		ps := prefetch.Scan{Filtered: sc.Filtered, FieldIDs: make([]int32, len(sc.FieldIDs)), Files: make([]prefetch.File, len(sc.Files))}
		for i, id := range sc.FieldIDs {
			ps.FieldIDs[i] = int32(id)
		}
		for _, id := range sc.Filters {
			ps.Filters = append(ps.Filters, int32(id))
		}
		for i, f := range sc.Files {
			ps.Files[i] = prefetch.File{Path: f.Path, Size: f.Size}
		}
		out = append(out, ps)
	}
	return out
}
