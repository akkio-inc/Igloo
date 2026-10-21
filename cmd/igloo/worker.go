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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/distributed"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/gateway"
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
	limitMemory(memory, log)
	cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(eng.Metrics()...)
	locations := append([]string{cfg.Storage.Root()}, cfg.Storage.AllowedLocations...)
	w, err := distributed.NewWorker(distributed.WorkerOptions{
		Engine: eng, Secret: cfg.Workers.Secret, Version: version, TempDir: cfg.DuckDB.TempDir,
		Locations: locations, Threads: threads, MemoryBytes: memory, Metrics: distributed.NewWorkerMetrics(reg),
	}, log)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.Workers.ListenAddr)
	if err != nil {
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- w.Serve(ln) }()

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

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	stopping.Store(true)
	log.Info("shutting down")
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	return w.Shutdown(shutdownCtx)
}

// newCoordinator sets up distributed execution on the server, when
// IGLOO_WORKERS names workers.
func newCoordinator(ctx context.Context, cfg config.Config, source distributed.Source, eng *engine.Engine, reg prometheus.Registerer, log *slog.Logger) (*distributed.Coordinator, error) {
	compute, err := workerCompute(ctx, cfg.Workers)
	if err != nil {
		return nil, err
	}
	threads, err := eng.Threads(ctx)
	if err != nil {
		return nil, err
	}
	co, err := distributed.NewCoordinator(distributed.Options{
		Source: source, Secret: cfg.Workers.Secret, Version: version, TempDir: cfg.DuckDB.TempDir,
		Warehouse: cfg.Storage.Root(), Threads: threads, Metrics: distributed.NewMetrics(reg),
		Pool: distributed.PoolOptions{
			Addresses: cfg.Workers.Addresses, Compute: compute, MaxWorkers: cfg.Workers.Max,
			IdleAfter: cfg.Workers.IdleAfter, StartTimeout: cfg.Workers.StartTimeout,
		},
	}, log)
	if err != nil {
		return nil, fmt.Errorf("distributed execution: %w", err)
	}
	log.Info("distributed execution is on", "workers", cfg.Workers.Addresses, "compute", cfg.Workers.Compute, "max_workers", cfg.Workers.Max)
	return co, nil
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
		return distributed.StartStop{Start: e.Start, Stop: e.Stop}, nil
	}
	return nil, nil
}

// kubernetesWorkers scales the workers' Deployment up as statements need
// workers, never below what another Igloo replica asked for.
type kubernetesWorkers struct{ k *gateway.Kubernetes }

func (k kubernetesWorkers) Scale(ctx context.Context, n int) error { return k.k.ScaleAtLeast(ctx, n) }
