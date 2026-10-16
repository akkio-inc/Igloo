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
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/akkio-inc/igloo/internal/gateway"
)

// serveGateway runs the scale-to-zero gateway in front of Igloo.
func serveGateway() error {
	cfg, err := gateway.LoadConfig(os.LookupEnv)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(os.Getenv("IGLOO_LOG_LEVEL"))); err != nil && os.Getenv("IGLOO_LOG_LEVEL") != "" {
		return fmt.Errorf("IGLOO_LOG_LEVEL: %w", err)
	}
	var log *slog.Logger
	if os.Getenv("IGLOO_LOG_FORMAT") == "json" {
		log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	} else {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	compute, err := gateway.NewCompute(ctx, cfg)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	gw := gateway.New(gateway.Options{
		Backend:        cfg.Backend,
		ConsoleBackend: cfg.ConsoleBackend,
		ActivityURL:    cfg.ActivityURL,
		SuspendAfter:   cfg.SuspendAfter,
		ResumeTimeout:  cfg.ResumeTimeout,
		Compute:        compute,
		Metrics:        gateway.NewMetrics(reg),
		Log:            log,
	})

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	errc := make(chan error, 3)
	go func() { errc <- gw.Serve(ctx, ln) }()
	if cfg.ConsoleBackend != "" {
		consoleLn, err := net.Listen("tcp", cfg.ConsoleListenAddr)
		if err != nil {
			return err
		}
		go func() { errc <- gw.ServeConsole(ctx, consoleLn) }()
	}

	var httpSrv *http.Server
	if cfg.HTTPAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		httpSrv = &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("http: %w", err)
			}
		}()
	}
	log.Info("igloo gateway is ready", "sql", ln.Addr().String(), "backend", cfg.Backend,
		"console_backend", cfg.ConsoleBackend, "compute", cfg.Compute, "suspend_after", cfg.SuspendAfter,
		"activity", cfg.ActivityURL)

	select {
	case err = <-errc:
	case <-ctx.Done():
		err = <-errc
	}
	if httpSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	return err
}
