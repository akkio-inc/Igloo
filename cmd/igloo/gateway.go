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
	log := logTo(os.Getenv("IGLOO_LOG_FORMAT"), level)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	compute, err := gateway.NewCompute(ctx, cfg)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), logDropped())
	gw := gateway.New(gateway.Options{
		Backend:          cfg.Backend,
		ConsoleBackend:   cfg.ConsoleBackend,
		FlightBackend:    cfg.FlightBackend,
		IcebergBackend:   cfg.IcebergBackend,
		ActivityURL:      cfg.ActivityURL,
		SuspendAfter:     cfg.SuspendAfter,
		ResumeTimeout:    cfg.ResumeTimeout,
		FirstByteTimeout: cfg.FirstByteTimeout,
		MaxWaiting:       cfg.MaxWaiting,
		Compute:          compute,
		Metrics:          gateway.NewMetrics(reg),
		Log:              log,
	})

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	errc := make(chan error, 5)
	go func() { errc <- gw.Serve(ctx, ln) }()
	if cfg.ConsoleBackend != "" {
		consoleLn, err := net.Listen("tcp", cfg.ConsoleListenAddr)
		if err != nil {
			return err
		}
		go func() { errc <- gw.ServeConsole(ctx, consoleLn) }()
	}
	if cfg.FlightBackend != "" {
		flightLn, err := net.Listen("tcp", cfg.FlightListenAddr)
		if err != nil {
			return err
		}
		go func() { errc <- gw.ServeFlight(ctx, flightLn) }()
	}
	if cfg.IcebergBackend != "" {
		icebergLn, err := net.Listen("tcp", cfg.IcebergListenAddr)
		if err != nil {
			return err
		}
		go func() { errc <- gw.ServeIceberg(ctx, icebergLn) }()
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
		"console_backend", cfg.ConsoleBackend, "flight_backend", cfg.FlightBackend, "iceberg_backend", cfg.IcebergBackend,
		"compute", cfg.Compute,
		"suspend_after", cfg.SuspendAfter, "activity", cfg.ActivityURL)

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
