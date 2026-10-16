package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/akkio-inc/igloo/internal/activity"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/tasks"
)

// health answers liveness and readiness probes.
type health struct {
	engine   *engine.Engine
	users    auth.Store
	catalog  config.Catalog
	client   *http.Client
	activity *activity.Tracker
	// jobs says when scheduled work is next due.
	jobs     *tasks.Scheduler
	stopping atomic.Bool
	// gatewaySeen is when a scale-to-zero gateway last read /activity, in
	// Unix nanoseconds, for the console's compute page.
	gatewaySeen atomic.Int64
}

// gatewayAgent is the User-Agent of the scale-to-zero gateway's requests.
const gatewayAgent = "igloo-gateway"

func (h *health) handler(reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	// Liveness: the process is running and serving HTTP.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ok")
	})
	// Readiness: queries can run, users can log in, the catalog answers.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := h.ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// Activity: whether statements or maintenance are running, and when
	// scheduled work is next due, for the scale-to-zero gateway.
	mux.HandleFunc("GET /activity", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.UserAgent(), gatewayAgent) {
			h.gatewaySeen.Store(time.Now().UnixNano())
		}
		running, idle := h.activity.Snapshot()
		report := map[string]any{"running": running, "idle_seconds": idle.Seconds()}
		if due, ok := h.nextDue(r.Context()); ok {
			report["next_due"] = due.UTC().Format(time.RFC3339)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(report)
	})
	return mux
}

// nextDue is when the soonest task, pipe or materialized view is due, if
// any is scheduled and the store answers.
func (h *health) nextDue(ctx context.Context) (time.Time, bool) {
	if h.jobs == nil {
		return time.Time{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	due, ok, err := h.jobs.NextDue(ctx)
	return due, ok && err == nil
}

func (h *health) ready(ctx context.Context) error {
	if h.stopping.Load() {
		return fmt.Errorf("shutting down")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := h.engine.Ping(ctx); err != nil {
		return fmt.Errorf("engine: %w", err)
	}
	if err := h.users.Ping(ctx); err != nil {
		return fmt.Errorf("metadata database: %w", err)
	}
	if h.catalog.URI == "" {
		return nil
	}

	u := h.catalog.URI + "/v1/config?warehouse=" + url.QueryEscape(h.catalog.Warehouse)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("catalog: %s", resp.Status)
	}
	return nil
}
