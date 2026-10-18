package gateway

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestFlightConnectionWakesIgloo(t *testing.T) {
	// The second port of twoPorts stands in for Flight SQL.
	compute := newTwoPorts(t)
	sqlLn := listenFree(t)
	flightLn := listenFree(t)
	ctx, cancel := context.WithCancel(context.Background())
	gw := New(Options{
		Backend: compute.sql, FlightBackend: compute.console,
		SuspendAfter: time.Minute, ResumeTimeout: 5 * time.Second, Compute: compute,
	})
	done := make(chan error, 2)
	go func() { done <- gw.Serve(ctx, sqlLn) }()
	go func() { done <- gw.ServeFlight(ctx, flightLn) }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			if err := <-done; err != nil {
				t.Errorf("serve: %v", err)
			}
		}
	})

	conn, err := net.Dial("tcp", flightLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, "PRI * HTTP/2.0"); err != nil {
		t.Fatal(err)
	}
	if got, err := bufio.NewReader(conn).ReadString('\n'); err != nil || got != "console: PRI * HTTP/2.0\n" {
		t.Fatalf("flight got %q, %v", got, err)
	}
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
}

func TestFlightConfig(t *testing.T) {
	base := map[string]string{"IGLOO_GATEWAY_BACKEND": "igloo:5432", "IGLOO_GATEWAY_COMPUTE": "command",
		"IGLOO_GATEWAY_START_COMMAND": "true", "IGLOO_GATEWAY_STOP_COMMAND": "true"}
	lookup := func(extra map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			if v, ok := extra[k]; ok {
				return v, true
			}
			v, ok := base[k]
			return v, ok
		}
	}

	cfg, err := LoadConfig(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlightBackend != "" {
		t.Errorf("Flight SQL forwarding is on by default: %q", cfg.FlightBackend)
	}
	cfg, err = LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_FLIGHT_BACKEND": "igloo:31337"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FlightBackend != "igloo:31337" || cfg.FlightListenAddr != ":31337" {
		t.Errorf("flight settings: %q on %q", cfg.FlightBackend, cfg.FlightListenAddr)
	}
	if _, err := LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_FLIGHT_BACKEND": "igloo"})); err == nil {
		t.Error("a Flight SQL backend without a port was accepted")
	}
}
