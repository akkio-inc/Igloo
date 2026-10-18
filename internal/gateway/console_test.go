package gateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// twoPorts is compute whose Igloo serves SQL and the web console on two
// ports. The console echoes lines back prefixed with "console: ", so tests
// can tell which port a connection reached.
type twoPorts struct {
	t                *testing.T
	sql, console     string
	mu               sync.Mutex
	listeners, conns []io.Closer
	starts, stops    int
}

func newTwoPorts(t *testing.T) *twoPorts {
	c := &twoPorts{t: t, sql: reserveAddr(t), console: reserveAddr(t)}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func (c *twoPorts) serve(addr, prefix string) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		c.t.Errorf("listen: %v", err)
		return
	}
	c.mu.Lock()
	c.listeners = append(c.listeners, ln)
	c.mu.Unlock()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			c.mu.Lock()
			c.conns = append(c.conns, conn)
			c.mu.Unlock()
			go func() {
				r := bufio.NewReader(conn)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := io.WriteString(conn, prefix+line); err != nil {
						return
					}
				}
			}()
		}
	}()
}

func (c *twoPorts) Start(context.Context) error {
	c.mu.Lock()
	c.starts++
	running := len(c.listeners) > 0
	c.mu.Unlock()
	if !running {
		// As in cmd/igloo, the console's port opens before the SQL port.
		go func() {
			time.Sleep(50 * time.Millisecond)
			c.serve(c.console, "console: ")
			c.serve(c.sql, "")
		}()
	}
	return nil
}

func (c *twoPorts) Stop(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stops++
	for _, l := range append(c.listeners, c.conns...) {
		_ = l.Close()
	}
	c.listeners, c.conns = nil, nil
	return nil
}

func (c *twoPorts) counts() (starts, stops int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts, c.stops
}

// startBoth runs a gateway that forwards SQL and the console, and returns
// the addresses clients connect to.
func startBoth(t *testing.T, opts Options) (sql, console string) {
	t.Helper()
	sqlLn := listenFree(t)
	consoleLn := listenFree(t)
	ctx, cancel := context.WithCancel(context.Background())
	gw := New(opts)
	done := make(chan error, 2)
	go func() { done <- gw.Serve(ctx, sqlLn) }()
	go func() { done <- gw.ServeConsole(ctx, consoleLn) }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			if err := <-done; err != nil {
				t.Errorf("serve: %v", err)
			}
		}
	})
	return sqlLn.Addr().String(), consoleLn.Addr().String()
}

func TestConsoleConnectionWakesIgloo(t *testing.T) {
	compute := newTwoPorts(t)
	sqlAddr, consoleAddr := startBoth(t, Options{
		Backend:        compute.sql,
		ConsoleBackend: compute.console,
		SuspendAfter:   300 * time.Millisecond,
		ResumeTimeout:  5 * time.Second,
		Compute:        compute,
	})

	// A browser connects to the console while Igloo is stopped: the
	// gateway starts it and the connection reaches the console's port.
	conn, err := net.Dial("tcp", consoleAddr)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(conn)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, "GET / HTTP/1.1"); err != nil {
		t.Fatal(err)
	}
	if got, err := r.ReadString('\n'); err != nil || got != "console: GET / HTTP/1.1\n" {
		t.Fatalf("console got %q, %v", got, err)
	}
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}

	// SQL still goes to the SQL port, without another start.
	sqlConn, err := net.Dial("tcp", sqlAddr)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, sqlConn, bufio.NewReader(sqlConn), "SELECT 1")
	_ = sqlConn.Close()
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d after a SQL connection, want 1", starts)
	}

	// Console traffic keeps Igloo up; once it stops, Igloo is stopped and
	// the browser's connection closed.
	for range 5 {
		time.Sleep(100 * time.Millisecond)
		if _, err := fmt.Fprintln(conn, "still here"); err != nil {
			t.Fatal(err)
		}
		if got, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(got, "console: ") {
			t.Fatalf("console got %q, %v", got, err)
		}
	}
	if _, stops := compute.counts(); stops != 0 {
		t.Fatal("stopped while the console was in use")
	}
	waitFor(t, "an idle stop", func() bool { _, stops := compute.counts(); return stops == 1 })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := r.ReadString('\n'); err == nil {
		t.Error("the console connection survived the stop")
	}

	// The next console connection starts Igloo again.
	again, err := net.Dial("tcp", consoleAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close() //nolint:errcheck
	_ = again.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(again, "hello"); err != nil {
		t.Fatal(err)
	}
	if got, err := bufio.NewReader(again).ReadString('\n'); err != nil || got != "console: hello\n" {
		t.Fatalf("after restart, console got %q, %v", got, err)
	}
	if starts, _ := compute.counts(); starts != 2 {
		t.Fatalf("starts = %d, want 2", starts)
	}
}

func TestIcebergConnectionWakesIgloo(t *testing.T) {
	// The console's echo port stands in for the Iceberg REST port: the
	// gateway forwards bytes, whatever they are.
	compute := newTwoPorts(t)
	icebergLn := listenFree(t)
	sqlLn := listenFree(t)
	ctx, cancel := context.WithCancel(context.Background())
	gw := New(Options{Backend: compute.sql, IcebergBackend: compute.console,
		SuspendAfter: time.Minute, ResumeTimeout: 5 * time.Second, Compute: compute})
	done := make(chan error, 2)
	go func() { done <- gw.Serve(ctx, sqlLn) }()
	go func() { done <- gw.ServeIceberg(ctx, icebergLn) }()
	defer func() {
		cancel()
		for range 2 {
			if err := <-done; err != nil {
				t.Errorf("serve: %v", err)
			}
		}
	}()

	conn, err := net.Dial("tcp", icebergLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, "GET /v1/config HTTP/1.1"); err != nil {
		t.Fatal(err)
	}
	if got, err := bufio.NewReader(conn).ReadString('\n'); err != nil || got != "console: GET /v1/config HTTP/1.1\n" {
		t.Fatalf("got %q, %v", got, err)
	}
	if starts, _ := compute.counts(); starts != 1 {
		t.Fatalf("starts = %d, want 1", starts)
	}
}

func TestConsoleConfig(t *testing.T) {
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
	if cfg.ConsoleBackend != "" {
		t.Errorf("console forwarding is on by default: %q", cfg.ConsoleBackend)
	}

	cfg, err = LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_CONSOLE_BACKEND": "igloo:8443"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsoleBackend != "igloo:8443" || cfg.ConsoleListenAddr != ":8443" {
		t.Errorf("console settings: %q on %q", cfg.ConsoleBackend, cfg.ConsoleListenAddr)
	}

	if _, err := LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_CONSOLE_BACKEND": "igloo"})); err == nil {
		t.Error("a console backend without a port was accepted")
	}

	if cfg.IcebergBackend != "" {
		t.Errorf("Iceberg forwarding is on by default: %q", cfg.IcebergBackend)
	}
	cfg, err = LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_ICEBERG_BACKEND": "igloo:8181"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IcebergBackend != "igloo:8181" || cfg.IcebergListenAddr != ":8181" {
		t.Errorf("Iceberg settings: %q on %q", cfg.IcebergBackend, cfg.IcebergListenAddr)
	}
	if _, err := LoadConfig(lookup(map[string]string{"IGLOO_GATEWAY_ICEBERG_BACKEND": "igloo"})); err == nil {
		t.Error("an Iceberg backend without a port was accepted")
	}
}
