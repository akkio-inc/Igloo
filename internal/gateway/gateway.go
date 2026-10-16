// Package gateway lets Igloo scale to zero. The gateway is a small,
// always-on process that SQL clients connect to in Igloo's place. When a
// client connects while Igloo is stopped, the gateway starts Igloo's compute
// and holds the connection until Igloo accepts it; from then on it copies
// bytes in both directions, so TLS and authentication stay end to end
// between the client and Igloo. After a period without client traffic or
// running work, it stops the compute again.
//
// Stopping closes open sessions, as a database restart would; clients
// reconnect, and their next connection starts the compute.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Compute starts and stops the machines or pods Igloo runs on.
type Compute interface {
	// Start asks for the compute to run. It may return before Igloo is
	// ready; the gateway then waits for Igloo's SQL port to accept
	// connections. Starting compute that is already running must succeed.
	Start(ctx context.Context) error
	// Stop asks for the compute to stop. Stopping compute that is already
	// stopped must succeed.
	Stop(ctx context.Context) error
}

// Options configure a Gateway.
type Options struct {
	// Backend is Igloo's SQL address, host:port.
	Backend string
	// ActivityURL is Igloo's /activity endpoint. Every address its host
	// resolves to is asked, so a headless Kubernetes Service covers all
	// replicas. Empty means the gateway judges idleness by traffic alone,
	// and may stop Igloo during a long query that sends nothing.
	ActivityURL string
	// SuspendAfter is how long Igloo must be idle before it is stopped.
	SuspendAfter time.Duration
	// ResumeTimeout bounds how long a client waits for Igloo to start.
	ResumeTimeout time.Duration
	Compute       Compute
	// Metrics may be nil.
	Metrics *Metrics
	Log     *slog.Logger
}

type state int

const (
	// stateUnknown: the gateway has not started or stopped the compute yet,
	// or the last start failed.
	stateUnknown state = iota
	stateStarting
	stateRunning
	// stateStopped also covers stopping; Compute.Start waits that out.
	stateStopped
)

// wake is one start of the compute, shared by every client that arrives
// while it runs.
type wake struct {
	done chan struct{}
	err  error
}

// Gateway forwards client connections to Igloo, starting and stopping its
// compute as needed.
type Gateway struct {
	opts   Options
	dialer net.Dialer
	client *http.Client

	// op serializes Compute calls, so a start waits for a stop to finish.
	op sync.Mutex

	// traffic is when a client last connected or sent or received a
	// byte, in Unix nanoseconds.
	traffic atomic.Int64

	mu      sync.Mutex
	state   state
	waking  *wake
	clients map[net.Conn]struct{}
}

// New returns a gateway; call Serve to accept connections.
func New(opts Options) *Gateway {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	g := &Gateway{
		opts:    opts,
		dialer:  net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second},
		client:  &http.Client{Timeout: 3 * time.Second},
		clients: make(map[net.Conn]struct{}),
	}
	g.touch()
	return g
}

// Serve accepts client connections on ln, and stops idle compute, until ctx
// is done. It closes ln and every open connection before returning.
func (g *Gateway) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g.suspendWhenIdle(ctx)
	}()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	defer func() {
		g.closeClients()
		wg.Wait()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.handle(ctx, conn)
		}()
	}
}

func (g *Gateway) handle(ctx context.Context, client net.Conn) {
	g.track(client, true)
	defer g.track(client, false)
	defer client.Close() //nolint:errcheck

	backend, err := g.connect(ctx)
	if err != nil {
		g.opts.Log.Warn("could not reach igloo", "client", client.RemoteAddr().String(), "err", err)
		return
	}
	defer backend.Close() //nolint:errcheck

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, touchReader{r: src, g: g})
		// Either side finishing ends the session.
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}
	go pipe(backend, client)
	go pipe(client, backend)
	<-done
	<-done
}

// connect returns a connection to Igloo, starting its compute first if
// needed.
func (g *Gateway) connect(ctx context.Context) (net.Conn, error) {
	g.mu.Lock()
	st := g.state
	g.mu.Unlock()
	if st == stateRunning || st == stateUnknown {
		conn, err := g.dialer.DialContext(ctx, "tcp", g.opts.Backend)
		if err == nil {
			g.setState(stateRunning)
			return conn, nil
		}
		if st == stateRunning {
			// It went away without the gateway stopping it.
			g.mu.Lock()
			if g.state == stateRunning {
				g.state = stateUnknown
			}
			g.mu.Unlock()
		}
	}
	if err := g.wake(ctx); err != nil {
		return nil, err
	}
	return g.dialer.DialContext(ctx, "tcp", g.opts.Backend)
}

// wake starts the compute, or joins a start already under way, and returns
// once Igloo accepts connections.
func (g *Gateway) wake(ctx context.Context) error {
	g.mu.Lock()
	if g.state == stateRunning {
		// A start finished since this client looked.
		g.mu.Unlock()
		return nil
	}
	w := g.waking
	if w == nil {
		w = &wake{done: make(chan struct{})}
		g.waking = w
		go g.start(w)
	}
	g.mu.Unlock()

	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Gateway) start(w *wake) {
	g.op.Lock()
	defer g.op.Unlock()
	g.setState(stateStarting)
	g.opts.Log.Info("starting igloo")

	began := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), g.opts.ResumeTimeout)
	defer cancel()
	err := g.opts.Compute.Start(ctx)
	if err == nil {
		err = g.waitReady(ctx)
	}
	elapsed := time.Since(began)

	g.mu.Lock()
	if err == nil {
		g.state = stateRunning
		g.touch()
	} else {
		g.state = stateUnknown
	}
	g.waking = nil
	g.mu.Unlock()

	g.opts.Metrics.setRunning(err == nil)
	g.opts.Metrics.resumed(err == nil, elapsed)
	if err != nil {
		err = fmt.Errorf("start igloo: %w", err)
		g.opts.Log.Error("could not start igloo", "after", elapsed.Round(time.Millisecond), "err", err)
	} else {
		g.opts.Log.Info("igloo is ready", "after", elapsed.Round(time.Millisecond))
	}
	w.err = err
	close(w.done)
}

// waitReady polls Igloo's SQL port until it accepts a connection. Igloo
// opens it only once it can serve queries.
func (g *Gateway) waitReady(ctx context.Context) error {
	for {
		conn, err := g.dialer.DialContext(ctx, "tcp", g.opts.Backend)
		if err == nil {
			return conn.Close()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("igloo did not accept connections: %w", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// suspendWhenIdle stops the compute after SuspendAfter without client
// traffic or running work.
func (g *Gateway) suspendWhenIdle(ctx context.Context) {
	interval := min(g.opts.SuspendAfter/4, 10*time.Second)
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := g.suspendIfIdle(ctx); err != nil {
			g.opts.Log.Error("could not stop igloo", "err", err)
		}
	}
}

func (g *Gateway) suspendIfIdle(ctx context.Context) error {
	g.mu.Lock()
	st, quiet := g.state, g.quiet()
	g.mu.Unlock()
	if st == stateStopped || st == stateStarting || quiet < g.opts.SuspendAfter {
		return nil
	}
	if g.opts.ActivityURL != "" {
		busy, err := g.busy(ctx)
		if err != nil {
			g.opts.Log.Debug("could not read igloo activity", "err", err)
		}
		if busy {
			return nil
		}
	}

	g.op.Lock()
	defer g.op.Unlock()
	g.mu.Lock()
	// A client may have connected, or a start begun, while activity was
	// read.
	if g.state == stateStarting || g.waking != nil || g.quiet() < g.opts.SuspendAfter {
		g.mu.Unlock()
		return nil
	}
	g.state = stateStopped
	g.mu.Unlock()
	g.opts.Metrics.setRunning(false)

	g.opts.Log.Info("stopping idle igloo", "idle", quiet.Round(time.Second))
	g.closeClients()
	stopCtx, cancel := context.WithTimeout(ctx, g.opts.ResumeTimeout)
	defer cancel()
	if err := g.opts.Compute.Stop(stopCtx); err != nil {
		g.setState(stateUnknown)
		return err
	}
	// Stopping takes a while, and the platform may still report the
	// compute as running. Hold off starts until Igloo has gone, so a client
	// that arrives now waits for a fresh start instead of reaching an Igloo
	// that is shutting down.
	if err := g.waitDown(stopCtx); err != nil {
		g.opts.Log.Warn("igloo still accepts connections after stopping", "err", err)
	}
	g.opts.Metrics.suspended()
	return nil
}

// waitDown polls Igloo's SQL port until it refuses connections.
func (g *Gateway) waitDown(ctx context.Context) error {
	for {
		conn, err := g.dialer.DialContext(ctx, "tcp", g.opts.Backend)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		}
		_ = conn.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// activityReport is the body of Igloo's GET /activity.
type activityReport struct {
	Running     int64   `json:"running"`
	IdleSeconds float64 `json:"idle_seconds"`
}

// busy asks every Igloo replica behind ActivityURL whether it is working
// or worked within SuspendAfter. A replica that cannot be asked is taken to
// be idle: it is down, or it is not serving clients.
func (g *Gateway) busy(ctx context.Context) (bool, error) {
	u, err := url.Parse(g.opts.ActivityURL)
	if err != nil {
		return false, err
	}
	addrs, err := net.DefaultResolver.LookupHost(ctx, u.Hostname())
	if err != nil {
		return false, err
	}
	var errs []error
	for _, addr := range addrs {
		target := *u
		target.Host = net.JoinHostPort(addr, u.Port())
		if u.Port() == "" {
			target.Host = addr
		}
		report, err := g.activity(ctx, target.String())
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if report.Running > 0 || report.IdleSeconds < g.opts.SuspendAfter.Seconds() {
			return true, nil
		}
	}
	return false, errors.Join(errs...)
}

func (g *Gateway) activity(ctx context.Context, u string) (activityReport, error) {
	var report activityReport
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return report, err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return report, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return report, fmt.Errorf("%s: %s", u, resp.Status)
	}
	return report, json.NewDecoder(resp.Body).Decode(&report)
}

func (g *Gateway) setState(s state) {
	g.mu.Lock()
	g.state = s
	g.mu.Unlock()
	g.opts.Metrics.setRunning(s == stateRunning)
}

// track adds or removes a client connection, which counts as traffic.
// Closing a client's connection also ends its connection to Igloo.
func (g *Gateway) track(client net.Conn, open bool) {
	g.touch()
	g.mu.Lock()
	defer g.mu.Unlock()
	if open {
		g.clients[client] = struct{}{}
	} else {
		delete(g.clients, client)
	}
	g.opts.Metrics.connections(len(g.clients), open)
}

func (g *Gateway) touch() { g.traffic.Store(time.Now().UnixNano()) }

// quiet is how long it has been since the last client traffic.
func (g *Gateway) quiet() time.Duration { return time.Since(time.Unix(0, g.traffic.Load())) }

func (g *Gateway) closeClients() {
	g.mu.Lock()
	conns := make([]net.Conn, 0, len(g.clients))
	for c := range g.clients {
		conns = append(conns, c)
	}
	g.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// touchReader records traffic each time it reads.
type touchReader struct {
	r io.Reader
	g *Gateway
}

func (t touchReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.g.touch()
	}
	return n, err
}
