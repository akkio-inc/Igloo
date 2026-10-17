package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// Compute starts and stops workers: pods of a Kubernetes Deployment, or
// machines started and stopped by commands.
type Compute interface {
	// Scale asks for at least n workers to run; n = 0 stops them all. It
	// never stops workers otherwise, since another coordinator may be
	// using them.
	Scale(ctx context.Context, n int) error
}

// PoolOptions say where workers are and how to start them.
type PoolOptions struct {
	// Addresses are the workers' host:port. A host may resolve to several
	// workers, as a Kubernetes headless Service does.
	Addresses []string
	// Compute, if set, starts workers when a statement needs them and stops
	// them after IdleAfter without tasks. Without it workers are expected
	// to be running.
	Compute Compute
	// MaxWorkers caps the workers Compute starts.
	MaxWorkers int
	// IdleAfter is how long workers run without tasks before Compute stops
	// them.
	IdleAfter time.Duration
	// StartTimeout bounds how long a statement waits for workers to start.
	StartTimeout time.Duration
}

// pool tracks the workers.
type pool struct {
	opts    PoolOptions
	version string
	client  *http.Client
	log     *slog.Logger
	metrics *Metrics

	mu        sync.Mutex
	workers   []*worker
	refreshed time.Time
	// down are workers that failed recently, by address, until when.
	down map[string]time.Time
	// used is when this coordinator last ran a task.
	used time.Time
	// scaled is the number of workers last asked for, and when.
	scaled   int
	scaledAt time.Time
	// startTook is how long the last start took, to estimate the next.
	startTook time.Duration
}

// worker is a worker that answered the last check.
type worker struct {
	addr   string
	status workerStatus
}

// statusTimeout bounds a status check.
const statusTimeout = 3 * time.Second

func newPool(opts PoolOptions, t *transport, version string, metrics *Metrics, log *slog.Logger) *pool {
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 3 * time.Minute
	}
	if opts.IdleAfter == 0 {
		opts.IdleAfter = 10 * time.Minute
	}
	return &pool{
		opts:    opts,
		version: version,
		log:     log,
		metrics: metrics,
		client: &http.Client{Transport: &http.Transport{
			TLSClientConfig:     t.client,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     time.Minute,
			// A task's response starts only when its result is ready, which
			// may take a while. TCP keepalives, which the worker's kernel
			// answers however busy the worker is, tell a worker that is
			// working from one whose machine is gone, within half a minute.
			DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAliveConfig: net.KeepAliveConfig{
				Enable: true, Idle: 10 * time.Second, Interval: 5 * time.Second, Count: 3,
			}}).DialContext,
		}},
		down:      map[string]time.Time{},
		startTook: 30 * time.Second,
	}
}

// known returns the workers that answered the last check, checking now
// only if there has been none.
func (p *pool) known(ctx context.Context) []*worker {
	p.mu.Lock()
	ws, checked := p.workers, !p.refreshed.IsZero()
	p.mu.Unlock()
	if !checked {
		ws = p.refresh(ctx)
	}
	return p.usable(ws)
}

// ready returns the workers that answer, checking again if the last check
// is more than a couple of seconds old.
func (p *pool) ready(ctx context.Context) []*worker {
	p.mu.Lock()
	fresh := time.Since(p.refreshed) < 2*time.Second
	ws := p.workers
	p.mu.Unlock()
	if fresh {
		return p.usable(ws)
	}
	return p.usable(p.refresh(ctx))
}

// usable leaves out workers that failed recently.
func (p *pool) usable(ws []*worker) []*worker {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*worker
	for _, w := range ws {
		if until, ok := p.down[w.addr]; ok && time.Now().Before(until) {
			continue
		}
		out = append(out, w)
	}
	return out
}

// markDown keeps a worker out of new tasks for a while after it failed one.
func (p *pool) markDown(addr string) {
	p.mu.Lock()
	p.down[addr] = time.Now().Add(30 * time.Second)
	p.refreshed = time.Time{}
	p.mu.Unlock()
}

// refresh resolves the worker addresses and asks every worker for its
// status. Workers of another Igloo version are left out.
func (p *pool) refresh(ctx context.Context) []*worker {
	var addrs []string
	for _, a := range p.opts.Addresses {
		host, port, err := net.SplitHostPort(a)
		if err != nil {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, statusTimeout)
		ips, err := net.DefaultResolver.LookupHost(rctx, host)
		cancel()
		if err != nil {
			continue
		}
		for _, ip := range ips {
			addrs = append(addrs, net.JoinHostPort(ip, port))
		}
	}
	slices.Sort(addrs)
	addrs = slices.Compact(addrs)

	results := make([]*worker, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Go(func() {
			st, err := p.status(ctx, addr)
			if err != nil {
				p.log.Debug("worker did not answer", "addr", addr, "err", err)
				return
			}
			if st.Version != p.version {
				p.log.Warn("worker runs another version of Igloo; not using it", "addr", addr, "worker", st.Version, "coordinator", p.version)
				return
			}
			results[i] = &worker{addr: addr, status: st}
		})
	}
	wg.Wait()
	var ws []*worker
	for _, w := range results {
		if w != nil {
			ws = append(ws, w)
		}
	}
	p.mu.Lock()
	p.workers, p.refreshed = ws, time.Now()
	p.mu.Unlock()
	p.metrics.setWorkers(len(ws))
	return ws
}

func (p *pool) status(ctx context.Context, addr string) (workerStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	var st workerStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+statusPath, nil)
	if err != nil {
		return st, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status %s", resp.Status)
	}
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// canStart reports whether workers can be started on demand.
func (p *pool) canStart() bool { return p.opts.Compute != nil && p.opts.MaxWorkers > 0 }

// startEstimate is how long starting workers is expected to take.
func (p *pool) startEstimate() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.startTook
}

// ensure makes sure up to want workers run, starting them if it can, and
// returns the ones ready. It waits for a start only when wait is set,
// and then for at most StartTimeout.
func (p *pool) ensure(ctx context.Context, want int, wait bool) ([]*worker, error) {
	ws := p.ready(ctx)
	if len(ws) >= want || !p.canStart() {
		return ws, nil
	}
	want = min(want, p.opts.MaxWorkers)
	start := time.Now()
	if err := p.scale(ctx, want); err != nil {
		if len(ws) > 0 {
			return ws, nil
		}
		return nil, err
	}
	if !wait {
		return ws, nil
	}
	// The deadline bounds the wait, not the checks: a check cut short by
	// it would find no worker at all.
	deadline, cancel := context.WithTimeout(ctx, p.opts.StartTimeout)
	defer cancel()
	for {
		ws = p.usable(p.refresh(ctx))
		if len(ws) >= want {
			p.mu.Lock()
			p.startTook = time.Since(start)
			p.mu.Unlock()
			return ws, nil
		}
		select {
		case <-deadline.Done():
			if len(ws) > 0 {
				return ws, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("workers did not start in time")
		case <-time.After(time.Second):
		}
	}
}

// scale asks Compute for n workers, unless as many were asked for in the
// last half minute: something else, such as the scale-to-zero gateway, may
// have stopped them since.
func (p *pool) scale(ctx context.Context, n int) error {
	p.mu.Lock()
	if n <= p.scaled && time.Since(p.scaledAt) < 30*time.Second {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	err := p.opts.Compute.Scale(ctx, n)
	p.metrics.compute("start", err)
	if err != nil {
		p.log.Warn("could not start workers", "workers", n, "err", err)
		return fmt.Errorf("start workers: %w", err)
	}
	p.log.Info("started workers", "workers", n)
	p.mu.Lock()
	p.scaled, p.scaledAt = n, time.Now()
	p.mu.Unlock()
	return nil
}

// touch records that this coordinator used workers.
func (p *pool) touch() {
	p.mu.Lock()
	p.used = time.Now()
	p.mu.Unlock()
}

// run checks the workers every few seconds, so statements decide from a
// recent view of them without waiting, and stops idle workers until ctx is
// done: once this coordinator has not used them for IdleAfter and every
// worker reports as long without tasks, so workers another coordinator
// uses keep running.
func (p *pool) run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ws := p.refresh(ctx)
		if !p.canStart() {
			continue
		}
		p.mu.Lock()
		idle := time.Since(p.used) >= p.opts.IdleAfter
		p.mu.Unlock()
		if !idle {
			continue
		}
		busy := slices.ContainsFunc(ws, func(w *worker) bool {
			return w.status.Running > 0 || time.Duration(w.status.IdleSeconds*float64(time.Second)) < p.opts.IdleAfter
		})
		p.mu.Lock()
		scaled := p.scaled
		p.mu.Unlock()
		if busy || (len(ws) == 0 && scaled == 0) {
			continue
		}
		err := p.opts.Compute.Scale(ctx, 0)
		p.metrics.compute("stop", err)
		if err != nil {
			p.log.Warn("could not stop idle workers", "err", err)
			continue
		}
		p.log.Info("stopped idle workers", "idle", p.opts.IdleAfter)
		p.mu.Lock()
		p.scaled, p.workers, p.refreshed = 0, nil, time.Time{}
		p.mu.Unlock()
	}
}

// ParseAddresses splits a comma-separated list of worker addresses,
// adding the default port where one is missing.
func ParseAddresses(s string, defaultPort string) ([]string, error) {
	var out []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			a = net.JoinHostPort(a, defaultPort)
			if _, _, err := net.SplitHostPort(a); err != nil {
				return nil, fmt.Errorf("worker address %q: %w", a, err)
			}
		}
		out = append(out, a)
	}
	return out, nil
}

// StartStop adapts a compute driver that only starts and stops all
// workers, such as a group of virtual machines.
type StartStop struct {
	Start, Stop func(ctx context.Context) error
}

// Scale starts the workers for n > 0 and stops them for n = 0.
func (s StartStop) Scale(ctx context.Context, n int) error {
	if n > 0 {
		return s.Start(ctx)
	}
	return s.Stop(ctx)
}
