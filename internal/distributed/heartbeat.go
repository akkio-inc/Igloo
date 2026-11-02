package distributed

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// A worker whose machine loses power closes no socket, and one whose Go threads all
// wait goes on taking what is sent: a request to it waits for good. So while a task
// runs on a worker, or a reader fetches exchange files from one, whoever waits asks
// for its status every taskCheckEvery, and takes it for lost once it has missed
// taskCheckMisses answers in a row, each after taskCheckTimeout, about 7 seconds on. A
// worker busy with tasks answers at once: of 280 answers from workers filling their
// memory, on a load of 56, the slowest took 100 ms, and with four threads a core, on a
// load of 103, 1.1 s. Powered off, one of 16 workers took 69 s to find before: a reader's
// minute of silence, and the check that followed.
//
// A worker busy past answering is not lost, though: its machine's kernel still takes a
// connection, which a machine without power does not, nor one whose process is gone.
// With each check the worker is also dialed, and a missed answer counts towards taking
// it for lost in seconds only when the dial fails too. One that takes the connection
// but answers nothing, busy or hung, is taken for lost only after heartbeatSilence
// without an answer, far longer than a busy worker takes.
//
// Everyone who waits on a worker shares its heartbeat. A statement of many tasks asks
// a worker once a second, not once for each. Missed answers do not count while nothing
// says it is the worker that went silent: while the process itself ran behind, past a
// check's timeout, or no worker it asks answered since, for up to heartbeatPatience.

// taskCheckEvery is how often a worker someone waits on is asked for its status,
// taskCheckTimeout how long each answer may take, and taskCheckMisses how many it may
// miss in a row before it is taken for lost; variables for tests.
var (
	taskCheckEvery   = time.Second
	taskCheckTimeout = 2 * time.Second
	taskCheckMisses  = 3
)

// heartbeatPatience bounds how long missed answers do not count because no worker
// answered; a variable for tests.
var heartbeatPatience = 30 * time.Second

// heartbeatSilence is how long a worker whose machine takes connections may answer no
// status before it is taken for lost; a variable for tests.
var heartbeatSilence = time.Minute

// reachWorker dials the worker at addr, and says whether its machine took the
// connection; a variable for tests, which take a worker's power away.
var reachWorker = func(ctx context.Context, addr string) error {
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return c.Close()
}

// behindBy is how far past a check's timeout the process must be to take itself to
// have run behind, rather than the worker to have been silent.
const behindBy = time.Second

// heartbeats watches the workers someone waits on, by asking with check.
type heartbeats struct {
	check func(ctx context.Context, addr string) error
	// onLost, if set, is told of each worker taken for lost.
	onLost func(addr string, err error)

	// answered is when any worker last answered, in Unix nanoseconds.
	answered atomic.Int64

	mu      sync.Mutex
	watches map[string]*heartbeat
	// lost is when each worker was last taken for lost.
	lost map[string]time.Time
}

// heartbeat is one worker's, while someone waits on it.
type heartbeat struct {
	refs   int
	stop   context.CancelFunc
	gone   chan struct{}
	reason error
}

func newHeartbeats(check func(ctx context.Context, addr string) error, onLost func(addr string, err error)) *heartbeats {
	return &heartbeats{check: check, onLost: onLost, watches: map[string]*heartbeat{}, lost: map[string]time.Time{}}
}

// watch starts or joins the heartbeat of the worker at addr: lost is closed if it is
// taken for lost, and why then says what it missed. release ends the caller's part.
func (h *heartbeats) watch(addr string) (lost <-chan struct{}, why func() error, release func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.watches[addr]
	if b == nil {
		ctx, stop := context.WithCancel(context.Background())
		b = &heartbeat{stop: stop, gone: make(chan struct{})}
		h.watches[addr] = b
		go h.run(ctx, addr, b)
	}
	b.refs++
	var once sync.Once
	return b.gone, func() error { return b.reason }, func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			if b.refs--; b.refs == 0 {
				b.stop()
				if h.watches[addr] == b {
					delete(h.watches, addr)
				}
			}
		})
	}
}

// lostWithin reports whether the worker at addr was taken for lost within d.
func (h *heartbeats) lostWithin(addr string, d time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	at, ok := h.lost[addr]
	return ok && time.Since(at) < d
}

// others reports whether workers other than addr are being watched.
func (h *heartbeats) others(addr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for a := range h.watches {
		if a != addr {
			return true
		}
	}
	return false
}

func (h *heartbeats) run(ctx context.Context, addr string, b *heartbeat) {
	t := time.NewTicker(taskCheckEvery)
	defer t.Stop()
	misses := 0
	// silent is when the worker last began to miss answers.
	var silent time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		began := time.Now()
		cctx, cancel := context.WithTimeout(ctx, taskCheckTimeout)
		// The check, and once it is slow to answer or failed, a dial of the worker's
		// machine beside it.
		checked := make(chan error, 1)
		go func() { checked <- h.check(cctx, addr) }()
		var reached chan error
		dial := func() {
			reached = make(chan error, 1)
			go func() { reached <- reachWorker(cctx, addr) }()
		}
		var err, unreached error
		select {
		case err = <-checked:
		case <-time.After(taskCheckTimeout / 4):
			dial()
			err = <-checked
		}
		if err != nil {
			if reached == nil {
				dial()
			}
			unreached = <-reached
		}
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			h.answered.Store(time.Now().UnixNano())
			misses, silent = 0, time.Time{}
			continue
		}
		if silent.IsZero() {
			silent = began
		}
		if unreached == nil {
			// Its machine takes connections: busy, or its process hung.
			misses = 0
			if time.Since(silent) < heartbeatSilence {
				continue
			}
			err = fmt.Errorf("took connections but answered no status for %s: %w", time.Since(silent).Round(time.Second), err)
		} else if !h.missed(addr, began, silent) {
			continue
		} else if misses++; misses < taskCheckMisses {
			continue
		}
		h.mu.Lock()
		b.reason = err
		h.lost[addr] = time.Now()
		if h.watches[addr] == b {
			// Whoever waits on it next finds out for themselves.
			delete(h.watches, addr)
		}
		h.mu.Unlock()
		close(b.gone)
		if h.onLost != nil {
			h.onLost(addr, err)
		}
		return
	}
}

// missed reports whether a check of the worker at addr that began at began, and that
// neither it nor its machine answered, counts against it: not while this process ran
// behind, or nobody answered, for up to heartbeatPatience from silent.
func (h *heartbeats) missed(addr string, began, silent time.Time) bool {
	patient := time.Since(silent) < heartbeatPatience
	if behind := time.Since(began) > taskCheckTimeout+behindBy; behind && patient {
		// This process ran behind, and may have read the answer late.
		return false
	}
	// Nobody answered: it may be this end that went silent.
	return !patient || h.answered.Load() >= began.UnixNano() || !h.others(addr)
}

// statusAnswered asks the worker at addr for its status with client, and says whether
// it answered.
func statusAnswered(ctx context.Context, client *http.Client, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+statusPath, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}
