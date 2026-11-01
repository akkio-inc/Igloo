package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/config"
)

// A second DuckDB for heavy statements. DuckDB runs every statement of a
// database on one pool of threads, with nothing to say one statement matters
// more than another or may use fewer of them: its threads setting is the
// whole database's. So beside one analyst's long reports, on a machine of 12
// cores, a dashboard's statement that used 6 to 15 of them alone got 3,
// whatever its size. Over 200 million rows dashboards' p95 rose from 1.7 s to
// 5.3 s; over a billion from 10 s to 245 s, and scheduler_process_partial,
// which has threads take other statements' tasks more often, brought that only
// to 190 s while it slowed a report alone by a fifth. They were waiting for
// nothing but cores: statements that only compute slowed as much, and those
// that take no DuckDB thread not at all; nothing queued in Igloo.
//
// The operating system can prefer one thread to another, though not one
// statement. So a second DuckDB in this process, its threads all at a lower
// priority (backgroundNice), runs what is known to be heavy (see package
// pgwire's background.go). Its threads run on whatever the first DuckDB's
// leave idle and give way at once, so a heavy statement alone takes what it
// takes in the first, and beside dashboards gets only the cores they are not
// using. A second replica at a low CPU weight, in the same measurements,
// brought dashboards' p95 to within 1.5 times of idle while the heavy
// statements ran 1.7 times as long, as the dashboards were using over half of
// the cores.
//
// It reads the same catalog and read cache, but holds only what a query of
// the catalog's tables needs: Igloo's functions over its own state are not
// defined in it, so a statement that calls them fails to bind there and runs
// in the first. It is opened at the first heavy statement.

// backgroundNice is the nice value of the background DuckDB's threads: when
// they compete for a core with threads at the default, they get about a
// tenth of it, so heavy statements still go on under a replica its other
// statements keep busy.
const backgroundNice = 10

const (
	// backgroundOpenLimit bounds opening it: the statement that wanted it
	// runs in the first DuckDB rather than wait for a catalog that is down.
	backgroundOpenLimit = 30 * time.Second
	// backgroundRetry is how long Background waits after failing to open it.
	backgroundRetry = time.Minute
)

// ErrNoBackground says the platform cannot run threads at a lower priority.
var ErrNoBackground = errors.New("this platform has no per-thread priorities")

// Background returns the second DuckDB for heavy statements, opening it the
// first time (see above). Its catalog is the same, and so are its limits.
func (e *Engine) Background(ctx context.Context) (*Engine, error) {
	if e.isBackground || e.catalog == "" {
		return nil, ErrNoBackground
	}
	e.bgMu.Lock()
	defer e.bgMu.Unlock()
	if e.bg != nil {
		return e.bg, nil
	}
	if e.bgErr != nil && time.Since(e.bgAt) < backgroundRetry {
		return nil, e.bgErr
	}
	// Past the statement that wanted it, which may be canceled meanwhile.
	openCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), backgroundOpenLimit)
	defer cancel()
	bg, err := openBackground(openCtx, e.cfg, e.log)
	if err != nil {
		e.bgErr, e.bgAt = err, time.Now()
		if !errors.Is(err, ErrNoBackground) {
			e.log.Warn("could not open the DuckDB for heavy statements; they run in the first", "err", err)
		}
		return nil, err
	}
	e.bg = bg
	e.bgStop = make(chan struct{})
	go e.balanceMemory(bg, e.bgStop)
	e.log.Info("opened the DuckDB for heavy statements", "nice", backgroundNice)
	return bg, nil
}

// openBackground opens it on a thread of its own at the lower priority,
// which the threads DuckDB starts as it opens inherit. The thread goes with
// the goroutine, which never unlocks it: without privileges a thread's
// priority can only go down.
func openBackground(ctx context.Context, cfg config.Config, log *slog.Logger) (*Engine, error) {
	if cfg.DuckDB.TempDir != "" {
		// A database's spill files are its own: it deletes the directory's
		// as it starts and as it closes.
		cfg.DuckDB.TempDir = filepath.Join(cfg.DuckDB.TempDir, "background")
	}
	// In memory: all it keeps of its own are the local copies of views.
	cfg.DuckDB.Path = ""
	type opened struct {
		e   *Engine
		err error
	}
	done := make(chan opened, 1)
	go func() {
		runtime.LockOSThread()
		if err := lowerThreadPriority(backgroundNice); err != nil {
			done <- opened{err: fmt.Errorf("%w: %v", ErrNoBackground, err)}
			return
		}
		e, err := Open(ctx, cfg, log, background())
		done <- opened{e, err}
	}()
	r := <-done
	return r.e, r.err
}

// Memory of the two. Together they keep to the first's limit, which memlimit
// sizes for the process. A heavy statement alone may use all of it, so the
// first's caches give way to it while the first runs nothing. While the first
// is reading, it keeps what it has, caches included, up to three quarters of
// the limit: beside a report that took its room, a dashboard's statements
// over a billion rows read their files again, 12 GB each, and took 60 s, not
// 8. The background DuckDB then has what the first does not hold, down to a
// quarter of the limit, and spills: what it gives up is its own to give up.
const (
	// backgroundMinShare is the least of the limit the background DuckDB
	// keeps, as a divisor.
	backgroundMinShare = 4
	// foregroundIdle is how long after the first's last read its caches
	// give way.
	foregroundIdle = 30 * time.Second
	// balanceEvery is how often the limits are weighed.
	balanceEvery = time.Second
	// backgroundIdleLimit is the background DuckDB's limit with nothing
	// running in it, so what it read last gives the first its room back.
	backgroundIdleLimit = 256 << 20
)

// ForegroundRead says a statement that reads tables is running in the first
// DuckDB, whose caches therefore keep their memory for a while.
func (e *Engine) ForegroundRead() {
	e.fgRead.Store(time.Now().UnixNano())
}

// UsingBackground says a statement runs in the background DuckDB until the
// function it returns is called, and gives it its room before it starts.
func (e *Engine) UsingBackground(ctx context.Context) func() {
	e.bgUsers.Add(1)
	e.balance(ctx)
	return func() {
		e.bgUsers.Add(-1)
		e.balanceNow()
	}
}

func (e *Engine) balanceNow() {
	select {
	case e.bgWake <- struct{}{}:
	default:
	}
}

// balanceMemory weighs the two limits every balanceEvery until stop.
func (e *Engine) balanceMemory(bg *Engine, stop <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-stop
		cancel()
	}()
	t := time.NewTicker(balanceEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-e.bgWake:
		}
		e.balance(ctx)
	}
}

// balance sets the two limits for what their statements hold now.
func (e *Engine) balance(ctx context.Context) {
	e.bgMu.Lock()
	bg := e.bg
	e.bgMu.Unlock()
	if bg == nil {
		return
	}
	e.balanceMu.Lock()
	defer e.balanceMu.Unlock()
	total := e.limit.Load()
	if total <= 0 {
		if total = e.MemoryLimit(ctx); total <= 0 {
			return
		}
		e.limit.CompareAndSwap(0, total)
	}
	wantMain, wantBg := total, int64(backgroundIdleLimit)
	if e.bgUsers.Load() > 0 {
		reading := time.Since(time.Unix(0, e.fgRead.Load())) < foregroundIdle
		query := queryMemory
		if reading {
			query = queryAllMemory
		}
		mainHeld, err := e.held(ctx, query)
		if err != nil {
			return
		}
		bgHeld, err := bg.held(ctx, queryAllMemory)
		if err != nil {
			return
		}
		wantMain, wantBg = balancedLimits(total, mainHeld, bgHeld, reading)
	}
	applyLimit(ctx, e, wantMain)
	applyLimit(ctx, bg, wantBg)
}

// balancedLimits returns the two DuckDBs' limits, out of total, given what
// they hold now: mainHeld, with its caches if reading says the first is
// reading, and bgHeld, with its own.
func balancedLimits(total, mainHeld, bgHeld int64, reading bool) (mainLimit, bgLimit int64) {
	bgLimit = max(total-mainHeld, total/backgroundMinShare)
	if reading {
		// So that the two keep to total, the quarter the background DuckDB
		// keeps is off the first's.
		return total - min(bgHeld, total/backgroundMinShare), bgLimit
	}
	return max(total-min(bgHeld, bgLimit), total/backgroundMinShare), bgLimit
}

// applyLimit sets e's limit to want, if it moved by more than a 64th.
// One it cannot free as much for yet stays.
func applyLimit(ctx context.Context, e *Engine, want int64) {
	if have := e.current.Load(); have > 0 && abs64(want-have) < max(have, want)/64 {
		return
	}
	_ = e.setLimit(ctx, want)
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// queryAllMemory reads what DuckDB holds, caches included.
const queryAllMemory = `SELECT coalesce(sum(memory_usage_bytes), 0)::VARCHAR FROM system.main.duckdb_memory()`

// held reads what DuckDB holds, by query.
func (e *Engine) held(ctx context.Context, query string) (int64, error) {
	c, err := e.connector.Connect(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Close() //nolint:errcheck
	v, err := queryString(ctx, c.(*duckdb.Conn), query)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}
