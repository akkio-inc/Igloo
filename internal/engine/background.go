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
	"github.com/akkio-inc/igloo/internal/memlimit"
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
// using. Over a billion rows, dashboards' p95 beside the reports is now what it
// is alone, 8.1 s, against 74 s; the longest report takes 567 s beside dashboards
// that use all the cores they can have, where it took 436 s with them at a fifth
// of their statements, and 267 s to 294 s alone in either.
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
// With statements in line behind the background DuckDB's and none running in
// the first, the caches give way all the same (see WatchForeground).
//
// What the background DuckDB holds it gives up only as fast as its spill files
// take it, and what it cannot write out it keeps, while the first takes memory
// faster than that. So the first gets what the background DuckDB has let go of and
// a lead more, and the background DuckDB is told to let go of that much ahead of
// it, on a goroutine of its own, so that the first's limit follows what it lets go
// of as it goes. The lead is what the machine has to spare past memlimit.High,
// between a sixteenth and a quarter of the limit: on a machine with room to spare, a
// dashboard's statements have their memory at once, and on one without, the two
// keep to the limit. With the first free to take three quarters of the limit at
// once, the two together held 134% of it beside a burst of heavy statements, and
// the process 29.7 GiB of a machine's 30.8, until the kernel killed it. With an
// eighth, and the first's limit waiting on what the second wrote to a slow disk,
// dashboards' p95 beside a report went from 8.8 s to 23 s.
//
// Two DuckDBs hold more than their limits count, each its own metadata and what
// its allocator keeps: a report over a billion rows and dashboards beside it took
// the process 11 GB past what they held, against 2 GB for one DuckDB, and left the
// machine so little that the memory watch stopped the report. So what the limits
// share is less what the process holds past them, beyond backgroundOver of it.
const (
	// backgroundOver is how much past what the two hold the limit leaves room
	// for, as a divisor of it.
	backgroundOver = 16
	// backgroundLeast is the least of the limit they share, as a part of it
	// out of 4.
	backgroundLeast = 3
	// backgroundMinShare is the least of the limit the background DuckDB
	// keeps, as a divisor.
	backgroundMinShare = 4
	// backgroundLeadLeast and backgroundLeadMost bound how far past what the
	// background DuckDB has let go of the first may go while reading, as divisors
	// of the limit.
	backgroundLeadLeast = 16
	backgroundLeadMost  = 4
	// foregroundIdle is how long after the first's last read its caches
	// give way.
	foregroundIdle = 30 * time.Second
	// balanceEvery is how often the limits are weighed.
	balanceEvery = time.Second
	// backgroundIdleLimit is the background DuckDB's limit with nothing
	// running in it while the first reads, so what it read last gives the first
	// its room back.
	backgroundIdleLimit = 256 << 20
)

// ForegroundRead says a statement that reads tables is running in the first
// DuckDB, whose caches therefore keep their memory for a while.
func (e *Engine) ForegroundRead() {
	last := e.fgRead.Swap(time.Now().UnixNano())
	if time.Since(time.Unix(0, last)) >= foregroundIdle {
		e.balanceNow()
	}
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

// WatchForeground has held say whether statements wait in line with none running
// in the first DuckDB. They wait for the memory of the one in the second, and until
// it ends nothing will read what the first's caches keep: a dashboard's heavy
// statements, held to the memory those caches left them, spilled and took half as
// long again as in the first, while the statements behind them waited.
func (e *Engine) WatchForeground(held func() bool) {
	e.fgHeld.Store(&held)
}

// foregroundReading says whether the first DuckDB's caches keep their memory.
func (e *Engine) foregroundReading() bool {
	if time.Since(time.Unix(0, e.fgRead.Load())) >= foregroundIdle {
		return false
	}
	held := e.fgHeld.Load()
	return held == nil || !(*held)()
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
	reading := e.foregroundReading()
	// With nothing running in the background DuckDB, what it holds stays for its
	// next statement until the first reads: a session's reports one after another
	// read the same files, and one of 24 s took 50 s starting with nothing.
	if e.bgUsers.Load() > 0 || !reading {
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
		mainAll := mainHeld
		if !reading {
			if mainAll, err = e.held(ctx, queryAllMemory); err != nil {
				return
			}
		}
		// Up at once, and down slowly: the limits should not follow what
		// the allocator hands back and takes again.
		over := max(processRSS()-mainAll-bgHeld, e.bgOver.Load()-total/64, 0)
		e.bgOver.Store(over)
		room := roomFor(total, over)
		wantMain, wantBg = balancedLimits(room, leadOf(room, spareMemory()), mainHeld, bgHeld, reading)
	}
	applyLimit(ctx, e, wantMain)
	e.applyBackgroundLimit(ctx, bg, wantBg)
}

// spareMemory is what the machine has left past what memlimit.Adapt keeps free.
func spareMemory() int64 {
	left, ok := memlimit.Available()
	if !ok {
		return 0
	}
	return left - memlimit.High(processMemory())
}

// leadOf is how far past what the background DuckDB has let go of the first may go,
// of total, with spare to spare.
func leadOf(total, spare int64) int64 {
	return min(max(spare, total/backgroundLeadLeast), total/backgroundLeadMost)
}

// applyBackgroundLimit sets the background DuckDB's limit to want, as applyLimit
// does. DuckDB writes out what it lets go of before it answers, which may take a
// while, so one that lowers it runs apart, and balances again when it ends.
func (e *Engine) applyBackgroundLimit(ctx context.Context, bg *Engine, want int64) {
	have := bg.current.Load()
	if have <= 0 || want >= have {
		if !e.bgLowering.Load() {
			applyLimit(ctx, bg, want)
		}
		return
	}
	if have-want < have/64 || !e.bgLowering.CompareAndSwap(false, true) {
		return
	}
	e.bgLowers.Go(func() {
		defer e.bgLowering.Store(false)
		// Past the statement that called for it.
		_ = bg.setLimit(context.WithoutCancel(ctx), want)
		e.balanceNow()
	})
}

// roomFor is what the two DuckDBs' limits share of total, when the process holds
// over past what they hold.
func roomFor(total, over int64) int64 {
	return max(total-max(over-total/backgroundOver, 0), total/4*backgroundLeast)
}

// balancedLimits returns the two DuckDBs' limits, out of total, given what
// they hold now: mainHeld, with its caches if reading says the first is
// reading, and bgHeld, with its own. While the first reads, it may go lead past
// what the background DuckDB has let go of.
func balancedLimits(total, lead, mainHeld, bgHeld int64, reading bool) (mainLimit, bgLimit int64) {
	if reading {
		bgLimit = max(total-mainHeld-lead, total/backgroundMinShare)
		// So that the two keep to total, the quarter the background DuckDB
		// keeps is off the first's.
		return min(total-bgHeld+lead, total-min(bgHeld, total/backgroundMinShare)), bgLimit
	}
	bgLimit = max(total-mainHeld, total/backgroundMinShare)
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
