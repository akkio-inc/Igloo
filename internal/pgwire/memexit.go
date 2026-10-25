package pgwire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"runtime/debug"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/memlimit"
)

// Giving up on a statement that does not stop.
//
// The memory watch cancels the statement that has run longest when the
// process runs low on memory (ShedMemory). A statement in a step of DuckDB
// that does not check for interruptions goes on, holding and taking
// memory: on 2026-10-24 one ran 19 minutes past its cancellation until the
// machine had 25 MiB left and spent its time paging, the kernel's
// out-of-memory killer never acted, and the machine stopped answering. So
// a statement the watch canceled that is still running shedStuckAfter
// later, while memory is still low, is interrupted again; and if memory
// then falls below memlimit.Critical, Igloo gives up on it: it fails every
// other statement in flight with errServerExiting (SQLSTATE 57P01), logs
// what it knows of the one that did not stop, and exits with
// MemoryExitCode, for its orchestration to start it again.
//
// A canceled statement can also stop while DuckDB's work for it does not:
// its own thread returns the error while DuckDB's other threads are still
// in the step that does not check, and go on taking memory that no
// statement Igloo could cancel holds. So when memory is below
// memlimit.Critical, nothing is running, and the watch canceled a statement
// within orphanWindow, Igloo first frees what it can (Go's free heap, and
// DuckDB's caches through Options.FreeMemory) and gives the memory
// shedStuckAfter to come back; if it is still critical then, with still
// nothing running, Igloo exits the same way, naming the statement it
// canceled last (orphanedWork).

// shedStuckAfter is how long a statement the memory watch canceled may go
// on before the watch interrupts it again, and gives up on it once memory
// is critical. Tests shorten it (Options.shedStuckAfter).
const shedStuckAfter = 10 * time.Second

// MemoryExitCode is Igloo's exit status when it gives up on a statement
// that did not stop (EX_TEMPFAIL: start it again).
const MemoryExitCode = 75

// exitGrace is how long the statements Igloo fails as it exits have to
// answer their clients.
const exitGrace = 3 * time.Second

// orphanWindow is how recently the watch must have canceled a statement for
// memory that is critical with nothing running to be put down to its work;
// orphanGap is the longest the watch goes between calls while memory stays
// low (memlimit's settle, and some), past which a critical spell is a new
// one.
const (
	orphanWindow = 5 * time.Minute
	orphanGap    = 30 * time.Second
)

// orphans is what ShedMemory knows of the statements it canceled, for when
// memory is critical with nothing running (see the top of this file).
type orphans struct {
	mu sync.Mutex
	// last is the statement the watch canceled last.
	last struct {
		pid       int32
		user      string
		statement string
		at        time.Time
	}
	// since is when memory was first seen critical with nothing running,
	// zero when it is not; seen when it last was.
	since, seen time.Time
}

// canceled records the statement the watch just canceled.
func (o *orphans) canceled(pid int32, user, statement string, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.last.pid, o.last.user, o.last.statement, o.last.at = pid, user, statement, at
	o.since = time.Time{}
}

// reset ends a critical spell with nothing running.
func (o *orphans) reset() {
	o.mu.Lock()
	o.since = time.Time{}
	o.mu.Unlock()
}

// orphanedWork is ShedMemory with no statement running: below
// memlimit.Critical, soon after the watch canceled one, it frees what it
// can the first time, and exits if memory is still critical
// shedStuckAfter later (see the top of this file). It reports whether it
// acted.
func (s *Server) orphanedWork(available int64, all []*session) bool {
	o := &s.orphans
	o.mu.Lock()
	now := time.Now()
	last := o.last
	if s.memoryCritical == 0 || available >= s.memoryCritical || last.at.IsZero() || now.Sub(last.at) > orphanWindow {
		o.since = time.Time{}
		o.mu.Unlock()
		s.log.Warn("memory is running out, and no statement is running to cancel", "available", memlimit.Format(available))
		return false
	}
	if o.since.IsZero() || now.Sub(o.seen) > orphanGap {
		o.since, o.seen = now, now
		o.mu.Unlock()
		s.log.Warn("memory is nearly gone and no statement is running: freeing what Igloo and DuckDB can, in case a canceled statement's work goes on",
			"available", memlimit.Format(available), "canceled_pid", last.pid, "canceled_for", now.Sub(last.at).Round(time.Millisecond))
		debug.FreeOSMemory()
		if s.freeMemory != nil {
			ctx, cancel := context.WithTimeout(context.Background(), exitGrace)
			s.freeMemory(ctx)
			cancel()
		}
		return true
	}
	o.seen = now
	since := o.since
	o.mu.Unlock()
	if now.Sub(since) < s.shedStuckAfter {
		return false
	}
	sum := sha256.Sum256([]byte(last.statement))
	s.metrics.memoryExit()
	s.log.Error("memory is nearly gone with no statement running: DuckDB's work for a statement canceled for running out of memory goes on without it; Igloo exits to be started again",
		"canceled_pid", last.pid, "canceled_user", last.user, "statement_sha256", hex.EncodeToString(sum[:8]),
		"canceled_for", now.Sub(last.at).Round(time.Millisecond), "critical_for", now.Sub(since).Round(time.Millisecond),
		"available", memlimit.Format(available), "exit_code", MemoryExitCode)
	s.failAndExit(nil, all)
	return true
}

// exitForMemory fails the statements in flight other than stuck, logs
// stuck, and exits (see the top of this file).
func (s *Server) exitForMemory(stuck *session, available int64, all []*session) {
	stuck.mu.Lock()
	text, user, pid := stuck.statement, stuck.user.Name, stuck.pid
	started, shedAt := stuck.started, stuck.shedAt
	stuck.mu.Unlock()
	sum := sha256.Sum256([]byte(text))
	percent, rows, total := stuck.conn.Progress()
	s.metrics.memoryExit()
	s.log.Error("a statement canceled for running out of memory did not stop, and memory is nearly gone: Igloo exits to be started again",
		"pid", pid, "user", user, "statement_sha256", hex.EncodeToString(sum[:8]),
		"running_for", time.Since(started).Round(time.Millisecond), "canceled_for", time.Since(shedAt).Round(time.Millisecond),
		"progress_percent", percent, "rows_processed", rows, "rows_expected", total,
		"available", memlimit.Format(available), "exit_code", MemoryExitCode)
	s.failAndExit(stuck, all)
}

// failAndExit fails the statements in flight other than stuck with
// errServerExiting, gives them exitGrace to answer, and exits with
// MemoryExitCode.
func (s *Server) failAndExit(stuck *session, all []*session) {
	var others []*session
	for _, sess := range all {
		if sess == stuck {
			continue
		}
		sess.mu.Lock()
		if sess.running != nil {
			sess.exiting = true
			sess.running()
			others = append(others, sess)
		}
		sess.mu.Unlock()
	}
	for deadline := time.Now().Add(exitGrace); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		left := 0
		for _, sess := range others {
			sess.mu.Lock()
			if sess.running != nil {
				left++
			}
			sess.mu.Unlock()
		}
		if left == 0 {
			// Their errors are on their way to the clients.
			time.Sleep(100 * time.Millisecond)
			break
		}
	}
	s.exitProcess(MemoryExitCode)
}
