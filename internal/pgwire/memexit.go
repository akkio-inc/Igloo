package pgwire

import (
	"crypto/sha256"
	"encoding/hex"
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
