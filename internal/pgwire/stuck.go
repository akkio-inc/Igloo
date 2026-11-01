package pgwire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
)

// A statement canceled by its client, past its statement timeout, or by
// the memory watch stops when DuckDB next checks for an interruption,
// which Igloo asks for again and again (see duckdb-go's
// runWithCtxInterrupt). Some of DuckDB's steps check for none until they
// end: at a hundred times the data, a report run on the replica after its
// workers failed went on after its client's cancel request, the client
// waiting in it, until Igloo was restarted. So once a
// statement's run has ended and it is still running stuckGrace later, its
// client's connection is closed, which lets the client go; the session
// ends once DuckDB stops the statement, and holds the client's connection
// to the limit of sessions until then.

// stuckGrace is how long a canceled statement may take to stop before its
// client's connection is closed. Tests shorten it (Options.stuckGrace).
const stuckGrace = 30 * time.Second

// stuckWatch is the server's part of watching for statements that do not
// stop once canceled.
type stuckWatch struct {
	grace time.Duration
	log   *slog.Logger
}

// watchStuck watches the statement that runs under ctx, which
// runCancelable gave it: if ctx ends, as a cancel request or the statement
// timeout end it, and the statement is still running the watch's grace
// later, it closes the client's connection. A run again under another
// context (see rearm) is watched in its place. stop ends the watch, once
// the statement returned.
func (sess *session) watchStuck(ctx context.Context) (stop func()) {
	w := sess.stuck
	if w == nil {
		return func() {}
	}
	sess.mu.Lock()
	sess.watched = ctx
	sess.mu.Unlock()
	done := make(chan struct{})
	go func() {
		for {
			sess.mu.Lock()
			watched := sess.watched
			sess.mu.Unlock()
			select {
			case <-done:
				return
			case <-watched.Done():
			}
			ended := time.Now()
			select {
			case <-done:
				return
			case <-time.After(w.grace):
			}
			sess.mu.Lock()
			again := sess.watched != watched
			text, user, pid, started := sess.statement, sess.user.Name, sess.pid, sess.started
			sess.mu.Unlock()
			if again {
				// It runs again, as it may after running out of memory.
				continue
			}
			sum := sha256.Sum256([]byte(text))
			percent, rows, total := sess.conn.Progress()
			w.log.Warn("a statement canceled or past its timeout has not stopped; closed its client's connection, and the session "+
				"ends once DuckDB stops the statement", "pid", pid, "user", user, "statement_sha256", hex.EncodeToString(sum[:8]),
				"running_for", time.Since(started).Round(time.Millisecond), "canceled_for", time.Since(ended).Round(time.Millisecond),
				"progress_percent", percent, "rows_processed", rows, "rows_expected", total)
			if c := wire.AcceptedConn(ctx); c != nil {
				_ = c.Close()
			}
			return
		}
	}()
	return func() { close(done) }
}
