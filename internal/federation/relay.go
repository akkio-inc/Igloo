package federation

import (
	"net"
	"sync"
	"time"
)

// relay is one DuckDB connection through a proxy, and the connection to
// the remote database it is relayed to, when there is one.
//
// DuckDB's extensions keep connections in pools and hand one out without
// checking it first, so a connection whose remote side went away would
// fail the next statement sent on it. When the remote side goes away while
// the session is quiet (no statement pending, no transaction open there),
// or is retired for new settings, only the remote side is dropped: DuckDB
// keeps its connection, and the session signs in again when it next needs
// the server, with the settings of the moment.
type relay struct {
	client net.Conn
	// timeout is how long the remote database may leave a statement
	// unanswered; zero means no limit.
	timeout time.Duration
	// quiet reports whether the session has no transaction open on the
	// remote database. It is set before the first sign-in and called with
	// mu held.
	quiet func() bool

	mu sync.Mutex
	// upstream is the connection to the remote database, while there is
	// one.
	upstream net.Conn
	// pending counts statements the remote database has not finished
	// answering.
	pending int
	// retired asks the relay to drop its remote side once the session is
	// quiet.
	retired bool
	// dropped is set when the remote side was dropped while quiet, until
	// the session takes note (see takeDropped).
	dropped bool
	closed  bool
}

// newRelay relays client; upstream is set once there is one.
func newRelay(client net.Conn, timeout time.Duration) *relay {
	if timeout > 0 {
		timeout += silenceGrace
	}
	return &relay{client: client, timeout: timeout}
}

// setUpstream gives the relay its connection to the remote database.
func (r *relay) setUpstream(up net.Conn) {
	r.mu.Lock()
	r.upstream, r.dropped, r.retired = up, false, false
	closed := r.closed
	r.mu.Unlock()
	if closed {
		_ = up.Close()
	}
}

// idle reports whether the remote database owes DuckDB no answer.
func (r *relay) idle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pending == 0
}

// begin notes that a statement was sent, starting the clock on the remote
// database's answer.
func (r *relay) begin() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending++
	r.deadline()
}

// progress notes that the remote database sent part of an answer, and end
// that it finished one.
func (r *relay) progress() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadline()
}

func (r *relay) end() {
	r.mu.Lock()
	if r.pending > 0 {
		r.pending--
	}
	r.deadline()
	up := r.dropNow()
	r.mu.Unlock()
	if up != nil {
		_ = up.Close()
	}
}

// dropNow detaches a retired relay's remote side if the session is quiet,
// returning it to be closed. r.mu is held.
func (r *relay) dropNow() net.Conn {
	if !r.retired || r.pending > 0 || r.upstream == nil || r.quiet == nil || !r.quiet() {
		return nil
	}
	up := r.upstream
	r.upstream, r.dropped, r.retired = nil, true, false
	return up
}

// deadline sets how long the next read from the remote database may take:
// unlimited while idle. r.mu is held.
func (r *relay) deadline() {
	if r.timeout <= 0 || r.upstream == nil {
		return
	}
	var t time.Time
	if r.pending > 0 {
		t = time.Now().Add(r.timeout)
	}
	_ = r.upstream.SetReadDeadline(t)
}

// retire drops the remote side, now if the session is quiet or else once
// it is, so the next sign-in uses new settings.
func (r *relay) retire() {
	r.mu.Lock()
	r.retired = true
	up := r.dropNow()
	r.mu.Unlock()
	if up != nil {
		_ = up.Close()
	}
}

// upstreamLost handles the remote side failing (or being closed by
// retire): if the session was quiet, DuckDB's side lives on and lost
// returns true; otherwise the whole relay is closed.
func (r *relay) upstreamLost(up net.Conn) (survived bool) {
	r.mu.Lock()
	switch {
	case r.closed:
	case r.upstream != up:
		// Already dropped by retire or end.
		survived = true
	case r.pending == 0 && r.quiet != nil && r.quiet():
		r.upstream, r.dropped, r.retired = nil, true, false
		survived = true
	}
	r.mu.Unlock()
	_ = up.Close()
	if !survived {
		r.close()
	}
	return survived
}

// takeDropped reports, once, that the remote side was dropped.
func (r *relay) takeDropped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.dropped
	r.dropped = false
	return d
}

func (r *relay) close() {
	r.mu.Lock()
	already, up := r.closed, r.upstream
	r.closed = true
	r.mu.Unlock()
	if !already {
		_ = r.client.Close()
		if up != nil {
			_ = up.Close()
		}
	}
}
