package pgwire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"syscall"
	"time"
)

// A session's goroutine runs its statement, and reads the client's connection
// again only once the statement has ended, so a client that goes away while one
// runs tells nobody. At a hundred times the data, a report whose client was killed
// went on from 302 GB of spill files to 355 GB in two minutes, and on until Igloo was
// restarted, holding the memory that statements behind it waited for. So the server
// looks at the sockets of the sessions running a statement every clientCheckEvery,
// without reading them, as PostgreSQL's client_connection_check_interval does, and
// cancels the statement of one that has closed, as a cancel request does: wherever
// it runs, it runs under that context (see runCancelable and cancelable). Flight SQL
// and the console need none of this, as the context of a gRPC stream or an HTTP
// request ends with its client.
//
// A client that closes only its side of the connection, to read on, looks the same.
// No PostgreSQL client does, and a proxy passes on its client's closing the same way.

// clientCheckEvery is how often the sockets of sessions running a statement are
// looked at: a system call each.
const clientCheckEvery = time.Second

// clientSocket returns the socket of c, a connection the listener accepted, or nil
// if it has none to look at.
func clientSocket(c net.Conn) syscall.RawConn {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return nil
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return nil
	}
	return raw
}

// innerSocket is SyscallConn for a connection that wraps inner.
func innerSocket(inner net.Conn) (syscall.RawConn, error) {
	if sc, ok := inner.(syscall.Conn); ok {
		return sc.SyscallConn()
	}
	return nil, errors.ErrUnsupported
}

// watchClients cancels the statements of clients that went away until Shutdown.
func (s *Server) watchClients() {
	t := time.NewTicker(clientCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-s.clientWatchStop:
			return
		case <-t.C:
			s.cancelForGoneClients()
		}
	}
}

// cancelForGoneClients cancels the statement of each session whose client went
// away. One still running at the next look is canceled again, as is the next of a
// query of several.
func (s *Server) cancelForGoneClients() {
	r := s.sessions
	r.mu.Lock()
	all := make([]*session, 0, len(r.byPID))
	for _, sess := range r.byPID {
		if sess.socket != nil {
			all = append(all, sess)
		}
	}
	r.mu.Unlock()
	for _, sess := range all {
		sess.mu.Lock()
		running := sess.running != nil
		sess.mu.Unlock()
		if !running || !socketClosed(sess.socket) {
			continue
		}
		sess.mu.Lock()
		cancel, first := sess.running, !sess.clientGone
		sess.clientGone = true
		text, user, pid, started := sess.statement, sess.user.Name, sess.pid, sess.started
		sess.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if first {
			sum := sha256.Sum256([]byte(text))
			s.log.Info("a client went away while its statement ran; canceled the statement", "pid", pid, "user", user,
				"statement_sha256", hex.EncodeToString(sum[:8]), "running_for", time.Since(started).Round(time.Millisecond))
		}
	}
}
