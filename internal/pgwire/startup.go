package pgwire

import (
	"cmp"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/akkio-inc/igloo/internal/lockorder"
)

// A connection is held to limits until its client has signed in. It must
// sign in within the login timeout (IGLOO_LOGIN_TIMEOUT), counted from
// connecting through TLS, the startup message and the password exchange,
// or it is closed; that ends its login too, so a client that stops halfway
// holds no login of the failed-login limits (see ratelimit.go) for longer.
// And only so many connections may be signing in at once: as many as
// sessions (MaxConnections), but at least minStarting, and half of those
// from one public client address, so that connections that send nothing
// cannot keep everyone else from signing in. A loopback or private address
// is the scale-to-zero gateway's, a pooler's or a Kubernetes Service's as
// often as a client's, and all the clients behind it share it, so it
// counts only toward the limit of all. Further ones are refused at once, as
// PostgreSQL refuses clients past max_connections. Without these,
// connections that never signed in stayed open for good, outside
// MaxConnections.

// defaultLoginTimeout is the login timeout when none is set.
const defaultLoginTimeout = 30 * time.Second

// minStarting is the fewest connections that may be signing in at once, so
// that with few sessions allowed, a pool that opens all of them together
// from one address is not refused half of them.
const minStarting = 100

// startupLimits counts the connections signing in.
type startupLimits struct {
	timeout time.Duration
	// max is how many connections may be signing in at once, and perAddr
	// how many of them from one address; zero means no limit.
	max, perAddr int
	log          *slog.Logger

	mu     lockorder.Mutex
	open   int
	byAddr map[string]int
	// warned is when a refusal or a timeout was last logged.
	warned time.Time
}

func newStartupLimits(timeout time.Duration, maxConns int, log *slog.Logger) *startupLimits {
	l := &startupLimits{timeout: cmp.Or(timeout, defaultLoginTimeout), log: log, byAddr: map[string]int{}}
	if maxConns > 0 {
		l.max = max(maxConns, minStarting)
		l.perAddr = l.max / 2
	}
	return l
}

// listener wraps ln so that its connections are held to the limits.
func (l *startupLimits) listener(ln net.Listener) net.Listener { return startupListener{ln, l} }

type startupListener struct {
	net.Listener
	limits *startupLimits
}

func (ln startupListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return ln.limits.admit(c), nil
}

// admit counts c as signing in, until the login timeout. Past the limits,
// c is refused instead, on its first read: in its own goroutine, so that
// Accept goes on.
func (l *startupLimits) admit(c net.Conn) net.Conn {
	key := addrKey(c.RemoteAddr())
	perAddr := !sharedAddr(key)
	l.mu.Lock()
	if l.max > 0 && (l.open >= l.max || perAddr && l.byAddr[key] >= l.perAddr) {
		l.mu.Unlock()
		return &startupConn{Conn: c, refused: true, limits: l}
	}
	l.open++
	l.byAddr[key]++
	l.mu.Unlock()
	_ = c.SetDeadline(time.Now().Add(l.timeout))
	return &startupConn{Conn: c, limits: l, key: key, counted: true}
}

// sharedAddressSpace is 100.64.0.0/10 (RFC 6598), which some Kubernetes
// networks give pods.
var sharedAddressSpace = netip.MustParsePrefix("100.64.0.0/10")

// sharedAddr reports whether key, a client address as addrKey gives it,
// may be many clients' at once: on this machine or a private network, or
// not a network address at all.
func sharedAddr(key string) bool {
	ip, err := netip.ParseAddr(key)
	if err != nil {
		return true
	}
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || sharedAddressSpace.Contains(ip)
}

// warn logs msg, at most once a minute: connections that never sign in
// come in floods.
func (l *startupLimits) warn(msg string, args ...any) {
	l.mu.Lock()
	now := time.Now()
	quiet := now.Sub(l.warned) < time.Minute
	if !quiet {
		l.warned = now
	}
	l.mu.Unlock()
	if !quiet && l.log != nil {
		l.log.Warn(msg, args...)
	}
}

// startupConn is a connection whose client may not have signed in yet.
type startupConn struct {
	net.Conn
	limits *startupLimits
	key    string
	// refused is set for a connection past the limits, and counted for
	// one that counts against them until it is released: once it has
	// signed in or closed.
	refused, counted bool
	released         atomic.Bool
}

func (c *startupConn) Read(p []byte) (int, error) {
	if c.refused {
		c.limits.warn("connection refused: too many connections signing in", "addr", c.RemoteAddr(),
			"max", c.limits.max, "per_address", c.limits.perAddr)
		_ = c.Conn.SetDeadline(time.Now().Add(time.Second))
		_, _ = c.Conn.Write(fatalError("53300", "sorry, too many clients already"))
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	n, err := c.Conn.Read(p)
	if errors.Is(err, os.ErrDeadlineExceeded) && c.counted && !c.released.Load() {
		// The connection ends here, which is no error of Igloo's.
		c.limits.warn("closed a connection that did not sign in within the login timeout",
			"addr", c.RemoteAddr(), "timeout", c.limits.timeout)
		return n, io.EOF
	}
	return n, err
}

// signedIn lifts the deadline of a connection whose client has signed in,
// which no longer counts against the limits: it holds a session now.
func (c *startupConn) signedIn() {
	_ = c.Conn.SetDeadline(time.Time{})
	c.release()
}

// SyscallConn gives the connection's socket, to look at (see clientgone.go).
func (c *startupConn) SyscallConn() (syscall.RawConn, error) { return innerSocket(c.Conn) }

func (c *startupConn) Close() error {
	c.release()
	return c.Conn.Close()
}

func (c *startupConn) release() {
	if !c.counted || !c.released.CompareAndSwap(false, true) {
		return
	}
	l := c.limits
	l.mu.Lock()
	defer l.mu.Unlock()
	l.open--
	if l.byAddr[c.key]--; l.byAddr[c.key] <= 0 {
		delete(l.byAddr, c.key)
	}
}
