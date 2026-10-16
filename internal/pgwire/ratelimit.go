package pgwire

import (
	"net"
	"sync"
	"time"
)

// Failed logins are limited per user and per client address: after
// userFailures failures for one user name, or addrFailures from one
// address, within failureWindow, further logins for that name or from that
// address may not use a password until the oldest failure is failureWindow
// old. This bounds online password guessing beyond what the failed-login
// delay does, and password spraying across users from one address. Logins
// still running count too, so parallel connections do not get more
// guesses. A successful login clears the user's count.
//
// Access tokens still sign in while a user or address is blocked: they
// cannot be guessed, and a service account's pipelines must not stop
// because someone else got its name wrong.
//
// The counts are kept per replica. Unknown user names count like real ones,
// so the limit tells nobody which users exist. Behind a proxy that hides
// clients' addresses, the address limit counts all of the proxy's clients
// together.
const (
	failureWindow = 15 * time.Minute
	userFailures  = 10
	addrFailures  = 50
	// maxTracked bounds the names and addresses remembered.
	maxTracked = 100_000
)

// loginLimiter counts recent failed logins, and logins in progress.
type loginLimiter struct {
	mu      sync.Mutex
	now     func() time.Time
	users   map[string][]time.Time
	addrs   map[string][]time.Time
	running map[string]int // by "u:"+user and "a:"+address
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{now: time.Now, users: map[string][]time.Time{}, addrs: map[string][]time.Time{}, running: map[string]int{}}
}

// addrKey is the address's host, without the port, which changes with
// every connection.
func addrKey(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	if a, ok := addr.(*net.TCPAddr); ok {
		if a == nil {
			return ""
		}
		return a.IP.String()
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// begin starts a login for user from addr. When it may not use a password,
// wait says for how long, and done does nothing; otherwise the login counts
// as running until done is called, once it has succeeded or failed.
func (l *loginLimiter) begin(user string, addr net.Addr) (done func(), wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	key := addrKey(addr)
	wait = waitFor(l.users[user], l.running["u:"+user], userFailures, now)
	if key != "" {
		wait = max(wait, waitFor(l.addrs[key], l.running["a:"+key], addrFailures, now))
	}
	if wait > 0 {
		return func() {}, wait
	}
	keys := []string{"u:" + user}
	if key != "" {
		keys = append(keys, "a:"+key)
	}
	for _, k := range keys {
		l.running[k]++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			for _, k := range keys {
				if l.running[k]--; l.running[k] <= 0 {
					delete(l.running, k)
				}
			}
		})
	}, 0
}

// waitFor is how long until fewer than limit failures are recent, counting
// the logins running as failures to come.
func waitFor(failures []time.Time, running, limit int, now time.Time) time.Duration {
	switch n := recent(failures, now); {
	case n+running < limit:
		return 0
	case n < limit:
		// The logins running end within a second or so.
		return time.Second
	}
	// failures holds at most limit entries, oldest first.
	return failures[len(failures)-limit].Add(failureWindow).Sub(now)
}

// failed counts a failed login for user from addr. It reports whether this
// failure is the one that makes the user, or the address, reach its limit.
func (l *loginLimiter) failed(user string, addr net.Addr) (userBlocked, addrBlocked bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	userBlocked = record(l.users, user, userFailures, now)
	if key := addrKey(addr); key != "" {
		addrBlocked = record(l.addrs, key, addrFailures, now)
	}
	return userBlocked, addrBlocked
}

// succeeded clears user's failures.
func (l *loginLimiter) succeeded(user string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.users, user)
}

// record adds a failure at now to key's, keeping the last limit. It
// reports whether key now has limit recent failures, and had fewer before.
func record(m map[string][]time.Time, key string, limit int, now time.Time) bool {
	if _, ok := m[key]; !ok && len(m) >= maxTracked {
		prune(m, limit, now)
	}
	before := recent(m[key], now)
	failures := append(m[key], now)
	if len(failures) > limit {
		failures = failures[len(failures)-limit:]
	}
	m[key] = failures
	return before == limit-1
}

// recent counts the failures within failureWindow of now.
func recent(failures []time.Time, now time.Time) int {
	n := 0
	for _, t := range failures {
		if now.Sub(t) < failureWindow {
			n++
		}
	}
	return n
}

// prune makes room: it forgets entries without recent failures, then
// those that block nothing, and only if a flood of distinct names leaves
// no other way, every entry.
func prune(m map[string][]time.Time, limit int, now time.Time) {
	for k, failures := range m {
		if len(failures) == 0 || now.Sub(failures[len(failures)-1]) >= failureWindow {
			delete(m, k)
		}
	}
	if len(m) >= maxTracked {
		for k, failures := range m {
			if len(failures) < limit {
				delete(m, k)
			}
		}
	}
	if len(m) >= maxTracked {
		clear(m)
	}
}
