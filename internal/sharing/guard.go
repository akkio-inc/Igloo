package sharing

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Providers decide which URLs Igloo fetches data files from, so a hostile
// or compromised provider could point Igloo at services only it can reach.
// The data goes to DuckDB's Parquet reader, which shows nothing of a
// response that is not Parquet, but a request alone can do harm, such as
// one to a cloud metadata service. So file downloads never connect to
// loopback, link-local (where cloud metadata services live), unspecified or
// multicast addresses, checked on the address actually dialed, which also
// defeats DNS rebinding. Private networks stay reachable: object stores
// there are common. A provider's own endpoint, which an admin chose, is
// always allowed, as it may serve the files itself (Igloo does).

// guardedDialer dials only addresses providers may send Igloo to.
type guardedDialer struct {
	dialer net.Dialer
	mu     sync.RWMutex
	// endpoints are the host:port of every provider's endpoint.
	endpoints map[string]bool
}

func newGuardedDialer() *guardedDialer {
	return &guardedDialer{dialer: net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}, endpoints: map[string]bool{}}
}

// allow exempts a provider's endpoint, as host:port.
func (g *guardedDialer) allow(hostport string) {
	g.mu.Lock()
	g.endpoints[hostport] = true
	g.mu.Unlock()
}

// blockedIP reports whether file downloads may not connect to ip.
func blockedIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() ||
		// AWS's IPv6 instance metadata service.
		ip.Equal(net.ParseIP("fd00:ec2::254"))
}

var errBlockedAddress = errors.New("refusing to connect to a loopback, link-local or multicast address")

func (g *guardedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	g.mu.RLock()
	exempt := g.endpoints[addr]
	g.mu.RUnlock()
	if exempt {
		return g.dialer.DialContext(ctx, network, addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var errs []error
	for _, ip := range ips {
		if blockedIP(ip.IP) {
			errs = append(errs, fmt.Errorf("%s: %w", ip.IP, errBlockedAddress))
			continue
		}
		conn, err := g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("no address for %s", host)
	}
	return nil, errors.Join(errs...)
}

// newHTTPClient returns a client for providers' APIs and files that dials
// through g. It uses no proxy from the environment, which would make the
// address check moot, and follows at most a few redirects.
func newHTTPClient(g *guardedDialer) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           g.DialContext,
			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: time.Minute,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   32,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("too many redirects")
			}
			// Never send the bearer token to wherever a redirect points.
			req.Header.Del("Authorization")
			return nil
		},
	}
}

// endpointHostPort returns the host:port a provider's endpoint is dialed at.
func endpointHostPort(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}
