package notify

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Resolver looks up a host's addresses; *net.Resolver is one.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// AddressError refuses a host whose address is not public.
type AddressError struct {
	Host   string
	Addr   netip.Addr
	Reason string
}

func (e *AddressError) Error() string {
	if e.Host == e.Addr.String() {
		return fmt.Sprintf("notifications may only reach public addresses, and %s is %s; "+
			"IGLOO_NOTIFICATION_ALLOW_HOSTS can allow it", e.Host, e.Reason)
	}
	return fmt.Sprintf("notifications may only reach public addresses, and %s resolves to %s, %s; "+
		"IGLOO_NOTIFICATION_ALLOW_HOSTS can allow the host", e.Host, e.Addr, e.Reason)
}

// Guard decides which hosts notifications may reach, and connects to
// them. A host is reached only at public addresses, unless the allow list
// names it; then any address will do, and so will plain HTTP and SMTP.
type Guard struct {
	allow    map[string]bool
	resolver Resolver
	// dial connects to an address the guard has checked; allowed says
	// the host is on the allow list. Tests replace it.
	dial func(ctx context.Context, network, addr string, allowed bool) (net.Conn, error)
}

// connectTimeout bounds each connection attempt.
const connectTimeout = 10 * time.Second

// NewGuard allows the hosts in allow, names or IP addresses, besides public
// ones. resolver nil means the system's.
func NewGuard(allow []string, resolver Resolver) *Guard {
	g := &Guard{allow: map[string]bool{}, resolver: resolver}
	for _, h := range allow {
		g.allow[canonicalHost(h)] = true
	}
	if g.resolver == nil {
		g.resolver = net.DefaultResolver
	}
	g.dial = connect
	return g
}

// canonicalHost lower-cases a name and writes an address the standard way.
func canonicalHost(h string) string {
	h = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(h)), ".")
	if a, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		return a.Unmap().String()
	}
	return h
}

// Allowed reports whether the allow list names host.
func (g *Guard) Allowed(host string) bool { return g.allow[canonicalHost(host)] }

// CheckURL parses a webhook URL and checks what can be checked without the
// network: https, or http to an allowed host; no user name or password in
// it; a host.
func (g *Guard) CheckURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() || u.Host == "" || u.Opaque != "" {
		return nil, fmt.Errorf("%w: the URL must be absolute, such as https://hooks.example.com/...", ErrInvalid)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%w: the URL may not carry a user name or password; send them in HEADERS", ErrInvalid)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !g.Allowed(u.Hostname()) {
			return nil, fmt.Errorf("%w: the URL must use https; plain http is only for hosts in IGLOO_NOTIFICATION_ALLOW_HOSTS", ErrInvalid)
		}
	default:
		return nil, fmt.Errorf("%w: the URL must use https", ErrInvalid)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("%w: the URL has no host", ErrInvalid)
	}
	return u, nil
}

// Resolve returns host's addresses, refusing it unless all of them may be
// reached. An allowed host may be at any address.
func (g *Guard) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	host = canonicalHost(host)
	var addrs []netip.Addr
	if a, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{a}
	} else {
		ctx, cancel := context.WithTimeout(ctx, connectTimeout)
		defer cancel()
		if addrs, err = g.resolver.LookupNetIP(ctx, "ip", host); err != nil {
			var dnsErr *net.DNSError
			if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
				return nil, fmt.Errorf("the host %s is not known", host)
			}
			return nil, fmt.Errorf("look up %s: %w", host, err)
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("the host %s has no address", host)
		}
	}
	if g.allow[host] {
		return addrs, nil
	}
	// Refuse the host if any address is not public, rather than use the
	// public ones: a name that resolves to both is not to be trusted.
	for _, a := range addrs {
		if reason := blocked(a); reason != "" {
			return nil, &AddressError{Host: host, Addr: a.Unmap(), Reason: reason}
		}
	}
	return addrs, nil
}

// DialContext connects to address, a host and port, at an address of the
// host that Resolve vetted just now. It is a net.Dialer's DialContext for
// HTTP transports and SMTP.
func (g *Guard) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addrs, err := g.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	allowed := g.Allowed(host)
	var errs []error
	for _, a := range addrs {
		conn, err := g.dial(ctx, network, net.JoinHostPort(a.Unmap().String(), port), allowed)
		if err == nil {
			return conn, nil
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("connect to %s: %w", host, errors.Join(errs...))
}

// connect dials addr, an address DialContext vetted. For hosts that are
// not allowed, the socket checks the address again as it connects, in case
// anything between the check and the connection changed it.
func connect(ctx context.Context, network, addr string, allowed bool) (net.Conn, error) {
	d := &net.Dialer{Timeout: connectTimeout}
	if !allowed {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("connecting to %s, which is not an address", address)
			}
			if reason := blocked(ap.Addr()); reason != "" {
				return &AddressError{Host: ap.Addr().String(), Addr: ap.Addr().Unmap(), Reason: reason}
			}
			return nil
		}
	}
	return d.DialContext(ctx, network, addr)
}

// blockedPrefixes are ranges that are not public besides those netip
// classifies: shared address space, IETF and documentation ranges,
// benchmarking, reserved space, deprecated and transition IPv6 ranges.
var blockedPrefixes = func() []netip.Prefix {
	var out []netip.Prefix
	for _, p := range []string{
		"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24",
		"203.0.113.0/24", "240.0.0.0/4",
		"::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "fec0::/10",
	} {
		out = append(out, netip.MustParsePrefix(p))
	}
	return out
}()

// nat64 is the well-known NAT64 prefix, whose addresses stand for the IPv4
// address in their last 32 bits.
var nat64 = netip.MustParsePrefix("64:ff9b::/96")

// blocked says why a may not be reached, or "" if it is public.
func blocked(a netip.Addr) string {
	a = a.Unmap()
	if nat64.Contains(a) {
		b := a.As16()
		return blocked(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}))
	}
	switch {
	case !a.IsValid(), a.IsUnspecified():
		return "an unspecified address"
	case a.IsLoopback():
		return "a loopback address"
	case a.IsPrivate():
		return "a private address"
	case a.IsLinkLocalUnicast():
		return "a link-local address, such as the cloud metadata endpoint's"
	case a.IsMulticast(), a.IsLinkLocalMulticast(), a.IsInterfaceLocalMulticast():
		return "a multicast address"
	}
	for _, p := range blockedPrefixes {
		if p.Contains(a) {
			return "not a public address"
		}
	}
	if !a.IsGlobalUnicast() {
		return "not a public address"
	}
	return ""
}
