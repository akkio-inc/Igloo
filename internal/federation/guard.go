package federation

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The servers no connection may reach (Options.Protected): Igloo's own
// metadata database's. This guards against a mistake, such as connecting
// the database that holds Igloo's users and grants, not against an admin
// who means to get past it with a name it cannot know (another of the
// server's addresses, a proxy in front of it).

// resolveTimeout bounds each name lookup the guard makes.
const resolveTimeout = 5 * time.Second

// server is a host and port, as written.
type server struct {
	host string
	port int
}

// parseServers parses host:port addresses, leaving out what does not parse.
func parseServers(addrs []string) []server {
	var out []server
	for _, a := range addrs {
		host, port, err := net.SplitHostPort(a)
		if err != nil {
			continue
		}
		p, err := strconv.Atoi(port)
		if err != nil || host == "" {
			continue
		}
		out = append(out, server{host, p})
	}
	return out
}

// canonicalHost is the form of a host name or IP address that two
// spellings of one share: lower case, without a trailing dot, an IP
// literal in its shortest form, IPv4-mapped IPv6 as IPv4.
func canonicalHost(h string) string {
	h = strings.TrimSuffix(strings.ToLower(strings.Trim(strings.TrimSpace(h), "[]")), ".")
	if ip, err := netip.ParseAddr(h); err == nil {
		return ip.Unmap().WithZone("").String()
	}
	return h
}

// resolve returns the addresses host stands for: itself if it is an IP
// literal, else what the resolver says now, or nothing if it cannot say.
func resolve(ctx context.Context, host string) []netip.Addr {
	h := canonicalHost(host)
	if ip, err := netip.ParseAddr(h); err == nil {
		return []netip.Addr{ip}
	}
	ctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip", h)
	if err != nil {
		return nil
	}
	for i, a := range addrs {
		addrs[i] = a.Unmap().WithZone("")
	}
	return addrs
}

// reachesProtected reports whether host:port is one of protected: by name,
// however written, or by the addresses both names resolve to when asked.
// It resolves names only here, when a connection is created or changed,
// so a resolver that is down never stops Igloo from starting; a name that
// does not resolve is compared by name alone, and a connection to it
// cannot sign in anyway.
func reachesProtected(ctx context.Context, protected []server, host string, port int) (server, bool) {
	var candidates []server
	for _, p := range protected {
		if p.port != port {
			continue
		}
		if canonicalHost(p.host) == canonicalHost(host) {
			return p, true
		}
		candidates = append(candidates, p)
	}
	if len(candidates) == 0 {
		return server{}, false
	}
	addrs := resolve(ctx, host)
	if len(addrs) == 0 {
		return server{}, false
	}
	for _, p := range candidates {
		if slices.ContainsFunc(resolve(ctx, p.host), func(a netip.Addr) bool { return slices.Contains(addrs, a) }) {
			return p, true
		}
	}
	return server{}, false
}
