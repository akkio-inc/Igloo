package notify

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
)

// fakeResolver answers lookups from a table; a host with several answers
// gives the next one each time, as a rebinding DNS server would.
type fakeResolver struct {
	mu      sync.Mutex
	answers map[string][][]string
	lookups map[string]int
}

func newResolver(answers map[string][][]string) *fakeResolver {
	return &fakeResolver{answers: answers, lookups: map[string]int{}}
}

func (r *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	answers, ok := r.answers[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	n := r.lookups[host]
	r.lookups[host]++
	var out []netip.Addr
	for _, a := range answers[min(n, len(answers)-1)] {
		out = append(out, netip.MustParseAddr(a))
	}
	return out, nil
}

func TestBlocked(t *testing.T) {
	for _, a := range []string{
		"127.0.0.1", "127.8.9.10", "10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.1.1", "169.254.169.254",
		"169.254.0.1", "100.64.0.1", "0.0.0.0", "0.1.2.3", "224.0.0.1", "239.255.255.250", "255.255.255.255",
		"240.0.0.1", "192.0.2.10", "198.18.0.1", "203.0.113.5",
		"::", "::1", "fe80::1", "fc00::1", "fd00:ec2::254", "ff02::1", "2001:db8::1", "2002:7f00:1::1", "2001::1",
		"::ffff:127.0.0.1", "::ffff:169.254.169.254", "::ffff:10.0.0.1", "::127.0.0.1",
		"64:ff9b::7f00:1", "64:ff9b::a9fe:a9fe", "64:ff9b:1::1", "fec0::1",
		"::ffff:0:7f00:1", "::ffff:0:a9fe:a9fe", "::ffff:0:808:808",
	} {
		if blocked(netip.MustParseAddr(a)) == "" {
			t.Errorf("%s is allowed", a)
		}
	}
	for _, a := range []string{"93.184.215.14", "8.8.8.8", "1.1.1.1", "2606:4700::1111", "64:ff9b::808:808", "::ffff:8.8.8.8"} {
		if r := blocked(netip.MustParseAddr(a)); r != "" {
			t.Errorf("%s is refused: %s", a, r)
		}
	}
	if !strings.Contains(blocked(netip.MustParseAddr("169.254.169.254")), "metadata") {
		t.Error("the metadata endpoint's address is not called out")
	}
}

func TestCheckURL(t *testing.T) {
	g := NewGuard([]string{"hooks.internal", "10.0.0.5"}, nil)
	for raw, ok := range map[string]bool{
		"https://hooks.slack.com/services/T/B/x": true,
		"https://hooks.internal:8443/x":          true,
		"http://hooks.internal/x":                true,
		"http://HOOKS.INTERNAL./x":               true,
		"http://10.0.0.5:8080/x":                 true,
		"http://hooks.slack.com/x":               false,
		"http://10.0.0.6/x":                      false,
		"https://user:pw@hooks.slack.com/x":      false,
		"ftp://hooks.slack.com/x":                false,
		"file:///etc/passwd":                     false,
		"hooks.slack.com/x":                      false,
		"https:///x":                             false,
		"https://":                               false,
	} {
		_, err := g.CheckURL(raw)
		if (err == nil) != ok {
			t.Errorf("%s: %v, want ok %v", raw, err, ok)
		}
		if err != nil && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v is not ErrInvalid", raw, err)
		}
	}
}

func TestResolve(t *testing.T) {
	r := newResolver(map[string][][]string{
		"public.example":   {{"93.184.215.14", "2606:4700::1111"}},
		"private.example":  {{"10.0.0.7"}},
		"mixed.example":    {{"93.184.215.14", "169.254.169.254"}},
		"localhost":        {{"127.0.0.1", "::1"}},
		"hooks.internal":   {{"10.0.0.8"}},
		"metadata.example": {{"fd00:ec2::254"}},
	})
	g := NewGuard([]string{"hooks.internal", "192.168.0.9"}, r)
	ctx := context.Background()
	for host, ok := range map[string]bool{
		"public.example":   true,
		"PUBLIC.example.":  true,
		"hooks.internal":   true,
		"192.168.0.9":      true,
		"93.184.215.14":    true,
		"private.example":  false,
		"mixed.example":    false,
		"localhost":        false,
		"metadata.example": false,
		"169.254.169.254":  false,
		"[::1]":            false,
		"127.0.0.1":        false,
		"0.0.0.0":          false,
	} {
		_, err := g.Resolve(ctx, host)
		if (err == nil) != ok {
			t.Errorf("%s: %v, want ok %v", host, err, ok)
		}
		var ae *AddressError
		if !ok && !errors.As(err, &ae) {
			t.Errorf("%s: %v is not an AddressError", host, err)
		}
	}
	if _, err := g.Resolve(ctx, "nowhere.example"); err == nil || !strings.Contains(err.Error(), "not known") {
		t.Errorf("an unknown host: %v", err)
	}
}

// TestDialRebinding checks that each connection looks the host up again
// and connects to the address it checked: a name that resolved to a public
// address once and to a loopback address the next time is refused.
func TestDialRebinding(t *testing.T) {
	r := newResolver(map[string][][]string{"rebind.example": {{"93.184.215.14"}, {"127.0.0.1"}}})
	g := NewGuard(nil, r)
	var dialed []string
	g.dial = func(_ context.Context, _, addr string, _ bool) (net.Conn, error) {
		dialed = append(dialed, addr)
		return nil, errors.New("test: not connecting")
	}
	ctx := context.Background()
	if _, err := g.DialContext(ctx, "tcp", "rebind.example:443"); err == nil || !strings.Contains(err.Error(), "test: not connecting") {
		t.Fatalf("first dial: %v", err)
	}
	_, err := g.DialContext(ctx, "tcp", "rebind.example:443")
	var ae *AddressError
	if !errors.As(err, &ae) || ae.Addr.String() != "127.0.0.1" {
		t.Fatalf("second dial: %v, want refused", err)
	}
	if len(dialed) != 1 || dialed[0] != "93.184.215.14:443" {
		t.Errorf("dialed %v, want only the vetted public address", dialed)
	}
}

// TestConnectChecksTheSocket checks the second line of defense: the socket
// itself refuses to connect to an address that is not public, unless the
// host is allowed.
func TestConnectChecksTheSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	ctx := context.Background()
	_, err = connect(ctx, "tcp", ln.Addr().String(), false)
	var ae *AddressError
	if !errors.As(err, &ae) {
		t.Errorf("connect to loopback: %v, want refused", err)
	}
	c, err := connect(ctx, "tcp", ln.Addr().String(), true)
	if err != nil {
		t.Fatalf("connect to an allowed host: %v", err)
	}
	_ = c.Close()
}
