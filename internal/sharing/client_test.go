package sharing

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseProfile(t *testing.T) {
	p, err := parseProfile(`{"shareCredentialsVersion": 1, "endpoint": "https://sharing.example.com/delta-sharing/",
		"bearerToken": "tok", "expirationTime": "2027-01-02T03:04:05.0Z"}`)
	if err != nil {
		t.Fatal(err)
	}
	if p.Endpoint != "https://sharing.example.com/delta-sharing" || p.Token != "tok" || p.TokenExpires == nil || p.TokenExpires.Year() != 2027 {
		t.Errorf("profile = %+v", p)
	}

	for _, ok := range []string{
		`{"shareCredentialsVersion": 2, "type": "bearer_token", "endpoint": "https://x.example.com", "bearerToken": "t"}`,
		// Plain HTTP only on this machine or a private network.
		`{"shareCredentialsVersion": 1, "endpoint": "http://localhost:8444/delta-sharing", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "http://10.1.2.3/delta-sharing", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "http://sharing-server:8080/delta-sharing", "bearerToken": "t"}`,
	} {
		if _, err := parseProfile(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{
		`not json`,
		`{"endpoint": "https://x.example.com", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 3, "endpoint": "https://x.example.com", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 2, "type": "oauth_client_credentials", "endpoint": "https://x.example.com"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "https://x.example.com"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "http://sharing.example.com/ds", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "ftp://x.example.com", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "https://user:pw@x.example.com", "bearerToken": "t"}`,
		`{"shareCredentialsVersion": 1, "endpoint": "https://x.example.com", "bearerToken": "t", "expirationTime": "soon"}`,
	} {
		if _, err := parseProfile(bad); err == nil {
			t.Errorf("%s: parsed", bad)
		}
	}
}

func TestRewriteRefs(t *testing.T) {
	external := func(schema, name string) bool {
		return strings.EqualFold(schema, "partners") && strings.EqualFold(name, "tvco") ||
			strings.EqualFold(schema, "public") && strings.EqualFold(name, "vz")
	}
	for _, c := range []struct{ in, want string }{
		{`SELECT * FROM partners.tvco`, `SELECT * FROM "memory".partners.tvco`},
		{`SELECT * FROM lake.partners.tvco JOIN lake . "Partners" . "TVCo" ON true`,
			`SELECT * FROM "memory".partners.tvco JOIN "memory" . "Partners" . "TVCo" ON true`},
		{`SELECT * FROM vz, public.vz`, `SELECT * FROM vz, "memory".public.vz`},
		// Other catalogs, longer names, literals and comments stay.
		{`SELECT * FROM other.partners.tvco`, `SELECT * FROM other.partners.tvco`},
		{`SELECT * FROM a.lake.partners.tvco`, `SELECT * FROM a.lake.partners.tvco`},
		{`SELECT 'partners.tvco' -- partners.tvco`, `SELECT 'partners.tvco' -- partners.tvco`},
		{`SELECT * FROM partners.other`, `SELECT * FROM partners.other`},
		{`SELECT * FROM memory.partners.tvco`, `SELECT * FROM memory.partners.tvco`},
	} {
		if got := rewriteRefs(c.in, "lake", `"memory"`, external); got != c.want {
			t.Errorf("rewriteRefs(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

func TestGuardedDialer(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "::1", "169.254.169.254", "fd00:ec2::254", "0.0.0.0", "224.0.0.1", "fe80::1"} {
		if !blockedIP(net.ParseIP(ip)) {
			t.Errorf("%s is not blocked", ip)
		}
	}
	for _, ip := range []string{"10.0.0.5", "192.168.1.1", "172.20.0.3", "52.216.1.1", "2600:1f18::1"} {
		if blockedIP(net.ParseIP(ip)) {
			t.Errorf("%s is blocked", ip)
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer srv.Close()
	g := newGuardedDialer()
	c := newHTTPClient(g)
	_, err := c.Get(srv.URL)
	if err == nil || !errors.Is(err, errBlockedAddress) {
		t.Errorf("a loopback URL: %v", err)
	}
	g.allow(endpointHostPort(srv.URL))
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("a provider's own endpoint: %v", err)
	}
	resp.Body.Close() //nolint:errcheck

	// Redirects never carry the bearer token.
	var sawAuth string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sawAuth = r.Header.Get("Authorization") }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	g.allow(endpointHostPort(target.URL))
	g.allow(endpointHostPort(redirect.URL))
	cl := &client{endpoint: redirect.URL, token: "secret", http: c}
	if resp, err := cl.do(context.Background(), http.MethodGet, "/x", nil, ""); err == nil {
		resp.Body.Close() //nolint:errcheck
	}
	if sawAuth != "" {
		t.Errorf("a redirect carried the token: %q", sawAuth)
	}
}
