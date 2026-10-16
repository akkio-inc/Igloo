package openlineage

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
)

func TestNewRefusesUnsafeEndpoints(t *testing.T) {
	for _, u := range []string{
		"http://marquez.example.com/api/v1/lineage", // not encrypted
		"https://user:secret@marquez.example.com/",  // credentials in the URL
		"https://169.254.169.254/latest/meta-data",  // link-local: cloud metadata
		"https://[fe80::1]/",                        // link-local
		"https://10.0.0.5/api/v1/lineage",           // private
		"https://127.0.0.1:5000/api/v1/lineage",     // loopback
		"https://100.100.100.200/",                  // carrier-grade NAT
		"https://[::ffff:169.254.169.254]/",         // mapped link-local
		"https://[64:ff9b::a9fe:a9fe]/",             // NAT64 of link-local
		"https://0.0.0.0/",                          // unspecified
		"file:///etc/passwd",                        // not HTTP
		"https:///api/v1/lineage",                   // no host
	} {
		if _, err := New(Options{URL: u}); err == nil {
			t.Errorf("%s was accepted", u)
		}
	}
	for _, u := range []string{"http://10.0.0.5/api/v1/lineage", "https://127.0.0.1:5000/"} {
		if _, err := New(Options{URL: u, AllowPrivate: true}); err != nil {
			t.Errorf("%s with private addresses allowed: %v", u, err)
		}
	}
	if _, err := New(Options{URL: "https://169.254.169.254/", AllowPrivate: true}); err == nil {
		t.Error("a link-local address was accepted with private addresses allowed")
	}
}

func TestAllowed(t *testing.T) {
	// Whether events may go to an address, with private addresses refused
	// and allowed.
	for addr, want := range map[string][2]bool{
		"8.8.8.8":              {true, true},
		"2001:4860:4860::8888": {true, true},
		"10.1.2.3":             {false, true},
		"192.168.1.1":          {false, true},
		"172.16.0.1":           {false, true},
		"127.0.0.1":            {false, true},
		"::1":                  {false, true},
		"fd00::1":              {false, true}, // unique local
		"fec0::1":              {false, true}, // site-local
		"100.64.0.1":           {false, true}, // carrier-grade NAT
		"198.18.0.1":           {false, true}, // benchmarking
		"198.19.255.255":       {false, true},
		"169.254.169.254":      {false, false}, // link-local: cloud metadata
		"fe80::1":              {false, false},
		"100.100.100.200":      {false, false}, // Alibaba's metadata service
		"fd00:ec2::254":        {false, false}, // AWS's over IPv6
		"0.0.0.0":              {false, false},
		"0.1.2.3":              {false, false}, // this network
		"240.0.0.1":            {false, false}, // reserved
		"255.255.255.255":      {false, false},
		"::":                   {false, false},
		"224.0.0.1":            {false, false},
		"ff02::1":              {false, false},
		// IPv6 that carries IPv4: checked as the IPv4 address it carries,
		// or refused when that cannot be read off it.
		"::ffff:169.254.169.254":               {false, false}, // IPv4-mapped
		"::ffff:10.0.0.1":                      {false, true},
		"::ffff:8.8.8.8":                       {true, true},
		"::ffff:0:169.254.169.254":             {false, false}, // IPv4-translated
		"::ffff:0:8.8.8.8":                     {true, true},
		"64:ff9b::a9fe:a9fe":                   {false, false}, // NAT64 of link-local
		"64:ff9b::808:808":                     {true, true},
		"64:ff9b::a00:1":                       {false, true},
		"64:ff9b:1::a9fe:a9fe":                 {false, false}, // local-use NAT64
		"2002:a9fe:a9fe::1":                    {false, false}, // 6to4 of link-local
		"2002:a00:1::1":                        {false, true},
		"2002:808:808::1":                      {true, true},
		"2001:0:4136:e378:8000:63bf:3fff:fdd2": {false, false}, // Teredo
		"::169.254.169.254":                    {false, false}, // IPv4-compatible
		"::8.8.8.8":                            {false, false},
	} {
		for i, private := range []bool{false, true} {
			if got := allowed(netip.MustParseAddr(addr), private) == nil; got != want[i] {
				t.Errorf("allowed(%s, private %v) = %v, want %v", addr, private, got, want[i])
			}
		}
	}
}

// TestDialPinsAddresses checks that a name is resolved once, refused if
// any of its addresses is, and connected to at the address checked.
func TestDialPinsAddresses(t *testing.T) {
	received := make(chan bool, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	_, port, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	c, err := New(Options{URL: "http://lineage.example:" + port + "/api/v1/lineage", AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	g := c.http.Transport.(*http.Transport)
	var answers [][]netip.Addr
	lookups := 0
	pinned := &guard{private: true, dialer: &net.Dialer{Timeout: time.Second}, lookup: func(context.Context, string, string) ([]netip.Addr, error) {
		lookups++
		return answers[min(lookups, len(answers))-1], nil
	}}
	g.DialContext = pinned.dial

	answers = [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}}
	if err := c.post(context.Background(), []byte("{}")); err != nil {
		t.Fatalf("a name resolving to an allowed address: %v", err)
	}
	<-received
	g.CloseIdleConnections()
	// A name that resolves to an address events may not go to as well is
	// refused, whichever address comes first.
	answers = [][]netip.Addr{{netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("169.254.169.254")}}
	lookups = 0
	if err := c.post(context.Background(), []byte("{}")); err == nil || !strings.Contains(err.Error(), "169.254.169.254") {
		t.Errorf("a name resolving to a link-local address too: %v", err)
	}
	select {
	case <-received:
		t.Error("an event went to a name that also resolves to a refused address")
	default:
	}
}

func TestEvents(t *testing.T) {
	received := make(chan map[string]any, 10)
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		var e map[string]any
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &e); err != nil {
			t.Errorf("not JSON: %s", body)
		}
		received <- e
	}))
	defer srv.Close()

	c, err := New(Options{URL: srv.URL, APIKey: "k", AllowPrivate: true, Namespace: "agency"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	start := time.Date(2026, 10, 16, 6, 0, 0, 0, time.UTC)
	c.Statement(audit.Query{ID: "0192a3b4-0000-7000-8000-000000000001", Start: start, Duration: time.Second, User: "dbt",
		StatementType: "INSERT", Text: "INSERT INTO report SELECT * FROM orders WHERE email = 'ann@acme.com'",
		TablesRead: []string{"lake.public.orders"}, LocationsRead: []string{"s3://landing/sales/day1.csv"},
		TablesWritten: []string{"lake.public.report"}, Job: "task nightly"})
	var e map[string]any
	select {
	case e = <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("no event")
	}
	if auth != "Bearer k" {
		t.Errorf("authorization: %q", auth)
	}
	got, _ := json.Marshal(e)
	for _, want := range []string{
		`"eventType":"COMPLETE"`, `"eventTime":"2026-10-16T06:00:01Z"`, `"runId":"0192a3b4-0000-7000-8000-000000000001"`,
		`"name":"task.nightly"`, `"namespace":"agency"`, `"jobType":"TASK"`,
		`{"name":"lake.public.orders","namespace":"agency"}`, `{"name":"sales/day1.csv","namespace":"s3://landing"}`,
		`"outputs":[{"name":"lake.public.report","namespace":"agency"}]`,
	} {
		if !strings.Contains(string(got), want) {
			t.Errorf("event lacks %s: %s", want, got)
		}
	}
	// No statement text, user or values unless asked for.
	for _, never := range []string{"ann@acme.com", "SELECT", "dbt"} {
		if strings.Contains(string(got), never) {
			t.Errorf("event shows %q: %s", never, got)
		}
	}

	c.opts.SendSQL = true
	c.Statement(audit.Query{ID: "0192a3b4-0000-7000-8000-000000000002", Start: start, StatementType: "CREATE TABLE",
		Text: "CREATE TABLE t AS SELECT 1", TablesWritten: []string{"lake.public.t"}})
	e = <-received
	got, _ = json.Marshal(e)
	if !strings.Contains(string(got), `"query":"CREATE TABLE t AS SELECT 1"`) || !strings.Contains(string(got), `"name":"sql.lake.public.t"`) {
		t.Errorf("with SQL: %s", got)
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	hit := make(chan bool, 1)
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit <- true }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c, err := New(Options{URL: redirect.URL, AllowPrivate: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.post(context.Background(), []byte("{}")); err == nil {
		t.Error("a redirect counted as sent")
	}
	select {
	case <-hit:
		t.Error("the redirect was followed")
	default:
	}
	// Without private addresses allowed, connections to them fail even
	// when the name resolves there.
	c, err = New(Options{URL: strings.Replace(target.URL, "http://127.0.0.1", "https://localhost", 1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.post(context.Background(), []byte("{}")); err == nil || !strings.Contains(err.Error(), "private address") {
		t.Errorf("a name resolving to loopback: %v", err)
	}
}
