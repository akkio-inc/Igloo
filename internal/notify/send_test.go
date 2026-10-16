package notify

import (
	"context"
	"crypto/hmac"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// receiver is a webhook receiver: it records each request and answers
// with the statuses in answers, then 204.
type receiver struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*received
	answers  []int
	handle   func(w http.ResponseWriter, r *http.Request) bool
}

type received struct {
	path   string
	header http.Header
	body   []byte
}

func newReceiver(t *testing.T, tls bool) *receiver {
	t.Helper()
	rc := &receiver{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rc.mu.Lock()
		rc.requests = append(rc.requests, &received{path: r.URL.Path, header: r.Header.Clone(), body: body})
		status := http.StatusNoContent
		if len(rc.answers) > 0 {
			status, rc.answers = rc.answers[0], rc.answers[1:]
		}
		handle := rc.handle
		rc.mu.Unlock()
		if handle != nil && handle(w, r) {
			return
		}
		w.WriteHeader(status)
	})
	if tls {
		rc.Server = httptest.NewTLSServer(h)
	} else {
		rc.Server = httptest.NewServer(h)
	}
	t.Cleanup(rc.Close)
	return rc
}

func (rc *receiver) got() []*received {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]*received(nil), rc.requests...)
}

// newTestService is a service that may reach 127.0.0.1, waiting little
// between attempts.
func newTestService(t *testing.T, opts Options) *Service {
	t.Helper()
	if opts.AllowHosts == nil {
		opts.AllowHosts = []string{"127.0.0.1"}
	}
	s := New(opts)
	s.backoff = []time.Duration{time.Millisecond}
	return s
}

func testMessage() Message {
	v := "acme"
	return Message{Type: RecordAlert, Name: "pacing", User: "ana", Title: "Igloo alert pacing",
		Body:   []Part{Text("Spend is off plan for "), Value("acme")},
		Result: &Result{Columns: []string{"client", "spend"}, Rows: [][]*string{{&v, nil}}}}
}

func TestWebhook(t *testing.T) {
	rc := newReceiver(t, false)
	s := newTestService(t, Options{})
	in := Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL + "/hook/tok3n-0123456789",
		Headers: map[string]string{"Authorization": "Bearer header-secret-123"}, Secret: "signing-secret-0123456789"}
	if err := s.Check(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	attempts, err := s.Send(context.Background(), in, testMessage())
	if err != nil || attempts != 1 {
		t.Fatalf("send: %d attempts, %v", attempts, err)
	}
	got := rc.got()
	if len(got) != 1 {
		t.Fatalf("%d requests", len(got))
	}
	r := got[0]
	if r.path != "/hook/tok3n-0123456789" || r.header.Get("Authorization") != "Bearer header-secret-123" ||
		r.header.Get("Content-Type") != "application/json" {
		t.Errorf("request: %s %v", r.path, r.header)
	}
	sig := strings.TrimPrefix(r.header.Get("X-Igloo-Signature"), "v1=")
	if want := Sign(in.Secret, r.header.Get("X-Igloo-Timestamp"), r.body); !hmac.Equal([]byte(sig), []byte(want)) {
		t.Errorf("signature %q, want %q", sig, want)
	}
	var p struct {
		Type, Name, User, Integration, Title, Message string
		Result                                        struct {
			Columns []string
			Rows    [][]*string
			More    bool
		}
	}
	if err := json.Unmarshal(r.body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Type != "alert" || p.Name != "pacing" || p.User != "ana" || p.Integration != "ops" ||
		p.Message != "Spend is off plan for acme" || len(p.Result.Rows) != 1 || *p.Result.Rows[0][0] != "acme" ||
		p.Result.Rows[0][1] != nil {
		t.Errorf("payload: %s", r.body)
	}
}

// TestSlackAndTeamsEscaping checks that values from data cannot mention
// anyone or make links, while the alert's own text may.
func TestSlackAndTeamsEscaping(t *testing.T) {
	evil := "<!here> <!channel> @channel <@U123> <https://evil.example|click> https://evil.example ```x``` *bold*"
	m := Message{Title: "Igloo alert x", Body: []Part{Text("<!here> look: "), Value(evil)},
		Result: &Result{Columns: []string{"v"}, Rows: [][]*string{{&evil}}}}

	text := slackText(m)
	if !strings.HasPrefix(text, "*Igloo alert x*\n<!here> look: `") {
		t.Errorf("the template's own mention is gone: %q", text)
	}
	values := strings.TrimPrefix(text, "*Igloo alert x*\n<!here> look: ")
	for _, bad := range []string{"<!here>", "<!channel>", "<@U123>", "<https://", "```x"} {
		if strings.Contains(values, bad) {
			t.Errorf("a value's %q reaches Slack: %q", bad, values)
		}
	}
	// Every value is code: the inline one, and the result's block.
	inline, block, _ := strings.Cut(values, "\n```\n")
	if !strings.HasPrefix(inline, "`") || !strings.HasSuffix(inline, "`") || strings.Count(inline, "`") != 2 {
		t.Errorf("the value is not one code span: %q", inline)
	}
	if !strings.HasSuffix(block, "```") || strings.Count(block, "```") != 1 {
		t.Errorf("the result is not one code block: %q", block)
	}
	if !strings.Contains(values, "&lt;!here&gt;") {
		t.Errorf("not escaped: %q", values)
	}
	payload, _ := slackPayload(m)
	if !strings.Contains(string(payload), `"unfurl_links":false`) {
		t.Errorf("Slack would unfurl links: %s", payload)
	}

	body, err := teamsPayload(m)
	if err != nil {
		t.Fatal(err)
	}
	var card struct {
		Attachments []struct {
			Content struct {
				Body []struct {
					Type    string
					Inlines []struct {
						Type, Text, FontType string
					}
				}
			}
		}
	}
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatal(err)
	}
	var values2 []string
	for _, b := range card.Attachments[0].Content.Body {
		if b.Type != "RichTextBlock" {
			t.Errorf("a %s, which Teams reads as Markdown", b.Type)
		}
		for _, in := range b.Inlines {
			if in.Type != "TextRun" {
				t.Errorf("an inline %s", in.Type)
			}
			if in.FontType == "Monospace" {
				values2 = append(values2, in.Text)
			}
		}
	}
	if len(values2) < 2 {
		t.Fatalf("Teams values: %q", values2)
	}
	for _, v := range values2 {
		if strings.Contains(v, "://") || strings.Contains(v, "@c") || strings.Contains(v, "@U") {
			t.Errorf("Teams could make a link or mention of %q", v)
		}
	}
}

func TestRetries(t *testing.T) {
	rc := newReceiver(t, false)
	rc.answers = []int{503, 429, 200}
	s := newTestService(t, Options{})
	in := Integration{Name: "ops", Type: TypeSlack, Enabled: true, URL: rc.URL + "/services/T1/B2/abcdefghijklmnop"}
	attempts, err := s.Send(context.Background(), in, testMessage())
	if err != nil || attempts != 3 {
		t.Errorf("after 503, 429, 200: %d attempts, %v", attempts, err)
	}

	// Permanent failures are not tried again, and errors never show the
	// URL's token, even when the receiver quotes it.
	rc.answers = []int{404}
	rc.handle = func(w http.ResponseWriter, r *http.Request) bool {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "no_service for "+r.URL.Path)
		return true
	}
	attempts, err = s.Send(context.Background(), in, testMessage())
	var se *SendError
	if !errors.As(err, &se) || attempts != 1 || !strings.Contains(err.Error(), "404") {
		t.Errorf("404: %d attempts, %v", attempts, err)
	}
	if strings.Contains(err.Error(), "abcdefghijklmnop") || !strings.Contains(err.Error(), "***") {
		t.Errorf("the error shows the token: %v", err)
	}

	// Nor when the connection fails.
	rc.handle = nil
	rc.Close()
	_, err = s.Send(context.Background(), in, testMessage())
	if err == nil || strings.Contains(err.Error(), "abcdefghijklmnop") {
		t.Errorf("a closed receiver: %v", err)
	}
}

func TestTimeout(t *testing.T) {
	rc := newReceiver(t, false)
	rc.handle = func(http.ResponseWriter, *http.Request) bool { time.Sleep(300 * time.Millisecond); return false }
	s := newTestService(t, Options{})
	s.timeout = 50 * time.Millisecond
	in := Integration{Name: "slow", Type: TypeWebhook, Enabled: true, URL: rc.URL}
	start := time.Now()
	attempts, err := s.Send(context.Background(), in, testMessage())
	if err == nil || attempts != maxAttempts {
		t.Errorf("a slow receiver: %d attempts, %v", attempts, err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("took %v", took)
	}
}

func TestNoRedirects(t *testing.T) {
	other := newReceiver(t, false)
	rc := newReceiver(t, false)
	rc.handle = func(w http.ResponseWriter, r *http.Request) bool {
		http.Redirect(w, r, other.URL+"/elsewhere", http.StatusFound)
		return true
	}
	s := newTestService(t, Options{})
	in := Integration{Name: "moved", Type: TypeWebhook, Enabled: true, URL: rc.URL}
	if _, err := s.Send(context.Background(), in, testMessage()); err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Errorf("a redirect: %v", err)
	}
	if n := len(other.got()); n != 0 {
		t.Errorf("the redirect was followed %d times", n)
	}
}

func TestRateLimit(t *testing.T) {
	rc := newReceiver(t, false)
	s := newTestService(t, Options{})
	s.burst, s.every, s.wait = 2, time.Hour, time.Minute
	in := Integration{Name: "ops", Type: TypeWebhook, Enabled: true, URL: rc.URL}
	other := Integration{Name: "other", Type: TypeWebhook, Enabled: true, URL: rc.URL}
	ctx := context.Background()
	for i := range 2 {
		if _, err := s.Send(ctx, in, testMessage()); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if _, err := s.Send(ctx, in, testMessage()); !errors.Is(err, ErrRateLimited) {
		t.Errorf("third send: %v, want rate limited", err)
	}
	if _, err := s.Send(ctx, other, testMessage()); err != nil {
		t.Errorf("each integration has its own limit: %v", err)
	}

	// Within the wait, a send waits its turn.
	s.burst, s.every, s.wait = 1, 100*time.Millisecond, time.Second
	s.buckets = map[string]*bucket{}
	start := time.Now()
	for i := range 2 {
		if _, err := s.Send(ctx, in, testMessage()); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if took := time.Since(start); took < 80*time.Millisecond {
		t.Errorf("the second send did not wait: %v", took)
	}
}

// TestHTTPS sends to a public host name over TLS: the host is resolved
// (here by a fake resolver), checked, and connected to at that address
// (here redirected to a local TLS server), with the certificate checked for
// the name.
func TestHTTPS(t *testing.T) {
	rc := newReceiver(t, true)
	r := newResolver(map[string][][]string{"example.com": {{"93.184.215.14"}}})
	s := newTestService(t, Options{AllowHosts: []string{}, Resolver: r})
	pool := x509.NewCertPool()
	pool.AddCert(rc.Certificate())
	s.tlsConfig.RootCAs = pool
	local := rc.Listener.Addr().String()
	var dialed []string
	s.guard.dial = func(ctx context.Context, network, addr string, allowed bool) (net.Conn, error) {
		dialed = append(dialed, addr)
		var d net.Dialer
		return d.DialContext(ctx, network, local)
	}
	in := Integration{Name: "ops", Type: TypeTeams, Enabled: true, URL: "https://example.com/webhookb2/abc"}
	if err := s.Check(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(context.Background(), in, testMessage()); err != nil {
		t.Fatal(err)
	}
	if len(dialed) != 1 || dialed[0] != "93.184.215.14:443" {
		t.Errorf("dialed %v", dialed)
	}
	if got := rc.got(); len(got) != 1 || !strings.Contains(string(got[0].body), "AdaptiveCard") {
		t.Errorf("received %v", got)
	}
}

func TestCheck(t *testing.T) {
	r := newResolver(map[string][][]string{"hooks.slack.com": {{"93.184.215.14"}}, "internal.example": {{"10.1.1.1"}}})
	s := New(Options{Resolver: r})
	ctx := context.Background()
	for _, tc := range []struct {
		in      Integration
		address bool
	}{
		{in: Integration{Type: "pager", URL: "https://hooks.slack.com/x"}},
		{in: Integration{Type: TypeSlack}},
		{in: Integration{Type: TypeSlack, URL: "https://hooks.slack.com/x", Headers: map[string]string{"X": "y"}}},
		{in: Integration{Type: TypeWebhook, URL: "https://hooks.slack.com/x", Secret: "short"}},
		{in: Integration{Type: TypeWebhook, URL: "https://hooks.slack.com/x", Headers: map[string]string{"Host": "evil"}}},
		{in: Integration{Type: TypeWebhook, URL: "https://hooks.slack.com/x", Headers: map[string]string{"X-A": "a\r\nB: c"}}},
		{in: Integration{Type: TypeWebhook, URL: "https://hooks.slack.com/x", Headers: map[string]string{"Bad Name": "a"}}},
		{in: Integration{Type: TypeWebhook, URL: "http://hooks.slack.com/x"}},
		{in: Integration{Type: TypeEmail, Recipients: []string{"a@example.com"}}},
		{in: Integration{Type: TypeWebhook, URL: "https://internal.example/x"}, address: true},
		{in: Integration{Type: TypeWebhook, URL: "https://169.254.169.254/latest/meta-data/"}, address: true},
	} {
		err := s.Check(ctx, tc.in)
		var ae *AddressError
		switch {
		case err == nil:
			t.Errorf("%+v: accepted", tc.in)
		case tc.address && !errors.As(err, &ae):
			t.Errorf("%+v: %v, want an address refusal", tc.in, err)
		case !tc.address && !errors.Is(err, ErrInvalid):
			t.Errorf("%+v: %v, want invalid", tc.in, err)
		}
	}
	if err := s.Check(ctx, Integration{Type: TypeWebhook, URL: "https://hooks.slack.com/x", Secret: "a-long-enough-secret"}); err != nil {
		t.Error(err)
	}
}

func TestScrub(t *testing.T) {
	in := Integration{URL: "https://hooks.slack.com/services/T0001/B0002/abcdefabcdef?key=qwertyuiop",
		Headers: map[string]string{"Authorization": "Bearer zxcvbnmasdf", "X-One": "1"}, Secret: "signing-secret-value"}
	msg := in.Scrub("Post " + in.URL + ": Bearer zxcvbnmasdf, signing-secret-value, 1 more, qwertyuiop")
	for _, s := range []string{"abcdefabcdef", "qwertyuiop", "zxcvbnmasdf", "signing-secret-value", "services/T0001"} {
		if strings.Contains(msg, s) {
			t.Errorf("%q shows %q", msg, s)
		}
	}
	if !strings.Contains(msg, "1 more") {
		t.Errorf("short values garbled the message: %q", msg)
	}
}
