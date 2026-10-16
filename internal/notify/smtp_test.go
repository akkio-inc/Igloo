package notify

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"mime"
	"net"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/akkio-inc/igloo/internal/config"
)

// smtpServer is a small SMTP server for tests. It offers STARTTLS when it
// has a certificate, takes AUTH PLAIN with one user, and keeps each
// message it accepts.
type smtpServer struct {
	ln       net.Listener
	tls      *tls.Config
	user     string
	password string
	mu       sync.Mutex
	messages []smtpMessage
	// usedTLS and authed record what the last session did.
	usedTLS, authed bool
}

type smtpMessage struct {
	from string
	to   []string
	data string
}

func newSMTPServer(t *testing.T, cfg *tls.Config) *smtpServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &smtpServer{ln: ln, tls: cfg, user: "alerts", password: "smtp-password-123"}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *smtpServer) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *smtpServer) got() []smtpMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]smtpMessage(nil), s.messages...)
}

func (s *smtpServer) serve(c net.Conn) {
	defer c.Close() //nolint:errcheck
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	say := func(line string) {
		_, _ = w.WriteString(line + "\r\n")
		_ = w.Flush()
	}
	say("220 test ESMTP")
	var msg smtpMessage
	usedTLS, authed := false, false
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(cmd)
		switch {
		case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
			if s.tls != nil && !usedTLS {
				say("250-test")
				say("250-AUTH PLAIN")
				say("250 STARTTLS")
			} else {
				say("250-test")
				say("250 AUTH PLAIN")
			}
		case upper == "STARTTLS" && s.tls != nil:
			say("220 go ahead")
			tc := tls.Server(c, s.tls)
			if err := tc.Handshake(); err != nil {
				return
			}
			c, usedTLS = tc, true
			r, w = bufio.NewReader(tc), bufio.NewWriter(tc)
		case strings.HasPrefix(upper, "AUTH PLAIN"):
			raw, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(cmd[len("AUTH PLAIN"):]))
			if string(raw) != "\x00"+s.user+"\x00"+s.password {
				say("535 5.7.8 authentication failed")
				continue
			}
			authed = true
			say("235 ok")
		case strings.HasPrefix(upper, "MAIL FROM:"):
			msg = smtpMessage{from: strings.Trim(cmd[len("MAIL FROM:"):], "<> ")}
			say("250 ok")
		case strings.HasPrefix(upper, "RCPT TO:"):
			msg.to = append(msg.to, strings.Trim(cmd[len("RCPT TO:"):], "<> "))
			say("250 ok")
		case upper == "DATA":
			say("354 go on")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			msg.data = b.String()
			s.mu.Lock()
			s.messages = append(s.messages, msg)
			s.usedTLS, s.authed = usedTLS, authed
			s.mu.Unlock()
			say("250 queued")
		case upper == "QUIT":
			say("221 bye")
			return
		default:
			say("502 unknown")
		}
	}
}

func TestEmail(t *testing.T) {
	srv := newSMTPServer(t, nil)
	s := newTestService(t, Options{SMTP: config.SMTP{Host: "127.0.0.1", Port: srv.port(), From: "alerts@agency.example",
		AllowedDomains: []string{"agency.example"}}})
	in := Integration{Name: "team", Type: TypeEmail, Enabled: true, Recipients: []string{"ana@agency.example", "ops@agency.example"}}
	if err := s.Check(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	m := testMessage()
	m.Title = "Igloo alert pacing\r\nBcc: someone@else.example"
	if _, err := s.Send(context.Background(), in, m); err != nil {
		t.Fatal(err)
	}
	got := srv.got()
	if len(got) != 1 || got[0].from != "alerts@agency.example" || strings.Join(got[0].to, ",") != "ana@agency.example,ops@agency.example" {
		t.Fatalf("messages: %+v", got)
	}
	head, body, _ := strings.Cut(got[0].data, "\r\n\r\n")
	if strings.Contains(head, "\r\nBcc:") {
		t.Errorf("the title added a header:\n%s", head)
	}
	var subject string
	for _, l := range strings.Split(head, "\r\n") {
		if v, ok := strings.CutPrefix(l, "Subject: "); ok {
			subject, _ = new(mime.WordDecoder).DecodeHeader(v)
		}
	}
	if !strings.HasPrefix(subject, "Igloo alert pacing") {
		t.Errorf("subject %q", subject)
	}
	if !strings.Contains(body, "Spend is off plan for acme") || !strings.Contains(body, "client | spend") {
		t.Errorf("body:\n%s", body)
	}

	// Recipients outside the allowed domains are refused.
	bad := Integration{Name: "x", Type: TypeEmail, Enabled: true, Recipients: []string{"someone@gmail.example"}}
	if err := s.Check(context.Background(), bad); err == nil {
		t.Error("a recipient outside the allowed domains was accepted")
	}
	for _, r := range []string{"Ana <ana@agency.example>", "ana@agency.example\r\nRCPT TO:<x@y>", "not-an-address"} {
		if err := s.Check(context.Background(), Integration{Type: TypeEmail, Recipients: []string{r}}); err == nil {
			t.Errorf("%q accepted", r)
		}
	}
}

// TestEmailNeedsTLS checks that mail goes to a server that is not allowed
// only over STARTTLS, and that the password is never in errors.
func TestEmailNeedsTLS(t *testing.T) {
	cert := httptest.NewTLSServer(nil)
	defer cert.Close()
	plainSrv := newSMTPServer(t, nil)
	tlsSrv := newSMTPServer(t, cert.TLS)
	r := newResolver(map[string][][]string{"example.com": {{"93.184.215.14"}}})
	in := Integration{Name: "team", Type: TypeEmail, Enabled: true, Recipients: []string{"ana@agency.example"}}

	service := func(port int, password string) *Service {
		s := newTestService(t, Options{AllowHosts: []string{}, Resolver: r,
			SMTP: config.SMTP{Host: "example.com", Port: port, From: "alerts@agency.example", Username: "alerts",
				Password: password, AllowedDomains: []string{"agency.example"}}})
		pool := x509.NewCertPool()
		pool.AddCert(cert.Certificate())
		s.tlsConfig.RootCAs = pool
		s.guard.dial = func(ctx context.Context, network, _ string, _ bool) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		}
		return s
	}

	_, err := service(plainSrv.port(), "smtp-password-123").Send(context.Background(), in, testMessage())
	if err == nil || !strings.Contains(err.Error(), "STARTTLS") {
		t.Errorf("a server without STARTTLS: %v", err)
	}
	if n := len(plainSrv.got()); n != 0 {
		t.Errorf("%d messages went without TLS", n)
	}

	if _, err := service(tlsSrv.port(), "smtp-password-123").Send(context.Background(), in, testMessage()); err != nil {
		t.Fatal(err)
	}
	tlsSrv.mu.Lock()
	usedTLS, authed := tlsSrv.usedTLS, tlsSrv.authed
	tlsSrv.mu.Unlock()
	if len(tlsSrv.got()) != 1 || !usedTLS || !authed {
		t.Errorf("over STARTTLS: %d messages, TLS %v, authenticated %v", len(tlsSrv.got()), usedTLS, authed)
	}

	attempts, err := service(tlsSrv.port(), "wrong-password-456").Send(context.Background(), in, testMessage())
	if err == nil || attempts != 1 || strings.Contains(err.Error(), "wrong-password-456") || !strings.Contains(err.Error(), "535") {
		t.Errorf("a wrong password: %d attempts, %v", attempts, err)
	}
}
