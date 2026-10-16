package notify

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// sendEmail sends m to in's recipients through the configured mail
// server: over TLS from the start on port 465, and otherwise with
// STARTTLS, which is required unless the allow list names the server.
func (s *Service) sendEmail(ctx context.Context, in Integration, m Message) error {
	cfg := s.smtp
	fail := func(temporary bool, format string, args ...any) *SendError {
		msg := scrub(fmt.Sprintf(format, args...), []string{cfg.Password})
		return &SendError{Integration: in.Name, Msg: msg, Temporary: temporary}
	}
	if !cfg.Enabled() {
		return fail(false, "no mail server is configured (IGLOO_SMTP_HOST)")
	}
	if err := s.checkRecipients(in.Recipients); err != nil {
		return fail(false, "%v", err)
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	conn, err := s.guard.DialContext(ctx, "tcp", addr)
	if err != nil {
		var ae *AddressError
		return fail(!errors.As(err, &ae), "%v", err)
	}
	defer conn.Close() //nolint:errcheck
	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)

	tlsConfig := s.tlsConfig.Clone()
	tlsConfig.ServerName = cfg.Host
	if cfg.Port == 465 {
		tc := tls.Client(conn, tlsConfig)
		if err := tc.HandshakeContext(ctx); err != nil {
			return fail(true, "TLS with %s: %v", addr, err)
		}
		conn = tc
	}
	c, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		return smtpError(fail, addr, err)
	}
	defer c.Close() //nolint:errcheck
	if err := c.Hello("igloo"); err != nil {
		return smtpError(fail, addr, err)
	}
	if cfg.Port != 465 {
		if ok, _ := c.Extension("STARTTLS"); ok {
			if err := c.StartTLS(tlsConfig); err != nil {
				return fail(true, "STARTTLS with %s: %v", addr, err)
			}
		} else if !s.guard.Allowed(cfg.Host) {
			return fail(false, "the mail server %s does not offer STARTTLS; Igloo sends mail without TLS only to hosts in "+
				"IGLOO_NOTIFICATION_ALLOW_HOSTS", addr)
		}
	}
	if cfg.Username != "" {
		if err := c.Auth(&plainAuth{username: cfg.Username, password: cfg.Password}); err != nil {
			return smtpError(fail, addr, err)
		}
	}
	if err := c.Mail(cfg.From); err != nil {
		return smtpError(fail, addr, err)
	}
	for _, r := range in.Recipients {
		if err := c.Rcpt(r); err != nil {
			return smtpError(fail, addr, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return smtpError(fail, addr, err)
	}
	if _, err := w.Write(emailMessage(cfg.From, in.Recipients, m)); err != nil {
		return smtpError(fail, addr, err)
	}
	if err := w.Close(); err != nil {
		return smtpError(fail, addr, err)
	}
	_ = c.Quit()
	return nil
}

// smtpError tells temporary failures (network errors, 4xx replies) from
// permanent ones (5xx).
func smtpError(fail func(bool, string, ...any) *SendError, addr string, err error) error {
	var te *textproto.Error
	if errors.As(err, &te) {
		return fail(te.Code < 500, "the mail server %s answered %d %s", addr, te.Code, te.Msg)
	}
	return fail(true, "mail server %s: %v", addr, err)
}

// emailMessage is the message, headers and body, for recipients.
func emailMessage(from string, recipients []string, m Message) []byte {
	subject, body := emailContent(m)
	var id [12]byte
	_, _ = rand.Read(id[:])
	var b strings.Builder
	header := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	header("From", from)
	header("To", strings.Join(recipients, ", "))
	header("Subject", mime.QEncoding.Encode("utf-8", subject))
	header("Date", m.Time.UTC().Format(time.RFC1123Z))
	_, domain, _ := strings.Cut(from, "@")
	header("Message-ID", "<"+hex.EncodeToString(id[:])+"@"+domain+">")
	header("MIME-Version", "1.0")
	header("Content-Type", "text/plain; charset=utf-8")
	header("Content-Transfer-Encoding", "quoted-printable")
	header("Auto-Submitted", "auto-generated")
	b.WriteString("\r\n")
	qp := quotedprintable.NewWriter(&b)
	_, _ = qp.Write([]byte(strings.ReplaceAll(body, "\n", "\r\n")))
	_ = qp.Close()
	return []byte(b.String())
}

// plainAuth is SMTP's PLAIN authentication, which sends the password.
// sendEmail uses it inside TLS, or with a server the allow list names;
// net/smtp's own PlainAuth would refuse the latter.
type plainAuth struct{ username, password string }

func (a *plainAuth) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "PLAIN", []byte("\x00" + a.username + "\x00" + a.password), nil
}

func (a *plainAuth) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return nil, errors.New("unexpected challenge from the mail server")
	}
	return nil, nil
}
