package pgwire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/xdg-go/scram"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Authentication message codes from the PostgreSQL protocol.
const (
	authOK           = 0
	authSASL         = 10
	authSASLContinue = 11
	authSASLFinal    = 12
)

const saslMechanism = "SCRAM-SHA-256"

// failedLoginDelay slows down password guessing.
const failedLoginDelay = 500 * time.Millisecond

// authenticate runs a SCRAM-SHA-256 exchange, the PostgreSQL default, so the
// password never crosses the network. On success the returned context
// carries a new session for the user.
func (s *Server) authenticate(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (context.Context, error) {
	name := wire.ClientParameters(ctx)[wire.ParamUsername]

	user, err := s.users.Get(ctx, name)
	known := err == nil
	if err != nil && !errors.Is(err, auth.ErrNotFound) {
		return ctx, fmt.Errorf("look up user: %w", err)
	}

	// Unknown users go through the same exchange against mock credentials
	// and fail at the end, like a wrong password.
	creds := s.mockCredentials(name)
	if known {
		if creds, err = auth.ParseVerifier(user.Verifier); err != nil {
			s.log.Error("stored password verifier is invalid", "user", name)
			creds, known = s.mockCredentials(name), false
		}
	}

	server, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) { return creds, nil })
	if err != nil {
		return ctx, err
	}
	conv := server.NewConversation()

	if err := writeAuth(writer, authSASL, []byte(saslMechanism+"\x00\x00")); err != nil {
		return ctx, err
	}

	// SASLInitialResponse: mechanism name, then the length-prefixed client-first message.
	if err := expectPasswordMessage(reader); err != nil {
		return ctx, err
	}
	mechanism, err := reader.GetString()
	if err != nil {
		return ctx, err
	}
	if mechanism != saslMechanism {
		return s.loginFailed(ctx, writer, name, fmt.Errorf("unsupported SASL mechanism %q", mechanism), "unsupported authentication method")
	}
	n, err := reader.GetInt32()
	if err != nil {
		return ctx, err
	}
	// The length comes from the client; anything but a length that fits the
	// message is malformed (the protocol library panics on negative ones).
	if n < 0 || int(n) > len(reader.Msg) {
		return s.loginFailed(ctx, writer, name, errors.New("malformed SASL initial response"), "malformed authentication message")
	}
	clientFirst, err := reader.GetBytes(int(n))
	if err != nil {
		return ctx, err
	}

	serverFirst, err := conv.Step(string(clientFirst))
	if err != nil {
		return s.loginFailed(ctx, writer, name, err, "malformed authentication message")
	}
	if err := writeAuth(writer, authSASLContinue, []byte(serverFirst)); err != nil {
		return ctx, err
	}

	// SASLResponse: the client-final message is the whole body.
	if err := expectPasswordMessage(reader); err != nil {
		return ctx, err
	}
	serverFinal, err := conv.Step(string(reader.Msg))
	if err != nil || !conv.Valid() || !known {
		reason := "wrong password"
		if !known {
			reason = "unknown user"
		}
		return s.loginFailed(ctx, writer, name, err, reason)
	}
	if err := writeAuth(writer, authSASLFinal, []byte(serverFinal)); err != nil {
		return ctx, err
	}
	if err := writeAuth(writer, authOK, nil); err != nil {
		return ctx, err
	}

	if !s.acquireConn() {
		err := pgerror.WithSeverity(pgerror.WithCode(
			errors.New("sorry, too many clients already"), codes.TooManyConnections), pgerror.LevelFatal)
		s.log.Warn("connection refused: too many sessions", "user", name, "max", s.maxConns)
		s.recordLogin(name, wire.RemoteAddress(ctx), wire.ClientParameters(ctx)[wire.ParamApplicationName], nil, "too many connections")
		if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
			return ctx, werr
		}
		return ctx, err
	}

	addr := wire.RemoteAddress(ctx)
	sess := &session{
		user:     user,
		checked:  time.Now(),
		timeout:  s.timeout,
		activity: s.activity,
		open:     make(map[*wire.PreparedStatement]*statement),
		appName:  wire.ClientParameters(ctx)[wire.ParamApplicationName],
	}
	if addr != nil {
		sess.client = addr.String()
	}
	s.loginSucceededSession(user, addr, sess)
	return context.WithValue(ctx, sessionKey{}, sess), nil
}

// loginSucceeded counts, logs and records in the login history a login from
// addr, which may be nil. Logins through the web console use it too; attrs
// say so.
func (s *Server) loginSucceeded(user auth.User, addr net.Addr, attrs ...any) {
	s.loginSucceededSession(user, addr, nil, attrs...)
}

// loginSucceededSession is loginSucceeded for a login that opened sess.
func (s *Server) loginSucceededSession(user auth.User, addr net.Addr, sess *session, attrs ...any) {
	s.metrics.login(true)
	s.log.Info("login", append([]any{"user", user.Name, "role", user.Role, "addr", addr}, attrs...)...)
	appName := clientAttr(attrs)
	if sess != nil {
		appName = sess.applicationName()
	}
	s.recordLogin(user.Name, addr, appName, sess, "")
}

// loginRejected slows down password guessing, then counts, logs and records
// a failed login. Logins through the web console use it too; attrs say so.
func (s *Server) loginRejected(name string, addr net.Addr, cause error, attrs ...any) {
	reason := "wrong password"
	if cause != nil {
		reason = cause.Error()
	}
	s.loginRejectedReason(name, addr, cause, reason, attrs...)
}

// loginRejectedReason is loginRejected with the reason the login history
// gives, which says why without detail a client could use.
func (s *Server) loginRejectedReason(name string, addr net.Addr, cause error, reason string, attrs ...any) {
	time.Sleep(failedLoginDelay)
	s.metrics.login(false)
	s.recordLogin(name, addr, clientAttr(attrs), nil, reason)
	attrs = append([]any{"user", name, "addr", addr}, attrs...)
	if cause != nil {
		attrs = append(attrs, "reason", cause)
	}
	s.log.Warn("login failed", attrs...)
}

// clientAttr returns the "client" of log attributes, such as "console".
func clientAttr(attrs []any) string {
	for i := 0; i+1 < len(attrs); i += 2 {
		if k, _ := attrs[i].(string); k == "client" {
			v, _ := attrs[i+1].(string)
			return v
		}
	}
	return ""
}

// loginFailed refuses a login. reason is for the login history.
func (s *Server) loginFailed(ctx context.Context, writer *buffer.Writer, name string, cause error, reason string) (context.Context, error) {
	s.loginRejectedReason(name, wire.RemoteAddress(ctx), cause, reason)

	err := pgerror.WithSeverity(pgerror.WithCode(
		fmt.Errorf("password authentication failed for user %q", name), codes.InvalidPassword),
		pgerror.LevelFatal)
	if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
		return ctx, werr
	}
	return ctx, err
}

func writeAuth(w *buffer.Writer, code int32, data []byte) error {
	w.Start(types.ServerAuth)
	w.AddInt32(code)
	if data != nil {
		w.AddBytes(data)
	}
	return w.End()
}

func expectPasswordMessage(reader *buffer.Reader) error {
	t, _, err := reader.ReadTypedMsg()
	if err != nil {
		return err
	}
	if t != types.ClientPassword {
		return fmt.Errorf("expected a SASL response, got message type %q", t)
	}
	return nil
}

// mockCredentials are what a login for an unknown user is checked against.
// They are derived from the user name and a secret shared by all replicas, so
// an unknown user gets the same salt every time, just like a real one, and
// probing salts cannot tell the two apart.
func (s *Server) mockCredentials(name string) scram.StoredCredentials {
	derive := func(purpose string) []byte {
		mac := hmac.New(sha256.New, s.mockSecret)
		mac.Write([]byte(purpose + "\x00" + name))
		return mac.Sum(nil)
	}
	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{Salt: string(derive("salt")[:16]), Iters: 4096},
		StoredKey:  derive("stored-key"),
		ServerKey:  derive("server-key"),
	}
}
