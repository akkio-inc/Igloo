package pgwire

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
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
		return s.loginFailed(ctx, writer, name, fmt.Errorf("unsupported SASL mechanism %q", mechanism))
	}
	n, err := reader.GetInt32()
	if err != nil {
		return ctx, err
	}
	// The length comes from the client; anything but a length that fits the
	// message is malformed (the protocol library panics on negative ones).
	if n < 0 || int(n) > len(reader.Msg) {
		return s.loginFailed(ctx, writer, name, errors.New("malformed SASL initial response"))
	}
	clientFirst, err := reader.GetBytes(int(n))
	if err != nil {
		return ctx, err
	}

	serverFirst, err := conv.Step(string(clientFirst))
	if err != nil {
		return s.loginFailed(ctx, writer, name, err)
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
		return s.loginFailed(ctx, writer, name, err)
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
		if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
			return ctx, werr
		}
		return ctx, err
	}

	s.metrics.login(true)
	s.log.Info("login", "user", name, "role", user.Role, "addr", wire.RemoteAddress(ctx))

	sess := &session{
		user:     user,
		checked:  time.Now(),
		timeout:  s.timeout,
		activity: s.activity,
		open:     make(map[*wire.PreparedStatement]*statement),
	}
	return context.WithValue(ctx, sessionKey{}, sess), nil
}

func (s *Server) loginFailed(ctx context.Context, writer *buffer.Writer, name string, cause error) (context.Context, error) {
	time.Sleep(failedLoginDelay)
	s.metrics.login(false)
	attrs := []any{"user", name, "addr", wire.RemoteAddress(ctx)}
	if cause != nil {
		attrs = append(attrs, "reason", cause)
	}
	s.log.Warn("login failed", attrs...)

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
