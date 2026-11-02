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

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// Authentication message codes from the PostgreSQL protocol.
const (
	authOK                = 0
	authCleartextPassword = 3
	authSASL              = 10
	authSASLContinue      = 11
	authSASLFinal         = 12
)

const saslMechanism = "SCRAM-SHA-256"

// failedLoginDelay slows down password guessing.
const failedLoginDelay = 500 * time.Millisecond

// login describes a login attempt for the log and the history.
type login struct {
	// method is one of audit's Method constants.
	method string
	// token names the access token the login used.
	token string
	// attrs are extra log attributes, such as client=console.
	attrs []any
}

// authenticate runs a SCRAM-SHA-256 exchange, the PostgreSQL default, so no
// secret crosses the network. The client may prove it knows the user's
// password or any of their access tokens (see auth.SCRAMExchange). On
// success the returned context carries a new session for the user.
func (s *Server) authenticate(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (context.Context, error) {
	name := wire.ClientParameters(ctx)[wire.ParamUsername]
	addr := wire.RemoteAddress(ctx)

	// After too many failures, only access tokens sign in (see
	// ratelimit.go).
	done, wait := s.limiter.begin(ctx, name, addr)
	defer done()
	lim := limits{wait: wait}
	// fail ends a login that went wrong. While passwords are blocked, it
	// is refused without counting as another failure: a blocked client
	// must not add to the counts, nor raise more failed-login alerts.
	fail := func(cause error, reason string) (context.Context, error) {
		if lim.blocked() {
			return s.loginRefused(ctx, writer, name, "too many failed logins", login{}, lim.message(name))
		}
		return s.loginFailed(ctx, writer, name, cause, reason, login{})
	}

	user, err := s.users.Get(ctx, name)
	known := err == nil
	if err != nil && !errors.Is(err, auth.ErrNotFound) {
		return ctx, fmt.Errorf("look up user: %w", err)
	}
	if known && s.acceptsAccessTokens(ctx, user) {
		return s.authenticateCleartext(ctx, writer, reader, user, lim)
	}

	// Unknown users go through the same exchange against mock credentials
	// and fail at the end, like a wrong password.
	candidates, tokens, err := s.loginSecrets(ctx, name, user, known, !lim.blocked())
	if err != nil {
		return ctx, err
	}
	x, err := auth.NewSCRAMExchange(candidates)
	if err != nil {
		return ctx, err
	}

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
		return fail(fmt.Errorf("unsupported SASL mechanism %q", mechanism), "unsupported authentication method")
	}
	n, err := reader.GetInt32()
	if err != nil {
		return ctx, err
	}
	// The length comes from the client; anything but a length that fits the
	// message is malformed (the protocol library panics on negative ones).
	if n < 0 || int(n) > len(reader.Msg) {
		return fail(errors.New("malformed SASL initial response"), "malformed authentication message")
	}
	clientFirst, err := reader.GetBytes(int(n))
	if err != nil {
		return ctx, err
	}

	serverFirst, err := x.First(string(clientFirst))
	if err != nil {
		return fail(err, "malformed authentication message")
	}
	if err := writeAuth(writer, authSASLContinue, []byte(serverFirst)); err != nil {
		return ctx, err
	}

	// SASLResponse: the client-final message is the whole body.
	if err := expectPasswordMessage(reader); err != nil {
		return ctx, err
	}
	serverFinal, err := x.Final(string(reader.Msg))
	matched := x.Matched()
	if err != nil || matched < 0 || !known {
		reason := "wrong password"
		switch {
		case !known:
			reason = "unknown user"
		case len(tokens) > 0:
			reason = "wrong password or access token"
		}
		return fail(err, reason)
	}
	how := login{method: audit.MethodPassword}
	var token *auth.Token
	if first := len(candidates) - len(tokens); matched >= first {
		token = &tokens[matched-first].Token
		how = login{method: audit.MethodToken, token: token.Name}
	}
	if err := writeAuth(writer, authSASLFinal, []byte(serverFinal)); err != nil {
		return ctx, err
	}
	// As PostgreSQL does, say that a user may not log in only to a client
	// that proved it knows their secret.
	if user.Disabled {
		return s.loginRefused(ctx, writer, name, "user is disabled", how,
			fmt.Errorf("user %q is not permitted to log in", name))
	}
	return s.loginAccepted(ctx, writer, user, token, how)
}

// loginAccepted finishes a successful login: it opens a session for user,
// who signed in with token if it is not nil.
func (s *Server) loginAccepted(ctx context.Context, writer *buffer.Writer, user auth.User, token *auth.Token, how login) (context.Context, error) {
	if err := writeAuth(writer, authOK, nil); err != nil {
		return ctx, err
	}
	s.limiter.succeeded(user.Name)

	// The connection may choose its SQL dialect, as a startup parameter.
	dialect, err := startupDialect(wire.ClientParameters(ctx))
	if err != nil {
		err = pgerror.WithSeverity(err, pgerror.LevelFatal)
		if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
			return ctx, werr
		}
		return ctx, err
	}

	if !s.acquireConn() {
		err := pgerror.WithSeverity(pgerror.WithCode(
			errors.New("sorry, too many clients already"), codes.TooManyConnections), pgerror.LevelFatal)
		s.log.Warn("connection refused: too many sessions", "user", user.Name, "max", s.maxConns)
		s.recordLogin(user.Name, wire.RemoteAddress(ctx), wire.ClientParameters(ctx)[wire.ParamApplicationName], nil, "too many connections", how)
		if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
			return ctx, werr
		}
		return ctx, err
	}
	// Holding a session, the connection is no longer signing in (see
	// startup.go).
	if c, ok := wire.AcceptedConn(ctx).(*startupConn); ok {
		c.signedIn()
	}

	addr := wire.RemoteAddress(ctx)
	sess := &session{
		user:     user,
		checked:  time.Now(),
		timeout:  s.timeout,
		activity: s.activity,
		stuck:    s.stuck,
		socket:   clientSocket(wire.AcceptedConn(ctx)),
		open:     make(map[*wire.PreparedStatement]*statement),
		appName:  wire.ClientParameters(ctx)[wire.ParamApplicationName],

		startDialect: dialect,
	}
	if addr != nil {
		sess.client = addr.String()
	}
	sess.method = how.method
	if token != nil {
		sess.tokenID, sess.tokenName, sess.tokenExpires = token.ID, token.Name, token.ExpiresAt
		s.tokenChecker.Used(token.ID)
	}
	s.loginSucceededSession(user, addr, sess, how)
	return context.WithValue(ctx, sessionKey{}, sess), nil
}

// limits is what the failed-login limits allow a login.
type limits struct {
	// wait, if not zero, is how long passwords may not be used.
	wait time.Duration
}

func (l limits) blocked() bool { return l.wait > 0 }

// message is what a client blocked by the limits is told.
func (l limits) message(name string) error {
	return fmt.Errorf("too many failed logins for user %q or from this address; try again in %s, or sign in with an access token",
		name, roundUp(l.wait))
}

// loginSecrets returns what a SCRAM login as name may prove it knows, all
// with one salt: the user's password, if password is set and they may use
// it, and their unexpired access tokens, which come last in candidates.
// Unknown users, and users with neither, get mock credentials nothing
// matches.
func (s *Server) loginSecrets(ctx context.Context, name string, user auth.User, known, password bool) ([]scram.StoredCredentials, []auth.TokenCredential, error) {
	factors, ok := auth.TokenFactors(user)
	if !known || !ok {
		return []scram.StoredCredentials{s.mockCredentials(name)}, nil, nil
	}
	var candidates []scram.StoredCredentials
	if user.HasPassword() && password {
		allowed, err := s.passwordAllowed(ctx, user)
		if err != nil {
			return nil, nil, err
		}
		creds, err := auth.ParseVerifier(user.Verifier)
		if err != nil {
			s.log.Error("stored password verifier is invalid", "user", name)
		} else if allowed {
			candidates = append(candidates, creds)
		}
	}
	var tokens []auth.TokenCredential
	if s.tokens != nil {
		all, err := s.tokens.TokenCredentials(ctx, name)
		if err != nil {
			return nil, nil, fmt.Errorf("look up access tokens: %w", err)
		}
		for _, t := range all {
			// Tokens made before a password change that picked a new
			// salt cannot be checked in this exchange.
			if t.SCRAM.KeyFactors == factors {
				tokens = append(tokens, t)
				candidates = append(candidates, t.SCRAM)
			}
		}
	}
	if len(candidates) == 0 {
		mock := s.mockCredentials(name)
		mock.KeyFactors = factors
		return []scram.StoredCredentials{mock}, nil, nil
	}
	return candidates, tokens, nil
}

// passwordAllowed reports whether user may sign in with their password:
// service accounts may not, nor, with DisableSSOPasswords, users linked to
// single sign-on.
func (s *Server) passwordAllowed(ctx context.Context, user auth.User) (bool, error) {
	if user.IsService() {
		return false, nil
	}
	if !s.disableSSOPasswords || s.sso == nil {
		return true, nil
	}
	_, err := s.sso.IdentityOf(ctx, user.Name)
	if errors.Is(err, auth.ErrIdentityNotFound) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up single sign-on: %w", err)
	}
	return false, nil
}

// loginSucceeded counts, logs and records in the login history a login from
// addr, which may be nil. Logins through the web console use it too; how
// says so.
func (s *Server) loginSucceeded(user auth.User, addr net.Addr, how login) {
	s.loginSucceededSession(user, addr, nil, how)
}

// loginSucceededSession is loginSucceeded for a login that opened sess.
func (s *Server) loginSucceededSession(user auth.User, addr net.Addr, sess *session, how login) {
	s.metrics.login(true)
	attrs := append([]any{"user", user.Name, "role", user.Role, "addr", addr}, how.attrs...)
	if how.method != "" {
		attrs = append(attrs, "method", how.method)
	}
	if how.token != "" {
		attrs = append(attrs, "token", how.token)
	}
	s.log.Info("login", attrs...)
	appName := clientAttr(how.attrs)
	if sess != nil {
		appName = sess.applicationName()
	}
	s.recordLogin(user.Name, addr, appName, sess, "", how)
}

// loginRejected slows down password guessing, then counts, logs and records
// a failed login, which counts towards the limits on failures. reason is
// for the login history, and says why without detail a client could use.
func (s *Server) loginRejected(name string, addr net.Addr, cause error, reason string, how login) {
	time.Sleep(failedLoginDelay)
	if user, address := s.limiter.failed(name, addr); user || address {
		s.notifyLoginsBlocked(name, addr, user, address)
	}
	s.metrics.login(false)
	s.recordLogin(name, addr, clientAttr(how.attrs), nil, reason, how)
	attrs := append([]any{"user", name, "addr", addr}, how.attrs...)
	if how.method != "" {
		attrs = append(attrs, "method", how.method)
	}
	if cause != nil {
		attrs = append(attrs, "reason", cause)
	} else {
		attrs = append(attrs, "reason", reason)
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
func (s *Server) loginFailed(ctx context.Context, writer *buffer.Writer, name string, cause error, reason string, how login) (context.Context, error) {
	s.loginRejected(name, wire.RemoteAddress(ctx), cause, reason, how)

	err := pgerror.WithSeverity(pgerror.WithCode(
		fmt.Errorf("password authentication failed for user %q", name), codes.InvalidPassword),
		pgerror.LevelFatal)
	if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
		return ctx, werr
	}
	return ctx, err
}

// loginRefused refuses a login without counting it as a guess: one that
// came too soon after too many failures, or of a disabled user. message
// is what the client is told.
func (s *Server) loginRefused(ctx context.Context, writer *buffer.Writer, name, reason string, how login, message error) (context.Context, error) {
	addr := wire.RemoteAddress(ctx)
	s.metrics.login(false)
	s.recordLogin(name, addr, wire.ClientParameters(ctx)[wire.ParamApplicationName], nil, reason, how)
	s.log.Warn("login refused", "user", name, "addr", addr, "reason", reason)

	err := pgerror.WithSeverity(pgerror.WithCode(message, codes.InvalidAuthorizationSpecification), pgerror.LevelFatal)
	if werr := wire.WriteUnterminatedError(writer, err); werr != nil {
		return ctx, werr
	}
	return ctx, err
}

// roundUp renders a wait in whole minutes, or seconds under a minute.
func roundUp(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds())+1)
	}
	return fmt.Sprintf("%d minutes", int(d.Minutes())+1)
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
