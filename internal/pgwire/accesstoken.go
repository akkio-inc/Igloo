package pgwire

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
)

// A user without a password who signs in through single sign-on may also
// sign in to SQL with an access token their identity provider issued for
// Igloo, when the server is configured to accept them (AccessTokens). SCRAM
// never shows the server the secret, which a token from elsewhere needs, so
// for those users, and only on connections encrypted with TLS, the server
// asks for the password in clear text instead. Igloo's own access tokens
// work there too, checked against their hash.
//
// Which method a login asks for therefore tells a client that a user
// without a password exists. Unknown users get SCRAM, as users with a
// password do, so no password user is revealed: those are the accounts
// worth guessing at, and keeping their logins on SCRAM is the point.

// AccessTokenVerifier checks an identity provider's access tokens.
type AccessTokenVerifier interface {
	// VerifyAccessToken checks that token is a valid access token for
	// Igloo, signed by the identity provider and unexpired, and belongs to
	// user.
	VerifyAccessToken(ctx context.Context, user auth.User, token string) error
}

// acceptsAccessTokens reports whether user's login asks for a clear-text
// password, to accept an access token from the identity provider.
func (s *Server) acceptsAccessTokens(ctx context.Context, user auth.User) bool {
	return s.accessTokens != nil && s.tokenChecker != nil && !user.HasPassword() && !user.IsService() && wire.IsTLS(ctx)
}

// authenticateCleartext asks for a password in clear text, inside TLS, and
// accepts an Igloo access token or an identity provider's access token of
// user, even when the failed-login limits block passwords.
func (s *Server) authenticateCleartext(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader, user auth.User, lim limits) (context.Context, error) {
	failed := func(cause error, reason string, how login) (context.Context, error) {
		if lim.blocked() {
			return s.loginRefused(ctx, writer, user.Name, "too many failed logins", how, lim.message(user.Name))
		}
		return s.loginFailed(ctx, writer, user.Name, cause, reason, how)
	}
	if err := writeAuth(writer, authCleartextPassword, nil); err != nil {
		return ctx, err
	}
	if err := expectPasswordMessage(reader); err != nil {
		return ctx, err
	}
	secret, err := reader.GetString()
	if err != nil {
		return ctx, err
	}

	if auth.LooksLikeToken(secret) {
		how := login{method: audit.MethodToken}
		u, token, err := s.tokenChecker.Verify(ctx, secret)
		if err == nil && u.Name != user.Name {
			err = fmt.Errorf("%w: the token is another user's", auth.ErrInvalidToken)
		}
		if errors.Is(err, auth.ErrInvalidToken) {
			return failed(err, "invalid access token", how)
		}
		if err != nil {
			return ctx, fmt.Errorf("check access token: %w", err)
		}
		how.token = token.Name
		return s.loginAccepted(ctx, writer, u, &token, how)
	}

	how := login{method: audit.MethodAccessToken}
	if err := s.accessTokens.VerifyAccessToken(ctx, user, secret); err != nil {
		return failed(err, "invalid identity provider access token", how)
	}
	if user.Disabled {
		return s.loginRefused(ctx, writer, user.Name, "user is disabled", how,
			fmt.Errorf("user %q is not permitted to log in", user.Name))
	}
	return s.loginAccepted(ctx, writer, user, nil, how)
}
