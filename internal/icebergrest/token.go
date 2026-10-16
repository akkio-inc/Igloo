package icebergrest

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Engines sign in with the OAuth2 client-credentials grant of the Iceberg
// REST specification: an Igloo user name as client_id and its password as
// client_secret, which stock Spark, Trino and PyIceberg configurations send
// as credential=user:password, or an Igloo access token (igloo_...) as
// client_secret, with or without the user name. Service accounts have no
// password, so jobs sign in with tokens. Igloo answers with an access token
// of its own: the user, a session and an expiry, encrypted and
// authenticated with a key every replica shares, so any replica accepts it
// and nothing needs storing. A token ends after tokenLifetime, or at once
// when its user is dropped or disabled, their password changes (one made
// from a password), or the access token it was made from is dropped or
// expires (one made from that), which it never outlives. Clients renew it
// before then, with their credentials or by exchanging the token itself
// (RFC 8693).

// failedTokenDelay slows down guessing tokens, as SQL logins slow down
// guessing passwords. Tests shorten it.
var failedTokenDelay = 500 * time.Millisecond

// At most maxDelayed failed requests wait out the delay at once, and at
// most maxSignIns token requests, each of which may hash a password, run
// at once; more are refused as busy, so guessing cannot tie up the server.
const (
	maxDelayed = 64
	maxSignIns = 32
)

// slowDown delays a failed request, or reports false at once when too many
// already wait.
func (s *Server) slowDown() bool {
	select {
	case s.delayed <- struct{}{}:
	default:
		return false
	}
	time.Sleep(failedTokenDelay)
	<-s.delayed
	return true
}

// tooBusy is the token endpoint's answer when slowDown or the sign-in limit
// refuses a request.
var tooBusy = &oauthError{status: http.StatusServiceUnavailable, code: "temporarily_unavailable",
	desc: "too many sign-ins at once; try again shortly", retryAfter: time.Second}

const (
	tokenLifetime = time.Hour
	// maxTokenRequest bounds a token request's form.
	maxTokenRequest = 16 << 10

	grantClientCredentials = "client_credentials"
	grantTokenExchange     = "urn:ietf:params:oauth:grant-type:token-exchange"
	accessTokenType        = "urn:ietf:params:oauth:token-type:access_token"
)

// token is what an access token holds.
type token struct {
	User string `json:"u"`
	// Session ties a token and its renewals together in the history.
	Session string `json:"s"`
	// Expires is a Unix time.
	Expires int64 `json:"e"`
	// Password fingerprints the user's verifier when the session began, so
	// changing the password ends it; empty for a session that began with an
	// access token.
	Password string `json:"p,omitempty"`
	// TokenID is the Igloo access token the session began with, which it
	// stands and falls with.
	TokenID string `json:"t,omitempty"`
}

// signedIn is a session a token request began or renewed.
type signedIn struct {
	user    auth.User
	session string
	tokenID string
	expires time.Time
}

// sealer encrypts and authenticates tokens with AES-256-GCM.
type sealer struct {
	aead cipher.AEAD
	// mac fingerprints password verifiers.
	mac []byte
}

// tokenAD binds sealed tokens to their purpose.
var tokenAD = []byte("igloo-iceberg-token")

func newSealer(secret []byte) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("icebergrest: the token key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo iceberg access token v1", 32)
	if err != nil {
		return nil, err
	}
	mac, err := hkdf.Key(sha256.New, secret, nil, "igloo iceberg password fingerprint v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead, mac: mac}, nil
}

func (s *sealer) seal(t token) (string, error) {
	plain, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(s.aead.Seal(nonce, nonce, plain, tokenAD)), nil
}

// open returns the token value seals, if it is genuine and has not expired.
func (s *sealer) open(value string) (token, error) {
	var t token
	raw, err := base64.RawURLEncoding.Strict().DecodeString(value)
	if err != nil || len(raw) < s.aead.NonceSize() {
		return t, errors.New("malformed token")
	}
	n := s.aead.NonceSize()
	plain, err := s.aead.Open(nil, raw[:n], raw[n:], tokenAD)
	if err != nil {
		return t, errors.New("token does not verify")
	}
	if err := json.Unmarshal(plain, &t); err != nil {
		return t, err
	}
	if time.Now().Unix() >= t.Expires {
		return t, errors.New("token expired")
	}
	return t, nil
}

// fingerprint identifies a password verifier without revealing it.
func (s *sealer) fingerprint(verifier string) string {
	m := hmac.New(sha256.New, s.mac)
	m.Write([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:16])
}

// current reports whether t was issued for user as they are now: same
// password.
func (s *sealer) current(t token, user auth.User) bool {
	return subtle.ConstantTimeCompare([]byte(s.fingerprint(user.Verifier)), []byte(t.Password)) == 1
}

func newSession() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// oauthError is an error response of the token endpoint (RFC 6749).
type oauthError struct {
	status int
	code   string
	desc   string
	// retryAfter, if set, says when to try again.
	retryAfter time.Duration
}

// token serves POST /v1/oauth/tokens.
func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Pragma", "no-cache")
	var (
		in   signedIn
		oerr *oauthError
	)
	select {
	case s.signIns <- struct{}{}:
		in, oerr = s.grant(w, r)
		<-s.signIns
	default:
		oerr = tooBusy
	}
	if oerr == nil && !time.Now().Before(in.expires) {
		oerr = &oauthError{status: http.StatusUnauthorized, code: "invalid_grant", desc: "the access token has expired"}
	}
	outcome := outcomeOK
	if oerr != nil {
		outcome = outcomeDenied
		if oerr.status >= 500 {
			outcome = outcomeError
		}
		if oerr.retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(oerr.retryAfter.Round(time.Second).Seconds())))
		}
		s.opts.Metrics.request(opToken, outcome)
		writeJSON(w, oerr.status, map[string]string{"error": oerr.code, "error_description": oerr.desc})
		return
	}
	t := token{User: in.user.Name, Session: in.session, Expires: in.expires.Unix(), TokenID: in.tokenID}
	if in.tokenID == "" {
		t.Password = s.tokens.fingerprint(in.user.Verifier)
	}
	value, err := s.tokens.seal(t)
	if err != nil {
		s.log.Error("could not seal a token", "err", err)
		s.opts.Metrics.request(opToken, outcomeError)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "server_error"})
		return
	}
	s.opts.Metrics.request(opToken, outcome)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":      value,
		"token_type":        "bearer",
		"expires_in":        max(1, int(time.Until(in.expires).Round(time.Second).Seconds())),
		"issued_token_type": accessTokenType,
	})
}

// grant decides a token request: whose token it is, its session and when
// it expires.
func (s *Server) grant(w http.ResponseWriter, r *http.Request) (signedIn, *oauthError) {
	r.Body = http.MaxBytesReader(w, r.Body, maxTokenRequest)
	if err := r.ParseForm(); err != nil {
		return signedIn{}, &oauthError{status: http.StatusBadRequest, code: "invalid_request", desc: "send an application/x-www-form-urlencoded form"}
	}
	form := r.PostForm
	id, secret := form.Get("client_id"), form.Get("client_secret")
	basicID, basicSecret, basic := r.BasicAuth()
	switch form.Get("grant_type") {
	case grantClientCredentials:
		if id == "" && secret == "" && basic {
			id, secret = basicID, basicSecret
		}
		return s.signIn(r, id, secret)
	case grantTokenExchange:
		// Iceberg clients renew an expired token by exchanging it with
		// their credentials in an Authorization: Basic header.
		if basic {
			return s.signIn(r, basicID, basicSecret)
		}
		t, err := s.tokens.open(form.Get("subject_token"))
		if err != nil {
			if !s.slowDown() {
				return signedIn{}, tooBusy
			}
			return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_grant", desc: "the subject token is invalid or expired; sign in again"}
		}
		in, err := s.session(r.Context(), t)
		if err != nil {
			var api *apiError
			if errors.As(err, &api) && api.status == http.StatusUnauthorized {
				return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_grant", desc: api.msg + "; sign in again"}
			}
			return signedIn{}, &oauthError{status: http.StatusServiceUnavailable, code: "temporarily_unavailable", desc: "could not reach the metadata database"}
		}
		return in, nil
	}
	return signedIn{}, &oauthError{status: http.StatusBadRequest, code: "unsupported_grant_type",
		desc: "use grant_type=client_credentials with an Igloo user name and a password or access token"}
}

// signIn checks a user name and password, or an access token, like a SQL
// login does, and begins a session.
func (s *Server) signIn(r *http.Request, name, secret string) (signedIn, *oauthError) {
	name = strings.ToLower(strings.TrimSpace(name))
	if auth.LooksLikeToken(secret) {
		return s.signInWithToken(r, name, secret)
	}
	if name == "" || secret == "" {
		return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_client",
			desc: "send an Igloo user name as client_id and its password as client_secret (credential=user:password), " +
				"or an access token as client_secret (credential=igloo_...)"}
	}
	user, err := s.opts.Authenticate(r.Context(), name, secret, remoteAddr(r))
	var blocked *LoginBlockedError
	switch {
	case errors.Is(err, ErrLoginFailed):
		return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_client", desc: "password authentication failed for user \"" + name + "\""}
	case errors.Is(err, ErrUserDisabled):
		return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_client", desc: "user \"" + name + "\" is not permitted to sign in"}
	case errors.As(err, &blocked):
		return signedIn{}, &oauthError{status: http.StatusTooManyRequests, code: "temporarily_unavailable", desc: blocked.Error(), retryAfter: blocked.Wait}
	case err != nil:
		s.log.Warn("could not check a password", "user", name, "err", err)
		return signedIn{}, &oauthError{status: http.StatusServiceUnavailable, code: "temporarily_unavailable", desc: "could not check the password; try again"}
	}
	return signedIn{user: user, session: newSession(), expires: time.Now().Add(tokenLifetime)}, nil
}

// signInWithToken signs in with an Igloo access token, of name's if name is
// not empty. The session lasts no longer than the token, and ends when it
// is dropped.
func (s *Server) signInWithToken(r *http.Request, name, secret string) (signedIn, *oauthError) {
	if s.opts.AuthenticateToken == nil || s.opts.Tokens == nil {
		return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_client", desc: "this server takes no access tokens; sign in with a password"}
	}
	user, t, err := s.opts.AuthenticateToken(r.Context(), name, secret, remoteAddr(r))
	if errors.Is(err, ErrLoginFailed) {
		desc := "the access token is invalid, expired or dropped"
		if name != "" {
			desc += ", or not user \"" + name + "\"'s"
		}
		return signedIn{}, &oauthError{status: http.StatusUnauthorized, code: "invalid_client", desc: desc}
	}
	if err != nil {
		s.log.Warn("could not check an access token", "user", name, "err", err)
		return signedIn{}, &oauthError{status: http.StatusServiceUnavailable, code: "temporarily_unavailable", desc: "could not check the access token; try again"}
	}
	return signedIn{user: user, session: newSession(), tokenID: t.ID, expires: earlier(time.Now().Add(tokenLifetime), t.ExpiresAt)}, nil
}

// earlier returns the earlier of a and b.
func earlier(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// session checks that t's session still stands, as SQL sessions are
// checked (refreshUser): its user exists and may sign in, and still has the
// password, or the access token, it began with. It returns the session as
// a renewal of t would be, which a token's expiry may shorten. Errors are
// *apiError, 401 for an ended session.
func (s *Server) session(ctx context.Context, t token) (signedIn, error) {
	ended := func(format string, args ...any) (signedIn, error) {
		return signedIn{}, errorf(http.StatusUnauthorized, errNotAuthorized, format, args...)
	}
	unavailable := func(what string, err error) (signedIn, error) {
		s.log.Warn("could not check a session", "user", t.User, "checking", what, "err", err)
		return signedIn{}, errorf(http.StatusServiceUnavailable, errUnavailable, "could not reach the metadata database")
	}
	user, err := s.opts.Users.Get(ctx, t.User)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return ended("user %q no longer exists", t.User)
	case err != nil:
		return unavailable("user", err)
	case user.Disabled:
		return ended("user %q is not permitted to sign in", t.User)
	}
	in := signedIn{user: user, session: t.Session, tokenID: t.TokenID, expires: time.Now().Add(tokenLifetime)}
	if t.TokenID == "" {
		if !s.tokens.current(t, user) {
			return ended("the password of %q changed", t.User)
		}
		return in, nil
	}
	if s.opts.Tokens == nil {
		return ended("this server takes no access tokens")
	}
	at, _, err := s.opts.Tokens.TokenByID(ctx, t.TokenID)
	switch {
	case errors.Is(err, auth.ErrTokenNotFound) || err == nil && at.User != t.User:
		return ended("the access token this session began with was dropped")
	case err != nil:
		return unavailable("access token", err)
	case at.Expired(time.Now()):
		return ended("the access token this session began with has expired")
	}
	in.expires = earlier(in.expires, at.ExpiresAt)
	return in, nil
}

// ErrLoginFailed is what Options.Authenticate and AuthenticateToken return
// for a sign-in that fails: an unknown user, a wrong password, or a token
// that is not valid.
var ErrLoginFailed = errors.New("authentication failed")

// ErrUserDisabled is what Options.Authenticate returns, after the password
// checked out, for a user who may not sign in.
var ErrUserDisabled = errors.New("this user is not permitted to sign in")

// LoginBlockedError is what Options.Authenticate returns after too many
// failed logins for the user or from the address.
type LoginBlockedError struct{ Wait time.Duration }

func (e *LoginBlockedError) Error() string {
	return fmt.Sprintf("too many failed sign-ins; try again in %s", e.Wait.Round(time.Second))
}

// authenticate returns who sent r, from its bearer token.
func (s *Server) authenticate(r *http.Request) (*caller, error) {
	scheme, raw, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized,
			"sign in: get a token from /v1/oauth/tokens with an Igloo user name and a password or access token")
	}
	t, err := s.tokens.open(strings.TrimSpace(raw))
	if err != nil {
		if !s.slowDown() {
			return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "too many failed requests at once; try again shortly")
		}
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized, "the access token is invalid or expired; sign in again")
	}
	in, err := s.session(r.Context(), t)
	if err != nil {
		var api *apiError
		if errors.As(err, &api) && api.status == http.StatusUnauthorized {
			api.msg += "; sign in again"
		}
		return nil, err
	}
	user := in.user
	c := &caller{user: user, session: t.Session, client: clientName(r)}
	if !c.writer() {
		p, err := s.opts.Grants.UserPrivileges(r.Context(), user.Name)
		if err != nil {
			s.log.Warn("could not load grants", "user", user.Name, "err", err)
			return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "could not load grants")
		}
		c.privs = &p
	}
	return c, nil
}

// clientName describes the engine a request came from for the history:
// "iceberg-rest", with the client's version when it says.
func clientName(r *http.Request) string {
	name := r.Header.Get("X-Client-Version")
	if name == "" {
		name = r.UserAgent()
	}
	if name == "" {
		return "iceberg-rest"
	}
	return "iceberg-rest: " + clip(name, 100)
}

// clip makes s, which a client sent, fit for the history: valid UTF-8 of
// at most n characters.
func clip(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	if utf8.RuneCountInString(s) > n {
		s = string([]rune(s)[:n]) + "..."
	}
	return s
}

// remoteAddr is the client's address as a net.Addr, for logs.
func remoteAddr(r *http.Request) net.Addr {
	ap, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil {
		return nil
	}
	return net.TCPAddrFromAddrPort(ap)
}
