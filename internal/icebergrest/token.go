package icebergrest

import (
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
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Engines sign in with the OAuth2 client-credentials grant of the Iceberg
// REST specification: an Igloo user name as client_id and its password as
// client_secret, which stock Spark, Trino and PyIceberg configurations send
// as credential=user:password. Igloo answers with an access token of its
// own: the user, a session and an expiry, encrypted and authenticated with
// a key every replica shares, so any replica accepts it and nothing needs
// storing. A token ends after tokenLifetime, or at once when its user is
// dropped or their password changes. Clients renew it before then, with
// their credentials or by exchanging the token itself (RFC 8693).

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
var tooBusy = &oauthError{http.StatusServiceUnavailable, "temporarily_unavailable", "too many sign-ins at once; try again shortly"}

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
	// changing the password ends it.
	Password string `json:"p"`
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
}

// token serves POST /v1/oauth/tokens.
func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Pragma", "no-cache")
	var (
		user    auth.User
		session string
		oerr    *oauthError
	)
	select {
	case s.signIns <- struct{}{}:
		user, session, oerr = s.grant(w, r)
		<-s.signIns
	default:
		oerr = tooBusy
	}
	outcome := outcomeOK
	if oerr != nil {
		outcome = outcomeDenied
		if oerr.status >= 500 {
			outcome = outcomeError
		}
		if oerr == tooBusy {
			w.Header().Set("Retry-After", "1")
		}
		s.opts.Metrics.request(opToken, outcome)
		writeJSON(w, oerr.status, map[string]string{"error": oerr.code, "error_description": oerr.desc})
		return
	}
	value, err := s.tokens.seal(token{
		User:     user.Name,
		Session:  session,
		Expires:  time.Now().Add(tokenLifetime).Unix(),
		Password: s.tokens.fingerprint(user.Verifier),
	})
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
		"expires_in":        int(tokenLifetime.Seconds()),
		"issued_token_type": accessTokenType,
	})
}

// grant decides a token request: whose token it is, and its session.
func (s *Server) grant(w http.ResponseWriter, r *http.Request) (auth.User, string, *oauthError) {
	r.Body = http.MaxBytesReader(w, r.Body, maxTokenRequest)
	if err := r.ParseForm(); err != nil {
		return auth.User{}, "", &oauthError{http.StatusBadRequest, "invalid_request", "send an application/x-www-form-urlencoded form"}
	}
	form := r.PostForm
	id, secret := form.Get("client_id"), form.Get("client_secret")
	basicID, basicSecret, basic := r.BasicAuth()
	switch form.Get("grant_type") {
	case grantClientCredentials:
		if id == "" && basic {
			id, secret = basicID, basicSecret
		}
		user, err := s.signIn(r, id, secret)
		return user, newSession(), err
	case grantTokenExchange:
		// Iceberg clients renew an expired token by exchanging it with
		// their credentials in an Authorization: Basic header.
		if basic {
			user, err := s.signIn(r, basicID, basicSecret)
			return user, newSession(), err
		}
		t, err := s.tokens.open(form.Get("subject_token"))
		if err != nil {
			if !s.slowDown() {
				return auth.User{}, "", tooBusy
			}
			return auth.User{}, "", &oauthError{http.StatusUnauthorized, "invalid_grant", "the subject token is invalid or expired; sign in again"}
		}
		user, err := s.opts.Users.Get(r.Context(), t.User)
		switch {
		case errors.Is(err, auth.ErrNotFound) || err == nil && !s.tokens.current(t, user):
			return auth.User{}, "", &oauthError{http.StatusUnauthorized, "invalid_grant", "the user was dropped or their password changed; sign in again"}
		case err != nil:
			s.log.Warn("could not look up user", "user", t.User, "err", err)
			return auth.User{}, "", &oauthError{http.StatusServiceUnavailable, "temporarily_unavailable", "could not reach the metadata database"}
		}
		return user, t.Session, nil
	}
	return auth.User{}, "", &oauthError{http.StatusBadRequest, "unsupported_grant_type",
		"use grant_type=client_credentials with an Igloo user name and password"}
}

// signIn checks a user name and password like a SQL login does.
func (s *Server) signIn(r *http.Request, name, password string) (auth.User, *oauthError) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || password == "" {
		return auth.User{}, &oauthError{http.StatusUnauthorized, "invalid_client",
			"send an Igloo user name as client_id and its password as client_secret (credential=user:password)"}
	}
	user, err := s.opts.Authenticate(r.Context(), name, password, remoteAddr(r))
	if errors.Is(err, ErrLoginFailed) {
		return auth.User{}, &oauthError{http.StatusUnauthorized, "invalid_client", "password authentication failed for user \"" + name + "\""}
	}
	if err != nil {
		s.log.Warn("could not check a password", "user", name, "err", err)
		return auth.User{}, &oauthError{http.StatusServiceUnavailable, "temporarily_unavailable", "could not check the password; try again"}
	}
	return user, nil
}

// ErrLoginFailed is what Options.Authenticate returns for an unknown user
// and a wrong password alike.
var ErrLoginFailed = errors.New("password authentication failed")

// authenticate returns who sent r, from its bearer token.
func (s *Server) authenticate(r *http.Request) (*caller, error) {
	scheme, raw, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized,
			"sign in: get a token from /v1/oauth/tokens with an Igloo user name and password")
	}
	t, err := s.tokens.open(strings.TrimSpace(raw))
	if err != nil {
		if !s.slowDown() {
			return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "too many failed requests at once; try again shortly")
		}
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized, "the access token is invalid or expired; sign in again")
	}
	user, err := s.opts.Users.Get(r.Context(), t.User)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized, "user %q no longer exists", t.User)
	case err != nil:
		s.log.Warn("could not look up user", "user", t.User, "err", err)
		return nil, errorf(http.StatusServiceUnavailable, errUnavailable, "could not reach the metadata database")
	case !s.tokens.current(t, user):
		return nil, errorf(http.StatusUnauthorized, errNotAuthorized, "the password of %q changed; sign in again", t.User)
	}
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
