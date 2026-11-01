// Package sso signs people in to the web console with an OpenID Connect
// identity provider such as Okta, Microsoft Entra ID, Google Workspace or
// Keycloak, and maps the provider's accounts and groups to Igloo users and
// roles.
//
// A sign-in is the authorization code flow with PKCE: Begin returns the
// provider's authorization URL and what the console keeps, sealed, in a
// short-lived cookie (state, nonce and PKCE verifier); Finish exchanges the
// code the provider returned and verifies the ID token (signature against
// the provider's published keys, issuer, audience, expiry, nonce); SignIn
// admits the identity by the configured domains and groups, finds or
// creates its Igloo user, and brings the user's roles in line with the
// identity's groups. VerifyAccessToken checks access tokens SQL clients send
// as passwords.
//
// Nothing here is logged or stored that could sign anyone in: not codes,
// tokens, state or verifiers.
package sso

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// PendingLifetime is how long a sign-in may take at the identity provider.
const PendingLifetime = 10 * time.Minute

// discoveryRetry spaces out attempts to reach a provider that failed.
const discoveryRetry = 30 * time.Second

// googleIssuer is shared by every Google account; only its "hd" claim says
// which Workspace an account belongs to.
const googleIssuer = "https://accounts.google.com"

// Options configure a Service.
type Options struct {
	Config   config.OIDC
	Users    auth.Store
	Accounts auth.AccountStore
	SSO      auth.SSOStore
	// SCIM, if set, holds the groups SCIM provisioning gives users, which
	// count with their token's at each sign-in, and alone decide who is in
	// the groups it pushes.
	SCIM auth.SCIMStore
	// HTTP reaches the provider; nil means a client with a 15 second
	// timeout.
	HTTP *http.Client
	Log  *slog.Logger
	// Changed, if set, is told the name of a user whose built-in role or
	// custom roles a sign-in changed, so open sessions see it.
	Changed func(user string)
}

// Service signs users in with one identity provider. It is safe for
// concurrent use.
type Service struct {
	cfg      config.OIDC
	users    auth.Store
	accounts auth.AccountStore
	sso      auth.SSOStore
	scim     auth.SCIMStore
	http     *http.Client
	log      *slog.Logger
	changed  func(string)
	now      func() time.Time

	mu       sync.Mutex
	provider *oidc.Provider
	// lastErr and lastTry remember a failed discovery, so a provider
	// that is down is not asked again on every request.
	lastErr error
	lastTry time.Time
}

// New returns a Service for opts.Config, which must be enabled. It does
// not contact the provider until a sign-in needs it.
func New(opts Options) (*Service, error) {
	if !opts.Config.Enabled() {
		return nil, errors.New("sso: no issuer configured")
	}
	if opts.Users == nil || opts.Accounts == nil || opts.SSO == nil {
		return nil, errors.New("sso: single sign-on needs IGLOO_METADATA_DATABASE_URL")
	}
	if opts.HTTP == nil {
		opts.HTTP = &http.Client{Timeout: 15 * time.Second}
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Changed == nil {
		opts.Changed = func(string) {}
	}
	return &Service{
		cfg: opts.Config, users: opts.Users, accounts: opts.Accounts, sso: opts.SSO, scim: opts.SCIM,
		http: opts.HTTP, log: opts.Log.With("component", "sso"), changed: opts.Changed, now: time.Now,
	}, nil
}

// DisplayName labels the sign-in button.
func (s *Service) DisplayName() string { return s.cfg.DisplayName }

// RedirectURL is the configured callback URL, or empty when the console
// derives it from each request.
func (s *Service) RedirectURL() string { return s.cfg.RedirectURL }

// discover returns the provider's configuration, fetching it the first
// time. The keys it publishes are fetched as tokens need them, and again
// when a token is signed with a key not seen before.
func (s *Service) discover(ctx context.Context) (*oidc.Provider, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.provider != nil {
		return s.provider, nil
	}
	if s.lastErr != nil && s.now().Sub(s.lastTry) < discoveryRetry {
		return nil, s.lastErr
	}
	// The provider keeps this context to fetch keys later, so it must
	// carry the client but outlive the request.
	base := oidc.ClientContext(context.WithoutCancel(ctx), s.http)
	dctx, cancel := context.WithTimeout(base, 20*time.Second)
	defer cancel()
	p, err := oidc.NewProvider(dctx, s.cfg.Issuer)
	s.lastTry = s.now()
	if err != nil {
		s.lastErr = fmt.Errorf("reach the identity provider at %s: %w", s.cfg.Issuer, err)
		s.log.Warn("single sign-on provider unreachable", "issuer", s.cfg.Issuer, "err", err)
		return nil, s.lastErr
	}
	s.provider, s.lastErr = p, nil
	return p, nil
}

func (s *Service) oauth(p *oidc.Provider, redirectURL string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     s.cfg.ClientID,
		ClientSecret: s.cfg.ClientSecret,
		Endpoint:     p.Endpoint(),
		RedirectURL:  redirectURL,
		Scopes:       s.cfg.Scopes,
	}
}

// Pending is a sign-in in progress, kept by the browser in a sealed cookie
// between Begin and Finish.
type Pending struct {
	State       string    `json:"s"`
	Nonce       string    `json:"n"`
	Verifier    string    `json:"v"`
	RedirectURL string    `json:"r"`
	Started     time.Time `json:"t"`
}

// Expired reports whether the sign-in took too long.
func (p Pending) Expired(now time.Time) bool {
	return p.Started.IsZero() || now.Sub(p.Started) > PendingLifetime || now.Before(p.Started.Add(-time.Minute))
}

// Begin starts a sign-in whose callback is redirectURL. It returns where to
// send the browser, and what to keep for Finish.
func (s *Service) Begin(ctx context.Context, redirectURL string) (string, Pending, error) {
	p, err := s.discover(ctx)
	if err != nil {
		return "", Pending{}, err
	}
	pending := Pending{
		State:       random(),
		Nonce:       random(),
		Verifier:    oauth2.GenerateVerifier(),
		RedirectURL: redirectURL,
		Started:     s.now(),
	}
	url := s.oauth(p, redirectURL).AuthCodeURL(pending.State,
		oauth2.S256ChallengeOption(pending.Verifier), oidc.Nonce(pending.Nonce))
	return url, pending, nil
}

// random is 32 random bytes, base64url-encoded.
func random() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// CheckState reports whether the state a callback carries is pending's.
func (p Pending) CheckState(state string) bool {
	return p.State != "" && subtle.ConstantTimeCompare([]byte(state), []byte(p.State)) == 1
}

// Finish completes a sign-in: it exchanges code, with pending's PKCE
// verifier, for tokens and returns the identity the verified ID token
// describes. The caller has checked the state.
func (s *Service) Finish(ctx context.Context, pending Pending, code string) (Identity, error) {
	if pending.Expired(s.now()) {
		return Identity{}, errors.New("the sign-in took too long")
	}
	p, err := s.discover(ctx)
	if err != nil {
		return Identity{}, err
	}
	ctx = oidc.ClientContext(ctx, s.http)
	tok, err := s.oauth(p, pending.RedirectURL).Exchange(ctx, code, oauth2.VerifierOption(pending.Verifier))
	if err != nil {
		return Identity{}, fmt.Errorf("exchange the authorization code: %w", redactOAuthError(err))
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok || raw == "" {
		return Identity{}, errors.New("the identity provider returned no ID token; is the openid scope allowed?")
	}
	idToken, err := p.VerifierContext(ctx, &oidc.Config{ClientID: s.cfg.ClientID}).Verify(ctx, raw)
	if err != nil {
		return Identity{}, fmt.Errorf("verify the ID token: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(idToken.Nonce), []byte(pending.Nonce)) != 1 {
		return Identity{}, errors.New("the ID token's nonce does not match this sign-in")
	}
	var claims map[string]any
	if err := idToken.Claims(&claims); err != nil {
		return Identity{}, fmt.Errorf("read the ID token's claims: %w", err)
	}
	// With several audiences, the token must have been issued to Igloo.
	if len(idToken.Audience) > 1 {
		if azp, _ := claims["azp"].(string); azp != s.cfg.ClientID {
			return Identity{}, errors.New("the ID token was issued to another client")
		}
	}
	id := s.identity(idToken.Issuer, idToken.Subject, claims)
	if s.cfg.AccessTokenIDClaim != "" && id.AccountID == "" {
		// Okta's ID tokens have no uid: their subject is the ID its
		// access tokens carry there.
		id.AccountID = id.Subject
	}
	return id, nil
}

// redactOAuthError keeps a token endpoint's error code and description but
// not its raw response, which could echo what was sent.
func redactOAuthError(err error) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		msg := re.ErrorCode
		if re.ErrorDescription != "" {
			msg += ": " + re.ErrorDescription
		}
		if msg == "" && re.Response != nil {
			msg = re.Response.Status
		}
		return errors.New(truncate(msg, 300))
	}
	return err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Identity is what a verified token says about its user.
type Identity struct {
	Issuer  string
	Subject string
	// Username is the value of the configured username claim.
	Username string
	// Email is the user's email if the provider verified it, else empty.
	Email  string
	Groups []string
	// Domain is the domain the allowed domains are checked against:
	// Google's hosted domain, or the username's or email's domain.
	Domain string
	// AccountID is the value of the claim IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM
	// names, if set: the account's ID at the provider, which access tokens
	// whose subject is not the ID token's are matched by.
	AccountID string
}

// identity reads an Identity from verified claims.
func (s *Service) identity(issuer, subject string, claims map[string]any) Identity {
	id := Identity{Issuer: issuer, Subject: subject, Username: stringClaim(claims, s.cfg.UsernameClaim)}
	if s.cfg.AccessTokenIDClaim != "" {
		id.AccountID = stringClaim(claims, s.cfg.AccessTokenIDClaim)
	}
	if email := stringClaim(claims, "email"); email != "" && boolClaim(claims, "email_verified") {
		id.Email = auth.NormalizeEmail(email)
	}
	if s.cfg.UsernameClaim == "email" && id.Email == "" {
		// An unverified email names no one.
		id.Username = ""
	}
	id.Groups = listClaim(claims, s.cfg.GroupsClaim)
	switch {
	case strings.TrimSuffix(issuer, "/") == googleIssuer:
		id.Domain = strings.ToLower(stringClaim(claims, "hd"))
	case strings.Contains(id.Username, "@"):
		id.Domain = domainOf(id.Username)
	case id.Email != "":
		id.Domain = domainOf(id.Email)
	}
	return id
}

func domainOf(address string) string {
	_, domain, _ := strings.Cut(strings.ToLower(address), "@")
	return domain
}

func stringClaim(claims map[string]any, name string) string {
	s, _ := claims[name].(string)
	return strings.TrimSpace(s)
}

// boolClaim reads a boolean claim, which some providers send as a string.
func boolClaim(claims map[string]any, name string) bool {
	switch v := claims[name].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// listClaim reads a claim holding a list of strings, or one string.
func listClaim(claims map[string]any, name string) []string {
	var out []string
	switch v := claims[name].(type) {
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok && s != "" && !slices.Contains(out, s) {
				out = append(out, s)
			}
		}
	case string:
		if v != "" {
			out = []string{v}
		}
	}
	return out
}
