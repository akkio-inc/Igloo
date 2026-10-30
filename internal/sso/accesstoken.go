package sso

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/akkio-inc/igloo/internal/auth"
)

// AccessTokensEnabled reports whether SQL clients may sign in with the
// provider's access tokens.
func (s *Service) AccessTokensEnabled() bool { return s.cfg.AccessTokenAudience != "" }

// VerifyAccessToken checks an access token a SQL client sent as user's
// password: a JWT signed with the provider's keys, from its issuer, for the
// configured audience and unexpired, of an identity the allowed domains
// and groups admit, that is user's. The identity is user's when it is the
// one user signed in to the console with: the same subject, or, with
// IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM, the same account ID. A user not linked
// yet is matched as a first console sign-in would be: by their email, or,
// when they have no email and could be claimed by name (see claimable), by
// a username that maps to their name. It implements
// pgwire.AccessTokenVerifier.
func (s *Service) VerifyAccessToken(ctx context.Context, user auth.User, token string) error {
	if !s.AccessTokensEnabled() {
		return errors.New("access tokens are not accepted")
	}
	p, err := s.discover(ctx)
	if err != nil {
		return err
	}
	ctx = oidc.ClientContext(ctx, s.http)
	verified, err := p.VerifierContext(ctx, &oidc.Config{ClientID: s.cfg.AccessTokenAudience, Now: s.now}).Verify(ctx, token)
	if err != nil {
		return fmt.Errorf("verify the access token: %w", err)
	}
	var claims map[string]any
	if err := verified.Claims(&claims); err != nil {
		return err
	}
	id := s.identity(verified.Issuer, verified.Subject, claims)
	if err := s.admit(id); err != nil {
		return err
	}
	if user.IsService() {
		return fmt.Errorf("%s is a service account", user.Name)
	}

	link, err := s.sso.IdentityOf(ctx, user.Name)
	switch {
	case err == nil:
		// Some providers give access tokens a subject of their own (Okta's
		// is the username, Entra ID's differs between applications); an
		// immutable ID claim stands in for it then. Never the email: a
		// provider may give a deleted account's to someone else.
		if link.Issuer == id.Issuer && (link.Subject == id.Subject || id.AccountID != "" && link.AccountID == id.AccountID) {
			return nil
		}
		switch {
		case s.cfg.AccessTokenIDClaim == "":
			return fmt.Errorf("the access token is not %s's: its subject is not the one %s signs in to the console with "+
				"(for a provider whose access tokens have a subject of their own, set IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM)", user.Name, user.Name)
		case link.AccountID == "":
			return fmt.Errorf("the access token is not %s's: its subject is not the one %s signs in to the console with, "+
				"and %s has not signed in to the console since IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM was set", user.Name, user.Name, user.Name)
		}
		return fmt.Errorf("the access token is not %s's: neither its subject nor its %s claim is %s's",
			user.Name, s.cfg.AccessTokenIDClaim, user.Name)
	case !errors.Is(err, auth.ErrIdentityNotFound):
		return err
	}
	// Not linked yet: the same rules as a first console sign-in.
	if user.Email != "" {
		if id.Email == user.Email {
			return nil
		}
		return fmt.Errorf("the access token is not %s's: another email", user.Name)
	}
	claimable, err := s.claimable(ctx, user)
	if err != nil {
		return err
	}
	if claimable && slices.Contains(candidateNames(id.Username), user.Name) {
		return nil
	}
	return fmt.Errorf("the access token is not %s's", user.Name)
}

// Status describes single sign-on for the console's admin page.
type Status struct {
	Issuer         string   `json:"issuer"`
	ClientID       string   `json:"client_id"`
	ClientSecret   bool     `json:"client_secret"`
	RedirectURL    string   `json:"redirect_url"`
	Scopes         []string `json:"scopes"`
	UsernameClaim  string   `json:"username_claim"`
	GroupsClaim    string   `json:"groups_claim"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedGroups  []string `json:"allowed_groups"`
	CreateUsers    bool     `json:"create_users"`
	PasswordSignIn bool     `json:"password_sign_in"`
	DisplayName    string   `json:"display_name"`
	// AccessTokenAudience is set when SQL clients may sign in with the
	// provider's access tokens.
	AccessTokenAudience string `json:"access_token_audience"`
	// Reachable says whether the provider's configuration was fetched;
	// Error why not.
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
}

// Status reports the configuration, and tries to reach the provider if it
// has not yet.
func (s *Service) Status(ctx context.Context) Status {
	st := Status{
		Issuer: s.cfg.Issuer, ClientID: s.cfg.ClientID, ClientSecret: s.cfg.ClientSecret != "",
		RedirectURL: s.cfg.RedirectURL, Scopes: s.cfg.Scopes, UsernameClaim: s.cfg.UsernameClaim,
		GroupsClaim: s.cfg.GroupsClaim, AllowedDomains: s.cfg.AllowedDomains, AllowedGroups: s.cfg.AllowedGroups,
		CreateUsers: s.cfg.CreateUsers, PasswordSignIn: s.cfg.PasswordSignIn, DisplayName: s.cfg.DisplayName,
		AccessTokenAudience: s.cfg.AccessTokenAudience,
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := s.discover(ctx); err != nil {
		st.Error = err.Error()
	} else {
		st.Reachable = true
	}
	return st
}

// Identities lists the users linked to single sign-on identities.
func (s *Service) Identities(ctx context.Context) ([]auth.Identity, error) {
	return s.sso.Identities(ctx)
}
