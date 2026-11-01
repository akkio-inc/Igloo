package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// OIDC configures single sign-on to the web console with an OpenID Connect
// identity provider, such as Okta, Microsoft Entra ID or Google Workspace.
// An empty Issuer turns it off. docs/sso.md describes each setting.
type OIDC struct {
	// Issuer is the provider's issuer URL, which must match the "iss" of
	// its tokens exactly.
	Issuer       string
	ClientID     string
	ClientSecret string
	// RedirectURL is the console's callback as registered at the provider,
	// https://<host>/sso/callback. Empty derives it from each request.
	RedirectURL string
	Scopes      []string
	// UsernameClaim names the claim a user is recognised and named by.
	UsernameClaim string
	GroupsClaim   string
	// AllowedDomains and AllowedGroups, when set, admit only users of
	// those email domains, and members of those groups.
	AllowedDomains []string
	AllowedGroups  []string
	// CreateUsers creates an Igloo user, with role none, at the first
	// sign-in of someone without one.
	CreateUsers bool
	// PasswordSignIn lets users linked to the provider still sign in with
	// a password.
	PasswordSignIn bool
	// DisplayName labels the sign-in button.
	DisplayName string
	// AccessTokenAudience, when set, lets SQL clients of users without a
	// password sign in with an access token from the provider whose
	// audience includes it.
	AccessTokenAudience string
	// AccessTokenIDClaim, when set, names a claim holding the account's
	// immutable ID at the provider, such as Entra ID's oid: an access token
	// whose subject is not the ID token's is the account's when this claim
	// matches the one recorded at sign-in.
	AccessTokenIDClaim string
}

// Enabled reports whether single sign-on is configured.
func (o OIDC) Enabled() bool { return o.Issuer != "" }

// googleIssuer is Google's issuer, shared by every Google account: Google
// Workspace and personal ones alike.
const googleIssuer = "https://accounts.google.com"

// loadOIDC reads the IGLOO_OIDC_* variables, and the access tokens' limit.
func loadOIDC(cfg *Config, lookup func(string) (string, bool), addErr func(string, ...any)) {
	get := func(key, fallback string) string {
		if v, _ := lookup(key); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}
	list := func(key string) []string {
		var out []string
		for _, v := range strings.Split(get(key, ""), ",") {
			if v = strings.TrimSpace(v); v != "" && !slices.Contains(out, v) {
				out = append(out, v)
			}
		}
		return out
	}

	cfg.TokenMaxLifetime = 365 * 24 * time.Hour
	if v := get("IGLOO_TOKEN_MAX_LIFETIME", ""); v != "" {
		d, err := parseDays(v)
		if err != nil || d < time.Hour {
			addErr("IGLOO_TOKEN_MAX_LIFETIME must be a duration of at least 1h, such as 365d, got %q", v)
		} else {
			cfg.TokenMaxLifetime = d
		}
	}

	o := OIDC{
		Issuer:              get("IGLOO_OIDC_ISSUER", ""),
		ClientID:            get("IGLOO_OIDC_CLIENT_ID", ""),
		RedirectURL:         get("IGLOO_OIDC_REDIRECT_URL", ""),
		UsernameClaim:       get("IGLOO_OIDC_USERNAME_CLAIM", "email"),
		GroupsClaim:         get("IGLOO_OIDC_GROUPS_CLAIM", "groups"),
		DisplayName:         get("IGLOO_OIDC_DISPLAY_NAME", "SSO"),
		AccessTokenAudience: get("IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE", ""),
		AccessTokenIDClaim:  get("IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM", ""),
		AllowedGroups:       list("IGLOO_OIDC_ALLOWED_GROUPS"),
	}
	for _, d := range list("IGLOO_OIDC_ALLOWED_DOMAINS") {
		d = strings.ToLower(strings.TrimPrefix(d, "@"))
		if !slices.Contains(o.AllowedDomains, d) {
			o.AllowedDomains = append(o.AllowedDomains, d)
		}
	}
	for _, s := range strings.FieldsFunc(get("IGLOO_OIDC_SCOPES", "openid profile email"), func(r rune) bool { return r == ' ' || r == ',' }) {
		if !slices.Contains(o.Scopes, s) {
			o.Scopes = append(o.Scopes, s)
		}
	}
	if !slices.Contains(o.Scopes, "openid") {
		o.Scopes = append([]string{"openid"}, o.Scopes...)
	}
	for key, dst := range map[string]*bool{"IGLOO_OIDC_CREATE_USERS": &o.CreateUsers, "IGLOO_OIDC_PASSWORD_SIGNIN": &o.PasswordSignIn} {
		b, err := strconv.ParseBool(get(key, "true"))
		if err != nil {
			addErr("%s must be true or false: %w", key, err)
		}
		*dst = b
	}

	// The client secret comes from a file, as Kubernetes secrets are
	// mounted, or from the environment.
	secret, inline := lookup("IGLOO_OIDC_CLIENT_SECRET")
	file := get("IGLOO_OIDC_CLIENT_SECRET_FILE", "")
	switch {
	case inline && secret != "" && file != "":
		addErr("set IGLOO_OIDC_CLIENT_SECRET or IGLOO_OIDC_CLIENT_SECRET_FILE, not both")
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			addErr("IGLOO_OIDC_CLIENT_SECRET_FILE: %w", err)
		}
		o.ClientSecret = strings.TrimSpace(string(b))
	default:
		o.ClientSecret = strings.TrimSpace(secret)
	}

	if o.Issuer == "" {
		for _, key := range []string{"IGLOO_OIDC_CLIENT_ID", "IGLOO_OIDC_CLIENT_SECRET", "IGLOO_OIDC_CLIENT_SECRET_FILE",
			"IGLOO_OIDC_ALLOWED_DOMAINS", "IGLOO_OIDC_ALLOWED_GROUPS", "IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE",
			"IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM"} {
			if get(key, "") != "" {
				addErr("%s is set but IGLOO_OIDC_ISSUER is not", key)
			}
		}
		cfg.OIDC = OIDC{}
		return
	}
	if err := secureURL(o.Issuer); err != nil {
		addErr("IGLOO_OIDC_ISSUER: %w", err)
	}
	if o.ClientID == "" {
		addErr("IGLOO_OIDC_CLIENT_ID must be set with IGLOO_OIDC_ISSUER")
	}
	if o.RedirectURL != "" {
		if err := secureURL(o.RedirectURL); err != nil {
			addErr("IGLOO_OIDC_REDIRECT_URL: %w", err)
		} else if u, _ := url.Parse(o.RedirectURL); u.Path != "/sso/callback" || u.RawQuery != "" || u.Fragment != "" {
			addErr("IGLOO_OIDC_REDIRECT_URL must end in /sso/callback, such as https://igloo.example.com:8443/sso/callback, got %q", o.RedirectURL)
		}
	}
	// Anyone can make a Google account with any email address; only the
	// hosted domain claim says an account belongs to a Workspace.
	if strings.TrimSuffix(o.Issuer, "/") == googleIssuer && len(o.AllowedDomains) == 0 {
		addErr("IGLOO_OIDC_ALLOWED_DOMAINS must name your Google Workspace domains: every Google account can sign in to %s", googleIssuer)
	}
	// ID tokens carry the client ID as their audience; they are meant for
	// the console's sign-in, and must not double as SQL passwords.
	if o.AccessTokenAudience != "" && o.AccessTokenAudience == o.ClientID {
		addErr("IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE must differ from IGLOO_OIDC_CLIENT_ID, or ID tokens would sign in to SQL")
	}
	if o.AccessTokenIDClaim != "" {
		if o.AccessTokenAudience == "" {
			addErr("IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM is set but IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE is not")
		}
		// Providers give a deleted account's email or username to someone
		// else, so they must not stand for the account.
		switch strings.ToLower(o.AccessTokenIDClaim) {
		case "email", "preferred_username", "upn", "unique_name", "name", "username", strings.ToLower(o.UsernameClaim):
			addErr("IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM must name a claim holding the account's immutable ID, such as oid (Entra ID) "+
				"or uid (Okta), not %s, which may pass to another person", o.AccessTokenIDClaim)
		}
	}
	for _, c := range []string{o.UsernameClaim, o.GroupsClaim, o.AccessTokenIDClaim} {
		if strings.ContainsAny(c, " ,") {
			addErr("an OIDC claim name may not contain spaces or commas, got %q", c)
		}
	}
	cfg.OIDC = o
}

// secureURL accepts https URLs, and http ones on this machine for trying
// single sign-on out locally: tokens and keys must not cross a network in
// the clear.
func secureURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil {
		return fmt.Errorf("%q is not an absolute URL", raw)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if ip := net.ParseIP(host); host == "localhost" || ip != nil && ip.IsLoopback() {
			return nil
		}
	}
	return fmt.Errorf("%q must use https", raw)
}
