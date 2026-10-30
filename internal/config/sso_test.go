package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOIDCDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OIDC.Enabled() || cfg.TokenMaxLifetime != 365*24*time.Hour {
		t.Errorf("defaults: %+v %s", cfg.OIDC, cfg.TokenMaxLifetime)
	}

	cfg, err = Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":     "password",
		"IGLOO_OIDC_ISSUER":        "https://example.okta.com",
		"IGLOO_OIDC_CLIENT_ID":     "igloo",
		"IGLOO_OIDC_CLIENT_SECRET": "  s3cret\n",
	}))
	if err != nil {
		t.Fatal(err)
	}
	o := cfg.OIDC
	if !o.Enabled() || o.ClientSecret != "s3cret" || !slices.Equal(o.Scopes, []string{"openid", "profile", "email"}) ||
		o.UsernameClaim != "email" || o.GroupsClaim != "groups" || !o.CreateUsers || !o.PasswordSignIn ||
		o.DisplayName != "SSO" || o.RedirectURL != "" || o.AccessTokenAudience != "" || o.AccessTokenIDClaim != "" {
		t.Errorf("OIDC defaults: %+v", o)
	}
}

func TestOIDC(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "client-secret")
	if err := os.WriteFile(secretFile, []byte("from-a-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(env(map[string]string{
		"IGLOO_ADMIN_PASSWORD":             "password",
		"IGLOO_OIDC_ISSUER":                "https://login.microsoftonline.com/tenant/v2.0",
		"IGLOO_OIDC_CLIENT_ID":             "app",
		"IGLOO_OIDC_CLIENT_SECRET_FILE":    secretFile,
		"IGLOO_OIDC_REDIRECT_URL":          "https://igloo.example.com:8443/sso/callback",
		"IGLOO_OIDC_SCOPES":                "profile, email groups",
		"IGLOO_OIDC_USERNAME_CLAIM":        "preferred_username",
		"IGLOO_OIDC_GROUPS_CLAIM":          "roles",
		"IGLOO_OIDC_ALLOWED_DOMAINS":       "Agency.com, @brand.example ,agency.com",
		"IGLOO_OIDC_ALLOWED_GROUPS":        "Igloo Users,Data Team",
		"IGLOO_OIDC_CREATE_USERS":          "false",
		"IGLOO_OIDC_PASSWORD_SIGNIN":       "false",
		"IGLOO_OIDC_DISPLAY_NAME":          "Entra ID",
		"IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE": "api://igloo",
		"IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM": "oid",
		"IGLOO_TOKEN_MAX_LIFETIME":         "30d",
	}))
	if err != nil {
		t.Fatal(err)
	}
	o := cfg.OIDC
	if o.ClientSecret != "from-a-file" || !slices.Equal(o.Scopes, []string{"openid", "profile", "email", "groups"}) ||
		!slices.Equal(o.AllowedDomains, []string{"agency.com", "brand.example"}) ||
		!slices.Equal(o.AllowedGroups, []string{"Igloo Users", "Data Team"}) || o.CreateUsers || o.PasswordSignIn ||
		o.UsernameClaim != "preferred_username" || o.GroupsClaim != "roles" || o.DisplayName != "Entra ID" ||
		o.AccessTokenAudience != "api://igloo" || o.AccessTokenIDClaim != "oid" || cfg.TokenMaxLifetime != 30*24*time.Hour {
		t.Errorf("got %+v, token lifetime %s", o, cfg.TokenMaxLifetime)
	}

	// Plain HTTP is only for trying it out on this machine.
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password",
		"IGLOO_OIDC_ISSUER": "http://localhost:5556/dex", "IGLOO_OIDC_CLIENT_ID": "igloo"})); err != nil {
		t.Errorf("a local issuer: %v", err)
	}

	base := map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_OIDC_ISSUER": "https://idp.example.com", "IGLOO_OIDC_CLIENT_ID": "igloo"}
	for _, c := range []struct {
		key, value, want string
	}{
		{"IGLOO_OIDC_ISSUER", "http://idp.example.com", "https"},
		{"IGLOO_OIDC_ISSUER", "idp.example.com", "absolute"},
		{"IGLOO_OIDC_CLIENT_ID", "", "IGLOO_OIDC_CLIENT_ID"},
		{"IGLOO_OIDC_CLIENT_SECRET_FILE", filepath.Join(dir, "missing"), "IGLOO_OIDC_CLIENT_SECRET_FILE"},
		{"IGLOO_OIDC_REDIRECT_URL", "http://igloo.example.com/sso/callback", "https"},
		{"IGLOO_OIDC_REDIRECT_URL", "https://igloo.example.com/callback", "/sso/callback"},
		{"IGLOO_OIDC_CREATE_USERS", "sometimes", "IGLOO_OIDC_CREATE_USERS"},
		{"IGLOO_OIDC_USERNAME_CLAIM", "my claim", "claim"},
		{"IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE", "igloo", "must differ"},
		{"IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM", "oid", "IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE is not"},
		{"IGLOO_TOKEN_MAX_LIFETIME", "1m", "IGLOO_TOKEN_MAX_LIFETIME"},
		{"IGLOO_TOKEN_MAX_LIFETIME", "forever", "IGLOO_TOKEN_MAX_LIFETIME"},
	} {
		vars := map[string]string{c.key: c.value}
		for k, v := range base {
			if _, ok := vars[k]; !ok {
				vars[k] = v
			}
		}
		if _, err := Load(env(vars)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s=%q: %v", c.key, c.value, err)
		}
	}

	both := map[string]string{"IGLOO_OIDC_CLIENT_SECRET": "x", "IGLOO_OIDC_CLIENT_SECRET_FILE": secretFile}
	for k, v := range base {
		both[k] = v
	}
	if _, err := Load(env(both)); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Errorf("both secrets: %v", err)
	}
	// An access token's account is never matched by what another person
	// may get.
	for _, claim := range []string{"email", "Preferred_Username", "upn", "sub_email"} {
		vars := map[string]string{"IGLOO_OIDC_ACCESS_TOKEN_AUDIENCE": "api://igloo", "IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM": claim,
			"IGLOO_OIDC_USERNAME_CLAIM": "sub_email"}
		for k, v := range base {
			vars[k] = v
		}
		if _, err := Load(env(vars)); err == nil || !strings.Contains(err.Error(), "immutable ID") {
			t.Errorf("IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM=%s: %v", claim, err)
		}
	}
	// Every Google account shares Google's issuer.
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password",
		"IGLOO_OIDC_ISSUER": "https://accounts.google.com", "IGLOO_OIDC_CLIENT_ID": "igloo"})); err == nil {
		t.Error("Google without allowed domains was accepted")
	}
	// Settings without an issuer are a mistake.
	if _, err := Load(env(map[string]string{"IGLOO_ADMIN_PASSWORD": "password", "IGLOO_OIDC_CLIENT_ID": "igloo"})); err == nil {
		t.Error("a client ID without an issuer was accepted")
	}
}
