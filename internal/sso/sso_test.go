package sso

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/sso/ssotest"
)

const callback = "https://igloo.example.com/sso/callback"

type fixture struct {
	idp     *ssotest.Provider
	dir     *auth.MemoryDirectory
	svc     *Service
	changed []string
}

func newFixture(t *testing.T, configure ...func(*config.OIDC)) *fixture {
	t.Helper()
	f := &fixture{idp: ssotest.New(t, "igloo", "client-secret"), dir: auth.NewMemoryDirectory()}
	if _, err := f.dir.EnsureAdmin(context.Background(), "root", "v"); err != nil {
		t.Fatal(err)
	}
	cfg := config.OIDC{
		Issuer: f.idp.Issuer(), ClientID: "igloo", ClientSecret: "client-secret", Scopes: []string{"openid", "email", "groups"},
		UsernameClaim: "email", GroupsClaim: "groups", CreateUsers: true, PasswordSignIn: true, DisplayName: "Test IdP",
		AccessTokenAudience: "api://igloo",
	}
	for _, c := range configure {
		c(&cfg)
	}
	svc, err := New(Options{Config: cfg, Users: f.dir, Accounts: f.dir, SSO: f.dir,
		Changed: func(u string) { f.changed = append(f.changed, u) }})
	if err != nil {
		t.Fatal(err)
	}
	f.svc = svc
	return f
}

// signIn runs a whole sign-in of someone with claims.
func (f *fixture) signIn(t *testing.T, claims map[string]any) (auth.User, error) {
	t.Helper()
	id := f.identity(t, claims)
	return f.svc.SignIn(context.Background(), id)
}

// identity runs the flow up to a verified identity.
func (f *fixture) identity(t *testing.T, claims map[string]any) Identity {
	t.Helper()
	ctx := context.Background()
	authURL, pending, err := f.svc.Begin(ctx, callback)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(authURL, "code_challenge_method=S256") || !strings.Contains(authURL, "nonce=") ||
		strings.Contains(authURL, pending.Verifier) || strings.Contains(authURL, "client-secret") {
		t.Fatalf("authorization URL %s", authURL)
	}
	f.idp.SignInAs(claims)
	back, err := f.idp.Authorize(authURL)
	if err != nil {
		t.Fatal(err)
	}
	if back.Host != "igloo.example.com" || !pending.CheckState(back.Query().Get("state")) {
		t.Fatalf("callback %s", back)
	}
	id, err := f.svc.Finish(ctx, pending, back.Query().Get("code"))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func person(sub, email string, groups ...string) map[string]any {
	return map[string]any{"sub": sub, "email": email, "email_verified": true, "groups": groups, "name": "Someone"}
}

func TestSignInCreatesAndFindsUsers(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	id := f.identity(t, person("s-ana", "Ana.Lopez@Agency.com", "Data Team"))
	if id.Subject != "s-ana" || id.Email != "ana.lopez@agency.com" || id.Domain != "agency.com" ||
		!slices.Equal(id.Groups, []string{"Data Team"}) || id.Issuer != f.idp.Issuer() {
		t.Fatalf("identity %+v", id)
	}
	ana, err := f.svc.SignIn(ctx, id)
	if err != nil || ana.Name != "ana_lopez" || ana.Role != auth.RoleNone || ana.HasPassword() || ana.Email != "ana.lopez@agency.com" {
		t.Fatalf("first sign-in: %+v %v", ana, err)
	}
	if _, ok := auth.TokenFactors(ana); !ok {
		t.Error("a new user has no token salt")
	}
	// The next sign-in finds the same user by subject, even with a new
	// email.
	again, err := f.signIn(t, person("s-ana", "ana@agency.com"))
	if err != nil || again.Name != "ana_lopez" || again.Email != "ana@agency.com" {
		t.Errorf("second sign-in: %+v %v", again, err)
	}
	if link, _ := f.dir.IdentityOf(ctx, "ana_lopez"); link.Email != "ana@agency.com" || len(link.Groups) != 0 {
		t.Errorf("identity after the second sign-in: %+v", link)
	}

	// Someone else whose name is taken gets one with their domain.
	other, err := f.signIn(t, person("s-other", "ana.lopez@brand.example"))
	if err != nil || other.Name != "ana_lopez_brand_example" {
		t.Errorf("a second ana: %+v %v", other, err)
	}

	// Admins prepare users by email, or by name without a password.
	if err := f.dir.Create(ctx, auth.User{Name: "bea", Role: auth.RoleWriter, Email: "beatriz@agency.com", Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if bea, err := f.signIn(t, person("s-bea", "Beatriz@agency.com")); err != nil || bea.Name != "bea" || bea.Role != auth.RoleWriter {
		t.Errorf("by email: %+v %v", bea, err)
	}
	if err := f.dir.Create(ctx, auth.User{Name: "carl", Role: auth.RoleReader}); err != nil {
		t.Fatal(err)
	}
	if carl, err := f.signIn(t, person("s-carl", "carl@agency.com")); err != nil || carl.Name != "carl" || carl.Role != auth.RoleReader {
		t.Errorf("by name: %+v %v", carl, err)
	}
	// A user with a password is never taken over by name.
	if err := f.dir.Create(ctx, auth.User{Name: "dora", Role: auth.RoleAdmin, Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if dora, err := f.signIn(t, person("s-dora", "dora@agency.com")); err != nil || dora.Name != "dora_agency_com" || dora.Role != auth.RoleNone {
		t.Errorf("a password user's name: %+v %v", dora, err)
	}
	// Nor is a service account, even by email.
	if err := f.dir.Create(ctx, auth.User{Name: "etl", Role: auth.RoleWriter, Kind: auth.KindService, Email: "etl@agency.com"}); err != nil {
		t.Fatal(err)
	}
	var refusal *Refusal
	if _, err := f.signIn(t, person("s-etl", "etl@agency.com")); !errors.As(err, &refusal) || refusal.Code != RefusedService {
		t.Errorf("a service account's email: %v", err)
	}
	// Disabled users are refused.
	if err := f.dir.SetLogin(ctx, "carl", false); err != nil {
		t.Fatal(err)
	}
	if _, err := f.signIn(t, person("s-carl", "carl@agency.com")); !errors.As(err, &refusal) || refusal.Code != RefusedDisabled || refusal.User != "carl" {
		t.Errorf("a disabled user: %v", err)
	}
}

func TestSignInPolicy(t *testing.T) {
	f := newFixture(t, func(c *config.OIDC) {
		c.AllowedDomains = []string{"agency.com"}
		c.AllowedGroups = []string{"igloo-users"}
		c.CreateUsers = false
	})
	ctx := context.Background()
	var refusal *Refusal
	refused := func(name string, claims map[string]any, code string) {
		t.Helper()
		if _, err := f.signIn(t, claims); !errors.As(err, &refusal) || refusal.Code != code {
			t.Errorf("%s: %v, want %s", name, err, code)
		}
	}
	refused("another domain", person("s1", "ana@elsewhere.com", "igloo-users"), RefusedDomain)
	refused("no allowed group", person("s1", "ana@agency.com", "other"), RefusedGroup)
	unverified := person("s1", "ana@agency.com", "igloo-users")
	unverified["email_verified"] = false
	refused("an unverified email", unverified, RefusedUnverified)
	refused("no user and no creating", person("s1", "ana@agency.com", "igloo-users"), RefusedNoUser)

	if err := f.dir.Create(ctx, auth.User{Name: "ana", Role: auth.RoleReader}); err != nil {
		t.Fatal(err)
	}
	if ana, err := f.signIn(t, person("s1", "ana@agency.com", "igloo-users")); err != nil || ana.Name != "ana" {
		t.Errorf("an admitted user: %+v %v", ana, err)
	}
	// Some providers send email_verified as a string.
	asString := person("s1", "ana@agency.com", "igloo-users")
	asString["email_verified"] = "true"
	if _, err := f.signIn(t, asString); err != nil {
		t.Errorf("email_verified as a string: %v", err)
	}
}

func TestUsernameClaims(t *testing.T) {
	// Entra ID's email is not verified; its user principal name is.
	f := newFixture(t, func(c *config.OIDC) {
		c.UsernameClaim = "preferred_username"
		c.AllowedDomains = []string{"agency.onmicrosoft.com"}
	})
	u, err := f.signIn(t, map[string]any{"sub": "oid-1", "preferred_username": "Ana.Lopez@agency.onmicrosoft.com",
		"email": "ana@personal.example", "groups": "single-group"})
	if err != nil || u.Name != "ana_lopez" || u.Email != "" {
		t.Errorf("preferred_username: %+v %v", u, err)
	}
	if link, _ := f.dir.IdentityOf(context.Background(), "ana_lopez"); !slices.Equal(link.Groups, []string{"single-group"}) {
		t.Errorf("a group claim of one string: %+v", link)
	}

	// Google admits only its Workspace domains, by the hd claim.
	g := newFixture(t, func(c *config.OIDC) { c.AllowedDomains = []string{"agency.com"} })
	id := g.svc.identity(googleIssuer, "1", map[string]any{"email": "ana@agency.com", "email_verified": true})
	if err := g.svc.admit(id); err == nil {
		t.Error("a Google account without a hosted domain was admitted")
	}
	id = g.svc.identity(googleIssuer, "1", map[string]any{"email": "ana@agency.com", "email_verified": true, "hd": "agency.com"})
	if err := g.svc.admit(id); err != nil {
		t.Errorf("a Workspace account: %v", err)
	}
}

func TestNames(t *testing.T) {
	for in, want := range map[string][]string{
		"Ana.Lopez@Agency.com":  {"ana_lopez", "ana_lopez_agency_com"},
		"ana":                   {"ana"},
		"José María@agency.es":  {"jos_mar_a", "jos_mar_a_agency_es"},
		"123@x.com":             {"u123", "u123_x_com"},
		"__@__":                 nil,
		strings.Repeat("a", 80): {strings.Repeat("a", 63)},
	} {
		if got := candidateNames(in); !slices.Equal(got, want) {
			t.Errorf("candidateNames(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestGroupRoles(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, r := range []string{"analysts", "editors"} {
		if err := f.dir.CreateRole(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range []auth.GroupRole{{Group: "Data Team", Role: "analysts"}, {Group: "Editors", Role: "editors"},
		{Group: "igloo-writers", Role: "writer"}, {Group: "igloo-admins", Role: "admin"}} {
		if err := f.dir.GrantGroupRole(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	roles := func(user string) []string {
		p, _ := f.dir.UserPrivileges(ctx, user)
		return p.Roles
	}

	ana, err := f.signIn(t, person("s-ana", "ana@agency.com", "Data Team", "Unmapped"))
	if err != nil || ana.Role != auth.RoleNone || !slices.Equal(roles("ana"), []string{"analysts"}) {
		t.Fatalf("first sign-in: %+v %v %v", ana, roles("ana"), err)
	}
	if !slices.Contains(f.changed, "ana") {
		t.Error("open sessions were not told")
	}
	ana, _ = f.signIn(t, person("s-ana", "ana@agency.com", "Data Team", "Editors", "igloo-writers", "igloo-admins"))
	if ana.Role != auth.RoleAdmin || !slices.Equal(roles("ana"), []string{"analysts", "editors"}) {
		t.Errorf("more groups: %s %v", ana.Role, roles("ana"))
	}
	// An admin's own grant of a role stays when the group goes.
	if err := f.dir.AddMember(ctx, "editors", "ana", "root"); err != nil {
		t.Fatal(err)
	}
	ana, _ = f.signIn(t, person("s-ana", "ana@agency.com", "igloo-writers"))
	if ana.Role != auth.RoleWriter || !slices.Equal(roles("ana"), []string{"editors"}) {
		t.Errorf("fewer groups: %s %v", ana.Role, roles("ana"))
	}
	if u, _ := f.dir.Get(ctx, "ana"); u.Role != auth.RoleWriter {
		t.Errorf("stored role %s", u.Role)
	}
	ana, _ = f.signIn(t, person("s-ana", "ana@agency.com"))
	if ana.Role != auth.RoleNone {
		t.Errorf("no groups: %s", ana.Role)
	}

	// Single sign-on never removes the last admin.
	if err := f.dir.Delete(ctx, "root"); !errors.Is(err, auth.ErrLastAdmin) {
		t.Fatalf("dropping root: %v", err)
	}
	if _, err := f.signIn(t, person("s-ana", "ana@agency.com", "igloo-admins")); err != nil {
		t.Fatal(err)
	}
	if err := f.dir.SetRole(ctx, "root", auth.RoleReader); err != nil {
		t.Fatal(err)
	}
	if ana, err := f.signIn(t, person("s-ana", "ana@agency.com")); err != nil || ana.Role != auth.RoleAdmin {
		t.Errorf("the last admin: %+v %v", ana, err)
	}
}

func TestFlowRejectsTampering(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	start := func() (Pending, string) {
		authURL, pending, err := f.svc.Begin(ctx, callback)
		if err != nil {
			t.Fatal(err)
		}
		f.idp.SignInAs(person("s1", "ana@agency.com"))
		back, err := f.idp.Authorize(authURL)
		if err != nil {
			t.Fatal(err)
		}
		return pending, back.Query().Get("code")
	}

	pending, code := start()
	if pending.CheckState("forged") || (Pending{}).CheckState("") {
		t.Error("a forged state passed")
	}
	tampered := pending
	tampered.Verifier = "another-verifier-of-sufficient-length-0123456789"
	if _, err := f.svc.Finish(ctx, tampered, code); err == nil {
		t.Error("a code was exchanged without its PKCE verifier")
	}
	pending, code = start()
	tampered = pending
	tampered.Nonce = "other"
	if _, err := f.svc.Finish(ctx, tampered, code); err == nil || !strings.Contains(err.Error(), "nonce") {
		t.Errorf("a replayed ID token: %v", err)
	}
	pending, code = start()
	if _, err := f.svc.Finish(ctx, pending, code); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Finish(ctx, pending, code); err == nil {
		t.Error("a code was exchanged twice")
	}
	pending, code = start()
	pending.Started = time.Now().Add(-PendingLifetime - time.Second)
	if _, err := f.svc.Finish(ctx, pending, code); err == nil {
		t.Error("a sign-in that took too long finished")
	}

	// A provider that cannot be reached fails sign-ins, and says so.
	down, err := New(Options{Config: config.OIDC{Issuer: "http://127.0.0.1:1/nothing", ClientID: "x"}, Users: f.dir, Accounts: f.dir, SSO: f.dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := down.Begin(ctx, callback); err == nil {
		t.Error("began a sign-in with an unreachable provider")
	}
	if st := down.Status(ctx); st.Reachable || st.Error == "" {
		t.Errorf("status of an unreachable provider: %+v", st)
	}
	if st := f.svc.Status(ctx); !st.Reachable || !st.ClientSecret || st.Issuer != f.idp.Issuer() {
		t.Errorf("status: %+v", st)
	}
}

func TestAccessTokens(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ana, err := f.signIn(t, person("s-ana", "ana@agency.com"))
	if err != nil {
		t.Fatal(err)
	}
	claims := person("s-ana", "ana@agency.com")
	good := f.idp.Sign(claims, "api://igloo", time.Hour)
	if err := f.svc.VerifyAccessToken(ctx, ana, good); err != nil {
		t.Errorf("a good access token: %v", err)
	}
	// A token of another subject is not ana's, though it has her email: the
	// provider may have given a deleted account's email to someone else.
	if err := f.svc.VerifyAccessToken(ctx, ana, f.idp.Sign(person("s-recycled", "ana@agency.com"), "api://igloo", time.Hour)); err == nil {
		t.Error("another subject with ana's email was accepted")
	}
	if err := f.dir.Create(ctx, auth.User{Name: "bob", Role: auth.RoleReader}); err != nil {
		t.Fatal(err)
	}
	bob, _ := f.dir.Get(ctx, "bob")
	for name, c := range map[string]struct {
		user  auth.User
		token string
	}{
		"another user's":        {bob, good},
		"another audience's":    {ana, f.idp.Sign(claims, "igloo", time.Hour)},
		"an expired":            {ana, f.idp.Sign(claims, "api://igloo", -time.Minute)},
		"an unknown key's":      {ana, f.idp.SignWithOtherKey(claims, "api://igloo")},
		"a malformed":           {ana, "not-a-jwt"},
		"another identity's":    {ana, f.idp.Sign(person("s-mallory", "mallory@agency.com"), "api://igloo", time.Hour)},
		"an unverified email's": {bob, f.idp.Sign(map[string]any{"sub": "s-bob", "email": "bob@agency.com"}, "api://igloo", time.Hour)},
	} {
		if err := f.svc.VerifyAccessToken(ctx, c.user, c.token); err == nil {
			t.Errorf("%s access token was accepted", name)
		}
	}
	// A user without an identity yet is recognised by name.
	if err := f.svc.VerifyAccessToken(ctx, bob, f.idp.Sign(person("s-bob", "bob@agency.com"), "api://igloo", time.Hour)); err != nil {
		t.Errorf("bob by name: %v", err)
	}
	// But not when an admin gave them an email, which must match then.
	if err := f.dir.Create(ctx, auth.User{Name: "cara", Role: auth.RoleWriter, Email: "cara.diaz@brand.example"}); err != nil {
		t.Fatal(err)
	}
	cara, _ := f.dir.Get(ctx, "cara")
	if err := f.svc.VerifyAccessToken(ctx, cara, f.idp.Sign(person("s-mallory", "cara@agency.com"), "api://igloo", time.Hour)); err == nil {
		t.Error("someone named like cara signed in as her, though she has another email")
	}
	if err := f.svc.VerifyAccessToken(ctx, cara, f.idp.Sign(person("s-cara", "cara.diaz@brand.example"), "api://igloo", time.Hour)); err != nil {
		t.Errorf("cara by email: %v", err)
	}

	off := newFixture(t, func(c *config.OIDC) { c.AccessTokenAudience = "" })
	if off.svc.AccessTokensEnabled() || off.svc.VerifyAccessToken(ctx, ana, good) == nil {
		t.Error("access tokens without an audience configured")
	}
}

// TestAccessTokenIDClaim: for providers whose access tokens have a subject
// of their own, the account ID claim recorded at sign-in stands in for it,
// and the email never does.
func TestAccessTokenIDClaim(t *testing.T) {
	ctx := context.Background()
	with := func(claims map[string]any, extra map[string]any) map[string]any {
		out := map[string]any{}
		for _, m := range []map[string]any{claims, extra} {
			for k, v := range m {
				out[k] = v
			}
		}
		return out
	}

	// Entra ID: the subject differs between applications; oid does not.
	f := newFixture(t)
	ana, err := f.signIn(t, with(person("s-ana-console", "ana@agency.com"), map[string]any{"oid": "oid-ana"}))
	if err != nil {
		t.Fatal(err)
	}
	apiToken := f.idp.Sign(with(person("s-ana-api", "ana@agency.com"), map[string]any{"oid": "oid-ana"}), "api://igloo", time.Hour)
	if err := f.svc.VerifyAccessToken(ctx, ana, apiToken); err == nil {
		t.Error("another subject was accepted without IGLOO_OIDC_ACCESS_TOKEN_ID_CLAIM")
	}
	cfg := f.svc.cfg
	cfg.AccessTokenIDClaim = "oid"
	if f.svc, err = New(Options{Config: cfg, Users: f.dir, Accounts: f.dir, SSO: f.dir}); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.VerifyAccessToken(ctx, ana, apiToken); err == nil || !strings.Contains(err.Error(), "has not signed in") {
		t.Errorf("before ana signed in with the claim set: %v", err)
	}
	if _, err := f.signIn(t, with(person("s-ana-console", "ana@agency.com"), map[string]any{"oid": "oid-ana"})); err != nil {
		t.Fatal(err)
	}
	if link, _ := f.dir.IdentityOf(ctx, ana.Name); link.AccountID != "oid-ana" {
		t.Errorf("recorded account ID %q", link.AccountID)
	}
	if err := f.svc.VerifyAccessToken(ctx, ana, apiToken); err != nil {
		t.Errorf("ana's oid: %v", err)
	}
	for name, claims := range map[string]map[string]any{
		"another account's oid, with ana's email": with(person("s-new-api", "ana@agency.com"), map[string]any{"oid": "oid-new"}),
		"no oid, with ana's email":                person("s-new-api", "ana@agency.com"),
	} {
		if err := f.svc.VerifyAccessToken(ctx, ana, f.idp.Sign(claims, "api://igloo", time.Hour)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}

	// Okta: the access token's subject is the username, its uid the ID
	// token's subject.
	okta := newFixture(t, func(c *config.OIDC) { c.AccessTokenIDClaim = "uid" })
	bo, err := okta.signIn(t, person("00u-bo", "bo@agency.com"))
	if err != nil {
		t.Fatal(err)
	}
	if err := okta.svc.VerifyAccessToken(ctx, bo, okta.idp.Sign(with(person("bo@agency.com", "bo@agency.com"),
		map[string]any{"uid": "00u-bo"}), "api://igloo", time.Hour)); err != nil {
		t.Errorf("bo's uid: %v", err)
	}
	if err := okta.svc.VerifyAccessToken(ctx, bo, okta.idp.Sign(with(person("bo@agency.com", "bo@agency.com"),
		map[string]any{"uid": "00u-new"}), "api://igloo", time.Hour)); err == nil {
		t.Error("a new account given bo's username was accepted")
	}
}
