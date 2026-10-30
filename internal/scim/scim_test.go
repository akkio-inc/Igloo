package scim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
	"github.com/akkio-inc/igloo/internal/sso"
)

type fixture struct {
	t       *testing.T
	dir     *auth.MemoryDirectory
	sso     *sso.Service
	srv     *httptest.Server
	token   string
	changed []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	f := &fixture{t: t, dir: auth.NewMemoryDirectory()}
	if _, err := f.dir.EnsureAdmin(ctx, "root", "v"); err != nil {
		t.Fatal(err)
	}
	svc, err := sso.New(sso.Options{Config: config.OIDC{Issuer: "https://idp.example.com", ClientID: "igloo",
		UsernameClaim: "email", GroupsClaim: "groups", CreateUsers: true}, Users: f.dir, Accounts: f.dir, SSO: f.dir, SCIM: f.dir})
	if err != nil {
		t.Fatal(err)
	}
	f.sso = svc
	f.token = f.mint("okta_scim", auth.RoleAdmin)
	h, err := New(Options{Users: f.dir, Accounts: f.dir, SCIM: f.dir, Tokens: auth.NewTokenChecker(f.dir, f.dir), SSO: svc,
		Changed: func(u string) { f.changed = append(f.changed, u) }})
	if err != nil {
		t.Fatal(err)
	}
	f.srv = httptest.NewServer(h)
	t.Cleanup(f.srv.Close)
	return f
}

// mint makes a service account with role and returns a token of it.
func (f *fixture) mint(name string, role auth.Role) string {
	f.t.Helper()
	salt, _ := auth.NewSalt()
	if err := f.dir.Create(context.Background(), auth.User{Name: name, Role: role, Kind: auth.KindService, TokenSalt: salt}); err != nil {
		f.t.Fatal(err)
	}
	u, _ := f.dir.Get(context.Background(), name)
	factors, _ := auth.TokenFactors(u)
	secret, tok, hashes, err := auth.NewToken(name, "scim", "root", time.Now().Add(time.Hour), factors)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.dir.CreateToken(context.Background(), tok, hashes); err != nil {
		f.t.Fatal(err)
	}
	return secret
}

// call sends a request with f's token and decodes the response.
func (f *fixture) call(method, path string, body any) (int, map[string]any) {
	f.t.Helper()
	return f.callAs(f.token, method, path, body)
}

func (f *fixture) callAs(token, method, path string, body any) (int, map[string]any) {
	f.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, f.srv.URL+Prefix+path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/scim+json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if ct := resp.Header.Get("Content-Type"); out != nil && ct != "application/scim+json" {
		f.t.Errorf("%s %s: Content-Type %q", method, path, ct)
	}
	return resp.StatusCode, out
}

func (f *fixture) user(name string) auth.User {
	f.t.Helper()
	u, err := f.dir.Get(context.Background(), name)
	if err != nil {
		f.t.Fatalf("user %s: %v", name, err)
	}
	return u
}

func (f *fixture) roles(name string) []string {
	f.t.Helper()
	p, err := f.dir.UserPrivileges(context.Background(), name)
	if err != nil {
		f.t.Fatal(err)
	}
	return p.Roles
}

func filterQuery(attr, value string) string {
	return "?filter=" + url.QueryEscape(attr+` eq "`+value+`"`)
}

func TestAuthentication(t *testing.T) {
	f := newFixture(t)
	if code, _ := f.callAs("", "GET", "/ServiceProviderConfig", nil); code != http.StatusUnauthorized {
		t.Errorf("no token: %d", code)
	}
	if code, out := f.callAs("igloo_"+strings.Repeat("a", 68), "GET", "/Users", nil); code != http.StatusUnauthorized || out["status"] != "401" {
		t.Errorf("a wrong token: %d %v", code, out)
	}
	reader := f.mint("reader_bot", auth.RoleReader)
	if code, _ := f.callAs(reader, "GET", "/Users", nil); code != http.StatusForbidden {
		t.Errorf("a non-admin's token: %d", code)
	}
	code, out := f.call("GET", "/ServiceProviderConfig", nil)
	if code != http.StatusOK || out["patch"].(map[string]any)["supported"] != true {
		t.Errorf("ServiceProviderConfig: %d %v", code, out)
	}
	for _, p := range []string{"/ResourceTypes", "/ResourceTypes/User", "/Schemas", "/Schemas/" + schemaGroup} {
		if code, _ := f.call("GET", p, nil); code != http.StatusOK {
			t.Errorf("%s: %d", p, code)
		}
	}
	if code, _ := f.call("GET", "/Nothing", nil); code != http.StatusNotFound {
		t.Errorf("an unknown endpoint: %d", code)
	}
}

// TestOktaUsers follows what Okta sends.
func TestOktaUsers(t *testing.T) {
	f := newFixture(t)
	if code, out := f.call("GET", "/Users"+filterQuery("userName", "ana.lopez@agency.com")+"&startIndex=1&count=100", nil); code != http.StatusOK ||
		out["totalResults"] != float64(0) || len(out["Resources"].([]any)) != 0 {
		t.Fatalf("searching before provisioning: %d %v", code, out)
	}
	code, out := f.call("POST", "/Users", map[string]any{
		"schemas":     []string{schemaUser},
		"userName":    "ana.lopez@agency.com",
		"name":        map[string]any{"givenName": "Ana", "familyName": "Lopez"},
		"emails":      []map[string]any{{"primary": true, "value": "Ana.Lopez@agency.com", "type": "work"}},
		"displayName": "Ana Lopez", "locale": "en-US", "externalId": "00u1",
		"groups": []any{}, "password": "ignored-secret", "active": true,
	})
	if code != http.StatusCreated || out["id"] != "ana_lopez" || out["active"] != true || out["externalId"] != "00u1" {
		t.Fatalf("POST /Users: %d %v", code, out)
	}
	if u := f.user("ana_lopez"); u.Email != "ana.lopez@agency.com" || u.Role != auth.RoleNone || u.Disabled || u.HasPassword() {
		t.Errorf("provisioned user %+v", u)
	}
	if code, out := f.call("GET", "/Users"+filterQuery("userName", "ANA.LOPEZ@agency.com"), nil); code != http.StatusOK ||
		out["totalResults"] != float64(1) {
		t.Errorf("searching after: %d %v", code, out)
	}
	if code, _ := f.call("POST", "/Users", map[string]any{"userName": "ana.lopez@agency.com"}); code != http.StatusConflict {
		t.Errorf("provisioning twice: %d", code)
	}

	code, out = f.call("PUT", "/Users/ana_lopez", map[string]any{
		"schemas": []string{schemaUser}, "id": "ana_lopez", "userName": "ana.lopez@agency.com",
		"name": map[string]any{"givenName": "Ana María", "familyName": "Lopez"}, "active": true,
		"emails": []map[string]any{{"primary": true, "value": "ana.lopez@agency.com"}},
	})
	if code != http.StatusOK || out["name"].(map[string]any)["givenName"] != "Ana María" {
		t.Errorf("PUT: %d %v", code, out)
	}

	// Deactivating disables the user, which ends their sessions.
	code, out = f.call("PATCH", "/Users/ana_lopez", map[string]any{
		"schemas": []string{schemaPatch}, "Operations": []any{map[string]any{"op": "replace", "value": map[string]any{"active": false}}},
	})
	if code != http.StatusOK || out["active"] != false || !f.user("ana_lopez").Disabled || !slices.Contains(f.changed, "ana_lopez") {
		t.Errorf("deactivating: %d %v", code, out)
	}
	if code, _ := f.call("PATCH", "/Users/ana_lopez", map[string]any{
		"Operations": []any{map[string]any{"op": "replace", "value": map[string]any{"active": true}}}}); code != http.StatusOK || f.user("ana_lopez").Disabled {
		t.Errorf("reactivating: %d", code)
	}
	if code, out := f.call("GET", "/Users/ana_lopez", nil); code != http.StatusOK || out["userName"] != "ana.lopez@agency.com" {
		t.Errorf("GET: %d %v", code, out)
	}
	if code, _ := f.call("GET", "/Users/root", nil); code != http.StatusNotFound {
		t.Errorf("an unprovisioned user: %d", code)
	}
}

// TestEntraUsers follows what Microsoft Entra ID sends.
func TestEntraUsers(t *testing.T) {
	f := newFixture(t)
	code, out := f.call("POST", "/Users", map[string]any{
		"schemas": []string{schemaUser, schemaEnterprise}, "externalId": "e1a2", "userName": "bo@agency.com", "active": true,
		"emails":         []map[string]any{{"primary": true, "type": "work", "value": "bo@agency.com"}},
		"name":           map[string]any{"formatted": "Bo Diaz", "familyName": "Diaz", "givenName": "Bo"},
		schemaEnterprise: map[string]any{"department": "Data"},
	})
	if code != http.StatusCreated || out["id"] != "bo" {
		t.Fatalf("POST: %d %v", code, out)
	}
	patch := func(ops ...map[string]any) (int, map[string]any) {
		t.Helper()
		list := make([]any, len(ops))
		for i, op := range ops {
			list[i] = op
		}
		return f.call("PATCH", "/Users/bo", map[string]any{"schemas": []string{schemaPatch}, "Operations": list})
	}
	if code, out := patch(map[string]any{"op": "Replace", "path": "active", "value": "False"}); code != http.StatusOK || !f.user("bo").Disabled {
		t.Errorf("Entra's deactivation: %d %v", code, out)
	}
	if code, _ := patch(map[string]any{"op": "Replace", "path": "active", "value": "True"},
		map[string]any{"op": "Add", "path": `emails[type eq "work"].value`, "value": "bo.diaz@agency.com"},
		map[string]any{"op": "Replace", "path": "name.givenName", "value": "Bonifacio"},
		map[string]any{"op": "Replace", "path": schemaEnterprise + ":department", "value": "Analytics"},
		map[string]any{"op": "Add", "path": "title", "value": "Analyst"}); code != http.StatusOK {
		t.Errorf("Entra's updates: %d", code)
	}
	if u := f.user("bo"); u.Disabled || u.Email != "bo.diaz@agency.com" {
		t.Errorf("after updates: %+v", u)
	}
	if code, out := f.call("GET", "/Users"+filterQuery("externalId", "e1a2"), nil); code != http.StatusOK || out["totalResults"] != float64(1) {
		t.Errorf("by externalId: %d %v", code, out)
	}
	for name, op := range map[string]map[string]any{
		"active as a word": {"op": "Replace", "path": "active", "value": "maybe"},
		"an unknown op":    {"op": "Move", "path": "active", "value": true},
		"remove all":       {"op": "Remove"},
	} {
		if code, _ := patch(op); code != http.StatusBadRequest {
			t.Errorf("%s: %d", name, code)
		}
	}

	// Deleting disables the user and ends their provisioning; an admin
	// drops them.
	if code, _ := f.call("DELETE", "/Users/bo", nil); code != http.StatusNoContent {
		t.Errorf("DELETE: %d", code)
	}
	if u := f.user("bo"); !u.Disabled {
		t.Errorf("deleted user %+v", u)
	}
	if code, _ := f.call("GET", "/Users/bo", nil); code != http.StatusNotFound {
		t.Errorf("GET after DELETE: %d", code)
	}
	// Provisioning them again picks up the same user, who stays disabled
	// until an admin enables them; later changes apply.
	if code, out := f.call("POST", "/Users", map[string]any{"userName": "bo@agency.com", "active": true,
		"emails": []map[string]any{{"value": "bo.diaz@agency.com"}}}); code != http.StatusCreated || out["id"] != "bo" ||
		out["active"] != false || !f.user("bo").Disabled {
		t.Errorf("provisioning again: %d %v", code, out)
	}
	if code, _ := patch(map[string]any{"op": "Replace", "path": "active", "value": "True"}); code != http.StatusOK || f.user("bo").Disabled {
		t.Errorf("activating after provisioning again: %d", code)
	}
}

func TestProvisioningFindsExistingUsers(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.dir.Create(ctx, auth.User{Name: "cara", Role: auth.RoleWriter, Email: "cara@agency.com", Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if code, out := f.call("POST", "/Users", map[string]any{"userName": "cara@agency.com"}); code != http.StatusCreated || out["id"] != "cara" ||
		f.user("cara").Role != auth.RoleWriter {
		t.Errorf("an existing user by email: %d %v", code, out)
	}
	if err := f.dir.Create(ctx, auth.User{Name: "etl", Role: auth.RoleWriter, Kind: auth.KindService, Email: "etl@agency.com"}); err != nil {
		t.Fatal(err)
	}
	if code, _ := f.call("POST", "/Users", map[string]any{"userName": "etl@agency.com"}); code != http.StatusConflict {
		t.Errorf("a service account's email: %d", code)
	}
	for name, body := range map[string]any{
		"no userName":  map[string]any{"emails": []any{}},
		"a bad active": map[string]any{"userName": "x@agency.com", "active": "sometimes"},
	} {
		if code, _ := f.call("POST", "/Users", body); code != http.StatusBadRequest {
			t.Errorf("%s: %d", name, code)
		}
	}
	if code, out := f.call("GET", "/Users?filter="+url.QueryEscape(`title co "x"`), nil); code != http.StatusBadRequest || out["scimType"] != "invalidFilter" {
		t.Errorf("an unsupported filter: %d %v", code, out)
	}
	req, _ := http.NewRequest("POST", f.srv.URL+Prefix+"/Users", strings.NewReader("userName=x"))
	req.Header.Set("Authorization", "Bearer "+f.token)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("a form: %v %v", resp, err)
	}
}

func TestGroups(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for _, u := range []string{"ana@agency.com", "bo@agency.com"} {
		if code, _ := f.call("POST", "/Users", map[string]any{"userName": u}); code != http.StatusCreated {
			t.Fatalf("provisioning %s: %d", u, code)
		}
	}
	if err := f.dir.CreateRole(ctx, "analysts"); err != nil {
		t.Fatal(err)
	}
	for _, m := range []auth.GroupRole{{Group: "Data Team", Role: "analysts"}, {Group: "Igloo Writers", Role: "writer"}} {
		if err := f.dir.GrantGroupRole(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	code, out := f.call("POST", "/Groups", map[string]any{"schemas": []string{schemaGroup}, "displayName": "Data Team",
		"externalId": "g-ext", "members": []map[string]any{{"value": "ana"}}})
	if code != http.StatusCreated || out["displayName"] != "Data Team" || len(out["members"].([]any)) != 1 {
		t.Fatalf("POST /Groups: %d %v", code, out)
	}
	id := out["id"].(string)
	if r := f.roles("ana"); !slices.Equal(r, []string{"analysts"}) {
		t.Errorf("ana's roles after joining: %v", r)
	}
	if code, _ := f.call("POST", "/Groups", map[string]any{"displayName": "Data Team"}); code != http.StatusConflict {
		t.Errorf("a second group of the name: %d", code)
	}
	if code, _ := f.call("POST", "/Groups", map[string]any{"displayName": "Ghosts", "members": []map[string]any{{"value": "ghost"}}}); code != http.StatusBadRequest {
		t.Errorf("a member who is not a user: %d", code)
	}

	patch := func(ops ...map[string]any) int {
		t.Helper()
		list := make([]any, len(ops))
		for i, op := range ops {
			list[i] = op
		}
		code, _ := f.call("PATCH", "/Groups/"+id, map[string]any{"schemas": []string{schemaPatch}, "Operations": list})
		return code
	}
	// Okta adds and removes members one way, Entra ID another.
	if code := patch(map[string]any{"op": "add", "path": "members", "value": []map[string]any{{"value": "bo", "display": "bo@agency.com"}}}); code != http.StatusNoContent {
		t.Errorf("adding a member: %d", code)
	}
	if r := f.roles("bo"); !slices.Equal(r, []string{"analysts"}) {
		t.Errorf("bo's roles: %v", r)
	}
	if code := patch(map[string]any{"op": "remove", "path": `members[value eq "ana"]`}); code != http.StatusNoContent || len(f.roles("ana")) != 0 {
		t.Errorf("Okta's removal: %d %v", code, f.roles("ana"))
	}
	if code := patch(map[string]any{"op": "Remove", "path": "members", "value": []map[string]any{{"value": "bo"}}}); code != http.StatusNoContent || len(f.roles("bo")) != 0 {
		t.Errorf("Entra's removal: %d %v", code, f.roles("bo"))
	}
	if code := patch(map[string]any{"op": "Replace", "path": "members", "value": []map[string]any{{"value": "ana"}, {"value": "bo"}}}); code != http.StatusNoContent {
		t.Errorf("replacing members: %d", code)
	}
	// A rename changes which mapping applies.
	if code := patch(map[string]any{"op": "replace", "value": map[string]any{"id": id, "displayName": "Igloo Writers"}}); code != http.StatusNoContent {
		t.Errorf("renaming: %d", code)
	}
	if u := f.user("ana"); u.Role != auth.RoleWriter || len(f.roles("ana")) != 0 {
		t.Errorf("after the rename: %s %v", u.Role, f.roles("ana"))
	}
	for name, op := range map[string]map[string]any{
		"an empty name":     {"op": "replace", "path": "displayName", "value": ""},
		"an unknown path":   {"op": "add", "path": "owners", "value": "x"},
		"a member of no id": {"op": "add", "path": "members", "value": []map[string]any{{"display": "x"}}},
	} {
		if code := patch(op); code != http.StatusBadRequest {
			t.Errorf("%s: %d", name, code)
		}
	}

	code, out = f.call("GET", "/Groups"+filterQuery("displayName", "igloo writers")+"&excludedAttributes=members", nil)
	if code != http.StatusOK || out["totalResults"] != float64(1) {
		t.Fatalf("filtering groups: %d %v", code, out)
	}
	if _, ok := out["Resources"].([]any)[0].(map[string]any)["members"]; ok {
		t.Error("members were not excluded")
	}
	// Single sign-on counts the provisioned groups with the token's.
	if _, err := f.sso.SignIn(ctx, sso.Identity{Issuer: "https://idp.example.com", Subject: "s-ana", Username: "ana@agency.com",
		Email: "ana@agency.com", Groups: []string{"Data Team"}}); err != nil {
		t.Fatal(err)
	}
	if u := f.user("ana"); u.Role != auth.RoleWriter || !slices.Equal(f.roles("ana"), []string{"analysts"}) {
		t.Errorf("after a sign-in: %s %v", u.Role, f.roles("ana"))
	}

	code, out = f.call("PUT", "/Groups/"+id, map[string]any{"displayName": "Igloo Writers", "members": []map[string]any{{"value": "bo"}}})
	if code != http.StatusOK || len(out["members"].([]any)) != 1 || f.user("ana").Role != auth.RoleNone {
		t.Errorf("PUT /Groups: %d %v, ana is %s", code, out, f.user("ana").Role)
	}
	if code, _ := f.call("DELETE", "/Groups/"+id, nil); code != http.StatusNoContent || f.user("bo").Role != auth.RoleNone {
		t.Errorf("DELETE /Groups: %d, bo is %s", code, f.user("bo").Role)
	}
	if code, _ := f.call("GET", "/Groups/"+id, nil); code != http.StatusNotFound {
		t.Errorf("a deleted group: %d", code)
	}
	if code, _ := f.call("PATCH", "/Groups/"+id, map[string]any{"Operations": []any{}}); code != http.StatusNotFound {
		t.Errorf("patching a deleted group: %d", code)
	}
}

// TestSCIMDecidesItsGroups: a group the provider pushes has the members
// SCIM says, whatever the groups saved from a sign-in's token: removing
// someone from it takes its roles away at once. The token's other groups
// still count.
func TestSCIMDecidesItsGroups(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if code, _ := f.call("POST", "/Users", map[string]any{"userName": "ana@agency.com"}); code != http.StatusCreated {
		t.Fatalf("provisioning ana: %d", code)
	}
	if err := f.dir.CreateRole(ctx, "analysts"); err != nil {
		t.Fatal(err)
	}
	for _, m := range []auth.GroupRole{{Group: "Data Team", Role: "analysts"}, {Group: "Igloo Writers", Role: "writer"}} {
		if err := f.dir.GrantGroupRole(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	code, out := f.call("POST", "/Groups", map[string]any{"displayName": "Igloo Writers", "members": []map[string]any{{"value": "ana"}}})
	if code != http.StatusCreated {
		t.Fatalf("POST /Groups: %d %v", code, out)
	}
	id := out["id"].(string)
	signIn := func() {
		t.Helper()
		if _, err := f.sso.SignIn(ctx, sso.Identity{Issuer: "https://idp.example.com", Subject: "s-ana", Username: "ana@agency.com",
			Email: "ana@agency.com", Groups: []string{"Igloo Writers", "Data Team"}}); err != nil {
			t.Fatal(err)
		}
	}
	signIn()
	if u := f.user("ana"); u.Role != auth.RoleWriter || !slices.Equal(f.roles("ana"), []string{"analysts"}) {
		t.Fatalf("after signing in: %s %v", u.Role, f.roles("ana"))
	}

	code, _ = f.call("PATCH", "/Groups/"+id, map[string]any{"schemas": []string{schemaPatch},
		"Operations": []any{map[string]any{"op": "remove", "path": `members[value eq "ana"]`}}})
	if code != http.StatusNoContent {
		t.Fatalf("removing ana: %d", code)
	}
	if u := f.user("ana"); u.Role != auth.RoleNone || !slices.Equal(f.roles("ana"), []string{"analysts"}) {
		t.Errorf("after SCIM removed ana from Igloo Writers: %s %v", u.Role, f.roles("ana"))
	}
	// A token that still lists the group does not put her back in it.
	signIn()
	if u := f.user("ana"); u.Role != auth.RoleNone || !slices.Equal(f.roles("ana"), []string{"analysts"}) {
		t.Errorf("after signing in with a token listing Igloo Writers: %s %v", u.Role, f.roles("ana"))
	}
	// Once SCIM no longer pushes the group, the token decides again.
	if code, _ := f.call("DELETE", "/Groups/"+id, nil); code != http.StatusNoContent {
		t.Fatalf("DELETE /Groups: %d", code)
	}
	signIn()
	if u := f.user("ana"); u.Role != auth.RoleWriter {
		t.Errorf("after the group stopped being pushed: %s", u.Role)
	}
}

func TestFilters(t *testing.T) {
	for in, want := range map[string]filter{
		`userName eq "ana@x.com"`:                                    {attr: "username", value: "ana@x.com"},
		`urn:ietf:params:scim:schemas:core:2.0:User:userName EQ "a"`: {attr: "username", value: "a"},
		`active eq true`:                                             {attr: "active", value: "true"},
		`displayName eq "Data \"Team\""`:                             {attr: "displayname", value: `Data "Team"`},
		` externalId   eq   "e1" `:                                   {attr: "externalid", value: "e1"},
	} {
		got, err := parseFilter(in)
		if err != nil || got == nil || *got != want {
			t.Errorf("parseFilter(%q) = %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{`userName co "a"`, `userName eq a`, `emails[type eq "work"].value eq "a"`, `a eq "b" and c eq "d"`, `eq "x"`} {
		if _, err := parseFilter(bad); err == nil {
			t.Errorf("parseFilter(%q) accepted", bad)
		}
	}
	if f, err := parseFilter(""); f != nil || err != nil {
		t.Error("an empty filter")
	}
	items := []int{1, 2, 3, 4, 5}
	if got := slice(items, 2, 2); !slices.Equal(got, []int{2, 3}) {
		t.Errorf("slice: %v", got)
	}
	if got := slice(items, 9, 2); got != nil {
		t.Errorf("slice past the end: %v", got)
	}
}
