package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// directory is a store with everything: users, grants, accounts, tokens
// and single sign-on.
type directory interface {
	Store
	GrantStore
	AccountStore
	TokenStore
	SSOStore
	SCIMStore
}

// testDirectory checks a directory that holds no users yet.
func testDirectory(t *testing.T, d directory) {
	t.Helper()
	ctx := context.Background()
	rootVerifier, err := NewVerifier("root-password")
	if err != nil {
		t.Fatal(err)
	}
	salt, _ := NewSalt()
	for _, u := range []User{
		{Name: "root", Role: RoleAdmin, Verifier: rootVerifier},
		{Name: "ana", Role: RoleNone, TokenSalt: salt},
		{Name: "svc", Role: RoleWriter, Kind: KindService, Owner: "root", TokenSalt: salt},
	} {
		if err := d.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	if u, err := d.Get(ctx, "svc"); err != nil || !u.IsService() || u.Owner != "root" || u.HasPassword() {
		t.Errorf("service account: %+v %v", u, err)
	}
	if u, _ := d.Get(ctx, "ana"); u.Kind != KindPerson || string(u.TokenSalt) != string(salt) {
		t.Errorf("ana: %+v", u)
	}

	// Emails are unique, compared without case.
	if err := d.SetEmail(ctx, "ana", "Ana@Example.com"); err != nil {
		t.Fatal(err)
	}
	if u, err := d.UserByEmail(ctx, "ana@example.COM"); err != nil || u.Name != "ana" {
		t.Errorf("UserByEmail: %+v %v", u, err)
	}
	if _, err := d.UserByEmail(ctx, "nobody@example.com"); !errors.Is(err, ErrNotFound) {
		t.Errorf("UserByEmail of nobody: %v", err)
	}
	if err := d.Create(ctx, User{Name: "bob", Role: RoleReader, Email: "ana@example.com", Verifier: rootVerifier}); !errors.Is(err, ErrEmailTaken) {
		t.Errorf("a second user with ana's email: %v", err)
	}
	if err := d.SetEmail(ctx, "root", "ANA@example.com"); !errors.Is(err, ErrEmailTaken) {
		t.Errorf("taking ana's email: %v", err)
	}
	if err := d.SetEmail(ctx, "ghost", "x@example.com"); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetEmail of nobody: %v", err)
	}

	// The last enabled admin cannot be disabled; disabling ends sessions.
	if err := d.SetLogin(ctx, "root", false); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("disabling the last admin: %v", err)
	}
	if err := d.Create(ctx, User{Name: "boss", Role: RoleAdmin, Verifier: rootVerifier}); err != nil {
		t.Fatal(err)
	}
	if err := d.SetLogin(ctx, "root", false); err != nil {
		t.Fatal(err)
	}
	if u, _ := d.Get(ctx, "root"); !u.Disabled || u.Epoch != 1 {
		t.Errorf("disabled root: %+v", u)
	}
	if err := d.SetRole(ctx, "boss", RoleReader); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("demoting the last enabled admin: %v", err)
	}
	// An admin service account, which cannot use the console, does not
	// count.
	if err := d.Create(ctx, User{Name: "robot", Role: RoleAdmin, Kind: KindService, TokenSalt: salt}); err != nil {
		t.Fatal(err)
	}
	if err := d.SetLogin(ctx, "boss", false); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("disabling the last person admin beside an admin service account: %v", err)
	}
	if err := d.Delete(ctx, "robot"); err != nil {
		t.Errorf("dropping an admin service account: %v", err)
	}
	if err := d.SetLogin(ctx, "root", true); err != nil {
		t.Fatal(err)
	}
	if u, _ := d.Get(ctx, "root"); u.Disabled || u.Epoch != 1 {
		t.Errorf("enabled root: %+v", u)
	}
	if err := d.SetOwner(ctx, "svc", "boss"); err != nil {
		t.Fatal(err)
	}
	if u, _ := d.Get(ctx, "svc"); u.Owner != "boss" {
		t.Errorf("owner: %+v", u)
	}

	// Removing a password keeps its salt for tokens.
	if err := d.Create(ctx, User{Name: "pat", Role: RoleReader, Verifier: rootVerifier}); err != nil {
		t.Fatal(err)
	}
	pat, _ := d.Get(ctx, "pat")
	factors, _ := TokenFactors(pat)
	if err := d.SetPassword(ctx, "pat", "", []byte(factors.Salt)); err != nil {
		t.Fatal(err)
	}
	pat, _ = d.Get(ctx, "pat")
	if after, ok := TokenFactors(pat); !ok || after != factors || pat.HasPassword() {
		t.Errorf("after removing the password: %+v %+v", pat, after)
	}

	// Tokens.
	ana, _ := d.Get(ctx, "ana")
	factors, _ = TokenFactors(ana)
	week := time.Now().Add(7 * 24 * time.Hour)
	secret, tok, hashes, err := NewToken("ana", "dbt", "ana", week, factors)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateToken(ctx, tok, hashes); err != nil {
		t.Fatal(err)
	}
	_, again, againHashes, _ := NewToken("ana", "dbt", "ana", week, factors)
	if err := d.CreateToken(ctx, again, againHashes); !errors.Is(err, ErrTokenExists) {
		t.Errorf("a second token named dbt: %v", err)
	}
	_, ghost, ghostHashes, _ := NewToken("ghost", "x", "root", week, factors)
	if err := d.CreateToken(ctx, ghost, ghostHashes); !errors.Is(err, ErrNotFound) {
		t.Errorf("a token for nobody: %v", err)
	}
	got, hash, err := d.TokenByID(ctx, tok.ID)
	if err != nil || got.Name != "dbt" || got.User != "ana" || string(hash) != string(HashToken(secret)) ||
		got.ExpiresAt.Sub(tok.ExpiresAt).Abs() > time.Millisecond {
		t.Errorf("TokenByID: %+v %v", got, err)
	}
	if _, _, err := d.TokenByID(ctx, "nosuchtoken00000"); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("TokenByID of nothing: %v", err)
	}
	creds, err := d.TokenCredentials(ctx, "ana")
	if err != nil || len(creds) != 1 || !CheckSecret(creds[0].SCRAM, secret) || creds[0].Token.ID != tok.ID {
		t.Errorf("TokenCredentials: %+v %v", creds, err)
	}
	used := time.Now().Truncate(time.Second)
	if err := d.TokenUsed(ctx, tok.ID, used); err != nil {
		t.Fatal(err)
	}
	if err := d.TokenUsed(ctx, tok.ID, used.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	list, err := d.Tokens(ctx, "ana")
	if err != nil || len(list) != 1 || !list[0].LastUsedAt.Equal(used) {
		t.Errorf("Tokens: %+v %v", list, err)
	}

	// An expired token signs in no more, and its name is free again.
	_, old, oldHashes, _ := NewToken("ana", "old", "ana", time.Now().Add(-time.Minute), factors)
	if err := d.CreateToken(ctx, old, oldHashes); err != nil {
		t.Fatal(err)
	}
	if creds, _ := d.TokenCredentials(ctx, "ana"); len(creds) != 1 {
		t.Errorf("an expired token has credentials: %+v", creds)
	}
	_, renewed, renewedHashes, _ := NewToken("ana", "old", "ana", week, factors)
	if err := d.CreateToken(ctx, renewed, renewedHashes); err != nil {
		t.Errorf("reusing an expired token's name: %v", err)
	}
	if all, _ := d.Tokens(ctx, ""); len(all) != 2 {
		t.Errorf("every token: %+v", all)
	}
	dropped, err := d.DropToken(ctx, "ana", "old")
	if err != nil || dropped.ID != renewed.ID {
		t.Errorf("DropToken: %+v %v", dropped, err)
	}
	if _, err := d.DropToken(ctx, "ana", "old"); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("dropping twice: %v", err)
	}

	// Each user has a limited number of tokens.
	for i := range MaxTokensPerUser {
		_, tk, h, _ := NewToken("svc", fmt.Sprintf("t%d", i), "root", week, factors)
		if err := d.CreateToken(ctx, tk, h); err != nil {
			t.Fatal(err)
		}
	}
	_, extra, extraHashes, _ := NewToken("svc", "one-too-many", "root", week, factors)
	if err := d.CreateToken(ctx, extra, extraHashes); !errors.Is(err, ErrTooManyTokens) {
		t.Errorf("too many tokens: %v", err)
	}

	// Single sign-on identities.
	link := Identity{Issuer: "https://idp", Subject: "s-ana", User: "ana", Email: "ana@example.com", Groups: []string{"g1"}}
	if err := d.LinkIdentity(ctx, link); err != nil {
		t.Fatal(err)
	}
	if err := d.LinkIdentity(ctx, Identity{Issuer: "https://idp", Subject: "s-ana", User: "pat"}); !errors.Is(err, ErrIdentityTaken) {
		t.Errorf("linking a linked identity: %v", err)
	}
	if err := d.LinkIdentity(ctx, Identity{Issuer: "https://idp", Subject: "s-other", User: "ana"}); !errors.Is(err, ErrIdentityTaken) {
		t.Errorf("linking a linked user: %v", err)
	}
	if err := d.LinkIdentity(ctx, Identity{Issuer: "https://idp", Subject: "s-ghost", User: "ghost"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("linking nobody: %v", err)
	}
	if id, err := d.IdentityBySubject(ctx, "https://idp", "s-ana"); err != nil || id.User != "ana" || !slices.Equal(id.Groups, []string{"g1"}) {
		t.Errorf("IdentityBySubject: %+v %v", id, err)
	}
	if _, err := d.IdentityBySubject(ctx, "https://other", "s-ana"); !errors.Is(err, ErrIdentityNotFound) {
		t.Errorf("another issuer's subject: %v", err)
	}
	link.Groups, link.Email = []string{"g2", "g3"}, "ana@example.org"
	if err := d.UpdateIdentity(ctx, link); err != nil {
		t.Fatal(err)
	}
	if id, err := d.IdentityOf(ctx, "ana"); err != nil || id.Subject != "s-ana" || !slices.Equal(id.Groups, []string{"g2", "g3"}) ||
		id.Email != "ana@example.org" {
		t.Errorf("IdentityOf: %+v %v", id, err)
	}
	if ids, err := d.Identities(ctx); err != nil || len(ids) != 1 {
		t.Errorf("Identities: %+v %v", ids, err)
	}
	// Unlinking ends the user's console sessions.
	before, _ := d.Get(ctx, "pat")
	if err := d.LinkIdentity(ctx, Identity{Issuer: "https://idp", Subject: "s-pat", User: "pat"}); err != nil {
		t.Fatal(err)
	}
	if err := d.UnlinkIdentity(ctx, "pat"); err != nil {
		t.Fatal(err)
	}
	if after, _ := d.Get(ctx, "pat"); after.Epoch != before.Epoch+1 {
		t.Errorf("epoch after unlinking: %d, was %d", after.Epoch, before.Epoch)
	}
	if _, err := d.IdentityOf(ctx, "pat"); !errors.Is(err, ErrIdentityNotFound) {
		t.Errorf("after unlinking: %v", err)
	}

	// Group mappings and the memberships they give.
	for _, r := range []string{"analysts", "editors"} {
		if err := d.CreateRole(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, g := range []GroupRole{{Group: "g2", Role: "analysts"}, {Group: "g2", Role: "writer"}, {Group: "g9", Role: "editors"}} {
		g.GrantedBy = "root"
		if err := d.GrantGroupRole(ctx, g); err != nil {
			t.Fatal(err)
		}
		if err := d.GrantGroupRole(ctx, g); err != nil {
			t.Errorf("mapping twice: %v", err)
		}
	}
	if err := d.GrantGroupRole(ctx, GroupRole{Group: "g2", Role: "ghosts"}); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("mapping to no role: %v", err)
	}
	if gr, err := d.GroupRoles(ctx); err != nil || len(gr) != 3 || gr[0].Group != "g2" || gr[0].Role != "analysts" || gr[0].GrantedBy != "root" {
		t.Errorf("GroupRoles: %+v %v", gr, err)
	}
	if err := d.RevokeGroupRole(ctx, "g2", "writer"); err != nil {
		t.Fatal(err)
	}

	roles := func() []string {
		t.Helper()
		p, err := d.UserPrivileges(ctx, "ana")
		if err != nil {
			t.Fatal(err)
		}
		return p.Roles
	}
	if err := d.SyncRoles(ctx, "ana", []string{"analysts", "ghosts"}); err != nil {
		t.Fatal(err)
	}
	if r := roles(); !slices.Equal(r, []string{"analysts"}) {
		t.Errorf("after the first sync: %v", r)
	}
	if err := d.AddMember(ctx, "editors", "ana", "root"); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncRoles(ctx, "ana", nil); err != nil {
		t.Fatal(err)
	}
	if r := roles(); !slices.Equal(r, []string{"editors"}) {
		t.Errorf("a sync removed an admin's grant, or kept its own: %v", r)
	}
	// An admin granting a role single sign-on gave makes it theirs.
	if err := d.SyncRoles(ctx, "ana", []string{"analysts"}); err != nil {
		t.Fatal(err)
	}
	if err := d.AddMember(ctx, "analysts", "ana", "root"); err != nil {
		t.Fatal(err)
	}
	if err := d.SyncRoles(ctx, "ana", nil); err != nil {
		t.Fatal(err)
	}
	if r := roles(); !slices.Equal(r, []string{"analysts", "editors"}) {
		t.Errorf("a sync removed a membership an admin granted again: %v", r)
	}
	if err := d.DropRole(ctx, "editors"); err != nil {
		t.Fatal(err)
	}
	if gr, _ := d.GroupRoles(ctx); len(gr) != 1 || gr[0].Role != "analysts" {
		t.Errorf("a dropped role's mapping remains: %+v", gr)
	}

	// SCIM provisioning.
	if err := d.PutSCIMUser(ctx, SCIMUser{User: "ghost"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("provisioning nobody: %v", err)
	}
	for _, u := range []SCIMUser{{User: "ana", ExternalID: "x1", UserName: "Ana@Example.com"}, {User: "ana", ExternalID: "x1",
		UserName: "ana@example.com", GivenName: "Ana"}, {User: "pat", ExternalID: "x2", UserName: "pat@example.com"}} {
		if err := d.PutSCIMUser(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	if u, err := d.SCIMUser(ctx, "ana"); err != nil || u.UserName != "ana@example.com" || u.GivenName != "Ana" || u.ExternalID != "x1" {
		t.Errorf("SCIMUser: %+v %v", u, err)
	}
	if _, err := d.SCIMUser(ctx, "root"); !errors.Is(err, ErrSCIMNotFound) {
		t.Errorf("SCIMUser of an unprovisioned user: %v", err)
	}
	if all, err := d.SCIMUsers(ctx); err != nil || len(all) != 2 {
		t.Errorf("SCIMUsers: %+v %v", all, err)
	}
	if err := d.CreateSCIMGroup(ctx, SCIMGroup{ID: "g-1", DisplayName: "Data Team", Members: []string{"pat", "ana", "ana"}}); err != nil {
		t.Fatal(err)
	}
	if err := d.CreateSCIMGroup(ctx, SCIMGroup{ID: "g-2", DisplayName: "Data Team"}); !errors.Is(err, ErrSCIMGroupExists) {
		t.Errorf("a second group of the same name: %v", err)
	}
	if err := d.CreateSCIMGroup(ctx, SCIMGroup{ID: "g-3", DisplayName: "Ghosts", Members: []string{"ghost"}}); !errors.Is(err, ErrNotFound) {
		t.Errorf("a group with a member who is not a user: %v", err)
	}
	if g, err := d.SCIMGroup(ctx, "g-1"); err != nil || !slices.Equal(g.Members, []string{"ana", "pat"}) {
		t.Errorf("SCIMGroup: %+v %v", g, err)
	}
	err = d.ModifySCIMGroup(ctx, "g-1", func(g *SCIMGroup) error {
		g.DisplayName = "Analysts"
		g.Members = slices.DeleteFunc(g.Members, func(m string) bool { return m == "pat" })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if groups, err := d.UserSCIMGroups(ctx, "ana"); err != nil || !slices.Equal(groups, []string{"Analysts"}) {
		t.Errorf("UserSCIMGroups: %v %v", groups, err)
	}
	if groups, _ := d.UserSCIMGroups(ctx, "pat"); len(groups) != 0 {
		t.Errorf("pat's groups: %v", groups)
	}
	if names, err := d.SCIMGroupNames(ctx, []string{"Writers", "Analysts", "analysts"}); err != nil || !slices.Equal(names, []string{"Analysts"}) {
		t.Errorf("SCIMGroupNames: %v %v", names, err)
	}
	if names, err := d.SCIMGroupNames(ctx, nil); err != nil || len(names) != 0 {
		t.Errorf("SCIMGroupNames of none: %v %v", names, err)
	}
	if err := d.ModifySCIMGroup(ctx, "nope", func(*SCIMGroup) error { return nil }); !errors.Is(err, ErrSCIMNotFound) {
		t.Errorf("modifying no group: %v", err)
	}
	failed := errors.New("no")
	if err := d.ModifySCIMGroup(ctx, "g-1", func(g *SCIMGroup) error { g.DisplayName = "x"; return failed }); !errors.Is(err, failed) {
		t.Errorf("a failed change: %v", err)
	}
	if g, _ := d.SCIMGroup(ctx, "g-1"); g.DisplayName != "Analysts" {
		t.Errorf("a failed change was kept: %+v", g)
	}
	if groups, _ := d.SCIMGroups(ctx); len(groups) != 1 {
		t.Errorf("SCIMGroups: %+v", groups)
	}
	if err := d.DeleteSCIMUser(ctx, "pat"); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SCIMUser(ctx, "pat"); !errors.Is(err, ErrSCIMNotFound) {
		t.Errorf("after DeleteSCIMUser: %v", err)
	}
	if u, err := d.Get(ctx, "pat"); err != nil || u.Name != "pat" {
		t.Errorf("DeleteSCIMUser dropped the user: %v", err)
	}
	if err := d.DeleteSCIMGroup(ctx, "g-1"); err != nil {
		t.Fatal(err)
	}
	if err := d.DeleteSCIMGroup(ctx, "g-1"); !errors.Is(err, ErrSCIMNotFound) {
		t.Errorf("deleting twice: %v", err)
	}
	if err := d.CreateSCIMGroup(ctx, SCIMGroup{ID: "g-4", DisplayName: "Everyone", Members: []string{"ana"}}); err != nil {
		t.Fatal(err)
	}

	// Dropping a user drops their tokens and identity, and their
	// provisioning.
	if err := d.Delete(ctx, "ana"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.TokenByID(ctx, tok.ID); !errors.Is(err, ErrTokenNotFound) {
		t.Errorf("a dropped user's token remains: %v", err)
	}
	if _, err := d.IdentityBySubject(ctx, "https://idp", "s-ana"); !errors.Is(err, ErrIdentityNotFound) {
		t.Errorf("a dropped user's identity remains: %v", err)
	}
	if err := d.UnlinkIdentity(ctx, "ana"); err != nil {
		t.Errorf("unlinking nobody: %v", err)
	}
	if _, err := d.SCIMUser(ctx, "ana"); !errors.Is(err, ErrSCIMNotFound) {
		t.Errorf("a dropped user is still provisioned: %v", err)
	}
	if g, _ := d.SCIMGroup(ctx, "g-4"); len(g.Members) != 0 {
		t.Errorf("a dropped user is still a member: %+v", g)
	}
}

func TestMemoryDirectory(t *testing.T) {
	testDirectory(t, NewMemoryDirectory())
}

// TestPostgresDirectory runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresDirectory(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, `TRUNCATE igloo_auth.users, igloo_auth.roles, igloo_auth.grants, igloo_auth.sso_group_roles,
		igloo_auth.scim_groups CASCADE`); err != nil {
		t.Fatal(err)
	}
	testDirectory(t, s)
}

// TestPostgresAccountsMigration checks that users made by an older
// version, without this version's columns, get them with defaults.
func TestPostgresAccountsMigration(t *testing.T) {
	url := testdb.URL()
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, stmt := range []string{
		"TRUNCATE igloo_auth.users, igloo_auth.roles, igloo_auth.grants CASCADE",
		"DROP TABLE igloo_auth.tokens, igloo_auth.identities",
		"DROP INDEX igloo_auth.users_email",
		`ALTER TABLE igloo_auth.users DROP COLUMN kind, DROP COLUMN owner, DROP COLUMN disabled,
			DROP COLUMN email, DROP COLUMN token_salt, DROP COLUMN epoch`,
		"INSERT INTO igloo_auth.users (name, role, verifier) VALUES ('old', 'admin', 'v')",
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
	u, err := s.Get(ctx, "old")
	if err != nil || u.Kind != KindPerson || u.Disabled || u.Email != "" || u.TokenSalt != nil || u.Epoch != 0 {
		t.Errorf("an older user after the migration: %+v %v", u, err)
	}
	if err := s.Create(ctx, User{Name: "svc", Role: RoleReader, Kind: "robot"}); err == nil {
		t.Error("an unknown user type was accepted")
	}
}
