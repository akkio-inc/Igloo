package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/testdb"
)

// testSettingStore checks a SettingStore whose users ana and bob exist,
// with no custom roles or settings yet. createRole and dropRole manage
// custom roles; dropUser drops a user with everything that is theirs.
func testSettingStore(t *testing.T, s SettingStore, g GrantStore, dropUser func(string) error) {
	t.Helper()
	ctx := context.Background()
	if err := g.CreateRole(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddMember(ctx, "acme", "ana", "root"); err != nil {
		t.Fatal(err)
	}
	for _, set := range []Setting{
		{TargetType: GranteeRole, Target: "acme", Name: "query_tag", Value: "client:acme", SetBy: "root"},
		{TargetType: GranteeRole, Target: "acme", Name: "max_concurrent_statements", Value: "4"},
		{TargetType: TargetBuiltin, Target: "reader", Name: "statement_timeout", Value: "1h"},
		{TargetType: GranteeUser, Target: "ana", Name: "memory_limit", Value: "2GB"},
		{TargetType: GranteeUser, Target: "bob", Name: "query_tag", Value: "client:bob"},
		// Setting it again replaces it.
		{TargetType: GranteeRole, Target: "acme", Name: "max_concurrent_statements", Value: "8"},
	} {
		if err := s.SetSetting(ctx, set); err != nil {
			t.Fatalf("%+v: %v", set, err)
		}
	}
	if err := s.SetSetting(ctx, Setting{TargetType: GranteeUser, Target: "nobody", Name: "query_tag", Value: "x"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("a missing user: %v", err)
	}
	if err := s.SetSetting(ctx, Setting{TargetType: GranteeRole, Target: "nobody", Name: "query_tag", Value: "x"}); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("a missing role: %v", err)
	}

	str := func(ss []Setting) string {
		var parts []string
		for _, x := range ss {
			parts = append(parts, x.TargetType+":"+x.Target+":"+x.Name+"="+x.Value)
		}
		return strings.Join(parts, ",")
	}
	all, err := s.Settings(ctx)
	if want := "builtin:reader:statement_timeout=1h,role:acme:max_concurrent_statements=8,role:acme:query_tag=client:acme," +
		"user:ana:memory_limit=2GB,user:bob:query_tag=client:bob"; err != nil || str(all) != want {
		t.Errorf("settings: %s, %v\nwant %s", str(all), err, want)
	}
	mine, err := s.UserSettings(ctx, "ana", RoleReader)
	if want := "builtin:reader:statement_timeout=1h,role:acme:max_concurrent_statements=8,role:acme:query_tag=client:acme," +
		"user:ana:memory_limit=2GB"; err != nil || str(mine) != want {
		t.Errorf("ana's settings: %s, %v\nwant %s", str(mine), err, want)
	}
	if mine, _ := s.UserSettings(ctx, "bob", RoleWriter); str(mine) != "user:bob:query_tag=client:bob" {
		t.Errorf("bob's settings: %s", str(mine))
	}

	if err := s.ResetSettings(ctx, GranteeRole, "acme", []string{"query_tag"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetSettings(ctx, TargetBuiltin, "reader", nil); err != nil {
		t.Fatal(err)
	}
	// Dropping a role or a user drops their settings.
	if err := g.DropRole(ctx, "acme"); err != nil {
		t.Fatal(err)
	}
	if err := dropUser("bob"); err != nil {
		t.Fatal(err)
	}
	if all, _ := s.Settings(ctx); str(all) != "user:ana:memory_limit=2GB" {
		t.Errorf("after resets and drops: %s", str(all))
	}
}

func TestMemorySettings(t *testing.T) {
	ctx := context.Background()
	users := &memoryUsers{MemoryStore: NewMemoryStore(), extra: map[string]bool{"ana": true, "bob": true}}
	g := NewMemoryGrants(users)
	testSettingStore(t, g, g, func(name string) error { return g.ForgetUser(ctx, name) })
}

// TestPostgresSettings runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresSettings(t *testing.T) {
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
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_auth.users, igloo_auth.roles, igloo_auth.grants, igloo_auth.role_settings CASCADE"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []User{{Name: "root", Role: RoleAdmin}, {Name: "ana", Role: RoleReader}, {Name: "bob", Role: RoleWriter}} {
		u.Verifier = "v"
		if err := s.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	testSettingStore(t, s, s, func(name string) error { return s.Delete(ctx, name) })
	// The database refuses targets that are not built-in roles.
	if err := s.SetSetting(ctx, Setting{TargetType: TargetBuiltin, Target: "superuser", Name: "query_tag", Value: "x"}); err == nil {
		t.Error("a setting on an unknown built-in role was stored")
	}
}
