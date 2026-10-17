package auth

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/akkio-inc/igloo/internal/testdb"
)

type ownerStore interface {
	GrantStore
	OwnerStore
}

// testOwnerStore checks an owner store with users ana and bob and no
// roles.
func testOwnerStore(t *testing.T, s ownerStore) {
	t.Helper()
	ctx := context.Background()
	orders := Object{Schema: "public", Table: "orders"}
	owners := func() []string {
		t.Helper()
		ws, err := s.ObjectOwners(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, w := range ws {
			out = append(out, w.Object.String()+"="+w.Owner+"/"+w.OwnerType)
		}
		slices.Sort(out)
		return out
	}
	set := func(o Object, owner, kind string, keep bool) error {
		return s.SetObjectOwner(ctx, Ownership{Object: o, Owner: owner, OwnerType: kind, SetBy: "root"}, keep)
	}

	if err := set(orders, "ana", GranteeUser, false); err != nil {
		t.Fatal(err)
	}
	if err := set(Object{Schema: "public"}, "bob", GranteeUser, false); err != nil {
		t.Fatal(err)
	}
	// A schema and a table in it have owners of their own; keep leaves
	// one alone, and names match without regard to case.
	if err := set(Object{Schema: "PUBLIC", Table: "Orders"}, "bob", GranteeUser, true); err != nil {
		t.Fatal(err)
	}
	if got := owners(); !slices.Equal(got, []string{"public.orders=ana/user", "public=bob/user"}) {
		t.Errorf("owners: %q", got)
	}
	if err := set(orders, "nobody", GranteeUser, false); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown user: %v", err)
	}
	if err := set(orders, "team", GranteeRole, false); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("an unknown role: %v", err)
	}
	if err := s.CreateRole(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	if err := set(orders, "team", GranteeRole, false); err != nil {
		t.Fatal(err)
	}
	w, ok := OwnerOf(mustOwners(t, s), orders)
	if !ok || !w.Owns("ana", []string{"team"}) || w.Owns("ana", nil) {
		t.Errorf("owner of orders: %+v %v", w, ok)
	}

	// Owners follow renames, and go with drops of the object or the owner.
	if err := s.RenameObject(ctx, orders, Object{Schema: "public", Table: "sales"}); err != nil {
		t.Fatal(err)
	}
	if got := owners(); !slices.Equal(got, []string{"public.sales=team/role", "public=bob/user"}) {
		t.Errorf("after a rename: %q", got)
	}
	if err := s.DropRole(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	if got := owners(); !slices.Equal(got, []string{"public=bob/user"}) {
		t.Errorf("after the owning role was dropped: %q", got)
	}
	if err := set(orders, "ana", GranteeUser, false); err != nil {
		t.Fatal(err)
	}
	if err := s.ForgetObject(ctx, Object{Schema: "public"}); err != nil {
		t.Fatal(err)
	}
	if got := owners(); len(got) != 0 {
		t.Errorf("after the schema was dropped: %q", got)
	}
}

func mustOwners(t *testing.T, s OwnerStore) []Ownership {
	t.Helper()
	ws, err := s.ObjectOwners(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func TestMemoryOwners(t *testing.T) {
	users := &memoryUsers{MemoryStore: NewMemoryStore(), extra: map[string]bool{"ana": true, "bob": true}}
	g := NewMemoryGrants(users)
	testOwnerStore(t, g)
	if err := g.SetObjectOwner(context.Background(), Ownership{Object: Object{Schema: "s"}, Owner: "ana", OwnerType: GranteeUser}, false); err != nil {
		t.Fatal(err)
	}
	if err := g.ForgetUser(context.Background(), "ana"); err != nil {
		t.Fatal(err)
	}
	if ws := mustOwners(t, g); len(ws) != 0 {
		t.Errorf("a forgotten user still owns: %+v", ws)
	}
}

// TestPostgresOwners runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresOwners(t *testing.T) {
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
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_auth.users, igloo_auth.roles, igloo_auth.grants, igloo_auth.owners CASCADE"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []User{{Name: "root", Role: RoleAdmin}, {Name: "ana", Role: RoleNone}, {Name: "bob", Role: RoleReader}} {
		u.Verifier = "v"
		if err := s.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	testOwnerStore(t, s)

	// Dropping a user drops what they owned.
	if err := s.SetObjectOwner(ctx, Ownership{Object: Object{Schema: "s"}, Owner: "bob", OwnerType: GranteeUser}, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	if ws := mustOwners(t, s); len(ws) != 0 {
		t.Errorf("a dropped user still owns: %+v", ws)
	}
}
