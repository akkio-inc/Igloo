package auth

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
)

func TestPrivileges(t *testing.T) {
	p := &Privileges{Grants: []Grant{
		{Privilege: PrivilegeSelect, Object: Object{Schema: "public", Table: "orders"}},
		{Privilege: PrivilegeSelect, Object: Object{Schema: "Sales"}},
		{Privilege: PrivilegeCreate, Object: Object{Schema: "sales"}},
	}}
	cases := []struct {
		priv          Privilege
		schema, table string
		want          bool
	}{
		{PrivilegeSelect, "public", "orders", true},
		{PrivilegeSelect, "PUBLIC", "Orders", true},
		{PrivilegeInsert, "public", "orders", false},
		{PrivilegeSelect, "public", "secrets", false},
		{PrivilegeSelect, "sales", "anything", true},
		{PrivilegeSelect, "salesx", "anything", false},
	}
	for _, c := range cases {
		if got := p.Allows(c.priv, c.schema, c.table); got != c.want {
			t.Errorf("Allows(%s, %s.%s) = %v", c.priv, c.schema, c.table, got)
		}
	}
	if !p.AllowsSchema(PrivilegeCreate, "SALES") || p.AllowsSchema(PrivilegeCreate, "public") ||
		p.AllowsSchema(PrivilegeSelect, "public") {
		t.Error("AllowsSchema")
	}
	if !(&Privileges{}).Empty() || p.Empty() || (*Privileges)(nil).Allows(PrivilegeSelect, "a", "b") {
		t.Error("Empty")
	}
}

// TestConnectionPrivileges checks grants on connections, which never
// stand for the Iceberg catalog's objects of the same names, nor the other
// way round.
func TestConnectionPrivileges(t *testing.T) {
	p := &Privileges{Grants: []Grant{
		{Privilege: PrivilegeSelect, Object: Object{Catalog: "crm", Schema: "public", Table: "customers"}},
		{Privilege: PrivilegeSelect, Object: Object{Catalog: "crm", Schema: "Sales"}},
		{Privilege: PrivilegeSelect, Object: Object{Catalog: "ops"}},
		{Privilege: PrivilegeSelect, Object: Object{Schema: "marts"}},
	}}
	cases := []struct {
		catalog, schema, table string
		want                   bool
	}{
		{"crm", "public", "customers", true},
		{"CRM", "PUBLIC", "Customers", true},
		{"crm", "public", "pii", false},
		{"crm", "sales", "anything", true},
		{"ops", "any", "thing", true},
		{"other", "public", "customers", false},
		{"", "public", "customers", false},
		{"crm", "marts", "x", false},
	}
	for _, c := range cases {
		if got := p.AllowsConnection(PrivilegeSelect, c.catalog, c.schema, c.table); got != c.want {
			t.Errorf("AllowsConnection(%s.%s.%s) = %v", c.catalog, c.schema, c.table, got)
		}
	}
	if p.Allows(PrivilegeSelect, "public", "customers") || p.Allows(PrivilegeSelect, "sales", "x") || p.AllowsSchema(PrivilegeSelect, "sales") {
		t.Error("a grant on a connection's object allowed the Iceberg catalog's of the same name")
	}
	if !p.Allows(PrivilegeSelect, "marts", "x") {
		t.Error("a grant on an Iceberg schema")
	}
	for o, want := range map[Object]string{
		{Catalog: "crm"}:                                    "connection crm",
		{Catalog: "crm", Schema: "public"}:                  "schema crm.public",
		{Catalog: "crm", Schema: "public", Table: "orders"}: "table crm.public.orders",
		{Schema: "public", Table: "orders"}:                 "table public.orders",
		{Schema: "public"}:                                  "schema public",
	} {
		if got := o.Kind() + " " + o.String(); got != want {
			t.Errorf("%+v: %s, want %s", o, got, want)
		}
	}
	if (Object{Catalog: "crm", Schema: "s", Table: "t"}).Equal(Object{Schema: "s", Table: "t"}) {
		t.Error("Equal ignores the catalog")
	}
}

// testConnectionGrants checks a GrantStore's grants on connections; ana and
// bob exist.
func testConnectionGrants(t *testing.T, g GrantStore) {
	t.Helper()
	ctx := context.Background()
	customers := Object{Catalog: "crm", Schema: "public", Table: "customers"}
	lakeCustomers := Object{Schema: "public", Table: "customers"}
	grants := []Grant{
		{Grantee: "ana", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: customers, GrantedBy: "root"},
		{Grantee: "ana", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: Object{Catalog: "ops"}, GrantedBy: "root"},
		{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: lakeCustomers, GrantedBy: "root"},
	}
	if err := g.Grant(ctx, grants); err != nil {
		t.Fatal(err)
	}
	if err := g.Grant(ctx, grants[:1]); err != nil {
		t.Errorf("granting twice: %v", err)
	}
	p, err := g.UserPrivileges(ctx, "ana")
	if err != nil || !p.AllowsConnection(PrivilegeSelect, "crm", "public", "customers") || p.Allows(PrivilegeSelect, "public", "customers") ||
		!p.AllowsConnection(PrivilegeSelect, "ops", "x", "y") {
		t.Errorf("ana: %+v %v", p, err)
	}
	p, _ = g.UserPrivileges(ctx, "bob")
	if p.AllowsConnection(PrivilegeSelect, "crm", "public", "customers") || !p.Allows(PrivilegeSelect, "public", "customers") {
		t.Errorf("bob: %+v", p)
	}
	all, _ := g.Grants(ctx)
	if !slices.ContainsFunc(all, func(x Grant) bool { return x.Object.Equal(customers) }) {
		t.Errorf("Grants does not list a grant on a connection: %+v", all)
	}
	// Dropping or renaming the Iceberg table leaves the connection's alone.
	if err := g.ForgetObject(ctx, lakeCustomers); err != nil {
		t.Fatal(err)
	}
	if err := g.RenameObject(ctx, Object{Schema: "public", Table: "customers"}, Object{Schema: "public", Table: "other"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); !p.AllowsConnection(PrivilegeSelect, "crm", "public", "customers") {
		t.Errorf("an Iceberg table's drop took a connection's grant: %+v", p)
	}
	if err := g.Revoke(ctx, grants[1:2]); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); p.AllowsConnection(PrivilegeSelect, "ops", "x", "y") {
		t.Errorf("after revoke: %+v", p)
	}
	// Dropping the connection takes its grants.
	if err := g.ForgetObject(ctx, Object{Catalog: "CRM"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); !p.Empty() {
		t.Errorf("after the connection is dropped: %+v", p)
	}
	// Granted again, for the callers to check that dropping the user takes
	// it.
	if err := g.Grant(ctx, grants[:1]); err != nil {
		t.Fatal(err)
	}
}

// testGrantStore checks a GrantStore whose users ana and bob exist.
func testGrantStore(t *testing.T, g GrantStore) {
	t.Helper()
	ctx := context.Background()
	orders := Object{Schema: "public", Table: "orders"}
	sales := Object{Schema: "sales"}

	if err := g.CreateRole(ctx, "analyst"); err != nil {
		t.Fatal(err)
	}
	if err := g.CreateRole(ctx, "analyst"); !errors.Is(err, ErrRoleExists) {
		t.Errorf("duplicate role: %v", err)
	}
	if err := g.CreateRole(ctx, "ana"); !errors.Is(err, ErrNameTaken) {
		t.Errorf("role named like a user: %v", err)
	}
	if err := g.AddMember(ctx, "analyst", "ana", "root"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddMember(ctx, "analyst", "ana", "root"); err != nil {
		t.Errorf("adding a member twice: %v", err)
	}
	if err := g.AddMember(ctx, "ghost", "ana", "root"); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("unknown role: %v", err)
	}
	if err := g.AddMember(ctx, "analyst", "ghost", "root"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown user: %v", err)
	}

	grants := []Grant{
		{Grantee: "analyst", GranteeType: GranteeRole, Privilege: PrivilegeSelect, Object: orders, GrantedBy: "root"},
		{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeCreate, Object: sales, GrantedBy: "root"},
		{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: sales, GrantedBy: "root"},
	}
	if err := g.Grant(ctx, grants); err != nil {
		t.Fatal(err)
	}
	if err := g.Grant(ctx, grants[:1]); err != nil {
		t.Errorf("granting twice: %v", err)
	}
	if err := g.Grant(ctx, []Grant{{Grantee: "ghost", GranteeType: GranteeRole, Privilege: PrivilegeSelect, Object: orders}}); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("grant to an unknown role: %v", err)
	}
	all, err := g.Grants(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("Grants: %d %v", len(all), err)
	}

	p, err := g.UserPrivileges(ctx, "ana")
	if err != nil || !slices.Equal(p.Roles, []string{"analyst"}) || !p.Allows(PrivilegeSelect, "public", "orders") ||
		p.Allows(PrivilegeSelect, "sales", "leads") {
		t.Errorf("ana: %+v %v", p, err)
	}
	p, _ = g.UserPrivileges(ctx, "bob")
	if !p.AllowsSchema(PrivilegeCreate, "sales") || !p.Allows(PrivilegeSelect, "sales", "leads") || p.Allows(PrivilegeSelect, "public", "orders") {
		t.Errorf("bob: %+v", p)
	}

	roles, err := g.Roles(ctx)
	if err != nil || len(roles) != 1 || !slices.Equal(roles[0].Members, []string{"ana"}) {
		t.Errorf("Roles: %+v %v", roles, err)
	}

	// Renaming to the same name, or a case variant of it, keeps the grants.
	for _, same := range []Object{orders, {Schema: "PUBLIC", Table: "Orders"}, orders} {
		if err := g.RenameObject(ctx, orders, same); err != nil {
			t.Fatal(err)
		}
		if p, _ := g.UserPrivileges(ctx, "ana"); !p.Allows(PrivilegeSelect, "public", "orders") {
			t.Errorf("a rename to %s dropped the grants: %+v", same, p)
		}
	}

	// Renaming moves grants; dropping removes them.
	if err := g.RenameObject(ctx, orders, Object{Schema: "public", Table: "orders2"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); !p.Allows(PrivilegeSelect, "public", "orders2") || p.Allows(PrivilegeSelect, "public", "orders") {
		t.Errorf("after rename: %+v", p)
	}
	if err := g.ForgetObject(ctx, Object{Schema: "public", Table: "ORDERS2"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); !p.Empty() {
		t.Errorf("after drop: %+v", p)
	}
	if err := g.ForgetObject(ctx, sales); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "bob"); !p.Empty() {
		t.Errorf("after dropping the schema: %+v", p)
	}

	if err := g.Grant(ctx, grants); err != nil {
		t.Fatal(err)
	}
	if err := g.Revoke(ctx, grants[1:2]); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "bob"); p.AllowsSchema(PrivilegeCreate, "sales") || !p.Allows(PrivilegeSelect, "sales", "x") {
		t.Errorf("after revoke: %+v", p)
	}
	if err := g.RemoveMember(ctx, "analyst", "ana"); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); len(p.Roles) != 0 || !p.Empty() {
		t.Errorf("after removing the member: %+v", p)
	}
	if err := g.DropRole(ctx, "analyst"); err != nil {
		t.Fatal(err)
	}
	if err := g.DropRole(ctx, "analyst"); !errors.Is(err, ErrRoleNotFound) {
		t.Errorf("dropping twice: %v", err)
	}
	all, _ = g.Grants(ctx)
	for _, gr := range all {
		if gr.Grantee == "analyst" {
			t.Errorf("a dropped role's grant remains: %+v", gr)
		}
	}
}

func TestMemoryGrants(t *testing.T) {
	ctx := context.Background()
	users := &memoryUsers{MemoryStore: NewMemoryStore(), extra: map[string]bool{"ana": true, "bob": true}}
	if _, err := users.EnsureAdmin(ctx, "root", "v"); err != nil {
		t.Fatal(err)
	}
	g := NewMemoryGrants(users)
	testGrantStore(t, g)
	testConnectionGrants(t, g)
	if err := g.ForgetUser(ctx, "ana"); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "ana"); !p.Empty() {
		t.Errorf("a forgotten user keeps grants on connections: %+v", p)
	}

	if err := g.Grant(ctx, []Grant{{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: Object{Schema: "s"}}}); err != nil {
		t.Fatal(err)
	}
	if err := g.ForgetUser(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	if p, _ := g.UserPrivileges(ctx, "bob"); !p.Empty() {
		t.Errorf("a forgotten user keeps grants: %+v", p)
	}
}

// memoryUsers is a MemoryStore with extra users that Get finds.
type memoryUsers struct {
	*MemoryStore
	extra map[string]bool
}

func (m *memoryUsers) Get(ctx context.Context, name string) (User, error) {
	if m.extra[name] {
		return User{Name: name, Role: RoleNone}, nil
	}
	return m.MemoryStore.Get(ctx, name)
}

// TestPostgresGrants runs against a real database when
// IGLOO_TEST_DATABASE_URL is set.
func TestPostgresGrants(t *testing.T) {
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("IGLOO_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.pool.Exec(ctx, "TRUNCATE igloo_auth.users, igloo_auth.roles, igloo_auth.grants, igloo_auth.connection_grants CASCADE"); err != nil {
		t.Fatal(err)
	}
	for _, u := range []User{{Name: "root", Role: RoleAdmin}, {Name: "ana", Role: RoleNone}, {Name: "bob", Role: RoleReader}} {
		u.Verifier = "v"
		if err := s.Create(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	testGrantStore(t, s)
	testConnectionGrants(t, s)
	if err := s.Delete(ctx, "ana"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, User{Name: "ana", Role: RoleNone, Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.UserPrivileges(ctx, "ana"); !p.Empty() {
		t.Errorf("a dropped user's grants on connections reach a new user of the name: %+v", p)
	}
	// The table refuses privileges other than SELECT on connections.
	if err := s.Grant(ctx, []Grant{{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeInsert,
		Object: Object{Catalog: "crm"}}}); err == nil {
		t.Error("INSERT was granted on a connection")
	}

	// Users and roles share names.
	if err := s.CreateRole(ctx, "team"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, User{Name: "team", Role: RoleReader, Verifier: "v"}); !errors.Is(err, ErrNameTaken) {
		t.Errorf("user named like a role: %v", err)
	}
	// Dropping a user drops their memberships and grants with them.
	if err := s.AddMember(ctx, "team", "bob", "root"); err != nil {
		t.Fatal(err)
	}
	if err := s.Grant(ctx, []Grant{{Grantee: "bob", GranteeType: GranteeUser, Privilege: PrivilegeSelect, Object: Object{Schema: "s"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, User{Name: "bob", Role: RoleReader, Verifier: "v"}); err != nil {
		t.Fatal(err)
	}
	if p, err := s.UserPrivileges(ctx, "bob"); err != nil || len(p.Roles) != 0 || !p.Empty() {
		t.Errorf("a new bob inherited the old one's grants: %+v %v", p, err)
	}
}

// TestPostgresRoleNoneMigration checks that a database made by an older
// version, whose role check lacks none, accepts it after the migration.
func TestPostgresRoleNoneMigration(t *testing.T) {
	url := os.Getenv("IGLOO_TEST_DATABASE_URL")
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
		"ALTER TABLE igloo_auth.users DROP CONSTRAINT users_role_check",
		"ALTER TABLE igloo_auth.users ADD CHECK (role IN ('admin', 'writer', 'reader'))",
	} {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Create(ctx, User{Name: "nobody", Role: RoleNone, Verifier: "v"}); err == nil {
		t.Fatal("the old check accepted role none")
	}
	s2, err := OpenPostgresStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
	if err := s.Create(ctx, User{Name: "nobody", Role: RoleNone, Verifier: "v"}); err != nil {
		t.Errorf("after the migration: %v", err)
	}
	if err := s.Create(ctx, User{Name: "odd", Role: "superuser", Verifier: "v"}); err == nil {
		t.Error("the new check accepted an unknown role")
	}
}
