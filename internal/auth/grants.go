package auth

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Privilege is what a grant allows on a table or schema of the Iceberg
// catalog.
type Privilege string

const (
	PrivilegeSelect Privilege = "SELECT"
	PrivilegeInsert Privilege = "INSERT"
	PrivilegeUpdate Privilege = "UPDATE"
	PrivilegeDelete Privilege = "DELETE"
	// PrivilegeCreate exists on schemas only. It allows creating,
	// replacing, altering, commenting on and dropping the schema's tables
	// and views.
	PrivilegeCreate Privilege = "CREATE"
)

var (
	// TablePrivileges are what ALL PRIVILEGES means on a table.
	TablePrivileges = []Privilege{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate, PrivilegeDelete}
	// SchemaPrivileges are what ALL PRIVILEGES means on a schema.
	SchemaPrivileges = []Privilege{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate, PrivilegeDelete, PrivilegeCreate}
)

// ParsePrivilege validates a privilege name.
func ParsePrivilege(s string) (Privilege, bool) {
	p := Privilege(strings.ToUpper(s))
	return p, slices.Contains(SchemaPrivileges, p)
}

// Object is a schema of the Iceberg catalog, or a table or view in one.
// Names compare case-insensitively, as DuckDB resolves them.
type Object struct {
	Schema string
	// Table is empty for the schema itself.
	Table string
}

// IsSchema reports whether o is a schema rather than a table or view.
func (o Object) IsSchema() bool { return o.Table == "" }

// Kind is "schema" or "table".
func (o Object) Kind() string {
	if o.IsSchema() {
		return "schema"
	}
	return "table"
}

// String names o for messages: schema or schema.table.
func (o Object) String() string {
	if o.IsSchema() {
		return o.Schema
	}
	return o.Schema + "." + o.Table
}

// Equal compares objects case-insensitively.
func (o Object) Equal(other Object) bool {
	return strings.EqualFold(o.Schema, other.Schema) && strings.EqualFold(o.Table, other.Table)
}

// Grantee types: a grant names a user or a custom role. Users and custom
// roles share one namespace, so a name is never both.
const (
	GranteeUser = "user"
	GranteeRole = "role"
)

// Grant gives a user or custom role a privilege on an object. A grant on a
// schema covers every table and view in it, including ones created later.
type Grant struct {
	Grantee     string
	GranteeType string
	Privilege   Privilege
	Object      Object
	GrantedBy   string
	GrantedAt   time.Time
}

// sameGrant reports whether a and b give the same privilege to the same
// grantee on the same object.
func sameGrant(a, b Grant) bool {
	return a.Grantee == b.Grantee && a.GranteeType == b.GranteeType && a.Privilege == b.Privilege &&
		a.Object.Equal(b.Object) && a.Object.IsSchema() == b.Object.IsSchema()
}

// CustomRole is a named set of grants that users can be given.
type CustomRole struct {
	Name      string
	Members   []string
	CreatedAt time.Time
}

// Privileges are the grants that apply to one user: their own and those of
// the custom roles they have.
type Privileges struct {
	Roles  []string
	Grants []Grant
}

// Empty reports whether p allows nothing.
func (p *Privileges) Empty() bool { return p == nil || len(p.Grants) == 0 }

// Allows reports whether p includes priv on the table or view schema.table,
// through a grant on it or on its schema.
func (p *Privileges) Allows(priv Privilege, schema, table string) bool {
	if p == nil {
		return false
	}
	for _, g := range p.Grants {
		if g.Privilege == priv && strings.EqualFold(g.Object.Schema, schema) &&
			(g.Object.IsSchema() || strings.EqualFold(g.Object.Table, table)) {
			return true
		}
	}
	return false
}

// AllowsSchema reports whether p includes priv on schema itself.
func (p *Privileges) AllowsSchema(priv Privilege, schema string) bool {
	if p == nil {
		return false
	}
	for _, g := range p.Grants {
		if g.Privilege == priv && g.Object.IsSchema() && strings.EqualFold(g.Object.Schema, schema) {
			return true
		}
	}
	return false
}

var (
	ErrRoleNotFound = errors.New("role does not exist")
	ErrRoleExists   = errors.New("role already exists")
)

// GrantStore keeps custom roles, their members and grants. Implementations
// must be safe for concurrent use.
type GrantStore interface {
	// CreateRole fails with ErrRoleExists, or ErrNameTaken if a user has
	// the name.
	CreateRole(ctx context.Context, name string) error
	// DropRole removes the role, its memberships and its grants.
	DropRole(ctx context.Context, name string) error
	// Roles lists the custom roles and their members, by name.
	Roles(ctx context.Context) ([]CustomRole, error)
	// AddMember grants role to user. It fails with ErrRoleNotFound or
	// ErrNotFound when either does not exist.
	AddMember(ctx context.Context, role, user, by string) error
	// RemoveMember revokes role from user.
	RemoveMember(ctx context.Context, role, user string) error
	// Grant adds grants; ones that exist already are left as they are. It
	// fails with ErrNotFound or ErrRoleNotFound if a grantee does not
	// exist.
	Grant(ctx context.Context, grants []Grant) error
	// Revoke removes grants, ignoring ones that do not exist.
	Revoke(ctx context.Context, grants []Grant) error
	// Grants lists every grant.
	Grants(ctx context.Context) ([]Grant, error)
	// UserPrivileges returns what applies to user: their custom roles and
	// every grant to them or to those roles.
	UserPrivileges(ctx context.Context, user string) (Privileges, error)
	// ForgetUser removes a dropped user's memberships and grants.
	ForgetUser(ctx context.Context, user string) error
	// ForgetObject removes the grants on a dropped table or view, or on a
	// dropped schema and everything in it.
	ForgetObject(ctx context.Context, o Object) error
	// RenameObject moves a renamed table's grants to its new name.
	RenameObject(ctx context.Context, from, to Object) error
}

// MemoryGrants is a GrantStore in memory, for tests and for servers without
// a metadata database. Users is consulted to check that grantees exist.
type MemoryGrants struct {
	users    Store
	mu       sync.Mutex
	roles    map[string]*CustomRole
	grants   []Grant
	settings []Setting
}

// NewMemoryGrants returns an empty store whose users come from users.
func NewMemoryGrants(users Store) *MemoryGrants {
	return &MemoryGrants{users: users, roles: map[string]*CustomRole{}}
}

func (m *MemoryGrants) CreateRole(ctx context.Context, name string) error {
	if _, err := m.users.Get(ctx, name); err == nil {
		return ErrNameTaken
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.roles[name]; ok {
		return ErrRoleExists
	}
	m.roles[name] = &CustomRole{Name: name, CreatedAt: time.Now()}
	return nil
}

func (m *MemoryGrants) DropRole(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.roles[name]; !ok {
		return ErrRoleNotFound
	}
	delete(m.roles, name)
	m.removeGrants(func(g Grant) bool { return g.GranteeType == GranteeRole && g.Grantee == name })
	m.forgetSettings(GranteeRole, name)
	return nil
}

func (m *MemoryGrants) Roles(context.Context) ([]CustomRole, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CustomRole, 0, len(m.roles))
	for _, r := range m.roles {
		c := *r
		c.Members = slices.Clone(r.Members)
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *MemoryGrants) AddMember(ctx context.Context, role, user, _ string) error {
	if _, err := m.users.Get(ctx, user); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roles[role]
	if !ok {
		return ErrRoleNotFound
	}
	if !slices.Contains(r.Members, user) {
		r.Members = append(r.Members, user)
		slices.Sort(r.Members)
	}
	return nil
}

func (m *MemoryGrants) RemoveMember(_ context.Context, role, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.roles[role]
	if !ok {
		return ErrRoleNotFound
	}
	r.Members = slices.DeleteFunc(r.Members, func(u string) bool { return u == user })
	return nil
}

func (m *MemoryGrants) Grant(ctx context.Context, grants []Grant) error {
	for _, g := range grants {
		if g.GranteeType == GranteeUser {
			if _, err := m.users.Get(ctx, g.Grantee); err != nil {
				return err
			}
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range grants {
		if g.GranteeType == GranteeRole && m.roles[g.Grantee] == nil {
			return ErrRoleNotFound
		}
	}
	for _, g := range grants {
		if !slices.ContainsFunc(m.grants, func(h Grant) bool { return sameGrant(g, h) }) {
			if g.GrantedAt.IsZero() {
				g.GrantedAt = time.Now()
			}
			m.grants = append(m.grants, g)
		}
	}
	return nil
}

func (m *MemoryGrants) Revoke(_ context.Context, grants []Grant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeGrants(func(h Grant) bool {
		return slices.ContainsFunc(grants, func(g Grant) bool { return sameGrant(g, h) })
	})
	return nil
}

func (m *MemoryGrants) Grants(context.Context) ([]Grant, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.grants), nil
}

func (m *MemoryGrants) UserPrivileges(_ context.Context, user string) (Privileges, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var p Privileges
	for _, r := range m.roles {
		if slices.Contains(r.Members, user) {
			p.Roles = append(p.Roles, r.Name)
		}
	}
	slices.Sort(p.Roles)
	for _, g := range m.grants {
		if (g.GranteeType == GranteeUser && g.Grantee == user) ||
			(g.GranteeType == GranteeRole && slices.Contains(p.Roles, g.Grantee)) {
			p.Grants = append(p.Grants, g)
		}
	}
	return p, nil
}

func (m *MemoryGrants) ForgetUser(_ context.Context, user string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.roles {
		r.Members = slices.DeleteFunc(r.Members, func(u string) bool { return u == user })
	}
	m.removeGrants(func(g Grant) bool { return g.GranteeType == GranteeUser && g.Grantee == user })
	m.forgetSettings(GranteeUser, user)
	return nil
}

func (m *MemoryGrants) ForgetObject(_ context.Context, o Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeGrants(func(g Grant) bool { return covers(o, g.Object) })
	return nil
}

func (m *MemoryGrants) RenameObject(_ context.Context, from, to Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// A rename to the same name, or one that changes its case only, keeps
	// the table's grants.
	if !from.Equal(to) {
		m.removeGrants(func(g Grant) bool { return !g.Object.IsSchema() && g.Object.Equal(to) })
	}
	for i, g := range m.grants {
		if !g.Object.IsSchema() && g.Object.Equal(from) {
			m.grants[i].Object = to
		}
	}
	return nil
}

func (m *MemoryGrants) removeGrants(drop func(Grant) bool) {
	m.grants = slices.DeleteFunc(m.grants, drop)
}

// covers reports whether forgetting o removes grants on target: o itself,
// or anything in o when o is a schema.
func covers(o, target Object) bool {
	if o.IsSchema() {
		return strings.EqualFold(o.Schema, target.Schema)
	}
	return !target.IsSchema() && o.Equal(target)
}
