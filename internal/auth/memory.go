package auth

import (
	"context"
	"crypto/rand"
	"slices"
	"sort"
	"sync"
	"time"
)

// MemoryDirectory keeps users, custom roles and grants, access tokens and
// single sign-on links in memory. It implements every store interface of
// this package, for tests of the packages that use them; servers without a
// metadata database use MemoryStore, which refuses changes.
type MemoryDirectory struct {
	*MemoryGrants

	mu         sync.Mutex
	users      map[string]User
	tokens     map[string]memoryToken
	identities []Identity
	groupRoles []GroupRole
	// ssoMembers are the custom role memberships SyncRoles added, by user.
	ssoMembers map[string][]string
	scimUsers  map[string]SCIMUser
	scimGroups map[string]SCIMGroup
	secret     []byte
}

type memoryToken struct {
	Token
	secret TokenSecret
}

// NewMemoryDirectory returns an empty directory.
func NewMemoryDirectory() *MemoryDirectory {
	d := &MemoryDirectory{
		users:      map[string]User{},
		tokens:     map[string]memoryToken{},
		ssoMembers: map[string][]string{},
		scimUsers:  map[string]SCIMUser{},
		scimGroups: map[string]SCIMGroup{},
		secret:     make([]byte, 32),
	}
	_, _ = rand.Read(d.secret)
	d.MemoryGrants = NewMemoryGrants(d)
	return d
}

func (d *MemoryDirectory) Get(_ context.Context, name string) (User, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	u, ok := d.users[name]
	if !ok {
		return User{}, ErrNotFound
	}
	return u, nil
}

func (d *MemoryDirectory) List(context.Context) ([]User, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]User, 0, len(d.users))
	for _, u := range d.users {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (d *MemoryDirectory) Create(ctx context.Context, u User) error {
	if roles, _ := d.Roles(ctx); slices.ContainsFunc(roles, func(r CustomRole) bool { return r.Name == u.Name }) {
		return ErrNameTaken
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.users[u.Name]; ok {
		return ErrExists
	}
	if u.Email != "" && d.emailTaken(u.Name, u.Email) {
		return ErrEmailTaken
	}
	if u.Kind == "" {
		u.Kind = KindPerson
	}
	u.CreatedAt = time.Now()
	d.users[u.Name] = u
	return nil
}

func (d *MemoryDirectory) emailTaken(name, email string) bool {
	for _, other := range d.users {
		if other.Name != name && other.Email != "" && NormalizeEmail(other.Email) == NormalizeEmail(email) {
			return true
		}
	}
	return false
}

// update changes one user. With admins set it refuses to leave no enabled
// admin.
func (d *MemoryDirectory) update(name string, admins bool, change func(*User) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	u, ok := d.users[name]
	if !ok {
		return ErrNotFound
	}
	before := u
	if err := change(&u); err != nil {
		return err
	}
	if admins && enabledAdmin(before) && !enabledAdmin(u) && d.enabledAdmins() == 1 {
		return ErrLastAdmin
	}
	d.users[name] = u
	return nil
}

// enabledAdmin reports whether u counts towards the admins that must
// remain: enabled people with the admin role.
func enabledAdmin(u User) bool { return u.Role == RoleAdmin && !u.Disabled && !u.IsService() }

func (d *MemoryDirectory) enabledAdmins() int {
	n := 0
	for _, u := range d.users {
		if enabledAdmin(u) {
			n++
		}
	}
	return n
}

func (d *MemoryDirectory) SetVerifier(_ context.Context, name, verifier string) error {
	return d.update(name, false, func(u *User) error { u.Verifier = verifier; return nil })
}

func (d *MemoryDirectory) SetRole(_ context.Context, name string, role Role) error {
	return d.update(name, true, func(u *User) error { u.Role = role; return nil })
}

func (d *MemoryDirectory) Delete(ctx context.Context, name string) error {
	d.mu.Lock()
	u, ok := d.users[name]
	if !ok {
		d.mu.Unlock()
		return ErrNotFound
	}
	if enabledAdmin(u) && d.enabledAdmins() == 1 {
		d.mu.Unlock()
		return ErrLastAdmin
	}
	delete(d.users, name)
	for id, t := range d.tokens {
		if t.User == name {
			delete(d.tokens, id)
		}
	}
	d.identities = slices.DeleteFunc(d.identities, func(i Identity) bool { return i.User == name })
	delete(d.ssoMembers, name)
	d.forgetSCIMUser(name)
	d.mu.Unlock()
	return d.ForgetUser(ctx, name)
}

func (d *MemoryDirectory) EnsureAdmin(_ context.Context, name, verifier string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.users[name]; ok {
		return false, nil
	}
	d.users[name] = User{Name: name, Role: RoleAdmin, Verifier: verifier, Kind: KindPerson, CreatedAt: time.Now()}
	return true, nil
}

func (d *MemoryDirectory) MockSecret(context.Context) ([]byte, error) { return d.secret, nil }
func (d *MemoryDirectory) Ping(context.Context) error                 { return nil }
func (d *MemoryDirectory) Close()                                     {}

func (d *MemoryDirectory) SetLogin(_ context.Context, name string, enabled bool) error {
	return d.update(name, true, func(u *User) error {
		if !enabled && !u.Disabled {
			u.Epoch++
		}
		u.Disabled = !enabled
		return nil
	})
}

func (d *MemoryDirectory) SetOwner(_ context.Context, name, owner string) error {
	return d.update(name, false, func(u *User) error { u.Owner = owner; return nil })
}

func (d *MemoryDirectory) SetEmail(_ context.Context, name, email string) error {
	return d.update(name, false, func(u *User) error {
		if email != "" && d.emailTaken(name, email) {
			return ErrEmailTaken
		}
		u.Email = email
		return nil
	})
}

func (d *MemoryDirectory) SetPassword(_ context.Context, name, verifier string, tokenSalt []byte) error {
	return d.update(name, false, func(u *User) error {
		u.Verifier = verifier
		if tokenSalt != nil {
			u.TokenSalt = tokenSalt
		}
		return nil
	})
}

func (d *MemoryDirectory) EnsureTokenSalt(_ context.Context, name string, salt []byte) ([]byte, error) {
	var stored []byte
	err := d.update(name, false, func(u *User) error {
		if len(u.TokenSalt) == 0 {
			u.TokenSalt = salt
		}
		stored = u.TokenSalt
		return nil
	})
	return stored, err
}

func (d *MemoryDirectory) UserByEmail(_ context.Context, email string) (User, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, u := range d.users {
		if u.Email != "" && NormalizeEmail(u.Email) == NormalizeEmail(email) {
			return u, nil
		}
	}
	return User{}, ErrNotFound
}

func (d *MemoryDirectory) CreateToken(_ context.Context, t Token, secret TokenSecret) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.users[t.User]; !ok {
		return ErrNotFound
	}
	live := 0
	for id, other := range d.tokens {
		if other.User != t.User {
			continue
		}
		switch {
		case other.Name == t.Name && other.Expired(time.Now()):
			delete(d.tokens, id)
		case other.Name == t.Name:
			return ErrTokenExists
		case !other.Expired(time.Now()):
			live++
		}
	}
	if live >= MaxTokensPerUser {
		return ErrTooManyTokens
	}
	d.tokens[t.ID] = memoryToken{Token: t, secret: secret}
	return nil
}

func (d *MemoryDirectory) Tokens(_ context.Context, user string) ([]Token, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []Token
	for _, t := range d.tokens {
		if user == "" || t.User == user {
			out = append(out, t.Token)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (d *MemoryDirectory) DropToken(_ context.Context, user, name string) (Token, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, t := range d.tokens {
		if t.User == user && t.Name == name {
			delete(d.tokens, id)
			return t.Token, nil
		}
	}
	return Token{}, ErrTokenNotFound
}

func (d *MemoryDirectory) TokenCredentials(_ context.Context, user string) ([]TokenCredential, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []TokenCredential
	for _, t := range d.tokens {
		if t.User == user && !t.Expired(time.Now()) {
			out = append(out, TokenCredential{Token: t.Token, SCRAM: t.secret.SCRAM})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Token.Name < out[j].Token.Name })
	return out, nil
}

func (d *MemoryDirectory) TokenByID(_ context.Context, id string) (Token, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	t, ok := d.tokens[id]
	if !ok {
		return Token{}, nil, ErrTokenNotFound
	}
	return t.Token, t.secret.Hash, nil
}

func (d *MemoryDirectory) TokenUsed(_ context.Context, id string, at time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.tokens[id]; ok && at.After(t.LastUsedAt) {
		t.LastUsedAt = at
		d.tokens[id] = t
	}
	return nil
}

func (d *MemoryDirectory) IdentityBySubject(_ context.Context, issuer, subject string) (Identity, error) {
	return d.findIdentity(func(i Identity) bool { return i.Issuer == issuer && i.Subject == subject })
}

func (d *MemoryDirectory) IdentityOf(_ context.Context, user string) (Identity, error) {
	return d.findIdentity(func(i Identity) bool { return i.User == user })
}

func (d *MemoryDirectory) findIdentity(match func(Identity) bool) (Identity, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, i := range d.identities {
		if match(i) {
			i.Groups = slices.Clone(i.Groups)
			return i, nil
		}
	}
	return Identity{}, ErrIdentityNotFound
}

func (d *MemoryDirectory) Identities(context.Context) ([]Identity, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.identities), nil
}

func (d *MemoryDirectory) LinkIdentity(_ context.Context, id Identity) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.users[id.User]; !ok {
		return ErrNotFound
	}
	for _, i := range d.identities {
		if i.User == id.User || i.Issuer == id.Issuer && i.Subject == id.Subject {
			return ErrIdentityTaken
		}
	}
	id.CreatedAt, id.LastSignIn = time.Now(), time.Now()
	d.identities = append(d.identities, id)
	return nil
}

func (d *MemoryDirectory) UpdateIdentity(_ context.Context, id Identity) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, i := range d.identities {
		if i.Issuer == id.Issuer && i.Subject == id.Subject {
			d.identities[k].Email, d.identities[k].Groups, d.identities[k].LastSignIn = id.Email, slices.Clone(id.Groups), time.Now()
			return nil
		}
	}
	return ErrIdentityNotFound
}

func (d *MemoryDirectory) UnlinkIdentity(_ context.Context, user string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	before := len(d.identities)
	d.identities = slices.DeleteFunc(d.identities, func(i Identity) bool { return i.User == user })
	if u, ok := d.users[user]; ok && len(d.identities) < before {
		u.Epoch++
		d.users[user] = u
	}
	return nil
}

func (d *MemoryDirectory) GroupRoles(context.Context) ([]GroupRole, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := slices.Clone(d.groupRoles)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Role < out[j].Role
	})
	return out, nil
}

func (d *MemoryDirectory) GrantGroupRole(ctx context.Context, g GroupRole) error {
	if !IsBuiltinRole(g.Role) {
		roles, _ := d.Roles(ctx)
		if !slices.ContainsFunc(roles, func(r CustomRole) bool { return r.Name == g.Role }) {
			return ErrRoleNotFound
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !slices.ContainsFunc(d.groupRoles, func(h GroupRole) bool { return h.Group == g.Group && h.Role == g.Role }) {
		g.GrantedAt = time.Now()
		d.groupRoles = append(d.groupRoles, g)
	}
	return nil
}

func (d *MemoryDirectory) RevokeGroupRole(_ context.Context, group, role string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.groupRoles = slices.DeleteFunc(d.groupRoles, func(g GroupRole) bool { return g.Group == group && g.Role == role })
	return nil
}

// DropRole also removes the role's group mappings.
func (d *MemoryDirectory) DropRole(ctx context.Context, name string) error {
	if err := d.MemoryGrants.DropRole(ctx, name); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.groupRoles = slices.DeleteFunc(d.groupRoles, func(g GroupRole) bool { return g.Role == name })
	return nil
}

// AddMember makes a membership single sign-on added the admin's.
func (d *MemoryDirectory) AddMember(ctx context.Context, role, user, by string) error {
	if err := d.MemoryGrants.AddMember(ctx, role, user, by); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ssoMembers[user] = slices.DeleteFunc(d.ssoMembers[user], func(r string) bool { return r == role })
	return nil
}

func (d *MemoryDirectory) SyncRoles(ctx context.Context, user string, roles []string) error {
	privs, err := d.UserPrivileges(ctx, user)
	if err != nil {
		return err
	}
	existing, err := d.Roles(ctx)
	if err != nil {
		return err
	}
	d.mu.Lock()
	managed := slices.Clone(d.ssoMembers[user])
	d.mu.Unlock()
	var keep []string
	for _, r := range managed {
		if !slices.Contains(roles, r) {
			if err := d.MemoryGrants.RemoveMember(ctx, r, user); err != nil {
				return err
			}
			continue
		}
		keep = append(keep, r)
	}
	for _, r := range roles {
		if slices.Contains(privs.Roles, r) || !slices.ContainsFunc(existing, func(c CustomRole) bool { return c.Name == r }) {
			continue
		}
		if err := d.MemoryGrants.AddMember(ctx, r, user, SSOGrantor); err != nil {
			return err
		}
		keep = append(keep, r)
	}
	d.mu.Lock()
	d.ssoMembers[user] = keep
	d.mu.Unlock()
	return nil
}

func (d *MemoryDirectory) SCIMUsers(context.Context) ([]SCIMUser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]SCIMUser, 0, len(d.scimUsers))
	for _, u := range d.scimUsers {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].User < out[j].User })
	return out, nil
}

func (d *MemoryDirectory) SCIMUser(_ context.Context, user string) (SCIMUser, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	u, ok := d.scimUsers[user]
	if !ok {
		return SCIMUser{}, ErrSCIMNotFound
	}
	return u, nil
}

func (d *MemoryDirectory) PutSCIMUser(_ context.Context, u SCIMUser) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.users[u.User]; !ok {
		return ErrNotFound
	}
	now := time.Now()
	u.Created, u.Modified = now, now
	if old, ok := d.scimUsers[u.User]; ok {
		u.Created = old.Created
	}
	d.scimUsers[u.User] = u
	return nil
}

func (d *MemoryDirectory) DeleteSCIMUser(_ context.Context, user string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.forgetSCIMUser(user)
	return nil
}

// forgetSCIMUser removes user from provisioning; d.mu is held.
func (d *MemoryDirectory) forgetSCIMUser(user string) {
	delete(d.scimUsers, user)
	for id, g := range d.scimGroups {
		g.Members = slices.DeleteFunc(slices.Clone(g.Members), func(m string) bool { return m == user })
		d.scimGroups[id] = g
	}
}

func (d *MemoryDirectory) SCIMGroups(context.Context) ([]SCIMGroup, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]SCIMGroup, 0, len(d.scimGroups))
	for _, g := range d.scimGroups {
		g.Members = slices.Clone(g.Members)
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DisplayName < out[j].DisplayName })
	return out, nil
}

func (d *MemoryDirectory) SCIMGroup(_ context.Context, id string) (SCIMGroup, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	g, ok := d.scimGroups[id]
	if !ok {
		return SCIMGroup{}, ErrSCIMNotFound
	}
	g.Members = slices.Clone(g.Members)
	return g, nil
}

func (d *MemoryDirectory) CreateSCIMGroup(_ context.Context, g SCIMGroup) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.putSCIMGroup(g, true)
}

func (d *MemoryDirectory) ModifySCIMGroup(_ context.Context, id string, change func(*SCIMGroup) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	g, ok := d.scimGroups[id]
	if !ok {
		return ErrSCIMNotFound
	}
	g.Members = slices.Clone(g.Members)
	if err := change(&g); err != nil {
		return err
	}
	g.ID = id
	return d.putSCIMGroup(g, false)
}

// putSCIMGroup checks and stores g; d.mu is held.
func (d *MemoryDirectory) putSCIMGroup(g SCIMGroup, create bool) error {
	for id, other := range d.scimGroups {
		if id != g.ID && other.DisplayName == g.DisplayName || create && id == g.ID {
			return ErrSCIMGroupExists
		}
	}
	var members []string
	for _, m := range g.Members {
		if _, ok := d.scimUsers[m]; !ok {
			return ErrNotFound
		}
		if !slices.Contains(members, m) {
			members = append(members, m)
		}
	}
	slices.Sort(members)
	g.Members = members
	now := time.Now()
	if create {
		g.Created = now
	}
	g.Modified = now
	d.scimGroups[g.ID] = g
	return nil
}

func (d *MemoryDirectory) DeleteSCIMGroup(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.scimGroups[id]; !ok {
		return ErrSCIMNotFound
	}
	delete(d.scimGroups, id)
	return nil
}

func (d *MemoryDirectory) UserSCIMGroups(_ context.Context, user string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, g := range d.scimGroups {
		if slices.Contains(g.Members, user) {
			out = append(out, g.DisplayName)
		}
	}
	slices.Sort(out)
	return out, nil
}

func (d *MemoryDirectory) SCIMGroupNames(_ context.Context, names []string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, g := range d.scimGroups {
		if slices.Contains(names, g.DisplayName) {
			out = append(out, g.DisplayName)
		}
	}
	slices.Sort(out)
	return out, nil
}

var (
	_ Store        = (*MemoryDirectory)(nil)
	_ GrantStore   = (*MemoryDirectory)(nil)
	_ AccountStore = (*MemoryDirectory)(nil)
	_ TokenStore   = (*MemoryDirectory)(nil)
	_ SSOStore     = (*MemoryDirectory)(nil)
	_ SCIMStore    = (*MemoryDirectory)(nil)
)
