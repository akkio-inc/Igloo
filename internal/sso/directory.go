package sso

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// Refusal is a sign-in the policy or the directory turns down. Code names
// the case for the sign-in page; Reason says why, for the login history.
type Refusal struct {
	Code   string
	Reason string
	// User is the Igloo user it concerned, when known.
	User string
}

func (r *Refusal) Error() string { return r.Reason }

// Refusal codes.
const (
	RefusedUnverified = "unverified"
	RefusedDomain     = "domain"
	RefusedGroup      = "group"
	RefusedNoUser     = "no_user"
	RefusedConflict   = "conflict"
	RefusedService    = "service"
	RefusedDisabled   = "disabled"
)

func refuse(code, user, format string, args ...any) error {
	return &Refusal{Code: code, User: user, Reason: fmt.Sprintf(format, args...)}
}

// admit checks id against the allowed domains and groups.
func (s *Service) admit(id Identity) error {
	if id.Username == "" {
		return refuse(RefusedUnverified, "", "the identity provider sent no verified %s claim", s.cfg.UsernameClaim)
	}
	if len(s.cfg.AllowedDomains) > 0 && !slices.Contains(s.cfg.AllowedDomains, id.Domain) {
		return refuse(RefusedDomain, "", "domain %q is not allowed", id.Domain)
	}
	if len(s.cfg.AllowedGroups) > 0 && !slices.ContainsFunc(id.Groups, func(g string) bool { return slices.Contains(s.cfg.AllowedGroups, g) }) {
		return refuse(RefusedGroup, "", "%s is in none of the allowed groups", id.Username)
	}
	return nil
}

// SignIn admits id, finds its Igloo user, creating one if configured to,
// and brings the user's email and roles in line with the identity. Users
// are found by, in order:
//
//  1. an earlier sign-in of the same identity (issuer and subject);
//  2. their email, if the provider verified the identity's;
//  3. their name, derived from the username claim (the part before any @,
//     lowercase, other characters as _), if that user has no password, no
//     email, no other identity and no SCIM provisioning, as an admin
//     creates one for single sign-on;
//  4. or else, with CreateUsers, a new user of that name with role none,
//     or if it is taken, the name followed by the domain.
//
// A user found by 2 to 4 is linked to the identity. Service accounts and
// disabled users are refused. The roles follow the token's groups, which
// the identity keeps, and those SCIM gives the user (see applyRoles). A
// failure is a *Refusal or an error of the stores.
func (s *Service) SignIn(ctx context.Context, id Identity) (auth.User, error) {
	if err := s.admit(id); err != nil {
		return auth.User{}, err
	}
	user, err := s.find(ctx, id)
	if err != nil {
		return auth.User{}, err
	}
	switch {
	case user.IsService():
		return auth.User{}, refuse(RefusedService, user.Name, "%s is a service account, which cannot sign in", user.Name)
	case user.Disabled:
		return auth.User{}, refuse(RefusedDisabled, user.Name, "user is disabled")
	}
	link := auth.Identity{Issuer: id.Issuer, Subject: id.Subject, User: user.Name, Email: id.Email, Groups: id.Groups, AccountID: id.AccountID}
	if err := s.sso.UpdateIdentity(ctx, link); err != nil {
		return auth.User{}, err
	}
	if id.Email != "" && user.Email != id.Email {
		switch err := s.accounts.SetEmail(ctx, user.Name, id.Email); {
		case errors.Is(err, auth.ErrEmailTaken):
			s.log.Warn("cannot record a user's new email: another user has it", "user", user.Name, "email", id.Email)
		case err != nil:
			return auth.User{}, err
		}
	}
	return s.applyRoles(ctx, user.Name)
}

// find returns id's user, linking or creating one if needed.
func (s *Service) find(ctx context.Context, id Identity) (auth.User, error) {
	for attempt := 0; ; attempt++ {
		user, err := s.lookup(ctx, id)
		// A concurrent first sign-in of the same identity, on this
		// replica or another, may have linked or created the user.
		if attempt == 0 && (errors.Is(err, auth.ErrIdentityTaken) || errors.Is(err, auth.ErrExists) || errors.Is(err, auth.ErrEmailTaken)) {
			continue
		}
		return user, err
	}
}

func (s *Service) lookup(ctx context.Context, id Identity) (auth.User, error) {
	link, err := s.sso.IdentityBySubject(ctx, id.Issuer, id.Subject)
	switch {
	case err == nil:
		return s.users.Get(ctx, link.User)
	case !errors.Is(err, auth.ErrIdentityNotFound):
		return auth.User{}, err
	}
	user, _, err := s.candidate(ctx, id.Username, id.Email, s.cfg.CreateUsers)
	if err != nil {
		return auth.User{}, err
	}
	return s.link(ctx, id, user)
}

// candidate finds the user an account with username and, if verified,
// email stands for: the user with that email, or else the first user named
// after username (see candidateNames) that is free to take, or with create
// a new user of the first free name, which created then says. It does not
// link the account.
func (s *Service) candidate(ctx context.Context, username, email string, create bool) (user auth.User, created bool, err error) {
	if email != "" {
		user, err := s.accounts.UserByEmail(ctx, email)
		if !errors.Is(err, auth.ErrNotFound) {
			return user, false, err
		}
	}
	names := candidateNames(username)
	if len(names) == 0 {
		return auth.User{}, false, refuse(RefusedConflict, "", "%q makes no valid user name", username)
	}
	for _, name := range names {
		user, err := s.users.Get(ctx, name)
		if errors.Is(err, auth.ErrNotFound) {
			if !create {
				return auth.User{}, false, refuse(RefusedNoUser, name, "no Igloo user for %s, and IGLOO_OIDC_CREATE_USERS is off", username)
			}
			user, err := s.create(ctx, username, email, name)
			return user, err == nil, err
		}
		if err != nil {
			return auth.User{}, false, err
		}
		claimable, err := s.claimable(ctx, user)
		if err != nil {
			return auth.User{}, false, err
		}
		if claimable {
			return user, false, nil
		}
	}
	return auth.User{}, false, refuse(RefusedConflict, names[0], "the user names for %s (%s) belong to other users",
		username, strings.Join(names, ", "))
}

// claimable reports whether an account may take over user by name: a
// person without a password, an email, an identity or SCIM provisioning,
// as an admin creates one for single sign-on (CREATE USER name PASSWORD
// NULL).
func (s *Service) claimable(ctx context.Context, user auth.User) (bool, error) {
	if user.IsService() || user.HasPassword() || user.Email != "" {
		return false, nil
	}
	if s.scim != nil {
		if _, err := s.scim.SCIMUser(ctx, user.Name); !errors.Is(err, auth.ErrSCIMNotFound) {
			return false, err
		}
	}
	_, err := s.sso.IdentityOf(ctx, user.Name)
	if errors.Is(err, auth.ErrIdentityNotFound) {
		return true, nil
	}
	return false, err
}

// Provision finds or creates the user for an account the identity provider
// pushes with SCIM: as a sign-in would (by verified email, then by name),
// but always creating one when none is found; created says which. The user
// may be a service account or provisioned already; the caller decides.
func (s *Service) Provision(ctx context.Context, username, email string) (user auth.User, created bool, err error) {
	for attempt := 0; ; attempt++ {
		user, created, err = s.candidate(ctx, username, auth.NormalizeEmail(email), true)
		if attempt == 0 && (errors.Is(err, auth.ErrExists) || errors.Is(err, auth.ErrEmailTaken)) {
			continue
		}
		return user, created, err
	}
}

// RefreshRoles applies the group mappings to the groups user is in now:
// those SCIM provisioning gives them and those their last sign-in's token
// listed (see withSCIMGroups). SCIM calls it when groups change.
func (s *Service) RefreshRoles(ctx context.Context, name string) error {
	_, err := s.applyRoles(ctx, name)
	return err
}

// applyRoles applies the group mappings to the groups user name is in
// now, as RefreshRoles says, and returns the user. It holds the user's
// roles lock (auth.SSOStore.LockRoles) from before it reads the user and
// their groups until their roles are written: two replicas applying one
// user's roles at once, after changes to their groups, take turns, and the
// second works them out from the groups the first left, so the roles of
// the latest change are those that stay.
func (s *Service) applyRoles(ctx context.Context, name string) (auth.User, error) {
	var user auth.User
	err := s.sso.LockRoles(ctx, name, func(ctx context.Context) error {
		var err error
		if user, err = s.users.Get(ctx, name); err != nil {
			return err
		}
		var groups []string
		if link, err := s.sso.IdentityOf(ctx, name); err == nil {
			groups = link.Groups
		} else if !errors.Is(err, auth.ErrIdentityNotFound) {
			return err
		}
		if groups, err = s.withSCIMGroups(ctx, name, groups); err != nil {
			return err
		}
		user, err = s.syncRoles(ctx, user, groups)
		return err
	})
	if err != nil {
		return auth.User{}, err
	}
	if !user.IsService() {
		s.changed(user.Name)
	}
	return user, nil
}

// withSCIMGroups returns the groups user is in, given the groups a token
// listed: those SCIM provisioning gives them, and those of groups that
// SCIM does not push. SCIM alone decides who is in a group it pushes: the
// groups a sign-in saved would otherwise keep someone SCIM removed from one
// in it until they next signed in.
func (s *Service) withSCIMGroups(ctx context.Context, user string, groups []string) ([]string, error) {
	if s.scim == nil {
		return groups, nil
	}
	provisioned, err := s.scim.UserSCIMGroups(ctx, user)
	if err != nil {
		return nil, err
	}
	pushed, err := s.scim.SCIMGroupNames(ctx, groups)
	if err != nil {
		return nil, err
	}
	out := slices.DeleteFunc(slices.Clone(groups), func(g string) bool { return slices.Contains(pushed, g) })
	for _, g := range provisioned {
		if !slices.Contains(out, g) {
			out = append(out, g)
		}
	}
	return out, nil
}

// link links user to id. A user linked to another identity is refused.
func (s *Service) link(ctx context.Context, id Identity, user auth.User) (auth.User, error) {
	if user.IsService() {
		return auth.User{}, refuse(RefusedService, user.Name, "%s is a service account, which cannot sign in", user.Name)
	}
	if other, err := s.sso.IdentityOf(ctx, user.Name); err == nil {
		return auth.User{}, refuse(RefusedConflict, user.Name, "%s is linked to another identity (%s at %s)", user.Name, other.Subject, other.Issuer)
	} else if !errors.Is(err, auth.ErrIdentityNotFound) {
		return auth.User{}, err
	}
	err := s.sso.LinkIdentity(ctx, auth.Identity{Issuer: id.Issuer, Subject: id.Subject, User: user.Name, Email: id.Email, Groups: id.Groups,
		AccountID: id.AccountID})
	if err != nil {
		return auth.User{}, err
	}
	s.log.Info("linked a user to a single sign-on identity", "user", user.Name, "subject", id.Subject, "username", id.Username)
	return user, nil
}

// create makes a user called name for the account username, with role none,
// no password and the account's verified email.
func (s *Service) create(ctx context.Context, username, email, name string) (auth.User, error) {
	salt, err := auth.NewSalt()
	if err != nil {
		return auth.User{}, err
	}
	user := auth.User{Name: name, Role: auth.RoleNone, Kind: auth.KindPerson, Email: email, TokenSalt: salt}
	if err := s.users.Create(ctx, user); err != nil {
		if errors.Is(err, auth.ErrNameTaken) {
			return auth.User{}, refuse(RefusedConflict, name, "a role is named %s", name)
		}
		return auth.User{}, err
	}
	s.log.Info("created a user for an identity provider account", "user", name, "username", username)
	return s.users.Get(ctx, name)
}

// candidateNames are the user names a username maps to: its part before
// any @, and that with the domain, made valid Igloo names.
func candidateNames(username string) []string {
	local, domain, _ := strings.Cut(strings.ToLower(username), "@")
	var names []string
	for _, n := range []string{userName(local), userName(local + "_" + domain)} {
		if n != "" && !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	return names
}

// userName makes s a valid user name: lowercase letters, digits and
// underscores, starting with a letter or underscore.
func userName(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case !strings.HasSuffix(b.String(), "_"):
			b.WriteByte('_')
		}
	}
	name := strings.Trim(b.String(), "_")
	if name == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "u" + name
	}
	if len(name) > 63 {
		name = strings.TrimRight(name[:63], "_")
	}
	if !config.ValidName(name) {
		return ""
	}
	return name
}

// roleRank orders the built-in roles by what they allow.
var roleRank = map[auth.Role]int{auth.RoleNone: 0, auth.RoleReader: 1, auth.RoleWriter: 2, auth.RoleAdmin: 3}

// syncRoles gives user the custom roles their groups map to, and, if any
// group maps to a built-in role, the highest built-in role their groups
// give (none if no group does). The caller tells Options.Changed.
func (s *Service) syncRoles(ctx context.Context, user auth.User, groups []string) (auth.User, error) {
	if user.IsService() {
		// Admins alone decide what service accounts may do.
		return user, nil
	}
	mappings, err := s.sso.GroupRoles(ctx)
	if err != nil {
		return auth.User{}, err
	}
	var custom []string
	builtinMapped, role := false, auth.RoleNone
	for _, m := range mappings {
		member := slices.Contains(groups, m.Group)
		if !auth.IsBuiltinRole(m.Role) {
			if member && !slices.Contains(custom, m.Role) {
				custom = append(custom, m.Role)
			}
			continue
		}
		builtinMapped = true
		if r := auth.Role(m.Role); member && roleRank[r] > roleRank[role] {
			role = r
		}
	}
	if err := s.sso.SyncRoles(ctx, user.Name, custom); err != nil {
		return auth.User{}, err
	}
	if builtinMapped && user.Role != role {
		switch err := s.users.SetRole(ctx, user.Name, role); {
		case errors.Is(err, auth.ErrLastAdmin):
			s.log.Warn("single sign-on would remove the last admin; keeping the role", "user", user.Name, "role", role)
		case err != nil:
			return auth.User{}, err
		default:
			s.log.Info("single sign-on changed a user's role", "user", user.Name, "from", user.Role, "to", role)
			user.Role = role
		}
	}
	return user, nil
}
