package auth

import (
	"context"
	"errors"
	"strings"
	"time"
)

// AccountStore changes what a user is besides their password and role:
// whether they may sign in, a service account's owner, their email. Stores
// that support it also implement TokenStore and SSOStore. Implementations
// must be safe for concurrent use.
type AccountStore interface {
	// SetLogin enables or disables a user. Disabling one ends their web
	// console sessions (it grows User.Epoch) and fails with ErrLastAdmin
	// for the last enabled admin.
	SetLogin(ctx context.Context, name string, enabled bool) error
	// SetOwner makes owner responsible for a service account.
	SetOwner(ctx context.Context, name, owner string) error
	// SetEmail sets or, when empty, removes a user's email. It fails with
	// ErrEmailTaken if another user has it.
	SetEmail(ctx context.Context, name, email string) error
	// SetPassword sets a user's verifier, which may be empty for none, and
	// with tokenSalt not nil their token salt, in one change.
	SetPassword(ctx context.Context, name, verifier string, tokenSalt []byte) error
	// EnsureTokenSalt gives a user salt as their token salt unless they
	// have one, and returns the one they have then.
	EnsureTokenSalt(ctx context.Context, name string, salt []byte) ([]byte, error)
	// UserByEmail finds the user with email, compared case-insensitively.
	UserByEmail(ctx context.Context, email string) (User, error)
}

// Identity links a user to an account at a single sign-on identity
// provider: the issuer's stable subject identifier, and what the user's
// last sign-in said about them.
type Identity struct {
	Issuer  string
	Subject string
	User    string
	Email   string
	// Groups are the identity provider's groups the user was in at their
	// last sign-in.
	Groups     []string
	CreatedAt  time.Time
	LastSignIn time.Time
}

// GroupRole makes the members of an identity provider's group members of
// a custom role, or gives them a built-in role, at each single sign-on.
type GroupRole struct {
	Group     string
	Role      string
	GrantedBy string
	GrantedAt time.Time
}

// SSOGrantor marks custom role memberships that single sign-on manages:
// those are added and removed at each sign-in, while memberships an admin
// granted are left alone. It is not a valid user name.
const SSOGrantor = "(sso)"

var (
	ErrIdentityNotFound = errors.New("no user is linked to that identity")
	// ErrIdentityTaken is returned when linking an identity that is
	// linked already, or a user who has another identity.
	ErrIdentityTaken = errors.New("identity or user is linked already")
)

// SSOStore keeps the links between users and single sign-on identities, and
// which identity provider groups give which roles. Implementations must be
// safe for concurrent use.
type SSOStore interface {
	// IdentityBySubject returns the identity issuer and subject name, or
	// ErrIdentityNotFound.
	IdentityBySubject(ctx context.Context, issuer, subject string) (Identity, error)
	// IdentityOf returns user's identity, or ErrIdentityNotFound.
	IdentityOf(ctx context.Context, user string) (Identity, error)
	// Identities lists every linked identity.
	Identities(ctx context.Context) ([]Identity, error)
	// LinkIdentity links id.User to the identity. It fails with
	// ErrIdentityTaken or ErrNotFound.
	LinkIdentity(ctx context.Context, id Identity) error
	// UpdateIdentity records a sign-in: id's email, groups and time.
	UpdateIdentity(ctx context.Context, id Identity) error
	// UnlinkIdentity removes user's identity, if any, which ends their web
	// console sessions (it grows User.Epoch).
	UnlinkIdentity(ctx context.Context, user string) error

	// GroupRoles lists the group mappings, by group and role.
	GroupRoles(ctx context.Context) ([]GroupRole, error)
	// GrantGroupRole adds a mapping; one that exists is left as it is. A
	// custom role must exist (ErrRoleNotFound).
	GrantGroupRole(ctx context.Context, g GroupRole) error
	// RevokeGroupRole removes a mapping, if it exists.
	RevokeGroupRole(ctx context.Context, group, role string) error
	// SyncRoles makes user a member of exactly roles among the custom roles
	// that SSOGrantor granted, adding and removing such memberships;
	// memberships an admin granted stay. Roles that do not exist are
	// skipped.
	SyncRoles(ctx context.Context, user string, roles []string) error
}

// NormalizeEmail is how emails are compared: trimmed and lowercase.
func NormalizeEmail(email string) string { return strings.ToLower(strings.TrimSpace(email)) }
