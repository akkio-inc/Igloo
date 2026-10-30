package auth

import (
	"context"
	"errors"
	"time"
)

// SCIM provisioning (package scim) lets an identity provider create,
// update and deactivate users, and keep groups and their members, as they
// change at the provider. Users stay ordinary Igloo users; this is what
// SCIM keeps about them besides, and its groups, whose names the group
// mappings (GroupRole) match like the groups of a single sign-on token.

// SCIMUser is what provisioning keeps about a user it manages.
type SCIMUser struct {
	User string
	// ExternalID is the provider's identifier for the user.
	ExternalID string
	// UserName is the SCIM userName as the provider sent it, such as
	// ana.lopez@agency.com.
	UserName    string
	DisplayName string
	GivenName   string
	FamilyName  string
	Created     time.Time
	Modified    time.Time
}

// SCIMGroup is a group the provider pushes, and its members (user names).
type SCIMGroup struct {
	ID          string
	DisplayName string
	ExternalID  string
	Members     []string
	Created     time.Time
	Modified    time.Time
}

var (
	ErrSCIMNotFound = errors.New("not provisioned")
	// ErrSCIMGroupExists is returned for a second group of the same name.
	ErrSCIMGroupExists = errors.New("a group of that name exists")
)

// SCIMStore keeps provisioned users and groups. Implementations must be
// safe for concurrent use.
type SCIMStore interface {
	// SCIMUsers lists provisioned users, by user name.
	SCIMUsers(ctx context.Context) ([]SCIMUser, error)
	// SCIMUser returns user's record, or ErrSCIMNotFound.
	SCIMUser(ctx context.Context, user string) (SCIMUser, error)
	// PutSCIMUser stores u, replacing what was there. The user must exist
	// (ErrNotFound).
	PutSCIMUser(ctx context.Context, u SCIMUser) error
	// DeleteSCIMUser forgets that provisioning manages user, and removes
	// them from every group.
	DeleteSCIMUser(ctx context.Context, user string) error

	// SCIMGroups lists the groups, by name, with their members.
	SCIMGroups(ctx context.Context) ([]SCIMGroup, error)
	// SCIMGroup returns a group, or ErrSCIMNotFound.
	SCIMGroup(ctx context.Context, id string) (SCIMGroup, error)
	// CreateSCIMGroup stores a new group. It fails with ErrSCIMGroupExists,
	// or ErrNotFound if a member is not a provisioned user.
	CreateSCIMGroup(ctx context.Context, g SCIMGroup) error
	// ModifySCIMGroup changes group id with change, atomically. Members
	// that are not provisioned users fail it with ErrNotFound.
	ModifySCIMGroup(ctx context.Context, id string, change func(*SCIMGroup) error) error
	// DeleteSCIMGroup removes a group, or fails with ErrSCIMNotFound.
	DeleteSCIMGroup(ctx context.Context, id string) error
	// UserSCIMGroups returns the names of the groups user is in.
	UserSCIMGroups(ctx context.Context, user string) ([]string, error)
	// SCIMGroupNames returns those of names that are groups' names.
	SCIMGroupNames(ctx context.Context, names []string) ([]string, error)
}
