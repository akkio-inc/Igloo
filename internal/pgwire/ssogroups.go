package pgwire

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Which groups of the single sign-on identity provider give which roles:
//
//	GRANT role TO SSO GROUP 'group'
//	REVOKE role FROM SSO GROUP 'group'
//	SHOW SSO GROUPS
//
// A custom role makes the group's members members of it; a built-in role
// makes them that role, the highest when several groups give one. Single
// sign-on applies them at each sign-in (see package sso). Only admins see
// and change them.

type groupVerb int

const (
	groupGrant groupVerb = iota
	groupRevoke
	groupShow
)

// groupCommand is a parsed single sign-on group mapping statement.
type groupCommand struct {
	kind  groupVerb
	role  string
	group string
}

func (c *groupCommand) verb() string {
	switch c.kind {
	case groupGrant:
		return "GRANT"
	case groupRevoke:
		return "REVOKE"
	}
	return "SHOW"
}

var groupColumns = wire.Columns{
	{Name: "group_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "role", Oid: pgtype.TextOID, Width: -1},
	{Name: "role_type", Oid: pgtype.TextOID, Width: -1},
	{Name: "granted_by", Oid: pgtype.TextOID, Width: -1},
	{Name: "granted_at", Oid: pgtype.TimestamptzOID, Width: 8},
}

func (c *groupCommand) columns() wire.Columns {
	if c.kind == groupShow {
		return groupColumns
	}
	return nil
}

// parseGroupCommand parses one statement's tokens. ok is false when it is
// not a group mapping statement.
func parseGroupCommand(toks []token) (c *groupCommand, ok bool, err error) {
	word := func(i int) string {
		if i < len(toks) {
			return toks[i].upper()
		}
		return ""
	}
	if word(0) == "SHOW" && word(1) == "SSO" && word(2) == "GROUPS" {
		if len(toks) != 3 {
			return nil, true, pgerror.WithCode(fmt.Errorf("unexpected %q after SHOW SSO GROUPS", toks[3].text), codes.Syntax)
		}
		return &groupCommand{kind: groupShow}, true, nil
	}
	to := "TO"
	c = &groupCommand{kind: groupGrant}
	switch word(0) {
	case "GRANT":
	case "REVOKE":
		to, c.kind = "FROM", groupRevoke
	default:
		return nil, false, nil
	}
	if len(toks) < 5 || word(2) != to || word(3) != "SSO" || word(4) != "GROUP" {
		return nil, false, nil
	}
	if len(toks) != 6 || toks[5].kind != tokString && toks[5].kind != tokQuoted || toks[5].text == "" {
		return nil, true, pgerror.WithCode(fmt.Errorf("expected %s role %s SSO GROUP 'group name'", word(0), to), codes.Syntax)
	}
	if c.role, err = principalName(toks[1]); err != nil {
		return nil, true, err
	}
	c.group = toks[5].text
	return c, true, nil
}

// runGroupCommand executes a group mapping statement for sess.
func (s *Server) runGroupCommand(ctx context.Context, sess *session, c *groupCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	if me.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s of SSO groups requires the admin role", c.verb()),
			codes.InsufficientPrivilege)
	}
	if s.sso == nil {
		return pgerror.WithCode(errors.New("single sign-on needs IGLOO_METADATA_DATABASE_URL"), codes.FeatureNotSupported)
	}
	switch c.kind {
	case groupShow:
		mappings, err := s.sso.GroupRoles(ctx)
		if err != nil {
			return err
		}
		for _, m := range mappings {
			kind := "custom"
			if auth.IsBuiltinRole(m.Role) {
				kind = "built-in"
			}
			if err := w.Row([]any{m.Group, m.Role, kind, m.GrantedBy, m.GrantedAt}); err != nil {
				return err
			}
		}
		return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
	case groupGrant:
		err := s.sso.GrantGroupRole(ctx, auth.GroupRole{Group: c.group, Role: c.role, GrantedBy: me.Name})
		if errors.Is(err, auth.ErrRoleNotFound) {
			return pgerror.WithCode(fmt.Errorf("role %q does not exist", c.role), codes.UndefinedObject)
		}
		if err != nil {
			return err
		}
	case groupRevoke:
		if err := s.sso.RevokeGroupRole(ctx, c.group, c.role); err != nil {
			return err
		}
	}
	s.log.Info("sso group mapping changed", "action", c.verb(), "group", c.group, "role", c.role, "by", me.Name)
	return w.Complete(c.verb())
}
