package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// Custom roles and grants. DuckDB has neither, so Igloo handles these
// statements itself, like user management:
//
//	CREATE ROLE name
//	DROP ROLE [IF EXISTS] name
//	GRANT role TO user
//	REVOKE role FROM user
//	GRANT privilege[, ...] | ALL [PRIVILEGES]
//	    ON [TABLE] name | SCHEMA name | ALL TABLES IN SCHEMA name TO role_or_user
//	REVOKE privilege[, ...] | ALL [PRIVILEGES] ON ... FROM role_or_user
//	SHOW ROLES
//	SHOW GRANTS [TO role_or_user | ON [TABLE] name | ON SCHEMA name]
//
// Privileges are SELECT, INSERT, UPDATE, DELETE and, on schemas, CREATE.
// ALL TABLES IN SCHEMA grants on the tables and views there now, as in
// PostgreSQL; a grant ON SCHEMA also covers ones created later. Only admins
// may change roles and grants; anyone may see the ones that apply to them.

type aclKind int

const (
	aclCreateRole aclKind = iota
	aclDropRole
	aclGrantRole
	aclRevokeRole
	aclGrant
	aclRevoke
	aclShowRoles
	aclShowGrants
)

// Grant targets.
const (
	targetTable     = "TABLE"
	targetSchema    = "SCHEMA"
	targetAllTables = "ALL TABLES IN SCHEMA"
)

// aclCommand is a parsed role or grant statement.
type aclCommand struct {
	kind aclKind
	// role is the custom role of CREATE, DROP, GRANT and REVOKE ROLE.
	role     string
	ifExists bool
	// privileges are those granted or revoked; nil means ALL.
	privileges []auth.Privilege
	// targetKind and target name what a grant is on, or what SHOW GRANTS
	// ON asks about.
	targetKind string
	target     []string
	// grantee is the user or role a grant goes to, or whose grants SHOW
	// GRANTS TO lists; for role grants, the user.
	grantee string
}

// verb is the statement's command tag, or for SHOW its first word.
func (c *aclCommand) verb() string {
	switch c.kind {
	case aclCreateRole:
		return "CREATE ROLE"
	case aclDropRole:
		return "DROP ROLE"
	case aclGrantRole, aclGrant:
		return "GRANT"
	case aclRevokeRole, aclRevoke:
		return "REVOKE"
	}
	return "SHOW"
}

var (
	roleColumns = wire.Columns{
		{Name: "name", Oid: pgtype.TextOID, Width: -1},
		{Name: "members", Oid: pgtype.TextOID, Width: -1},
		{Name: "created_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
	grantColumns = wire.Columns{
		{Name: "grantee", Oid: pgtype.TextOID, Width: -1},
		{Name: "grantee_type", Oid: pgtype.TextOID, Width: -1},
		{Name: "privilege", Oid: pgtype.TextOID, Width: -1},
		{Name: "object_type", Oid: pgtype.TextOID, Width: -1},
		{Name: "object_name", Oid: pgtype.TextOID, Width: -1},
		{Name: "granted_by", Oid: pgtype.TextOID, Width: -1},
		{Name: "granted_at", Oid: pgtype.TimestamptzOID, Width: 8},
	}
)

func (c *aclCommand) columns() wire.Columns {
	switch c.kind {
	case aclShowRoles:
		return roleColumns
	case aclShowGrants:
		return grantColumns
	}
	return nil
}

// parseACLCommand parses one statement's tokens. ok is false when it is
// not a role or grant statement.
func parseACLCommand(toks []token) (c *aclCommand, ok bool, err error) {
	word := func(i int) string {
		if i < len(toks) {
			return toks[i].upper()
		}
		return ""
	}
	syntax := func(format string, args ...any) (*aclCommand, bool, error) {
		return nil, true, pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	c = &aclCommand{}

	switch {
	case word(0) == "CREATE" && word(1) == "ROLE":
		c.kind = aclCreateRole
		if len(toks) != 3 {
			return syntax("CREATE ROLE takes a role name and nothing else")
		}
		c.role, err = principalName(toks[2])
		return c, true, err

	case word(0) == "DROP" && word(1) == "ROLE":
		c.kind = aclDropRole
		rest := toks[2:]
		if len(rest) >= 2 && rest[0].upper() == "IF" && rest[1].upper() == "EXISTS" {
			c.ifExists, rest = true, rest[2:]
		}
		if len(rest) != 1 {
			return syntax("DROP ROLE takes one role name")
		}
		c.role, err = principalName(rest[0])
		return c, true, err

	case word(0) == "SHOW" && word(1) == "ROLES":
		c.kind = aclShowRoles
		if len(toks) != 2 {
			return syntax("unexpected %q after SHOW ROLES", toks[2].text)
		}
		return c, true, nil

	case word(0) == "SHOW" && word(1) == "GRANTS":
		c.kind = aclShowGrants
		rest := toks[2:]
		switch {
		case len(rest) == 0:
		case rest[0].upper() == "TO" && len(rest) == 2:
			if c.grantee, err = principalName(rest[1]); err != nil {
				return nil, true, err
			}
		case rest[0].upper() == "ON":
			kind, name, n, perr := parseTarget(rest[1:])
			if perr != nil || n != len(rest)-1 || kind == targetAllTables {
				return syntax("expected SHOW GRANTS ON [TABLE] name or ON SCHEMA name")
			}
			c.targetKind, c.target = kind, name
		default:
			return syntax("expected SHOW GRANTS [TO name | ON [TABLE] name | ON SCHEMA name]")
		}
		return c, true, nil

	case word(0) == "GRANT" || word(0) == "REVOKE":
		grant := word(0) == "GRANT"
		toWord := "TO"
		if !grant {
			toWord = "FROM"
		}
		on := slices.IndexFunc(toks, func(t token) bool { return t.upper() == "ON" })
		if on < 0 {
			// GRANT role TO user, REVOKE role FROM user.
			c.kind = aclGrantRole
			if !grant {
				c.kind = aclRevokeRole
			}
			if len(toks) != 4 || toks[2].upper() != toWord {
				return syntax("expected %s role %s user, or %s privilege ON object %s role_or_user", word(0), toWord, word(0), toWord)
			}
			if c.role, err = principalName(toks[1]); err != nil {
				return nil, true, err
			}
			c.grantee, err = principalName(toks[3])
			return c, true, err
		}

		c.kind = aclGrant
		if !grant {
			c.kind = aclRevoke
		}
		if c.privileges, err = parsePrivileges(toks[1:on]); err != nil {
			return nil, true, err
		}
		kind, name, n, perr := parseTarget(toks[on+1:])
		if perr != nil {
			return nil, true, perr
		}
		c.targetKind, c.target = kind, name
		rest := toks[on+1+n:]
		if len(rest) != 2 || rest[0].upper() != toWord {
			return syntax("expected %s role_or_user after the object", toWord)
		}
		if c.grantee, err = principalName(rest[1]); err != nil {
			return nil, true, err
		}
		if c.targetKind != targetSchema && slices.Contains(c.privileges, auth.PrivilegeCreate) {
			return nil, true, pgerror.WithCode(errors.New("CREATE can only be granted on a schema"), codes.InvalidGrantOperation)
		}
		return c, true, nil
	}
	return nil, false, nil
}

// parsePrivileges reads "SELECT, INSERT" or "ALL [PRIVILEGES]". ALL comes
// back as nil.
func parsePrivileges(toks []token) ([]auth.Privilege, error) {
	syntax := func(format string, args ...any) error {
		return pgerror.WithCode(fmt.Errorf(format, args...), codes.Syntax)
	}
	if len(toks) == 0 {
		return nil, syntax("expected privileges, such as SELECT or ALL")
	}
	if toks[0].upper() == "ALL" {
		if len(toks) == 1 || len(toks) == 2 && toks[1].upper() == "PRIVILEGES" {
			return nil, nil
		}
		return nil, syntax("unexpected %q after ALL", toks[1].text)
	}
	var privs []auth.Privilege
	for i, t := range toks {
		if i%2 == 1 {
			if t.kind != tokComma {
				return nil, syntax("unexpected %q in the privilege list", t.text)
			}
			continue
		}
		p, ok := auth.ParsePrivilege(t.upper())
		if !ok || t.kind != tokWord {
			return nil, syntax("unsupported privilege %q; use SELECT, INSERT, UPDATE, DELETE, CREATE or ALL", t.text)
		}
		if !slices.Contains(privs, p) {
			privs = append(privs, p)
		}
	}
	if toks[len(toks)-1].kind == tokComma {
		return nil, syntax("expected a privilege after the comma")
	}
	return privs, nil
}

// parseTarget reads what a grant is on: [TABLE] name, SCHEMA name or ALL
// TABLES IN SCHEMA name. n is how many tokens it used.
func parseTarget(toks []token) (kind string, name []string, n int, err error) {
	word := func(i int) string {
		if i < len(toks) {
			return toks[i].upper()
		}
		return ""
	}
	switch {
	case word(0) == "ALL" && word(1) == "TABLES" && word(2) == "IN" && word(3) == "SCHEMA":
		kind, n = targetAllTables, 4
	case word(0) == "SCHEMA":
		kind, n = targetSchema, 1
	case word(0) == "TABLE":
		kind, n = targetTable, 1
	default:
		kind = targetTable
	}
	name, used := tokenName(toks[n:])
	if used == 0 {
		return "", nil, 0, pgerror.WithCode(errors.New("expected the name of a table or schema"), codes.Syntax)
	}
	if kind != targetTable && len(name) > 2 || len(name) > 3 {
		return "", nil, 0, pgerror.WithCode(fmt.Errorf("improper qualified name %q", strings.Join(name, ".")), codes.Syntax)
	}
	return kind, name, n + used, nil
}

// tokenName reads a possibly qualified name such as lake."My Schema".t from
// the start of toks, returning its parts and how many tokens it used.
func tokenName(toks []token) ([]string, int) {
	var parts []string
	i := 0
	for i < len(toks) && (toks[i].kind == tokWord || toks[i].kind == tokQuoted) {
		parts = append(parts, toks[i].ident())
		i++
		if i < len(toks) && toks[i].kind == tokDot {
			i++
			continue
		}
		return parts, i
	}
	return nil, 0
}

// principalName validates a user or role name token.
func principalName(t token) (string, error) {
	if t.kind != tokWord && t.kind != tokQuoted {
		return "", pgerror.WithCode(fmt.Errorf("expected a name, got %q", t.text), codes.Syntax)
	}
	name := t.ident()
	if !config.ValidName(name) {
		return "", pgerror.WithCode(fmt.Errorf("invalid name %q: use lowercase letters, digits and underscores", name), codes.Syntax)
	}
	return name, nil
}

// runACLCommand executes a role or grant statement for sess.
func (s *Server) runACLCommand(ctx context.Context, sess *session, c *aclCommand, w wire.DataWriter) error {
	me := sess.currentUser()
	switch c.kind {
	case aclShowRoles:
		return s.showRoles(ctx, me, w)
	case aclShowGrants:
		return s.showGrants(ctx, sess, me, c, w)
	}
	if me.Role != auth.RoleAdmin {
		return pgerror.WithCode(fmt.Errorf("permission denied: %s requires the admin role", c.verb()),
			codes.InsufficientPrivilege)
	}

	var err error
	switch c.kind {
	case aclCreateRole:
		if auth.IsBuiltinRole(c.role) {
			return pgerror.WithCode(fmt.Errorf("role name %q is reserved for a built-in role", c.role), codes.ReservedName)
		}
		err = s.grants.CreateRole(ctx, c.role)
	case aclDropRole:
		err = s.grants.DropRole(ctx, c.role)
		if errors.Is(err, auth.ErrRoleNotFound) && c.ifExists {
			err = nil
		}
		s.sessions.expireAll()
	case aclGrantRole:
		if auth.IsBuiltinRole(c.role) {
			return pgerror.WithCode(fmt.Errorf("%q is a built-in role; set it with ALTER USER %s ROLE %s", c.role, c.grantee, c.role),
				codes.InvalidGrantOperation)
		}
		err = s.grants.AddMember(ctx, c.role, c.grantee, me.Name)
		s.sessions.expire(c.grantee)
	case aclRevokeRole:
		err = s.grants.RemoveMember(ctx, c.role, c.grantee)
		s.sessions.expire(c.grantee)
	case aclGrant, aclRevoke:
		err = s.changeGrants(ctx, sess, me, c)
	}
	if err != nil {
		return grantStoreError(err, c)
	}
	s.log.Info("grants changed", "action", strings.ToLower(c.verb()), "role", c.role, "grantee", c.grantee,
		"object", strings.Join(c.target, "."), "by", me.Name)
	return w.Complete(c.verb())
}

// changeGrants carries out GRANT or REVOKE of privileges on objects.
func (s *Server) changeGrants(ctx context.Context, sess *session, me auth.User, c *aclCommand) error {
	granteeType, err := s.granteeType(ctx, c.grantee)
	if err != nil {
		return err
	}
	objects, err := s.grantObjects(ctx, sess, c.targetKind, c.target)
	if err != nil {
		return err
	}
	privs := c.privileges
	if privs == nil {
		privs = auth.TablePrivileges
		if c.targetKind == targetSchema {
			privs = auth.SchemaPrivileges
		}
	}
	var grants []auth.Grant
	for _, o := range objects {
		for _, p := range privs {
			grants = append(grants, auth.Grant{Grantee: c.grantee, GranteeType: granteeType, Privilege: p, Object: o, GrantedBy: me.Name})
		}
	}
	if c.kind == aclGrant {
		err = s.grants.Grant(ctx, grants)
	} else {
		err = s.grants.Revoke(ctx, grants)
	}
	if granteeType == auth.GranteeUser {
		s.sessions.expire(c.grantee)
	} else {
		s.sessions.expireAll()
	}
	return err
}

// granteeType reports whether name is a custom role or a user.
func (s *Server) granteeType(ctx context.Context, name string) (string, error) {
	if isRole, err := s.isCustomRole(ctx, name); err != nil || isRole {
		return auth.GranteeRole, err
	}
	if _, err := s.users.Get(ctx, name); err != nil {
		if errors.Is(err, auth.ErrNotFound) {
			return "", pgerror.WithCode(fmt.Errorf("role or user %q does not exist", name), codes.UndefinedObject)
		}
		return "", err
	}
	return auth.GranteeUser, nil
}

// isCustomRole reports whether a custom role is called name.
func (s *Server) isCustomRole(ctx context.Context, name string) (bool, error) {
	roles, err := s.grants.Roles(ctx)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(roles, func(r auth.CustomRole) bool { return r.Name == name }), nil
}

// grantObjects resolves what a grant names to existing objects of the
// Iceberg catalog: one schema, one table or view, or every table and view
// in a schema.
func (s *Server) grantObjects(ctx context.Context, sess *session, kind string, name []string) ([]auth.Object, error) {
	catalogName := s.catalogName()
	if catalogName == "" {
		return nil, pgerror.WithCode(errors.New("grants need an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	if kind == targetTable {
		o, err := s.resolveGrantTable(ctx, sess, name)
		if err != nil {
			return nil, err
		}
		return []auth.Object{o}, nil
	}

	schemaName := name[len(name)-1]
	if len(name) == 2 && !strings.EqualFold(name[0], catalogName) {
		return nil, pgerror.WithCode(fmt.Errorf("grants apply to the %s catalog only, not %q", catalogName, name[0]), codes.FeatureNotSupported)
	}
	schema, ok, err := s.catalogSchema(ctx, sess, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, pgerror.WithCode(fmt.Errorf("schema %q does not exist", schemaName), codes.InvalidSchemaName)
	}
	if kind == targetSchema {
		return []auth.Object{{Schema: schema}}, nil
	}
	tables, err := s.catalogRelations(ctx, sess, schema)
	if err != nil {
		return nil, err
	}
	objects := make([]auth.Object, len(tables))
	for i, t := range tables {
		objects[i] = auth.Object{Schema: schema, Table: t}
	}
	return objects, nil
}

// resolveGrantTable finds the table or view a grant names. An unqualified
// name is looked up along the session's search path, as a query would.
func (s *Server) resolveGrantTable(ctx context.Context, sess *session, name []string) (auth.Object, error) {
	catalogName := s.catalogName()
	var schemas []string
	switch len(name) {
	case 3:
		if !strings.EqualFold(name[0], catalogName) {
			return auth.Object{}, pgerror.WithCode(fmt.Errorf("grants apply to the %s catalog only, not %q", catalogName, name[0]), codes.FeatureNotSupported)
		}
		schemas = []string{name[1]}
	case 2:
		schemas = []string{name[0]}
	default:
		path, err := sessionPath(ctx, sess.conn)
		if err != nil {
			return auth.Object{}, err
		}
		for _, e := range path.entries {
			if strings.EqualFold(e.db, catalogName) {
				schemas = append(schemas, e.schema)
			}
		}
	}
	table := name[len(name)-1]
	for _, schema := range schemas {
		relations, err := s.catalogRelations(ctx, sess, schema)
		if err != nil {
			return auth.Object{}, err
		}
		for _, r := range relations {
			if strings.EqualFold(r, table) {
				actual, _, err := s.catalogSchema(ctx, sess, schema)
				if err != nil {
					return auth.Object{}, err
				}
				return auth.Object{Schema: actual, Table: r}, nil
			}
		}
	}
	return auth.Object{}, pgerror.WithCode(fmt.Errorf("relation %q does not exist", strings.Join(name, ".")), codes.UndefinedTable)
}

// catalogSchema returns the catalog schema called name, in its own case.
func (s *Server) catalogSchema(ctx context.Context, sess *session, name string) (string, bool, error) {
	names, err := queryStrings(ctx, sess, `SELECT schema_name FROM system.main.duckdb_schemas()
		WHERE database_name = $1 AND system.main.lower(schema_name) = system.main.lower($2)`, s.catalogName(), name)
	if err != nil || len(names) == 0 {
		return "", false, err
	}
	return names[0], true, nil
}

// catalogRelations lists the tables and views in a schema of the catalog.
// Views are the catalog's, whose copies live in the local database, and
// external tables (package sharing) count as tables.
func (s *Server) catalogRelations(ctx context.Context, sess *session, schema string) ([]string, error) {
	tables, err := queryStrings(ctx, sess, `SELECT table_name FROM system.main.duckdb_tables()
		WHERE database_name = $1 AND system.main.lower(schema_name) = system.main.lower($2) ORDER BY 1`, s.catalogName(), schema)
	if err == nil && s.shares != nil {
		tables = append(tables, s.shares.Consumer().TableNames(schema)...)
	}
	if err != nil || s.ddl == nil {
		return tables, err
	}
	views, err := queryStrings(ctx, sess, `SELECT view_name FROM system.main.duckdb_views()
		WHERE database_name = $1 AND system.main.lower(schema_name) = system.main.lower($2) AND NOT internal ORDER BY 1`,
		s.engine.LocalDatabase(), schema)
	if err != nil {
		return nil, err
	}
	for _, v := range views {
		if s.ddl.IsView(schema, v) {
			tables = append(tables, v)
		}
	}
	return tables, nil
}

// queryStrings runs a query returning one string column on the session's
// connection.
func queryStrings(ctx context.Context, sess *session, query string, args ...any) ([]string, error) {
	named := make([]driver.NamedValue, len(args))
	for i, a := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	rows, err := sess.conn.QueryContext(ctx, query, named)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var out []string
	dest := make([]driver.Value, 1)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		v, _ := dest[0].(string)
		out = append(out, v)
	}
}

// showRoles lists custom roles: all of them for admins, a user's own
// otherwise.
func (s *Server) showRoles(ctx context.Context, me auth.User, w wire.DataWriter) error {
	roles, err := s.grants.Roles(ctx)
	if err != nil {
		return err
	}
	for _, r := range roles {
		members := r.Members
		if me.Role != auth.RoleAdmin {
			if !slices.Contains(r.Members, me.Name) {
				continue
			}
			members = []string{me.Name}
		}
		if err := w.Row([]any{r.Name, strings.Join(members, ", "), r.CreatedAt}); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// showGrants lists grants, filtered by c's TO or ON. Non-admins see only
// grants that apply to them.
func (s *Server) showGrants(ctx context.Context, sess *session, me auth.User, c *aclCommand, w wire.DataWriter) error {
	all, err := s.grants.Grants(ctx)
	if err != nil {
		return err
	}
	isAdmin := me.Role == auth.RoleAdmin
	mine, err := s.grants.UserPrivileges(ctx, me.Name)
	if err != nil {
		return err
	}
	appliesTo := func(g auth.Grant, user string, roles []string) bool {
		return g.GranteeType == auth.GranteeUser && g.Grantee == user ||
			g.GranteeType == auth.GranteeRole && slices.Contains(roles, g.Grantee)
	}

	keep := func(auth.Grant) bool { return true }
	switch {
	case c.grantee != "":
		roles := []string{c.grantee}
		if isRole, err := s.isCustomRole(ctx, c.grantee); err != nil {
			return err
		} else if !isRole {
			// A user's grants include those of their roles.
			p, err := s.grants.UserPrivileges(ctx, c.grantee)
			if err != nil {
				return err
			}
			roles = p.Roles
		}
		if !isAdmin && c.grantee != me.Name && !slices.Contains(mine.Roles, c.grantee) {
			return pgerror.WithCode(fmt.Errorf("permission denied: only admins may see the grants of %q", c.grantee),
				codes.InsufficientPrivilege)
		}
		keep = func(g auth.Grant) bool { return appliesTo(g, c.grantee, roles) }
	case c.targetKind == targetSchema:
		schema := c.target[len(c.target)-1]
		keep = func(g auth.Grant) bool { return strings.EqualFold(g.Object.Schema, schema) }
	case c.targetKind == targetTable:
		o, err := s.resolveGrantTable(ctx, sess, c.target)
		if err != nil {
			return err
		}
		keep = func(g auth.Grant) bool {
			return strings.EqualFold(g.Object.Schema, o.Schema) && (g.Object.IsSchema() || g.Object.Equal(o))
		}
	}

	for _, g := range all {
		if !keep(g) || !isAdmin && !appliesTo(g, me.Name, mine.Roles) {
			continue
		}
		row := []any{g.Grantee, g.GranteeType, string(g.Privilege), g.Object.Kind(), g.Object.String(), g.GrantedBy, g.GrantedAt}
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

// grantStoreError gives role and grant errors PostgreSQL codes.
func grantStoreError(err error, c *aclCommand) error {
	switch {
	case pgerror.GetCode(err) != codes.Uncategorized:
		return err
	case errors.Is(err, auth.ErrRoleExists):
		return pgerror.WithCode(fmt.Errorf("role %q already exists", c.role), codes.DuplicateObject)
	case errors.Is(err, auth.ErrNameTaken):
		return pgerror.WithCode(fmt.Errorf("a user or role named %q already exists", c.role), codes.DuplicateObject)
	case errors.Is(err, auth.ErrRoleNotFound):
		name := c.role
		if name == "" {
			name = c.grantee
		}
		return pgerror.WithCode(fmt.Errorf("role %q does not exist", name), codes.UndefinedObject)
	case errors.Is(err, auth.ErrNotFound):
		return pgerror.WithCode(fmt.Errorf("user %q does not exist", c.grantee), codes.UndefinedObject)
	}
	return err
}

// privilegeSnapshot is the grants a session last loaded.
type privilegeSnapshot struct {
	privs  *auth.Privileges
	loaded time.Time
}

// grantEffect is what a DROP or ALTER TABLE ... RENAME TO does to the
// objects grants name. Grants follow a renamed table and go with a dropped
// one, so a table created later under the same name starts with none.
type grantEffect struct {
	// objects are what the statement may drop or rename; it is applied to
	// those that no longer exist once the statement succeeds.
	objects []auth.Object
	// renameTo is the new name of a renamed table.
	renameTo string
}

// ddlEffect works out the grantEffect of a single DROP TABLE, DROP VIEW,
// DROP SCHEMA or ALTER TABLE ... RENAME TO on the catalog, or nil.
func (s *Server) ddlEffect(ctx context.Context, sess *session, query string) *grantEffect {
	catalogName := s.catalogName()
	if catalogName == "" {
		return nil
	}
	if n, err := s.engine.CountStatements(query); err != nil || n != 1 {
		return nil
	}
	sc := &scanner{s: query}
	e := &grantEffect{}
	var name []string
	switch {
	case sc.keywords("DROP", "SCHEMA"):
		sc.keywords("IF", "EXISTS")
		schema, ok := sc.name()
		if !ok || len(schema) > 2 || len(schema) == 2 && !strings.EqualFold(schema[0], catalogName) {
			return nil
		}
		e.objects = []auth.Object{{Schema: schema[len(schema)-1]}}
		return e
	case sc.keywords("DROP", "TABLE"), sc.keywords("DROP", "VIEW"):
		sc.keywords("IF", "EXISTS")
		name, _ = sc.name()
	case sc.keywords("ALTER", "TABLE"):
		sc.keywords("IF", "EXISTS")
		name, _ = sc.name()
		if !sc.keywords("RENAME", "TO") {
			return nil
		}
		to, ok := sc.name()
		if !ok || len(to) != 1 {
			return nil
		}
		e.renameTo = to[0]
	}
	if name == nil {
		return nil
	}
	objects, err := s.catalogCandidates(ctx, sess, name, false)
	if err != nil || len(objects) == 0 {
		return nil
	}
	e.objects = objects
	return e
}

// applyEffect moves or removes the grants of the objects a statement
// renamed or dropped.
func (s *Server) applyEffect(ctx context.Context, sess *session, e *grantEffect) {
	for _, o := range e.objects {
		var (
			exists bool
			err    error
		)
		if o.IsSchema() {
			_, exists, err = s.catalogSchema(ctx, sess, o.Schema)
		} else {
			var relations []string
			relations, err = s.catalogRelations(ctx, sess, o.Schema)
			exists = slices.ContainsFunc(relations, func(r string) bool { return strings.EqualFold(r, o.Table) })
		}
		if err != nil || exists {
			continue
		}
		if e.renameTo != "" {
			err = s.grants.RenameObject(ctx, o, auth.Object{Schema: o.Schema, Table: e.renameTo})
		} else {
			err = s.grants.ForgetObject(ctx, o)
		}
		if err != nil {
			s.log.Warn("could not update grants after a change to the catalog", "object", o.String(), "err", err)
		}
		if !o.IsSchema() {
			s.tableGone(ctx, o, e.renameTo)
		}
		s.sessions.expireAll()
	}
}
