package pgwire

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Owners of the catalog's schemas, tables and views, and the catalog's
// listing of them:
//
//	ALTER TABLE | VIEW | MATERIALIZED VIEW | SCHEMA [IF EXISTS] name OWNER TO user_or_role
//	SHOW OBJECTS [IN SCHEMA name]
//
// Whoever creates an object owns it. Owning one lets its owner set its
// comments and give it to another owner, and nothing else (see
// auth.Ownership). Changing an owner takes what changing the object does,
// the writer role or CREATE on a table's schema, or owning it.

// ownerCommand is a parsed ALTER ... OWNER TO.
type ownerCommand struct {
	// kind is TABLE, VIEW or SCHEMA; MATERIALIZED VIEW is a TABLE.
	kind     string
	ifExists bool
	name     []string
	owner    string
}

// parseOwnerCommand recognizes ALTER ... OWNER TO. ok is false for any
// other statement.
func parseOwnerCommand(query string) (c ownerCommand, ok bool, err error) {
	sc := &scanner{s: query}
	switch {
	case sc.keywords("ALTER", "TABLE"), sc.keywords("ALTER", "MATERIALIZED", "VIEW"):
		c.kind = "TABLE"
	case sc.keywords("ALTER", "VIEW"):
		c.kind = "VIEW"
	case sc.keywords("ALTER", "SCHEMA"):
		c.kind = "SCHEMA"
	default:
		return c, false, nil
	}
	c.ifExists = sc.keywords("IF", "EXISTS")
	if c.name, ok = sc.name(); !ok || !sc.keywords("OWNER", "TO") {
		return c, false, nil
	}
	owner, ok := sc.name()
	if !ok || len(owner) != 1 || !sc.done() {
		return c, true, pgerror.WithCode(errors.New("OWNER TO takes the name of a user or a custom role"), codes.Syntax)
	}
	c.owner = owner[0]
	return c, true, nil
}

var objectColumns = wire.Columns{
	{Name: "schema_name", Oid: pgtype.TextOID, Width: -1},
	{Name: "name", Oid: pgtype.TextOID, Width: -1},
	{Name: "kind", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner", Oid: pgtype.TextOID, Width: -1},
	{Name: "owner_type", Oid: pgtype.TextOID, Width: -1},
	{Name: "comment", Oid: pgtype.TextOID, Width: -1},
	{Name: "row_count", Oid: pgtype.Int8OID, Width: 8},
	{Name: "bytes", Oid: pgtype.Int8OID, Width: 8},
	{Name: "last_altered", Oid: pgtype.TimestamptzOID, Width: 8},
}

// parseShowObjects recognizes SHOW OBJECTS [IN SCHEMA name].
func parseShowObjects(query string) (schema []string, ok bool, err error) {
	sc := &scanner{s: query}
	if !sc.keywords("SHOW", "OBJECTS") {
		return nil, false, nil
	}
	if sc.keywords("IN", "SCHEMA") {
		if schema, ok = sc.name(); !ok || len(schema) > 2 {
			return nil, true, pgerror.WithCode(errors.New("SHOW OBJECTS IN SCHEMA takes a schema's name"), codes.Syntax)
		}
	}
	if !sc.done() {
		return nil, true, pgerror.WithCode(errors.New("expected SHOW OBJECTS [IN SCHEMA name]"), codes.Syntax)
	}
	return schema, true, nil
}

// ownerCommand carries out ALTER ... OWNER TO and SHOW OBJECTS.
func (s *Server) ownerCommand(sess *session, query string) (wire.PreparedStatements, bool, error) {
	c, isOwner, err := parseOwnerCommand(query)
	var schema []string
	isShow := false
	if !isOwner {
		schema, isShow, err = parseShowObjects(query)
	}
	if !isOwner && !isShow {
		return nil, false, nil
	}
	if err == nil && len(sqllex.Split(query)) != 1 {
		err = pgerror.WithCode(errors.New("send ALTER ... OWNER TO and SHOW OBJECTS on their own"), codes.FeatureNotSupported)
	}
	if err != nil {
		s.recordQuery(sess, queryRecord{text: query, start: time.Now(), err: err, rows: -1})
		return nil, true, err
	}
	var opts []wire.PreparedOptionFn
	if isShow {
		opts = append(opts, wire.WithColumns(objectColumns))
	}
	return wire.Prepared(wire.NewStatement(s.audited(sess, query, func(ctx context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		start := time.Now()
		user, err := s.refreshUser(ctx, sess)
		if err == nil {
			err = sess.runCancelable(ctx, query, func(ctx context.Context) error {
				if isShow {
					return s.showObjects(ctx, sess, user, schema, w)
				}
				return s.changeOwner(ctx, sess, user, c, w)
			})
		}
		s.metrics.query(start, err)
		return pgError(err)
	}), opts...)), true, nil
}

// changeOwner carries out ALTER ... OWNER TO.
func (s *Server) changeOwner(ctx context.Context, sess *session, user auth.User, c ownerCommand, w wire.DataWriter) error {
	tag := "ALTER " + c.kind
	catalogName := s.catalogName()
	if catalogName == "" {
		return pgerror.WithCode(errors.New("owners need an Iceberg catalog (IGLOO_CATALOG_URI)"), codes.FeatureNotSupported)
	}
	var o auth.Object
	if c.kind == "SCHEMA" {
		if len(c.name) > 2 || len(c.name) == 2 && !strings.EqualFold(c.name[0], catalogName) {
			return pgerror.WithCode(fmt.Errorf("%s is not a schema of the %s catalog", strings.Join(c.name, "."), catalogName),
				codes.FeatureNotSupported)
		}
		name, exists, err := s.catalogSchema(ctx, sess, c.name[len(c.name)-1])
		if err != nil {
			return err
		}
		if !exists {
			if c.ifExists {
				return w.Complete(tag)
			}
			return pgerror.WithCode(fmt.Errorf("schema %q does not exist", c.name[len(c.name)-1]), codes.InvalidSchemaName)
		}
		o = auth.Object{Schema: name}
	} else {
		objects, err := s.catalogCandidates(ctx, sess, c.name, false)
		if err != nil && !errors.Is(err, errNoGrantPath) {
			return err
		}
		if len(objects) == 0 && c.kind == "TABLE" && s.external != nil {
			// External tables have their names in the catalog's schemas.
			if schema, table, err := s.externalTarget(ctx, sess, c.name); err == nil {
				if _, ok := s.external.IsExternal(schema, table); ok {
					objects = []auth.Object{{Schema: schema, Table: table}}
				}
			}
		}
		switch {
		case len(objects) == 0 && c.ifExists:
			return w.Complete(tag)
		case len(objects) == 0:
			return pgerror.WithCode(fmt.Errorf("relation %q does not exist in the %s catalog", strings.Join(c.name, "."), catalogName),
				codes.UndefinedTable)
		case len(objects) > 1:
			return pgerror.WithCode(fmt.Errorf("%s could mean %d tables or views; qualify it", strings.Join(c.name, "."), len(objects)),
				codes.AmbiguousAlias)
		}
		o = objects[0]
	}
	if err := s.authorizeOwnerChange(ctx, sess, user, o); err != nil {
		s.log.Info("statement denied", "user", user.Name, "role", user.Role, "err", err)
		return err
	}
	ownerType, err := s.granteeType(ctx, c.owner)
	if err != nil {
		return err
	}
	err = s.owners.SetObjectOwner(ctx, auth.Ownership{Object: o, Owner: c.owner, OwnerType: ownerType, SetBy: user.Name}, false)
	if errors.Is(err, auth.ErrNotFound) || errors.Is(err, auth.ErrRoleNotFound) {
		return pgerror.WithCode(fmt.Errorf("role or user %q does not exist", c.owner), codes.UndefinedObject)
	}
	if err != nil {
		return err
	}
	s.ownersChanged()
	s.log.Info("owner changed", "object", o.String(), "owner", c.owner, "by", user.Name)
	return w.Complete(tag)
}

// authorizeOwnerChange allows changing the owner of o to admins, to its
// owner, and to whoever may change o: the writer role, or for a table or
// view CREATE on its schema.
func (s *Server) authorizeOwnerChange(ctx context.Context, sess *session, user auth.User, o auth.Object) error {
	if rank[user.Role] >= rank[auth.RoleWriter] {
		return nil
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return denied("could not load grants: %v", err)
	}
	if w, ok := auth.OwnerOf(s.objectOwners(ctx), o); ok && w.Owns(user.Name, privs.Roles) {
		return nil
	}
	if !o.IsSchema() && privs.AllowsSchema(auth.PrivilegeCreate, o.Schema) {
		return nil
	}
	need := "the writer role or owning it"
	if !o.IsSchema() {
		need = "the writer role, CREATE on schema " + o.Schema + ", or owning it"
	}
	return pgerror.WithCode(fmt.Errorf("permission denied for %s: changing the owner of %s %s needs %s",
		user.Name, o.Kind(), o, need), codes.InsufficientPrivilege)
}

// mayComment reports whether user owns o, which lets them set its comments
// whatever their role.
func (s *Server) mayComment(ctx context.Context, sess *session, user auth.User, o auth.Object) bool {
	w, ok := auth.OwnerOf(s.objectOwners(ctx), o)
	if !ok {
		return false
	}
	privs, err := s.privileges(ctx, sess, user)
	return err == nil && w.Owns(user.Name, privs.Roles)
}

// showObjects lists the schemas, tables and views user may see, with
// their owners, comments and sizes; in one schema, if schema is set.
func (s *Server) showObjects(ctx context.Context, sess *session, user auth.User, schema []string, w wire.DataWriter) error {
	catalogName := s.catalogName()
	if len(schema) == 2 && !strings.EqualFold(schema[0], catalogName) {
		return pgerror.WithCode(fmt.Errorf("%s is not a schema of the %s catalog", strings.Join(schema, "."), catalogName),
			codes.FeatureNotSupported)
	}
	s.refreshMetadata(ctx)
	access, err := s.metadataAccess(ctx, sess, user)
	if err != nil {
		return denied("could not load grants: %v", err)
	}
	view := &catalogView{s: s, access: access}
	md, owners := view.Metadata(), view.Owners()
	if md == nil {
		md = &catalog.Metadata{}
	}
	in := func(name string) bool { return len(schema) == 0 || strings.EqualFold(schema[len(schema)-1], name) }
	type row struct {
		schema, name, kind string
		values             []any
	}
	var rows []row
	for _, sc := range md.Schemas {
		if in(sc.Name) {
			owner, kind := ownerValues(owners, auth.Object{Schema: sc.Name})
			rows = append(rows, row{sc.Name, "", "SCHEMA", []any{sc.Name, nil, "SCHEMA", owner, kind, nullText(sc.Comment), nil, nil, nil}})
		}
	}
	for _, t := range md.Tables {
		// Connections' tables are not in the catalog's schemas.
		if t.Catalog != "" || !in(t.Schema) {
			continue
		}
		owner, kind := ownerValues(owners, auth.Object{Schema: t.Schema, Table: t.Name})
		values := []any{t.Schema, t.Name, "TABLE", owner, kind, nullText(t.Comment), nullCount(t.Rows), nullCount(t.Bytes), nullTime(t.LastModified)}
		switch {
		case t.View:
			values[2], values[6], values[7] = "VIEW", nil, nil
		case t.Provider != "":
			values[2] = "EXTERNAL TABLE"
		}
		rows = append(rows, row{t.Schema, t.Name, values[2].(string), values})
	}
	slices.SortFunc(rows, func(a, b row) int {
		if c := strings.Compare(strings.ToLower(a.schema), strings.ToLower(b.schema)); c != 0 {
			return c
		}
		return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
	})
	for _, r := range rows {
		if err := w.Row(r.values); err != nil {
			return err
		}
	}
	return w.Complete(fmt.Sprintf("SELECT %d", w.Written()))
}

func ownerValues(owners []auth.Ownership, o auth.Object) (owner, kind any) {
	if w, ok := auth.OwnerOf(owners, o); ok {
		return w.Owner, w.OwnerType
	}
	return nil, nil
}

func nullCount(n int64) any {
	if n < 0 {
		return nil
	}
	return n
}
