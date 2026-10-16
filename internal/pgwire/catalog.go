package pgwire

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/akkio-inc/igloo/internal/audit"
	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The data catalog: comments, owners, sizes and freshness of the catalog's
// schemas, tables and views, the lineage between tables and how much each
// is used, served by the engine's catalog functions (igloo.tables,
// igloo.lineage and the others) and SHOW OBJECTS.
//
// Everything it shows is limited to what the user may see, which is what
// they could read or change: users with the reader role or above may read
// every table, so they see every object, the locations files were loaded
// from, and the lineage between them. A user with the role none sees only
// the tables and views they have a grant on (any privilege, on the table or
// its schema) or own, and the schemas those are in. Of the rest they learn
// nothing, not even names: an edge of the lineage with one end they may not
// see shows that end without a name, and one with neither end is left out.
// Which statement made an edge, and who ran it, shows only to those who may
// see that statement in the query history: its user and admins.

// MetadataSource keeps the catalog's metadata for the catalog functions;
// *ddl.Manager is one.
type MetadataSource interface {
	// Metadata returns the metadata as last read, or nil.
	Metadata() *catalog.Metadata
	// RefreshMetadata reads it, or starts reading it again, before a query
	// that shows it.
	RefreshMetadata(ctx context.Context) error
	// TableChanged reads one table's metadata again soon, after this
	// replica changed it.
	TableChanged(schema, name string)
}

// catalogEngine is an engine that serves the catalog functions, as
// *engine.Engine does.
type catalogEngine interface {
	SetCatalogViewer(engine.CatalogViewer)
}

// LineageSink is told about each statement that wrote a table, as the
// history records it; package openlineage sends them on as OpenLineage
// events.
type LineageSink interface {
	Statement(q audit.Query)
}

// ownersTTL is how long a replica uses the owners it read: changes made on
// another replica take this long to show.
const ownersTTL = 30 * time.Second

// ownerCache keeps the owners of the catalog's objects.
type ownerCache struct {
	mu     sync.Mutex
	at     time.Time
	owners []auth.Ownership
}

// objectOwners returns the owners recorded, read again once ownersTTL old.
// If the store fails, the last ones read stay in use.
func (s *Server) objectOwners(ctx context.Context) []auth.Ownership {
	c := &s.ownerCache
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.owners == nil || !c.at.IsZero() && time.Since(c.at) < ownersTTL {
		return c.owners
	}
	owners, err := s.owners.ObjectOwners(ctx)
	if err != nil {
		s.log.Warn("could not read the owners of the catalog's objects", "err", err)
		return c.owners
	}
	c.owners, c.at = owners, time.Now()
	return owners
}

// ownersChanged makes the next objectOwners read them again.
func (s *Server) ownersChanged() {
	s.ownerCache.mu.Lock()
	s.ownerCache.at = time.Time{}
	s.ownerCache.mu.Unlock()
}

// refreshMetadata reads the catalog's metadata, if it is not read already,
// before a statement that shows it.
func (s *Server) refreshMetadata(ctx context.Context) {
	if s.meta == nil {
		return
	}
	if err := s.meta.RefreshMetadata(ctx); err != nil {
		s.log.Warn("could not read the catalog's metadata", "err", err)
	}
}

// metadataChanged reads again the metadata of the tables a statement
// wrote, so their sizes and freshness show soon.
func (s *Server) metadataChanged(objects []auth.Object) {
	if s.meta == nil {
		return
	}
	for _, o := range objects {
		if !o.IsSchema() {
			s.meta.TableChanged(o.Schema, o.Table)
		}
	}
}

// metadataAccess is what of the catalog's metadata one user may see.
type metadataAccess struct {
	// all is set for users who may read every table.
	all bool
	// admin may see every statement in the history.
	admin bool
	user  string
	roles []string
	privs *auth.Privileges
	// owners are every owner recorded.
	owners []auth.Ownership
}

// metadataAccess works out what user may see, with the session's grants.
func (s *Server) metadataAccess(ctx context.Context, sess *session, user auth.User) (*metadataAccess, error) {
	a := &metadataAccess{
		all:    rank[user.Role] >= rank[auth.RoleReader],
		admin:  user.Role == auth.RoleAdmin,
		user:   user.Name,
		owners: s.objectOwners(ctx),
	}
	if a.all {
		return a, nil
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return nil, err
	}
	a.privs, a.roles = privs, privs.Roles
	return a, nil
}

// owns reports whether the user owns o, directly or through a custom role.
func (a *metadataAccess) owns(o auth.Object) bool {
	w, ok := auth.OwnerOf(a.owners, o)
	return ok && w.Owns(a.user, a.roles)
}

// SeesTable reports whether the user may see the table or view
// schema.name: they may read every table, or have a grant on it or its
// schema, or own it or its schema.
func (a *metadataAccess) SeesTable(schema, name string) bool {
	if a.all {
		return true
	}
	for _, g := range a.privs.Grants {
		if strings.EqualFold(g.Object.Schema, schema) && (g.Object.IsSchema() || strings.EqualFold(g.Object.Table, name)) {
			return true
		}
	}
	return a.owns(auth.Object{Schema: schema, Table: name}) || a.owns(auth.Object{Schema: schema})
}

// seesSchema reports whether the user may see the schema: they may see
// something in it, or have a grant on it, or own it.
func (a *metadataAccess) seesSchema(schema string, md *catalog.Metadata) bool {
	if a.all || a.owns(auth.Object{Schema: schema}) {
		return true
	}
	if slices.ContainsFunc(a.privs.Grants, func(g auth.Grant) bool { return strings.EqualFold(g.Object.Schema, schema) }) {
		return true
	}
	return slices.ContainsFunc(a.owners, func(w auth.Ownership) bool {
		return !w.Object.IsSchema() && strings.EqualFold(w.Object.Schema, schema) && w.Owns(a.user, a.roles)
	}) || md != nil && slices.ContainsFunc(md.Tables, func(t catalog.TableMeta) bool {
		return strings.EqualFold(t.Schema, schema) && a.SeesTable(t.Schema, t.Name)
	})
}

// seesHistoryName reports whether the user may see a table the history
// names as catalog.schema.table.
func (a *metadataAccess) seesHistoryName(catalogName, name string) bool {
	if a.all {
		return true
	}
	rest, ok := strings.CutPrefix(name, catalogName+".")
	if !ok {
		return false
	}
	i := strings.LastIndexByte(rest, '.')
	return i > 0 && a.SeesTable(rest[:i], rest[i+1:])
}

// seesStatement reports whether the user may see a statement of user's in
// the query history: their own, or anyone's for admins.
func (a *metadataAccess) seesStatement(user string) bool {
	return a.admin || a.user == user
}

// catalogView is the CatalogSource of one statement.
type catalogView struct {
	s      *Server
	access *metadataAccess
	once   sync.Once
	md     *catalog.Metadata
}

// CatalogViewer is the engine.CatalogViewer for Igloo's sessions: a
// statement shows what its session's user may see when it runs.
// Statements from outside a session see nothing.
func (s *Server) CatalogViewer(ctx context.Context) func() (engine.CatalogSource, bool) {
	sess := sessionFrom(ctx)
	if sess == nil {
		return nil
	}
	return func() (engine.CatalogSource, bool) {
		user := sess.currentUser()
		if user.Name == "" {
			return nil, false
		}
		// The statement's own context is gone by now; the session's is
		// not needed for anything but the grants, which are cached.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		access, err := s.metadataAccess(ctx, sess, user)
		if err != nil {
			s.log.Warn("could not load grants", "user", user.Name, "err", err)
			return nil, false
		}
		return &catalogView{s: s, access: access}, true
	}
}

func (v *catalogView) Catalog() string                    { return v.s.catalogName() }
func (v *catalogView) SeesTable(schema, name string) bool { return v.access.SeesTable(schema, name) }

// Metadata returns the schemas, tables and views the user may see.
func (v *catalogView) Metadata() *catalog.Metadata {
	v.once.Do(func() {
		if v.s.meta == nil {
			return
		}
		md := v.s.meta.Metadata()
		if md == nil || v.access.all {
			v.md = md
			return
		}
		out := &catalog.Metadata{}
		for _, t := range md.Tables {
			if v.access.SeesTable(t.Schema, t.Name) {
				out.Tables = append(out.Tables, t)
			}
		}
		for _, sc := range md.Schemas {
			if v.access.seesSchema(sc.Name, md) {
				out.Schemas = append(out.Schemas, sc)
			}
		}
		v.md = out
	})
	return v.md
}

// Owners returns the owners of what the user may see.
func (v *catalogView) Owners() []auth.Ownership {
	if v.access.all {
		return v.access.owners
	}
	var out []auth.Ownership
	for _, w := range v.access.owners {
		if w.Object.IsSchema() && v.access.seesSchema(w.Object.Schema, v.Metadata()) ||
			!w.Object.IsSchema() && v.access.SeesTable(w.Object.Schema, w.Object.Table) {
			out = append(out, w)
		}
	}
	return out
}

// Lineage returns the edges of the lineage the user may know of: with the
// names of the ends they may not see, and the statements they may not see
// in the history, left out.
func (v *catalogView) Lineage(ctx context.Context) ([]audit.Edge, error) {
	if v.s.audit == nil {
		return nil, nil
	}
	edges, err := v.s.audit.Lineage(ctx)
	if err != nil {
		return nil, err
	}
	catalogName := v.s.catalogName()
	out := make([]audit.Edge, 0, len(edges))
	for _, e := range edges {
		from := e.FromLocation && v.access.all || !e.FromLocation && v.access.seesHistoryName(catalogName, e.From)
		to := v.access.seesHistoryName(catalogName, e.To)
		if !from && !to {
			continue
		}
		if !v.access.seesStatement(e.LastUser) || !from || !to {
			e.LastQueryID, e.LastUser, e.LastStatementType, e.LastJob = "", "", "", ""
		}
		if !from || !to {
			// That data flows between a table they may see and one they may
			// not, and nothing else.
			e.FirstSeen, e.LastSeen, e.Statements = time.Time{}, time.Time{}, 0
		}
		if !from {
			e.From = ""
		}
		if !to {
			e.To = ""
		}
		out = append(out, e)
	}
	return out, nil
}

// TableUsage returns how often the tables the user may see were used.
func (v *catalogView) TableUsage(ctx context.Context) ([]audit.TableUsage, error) {
	if v.s.audit == nil {
		return nil, nil
	}
	usage, err := v.s.audit.TableUsage(ctx)
	if err != nil || v.access.all {
		return usage, err
	}
	catalogName := v.s.catalogName()
	out := make([]audit.TableUsage, 0, len(usage))
	for _, u := range usage {
		if v.access.seesHistoryName(catalogName, u.Table) {
			out = append(out, u)
		}
	}
	return out, nil
}

// createdObject is the catalog object a CREATE TABLE or CREATE SCHEMA
// makes.
type createdObject struct {
	object auth.Object
	// keep says the object may exist already (OR REPLACE, IF NOT EXISTS),
	// and keeps any owner it has.
	keep bool
}

// createdObject works out what a CREATE statement makes in the catalog,
// or nil.
func (s *Server) createdObject(ctx context.Context, sess *session, query string) *createdObject {
	catalogName := s.catalogName()
	sc := &scanner{s: query}
	if catalogName == "" || sc.word() != "CREATE" || len(sqllex.Split(query)) != 1 {
		return nil
	}
	if sc.keywords("CREATE", "SCHEMA") {
		keep := sc.keywords("IF", "NOT", "EXISTS")
		name, ok := sc.name()
		switch {
		case !ok || len(name) > 2 || len(name) == 2 && !strings.EqualFold(name[0], catalogName):
			return nil
		case len(name) == 1:
			if db, _, err := currentSchema(ctx, sess.conn); err != nil || !strings.EqualFold(db, catalogName) {
				return nil
			}
		}
		return &createdObject{object: auth.Object{Schema: name[len(name)-1]}, keep: keep}
	}
	c, ok := parseCreateTable(query)
	if !ok || c.temporary {
		return nil
	}
	o, err := s.newTableTarget(ctx, sess, c.name)
	if err != nil {
		return nil
	}
	return &createdObject{object: o, keep: c.replace || c.ifNotExists}
}

// recordOwner makes user the owner of what c created.
func (s *Server) recordOwner(ctx context.Context, user auth.User, c *createdObject) {
	err := s.owners.SetObjectOwner(context.WithoutCancel(ctx),
		auth.Ownership{Object: c.object, Owner: user.Name, OwnerType: auth.GranteeUser, SetBy: user.Name}, c.keep)
	if err != nil {
		s.log.Warn("could not record the owner of a new object", "object", c.object.String(), "owner", user.Name, "err", err)
	}
	s.ownersChanged()
}

// readsMetadata matches queries that read the catalog's metadata, before
// which it is read if it has not been.
var readsMetadata = regexp.MustCompile(`(?i)\b(pg_description|obj_description|col_description|igloo_comments|igloo_schemata|igloo_tables|igloo_columns)\b` +
	`|\bigloo"?\s*\.\s*"?(schemata|tables|columns|information_schema_schemata|information_schema_tables)\b`)
