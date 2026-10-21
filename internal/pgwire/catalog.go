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
	"github.com/akkio-inc/igloo/internal/policy"
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
// see that statement in the query history: its user and admins. A table a
// clean room keeps to a party shows to that party alone, whatever the
// role, and no edge to or from it shows to anyone else.

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
// and lists the connections' tables, before a statement of sess that shows
// them.
func (s *Server) refreshMetadata(ctx context.Context, sess *session) {
	s.listConnectionTables(ctx, sess)
	if s.meta == nil {
		return
	}
	if err := s.meta.RefreshMetadata(ctx); err != nil {
		s.log.Warn("could not read the catalog's metadata", "err", err)
	}
}

const (
	// connectionsTTL is how long a listing of the connections' tables is
	// used, unless connections change. DuckDB lists them from what it read
	// when it attached each connection, without asking the remote
	// database again; tables created since show once a statement names
	// them, or after ALTER CONNECTION ... REFRESH.
	connectionsTTL = time.Minute
	// connectionsWait is how long the first listing may hold up the
	// statement that needs it; later ones are made in the background.
	connectionsWait = 5 * time.Second
)

// connectionListing keeps the tables and columns of the connections.
type connectionListing struct {
	mu      sync.Mutex
	gen     uint64
	at      time.Time
	listing bool
	tables  []catalog.TableMeta
}

// connectionTables returns the connections' tables as last listed.
func (s *Server) connectionTables() []catalog.TableMeta {
	c := &s.connections
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tables
}

// listConnectionTables lists the connections' tables again when
// connections changed or the listing is connectionsTTL old. Only the
// first listing waits, and for at most connectionsWait; when one fails,
// the last stays in use and the next is tried connectionsTTL later, so a
// database that is down never keeps the catalog from answering. The
// listing takes the catalog lock (see readConnectionTables), so sess, whose
// statement may hold it, lets go of it while it waits (see withoutCatalog).
func (s *Server) listConnectionTables(ctx context.Context, sess *session) {
	catalogs := s.connectionCatalogs()
	c := &s.connections
	c.mu.Lock()
	if len(catalogs) == 0 {
		c.tables = nil
		c.mu.Unlock()
		return
	}
	gen := s.remotes.Generation()
	if c.listing || c.gen == gen && time.Since(c.at) < connectionsTTL {
		c.mu.Unlock()
		return
	}
	first := c.at.IsZero()
	c.listing = true
	c.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		tables, err := s.readConnectionTables(ctx, catalogs)
		if err != nil {
			s.log.Warn("could not list the tables of connections", "err", err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.listing, c.gen, c.at = false, gen, time.Now()
		if err == nil {
			c.tables = tables
		}
	}()
	if first {
		s.withoutCatalog(sess, func() {
			select {
			case <-done:
			case <-ctx.Done():
			case <-time.After(connectionsWait):
			}
		})
	}
}

// remoteSystemSchemas are the schemas in which PostgreSQL and MySQL
// describe themselves, which DuckDB attaches with a connection's own: the
// catalog leaves them out.
var remoteSystemSchemas = []string{"information_schema", "pg_catalog", "mysql", "performance_schema", "sys"}

// readConnectionTables lists the tables of the connections catalogs, with
// their columns, as DuckDB has them. duckdb_columns() walks every attached
// database's tables, the Iceberg catalog's in-memory set too, whatever the
// filter, so this holds the catalog lock exclusively (see
// engine.Engine.CatalogLock); it runs off any session, which holds no lock
// for it.
func (s *Server) readConnectionTables(ctx context.Context, catalogs map[string]string) ([]catalog.TableMeta, error) {
	defer s.engine.CatalogLock()()
	conn, err := s.engine.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	names := make([]string, 0, len(catalogs))
	for name := range catalogs {
		names = append(names, quoteLiteral(name))
	}
	system := make([]string, len(remoteSystemSchemas))
	for i, name := range remoteSystemSchemas {
		system[i] = quoteLiteral(name)
	}
	rows, err := queryRows(ctx, conn, `SELECT database_name, schema_name, table_name, column_name, data_type
		FROM system.main.duckdb_columns() WHERE system.main.lower(database_name) IN (`+strings.Join(names, ", ")+`)
			AND system.main.lower(schema_name) NOT IN (`+strings.Join(system, ", ")+`)
		ORDER BY database_name, schema_name, table_name, column_index`)
	if err != nil {
		return nil, err
	}
	var out []catalog.TableMeta
	for _, row := range rows {
		var db, schema, table, column, typ string
		db, _ = row[0].(string)
		schema, _ = row[1].(string)
		table, _ = row[2].(string)
		column, _ = row[3].(string)
		typ, _ = row[4].(string)
		db = strings.ToLower(db)
		if n := len(out); n == 0 || out[n-1].Catalog != db || out[n-1].Schema != schema || out[n-1].Name != table {
			out = append(out, catalog.TableMeta{Catalog: db, Schema: schema, Name: table, Rows: -1, Bytes: -1, Files: -1})
		}
		t := &out[len(out)-1]
		t.Columns = append(t.Columns, catalog.ColumnMeta{Name: column, Type: typ})
	}
	return out, nil
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
	// all is set for users who may read every table of the catalog.
	all bool
	// admin may see every statement in the history, and every table of
	// every connection.
	admin bool
	user  string
	roles []string
	// privs are the user's grants; nil for readers, writers and admins
	// whose grants could not be read.
	privs *auth.Privileges
	// owners are every owner recorded.
	owners []auth.Ownership
	// policies say which tables clean rooms keep to a party: those of
	// other parties are not seen, whatever the role.
	policies *policy.Snapshot
}

// metadataAccess works out what user may see, with the session's grants.
func (s *Server) metadataAccess(ctx context.Context, sess *session, user auth.User) (*metadataAccess, error) {
	a := &metadataAccess{
		all:      rank[user.Role] >= rank[auth.RoleReader],
		admin:    user.Role == auth.RoleAdmin,
		user:     user.Name,
		owners:   s.objectOwners(ctx),
		policies: s.gov.snapshot(ctx),
	}
	privs, err := s.privileges(ctx, sess, user)
	switch {
	case err != nil && !a.all:
		return nil, err
	case err != nil:
		// Readers and writers need grants only for connections' tables,
		// which they then do not see, and roles only for the tables clean
		// rooms keep to them, which they then do not see either.
		s.log.Warn("could not load grants", "user", user.Name, "err", err)
	default:
		a.privs, a.roles = privs, privs.Roles
	}
	return a, nil
}

// kept reports whether a clean room keeps the catalog's table schema.name
// to another party than the user (see policy.Confinement): its name,
// description, columns, tags, size, usage and lineage show to that party
// alone, as reading it is theirs alone.
func (a *metadataAccess) kept(schema, name string) bool {
	r := a.policies.Rules(schema, name)
	return r != nil && len(r.Confined) > 0 && !r.Admits(a.user, a.roles)
}

// grants are the user's grants on the catalog's schemas and tables.
func (a *metadataAccess) grants() []auth.Grant {
	if a.privs == nil {
		return nil
	}
	var out []auth.Grant
	for _, g := range a.privs.Grants {
		if g.Object.Catalog == "" {
			out = append(out, g)
		}
	}
	return out
}

// owns reports whether the user owns o, directly or through a custom role.
func (a *metadataAccess) owns(o auth.Object) bool {
	w, ok := auth.OwnerOf(a.owners, o)
	return ok && w.Owns(a.user, a.roles)
}

// SeesTable reports whether the user may see the table or view
// schema.name of the catalog, or the external table: no clean room keeps
// it to another party, and they may read every table, or have a grant on
// it or its schema, or own it or its schema.
func (a *metadataAccess) SeesTable(schema, name string) bool {
	if a.kept(schema, name) {
		return false
	}
	if a.all {
		return true
	}
	for _, g := range a.grants() {
		if strings.EqualFold(g.Object.Schema, schema) && (g.Object.IsSchema() || strings.EqualFold(g.Object.Table, name)) {
			return true
		}
	}
	return a.owns(auth.Object{Schema: schema, Table: name}) || a.owns(auth.Object{Schema: schema})
}

// seesConnectionTable reports whether the user may see the table
// schema.name of connection catalogName: as with reading it, admins may,
// and everyone else through a grant on it, its schema or the connection;
// the built-in roles do not cover connections.
func (a *metadataAccess) seesConnectionTable(catalogName, schema, name string) bool {
	return a.admin || a.privs.AllowsConnection(auth.PrivilegeSelect, catalogName, schema, name)
}

// seesSchema reports whether the user may see the catalog's schema: they
// may see something in it, or have a grant on it, or own it.
func (a *metadataAccess) seesSchema(schema string, md *catalog.Metadata) bool {
	if a.all || a.owns(auth.Object{Schema: schema}) {
		return true
	}
	if slices.ContainsFunc(a.grants(), func(g auth.Grant) bool { return strings.EqualFold(g.Object.Schema, schema) }) {
		return true
	}
	return slices.ContainsFunc(a.owners, func(w auth.Ownership) bool {
		return !w.Object.IsSchema() && strings.EqualFold(w.Object.Schema, schema) && w.Owns(a.user, a.roles)
	}) || md != nil && slices.ContainsFunc(md.Tables, func(t catalog.TableMeta) bool {
		return t.Catalog == "" && strings.EqualFold(t.Schema, schema) && a.SeesTable(t.Schema, t.Name)
	})
}

// keptHistoryName reports whether a table the history names is one a clean
// room keeps to another party (see kept).
func (a *metadataAccess) keptHistoryName(catalogName, name string) bool {
	db, rest, _ := strings.Cut(name, ".")
	i := strings.LastIndexByte(rest, '.')
	return i > 0 && strings.EqualFold(db, catalogName) && a.kept(rest[:i], rest[i+1:])
}

// seesHistoryName reports whether the user may see a table the history
// names as catalog.schema.table, or connection.schema.table for a
// connection's (see objectNames).
func (a *metadataAccess) seesHistoryName(catalogName, name string) bool {
	db, rest, ok := strings.Cut(name, ".")
	i := strings.LastIndexByte(rest, '.')
	switch {
	case !ok || i <= 0:
		return false
	case strings.EqualFold(db, catalogName):
		return a.SeesTable(rest[:i], rest[i+1:])
	}
	return a.seesConnectionTable(db, rest[:i], rest[i+1:])
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

func (v *catalogView) SeesConnectionTable(catalog, schema, name string) bool {
	return v.access.seesConnectionTable(catalog, schema, name)
}

// Metadata returns the schemas, tables and views the user may see: the
// catalog's, the external tables in its schemas, and the connections'
// tables (see connectionTables).
func (v *catalogView) Metadata() *catalog.Metadata {
	v.once.Do(func() {
		var md *catalog.Metadata
		if v.s.meta != nil {
			md = v.s.meta.Metadata()
		}
		var external []catalog.TableMeta
		if v.s.external != nil {
			external = v.s.external.CatalogTables()
		}
		connections := v.s.connectionTables()
		if md == nil && len(external) == 0 && len(connections) == 0 {
			return
		}
		all := &catalog.Metadata{}
		if md != nil {
			all.Schemas, all.Tables = md.Schemas, slices.Clip(md.Tables)
		}
		all.Tables = append(all.Tables, external...)
		out := &catalog.Metadata{}
		for _, t := range all.Tables {
			if v.access.SeesTable(t.Schema, t.Name) {
				out.Tables = append(out.Tables, t)
			}
		}
		for _, sc := range all.Schemas {
			if v.access.seesSchema(sc.Name, all) {
				out.Schemas = append(out.Schemas, sc)
			}
		}
		for _, t := range connections {
			if v.access.seesConnectionTable(t.Catalog, t.Schema, t.Name) {
				out.Tables = append(out.Tables, t)
			}
		}
		v.md = out
	})
	return v.md
}

// Owners returns the owners of what the user may see.
func (v *catalogView) Owners() []auth.Ownership {
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
		// Not even that data flows to or from a table a clean room keeps
		// to another party.
		if !e.FromLocation && v.access.keptHistoryName(catalogName, e.From) || v.access.keptHistoryName(catalogName, e.To) {
			continue
		}
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
	if err != nil {
		return nil, err
	}
	catalogName := v.s.catalogName()
	out := make([]audit.TableUsage, 0, len(usage))
	for _, u := range usage {
		sees := v.access.seesHistoryName(catalogName, u.Table)
		if v.access.admin {
			sees = !v.access.keptHistoryName(catalogName, u.Table)
		}
		if sees {
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

// createdObjects works out what the CREATE statements of query make in
// the catalog: only admins may send several statements in one query.
func (s *Server) createdObjects(ctx context.Context, sess *session, query string) []*createdObject {
	if s.catalogName() == "" {
		return nil
	}
	var out []*createdObject
	for _, stmt := range sqllex.Split(query) {
		if c := s.createdObject(ctx, sess, stmt.Text); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// createdObject works out what one CREATE statement makes in the
// catalog, or nil.
func (s *Server) createdObject(ctx context.Context, sess *session, query string) *createdObject {
	catalogName := s.catalogName()
	sc := &scanner{s: query}
	if sc.word() != "CREATE" {
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
