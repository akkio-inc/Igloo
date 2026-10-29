package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Working out which tables a query reads from DuckDB's parse of it: table
// references are taken from json_serialize_sql, which is DuckDB's own
// parser, and resolved the way DuckDB's binder resolves them. Views are
// expanded by parsing their definitions in turn.

// tableRef is a table name as a statement writes it, parts unquoted, and
// its alias.
type tableRef struct {
	parts []string
	alias string
}

// object is a table or view a name can resolve to.
type object struct {
	db, schema, name string
	view             bool
	// sql is a view's definition.
	sql      string
	internal bool
	// catalogView is a local copy of a view of the Iceberg catalog.
	catalogView bool
	// external is the view of an external table (package sharing), and
	// externalScans how many Parquet scans it makes.
	external      bool
	externalScans int
}

func (o object) key() string { return strings.ToLower(o.db + "." + o.schema + "." + o.name) }

// resolution is what a table name may mean.
type resolution struct {
	// first are the objects DuckDB binds the name to, as far as Igloo can
	// tell: those at the first place DuckDB looks that has any.
	first []object
	// all are every object at the places DuckDB looks for the name,
	// including ones the first hide. Grants are checked for all of them, so
	// a mistake in modelling the order DuckDB looks in can only refuse a
	// statement, never allow one, and so can a prepared statement that
	// DuckDB binds again after the first is dropped. Objects at places
	// DuckDB does not look for the name, such as a table of a connection's
	// schema named like one of the catalog's that is not on the search
	// path, are not among them: DuckDB cannot bind the name to those.
	all []object
}

// pathEntry is a database and schema on the search path. In the places
// DuckDB looks for a name (see searchPath), db is "" for the session's
// current database and schema "" for any schema of db.
type pathEntry struct{ db, schema string }

type searchPath struct {
	entries   []pathEntry
	defaultDB string
}

// sessionPath returns the session's search path and default database.
func sessionPath(ctx context.Context, conn *duckdb.Conn) (searchPath, error) {
	rows, err := conn.QueryContext(ctx, "SELECT system.main.current_setting('search_path')::VARCHAR, system.main.current_database()", nil)
	if err != nil {
		return searchPath{}, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		return searchPath{}, err
	}
	setting, _ := dest[0].(string)
	defaultDB, _ := dest[1].(string)
	return parseSearchPath(setting, defaultDB), nil
}

// parseSearchPath reads the search_path setting, with defaultDB the
// session's current database.
func parseSearchPath(setting, defaultDB string) searchPath {
	p := searchPath{defaultDB: defaultDB}
	for _, e := range splitTopLevelQuoted(setting, ',') {
		parts := splitQualified(strings.TrimSpace(e))
		switch len(parts) {
		case 2:
			p.entries = append(p.entries, pathEntry{parts[0], parts[1]})
		case 1:
			// A bare name is a schema of the default database, or a
			// database's main schema; take both.
			p.entries = append(p.entries, pathEntry{p.defaultDB, parts[0]}, pathEntry{parts[0], "main"})
		}
	}
	return p
}

// splitTopLevelQuoted splits s at sep outside double quotes.
func splitTopLevelQuoted(s string, sep byte) []string {
	var out []string
	start, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '"':
			quoted = !quoted
		case s[i] == sep && !quoted:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// setsSearchPath matches the SET and RESET that change the search path.
var setsSearchPath = regexp.MustCompile(`(?is)^(SET|RESET)\s+(?:SESSION\s+|LOCAL\s+)?"?(search_path|schema)"?(\s|=|;|$)`)

// resetsSearchPath matches the RESET and SET ... DEFAULT of the search path
// or schema at session scope, which DuckDB takes to its own default, its
// own database with an empty path, not to the path a session starts with.
var resetsSearchPath = regexp.MustCompile(
	`(?is)^(?:RESET\s+(?:SESSION\s+)?"?(?:search_path|schema)"?|SET\s+(?:SESSION\s+)?"?(?:search_path|schema)"?\s*(?:=|\s+TO)\s*DEFAULT)\s*;?\s*$`)

// keepViewsOnPath puts the local copies of the catalog's views back on the
// session's search path after a client set it, next to each catalog schema
// there, as on the path sessions start with (lake.public,memory.public).
// Superset's SQL Lab, for one, sets the path to the schema the analyst
// picked, and its views would no longer resolve. A schema without views
// has no local copies and needs none on the path, where DuckDB refuses a
// schema that does not exist.
func (s *Server) keepViewsOnPath(ctx context.Context, sess *session) error {
	local := s.engine.LocalDatabase()
	rows, err := sess.conn.QueryContext(ctx,
		`SELECT system.main.current_setting('search_path')::VARCHAR, system.main.current_database(), system.main.current_schema(),
			(SELECT system.main.string_agg(schema_name, chr(10)) FROM system.main.duckdb_schemas() WHERE database_name = $1)`,
		[]driver.NamedValue{{Ordinal: 1, Value: local}})
	if err != nil {
		return err
	}
	dest := make([]driver.Value, 4)
	err = rows.Next(dest)
	_ = rows.Close()
	if err != nil {
		return err
	}
	setting, _ := dest[0].(string)
	db, _ := dest[1].(string)
	schema, _ := dest[2].(string)
	schemas, _ := dest[3].(string)
	copies := map[string]bool{}
	for _, name := range strings.Split(schemas, "\n") {
		copies[strings.ToLower(name)] = true
	}
	hasCopies := func(schema string) bool { return copies[strings.ToLower(schema)] }
	if path, ok := withViewSchemas(setting, db, schema, s.catalogName(), local, hasCopies); ok {
		_, err = sess.conn.ExecContext(ctx, "SET SESSION search_path = "+quoteLiteral(path), nil)
	}
	return err
}

// withViewSchemas returns the search path setting with local, the database
// of views' local copies, after each schema of catalog on it whose views
// have copies there, as hasCopies says. setting is empty for the default
// path, schema of db. ok is false if nothing was missing.
func withViewSchemas(setting, db, schema, catalog, local string, hasCopies func(schema string) bool) (path string, ok bool) {
	entries := splitTopLevelQuoted(setting, ',')
	if strings.TrimSpace(setting) == "" {
		entries = []string{quoteName([]string{db, schema})}
	}
	key := func(parts []string) string { return strings.ToLower(strings.Join(parts, ".")) }
	present := map[string]bool{}
	for _, e := range entries {
		present[key(splitQualified(strings.TrimSpace(e)))] = true
	}
	var out []string
	seen := map[string]bool{}
	add := func(parts []string) {
		if k := key(parts); !seen[k] {
			seen[k] = true
			out = append(out, quoteName(parts))
		}
	}
	for _, e := range entries {
		parts := splitQualified(strings.TrimSpace(e))
		var views []string
		switch {
		case len(parts) == 1 && strings.EqualFold(db, catalog):
			views = []string{local, parts[0]}
		case len(parts) == 2 && strings.EqualFold(parts[0], catalog):
			views = []string{local, parts[1]}
		case len(parts) == 0:
			continue
		}
		add(parts)
		if views != nil && hasCopies(views[1]) {
			add(views)
			ok = ok || !present[key(views)]
		}
	}
	return strings.Join(out, ","), ok
}

// resolver resolves names for one statement on one session.
type resolver struct {
	s    *Server
	ctx  context.Context
	sess *session
	path searchPath
	// databases are the attached databases' types, by lower-cased name.
	databases map[string]string
	catalog   string
	local     string
	objects   map[string][]object
	macros    map[string]bool
	// readsOf caches what objects read, by object key.
	readsOf map[string]readSet
	// policyMarker is the alias prefix of the tables that policies read in
	// a statement rewritten for them (see protect.go). Those count as
	// scans but are not the user's reads, so they need no grant.
	policyMarker string
	// lenient only counts scans: it lets table functions, macros and
	// names that are not tables through, for checks of statements of
	// users who may use them.
	lenient bool
	// connectionsOnly is set when only reads from connections matter, for
	// users whose role reads the rest (see connreads.go): table functions
	// and other DuckDB databases are theirs to read.
	connectionsOnly bool
	// schemas caches the schemas of connections, by lower-cased name.
	schemas map[string][]string
	// conns caches the attached connections, by lower-cased name (see
	// isConnection).
	conns map[string]string
	// lookup and parse, when set, stand in for the session's connection:
	// lookup returns the objects called a lower-cased name, and parse
	// parses a statement (see objects.go).
	lookup func(name string) []object
	parse  func(text string) (any, error)
}

func (s *Server) newResolver(ctx context.Context, sess *session) (*resolver, error) {
	path, err := sessionPath(ctx, sess.conn)
	if err != nil {
		return nil, err
	}
	dbs, err := databaseTypes(ctx, sess)
	if err != nil {
		return nil, err
	}
	return &resolver{
		s: s, ctx: ctx, sess: sess, path: path,
		databases: dbs,
		catalog:   s.catalogName(),
		local:     s.engine.LocalDatabase(),
		objects:   map[string][]object{},
		readsOf:   map[string]readSet{},
	}, nil
}

// isConnection reports whether db is a connection, or any other database
// that is neither DuckDB's own nor the Iceberg catalog. Connections to
// Iceberg REST catalogs are databases of DuckDB's type iceberg, as the
// catalog is, so they are told by name.
func (r *resolver) isConnection(db string) bool {
	db = strings.ToLower(db)
	if isConnectionType(r.databases[db]) {
		return true
	}
	if r.conns == nil {
		r.conns = r.s.connectionCatalogs()
		if r.conns == nil {
			r.conns = map[string]string{}
		}
	}
	_, ok := r.conns[db]
	return ok && !strings.EqualFold(db, r.catalog)
}

// load fetches the tables and views called any of names that are not
// loaded yet.
func (r *resolver) load(names []string) error {
	var missing []string
	for _, n := range names {
		n = strings.ToLower(n)
		if _, ok := r.objects[n]; !ok && !slices.Contains(missing, n) {
			missing = append(missing, n)
			r.objects[n] = nil
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if r.lookup != nil {
		for _, n := range missing {
			r.objects[n] = r.lookup(n)
		}
		return nil
	}
	// Listing tables and views walks the catalog's in-memory set of tables,
	// which takes the catalog lock exclusively (see
	// engine.Engine.CatalogLock): held already while a statement is
	// prepared, but not by the commands that resolve names outside one.
	defer r.s.holdCatalog(r.sess)()
	quoted := make([]string, len(missing))
	for i, n := range missing {
		quoted[i] = quoteLiteral(n)
	}
	in := strings.Join(quoted, ", ")
	query := `SELECT database_name, schema_name, table_name, false, '', false
			FROM system.main.duckdb_tables() WHERE system.main.lower(table_name) IN (` + in + `)
		UNION ALL SELECT database_name, schema_name, view_name, true, sql, internal
			FROM system.main.duckdb_views() WHERE system.main.lower(view_name) IN (` + in + `)`
	if !r.sess.temps {
		// A session with no temporary tables sees the tables every session
		// sees, which the server keeps an index of (see indexedTables), so
		// only views, cheap to list, are listed here.
		idx, err := r.s.indexedTables(r.ctx, r.sess)
		if err != nil {
			return err
		}
		for _, n := range missing {
			r.objects[n] = append(r.objects[n], idx[n]...)
		}
		query = `SELECT database_name, schema_name, view_name, true, sql, internal
			FROM system.main.duckdb_views() WHERE system.main.lower(view_name) IN (` + in + `)`
	}
	rows, err := r.sess.conn.QueryContext(r.ctx, query, nil)
	if err != nil {
		return err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 6)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		o := object{}
		o.db, _ = dest[0].(string)
		o.schema, _ = dest[1].(string)
		o.name, _ = dest[2].(string)
		o.view, _ = dest[3].(bool)
		o.sql, _ = dest[4].(string)
		o.internal, _ = dest[5].(bool)
		o.catalogView = o.view && r.s.ddl != nil && strings.EqualFold(o.db, r.local) && r.s.ddl.IsView(o.schema, o.name)
		if o.view && r.s.external != nil && strings.EqualFold(o.db, r.local) {
			o.externalScans, o.external = r.s.external.IsExternal(o.schema, o.name)
		}
		k := strings.ToLower(o.name)
		r.objects[k] = append(r.objects[k], o)
	}
}

// searchPath is DuckDB's search path for a name in the body of view, or at
// the top level when view is nil, as its binder walks it (DuckDB 1.5's
// CatalogSearchPath, and Binder::GetSearchPath for views): temporary
// objects first; in a view, the view's own schema and then its database
// with any schema, which only names qualified with a schema look in; the
// session's path; the current database's main schema; and the system
// catalog's schemas. A temporary view's names resolve as at the top level
// (CatalogEntryRetriever::SetSearchPath skips the temporary database).
func (r *resolver) searchPath(view *object) []pathEntry {
	ps := []pathEntry{{"temp", "main"}}
	if view != nil && !strings.EqualFold(view.db, "temp") && !strings.EqualFold(view.db, "system") {
		ps = append(ps, pathEntry{view.db, view.schema}, pathEntry{view.db, ""})
	}
	ps = append(ps, r.path.entries...)
	return append(ps, pathEntry{"", "main"}, pathEntry{"system", "main"}, pathEntry{"system", "pg_catalog"})
}

// positions returns where DuckDB looks, in order, for a table or view whose
// name is qualified with qualifier (none, a schema or a database, or both),
// in the body of view or at the top level when view is nil
// (Binder::BindSchemaOrCatalog and GetCatalogEntries in DuckDB 1.5). anyOf
// is set instead when DuckDB looks in the default schema of a database
// other than DuckDB's own, which Igloo does not know: then any schema of
// anyOf may be the one.
//
//   - name: every database and schema on the search path.
//   - schema.name: that schema, in each database on the search path with
//     it, and in a view's database; else in the current database. Never in
//     a database that is not on the path with the schema, even when it has
//     a schema of that name.
//   - db.name, when db is an attached database: its schemas on the search
//     path, else its default schema. (If a database DuckDB would look in
//     for a schema called db says it has one, DuckDB refuses the name as
//     ambiguous, and the statement fails to prepare.)
//   - db.schema.name: exactly that.
func (r *resolver) positions(qualifier []string, view *object) (positions []pathEntry, anyOf string) {
	path := r.searchPath(view)
	db := func(name string) string {
		if name == "" {
			return r.path.defaultDB
		}
		return name
	}
	switch len(qualifier) {
	case 2:
		return []pathEntry{{qualifier[0], qualifier[1]}}, ""
	case 1:
		a := qualifier[0]
		if _, ok := r.databases[strings.ToLower(a)]; ok {
			for _, p := range path {
				// The current database's main schema is not on the path
				// under its name, so it is not one of its schemas there.
				if p.db != "" && p.schema != "" && strings.EqualFold(p.db, a) {
					positions = append(positions, pathEntry{p.db, p.schema})
				}
			}
			switch {
			case len(positions) > 0:
				return positions, ""
			case strings.EqualFold(r.databases[strings.ToLower(a)], "duckdb"):
				return []pathEntry{{a, "main"}}, ""
			}
			return nil, a
		}
		if slices.Contains(systemSchemas, strings.ToLower(a)) {
			// DuckDB looks for these only in the system catalog
			// (DefaultSchemaGenerator::IsDefaultSchema).
			return []pathEntry{{"system", a}}, ""
		}
		for _, p := range path {
			if p.schema == "" || strings.EqualFold(p.schema, a) {
				positions = append(positions, pathEntry{db(p.db), a})
			}
		}
		if len(positions) == 0 {
			positions = []pathEntry{{r.path.defaultDB, a}}
		}
		return positions, ""
	}
	for _, p := range path {
		if p.schema != "" {
			positions = append(positions, pathEntry{db(p.db), p.schema})
		}
	}
	return positions, ""
}

// resolve works out what ref, written in the body of view (or at the top
// level when view is nil), may mean: the objects of its name at the places
// DuckDB looks for it.
func (r *resolver) resolve(ref tableRef, view *object) (resolution, error) {
	parts := ref.parts
	if err := r.load([]string{parts[len(parts)-1]}); err != nil {
		return resolution{}, err
	}
	objs := r.objects[strings.ToLower(parts[len(parts)-1])]
	var res resolution
	add := func(m []object) {
		if res.first == nil && len(m) > 0 {
			res.first = m
		}
		for _, o := range m {
			if !slices.Contains(res.all, o) {
				res.all = append(res.all, o)
			}
		}
	}
	positions, anyOf := r.positions(parts[:len(parts)-1], view)
	for _, p := range positions {
		var m []object
		for _, o := range objs {
			if strings.EqualFold(o.db, p.db) && strings.EqualFold(o.schema, p.schema) {
				m = append(m, o)
			}
		}
		add(m)
	}
	if anyOf != "" {
		var m []object
		for _, o := range objs {
			if strings.EqualFold(o.db, anyOf) {
				m = append(m, o)
			}
		}
		add(m)
	}
	return res, nil
}

// readSet is what a query or view reads from the catalog and from
// connections.
type readSet struct {
	// scans is how many catalog table scans DuckDB plans for it.
	scans int
	// lake counts the catalog's tables among scans, which DuckDB's plan
	// shows as ICEBERG_SCAN (as SEQ_SCAN when the catalog is a DuckDB
	// database, in tests), and icebergFns the calls of iceberg_scan, whose
	// scans the plan shows the same way. Checks for users whose role reads
	// the catalog count them with the scans of Iceberg REST catalogs'
	// tables, which the plan shows the same way too (see connreads.go).
	lake, icebergFns int
	tables           []auth.Object
	// policyTables are the tables that policies read (see
	// resolver.policyMarker).
	policyTables []auth.Object
	// connScans are the scans of connections' tables DuckDB plans for it,
	// and connObjects every table of a connection it may read.
	connScans   []connectionScan
	connObjects []auth.Object
}

func (rs *readSet) add(o auth.Object) {
	list := &rs.tables
	if o.Catalog != "" {
		list = &rs.connObjects
	}
	if !slices.ContainsFunc(*list, func(t auth.Object) bool { return t.Equal(o) }) {
		*list = append(*list, o)
	}
}

// maxViewDepth bounds views reading views.
const maxViewDepth = 16

// analyze returns what the query text reads, written in the body of view
// or at the top level.
func (r *resolver) analyze(text string, view *object, depth int) (readSet, error) {
	var rs readSet
	if depth > maxViewDepth {
		return rs, denied("views nest more than %d deep", maxViewDepth)
	}
	ast, err := r.serialize(text)
	if err != nil {
		return rs, denied("cannot analyze which tables this statement reads: %v", err)
	}
	w := &astWalk{}
	w.walk(ast, nil)

	if !r.lenient {
		for _, f := range w.tableFunctions {
			if r.connectionsOnly {
				// Scans a table function makes show in the plan, unmatched.
				break
			}
			if !allowedFunctions[strings.ToLower(f.name)] || f.schema != "" && !strings.EqualFold(f.schema, "main") {
				return rs, denied("%s reads data that grants do not cover, which needs the reader role", f.name)
			}
		}
		if err := r.checkMacros(append(w.functions, w.tableFunctions...)); err != nil {
			return rs, err
		}
	}
	if r.connectionsOnly {
		if err := r.tableArgCalls(w.tableFunctions, view, &rs); err != nil {
			return rs, err
		}
	}

	names := make([]string, len(w.refs))
	for i, ref := range w.refs {
		names[i] = ref.parts[len(ref.parts)-1]
	}
	if err := r.load(names); err != nil {
		return rs, err
	}
	for _, ref := range w.refs {
		res, err := r.resolve(ref, view)
		if err != nil {
			return rs, err
		}
		if len(res.all) == 0 {
			if r.lenient || r.connectionsOnly {
				// A file or a replacement scan: not a connection's table,
				// which DuckDB lists.
				continue
			}
			return rs, denied("%s is not a table or view; reading files directly needs the reader role", strings.Join(ref.parts, "."))
		}
		var first *readSet
		for _, o := range res.first {
			reads, err := r.reads(o, depth)
			if err != nil {
				return rs, err
			}
			if first != nil && (reads.scans != first.scans || !sameScans(reads.connScans, first.connScans)) {
				return rs, denied("%s is ambiguous", strings.Join(ref.parts, "."))
			}
			first = &reads
		}
		if first != nil {
			rs.scans += first.scans
			rs.lake += first.lake
			rs.icebergFns += first.icebergFns
			rs.connScans = append(rs.connScans, first.connScans...)
		}
		if r.policyMarker != "" && strings.HasPrefix(ref.alias, r.policyMarker) {
			// A policy reads it, not the user.
			for _, o := range res.first {
				if !o.view && strings.EqualFold(o.db, r.catalog) {
					rs.policyTables = append(rs.policyTables, auth.Object{Schema: o.schema, Table: o.name})
				}
			}
			continue
		}
		for _, o := range res.all {
			reads, err := r.reads(o, depth)
			if err != nil {
				return rs, err
			}
			for _, t := range append(reads.tables, reads.connObjects...) {
				rs.add(t)
			}
		}
	}
	return rs, nil
}

// reads returns what reading o reads from the catalog and from
// connections: a catalog table or a connection's table itself, a view what
// its query reads, and local, temporary and system objects nothing.
func (r *resolver) reads(o object, depth int) (readSet, error) {
	if rs, ok := r.readsOf[o.key()]; ok {
		return rs, nil
	}
	var rs readSet
	switch {
	case o.view && o.internal:
	case o.external:
		// Reading an external table is reading the table, whatever its
		// view does: Igloo wrote the view, over files only it can reach.
		// Its Parquet scans are counted against the plan's (planScans);
		// the lenient count is of catalog scans (see verify in
		// protect.go), of which it makes none.
		rs = readSet{scans: o.externalScans, tables: []auth.Object{{Schema: o.schema, Table: o.name}}}
		if r.lenient {
			rs.scans = 0
		}
	case o.view:
		if !r.ownDatabase(o.db) && (!r.connectionsOnly || r.isConnection(o.db)) {
			return rs, denied("reading views of database %s needs the reader role", o.db)
		}
		body := viewBody(o.sql)
		if body == "" {
			return rs, denied("cannot analyze view %s.%s", o.schema, o.name)
		}
		var err error
		if rs, err = r.analyze(body, &o, depth+1); err != nil {
			return rs, err
		}
	case strings.EqualFold(o.db, r.catalog):
		rs = readSet{scans: 1, lake: 1, tables: []auth.Object{{Schema: o.schema, Table: o.name}}}
	case r.isConnection(o.db):
		// The history's analysis (r.lookup) refuses nothing.
		if r.lookup == nil {
			if err := r.distinctInCase(o); err != nil {
				return rs, err
			}
		}
		// The postgres and mysql extensions show a remote view as a table.
		rs = readSet{
			connScans:   []connectionScan{{r.databases[strings.ToLower(o.db)], strings.ToLower(o.name)}},
			connObjects: []auth.Object{{Catalog: strings.ToLower(o.db), Schema: o.schema, Table: o.name}},
		}
	case strings.EqualFold(o.db, "temp") || strings.EqualFold(o.db, r.local) || strings.EqualFold(o.db, "system"):
	case r.connectionsOnly:
		// Another of DuckDB's databases, which the user's role reads.
	default:
		return rs, denied("reading tables of database %s needs the reader role", o.db)
	}
	r.readsOf[o.key()] = rs
	return rs, nil
}

// distinctInCase refuses a table of a connection whose name, or whose
// schema's name, differs only in case from another's in the same
// connection. PostgreSQL, and MySQL on Linux, tell such names apart, and
// DuckDB attaches both; but Igloo names a connection's schemas and tables
// without case, in grants and policies as DuckDB does in SQL (which of the
// two crm.s.CUSTOMERS means is DuckDB's choice), so a grant on one would
// cover the other.
func (r *resolver) distinctInCase(o object) error {
	for _, p := range r.objects[strings.ToLower(o.name)] {
		if strings.EqualFold(p.db, o.db) && strings.EqualFold(p.schema, o.schema) && (p.schema != o.schema || p.name != o.name) {
			return denied("%s.%s.%s and %s.%s.%s differ only in case, which Igloo's grants and policies do not tell apart; "+
				"rename one of them in the remote database", o.db, o.schema, o.name, p.db, p.schema, p.name)
		}
	}
	db := strings.ToLower(o.db)
	if _, ok := r.schemas[db]; !ok {
		names, err := queryStrings(r.ctx, r.sess, `SELECT schema_name FROM system.main.duckdb_schemas()
			WHERE system.main.lower(database_name) = $1`, db)
		if err != nil {
			return err
		}
		if r.schemas == nil {
			r.schemas = map[string][]string{}
		}
		r.schemas[db] = names
	}
	for _, s := range r.schemas[db] {
		if s != o.schema && strings.EqualFold(s, o.schema) {
			return denied("schemas %s.%s and %s.%s differ only in case, which Igloo's grants and policies do not tell apart; "+
				"rename one of them in the remote database", o.db, o.schema, o.db, s)
		}
	}
	return nil
}

// ownDatabase reports whether db is the catalog, the local database or the
// temporary one: the databases whose views may be expanded.
func (r *resolver) ownDatabase(db string) bool {
	return strings.EqualFold(db, r.catalog) || strings.EqualFold(db, r.local) || strings.EqualFold(db, "temp")
}

// checkMacros refuses calls to macros that Igloo did not define: a macro
// could read tables that its call does not name. Igloo's own are temporary
// macros of each session.
func (r *resolver) checkMacros(calls []fnRef) error {
	if len(calls) == 0 {
		return nil
	}
	if r.macros == nil {
		names, err := queryStrings(r.ctx, r.sess, `SELECT DISTINCT system.main.lower(function_name) FROM system.main.duckdb_functions()
			WHERE NOT internal AND function_type IN ('macro', 'table_macro') AND database_name <> 'temp'`)
		if err != nil {
			return err
		}
		r.macros = map[string]bool{}
		for _, n := range names {
			r.macros[n] = true
		}
	}
	for _, c := range calls {
		if r.macros[strings.ToLower(c.name)] {
			return denied("macro %s may read tables it does not name, which needs the reader role", c.name)
		}
	}
	return nil
}

// serialize returns DuckDB's parse of a query as JSON.
func (r *resolver) serialize(text string) (any, error) {
	if r.parse != nil {
		return r.parse(text)
	}
	rows, err := r.sess.conn.QueryContext(r.ctx,
		"SELECT system.main.json_serialize_sql($1::VARCHAR, skip_null := true, skip_empty := true)::VARCHAR",
		[]driver.NamedValue{{Ordinal: 1, Value: text}})
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return nil, err
	}
	raw, _ := dest[0].(string)
	return decodeParse(raw)
}

// decodeParse reads the one statement of json_serialize_sql's result.
func decodeParse(raw string) (any, error) {
	var doc struct {
		Error        bool   `json:"error"`
		ErrorMessage string `json:"error_message"`
		Statements   []any  `json:"statements"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return nil, err
	}
	if doc.Error {
		return nil, errors.New(doc.ErrorMessage)
	}
	if len(doc.Statements) != 1 {
		return nil, fmt.Errorf("expected one statement, found %d", len(doc.Statements))
	}
	return doc.Statements[0], nil
}

// viewBody returns the query of a view definition as DuckDB prints it:
// CREATE [OR REPLACE] [TEMP] VIEW name [(columns)] AS query;
func viewBody(sql string) string {
	sc := &scanner{s: sql}
	if !sc.keywords("CREATE") {
		return ""
	}
	sc.keywords("OR", "REPLACE")
	_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY")
	if !sc.keywords("VIEW") {
		return ""
	}
	sc.keywords("IF", "NOT", "EXISTS")
	if _, ok := sc.name(); !ok {
		return ""
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return ""
		}
		sc.i = end + 1
	}
	if !sc.keywords("AS") {
		return ""
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
}

// fnRef is a function a query calls, and for a table function its
// arguments, as DuckDB parsed them.
type fnRef struct {
	schema, name string
	args         []any
}

// astWalk collects the table references and function calls of a parsed
// query. References to common table expressions are followed into their
// definitions, each once, as DuckDB plans each used one once; unused ones
// are skipped, as DuckDB does not plan them. DESCRIBE reads no rows and is
// skipped too.
type astWalk struct {
	refs           []tableRef
	tableFunctions []fnRef
	functions      []fnRef
}

type cteDef struct {
	body   any
	scope  *cteScope
	walked bool
}

type cteScope struct {
	defs   map[string]*cteDef
	parent *cteScope
}

func (sc *cteScope) find(name string) *cteDef {
	for ; sc != nil; sc = sc.parent {
		if d := sc.defs[strings.ToLower(name)]; d != nil {
			return d
		}
	}
	return nil
}

func (w *astWalk) walk(n any, sc *cteScope) {
	switch n := n.(type) {
	case []any:
		for _, c := range n {
			w.walk(c, sc)
		}
	case map[string]any:
		if cm, ok := n["cte_map"].(map[string]any); ok {
			// Each CTE sees the ones before it, and itself if it is
			// recursive; the query sees them all.
			if entries, _ := cm["map"].([]any); len(entries) > 0 {
				for _, e := range entries {
					em, _ := e.(map[string]any)
					key, _ := em["key"].(string)
					value, _ := em["value"].(map[string]any)
					def := &cteDef{body: value["query"], scope: sc}
					sc = &cteScope{defs: map[string]*cteDef{strings.ToLower(key): def}, parent: sc}
					if q, _ := value["query"].(map[string]any); q != nil {
						if node, _ := q["node"].(map[string]any); str(node["type"]) == "RECURSIVE_CTE_NODE" {
							def.scope = sc
						}
					}
				}
			}
		}
		switch str(n["type"]) {
		case "BASE_TABLE":
			catalog, schema, table := str(n["catalog_name"]), str(n["schema_name"]), str(n["table_name"])
			if catalog == "" && schema == "" {
				if d := sc.find(table); d != nil {
					if !d.walked {
						d.walked = true
						w.walk(d.body, d.scope)
					}
					return
				}
			}
			var parts []string
			for _, p := range []string{catalog, schema, table} {
				if p != "" {
					parts = append(parts, p)
				}
			}
			w.refs = append(w.refs, tableRef{parts: parts, alias: str(n["alias"])})
			w.walk(n["at_clause"], sc)
			return
		case "SHOW_REF":
			if str(n["show_type"]) == "DESCRIBE" {
				return
			}
		case "TABLE_FUNCTION":
			if f, ok := n["function"].(map[string]any); ok {
				w.tableFunctions = append(w.tableFunctions, fnRef{schema: str(f["schema"]), name: str(f["function_name"]), args: asList(f["children"])})
			}
		}
		if str(n["class"]) == "FUNCTION" && n["is_operator"] != true {
			w.functions = append(w.functions, fnRef{schema: str(n["schema"]), name: str(n["function_name"])})
		}
		for k, v := range n {
			if k != "cte_map" {
				w.walk(v, sc)
			}
		}
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// tableIndexTTL bounds how long the server's index of tables is used. It is
// the catalog proxy's for listings, which the index is built from.
const tableIndexTTL = 10 * time.Second

// tableIndex is the tables of every database but temp, which is each
// session's own, by lower-cased name (see indexedTables).
type tableIndex struct {
	mu sync.Mutex
	// gen is the generations it was built at (see indexedTables), and at
	// when.
	gen    uint64
	at     time.Time
	byName map[string][]object
}

// indexedTables returns the server's index of the tables of every database
// but temp, by lower-cased name, from one listing of them shared by every
// session's checks: DuckDB lists the Iceberg catalog's tables, and those
// of connections, again in each statement, which a statement's checks
// would otherwise pay for each name they resolve. The index is built again
// whenever what it was built from may have changed: DDL here, local copies
// of views and connections (catalogGeneration), the Iceberg catalog's
// listings as DuckDB sees them (engine.ListingsGeneration, which changes
// when a table DuckDB loads by name was not in them, so that the checks of
// a statement that bound a table made elsewhere see it), and after
// tableIndexTTL. Callers hold the catalog lock (see holdCatalog), as for
// any listing. The map returned is never changed.
func (s *Server) indexedTables(ctx context.Context, sess *session) (map[string][]object, error) {
	gen := s.catalogGeneration() + s.engine.ListingsGeneration()
	s.tables.mu.Lock()
	if s.tables.byName != nil && s.tables.gen == gen && time.Since(s.tables.at) < tableIndexTTL {
		m := s.tables.byName
		s.tables.mu.Unlock()
		return m, nil
	}
	s.tables.mu.Unlock()
	rows, err := sess.conn.QueryContext(ctx, `SELECT database_name, schema_name, table_name
		FROM system.main.duckdb_tables() WHERE database_name <> 'temp'`, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	m := map[string][]object{}
	dest := make([]driver.Value, 3)
	for {
		if err := rows.Next(dest); err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			break
		}
		o := object{}
		o.db, _ = dest[0].(string)
		o.schema, _ = dest[1].(string)
		o.name, _ = dest[2].(string)
		k := strings.ToLower(o.name)
		m[k] = append(m[k], o)
	}
	s.tables.mu.Lock()
	s.tables.gen, s.tables.at, s.tables.byName = gen, time.Now(), m
	s.tables.mu.Unlock()
	return m, nil
}

// mayCreateTemporary reports whether a statement may make a temporary
// table or view: one that creates anything and mentions TEMP, TEMPORARY or
// the temp database.
func mayCreateTemporary(query string) bool {
	creates, temp := false, false
	for _, w := range sqllex.CodeWords(query) {
		switch strings.ToLower(w) {
		case "create":
			creates = true
		case "temp", "temporary":
			temp = true
		}
	}
	return creates && temp
}
