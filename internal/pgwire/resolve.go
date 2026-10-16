package pgwire

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
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
	// tell: those at the first place on the search path with any.
	first []object
	// all are every object the name could mean, including ones the first
	// hide. Grants are checked for all of them, so a mistake in modelling
	// DuckDB's search order can only refuse a statement, never allow one.
	all []object
}

// pathEntry is a database and schema on the search path.
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
	p := searchPath{}
	p.defaultDB, _ = dest[1].(string)
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
	return p, nil
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

// resolver resolves names for one statement on one session.
type resolver struct {
	s         *Server
	ctx       context.Context
	sess      *session
	path      searchPath
	databases map[string]bool
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
}

func (s *Server) newResolver(ctx context.Context, sess *session) (*resolver, error) {
	path, err := sessionPath(ctx, sess.conn)
	if err != nil {
		return nil, err
	}
	dbs, err := queryStrings(ctx, sess, "SELECT system.main.lower(database_name) FROM system.main.duckdb_databases()")
	if err != nil {
		return nil, err
	}
	r := &resolver{
		s: s, ctx: ctx, sess: sess, path: path,
		databases: map[string]bool{},
		catalog:   s.catalogName(),
		local:     s.engine.LocalDatabase(),
		objects:   map[string][]object{},
		readsOf:   map[string]readSet{},
	}
	for _, d := range dbs {
		r.databases[d] = true
	}
	return r, nil
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
	quoted := make([]string, len(missing))
	for i, n := range missing {
		quoted[i] = quoteLiteral(n)
	}
	in := strings.Join(quoted, ", ")
	rows, err := r.sess.conn.QueryContext(r.ctx, `SELECT database_name, schema_name, table_name, false, '', false
			FROM system.main.duckdb_tables() WHERE system.main.lower(table_name) IN (`+in+`)
		UNION ALL SELECT database_name, schema_name, view_name, true, sql, internal
			FROM system.main.duckdb_views() WHERE system.main.lower(view_name) IN (`+in+`)`, nil)
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
		if o.view && r.s.shares != nil && strings.EqualFold(o.db, r.local) {
			o.externalScans, o.external = r.s.shares.Consumer().IsExternal(o.schema, o.name)
		}
		k := strings.ToLower(o.name)
		r.objects[k] = append(r.objects[k], o)
	}
}

// positions is the search path for a name in the body of view, or at the
// top level when view is nil: temporary objects first, then the view's own
// schema, then the session's path and the system catalogs.
func (r *resolver) positions(view *object) []pathEntry {
	ps := []pathEntry{{"temp", "main"}}
	if view != nil {
		ps = append(ps, pathEntry{view.db, view.schema})
		if !strings.EqualFold(view.schema, "main") {
			ps = append(ps, pathEntry{view.db, "main"})
		}
	}
	ps = append(ps, r.path.entries...)
	return append(ps, pathEntry{r.path.defaultDB, "main"}, pathEntry{"system", "main"}, pathEntry{"system", "pg_catalog"})
}

// resolve works out what ref, written in the body of view (or at the top
// level when view is nil), may mean.
func (r *resolver) resolve(ref tableRef, view *object) (resolution, error) {
	parts := ref.parts
	if err := r.load([]string{parts[len(parts)-1]}); err != nil {
		return resolution{}, err
	}
	objs := r.objects[strings.ToLower(parts[len(parts)-1])]
	at := func(db, schema string) []object {
		var out []object
		for _, o := range objs {
			if strings.EqualFold(o.db, db) && strings.EqualFold(o.schema, schema) {
				out = append(out, o)
			}
		}
		return out
	}
	var res resolution
	firstOf := func(positions []pathEntry) {
		for _, p := range positions {
			if m := at(p.db, p.schema); len(m) > 0 {
				res.first = m
				return
			}
		}
	}

	switch len(parts) {
	case 3:
		res.first = at(parts[0], parts[1])
		res.all = res.first
	case 2:
		a := parts[0]
		var positions []pathEntry
		if r.databases[strings.ToLower(a)] {
			// db.name: the database's schemas on the path, else any.
			for _, p := range r.positions(view) {
				if strings.EqualFold(p.db, a) {
					positions = append(positions, p)
				}
			}
			for _, o := range objs {
				if strings.EqualFold(o.db, a) {
					res.all = append(res.all, o)
				}
			}
			if len(positions) == 0 {
				res.first = res.all
			}
		}
		// schema.name: that schema in the databases on the path, else in
		// the default database or the system catalog.
		var schemaPositions []pathEntry
		for _, p := range r.positions(view) {
			if strings.EqualFold(p.schema, a) {
				schemaPositions = append(schemaPositions, p)
			}
		}
		if len(schemaPositions) == 0 {
			schemaPositions = []pathEntry{{r.path.defaultDB, a}, {"system", a}}
		}
		for _, o := range objs {
			if strings.EqualFold(o.schema, a) && !slices.Contains(res.all, o) {
				res.all = append(res.all, o)
			}
		}
		if res.first == nil {
			firstOf(append(positions, schemaPositions...))
		}
	case 1:
		positions := r.positions(view)
		firstOf(positions)
		for _, p := range positions {
			for _, o := range at(p.db, p.schema) {
				if !slices.Contains(res.all, o) {
					res.all = append(res.all, o)
				}
			}
		}
	}
	return res, nil
}

// readSet is what a query or view reads from the catalog.
type readSet struct {
	// scans is how many catalog table scans DuckDB plans for it.
	scans  int
	tables []auth.Object
	// policyTables are the tables that policies read (see
	// resolver.policyMarker).
	policyTables []auth.Object
}

func (rs *readSet) add(o auth.Object) {
	if !slices.ContainsFunc(rs.tables, func(t auth.Object) bool { return t.Equal(o) }) {
		rs.tables = append(rs.tables, o)
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
			if !allowedFunctions[strings.ToLower(f.name)] || f.schema != "" && !strings.EqualFold(f.schema, "main") {
				return rs, denied("%s reads data that grants do not cover, which needs the reader role", f.name)
			}
		}
		if err := r.checkMacros(append(w.functions, w.tableFunctions...)); err != nil {
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
			if r.lenient {
				continue
			}
			return rs, denied("%s is not a table or view; reading files directly needs the reader role", strings.Join(ref.parts, "."))
		}
		scans := -1
		for _, o := range res.first {
			reads, err := r.reads(o, depth)
			if err != nil {
				return rs, err
			}
			if scans >= 0 && reads.scans != scans {
				return rs, denied("%s is ambiguous", strings.Join(ref.parts, "."))
			}
			scans = reads.scans
		}
		rs.scans += max(scans, 0)
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
			for _, t := range reads.tables {
				rs.add(t)
			}
		}
	}
	return rs, nil
}

// reads returns what reading o reads from the catalog: a catalog table
// itself, a view what its query reads, and local, temporary and system
// objects nothing.
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
		// Its Parquet scans are counted against the plan's (planScans).
		rs = readSet{scans: o.externalScans, tables: []auth.Object{{Schema: o.schema, Table: o.name}}}
	case o.view:
		if !r.ownDatabase(o.db) {
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
		rs = readSet{scans: 1, tables: []auth.Object{{Schema: o.schema, Table: o.name}}}
	case strings.EqualFold(o.db, "temp") || strings.EqualFold(o.db, r.local) || strings.EqualFold(o.db, "system"):
	default:
		return rs, denied("reading tables of database %s needs the reader role", o.db)
	}
	r.readsOf[o.key()] = rs
	return rs, nil
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

// fnRef is a function a query calls.
type fnRef struct{ schema, name string }

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
				w.tableFunctions = append(w.tableFunctions, fnRef{schema: str(f["schema"]), name: str(f["function_name"])})
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
