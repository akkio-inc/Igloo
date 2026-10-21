package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/federation"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Reads from connections to other databases (package federation), which
// DuckDB attaches as catalogs of their own, such as crm.public.customers.
//
// Unlike the Iceberg catalog's tables, which the built-in roles reader and
// writer may read, a connection's tables need a grant for everyone but
// admins: on the table, its schema or the whole connection. So whenever
// connections are attached, every statement of a non-admin is checked for
// them, whatever its role: DuckDB's plan of the statement shows every scan
// of an attached database, as POSTGRES_SCAN or MYSQL_SCAN with the bare
// table name; DuckDB's parse of it, resolved and with views expanded as
// for the none role (see resolve.go), says which catalog, schema and table
// each name means. The two must agree scan for scan, by type and table
// name, or the statement is refused; every table a name could mean must be
// granted. A statement whose plan scans no attached database reads none,
// and is left to the checks of its role.
//
// Users with the none role get this from readTables, alongside their
// Iceberg tables. Admins' statements are not refused, but their reads are
// worked out the same way, as far as possible, for the query history.

// connectionScanNodes are DuckDB's names, lower-cased, for scans of the tables
// of attached databases, by the database type (duckdb_databases().type)
// they read. A plan node that names a table and is none of these, nor a
// scan of DuckDB's own tables or of Iceberg, is refused.
var connectionScanNodes = map[string]string{"postgres_scan": "postgres", "mysql_scan": "mysql"}

// connectionScan is a scan of a table of an attached database: its database
// type and its table's name, lower-cased.
type connectionScan struct{ kind, table string }

// icebergKind is the kind of the scans of tables of connections to Iceberg
// REST catalogs. DuckDB's plan shows them as ICEBERG_SCAN, as it shows the
// catalog's own tables and iceberg_scan calls, without the table's name:
// those are counted together (see splitScans), and grants decide which
// tables the names in the statement may mean.
const icebergKind = "iceberg"

// splitScans returns the scans of connections' tables that the plan names,
// and how many of Iceberg REST catalogs' tables there are besides.
func splitScans(scans []connectionScan) (named []connectionScan, iceberg int) {
	for _, s := range scans {
		if s.kind == icebergKind {
			iceberg++
		} else {
			named = append(named, s)
		}
	}
	return named, iceberg
}

// icebergConnections reports whether a connection to an Iceberg REST
// catalog is attached.
func (s *Server) icebergConnections() bool {
	for _, t := range s.connectionCatalogs() {
		if t == icebergKind {
			return true
		}
	}
	return false
}

// tableArgCalls checks the table functions that take a table, such as
// iceberg_metadata(crm.public.t), for a user whose role reads the catalog:
// those that name a table of a connection are refused, since they would
// read it without its grants being checked, and iceberg_scan calls are
// counted, as their scans look like those of tables (see readSet.lake).
func (r *resolver) tableArgCalls(calls []fnRef, view *object, rs *readSet) error {
	for _, f := range calls {
		name := strings.ToLower(f.name)
		if !tableArgFunctions[name] || f.schema != "" && !strings.EqualFold(f.schema, "main") {
			continue
		}
		if name == "iceberg_scan" {
			rs.icebergFns++
		}
		if len(f.args) == 0 {
			continue
		}
		arg, _ := f.args[0].(map[string]any)
		if str(arg["class"]) != "COLUMN_REF" {
			// A file, which the checks of files confine.
			continue
		}
		var parts []string
		for _, p := range asList(arg["column_names"]) {
			parts = append(parts, str(p))
		}
		if len(parts) == 0 {
			continue
		}
		if len(parts) == 1 && r.isConnection(parts[0]) {
			return denied("%s on connection %s needs the admin role", f.name, parts[0])
		}
		res, err := r.resolve(tableRef{parts: parts}, view)
		if err != nil {
			return err
		}
		for _, o := range res.all {
			if r.isConnection(o.db) {
				return denied("%s on %s, a table of connection %s, needs the admin role; query the table instead", f.name,
					strings.Join(parts, "."), strings.ToLower(o.db))
			}
		}
	}
	return nil
}

// sameScans reports whether two lists hold the same scans, in any order.
func sameScans(a, b []connectionScan) bool {
	key := func(s connectionScan) string { return s.kind + "\x00" + s.table }
	ka, kb := make([]string, len(a)), make([]string, len(b))
	for i, s := range a {
		ka[i] = key(s)
	}
	for i, s := range b {
		kb[i] = key(s)
	}
	slices.Sort(ka)
	slices.Sort(kb)
	return slices.Equal(ka, kb)
}

// connectionSource is what checks of reads know of connections: those
// attached, by lower-cased name, and a generation that changes whenever
// one is attached, changed or dropped. *federation.Manager is one.
type connectionSource interface {
	Catalogs() map[string]federation.Type
	Generation() uint64
}

// connectionCatalogs returns the names, lower-cased, and types of the
// attached databases that are not DuckDB's own or the Iceberg catalog: the
// connections. It is empty without any.
func (s *Server) connectionCatalogs() map[string]string {
	if s.remotes == nil {
		return nil
	}
	out := map[string]string{}
	for name, t := range s.remotes.Catalogs() {
		out[name] = string(t)
	}
	return out
}

// isConnectionType reports whether a database type from duckdb_databases()
// is another database's rather than DuckDB's own or Iceberg's.
func isConnectionType(t string) bool {
	return t != "" && !strings.EqualFold(t, "duckdb") && !strings.EqualFold(t, "iceberg")
}

// authorizeConnectionReads checks what a statement reads from connections, for
// every user but admins, unless readTables already did. It checks the
// statement as it runs, st.query, which policies may have rewritten. acc is
// what the statement's other checks found, nil when its role allowed it.
//
// For every user but admins it returns an access, acc with what it found
// added or a new one, even when no connection is attached: the access
// holds the catalog's generation, which every change to connections
// changes too (see catalogGeneration), so the statement is checked again
// before it runs once a connection is attached, changed or dropped, or a
// view it reads is replaced, whatever its role. For admins it returns acc,
// with their reads added for the history. On an error the caller keeps
// what it had.
func (s *Server) authorizeConnectionReads(ctx context.Context, sess *session, user auth.User, st *statement, acc *access) (*access, error) {
	admin := user.Role == auth.RoleAdmin
	if len(s.connectionCatalogs()) == 0 {
		// Nothing to read yet.
		if admin || acc != nil {
			return acc, nil
		}
		// Grants only tell when the decision is stale; without them it is
		// stale at once, and cheap to make again.
		privs, _ := s.privileges(ctx, sess, user)
		return &access{checked: time.Now(), privs: privs, catalogGen: s.catalogGeneration(), settingGen: sess.settingGen}, nil
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_DETACH {
		// Even admins drop connections with DROP CONNECTION: a detached
		// connection cannot be attached again until Igloo restarts.
		names := append(sqllex.CodeWords(st.query), sqllex.QuotedIdentifiers(st.query)...)
		for _, q := range names {
			if _, ok := s.connectionCatalogs()[strings.ToLower(q)]; ok {
				return nil, denied("%s is a connection; use DROP CONNECTION", strings.ToLower(q))
			}
		}
	}
	if acc != nil && acc.connectionsChecked {
		return acc, nil
	}
	if admin && viewQuery(st.query) != "" {
		// Creating a view reads nothing; nor is it for the history.
		return acc, nil
	}
	planText, parseText, err := connectionTexts(st.stmtType, st.query, st.prepared)
	var objects []auth.Object
	if err == nil && planText != "" {
		objects, err = s.connectionReads(ctx, sess, planText, parseText, st.policyMarker())
	}
	if admin && (err != nil || len(objects) == 0) {
		// Admins may read anything; what they read is only for the
		// history.
		return acc, nil
	}
	if err != nil {
		return nil, err
	}
	var out access
	if acc != nil {
		out = *acc
	} else {
		out = access{checked: time.Now(), catalogGen: s.catalogGeneration(), settingGen: sess.settingGen}
	}
	out.connectionsChecked, out.ranStatements = true, out.ranStatements || planText != ""
	out.read = append(slices.Clone(out.read), objects...)
	if !admin {
		privs, err := s.privileges(ctx, sess, user)
		if err != nil {
			return nil, denied("could not load grants: %v", err)
		}
		if out.privs == nil {
			out.privs = privs
		}
		for _, o := range objects {
			n := need{auth.PrivilegeSelect, o}
			if !allows(user, privs, n) {
				return nil, needError(user, n)
			}
		}
	}
	return &out, nil
}

// authorizeViewReads checks what the query of a view that user creates
// reads from connections: like the statements that read it later, the
// view may read only connection tables user may read. Views read with the
// grants of whoever reads them; this keeps users from leaving views that
// read what they themselves may not.
func (s *Server) authorizeViewReads(ctx context.Context, sess *session, user auth.User, query string) error {
	if user.Role == auth.RoleAdmin || len(s.connectionCatalogs()) == 0 {
		return nil
	}
	objects, err := s.connectionReads(ctx, sess, query, query, "")
	if err != nil || len(objects) == 0 {
		return err
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		return denied("could not load grants: %v", err)
	}
	for _, o := range objects {
		if n := (need{auth.PrivilegeSelect, o}); !allows(user, privs, n) {
			return needError(user, n)
		}
	}
	return nil
}

// connectionReads returns every object of a connection that reading
// planText, which DuckDB plans, and parseText, its query as written, may
// read, or an error when that cannot be worked out. policyMarker marks the
// tables that policies read in a statement they rewrote.
func (s *Server) connectionReads(ctx context.Context, sess *session, planText, parseText, policyMarker string) ([]auth.Object, error) {
	plan, err := s.explain(ctx, sess, planText)
	if err != nil {
		return nil, denied("cannot analyze which tables of connections this statement reads: %v", err)
	}
	scans, err := s.scansOf(ctx, sess, plan, false)
	if err != nil {
		return nil, err
	}
	// With connections to Iceberg REST catalogs, a scan of Iceberg may be
	// one of theirs, and a table function may name one of their tables.
	iceberg := s.icebergConnections()
	if len(scans.connections) == 0 && (!iceberg || scans.iceberg == 0 && scans.tableFns == 0 && !mentionsTableArgFunction(parseText)) {
		return nil, nil
	}
	if parseText == "" {
		return nil, denied("cannot tell which tables of connections this statement reads")
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	r.connectionsOnly, r.policyMarker = true, policyMarker
	reads, err := r.analyze(parseText, nil, 0)
	if err != nil {
		return nil, err
	}
	named, ice := splitScans(reads.connScans)
	if !sameScans(named, scans.connections) {
		return nil, denied("cannot verify which tables of connections this statement reads (the plan scans %d, the statement names %d)",
			len(scans.connections), len(named))
	}
	if iceberg && reads.lake+reads.icebergFns+ice != scans.iceberg+len(scans.named) {
		return nil, denied("cannot verify which tables of connections this statement reads (the plan scans %d Iceberg tables, the statement names %d)",
			scans.iceberg+len(scans.named), reads.lake+reads.icebergFns+ice)
	}
	return reads.connObjects, nil
}

// guardTableArgs refuses, before DuckDB prepares a statement of a user who
// is not an admin, a call of a table function that takes a table
// (iceberg_scan, iceberg_metadata, iceberg_snapshots) on a table of a
// connection: DuckDB reads the table's metadata, through the connection,
// while it prepares the call. The statement's checks after it is prepared
// find such calls in the views it reads too (see tableArgCalls).
func (s *Server) guardTableArgs(ctx context.Context, sess *session, user auth.User, query string) error {
	if user.Role == auth.RoleAdmin || len(s.connectionCatalogs()) == 0 || !mentionsTableArgFunction(query) {
		return nil
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return err
	}
	ast, err := r.serialize(query)
	if err != nil {
		// Not a query DuckDB serializes; its checks after preparing apply.
		return nil
	}
	w := &astWalk{}
	w.walk(ast, nil)
	var rs readSet
	return r.tableArgCalls(w.tableFunctions, nil, &rs)
}

// mentionsTableArgFunction reports whether text may call a table function
// that takes a table (see tableArgFunctions).
func mentionsTableArgFunction(text string) bool {
	for _, w := range sqllex.CodeWords(text) {
		if tableArgFunctions[strings.ToLower(w)] {
			return true
		}
	}
	return false
}

// connectionTexts decides what to EXPLAIN for a statement's reads and what to
// parse for the tables it names, for users of any role. An empty planText
// means the statement reads no table.
func connectionTexts(t duckdb.StmtType, query, prepared string) (planText, parseText string, err error) {
	switch t {
	case duckdb.STATEMENT_TYPE_TRANSACTION, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER,
		duckdb.STATEMENT_TYPE_PRAGMA, duckdb.STATEMENT_TYPE_ATTACH, duckdb.STATEMENT_TYPE_DETACH,
		duckdb.STATEMENT_TYPE_CREATE_FUNC, duckdb.STATEMENT_TYPE_PREPARE, duckdb.STATEMENT_TYPE_EXTENSION,
		duckdb.STATEMENT_TYPE_LOAD, duckdb.STATEMENT_TYPE_VACUUM, duckdb.STATEMENT_TYPE_ANALYZE:
		// None reads a table's rows; those that only admins may run are
		// checked as admins' anyway.
		return "", "", nil
	case duckdb.STATEMENT_TYPE_EXPLAIN:
		inner, iPrepared := explainedStatement(query), explainedStatement(prepared)
		if inner == "" {
			return "", "", denied("cannot analyze this EXPLAIN")
		}
		return iPrepared, inner, nil
	case duckdb.STATEMENT_TYPE_COPY:
		// COPY ... TO reads its table or query, planned and parsed on its
		// own; COPY ... FROM reads a file into a table, no table's rows.
		if src, ok := copySource(query); ok {
			return src, src, nil
		}
		if isCopyFrom(query) {
			return "", "", nil
		}
		return "", "", denied("cannot analyze this COPY; copy a query, as in COPY (SELECT ...) TO ...")
	case duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET:
		sel := setVariableAsSelect(query)
		if sel == "" {
			return "", "", nil
		}
		return prepared, sel, nil
	case duckdb.STATEMENT_TYPE_CREATE:
		if isCreateView(query) {
			// The view's query, as reading it would.
			q := viewQuery(query)
			if q == "" {
				return "", "", denied("cannot analyze this CREATE VIEW")
			}
			return q, q, nil
		}
	}
	return readTexts(t, query, prepared)
}

// isCreateView reports whether query is CREATE [OR REPLACE] [TEMP] VIEW.
func isCreateView(query string) bool {
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return false
	}
	sc.keywords("OR", "REPLACE")
	_ = sc.keywords("TEMP") || sc.keywords("TEMPORARY")
	return sc.keywords("VIEW")
}

// viewQuery returns the query of CREATE [OR REPLACE] [TEMP] VIEW ... AS
// query, or "".
func viewQuery(query string) string {
	if !isCreateView(query) {
		return ""
	}
	i := topLevelKeyword(query, "AS")
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query[i+2:]), ";"))
}

// copySource returns the query a COPY ... TO reads: the table it copies, as
// SELECT * FROM name, or the query in parentheses. ok is false for COPY ...
// FROM and for shapes it does not know.
func copySource(query string) (string, bool) {
	sc := &scanner{s: query}
	if !sc.keywords("COPY") {
		return "", false
	}
	sc.skip()
	var src string
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return "", false
		}
		src = strings.TrimSpace(sc.s[sc.i+1 : end])
		sc.i = end + 1
	} else {
		name, ok := sc.name()
		if !ok {
			return "", false
		}
		src = "SELECT * FROM " + quoteName(name)
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
			// A column list: the columns of the same table.
			end := matchParen(sc.s, sc.i)
			if end < 0 {
				return "", false
			}
			sc.i = end + 1
		}
	}
	if !sc.keywords("TO") {
		return "", false
	}
	// Only a file name and options may follow; the file name is a string
	// literal and options are constants, neither of which reads a table.
	if lit, ok := sc.literal(); !ok || lit == nil {
		return "", false
	}
	rest := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";"))
	if rest != "" && (!strings.HasPrefix(rest, "(") || matchParen(rest, 0) != len(rest)-1 || strings.Contains(strings.ToUpper(sqllex.CodeOnly(rest)), "SELECT")) {
		return "", false
	}
	return src, true
}

// isCopyFrom reports whether query is COPY name ... FROM, which reads a
// file into a table.
func isCopyFrom(query string) bool {
	words := sqllex.CodeWords(query)
	return len(words) > 0 && words[0] == "COPY" && slices.Contains(words, "FROM") && !slices.Contains(words, "TO")
}

// pivotReachesConnection decides, before it is prepared, whether a PIVOT
// that takes its columns from the data may read a connection. DuckDB reads
// the columns while preparing, before any plan can be checked, so this is
// decided from the statement's words alone, erring towards yes: any name
// in it that is a connection, a table of one, a view that may read one or
// a macro could. Such a PIVOT is refused; listing its columns works.
func (s *Server) pivotReachesConnection(ctx context.Context, sess *session, query string) error {
	ext := s.connectionCatalogs()
	if len(ext) == 0 {
		return nil
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return err
	}
	refused := denied("a PIVOT that takes its columns from data that may come from a connection runs before Igloo can check it; " +
		"list the columns instead, as in PIVOT ... ON col IN ('a', 'b')")
	seen := map[string]bool{}
	var check func(text string, depth int) error
	check = func(text string, depth int) error {
		if depth > maxViewDepth {
			return refused
		}
		var names []string
		for _, w := range sqllex.CodeWords(text) {
			names = append(names, strings.ToLower(w))
		}
		for _, q := range sqllex.QuotedIdentifiers(text) {
			names = append(names, strings.ToLower(q))
		}
		var fresh []string
		for _, n := range names {
			if _, ok := ext[n]; ok {
				return refused
			}
			if !seen[n] {
				seen[n] = true
				fresh = append(fresh, n)
			}
		}
		if err := r.load(fresh); err != nil {
			return err
		}
		if err := r.checkMacros(fnRefs(fresh)); err != nil {
			return refused
		}
		for _, n := range fresh {
			for _, o := range r.objects[n] {
				switch {
				case r.isConnection(o.db):
					return refused
				case o.view && !o.internal:
					if err := check(viewBody(o.sql), depth+1); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return check(query, 0)
}

func fnRefs(names []string) []fnRef {
	out := make([]fnRef, len(names))
	for i, n := range names {
		out[i] = fnRef{name: n}
	}
	return out
}

// databaseTypes returns every attached database's type, by lower-cased
// name.
func databaseTypes(ctx context.Context, sess *session) (map[string]string, error) {
	rows, err := sess.conn.QueryContext(ctx, "SELECT system.main.lower(database_name), type FROM system.main.duckdb_databases()", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	out := map[string]string{}
	dest := make([]driver.Value, 2)
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		out[name] = typ
	}
}
