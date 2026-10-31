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
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Grants let a user run a statement their built-in role does not allow,
// when every access the statement makes to the Iceberg catalog is granted.
//
// What a statement writes comes from its text: the target of INSERT,
// UPDATE, DELETE, MERGE and COPY, and the table or schema of DDL. What it
// reads matters only for users with the built-in role none, since every
// other role reads the whole warehouse. Reads come from DuckDB itself:
// EXPLAIN shows every scan the statement makes, views expanded, so nothing
// is read that the plan does not show. Iceberg scans in DuckDB's plans do
// not name their table, though, so the tables come from DuckDB's own parse
// of the statement (json_serialize_sql), with names resolved the way DuckDB
// resolves them and views expanded the same way; the plan must then scan
// exactly as many catalog tables as the parse found. Anything Igloo cannot
// account for in this way is refused.

// access is what a statement allowed by grants touches, and what any
// non-admin's statement reads from connections (see connreads.go).
type access struct {
	read    []auth.Object
	written []auth.Object
	// connectionsChecked is set once reads from connections were checked.
	connectionsChecked bool
	// ranStatements says the checks ran statements on the session's
	// connection, after which the statement is prepared again.
	ranStatements bool
	// checked is when the decision was made, with which privileges, and
	// with the catalog and session settings at which generations. DuckDB
	// binds a prepared statement again when the catalog changes, with the
	// session's search path at that time, so a change to either makes the
	// decision stale.
	checked    time.Time
	privs      *auth.Privileges
	catalogGen uint64
	settingGen uint64
}

// catalogGeneration changes whenever a change to the catalog may change
// what a statement binds to: DDL run on this replica, and changes to the
// local copies of views.
func (s *Server) catalogGeneration() uint64 {
	g := s.ddlGen.Load()
	if s.ddl != nil {
		g += s.ddl.Generation()
	}
	if s.remotes != nil {
		g += s.remotes.Generation()
	}
	return g
}

// changesCatalog reports whether a statement of type t may change what
// statements bind to.
func changesCatalog(t duckdb.StmtType) bool {
	switch t {
	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER,
		duckdb.STATEMENT_TYPE_CREATE_FUNC, duckdb.STATEMENT_TYPE_ATTACH, duckdb.STATEMENT_TYPE_DETACH:
		return true
	}
	return false
}

// need is a privilege a statement needs on an object.
type need struct {
	priv auth.Privilege
	obj  auth.Object
}

// errNoGrantPath means grants cannot allow a statement; the built-in role's
// own refusal stands.
var errNoGrantPath = errors.New("not grantable")

// authorizeByGrants decides a statement that the user's built-in role does
// not allow, by their grants. roleErr is the built-in role's refusal, which
// is returned unchanged whenever grants cannot apply: for statements that
// need admin, and for readers and writers who have no grants at all.
// query is the statement as prepared.
func (s *Server) authorizeByGrants(ctx context.Context, sess *session, user auth.User, st *statement, query string, roleErr error) (*access, error) {
	if s.catalogName() == "" || !grantable(st.stmtType, query) {
		return nil, roleErr
	}
	privs, err := s.privileges(ctx, sess, user)
	if err != nil {
		s.log.Warn("could not load grants", "user", user.Name, "err", err)
		if user.Role != auth.RoleNone {
			return nil, roleErr
		}
		return nil, denied("could not load grants: %v", err)
	}
	if user.Role != auth.RoleNone && privs.Empty() {
		return nil, roleErr
	}

	acc := &access{checked: time.Now(), privs: privs, catalogGen: s.catalogGeneration(), settingGen: sess.settingGen, ranStatements: true}
	needs, written, err := s.writeNeeds(ctx, sess, st.stmtType, query)
	if errors.Is(err, errNoGrantPath) {
		return nil, roleErr
	}
	if err != nil {
		return nil, err
	}
	acc.written = written
	if rank[user.Role] < rank[auth.RoleReader] {
		// What the statement reads is what runs: the statement as
		// policies rewrote it.
		read, err := s.readTables(ctx, sess, st, st.query)
		if err != nil {
			return nil, err
		}
		acc.read, acc.connectionsChecked = read, true
		for _, o := range read {
			needs = append(needs, need{auth.PrivilegeSelect, o})
		}
	}
	for _, n := range needs {
		if !allows(user, privs, n) {
			return nil, needError(user, n)
		}
	}
	return acc, nil
}

// allows reports whether user may do n, by their built-in role or grants.
// The built-in roles cover the Iceberg catalog only: a connection's tables
// need grants.
func allows(user auth.User, privs *auth.Privileges, n need) bool {
	switch {
	case n.obj.Catalog != "":
		return n.priv == auth.PrivilegeSelect && privs.AllowsConnection(n.priv, n.obj.Catalog, n.obj.Schema, n.obj.Table)
	case n.priv == auth.PrivilegeSelect && rank[user.Role] >= rank[auth.RoleReader]:
		return true
	case n.obj.IsSchema():
		return privs.AllowsSchema(n.priv, n.obj.Schema)
	}
	return privs.Allows(n.priv, n.obj.Schema, n.obj.Table)
}

// needError refuses a statement that needs n, naming the object and the
// privilege.
func needError(user auth.User, n need) error {
	var msg string
	switch {
	case n.obj.Catalog != "":
		msg = fmt.Sprintf("SELECT on table %s needs a SELECT grant on it, its schema or connection %s", n.obj, n.obj.Catalog)
		return pgerror.WithCode(fmt.Errorf("permission denied for %s: %s", user.Name, msg), codes.InsufficientPrivilege)
	}
	switch n.priv {
	case auth.PrivilegeCreate:
		msg = fmt.Sprintf("creating, changing or dropping tables in schema %s needs the writer role or CREATE on the schema", n.obj.Schema)
	case auth.PrivilegeSelect:
		msg = fmt.Sprintf("SELECT on table %s needs the reader role or a SELECT grant", n.obj)
	case auth.PrivilegeInsert, auth.PrivilegeUpdate:
		msg = fmt.Sprintf("%s on table %s needs the writer role or an %[1]s grant", n.priv, n.obj)
	default:
		msg = fmt.Sprintf("%s on table %s needs the writer role or a %[1]s grant", n.priv, n.obj)
	}
	return pgerror.WithCode(fmt.Errorf("permission denied for %s: %s", user.Name, msg), codes.InsufficientPrivilege)
}

// denied refuses a statement that grants cannot be checked for.
func denied(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf("permission denied: "+format, args...), codes.InsufficientPrivilege)
}

// catalogName is the Iceberg catalog's name in SQL, the one grants apply
// to, or "" without one.
func (s *Server) catalogName() string {
	if s.catalog != "" {
		return s.catalog
	}
	if c, ok := s.engine.(interface{ Catalog() string }); ok {
		return c.Catalog()
	}
	return ""
}

// privileges returns the session's grants, reloading them once they are as
// old as the role refresh interval. If the store fails, the last ones
// loaded stay in force, as the role does, for up to maxStale (see
// refreshAccess).
func (s *Server) privileges(ctx context.Context, sess *session, user auth.User) (*auth.Privileges, error) {
	sess.mu.Lock()
	snap, fixed := sess.privs, sess.fixed
	sess.mu.Unlock()
	if fixed != nil {
		return fixed, nil
	}
	if snap.privs != nil && time.Since(snap.loaded) < s.roleRefresh {
		return snap.privs, nil
	}
	p, err := s.grants.UserPrivileges(ctx, user.Name)
	if err != nil {
		if snap.privs != nil {
			s.log.Warn("could not refresh grants", "user", user.Name, "err", err)
			if age := time.Since(snap.loaded); age >= s.maxStale {
				return nil, staleAccess("the session's grants", age)
			}
			return snap.privs, nil
		}
		return nil, err
	}
	sess.mu.Lock()
	sess.privs = privilegeSnapshot{privs: &p, loaded: time.Now()}
	sess.mu.Unlock()
	return &p, nil
}

// --- writes ---------------------------------------------------------------

// writeNeeds works out what a statement writes in the catalog and which
// privileges that takes. errNoGrantPath means grants cannot allow it, such
// as for writes to files or temporary tables.
func (s *Server) writeNeeds(ctx context.Context, sess *session, t duckdb.StmtType, query string) ([]need, []auth.Object, error) {
	var (
		name  []string
		privs []auth.Privilege
	)
	sc := &scanner{s: query}
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION, duckdb.STATEMENT_TYPE_EXPLAIN,
		duckdb.STATEMENT_TYPE_TRANSACTION, duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET,
		duckdb.STATEMENT_TYPE_PRAGMA, duckdb.STATEMENT_TYPE_CALL:
		return nil, nil, nil

	case duckdb.STATEMENT_TYPE_INSERT:
		privs = []auth.Privilege{auth.PrivilegeInsert}
		if !sc.keywords("INSERT") {
			return nil, nil, errNoGrantPath
		}
		if sc.keywords("OR", "REPLACE") || sc.keywords("OR", "IGNORE") {
			privs = append(privs, auth.PrivilegeUpdate)
		}
		if !sc.keywords("INTO") {
			return nil, nil, errNoGrantPath
		}
		name, _ = sc.name()

	case duckdb.STATEMENT_TYPE_UPDATE:
		privs = []auth.Privilege{auth.PrivilegeUpdate}
		if sc.keywords("UPDATE") {
			name, _ = sc.name()
		}

	case duckdb.STATEMENT_TYPE_DELETE:
		privs = []auth.Privilege{auth.PrivilegeDelete}
		switch {
		case sc.keywords("DELETE", "FROM"), sc.keywords("TRUNCATE", "TABLE"), sc.keywords("TRUNCATE"):
			name, _ = sc.name()
		}

	case statementTypeMergeInto:
		if sc.keywords("MERGE", "INTO") {
			name, _ = sc.name()
		}
		privs = mergeActions(query)

	case duckdb.STATEMENT_TYPE_COPY:
		// COPY table FROM a file inserts; every other COPY writes files.
		if !sc.keywords("COPY") {
			return nil, nil, errNoGrantPath
		}
		if name, _ = sc.name(); name == nil {
			return nil, nil, errNoGrantPath
		}
		if !slices.Contains(sqllex.CodeWords(sc.rest()), "FROM") || slices.Contains(sqllex.CodeWords(sc.rest()), "TO") {
			return nil, nil, errNoGrantPath
		}
		privs = []auth.Privilege{auth.PrivilegeInsert}

	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER:
		return s.ddlNeeds(ctx, sess, t, query)

	default:
		return nil, nil, errNoGrantPath
	}
	if name == nil {
		return nil, nil, errNoGrantPath
	}

	targets, err := s.existingTargets(ctx, sess, name)
	if err != nil {
		return nil, nil, err
	}
	var needs []need
	for _, o := range targets {
		for _, p := range privs {
			needs = append(needs, need{p, o})
		}
	}
	return needs, targets, nil
}

// mergeActions lists the privileges a MERGE needs on its target, from the
// actions of its WHEN clauses. It errs toward needing more.
func mergeActions(query string) []auth.Privilege {
	words := sqllex.CodeWords(query)
	var privs []auth.Privilege
	for i := 0; i+1 < len(words); i++ {
		if words[i] != "THEN" {
			continue
		}
		var p auth.Privilege
		switch words[i+1] {
		case "INSERT":
			p = auth.PrivilegeInsert
		case "UPDATE":
			p = auth.PrivilegeUpdate
		case "DELETE":
			p = auth.PrivilegeDelete
		default:
			continue
		}
		if !slices.Contains(privs, p) {
			privs = append(privs, p)
		}
	}
	if len(privs) == 0 {
		return []auth.Privilege{auth.PrivilegeInsert, auth.PrivilegeUpdate, auth.PrivilegeDelete}
	}
	return privs
}

// ddlNeeds handles CREATE TABLE, DROP TABLE and ALTER TABLE: each needs
// CREATE on the schema of the table. Other DDL needs the built-in role.
func (s *Server) ddlNeeds(ctx context.Context, sess *session, t duckdb.StmtType, query string) ([]need, []auth.Object, error) {
	var schemas []auth.Object
	var written []auth.Object
	switch t {
	case duckdb.STATEMENT_TYPE_CREATE:
		c, ok := parseCreateTable(query)
		if !ok || c.temporary {
			return nil, nil, errNoGrantPath
		}
		o, err := s.newTableTarget(ctx, sess, c.name)
		if err != nil {
			return nil, nil, err
		}
		schemas, written = append(schemas, auth.Object{Schema: o.Schema}), append(written, o)
		if c.replace {
			// The table it replaces may resolve elsewhere on the path.
			existing, err := s.catalogCandidates(ctx, sess, c.name, true)
			if err != nil {
				return nil, nil, err
			}
			for _, e := range existing {
				schemas = append(schemas, auth.Object{Schema: e.Schema})
			}
		}

	case duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER:
		sc := &scanner{s: query}
		alter := false
		switch {
		case sc.keywords("DROP", "TABLE"), sc.keywords("DROP", "VIEW"):
			sc.keywords("IF", "EXISTS")
		case sc.keywords("ALTER", "TABLE"):
			alter = true
		default:
			return nil, nil, errNoGrantPath
		}
		var (
			name []string
			ok   bool
		)
		if alter {
			// ALTER TABLE may carry IF EXISTS, ONLY and a trailing '*'.
			name, ok = sc.alterTarget()
		} else {
			name, ok = sc.name()
		}
		if !ok {
			return nil, nil, errNoGrantPath
		}
		if rest := strings.TrimSpace(sc.rest()); strings.HasPrefix(rest, ",") {
			return nil, nil, errNoGrantPath
		}
		// Moving a table to another schema would need CREATE there too.
		if words := sqllex.CodeWords(sc.rest()); slices.Contains(words, "SCHEMA") {
			return nil, nil, errNoGrantPath
		}
		existing, err := s.catalogCandidates(ctx, sess, name, true)
		if err != nil {
			return nil, nil, err
		}
		if len(existing) == 0 {
			// Nothing in the catalog to change: a missing table, which
			// DuckDB reports, or IF EXISTS.
			o, err := s.newTableTarget(ctx, sess, name)
			if err != nil {
				return nil, nil, err
			}
			existing = []auth.Object{o}
		}
		for _, e := range existing {
			schemas, written = append(schemas, auth.Object{Schema: e.Schema}), append(written, e)
		}
	}
	needs := make([]need, len(schemas))
	for i, o := range schemas {
		needs[i] = need{auth.PrivilegeCreate, o}
	}
	return needs, written, nil
}

// createTable is the start of CREATE [OR REPLACE] [TEMP] TABLE [IF NOT
// EXISTS] name, and the query of CREATE TABLE AS, if any.
type createTable struct {
	replace, temporary, ifNotExists bool
	name                            []string
	query                           string
	// partitionedBy is the PARTITIONED BY (...) clause before AS, if any.
	partitionedBy string
}

func parseCreateTable(query string) (createTable, bool) {
	var c createTable
	sc := &scanner{s: query}
	if !sc.keywords("CREATE") {
		return c, false
	}
	c.replace = sc.keywords("OR", "REPLACE")
	switch {
	case sc.keywords("TEMP"), sc.keywords("TEMPORARY"):
		c.temporary = true
	case sc.keywords("PERSISTENT"):
	}
	if !sc.keywords("TABLE") {
		return c, false
	}
	c.ifNotExists = sc.keywords("IF", "NOT", "EXISTS")
	name, ok := sc.name()
	if !ok {
		return c, false
	}
	c.name = name
	c.partitionedBy = sc.partitionedBy()
	if i := topLevelKeyword(sc.rest(), "AS"); i >= 0 {
		q := strings.TrimSpace(sc.rest()[i+2:])
		q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
		if j := topLevelKeyword(q, "WITH", "NO", "DATA"); j >= 0 {
			q = q[:j]
		} else if j := topLevelKeyword(q, "WITH", "DATA"); j >= 0 {
			q = q[:j]
		}
		c.query = strings.TrimSpace(q)
	}
	return c, true
}

// newTableTarget is the catalog table a CREATE of name makes: in the
// schema named, or the session's current one.
func (s *Server) newTableTarget(ctx context.Context, sess *session, name []string) (auth.Object, error) {
	catalogName := s.catalogName()
	switch len(name) {
	case 3:
		if !strings.EqualFold(name[0], catalogName) {
			return auth.Object{}, errNoGrantPath
		}
		return auth.Object{Schema: name[1], Table: name[2]}, nil
	case 2:
		if isDatabase, err := s.isDatabase(ctx, sess, name[0]); err != nil || isDatabase {
			// db.table means that database's default schema; leave it to
			// the built-in role.
			if err == nil {
				err = errNoGrantPath
			}
			return auth.Object{}, err
		}
		return auth.Object{Schema: name[0], Table: name[1]}, nil
	case 1:
		db, schema, err := currentSchema(ctx, sess.conn)
		if err != nil {
			return auth.Object{}, err
		}
		if !strings.EqualFold(db, catalogName) {
			return auth.Object{}, errNoGrantPath
		}
		return auth.Object{Schema: schema, Table: name[0]}, nil
	}
	return auth.Object{}, errNoGrantPath
}

func (s *Server) isDatabase(ctx context.Context, sess *session, name string) (bool, error) {
	dbs, err := queryStrings(ctx, sess, `SELECT database_name FROM system.main.duckdb_databases()
		WHERE system.main.lower(database_name) = system.main.lower($1)`, name)
	return len(dbs) > 0, err
}

// existingTargets resolves the table a write names: every catalog table or
// view the name could mean along the session's path, or, when there is
// none, the table it would be, which DuckDB then reports missing.
func (s *Server) existingTargets(ctx context.Context, sess *session, name []string) ([]auth.Object, error) {
	targets, err := s.catalogCandidates(ctx, sess, name, true)
	if err != nil || len(targets) > 0 {
		return targets, err
	}
	o, err := s.newTableTarget(ctx, sess, name)
	if err != nil {
		return nil, err
	}
	return []auth.Object{o}, nil
}

// catalogCandidates lists the catalog tables and views name could resolve
// to along the session's path. With strict, a name that could also mean
// something outside the catalog, such as a temporary or local table, gets
// errNoGrantPath, since grants do not cover those; otherwise those are left
// out.
func (s *Server) catalogCandidates(ctx context.Context, sess *session, name []string, strict bool) ([]auth.Object, error) {
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	res, err := r.resolve(tableRef{parts: name}, nil)
	if err != nil {
		return nil, err
	}
	var out []auth.Object
	for _, o := range res.all {
		if !strings.EqualFold(o.db, s.catalogName()) && !o.catalogView {
			if strict {
				return nil, errNoGrantPath
			}
			continue
		}
		out = append(out, auth.Object{Schema: o.schema, Table: o.name})
	}
	return out, nil
}

// --- reads ----------------------------------------------------------------

// readTables returns the catalog tables a statement reads, for a user whose
// built-in role does not allow reading. It refuses statements that read
// files or other locations directly, and ones whose reads it cannot
// account for.
func (s *Server) readTables(ctx context.Context, sess *session, st *statement, query string) ([]auth.Object, error) {
	planText, parseText, err := readTexts(st.stmtType, query, st.prepared)
	if err != nil {
		return nil, err
	}
	if planText == "" {
		return nil, nil
	}
	scans, err := s.planScans(ctx, sess, planText)
	if err != nil {
		return nil, err
	}
	if scans.total() == 0 {
		return nil, nil
	}
	if parseText == "" {
		return nil, denied("cannot tell which tables this statement reads; users with the none role may read granted tables only through statements Igloo can analyze")
	}

	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	r.policyMarker = st.policyMarker()
	reads, err := r.analyze(parseText, nil, 0)
	if err != nil {
		return nil, err
	}
	// The plan shows the scans of Iceberg REST catalogs' tables as it shows
	// the catalog's.
	connScans, iceberg := splitScans(reads.connScans)
	if reads.scans+iceberg != scans.catalogTotal() {
		return nil, denied("cannot verify which tables this statement reads (the plan scans %d catalog tables, the statement names %d)",
			scans.catalogTotal(), reads.scans+iceberg)
	}
	if !sameScans(connScans, scans.connections) {
		return nil, denied("cannot verify which tables of connections this statement reads (the plan scans %d, the statement names %d)",
			len(scans.connections), len(connScans))
	}
	for _, named := range scans.named {
		if !slices.ContainsFunc(slices.Concat(reads.tables, reads.policyTables), func(o auth.Object) bool { return o.Equal(named) }) {
			return nil, denied("cannot verify which tables this statement reads (%s)", named)
		}
	}
	return append(reads.tables, reads.connObjects...), nil
}

// readTexts decides what to EXPLAIN for a statement's reads and what to
// parse for the tables it names. prepared is the text DuckDB prepared,
// which the plan must use so parameters line up. An empty planText means
// the statement reads nothing.
func readTexts(t duckdb.StmtType, query, prepared string) (planText, parseText string, err error) {
	switch t {
	case duckdb.STATEMENT_TYPE_TRANSACTION, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER:
		return "", "", nil
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION:
		return prepared, query, nil
	case duckdb.STATEMENT_TYPE_EXPLAIN:
		inner, iPrepared := explainedStatement(query), explainedStatement(prepared)
		if inner == "" || !selectLike(inner) {
			return "", "", denied("EXPLAIN of anything but a query needs the reader role")
		}
		return iPrepared, inner, nil
	case duckdb.STATEMENT_TYPE_INSERT:
		return prepared, insertSource(query), nil
	case duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE, statementTypeMergeInto:
		return prepared, dmlAsSelect(t, query), nil
	case duckdb.STATEMENT_TYPE_CREATE:
		c, ok := parseCreateTable(query)
		if !ok || c.query == "" {
			return "", "", nil
		}
		return prepared, c.query, nil
	case duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET:
		return prepared, setVariableAsSelect(query), nil
	case duckdb.STATEMENT_TYPE_CALL:
		sc := &scanner{s: query}
		if !sc.keywords("CALL") {
			return "", "", denied("cannot analyze this CALL")
		}
		return prepared, "SELECT * FROM " + sc.rest(), nil
	case duckdb.STATEMENT_TYPE_COPY:
		return "", "", denied("COPY FROM reads files directly, which needs the reader role")
	case duckdb.STATEMENT_TYPE_PRAGMA:
		return "", "", denied("PRAGMA statements need the reader role; query the pragma_ table functions instead, such as SELECT * FROM pragma_table_info('t')")
	}
	return "", "", denied("cannot analyze this statement")
}

// explainedStatement returns the statement EXPLAIN [ANALYZE] [(options)]
// explains, or "".
func explainedStatement(query string) string {
	sc := &scanner{s: query}
	if !sc.keywords("EXPLAIN") {
		return ""
	}
	sc.keywords("ANALYZE")
	sc.keywords("ANALYSE")
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end := matchParen(sc.s, sc.i)
		if end < 0 {
			return ""
		}
		sc.i = end + 1
	}
	return strings.TrimSpace(sc.rest())
}

// selectLike reports whether a statement is a query, by its first word.
func selectLike(query string) bool {
	switch w := (&scanner{s: query}).word(); w {
	case "SELECT", "WITH", "FROM", "VALUES", "TABLE", "DESCRIBE", "SUMMARIZE", "SHOW", "PIVOT", "UNPIVOT":
		return true
	case "":
		return strings.HasPrefix(strings.TrimSpace(stripComments(query)), "(")
	}
	return false
}

// insertSource returns the query an INSERT inserts: the text after the
// target, its columns and BY NAME, up to ON CONFLICT or RETURNING. It is
// empty for DEFAULT VALUES.
func insertSource(query string) string {
	sc := &scanner{s: query}
	if !sc.keywords("INSERT") {
		return ""
	}
	_ = sc.keywords("OR", "REPLACE") || sc.keywords("OR", "IGNORE")
	if !sc.keywords("INTO") {
		return ""
	}
	if _, ok := sc.name(); !ok {
		return ""
	}
	if sc.keywords("AS") {
		sc.name()
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		// A column list, unless it opens the query itself.
		end := matchParen(sc.s, sc.i)
		if end > 0 && !selectLike(sc.s[sc.i+1:end]) {
			sc.i = end + 1
		}
	}
	_ = sc.keywords("BY", "NAME") || sc.keywords("BY", "POSITION")
	if sc.keywords("DEFAULT", "VALUES") {
		return ""
	}
	rest := sc.rest()
	for _, end := range [][]string{{"ON", "CONFLICT"}, {"RETURNING"}} {
		if i := topLevelKeyword(rest, end...); i >= 0 {
			rest = rest[:i]
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), ";"))
}

// dmlAsSelect rewrites UPDATE, DELETE, TRUNCATE and MERGE as a query that
// names the same tables, for parsing only: DuckDB serializes only queries.
// Parts it cannot place are left out, so the plan then scans more than the
// query names and the statement is refused. It returns "" if the statement
// does not have the expected shape.
func dmlAsSelect(t duckdb.StmtType, query string) string {
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))
	sc := &scanner{s: body}
	cut := func(s string, words ...string) (before, after string, ok bool) {
		i := topLevelKeyword(s, words...)
		if i < 0 {
			return strings.TrimSpace(s), "", false
		}
		k := &scanner{s: s, i: i}
		k.keywords(words...)
		return strings.TrimSpace(s[:i]), strings.TrimSpace(s[k.i:]), true
	}
	cutWhere := func(s string) (before, where string, ok bool) { return cut(s, "WHERE") }

	switch t {
	case duckdb.STATEMENT_TYPE_DELETE:
		if sc.keywords("TRUNCATE") {
			sc.keywords("TABLE")
			return "SELECT 1 FROM " + strings.TrimSpace(sc.rest())
		}
		if !sc.keywords("DELETE", "FROM") {
			return ""
		}
		rest := sc.rest()
		rest, returning, hasReturning := cut(rest, "RETURNING")
		target, using, hasUsing := cut(rest, "USING")
		where := ""
		if hasUsing {
			using, where, _ = cutWhere(using)
		} else {
			target, where, _ = cutWhere(target)
		}
		sel := "1"
		if hasReturning {
			sel = returning
		}
		q := "SELECT " + sel + " FROM " + target
		if hasUsing {
			q += ", " + using
		}
		if where != "" {
			q += " WHERE " + where
		}
		return q

	case duckdb.STATEMENT_TYPE_UPDATE:
		if !sc.keywords("UPDATE") {
			return ""
		}
		rest := sc.rest()
		target, assignments, ok := cut(rest, "SET")
		if !ok {
			return ""
		}
		assignments, returning, hasReturning := cut(assignments, "RETURNING")
		assignments, where, _ := cutWhere(assignments)
		assignments, from, hasFrom := cut(assignments, "FROM")
		var exprs []string
		for _, a := range splitTopLevel(assignments, ',') {
			eq := topLevelIndex(a, '=')
			if eq < 0 {
				return ""
			}
			exprs = append(exprs, strings.TrimSpace(a[eq+1:]))
		}
		if hasReturning {
			exprs = append(exprs, returning)
		}
		q := "SELECT " + strings.Join(exprs, ", ") + " FROM " + target
		if hasFrom {
			q += ", " + from
		}
		if where != "" {
			q += " WHERE " + where
		}
		return q

	case statementTypeMergeInto:
		if !sc.keywords("MERGE", "INTO") {
			return ""
		}
		rest := sc.rest()
		target, using, ok := cut(rest, "USING")
		if !ok {
			return ""
		}
		using, _, _ = cut(using, "WHEN")
		source, cond, ok := cut(using, "ON")
		if !ok {
			return ""
		}
		return "SELECT 1 FROM " + target + " JOIN " + source + " ON " + cond
	}
	return ""
}

// setVariableAsSelect turns SET VARIABLE x = expr into SELECT expr. Other
// SET statements read nothing, and come back empty.
func setVariableAsSelect(query string) string {
	sc := &scanner{s: query}
	if !sc.keywords("SET", "VARIABLE") && !sc.keywords("SET", "SESSION", "VARIABLE") {
		return ""
	}
	if _, ok := sc.name(); !ok {
		return ""
	}
	sc.skip()
	switch {
	case strings.HasPrefix(sc.rest(), "="):
		sc.i++
	case sc.keywords("TO"):
	default:
		return ""
	}
	return "SELECT " + strings.TrimSuffix(strings.TrimSpace(sc.rest()), ";")
}

// topLevelKeyword returns where words first follow each other in query's
// code outside parentheses, or -1. Words match whole and case-insensitively,
// with whitespace and comments between them.
func topLevelKeyword(query string, words ...string) int {
	depth := 0
	pos := 0
	for _, seg := range sqllex.Segments(query) {
		start := pos
		pos += len(seg.Text)
		if seg.Kind != sqllex.Code {
			continue
		}
		for i := 0; i < len(seg.Text); i++ {
			switch c := seg.Text[i]; {
			case c == '(':
				depth++
			case c == ')':
				depth--
			case depth == 0 && sqllex.IdentByte(c) && (i == 0 || !sqllex.IdentByte(seg.Text[i-1])):
				at := start + i
				k := &scanner{s: query, i: at}
				if k.keywords(words...) && (k.i == len(query) || !sqllex.IdentByte(query[k.i])) {
					return at
				}
			}
		}
	}
	return -1
}

// topLevelIndex returns the index of the first c in s's code outside
// parentheses, or -1.
func topLevelIndex(s string, c byte) int {
	depth, pos := 0, 0
	for _, seg := range sqllex.Segments(s) {
		start := pos
		pos += len(seg.Text)
		if seg.Kind != sqllex.Code {
			continue
		}
		for i := 0; i < len(seg.Text); i++ {
			switch seg.Text[i] {
			case '(':
				depth++
			case ')':
				depth--
			case c:
				if depth == 0 {
					return start + i
				}
			}
		}
	}
	return -1
}

// splitTopLevel splits s at each sep in its code outside parentheses.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	for {
		i := topLevelIndex(s, sep)
		if i < 0 {
			return append(parts, s)
		}
		parts = append(parts, s[:i])
		s = s[i+1:]
	}
}

// matchParen returns the index of the parenthesis closing the one at
// s[open], skipping literals and comments, or -1.
func matchParen(s string, open int) int {
	depth, pos := 0, 0
	for _, seg := range sqllex.Segments(s) {
		start := pos
		pos += len(seg.Text)
		if seg.Kind != sqllex.Code || pos <= open {
			continue
		}
		for i := max(open-start, 0); i < len(seg.Text); i++ {
			switch seg.Text[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return start + i
				}
			}
		}
	}
	return -1
}

// --- plans ----------------------------------------------------------------

// planScans is what a statement's unoptimized plan reads from the catalog
// and from connections.
type planScans struct {
	// iceberg counts Iceberg scans, which do not say which table they
	// read.
	iceberg int
	// named are scans of catalog tables that name their table, which only
	// happens when the catalog is a DuckDB database (as in tests).
	named []auth.Object
	// external counts Parquet scans, which statements of users who need
	// grants can make only through external tables' views: they may not
	// call read_parquet themselves, and the views they may read are
	// analyzed, and refused if they call it, unless they are external
	// tables' own.
	external int
	// connections are scans of connections' tables (see connreads.go).
	connections []connectionScan
	// tableFns counts calls of table functions that take a table other
	// than iceberg_scan, such as iceberg_metadata (see tableArgCalls).
	tableFns int
}

// catalogTotal counts the scans that are not of connections' tables: of
// the catalog's tables and of external tables' files.
func (p planScans) catalogTotal() int { return p.iceberg + len(p.named) + p.external }

func (p planScans) total() int { return p.catalogTotal() + len(p.connections) }

// allowedFunctions are the table functions users with the none role may
// use: generators and catalog listings, which read no table data and no
// files. Everything else, including every function that reads files or
// takes a table name (read_parquet, iceberg_scan, query_table, ...), is
// refused.
var allowedFunctions = map[string]bool{
	"range": true, "generate_series": true, "unnest": true, "repeat": true, "repeat_row": true,
	"json_each": true, "json_tree": true, "pg_timezone_names": true, "icu_calendar_names": true,
	"duckdb_approx_database_count": true, "duckdb_columns": true, "duckdb_constraints": true,
	"duckdb_databases": true, "duckdb_dependencies": true, "duckdb_extensions": true,
	"duckdb_functions": true, "duckdb_indexes": true, "duckdb_keywords": true, "duckdb_optimizers": true,
	"duckdb_schemas": true, "duckdb_sequences": true, "duckdb_settings": true, "duckdb_tables": true,
	"duckdb_types": true, "duckdb_variables": true, "duckdb_views": true,
	"pragma_collations": true, "pragma_database_size": true, "pragma_metadata_info": true,
	"pragma_platform": true, "pragma_show": true, "pragma_table_info": true, "pragma_user_agent": true,
	"pragma_version": true,
	"igloo_comments": true, "igloo_query_history": true, "igloo_login_history": true,
	"igloo_schemata": true, "igloo_tables": true, "igloo_columns": true, "igloo_lineage": true, "igloo_table_usage": true,
	"igloo_task_history": true, "igloo_load_history": true, "igloo_usage": true, "igloo_alert_history": true,
	"igloo_policy_references": true, "igloo_tag_references": true, "igloo_data_retention": true,
	"igloo_erasure_requests": true,
}

// planScans explains text on the session's connection and returns the
// catalog and connection scans in its unoptimized plan, refusing scans of
// anything else that needs a grant.
func (s *Server) planScans(ctx context.Context, sess *session, text string) (planScans, error) {
	plan, err := s.explain(ctx, sess, text)
	if err != nil {
		return planScans{}, denied("cannot analyze this statement: %v", err)
	}
	return s.scansOf(ctx, sess, plan, true)
}

// scansOf returns the scans in a plan. strict, for users with the none
// role, refuses table functions they may not use and scans of databases
// other than the catalog and connections; otherwise only scans of
// connections are of interest.
func (s *Server) scansOf(ctx context.Context, sess *session, plan any, strict bool) (planScans, error) {
	var scans planScans
	var tableFunctions map[string]bool
	if strict {
		// Only strict scans refuse table functions. Listing them takes
		// DuckDB some 25 ms, which every statement would otherwise spend
		// holding the catalog lock while connections are attached.
		var err error
		if tableFunctions, err = s.tableFunctions(ctx, sess); err != nil {
			return scans, err
		}
	}
	catalogName, local := s.catalogName(), s.engine.LocalDatabase()
	connections := len(s.connectionCatalogs()) > 0

	var visit func(n any) error
	visit = func(n any) error {
		switch n := n.(type) {
		case []any:
			for _, c := range n {
				if err := visit(c); err != nil {
					return err
				}
			}
		case map[string]any:
			name, _ := n["name"].(string)
			fn := strings.ToLower(strings.TrimSpace(name))
			extra, _ := n["extra_info"].(map[string]any)
			table, hasTable := extra["Table"].(string)
			if tableArgFunctions[fn] && fn != "iceberg_scan" {
				scans.tableFns++
			}
			switch {
			case fn == "iceberg_scan":
				scans.iceberg++
			case fn == "read_parquet" && s.external != nil:
				scans.external++
			case fn == "seq_scan":
				parts := splitQualified(table)
				switch {
				case len(parts) == 3 && strings.EqualFold(parts[0], catalogName):
					scans.named = append(scans.named, auth.Object{Schema: parts[1], Table: parts[2]})
				case !strict:
				case len(parts) != 3:
					return denied("cannot tell which table a scan reads (%q)", table)
				case strings.EqualFold(parts[0], "temp") || strings.EqualFold(parts[0], local) ||
					strings.EqualFold(parts[0], "system"):
				default:
					return denied("reading tables of database %s needs the reader role", parts[0])
				}
			case connectionScanNodes[fn] != "":
				if table == "" {
					return denied("cannot tell which table of a connection a scan reads")
				}
				scans.connections = append(scans.connections, connectionScan{connectionScanNodes[fn], strings.ToLower(table)})
			case hasTable && connections && isLeaf(n):
				// A scan of some other kind of database (scans have no
				// children; writes to a table do): it would have to be
				// accounted for like the known ones, and is not.
				return denied("cannot tell what a %s scan reads", name)
			case strict && tableFunctions[fn] && !allowedFunctions[fn]:
				return denied("%s reads data that grants do not cover, which needs the reader role", fn)
			}
			return visit(n["children"])
		}
		return nil
	}
	return scans, visit(plan)
}

// isLeaf reports whether a plan node has no children.
func isLeaf(n map[string]any) bool {
	children, _ := n["children"].([]any)
	return len(children) == 0
}

// tableFunctions returns the names of every table function, lower-cased.
func (s *Server) tableFunctions(ctx context.Context, sess *session) (map[string]bool, error) {
	if m := sess.memo; m != nil && m.tableFns != nil {
		return m.tableFns, nil
	}
	names, err := queryStrings(ctx, sess, `SELECT DISTINCT system.main.lower(function_name) FROM system.main.duckdb_functions()
		WHERE function_type IN ('table', 'table_macro')`)
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	if sess.memo != nil {
		sess.memo.tableFns = m
	}
	return m, nil
}

// checkMemo is what the checks of one statement work out once and use
// again: the catalog check of a user with the none role and the check of
// connections' tables both read the statement's plan, the first also
// DuckDB's table functions, and neither changes while the statement is
// prepared (see prepareChecked).
type checkMemo struct {
	// plans are explainLogical's plans, by the text explained. They are
	// only read.
	plans    map[string]any
	tableFns map[string]bool
}

// explain is explainLogical on the session's connection, once for each
// text while a statement is prepared (see checkMemo).
func (s *Server) explain(ctx context.Context, sess *session, text string) (any, error) {
	m := sess.memo
	if m == nil {
		return explainLogical(ctx, sess.conn, text)
	}
	if p, ok := m.plans[text]; ok {
		return p, nil
	}
	p, err := explainLogical(ctx, sess.conn, text)
	if err == nil {
		m.plans[text] = p
	}
	return p, err
}

// explainLogical returns DuckDB's unoptimized plan for text, which has one
// scan for every table it binds. The session's explain_output is restored
// afterwards. Parameters are bound to NULL, which does not change what the
// statement scans.
func explainLogical(ctx context.Context, conn *duckdb.Conn, text string) (any, error) {
	old, err := settingValue(ctx, conn, "explain_output")
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION explain_output = 'all'", nil); err != nil {
		return nil, err
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), "SET SESSION explain_output = "+quoteLiteral(strings.ToLower(old)), nil) //nolint:errcheck

	ds, err := conn.PrepareContext(ctx, "EXPLAIN (FORMAT JSON) "+text)
	if err != nil {
		return nil, err
	}
	stmt := ds.(*duckdb.Stmt)
	defer stmt.Close() //nolint:errcheck
	args := make([]driver.NamedValue, stmt.NumInput())
	for i := range args {
		args[i] = driver.NamedValue{Ordinal: i + 1}
	}
	rows, err := stmt.QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("no logical plan")
			}
			return nil, err
		}
		if key, _ := dest[0].(string); key == "logical_plan" {
			var plan any
			value, _ := dest[1].(string)
			if err := json.Unmarshal([]byte(value), &plan); err != nil {
				return nil, fmt.Errorf("read plan: %w", err)
			}
			return plan, nil
		}
	}
}

// settingValue returns a DuckDB setting's current value.
func settingValue(ctx context.Context, conn *duckdb.Conn, name string) (string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT system.main.current_setting("+quoteLiteral(name)+")::VARCHAR", nil)
	if err != nil {
		return "", err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return "", err
	}
	v, _ := dest[0].(string)
	return v, nil
}

// splitQualified splits a name such as "temp".main.t, as DuckDB prints
// them, into unquoted parts.
func splitQualified(s string) []string {
	var parts []string
	for len(s) > 0 {
		if s[0] == '"' {
			end := sqllex.CloseQuote(s, 0, '"', false)
			parts = append(parts, strings.ReplaceAll(s[1:end-1], `""`, `"`))
			s = s[end:]
		} else {
			i := strings.IndexByte(s, '.')
			if i < 0 {
				i = len(s)
			}
			parts = append(parts, s[:i])
			s = s[i:]
		}
		if len(s) > 0 {
			if s[0] != '.' {
				return nil
			}
			s = s[1:]
		}
	}
	return parts
}
