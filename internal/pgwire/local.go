package pgwire

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
)

// DuckDB's own database (Engine.LocalDatabase) holds each replica's copies
// of the catalog's views and Igloo's history views, which every session on
// the replica reads. Only admins may change it: a change there would differ
// from the catalog and from other replicas, and would change what other
// users' queries read. A statement names its target by name, which DuckDB
// resolves against the session's current database and search path, so the
// checks below resolve the target the way DuckDB does rather than look for
// the database's name in the text, and non-admins may not make the local
// database the session's current one.

// authorizeLocal refuses a non-admin's statement of type t that would
// create, change or drop something in the local database. original is the
// statement as the client sent it, and query the statement as Igloo
// rewrote it, with view names pointing at their local copies: a statement
// may read views so, as dbt's CREATE TABLE ... AS SELECT from a view does,
// but not write to one.
func (s *Server) authorizeLocal(ctx context.Context, sess *session, role auth.Role, t duckdb.StmtType, original, query string) error {
	// Reads, settings (see runSetting), and the read-only PRAGMA and CALL
	// non-admins may run.
	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_EXPLAIN, duckdb.STATEMENT_TYPE_RELATION,
		duckdb.STATEMENT_TYPE_TRANSACTION, duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET,
		duckdb.STATEMENT_TYPE_PRAGMA, duckdb.STATEMENT_TYPE_CALL:
		return nil
	}
	local := s.engine.LocalDatabase()
	refuse := pgerror.WithCode(fmt.Errorf("permission denied: only admins may change the %s database", local),
		codes.InsufficientPrivilege)
	if role == auth.RoleAdmin {
		return nil
	}
	if namesDatabase(original, local) {
		return refuse
	}
	target, known := parseLocalTarget(original)
	if s.catalogName() != "" {
		// With the catalog attached, the local database is not where
		// anything of a non-admin's belongs.
		if !known {
			return pgerror.WithCode(errors.New("permission denied: cannot tell which database this statement changes"),
				codes.InsufficientPrivilege)
		}
		inLocal, err := s.targetsLocal(ctx, sess, target)
		if err != nil {
			return err
		}
		if inLocal {
			return refuse
		}
	}
	if namesDatabase(query, local) {
		// Igloo's rewrite named it, for views the statement reads; the
		// target must still be elsewhere.
		rewritten, ok := parseLocalTarget(query)
		if !ok || rewritten.kind != writesNone && len(rewritten.name) > 1 && strings.EqualFold(rewritten.name[0], local) {
			return refuse
		}
	}
	return nil
}

// targetKind is what a statement's target is.
type targetKind int

const (
	// writesNone: the statement writes no database's objects (a temporary
	// object, or COPY ... TO a file).
	writesNone targetKind = iota
	// writesNew: an object CREATE makes where DuckDB puts new objects.
	writesNew
	// writesSchema: a schema, written [database.]schema.
	writesSchema
	// writesRelation: an existing table or view.
	writesRelation
	// writesRoutine: an existing macro, function, sequence, type or index.
	writesRoutine
)

// localTarget is the object a statement creates, changes or drops, by name
// as written, parts unquoted.
type localTarget struct {
	kind targetKind
	name []string
}

// parseLocalTarget works out what query, a statement of a kind non-admins
// may run, creates, changes or drops. ok is false for other statements.
func parseLocalTarget(query string) (t localTarget, ok bool) {
	if c, ok := parseCreateTable(query); ok {
		if c.temporary {
			return localTarget{}, true
		}
		return localTarget{writesNew, c.name}, true
	}
	sc := &scanner{s: query}
	if sc.keywords("WITH") {
		// A WITH clause may come before INSERT, UPDATE, DELETE or MERGE.
		at := -1
		for _, w := range []string{"INSERT", "UPDATE", "DELETE", "MERGE"} {
			if i := topLevelKeyword(query, w); i >= 0 && (at < 0 || i < at) {
				at = i
			}
		}
		if at < 0 {
			return localTarget{}, false
		}
		sc = &scanner{s: query, i: at}
	}
	named := func(kind targetKind, name []string, ok bool) (localTarget, bool) {
		return localTarget{kind, name}, ok
	}
	ifExists := func() { sc.keywords("IF", "EXISTS") }
	switch {
	case sc.keywords("INSERT"):
		_ = sc.keywords("OR", "REPLACE") || sc.keywords("OR", "IGNORE")
		if !sc.keywords("INTO") {
			return localTarget{}, false
		}
		name, ok := sc.name()
		return named(writesRelation, name, ok)
	case sc.keywords("UPDATE"), sc.keywords("DELETE", "FROM"), sc.keywords("MERGE", "INTO"):
		name, ok := sc.name()
		return named(writesRelation, name, ok)
	case sc.keywords("TRUNCATE"):
		sc.keywords("TABLE")
		name, ok := sc.name()
		return named(writesRelation, name, ok)
	case sc.keywords("COPY"):
		return parseCopyTarget(sc)
	case sc.keywords("CREATE"):
		sc.keywords("OR", "REPLACE")
		if sc.keywords("TEMP") || sc.keywords("TEMPORARY") {
			return localTarget{}, true
		}
		sc.keywords("PERSISTENT")
		switch {
		case sc.keywords("SCHEMA"):
			sc.keywords("IF", "NOT", "EXISTS")
			name, ok := sc.name()
			return named(writesSchema, name, ok)
		case sc.keywords("INDEX"), sc.keywords("UNIQUE", "INDEX"):
			sc.keywords("IF", "NOT", "EXISTS")
			if _, ok := sc.name(); !ok || !sc.keywords("ON") {
				return localTarget{}, false
			}
			name, ok := sc.name()
			return named(writesRelation, name, ok)
		case sc.keywords("VIEW"), sc.keywords("MACRO"), sc.keywords("FUNCTION"), sc.keywords("SEQUENCE"),
			sc.keywords("TYPE"):
			sc.keywords("IF", "NOT", "EXISTS")
			name, ok := sc.name()
			return named(writesNew, name, ok)
		}
	case sc.keywords("DROP"):
		switch {
		case sc.keywords("TABLE"), sc.keywords("VIEW"):
			ifExists()
			name, ok := sc.name()
			return named(writesRelation, name, ok)
		case sc.keywords("SCHEMA"):
			ifExists()
			name, ok := sc.name()
			return named(writesSchema, name, ok)
		case sc.keywords("MACRO", "TABLE"), sc.keywords("MACRO"), sc.keywords("FUNCTION"), sc.keywords("SEQUENCE"),
			sc.keywords("TYPE"), sc.keywords("INDEX"):
			ifExists()
			name, ok := sc.name()
			return named(writesRoutine, name, ok)
		}
	case sc.keywords("ALTER"):
		switch {
		case sc.keywords("TABLE"), sc.keywords("VIEW"):
			ifExists()
			name, ok := sc.name()
			return named(writesRelation, name, ok)
		case sc.keywords("SEQUENCE"):
			ifExists()
			name, ok := sc.name()
			return named(writesRoutine, name, ok)
		}
	case sc.keywords("COMMENT", "ON"):
		switch {
		case sc.keywords("TABLE"), sc.keywords("VIEW"):
			name, ok := sc.name()
			return named(writesRelation, name, ok)
		case sc.keywords("COLUMN"):
			name, ok := sc.name()
			if !ok || len(name) < 2 {
				return localTarget{}, false
			}
			return named(writesRelation, name[:len(name)-1], true)
		case sc.keywords("MACRO", "TABLE"), sc.keywords("MACRO"), sc.keywords("FUNCTION"), sc.keywords("SEQUENCE"),
			sc.keywords("TYPE"), sc.keywords("INDEX"):
			name, ok := sc.name()
			return named(writesRoutine, name, ok)
		}
	}
	return localTarget{}, false
}

// parseCopyTarget reads the rest of COPY: a table it loads, the database
// COPY FROM DATABASE copies into, or nothing for COPY ... TO a file.
func parseCopyTarget(sc *scanner) (localTarget, bool) {
	if sc.keywords("FROM", "DATABASE") {
		if _, ok := sc.name(); !ok || !sc.keywords("TO") {
			return localTarget{}, false
		}
		db, ok := sc.name()
		if !ok || len(db) != 1 {
			return localTarget{}, false
		}
		return localTarget{writesSchema, []string{db[0], "main"}}, true
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		// COPY (query) TO a file.
		return localTarget{}, true
	}
	name, ok := sc.name()
	if !ok {
		return localTarget{}, false
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
		end, ok := closingParen(sc.s, sc.i)
		if !ok {
			return localTarget{}, false
		}
		sc.i = end + 1
	}
	switch {
	case sc.keywords("FROM"):
		return localTarget{writesRelation, name}, true
	case sc.keywords("TO"):
		return localTarget{}, true
	}
	return localTarget{}, false
}

// targetsLocal reports whether t would be in the local database, resolved
// as DuckDB resolves names for sess.
func (s *Server) targetsLocal(ctx context.Context, sess *session, t localTarget) (bool, error) {
	local := s.engine.LocalDatabase()
	isLocal := func(db string) bool { return strings.EqualFold(db, local) }
	switch {
	case t.kind == writesNone:
		return false, nil
	case len(t.name) == 3, t.kind == writesSchema && len(t.name) == 2:
		return isLocal(t.name[0]), nil
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return false, err
	}
	_, dbFirst := r.databases[strings.ToLower(t.name[0])]
	dbFirst = dbFirst && len(t.name) == 2
	if !dbFirst && isLocal(r.path.defaultDB) {
		// A name with no database finds or makes its object through the
		// current database.
		return true, nil
	}
	switch t.kind {
	case writesSchema:
		// The first database on the search path with the schema, as
		// DuckDB looks schemas up, else the current one.
		return isLocal(r.createDatabase([]string{t.name[0], ""})), nil
	case writesNew:
		return isLocal(r.createDatabase(t.name)), nil
	case writesRelation:
		res, err := r.resolve(tableRef{parts: t.name}, nil)
		if err != nil {
			return false, err
		}
		found := res.first
		if len(found) == 0 {
			found = res.all
		}
		for _, o := range found {
			if isLocal(o.db) {
				return true, nil
			}
		}
		return false, nil
	}
	if dbFirst {
		return isLocal(t.name[0]), nil
	}
	return localHasRoutine(ctx, sess.conn, local, t.name[len(t.name)-1])
}

// createDatabase is the database where CREATE puts name, written with no
// database: as DuckDB binds it (Binder::BindSchema), a schema-qualified
// name goes to the first database on the search path with the schema, and
// an unqualified one to the current database. A first part that names a
// database names the database.
func (r *resolver) createDatabase(name []string) string {
	if len(name) == 2 {
		if _, ok := r.databases[strings.ToLower(name[0])]; ok {
			return name[0]
		}
		for _, e := range r.path.entries {
			if strings.EqualFold(e.schema, name[0]) {
				return e.db
			}
		}
	}
	return r.path.defaultDB
}

// localHasRoutine reports whether the local database has a macro, function,
// sequence, type or index called name, which a statement that names it
// without a database could reach through the search path.
func localHasRoutine(ctx context.Context, conn *duckdb.Conn, local, name string) (bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT system.main.count_star() FROM (
			SELECT 1 FROM system.main.duckdb_functions() WHERE database_name = $1 AND system.main.lower(function_name) = system.main.lower($2)
			UNION ALL SELECT 1 FROM system.main.duckdb_sequences() WHERE database_name = $1 AND system.main.lower(sequence_name) = system.main.lower($2)
			UNION ALL SELECT 1 FROM system.main.duckdb_types() WHERE database_name = $1 AND system.main.lower(type_name) = system.main.lower($2)
			UNION ALL SELECT 1 FROM system.main.duckdb_indexes() WHERE database_name = $1 AND system.main.lower(index_name) = system.main.lower($2))`,
		[]driver.NamedValue{{Ordinal: 1, Value: local}, {Ordinal: 2, Value: name}})
	if err != nil {
		return false, err
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return false, err
	}
	n, _ := dest[0].(int64)
	return n > 0, nil
}

// runSetting runs st, a SET or RESET, and keeps the session's search path
// in order: the local copies of views next to the catalog's schemas on it
// (see keepViewsOnPath), and, for non-admins, the local database not the
// current one, where unqualified CREATE and DROP would change it. Such a
// SET is undone and refused.
func (s *Server) runSetting(ctx context.Context, sess *session, st *statement, args []driver.NamedValue, w wire.DataWriter) error {
	guard := s.catalogName() != "" && sess.currentUser().Role != auth.RoleAdmin
	var before string
	if guard {
		var err error
		if before, err = settingValue(ctx, sess.conn, "search_path"); err != nil {
			return err
		}
	}
	held := &heldCompletion{DataWriter: w}
	if err := s.runStatement(ctx, sess, st, args, held); err != nil {
		return err
	}
	if s.ddl != nil && setsSearchPath.MatchString(stripComments(st.query)) {
		if err := s.keepViewsOnPath(ctx, sess); err != nil {
			s.log.Warn("could not keep views on the search path", "err", err)
		}
	}
	if guard {
		db, _, err := currentSchema(ctx, sess.conn)
		if err == nil && strings.EqualFold(db, s.engine.LocalDatabase()) {
			_, err = sess.conn.ExecContext(ctx, "SET SESSION search_path = "+quoteLiteral(before), nil)
			if err == nil {
				err = pgerror.WithCode(fmt.Errorf("permission denied: only admins may make %s the current database; put a schema of %s first on the search path",
					s.engine.LocalDatabase(), s.catalogName()), codes.InsufficientPrivilege)
			}
		}
		if err != nil {
			return err
		}
	}
	return held.release()
}

// heldCompletion holds back a statement's command tag until release, so
// the statement can still fail after running.
type heldCompletion struct {
	wire.DataWriter
	tag  string
	done bool
}

func (h *heldCompletion) Complete(tag string) error {
	h.tag, h.done = tag, true
	return nil
}

func (h *heldCompletion) release() error {
	if !h.done {
		return errors.New("the statement did not complete")
	}
	return h.DataWriter.Complete(h.tag)
}
