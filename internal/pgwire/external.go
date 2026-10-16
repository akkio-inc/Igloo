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

// externalScans are DuckDB's names, lower-cased, for scans of the tables
// of attached databases, by the database type (duckdb_databases().type)
// they read. A plan node that names a table and is none of these, nor a
// scan of DuckDB's own tables or of Iceberg, is refused.
var externalScans = map[string]string{"postgres_scan": "postgres", "mysql_scan": "mysql"}

// externalScan is a scan of a table of an attached database: its database
// type and its table's name, lower-cased.
type externalScan struct{ kind, table string }

// sameScans reports whether two lists hold the same scans, in any order.
func sameScans(a, b []externalScan) bool {
	key := func(s externalScan) string { return s.kind + "\x00" + s.table }
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

// externalCatalogs returns the names, lower-cased, and types of the
// attached databases that are not DuckDB's own or the Iceberg catalog: the
// connections. It is empty without any.
func (s *Server) externalCatalogs() map[string]string {
	if s.fed == nil {
		return nil
	}
	out := map[string]string{}
	for name, t := range s.fed.Catalogs() {
		out[name] = string(t)
	}
	return out
}

// isExternalType reports whether a database type from duckdb_databases()
// is another database's rather than DuckDB's own or Iceberg's.
func isExternalType(t string) bool {
	return t != "" && !strings.EqualFold(t, "duckdb") && !strings.EqualFold(t, "iceberg")
}

// authorizeExternal checks what a statement reads from connections, for
// every user but admins, unless readTables already did. acc is what the
// statement's other checks found, nil when its role allowed it. It returns
// acc with what it found added, or a new access when acc is nil and
// connections are attached, so the statement is checked again when grants,
// views or connections change; on an error the caller keeps what it had.
func (s *Server) authorizeExternal(ctx context.Context, sess *session, user auth.User, st *statement, query string, acc *access) (*access, error) {
	if len(s.externalCatalogs()) == 0 {
		return acc, nil
	}
	if st.stmtType == duckdb.STATEMENT_TYPE_DETACH {
		// Even admins drop connections with DROP CONNECTION: a detached
		// connection cannot be attached again until Igloo restarts.
		names := append(sqllex.CodeWords(query), sqllex.QuotedIdentifiers(query)...)
		for _, q := range names {
			if _, ok := s.externalCatalogs()[strings.ToLower(q)]; ok {
				return nil, denied("%s is a connection; use DROP CONNECTION", strings.ToLower(q))
			}
		}
	}
	if acc != nil && acc.externalChecked {
		return acc, nil
	}
	admin := user.Role == auth.RoleAdmin
	objects, err := s.externalReads(ctx, sess, st, query)
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
	out.externalChecked = true
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

// externalReads returns every object of a connection a statement may read,
// or an error when that cannot be worked out.
func (s *Server) externalReads(ctx context.Context, sess *session, st *statement, query string) ([]auth.Object, error) {
	planText, parseText, err := externalTexts(st.stmtType, query, st.prepared)
	if err != nil || planText == "" {
		return nil, err
	}
	plan, err := explainLogical(ctx, sess.conn, planText)
	if err != nil {
		return nil, denied("cannot analyze which tables of connections this statement reads: %v", err)
	}
	scans, err := s.scansOf(ctx, sess, plan, false)
	if err != nil {
		return nil, err
	}
	if len(scans.connections) == 0 {
		return nil, nil
	}
	if parseText == "" {
		return nil, denied("cannot tell which tables of connections this statement reads")
	}
	r, err := s.newResolver(ctx, sess)
	if err != nil {
		return nil, err
	}
	r.external = true
	reads, err := r.analyze(parseText, nil, 0)
	if err != nil {
		return nil, err
	}
	if !sameScans(reads.extScans, scans.connections) {
		return nil, denied("cannot verify which tables of connections this statement reads (the plan scans %d, the statement names %d)",
			len(scans.connections), len(reads.extScans))
	}
	return reads.extObjects, nil
}

// externalTexts decides what to EXPLAIN for a statement's reads and what to
// parse for the tables it names, for users of any role. An empty planText
// means the statement reads no table.
func externalTexts(t duckdb.StmtType, query, prepared string) (planText, parseText string, err error) {
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
	}
	return readTexts(t, query, prepared)
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
	ext := s.externalCatalogs()
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
				case r.isExternal(o.db):
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
	rows, err := sess.conn.QueryContext(ctx, "SELECT lower(database_name), type FROM duckdb_databases()", nil)
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
