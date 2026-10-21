package pgwire

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/sqllex"
)

// rank orders roles so a higher role can do everything a lower one can.
// The none role, like any unknown one, ranks below them all.
var rank = map[auth.Role]int{auth.RoleNone: 0, auth.RoleReader: 1, auth.RoleWriter: 2, auth.RoleAdmin: 3}

// statementTypeMergeInto is DuckDB's statement type for MERGE INTO, which
// duckdb-go does not name.
const statementTypeMergeInto duckdb.StmtType = 30

// authorize returns an insufficient_privilege error unless role may run a
// statement of type t. DuckDB's sandbox already confines everyone to the
// warehouse and allowed locations; roles decide who may change data and who
// may touch instance-wide state such as secrets, attachments and settings.
func authorize(role auth.Role, t duckdb.StmtType, query string) error {
	if err := forbidden(query); err != nil {
		return err
	}
	need := requiredRole(t, query)
	if rank[role] >= rank[need] {
		return nil
	}
	return pgerror.WithCode(
		fmt.Errorf("permission denied: %s requires the %s role", describe(t, query), need),
		codes.InsufficientPrivilege,
	)
}

// requiredRole is the least role that may run a statement. It relies on
// DuckDB's statement type, refined by the words of the query's SQL code
// (never literals or comments, see codeWords). Wherever those words cannot
// settle the question it asks for admin, and statement types it does not
// know require admin too, so new DuckDB features are closed by default.
func requiredRole(t duckdb.StmtType, query string) auth.Role {
	words := sqllex.CodeWords(query)

	// Secret metadata (key ids, scopes) and secrets themselves are shared
	// by every session. iceberg_to_ducklake copies tables into another
	// catalog, and dbgen writes tables wherever it is told. DuckDB's log
	// and profiler are shared too: once on, the log holds every session's
	// statements.
	if mentionsFunction(query, words, "duckdb_secrets", "which_secret", "iceberg_to_ducklake", "dbgen",
		"enable_logging", "disable_logging", "truncate_duckdb_logs", "duckdb_logs", "duckdb_logs_parsed",
		"duckdb_log_contexts", "enable_profiling", "disable_profiling") {
		return auth.RoleAdmin
	}
	// query_table reads tables named in strings. The read cache's functions
	// and settings (cache_httpfs_*) are shared by every session: clearing
	// it, changing its rules, listing what it holds. So are the lists of
	// connections' tables that the postgres and mysql extensions keep (see
	// package federation), and their connection pools.
	if mentionsFunction(query, words, "query_table") || mentionsPrefix(query, words, "cache_httpfs_") ||
		mentionsFunction(query, words, connectionAdminFunctions...) {
		return auth.RoleAdmin
	}

	role := statementRole(t, query, words)
	// These change the catalog from inside a SELECT. Table properties
	// include snapshot retention, which decides how long history and time
	// travel survive table maintenance.
	if rank[role] < rank[auth.RoleWriter] && mentionsFunction(query, words, propertyFunctions...) {
		return auth.RoleWriter
	}
	return role
}

var propertyFunctions = []string{
	"set_iceberg_table_properties", "remove_iceberg_table_properties",
	"set_iceberg_schema_properties", "remove_iceberg_schema_properties",
}

// grantable reports whether grants can allow a statement that the user's
// built-in role does not: one that needs no more than the writer role for
// what it is, rather than for the functions it calls. Grants never allow
// what needs admin, nor changing table properties.
func grantable(t duckdb.StmtType, query string) bool {
	if forbidden(query) != nil || requiredRole(t, query) == auth.RoleAdmin {
		return false
	}
	return !mentionsFunction(query, sqllex.CodeWords(query), propertyFunctions...)
}

// statementRole is the least role for a statement of type t, from its type
// and the words of its code.
func statementRole(t duckdb.StmtType, query string, words []string) auth.Role {
	has := func(word string) bool { return slices.Contains(words, word) }
	at := func(i int) string {
		if i < len(words) {
			return words[i]
		}
		return ""
	}

	switch t {
	case duckdb.STATEMENT_TYPE_SELECT, duckdb.STATEMENT_TYPE_RELATION, duckdb.STATEMENT_TYPE_TRANSACTION:
		return auth.RoleReader

	case duckdb.STATEMENT_TYPE_EXPLAIN:
		// EXPLAIN ANALYZE runs the statement it explains, which may be
		// anything. Telling what that is would take a full parser, so only
		// admins may use it.
		if has("ANALYZE") || has("ANALYSE") {
			return auth.RoleAdmin
		}
		return auth.RoleReader

	case duckdb.STATEMENT_TYPE_PRAGMA:
		if readOnlyPragmas[strings.ToLower(at(1))] && !strings.Contains(sqllex.CodeOnly(query), "=") {
			return auth.RoleReader
		}
		return auth.RoleAdmin

	case duckdb.STATEMENT_TYPE_CALL:
		if strings.HasPrefix(at(1), "PRAGMA_") {
			return auth.RoleReader
		}
		return auth.RoleAdmin

	case duckdb.STATEMENT_TYPE_SET, duckdb.STATEMENT_TYPE_VARIABLE_SET:
		if sessionSetting(query) {
			return auth.RoleReader
		}
		return auth.RoleAdmin

	case duckdb.STATEMENT_TYPE_INSERT, duckdb.STATEMENT_TYPE_UPDATE, duckdb.STATEMENT_TYPE_DELETE,
		duckdb.STATEMENT_TYPE_ALTER, duckdb.STATEMENT_TYPE_CREATE_FUNC, duckdb.STATEMENT_TYPE_COPY,
		statementTypeMergeInto:
		return auth.RoleWriter

	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP:
		// Secrets are shared by every session. Any unquoted SECRET counts,
		// so a table named secret needs quoting to be created by a writer.
		if has("SECRET") {
			return auth.RoleAdmin
		}
		return auth.RoleWriter
	}

	return auth.RoleAdmin
}

// mentionsFunction reports whether query names any of fns in its code,
// unquoted or quoted.
func mentionsFunction(query string, words []string, fns ...string) bool {
	for _, fn := range fns {
		if slices.Contains(words, strings.ToUpper(fn)) || slices.Contains(sqllex.QuotedIdentifiers(query), fn) {
			return true
		}
	}
	return false
}

// mentionsPrefix reports whether query's code has a name, unquoted or
// quoted, that starts with prefix (in lower case).
func mentionsPrefix(query string, words []string, prefix string) bool {
	upper := strings.ToUpper(prefix)
	return slices.ContainsFunc(words, func(w string) bool { return strings.HasPrefix(w, upper) }) ||
		slices.ContainsFunc(sqllex.QuotedIdentifiers(query), func(name string) bool { return strings.HasPrefix(name, prefix) })
}

// fixedCacheSettings are the read cache's settings and functions that
// decide where its files live, whether it wraps other filesystems and which
// files it caches. Igloo sets them up at startup (see engine.setupCache).
// Moving the cache directory would have the extension create and delete
// directories outside DuckDB's sandbox, which is why DuckDB refuses to move
// its own temp_directory once it is in use; caching files outside the
// warehouse would keep copies of files that change.
var fixedCacheSettings = []string{
	"cache_httpfs_cache_directory", "cache_httpfs_cache_directories_config",
	"cache_httpfs_wrap_cache_filesystem",
	"cache_httpfs_add_exclusion_regex", "cache_httpfs_reset_exclusion_regex",
}

// connectionAdminFunctions are the postgres and mysql extensions' functions
// that change state every session shares: their lists of tables, which
// ALTER CONNECTION ... REFRESH reloads safely, and their pools.
var connectionAdminFunctions = []string{
	"pg_clear_cache", "mysql_clear_cache", "postgres_configure_pool",
	"mysql_debug_execution_plan", "mysql_explain_federated",
}

// remoteSQLFunctions are the postgres and mysql extensions' functions that
// run SQL given as a string on a remote server, write to it, or connect to
// any server at all with credentials given in the call. None of what they
// do can be checked against grants, and they ignore that connections are
// attached read-only.
var remoteSQLFunctions = []string{
	"postgres_query", "postgres_execute", "postgres_scan", "postgres_scan_pushdown", "postgres_attach",
	"mysql_query", "mysql_execute",
}

// forbidden returns an insufficient_privilege error for a statement that
// no one may run, admins included, or nil.
//
// query() and json_execute_serialized_sql() run SQL that a statement builds
// from strings, which no check of the statement's text can see into: with
// them, a reader could call functions only admins may call, and an admin
// could change the settings below. Without them, every function and
// setting a statement uses is named in its text. (query_table() takes only
// table names and is admin-only, see requiredRole; a name it builds at run
// time is refused here, see queryTableComputed.) Spatial's GDAL readers and
// writers open files outside the sandbox (see gdalUse). The functions of
// remoteSQLFunctions run SQL from strings on other databases.
func forbidden(query string) error {
	words := sqllex.CodeWords(query)
	var err error
	switch {
	case callsQuery(query):
		err = errors.New("permission denied: query() runs SQL from a string, which Igloo cannot check; write the query itself instead")
	case mentionsFunction(query, words, "json_execute_serialized_sql"):
		err = errors.New("permission denied: json_execute_serialized_sql runs SQL that Igloo cannot check; write the query itself instead")
	case gdalUse(query) != "":
		err = fmt.Errorf("permission denied: %s goes through GDAL, which can open files and URLs outside Igloo's sandbox; "+
			"read GeoParquet with read_parquet, shapefiles with ST_ReadSHP, or GeoJSON with read_json and ST_GeomFromGeoJSON instead",
			gdalUse(query))
	case queryTableComputed(query):
		err = errors.New("permission denied: query_table needs a table name written out as text; a name built at run time " +
			"could resolve to a file, including spatial's GDAL formats, that reads outside Igloo's sandbox")
	case mentionsFunction(query, words, remoteSQLFunctions...):
		err = errors.New("permission denied: the functions that run SQL on other databases (postgres_query, mysql_query and the like) " +
			"are not available, as Igloo cannot check that SQL against grants; query connections' tables by name instead, " +
			"as in SELECT * FROM crm.public.customers")
	default:
		for _, name := range fixedCacheSettings {
			if mentionsFunction(query, words, name) {
				err = fmt.Errorf("permission denied: %s is set when Igloo starts (IGLOO_CACHE_DIR) and cannot be changed", name)
				break
			}
		}
	}
	if err != nil {
		return pgerror.WithCode(err, codes.InsufficientPrivilege)
	}
	return nil
}

// callsQuery reports whether query calls DuckDB's query table function: it
// has the name query, unquoted or quoted and perhaps qualified, followed by
// an opening parenthesis, other than where the word before makes it the
// name of a table, view, CTE or macro being defined or written, as in
// INSERT INTO query (a) VALUES (1). Where it cannot tell, it reports a call,
// so a CTE named query with a column list after the first one is refused.
func callsQuery(query string) bool {
	toks := sqlTokens(query)
	for i, t := range toks {
		if t.name != "query" || i+1 >= len(toks) || toks[i+1].punct != '(' {
			continue
		}
		// Step back over qualifiers (main.query, "system"."main".query).
		j := i
		for j >= 2 && toks[j-1].punct == '.' && toks[j-2].name != "" {
			j -= 2
		}
		if j == 0 || !namesTableBefore[toks[j-1].keyword] {
			return true
		}
	}
	return false
}

// queryTableComputed reports whether query calls DuckDB's query_table
// function with a name it builds at run time rather than writes out. The
// function turns its argument into a table reference, which can be a path a
// replacement scan opens, including spatial's GDAL formats (see gdalUse).
// A name written as string literals is visible to gdalUse; one built from a
// variable, a concatenation, a column or a subquery is not, so Igloo cannot
// tell whether it reaches GDAL and refuses it. A name written out as one
// literal, or a list of literals (query_table(['a', 'b'])), is allowed.
func queryTableComputed(query string) bool {
	toks := sqlTokens(query)
	for i, t := range toks {
		if t.name != "query_table" || i+1 >= len(toks) || toks[i+1].punct != '(' {
			continue
		}
		// Scan the argument: a written-out name is string literals (which
		// sqlTokens shows as a ' token), perhaps in a [ ] list. A name, an
		// operator or a nested call means it is computed.
		for _, a := range toks[i+2:] {
			switch a.punct {
			case ')':
				return false
			case '\'', '[', ']', ',':
			default:
				return true
			}
		}
		return true // no closing parenthesis: treat as computed
	}
	return false
}

// namesTableBefore are the keywords after which query (...) is the name of
// a table, view, CTE or macro with a column or parameter list rather than
// a call.
var namesTableBefore = map[string]bool{
	"INTO": true, "TABLE": true, "VIEW": true, "EXISTS": true, "UPDATE": true, "REFERENCES": true,
	"ON": true, "AS": true, "COPY": true, "WITH": true, "RECURSIVE": true, "MACRO": true, "FUNCTION": true,
}

// sqlToken is a token of SQL code: a name, a punctuation character or a
// literal. Comments are left out.
type sqlToken struct {
	// name is a name, unquoted or quoted, in lower case.
	name string
	// keyword is an unquoted name in upper case.
	keyword string
	// punct is a punctuation character.
	punct byte
}

// sqlTokens splits query into tokens.
func sqlTokens(query string) []sqlToken {
	var out []sqlToken
	for _, seg := range sqllex.Segments(query) {
		switch seg.Kind {
		case sqllex.String:
			out = append(out, sqlToken{punct: '\''})
		case sqllex.Ident:
			name := strings.ReplaceAll(strings.Trim(seg.Text, `"`), `""`, `"`)
			out = append(out, sqlToken{name: strings.ToLower(name)})
		case sqllex.Code:
			s := seg.Text
			for i := 0; i < len(s); {
				switch c := s[i]; {
				case sqllex.IdentByte(c):
					j := i
					for j < len(s) && sqllex.IdentByte(s[j]) {
						j++
					}
					out = append(out, sqlToken{name: strings.ToLower(s[i:j]), keyword: strings.ToUpper(s[i:j])})
					i = j
				case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
					i++
				default:
					out = append(out, sqlToken{punct: c})
					i++
				}
			}
		}
	}
	return out
}

// readOnlyPragmas only report information.
var readOnlyPragmas = map[string]bool{
	"table_info": true, "show": true, "show_tables": true, "show_tables_expanded": true,
	"show_databases": true, "database_list": true, "database_size": true, "storage_info": true,
	"version": true, "platform": true, "functions": true, "collations": true, "metadata_info": true,
	"user_agent": true, "tpch": true, "tpcds": true,
}

// sessionSettings may be changed by any user. Igloo makes their SET and
// RESET apply to the session only (see sessionScoped): some, such as
// default_order, are otherwise global in DuckDB.
var sessionSettings = map[string]bool{
	"search_path": true, "schema": true, "timezone": true, "calendar": true,
	"default_null_order": true, "default_order": true, "explain_output": true,
	"errors_as_json": true, "integer_division": true, "enable_progress_bar": true,
	"enable_progress_bar_print": true, "progress_bar_time": true,
}

// settingStatement matches SET and RESET with no comments between the
// keywords, after leading comments are stripped.
var settingStatement = regexp.MustCompile(`(?is)^(SET|RESET)\s+(?:(SESSION|LOCAL|GLOBAL)\s+)?(VARIABLE\s+)?"?([a-z_]+)"?(\s|=|;|$)`)

// useStatement matches USE, which DuckDB runs as a SET of the session's
// current schema, and so of its search path. Snowflake's USE SCHEMA and
// USE DATABASE arrive translated to it.
var useStatement = regexp.MustCompile(`(?is)^USE\s`)

// sessionSetting reports whether a SET, RESET or USE may be run by any
// user: it is a USE, like a SET of schema, or names a SQL variable or one
// of sessionSettings, at session scope or no scope.
func sessionSetting(query string) bool {
	if useStatement.MatchString(stripComments(query)) {
		return true
	}
	m := settingStatement.FindStringSubmatch(stripComments(query))
	if m == nil || (m[2] != "" && !strings.EqualFold(m[2], "SESSION")) {
		return false
	}
	return m[3] != "" || sessionSettings[strings.ToLower(m[4])]
}

// sessionScoped rewrites a SET or RESET of one of sessionSettings that
// names no scope so it applies to the current session only. Other queries
// come back unchanged.
func sessionScoped(query string) string {
	stripped := stripComments(query)
	m := settingStatement.FindStringSubmatchIndex(stripped)
	if m == nil || m[4] != -1 || m[6] != -1 { // has a scope, or is SET VARIABLE
		return query
	}
	if !sessionSettings[strings.ToLower(stripped[m[8]:m[9]])] {
		return query
	}
	verbEnd := m[3]
	return stripped[:verbEnd] + " SESSION" + stripped[verbEnd:]
}

// describe names a statement for error messages.
func describe(t duckdb.StmtType, query string) string {
	switch t {
	case duckdb.STATEMENT_TYPE_CREATE, duckdb.STATEMENT_TYPE_DROP, duckdb.STATEMENT_TYPE_ALTER:
		return ddlTag(query)
	}
	if words := keywords(query, 1); len(words) == 1 {
		return words[0]
	}
	return "this statement"
}
