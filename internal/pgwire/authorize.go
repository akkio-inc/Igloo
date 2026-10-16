package pgwire

import (
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
var rank = map[auth.Role]int{auth.RoleReader: 1, auth.RoleWriter: 2, auth.RoleAdmin: 3}

// authorize returns an insufficient_privilege error unless role may run a
// statement of type t. DuckDB's sandbox already confines everyone to the
// warehouse and allowed locations; roles decide who may change data and who
// may touch instance-wide state such as secrets, attachments and settings.
func authorize(role auth.Role, t duckdb.StmtType, query string) error {
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
	// catalog.
	if mentionsFunction(query, words, "duckdb_secrets", "which_secret", "iceberg_to_ducklake") {
		return auth.RoleAdmin
	}

	role := statementRole(t, query, words)
	// These change the catalog from inside a SELECT. Table properties
	// include snapshot retention, which decides how long history and time
	// travel survive table maintenance.
	if rank[role] < rank[auth.RoleWriter] && mentionsFunction(query, words,
		"set_iceberg_table_properties", "remove_iceberg_table_properties",
		"set_iceberg_schema_properties", "remove_iceberg_schema_properties") {
		return auth.RoleWriter
	}
	return role
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
		duckdb.STATEMENT_TYPE_ALTER, duckdb.STATEMENT_TYPE_CREATE_FUNC, duckdb.STATEMENT_TYPE_COPY:
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

// sessionSetting reports whether a SET or RESET may be run by any user: it
// names a SQL variable or one of sessionSettings, at session scope or no
// scope.
func sessionSetting(query string) bool {
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
