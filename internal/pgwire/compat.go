package pgwire

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/akkio-inc/igloo/internal/engine"
	"github.com/akkio-inc/igloo/internal/sqllex"
	"github.com/akkio-inc/igloo/internal/workload"
)

// postgresOnlySettings are session settings Postgres drivers send when they
// connect but DuckDB does not have. Igloo accepts them as no-ops so those
// drivers can connect; the values Igloo reports at login already match what
// the drivers ask for.
var postgresOnlySettings = map[string]bool{
	"application_name":                    true,
	"bytea_output":                        true,
	"client_encoding":                     true,
	"client_min_messages":                 true,
	"datestyle":                           true,
	"extra_float_digits":                  true,
	"idle_in_transaction_session_timeout": true,
	"intervalstyle":                       true,
	"jit":                                 true,
	"lock_timeout":                        true,
	"standard_conforming_strings":         true,
}

var (
	setStatement = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?"?([a-z_]+)"?\s*(?:=|\bTO\b)`)
	// JDBC sends this on connect. DuckDB has one isolation level, so there is
	// nothing to change.
	setTransactionCharacteristics = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+CHARACTERISTICS\s+AS\s+)?TRANSACTION\s`)
)

// ignoredCommand reports whether query is a Postgres-only command that Igloo
// acknowledges without running, and the command tag to answer with.
func ignoredCommand(query string) (string, bool) {
	query = strings.TrimSpace(stripComments(query))
	if m := setStatement.FindStringSubmatch(query); m != nil && postgresOnlySettings[strings.ToLower(m[1])] {
		return "SET", true
	}
	if setTransactionCharacteristics.MatchString(query) {
		return "SET", true
	}
	return "", false
}

// keywords returns the first n words of query's SQL code in upper case. It
// is used for command tags and messages.
func keywords(query string, n int) []string {
	words := sqllex.CodeWords(query)
	if len(words) > n {
		words = words[:n]
	}
	return words
}

// stripComments removes leading -- and /* */ comments.
func stripComments(query string) string {
	for {
		query = strings.TrimLeftFunc(query, unicode.IsSpace)
		switch {
		case strings.HasPrefix(query, "--"):
			end := strings.IndexByte(query, '\n')
			if end < 0 {
				return ""
			}
			query = query[end+1:]
		case strings.HasPrefix(query, "/*"):
			end := strings.Index(query, "*/")
			if end < 0 {
				return ""
			}
			query = query[end+2:]
		default:
			return query
		}
	}
}

// catalogRewrites adapt PostgreSQL-only syntax that psql and drivers use in
// catalog queries into forms DuckDB understands.
var catalogRewrites = []struct {
	re   *regexp.Regexp
	repl string
}{
	// WHERE relname OPERATOR(pg_catalog.~) '^(t)$'
	{regexp.MustCompile(`(?i)OPERATOR\(\s*pg_catalog\.(~|!~|~~|!~~)\s*\)`), "$1"},
	// ... COLLATE pg_catalog.default
	{regexp.MustCompile(`(?i)\s+COLLATE\s+pg_catalog\."?default"?`), ""},
	// x::pg_catalog.regtype. DuckDB has no reg* object-identifier types;
	// psql only uses them to print names, so text is close enough.
	{regexp.MustCompile(`(?i)::\s*(pg_catalog\.)?(regtype|regclass|regproc|regprocedure|regoper|regnamespace|regrole)\b`), "::VARCHAR"},
	// x::pg_catalog.text
	{regexp.MustCompile(`(?i)::\s*pg_catalog\.`), "::"},
	// pg_catalog.array_to_string(...): DuckDB keeps most functions outside
	// its pg_catalog schema but finds every function, including the ones
	// sessionCatalogSetup adds, when unqualified.
	{regexp.MustCompile(`(?i)\bpg_catalog\.(\w+)\s*\(`), "$1("},
	// FROM generate_series(...) s: PostgreSQL lets s name both the row and
	// its single column; DuckDB needs the column alias spelled out.
	{regexp.MustCompile(`(?i)(\bgenerate_series\((?:[^()]|\([^()]*\))*\))\s+s\s*,`), "$1 AS s(s),"},
	// pg_get_expr(expr, relation, pretty): DuckDB's takes no pretty flag.
	{regexp.MustCompile(`(?i)\bpg_get_expr\(([^(),]+),([^(),]+),\s*(?:true|false)\s*\)`), "pg_get_expr($1,$2)"},
	// a.attnum = ANY(ix.indkey) in a LEFT JOIN, as SQLAlchemy 1.4 lists
	// indexes: DuckDB plans ANY over a list as a subquery, which it cannot
	// join that way.
	{regexp.MustCompile(`(?i)\b(\w+\.\w+)\s*=\s*ANY\s*\(\s*(\w+\.\w+)\s*\)`), "list_contains($2, $1)"},
	// information_schema._pg_expandarray(a), as JDBC lists keys.
	{regexp.MustCompile(`(?i)\binformation_schema\s*\.\s*_pg_expandarray\s*\(`), "_pg_expandarray("},
	// json_build_object(k, v, ...), as SQLAlchemy reads identity columns.
	{regexp.MustCompile(`(?i)\bjson_build_object\s*\(`), "json_object("},
	// typreceive != 0: a type's I/O functions are names here, and a
	// missing one is '-', as PostgreSQL prints it.
	{regexp.MustCompile(`(?i)\b(typinput|typoutput|typreceive|typsend)\s*(!=|<>|=)\s*0\b`), "$1 $2 '-'"},
	// proc.oid = typ.typreceive, as Npgsql looks up array types: the names
	// stand in for the functions' OIDs.
	{regexp.MustCompile(`(?i)\b(\w+)\.oid\s*=\s*(\w+)\.(typinput|typoutput|typreceive|typsend)\b`), "$1.proname = $2.$3"},
	// Relations DuckDB's pg_catalog lacks or lacks columns of, which each
	// session defines as temporary views; see sessionRelations.
	{regexp.MustCompile(`(?i)\bpg_catalog\.(` + strings.Join(sessionRelations, "|") + `)\b`), "$1"},
}

// sessionRelations are the catalog relations DuckDB's pg_catalog lacks or
// lacks columns of. Each session defines them as temporary views (see
// sessionCatalogSetup), which are found only unqualified.
var sessionRelations = []string{
	"pg_class", "pg_attribute", "pg_type", "pg_namespace", "pg_database", "pg_roles", "pg_user", "pg_inherits",
	"pg_policy", "pg_trigger", "pg_statistic_ext", "pg_publication", "pg_publication_rel",
	"pg_publication_namespace", "pg_publication_tables", "pg_partitioned_table", "pg_rewrite", "pg_description",
	"pg_matviews", "pg_collation", "pg_index", "pg_opclass", "pg_settings", "pg_range", "pg_proc",
	"pg_stat_user_tables",
}

// rewriteCatalogNames points names of the system catalogs, quoted or not,
// at the session's versions of them: pg_catalog.name, where name is one of
// sessionRelations or a function, becomes name (Metabase quotes both
// parts), and information_schema.columns becomes
// pg_information_schema_columns, which also fills in udt_schema and udt_name
// for the clients that join them with pg_type, Npgsql and Metabase. Literals,
// comments and every other name are left as written.
func rewriteCatalogNames(query string) string {
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "pg_catalog") && !strings.Contains(lower, "information_schema") {
		return query
	}
	toks := nameTokens(query)
	var out strings.Builder
	last := 0
	for i := 0; i+2 < len(toks); i++ {
		if i > 0 && toks[i-1].dot || !toks[i+1].dot || toks[i+2].name == "" || i+3 < len(toks) && toks[i+3].dot {
			continue
		}
		schema, name := strings.ToLower(toks[i].name), strings.ToLower(toks[i+2].name)
		switch {
		case schema == "information_schema" && name == "columns":
			out.WriteString(query[last:toks[i].start])
			out.WriteString("pg_information_schema_columns")
			last = toks[i+2].end
		case schema == "pg_catalog" && (slices.Contains(sessionRelations, name) || i+3 < len(toks) && query[toks[i+3].start] == '('):
			out.WriteString(query[last:toks[i].start])
			last = toks[i+2].start
		}
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// regclassValue matches a literal or a parameter cast to regclass, as in
// 'pg_class'::regclass, which drivers compare pg_description.classoid and
// dbt compares pg_depend.classid with.
var regclassValue = regexp.MustCompile(`(?i)('(?:[^']|'')*'|\$\d+)\s*::\s*(?:pg_catalog\.)?regclass\b`)

// pgFormat matches PostgreSQL's format() with a literal format string that
// has % placeholders.
var pgFormat = regexp.MustCompile(`(?i)\bformat\s*\(\s*('(?:[^']|'')*%(?:[^']|'')*')`)

// castStart and castToRegclass find CAST(x AS REGCLASS), which SQLAlchemy
// writes.
var (
	castStart      = regexp.MustCompile(`(?i)\bCAST\s*\(`)
	castToRegclass = regexp.MustCompile(`(?is)\s+AS\s+(?:pg_catalog\.)?regclass\s*$`)
)

// catalogOIDs are the system catalogs' OIDs in PostgreSQL, which Igloo's
// pg_description uses too.
var catalogOIDs = map[string]int{
	"pg_type": 1247, "pg_attribute": 1249, "pg_proc": 1255, "pg_class": 1259,
	"pg_database": 1262, "pg_namespace": 2615, "pg_constraint": 2606,
	"pg_depend": 2608, "pg_description": 2609, "pg_index": 2610, "pg_rewrite": 2618,
	"pg_trigger": 2620,
}

// rewriteCatalogQuery applies catalogRewrites to the SQL code of queries
// that mention the system catalogs or reg* types. String literals,
// quoted identifiers and comments are left exactly as written, as is every
// other query.
func rewriteCatalogQuery(query string) string {
	lower := strings.ToLower(query)
	query = rewriteCatalogNames(query)
	if !strings.Contains(lower, "pg_") && !strings.Contains(lower, "reg") && !strings.Contains(lower, "information_schema") {
		return query
	}
	// format('%I.%I', schema, name), as Metabase names relations: DuckDB's
	// format takes {} placeholders, its printf %s.
	query = pgFormat.ReplaceAllStringFunc(query, func(m string) string {
		literal := pgFormat.FindStringSubmatch(m)[1]
		if strings.Contains(literal, "{") {
			return m
		}
		return "printf(" + strings.NewReplacer("%I", "%s", "%L", "%s").Replace(literal)
	})
	// Names and OIDs cast to regclass become OIDs: system catalogs' at once,
	// others through pg_regclass (see sessionCatalogSetup). Before the
	// code-only rewrites, which cannot see a pattern that starts inside a
	// string literal.
	query = regclassValue.ReplaceAllStringFunc(query, func(m string) string {
		value := regclassValue.FindStringSubmatch(m)[1]
		if oid, ok := catalogOIDs[strings.TrimPrefix(strings.ToLower(strings.Trim(value, "'")), "pg_catalog.")]; ok {
			return strconv.Itoa(oid)
		}
		return regclassCall(value)
	})
	query = rewriteRegclassCasts(query)
	return sqllex.MapCode(query, func(code string) string {
		for _, r := range catalogRewrites {
			code = r.re.ReplaceAllString(code, r.repl)
		}
		return code
	})
}

// rewriteRegclassCasts replaces CAST(x AS REGCLASS) in SQL code with
// pg_regclass(x).
func rewriteRegclassCasts(query string) string {
	for from := 0; ; {
		loc := castStart.FindStringIndex(query[from:])
		if loc == nil {
			return query
		}
		start, open := from+loc[0], from+loc[1]-1
		close, ok := closingParen(query, open)
		if !ok || !inCode(query, start) {
			from = open + 1
			continue
		}
		inner := query[open+1 : close]
		m := castToRegclass.FindStringIndex(inner)
		if m == nil {
			from = open + 1
			continue
		}
		query = query[:start] + regclassCall(inner[:m[0]]) + query[close+1:]
		from = start + len("pg_regclass(")
	}
}

var bareParam = regexp.MustCompile(`^\s*\$\d+\s*$`)

// regclassCall calls pg_regclass on x. A parameter is a relation's name: in
// pg_regclass, DuckDB could not tell its type, nor so the call's.
func regclassCall(x string) string {
	if bareParam.MatchString(x) {
		x = strings.TrimSpace(x) + "::VARCHAR"
	}
	return "pg_regclass(" + x + ")"
}

// inCode reports whether query[at] is SQL code rather than a literal,
// quoted identifier or comment.
func inCode(query string, at int) bool {
	pos := 0
	for _, seg := range sqllex.Segments(query) {
		if at < pos+len(seg.Text) {
			return seg.Kind == sqllex.Code
		}
		pos += len(seg.Text)
	}
	return false
}

// toCharPatterns are the template patterns of PostgreSQL's to_char for
// dates and times, longest first, and DuckDB's strftime specifiers for
// them. FM (fill mode) is dropped: strftime does not pad names.
var toCharPatterns = [][2]string{
	{"%", "%%"}, {"FM", ""}, {"HH24", "%H"}, {"HH12", "%I"}, {"HH", "%I"}, {"IYYY", "%G"}, {"YYYY", "%Y"},
	{"YY", "%y"}, {"Month", "%B"}, {"Mon", "%b"}, {"MM", "%m"}, {"MI", "%M"}, {"MS", "%g"}, {"US", "%f"},
	{"SS", "%S"}, {"DDD", "%j"}, {"DD", "%d"}, {"Day", "%A"}, {"Dy", "%a"}, {"IW", "%V"}, {"AM", "%p"},
	{"PM", "%p"},
}

// toCharMacro defines to_char(value, pattern) for dates and timestamps,
// as Looker formats months ('YYYY-MM') and BI tools label dates.
var toCharMacro = func() string {
	format := "pattern"
	for _, p := range toCharPatterns {
		format = fmt.Sprintf("replace(%s, '%s', '%s')", format, p[0], p[1])
	}
	return "CREATE OR REPLACE TEMP MACRO to_char(value, pattern) AS strftime(value, " + format + ")"
}()

// pgRegclassMacro defines pg_regclass(x), which stands in for x::regclass:
// x itself when it is an OID, the OID PostgreSQL gives a system catalog, or
// the OID of the relation named, perhaps with its schema.
var pgRegclassMacro = func() string {
	names := make([]string, 0, len(catalogOIDs))
	for name := range catalogOIDs {
		names = append(names, name)
	}
	slices.Sort(names)
	var cases strings.Builder
	for _, name := range names {
		fmt.Fprintf(&cases, " WHEN '%s' THEN %d", name, catalogOIDs[name])
	}
	return `CREATE OR REPLACE TEMP MACRO pg_regclass(x) AS coalesce(
		TRY_CAST(x AS BIGINT),
		CASE regexp_replace(lower(x::VARCHAR), '^pg_catalog\.', '')` + cases.String() + ` END,
		(SELECT min(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relname = string_split(x::VARCHAR, '.')[-1]
			AND (strpos(x::VARCHAR, '.') = 0 OR n.nspname = string_split(x::VARCHAR, '.')[-2])))`
}()

// sessionUser matches the SQL functions naming the current user. DuckDB
// answers "duckdb" for them; Igloo substitutes the logged-in user. They are
// reserved words, so they cannot be unquoted column names.
var sessionUser = regexp.MustCompile(`(?i)\b(current_user|session_user|current_role)\b(\s*\(\s*\))?`)

// rewriteSessionUser replaces current_user and friends, in SQL code only,
// with the logged-in user's name.
func rewriteSessionUser(query, user string) string {
	lower := strings.ToLower(query)
	if !strings.Contains(lower, "current_user") && !strings.Contains(lower, "session_user") &&
		!strings.Contains(lower, "current_role") {
		return query
	}
	name := "'" + strings.ReplaceAll(user, "'", "''") + "'"
	return sqllex.MapCode(query, func(code string) string {
		return sessionUser.ReplaceAllLiteralString(code, name)
	})
}

// historyViews are the views of the history, the usage, the policies and
// the catalog (see engine.CatalogViews), data retentions and erasure
// requests, in the local database's schema
// engine.HistorySchema.
var historyViews = append([]string{"query_history", "login_history", "task_history", "load_history", "alert_history", "usage",
	"policy_references", "tag_references", "data_retention", "erasure_requests"}, engine.CatalogViews()...)

// informationSchemaViews are the views of information_schema that Igloo
// adds owners, comments and sizes to (see engine.InformationSchemaView).
var informationSchemaViews = []string{"schemata", "tables"}

// isHistoryView reports whether name is one of historyViews.
func isHistoryView(name string) bool {
	return slices.ContainsFunc(historyViews, func(v string) bool { return strings.EqualFold(v, name) })
}

// rewriteHistoryRefs points igloo.query_history, igloo.login_history and
// the other historyViews at the local database, where the views are,
// whatever the session's search path, and information_schema.schemata and
// information_schema.tables at Igloo's versions of them. Literals, comments
// and every other name are left as written.
func rewriteHistoryRefs(query, local string) string {
	lower := strings.ToLower(query)
	if !(strings.Contains(lower, engine.HistorySchema) &&
		slices.ContainsFunc(historyViews, func(v string) bool { return strings.Contains(lower, v) })) &&
		!strings.Contains(lower, "information_schema") {
		return query
	}
	toks := nameTokens(query)
	var out strings.Builder
	last := 0
	for i := 0; i+2 < len(toks); i++ {
		if i > 0 && toks[i-1].dot || !toks[i+1].dot || i+3 < len(toks) && toks[i+3].dot {
			continue
		}
		switch schema, name := toks[i].name, toks[i+2].name; {
		case strings.EqualFold(schema, engine.HistorySchema) && isHistoryView(name):
			out.WriteString(query[last:toks[i].start])
			out.WriteString(quoteIdent(local) + "." + quoteIdent(engine.HistorySchema))
			last = toks[i].end
		case strings.EqualFold(schema, "information_schema") &&
			slices.ContainsFunc(informationSchemaViews, func(v string) bool { return strings.EqualFold(v, name) }):
			out.WriteString(query[last:toks[i].start])
			out.WriteString(quoteIdent(local) + "." + quoteIdent(engine.HistorySchema) + "." +
				quoteIdent(engine.InformationSchemaView(strings.ToLower(name))))
			last = toks[i+2].end
		}
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// nameTok is a name, a dot, or (with neither) anything else in a query.
type nameTok struct {
	start, end int
	name       string
	dot        bool
}

// nameTokens splits query into names, dots and other tokens, for rewrites
// of qualified names. Quoted names are unquoted and white space is left
// out; a comment is a token of its own.
func nameTokens(query string) []nameTok {
	var toks []nameTok
	pos := 0
	for _, seg := range sqllex.Segments(query) {
		start := pos
		pos += len(seg.Text)
		switch seg.Kind {
		case sqllex.Ident:
			if len(seg.Text) >= 2 {
				toks = append(toks, nameTok{start: start, end: pos, name: strings.ReplaceAll(seg.Text[1:len(seg.Text)-1], `""`, `"`)})
			}
		case sqllex.Code:
			for i := 0; i < len(seg.Text); {
				c := seg.Text[i]
				switch {
				case unicode.IsSpace(rune(c)):
					i++
				case c == '.':
					toks = append(toks, nameTok{start: start + i, end: start + i + 1, dot: true})
					i++
				case sqllex.IdentByte(c) && (c < '0' || c > '9'):
					j := i
					for j < len(seg.Text) && sqllex.IdentByte(seg.Text[j]) {
						j++
					}
					toks = append(toks, nameTok{start: start + i, end: start + j, name: seg.Text[i:j]})
					i = j
				default:
					toks = append(toks, nameTok{start: start + i, end: start + i + 1})
					i++
				}
			}
		default:
			toks = append(toks, nameTok{start: start, end: pos})
		}
	}
	return toks
}

// systemSchemas are the schemas DuckDB keeps once, outside every database.
var systemSchemas = []string{"information_schema", "pg_catalog"}

// rewriteSystemSchemaRefs drops the database from catalog.information_schema.x
// and catalog.pg_catalog.x, where catalog is the Iceberg catalog's name.
// PostgreSQL clients qualify these schemas with the database they connected
// to (dbt does, with the database named in its profile); DuckDB has them
// only in its system database. Literals, comments and every other name are
// left as written.
func rewriteSystemSchemaRefs(query, catalog string) string {
	if catalog == "" || !strings.Contains(strings.ToLower(query), strings.ToLower(catalog)) {
		return query
	}
	toks := nameTokens(query)
	var out strings.Builder
	last := 0
	for i := 0; i+3 < len(toks); i++ {
		if !strings.EqualFold(toks[i].name, catalog) || i > 0 && toks[i-1].dot || !toks[i+1].dot ||
			!slices.ContainsFunc(systemSchemas, func(s string) bool { return strings.EqualFold(s, toks[i+2].name) }) ||
			!toks[i+3].dot {
			continue
		}
		out.WriteString(query[last:toks[i].start])
		last = toks[i+2].start
	}
	if last == 0 {
		return query
	}
	out.WriteString(query[last:])
	return out.String()
}

// sessionCatalogSetup creates, for one session, the PostgreSQL catalog
// functions and relations that psql's \d commands need and DuckDB lacks.
// Relations Igloo has no use for (policies, triggers, publications) are
// empty. user owns everything, as far as clients can tell.
func sessionCatalogSetup(user string, superuser bool, version string) []string {
	name := "'" + strings.ReplaceAll(user, "'", "''") + "'"
	return []string{
		"CREATE OR REPLACE TEMP MACRO pg_get_userbyid(id) AS " + name,
		// Tools parse version() for "PostgreSQL <major>.<minor>"; 15 matches
		// the server_version Igloo reports at login.
		"CREATE OR REPLACE TEMP MACRO version() AS 'PostgreSQL 15.0 (Igloo " +
			strings.ReplaceAll(version, "'", "''") + ", DuckDB ' || (SELECT library_version FROM pragma_version()) || ')'",
		"CREATE OR REPLACE TEMP MACRO pg_encoding_to_char(e) AS 'UTF8'",
		// Definitions of objects Iceberg tables do not have (indexes,
		// triggers, rules, partitions, statistics) and storage sizes the
		// catalog does not track.
		`CREATE OR REPLACE TEMP MACRO pg_get_statisticsobjdef_columns(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_statisticsobjdef(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_indexdef(oid) AS NULL::VARCHAR, (oid, col, pretty) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_triggerdef(oid) AS NULL::VARCHAR, (oid, pretty) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_ruledef(oid) AS NULL::VARCHAR, (oid, pretty) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_partkeydef(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_partition_constraintdef(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_serial_sequence(t, c) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_tablespace_location(oid) AS ''`,
		`CREATE OR REPLACE TEMP MACRO pg_relation_is_publishable(oid) AS false`,
		`CREATE OR REPLACE TEMP MACRO pg_table_size(oid) AS NULL::BIGINT`,
		`CREATE OR REPLACE TEMP MACRO pg_relation_size(oid) AS NULL::BIGINT, (oid, fork) AS NULL::BIGINT`,
		`CREATE OR REPLACE TEMP MACRO pg_total_relation_size(oid) AS NULL::BIGINT`,
		`CREATE OR REPLACE TEMP MACRO pg_backend_pid() AS 0`,
		`CREATE OR REPLACE TEMP MACRO pg_is_in_recovery() AS false`,
		`CREATE OR REPLACE TEMP MACRO array_upper(arr, dim) AS len(arr)`,
		`CREATE OR REPLACE TEMP MACRO array_lower(arr, dim) AS CASE WHEN len(arr) > 0 THEN 1 END`,
		`CREATE OR REPLACE TEMP MACRO quote_ident(s) AS '"' || replace(s, '"', '""') || '"'`,
		`CREATE OR REPLACE TEMP MACRO pg_get_function_result(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_function_arguments(oid) AS NULL::VARCHAR`,
		`CREATE OR REPLACE TEMP MACRO pg_get_function_identity_arguments(oid) AS NULL::VARCHAR`,
		// DuckDB's pg_attribute reports DuckDB's internal type ids, which
		// do not match the PostgreSQL OIDs in pg_type, so clients joining
		// the two (JDBC's getColumns, BI schema browsers) lose columns.
		// These versions report the types Igloo actually sends: see
		// wireType.
		pgNamespaceView,
		pgTypeView,
		pgAttributeView,
		formatTypeMacro,
		// Comments, which DuckDB does not read from Iceberg metadata; see
		// igloo_comments().
		pgDescriptionView,
		`CREATE OR REPLACE TEMP MACRO obj_description(obj) AS
			(SELECT description FROM pg_description d WHERE d.objoid = obj AND d.objsubid = 0 LIMIT 1),
			(obj, catalog_name) AS
			(SELECT description FROM pg_description d WHERE d.objoid = obj AND d.objsubid = 0 LIMIT 1)`,
		`CREATE OR REPLACE TEMP MACRO col_description(obj, column_number) AS
			(SELECT description FROM pg_description d WHERE d.objoid = obj AND d.objsubid = column_number LIMIT 1)`,
		// DuckDB's own pg_class and pg_database, plus columns newer psql
		// versions read. pg_namespace lists only the current database's
		// schemas, so relations in a schema of the same name elsewhere, such
		// as the views Igloo keeps in its local database, are given that
		// schema's OID, and temporary ones pg_temp's; otherwise clients
		// joining the two drop them.
		fmt.Sprintf(`CREATE OR REPLACE TEMP VIEW pg_class AS SELECT c.* REPLACE (NULL::VARCHAR[] AS reloptions,
			CASE WHEN s.database_name = 'temp' THEN %d ELSE coalesce(n.oid, c.relnamespace) END AS relnamespace),
			false AS relforcerowsecurity
			FROM pg_catalog.pg_class c
			LEFT JOIN duckdb_schemas() s ON s.oid = c.relnamespace
			LEFT JOIN (SELECT nspname, min(oid) AS oid FROM pg_catalog.pg_namespace GROUP BY nspname) n
				ON n.nspname = s.schema_name`, pgTempNamespace),
		fmt.Sprintf(`CREATE OR REPLACE TEMP MACRO pg_my_temp_schema() AS %d`, pgTempNamespace),
		`CREATE OR REPLACE TEMP MACRO pg_is_other_temp_schema(oid) AS false`,
		`CREATE OR REPLACE TEMP VIEW pg_database AS SELECT *, 6 AS encoding, 0::BIGINT AS datdba,
			'c' AS datlocprovider, 'C' AS datcollate, 'C' AS datctype, NULL::VARCHAR AS daticulocale,
			NULL::VARCHAR AS datlocale, NULL::VARCHAR AS daticurules, NULL::VARCHAR AS datcollversion,
			-1 AS datconnlimit, 0::BIGINT AS dattablespace, NULL::VARCHAR[] AS datacl
			FROM pg_catalog.pg_database`,
		fmt.Sprintf(`CREATE OR REPLACE TEMP VIEW pg_roles AS SELECT
			0::BIGINT AS oid, %[1]s AS rolname, %[2]t AS rolsuper, true AS rolinherit,
			%[2]t AS rolcreaterole, %[2]t AS rolcreatedb, true AS rolcanlogin,
			false AS rolreplication, -1 AS rolconnlimit, NULL::TIMESTAMPTZ AS rolvaliduntil,
			false AS rolbypassrls, NULL::VARCHAR[] AS rolconfig`, name, superuser),
		`CREATE OR REPLACE TEMP VIEW pg_user AS SELECT rolname AS usename, 0::BIGINT AS usesysid,
			rolcreatedb AS usecreatedb, rolsuper AS usesuper FROM pg_roles`,
		`CREATE OR REPLACE TEMP VIEW pg_inherits AS SELECT 0::BIGINT AS inhrelid, 0::BIGINT AS inhparent,
			0 AS inhseqno, false AS inhdetachpending WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_policy AS SELECT 0::BIGINT AS oid, ''::VARCHAR AS polname,
			0::BIGINT AS polrelid, ''::VARCHAR AS polcmd, true AS polpermissive, []::BIGINT[] AS polroles,
			NULL::VARCHAR AS polqual, NULL::VARCHAR AS polwithcheck WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_trigger AS SELECT 0::BIGINT AS oid, 0::BIGINT AS tgrelid,
			''::VARCHAR AS tgname, false AS tgisinternal, ''::VARCHAR AS tgenabled, 0::BIGINT AS tgparentid WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_statistic_ext AS SELECT 0::BIGINT AS oid, 0::BIGINT AS stxrelid,
			''::VARCHAR AS stxname, 0::BIGINT AS stxnamespace, []::VARCHAR[] AS stxkind, -1 AS stxstattarget WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_publication AS SELECT 0::BIGINT AS oid, ''::VARCHAR AS pubname,
			false AS puballtables WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_publication_rel AS SELECT 0::BIGINT AS oid, 0::BIGINT AS prpubid,
			0::BIGINT AS prrelid, NULL::VARCHAR AS prqual, NULL::SMALLINT[] AS prattrs WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_publication_namespace AS SELECT 0::BIGINT AS oid, 0::BIGINT AS pnpubid,
			0::BIGINT AS pnnspid WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_publication_tables AS SELECT ''::VARCHAR AS pubname,
			''::VARCHAR AS schemaname, ''::VARCHAR AS tablename WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_partitioned_table AS SELECT 0::BIGINT AS partrelid WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_rewrite AS SELECT 0::BIGINT AS oid, 0::BIGINT AS ev_class,
			''::VARCHAR AS rulename WHERE false`,
		// dbt lists materialized views along with tables and views.
		`CREATE OR REPLACE TEMP VIEW pg_matviews AS SELECT ''::VARCHAR AS schemaname, ''::VARCHAR AS matviewname,
			''::VARCHAR AS matviewowner, NULL::VARCHAR AS tablespace, false AS hasindexes,
			false AS ispopulated, ''::VARCHAR AS definition WHERE false`,
		// Collations and indexes, with the columns SQLAlchemy reads.
		`CREATE OR REPLACE TEMP VIEW pg_collation AS SELECT 0::BIGINT AS oid, ''::VARCHAR AS collname,
			0::BIGINT AS collnamespace, 0::BIGINT AS collowner, 'c' AS collprovider, true AS collisdeterministic,
			6 AS collencoding, 'C' AS collcollate, 'C' AS collctype, NULL::VARCHAR AS colliculocale,
			NULL::VARCHAR AS collicurules, NULL::VARCHAR AS collversion WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_index AS SELECT 0::BIGINT AS indexrelid, 0::BIGINT AS indrelid,
			0::SMALLINT AS indnatts, 0::SMALLINT AS indnkeyatts, false AS indisunique, false AS indnullsnotdistinct,
			false AS indisprimary, false AS indisexclusion, true AS indimmediate, false AS indisclustered,
			true AS indisvalid, false AS indcheckxmin, true AS indisready, true AS indislive, false AS indisreplident,
			[]::SMALLINT[] AS indkey, []::BIGINT[] AS indcollation, []::BIGINT[] AS indclass,
			[]::SMALLINT[] AS indoption, NULL::VARCHAR AS indexprs, NULL::VARCHAR AS indpred WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_information_schema_columns AS SELECT * REPLACE (
			'pg_catalog' AS udt_schema, (SELECT typname FROM pg_type WHERE oid = (` + pgTypeOID + `)) AS udt_name)
			FROM information_schema.columns`,
		// Metabase's row count estimates; Iceberg tables have none here.
		`CREATE OR REPLACE TEMP VIEW pg_stat_user_tables AS SELECT 0::BIGINT AS relid, ''::VARCHAR AS schemaname,
			''::VARCHAR AS relname, 0::BIGINT AS seq_scan, 0::BIGINT AS idx_scan, 0::BIGINT AS n_live_tup,
			0::BIGINT AS n_dead_tup, NULL::TIMESTAMPTZ AS last_vacuum, NULL::TIMESTAMPTZ AS last_analyze WHERE false`,
		// Npgsql loads types with their ranges and I/O functions.
		`CREATE OR REPLACE TEMP VIEW pg_range AS SELECT 0::BIGINT AS rngtypid, 0::BIGINT AS rngsubtype,
			0::BIGINT AS rngmultitypid, 0::BIGINT AS rngcollation, 0::BIGINT AS rngsubopc,
			NULL::VARCHAR AS rngcanonical, NULL::VARCHAR AS rngsubdiff WHERE false`,
		`CREATE OR REPLACE TEMP VIEW pg_proc AS SELECT * FROM pg_catalog.pg_proc
			UNION ALL BY NAME SELECT proname, ` + fmt.Sprint(pgCatalogNamespace) + `::BIGINT AS pronamespace,
				'f' AS prokind, false AS proretset
			FROM (SELECT typreceive FROM pg_type UNION ALL SELECT typsend FROM pg_type) t(proname)`,
		`CREATE OR REPLACE TEMP VIEW pg_opclass AS SELECT 0::BIGINT AS oid, 0::BIGINT AS opcmethod, ''::VARCHAR AS opcname,
			0::BIGINT AS opcnamespace, 0::BIGINT AS opcowner, 0::BIGINT AS opcfamily, 0::BIGINT AS opcintype,
			false AS opcdefault, 0::BIGINT AS opckeytype WHERE false`,
		// psycopg looks types up by name, as SQLAlchemy does for hstore.
		`CREATE OR REPLACE TEMP MACRO to_regtype(name) AS (SELECT oid FROM pg_type WHERE typname = name)`,
		pgRegclassMacro,
		pgSettingMacro,
		// DuckDB's settings and the PostgreSQL ones drivers read, such as
		// JDBC's max_index_keys.
		`CREATE OR REPLACE TEMP VIEW pg_settings AS
			SELECT name, setting, NULL::VARCHAR AS unit, NULL::VARCHAR AS category, short_desc,
				NULL::VARCHAR AS extra_desc, 'user' AS context, vartype, 'default' AS source
			FROM pg_catalog.pg_settings
			UNION ALL SELECT * FROM (VALUES ('max_index_keys', '32', NULL, NULL, 'Shows the maximum number of index keys.',
				NULL, 'internal', 'integer', 'default')) t
			UNION ALL SELECT name, pg_setting(name), NULL, NULL, '', NULL, 'internal', 'string', 'default'
				FROM (VALUES ` + pgSettingNames() + `) t(name)`,
		// A row (x, n) per element of an array: its value and position.
		`CREATE OR REPLACE TEMP MACRO _pg_expandarray(arr) AS
			unnest(list_transform(range(1, len(arr) + 1), i -> {'x': arr[i], 'n': i}))`,
		toCharMacro,
		`CREATE OR REPLACE TEMP MACRO pg_get_viewdef(oid) AS (SELECT sql FROM duckdb_views() WHERE view_oid = oid),
			(oid, pretty) AS (SELECT sql FROM duckdb_views() WHERE view_oid = oid)`,
	}
}

// showSetting matches SHOW of one setting, SHOW TRANSACTION ISOLATION LEVEL
// included.
var showSetting = regexp.MustCompile(`(?is)^\s*SHOW\s+(TRANSACTION\s+ISOLATION\s+LEVEL|"?[a-z_]+"?)\s*;?\s*$`)

// pgSettings are the PostgreSQL settings that drivers and tools read with
// SHOW, as SQLAlchemy and JDBC do the isolation level, under PostgreSQL's
// names for them. DuckDB has none of them: SHOW name describes a table.
// An empty value is read from the session (see showCommand).
var pgSettings = map[string]struct{ name, value string }{
	// DuckDB transactions see a snapshot, as in PostgreSQL's REPEATABLE
	// READ, and fail with 40001 when another commits a conflicting change.
	"transaction isolation level":   {"transaction_isolation", "repeatable read"},
	"transaction_isolation":         {"transaction_isolation", "repeatable read"},
	"default_transaction_isolation": {"default_transaction_isolation", "repeatable read"},
	"transaction_read_only":         {"transaction_read_only", "off"},
	"default_transaction_read_only": {"default_transaction_read_only", "off"},
	"standard_conforming_strings":   {"standard_conforming_strings", "on"},
	"server_version":                {"server_version", "15.0"},
	"server_version_num":            {"server_version_num", "150000"},
	"server_encoding":               {"server_encoding", "UTF8"},
	"client_encoding":               {"client_encoding", "UTF8"},
	"datestyle":                     {"DateStyle", "ISO, MDY"},
	"intervalstyle":                 {"IntervalStyle", "postgres"},
	"integer_datetimes":             {"integer_datetimes", "on"},
	"max_identifier_length":         {"max_identifier_length", "63"},
	"lc_collate":                    {"lc_collate", "C"},
	"lc_ctype":                      {"lc_ctype", "C"},
	"default_table_access_method":   {"default_table_access_method", "heap"},
	"timezone":                      {"TimeZone", ""},
	"search_path":                   {"search_path", ""},
	"application_name":              {"application_name", ""},
	"is_superuser":                  {"is_superuser", ""},
}

// currentSetting matches current_setting('name') and current_setting($n).
var currentSetting = regexp.MustCompile(`(?i)\bcurrent_setting\s*\(\s*('[^']*'|\$\d+)(\s*::\s*\w+)?\s*\)`)

// rewriteCurrentSetting answers current_setting of the PostgreSQL settings
// in pgSettings, which DuckDB does not have, and current_setting of a
// parameter, which DuckDB refuses (SQLAlchemy reads
// default_table_access_method so): through pg_setting (see
// sessionCatalogSetup). DuckDB's own settings, named in literals, are left
// to DuckDB.
func rewriteCurrentSetting(query string) string {
	if !strings.Contains(strings.ToLower(query), "current_setting") {
		return query
	}
	return currentSetting.ReplaceAllStringFunc(query, func(m string) string {
		arg := currentSetting.FindStringSubmatch(m)[1]
		if strings.HasPrefix(arg, "$") {
			return "pg_setting(" + arg + "::VARCHAR)"
		}
		if s, ok := pgSettings[strings.ToLower(strings.Trim(arg, "'"))]; ok && s.value != "" {
			return "pg_setting(" + arg + ")"
		}
		return m
	})
}

// staticSettings lists the settings in pgSettings that do not depend on the
// session, sorted.
func staticSettings() []string {
	var names []string
	for name, s := range pgSettings {
		if s.value != "" && !strings.Contains(name, " ") {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// pgSettingNames lists staticSettings as SQL VALUES rows.
func pgSettingNames() string {
	rows := make([]string, 0, len(pgSettings))
	for _, name := range staticSettings() {
		rows = append(rows, "('"+pgSettings[name].name+"')")
	}
	return strings.Join(rows, ", ")
}

// pgSettingMacro defines pg_setting(name), the value of a setting in
// pgSettings that does not depend on the session, or NULL.
var pgSettingMacro = func() string {
	var cases strings.Builder
	for _, name := range staticSettings() {
		fmt.Fprintf(&cases, " WHEN '%s' THEN '%s'", name, pgSettings[name].value)
	}
	return "CREATE OR REPLACE TEMP MACRO pg_setting(name) AS CASE lower(name)" + cases.String() + " END"
}()

var (
	setStatementTimeout   = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?statement_timeout\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetStatementTimeout = regexp.MustCompile(`(?is)^\s*RESET\s+statement_timeout\s*;?\s*$`)
	showStatementTimeout  = regexp.MustCompile(`(?is)^\s*SHOW\s+statement_timeout\s*;?\s*$`)
)

// parseStatementTimeout parses a PostgreSQL statement_timeout value: a
// number of milliseconds, or a quoted number with a unit ('30s', '5min').
// Zero means no timeout; DEFAULT means the server default.
func parseStatementTimeout(value string) (d time.Duration, isDefault bool, err error) {
	if strings.EqualFold(strings.Trim(strings.TrimSpace(value), `'"`), "default") {
		return 0, true, nil
	}
	d, err = workload.ParseTimeout(value)
	return d, false, err
}

// formatTimeout renders a timeout the way PostgreSQL's SHOW does.
func formatTimeout(d time.Duration) string { return workload.FormatTimeout(d) }

// pgCatalogNamespace is the OID clients see for the pg_catalog schema, which
// DuckDB's pg_namespace does not list.
const pgCatalogNamespace = 11

// pgTempNamespace is the OID clients see for the schema of the session's
// temporary objects, which PostgreSQL calls pg_temp_N and DuckDB keeps in
// its temp database. Ibis looks temporary views up there.
const pgTempNamespace = 99

// pgNamespaceView adds to DuckDB's pg_namespace, which lists the current
// database's schemas, pg_catalog, pg_temp and the local database's schema
// of the history views, so clients list those as igloo.query_history and
// igloo.login_history.
var pgNamespaceView = fmt.Sprintf(`CREATE OR REPLACE TEMP VIEW pg_namespace AS
	SELECT * FROM pg_catalog.pg_namespace
	UNION ALL SELECT %d, 'pg_catalog', 0, NULL
	UNION ALL SELECT %d, 'pg_temp', 0, NULL
	UNION ALL SELECT oid, schema_name, 0, NULL FROM duckdb_schemas()
		WHERE schema_name = '%s' AND database_name NOT IN (current_database(), 'temp', 'system')
		AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = '%[3]s')`,
	pgCatalogNamespace, pgTempNamespace, engine.HistorySchema)

// pgTypes lists the PostgreSQL types Igloo sends: oid, name, length and
// category.
var pgTypes = []struct {
	oid      int
	name     string
	length   int
	category string
}{
	{16, "bool", 1, "B"}, {17, "bytea", -1, "U"}, {18, "char", 1, "S"}, {19, "name", 64, "S"},
	{20, "int8", 8, "N"}, {21, "int2", 2, "N"}, {23, "int4", 4, "N"}, {25, "text", -1, "S"},
	{26, "oid", 4, "N"}, {114, "json", -1, "U"}, {700, "float4", 4, "N"}, {701, "float8", 8, "N"},
	{1042, "bpchar", -1, "S"}, {1043, "varchar", -1, "S"}, {1082, "date", 4, "D"},
	{1083, "time", 8, "D"}, {1114, "timestamp", 8, "D"}, {1184, "timestamptz", 8, "D"},
	{1186, "interval", 16, "T"}, {1266, "timetz", 12, "D"}, {1700, "numeric", -1, "N"},
	{2950, "uuid", 16, "U"},
}

// pgTypeView lists pgTypes, with PostgreSQL's names for their binary I/O
// functions, by which ADBC tells types apart.
var pgTypeView = func() string {
	rows := make([]string, len(pgTypes))
	for i, t := range pgTypes {
		rows[i] = fmt.Sprintf("(%d, '%s', %d, '%s')", t.oid, t.name, t.length, t.category)
	}
	return fmt.Sprintf(`CREATE OR REPLACE TEMP VIEW pg_type AS SELECT
		oid::BIGINT AS oid, typname, %d::BIGINT AS typnamespace, 0::BIGINT AS typowner,
		typlen::SMALLINT AS typlen, typlen > 0 AS typbyval, 'b' AS typtype, typcategory,
		false AS typispreferred, true AS typisdefined, ',' AS typdelim, 0::BIGINT AS typrelid,
		0::BIGINT AS typelem, 0::BIGINT AS typarray, typname || 'in' AS typinput,
		typname || 'out' AS typoutput,
		typname || CASE WHEN oid IN (114, 1082, 1083, 1114, 1184, 1186, 1266, 1700, 2950) THEN '_recv' ELSE 'recv' END AS typreceive,
		typname || CASE WHEN oid IN (114, 1082, 1083, 1114, 1184, 1186, 1266, 1700, 2950) THEN '_send' ELSE 'send' END AS typsend,
		'd' AS typalign, 'p' AS typstorage, false AS typnotnull,
		0::BIGINT AS typbasetype, -1 AS typtypmod, 0 AS typndims, 0::BIGINT AS typcollation,
		NULL::VARCHAR AS typdefault, NULL::VARCHAR[] AS typacl
		FROM (VALUES %s) t(oid, typname, typlen, typcategory)`,
		pgCatalogNamespace, strings.Join(rows, ", "))
}()

// pgTypeOID maps a duckdb_columns() data_type to the PostgreSQL OID that
// wireType sends for it.
const pgTypeOID = `CASE
	WHEN data_type = 'BOOLEAN' THEN 16
	WHEN data_type IN ('TINYINT', 'SMALLINT', 'UTINYINT') THEN 21
	WHEN data_type IN ('INTEGER', 'USMALLINT') THEN 23
	WHEN data_type IN ('BIGINT', 'UINTEGER') THEN 20
	WHEN data_type IN ('UBIGINT', 'HUGEINT', 'UHUGEINT', 'BIGNUM') OR data_type LIKE 'DECIMAL%' THEN 1700
	WHEN data_type = 'FLOAT' THEN 700
	WHEN data_type = 'DOUBLE' THEN 701
	WHEN data_type = 'BLOB' THEN 17
	WHEN data_type = 'UUID' THEN 2950
	WHEN data_type = 'DATE' THEN 1082
	WHEN data_type = 'TIME' THEN 1083
	WHEN data_type = 'TIMESTAMP WITH TIME ZONE' THEN 1184
	WHEN data_type LIKE 'TIMESTAMP%' THEN 1114
	WHEN data_type = 'INTERVAL' THEN 1186
	WHEN data_type = 'JSON' OR data_type LIKE '%[]' OR data_type LIKE '%]'
		OR data_type LIKE 'STRUCT%' OR data_type LIKE 'MAP%' OR data_type LIKE 'UNION%' THEN 114
	ELSE 25
END`

// pgAttributeView is DuckDB's pg_attribute with PostgreSQL type OIDs and
// numeric type modifiers ((precision << 16 | scale) + 4).
var pgAttributeView = `CREATE OR REPLACE TEMP VIEW pg_attribute AS SELECT
	table_oid AS attrelid, column_name AS attname, (` + pgTypeOID + `)::BIGINT AS atttypid,
	0 AS attstattarget, -1::SMALLINT AS attlen, column_index AS attnum, 0 AS attndims, -1 AS attcacheoff,
	CASE WHEN data_type LIKE 'DECIMAL%' THEN (numeric_precision << 16 | numeric_scale) + 4 ELSE -1 END AS atttypmod,
	false AS attbyval, 'x' AS attstorage, 'i' AS attalign, NOT is_nullable AS attnotnull,
	column_default IS NOT NULL AS atthasdef, false AS atthasmissing, '' AS attidentity,
	'' AS attgenerated, false AS attisdropped, true AS attislocal, 0 AS attinhcount,
	0::BIGINT AS attcollation, NULL::VARCHAR AS attcompression, NULL::VARCHAR[] AS attacl,
	NULL::VARCHAR[] AS attoptions, NULL::VARCHAR[] AS attfdwoptions, NULL::VARCHAR AS attmissingval
	FROM duckdb_columns()`

// formatTypeMacro renders type names as PostgreSQL does, for the OIDs in
// pgTypeView.
const formatTypeMacro = `CREATE OR REPLACE TEMP MACRO format_type(type_oid, typemod) AS CASE type_oid
	WHEN 16 THEN 'boolean' WHEN 17 THEN 'bytea' WHEN 20 THEN 'bigint' WHEN 21 THEN 'smallint'
	WHEN 23 THEN 'integer' WHEN 25 THEN 'text' WHEN 114 THEN 'json' WHEN 700 THEN 'real'
	WHEN 701 THEN 'double precision' WHEN 1043 THEN 'character varying' WHEN 1082 THEN 'date'
	WHEN 1083 THEN 'time without time zone' WHEN 1114 THEN 'timestamp without time zone'
	WHEN 1184 THEN 'timestamp with time zone' WHEN 1186 THEN 'interval' WHEN 2950 THEN 'uuid'
	WHEN 1700 THEN CASE WHEN typemod > 4
		THEN 'numeric(' || ((typemod - 4) >> 16) || ',' || ((typemod - 4) & 65535) || ')'
		ELSE 'numeric' END
	ELSE (SELECT typname FROM pg_type WHERE oid = type_oid)
END`

// pgDescriptionView lists the comments igloo_comments() reports under the
// OIDs that pg_class, pg_attribute and pg_namespace give the tables, views,
// columns and schemas.
const pgDescriptionView = `CREATE OR REPLACE TEMP VIEW pg_description AS
	SELECT t.table_oid::BIGINT AS objoid, 1259::BIGINT AS classoid, 0 AS objsubid, c.comment AS description
	FROM igloo_comments() c JOIN duckdb_tables() t ON t.database_name = c.table_catalog
		AND t.schema_name = c.table_schema AND t.table_name = c.table_name
	WHERE c.column_name IS NULL
	UNION ALL
	SELECT v.view_oid::BIGINT, 1259::BIGINT, 0, c.comment
	FROM igloo_comments() c JOIN duckdb_views() v ON v.database_name = c.table_catalog
		AND v.schema_name = c.table_schema AND v.view_name = c.table_name
	WHERE c.column_name IS NULL
	UNION ALL
	SELECT col.table_oid::BIGINT, 1259::BIGINT, col.column_index, c.comment
	FROM igloo_comments() c JOIN duckdb_columns() col ON col.database_name = c.table_catalog
		AND col.schema_name = c.table_schema AND col.table_name = c.table_name
		AND lower(col.column_name) = lower(c.column_name)
	WHERE c.column_name IS NOT NULL
	UNION ALL
	SELECT s.oid::BIGINT, 2615::BIGINT, 0, c.comment
	FROM igloo_comments() c JOIN duckdb_schemas() s ON s.database_name = c.table_catalog
		AND s.schema_name = c.table_schema
	WHERE c.table_name IS NULL`
