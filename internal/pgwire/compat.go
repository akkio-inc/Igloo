package pgwire

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/akkio-inc/igloo/internal/sqllex"
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
	// Relations DuckDB's pg_catalog lacks or lacks columns of; each session
	// defines them as temporary views (see sessionCatalogSetup), which are
	// found only unqualified.
	{regexp.MustCompile(`(?i)\bpg_catalog\.(pg_class|pg_attribute|pg_type|pg_namespace|pg_database|pg_roles|pg_user|pg_inherits|pg_policy|pg_trigger|pg_statistic_ext|pg_publication|pg_publication_rel|pg_publication_namespace|pg_publication_tables|pg_partitioned_table|pg_rewrite|pg_description)\b`), "$1"},
}

// pgClassRegclass matches 'pg_class'::regclass, which drivers compare
// pg_description.classoid with: pg_class's OID in PostgreSQL, as Igloo's
// pg_description has it.
var pgClassRegclass = regexp.MustCompile(`(?i)'pg_class'\s*::\s*(pg_catalog\.)?regclass\b`)

// rewriteCatalogQuery applies catalogRewrites to the SQL code of queries
// that mention pg_catalog. String literals, quoted identifiers and comments
// are left exactly as written, as is every other query.
func rewriteCatalogQuery(query string) string {
	if !strings.Contains(strings.ToLower(query), "pg_catalog") {
		return query
	}
	// Before the code-only rewrites, which cannot see a pattern that starts
	// inside a string literal.
	query = pgClassRegclass.ReplaceAllString(query, "1259")
	return sqllex.MapCode(query, func(code string) string {
		for _, r := range catalogRewrites {
			code = r.re.ReplaceAllString(code, r.repl)
		}
		return code
	})
}

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
	}
}

var (
	setStatementTimeout   = regexp.MustCompile(`(?is)^\s*SET\s+(?:SESSION\s+|LOCAL\s+)?statement_timeout\s*(?:=|\bTO\b)\s*(.*?)\s*;?\s*$`)
	resetStatementTimeout = regexp.MustCompile(`(?is)^\s*RESET\s+statement_timeout\s*;?\s*$`)
	showStatementTimeout  = regexp.MustCompile(`(?is)^\s*SHOW\s+statement_timeout\s*;?\s*$`)
	timeoutValue          = regexp.MustCompile(`(?i)^(\d+)\s*(us|ms|s|min|h|d)?$`)
)

// parseStatementTimeout parses a PostgreSQL statement_timeout value: a
// number of milliseconds, or a quoted number with a unit ('30s', '5min').
// Zero means no timeout; DEFAULT means the server default.
func parseStatementTimeout(value string) (d time.Duration, isDefault bool, err error) {
	value = strings.Trim(strings.TrimSpace(value), `'"`)
	if strings.EqualFold(value, "default") {
		return 0, true, nil
	}
	m := timeoutValue.FindStringSubmatch(strings.TrimSpace(value))
	if m == nil {
		return 0, false, errors.New(`invalid value for statement_timeout; use milliseconds or a unit, e.g. '30s' or '5min'`)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, false, err
	}
	unit := map[string]time.Duration{
		"": time.Millisecond, "us": time.Microsecond, "ms": time.Millisecond, "s": time.Second,
		"min": time.Minute, "h": time.Hour, "d": 24 * time.Hour,
	}[strings.ToLower(m[2])]
	return time.Duration(n) * unit, false, nil
}

// formatTimeout renders a timeout the way PostgreSQL's SHOW does.
func formatTimeout(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dmin", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return fmt.Sprintf("%dms", d/time.Millisecond)
}

// pgCatalogNamespace is the OID clients see for the pg_catalog schema, which
// DuckDB's pg_namespace does not list.
const pgCatalogNamespace = 11

// pgTempNamespace is the OID clients see for the schema of the session's
// temporary objects, which PostgreSQL calls pg_temp_N and DuckDB keeps in
// its temp database. Ibis looks temporary views up there.
const pgTempNamespace = 99

var pgNamespaceView = fmt.Sprintf(`CREATE OR REPLACE TEMP VIEW pg_namespace AS
	SELECT * FROM pg_catalog.pg_namespace
	UNION ALL SELECT %d, 'pg_catalog', 0, NULL
	UNION ALL SELECT %d, 'pg_temp', 0, NULL`, pgCatalogNamespace, pgTempNamespace)

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
		typname || 'out' AS typoutput, 'd' AS typalign, 'p' AS typstorage, false AS typnotnull,
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
// OIDs that pg_class and pg_attribute give the tables, views and columns.
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
	WHERE c.column_name IS NOT NULL`
