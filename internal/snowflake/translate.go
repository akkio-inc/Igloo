package snowflake

import (
	"fmt"
	"strconv"
	"strings"
)

// Options configure a translation.
type Options struct {
	// IsSetting reports whether name, lower-cased, is a setting of the
	// session (DuckDB's, PostgreSQL's or Igloo's), which SET changes as in
	// DuckDB. SET of any other name sets a session variable, as in
	// Snowflake. Nil means no name is a setting.
	IsSetting func(name string) bool
	// Variable returns the value, as text, of the session variable name
	// (lower-cased), for IDENTIFIER($name). Nil means none are known.
	Variable func(name string) (value string, ok bool, err error)
	// Sketches says the session has DuckDB's datasketches extension, with
	// which APPROX_COUNT_DISTINCT and HLL count as accurately as in
	// Snowflake; without it they are DuckDB's approx_count_distinct.
	Sketches bool
}

// Result is a translated statement.
type Result struct {
	// SQL is the statement in DuckDB's dialect. It is empty when Command
	// is set.
	SQL string
	// Command, when set, means the statement changes nothing in Igloo (an
	// ALTER SESSION of a parameter Igloo does not have, USE WAREHOUSE) and
	// is acknowledged with this command tag.
	Command string
	// Notices are warnings for the client: what the translation ignored or
	// changed.
	Notices []string
	// Quoted are the names the statement writes as quoted identifiers.
	// Result columns named by one of them keep their case (see
	// ColumnName); Snowflake folds every other name to upper case.
	Quoted map[string]bool
	// Untranslated says the statement was passed on as written: it reads
	// PostgreSQL's catalog, as drivers and BI tools do to list tables.
	Untranslated bool
}

// translator holds the state of one statement's translation.
type translator struct {
	src     string
	opts    Options
	notices []string
	quoted  map[string]bool
	// flatten holds the lower-cased aliases of the statement's FLATTENs,
	// whose VALUE and THIS columns are semi-structured; anon counts those
	// without an alias, whose columns are reached unqualified.
	flatten map[string]bool
	anon    int
	// grouped holds the APPROX_COUNT_DISTINCT and HLL calls, by their
	// names' nodes, of selects with GROUP BY (see collectGrouped).
	grouped map[*node]bool
	// gen numbers the names the translation makes up.
	gen int
}

func (t *translator) notice(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	for _, n := range t.notices {
		if n == msg {
			return
		}
	}
	t.notices = append(t.notices, msg)
}

// Translate translates one Snowflake SQL statement to DuckDB's dialect. A
// trailing semicolon is allowed. Statements Igloo carries out itself
// (CREATE USER, COPY INTO, CREATE TASK and the like) come back with their
// expressions, literals and comments translated and their syntax as
// written.
func Translate(sql string, opts Options) (Result, error) {
	toks, err := lex(sql)
	if err != nil {
		return Result{}, err
	}
	if readsPostgresCatalog(toks) {
		return Result{SQL: sql, Untranslated: true}, nil
	}
	nodes, err := build(toks)
	if err != nil {
		return Result{}, err
	}
	t := &translator{src: sql, opts: opts, quoted: map[string]bool{}, flatten: map[string]bool{}, grouped: map[*node]bool{}}
	for _, tok := range toks {
		if tok.kind == kQuoted {
			t.quoted[tok.val] = true
		}
	}
	t.collectFlattens(nodes)
	t.collectGrouped(nodes, false)
	out, cmd, err := t.statement(nodes)
	if err != nil {
		return Result{}, err
	}
	res := Result{Command: cmd, Notices: t.notices, Quoted: t.quoted}
	if cmd == "" {
		res.SQL = strings.TrimRight(render(out), " \t\r\n")
	}
	return res, nil
}

// readsPostgresCatalog reports whether a statement reads PostgreSQL's
// catalog (pg_catalog, pg_class and the like) or uses PostgreSQL-only
// syntax, as drivers' and BI tools' metadata queries do. Such statements
// are PostgreSQL SQL whatever the session's dialect, and pass untranslated.
func readsPostgresCatalog(toks []token) bool {
	for i, t := range toks {
		switch t.kind {
		case kWord:
			w := strings.ToLower(t.text)
			if w == "pg_catalog" || strings.HasPrefix(w, "pg_") && len(w) > 3 ||
				w == "operator" && i+1 < len(toks) && toks[i+1].text == "(" {
				return true
			}
		case kQuoted:
			if strings.EqualFold(t.val, "pg_catalog") {
				return true
			}
		case kString:
			if t.raw {
				return true
			}
		}
	}
	return false
}

// statement translates a statement's nodes. A non-empty command means the
// statement runs nothing.
func (t *translator) statement(nodes []*node) ([]*node, string, error) {
	idx := sig(nodes)
	if len(idx) == 0 {
		return nodes, "", nil
	}
	w := func(k int) string {
		if k < len(idx) {
			return nodes[idx[k]].word()
		}
		return ""
	}
	switch w(0) {
	case "SET":
		return t.setStatement(nodes, idx)
	case "UNSET":
		return t.unsetStatement(nodes, idx)
	case "ALTER":
		switch w(1) {
		case "SESSION":
			return t.alterSession(nodes, idx)
		case "TABLE":
			return t.alterTable(nodes, idx)
		case "WAREHOUSE", "USER", "ACCOUNT":
			if w(1) != "USER" {
				return nil, "", unsupported("ALTER "+w(1), "Igloo has no warehouses or accounts to alter")
			}
		}
	case "USE":
		return t.useStatement(nodes, idx)
	case "CREATE":
		return t.createStatement(nodes, idx)
	case "DESC", "DESCRIBE":
		if w(1) == "TABLE" || w(1) == "VIEW" {
			out := append([]*node{raw("DESCRIBE")}, nodes[idx[1]+1:]...)
			return t.expressions(out)
		}
		if w(0) == "DESC" {
			return t.expressions(append([]*node{raw("DESCRIBE")}, nodes[idx[0]+1:]...))
		}
	case "SHOW":
		if w(1) == "VARIABLES" && len(idx) == 2 {
			return []*node{raw("SELECT name, value, type FROM duckdb_variables() ORDER BY name")}, "", nil
		}
	case "INSERT":
		switch w(1) {
		case "OVERWRITE":
			return nil, "", unsupported("INSERT OVERWRITE",
				"replace the table with CREATE OR REPLACE TABLE ... AS SELECT, or DELETE its rows and then INSERT")
		case "ALL", "FIRST":
			return nil, "", unsupported("INSERT "+w(1)+" (inserting into several tables)", "send one INSERT per table")
		}
	case "BEGIN":
		switch w(1) {
		case "", "TRANSACTION", "WORK", "NAME":
		default:
			return nil, "", unsupported("Snowflake Scripting (BEGIN ... END blocks)", "send the block's statements one at a time")
		}
	case "DECLARE", "EXECUTE", "CALL", "LET", "FOR", "WHILE", "REPEAT", "LOOP", "RETURN", "EXCEPTION":
		return nil, "", unsupported("Snowflake Scripting and stored procedures ("+w(0)+")", "send the statements one at a time")
	case "PUT", "GET", "LIST", "LS", "REMOVE", "RM":
		return nil, "", unsupported(w(0)+" (stages)",
			"read files with read_parquet or read_csv, or load them with COPY INTO from a location in IGLOO_ALLOWED_LOCATIONS")
	case "GRANT", "REVOKE":
		return t.grantStatement(nodes, idx)
	case "UNDROP":
		return nil, "", unsupported("UNDROP", "query an earlier snapshot of a table with AT (TIMESTAMP => ...) instead")
	}
	return t.expressions(nodes)
}

// expressions translates the expressions and clauses of a statement.
func (t *translator) expressions(nodes []*node) ([]*node, string, error) {
	out, err := t.seq(nodes, true)
	if err != nil {
		return nil, "", err
	}
	out, err = t.resultNames(out)
	return out, "", err
}

// setStatement translates SET. SET name = expr sets a session variable,
// except for names of the session's settings (search_path, timezone,
// statement_timeout, ...), which keep their meaning.
func (t *translator) setStatement(nodes []*node, idx []int) ([]*node, string, error) {
	if len(idx) < 2 {
		return t.expressions(nodes)
	}
	target := nodes[idx[1]]
	if target.isGroup('(') {
		return nil, "", unsupported("SET (a, b) = (...), setting several variables at once", "set one variable per statement")
	}
	switch target.word() {
	case "SESSION", "LOCAL", "GLOBAL", "VARIABLE":
		return t.expressions(nodes)
	}
	name, quoted, ok := target.name()
	if !ok || len(idx) < 3 || !(nodes[idx[2]].isOp("=") || nodes[idx[2]].word() == "TO") {
		return t.expressions(nodes)
	}
	lower := strings.ToLower(name)
	if !quoted && t.opts.IsSetting != nil && t.opts.IsSetting(lower) {
		return t.expressions(nodes)
	}
	value, err := t.seq(nodes[idx[2]+1:], false)
	if err != nil {
		return nil, "", err
	}
	out := []*node{raw("SET VARIABLE " + quoteIdent(variableName(name, quoted)) + " = ")}
	return append(out, trim(value)...), "", nil
}

// variableName is the name a Snowflake session variable has in DuckDB:
// unquoted names are case-insensitive, as in Snowflake.
func variableName(name string, quoted bool) string {
	if quoted {
		return name
	}
	return strings.ToLower(name)
}

// unsetStatement translates UNSET name and UNSET (a, b).
func (t *translator) unsetStatement(nodes []*node, idx []int) ([]*node, string, error) {
	if len(idx) != 2 {
		return nil, "", &Error{Code: CodeSyntax, Message: "expected UNSET name"}
	}
	target := nodes[idx[1]]
	if target.isGroup('(') {
		return nil, "", unsupported("UNSET (a, b), removing several variables at once", "remove one variable per statement")
	}
	name, quoted, ok := target.name()
	if !ok {
		return nil, "", &Error{Code: CodeSyntax, Message: "expected UNSET name"}
	}
	return []*node{raw("RESET VARIABLE " + quoteIdent(variableName(name, quoted)))}, "", nil
}

// sessionParameter is how Igloo treats one of Snowflake's session
// parameters in ALTER SESSION SET.
type sessionParameter struct {
	// accept lists the values that already hold in Igloo; any value is
	// accepted when it is nil and sql is nil.
	accept []string
	// sql, if set, returns the DuckDB statement that sets the value.
	sql func(value string) string
	// why explains a refusal.
	why string
}

// sessionParameters are Snowflake's session parameters Igloo knows.
// Parameters about clients, caching and queuing change nothing in Igloo.
var sessionParameters = map[string]sessionParameter{
	"TIMEZONE": {sql: func(v string) string { return "SET TimeZone = " + quoteString(v) }},
	"STATEMENT_TIMEOUT_IN_SECONDS": {sql: func(v string) string {
		return "SET statement_timeout = " + quoteString(v+"s")
	}},
	"WEEK_START":                     {accept: []string{"0"}, why: "Igloo's weeks start on Monday and DAYOFWEEK counts from Sunday, as with WEEK_START = 0"},
	"WEEK_OF_YEAR_POLICY":            {accept: []string{"0"}, why: "Igloo numbers weeks as ISO 8601 does, as with WEEK_OF_YEAR_POLICY = 0"},
	"TIMESTAMP_TYPE_MAPPING":         {accept: []string{"TIMESTAMP_NTZ"}, why: "TIMESTAMP means TIMESTAMP_NTZ in Igloo"},
	"DATE_INPUT_FORMAT":              {accept: []string{"AUTO"}, why: "give the format to TO_DATE instead"},
	"TIMESTAMP_INPUT_FORMAT":         {accept: []string{"AUTO"}, why: "give the format to TO_TIMESTAMP instead"},
	"TIME_INPUT_FORMAT":              {accept: []string{"AUTO"}, why: "give the format to TO_TIME instead"},
	"TIMESTAMP_OUTPUT_FORMAT":        {accept: []string{"YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"}, why: outputFormatWhy},
	"TIMESTAMP_LTZ_OUTPUT_FORMAT":    {accept: []string{"", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"}, why: outputFormatWhy},
	"TIMESTAMP_TZ_OUTPUT_FORMAT":     {accept: []string{"", "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM"}, why: outputFormatWhy},
	"TIMESTAMP_NTZ_OUTPUT_FORMAT":    {accept: []string{"YYYY-MM-DD HH24:MI:SS.FF3"}, why: outputFormatWhy},
	"TIME_OUTPUT_FORMAT":             {accept: []string{"HH24:MI:SS"}, why: outputFormatWhy},
	"DATE_OUTPUT_FORMAT":             {accept: []string{"YYYY-MM-DD"}, why: outputFormatWhy},
	"BINARY_OUTPUT_FORMAT":           {accept: []string{"HEX"}, why: outputFormatWhy},
	"QUOTED_IDENTIFIERS_IGNORE_CASE": {accept: []string{"FALSE", "TRUE"}},
	"AUTOCOMMIT":                     {accept: []string{"TRUE"}, why: "use BEGIN and COMMIT for transactions"},
	"USE_CACHED_RESULT":              {}, "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS": {}, "ROWS_PER_RESULTSET": {},
	"ABORT_DETACHED_QUERY": {}, "LOCK_TIMEOUT": {}, "JSON_INDENT": {}, "MULTI_STATEMENT_COUNT": {},
	"TRANSACTION_DEFAULT_ISOLATION_LEVEL": {}, "TIMESTAMP_DAY_IS_ALWAYS_24H": {}, "ERROR_ON_NONDETERMINISTIC_MERGE": {},
	"ERROR_ON_NONDETERMINISTIC_UPDATE": {}, "BINARY_INPUT_FORMAT": {}, "GEOGRAPHY_OUTPUT_FORMAT": {},
	"ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION": {}, "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": {},
	"CLIENT_RESULT_CHUNK_SIZE": {}, "CLIENT_TIMESTAMP_TYPE_MAPPING": {}, "SEARCH_PATH": {},
}

// outputFormatWhy explains refusing an output format: TO_VARCHAR writes
// values in Snowflake's default output formats.
const outputFormatWhy = "Igloo writes values in Snowflake's default output formats; give TO_CHAR a format instead"

// alterSession translates ALTER SESSION SET and UNSET.
func (t *translator) alterSession(nodes []*node, idx []int) ([]*node, string, error) {
	if len(idx) < 3 {
		return nil, "", &Error{Code: CodeSyntax, Message: "expected ALTER SESSION SET or UNSET"}
	}
	verb := nodes[idx[2]].word()
	rest := idx[3:]
	if verb == "UNSET" {
		var stmts []string
		for _, i := range rest {
			if nodes[i].isOp(",") {
				continue
			}
			name := nodes[i].word()
			switch name {
			case "QUERY_TAG", "SQL_DIALECT":
				// Igloo carries these out itself.
				return nodes, "", nil
			case "TIMEZONE":
				stmts = append(stmts, "RESET TimeZone")
			case "STATEMENT_TIMEOUT_IN_SECONDS":
				stmts = append(stmts, "RESET statement_timeout")
			case "":
				return nil, "", &Error{Code: CodeSyntax, Message: "expected a parameter name after ALTER SESSION UNSET"}
			}
		}
		return t.sessionStatements(stmts)
	}
	if verb != "SET" {
		return nil, "", &Error{Code: CodeSyntax, Message: "expected ALTER SESSION SET or UNSET"}
	}
	var stmts []string
	for k := 0; k < len(rest); {
		if nodes[rest[k]].isOp(",") {
			k++
			continue
		}
		name := nodes[rest[k]].word()
		if name == "" || k+2 >= len(rest) || !nodes[rest[k+1]].isOp("=") {
			return nil, "", &Error{Code: CodeSyntax, Message: "expected ALTER SESSION SET name = value"}
		}
		if name == "QUERY_TAG" || name == "SQL_DIALECT" {
			if len(rest) > 3 {
				return nil, "", unsupported("Setting "+name+" with other parameters", "set "+name+" in an ALTER SESSION of its own")
			}
			// Igloo carries these out itself; only the literal is translated.
			return nodes, "", nil
		}
		v := nodes[rest[k+2]]
		value, ok := v.stringValue()
		if !ok {
			value = v.tok.text
			if v.kind == kOp && (v.tok.text == "-" || v.tok.text == "+") && k+3 < len(rest) {
				value += nodes[rest[k+3]].tok.text
				k++
			}
		}
		p, known := sessionParameters[name]
		switch {
		case !known:
			return nil, "", unsupported("The session parameter "+name, "")
		case p.sql != nil:
			stmts = append(stmts, p.sql(value))
		case p.accept != nil && !containsFold(p.accept, value):
			return nil, "", unsupported(fmt.Sprintf("%s = %s", name, value), p.why)
		}
		k += 3
	}
	return t.sessionStatements(stmts)
}

// sessionStatements is the outcome of ALTER SESSION: one statement to run,
// or none.
func (t *translator) sessionStatements(stmts []string) ([]*node, string, error) {
	switch len(stmts) {
	case 0:
		return nil, "ALTER SESSION", nil
	case 1:
		return []*node{raw(stmts[0])}, "", nil
	}
	return nil, "", unsupported("Setting TIMEZONE and STATEMENT_TIMEOUT_IN_SECONDS in one ALTER SESSION", "set them one at a time")
}

func containsFold(list []string, s string) bool {
	for _, x := range list {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}

// useStatement translates USE.
func (t *translator) useStatement(nodes []*node, idx []int) ([]*node, string, error) {
	if len(idx) < 2 {
		return nodes, "", nil
	}
	switch nodes[idx[1]].word() {
	case "ROLE", "SECONDARY":
		t.notice("USE ROLE changes nothing: an Igloo session acts with all of its user's roles at once")
		return nil, "USE", nil
	case "WAREHOUSE":
		t.notice("USE WAREHOUSE changes nothing: Igloo has no warehouses to choose")
		return nil, "USE", nil
	case "DATABASE", "SCHEMA":
		return append([]*node{raw("USE ")}, trim(nodes[idx[1]+1:])...), "", nil
	}
	return nodes, "", nil
}

// grantStatement translates Snowflake's role syntax in GRANT and REVOKE:
// TO ROLE r and FROM ROLE r become TO r and FROM r, GRANT ROLE r TO USER u
// becomes GRANT r TO u, and grants of USAGE on databases, schemas and
// warehouses, which Igloo has no use for, change nothing.
func (t *translator) grantStatement(nodes []*node, idx []int) ([]*node, string, error) {
	words := make([]string, len(idx))
	for k, i := range idx {
		words[k] = nodes[i].word()
	}
	if len(words) > 3 && words[1] == "USAGE" && words[2] == "ON" {
		switch words[3] {
		case "DATABASE", "SCHEMA", "WAREHOUSE", "ALL", "FUTURE":
			t.notice("GRANT USAGE on a %s changes nothing: Igloo needs no USAGE grant to reach its schemas", strings.ToLower(words[3]))
			return nil, words[0], nil
		}
	}
	drop := map[int]bool{}
	for k := 1; k < len(words); k++ {
		switch {
		case (words[k] == "ROLE" || words[k] == "USER") && (words[k-1] == "TO" || words[k-1] == "FROM"):
			drop[idx[k]] = true
		case words[k] == "ROLE" && k == 1:
			drop[idx[k]] = true
		}
	}
	var out []*node
	for i, n := range nodes {
		if drop[i] {
			// Drop the keyword and the space after it.
			if i+1 < len(nodes) && nodes[i+1].kind == kSpace {
				drop[i+1] = true
			}
			continue
		}
		out = append(out, n)
	}
	return t.expressions(out)
}

// alterTable translates ALTER TABLE: column types, and clauses about
// clustering and retention, which change nothing in Igloo.
func (t *translator) alterTable(nodes []*node, idx []int) ([]*node, string, error) {
	words := make([]string, len(idx))
	for k, i := range idx {
		words[k] = nodes[i].word()
	}
	for k := 3; k < len(words); k++ {
		switch words[k] {
		case "CLUSTER", "RECLUSTER", "CLUSTERING":
			t.notice("%s is ignored: Igloo does not cluster tables", strings.Join(words[3:min(k+2, len(words))], " "))
			return nil, "ALTER TABLE", nil
		case "SWAP":
			return nil, "", unsupported("ALTER TABLE ... SWAP WITH", "rename the tables one at a time with ALTER TABLE ... RENAME TO")
		case "SET":
			if k+1 < len(words) {
				switch words[k+1] {
				case "DATA_RETENTION_TIME_IN_DAYS", "MAX_DATA_EXTENSION_TIME_IN_DAYS", "CHANGE_TRACKING",
					"ENABLE_SCHEMA_EVOLUTION", "COMMENT", "DEFAULT_DDL_COLLATION":
					t.notice("ALTER TABLE ... SET %s is ignored", words[k+1])
					return nil, "ALTER TABLE", nil
				}
			}
		}
		if k == 3 && (words[k] == "SUSPEND" || words[k] == "RESUME") {
			t.notice("ALTER TABLE ... %s RECLUSTER is ignored: Igloo does not cluster tables", words[k])
			return nil, "ALTER TABLE", nil
		}
	}
	// Column types: ADD [COLUMN] name type, ALTER [COLUMN] name [SET DATA] TYPE type.
	out := append([]*node(nil), nodes...)
	for k := 0; k < len(idx); k++ {
		var typeAt int = -1
		switch {
		case words[k] == "ADD" && k+1 < len(idx):
			j := k + 1
			if words[j] == "COLUMN" {
				j++
			}
			if words[j] == "IF" && j+2 < len(idx) {
				j += 3
			}
			if j+1 < len(idx) && !isConstraintWord(words[j]) {
				typeAt = idx[j+1]
			}
		case words[k] == "TYPE" && k+1 < len(idx):
			typeAt = idx[k+1]
		}
		if typeAt >= 0 {
			if err := t.mapTypeAt(out, typeAt); err != nil {
				return nil, "", err
			}
		}
	}
	return t.expressions(out)
}

func isConstraintWord(w string) bool {
	switch w {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "ROW", "SEARCH", "TAG":
		return true
	}
	return false
}

// mapTypeAt replaces the Snowflake type at nodes[i] by DuckDB's.
func (t *translator) mapTypeAt(nodes []*node, i int) error {
	typ, end, ok, err := parseType(nodes, i)
	if err != nil || !ok {
		return err
	}
	nodes[i] = &node{kind: kRaw, text: typ.duck, pos: nodes[i].pos, end: nodes[end-1].end}
	for j := i + 1; j < end; j++ {
		nodes[j] = &node{kind: kRaw}
	}
	return nil
}

// createStatement translates CREATE: Snowflake's kinds of tables and
// views, their options, and the objects Igloo does not have.
func (t *translator) createStatement(nodes []*node, idx []int) ([]*node, string, error) {
	k := 1
	word := func(k int) string {
		if k < len(idx) {
			return nodes[idx[k]].word()
		}
		return ""
	}
	drop := map[int]bool{}
	if word(k) == "OR" && word(k+1) == "REPLACE" {
		k += 2
	}
	// Modifiers before TABLE or VIEW.
	for {
		switch word(k) {
		case "TRANSIENT", "VOLATILE":
			drop[idx[k]] = true
			k++
			continue
		case "LOCAL", "GLOBAL":
			drop[idx[k]] = true
			k++
			continue
		case "SECURE":
			t.notice("SECURE is ignored: Igloo views run with their reader's privileges and their definitions are visible")
			drop[idx[k]] = true
			k++
			continue
		case "TEMPORARY", "TEMP", "RECURSIVE", "MATERIALIZED":
			k++
			continue
		}
		break
	}
	switch kind := word(k); kind {
	case "TABLE":
		return t.createTable(nodes, idx, k, drop)
	case "VIEW":
		return t.createView(nodes, idx, k, drop)
	case "DATABASE", "WAREHOUSE", "STAGE", "FILE", "STREAM", "PROCEDURE", "PIPE_", "EXTERNAL_FUNCTION",
		"SEQUENCE", "DYNAMIC", "EVENT", "NETWORK", "STORAGE", "API", "SECURITY", "ICEBERG", "HYBRID":
		if kind == "FILE" {
			kind = "FILE FORMAT"
		}
		return nil, "", unsupported("CREATE "+kind, createHints[kind])
	case "FUNCTION":
		return nil, "", unsupported("CREATE FUNCTION", "write the expression where it is used, or ask an admin for a DuckDB macro")
	}
	return t.expressions(dropNodes(nodes, drop))
}

var createHints = map[string]string{
	"DATABASE":  "Igloo has one catalog; create a schema instead",
	"WAREHOUSE": "Igloo needs no warehouses; see compute pools in docs/workload-management.md",
	"STAGE":     "load files with COPY INTO from a location in IGLOO_ALLOWED_LOCATIONS",
	"SEQUENCE":  "Iceberg tables have no sequences; generate keys with uuid() or row_number()",
	"STREAM":    "query a table's changes with time travel (AT (TIMESTAMP => ...))",
	"DYNAMIC":   "use CREATE MATERIALIZED VIEW, refreshed on a schedule by a task",
}

// dropNodes returns nodes without those at the indexes in drop, and the
// white space after each.
func dropNodes(nodes []*node, drop map[int]bool) []*node {
	if len(drop) == 0 {
		return nodes
	}
	var out []*node
	skipSpace := false
	for i, n := range nodes {
		if drop[i] {
			skipSpace = true
			continue
		}
		if skipSpace && n.kind == kSpace {
			skipSpace = false
			continue
		}
		skipSpace = false
		out = append(out, n)
	}
	return out
}

// tableOptions are options of CREATE TABLE and CREATE VIEW, written
// NAME = value, that change nothing in Igloo.
var tableOptions = map[string]bool{
	"DATA_RETENTION_TIME_IN_DAYS": true, "MAX_DATA_EXTENSION_TIME_IN_DAYS": true, "CHANGE_TRACKING": true,
	"DEFAULT_DDL_COLLATION": true, "ENABLE_SCHEMA_EVOLUTION": true, "STAGE_FILE_FORMAT": true,
	"STAGE_COPY_OPTIONS": true, "COMMENT": true, "ROW_ACCESS": true,
}

// createTable translates CREATE TABLE: column types, and the options that
// change nothing in Igloo (CLUSTER BY, COPY GRANTS, retention, comments),
// which are dropped, with a notice for those a reader could miss.
func (t *translator) createTable(nodes []*node, idx []int, k int, drop map[int]bool) ([]*node, string, error) {
	// The name: k+1, after IF NOT EXISTS.
	j := k + 1
	if j+2 < len(idx) && nodes[idx[j]].word() == "IF" {
		j += 3
	}
	j++ // past the name
	// Column definitions.
	if j < len(idx) && nodes[idx[j]].isGroup('(') {
		g := nodes[idx[j]]
		if err := t.columnDefinitions(g); err != nil {
			return nil, "", err
		}
		j++
	}
	if err := t.tableClauses(nodes, idx, j, drop); err != nil {
		return nil, "", err
	}
	return t.expressions(dropNodes(nodes, drop))
}

// tableClauses marks the options after a table's or view's columns that
// Igloo drops, from idx[j] up to AS or the end.
func (t *translator) tableClauses(nodes []*node, idx []int, j int, drop map[int]bool) error {
	for j < len(idx) {
		n := nodes[idx[j]]
		w := n.word()
		switch {
		case w == "AS" || w == "CLONE" || w == "LIKE" || w == "USING":
			if w == "LIKE" || w == "USING" {
				return unsupported("CREATE TABLE ... "+w, "create the table with its columns, or with CREATE TABLE ... AS SELECT")
			}
			return nil
		case w == "CLUSTER" && j+2 < len(idx) && nodes[idx[j+1]].word() == "BY":
			t.notice("CLUSTER BY is ignored: Igloo does not cluster tables")
			drop[idx[j]], drop[idx[j+1]], drop[idx[j+2]] = true, true, true
			j += 3
		case w == "COPY" && j+1 < len(idx) && nodes[idx[j+1]].word() == "GRANTS":
			// Igloo's grants follow a table's name, so a replaced table
			// keeps them anyway.
			drop[idx[j]], drop[idx[j+1]] = true, true
			j += 2
		case tableOptions[w] && j+2 < len(idx) && nodes[idx[j+1]].isOp("="):
			if w == "COMMENT" {
				t.notice("COMMENT = ... is ignored; set the comment with COMMENT ON")
			} else {
				t.notice("%s is ignored", w)
			}
			drop[idx[j]], drop[idx[j+1]], drop[idx[j+2]] = true, true, true
			j += 3
		case w == "WITH" && j+1 < len(idx) && (nodes[idx[j+1]].word() == "TAG" || nodes[idx[j+1]].word() == "ROW"):
			return unsupported("CREATE TABLE ... WITH "+nodes[idx[j+1]].word(),
				"attach tags and row access policies with ALTER TABLE after creating the table")
		case w == "TAG" && j+1 < len(idx) && nodes[idx[j+1]].isGroup('('):
			return unsupported("CREATE TABLE ... TAG (...)", "attach tags with ALTER TABLE ... SET TAG after creating the table")
		default:
			j++
		}
	}
	return nil
}

// columnDefinitions translates a CREATE TABLE's column list in place:
// each column's type, and column comments, which are dropped.
func (t *translator) columnDefinitions(g *node) error {
	for _, def := range splitArgs(g.kids) {
		di := sig(def)
		if len(di) < 2 {
			continue
		}
		if isConstraintWord(def[di[0]].word()) {
			continue
		}
		if err := t.mapTypeAt(def, di[1]); err != nil {
			return err
		}
		for k := 2; k < len(di); k++ {
			switch def[di[k]].word() {
			case "COMMENT":
				if k+1 < len(di) && def[di[k+1]].kind == kString {
					t.notice("column comments in CREATE TABLE are ignored; set them with COMMENT ON COLUMN")
					*def[di[k]] = node{kind: kRaw}
					*def[di[k+1]] = node{kind: kRaw}
				}
			case "AUTOINCREMENT", "IDENTITY":
				return unsupported(def[di[k]].word()+" columns", "Iceberg tables have no sequences; generate keys with uuid() or row_number()")
			case "COLLATE":
				return unsupported("COLLATE", "compare with ILIKE or lower() instead")
			case "MASKING", "PROJECTION", "TAG", "WITH":
				return unsupported("Policies and tags in CREATE TABLE", "attach them with ALTER TABLE after creating the table")
			}
		}
	}
	return nil
}

// createView translates CREATE VIEW: COPY GRANTS, COMMENT and column
// comments are dropped.
func (t *translator) createView(nodes []*node, idx []int, k int, drop map[int]bool) ([]*node, string, error) {
	j := k + 1
	if j+2 < len(idx) && nodes[idx[j]].word() == "IF" {
		j += 3
	}
	j++
	if j < len(idx) && nodes[idx[j]].isGroup('(') {
		// Column names, perhaps with comments.
		g := nodes[idx[j]]
		for _, def := range splitArgs(g.kids) {
			di := sig(def)
			for m := 1; m+1 < len(di); m++ {
				if def[di[m]].word() == "COMMENT" {
					t.notice("column comments in CREATE VIEW are ignored; set them with COMMENT ON COLUMN")
					*def[di[m]] = node{kind: kRaw}
					*def[di[m+1]] = node{kind: kRaw}
				}
			}
		}
		j++
	}
	if err := t.tableClauses(nodes, idx, j, drop); err != nil {
		return nil, "", err
	}
	return t.expressions(dropNodes(nodes, drop))
}

// identifierText renders the name a string gives to IDENTIFIER(): parts
// as Snowflake reads them, unquoted or quoted.
func identifierText(s string) (string, error) {
	toks, err := lex(s)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	expectName := true
	for _, tok := range toks {
		switch {
		case tok.kind == kSpace:
		case expectName && tok.kind == kWord:
			b.WriteString(tok.text)
			expectName = false
		case expectName && tok.kind == kQuoted:
			b.WriteString(quoteIdent(tok.val))
			expectName = false
		case !expectName && tok.kind == kOp && tok.text == ".":
			b.WriteByte('.')
			expectName = true
		default:
			return "", &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%q is not a valid identifier for IDENTIFIER()", s)}
		}
	}
	if expectName {
		return "", &Error{Code: CodeInvalidValue, Message: fmt.Sprintf("%q is not a valid identifier for IDENTIFIER()", s)}
	}
	return b.String(), nil
}

// itoa is strconv.Itoa.
func itoa(n int) string { return strconv.Itoa(n) }
