package federation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// What Igloo knows of a Snowflake database: its schemas, tables and
// columns, read with SHOW commands, which Snowflake answers without a
// warehouse, so listing them costs no credits. The gateway serves DuckDB's
// listings from it, so they keep working while Snowflake is down.
//
// Names are shown to DuckDB the way PostgreSQL users expect: a name that
// Snowflake keeps in upper case, as it does every name written without
// quotes, is shown in lower case (LINEITEM is lineitem), and any other name
// as it is ("MixedCase" stays MixedCase). DuckDB compares names without
// case anyway; this makes SELECT * and CREATE TABLE AS give the lower-case
// names Igloo's tables use. Names Snowflake has in two spellings that would
// then be shown alike keep their own spellings instead.

// sfKind is how a Snowflake type reaches DuckDB.
type sfKind int

const (
	sfNumber       sfKind = iota // NUMBER(p,s): numeric(p,s), DECIMAL(p,s) in DuckDB
	sfFloat                      // FLOAT: float8
	sfText                       // VARCHAR: text
	sfBinary                     // BINARY: bytea, BLOB in DuckDB
	sfBoolean                    // BOOLEAN: bool
	sfDate                       // DATE: date
	sfTime                       // TIME: time, to microseconds
	sfTimestamp                  // TIMESTAMP_NTZ: timestamp, to microseconds
	sfTimestampLTZ               // TIMESTAMP_LTZ: timestamptz
	sfTimestampTZ                // TIMESTAMP_TZ: timestamptz, the instant (its offset is not kept)
	sfJSON                       // VARIANT, OBJECT, ARRAY, MAP, VECTOR: json, VARCHAR in DuckDB, as TO_JSON writes it
	sfAsText                     // GEOGRAPHY, GEOMETRY and other types: text
)

// sfType is a column's Snowflake type.
type sfType struct {
	kind sfKind
	// name is Snowflake's name of the type, as SHOW COLUMNS gives it.
	name string
	// precision and scale of NUMBER; scale (fractional digits of seconds)
	// of TIME and the timestamps.
	precision, scale int
}

// parseSnowflakeType reads the data_type of SHOW COLUMNS, such as
// {"type":"FIXED","precision":38,"scale":0,"nullable":false}.
func parseSnowflakeType(dataType string) (sfType, bool, error) {
	var d struct {
		Type      string `json:"type"`
		Precision *int   `json:"precision"`
		Scale     *int   `json:"scale"`
		Nullable  *bool  `json:"nullable"`
	}
	if err := json.Unmarshal([]byte(dataType), &d); err != nil {
		return sfType{}, false, fmt.Errorf("unexpected data type %q", dataType)
	}
	t := sfType{name: strings.ToUpper(d.Type), scale: 9}
	if d.Scale != nil {
		t.scale = *d.Scale
	}
	notNull := d.Nullable != nil && !*d.Nullable
	switch t.name {
	case "FIXED":
		t.kind, t.precision, t.scale = sfNumber, 38, 0
		if d.Precision != nil {
			t.precision = *d.Precision
		}
		if d.Scale != nil {
			t.scale = *d.Scale
		}
		if t.precision < 1 || t.precision > 38 || t.scale < 0 || t.scale > t.precision {
			return t, false, fmt.Errorf("unexpected NUMBER(%d,%d)", t.precision, t.scale)
		}
	case "REAL", "FLOAT", "DOUBLE":
		t.kind = sfFloat
	case "TEXT":
		t.kind = sfText
	case "BINARY":
		t.kind = sfBinary
	case "BOOLEAN":
		t.kind = sfBoolean
	case "DATE":
		t.kind = sfDate
	case "TIME":
		t.kind = sfTime
	case "TIMESTAMP_NTZ":
		t.kind = sfTimestamp
	case "TIMESTAMP_LTZ":
		t.kind = sfTimestampLTZ
	case "TIMESTAMP_TZ":
		t.kind = sfTimestampTZ
	case "VARIANT", "OBJECT", "ARRAY", "MAP", "VECTOR":
		t.kind = sfJSON
	default:
		t.kind = sfAsText
	}
	if t.scale < 0 || t.scale > 9 && t.kind != sfNumber {
		return t, false, fmt.Errorf("unexpected scale %d of %s", t.scale, t.name)
	}
	return t, notNull, nil
}

// pgType is the PostgreSQL type the gateway reports for t, by name, with
// its type modifier, which DuckDB's postgres extension maps to its own.
func (t sfType) pgType() (string, int) {
	switch t.kind {
	case sfNumber:
		return "numeric", (t.precision<<16 | t.scale) + 4
	case sfFloat:
		return "float8", -1
	case sfBinary:
		return "bytea", -1
	case sfBoolean:
		return "bool", -1
	case sfDate:
		return "date", -1
	case sfTime:
		return "time", -1
	case sfTimestamp:
		return "timestamp", -1
	case sfTimestampLTZ, sfTimestampTZ:
		return "timestamptz", -1
	case sfJSON:
		return "json", -1
	}
	return "text", -1
}

// selectExpr is what the gateway selects from Snowflake for a column, col
// quoted: the column itself, or an expression whose result DuckDB reads as
// the column's PostgreSQL type. Filters on the column are applied to the
// same expression unless sameAsColumn, so they see what DuckDB sees.
func (t sfType) selectExpr(col string) (expr string, sameAsColumn bool) {
	switch t.kind {
	case sfTime:
		// Snowflake's driver cannot convert TIME results with fewer than 9
		// digits of fractions (which arrive as 32-bit integers); TIME(9)
		// has the same values.
		return "CAST(" + col + " AS TIME(9))", true
	case sfJSON:
		if t.name == "VECTOR" {
			return "TO_JSON(CAST(" + col + " AS ARRAY))", false
		}
		return "TO_JSON(CAST(" + col + " AS VARIANT))", false
	case sfAsText:
		if t.name == "GEOGRAPHY" || t.name == "GEOMETRY" {
			return "ST_ASWKT(" + col + ")", false
		}
		return "TO_VARCHAR(" + col + ")", false
	}
	return col, true
}

// sfCatalog is what Igloo read of a Snowflake database.
type sfCatalog struct {
	// database is the database's name as Snowflake spells it.
	database string
	schemas  []*sfSchema
}

type sfSchema struct {
	// name is as Snowflake spells it; pgName as DuckDB sees it.
	name, pgName string
	oid          int
	tables       []*sfTable
}

type sfTable struct {
	name, pgName string
	comment      string
	// bytes is Snowflake's size of the table, 0 for views.
	bytes   int64
	columns []*sfColumn
}

type sfColumn struct {
	name, pgName string
	comment      string
	notNull      bool
	typ          sfType
}

// schema returns the schema DuckDB calls name, which it compares without
// case.
func (c *sfCatalog) schema(name string) *sfSchema {
	for _, s := range c.schemas {
		if strings.EqualFold(s.pgName, name) {
			return s
		}
	}
	return nil
}

func (s *sfSchema) table(name string) *sfTable {
	for _, t := range s.tables {
		if t.pgName == name {
			return t
		}
	}
	for _, t := range s.tables {
		if strings.EqualFold(t.pgName, name) {
			return t
		}
	}
	return nil
}

func (t *sfTable) column(name string) *sfColumn {
	for _, c := range t.columns {
		if c.pgName == name {
			return c
		}
	}
	return nil
}

// pgNames gives Snowflake's names the names DuckDB sees (see above).
// Columns must stay distinct without regard to case, as DuckDB requires of
// a table's columns: a column whose name still clashes gets a suffix, and
// renamed is told so.
func pgNames(names []string, columns bool, renamed func(from, to string)) []string {
	out := make([]string, len(names))
	byFold := map[string][]int{}
	for i, n := range names {
		out[i] = n
		if n == strings.ToUpper(n) {
			out[i] = strings.ToLower(n)
		}
		byFold[strings.ToLower(out[i])] = append(byFold[strings.ToLower(out[i])], i)
	}
	for _, idx := range byFold {
		if len(idx) > 1 {
			for _, i := range idx {
				out[i] = names[i]
			}
		}
	}
	if !columns {
		return out
	}
	seen := map[string]bool{}
	for i, n := range out {
		name := n
		for k := 2; seen[strings.ToLower(name)]; k++ {
			name = fmt.Sprintf("%s_%d", n, k)
		}
		if name != n {
			renamed(names[i], name)
		}
		out[i] = name
		seen[strings.ToLower(name)] = true
	}
	return out
}

// showLimit is the most rows Snowflake's SHOW commands return.
const showLimit = 10000

// loadCatalog reads the schemas, tables and columns of the connection's
// database, or of its schemas when the connection names some.
func (cl *sfClient) loadCatalog(ctx context.Context, warn func(msg string, args ...any)) (*sfCatalog, error) {
	conn, err := cl.session(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close() //nolint:errcheck
	db := sfQuote(cl.database)
	names, err := showNames(ctx, conn, "SHOW TERSE SCHEMAS IN DATABASE "+db)
	if err != nil {
		return nil, snowflakeError("list the schemas of "+cl.database, err)
	}
	var wanted []string
	for _, s := range cl.c.Schemas {
		wanted = append(wanted, sfIdent(s))
	}
	names = slices.DeleteFunc(names, func(n string) bool {
		// INFORMATION_SCHEMA is Snowflake's views of the catalog, which
		// need a warehouse; DuckDB has its own.
		return n == "INFORMATION_SCHEMA" || len(wanted) > 0 && !slices.Contains(wanted, n)
	})
	for _, w := range wanted {
		if !slices.Contains(names, w) {
			return nil, fmt.Errorf("Snowflake: database %s has no schema %s that role %s can see", cl.database, w, cl.c.Role)
		}
	}
	cat := &sfCatalog{database: cl.database}
	pg := pgNames(names, false, nil)
	for i, n := range names {
		cat.schemas = append(cat.schemas, &sfSchema{name: n, pgName: pg[i], oid: 16384 + i})
	}

	// A few schemas at a time, each on a session of its own: listing a
	// whole database with one SHOW is slower than its schemas side by side.
	sem := make(chan struct{}, 8)
	errs := make([]error, len(cat.schemas))
	var wg sync.WaitGroup
	for i, s := range cat.schemas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			errs[i] = cl.loadSchema(ctx, s, warn)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return cat, nil
}

// loadSchema reads one schema's tables, views and columns.
func (cl *sfClient) loadSchema(ctx context.Context, s *sfSchema, warn func(msg string, args ...any)) error {
	conn, err := cl.session(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	in := sfQuote(cl.database) + "." + sfQuote(s.name)
	objects, err := showObjects(ctx, conn, in)
	if err != nil {
		return snowflakeError("list the tables of "+s.name, err)
	}
	cols, err := showColumns(ctx, conn, "SHOW COLUMNS IN SCHEMA "+in)
	if err != nil {
		return snowflakeError("list the columns of "+s.name, err)
	}
	if len(cols) == showLimit {
		// SHOW may have cut its result off there: list each table's
		// instead. SHOW COLUMNS IN SCHEMA lists more when there are more,
		// such as 68,632 in 14 seconds where listing each of the schema's
		// 812 tables took 4.5 minutes.
		cols = nil
		for _, o := range objects {
			kind := "TABLE"
			if o.view {
				kind = "VIEW"
			}
			tc, err := showColumns(ctx, conn, "SHOW COLUMNS IN "+kind+" "+in+"."+sfQuote(o.name))
			if err != nil {
				return snowflakeError("list the columns of "+s.name+"."+o.name, err)
			}
			cols = append(cols, tc...)
		}
	}
	s.tables = buildTables(s.name, cols, objects, warn)
	return nil
}

// buildTables makes a schema's tables of its columns, as SHOW COLUMNS
// lists them, with the sizes and comments of SHOW OBJECTS.
func buildTables(schema string, cols []sfShownColumn, objects map[string]sfObject, warn func(msg string, args ...any)) []*sfTable {
	byTable := map[string]*sfTable{}
	var tables []*sfTable
	for _, c := range cols {
		t := byTable[c.table]
		if t == nil {
			t = &sfTable{name: c.table}
			if o, ok := objects[c.table]; ok {
				t.comment, t.bytes = o.comment, o.bytes
			}
			byTable[c.table] = t
			tables = append(tables, t)
		}
		t.columns = append(t.columns, c.column)
	}
	slices.SortFunc(tables, func(a, b *sfTable) int { return strings.Compare(a.name, b.name) })
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.name
	}
	for i, n := range pgNames(tableNames, false, nil) {
		t := tables[i]
		t.pgName = n
		colNames := make([]string, len(t.columns))
		for j, c := range t.columns {
			colNames[j] = c.name
		}
		for j, n := range pgNames(colNames, true, func(from, to string) {
			warn("Snowflake columns differ only in case; one is shown under another name",
				"table", schema+"."+t.name, "column", from, "shown_as", to)
		}) {
			t.columns[j].pgName = n
		}
	}
	return tables
}

// sfObject is a table or view, as SHOW OBJECTS lists it.
type sfObject struct {
	name, comment string
	view          bool
	bytes         int64
}

// showObjects lists the tables and views of a schema, by name, a page of
// showLimit at a time.
func showObjects(ctx context.Context, conn *sql.Conn, in string) (map[string]sfObject, error) {
	out := map[string]sfObject{}
	from := ""
	for {
		q := fmt.Sprintf("SHOW OBJECTS IN SCHEMA %s LIMIT %d", in, showLimit)
		if from != "" {
			q += " FROM " + sfString(from)
		}
		rows, err := showRows(ctx, conn, q)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			o := sfObject{name: r["name"], comment: r["comment"], view: strings.Contains(r["kind"], "VIEW")}
			fmt.Sscan(r["bytes"], &o.bytes) //nolint:errcheck
			out[o.name] = o
			from = o.name
		}
		if len(rows) < showLimit {
			return out, nil
		}
	}
}

// sfShownColumn is a column as SHOW COLUMNS lists it.
type sfShownColumn struct {
	table  string
	column *sfColumn
}

func showColumns(ctx context.Context, conn *sql.Conn, q string) ([]sfShownColumn, error) {
	rows, err := showRows(ctx, conn, q)
	if err != nil {
		return nil, err
	}
	out := make([]sfShownColumn, 0, len(rows))
	for _, r := range rows {
		typ, notNull, err := parseSnowflakeType(r["data_type"])
		if err != nil {
			return nil, fmt.Errorf("column %s.%s: %w", r["table_name"], r["column_name"], err)
		}
		out = append(out, sfShownColumn{table: r["table_name"], column: &sfColumn{
			name: r["column_name"], comment: r["comment"], typ: typ, notNull: notNull || r["null?"] == "NOT_NULL",
		}})
	}
	return out, nil
}

func showNames(ctx context.Context, conn *sql.Conn, q string) ([]string, error) {
	rows, err := showRows(ctx, conn, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r["name"]
	}
	return out, nil
}

// showRows runs a SHOW command, returning each row's values by column
// name, as text.
func showRows(ctx context.Context, conn *sql.Conn, q string) ([]map[string]string, error) {
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]string
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]string, len(cols))
		for i, c := range cols {
			m[c] = vals[i].String
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
