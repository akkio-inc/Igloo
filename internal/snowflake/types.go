package snowflake

import (
	"strconv"
	"strings"
)

// typeClass is what a Snowflake type becomes in DuckDB, as far as casts to
// it care.
type typeClass int

const (
	classScalar typeClass = iota // any other type
	classString                  // VARCHAR and its synonyms
	classJSON                    // VARIANT, OBJECT, ARRAY
	classNested                  // ARRAY(type), OBJECT(name type, ...), MAP(type, type)
)

// sqlType is a Snowflake type written in a statement, as DuckDB names it.
type sqlType struct {
	duck  string
	class typeClass
	// column, when set, is DuckDB's type for a column of the type, where
	// it differs from duck: a value of the semi-structured ARRAY or
	// OBJECT is JSON, but a column of one is a list of strings or a map
	// of strings (see docs/snowflake-sql.md), which Iceberg tables hold
	// and engines read natively.
	column string
}

// semi is 'A' for ARRAY and 'O' for OBJECT (see node.semi).
func (t sqlType) semi() byte {
	switch t.column {
	case arrayColumn:
		return 'A'
	case objectColumn:
		return 'O'
	}
	return 0
}

// columnType is DuckDB's type for a column of t.
func (t sqlType) columnType() string {
	if t.column != "" {
		return t.column
	}
	return t.duck
}

// Igloo's columns of Snowflake's ARRAY and OBJECT: a list of strings and a
// map of strings, each element or value as Snowflake's TO_VARCHAR writes
// it, a string as itself and anything else as its JSON text. Snowflake
// converts a semi-structured value to its ARRAY(VARCHAR) and MAP(VARCHAR,
// VARCHAR) the same way. The macros read them back (see sf_elem).
const (
	arrayColumn  = "VARCHAR[]"
	objectColumn = "MAP(VARCHAR, VARCHAR)"
)

// parseType reads the Snowflake type that starts at nodes[i] (significant)
// and returns it and the index just past it. ok is false when nodes[i] does
// not start a type Igloo maps; such types are left as written.
func parseType(nodes []*node, i int) (t sqlType, end int, ok bool, err error) {
	first := nodes[i].word()
	if first == "" {
		return sqlType{}, 0, false, nil
	}
	end = i + 1
	// word(k) is the k-th significant node after nodes[i]'s word, upper-cased.
	next := func(from int) (int, string) {
		j := nextSig(nodes, from-1)
		if j < 0 {
			return -1, ""
		}
		return j, nodes[j].word()
	}
	// params reads an optional parenthesized list of integers after end.
	params := func() ([]int, bool, error) {
		j := nextSig(nodes, end-1)
		if j < 0 || !nodes[j].isGroup('(') {
			return nil, false, nil
		}
		var out []int
		for _, a := range splitArgs(nodes[j].kids) {
			a = trim(a)
			if len(a) != 1 || a[0].kind != kNumber {
				return nil, true, nil
			}
			n, err := strconv.Atoi(a[0].tok.text)
			if err != nil {
				return nil, true, &Error{Code: CodeSyntax, Message: "invalid type parameter " + a[0].tok.text}
			}
			out = append(out, n)
		}
		end = j + 1
		return out, true, nil
	}
	skipParams := func() {
		if j := nextSig(nodes, end-1); j >= 0 && nodes[j].isGroup('(') {
			end = j + 1
		}
	}
	switch first {
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC":
		p, has, err := params()
		if err != nil {
			return sqlType{}, 0, false, err
		}
		switch {
		case !has || len(p) == 0:
			return sqlType{duck: "DECIMAL(38,0)"}, end, true, nil
		case len(p) == 1 && p[0] >= 1 && p[0] <= 38:
			return sqlType{duck: "DECIMAL(" + strconv.Itoa(p[0]) + ",0)"}, end, true, nil
		case len(p) == 2 && p[0] >= 1 && p[0] <= 38 && p[1] >= 0 && p[1] <= p[0]:
			return sqlType{duck: "DECIMAL(" + strconv.Itoa(p[0]) + "," + strconv.Itoa(p[1]) + ")"}, end, true, nil
		}
		return sqlType{}, 0, false, &Error{Code: CodeSyntax, Message: "invalid precision or scale for " + first,
			Hint: "NUMBER takes a precision from 1 to 38 and a scale from 0 to the precision"}
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return sqlType{duck: "BIGINT"}, end, true, nil
	case "FLOAT", "FLOAT4", "FLOAT8", "REAL":
		skipParams()
		return sqlType{duck: "DOUBLE"}, end, true, nil
	case "DOUBLE":
		if j, w := next(end); w == "PRECISION" {
			end = j + 1
		}
		return sqlType{duck: "DOUBLE"}, end, true, nil
	case "VARCHAR", "STRING", "TEXT", "NVARCHAR", "NVARCHAR2", "CHAR", "CHARACTER", "NCHAR":
		if j, w := next(end); w == "VARYING" {
			end = j + 1
		}
		skipParams()
		return sqlType{duck: "VARCHAR", class: classString}, end, true, nil
	case "BINARY", "VARBINARY":
		skipParams()
		return sqlType{duck: "BLOB"}, end, true, nil
	case "BOOLEAN", "BOOL":
		return sqlType{duck: "BOOLEAN"}, end, true, nil
	case "DATE":
		return sqlType{duck: "DATE"}, end, true, nil
	case "DATETIME", "TIMESTAMP_NTZ", "TIMESTAMPNTZ":
		skipParams()
		return sqlType{duck: "TIMESTAMP"}, end, true, nil
	case "TIMESTAMP_LTZ", "TIMESTAMPLTZ", "TIMESTAMP_TZ", "TIMESTAMPTZ":
		skipParams()
		return sqlType{duck: "TIMESTAMPTZ"}, end, true, nil
	case "TIMESTAMP":
		skipParams()
		// TIMESTAMP WITH [LOCAL] TIME ZONE, TIMESTAMP WITHOUT TIME ZONE.
		if j, w := next(end); w == "WITH" || w == "WITHOUT" {
			k, w2 := next(j + 1)
			if w2 == "LOCAL" {
				k, w2 = next(k + 1)
			}
			if w2 == "TIME" {
				if z, w3 := next(k + 1); w3 == "ZONE" {
					end = z + 1
					if w == "WITH" {
						return sqlType{duck: "TIMESTAMPTZ"}, end, true, nil
					}
				}
			}
		}
		// TIMESTAMP_TYPE_MAPPING is TIMESTAMP_NTZ by default.
		return sqlType{duck: "TIMESTAMP"}, end, true, nil
	case "TIME":
		skipParams()
		return sqlType{duck: "TIME"}, end, true, nil
	case "VARIANT":
		return sqlType{duck: "JSON", class: classJSON}, end, true, nil
	case "ARRAY", "OBJECT", "MAP":
		if j := nextSig(nodes, end-1); j >= 0 && nodes[j].isGroup('(') && len(sig(nodes[j].kids)) > 0 {
			duck, err := structuredType(first, nodes[j].kids)
			if err != nil {
				return sqlType{}, 0, false, err
			}
			return sqlType{duck: duck, class: classNested}, j + 1, true, nil
		}
		switch first {
		case "ARRAY":
			skipParams()
			return sqlType{duck: "JSON", class: classJSON, column: arrayColumn}, end, true, nil
		case "OBJECT":
			skipParams()
			return sqlType{duck: "JSON", class: classJSON, column: objectColumn}, end, true, nil
		}
		return sqlType{}, 0, false, nil
	case "GEOGRAPHY", "GEOMETRY":
		skipParams()
		return sqlType{duck: "GEOMETRY"}, end, true, nil
	case "VECTOR":
		return vectorType(nodes, end)
	}
	return sqlType{}, 0, false, nil
}

// maxVectorDimension is the largest VECTOR Snowflake has.
const maxVectorDimension = 4096

// vectorType reads VECTOR(INT | FLOAT, n), whose parameters start at
// nodes[end] (significant), as DuckDB's fixed-size array of 32-bit
// numbers, FLOAT[n] or INTEGER[n]: a cast to it checks the dimension.
// DuckDB-Iceberg has no fixed-size arrays, so an Iceberg table's column
// of one is a list, FLOAT[] or INTEGER[] (see pgwire's icebergTypes),
// which the vector functions read alike.
func vectorType(nodes []*node, end int) (sqlType, int, bool, error) {
	invalid := &Error{Code: CodeSyntax, Message: "invalid VECTOR type",
		Hint: "VECTOR(FLOAT, n) or VECTOR(INT, n), with n from 1 to 4096"}
	j := nextSig(nodes, end-1)
	if j < 0 || !nodes[j].isGroup('(') {
		return sqlType{}, 0, false, invalid
	}
	args := splitArgs(nodes[j].kids)
	if len(args) != 2 {
		return sqlType{}, 0, false, invalid
	}
	elem, dim := trim(args[0]), trim(args[1])
	if len(elem) != 1 || len(dim) != 1 || dim[0].kind != kNumber {
		return sqlType{}, 0, false, invalid
	}
	var duck string
	switch elem[0].word() {
	case "FLOAT":
		duck = "FLOAT"
	case "INT":
		duck = "INTEGER"
	default:
		return sqlType{}, 0, false, invalid
	}
	n, err := strconv.Atoi(dim[0].tok.text)
	if err != nil || n < 1 || n > maxVectorDimension {
		return sqlType{}, 0, false, invalid
	}
	return sqlType{duck: duck + "[" + strconv.Itoa(n) + "]"}, j + 1, true, nil
}

// structuredType maps a structured type, kind ARRAY, OBJECT or MAP with
// the nodes inside its parentheses, to DuckDB's list, struct or map: an
// ARRAY(VARCHAR) is a VARCHAR[], an OBJECT(city VARCHAR, zip NUMBER) a
// STRUCT("city" VARCHAR, "zip" DECIMAL(38,0)), a MAP(VARCHAR, NUMBER) a
// MAP(VARCHAR, DECIMAL(38,0)). Keys of an OBJECT keep the case they are
// written in, as Snowflake compares them; NOT NULL after a type is
// dropped, as Iceberg's nested fields are all optional to DuckDB.
func structuredType(kind string, kids []*node) (string, error) {
	invalid := &Error{Code: CodeSyntax, Message: "invalid " + kind + "(...) type",
		Hint: "ARRAY(type), OBJECT(name type, ...) or MAP(key type, value type)"}
	// elem reads a type and an optional NOT NULL, which must be all of def.
	elem := func(def []*node) (string, error) {
		idx := sig(def)
		if len(idx) == 0 {
			return "", invalid
		}
		t, end, ok, err := parseType(def, idx[0])
		if err != nil {
			return "", err
		}
		if !ok {
			return "", &Error{Code: CodeUnsupported, Message: "type " + render(trim(def)) + " in " + kind + "(...) is not supported"}
		}
		rest := sig(def[end:])
		if len(rest) == 2 && def[end+rest[0]].word() == "NOT" && def[end+rest[1]].word() == "NULL" {
			rest = nil
		}
		if len(rest) > 0 {
			return "", invalid
		}
		return t.columnType(), nil
	}
	args := splitArgs(kids)
	switch kind {
	case "ARRAY":
		if len(args) != 1 {
			return "", invalid
		}
		e, err := elem(args[0])
		return e + "[]", err
	case "MAP":
		if len(args) != 2 {
			return "", invalid
		}
		k, err := elem(args[0])
		if err != nil {
			return "", err
		}
		v, err := elem(args[1])
		return "MAP(" + k + ", " + v + ")", err
	}
	fields := make([]string, len(args))
	for i, a := range args {
		idx := sig(a)
		if len(idx) < 2 {
			return "", invalid
		}
		name, _, ok := a[idx[0]].name()
		if !ok {
			return "", invalid
		}
		t, err := elem(a[idx[1]:])
		if err != nil {
			return "", err
		}
		fields[i] = quoteIdent(name) + " " + t
	}
	return "STRUCT(" + strings.Join(fields, ", ") + ")", nil
}

// ColumnType maps a Snowflake type written as text, such as a masking
// policy's, to DuckDB's type for a column of it: VARCHAR for STRING,
// VARCHAR[] for ARRAY. ok is false for text that is not one of the types
// Igloo maps, which may be DuckDB's own.
func ColumnType(s string) (string, bool) {
	t, ok := typeName(s)
	if !ok {
		return "", false
	}
	return t.columnType(), true
}

// typeName maps a type written as text, as in a format or a TRY_CAST
// target given as a string, to DuckDB's name for it.
func typeName(s string) (sqlType, bool) {
	toks, err := lex(s)
	if err != nil {
		return sqlType{}, false
	}
	nodes, err := build(toks)
	if err != nil {
		return sqlType{}, false
	}
	idx := sig(nodes)
	if len(idx) == 0 {
		return sqlType{}, false
	}
	t, end, ok, err := parseType(nodes, idx[0])
	if err != nil || !ok || nextSig(nodes, end-1) >= 0 {
		return sqlType{}, false
	}
	return t, true
}

// isTypeWord reports whether w, upper-cased, starts a Snowflake type.
func isTypeWord(w string) bool {
	switch strings.ToUpper(w) {
	case "NUMBER", "NUMERIC", "DECIMAL", "DEC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
		"FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "VARCHAR", "STRING", "TEXT", "NVARCHAR", "NVARCHAR2",
		"CHAR", "CHARACTER", "NCHAR", "BINARY", "VARBINARY", "BOOLEAN", "BOOL", "DATE", "DATETIME",
		"TIMESTAMP_NTZ", "TIMESTAMPNTZ", "TIMESTAMP_LTZ", "TIMESTAMPLTZ", "TIMESTAMP_TZ", "TIMESTAMPTZ",
		"TIMESTAMP", "TIME", "VARIANT", "OBJECT", "ARRAY", "MAP", "GEOGRAPHY", "GEOMETRY", "VECTOR":
		return true
	}
	return false
}
