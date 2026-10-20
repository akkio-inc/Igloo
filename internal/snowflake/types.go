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
)

// sqlType is a Snowflake type written in a statement, as DuckDB names it.
type sqlType struct {
	duck  string
	class typeClass
}

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
	case "VARIANT", "OBJECT", "ARRAY":
		skipParams()
		return sqlType{duck: "JSON", class: classJSON}, end, true, nil
	case "GEOGRAPHY", "GEOMETRY":
		skipParams()
		return sqlType{duck: "GEOMETRY"}, end, true, nil
	case "VECTOR":
		return sqlType{}, 0, false, unsupported("The VECTOR type", "store embeddings as ARRAY (JSON) or FLOAT columns")
	}
	return sqlType{}, 0, false, nil
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
		"TIMESTAMP", "TIME", "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY", "VECTOR":
		return true
	}
	return false
}
