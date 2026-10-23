package snowflake

import (
	"regexp"
	"strconv"
	"strings"
)

// vmode is how a semi-structured value is read where it is used.
type vmode int

const (
	// vJSON keeps it JSON, as Snowflake keeps a VARIANT: in a select list,
	// GROUP BY, or as the argument of a function that builds JSON.
	vJSON vmode = iota
	// vString reads it as text, without quotes around a string, as
	// Snowflake converts a VARIANT compared with text or passed to a
	// string function.
	vString
	// vNumber reads it as a DOUBLE, as Snowflake converts a VARIANT
	// compared with a number or in arithmetic.
	vNumber
)

// pathSeg is one step of a path: a key, an index, or a key or index
// computed as the statement runs.
type pathSeg struct {
	key     string
	index   int
	isIndex bool
	dyn     *node
}

// variant is a value read from semi-structured data: a path into a value,
// or a value FLATTEN produced.
type variant struct {
	base *node
	segs []pathSeg
	// json says base is already a JSON value to read, FLATTEN's VALUE.
	json bool
	mode vmode
	// try reads a number with TRY_CAST, NULL for what is not one.
	try bool
	// column says base is a column, which may be one of Igloo's ARRAY or
	// OBJECT columns, a list or map of strings (see render).
	column bool
	// obj, when set, says base is the VALUE of a FLATTEN of objects that
	// the translation keeps as structs (see structRead).
	obj *structFlatten
	// elem, when set, is what the variant reads in place of base: the
	// element of a lambda that filters a FLATTEN's elements (see
	// structWhere), or the column of the select list that a hoisted
	// FLATTEN's elements are unnested as (see hoistOf), and text says that
	// column holds their text, which every read of it casts (see
	// readsText).
	elem string
	text bool
	// guard says the read is in a WHERE, where text that a hoisted
	// FLATTEN's select list unnested or built is read through sf_text_read.
	guard bool
}

var plainKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// path renders the variant's JSONPath as a SQL expression.
func (v *variant) path() string {
	var parts []string
	var lit strings.Builder
	lit.WriteByte('$')
	for _, s := range v.segs {
		switch {
		case s.dyn != nil:
			parts = append(parts, quoteString(lit.String()))
			lit.Reset()
			e := "(" + s.dyn.String() + ")"
			parts = append(parts, "CASE WHEN typeof("+e+") = 'VARCHAR' THEN '.\"' || "+e+
				" || '\"' ELSE '[' || CAST("+e+" AS VARCHAR) || ']' END")
		case s.isIndex:
			lit.WriteString("[" + strconv.Itoa(s.index) + "]")
		case plainKey.MatchString(s.key):
			lit.WriteString("." + s.key)
		default:
			lit.WriteString(`."` + strings.ReplaceAll(s.key, `"`, `\"`) + `"`)
		}
	}
	if lit.Len() > 0 || len(parts) == 0 {
		parts = append(parts, quoteString(lit.String()))
	}
	return strings.Join(parts, " || ")
}

// render writes the variant in DuckDB SQL for its mode. A path into a
// column goes through sf_get and sf_get_string, which read Igloo's ARRAY
// and OBJECT columns (lists and maps of strings) as Snowflake's values and
// any other column as DuckDB's JSON functions do, JSON text included; one
// into any other value, which is JSON (the translations of Snowflake's
// functions and FLATTEN's VALUE and THIS are), through those functions.
func (v *variant) render() string {
	if s, ok := v.structRead(); ok {
		return s
	}
	base := v.base.String()
	if v.elem != "" {
		base = v.elem
		if v.text && len(v.segs) == 0 {
			if v.guard {
				base = "sf_text_read(" + base + ")"
			}
			switch v.mode {
			case vString:
				return base
			case vNumber:
				if v.try {
					return "TRY_CAST(" + base + " AS DOUBLE)"
				}
				return "CAST(" + base + " AS DOUBLE)"
			}
		}
	}
	extract, extractString := "json_extract(", "json_extract_string("
	if v.column {
		extract, extractString = "sf_get(", "sf_get_string("
	}
	str := extractString + base + ", " + v.path() + ")"
	switch v.mode {
	case vString:
		return str
	case vNumber:
		if v.try {
			return "TRY_CAST(" + str + " AS DOUBLE)"
		}
		return "CAST(" + str + " AS DOUBLE)"
	}
	if v.json && len(v.segs) == 0 {
		return base
	}
	return extract + base + ", " + v.path() + ")"
}
