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

// pathKey writes key as a step of DuckDB's JSONPath, ."key" with its
// backslashes and double quotes escaped, as DuckDB reads them back.
func pathKey(key string) string {
	if plainKey.MatchString(key) {
		return "." + key
	}
	return `."` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(key) + `"`
}

// paths renders the variant's path as the paths of the json_extract calls
// that read it one after another, each a SQL expression. DuckDB's JSONPath
// cannot name the empty key and '*' ('$.""' is an error and '$."*"' reads
// every value), which a JSON pointer, '/' or '/*', reads alone. A key or
// index computed as the statement runs is read alone too, by the path
// sf_path_step writes for it on each row, a pointer for those two keys, so
// that no other step is read as a pointer reads it: a pointer reads an
// index of an object as a key, and a key of digits of an array as an
// index, where Snowflake finds nothing.
func (v *variant) paths() []string {
	var parts []string
	lit := "$"
	flush := func() {
		if lit != "$" {
			parts = append(parts, quoteString(lit))
			lit = "$"
		}
	}
	for _, s := range v.segs {
		switch {
		case s.dyn != nil:
			flush()
			parts = append(parts, "sf_path_step("+s.dyn.String()+")")
		case s.isIndex:
			lit += "[" + strconv.Itoa(s.index) + "]"
		case s.key == "" || s.key == "*":
			flush()
			parts = append(parts, quoteString("/"+s.key))
		default:
			lit += pathKey(s.key)
		}
	}
	flush()
	if len(parts) == 0 {
		parts = append(parts, quoteString("$"))
	}
	return parts
}

// extract writes the reads of the variant's path from base: json_extract,
// or json_extract_string for the last when str is set. first and
// firstString are the functions of the first read, which reads base.
func (v *variant) extract(base, first, firstString string, str bool) string {
	pre, post := v.reads(first, firstString, str)
	return pre + base + post
}

// reads is extract's text before and after base.
func (v *variant) reads(first, firstString string, str bool) (pre, post string) {
	parts := v.paths()
	for i, p := range parts {
		fn := "json_extract("
		switch {
		case i == 0 && i == len(parts)-1 && str:
			fn = firstString
		case i == 0:
			fn = first
		case i == len(parts)-1 && str:
			fn = "json_extract_string("
		}
		pre = fn + pre
		post += ", " + p + ")"
	}
	return pre, post
}

// extractJSON writes the reads of path segs from base, a JSON value, as
// JSON, or as text when str is set.
func extractJSON(base string, segs []pathSeg, str bool) string {
	return (&variant{segs: segs}).extract(base, "json_extract(", "json_extract_string(", str)
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
		if v.text {
			// The element as JSON, as a FLATTEN of a SPLIT's strings reads
			// it; NULL is JSON null, which a test for NULL reads as a
			// FLATTEN's VALUE, never NULL (see readsText).
			base = "coalesce(to_json(" + base + "), CAST('null' AS JSON))"
		}
	}
	extract, extractString := "json_extract(", "json_extract_string("
	if v.column {
		extract, extractString = "sf_get(", "sf_get_string("
	}
	str := v.extract(base, extract, extractString, true)
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
	return v.extract(base, extract, extractString, false)
}
