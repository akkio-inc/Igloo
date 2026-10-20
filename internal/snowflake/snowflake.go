// Package snowflake translates Snowflake SQL into DuckDB's dialect, for
// sessions that set sql_dialect = 'snowflake'.
//
// The translator works on Snowflake's tokens, nested by brackets, rather
// than on a full syntax tree: it reads Snowflake's lexical rules (string
// escapes, $$ strings, // comments, $variables), rewrites the constructs
// whose meaning differs or that DuckDB lacks, function calls above all, and
// passes everything else through, so SQL that already means the same in
// both dialects stays exactly as written. Functions DuckDB lacks become
// expressions, or calls of the macros in Macros, which every session
// defines.
//
// Translation is not a security boundary. Igloo checks and runs the DuckDB
// statement this package returns exactly as it would a statement the
// client sent in DuckDB's dialect; whatever the translation got wrong, the
// statement can reach nothing the user could not reach in DuckDB's dialect.
// What the translation must get right is meaning: where DuckDB cannot
// reproduce Snowflake's behaviour it translates to the closest and the
// difference is documented (docs/snowflake-sql.md), or it refuses the
// construct with an Error naming it.
package snowflake

import "strings"

// Split cuts a script of Snowflake SQL into its statements at the
// semicolons outside strings, quoted identifiers and comments, as
// Snowflake's lexer reads them. Statements of only white space and
// comments are dropped. A script Igloo cannot read comes back as one
// statement, whose translation reports the error.
func Split(script string) []string {
	toks, err := lex(script)
	if err != nil {
		return []string{script}
	}
	var out []string
	start, hasCode := 0, false
	end := func(at int) {
		if hasCode {
			out = append(out, strings.TrimSpace(script[start:at]))
		}
		start, hasCode = at+1, false
	}
	for _, t := range toks {
		switch {
		case t.kind == kOp && t.text == ";":
			end(t.pos)
		case t.kind != kSpace && t.kind != kComment:
			hasCode = true
		}
	}
	end(len(script))
	return out
}

// ColumnName is the name of a result column as Snowflake gives it: in
// upper case, unless the statement wrote it as a quoted identifier
// (quoted, from Result.Quoted) or it has upper-case letters of its own, as
// a column created with a quoted mixed-case name does.
func ColumnName(name string, quoted map[string]bool) string {
	if quoted[name] || strings.ToLower(name) != name {
		return name
	}
	return strings.ToUpper(name)
}
