package pgwire

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// Semi-structured columns of Iceberg tables.
//
// Iceberg tables of format version 1 and 2, the versions Igloo's catalog
// allows, have no type for JSON, and DuckDB's VARIANT needs version 3, so
// DuckDB-Iceberg refuses a column of either ("Type JSON is not a valid
// Iceberg Type"). Igloo stores them as strings instead: a JSON or VARIANT
// column holds its values' JSON text, which Snowflake's dialect reads as
// JSON (VARIANT columns, PARSE_JSON's results, a connection's VARIANT and
// OBJECT columns); inside a list, map or struct, a JSON value is a string
// as Snowflake's TO_VARCHAR writes it, a string as itself and anything else
// as its JSON text, as Igloo's ARRAY and OBJECT columns hold them (lists
// and maps of strings, see package snowflake). Lists, maps and structs of
// other types are Iceberg's own, which other engines read.
//
// icebergTypes rewrites, to that end, the statements that give an Iceberg
// table its columns: the types in CREATE TABLE's column list and in ALTER
// TABLE ... ADD COLUMN, and the query of CREATE TABLE ... AS, whose JSON
// and VARIANT columns it casts. Any other statement, and any statement it
// cannot read, comes back as it was.
func (s *Server) icebergTypes(ctx context.Context, sess *session, query string) string {
	if s.catalogName() == "" || !strings.Contains(strings.ToUpper(query), "TABLE") {
		return query
	}
	if n, err := s.engine.CountStatements(query); err != nil || n != 1 {
		return query
	}
	sc := &scanner{s: query}
	if sc.keywords("ALTER", "TABLE") {
		if !s.changesIceberg(ctx, sess, duckdb.STATEMENT_TYPE_ALTER, query) {
			return query
		}
		return icebergAddColumn(query)
	}
	c, ok := parseCreateTable(query)
	if !ok || c.temporary {
		return query
	}
	if _, err := s.newTableTarget(ctx, sess, c.name); err != nil {
		return query
	}
	if c.query == "" {
		return icebergColumnList(query)
	}
	return s.icebergQueryTypes(ctx, sess, query, c.query)
}

// icebergColumnList rewrites the types in the column list of CREATE TABLE
// query.
func icebergColumnList(query string) string {
	sc := &scanner{s: query}
	sc.keywords("CREATE")
	sc.keywords("OR", "REPLACE")
	sc.keywords("PERSISTENT")
	if !sc.keywords("TABLE") {
		return query
	}
	sc.keywords("IF", "NOT", "EXISTS")
	if _, ok := sc.name(); !ok {
		return query
	}
	sc.skip()
	if sc.i >= len(query) || query[sc.i] != '(' {
		return query
	}
	toks := typeTokens(query[sc.i:])
	var edits []edit
	depth := 0
	start := 1 // the first token of the definition being read
	for k := 1; k < len(toks); k++ {
		switch toks[k].text {
		case "(":
			depth++
			continue
		case ")":
			if depth > 0 {
				depth--
				continue
			}
		case ",":
			if depth > 0 {
				continue
			}
		default:
			continue
		}
		// toks[start:k] is a definition.
		def := toks[start:k]
		if len(def) >= 2 && !isTableConstraint(def[0].text) {
			if _, e := icebergTypeAt(def, 1); e != nil {
				edits = append(edits, e...)
			}
		}
		start = k + 1
		if toks[k].text == ")" {
			break
		}
	}
	out, err := applyEdits(query, shiftEdits(edits, sc.i))
	if err != nil {
		return query
	}
	return out
}

// icebergAddColumn rewrites the type of ALTER TABLE ... ADD [COLUMN] [IF
// NOT EXISTS] name type.
func icebergAddColumn(query string) string {
	toks := typeTokens(query)
	for k := 0; k+1 < len(toks); k++ {
		if !strings.EqualFold(toks[k].text, "ADD") {
			continue
		}
		j := k + 1
		if strings.EqualFold(toks[j].text, "COLUMN") {
			j++
		}
		if j+2 < len(toks) && strings.EqualFold(toks[j].text, "IF") {
			j += 3
		}
		if j+1 >= len(toks) || isTableConstraint(toks[j].text) {
			return query
		}
		_, edits := icebergTypeAt(toks, j+1)
		out, err := applyEdits(query, edits)
		if err != nil {
			return query
		}
		return out
	}
	return query
}

// icebergQueryTypes rewrites CREATE TABLE ... AS q, query, so that the
// columns of q that Iceberg cannot hold reach the table as strings.
func (s *Server) icebergQueryTypes(ctx context.Context, sess *session, query, q string) string {
	at := strings.LastIndex(query, q)
	if at < 0 {
		return query
	}
	rows, err := sess.conn.QueryContext(ctx, "DESCRIBE "+q, nil)
	if err != nil {
		return query
	}
	defer rows.Close() //nolint:errcheck
	var casts []string
	dest := make([]driver.Value, len(rows.Columns()))
	for rows.Next(dest) == nil {
		name, _ := dest[0].(string)
		typ, _ := dest[1].(string)
		if c := icebergCast(quoteIdent(name), typ); c != "" {
			casts = append(casts, c+" AS "+quoteIdent(name))
		}
	}
	if len(casts) == 0 {
		return query
	}
	// On a line of its own, after a query that may end in a comment.
	wrapped := "SELECT * REPLACE (" + strings.Join(casts, ", ") + ") FROM (" + q + "\n) AS _igloo_values"
	return query[:at] + wrapped + query[at+len(q):]
}

// icebergCast is how a column col of DuckDB type typ reaches an Iceberg
// table as a type Iceberg has, or "" when typ is one: JSON as its text,
// and a list, map or struct with JSON in it as the same type with strings
// in its place, each holding its value's JSON text, as an INSERT of such
// values into the table stores them. VARIANT, whose text is not JSON, goes
// through JSON: alone, as its JSON text; in a list, map or struct, as
// Snowflake's TO_VARCHAR writes it, a string as itself and anything else as
// its JSON text.
func icebergCast(col, typ string) string {
	toks := typeTokens(typ)
	end, edits := icebergTypeAt(toks, 0)
	if len(edits) == 0 || end != len(toks) {
		return ""
	}
	switch strings.ToUpper(typ) {
	case "JSON":
		return "CAST(" + col + " AS VARCHAR)"
	case "VARIANT":
		return "CAST(CAST(" + col + " AS JSON) AS VARCHAR)"
	}
	target, err := applyEdits(typ, edits)
	if err != nil {
		return ""
	}
	for _, tok := range toks {
		if strings.EqualFold(tok.text, "VARIANT") {
			return "CAST(CAST(" + col + " AS JSON) AS " + target + ")"
		}
	}
	return "CAST(" + col + " AS " + target + ")"
}

// typeToken is a token of a type or a column list: a word, a quoted
// identifier, a string, a number or a punctuation character.
type typeToken struct {
	text       string
	start, end int
}

// typeTokens splits SQL text into tokens, comments dropped.
func typeTokens(text string) []typeToken {
	var out []typeToken
	pos := 0
	for _, seg := range sqllex.Segments(text) {
		start := pos
		pos += len(seg.Text)
		switch seg.Kind {
		case sqllex.Comment:
			continue
		case sqllex.String, sqllex.Ident:
			out = append(out, typeToken{seg.Text, start, pos})
			continue
		}
		for i := 0; i < len(seg.Text); {
			c := seg.Text[i]
			switch {
			case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
				i++
			case sqllex.IdentByte(c):
				j := i
				for j < len(seg.Text) && sqllex.IdentByte(seg.Text[j]) {
					j++
				}
				out = append(out, typeToken{seg.Text[i:j], start + i, start + j})
				i = j
			default:
				out = append(out, typeToken{seg.Text[i : i+1], start + i, start + i + 1})
				i++
			}
		}
	}
	return out
}

// icebergTypeAt reads the type that starts at toks[i] and returns the
// index just past it, and the edits that turn JSON and VARIANT in it into
// VARCHAR. It reads DuckDB's type names: a name of one or more words, its
// parameters (a struct's or union's fields, a map's key and value types),
// and any [] or [n] after it.
func icebergTypeAt(toks []typeToken, i int) (int, []edit) {
	if i >= len(toks) {
		return i, nil
	}
	var edits []edit
	name := strings.ToUpper(toks[i].text)
	switch name {
	case "JSON", "VARIANT":
		edits = append(edits, edit{toks[i].start, toks[i].end, "VARCHAR"})
	}
	i++
	// Names of more than one word.
	for i < len(toks) {
		w := strings.ToUpper(toks[i].text)
		if name == "DOUBLE" && w == "PRECISION" || (name == "CHARACTER" || name == "BIT") && w == "VARYING" ||
			(name == "TIMESTAMP" || name == "TIME") && (w == "WITH" || w == "WITHOUT") {
			i++
			if w == "WITH" || w == "WITHOUT" {
				for i < len(toks) && (strings.EqualFold(toks[i].text, "TIME") || strings.EqualFold(toks[i].text, "ZONE")) {
					i++
				}
			}
			continue
		}
		break
	}
	if i < len(toks) && toks[i].text == "(" {
		switch name {
		case "STRUCT", "ROW", "UNION":
			// (name type, ...)
			i++
			for i < len(toks) && toks[i].text != ")" {
				end, e := icebergTypeAt(toks, i+1)
				edits = append(edits, e...)
				i = end
				if i < len(toks) && toks[i].text == "," {
					i++
				}
			}
			i++
		case "MAP":
			// (type, type)
			i++
			for i < len(toks) && toks[i].text != ")" {
				end, e := icebergTypeAt(toks, i)
				edits = append(edits, e...)
				i = end
				if i < len(toks) && toks[i].text == "," {
					i++
				}
			}
			i++
		default:
			// (precision, scale), an enum's values and the like.
			depth := 0
			for ; i < len(toks); i++ {
				if toks[i].text == "(" {
					depth++
				} else if toks[i].text == ")" {
					if depth--; depth == 0 {
						i++
						break
					}
				}
			}
		}
	}
	for i < len(toks) && toks[i].text == "[" {
		for i < len(toks) && toks[i].text != "]" {
			i++
		}
		i++
	}
	return i, edits
}

// isTableConstraint reports whether a column list's entry starting with
// word w is a constraint of the table rather than a column.
func isTableConstraint(w string) bool {
	switch strings.ToUpper(w) {
	case "CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN":
		return true
	}
	return false
}
