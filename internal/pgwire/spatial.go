package pgwire

import (
	"strconv"
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The spatial extension opens files through GDAL in ST_Read, ST_Read_Meta,
// their replacement scans (FROM 'boundaries.gpkg' and FROM 'x.fgb') and
// COPY ... (FORMAT GDAL). GDAL hands DuckDB only the path it was given:
// files a dataset names inside itself, such as a VRT file's sources, it
// opens on its own, from the server's disk or the network, outside DuckDB's
// sandbox. So no one may use those, admins included. The rest of spatial
// reads through DuckDB's file system (ST_ReadSHP, ST_ReadOSM, GeoParquet
// with read_parquet) and stays confined to the warehouse and the allowed
// locations, like read_parquet.

// gdalUse says how query would reach GDAL, or "" if it would not. Views and
// macros are checked when they are created, so a statement that names
// neither ST_Read nor such a file cannot reach GDAL through them.
func gdalUse(query string) string {
	words := sqllex.CodeWords(query)
	switch {
	case mentionsFunction(query, words, "st_read"):
		return "ST_Read"
	case mentionsFunction(query, words, "st_read_meta"):
		return "ST_Read_Meta"
	}
	for _, s := range sqlStrings(query) {
		// The extension replaces a table named by a string or quoted
		// identifier that ends like this with ST_Read.
		lower := strings.ToLower(s)
		if strings.HasSuffix(lower, ".gpkg") || strings.HasSuffix(lower, ".fgb") {
			return "reading " + s
		}
	}
	// COPY may follow other statements, EXPLAIN ANALYZE or PREPARE.
	for _, stmt := range sqllex.Split(query) {
		format, ok := copyFormat(stmt.Text)
		switch {
		case !ok:
			return "a COPY format that is not a plain name"
		case strings.EqualFold(format, "gdal"):
			return "COPY ... (FORMAT GDAL)"
		}
	}
	return ""
}

// sqlStrings returns the values of query's string literals and quoted
// identifiers. Escapes in E'...' strings are decoded as DuckDB decodes
// them, so they cannot hide a file name.
func sqlStrings(query string) []string {
	var out []string
	segs := sqllex.Segments(query)
	for i, seg := range segs {
		switch seg.Kind {
		case sqllex.Ident:
			if len(seg.Text) >= 2 {
				out = append(out, strings.ReplaceAll(seg.Text[1:len(seg.Text)-1], `""`, `"`))
			}
		case sqllex.String:
			escapes := i > 0 && segs[i-1].Kind == sqllex.Code && escapePrefix(segs[i-1].Text)
			out = append(out, literalValue(seg.Text, escapes))
		}
	}
	return out
}

// escapePrefix reports whether code ends with the E of an E'...' string.
func escapePrefix(code string) bool {
	n := len(code)
	return n > 0 && (code[n-1] == 'E' || code[n-1] == 'e') && (n < 2 || !sqllex.IdentByte(code[n-2]))
}

// literalValue returns the value of a string literal: '...' with doubled
// quotes, E'...' with backslash escapes, or $tag$...$tag$.
func literalValue(text string, escapes bool) string {
	if strings.HasPrefix(text, "$") {
		tag := dollarTagOf(text)
		return strings.TrimSuffix(strings.TrimPrefix(text, tag), tag)
	}
	body := strings.TrimSuffix(strings.TrimPrefix(text, "'"), "'")
	if !escapes {
		return strings.ReplaceAll(body, "''", "'")
	}
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == '\'' && i+1 < len(body) && body[i+1] == '\'':
			b.WriteByte('\'')
			i++
		case c != '\\' || i+1 >= len(body):
			b.WriteByte(c)
		default:
			i++
			switch e := body[i]; {
			case e == 'x' || e == 'X':
				j := i + 1
				for j < len(body) && j < i+3 && isHex(body[j]) {
					j++
				}
				if n, err := strconv.ParseUint(body[i+1:j], 16, 8); err == nil {
					b.WriteByte(byte(n))
				}
				i = j - 1
			case e >= '0' && e <= '7':
				j := i
				for j < len(body) && j < i+3 && body[j] >= '0' && body[j] <= '7' {
					j++
				}
				n, _ := strconv.ParseUint(body[i:j], 8, 8)
				b.WriteByte(byte(n))
				i = j - 1
			case e == 'b':
				b.WriteByte('\b')
			case e == 'f':
				b.WriteByte('\f')
			case e == 'n':
				b.WriteByte('\n')
			case e == 'r':
				b.WriteByte('\r')
			case e == 't':
				b.WriteByte('\t')
			default:
				b.WriteByte(e)
			}
		}
	}
	return b.String()
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// dollarTagOf returns the $tag$ that opens a dollar-quoted string.
func dollarTagOf(text string) string {
	if i := strings.IndexByte(text[1:], '$'); i >= 0 {
		return text[:i+2]
	}
	return text
}

// copyFormat returns the FORMAT option of the COPY or EXPORT DATABASE in a
// statement, or "" when it has none or names none (DuckDB then picks the
// format from the file's extension, never GDAL). ok is false when the
// option's value is anything but a name, a quoted name or a plain string,
// such as an expression, a variable or a parameter, whose value only
// DuckDB knows.
func copyFormat(stmt string) (format string, ok bool) {
	toks := optionTokens(stmt)
	// Options follow the path, which follows TO or FROM in COPY, and
	// DATABASE in EXPORT DATABASE.
	start := -1
	for i, t := range toks {
		// COPY starts the statement, or follows EXPLAIN [ANALYZE] [(...)]
		// or PREPARE name AS.
		if i > 0 {
			switch p := toks[i-1]; {
			case p.word == "EXPLAIN", p.word == "ANALYZE", p.word == "ANALYSE", p.word == "AS", p.text == ")" && p.depth == 0:
			default:
				continue
			}
		}
		if t.depth == 0 && (t.word == "COPY" || t.word == "EXPORT") {
			for j := i + 1; j < len(toks); j++ {
				if w := toks[j].word; toks[j].depth == 0 && (w == "TO" || w == "FROM" || w == "DATABASE") {
					start = j + 1
					break
				}
			}
			break
		}
	}
	if start < 0 {
		return "", true
	}
	// An option starts a parenthesized list at the top level, or follows a
	// comma in one.
	for i := max(start, 1); i < len(toks); i++ {
		prev := toks[i-1]
		first := toks[i].depth == 1 && (prev.text == "(" && prev.depth == 0 || prev.text == "," && prev.depth == 1)
		if !first || !strings.EqualFold(toks[i].name, "format") {
			continue
		}
		// The value runs to the next comma in the list or its end.
		var value []optionToken
		for j := i + 1; j < len(toks) && toks[j].depth > 0 && !(toks[j].depth == 1 && toks[j].text == ","); j++ {
			value = append(value, toks[j])
		}
		if len(value) != 1 || value[0].name == "" {
			return "", false
		}
		if format == "" || strings.EqualFold(value[0].name, "gdal") {
			format = value[0].name
		}
	}
	return format, true
}

// optionToken is a token of SQL code with the parenthesis depth it is at.
// Parentheses themselves are at the depth outside them.
type optionToken struct {
	text string
	// word is an unquoted name in upper case.
	word string
	// name is a name, quoted or not, or a plain string literal's value;
	// empty for anything else, including E'...' strings with escapes.
	name  string
	depth int
}

func optionTokens(query string) []optionToken {
	var out []optionToken
	depth := 0
	segs := sqllex.Segments(query)
	for k, seg := range segs {
		switch seg.Kind {
		case sqllex.Ident:
			t := optionToken{text: seg.Text, depth: depth}
			if len(seg.Text) >= 2 {
				t.name = strings.ReplaceAll(seg.Text[1:len(seg.Text)-1], `""`, `"`)
			}
			out = append(out, t)
		case sqllex.String:
			t := optionToken{text: seg.Text, depth: depth}
			escapes := k > 0 && segs[k-1].Kind == sqllex.Code && escapePrefix(segs[k-1].Text)
			if !escapes || !strings.Contains(seg.Text, `\`) {
				t.name = literalValue(seg.Text, escapes)
			}
			out = append(out, t)
		case sqllex.Code:
			s := seg.Text
			for i := 0; i < len(s); {
				c := s[i]
				switch {
				case sqllex.IdentByte(c):
					j := i
					for j < len(s) && sqllex.IdentByte(s[j]) {
						j++
					}
					w := s[i:j]
					// The E of an E'...' string belongs to the string.
					if j == len(s) && k+1 < len(segs) && segs[k+1].Kind == sqllex.String && escapePrefix(w) && len(w) == 1 {
						i = j
						continue
					}
					t := optionToken{text: w, word: strings.ToUpper(w), depth: depth}
					// Names start with a letter; $1 is a parameter.
					if c := w[0]; c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80 {
						t.name = w
					}
					out = append(out, t)
					i = j
				case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
					i++
				case c == '(':
					out = append(out, optionToken{text: "(", depth: depth})
					depth++
					i++
				case c == ')':
					depth--
					out = append(out, optionToken{text: ")", depth: depth})
					i++
				default:
					out = append(out, optionToken{text: string(c), depth: depth})
					i++
				}
			}
		}
	}
	return out
}
