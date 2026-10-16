// Package sqllex splits SQL text the way DuckDB's (PostgreSQL's) lexer
// does: into code, string literals, quoted identifiers and comments.
// Authorization, catalog query rewrites and view name rewrites use it to look
// only at SQL code.
package sqllex

import (
	"strings"
	"unicode"
)

// Kind says how DuckDB reads a stretch of query text.
type Kind int

const (
	Code    Kind = iota // SQL code
	String              // '...', E'...', $tag$...$tag$
	Ident               // "quoted identifier"
	Comment             // -- ... and /* ... */ (nested)
)

// Segment is a stretch of query text of one kind.
type Segment struct {
	Text string
	Kind Kind
}

// Segments splits query into SQL code and the parts DuckDB does not read as
// code: string literals, quoted identifiers and comments. Authorization
// looks only at code, so this follows DuckDB's (PostgreSQL's) lexical rules
// closely; where the two could disagree it errs toward treating text as
// code, which makes checks stricter rather than looser.
func Segments(query string) []Segment {
	var out []Segment
	code := 0
	emit := func(end int, kind Kind, next int) {
		if end > code {
			out = append(out, Segment{query[code:end], Code})
		}
		out = append(out, Segment{query[end:next], kind})
		code = next
	}

	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '\'':
			// E'..' allows backslash escapes, but only when the E is a token
			// of its own: in name'..' the quote starts an ordinary string.
			escapes := i > 0 && (query[i-1] == 'E' || query[i-1] == 'e') && (i < 2 || !IdentByte(query[i-2]))
			end := CloseQuote(query, i, '\'', escapes)
			emit(i, String, end)
			i = end
		case c == '"':
			end := CloseQuote(query, i, '"', false)
			emit(i, Ident, end)
			i = end
		case c == '-' && strings.HasPrefix(query[i:], "--"):
			end := len(query)
			if n := strings.IndexByte(query[i:], '\n'); n >= 0 {
				end = i + n
			}
			emit(i, Comment, end)
			i = end
		case c == '/' && strings.HasPrefix(query[i:], "/*"):
			end := CloseComment(query, i)
			emit(i, Comment, end)
			i = end
		case c == '$' && (i == 0 || !IdentByte(query[i-1])):
			tag := dollarTag(query[i:])
			if tag == "" {
				i++
				continue
			}
			end := len(query)
			if n := strings.Index(query[i+len(tag):], tag); n >= 0 {
				end = i + len(tag) + n + len(tag)
			}
			emit(i, String, end)
			i = end
		default:
			i++
		}
	}
	if code < len(query) {
		out = append(out, Segment{query[code:], Code})
	}
	return out
}

// dollarTag returns the opening $tag$ at the start of s, or "". A tag is
// empty or an identifier that does not start with a digit, so $1 is a
// parameter, not a quote.
func dollarTag(s string) string {
	for i := 1; i < len(s); i++ {
		switch c := s[i]; {
		case c == '$':
			return s[:i+1]
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80:
		case c >= '0' && c <= '9' && i > 1:
		default:
			return ""
		}
	}
	return ""
}

// CloseQuote returns the index just past the quote that closes the literal
// opened at query[start]. A doubled quote stands for itself.
func CloseQuote(query string, start int, q byte, escapes bool) int {
	for i := start + 1; i < len(query); i++ {
		switch {
		case escapes && query[i] == '\\':
			i++
		case query[i] == q:
			if i+1 < len(query) && query[i+1] == q {
				i++
				continue
			}
			return i + 1
		}
	}
	return len(query)
}

// CloseComment returns the index just past the block comment opened at
// query[start], honouring nesting as PostgreSQL's lexer does.
func CloseComment(query string, start int) int {
	depth := 0
	for i := start; i+1 < len(query); i++ {
		switch query[i : i+2] {
		case "/*":
			depth++
			i++
		case "*/":
			depth--
			i++
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(query)
}

// IdentByte reports whether c can be part of an unquoted identifier.
func IdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}

// MapCode applies fn to the stretches of query that are SQL code, leaving
// string literals, quoted identifiers and comments untouched.
func MapCode(query string, fn func(string) string) string {
	var out strings.Builder
	for _, s := range Segments(query) {
		if s.Kind == Code {
			out.WriteString(fn(s.Text))
		} else {
			out.WriteString(s.Text)
		}
	}
	return out.String()
}

// CodeOnly returns query with every literal, quoted identifier and comment
// replaced by a space.
func CodeOnly(query string) string {
	var out strings.Builder
	for _, s := range Segments(query) {
		if s.Kind == Code {
			out.WriteString(s.Text)
		} else {
			out.WriteByte(' ')
		}
	}
	return out.String()
}

// CodeWords returns the words in query's SQL code, upper-cased, in order.
// Words inside literals, quoted identifiers and comments are not included.
func CodeWords(query string) []string {
	words := strings.FieldsFunc(CodeOnly(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
	})
	for i := range words {
		words[i] = strings.ToUpper(words[i])
	}
	return words
}

// QuotedIdentifiers returns the contents of query's "quoted identifiers",
// lower-cased: DuckDB matches names case-insensitively even when quoted.
func QuotedIdentifiers(query string) []string {
	var out []string
	for _, s := range Segments(query) {
		if s.Kind == Ident && len(s.Text) >= 2 {
			name := strings.ReplaceAll(s.Text[1:len(s.Text)-1], `""`, `"`)
			out = append(out, strings.ToLower(name))
		}
	}
	return out
}
