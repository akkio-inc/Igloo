package snowflake

import (
	"strings"
	"unicode/utf8"
)

// kind is what a node of a statement is.
type kind uint8

const (
	kSpace   kind = iota // white space
	kComment             // -- ..., // ..., /* ... */
	kWord                // an unquoted name or keyword, as written
	kQuoted              // a "quoted identifier"
	kString              // a string literal
	kNumber              // a numeric literal
	kVar                 // $name, a session variable
	kParam               // $1 or ?, a parameter
	kOp                  // an operator or punctuation
	kGroup               // ( ... ), [ ... ] or { ... }
	kRaw                 // DuckDB text the translation wrote
	kSeq                 // nodes rendered one after another
	kVariant             // a value read from semi-structured data
)

// token is a lexical token of Snowflake SQL.
type token struct {
	kind kind
	// text is the token as written. For a string it is the DuckDB literal
	// with the same value (see quoteString), unless raw.
	text string
	// val is a string's value, a quoted identifier's name, or a variable's
	// name.
	val string
	// pos and end are the token's byte offsets in the statement.
	pos, end int
	// raw marks a PostgreSQL E'...' string, which is passed on as written.
	raw bool
}

// lex splits Snowflake SQL into tokens. It follows Snowflake's lexical
// rules where they differ from DuckDB's (PostgreSQL's): backslash escapes
// in string literals, $$...$$ strings without tags, // comments, block
// comments that do not nest, and $name session variables.
func lex(src string) ([]token, error) {
	var toks []token
	add := func(k kind, start, end int, val string) {
		toks = append(toks, token{kind: k, text: src[start:end], val: val, pos: start, end: end})
	}
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			j := i
			for j < len(src) && strings.IndexByte(" \t\n\r\f\v", src[j]) >= 0 {
				j++
			}
			add(kSpace, i, j, "")
			i = j
		case strings.HasPrefix(src[i:], "--") || strings.HasPrefix(src[i:], "//"):
			j := strings.IndexByte(src[i:], '\n')
			if j < 0 {
				j = len(src) - i
			}
			add(kComment, i, i+j, "")
			i += j
		case strings.HasPrefix(src[i:], "/*"):
			j := strings.Index(src[i+2:], "*/")
			if j < 0 {
				return nil, syntaxError(i, "unterminated /* comment")
			}
			add(kComment, i, i+2+j+2, "")
			i += 2 + j + 2
		case c == '\'':
			// E'...' right after the word E is PostgreSQL's escape string,
			// which drivers send; it is passed on as written.
			if n := len(toks); n > 0 && toks[n-1].kind == kWord && toks[n-1].end == i && strings.EqualFold(toks[n-1].text, "E") {
				end, err := closeEString(src, i)
				if err != nil {
					return nil, err
				}
				prev := toks[n-1]
				toks[n-1] = token{kind: kString, text: src[prev.pos:end], pos: prev.pos, end: end, raw: true}
				i = end
				continue
			}
			val, end, err := readString(src, i)
			if err != nil {
				return nil, err
			}
			// X'...' right after the word X is a BINARY literal in hex,
			// which DuckDB would read as a string.
			if n := len(toks); n > 0 && toks[n-1].kind == kWord && toks[n-1].end == i && strings.EqualFold(toks[n-1].text, "X") {
				if len(val)%2 != 0 || strings.Trim(val, "0123456789abcdefABCDEF") != "" {
					return nil, syntaxError(i, "X'...' takes pairs of hexadecimal digits")
				}
				prev := toks[n-1]
				toks[n-1] = token{kind: kRaw, text: "unhex(" + quoteString(val) + ")", pos: prev.pos, end: end}
				i = end
				continue
			}
			if strings.IndexByte(val, 0) >= 0 {
				// DuckDB would read the statement only up to it.
				return nil, syntaxError(i, "a string may not hold a NUL character")
			}
			toks = append(toks, token{kind: kString, text: src[i:end], val: val, pos: i, end: end})
			i = end
		case c == '$' && strings.HasPrefix(src[i:], "$$"):
			j := strings.Index(src[i+2:], "$$")
			if j < 0 {
				return nil, syntaxError(i, "unterminated $$ string")
			}
			add(kString, i, i+2+j+2, src[i+2:i+2+j])
			i += 2 + j + 2
		case c == '$' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9':
			j := i + 1
			for j < len(src) && src[j] >= '0' && src[j] <= '9' {
				j++
			}
			add(kParam, i, j, "")
			i = j
		case c == '$' && i+1 < len(src) && identStart(src[i+1]):
			j := i + 1
			for j < len(src) && identByte(src[j]) {
				j++
			}
			add(kVar, i, j, strings.ToLower(src[i+1:j]))
			i = j
		case c == '"':
			name, end, err := readQuoted(src, i)
			if err != nil {
				return nil, err
			}
			add(kQuoted, i, end, name)
			i = end
		case c >= '0' && c <= '9' || c == '.' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9':
			j := i
			for j < len(src) && src[j] >= '0' && src[j] <= '9' {
				j++
			}
			if j < len(src) && src[j] == '.' {
				j++
				for j < len(src) && src[j] >= '0' && src[j] <= '9' {
					j++
				}
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && src[k] >= '0' && src[k] <= '9' {
					for k < len(src) && src[k] >= '0' && src[k] <= '9' {
						k++
					}
					j = k
				}
			}
			add(kNumber, i, j, "")
			i = j
		case identStart(c):
			j := i
			for j < len(src) && identByte(src[j]) {
				j++
			}
			add(kWord, i, j, "")
			i = j
		case c == '?':
			add(kParam, i, i+1, "")
			i++
		default:
			n := 1
			for _, op := range []string{"->>", "::", "=>", "->", "<=", ">=", "<>", "!=", "||", "==", ":="} {
				if strings.HasPrefix(src[i:], op) {
					n = len(op)
					break
				}
			}
			add(kOp, i, i+n, "")
			i += n
		}
	}
	return toks, nil
}

func identStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

func identByte(c byte) bool {
	return identStart(c) || c >= '0' && c <= '9' || c == '$'
}

// readString reads the Snowflake string literal that starts at src[start],
// a quote, and returns its value and the index just past it. A backslash
// escapes the next character, as in Snowflake.
func readString(src string, start int) (string, int, error) {
	var b strings.Builder
	for i := start + 1; i < len(src); {
		c := src[i]
		switch {
		case c == '\'':
			if i+1 < len(src) && src[i+1] == '\'' {
				b.WriteByte('\'')
				i += 2
				continue
			}
			return b.String(), i + 1, nil
		case c == '\\' && i+1 < len(src):
			n := readEscape(src[i+1:], &b)
			i += 1 + n
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", 0, syntaxError(start, "unterminated string literal")
}

// readEscape decodes the escape sequence at the start of s, the text after
// a backslash, into b and returns how many bytes it took.
func readEscape(s string, b *strings.Builder) int {
	hex := func(n int) (rune, bool) {
		if len(s) < 1+n {
			return 0, false
		}
		var r rune
		for _, c := range s[1 : 1+n] {
			switch {
			case c >= '0' && c <= '9':
				r = r*16 + c - '0'
			case c >= 'a' && c <= 'f':
				r = r*16 + c - 'a' + 10
			case c >= 'A' && c <= 'F':
				r = r*16 + c - 'A' + 10
			default:
				return 0, false
			}
		}
		return r, true
	}
	switch c := s[0]; c {
	case 'b':
		b.WriteByte('\b')
	case 'f':
		b.WriteByte('\f')
	case 'n':
		b.WriteByte('\n')
	case 'r':
		b.WriteByte('\r')
	case 't':
		b.WriteByte('\t')
	case 'x':
		if r, ok := hex(2); ok {
			b.WriteRune(r)
			return 3
		}
		b.WriteByte('x')
	case 'u':
		if r, ok := hex(4); ok {
			b.WriteRune(r)
			return 5
		}
		b.WriteByte('u')
	case '0', '1', '2', '3', '4', '5', '6', '7':
		n, r := 0, rune(0)
		for n < 3 && n < len(s) && s[n] >= '0' && s[n] <= '7' {
			r = r*8 + rune(s[n]-'0')
			n++
		}
		b.WriteRune(r)
		return n
	default:
		// Any other character stands for itself, a quote or a backslash
		// included.
		_, size := utf8.DecodeRuneInString(s)
		b.WriteString(s[:size])
		return size
	}
	return 1
}

// closeEString returns the index just past the PostgreSQL E'...' string
// whose quote is at src[start].
func closeEString(src string, start int) (int, error) {
	for i := start + 1; i < len(src); i++ {
		switch src[i] {
		case '\\':
			i++
		case '\'':
			if i+1 < len(src) && src[i+1] == '\'' {
				i++
				continue
			}
			return i + 1, nil
		}
	}
	return 0, syntaxError(start, "unterminated string literal")
}

// readQuoted reads the quoted identifier that starts at src[start] and
// returns its name and the index just past it.
func readQuoted(src string, start int) (string, int, error) {
	var b strings.Builder
	for i := start + 1; i < len(src); i++ {
		if src[i] == '"' {
			if i+1 < len(src) && src[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			if b.Len() == 0 {
				return "", 0, syntaxError(start, `an empty quoted identifier ("") is not allowed`)
			}
			return b.String(), i + 1, nil
		}
		b.WriteByte(src[i])
	}
	return "", 0, syntaxError(start, "unterminated quoted identifier")
}

// quoteString writes s as a DuckDB string literal, which reads backslashes
// as themselves.
func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// quoteIdent writes name as a DuckDB quoted identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// commentText rewrites a Snowflake comment as DuckDB reads it: a //
// comment becomes --, and a block comment, which does not nest in
// Snowflake but does in DuckDB, has any /* inside it broken up.
func commentText(c string) string {
	switch {
	case strings.HasPrefix(c, "//"):
		return "--" + c[2:]
	case strings.HasPrefix(c, "/*"):
		inner := c[2 : len(c)-2]
		return "/*" + strings.ReplaceAll(inner, "/*", "/ *") + "*/"
	}
	return c
}
