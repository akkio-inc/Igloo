package pgwire

import (
	"strings"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// scanner reads the start of a statement token by token, for the DDL
// statements Igloo carries out itself (see replace.go, comments.go and
// views.go). It understands just enough SQL for their headers: words,
// possibly qualified and quoted names, and string literals.
type scanner struct {
	s string
	i int
}

// skip moves past whitespace and comments.
func (sc *scanner) skip() {
	for sc.i < len(sc.s) {
		switch {
		case strings.ContainsRune(" \t\r\n\f", rune(sc.s[sc.i])):
			sc.i++
		case strings.HasPrefix(sc.s[sc.i:], "--"):
			if n := strings.IndexByte(sc.s[sc.i:], '\n'); n >= 0 {
				sc.i += n
			} else {
				sc.i = len(sc.s)
			}
		case strings.HasPrefix(sc.s[sc.i:], "/*"):
			sc.i = sqllex.CloseComment(sc.s, sc.i)
		default:
			return
		}
	}
}

// word returns the next bare word, upper-cased, without consuming it.
func (sc *scanner) word() string {
	sc.skip()
	j := sc.i
	for j < len(sc.s) && sqllex.IdentByte(sc.s[j]) {
		j++
	}
	return strings.ToUpper(sc.s[sc.i:j])
}

// keywords consumes words if the statement continues with exactly them.
func (sc *scanner) keywords(words ...string) bool {
	start := sc.i
	for _, w := range words {
		if sc.word() != w {
			sc.i = start
			return false
		}
		sc.i += len(w)
	}
	return true
}

// name reads an identifier chain such as lake."My Schema".t. Bare parts are
// folded to lower case, as DuckDB does; quoted parts keep their case.
func (sc *scanner) name() ([]string, bool) {
	var parts []string
	for {
		sc.skip()
		if sc.i >= len(sc.s) {
			return nil, false
		}
		if sc.s[sc.i] == '"' {
			end := sqllex.CloseQuote(sc.s, sc.i, '"', false)
			if end > len(sc.s) || end-sc.i < 3 || sc.s[end-1] != '"' {
				return nil, false
			}
			parts = append(parts, strings.ReplaceAll(sc.s[sc.i+1:end-1], `""`, `"`))
			sc.i = end
		} else {
			j := sc.i
			for j < len(sc.s) && sqllex.IdentByte(sc.s[j]) {
				j++
			}
			if j == sc.i || sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
				return nil, false
			}
			parts = append(parts, strings.ToLower(sc.s[sc.i:j]))
			sc.i = j
		}
		save := sc.i
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == '.' {
			sc.i++
			continue
		}
		sc.i = save
		return parts, true
	}
}

// literal reads a standard 'string' literal, or NULL, which it reports as
// nil.
func (sc *scanner) literal() (*string, bool) {
	if sc.keywords("NULL") {
		return nil, true
	}
	sc.skip()
	if sc.i >= len(sc.s) || sc.s[sc.i] != '\'' {
		return nil, false
	}
	end := sqllex.CloseQuote(sc.s, sc.i, '\'', false)
	if sc.s[end-1] != '\'' || end-sc.i < 2 {
		return nil, false
	}
	v := strings.ReplaceAll(sc.s[sc.i+1:end-1], "''", "'")
	sc.i = end
	return &v, true
}

// rest returns everything not yet read.
func (sc *scanner) rest() string { return sc.s[sc.i:] }

// alterTarget reads the target of ALTER TABLE: the optional IF EXISTS and
// ONLY modifiers, an optional parenthesis around the name (ONLY (t)), the
// qualified name, and an optional trailing '*', as DuckDB and PostgreSQL
// accept them. sc must be positioned right after "ALTER TABLE". It is the one
// place these modifiers are handled, so no check mistakes ONLY or a name in
// parentheses for the table (e.g. ALTER TABLE ONLY t ... would otherwise read
// "only" as the name and skip the table's policies).
func (sc *scanner) alterTarget() ([]string, bool) {
	sc.keywords("IF", "EXISTS")
	sc.keywords("ONLY")
	sc.skip()
	paren := sc.i < len(sc.s) && sc.s[sc.i] == '('
	if paren {
		sc.i++
	}
	name, ok := sc.name()
	if !ok {
		return nil, false
	}
	sc.skip()
	switch {
	case paren && sc.i < len(sc.s) && sc.s[sc.i] == ')':
		sc.i++
	case !paren && sc.i < len(sc.s) && sc.s[sc.i] == '*':
		sc.i++
	}
	return name, true
}

// done reports whether only whitespace, comments and a final semicolon
// remain.
func (sc *scanner) done() bool {
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == ';' {
		sc.i++
		sc.skip()
	}
	return sc.i == len(sc.s)
}

// quoteIdent quotes a name part for SQL.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteName quotes every part of a qualified name.
func quoteName(parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteIdent(p)
	}
	return strings.Join(quoted, ".")
}

// quoteLiteral quotes s as an SQL string literal.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
