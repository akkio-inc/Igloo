package sqllex

import (
	"strings"
	"unicode"
)

// Statement is one statement of a script.
type Statement struct {
	// Text is the statement without its terminating semicolon, comments
	// included, trimmed of surrounding white space.
	Text string
	// Offset is the byte offset of Text in the script.
	Offset int
}

// Split cuts a script into statements at the semicolons in its SQL code,
// as psql does before sending them one at a time. Statements that hold only
// comments and white space are dropped.
func Split(script string) []Statement {
	var (
		out     []Statement
		start   int
		hasCode bool
		pos     int
	)
	end := func(at int) {
		if hasCode {
			text := script[start:at]
			trimmed := strings.TrimLeftFunc(text, unicode.IsSpace)
			out = append(out, Statement{
				Text:   strings.TrimRightFunc(trimmed, unicode.IsSpace),
				Offset: start + len(text) - len(trimmed),
			})
		}
		start, hasCode = at+1, false
	}
	for _, s := range Segments(script) {
		if s.Kind != Code {
			// A string or quoted identifier is part of a statement; a
			// comment alone is not.
			hasCode = hasCode || s.Kind != Comment
			pos += len(s.Text)
			continue
		}
		for i := 0; i < len(s.Text); i++ {
			switch c := s.Text[i]; {
			case c == ';':
				end(pos + i)
			case c != ' ' && c != '\t' && c != '\n' && c != '\r':
				hasCode = true
			}
		}
		pos += len(s.Text)
	}
	end(len(script))
	return out
}
