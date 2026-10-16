package sqllex

import (
	"reflect"
	"testing"
)

func TestCodeWords(t *testing.T) {
	cases := map[string][]string{
		"EXPLAIN /*x*/ ANALYZE INSERT":         {"EXPLAIN", "ANALYZE", "INSERT"},
		"SELECT 'a b' AS \"c d\" -- e\nFROM t": {"SELECT", "AS", "FROM", "T"},
		// Nested comments end where PostgreSQL's lexer ends them.
		"SELECT /* a /* b */ c */ 1": {"SELECT", "1"},
		// name'..' is not an escape string, so the backslash does not
		// swallow the closing quote.
		"SELECT x LIKE'a\\' FROM t": {"SELECT", "X", "LIKE", "FROM", "T"},
		"SELECT E'it\\'s' FROM t":   {"SELECT", "E", "FROM", "T"},
		// $1 is a parameter, not a dollar quote.
		"SELECT $1 FROM t WHERE x = $1":              {"SELECT", "1", "FROM", "T", "WHERE", "X", "1"},
		"SELECT $tag$ DROP $tag$, $$ DROP $$ FROM t": {"SELECT", "FROM", "T"},
	}
	for in, want := range cases {
		if got := CodeWords(in); !reflect.DeepEqual(got, want) {
			t.Errorf("CodeWords(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuotedIdentifiers(t *testing.T) {
	got := QuotedIdentifiers(`SELECT "Duckdb_Secrets", 'x', "a""b" FROM t`)
	want := []string{"duckdb_secrets", `a"b`}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
