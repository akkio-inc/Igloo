package sqllex

import (
	"reflect"
	"testing"
)

func TestSplit(t *testing.T) {
	cases := []struct {
		script string
		want   []Statement
	}{
		{"SELECT 1", []Statement{{"SELECT 1", 0}}},
		{"  SELECT 1;\n\nSELECT 2;  ", []Statement{{"SELECT 1", 2}, {"SELECT 2", 13}}},
		// Semicolons in literals, identifiers and comments do not split.
		{"SELECT ';', \"a;b\" -- c;d\nFROM t; SELECT $$x;y$$",
			[]Statement{{"SELECT ';', \"a;b\" -- c;d\nFROM t", 0}, {"SELECT $$x;y$$", 33}}},
		{"SELECT /* ; */ 1", []Statement{{"SELECT /* ; */ 1", 0}}},
		// Empty statements and comments alone are dropped.
		{";; -- nothing\n; /* x */", nil},
		{"-- first\nSELECT 1;", []Statement{{"-- first\nSELECT 1", 0}}},
		{"SELECT 'x'", []Statement{{"SELECT 'x'", 0}}},
		{"", nil},
	}
	for _, c := range cases {
		if got := Split(c.script); !reflect.DeepEqual(got, c.want) {
			t.Errorf("Split(%q) = %+v, want %+v", c.script, got, c.want)
		}
	}
}
