package pgwire

import (
	"slices"
	"strings"
	"testing"
)

func TestScannerName(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
		rest string
	}{
		{"t AS SELECT 1", []string{"t"}, " AS SELECT 1"},
		{"Lake.Public.Events(id INT)", []string{"lake", "public", "events"}, "(id INT)"},
		{`"My Schema" . "Odd""Name" AS x`, []string{"My Schema", `Odd"Name`}, " AS x"},
		{"/* c */ s.t", []string{"s", "t"}, ""},
	} {
		sc := &scanner{s: tc.in}
		got, ok := sc.name()
		if !ok || !slices.Equal(got, tc.want) || sc.rest() != tc.rest {
			t.Errorf("%q: got %q %q %v, want %q %q", tc.in, got, sc.rest(), ok, tc.want, tc.rest)
		}
	}
	for _, bad := range []string{"", "1abc", `"unclosed`, "s."} {
		if got, ok := (&scanner{s: bad}).name(); ok {
			t.Errorf("%q: parsed as %q", bad, got)
		}
	}
}

func TestScannerLiteral(t *testing.T) {
	sc := &scanner{s: " 'it''s' ; "}
	v, ok := sc.literal()
	if !ok || v == nil || *v != "it's" || !sc.done() {
		t.Errorf("got %v %v", v, ok)
	}
	if v, ok := (&scanner{s: "null"}).literal(); !ok || v != nil {
		t.Errorf("NULL: got %v %v", v, ok)
	}
}

func TestParseTableReplace(t *testing.T) {
	r, ok := parseTableReplace("create or replace table lake.public.\"Scores\" AS SELECT 1 AS n")
	if !ok {
		t.Fatal("not recognized")
	}
	create, drop, rename := r.statements()
	if !strings.HasPrefix(create, `CREATE TABLE "lake"."public"."Scores__igloo_new_`) || !strings.HasSuffix(create, `" AS SELECT 1 AS n`) {
		t.Errorf("create: %s", create)
	}
	if drop != `DROP TABLE IF EXISTS "lake"."public"."Scores"` {
		t.Errorf("drop: %s", drop)
	}
	if !strings.HasPrefix(rename, `ALTER TABLE "lake"."public"."Scores__igloo_new_`) || !strings.HasSuffix(rename, `" RENAME TO "Scores"`) {
		t.Errorf("rename: %s", rename)
	}

	for _, q := range []string{
		"CREATE TABLE t AS SELECT 1",
		"CREATE OR REPLACE TEMP TABLE t AS SELECT 1",
		"CREATE OR REPLACE VIEW v AS SELECT 1",
	} {
		if _, ok := parseTableReplace(q); ok {
			t.Errorf("%q recognized", q)
		}
	}
}
