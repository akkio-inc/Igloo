package ddl

import (
	"strings"
	"testing"
)

func TestRewriteNames(t *testing.T) {
	views := map[string]bool{"public.v": true, "sales.Monthly": true}
	match := func(catalog, schema, name string) bool {
		return strings.EqualFold(catalog, "lake") && views[schema+"."+name]
	}
	for in, want := range map[string]string{
		"SELECT * FROM lake.public.v":                             `SELECT * FROM "memory".public.v`,
		`SELECT * FROM "lake"."public"."v" JOIN LAKE.public.v w`:  `SELECT * FROM "memory"."public"."v" JOIN "memory".public.v w`,
		"SELECT lake.public.v.x FROM lake . public . v":           `SELECT "memory".public.v.x FROM "memory" . public . v`,
		`SELECT * FROM lake.sales."Monthly"`:                      `SELECT * FROM "memory".sales."Monthly"`,
		"SELECT * FROM lake.public.t":                             "SELECT * FROM lake.public.t",
		"SELECT 'lake.public.v' AS s -- lake.public.v":            "SELECT 'lake.public.v' AS s -- lake.public.v",
		"SELECT * FROM other.lake.public.v":                       "SELECT * FROM other.lake.public.v",
		"SELECT * FROM public.v, v":                               "SELECT * FROM public.v, v",
		"SELECT 1.5 FROM lake.public.v WHERE x = lake.public.v.y": `SELECT 1.5 FROM "memory".public.v WHERE x = "memory".public.v.y`,
	} {
		if got := rewriteNames(in, match, `"memory"`); got != want {
			t.Errorf("rewriteNames(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}
