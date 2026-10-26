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

func TestRespellNames(t *testing.T) {
	catalog := map[string]string{"common.attributes": "ATTRIBUTES", "common.visits": "visits"}
	spell := func(cat, schema, name string) (string, bool) {
		if !strings.EqualFold(cat, "lake") {
			return "", false
		}
		n, ok := catalog[strings.ToLower(schema+"."+name)]
		return n, ok
	}
	for in, want := range map[string]string{
		"SELECT * FROM lake.common.attributes a JOIN lake.common.visits v USING (id)": `SELECT * FROM lake.common."ATTRIBUTES" a JOIN lake.common.visits v USING (id)`,
		`SELECT * FROM "lake"."common"."Attributes"`:                                  `SELECT * FROM "lake"."common"."ATTRIBUTES"`,
		"SELECT lake.common.attributes.age FROM lake.common.ATTRIBUTES":               `SELECT lake.common."ATTRIBUTES".age FROM lake.common.ATTRIBUTES`,
		"SELECT * FROM lake.common.VISITS":                                            `SELECT * FROM lake.common."visits"`,
		"SELECT * FROM lake.common.other, other.common.attributes":                    "SELECT * FROM lake.common.other, other.common.attributes",
		"SELECT 'lake.common.attributes' -- lake.common.attributes":                   "SELECT 'lake.common.attributes' -- lake.common.attributes",
	} {
		if got := respellNames(in, spell); got != want {
			t.Errorf("respellNames(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}

func TestQualifyNames(t *testing.T) {
	tables := map[string]bool{"sales.orders": true, "public.t": true}
	match := func(schema, name string) bool { return tables[schema+"."+name] }
	for in, want := range map[string]string{
		"SELECT * FROM sales.orders":                         `SELECT * FROM "lake".sales.orders`,
		`SELECT o.x FROM sales . orders o JOIN "public"."t"`: `SELECT o.x FROM "lake".sales . orders o JOIN "lake"."public"."t"`,
		"SELECT * FROM lake.sales.orders":                    "SELECT * FROM lake.sales.orders",
		"SELECT sales.orders.x FROM other.sales.orders":      "SELECT sales.orders.x FROM other.sales.orders",
		"SELECT 'sales.orders' -- sales.orders":              "SELECT 'sales.orders' -- sales.orders",
		"SELECT * FROM sales.other, orders":                  "SELECT * FROM sales.other, orders",
	} {
		if got := qualifyNames(in, match, `"lake"`); got != want {
			t.Errorf("qualifyNames(%q)\n got %q\nwant %q", in, got, want)
		}
	}
}
