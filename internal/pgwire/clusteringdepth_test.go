package pgwire

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestParseDepthCall(t *testing.T) {
	for _, c := range []struct {
		query, table, column, alias string
	}{
		{"SELECT SYSTEM$CLUSTERING_DEPTH('t')", "t", "", depthName},
		{"select system$clustering_depth('LAKE.\"My S\".T', '(b, a)') AS d;", "lake.My S.t", "b", "d"},
		{`SELECT SYSTEM$CLUSTERING_DEPTH('s.t', 'c') AS "Depth"`, "s.t", "c", "Depth"},
	} {
		got, ok, err := parseDepthCall(c.query)
		if !ok || err != nil {
			t.Errorf("%s: %v %v", c.query, ok, err)
			continue
		}
		if name := joinName(got.table); name != c.table || got.column != c.column || got.alias != c.alias {
			t.Errorf("%s: got %q %q %q", c.query, name, got.column, got.alias)
		}
	}
	for _, q := range []string{"SELECT 1", "SELECT SYSTEM$CLUSTERING_INFORMATION('t')", "SELECT SYSTEM$CLUSTERING_DEPTHS('t')"} {
		if _, ok, _ := parseDepthCall(q); ok {
			t.Errorf("%s: taken for a call of SYSTEM$CLUSTERING_DEPTH", q)
		}
	}
	for _, q := range []string{"SELECT SYSTEM$CLUSTERING_DEPTH(t)", "SELECT SYSTEM$CLUSTERING_DEPTH('t', '(a + 1)')",
		"SELECT SYSTEM$CLUSTERING_DEPTH('t', '(a)', 'a > 1')", "SELECT SYSTEM$CLUSTERING_DEPTH('t') FROM x", "SELECT SYSTEM$CLUSTERING_DEPTH('a.b.c.d')"} {
		if _, ok, err := parseDepthCall(q); !ok || err == nil {
			t.Errorf("%s: %v %v, want an error", q, ok, err)
		}
	}
}

func joinName(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "."
		}
		out += p
	}
	return out
}

// TestClusteringDepth measures a catalog table's clustering depth by its
// sort order and by a column, in both dialects, as its files stack.
func TestClusteringDepth(t *testing.T) {
	vc := newViewCatalog(t)
	a := vc.replica(t)
	admin := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.visits AS SELECT i AS id, 'site' || (i % 5) AS site FROM range(100) r(i)",
		"CREATE VIEW lake.public.recent AS SELECT * FROM lake.public.visits WHERE id > 50",
	} {
		mustExec(t, admin, q)
	}
	depth := func(q string) string {
		t.Helper()
		rows := texts(t, admin, q)
		if len(rows) != 1 {
			t.Fatalf("%s: %q", q, rows)
		}
		return rows[0]
	}
	expectCode(t, admin, "SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits')", "22023", "no clustering key")
	if got := depth("SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits', '(site)')"); got != "1" {
		t.Errorf("one file: depth %s", got)
	}
	mustExec(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (site)")
	// A second file of the same sites, and of other ids.
	mustExec(t, admin, "INSERT INTO lake.public.visits SELECT i AS id, 'site' || (i % 5) AS site FROM range(100, 200) r(i)")
	if got := depth("SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits') AS d"); got != "2" {
		t.Errorf("two files of the same sites: depth %s", got)
	}
	if got := depth("SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits', '(id, site)')"); got != "1" {
		t.Errorf("by id: depth %s", got)
	}
	mustExec(t, admin, "SET sql_dialect = 'snowflake'")
	if got := depth("SELECT SYSTEM$CLUSTERING_DEPTH('LAKE.PUBLIC.VISITS')"); got != "2" {
		t.Errorf("in Snowflake's dialect: depth %s", got)
	}
	mustExec(t, admin, "SET sql_dialect = 'duckdb'")

	expectCode(t, admin, "SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits', '(nope)')", "42703", "nope")
	expectCode(t, admin, "SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.recent')", "42809", "view")
	expectCode(t, admin, "SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.missing')", "42P01", "missing")
	expectCode(t, admin, "SELECT 1; SELECT SYSTEM$CLUSTERING_DEPTH('lake.public.visits')", "0A000", "on their own")
}
