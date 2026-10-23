package pgwire

import (
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/jackc/pgx/v5"

	"github.com/akkio-inc/igloo/internal/catalog"
)

func TestParseSortCommand(t *testing.T) {
	yes, no := true, false
	for _, c := range []struct {
		query string
		want  sortCommand
	}{
		{"ALTER TABLE t SET SORTED BY (a)", sortCommand{table: []string{"t"}, columns: []catalog.SortColumn{{Name: "a"}}}},
		{`ALTER TABLE IF EXISTS s.t SET SORTED BY (a DESC, "B" ASC NULLS LAST, c NULLS FIRST);`, sortCommand{table: []string{"s", "t"}, ifExists: true,
			columns: []catalog.SortColumn{{Name: "a", Descending: true}, {Name: "B", NullsFirst: &no}, {Name: "c", NullsFirst: &yes}}}},
		{"alter table lake.s.t reset sorted by", sortCommand{table: []string{"lake", "s", "t"}}},
	} {
		got, ok, err := parseSortCommand(c.query)
		if !ok || err != nil {
			t.Errorf("%s: %v %v", c.query, ok, err)
			continue
		}
		if strings.Join(got.table, ".") != strings.Join(c.want.table, ".") || got.ifExists != c.want.ifExists || len(got.columns) != len(c.want.columns) {
			t.Errorf("%s: got %+v", c.query, got)
			continue
		}
		for i, col := range got.columns {
			w := c.want.columns[i]
			if col.Name != w.Name || col.Descending != w.Descending || (col.NullsFirst == nil) != (w.NullsFirst == nil) ||
				col.NullsFirst != nil && *col.NullsFirst != *w.NullsFirst {
				t.Errorf("%s: column %d: %+v", c.query, i, col)
			}
		}
	}
	for _, q := range []string{"ALTER TABLE t ADD COLUMN sorted INTEGER", "ALTER TABLE t RENAME TO sorted", "ALTER TABLE t SET (data_retention_days = 1)"} {
		if _, ok, _ := parseSortCommand(q); ok {
			t.Errorf("%s: taken for a sort order statement", q)
		}
	}
	for _, q := range []string{"ALTER TABLE t SET SORTED BY a", "ALTER TABLE t SET SORTED BY ()", "ALTER TABLE t SET SORTED BY (a + 1)",
		"ALTER TABLE t SET SORTED BY (a) WHERE", "ALTER TABLE t RESET SORTED BY (a)", "ALTER TABLE t SET SORTED BY (s.a)"} {
		if _, ok, err := parseSortCommand(q); !ok || err == nil {
			t.Errorf("%s: %v %v, want an error", q, ok, err)
		}
	}
}

// TestSortOrders sets, changes and resets the sort order of a catalog table
// in both dialects, and reads it back from the catalog and the views that
// show it.
func TestSortOrders(t *testing.T) {
	vc := newViewCatalog(t)
	a := vc.replica(t)
	admin := a.admin(t, pgx.QueryExecModeSimpleProtocol)
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS lake.public",
		"CREATE TABLE lake.public.visits AS SELECT i AS id, 'site' || (i % 5) AS site, DATE '2026-01-01' + CAST(i % 9 AS INTEGER) AS day, " +
			"[i] AS tags FROM range(100) r(i)",
		"CREATE VIEW lake.public.recent AS SELECT * FROM lake.public.visits WHERE id > 50",
		"CREATE USER rita WITH PASSWORD 'rita-password' ROLE reader",
	} {
		mustExec(t, admin, q)
	}
	order := func() (out []string) {
		t.Helper()
		meta := vc.fake.Metadata("public", "visits")
		schema := meta.CurrentSchema()
		for _, f := range meta.SortOrder().Fields() {
			field, _ := schema.FindFieldByID(f.SourceID())
			out = append(out, field.Name+" "+string(f.Direction)+" "+string(f.NullOrder))
		}
		return out
	}
	key := func() string {
		t.Helper()
		rows := texts(t, admin, "SELECT coalesce(clustering_key, '') FROM igloo.tables WHERE table_schema = 'public' AND table_name = 'visits'")
		if len(rows) != 1 {
			t.Fatalf("igloo.tables: %q", rows)
		}
		return rows[0]
	}

	mustExec(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (site, day DESC)")
	if got := strings.Join(order(), ", "); got != "site asc nulls-first, day desc nulls-last" {
		t.Errorf("sort order: %s", got)
	}
	if got := key(); got != "LINEAR(site, day DESC)" {
		t.Errorf("clustering key: %s", got)
	}
	if rows := texts(t, admin, "SELECT clustering_key FROM information_schema.tables WHERE table_name = 'visits'"); len(rows) != 1 || rows[0] != "LINEAR(site, day DESC)" {
		t.Errorf("information_schema.tables: %q", rows)
	}
	shown := false
	for _, r := range texts(t, admin, "SHOW OBJECTS IN SCHEMA public") {
		shown = shown || strings.Contains(r, "visits") && strings.HasSuffix(r, "LINEAR(site, day DESC)")
	}
	if !shown {
		t.Errorf("SHOW OBJECTS does not show the clustering key: %q", texts(t, admin, "SHOW OBJECTS IN SCHEMA public"))
	}
	// Setting it again changes nothing.
	commits := vc.fake.Commits()
	mustExec(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (site, day DESC)")
	if vc.fake.Commits() != commits {
		t.Error("setting the same sort order committed")
	}

	// Snowflake's clustering keys.
	mustExec(t, admin, "SET sql_dialect = 'snowflake'")
	mustExec(t, admin, "ALTER TABLE LAKE.PUBLIC.VISITS CLUSTER BY (DAY)")
	mustExec(t, admin, "SET sql_dialect = 'duckdb'")
	if got := key(); got != "LINEAR(day)" {
		t.Errorf("after CLUSTER BY (DAY): %s", got)
	}
	mustExec(t, admin, "SET sql_dialect = 'snowflake'")
	mustExec(t, admin, "ALTER TABLE LAKE.PUBLIC.VISITS DROP CLUSTERING KEY")
	mustExec(t, admin, "CREATE TABLE lake.public.sites CLUSTER BY (site) AS SELECT DISTINCT site, id FROM lake.public.visits")
	mustExec(t, admin, "SET sql_dialect = 'duckdb'")
	if got := key(); got != "" || len(order()) != 0 {
		t.Errorf("after DROP CLUSTERING KEY: %q %q", got, order())
	}
	if meta := vc.fake.Metadata("public", "sites"); catalog.ClusteringKey(meta) != "LINEAR(site)" {
		t.Errorf("CREATE TABLE ... CLUSTER BY: %q", catalog.ClusteringKey(meta))
	}
	// An order the table had before is made the default again.
	mustExec(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (site, day DESC)")
	if n := len(vc.fake.Metadata("public", "visits").SortOrders()); n != 3 {
		t.Errorf("%d sort orders, want the unsorted one and two more", n)
	}
	mustExec(t, admin, "ALTER TABLE lake.public.visits RESET SORTED BY")
	if vc.fake.Metadata("public", "visits").SortOrder().OrderID() != table.UnsortedSortOrderID {
		t.Error("RESET SORTED BY left a sort order")
	}

	// What is refused.
	expectCode(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (nope)", "42703", "does not exist")
	expectCode(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (tags)", "42804", "primitive")
	expectCode(t, admin, "ALTER TABLE lake.public.recent SET SORTED BY (site)", "42809", "view")
	expectCode(t, admin, "ALTER TABLE lake.public.missing SET SORTED BY (site)", "42P01", "missing")
	mustExec(t, admin, "ALTER TABLE IF EXISTS lake.public.missing SET SORTED BY (site)")
	mustExec(t, admin, "BEGIN")
	expectCode(t, admin, "ALTER TABLE lake.public.visits SET SORTED BY (site)", "25001", "transaction")
	mustExec(t, admin, "ROLLBACK")
	expectCode(t, admin, "SELECT 1; ALTER TABLE lake.public.visits SET SORTED BY (site)", "0A000", "on their own")
	rita := connect(t, a.url("rita", "rita-password"), pgx.QueryExecModeSimpleProtocol)
	expectCode(t, rita, "ALTER TABLE lake.public.visits SET SORTED BY (site)", "42501", "")
	if len(order()) != 0 {
		t.Errorf("a refused statement changed the sort order: %q", order())
	}
}
