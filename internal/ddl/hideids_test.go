package ddl

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

func openDuckDB(t *testing.T) *duckdb.Conn {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		_ = connector.Close()
	})
	return c.(*duckdb.Conn)
}

func queryOne(t *testing.T, conn *duckdb.Conn, q string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), q, nil)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close() //nolint:errcheck
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	s, _ := dest[0].(string)
	return s
}

// TestHideIDs checks that a view's local copy reads its tables without
// their columns of ids, so that a star and a column list mean what they
// mean in statements; that a version, an alias, a column list on a table
// and CTEs survive; and that UnhideIDs gives back the view's own SQL from
// the definition DuckDB prints.
func TestHideIDs(t *testing.T) {
	ctx := context.Background()
	conn := openDuckDB(t)
	for _, q := range []string{
		"CREATE TABLE people (person_id VARCHAR, n INTEGER, _igloo_id_person_id BIGINT)",
		"INSERT INTO people VALUES ('p1', 1, 0), ('p2', 2, 1)",
		"CREATE TABLE survey (resp_id INTEGER, answer_text VARCHAR)",
		"INSERT INTO survey VALUES (1, 'yes')",
	} {
		if _, err := conn.ExecContext(ctx, q, nil); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name, query, columns string
	}{
		{"star and a column list", "SELECT s.*, sv.resp_id, sv.answer_text FROM memory.main.people AS s LEFT JOIN survey sv ON s.n = sv.resp_id",
			"who,num,resp_id,answer"},
		{"bare star", "SELECT * FROM people", "person_id,n"},
		{"the table's name as qualifier", "SELECT people.* FROM main.people", "person_id,n"},
		{"a CTE named like a table", "WITH people AS (SELECT 1 AS x) SELECT * FROM people", "x"},
		{"a CTE over the table", "WITH p AS (SELECT * FROM people) SELECT * FROM p", "person_id,n"},
		{"columns named on the table", "SELECT * FROM people AS q(a, b)", "a,b"},
		{"a subquery", "SELECT * FROM (SELECT * FROM people WHERE n > 0) AS x", "person_id,n"},
		{"in a predicate", "SELECT resp_id FROM survey WHERE resp_id IN (SELECT n FROM people)", "resp_id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hidden, err := hideIDs(ctx, conn, c.query)
			if err != nil {
				t.Fatal(err)
			}
			list := ""
			if c.name == "star and a column list" {
				list = " (who, num, resp_id, answer)"
			}
			if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE VIEW v"+list+" AS "+hidden, nil); err != nil {
				t.Fatalf("%s: %v", hidden, err)
			}
			got := queryOne(t, conn, "SELECT string_agg(column_name, ',' ORDER BY rowid) FROM (SELECT column_name, row_number() OVER () AS rowid FROM (DESCRIBE v))")
			if got != c.columns {
				t.Errorf("columns %q, want %q (%s)", got, c.columns, hidden)
			}
			// What DuckDB prints, unwrapped, is the view's SQL as DuckDB
			// prints it.
			printed := queryOne(t, conn, "SELECT sql FROM duckdb_views() WHERE view_name = 'v'")
			if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE VIEW plain"+list+" AS "+c.query, nil); err != nil {
				t.Fatal(err)
			}
			want := queryOne(t, conn, "SELECT sql FROM duckdb_views() WHERE view_name = 'plain'")
			want = strings.Replace(want, "CREATE VIEW plain", "CREATE VIEW v", 1)
			if got := UnhideIDs(printed); got != want {
				t.Errorf("unwrapped\n%s\nwant\n%s", got, want)
			}
			if strings.Contains(UnhideIDs(printed), "starts_with") {
				t.Errorf("left wrapped: %s", printed)
			}
		})
	}

	// A version moves into the subquery with its table.
	got, err := hideIDs(ctx, conn, "SELECT * FROM lake.s.t AS x AT (VERSION => 3) JOIN lake.s.u AT (TIMESTAMP => now()) ON true")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT * FROM (SELECT " + visibleStar + " FROM lake.s.t AT (VERSION => 3)) AS x JOIN (SELECT " + visibleStar +
		" FROM lake.s.u AT (TIMESTAMP => now())) AS \"u\" ON true"
	if got != want {
		t.Errorf("versions:\n%s\nwant\n%s", got, want)
	}
	// What DuckDB cannot parse stays as written.
	if got, err := hideIDs(ctx, conn, "SELEC nonsense"); err != nil || got != "SELEC nonsense" {
		t.Errorf("unparsable: %q, %v", got, err)
	}
}
