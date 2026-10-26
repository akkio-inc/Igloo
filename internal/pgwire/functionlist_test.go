package pgwire

import (
	"database/sql"
	"maps"
	"testing"
)

// TestFunctionSets checks that the one listing of DuckDB's functions gives
// the volatile functions shareSubqueries and factorUnions read as their own
// listing did, and the unstable ones the result cache reads.
func TestFunctionSets(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	for _, s := range []string{
		"CREATE MACRO draw(n) AS random() * n",
		"CREATE MACRO drawn(n) AS draw(n) + 1",
		"CREATE MACRO stamped(x) AS (x, now())",
		"CREATE MACRO setting(name) AS current_setting(name)",
		"CREATE MACRO plain(x) AS upper(x)",
		"CREATE MACRO guarded(x) AS CASE WHEN x < 0 THEN error('negative') ELSE x END",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	query := func(q string) []string {
		rows, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		return out
	}
	sets := functionSetsOf(query(functionListQuery))
	volatile, unstable := sets.volatile, sets.unstable
	for _, name := range []string{"sum", "list", "string_agg", "count"} {
		if !sets.aggregates[name] {
			t.Errorf("%s is not among the aggregates", name)
		}
	}
	if sets.aggregates["upper"] {
		t.Error("upper is among the aggregates")
	}
	if want := volatileSet(query(volatileFunctionsQuery)); !maps.Equal(volatile, want) {
		t.Errorf("volatile functions differ from volatileFunctionsQuery's")
	}
	for name, want := range map[string]bool{
		"random": true, "uuid": true, "now": true, "current_date": true, "current_setting": true, "getvariable": true,
		"current_user": true, "draw": true, "drawn": true, "stamped": true, "setting": true,
		"upper": false, "plain": false, "error": false, "guarded": false, "sum": false,
	} {
		if unstable[name] != want {
			t.Errorf("%s unstable: %v, want %v", name, unstable[name], want)
		}
	}
}
