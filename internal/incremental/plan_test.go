package incremental

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// dbParser parses with a DuckDB database of its own.
type dbParser struct{ db *sql.DB }

func (p dbParser) Parse(ctx context.Context, query string) (string, error) {
	var s string
	err := p.db.QueryRowContext(ctx, "SELECT json_serialize_sql($1::VARCHAR)::VARCHAR", query).Scan(&s)
	return s, err
}

func (p dbParser) Print(ctx context.Context, doc string) (string, error) {
	var s string
	err := p.db.QueryRowContext(ctx, "SELECT json_deserialize_sql($1::JSON)", doc).Scan(&s)
	return s, err
}

func testEnv(t *testing.T) Env {
	t.Helper()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	for _, q := range []string{
		"CREATE TABLE f (id INTEGER, g VARCHAR, x INTEGER, d DOUBLE, amount DECIMAL(10,2), u BIGINT, s VARCHAR)",
		"CREATE TABLE dim (g VARCHAR, name VARCHAR)",
		"CREATE VIEW v AS SELECT * FROM f",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	aggregates := map[string]bool{}
	rows, err := db.Query("SELECT DISTINCT lower(function_name) FROM duckdb_functions() WHERE function_type = 'aggregate'")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		aggregates[n] = true
	}
	_ = rows.Close()
	return Env{
		Parser: dbParser{db},
		Table: func(_ context.Context, parts []string) (string, string, bool, error) {
			name := parts[len(parts)-1]
			return "main", name, name == "f" || name == "dim", nil
		},
		Columns: func(ctx context.Context, schema, name string) ([]string, error) {
			rows, err := db.QueryContext(ctx, "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", name)
			if err != nil {
				return nil, err
			}
			defer rows.Close() //nolint:errcheck
			var out []string
			for rows.Next() {
				var c string
				if err := rows.Scan(&c); err != nil {
					return nil, err
				}
				out = append(out, c)
			}
			return out, rows.Err()
		},
		Describe: func(ctx context.Context, query string) ([]Column, error) {
			rows, err := db.QueryContext(ctx, "DESCRIBE "+query)
			if err != nil {
				return nil, err
			}
			defer rows.Close() //nolint:errcheck
			var out []Column
			for rows.Next() {
				var c Column
				var a, b, d, e any
				if err := rows.Scan(&c.Name, &c.Type, &a, &b, &d, &e); err != nil {
					return nil, err
				}
				out = append(out, c)
			}
			return out, rows.Err()
		},
		Aggregate: func(name string) bool { return aggregates[name] },
		Unstable:  func(name string) bool { return name == "now" || name == "random" },
		Bitmaps:   true,
	}
}

// TestAnalyze checks which queries a refresh keeps incrementally, and why
// the others it does not.
func TestAnalyze(t *testing.T) {
	env := testEnv(t)
	ctx := context.Background()
	for _, c := range []struct {
		query string
		// reason is part of the reason, "" for incremental.
		reason              string
		grouped, invertible bool
	}{
		{query: "SELECT id, x + 1 AS y FROM f WHERE g = 'a'"},
		{query: "SELECT f.id, dim.name FROM f JOIN dim ON f.g = dim.g"},
		{query: "SELECT f.id, dim.name FROM f LEFT JOIN dim USING (g)"},
		{query: "SELECT g, count(*), sum(x), avg(x), sum(amount), count(x) FROM f GROUP BY g", grouped: true, invertible: true},
		{query: "SELECT g, min(x), max(s) FROM f GROUP BY 1", grouped: true},
		{query: "SELECT count(DISTINCT u) FROM f", grouped: true},
		{query: "SELECT DISTINCT g, x FROM f", grouped: true, invertible: true},
		{query: "SELECT upper(g) AS k, sum(x) / count(*) FROM f GROUP BY g HAVING sum(x) > 3", grouped: true, invertible: true},
		{query: "SELECT d.name, count(*) FROM f AS a JOIN dim AS d ON a.g = d.g GROUP BY d.name ORDER BY 2", grouped: true, invertible: true},
		{query: "SELECT g, sum(d) FROM f GROUP BY g", reason: "sums values of type DOUBLE"},
		{query: "SELECT g, avg(amount) FROM f GROUP BY g", reason: "averages values of type DECIMAL"},
		{query: "SELECT g, count(DISTINCT s) FROM f GROUP BY g", reason: "counts distinct values of type VARCHAR"},
		{query: "SELECT g, median(x) FROM f GROUP BY g", reason: "the aggregate median"},
		{query: "SELECT g, sum(x) FILTER (WHERE x > 1) FROM f GROUP BY g", reason: "FILTER"},
		{query: "SELECT id, row_number() OVER () FROM f", reason: "window function"},
		{query: "SELECT * FROM f WHERE x IN (SELECT x FROM dim)", reason: "subquery"},
		{query: "SELECT * FROM v", reason: "not a table of the catalog"},
		{query: "SELECT * FROM f LIMIT 3", reason: "LIMIT"},
		{query: "WITH c AS (SELECT * FROM f) SELECT * FROM c", reason: "WITH"},
		{query: "SELECT id, now() FROM f", reason: "calls now"},
		{query: "SELECT g, x, sum(x) FROM f GROUP BY g", reason: "outside an aggregate"},
		{query: "SELECT g, count(*) FROM f GROUP BY ROLLUP (g)", reason: "ROLLUP"},
		{query: "SELECT a.x FROM f AS a FULL JOIN dim AS b ON a.g = b.g"},
		{query: "SELECT 1", reason: "reads no table"},
		{query: "SELECT * FROM f AT (VERSION => 3)", reason: "snapshot"},
		{query: "SELECT main.f.x FROM f", reason: "schema"},
		{query: "SELECT * FROM f UNION ALL SELECT * FROM f", reason: "not a plain SELECT"},
	} {
		p, err := Analyze(ctx, env, c.query)
		if err != nil {
			t.Errorf("%s: %v", c.query, err)
			continue
		}
		if p.Reason == "" {
			if err := p.Types(ctx); err != nil {
				t.Errorf("%s: types: %v", c.query, err)
				continue
			}
		}
		switch {
		case c.reason == "" && p.Reason != "":
			t.Errorf("%s: not incremental: %s", c.query, p.Reason)
		case c.reason != "" && !strings.Contains(p.Reason, c.reason):
			t.Errorf("%s: reason %q, want %q", c.query, p.Reason, c.reason)
		case c.reason == "" && (p.Grouped != c.grouped || p.Invertible() != c.invertible):
			t.Errorf("%s: grouped %v, invertible %v", c.query, p.Grouped, p.Invertible())
		}
	}
	// A full outer join's tables are both filled with NULLs.
	p, err := Analyze(ctx, env, "SELECT a.x FROM f AS a FULL JOIN dim AS b ON a.g = b.g")
	if err != nil || !p.Sources[0].Nullable || !p.Sources[1].Nullable {
		t.Errorf("full join: %+v %v", p, err)
	}
	p, err = Analyze(ctx, env, "SELECT a.x FROM f AS a LEFT JOIN dim AS b ON a.g = b.g")
	if err != nil || p.Sources[0].Nullable || !p.Sources[1].Nullable {
		t.Errorf("left join: %+v %v", p, err)
	}
}
