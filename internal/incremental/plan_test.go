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
		{query: "WITH c AS (SELECT * FROM f) SELECT * FROM c"},
		{query: "WITH c AS (SELECT id, g FROM f) SELECT a.id FROM c AS a JOIN c AS b ON a.g = b.g"},
		{query: "WITH a AS (SELECT * FROM f), b AS (SELECT * FROM a WHERE x > 1) SELECT id FROM b"},
		{query: "WITH f AS (SELECT * FROM dim) SELECT name FROM f"},
		{query: "WITH c AS (SELECT g, sum(x) AS s FROM f GROUP BY g) SELECT g, s * 2 FROM c WHERE s > 1", grouped: true, invertible: true},
		{query: "WITH c AS (SELECT g, min(x) AS m FROM f GROUP BY 1) SELECT * FROM c", grouped: true},
		{query: "WITH c AS (SELECT DISTINCT g, x FROM f) SELECT g FROM c", grouped: true, invertible: true},
		{query: "SELECT s FROM (SELECT sum(x) AS s FROM f) AS t", grouped: true, invertible: true},
		{query: "SELECT a FROM (SELECT b AS a FROM (SELECT g AS b, sum(x) AS c FROM f GROUP BY g) AS i WHERE c > 1) AS o", grouped: true, invertible: true},
		{query: "SELECT z FROM (SELECT id FROM f) AS s(z)"},
		{query: "SELECT id FROM (SELECT id FROM f)"},
		{query: "SELECT g, count(*) FROM (SELECT g, x FROM f UNION ALL SELECT g, 1 FROM dim) AS u GROUP BY g", grouped: true, invertible: true},
		{query: "SELECT u.g, d.name FROM (SELECT g FROM f UNION ALL SELECT g FROM dim) AS u JOIN dim AS d ON u.g = d.g"},
		{query: "SELECT id FROM f UNION ALL SELECT id FROM f"},
		{query: "SELECT id FROM f UNION ALL SELECT 1 ORDER BY 1"},
		{query: "SELECT g FROM f UNION SELECT g FROM dim", grouped: true, invertible: true},
		{query: "SELECT g FROM f UNION (SELECT g FROM dim UNION SELECT s FROM f)", grouped: true, invertible: true},
		{query: "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", reason: "recursive"},
		{query: "SELECT * FROM (SELECT * FROM f LIMIT 3) AS s", reason: "LIMIT"},
		{query: "WITH c AS (SELECT id, row_number() OVER () AS r FROM f) SELECT id FROM c", reason: "window function"},
		{query: "SELECT id FROM (SELECT id, now() AS t FROM f) AS s", reason: "calls now"},
		{query: "SELECT * FROM (SELECT * FROM f WHERE x IN (SELECT 1)) AS s", reason: "subquery"},
		{query: "SELECT id FROM (SELECT DISTINCT id FROM f) AS s JOIN dim ON true", reason: "DISTINCT"},
		{query: "SELECT c.g FROM (SELECT g, sum(x) AS s FROM f GROUP BY g) AS c JOIN dim ON c.g = dim.g", reason: "aggregates"},
		{query: "SELECT sum(s) FROM (SELECT g, sum(x) AS s FROM f GROUP BY g) AS c", reason: "what a subquery aggregated"},
		{query: "SELECT g FROM (SELECT g, sum(x) AS s FROM f GROUP BY g) AS c LIMIT 1", reason: "LIMIT"},
		{query: "SELECT * FROM f EXCEPT SELECT * FROM f", reason: "EXCEPT"},
		{query: "SELECT g FROM f INTERSECT SELECT g FROM dim", reason: "INTERSECT"},
		{query: "SELECT g FROM f UNION ALL BY NAME SELECT g FROM dim", reason: "UNION BY NAME"},
		{query: "SELECT g FROM f UNION ALL SELECT g FROM dim LIMIT 1", reason: "LIMIT"},
		{query: "SELECT * FROM (SELECT g FROM f UNION SELECT g FROM dim) AS u", reason: "UNION rather than UNION ALL"},
		{query: "SELECT g FROM (SELECT g FROM f EXCEPT SELECT g FROM dim) AS u", reason: "EXCEPT"},
		{query: "SELECT f.id, d.name FROM f, LATERAL (SELECT name FROM dim WHERE dim.g = f.g) AS d", reason: "on its own"},
		{query: "SELECT n FROM (SELECT 1 AS n) AS c", reason: "reads no table"},
		{query: "SELECT id, now() FROM f", reason: "calls now"},
		{query: "SELECT g, x, sum(x) FROM f GROUP BY g", reason: "outside an aggregate"},
		{query: "SELECT g, count(*) FROM f GROUP BY ROLLUP (g)", reason: "ROLLUP"},
		{query: "SELECT a.x FROM f AS a FULL JOIN dim AS b ON a.g = b.g"},
		{query: "SELECT 1", reason: "reads no table"},
		{query: "SELECT * FROM f AT (VERSION => 3)", reason: "snapshot"},
		{query: "SELECT main.f.x FROM f", reason: "schema"},
		{query: "SELECT * FROM f UNION ALL SELECT * FROM f"},
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
	// A table under a subquery on the side a join fills with NULLs is filled too.
	p, err := Analyze(ctx, env, "SELECT f.id, u.g FROM f LEFT JOIN (SELECT g FROM dim UNION ALL SELECT g FROM f) AS u ON f.g = u.g")
	if err != nil || p.Reason != "" || len(p.Sources) != 3 || p.Sources[0].Nullable || !p.Sources[1].Nullable || !p.Sources[2].Nullable {
		t.Errorf("under a left join: %+v %v", p, err)
	}
	if p.Sources[0].Outer != "f" || p.Sources[1].Outer != "u" || p.Sources[2].Outer != "u" {
		t.Errorf("what the query calls what reads its sources: %+v", p.Sources)
	}
	// A full outer join's tables are both filled with NULLs.
	p, err = Analyze(ctx, env, "SELECT a.x FROM f AS a FULL JOIN dim AS b ON a.g = b.g")
	if err != nil || !p.Sources[0].Nullable || !p.Sources[1].Nullable {
		t.Errorf("full join: %+v %v", p, err)
	}
	p, err = Analyze(ctx, env, "SELECT a.x FROM f AS a LEFT JOIN dim AS b ON a.g = b.g")
	if err != nil || p.Sources[0].Nullable || !p.Sources[1].Nullable {
		t.Errorf("left join: %+v %v", p, err)
	}
}

// TestPinnedUnion checks what a refresh reads of a UNION ALL: the branch of the
// source that changed, with its sign, and the others not at all; every branch
// when the change is outside it.
func TestPinnedUnion(t *testing.T) {
	env := testEnv(t)
	ctx := context.Background()
	p, err := Analyze(ctx, env, "SELECT g, count(*) AS n FROM (SELECT g FROM f UNION ALL SELECT g FROM dim WHERE name > 'a' UNION ALL SELECT s AS g FROM f) AS u JOIN dim AS d USING (g) GROUP BY g")
	if err != nil || p.Reason != "" {
		t.Fatalf("%+v %v", p, err)
	}
	if len(p.Sources) != 4 {
		t.Fatalf("sources %+v", p.Sources)
	}
	at := func(changed int, signed bool) string {
		t.Helper()
		pins := make([]Pin, len(p.Sources))
		for i := range pins {
			pins[i].Version = -1
		}
		pins[changed] = Pin{View: "changes", Changed: true, Signed: signed}
		sign := ""
		if signed {
			sign = p.Sources[changed].Outer
		}
		q, err := p.Input(ctx, pins, sign)
		if err != nil {
			t.Fatal(err)
		}
		// It reads what it says.
		if _, err := env.Describe(ctx, q); err != nil && !strings.Contains(err.Error(), "changes") {
			t.Errorf("%s: %v", q, err)
		}
		return q
	}
	q := at(1, true)
	if strings.Contains(q, "FROM f") || strings.Contains(q, "UNION") || !strings.Contains(q, "changes") || !strings.Contains(q, "__igloo_sign") {
		t.Errorf("a change of the second branch reads %s", q)
	}
	q = at(2, false)
	if strings.Contains(q, "UNION") || strings.Contains(q, "__igloo_sign") || strings.Contains(q, "name") {
		t.Errorf("a change of the third branch reads %s", q)
	}
	q = at(3, false)
	if strings.Count(q, "UNION ALL") != 2 {
		t.Errorf("a change outside the union reads %s", q)
	}
}
