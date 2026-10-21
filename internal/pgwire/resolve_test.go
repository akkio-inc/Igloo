package pgwire

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// TestResolveAsDuckDBBinds checks the resolver against DuckDB's binder: for
// each name, at the top level and in views' bodies, under several search
// paths, the table the resolver finds first must be the one DuckDB's plan
// scans, and it must find none where DuckDB finds none. Every database is
// a DuckDB one here, whose scans name their tables in full. lake and sf
// stand for the Iceberg catalog and a Snowflake connection that both have
// a schema media_demo.
func TestResolveAsDuckDBBinds(t *testing.T) {
	ctx := context.Background()
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(q string) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, q := range []string{
		"ATTACH ':memory:' AS lake", "ATTACH ':memory:' AS sf", "ATTACH ':memory:' AS other",
		"CREATE SCHEMA lake.media_demo", "CREATE SCHEMA sf.media_demo", "CREATE SCHEMA other.media_demo",
		"CREATE SCHEMA memory.media_demo", "CREATE SCHEMA memory.public", "CREATE SCHEMA lake.public",
		`CREATE TABLE lake.media_demo."AUDIT_EVENTS" (x INTEGER)`, "CREATE TABLE sf.media_demo.audit_events (x INTEGER)",
		"CREATE TABLE other.media_demo.audit_events (x INTEGER)", "CREATE TABLE sf.media_demo.only_sf (x INTEGER)",
		"CREATE TABLE lake.main.m (x INTEGER)", "CREATE TABLE sf.main.m (x INTEGER)",
		"CREATE TABLE memory.main.t1 (x INTEGER)", "CREATE TABLE lake.media_demo.t1 (x INTEGER)",
		"CREATE TABLE memory.media_demo.t2 (x INTEGER)", "CREATE TABLE lake.media_demo.t2 (x INTEGER)",
		"CREATE TABLE lake.main.t3 (x INTEGER)", "CREATE TABLE other.main.t3 (x INTEGER)",
		"CREATE TABLE lake.media_demo.t4 (x INTEGER)", "CREATE TABLE lake.public.p (x INTEGER)",
		"SET search_path = 'lake.media_demo'",
		// Views' bodies, bound when the views are read, under the path then.
		"CREATE VIEW memory.media_demo.v1 AS SELECT * FROM t1",
		"CREATE VIEW memory.public.v2 AS SELECT * FROM media_demo.t2",
		"CREATE VIEW other.media_demo.v3 AS SELECT * FROM t3",
		"CREATE VIEW memory.public.v4 AS SELECT * FROM media_demo.t4",
		"CREATE VIEW memory.public.v5 AS SELECT * FROM main.t3",
		"CREATE VIEW sf.media_demo.v6 AS SELECT * FROM media_demo.audit_events",
		"CREATE TEMP VIEW v7 AS SELECT * FROM media_demo.audit_events",
	} {
		exec(q)
	}
	views := map[string]object{}
	for _, v := range []object{
		{db: "memory", schema: "media_demo", name: "v1"}, {db: "memory", schema: "public", name: "v2"},
		{db: "other", schema: "media_demo", name: "v3"}, {db: "memory", schema: "public", name: "v4"},
		{db: "memory", schema: "public", name: "v5"}, {db: "sf", schema: "media_demo", name: "v6"},
		{db: "temp", schema: "main", name: "v7"},
	} {
		v.view = true
		views[v.name] = v
	}
	bodies := map[string][]string{
		"v1": {"t1"}, "v2": {"media_demo", "t2"}, "v3": {"t3"}, "v4": {"media_demo", "t4"}, "v5": {"main", "t3"},
		"v6": {"media_demo", "audit_events"}, "v7": {"media_demo", "audit_events"},
	}

	// scanned returns the table DuckDB's plan for query scans, or "" if
	// DuckDB cannot bind it.
	scanned := func(query string) string {
		t.Helper()
		exec("SET explain_output = 'all'")
		var key, value string
		rows, err := conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+query)
		if err != nil {
			return ""
		}
		defer rows.Close() //nolint:errcheck
		var tables []string
		for rows.Next() {
			if err := rows.Scan(&key, &value); err != nil {
				t.Fatal(err)
			}
			if key != "logical_plan" {
				continue
			}
			var plan any
			if err := json.Unmarshal([]byte(value), &plan); err != nil {
				t.Fatal(err)
			}
			var walk func(n any)
			walk = func(n any) {
				switch n := n.(type) {
				case []any:
					for _, c := range n {
						walk(c)
					}
				case map[string]any:
					extra, _ := n["extra_info"].(map[string]any)
					if table, ok := extra["Table"].(string); ok {
						tables = append(tables, strings.ToLower(table))
					}
					walk(n["children"])
				}
			}
			walk(plan)
		}
		if len(tables) != 1 {
			t.Fatalf("%s: scans %q", query, tables)
		}
		return tables[0]
	}
	// resolver is one for the session's path and databases now, which
	// finds objects as load does.
	resolver := func() *resolver {
		t.Helper()
		var setting, current string
		if err := conn.QueryRowContext(ctx, "SELECT current_setting('search_path')::VARCHAR, current_database()").Scan(&setting, &current); err != nil {
			t.Fatal(err)
		}
		dbs := map[string]string{}
		rows, err := conn.QueryContext(ctx, "SELECT lower(database_name), type FROM duckdb_databases()")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				t.Fatal(err)
			}
			dbs[name] = typ
		}
		_ = rows.Close()
		return &resolver{
			ctx: ctx, path: parseSearchPath(setting, current), databases: dbs,
			objects: map[string][]object{}, readsOf: map[string]readSet{},
			lookup: func(name string) []object {
				rows, err := conn.QueryContext(ctx, `SELECT database_name, schema_name, table_name, false FROM duckdb_tables() WHERE lower(table_name) = $1
					UNION ALL SELECT database_name, schema_name, view_name, true FROM duckdb_views() WHERE lower(view_name) = $1`, name)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close() //nolint:errcheck
				var out []object
				for rows.Next() {
					var o object
					if err := rows.Scan(&o.db, &o.schema, &o.name, &o.view); err != nil {
						t.Fatal(err)
					}
					out = append(out, o)
				}
				return out
			},
		}
	}
	key := func(objs []object) []string {
		var out []string
		for _, o := range objs {
			out = append(out, strings.ToLower(o.db+"."+o.schema+"."+o.name))
		}
		return out
	}

	cases := []struct {
		path string
		// name is a table's name, or a view's, whose body's one name is
		// resolved in the view.
		name []string
		// all, when set, is every object the name may mean.
		all []string
	}{
		{"lake.media_demo", []string{"media_demo", "audit_events"}, []string{"lake.media_demo.audit_events"}},
		{"lake.media_demo", []string{"MEDIA_DEMO", "AUDIT_EVENTS"}, []string{"lake.media_demo.audit_events"}},
		{"lake.media_demo", []string{"audit_events"}, []string{"lake.media_demo.audit_events"}},
		{"lake.media_demo", []string{"lake", "media_demo", "audit_events"}, nil},
		{"lake.media_demo", []string{"media_demo", "only_sf"}, []string{}},
		{"lake.media_demo", []string{"lake", "audit_events"}, nil},
		{"lake.media_demo", []string{"sf", "m"}, nil},
		{"lake.media_demo", []string{"sf", "audit_events"}, []string{}},
		{"lake.media_demo", []string{"main", "m"}, nil},
		{"lake.media_demo,memory.media_demo", []string{"media_demo", "t2"}, []string{"lake.media_demo.t2", "memory.media_demo.t2"}},
		{"lake.media_demo,sf.media_demo", []string{"media_demo", "audit_events"},
			[]string{"lake.media_demo.audit_events", "sf.media_demo.audit_events"}},
		{"lake.media_demo,sf.media_demo", []string{"media_demo", "only_sf"}, nil},
		{"lake.media_demo,sf.media_demo", []string{"sf", "audit_events"}, nil},
		{"sf.media_demo,lake.media_demo", []string{"media_demo", "audit_events"}, nil},
		{"sf.media_demo,lake.media_demo", []string{"sf", "m"}, nil},
		{"lake.public", []string{"media_demo", "audit_events"}, []string{"lake.media_demo.audit_events"}},
		{"lake.public", []string{"media_demo", "only_sf"}, []string{}},
		{"other.main", []string{"media_demo", "audit_events"}, []string{"other.media_demo.audit_events"}},
		{"memory.main", []string{"media_demo", "audit_events"}, []string{}},
		{"other.main,lake.media_demo", []string{"media_demo", "t4"}, nil},
		{"other.main", []string{"media_demo", "t4"}, []string{}},
		// Views read with the path lake.media_demo, and with others.
		{"lake.media_demo", []string{"v1"}, nil},
		{"lake.media_demo", []string{"v2"}, nil},
		{"lake.media_demo", []string{"v3"}, nil},
		{"lake.media_demo", []string{"v4"}, nil},
		{"lake.media_demo", []string{"v5"}, nil},
		{"lake.media_demo", []string{"v6"}, nil},
		{"lake.media_demo", []string{"v7"}, nil},
		{"other.main", []string{"v1"}, nil},
		{"other.main", []string{"v3"}, nil},
		{"sf.media_demo,lake.media_demo", []string{"v7"}, nil},
	}
	for _, c := range cases {
		exec("SET search_path = " + quoteLiteral(c.path))
		r := resolver()
		ref, query := c.name, "SELECT * FROM "+quoteName(c.name)
		var view *object
		if v, ok := views[c.name[0]]; ok && len(c.name) == 1 {
			view, ref = &v, bodies[v.name]
			query = "SELECT * FROM " + quoteName([]string{v.db, v.schema, v.name})
		}
		res, err := r.resolve(tableRef{parts: ref}, view)
		if err != nil {
			t.Fatal(err)
		}
		label := c.path + ": " + strings.Join(c.name, ".")
		want := scanned(query)
		switch {
		case want == "" && len(res.first) > 0:
			t.Errorf("%s: DuckDB binds nothing, the resolver %q", label, key(res.first))
		case want != "" && !slices.Equal(key(res.first), []string{want}):
			t.Errorf("%s: DuckDB binds %s, the resolver %q", label, want, key(res.first))
		}
		if c.all != nil && !slices.Equal(key(res.all), c.all) {
			t.Errorf("%s: may mean %q, want %q", label, key(res.all), c.all)
		}
	}
}
