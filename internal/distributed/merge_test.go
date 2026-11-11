package distributed

import (
	"context"
	"strings"
	"testing"
)

// TestMergeOrdered checks results ordered by ORDER BY without LIMIT: each
// task of the stage whose rows are the result sorts them, and the
// coordinator merges their files rather than sorting every row, in the
// order DuckDB gives: descending and ascending, NULLs first and last, by
// the session's defaults, over text, integers, dates, doubles and
// decimals, for whole groups and for rows a window keeps. An order the
// coordinator cannot merge by, an expression, or a session that does not
// keep the order rows are read in, it sorts as before.
func TestMergeOrdered(t *testing.T) {
	h := behaviorsHarness(t)
	// Cores enough to merge on (see mergesKeepUp); the last case has fewer.
	h.co.opts.Cores = MergeCores
	for _, tc := range []struct {
		query string
		// settings are SET before it runs; merged says the coordinator
		// merges rather than sorts.
		settings []string
		merged   bool
	}{
		{`SELECT bvalue, count(*) AS n FROM behaviors4 GROUP BY 1 ORDER BY n DESC, bvalue`, nil, true},
		{`SELECT bvalue, count(DISTINCT person) AS people, max(amount) AS most FROM behaviors GROUP BY 1 ORDER BY 2, 3 DESC, 1`, nil, true},
		{`SELECT CASE WHEN length(bvalue) > 10 THEN NULL ELSE bvalue END AS v, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY v NULLS FIRST`, nil, true},
		{`SELECT CASE WHEN length(bvalue) > 10 THEN NULL ELSE bvalue END AS v, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY v DESC`,
			[]string{"SET default_null_order = 'nulls_first'"}, true},
		{`SELECT CASE WHEN length(bvalue) > 10 THEN NULL ELSE bvalue END AS v, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY v`,
			[]string{"SET default_order = 'desc'", "SET default_null_order = 'nulls_first_on_asc_last_on_desc'"}, true},
		{`SELECT day, bvalue, sum(CAST(amount AS DECIMAL(18, 2))) AS d, min(amount) AS least FROM behaviors GROUP BY 1, 2 ORDER BY d DESC, least, day, bvalue`, nil, true},
		{`SELECT person, day, bvalue FROM behaviors4 QUALIFY row_number() OVER (PARTITION BY person ORDER BY day DESC, bvalue) = 1 ORDER BY person DESC`, nil, true},
		// The coordinator sorts.
		{`SELECT bvalue, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY n * 2 DESC, bvalue`, nil, false},
		{`SELECT bvalue, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY n DESC, bvalue`, []string{"SET preserve_insertion_order = false"}, false},
		{`SELECT bvalue, count(*) AS n FROM behaviors GROUP BY 1 ORDER BY n DESC, bvalue`, []string{"SET default_collation = 'nocase'"}, false},
	} {
		t.Run(short(strings.Join(tc.settings, "; ")+" "+tc.query), func(t *testing.T) {
			h.t = t
			for _, s := range tc.settings {
				h.exec(s)
			}
			defer func() {
				for _, s := range []string{"default_order", "default_null_order", "preserve_insertion_order", "default_collation"} {
					h.exec("RESET " + s)
				}
			}()
			ctx := context.Background()
			want, _, err := h.query(ctx, tc.query)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := h.co.Plan(ctx, tc.query, harnessEnv{h}, ModeOn)
			if err != nil {
				t.Fatal(err)
			}
			ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
			if err != nil {
				t.Fatal(err)
			}
			defer ex.Close()
			got, _, err := h.query(ctx, ex.SQL)
			if err != nil {
				t.Fatalf("%v\n%s", err, ex.SQL)
			}
			if msg := compareRows(want, got, true); msg != "" {
				t.Errorf("%d rows: %s\n%s\n%s", len(want), msg, plan.describe(), ex.SQL)
			}
			if merged := !strings.Contains(ex.SQL, "ORDER BY"); merged != tc.merged {
				t.Errorf("merged %v, want %v: %s\n%s", merged, tc.merged, ex.SQL, plan.describe())
			}
		})
	}
	// On fewer cores than a merge keeps up on, the coordinator sorts.
	t.Run("few cores", func(t *testing.T) {
		h.t = t
		h.co.opts.Cores = MergeCores - 2
		defer func() { h.co.opts.Cores = MergeCores }()
		q := `SELECT bvalue, count(*) AS n FROM behaviors4 GROUP BY 1 ORDER BY n DESC, bvalue`
		ctx := context.Background()
		want, _, err := h.query(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn)
		if err != nil {
			t.Fatal(err)
		}
		ex, err := h.co.Execute(ctx, plan, h.settings(ctx))
		if err != nil {
			t.Fatal(err)
		}
		defer ex.Close()
		got, _, err := h.query(ctx, ex.SQL)
		if err != nil {
			t.Fatalf("%v\n%s", err, ex.SQL)
		}
		if msg := compareRows(want, got, true); msg != "" || !strings.Contains(ex.SQL, "ORDER BY") {
			t.Errorf("on %d cores: %s, the statement %s", h.co.opts.Cores, msg, ex.SQL)
		}
	})
}

// TestResolveMerge checks the order keys take from the session's
// defaults.
func TestResolveMerge(t *testing.T) {
	cols := []Column{{Name: "a", Type: "VARCHAR"}, {Name: "b", Type: "DOUBLE"}, {Name: "c", Type: "INTEGER[]"}, {Name: "d", Type: "UUID"}}
	for _, tc := range []struct {
		order, nulls string
		settings     []setting
		desc, first  bool
		ok           bool
	}{
		{"ORDER_DEFAULT", "ORDER_DEFAULT", nil, false, false, true},
		{"DESCENDING", "ORDER_DEFAULT", nil, true, false, true},
		{"ORDER_DEFAULT", "ORDER_DEFAULT", []setting{{Name: "default_order", Value: "DESC"}}, true, false, true},
		{"ASCENDING", "ORDER_DEFAULT", []setting{{Name: "default_null_order", Value: "nulls_first"}}, false, true, true},
		{"ASCENDING", "ORDER_DEFAULT", []setting{{Name: "default_null_order", Value: "nulls_first_on_asc_last_on_desc"}}, false, true, true},
		{"DESCENDING", "ORDER_DEFAULT", []setting{{Name: "default_null_order", Value: "nulls_first_on_asc_last_on_desc"}}, true, false, true},
		{"DESCENDING", "ORDER_DEFAULT", []setting{{Name: "default_null_order", Value: "nulls_last_on_asc_first_on_desc"}}, true, true, true},
		{"ASCENDING", "NULLS FIRST", nil, false, true, true},
		{"ASCENDING", "NULLS LAST", []setting{{Name: "default_null_order", Value: "nulls_first"}}, false, false, true},
		{"ASCENDING", "ORDER_DEFAULT", []setting{{Name: "preserve_insertion_order", Value: "false"}}, false, false, false},
		{"ASCENDING", "ORDER_DEFAULT", []setting{{Name: "default_collation", Value: "nocase"}}, false, false, false},
	} {
		for _, col := range []string{"a", "b"} {
			keys, ok := resolveMerge([]mergeKey{{column: col, order: tc.order, nulls: tc.nulls}}, cols, tc.settings)
			if ok != tc.ok || ok && (keys[0].desc != tc.desc || keys[0].nullsFirst != tc.first) {
				t.Errorf("%s %s %v: %v %v, want desc %v nulls first %v ok %v", tc.order, tc.nulls, tc.settings, keys, ok, tc.desc, tc.first, tc.ok)
			}
		}
	}
	for _, col := range []string{"c", "d", "e"} {
		if _, ok := resolveMerge([]mergeKey{{column: col}}, cols, nil); ok {
			t.Errorf("merged by %s", col)
		}
	}
}
