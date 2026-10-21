package cleanroom

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestBuiltins(t *testing.T) {
	conn := fixture(t)
	if _, err := conn.ExecContext(context.Background(), `CREATE TABLE marts.sales AS
		SELECT 'e' || i AS email, DATE '2026-10-03' + CAST(i % 3 AS INTEGER) AS sold_on, i % 50 AS revenue
		FROM range(400) r(i) WHERE i % 5 = 0`, nil); err != nil {
		t.Fatal(err)
	}
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	datasets := append(testDatasets(), &Dataset{Name: "sales", Collaborator: "agency", Schema: "marts", Table: "sales",
		Policy: Policy{Join: []string{"email"}, Group: []string{"sold_on"}, Threshold: 10, RunsPerDay: 100}})
	for _, d := range datasets {
		d.Policy.Threshold = 10
	}
	for _, c := range []struct {
		name   string
		args   map[string]string
		values map[string]any
		want   []string
	}{
		{"audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "by": "crm.segment"}, nil,
			[]string{"big|200|200|100", "medium|120|100|83.33"}},
		{"audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "right_by": "views.genre"}, nil,
			[]string{"news|100", "sports|200"}},
		{"audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "approximate": "true"}, nil,
			nil},
		{"reach_frequency", map[string]string{"exposures": "views.device", "campaign": "views.genre", "time": "views.day"},
			map[string]any{"start_date": "2026-10-01", "end_date": "2026-10-08"}, nil},
		{"reach_frequency", map[string]string{"exposures": "views.email", "campaign": "views.genre", "time": "views.day", "audience": "crm.email"},
			map[string]any{"start_date": "2026-10-01", "end_date": "2026-10-08"}, nil},
		{"conversion_lift", map[string]string{"exposures": "views.email", "group": "views.grp", "conversions": "sales.email"}, nil,
			[]string{"225|45|75|15|20|20|0"}},
		// Within three days of exposure, too few converted to show.
		{"conversion_lift", map[string]string{"exposures": "views.email", "group": "views.grp", "conversions": "sales.email",
			"campaign": "views.genre", "exposure_time": "views.day", "conversion_time": "sales.sold_on", "window_days": "3"}, nil,
			[]string{}},
	} {
		sql, params, describe, err := Builtin(c.name, c.args)
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !strings.HasPrefix(describe, c.name+"(") {
			t.Errorf("describe: %s", describe)
		}
		tmpl := Template{SQL: sql, Parameters: params}
		pl, err := p.Plan(ctx, tmpl, datasets, Databases{"memory", "memory"}, c.values)
		if err != nil {
			t.Errorf("%s %v: %v\n%s", c.name, c.args, err, sql)
			continue
		}
		if c.args["approximate"] == "true" {
			// The sketches extension is not in tests; the template checks.
			continue
		}
		rows := mustRun(t, conn, pl)
		if c.want != nil && strings.Join(rows, ",") != strings.Join(c.want, ",") {
			t.Errorf("%s %v: %v, want %v", c.name, c.args, rows, c.want)
		}
		if len(rows) == 0 && c.want == nil {
			t.Errorf("%s %v: no rows", c.name, c.args)
		}
	}
	for _, bad := range []struct {
		name string
		args map[string]string
	}{
		{"nope", nil},
		{"audience_overlap", map[string]string{"left": "crm.email"}},
		{"audience_overlap", map[string]string{"left": "crm", "right": "views.email"}},
		{"audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "extra": "1"}},
		{"audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "approximate": "maybe"}},
		{"conversion_lift", map[string]string{"exposures": "views.email", "group": "views.grp", "conversions": "sales.email", "window_days": "7"}},
		{"conversion_lift", map[string]string{"exposures": "views.email", "group": "views.grp", "conversions": "sales.email",
			"exposure_time": "views.day", "conversion_time": "sales.sold_on", "window_days": "1; DROP TABLE x"}},
	} {
		if _, _, _, err := Builtin(bad.name, bad.args); !IsInvalid(err) {
			t.Errorf("%s %v: want refused, got %v", bad.name, bad.args, err)
		}
	}
	// Built-ins are checked like any template: one that would show an
	// identifier is refused.
	sqlBy, _, _, err := Builtin("audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "by": "crm.email"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Check(ctx, Template{SQL: sqlBy}, datasets); !IsInvalid(err) {
		t.Errorf("an overlap by an identifier: %v", err)
	}
}

// TestApproximateBuiltins runs sketches' estimates, whose functions a run
// qualifies like any other's, with the datasketches extension that
// scripts/test.sh installs into IGLOO_TEST_EXTENSION_DIR.
func TestApproximateBuiltins(t *testing.T) {
	ext := os.Getenv("IGLOO_TEST_EXTENSION_DIR")
	if ext == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set; run igloo install-extensions into it")
	}
	conn := fixture(t)
	for _, q := range []string{"SET extension_directory = '" + ext + "'", "LOAD datasketches"} {
		if _, err := conn.ExecContext(context.Background(), q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	sql, params, _, err := Builtin("audience_overlap", map[string]string{"left": "crm.email", "right": "views.email", "by": "crm.segment", "approximate": "true"})
	if err != nil {
		t.Fatal(err)
	}
	p := NewParser(openDuckDB(t))
	pl, err := p.Plan(context.Background(), Template{SQL: sql, Parameters: params}, testDatasets(), Databases{"memory", "memory"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// HLL counts a few hundred people all but exactly.
	if rows := mustRun(t, conn, pl); len(rows) != 2 || !strings.HasPrefix(rows[0], "big|200|200|") || !strings.HasPrefix(rows[1], "medium|120|100|") {
		t.Errorf("approximate overlap: %v", rows)
	}
}
