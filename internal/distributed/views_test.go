package distributed

import (
	"strings"
	"testing"

	"github.com/akkio-inc/igloo/internal/snowflake"
)

// TestViews checks that a part reading a view reads the view's query in
// its place: views that pass a large table's rows through, filter them,
// join small tables to them, rename their columns or read other views run
// distributed, with what one machine returns.
func TestViews(t *testing.T) {
	h := newHarness(t, 0.01, 2, 3)
	h.addPeople(20000, 6)
	for _, stmt := range snowflake.Macros() {
		h.exec(stmt)
	}
	h.addTable("survey", `(SELECT i::DECIMAL(18,0) AS survey_household_id, 'band_' || (i % 4) AS income
		FROM range(0, 7000, 2) t(i))`, 1)
	// As Snowflake's views mirrored into Igloo read: one table of another
	// schema, whole, and a left join to a small table on a cast key.
	h.addView("v_people_latest", `SELECT * FROM people`)
	h.addView("v_people_attrs", `SELECT s.person_id, s.segments, s.home, sv.income FROM people s
		LEFT JOIN survey sv ON TRY_CAST(s.household_id AS DECIMAL(18,0)) = sv.survey_household_id`)
	h.addView("v_nested", `SELECT person_id, segments FROM v_people_latest WHERE person_id > 100`)
	h.addView("v_survey", `SELECT * FROM survey WHERE income <> 'band_0'`)
	h.exec(`CREATE VIEW main.v_cols(pid, segs) AS SELECT person_id, segments FROM people WHERE person_id % 3 <> 0`)
	h.views["v_cols"] = `SELECT person_id, segments FROM people WHERE person_id % 3 <> 0`

	queries := []string{
		`SELECT count(*), count(DISTINCT person_id) FROM v_people_latest`,
		`SELECT seg, count(DISTINCT person_id) AS n FROM v_people_latest, unnest(segments) AS u(seg) GROUP BY 1 ORDER BY 1`,
		`SELECT income, count(*) AS n FROM v_people_attrs GROUP BY 1 ORDER BY 1 NULLS FIRST`,
		`SELECT v.home.city AS city, count(*) AS n FROM v_people_attrs v WHERE v.income IS NOT NULL GROUP BY 1 ORDER BY 1`,
		`SELECT count(DISTINCT pid), sum(len(segs)) FROM v_cols`,
		`SELECT count(DISTINCT p2) FROM main.v_cols AS x(p2) WHERE x.segs IS NOT NULL`,
		`SELECT seg, count(*) AS n FROM v_nested, unnest(segments) AS u(seg) GROUP BY 1 ORDER BY 1`,
		`SELECT v.person_id, v.income FROM v_people_attrs v ORDER BY v.person_id DESC LIMIT 5`,
		// Views read in full by every worker, in subqueries.
		`SELECT count(*) FROM people p WHERE TRY_CAST(p.household_id AS DECIMAL(18,0)) IN (SELECT survey_household_id FROM v_survey WHERE income = 'band_1')`,
		`SELECT home.city AS city, count(*) AS n FROM v_people_latest p WHERE EXISTS (SELECT 1 FROM v_survey s
			WHERE s.survey_household_id = TRY_CAST(p.household_id AS DECIMAL(18,0))) GROUP BY 1 ORDER BY 1`,
	}
	r, err := snowflake.Translate(`SELECT f.value::string AS seg, COUNT(DISTINCT p.person_id) AS n
		FROM v_people_latest p, LATERAL FLATTEN(input => p.segments) f GROUP BY 1 ORDER BY 1`, snowflake.Options{})
	if err != nil {
		t.Fatal(err)
	}
	queries = append(queries, r.SQL)
	for _, q := range queries {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t

	// A view whose query does not read as the view does, which the
	// planner would read in its place.
	h.exec(`CREATE VIEW main.v_other AS SELECT person_id FROM people`)
	h.views["v_other"] = `SELECT person_id, 1 AS extra FROM people`
	h.addView("v_cte", `WITH x AS (SELECT * FROM people) SELECT * FROM x`)
	h.addView("v_grouped", `SELECT person_id % 10 AS g, count(*) AS n FROM people GROUP BY 1`)
	h.addView("v_top", `SELECT person_id, segments FROM people ORDER BY person_id DESC LIMIT 50`)
	// A view that groups is a relation workers compute.
	h.t = t
	if !h.check(`SELECT sum(n), count(*) FROM v_grouped`) {
		t.Error("a view that groups did not run distributed")
	}
	for _, q := range []string{
		`SELECT count(*) FROM v_other`,
		`SELECT count(*) FROM v_cte`,
		`SELECT count(*) FROM v_top`,
		// The coordinator's part reads a view it cannot read as its query,
		// which could read a table at another snapshot.
		`SELECT x.c, (SELECT count(*) FROM v_cte) AS s FROM (SELECT count(*) AS c FROM v_people_latest) AS x`,
	} {
		t.Run("local "+short(q), func(t *testing.T) {
			_, _, _, _, err := h.run(t.Context(), q, ModeOn)
			if !IsUnsupported(err) {
				t.Errorf("want it to run on one machine, got %v", err)
			}
		})
	}
	// A view in the coordinator's part reads as its query there, its reads
	// pinned to the workers' snapshots: one that groups, and one that a
	// refused part read as its query first.
	for _, q := range []string{
		`SELECT x.c, (SELECT sum(n) FROM v_grouped) AS s FROM (SELECT count(*) AS c FROM v_people_latest) AS x`,
		`SELECT x.c, y.s FROM (SELECT count(*) AS c FROM lineitem) x, (SELECT sum(n) AS s FROM v_grouped) y`,
	} {
		t.Run(short(q), func(t *testing.T) {
			h.t = t
			if !h.check(q) {
				t.Error("did not run distributed")
			}
		})
	}
	h.t = t
	// The table the workers read, read again through a view, is read at
	// their snapshot.
	people := h.tables["people"]
	defer func() { people.Versioned, people.Snapshot = false, 0 }()
	people.Versioned, people.Snapshot = true, 77
	q := `SELECT x.c, (SELECT count(*) FROM v_people_latest) AS n FROM (SELECT count(DISTINCT person_id) AS c FROM people) AS x`
	plan, err := h.co.Plan(t.Context(), q, harnessEnv{h}, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := h.co.Execute(t.Context(), plan, h.settings(t.Context()))
	if err != nil {
		t.Fatal(err)
	}
	ex.Close()
	if !strings.Contains(ex.SQL, "AT (VERSION => 77)") || strings.Contains(ex.SQL, "FROM v_people_latest") {
		t.Errorf("want the view read as its query, at the workers' snapshot:\n%s", ex.SQL)
	}
}
