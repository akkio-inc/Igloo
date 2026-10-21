package cleanroom

// Attacks from a review of clean rooms: an agency (crm) after one person's
// facts in a TV maker's data (views), through templates it writes and
// data of its own it changes after the templates are approved.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
)

// probe puts every CRM person in segment 'big' but target, who is alone in
// segment.
func probe(t *testing.T, conn *duckdb.Conn, target, segment string) {
	t.Helper()
	for _, q := range []string{
		"UPDATE marts.crm_audience SET segment = 'big'",
		"UPDATE marts.crm_audience SET segment = '" + segment + "' WHERE email = '" + target + "'",
	} {
		if _, err := conn.ExecContext(context.Background(), q, nil); err != nil {
			t.Fatal(err)
		}
	}
}

func planOf(t *testing.T, p *Parser, sql string, params []Parameter, datasets []*Dataset, values map[string]any) *Plan {
	t.Helper()
	pl, err := p.Plan(context.Background(), Template{SQL: sql, Parameters: params}, datasets, Databases{"memory", "memory"}, values)
	if err != nil {
		t.Fatalf("refused:\n%s\n%v", sql, err)
	}
	return pl
}

// e7 is in the TV maker's log (sports, 2026-10-08, exposed); e350 is not.
const seen, unseen = "e7", "e350"

// A CASE, or any expression, inside an aggregate picked out one person's
// rows, which no threshold counted; and the max of dimensions read one
// person's values. Aggregates take bare columns now, and FILTER, which
// thresholds count, picks out parts of groups.
func TestOnePersonsValuesStayOut(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	datasets := testDatasets()
	datasets[1].Policy.Aggregates = append(datasets[1].Policy.Aggregates, Aggregate{"max", "minutes"})
	datasets[1].Policy.Bounds = map[string]Bound{"minutes": {0, 6}}
	datasets[0].Policy.Noise, datasets[1].Policy.Noise = 1, 1
	for _, sql := range []string{
		`SELECT count(DISTINCT v.email) AS reach, max(v.minutes) FILTER (WHERE c.segment = 'vip') AS vip
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT count(DISTINCT v.email) AS reach, count(CASE WHEN c.segment = 'vip' THEN 1 END) AS vip_rows,
			max(CASE WHEN c.segment = 'vip' THEN v.genre END) AS vip_genre, max(CASE WHEN c.segment = 'vip' THEN v.day END) AS vip_day
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT count(DISTINCT v.email) AS reach, max(c.segment || ':' || v.genre || ':' || CAST(v.day AS VARCHAR)) AS m
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT count(DISTINCT v.email) AS reach, max(CASE WHEN c.segment = 'vip' THEN v.minutes END) AS m
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT count(DISTINCT v.email) AS reach, sum(1) AS rows FROM crm c JOIN views v ON c.email = v.email`,
	} {
		if _, err := p.Check(context.Background(), Template{SQL: sql}, datasets); !IsInvalid(err) {
			t.Errorf("want refused: %v\n%s", err, sql)
		}
	}

	// FILTER picks out a part that must reach the thresholds, as must
	// every combination of the parts and their opposites, so neither the
	// part nor the rest less it is one person.
	for _, sql := range []string{
		`SELECT count(DISTINCT v.email) AS reach, count(*) FILTER (WHERE c.segment = 'vip') AS vip
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT sum(v.minutes) AS everyone, sum(v.minutes) FILTER (WHERE c.segment <> 'vip') AS others
			FROM crm c JOIN views v ON c.email = v.email`,
		`SELECT sum(v.minutes) FILTER (WHERE c.segment IN ('big', 'vip')) AS a, sum(v.minutes) FILTER (WHERE c.segment = 'big') AS b
			FROM crm c JOIN views v ON c.email = v.email`,
	} {
		pl := planOf(t, p, sql, nil, datasets, nil)
		probe(t, conn, seen, "vip")
		if rows := mustRun(t, conn, pl); len(rows) != 0 {
			t.Errorf("a result that tells one person's minutes: %v\n%s", rows, sql)
		}
	}
}

// The population check counted the joined datasets, so with an inner join
// its refusal told whether the one person a requester's own values picked
// out was in the other party's data. It counts each dataset alone now.
func TestPopulationCheckTellsNothingOfTheOtherParty(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	params := []Parameter{{Name: "segment", Type: "VARCHAR"}}
	pl := planOf(t, p, `SELECT v.genre, count(DISTINCT v.email) AS reach
		FROM crm c JOIN views v ON c.email = v.email WHERE c.segment = $segment GROUP BY v.genre`,
		params, testDatasets(), map[string]any{"segment": "probe"})
	for _, target := range []string{seen, unseen} {
		probe(t, conn, target, "probe")
		_, err := runPlan(t, conn, pl)
		var refused *RefusedError
		if !errors.As(err, &refused) {
			t.Errorf("%s alone in a segment: %v, want refused whether or not the TV maker saw them", target, err)
		}
	}
	// A filter of the TV maker's own columns counts its log alone.
	pl = planOf(t, p, `SELECT c.segment, count(DISTINCT c.email) AS n FROM crm c JOIN views v ON c.email = v.email
		WHERE v.day >= $first GROUP BY 1`, []Parameter{{Name: "first", Type: "DATE"}}, testDatasets(), map[string]any{"first": "2026-10-02"})
	args := []driver.NamedValue{{Ordinal: 1, Value: pl.Args[0]}}
	if got := fmt.Sprint(checkCounts(t, conn, pl, args)); got != "[300 270 300 270]" {
		t.Errorf("the log's own counts, email and device: %s", got)
	}
}

// Each copy of a count had its own noise, so the mean of a hundred copies
// was the exact count, and sum(1) was a count with none. A count has one
// noise in a run now, however often it is written, and each different
// count costs the budget a run.
func TestNoiseCannotBeAveragedAway(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	datasets := testDatasets()
	datasets[1].Policy.Noise = 1
	var items []string
	for i := range 100 {
		items = append(items, fmt.Sprintf("count(DISTINCT v.email) AS n%d", i))
	}
	template := "SELECT c.region, COUNT(DISTINCT V.EMAIL) AS upper_case, " + strings.Join(items, ", ") +
		" FROM crm c JOIN views v ON c.email = v.email GROUP BY 1 ORDER BY 1"
	exact := map[string]string{"east": "150", "west": "150"}
	differs := false
	// Laplace noise of scale 1 rounds to 0 four times in ten, so twenty
	// draws are all exact but for one time in 126 million. Each plan has
	// a seed of its own, as each run does.
	for range 10 {
		pl := planOf(t, p, template, nil, datasets, nil)
		if pl.Cost != 1 {
			t.Fatalf("a hundred copies of one count cost %d runs", pl.Cost)
		}
		for _, r := range mustRun(t, conn, pl) {
			parts := strings.Split(r, "|")
			for _, v := range parts[2:] {
				if v != parts[1] {
					t.Fatalf("copies of one count differ: %s", r)
				}
			}
			differs = differs || parts[1] != exact[parts[0]]
		}
	}
	if !differs {
		t.Error("noisy counts are exact")
	}
	// Different counts each cost the budget.
	pl := planOf(t, p, `SELECT count(*) AS rows, count(DISTINCT v.email) AS people, count(DISTINCT v.email) FILTER (WHERE true) AS again
		FROM views v`, nil, datasets, nil)
	if pl.Cost != 3 {
		t.Errorf("three different counts cost %d runs", pl.Cost)
	}
}

// measuresAcrossParties are testDatasets with noise on both, the TV
// maker's minutes bounded to 0 to 6 a person, and a measure of the
// agency's own, flag, which it sets as it likes, bounded to 0 to 1.
func measuresAcrossParties(t *testing.T, conn *duckdb.Conn) []*Dataset {
	t.Helper()
	mustExec(t, conn, "ALTER TABLE marts.crm_audience ADD COLUMN flag INTEGER DEFAULT 0")
	datasets := testDatasets()
	for _, fn := range []string{"sum", "avg", "min", "max", "median", "stddev"} {
		datasets[0].Policy.Aggregates = append(datasets[0].Policy.Aggregates, Aggregate{fn, "flag"})
	}
	datasets[0].Policy.Bounds = map[string]Bound{"flag": {0, 1}}
	datasets[1].Policy.Bounds = map[string]Bound{"minutes": {0, 6}}
	datasets[0].Policy.Noise, datasets[1].Policy.Noise = 1, 1
	return datasets
}

func mustExec(t *testing.T, conn *duckdb.Conn, query string) {
	t.Helper()
	if _, err := conn.ExecContext(context.Background(), query, nil); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// A measure joined across parties is summed only within its bounds, with
// every party's noise; its minimum, maximum, median, spread and average
// are refused. A template of one party's datasets is as before.
func TestMeasuresAcrossParties(t *testing.T) {
	p := NewParser(openDuckDB(t))
	ctx := context.Background()
	datasets := measuresAcrossParties(t, fixture(t))
	check := func(sql string, datasets []*Dataset) error {
		_, err := p.Check(ctx, Template{SQL: sql}, datasets)
		return err
	}
	for _, fn := range []string{"min", "max", "avg", "median", "stddev"} {
		sql := "SELECT c.segment, " + fn + "(c.flag) AS x FROM crm c JOIN views v ON c.email = v.email GROUP BY 1"
		if err := check(sql, datasets); !IsInvalid(err) || !strings.Contains(err.Error(), "different parties") {
			t.Errorf("%s across parties: %v", fn, err)
		}
		// Of one party's datasets alone, as its policy allows.
		if err := check("SELECT c.segment, "+fn+"(c.flag) AS x FROM crm c GROUP BY 1", datasets); err != nil {
			t.Errorf("%s of one party: %v", fn, err)
		}
	}
	sum := "SELECT c.segment, sum(c.flag) AS x FROM crm c JOIN views v ON c.email = v.email GROUP BY 1"
	if err := check(sum, datasets); err != nil {
		t.Errorf("a bounded, noised sum: %v", err)
	}
	unbounded := measuresAcrossParties(t, fixture(t))
	unbounded[0].Policy.Bounds = nil
	if err := check(sum, unbounded); !IsInvalid(err) || !strings.Contains(err.Error(), "BOUNDS (flag BETWEEN lo AND hi)") {
		t.Errorf("a sum without bounds: %v", err)
	}
	quiet := measuresAcrossParties(t, fixture(t))
	quiet[1].Policy.Noise = 0
	if err := check(sum, quiet); !IsInvalid(err) || !strings.Contains(err.Error(), "views has none") {
		t.Errorf("a sum without the other party's noise: %v", err)
	}

	// Bounds are a party's, on its own measures, and include 0: with lo
	// above it, one person would move a sum by up to hi, not hi - lo.
	cols := []string{"email", "spend", "segment"}
	for _, bad := range []Policy{
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"segment": {0, 1}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"spend": {5, 5}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"nope": {0, 1}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"spend": {1e6, 1e6 + 1}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"spend": {-2, -1}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"spend": {0, 1e16}}},
		{Join: []string{"email"}, Aggregates: []Aggregate{{"sum", "spend"}}, Bounds: map[string]Bound{"spend": {math.Inf(-1), 0}}},
	} {
		if err := bad.Check(cols); !IsInvalid(err) {
			t.Errorf("%+v: want refused, got %v", bad.Bounds, err)
		}
	}
	// With noise, a dataset allows ten runs a day unless it says otherwise:
	// averaging n runs shrinks noise only by the square root of n.
	noisy := Policy{Join: []string{"email"}, Noise: 1}
	if err := noisy.Check(cols); err != nil || noisy.RunsPerDay != DefaultNoisyRunsPerDay {
		t.Errorf("runs per day with noise: %d, %v", noisy.RunsPerDay, err)
	}
}

// The agency sets flag to 1 for one person and 0 for everyone else, and
// sums it over a group far above the thresholds: unbounded and without
// noise, the sum was over zero exactly when that person was in the TV
// maker's data, in one run. Now each person adds at most the bounds' width
// (1), however many rows they have, under noise of the same scale.
func TestIndicatorSumStaysInTheNoise(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	datasets := measuresAcrossParties(t, conn)
	mustExec(t, conn, "UPDATE marts.crm_audience SET flag = 1 WHERE email = 'e7'")
	mustExec(t, conn, "INSERT INTO partners.tvco_views SELECT v.* FROM partners.tvco_views v, range(50) WHERE v.email = 'e7'")
	noised := false
	// Each plan has a seed of its own, as each run does.
	for range 5 {
		pl := planOf(t, p, `SELECT c.segment, count(DISTINCT c.email) AS matched, sum(c.flag) AS total_flag
			FROM crm c JOIN views v ON c.email = v.email GROUP BY 1 ORDER BY 1`, nil, datasets, nil)
		if pl.Cost != 2 || !strings.Contains(pl.Groups, "least(CAST(1 AS DOUBLE)") {
			t.Fatalf("cost %d, plan %s", pl.Cost, pl.Groups)
		}
		rows := mustRun(t, conn, pl)
		if len(rows) == 0 || !strings.HasPrefix(rows[0], "big|") {
			t.Fatalf("rows: %v", rows)
		}
		var flag float64
		fmt.Sscan(strings.Split(rows[0], "|")[2], &flag)
		// Its 51 rows would have summed to 51; one person adds at most 1,
		// and noise of scale 1 is within 24 but for one draw in 10^10.
		if flag > 25 || flag < -25 {
			t.Errorf("one person's flag counts %v", flag)
		}
		noised = noised || flag != 1
	}
	if !noised {
		t.Error("the sum has no noise")
	}
}

// The agency moves one person from one region to the other of its CRM
// between two runs of a sum of the TV maker's minutes by region: the
// difference was that person's minutes. Now it is at most the bounds'
// width (6) a person, under noise of scale 6 in each run.
func TestMovingOnePersonShowsOnlyNoise(t *testing.T) {
	conn := fixture(t)
	p := NewParser(openDuckDB(t))
	datasets := measuresAcrossParties(t, conn)
	// e7, of the west, watched 50 times more, 6 minutes each.
	mustExec(t, conn, "INSERT INTO partners.tvco_views SELECT v.email, v.device, v.genre, v.day, v.grp, 6 FROM partners.tvco_views v, range(50) WHERE v.email = 'e7'")
	pl := planOf(t, p, `SELECT c.region, sum(v.minutes) AS minutes FROM crm c JOIN views v ON c.email = v.email GROUP BY 1 ORDER BY 1`,
		nil, datasets, nil)
	west := func() float64 {
		rows := mustRun(t, conn, pl)
		if len(rows) != 2 || !strings.HasPrefix(rows[1], "west|") {
			t.Fatalf("rows: %v", rows)
		}
		var v float64
		fmt.Sscan(strings.Split(rows[1], "|")[1], &v)
		return v
	}
	before := west()
	mustExec(t, conn, "UPDATE marts.crm_audience SET region = 'east' WHERE email = 'e7'")
	after := west()
	// Unbounded, the difference was e7's 300 minutes and more; bounded, e7
	// moves the sum by 6 at most, and two draws of noise of scale 6 differ
	// by less than 114 but for one pair in 10^8.
	if d := before - after; d > 120 || d < -120 {
		t.Errorf("moving one person moved the sum by %v", d)
	}
}

// An error is an answer: a DECIMAL(38,0) measure's per-person total
// overflowed only when the target's two rows joined the TV maker's, so
// whether a run failed told whether the target was in its data. Each value
// is clamped, as a DOUBLE, before it is summed, and no run fails.
func TestOverflowIsNoAnswer(t *testing.T) {
	p := NewParser(openDuckDB(t))
	for _, target := range []string{seen, unseen} {
		conn := fixture(t)
		mustExec(t, conn, "ALTER TABLE marts.crm_audience ADD COLUMN flag DECIMAL(38,0) DEFAULT 0")
		mustExec(t, conn, "UPDATE marts.crm_audience SET flag = 90000000000000000000000000000000000000 WHERE email = '"+target+"'")
		mustExec(t, conn, "INSERT INTO marts.crm_audience SELECT * FROM marts.crm_audience WHERE email = '"+target+"'")
		datasets := testDatasets()
		datasets[0].Policy.Aggregates = []Aggregate{{"sum", "flag"}}
		datasets[0].Policy.Bounds = map[string]Bound{"flag": {0, 1}}
		datasets[0].Policy.Noise, datasets[1].Policy.Noise = 1, 1
		pl := planOf(t, p, `SELECT c.segment, sum(c.flag) AS f FROM crm c JOIN views v ON c.email = v.email GROUP BY 1`, nil, datasets, nil)
		if rows := mustRun(t, conn, pl); len(rows) == 0 {
			t.Errorf("%s: no rows", target)
		}
	}
}
