package distributed

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// householdReport is shaped like a profile report of an audience's
// households: a common table expression keeps the households of a view
// over a table (a subquery with column aliases) whose ids are those of an
// audience's people, picked by a scalar subquery's version and joined to
// a view of people; two parts unnest each household's groups, one with
// windows over its aggregates, and the last reads a scalar subquery.
const householdReport = `WITH audience AS (
  SELECT part_date, child_groups, weight, hh_id FROM v_households
  WHERE hh_id IN (SELECT DISTINCT hh_id FROM (SELECT a.person_id, b.hh_id FROM ((SELECT person_id FROM lookup
      WHERE aud = 'a1' AND ver = (SELECT max(ver) FROM meta WHERE aud = 'a1'))) AS a INNER JOIN v_people AS b ON a.person_id = b.person_id))),
categories AS (
  SELECT f."key" AS category, CAST(f."value" AS VARCHAR) AS value, count(1) AS n, sum(weight) AS weighted,
         sum(count(1)) OVER () AS total, sum(sum(weight)) OVER () AS weighted_total
  FROM audience AS a, json_each(a.child_groups) AS f
  WHERE a.part_date BETWEEN DATE '2000-01-01' AND DATE '2050-01-01'
  GROUP BY f."key", f."value"),
fill AS (
  SELECT 1 - (CAST(count(CASE WHEN vals = 0 THEN 1 END) AS DOUBLE) / nullif(count(1), 0)) AS fill_rate FROM (
    SELECT hh_id, sum(CAST(f."value" AS INTEGER)) AS vals FROM audience AS a, json_each(a.child_groups) AS f
    WHERE a.part_date BETWEEN DATE '2000-01-01' AND DATE '2050-01-01' GROUP BY hh_id)),
calc AS (
  SELECT category, CASE WHEN sum(weighted) = 0 THEN 0 ELSE sum(CASE WHEN value = '1' THEN weighted ELSE 0 END) / sum(weighted) END AS balanced,
         sum(CASE WHEN value = '1' THEN n ELSE 0 END) / sum(n) AS unbalanced
  FROM categories GROUP BY category)
SELECT round(sum(balanced), 6) AS balanced, round(sum(unbalanced), 6) AS unbalanced, (SELECT round(fill_rate, 6) FROM fill) AS fill_rate FROM calc`

// householdHarness adds the tables and views householdReport reads.
func householdHarness(t *testing.T) *harness {
	h := newHarness(t, 0.01, 2, 3)
	h.addTable("households", `(SELECT i AS hh_id, DATE '2026-01-01' + (i % 30)::INTEGER AS part_date,
		'{"0_2": "' || (i % 2) || '", "3_5": "' || (i % 3 % 2) || '", "6_10": "' || (i % 5 % 2) || '"}' AS child_groups,
		1.0 + (i % 7) / 10 AS weight FROM range(20000) t(i))`, 4)
	h.addTable("people", `(SELECT i AS person_id, i % 20000 AS hh_id, ['f', 'm'][1 + i % 2] AS gender FROM range(50000) t(i))`, 4)
	h.addTable("lookup", `(SELECT ['a1', 'a2'][1 + i % 2] AS aud, 1 + (i % 3) AS ver, (i * 7) % 50000 AS person_id FROM range(60000) t(i))`, 4)
	h.addTable("meta", `(SELECT a AS aud, v AS ver FROM (VALUES ('a1'), ('a2')) x(a), range(1, 4) y(v))`, 1)
	h.addView("v_households", `SELECT part_date, hh_id, json(child_groups) AS child_groups, weight
		FROM (SELECT * FROM households) AS _v(hh_id, part_date, child_groups, weight)`)
	h.addView("v_people", `SELECT * FROM people`)
	// Every table but the audiences' versions too large for every worker
	// to read whole.
	h.co.maxBroadcast = h.tables["meta"].Bytes
	return h
}

// TestHouseholdReportPlans plans householdReport in bounded time and runs
// it on workers with one machine's rows. Its planning once recursed without
// end: the audience's semi join reads the people of a subquery joining
// two subqueries, whose tables are too deep to partition, so that subquery
// became a relation, of the columns read (projectRelation); producing it,
// its own SELECT, a projection of the subquery, was not taken for one, as
// the scalar subquery deep in its FROM counted against it, and so the
// subquery became a relation again, and again.
func TestHouseholdReportPlans(t *testing.T) {
	h := householdHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	p, err := h.co.Plan(ctx, householdReport, harnessEnv{h}, ModeOn)
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("planning took %s (%v)", took, err)
	}
	if err != nil {
		t.Fatal(err)
	}
	// Not by the guards against such recursion, which would keep the
	// audience on the coordinator.
	for _, r := range p.local {
		if strings.Contains(r, "computed from itself") || strings.Contains(r, "nest more than") {
			t.Errorf("planned by a guard: %s", r)
		}
	}
	if !h.check(householdReport) {
		t.Error("ran on one machine")
	}
}

// TestPlanningGivesUp checks that planning a forced statement ends with the
// statement's own deadline, and that planning one whose relations nest
// too deep gives up on them: a chain of common table expressions, each
// grouping the last, nests a relation in each.
func TestPlanningGivesUp(t *testing.T) {
	h := householdHarness(t)
	ctx := context.Background()

	// The statement's timeout, which ends planning as it ends the run.
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := h.co.Plan(short, householdReport, slowEnv{harnessEnv: harnessEnv{h}, delay: time.Second}, ModeOn)
	if err == nil || IsUnsupported(err) {
		t.Errorf("planned past the statement's deadline: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("gave up after %s", took)
	}

	// Too deep: relations nest past maxRelationDepth, each produced for
	// the next, which planning stops at rather than recurse on.
	const limit = 20 * time.Second
	var b strings.Builder
	b.WriteString("WITH c0 AS (SELECT hh_id % 5000 AS k, count(*) AS n FROM households GROUP BY 1)")
	const chain = maxRelationDepth + 6
	for i := 1; i <= chain; i++ {
		fmt.Fprintf(&b, ", c%d AS (SELECT k %% %d AS k, sum(n) AS n FROM c%d GROUP BY 1)", i, 5000-i, i-1)
	}
	fmt.Fprintf(&b, " SELECT count(*), sum(n) FROM c%d", chain)
	q := b.String()
	start = time.Now()
	p, err := h.co.Plan(ctx, q, harnessEnv{h}, ModeOn)
	if took := time.Since(start); took > limit+5*time.Second {
		t.Errorf("planning the chain took %s", took)
	}
	switch {
	case err != nil && !IsUnsupported(err):
		t.Fatal(err)
	case err == nil && !slices.ContainsFunc(p.local, func(r string) bool { return strings.Contains(r, "nest more than") }):
		// The parts too deep stay on the coordinator; those within the
		// depth run on workers.
		t.Errorf("planned a chain of %d relations, none too deep: %v", chain, p.local)
	}
}
